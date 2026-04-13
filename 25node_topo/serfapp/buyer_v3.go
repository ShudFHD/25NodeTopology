// cmd/acp2p-emitter/main.go
package main

/*
Usage examples:

# Random (original behavior):
./buyer_v2 \
  -rpc-addr 127.0.0.1:7373 \
  -event buyer.request \
  -ifname eth0 \
  -http-host 0.0.0.0 -http-port 8090 \
  -pause-min 25s -pause-max 45s \
  -lambda-vcpu 2 -lambda-ram 3 -lambda-storage 5 -lambda-vgpu 1

# Manual at startup (JSON file):
./buyer \
  -rpc-addr 127.0.0.1:7373 \
  -event buyer.request \
  -ifname eth0 \
  -http-host 0.0.0.0 -http-port 8090 \
  -manual-json /opt/serfapp/buyer.json

# Manual at runtime (HTTP):
POST http://<host>:8090/buyer
Content-Type: application/json
{
  "ip": "10.0.1.11",
  "resources": {
    "vcpu":    {"demand_per_unit": 2, "score": 2.2, "budget": 2.5},
    "ram":     {"demand_per_unit": 4, "score": 1.8, "budget": 2.3},
    "storage": {"demand_per_unit": 8, "score": 2.0, "budget": 2.2},
    "vgpu":    {"demand_per_unit": 0, "score": 0.0, "budget": 0.0}
  }
}

# Clear manual mode (resume random):
POST http://<host>:8090/buyer/manual/clear
*/

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	serfclient "github.com/hashicorp/serf/client"
)

// ---------------- Shared schema ----------------
type BuyerResource struct {
	DemandPerUnit int     `json:"demand_per_unit"`
	Score         float64 `json:"score"`
	Budget        float64 `json:"budget"`
}
type BuyerRequest struct {
	IP        string                   `json:"ip"`
	Resources map[string]BuyerResource `json:"resources"`
	Time      string                   `json:"time,omitempty"`
}

// ---------------- Logging helpers ----------------
func infof(format string, a ...any) { log.Printf("[INFO] "+format, a...) }
func warnf(format string, a ...any) { log.Printf("[WARN] "+format, a...) }
func errf(format string, a ...any)  { log.Printf("[ERROR] "+format, a...) }
func dbg(format string, a ...any)   { log.Printf("[DEBUG] "+format, a...) }

// ---------------- Small utils ----------------
func ipv4ForInterface(ifName string) (string, error) {
	ifi, err := net.InterfaceByName(ifName)
	if err != nil {
		return "", fmt.Errorf("interface %q: %w", ifName, err)
	}
	addrs, err := ifi.Addrs()
	if err != nil {
		return "", fmt.Errorf("interface %q addrs: %w", ifName, err)
	}
	for _, a := range addrs {
		var ip net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() {
			continue
		}
		if p4 := ip.To4(); p4 != nil {
			return p4.String(), nil
		}
	}
	return "", fmt.Errorf("no IPv4 found on interface %q", ifName)
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
func uniform2(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}

// Poisson via Knuth (for non-negative ints)
func poisson(lambda float64) int {
	if lambda <= 0 {
		return 0
	}
	L := math.Exp(-lambda)
	k := 0
	p := 1.0
	for p > L {
		k++
		p *= rand.Float64()
	}
	return k - 1
}

// Rand duration in [min,max]
func randRangeDur(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	return min + time.Duration(rand.Int63n(int64(max-min)+1))
}

// ---------------- HTTP state ----------------
type latestStore struct {
	mu     sync.RWMutex
	curr   BuyerRequest
	manual int32 // 0=false, 1=true
}

func (s *latestStore) set(br BuyerRequest) {
	s.mu.Lock()
	s.curr = br
	s.mu.Unlock()
}
func (s *latestStore) get() BuyerRequest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.curr
}
func (s *latestStore) setManual(on bool) {
	if on {
		atomic.StoreInt32(&s.manual, 1)
	} else {
		atomic.StoreInt32(&s.manual, 0)
	}
}
func (s *latestStore) isManual() bool { return atomic.LoadInt32(&s.manual) == 1 }

// ---------------- Main ----------------
func main() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	var (
		// Serf
		rpcAddr   = flag.String("rpc-addr", "127.0.0.1:7373", "Serf RPC address")
		eventName = flag.String("event", "buyer.request", "Serf user-event name")

		// Identity
		ip     = flag.String("ip", "", "Buyer IP (required if -ifname empty)")
		ifname = flag.String("ifname", "", "Interface to auto-detect IPv4 (used if -ip empty)")

		// HTTP
		httpHost = flag.String("http-host", "0.0.0.0", "HTTP bind host")
		httpPort = flag.Int("http-port", 8090, "HTTP bind port")

		// SINGLE-SHOT cadence (instead of busy/idle/Poisson stream)
		pauseMin = flag.Duration("pause-min", 20*time.Second, "Minimum pause between single emits")
		pauseMax = flag.Duration("pause-max", 40*time.Second, "Maximum pause between single emits")

		// Demand distributions (means for Poisson)
		lCPU     = flag.Float64("lambda-vcpu", 2.0, "Poisson mean for vCPU demand")
		lRAM     = flag.Float64("lambda-ram", 3.0, "Poisson mean for RAM demand")
		lStorage = flag.Float64("lambda-storage", 5.0, "Poisson mean for Storage demand")
		lGPU     = flag.Float64("lambda-vgpu", 1.0, "Poisson mean for vGPU demand")

		// Score & budget bounds (uniform in [min,max], rounded to 2 dp)
		scoreMin = flag.Float64("score-min", 1.0, "Min score")
		scoreMax = flag.Float64("score-max", 3.0, "Max score")
		budMin   = flag.Float64("budget-min", 2.0, "Min budget")
		budMax   = flag.Float64("budget-max", 3.0, "Max budget")

		// Manual input at startup (optional)
		manualJSON = flag.String("manual-json", "", "If provided, load BuyerRequest JSON and switch to manual mode")
		verbose    = flag.Bool("v", false, "Verbose debug logging")
	)
	flag.Parse()

	if *ip == "" && *ifname == "" {
		log.Fatal("must provide -ip or -ifname")
	}
	if *ip == "" {
		autoIP, err := ipv4ForInterface(*ifname)
		if err != nil {
			log.Fatalf("auto-detect IP from -ifname=%s: %v", *ifname, err)
		}
		*ip = autoIP
	}
	if *verbose {
		infof("starting single-shot emitter ip=%s serf=%s event=%s http=%s:%d pause=[%s,%s]",
			*ip, *rpcAddr, *eventName, *httpHost, *httpPort, pauseMin.String(), pauseMax.String())
	}

	// Serf RPC
	rc, err := serfclient.NewRPCClient(*rpcAddr)
	if err != nil {
		log.Fatalf("serf RPC connect: %v", err)
	}
	defer rc.Close()

	// Latest request store
	store := &latestStore{}

	// If manual JSON provided at startup, load it and enable manual mode.
	if *manualJSON != "" {
		f, err := os.Open(*manualJSON)
		if err != nil {
			log.Fatalf("open -manual-json: %v", err)
		}
		defer f.Close()
		var br BuyerRequest
		if err := json.NewDecoder(f).Decode(&br); err != nil {
			log.Fatalf("decode -manual-json: %v", err)
		}
		normalizeManual(&br, *ip)
		store.set(br)
		store.setManual(true)
		infof("manual mode enabled from %s", *manualJSON)
	}

	// HTTP server
	mux := http.NewServeMux()

	// GET returns current request (manual or last random).
	mux.HandleFunc("/buyer", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			br := store.get()
			if br.IP == "" {
				http.Error(w, "no demand generated yet", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(br)
		case http.MethodPost:
			// POST sets manual request and turns on manual mode.
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read body error", http.StatusBadRequest)
				return
			}
			var br BuyerRequest
			if err := json.Unmarshal(body, &br); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			normalizeManual(&br, *ip)
			if len(br.Resources) == 0 {
				http.Error(w, "resources required", http.StatusBadRequest)
				return
			}
			store.set(br)
			store.setManual(true)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status":      "ok",
				"manual_mode": true,
				"buyer":       br,
			})
			infof("manual mode enabled via POST /buyer")
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// POST to clear manual mode (resume random generation).
	mux.HandleFunc("/buyer/manual/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		store.setManual(false)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "manual_mode": false})
		infof("manual mode cleared (random generation will resume)")
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", *httpHost, *httpPort),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		infof("HTTP serving demand at http://%s:%d/buyer", *httpHost, *httpPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errf("http server error: %v", err)
		}
	}()

	// Graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer func() {
		shCtx, shCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shCancel()
		_ = srv.Shutdown(shCtx)
	}()

	// Single-shot loop: on each tick, either emit MANUAL (if enabled) or RANDOM.
	for {
		var br BuyerRequest
		if store.isManual() {
			// Reuse the manual request but refresh IP/time.
			br = store.get()
			normalizeManual(&br, *ip)
		} else {
			// Original behavior: generate random demand.
			br = makeRandomBuyer(*ip, *lCPU, *lRAM, *lStorage, *lGPU, *scoreMin, *scoreMax, *budMin, *budMax)
			store.set(br)
		}

		// Broadcast Serf user event
		payload, _ := json.Marshal(br)
		if err := rc.UserEvent(*eventName, payload, false); err != nil {
			warnf("send user-event %q failed: %v", *eventName, err)
		} else {
			infof("broadcast user:%s ip=%s at %s (manual=%v)", *eventName, br.IP, br.Time, store.isManual())
		}

		// JUST FOR NOW:: QUICK FIX. NEEDS TO BE CHANGED LATER
		go func() {
		    _, err := http.Post("http://localhost:4041/trigger", "application/json", nil)
		    if err != nil {
		        warnf("trigger POST failed: %v", err)
		    }
		}()

		// Sleep a single randomized pause
		pause := randRangeDur(*pauseMin, *pauseMax)
		infof("sleeping for %s before next single-shot", pause)
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			infof("shutting down (signal received)")
			return
		case <-timer.C:
		}
	}
}

// normalizeManual ensures IP and timestamp are set; keeps user-provided resources intact.
func normalizeManual(br *BuyerRequest, defaultIP string) {
	if br.IP == "" {
		br.IP = defaultIP
	}
	if br.Resources == nil {
		br.Resources = map[string]BuyerResource{}
	}
	// Ensure canonical keys exist (optional; leave as-is if user omits).
	need := []string{"vcpu", "ram", "storage", "vgpu"}
	for _, k := range need {
		if _, ok := br.Resources[k]; !ok {
			// keep missing keys absent; discovery side should tolerate zeros/missing.
			continue
		}
	}
	br.Time = time.Now().Format(time.RFC3339)
}

func makeRandomBuyer(ip string, lCPU, lRAM, lStorage, lGPU, sMin, sMax, bMin, bMax float64) BuyerRequest {
	now := time.Now().Format(time.RFC3339)

	vcpu := poisson(lCPU)
	ram := poisson(lRAM)
	storage := poisson(lStorage)
	vgpu := poisson(lGPU)
	if vcpu < 0 {
		vcpu = 0
	}
	if ram < 0 {
		ram = 0
	}
	if storage < 0 {
		storage = 0
	}
	if vgpu < 0 {
		vgpu = 0
	}

	score := func() float64 { return round2(uniform2(sMin, sMax)) }
	budget := func() float64 { return round2(uniform2(bMin, bMax)) }

	return BuyerRequest{
		IP:   ip,
		Time: now,
		Resources: map[string]BuyerResource{
			"storage": {DemandPerUnit: storage, Score: score(), Budget: budget()},
			"vcpu":    {DemandPerUnit: vcpu, Score: score(), Budget: budget()},
			"ram":     {DemandPerUnit: ram, Score: score(), Budget: budget()},
			"vgpu":    {DemandPerUnit: vgpu, Score: score(), Budget: budget()},
		},
	}
}
