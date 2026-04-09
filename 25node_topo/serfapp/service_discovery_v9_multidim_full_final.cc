#include <cctype>
#include <chrono>
// tests/serf_driver.cc  multidim+pruning
// Lawder Hilbert DB range query driver implementing professor Step 1–3
// with an efficient "sphere -> sub-quadrants" mapping.
//
// Core idea for performance in multidimensional search:
//   Use geometric pruning (minDist/maxDist) AND occupancy pruning
//   (skip boxes that contain no dataset points) to avoid exploring
//   billions of empty sub-quadrants.
//
// THIS VERSION:
//   - Generalizes the Hilbert + pruning core to dynamic dimensionality.
//   - Changes inputs/outputs to a Python-service-like interface.
//   - Removes TP/FP/FN CSV files, repeat-offender CSV logic, resource-only DB mode,
//     and combined experiment CSV outputs.
//   - Loads geometry once from --geom-url.
//   - Determines query node from /opt/serfapp/node.json or hostname.
//   - Uses ./serf members for local LAN members each cycle.
//   - Widens the local RTT threshold adaptively via build_threshold_plan(...).
//   - Falls back to a broad cluster-head remote query when local search fails or saturates.
//   - Returns JSON to stdout and optionally via a live HTTP endpoint.
//
// CLI compatibility target:
//   ./serf_driver \
//      --geom-url http://172.20.20.17:4040/cluster-status \
//      --rtt-threshold-ms 12 \
//      --rpc-addr 127.0.0.1:7373 --timeout-s 8 \
//      --pct-start 0.25 --max-steps 8 \
//      --sort score_per_cpu --limit 30 \
//      --http-serve --http-host 0.0.0.0 --http-port 4041 --http-path /hilbert-output \
//      --buyer-url http://127.0.0.1:8090/buyer \
//      --loop --busy-secs 50
//
// Notes:
//   - Hilbert order is kept internal in this service wrapper.
//   - pct-start / max-steps / dump-hilbert are accepted for CLI compatibility.
//   - Adaptive threshold widening is implemented through build_threshold_plan(...),
//     with percentage growth and a minimum useful step.
//   - Local discovery is location-intersect-resource; remote discovery is a broad
//     cluster-head query followed by local resource filtering/intersection.
//   - Programming language remains C++.
//
// IMPORTANT ALIGNMENT WITH PROF EMAIL:
//   Step 2 is a SHIFTED coordinate system:
//     - (0,0,...) is the leftmost/bottommost (per-dimension minimum) node
//     - units are latency units (ms) and identical across dimensions
//   We do NOT apply any extra global-span scaling/normalization.
//
// Compatibility fix:
//   Accept JSON root as either an array (old) or an object containing "nodes" array (new).

#ifdef DEV
  #include "../db/db.h"
  #include "../hilbert/hilbert.h"
#else
  #include "db.h"
#endif

#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <array>
#include <limits>
#include <cmath>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include <sys/stat.h>
#include <cstdint>
#include <climits>
#include <cerrno>
#include <thread>
#include <mutex>
#include <atomic>
#include <set>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "../tests/third-party/json.hpp"
using json = nlohmann::json;
using namespace std;

// ============================================================
// Service constants
// ============================================================
static constexpr int SERVICE_HORDER = 10;
static constexpr bool SERVICE_REBUILD_DB = false;
static constexpr bool SERVICE_DEBUG_CORE = false;
static constexpr bool SERVICE_VEC_ALREADY_MS = false;
static constexpr double SERVICE_LATENCY_MAX_MS = 83.283;
static constexpr int DEFAULT_TIMEOUT_S = 8;
static constexpr const char* NODE_JSON_PATH = "/opt/serfapp/node.json";

// Box hit record kept from original code family.
struct BoxHit {
    std::string box_key;
    std::string node_name; // full name
};

// ============================================================
// Service wrapper structs
// ============================================================
struct ServiceArgs {
    std::string geom_url;
    double rtt_threshold_ms = 0.0;

    std::string rpc_addr = "127.0.0.1:7373";
    int timeout_s = DEFAULT_TIMEOUT_S;

    // accepted for CLI compatibility with the Python script
    bool dump_hilbert = false;
    double pct_start = 0.02;
    int max_steps = 6;

    // resource thresholds
    int min_cpu = 0;
    double min_ram = 0.0;
    int min_storage = 0;
    int min_gpu = 0;

    // score floors
    double min_score_per_cpu = 0.0;
    double min_score_per_ram = 0.0;
    double min_score_per_storage = 0.0;
    double min_score_per_gpu = 0.0;

    std::string sort = "score_per_cpu";
    int limit = 0;

    // HTTP / loop
    bool http_serve = false;
    std::string http_host = "0.0.0.0";
    int http_port = 4041;
    std::string http_path = "/hilbert-output";
    std::string buyer_url;
    bool loop = false;
    double busy_secs = 30.0;
};

struct MemberInfo {
    std::string name;
    std::string ip;

    long long cpu = 0;
    double ram = std::numeric_limits<double>::quiet_NaN();
    long long storage = 0;
    long long gpu = 0;

    double price_per_cpu = std::numeric_limits<double>::quiet_NaN();
    double price_per_ram = std::numeric_limits<double>::quiet_NaN();
    double price_per_storage = std::numeric_limits<double>::quiet_NaN();
    double price_per_gpu = std::numeric_limits<double>::quiet_NaN();

    double score_per_cpu = std::numeric_limits<double>::quiet_NaN();
    double score_per_ram = std::numeric_limits<double>::quiet_NaN();
    double score_per_storage = std::numeric_limits<double>::quiet_NaN();
    double score_per_gpu = std::numeric_limits<double>::quiet_NaN();

    std::string origin; // local | wan
};

struct LiveState {
    std::mutex lock;
    json payload = json{{"scope","none"},{"results",json::array()}};
};

// ---------------- helpers ----------------
static bool read_all(const string& path, string& out) {
    ifstream f(path.c_str(), ios::in | ios::binary);
    if (!f) return false;
    ostringstream ss;
    ss << f.rdbuf();
    out = ss.str();
    return true;
}

static bool file_exists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

static bool ensure_dir_exists(const std::string& dir) {
    int rc = mkdir(dir.c_str(), 0755);
    if (rc == 0) return true;
    return errno == EEXIST;
}

static bool read_json_file(const std::string& path, json& out) {
    std::string text;
    if (!read_all(path, text)) return false;
    try { out = json::parse(text); }
    catch (...) { return false; }
    return true;
}

static bool extract_nodes_array(const json& root, json& out_nodes) {
    if (root.is_array()) { out_nodes = root; return true; }
    if (root.is_object() && root.contains("nodes") && root["nodes"].is_array()) {
        out_nodes = root["nodes"]; return true;
    }
    return false;
}

static string point_key(const array<PU_int,5>& p) {
    return to_string(p[0]) + "," + to_string(p[1]) + "," + to_string(p[2]) + "," +
           to_string(p[3]) + "," + to_string(p[4]);
}

static PU_int clamp_pu(long long v, PU_int lo, PU_int hi) {
    if (v < (long long)lo) return lo;
    if (v > (long long)hi) return hi;
    return (PU_int)v;
}

static std::string short_name(const std::string& full) {
    size_t pos = full.rfind('-');
    if (pos == std::string::npos) return full;
    if (pos + 1 >= full.size()) return full;
    return full.substr(pos + 1);
}

static std::string lower_str(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return (char)std::tolower(c); });
    return s;
}

static bool file_empty_or_missing(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) return true;
    return (st.st_size == 0);
}

static std::string csv_escape(const std::string& s) {
    bool need = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n' || c == '\r') { need = true; break; }
    }
    if (!need) return s;
    std::string out;
    out.reserve(s.size() + 8);
    out.push_back('"');
    for (char c : s) {
        if (c == '"') out.push_back('"');
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

static std::string join_names_sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    std::string out;
    for (size_t i = 0; i < v.size(); i++) {
        if (i) out += " ";
        out += v[i];
    }
    return out;
}

static inline std::string fmt_resources_units(const std::array<long long,4>& rv) {
    std::ostringstream oss;
    oss << " (cpu=" << rv[0] << " cores"
        << ", ram=" << rv[1] << " GB"
        << ", gpu=" << rv[2] << " GPUs"
        << ", storage=" << rv[3] << " GB)";
    return oss.str();
}

static std::vector<std::string> to_short_names(const std::vector<std::string>& in) {
    std::vector<std::string> out;
    out.reserve(in.size());
    for (const auto& s : in) out.push_back(short_name(s));
    return out;
}

// ============================================================
// Shell / URL helpers
// ============================================================
static std::string shell_quote(const std::string& s) {
    std::string out = "'";
    for (char c : s) {
        if (c == '\'') out += "'\\''";
        else out += c;
    }
    out += "'";
    return out;
}

static bool exec_capture(const std::string& cmd, std::string& out, int* rc = nullptr) {
    out.clear();
    FILE* fp = popen(cmd.c_str(), "r");
    if (!fp) return false;

    char buf[4096];
    while (fgets(buf, sizeof(buf), fp)) out += buf;

    int status = pclose(fp);
    if (rc) *rc = status;
    return true;
}

static bool load_json_url(const std::string& url, int timeout_s, json& out) {
    std::string body;
    int rc = 0;
    std::string cmd = "curl -fsSL --max-time " + std::to_string(timeout_s) + " " + shell_quote(url);
    if (!exec_capture(cmd, body, &rc) || rc != 0) return false;
    try {
        out = json::parse(body);
        return true;
    } catch (...) {
        return false;
    }
}

static long long to_ll_safe(const json& v) {
    try {
        if (v.is_number_integer()) return v.get<long long>();
        if (v.is_number_unsigned()) return (long long)v.get<unsigned long long>();
        if (v.is_number_float()) return (long long)std::llround(v.get<double>());
        if (v.is_string()) return (long long)std::llround(std::stod(v.get<std::string>()));
    } catch (...) {}
    return 0;
}

static double to_d_safe(const json& v) {
    try {
        if (v.is_number()) return v.get<double>();
        if (v.is_string()) return std::stod(v.get<std::string>());
    } catch (...) {}
    return std::numeric_limits<double>::quiet_NaN();
}

// ============================================================
// Local node meta
// ============================================================
static std::string get_hostname_str() {
    char buf[256];
    if (gethostname(buf, sizeof(buf)) == 0) {
        buf[sizeof(buf)-1] = '\0';
        return std::string(buf);
    }
    return "";
}

static std::string read_query_node_name() {
    json root;
    if (read_json_file(NODE_JSON_PATH, root) && root.is_object()) {
        if (root.contains("node_name") && root["node_name"].is_string()) {
            return root["node_name"].get<std::string>();
        }
        if (root.contains("name") && root["name"].is_string()) {
            return root["name"].get<std::string>();
        }
    }

    std::string hn = get_hostname_str();
    if (!hn.empty()) return hn;

    throw std::runtime_error("Cannot determine node name from /opt/serfapp/node.json or hostname.");
}


// ---------------- generic integer geometry ----------------
using VecLL = std::vector<long long>;
using VecPU = std::vector<PU_int>;
using VecD  = std::vector<double>;

static std::string point_key_vec(const VecPU& p) {
    std::string s;
    for (size_t i = 0; i < p.size(); ++i) {
        if (i) s.push_back(',');
        s += std::to_string(p[i]);
    }
    return s;
}

static bool is_single_cell(const VecLL& lo, const VecLL& hi) {
    if (lo.size() != hi.size()) return false;
    for (size_t d = 0; d < lo.size(); ++d) if (lo[d] != hi[d]) return false;
    return true;
}

static double minDist2_PointToBox_ms_nd(const VecD& c, const VecD& lo, const VecD& hi) {
    double acc = 0.0;
    for (size_t d = 0; d < c.size(); ++d) {
        double x = c[d];
        if (x < lo[d]) { double t = lo[d] - x; acc += t * t; }
        else if (x > hi[d]) { double t = x - hi[d]; acc += t * t; }
    }
    return acc;
}

static double maxDist2_PointToBox_ms_nd(const VecD& c, const VecD& lo, const VecD& hi) {
    double acc = 0.0;
    for (size_t d = 0; d < c.size(); ++d) {
        double a = std::fabs(c[d] - lo[d]);
        double b = std::fabs(c[d] - hi[d]);
        double m = std::max(a, b);
        acc += m * m;
    }
    return acc;
}

struct QueryStats {
    uint64_t open_ok = 0;
    uint64_t open_fail = 0;
    uint64_t fetched_rows = 0;
    uint64_t matched_names = 0;
    uint64_t lawder_box_queries = 0;
};

struct CoverStats {
    uint64_t visited = 0;
    uint64_t pruned_outside = 0;
    uint64_t pruned_empty = 0;
    uint64_t pruned_rtt_lb = 0;
    uint64_t accepted_inside = 0;
    uint64_t accepted_leaf = 0;

    uint64_t pruned_rtt_lb_points = 0;
    uint64_t accepted_leaf_points = 0;
    uint64_t accepted_inside_points = 0;
};

static bool box_has_any_point_nd(DBASE* DB, const VecLL& lo, const VecLL& hi) {
    const int dims = (int)lo.size();
    std::vector<PU_int> LB(dims), UB(dims), result(dims);
    for (int d = 0; d < dims; ++d) { LB[d] = (PU_int)lo[d]; UB[d] = (PU_int)hi[d]; }
    int set_id = -1;
    if (true != DB->db_range_open_set(LB.data(), UB.data(), &set_id)) return false;
    bool any = (true == DB->db_range_fetch_another(set_id, result.data()));
    DB->db_close_set(set_id);
    return any;
}

static void run_one_box_query_nd(
    DBASE* DB,
    const VecLL& lo,
    const VecLL& hi,
    const unordered_map<string, vector<string>>& point_to_names,
    const string& qname,
    unordered_map<string,int>& hilbert_set,
    vector<string>& hilbert_names,
    QueryStats& qs)
{
    const int dims = (int)lo.size();
    std::vector<PU_int> LB(dims), UB(dims), result(dims);
    for (int d = 0; d < dims; ++d) { LB[d] = (PU_int)lo[d]; UB[d] = (PU_int)hi[d]; }
    int set_id = -1;
    qs.lawder_box_queries++;
    if (true == DB->db_range_open_set(LB.data(), UB.data(), &set_id)) {
        qs.open_ok++;
        while (true == DB->db_range_fetch_another(set_id, result.data())) {
            qs.fetched_rows++;
            std::string k = point_key_vec(result);
            auto itp = point_to_names.find(k);
            if (itp == point_to_names.end()) continue;
            for (const std::string& nm : itp->second) {
                if (nm == qname) continue;
                if (hilbert_set.emplace(nm, 1).second) hilbert_names.push_back(nm);
                qs.matched_names++;
            }
        }
        DB->db_close_set(set_id);
    } else {
        qs.open_fail++;
    }
}

static void box_min_height_adj_nd(
    DBASE* DB,
    const unordered_map<string, vector<string>>& point_to_names,
    const unordered_map<string, int>& name_to_idx,
    const vector<double>& heights_ms,
    const vector<double>& adjs_ms,
    const VecLL& lo,
    const VecLL& hi,
    double& out_min_h,
    double& out_min_adj,
    uint64_t& out_point_count)
{
    const int dims = (int)lo.size();
    out_min_h = std::numeric_limits<double>::infinity();
    out_min_adj = std::numeric_limits<double>::infinity();
    out_point_count = 0;

    std::vector<PU_int> LB(dims), UB(dims), result(dims);
    for (int d = 0; d < dims; ++d) { LB[d] = (PU_int)lo[d]; UB[d] = (PU_int)hi[d]; }

    int set_id = -1;
    if (true != DB->db_range_open_set(LB.data(), UB.data(), &set_id)) return;

    while (true == DB->db_range_fetch_another(set_id, result.data())) {
        const std::string k = point_key_vec(result);
        auto itp = point_to_names.find(k);
        if (itp == point_to_names.end()) continue;
        for (const std::string& nm : itp->second) {
            auto iti = name_to_idx.find(nm);
            if (iti == name_to_idx.end()) continue;
            const size_t i = (size_t)iti->second;
            out_point_count++;
            if (heights_ms[i] < out_min_h) out_min_h = heights_ms[i];
            if (adjs_ms[i] < out_min_adj) out_min_adj = adjs_ms[i];
        }
    }

    DB->db_close_set(set_id);
}

static void coverSphereIndexed_nd(
    DBASE* DB,
    int ORDER,
    const VecD& center_ms,
    int T_ms,
    double cell_size_ms,
    const unordered_map<string, vector<string>>& point_to_names,
    const string& qname,
    const unordered_map<string, int>& name_to_idx,
    const vector<double>& heights_ms,
    const vector<double>& adjs_ms,
    double q_height_ms,
    double q_adj_ms,
    unordered_map<string,int>& hilbert_set,
    vector<string>& hilbert_names,
    const VecLL& lo,
    const VecLL& hi,
    int depth,
    CoverStats& st,
    QueryStats& qs)
{
    const int dims = (int)center_ms.size();
    st.visited++;
    const double r2_ms = (double)T_ms * (double)T_ms;

    VecD lo_ms(dims), hi_ms(dims);
    for (int d = 0; d < dims; ++d) {
        lo_ms[d] = (double)lo[d] * cell_size_ms;
        hi_ms[d] = (double)(hi[d] + 1) * cell_size_ms;
    }

    const double minDist2 = minDist2_PointToBox_ms_nd(center_ms, lo_ms, hi_ms);
    if (minDist2 > r2_ms) {
        st.pruned_outside++;
        return;
    }

    if (!box_has_any_point_nd(DB, lo, hi)) {
        st.pruned_empty++;
        return;
    }

    double box_min_h = 0.0, box_min_adj = 0.0;
    uint64_t box_point_count = 0;
    box_min_height_adj_nd(DB, point_to_names, name_to_idx, heights_ms, adjs_ms,
                          lo, hi, box_min_h, box_min_adj, box_point_count);

    if (std::isfinite(box_min_h) && std::isfinite(box_min_adj)) {
        const double min_vec_dist = std::sqrt(minDist2);
        const double base_lb = min_vec_dist + q_height_ms + box_min_h;
        const double best_case_rtt_lb = base_lb + q_adj_ms + box_min_adj;
        if (best_case_rtt_lb > (double)T_ms) {
            st.pruned_rtt_lb++;
            st.pruned_rtt_lb_points += box_point_count;
            return;
        }
    }

    const bool fully_inside = (maxDist2_PointToBox_ms_nd(center_ms, lo_ms, hi_ms) <= r2_ms);
    if (depth >= ORDER || is_single_cell(lo, hi)) {
        st.accepted_leaf++;
        st.accepted_leaf_points += box_point_count;
        if (fully_inside) {
            st.accepted_inside++;
            st.accepted_inside_points += box_point_count;
        }
        run_one_box_query_nd(DB, lo, hi, point_to_names, qname, hilbert_set, hilbert_names, qs);
        return;
    }

    const unsigned long long child_count = 1ULL << dims;
    VecLL mid(dims);
    for (int d = 0; d < dims; ++d) mid[d] = (lo[d] + hi[d]) >> 1;

    for (unsigned long long mask = 0; mask < child_count; ++mask) {
        VecLL clo(dims), chi(dims);
        bool empty = false;
        for (int d = 0; d < dims; ++d) {
            if (mask & (1ULL << d)) { clo[d] = mid[d] + 1; chi[d] = hi[d]; }
            else                    { clo[d] = lo[d];      chi[d] = mid[d]; }
            if (clo[d] > chi[d]) empty = true;
        }
        if (empty) continue;
        coverSphereIndexed_nd(DB, ORDER, center_ms, T_ms, cell_size_ms,
                              point_to_names, qname, name_to_idx,
                              heights_ms, adjs_ms, q_height_ms, q_adj_ms,
                              hilbert_set, hilbert_names,
                              clo, chi, depth + 1, st, qs);
    }
}

// ============================================================
// Service-mode member / filter helpers
// ============================================================
static bool finite_member(const MemberInfo& m) {
    return !m.ip.empty()
        && std::isfinite(m.ram)
        && std::isfinite(m.score_per_cpu)
        && std::isfinite(m.score_per_ram)
        && std::isfinite(m.score_per_storage)
        && std::isfinite(m.score_per_gpu);
}

static std::vector<MemberInfo> drop_nan_members(const std::vector<MemberInfo>& in) {
    std::vector<MemberInfo> out;
    out.reserve(in.size());
    for (const auto& m : in) if (finite_member(m)) out.push_back(m);
    return out;
}

static std::vector<MemberInfo> filter_by_resources(
    const std::vector<MemberInfo>& in,
    const ServiceArgs& args)
{
    std::vector<MemberInfo> out;
    out.reserve(in.size());

    for (const auto& m : in) {
        if (args.min_cpu > 0 && m.cpu < args.min_cpu) continue;
        if (args.min_ram > 0 && m.ram < args.min_ram) continue;
        if (args.min_storage > 0 && m.storage < args.min_storage) continue;
        if (args.min_gpu > 0 && m.gpu < args.min_gpu) continue;
        if (args.min_score_per_cpu > 0 && m.score_per_cpu < args.min_score_per_cpu) continue;
        if (args.min_score_per_ram > 0 && m.score_per_ram < args.min_score_per_ram) continue;
        if (args.min_score_per_storage > 0 && m.score_per_storage < args.min_score_per_storage) continue;
        if (args.min_score_per_gpu > 0 && m.score_per_gpu < args.min_score_per_gpu) continue;

        out.push_back(m);
    }

    return out;
}


static bool member_matches_exact(const MemberInfo& m, const ServiceArgs& args) {
    if (args.min_cpu > 0 && m.cpu < args.min_cpu) return false;
    if (args.min_ram > 0 && m.ram < args.min_ram) return false;
    if (args.min_storage > 0 && m.storage < args.min_storage) return false;
    if (args.min_gpu > 0 && m.gpu < args.min_gpu) return false;
    if (args.min_score_per_cpu > 0 && m.score_per_cpu < args.min_score_per_cpu) return false;
    if (args.min_score_per_ram > 0 && m.score_per_ram < args.min_score_per_ram) return false;
    if (args.min_score_per_storage > 0 && m.score_per_storage < args.min_score_per_storage) return false;
    if (args.min_score_per_gpu > 0 && m.score_per_gpu < args.min_score_per_gpu) return false;
    return true;
}

static std::vector<double> member_resource_vector(const MemberInfo& m) {
    return {
        (double)m.cpu,
        m.ram,
        (double)m.storage,
        (double)m.gpu,
        m.score_per_cpu,
        m.score_per_ram,
        m.score_per_storage,
        m.score_per_gpu
    };
}

static std::unordered_set<std::string> run_resource_db_query(
    const std::vector<MemberInfo>& universe_in,
    const ServiceArgs& args,
    const std::string& dbname_prefix,
    int order = SERVICE_HORDER)
{
    std::unordered_set<std::string> out;
    std::vector<MemberInfo> universe = drop_nan_members(universe_in);
    if (universe.empty()) return out;

    const int dims = 8;
    const PU_int GRID_MAX = (PU_int)((1u << order) - 1u);
    std::vector<std::vector<double>> vals(universe.size(), std::vector<double>(dims));
    std::vector<double> mn(dims, std::numeric_limits<double>::infinity());
    std::vector<double> mx(dims, -std::numeric_limits<double>::infinity());

    for (size_t i = 0; i < universe.size(); ++i) {
        vals[i] = member_resource_vector(universe[i]);
        for (int d = 0; d < dims; ++d) {
            mn[d] = std::min(mn[d], vals[i][d]);
            mx[d] = std::max(mx[d], vals[i][d]);
        }
    }

    std::vector<double> span(dims, 1.0), cell(dims, 1.0);
    for (int d = 0; d < dims; ++d) {
        span[d] = mx[d] - mn[d];
        if (!(span[d] > 0.0) || !std::isfinite(span[d])) span[d] = 1.0;
        cell[d] = span[d] / (double)(1u << order);
        if (!(cell[d] > 0.0) || !std::isfinite(cell[d])) cell[d] = 1.0;
    }

    auto quantize = [&](double x, int d) -> PU_int {
        double shifted = x - mn[d];
        if (shifted < 0.0) shifted = 0.0;
        if (shifted > span[d]) shifted = span[d];
        long long gi = (long long)std::floor(shifted / cell[d]);
        return clamp_pu(gi, (PU_int)0, GRID_MAX);
    };

    std::vector<VecPU> pts(universe.size(), VecPU(dims));
    std::unordered_map<std::string, std::vector<std::string>> point_to_names;
    point_to_names.reserve(universe.size() * 2 + 8);
    for (size_t i = 0; i < universe.size(); ++i) {
        for (int d = 0; d < dims; ++d) pts[i][d] = quantize(vals[i][d], d);
        point_to_names[point_key_vec(pts[i])].push_back(universe[i].name);
    }

    std::vector<double> qlb = {
        (double)args.min_cpu,
        args.min_ram,
        (double)args.min_storage,
        (double)args.min_gpu,
        (args.min_score_per_cpu > 0 ? args.min_score_per_cpu : mn[4]),
        (args.min_score_per_ram > 0 ? args.min_score_per_ram : mn[5]),
        (args.min_score_per_storage > 0 ? args.min_score_per_storage : mn[6]),
        (args.min_score_per_gpu > 0 ? args.min_score_per_gpu : mn[7])
    };
    std::vector<double> qub = {
        mx[0], mx[1], mx[2], mx[3],
        mx[4], mx[5], mx[6], mx[7]
    };

    for (int d = 0; d < dims; ++d) {
        if (qlb[d] < mn[d]) qlb[d] = mn[d];
        if (qub[d] > mx[d]) qub[d] = mx[d];
        if (qlb[d] > qub[d]) return out;
    }

    std::vector<PU_int> LB(dims), UB(dims), result(dims);
    for (int d = 0; d < dims; ++d) {
        LB[d] = quantize(qlb[d], d);
        UB[d] = quantize(qub[d], d);
        if (LB[d] > UB[d]) std::swap(LB[d], UB[d]);
    }

    std::string dbname = dbname_prefix + "_o" + std::to_string(order) + "_pid" + std::to_string((long long)getpid());
    remove((dbname + ".db").c_str());
    remove((dbname + ".idx").c_str());
    remove((dbname + ".inf").c_str());
    remove((dbname + ".fpl").c_str());

    const int BT_NODE_ENTRIES = 10;
    const int BUFFER_PAGES = 10;
    const int PAGE_RECORDS = 200;
    DBASE* DB = new DBASE(dbname, dims, BT_NODE_ENTRIES, BUFFER_PAGES, PAGE_RECORDS);
    if (!DB->db_create()) { delete DB; return out; }
    if (!DB->db_open()) { delete DB; return out; }

    for (size_t i = 0; i < universe.size(); ++i) {
        std::vector<PU_int> p = pts[i];
        if (!DB->db_data_insert(p.data())) {
            DB->db_close();
            delete DB;
            return out;
        }
    }

    int set_id = -1;
    if (true == DB->db_range_open_set(LB.data(), UB.data(), &set_id)) {
        while (true == DB->db_range_fetch_another(set_id, result.data())) {
            auto it = point_to_names.find(point_key_vec(result));
            if (it == point_to_names.end()) continue;
            for (const auto& nm : it->second) out.insert(nm);
        }
        DB->db_close_set(set_id);
    }
    DB->db_close();
    delete DB;

    remove((dbname + ".db").c_str());
    remove((dbname + ".idx").c_str());
    remove((dbname + ".inf").c_str());
    remove((dbname + ".fpl").c_str());

    std::unordered_set<std::string> exact;
    for (const auto& m : universe) {
        if (!out.count(m.name)) continue;
        if (member_matches_exact(m, args)) exact.insert(m.name);
    }
    return exact;
}

static std::vector<MemberInfo> intersect_rows_by_name(
    const std::vector<std::string>& location_names,
    const std::unordered_set<std::string>& resource_names,
    const std::unordered_map<std::string, MemberInfo>& row_map)
{
    std::vector<MemberInfo> rows;
    rows.reserve(location_names.size());
    std::unordered_set<std::string> seen;
    for (const auto& nm : location_names) {
        if (!resource_names.count(nm)) continue;
        if (!seen.emplace(nm).second) continue;
        auto it = row_map.find(nm);
        if (it != row_map.end()) rows.push_back(it->second);
    }
    return rows;
}

static void sort_candidates(std::vector<MemberInfo>& v, const std::string& key) {
    if (v.empty() || key == "none") return;

    auto less_num = [&](auto getter, bool ascending) {
        std::sort(v.begin(), v.end(), [&](const MemberInfo& a, const MemberInfo& b) {
            double av = getter(a);
            double bv = getter(b);
            if (av == bv) return a.name < b.name;
            return ascending ? (av < bv) : (av > bv);
        });
    };

    if      (key == "cpu") less_num([](const MemberInfo& x){ return (double)x.cpu; }, false);
    else if (key == "ram") less_num([](const MemberInfo& x){ return x.ram; }, false);
    else if (key == "storage") less_num([](const MemberInfo& x){ return (double)x.storage; }, false);
    else if (key == "gpu") less_num([](const MemberInfo& x){ return (double)x.gpu; }, false);
    else if (key == "score_per_cpu") less_num([](const MemberInfo& x){ return x.score_per_cpu; }, false);
    else if (key == "score_per_ram") less_num([](const MemberInfo& x){ return x.score_per_ram; }, false);
    else if (key == "score_per_storage") less_num([](const MemberInfo& x){ return x.score_per_storage; }, false);
    else if (key == "score_per_gpu") less_num([](const MemberInfo& x){ return x.score_per_gpu; }, false);
}

// ============================================================
// Serf LAN members and CH remote query
// ============================================================
static std::vector<MemberInfo> get_lan_members(const std::string& rpc_addr) {
    std::vector<MemberInfo> out;

    const int max_attempts = 6;
    const int base_ms = 200;
    const double cap_s = 2.0;

    std::string last_err;

    for (int attempt = 0; attempt < max_attempts; attempt++) {
        std::string cmd = "./serf members -rpc-addr=" + rpc_addr + " -format=json";
        std::string txt;
        int rc = 0;

        if (exec_capture(cmd, txt, &rc) && rc == 0) {
            try {
                json root = json::parse(txt);
                json members = root.contains("members") ? root["members"] : root.value("Members", json::array());
                if (!members.is_array()) return out;

                for (const auto& m : members) {
                    if (!m.is_object()) continue;

                    std::string name = m.value("name", m.value("Name", ""));
                    if (name.empty()) continue;
                    if (lower_str(name).find("-wan") != std::string::npos) continue;

                    json tags = m.contains("tags") ? m["tags"] : m.value("Tags", json::object());
                    if (!tags.is_object()) tags = json::object();

                    MemberInfo r;
                    r.name = name;
                    r.ip = tags.value("ip", m.value("addr", m.value("Addr", "")));
                    if (r.ip.find(':') != std::string::npos) {
                        r.ip = r.ip.substr(0, r.ip.find(':'));
                    }

                    if (tags.contains("cpu")) r.cpu = to_ll_safe(tags["cpu"]);
                    if (tags.contains("ram")) r.ram = to_d_safe(tags["ram"]);
                    if (tags.contains("storage")) r.storage = to_ll_safe(tags["storage"]);
                    if (tags.contains("gpu")) r.gpu = to_ll_safe(tags["gpu"]);
                    if (tags.contains("price_per_cpu")) r.price_per_cpu = to_d_safe(tags["price_per_cpu"]);
                    if (tags.contains("price_per_ram")) r.price_per_ram = to_d_safe(tags["price_per_ram"]);
                    if (tags.contains("price_per_storage")) r.price_per_storage = to_d_safe(tags["price_per_storage"]);
                    if (tags.contains("price_per_gpu")) r.price_per_gpu = to_d_safe(tags["price_per_gpu"]);
                    if (tags.contains("score_per_cpu")) r.score_per_cpu = to_d_safe(tags["score_per_cpu"]);
                    if (tags.contains("score_per_ram")) r.score_per_ram = to_d_safe(tags["score_per_ram"]);
                    if (tags.contains("score_per_storage")) r.score_per_storage = to_d_safe(tags["score_per_storage"]);
                    if (tags.contains("score_per_gpu")) r.score_per_gpu = to_d_safe(tags["score_per_gpu"]);

                    r.origin = "local";
                    out.push_back(r);
                }

                return out;
            } catch (const std::exception& e) {
                last_err = std::string("json decode error: ") + e.what();
            }
        } else {
            last_err = "serf members failed";
        }

        double sleep_s = std::min((base_ms / 1000.0) * (1 << attempt), cap_s);
        sleep_s += 0.001 * (attempt + 1);
        std::this_thread::sleep_for(std::chrono::milliseconds((int)(sleep_s * 1000.0)));
    }

    std::cerr << "[serf members] giving up after retries: " << last_err << "\n";
    return {};
}

static void print_names(const std::string& title, const std::vector<std::string>& names) {
    if (names.empty()) {
        std::cerr << title << ": (none)\n";
        return;
    }
    std::cerr << title << " (" << names.size() << "): ";
    for (size_t i = 0; i < names.size(); i++) {
        if (i) std::cerr << ", ";
        std::cerr << names[i];
    }
    std::cerr << "\n";
}

static std::vector<MemberInfo> ask_cluster_head_for_remote(
    const ServiceArgs& args)
{
    std::vector<MemberInfo> out;

    json payload;
    if (args.min_cpu > 0) payload["min_cpu"] = args.min_cpu;
    if (args.min_ram > 0) payload["min_ram"] = args.min_ram;
    if (args.min_storage > 0) payload["min_storage"] = args.min_storage;
    if (args.min_gpu > 0) payload["min_gpu"] = args.min_gpu;
    if (args.min_score_per_cpu > 0) payload["min_score_per_cpu"] = args.min_score_per_cpu;
    if (args.min_score_per_ram > 0) payload["min_score_per_ram"] = args.min_score_per_ram;
    if (args.min_score_per_storage > 0) payload["min_score_per_storage"] = args.min_score_per_storage;
    if (args.min_score_per_gpu > 0) payload["min_score_per_gpu"] = args.min_score_per_gpu;

    payload["request_id"] = "TRACE-HILBERT";
    payload["mode"] = "broad_remote_search";

    std::cerr << "[CH] broad remote search request\n";

    std::string cmd =
        "./serf query -rpc-addr=" + args.rpc_addr +
        " -timeout=" + std::to_string(args.timeout_s) + "s" +
        " -format=json ch.ask-remote-res " +
        shell_quote(payload.dump());

    std::string txt;
    int rc = 0;
    if (!exec_capture(cmd, txt, &rc) || rc != 0) {
        std::cerr << "[CH] serf query failed\n";
        return out;
    }

    try {
        json root = json::parse(txt);
        json responses = root.value("Responses", json::object());

        std::set<std::string> seen;

        for (auto it = responses.begin(); it != responses.end(); ++it) {
            json inner;
            try {
                if (it.value().is_string()) inner = json::parse(it.value().get<std::string>());
                else inner = it.value();
            } catch (...) {
                continue;
            }

            json arr;
            if (inner.is_array()) arr = inner;
            else if (inner.is_object()) arr = inner.value("nodes", inner.value("Nodes", json::array()));
            else arr = json::array();

            if (!arr.is_array()) continue;

            for (const auto& rec : arr) {
                if (!rec.is_object()) continue;

                std::string nm = rec.value("name", rec.value("Name", ""));
                if (nm.empty()) continue;
                if (seen.count(nm)) continue;
                seen.insert(nm);

                MemberInfo r;
                r.name = nm;
                r.ip = rec.value("ip", rec.value("IP", ""));

                if (rec.contains("cpu")) r.cpu = to_ll_safe(rec["cpu"]);
                else if (rec.contains("CPU")) r.cpu = to_ll_safe(rec["CPU"]);

                if (rec.contains("ram")) r.ram = to_d_safe(rec["ram"]);
                else if (rec.contains("RAM")) r.ram = to_d_safe(rec["RAM"]);

                if (rec.contains("storage")) r.storage = to_ll_safe(rec["storage"]);
                else if (rec.contains("Storage")) r.storage = to_ll_safe(rec["Storage"]);

                if (rec.contains("gpu")) r.gpu = to_ll_safe(rec["gpu"]);
                else if (rec.contains("GPU")) r.gpu = to_ll_safe(rec["GPU"]);
                if (rec.contains("price_per_cpu")) r.price_per_cpu = to_d_safe(rec["price_per_cpu"]);
                else if (rec.contains("PricePerCPU")) r.price_per_cpu = to_d_safe(rec["PricePerCPU"]);
                if (rec.contains("price_per_ram")) r.price_per_ram = to_d_safe(rec["price_per_ram"]);
                else if (rec.contains("PricePerRAM")) r.price_per_ram = to_d_safe(rec["PricePerRAM"]);
                if (rec.contains("price_per_storage")) r.price_per_storage = to_d_safe(rec["price_per_storage"]);
                else if (rec.contains("PricePerStorage")) r.price_per_storage = to_d_safe(rec["PricePerStorage"]);
                if (rec.contains("price_per_gpu")) r.price_per_gpu = to_d_safe(rec["price_per_gpu"]);
                else if (rec.contains("PricePerGPU")) r.price_per_gpu = to_d_safe(rec["PricePerGPU"]);
                if (rec.contains("score_per_cpu")) r.score_per_cpu = to_d_safe(rec["score_per_cpu"]);
                else if (rec.contains("ScorePerCPU")) r.score_per_cpu = to_d_safe(rec["ScorePerCPU"]);
                if (rec.contains("score_per_ram")) r.score_per_ram = to_d_safe(rec["score_per_ram"]);
                else if (rec.contains("ScorePerRAM")) r.score_per_ram = to_d_safe(rec["ScorePerRAM"]);
                if (rec.contains("score_per_storage")) r.score_per_storage = to_d_safe(rec["score_per_storage"]);
                else if (rec.contains("ScorePerStorage")) r.score_per_storage = to_d_safe(rec["ScorePerStorage"]);
                if (rec.contains("score_per_gpu")) r.score_per_gpu = to_d_safe(rec["score_per_gpu"]);
                else if (rec.contains("ScorePerGPU")) r.score_per_gpu = to_d_safe(rec["ScorePerGPU"]);

                r.origin = "wan";
                out.push_back(r);
            }
        }
    } catch (...) {
        return {};
    }

    return out;
}

// ============================================================
// Buyer override
// ============================================================
static void apply_buyer_overrides(ServiceArgs& args, const ServiceArgs& defaults) {
    if (args.buyer_url.empty()) return;

    json buyer;
    if (!load_json_url(args.buyer_url, 5, buyer) || !buyer.is_object()) {
        std::cerr << "[buyer] failed to load buyer JSON from " << args.buyer_url << "\n";
        return;
    }

    json res = buyer.value("resources", json::object());

    auto get_num = [&](const char* field, const char* key) -> double {
        if (!res.contains(field) || !res[field].is_object()) return 0.0;
        json obj = res[field];
        if (!obj.contains(key)) return 0.0;
        return to_d_safe(obj[key]);
    };

    // reset to CLI defaults every cycle
    args.min_cpu = defaults.min_cpu;
    args.min_ram = defaults.min_ram;
    args.min_storage = defaults.min_storage;
    args.min_gpu = defaults.min_gpu;
    args.min_score_per_cpu = defaults.min_score_per_cpu;
    args.min_score_per_ram = defaults.min_score_per_ram;
    args.min_score_per_storage = defaults.min_score_per_storage;
    args.min_score_per_gpu = defaults.min_score_per_gpu;

    // demands
    args.min_cpu     = std::max(args.min_cpu,     (int)get_num("vcpu","demand_per_unit"));
    args.min_ram     = std::max(args.min_ram,          get_num("ram","demand_per_unit"));
    args.min_storage = std::max(args.min_storage, (int)get_num("storage","demand_per_unit"));
    args.min_gpu     = std::max(args.min_gpu,     (int)get_num("vgpu","demand_per_unit"));

    double b;
    b = get_num("vcpu","score");     if (b > 0) args.min_score_per_cpu = b;
    b = get_num("ram","score");      if (b > 0) args.min_score_per_ram = b;
    b = get_num("storage","score");  if (b > 0) args.min_score_per_storage = b;
    b = get_num("vgpu","score");     if (b > 0) args.min_score_per_gpu = b;

    std::cerr
        << "[buyer] loaded from " << args.buyer_url
        << ": min(cpu=" << args.min_cpu
        << ", ram=" << args.min_ram
        << ", storage=" << args.min_storage
        << ", gpu=" << args.min_gpu
        << "); score_min(cpu=" << args.min_score_per_cpu
        << ", ram=" << args.min_score_per_ram
        << ", storage=" << args.min_score_per_storage
        << ", gpu=" << args.min_score_per_gpu
        << ")\n";
}

// ============================================================
// JSON payload helper
// ============================================================
static json member_to_json(const MemberInfo& m) {
    json out = json{
        {"name", m.name},
        {"ip", m.ip},
        {"origin", m.origin},
        {"cpu", m.cpu},
        {"ram", m.ram},
        {"storage", m.storage},
        {"gpu", m.gpu},
        {"score_per_cpu", m.score_per_cpu},
        {"score_per_ram", m.score_per_ram},
        {"score_per_storage", m.score_per_storage},
        {"score_per_gpu", m.score_per_gpu}
    };
    if (std::isfinite(m.price_per_cpu)) out["price_per_cpu"] = m.price_per_cpu;
    if (std::isfinite(m.price_per_ram)) out["price_per_ram"] = m.price_per_ram;
    if (std::isfinite(m.price_per_storage)) out["price_per_storage"] = m.price_per_storage;
    if (std::isfinite(m.price_per_gpu)) out["price_per_gpu"] = m.price_per_gpu;
    return out;
}

static json make_payload(const std::string& query,
                         const std::string& scope,
                         double rtt_threshold_ms,
                         const std::vector<MemberInfo>& rows)
{
    json out;
    out["query"] = query;
    out["scope"] = scope;
    out["rtt_threshold_ms"] = rtt_threshold_ms;
    out["results"] = json::array();
    for (const auto& r : rows) out["results"].push_back(member_to_json(r));
    return out;
}

// ============================================================
// Minimal live HTTP server
// ============================================================
static void start_live_http_server(LiveState* state,
                                   const std::string& host,
                                   int port,
                                   const std::string& path)
{
    std::thread([state, host, port, path]() {
        int server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0) return;

        int one = 1;
        setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)port);
        addr.sin_addr.s_addr = (host == "0.0.0.0") ? INADDR_ANY : inet_addr(host.c_str());

        if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            close(server_fd);
            return;
        }
        if (listen(server_fd, 16) < 0) {
            close(server_fd);
            return;
        }

        std::cerr << "[http] live endpoint at http://" << host << ":" << port << path << "\n";

        while (true) {
            int client = accept(server_fd, nullptr, nullptr);
            if (client < 0) continue;

            char buf[4096];
            int n = (int)read(client, buf, sizeof(buf)-1);
            if (n <= 0) {
                close(client);
                continue;
            }
            buf[n] = '\0';
            std::string req(buf);

            std::string target = "/";
            auto sp1 = req.find(' ');
            if (sp1 != std::string::npos) {
                auto sp2 = req.find(' ', sp1 + 1);
                if (sp2 != std::string::npos) {
                    target = req.substr(sp1 + 1, sp2 - sp1 - 1);
                }
            }

            std::string status = "200 OK";
            std::string ctype = "application/json";
            std::string body;

            if (target == path) {
                json copy;
                {
                    std::lock_guard<std::mutex> lk(state->lock);
                    copy = state->payload;
                }
                body = copy.dump();
            } else if (target == "/healthz") {
                body = "ok";
                ctype = "text/plain";
            } else {
                status = "404 Not Found";
                body = "{}";
            }

            std::ostringstream resp;
            resp << "HTTP/1.1 " << status << "\r\n"
                 << "Content-Type: " << ctype << "\r\n"
                 << "Cache-Control: no-store\r\n"
                 << "Content-Length: " << body.size() << "\r\n"
                 << "Connection: close\r\n\r\n"
                 << body;

            std::string wire = resp.str();
            send(client, wire.data(), wire.size(), 0);
            close(client);
        }
    }).detach();
}

// ============================================================
// Core discovery extraction from old main()
// ============================================================
static std::vector<std::string> discover_hilbert_names_core(
    const json& arr,
    const std::string& qname,
    int T_ms,
    int ORDER,
    bool rebuild,
    bool debug,
    bool vec_already_ms)
{
    std::vector<std::string> hilbert_names;

    const size_t N = arr.size();
    if (N == 0) {
        std::cerr << "[core] geometry is empty\n";
        return hilbert_names;
    }

    std::unordered_map<std::string, int> idx;
    idx.reserve(N * 2 + 8);
    for (size_t i = 0; i < N; ++i) {
        if (!arr[i].contains("name") || !arr[i]["name"].is_string()) continue;
        idx[arr[i]["name"].get<std::string>()] = (int)i;
    }

    auto it = idx.find(qname);
    if (it == idx.end()) {
        std::cerr << "[core] query node not found in geometry: " << qname << "\n";
        return hilbert_names;
    }
    const int qi = it->second;

    std::vector<std::string> names(N);
    for (size_t i = 0; i < N; ++i) names[i] = arr[i].value("name", std::string{});

    if (!arr[qi].contains("coordinate") || !arr[qi]["coordinate"].is_object() ||
        !arr[qi]["coordinate"].contains("Vec") || !arr[qi]["coordinate"]["Vec"].is_array()) {
        std::cerr << "[core] query node has no valid coordinate.Vec: " << qname << "\n";
        return hilbert_names;
    }

    const int dims = (int)arr[qi]["coordinate"]["Vec"].size();
    if (dims <= 0) {
        std::cerr << "[core] coordinate.Vec is empty for query node: " << qname << "\n";
        return hilbert_names;
    }

    const double VEC_TO_MS = vec_already_ms ? 1.0 : 1000.0;
    std::vector<VecD> vecs_ms(N, VecD(dims, 0.0));
    std::vector<double> heights_ms(N, 0.0);
    std::vector<double> adjs_ms(N, 0.0);

    for (size_t i = 0; i < N; ++i) {
        if (!arr[i].contains("coordinate") || !arr[i]["coordinate"].is_object()) {
            std::cerr << "[core] missing coordinate for node " << names[i] << "\n";
            return {};
        }
        const json& coord = arr[i]["coordinate"];
        if (!coord.contains("Vec") || !coord["Vec"].is_array()) {
            std::cerr << "[core] invalid coordinate.Vec for node " << names[i] << "\n";
            return {};
        }
        const json& v = coord["Vec"];
        if ((int)v.size() != dims) {
            std::cerr << "[core] inconsistent Vec dimensionality for node " << names[i]
                      << ": expected " << dims << ", got " << v.size() << "\n";
            return {};
        }
        for (int d = 0; d < dims; ++d) vecs_ms[i][d] = v[d].get<double>() * VEC_TO_MS;
        if (coord.contains("Height") && coord["Height"].is_number()) heights_ms[i] = coord["Height"].get<double>() * VEC_TO_MS;
        if (coord.contains("Adjustment") && coord["Adjustment"].is_number()) adjs_ms[i] = coord["Adjustment"].get<double>() * VEC_TO_MS;
    }

    const double q_height_ms = heights_ms[qi];
    const double q_adj_ms = adjs_ms[qi];

    const double latency_max = SERVICE_LATENCY_MAX_MS;
    const PU_int GRID_MAX = (PU_int)((1u << ORDER) - 1u);
    const double cell_size_ms = latency_max / (double)(1u << ORDER);
    std::string dbname = "serfdb_service_d" + std::to_string(dims) + "_o" + std::to_string(ORDER);

    if (debug) {
        std::cerr << "[core] dims=" << dims << " db=" << dbname
                  << " latency_max=" << latency_max
                  << " cell_size=" << cell_size_ms << "\n";
    }

    VecD mn(dims, std::numeric_limits<double>::infinity());
    VecD mx(dims, -std::numeric_limits<double>::infinity());
    for (size_t i = 0; i < N; ++i) {
        for (int d = 0; d < dims; ++d) {
            mn[d] = std::min(mn[d], vecs_ms[i][d]);
            mx[d] = std::max(mx[d], vecs_ms[i][d]);
        }
    }

    auto quantize_prof = [&](double x_ms, int d) -> PU_int {
        double shifted = x_ms - mn[d];
        if (shifted < 0.0) shifted = 0.0;
        if (shifted > latency_max) shifted = latency_max;
        long long gi = (long long)std::floor(shifted / cell_size_ms);
        return clamp_pu(gi, (PU_int)0, GRID_MAX);
    };

    std::vector<VecPU> pts(N, VecPU(dims, 0));
    for (size_t i = 0; i < N; ++i) {
        for (int d = 0; d < dims; ++d) pts[i][d] = quantize_prof(vecs_ms[i][d], d);
    }

    std::unordered_map<std::string, std::vector<std::string>> point_to_names;
    point_to_names.reserve(N * 2 + 8);
    for (size_t i = 0; i < N; ++i) point_to_names[point_key_vec(pts[i])].push_back(names[i]);

    const int BT_NODE_ENTRIES = 10;
    const int BUFFER_PAGES = 10;
    const int PAGE_RECORDS = 200;
    const int DIMS = dims;

    bool db_exists =
        file_exists(dbname + ".db") &&
        file_exists(dbname + ".idx") &&
        file_exists(dbname + ".inf");

    if (rebuild || !db_exists) {
        remove((dbname + ".db").c_str());
        remove((dbname + ".idx").c_str());
        remove((dbname + ".inf").c_str());
        remove((dbname + ".fpl").c_str());
    }

    DBASE* DB = new DBASE(dbname, DIMS, BT_NODE_ENTRIES, BUFFER_PAGES, PAGE_RECORDS);
    bool did_build = (rebuild || !db_exists);

    if (did_build) {
        if (!DB->db_create()) {
            std::cerr << "[core] DB create failed\n";
            delete DB;
            return {};
        }
    }
    if (!DB->db_open()) {
        std::cerr << "[core] DB open failed\n";
        delete DB;
        return {};
    }

    if (did_build) {
        for (size_t i = 0; i < N; ++i) {
            if (!DB->db_data_insert(pts[i].data())) {
                std::cerr << "[core] insert failed for " << names[i] << "\n";
                DB->db_close();
                delete DB;
                return {};
            }
        }
    }

    const double r_cont = (double)T_ms / cell_size_ms;
    const long long r_cells = (long long)std::ceil(r_cont);

    VecLL center(dims), root_lo(dims), root_hi(dims);
    for (int d = 0; d < dims; ++d) {
        center[d] = (long long)pts[qi][d];
        root_lo[d] = std::max(0LL, center[d] - r_cells);
        root_hi[d] = std::min((long long)GRID_MAX, center[d] + r_cells);
    }

    VecD center_ms(dims, 0.0);
    for (int d = 0; d < dims; ++d) {
        double s = vecs_ms[qi][d] - mn[d];
        if (s < 0.0) s = 0.0;
        if (s > latency_max) s = latency_max;
        center_ms[d] = s;
    }

    std::unordered_map<std::string, int> hilbert_set;
    hilbert_set.reserve(N * 4 + 8);
    QueryStats qs_cover;
    CoverStats st;

    coverSphereIndexed_nd(DB, ORDER, center_ms, T_ms, cell_size_ms,
                          point_to_names, qname,
                          idx,
                          heights_ms, adjs_ms, q_height_ms, q_adj_ms,
                          hilbert_set, hilbert_names,
                          root_lo, root_hi, 0, st, qs_cover);

    DB->db_close();
    delete DB;

    std::vector<std::string> filtered;
    filtered.reserve(hilbert_names.size());
    for (const auto& nm : hilbert_names) {
        if (nm == qname) continue;
        if (lower_str(nm).find("-wan") != std::string::npos) continue;
        filtered.push_back(nm);
    }

    if (debug) {
        std::cerr << "[core] candidate_count=" << filtered.size()
                  << " visited=" << st.visited
                  << " pruned_outside=" << st.pruned_outside
                  << " pruned_empty=" << st.pruned_empty
                  << " pruned_rtt_lb=" << st.pruned_rtt_lb
                  << " accepted_leaf=" << st.accepted_leaf << "\n";
    }

    return filtered;
}

// ============================================================
// One discovery run
// ============================================================
static json run_once(const std::string& query_node,
                     const ServiceArgs& args,
                     const std::vector<std::string>& hilbert_names)
{
    auto lan_members = get_lan_members(args.rpc_addr);

    if (lan_members.empty()) {
        std::cerr << "[warn] ./serf members unavailable after retries; skipping this cycle (no CH).\n";
        return json{{"query", query_node}, {"scope", "none"}, {"results", json::array()}};
    }

    std::unordered_map<std::string, MemberInfo> lan_map;
    std::unordered_set<std::string> lan_names;
    for (const auto& m : lan_members) {
        lan_map[m.name] = m;
        lan_names.insert(m.name);
    }

    std::vector<std::string> local_names;
    std::vector<std::string> remote_names;

    for (const auto& nm : hilbert_names) {
        if (nm == query_node) continue;
        if (lan_names.find(nm) != lan_names.end()) local_names.push_back(nm);
        else remote_names.push_back(nm);
    }

    print_names("• Hilbert local names", local_names);
    print_names("• Hilbert remote names", remote_names);

    // local first
    if (!local_names.empty()) {
        std::vector<MemberInfo> local_rows;
        local_rows.reserve(local_names.size());

        for (const auto& nm : local_names) {
            auto it = lan_map.find(nm);
            if (it != lan_map.end()) local_rows.push_back(it->second);
        }

        local_rows = drop_nan_members(local_rows);
        local_rows = filter_by_resources(local_rows, args);

        if (!local_rows.empty()) {
            sort_candidates(local_rows, args.sort);
            if (args.limit > 0 && (int)local_rows.size() > args.limit) {
                local_rows.resize((size_t)args.limit);
            }
            std::cerr << "[result] scope=hilbert-local count=" << local_rows.size() << "\n";
            return make_payload(query_node, "hilbert-local", args.rtt_threshold_ms, local_rows);
        }
    }

    // Fall back to a broad cluster-head remote search if local candidates fail.
    if (local_names.empty()) {
        std::cerr << "[result] no local Hilbert candidates; trying broad CH remote search\n";
    } else {
        std::cerr << "[result] no feasible local rows; trying broad CH remote search\n";
    }

    {
        auto remote_rows = ask_cluster_head_for_remote(args);
        remote_rows.erase(
            std::remove_if(remote_rows.begin(), remote_rows.end(),
                           [&](const MemberInfo& m) {
                               return lan_names.find(m.name) != lan_names.end();
                           }),
            remote_rows.end());
        remote_rows = drop_nan_members(remote_rows);
        remote_rows = filter_by_resources(remote_rows, args);

        if (!remote_rows.empty()) {
            sort_candidates(remote_rows, args.sort);
            if (args.limit > 0 && (int)remote_rows.size() > args.limit) {
                remote_rows.resize((size_t)args.limit);
            }
            std::cerr << "[result] scope=hilbert-remote count=" << remote_rows.size() << "\n";
            return make_payload(query_node, "hilbert-remote", args.rtt_threshold_ms, remote_rows);
        }
    }

    std::cerr << "[result] scope=none count=0\n";
    return json{{"query", query_node}, {"scope", "none"}, {"results", json::array()}};
}

static std::vector<int> build_threshold_plan(const ServiceArgs& args) {
    std::vector<int> plan;

    int start_ms = std::max(1, (int)std::llround(args.rtt_threshold_ms));
    int steps = std::max(1, args.max_steps);

    double growth = 1.0 + args.pct_start;
    if (growth <= 1.0) growth = 1.25;

    double cur = (double)start_ms;

    for (int i = 0; i < steps; ++i) {
        int thr = std::max(1, (int)std::llround(cur));
        if (plan.empty() || plan.back() != thr) {
            plan.push_back(thr);
        }

        double next = cur * growth;
        if (next - cur < 2.0) next = cur + 2.0;
        cur = next;
    }

    if (plan.empty()) plan.push_back(start_ms);
    return plan;
}

static json run_once_adaptive(const std::string& query_node,
                              const ServiceArgs& args,
                              const json& arr)
{
    auto lan_members = get_lan_members(args.rpc_addr);

    if (lan_members.empty()) {
        std::cerr << "[warn] ./serf members unavailable after retries; skipping this cycle (no CH).\n";
        return json{{"query", query_node}, {"scope", "none"}, {"rtt_threshold_ms", args.rtt_threshold_ms}, {"results", json::array()}};
    }

    std::unordered_map<std::string, MemberInfo> lan_map;
    std::unordered_set<std::string> lan_names;
    for (const auto& m : lan_members) {
        lan_map[m.name] = m;
        lan_names.insert(m.name);
    }

    const auto local_resource_names = run_resource_db_query(lan_members, args, "resdb_service_local");
    std::cerr << "[resource] feasible local names=" << local_resource_names.size() << "\n";

    const auto thresholds = build_threshold_plan(args);
    bool local_saturated = false;
    std::set<std::string> prev_local_set;
    int last_local_threshold_ms = std::max(1, (int)std::llround(args.rtt_threshold_ms));

    for (size_t step = 0; step < thresholds.size(); ++step) {
        int thr_ms = thresholds[step];
        last_local_threshold_ms = thr_ms;
        std::cerr << "[adaptive] step " << (step + 1) << "/" << thresholds.size() << " threshold=" << thr_ms << "ms\n";

        auto hilbert_names = discover_hilbert_names_core(arr, query_node, thr_ms,
                                                         SERVICE_HORDER,
                                                         SERVICE_REBUILD_DB,
                                                         SERVICE_DEBUG_CORE,
                                                         SERVICE_VEC_ALREADY_MS);

        if (args.dump_hilbert) {
            std::cerr << "[adaptive] raw hilbert names at " << thr_ms << "ms: ";
            if (hilbert_names.empty()) std::cerr << "(none)";
            for (size_t i = 0; i < hilbert_names.size(); ++i) {
                if (i) std::cerr << ", ";
                std::cerr << hilbert_names[i];
            }
            std::cerr << "\n";
        }

        std::vector<std::string> local_names;
        std::vector<std::string> remote_names;
        for (const auto& nm : hilbert_names) {
            if (nm == query_node) continue;
            if (lan_names.find(nm) != lan_names.end()) local_names.push_back(nm);
            else remote_names.push_back(nm);
        }

        print_names("• Hilbert local names", local_names);
        print_names("• Hilbert remote names", remote_names);

        std::vector<MemberInfo> local_rows = intersect_rows_by_name(local_names, local_resource_names, lan_map);
        if (!local_rows.empty()) {
            sort_candidates(local_rows, args.sort);
            if (args.limit > 0 && (int)local_rows.size() > args.limit) local_rows.resize((size_t)args.limit);
            std::cerr << "[result] scope=hilbert-local count=" << local_rows.size() << " threshold=" << thr_ms << "ms\n";
            return make_payload(query_node, "hilbert-local", (double)thr_ms, local_rows);
        }

        if (!local_names.empty()) {
            std::cerr << "[adaptive] no feasible local rows at " << thr_ms << "ms after location_intersect_resource intersection\n";
        }

        std::set<std::string> cur_local_set(local_names.begin(), local_names.end());
        if (!cur_local_set.empty() && cur_local_set == prev_local_set) {
            local_saturated = true;
            std::cerr << "[adaptive] local candidate set saturated at " << thr_ms << "ms; switching to CH remote phase\n";
            break;
        }
        prev_local_set = std::move(cur_local_set);
    }

    if (local_saturated || !thresholds.empty()) {
        std::cerr << "[adaptive] starting broad CH remote phase\n";

        auto remote_rows_raw = ask_cluster_head_for_remote(args);
        remote_rows_raw.erase(
            std::remove_if(remote_rows_raw.begin(), remote_rows_raw.end(),
                           [&](const MemberInfo& m) {
                               return lan_names.find(m.name) != lan_names.end();
                           }),
            remote_rows_raw.end());
        std::unordered_map<std::string, MemberInfo> remote_map;
        for (const auto& m : remote_rows_raw) remote_map[m.name] = m;

        const auto remote_resource_names = run_resource_db_query(remote_rows_raw, args, "resdb_service_remote");
        std::cerr << "[resource] feasible remote names=" << remote_resource_names.size() << "\n";

        std::vector<std::string> remote_location_names;
        remote_location_names.reserve(remote_rows_raw.size());
        for (const auto& m : remote_rows_raw) remote_location_names.push_back(m.name);

        std::vector<MemberInfo> remote_rows = intersect_rows_by_name(remote_location_names, remote_resource_names, remote_map);
        if (!remote_rows.empty()) {
            sort_candidates(remote_rows, args.sort);
            if (args.limit > 0 && (int)remote_rows.size() > args.limit) remote_rows.resize((size_t)args.limit);
            std::cerr << "[result] scope=hilbert-remote count=" << remote_rows.size() << " threshold=" << last_local_threshold_ms << "ms\n";
            return make_payload(query_node, "hilbert-remote", (double)last_local_threshold_ms, remote_rows);
        }

        std::cerr << "[adaptive] remote CH query returned no feasible rows after location_intersect_resource intersection\n";
    }

    std::cerr << "[result] scope=none count=0\n";
    return json{{"query", query_node}, {"scope", "none"}, {"rtt_threshold_ms", args.rtt_threshold_ms}, {"results", json::array()}};
}

// ============================================================
// CLI
// ============================================================
static void usage_service(const char* prog) {
    std::cerr
      << "Usage:\n"
      << "  " << prog << " --geom-url <url> --rtt-threshold-ms <ms>\n"
      << "         [--rpc-addr <addr>] [--timeout-s <sec>]\n"
      << "         [--dump-hilbert] [--pct-start <f>] [--max-steps <n>]   (adaptive threshold widening)\n"
      << "         [--min-cpu <n>] [--min-ram <n>] [--min-storage <n>] [--min-gpu <n>]\n"
      << "         [--min-score-per-cpu <n>] [--min-score-per-ram <n>] [--min-score-per-storage <n>] [--min-score-per-gpu <n>]\n"
      << "         [--sort <key>] [--limit <n>]\n"
      << "         [--http-serve] [--http-host <host>] [--http-port <port>] [--http-path <path>]\n"
      << "         [--buyer-url <url>] [--loop] [--busy-secs <sec>]\n";
}

static bool parse_service_args(int argc, char** argv, ServiceArgs& a) {
    for (int i = 1; i < argc; i++) {
        std::string s = argv[i];

        auto need = [&](const std::string& k) -> const char* {
            if (i + 1 >= argc) {
                std::cerr << "Missing value for " << k << "\n";
                std::exit(2);
            }
            return argv[++i];
        };

        if      (s == "--geom-url") a.geom_url = need(s);
        else if (s == "--rtt-threshold-ms") a.rtt_threshold_ms = std::stod(need(s));
        else if (s == "--rpc-addr") a.rpc_addr = need(s);
        else if (s == "--timeout-s") a.timeout_s = std::stoi(need(s));

        else if (s == "--dump-hilbert") a.dump_hilbert = true;
        else if (s == "--pct-start") a.pct_start = std::stod(need(s));
        else if (s == "--max-steps") a.max_steps = std::stoi(need(s));

        else if (s == "--min-cpu") a.min_cpu = std::stoi(need(s));
        else if (s == "--min-ram") a.min_ram = std::stod(need(s));
        else if (s == "--min-storage") a.min_storage = std::stoi(need(s));
        else if (s == "--min-gpu") a.min_gpu = std::stoi(need(s));
        else if (s == "--min-score-per-cpu") a.min_score_per_cpu = std::stod(need(s));
        else if (s == "--min-score-per-ram") a.min_score_per_ram = std::stod(need(s));
        else if (s == "--min-score-per-storage") a.min_score_per_storage = std::stod(need(s));
        else if (s == "--min-score-per-gpu") a.min_score_per_gpu = std::stod(need(s));

        else if (s == "--sort") a.sort = need(s);
        else if (s == "--limit") a.limit = std::stoi(need(s));

        else if (s == "--http-serve") a.http_serve = true;
        else if (s == "--http-host") a.http_host = need(s);
        else if (s == "--http-port") a.http_port = std::stoi(need(s));
        else if (s == "--http-path") a.http_path = need(s);

        else if (s == "--buyer-url") a.buyer_url = need(s);
        else if (s == "--loop") a.loop = true;
        else if (s == "--busy-secs") a.busy_secs = std::stod(need(s));

        else if (s == "--help" || s == "-h") {
            usage_service(argv[0]);
            std::exit(0);
        } else {
            std::cerr << "Unknown arg: " << s << "\n";
            return false;
        }
    }

    if (a.geom_url.empty() || a.rtt_threshold_ms <= 0.0) return false;
    return true;
}

// ============================================================
// main()
// ============================================================
int main(int argc, char** argv) {
    ServiceArgs args;
    if (!parse_service_args(argc, argv, args)) {
        usage_service(argv[0]);
        return 2;
    }

    ServiceArgs defaults = args;

    // ---------- load geometry ONCE ----------
    json root;
    if (!load_json_url(args.geom_url, 8, root)) {
        std::cerr << "[geom] failed to load geometry from " << args.geom_url << "\n";
        return 1;
    }

    json arr;
    if (!extract_nodes_array(root, arr)) {
        std::cerr << "[geom] payload must be an array or object containing nodes[]\n";
        return 1;
    }

    const size_t N = arr.size();
    if (N == 0) {
        std::cerr << "[geom] node array is empty\n";
        return 1;
    }

    std::string query_node;
    try {
        query_node = read_query_node_name();
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }

    bool found_query = false;
    for (const auto& n : arr) {
        if (n.is_object() && n.value("name", "") == query_node) {
            found_query = true;
            break;
        }
    }
    if (!found_query) {
        std::cerr << "[geom] query node " << query_node << " not found in geometry\n";
        return 1;
    }

    LiveState state;
    if (args.http_serve) {
        start_live_http_server(&state, args.http_host, args.http_port, args.http_path);
    }

    auto do_cycle = [&]() -> json {
        ServiceArgs cycle_args = args;
        if (!cycle_args.buyer_url.empty()) {
            apply_buyer_overrides(cycle_args, defaults);
        }

        json payload = run_once_adaptive(query_node, cycle_args, arr);

        {
            std::lock_guard<std::mutex> lk(state.lock);
            state.payload = payload;
        }

        std::cout << payload.dump(2) << "\n";
        std::cout.flush();
        return payload;
    };

    try {
        if (args.loop) {
            std::cerr << "[loop] fixed sleep " << args.busy_secs
                      << "s; latest results are served by HTTP endpoint if enabled.\n";
            while (true) {
                do_cycle();
                std::this_thread::sleep_for(std::chrono::milliseconds((int)(args.busy_secs * 1000.0)));
            }
        } else {
            do_cycle();

            if (args.http_serve) {
                std::cerr << "[single-shot] results published to HTTP; press Ctrl+C to stop.\n";
                while (true) {
                    std::this_thread::sleep_for(std::chrono::hours(1));
                }
            }
        }
    } catch (...) {
        std::cerr << "[main] stopped by exception\n";
        return 1;
    }

    return 0;
}
