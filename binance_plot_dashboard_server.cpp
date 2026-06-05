

// binance_plot_dashboard_server.cpp
// C++20 local dashboard for Binance collector .bin files and live timing CSV.
// No external libraries required. Uses a tiny built-in HTTP server and Plotly in the browser.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef _WIN32
  #ifndef NOMINMAX
    #define NOMINMAX
  #endif
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #include <windows.h>
  #include <shellapi.h>
  using socket_t = SOCKET;
  static constexpr socket_t invalid_socket_value = INVALID_SOCKET;
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/socket.h>
  #include <sys/types.h>
  #include <unistd.h>
  using socket_t = int;
  static constexpr socket_t invalid_socket_value = -1;
#endif

namespace fs = std::filesystem;
using namespace std::chrono_literals;

// =============================================================================
// CONFIG
// =============================================================================

struct AppConfig {
    fs::path base_dir = "data_dump";
    std::string host = "127.0.0.1";
    int port = 8060;
    bool auto_open_browser = true;
    bool log_requests = true;
    std::string default_market = "spot";
    std::string default_symbol = "BTCUSDT";
    int default_bars = 500;
    int default_timing_rows = 5000;
    int default_refresh_seconds = 3;
};

static constexpr const char* INTERVAL = "1m";
static constexpr int64_t INTERVAL_MS = 60'000;
static constexpr int MAX_BARS = 250'000;
static constexpr int MAX_TIMING_ROWS = 250'000;
static const std::array<std::string, 3> MARKETS = {"spot", "um", "cm"};

static AppConfig g_config;
static std::atomic<bool> g_stop{false};

// =============================================================================
// KLINE RECORD LAYOUT - same 128-byte layout as collector/Python numpy dtype.
// =============================================================================

#pragma pack(push, 1)
struct KlineRecord {
    char symbol[24];
    int64_t open_time;
    int64_t close_time;
    double open;
    double high;
    double low;
    double close;
    double volume;
    double quote_volume;
    int64_t trades;
    double taker_base_vol;
    double taker_quote_vol;
    double maker_base_vol;
    double maker_quote_vol;
};
#pragma pack(pop)
static_assert(sizeof(KlineRecord) == 128, "KlineRecord must be 128 bytes");

struct FileEdge {
    fs::path path;
    int64_t rows = 0;
    std::optional<int64_t> first_open_time_ms;
    std::optional<int64_t> last_open_time_ms;
    std::optional<int64_t> first_close_time_ms;
    std::optional<int64_t> last_close_time_ms;
};

struct TimingRow {
    std::string market;
    std::string symbol;
    int64_t scheduled_close_us = 0;
    int64_t exchange_event_us = 0;
    int64_t received_us = 0;
    int64_t saved_us = 0;
    int64_t scheduled_close_to_receive_us = 0;
    int64_t exchange_event_to_receive_us = 0;
    int64_t receive_to_saved_us = 0;
    int64_t write_call_us = 0;
    int64_t scheduled_close_to_saved_us = 0;
    int64_t rows_written = 0;
};

// =============================================================================
// SMALL HELPERS
// =============================================================================

static std::string trim(std::string s) {
    auto is_space = [](unsigned char c) { return std::isspace(c) != 0; };
    s.erase(s.begin(), std::find_if_not(s.begin(), s.end(), is_space));
    s.erase(std::find_if_not(s.rbegin(), s.rend(), is_space).base(), s.end());
    return s;
}

static std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return s;
}

static std::string upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

static bool is_market(const std::string& market) {
    return market == "spot" || market == "um" || market == "cm";
}

static std::string normalize_market(std::string market) {
    market = lower(trim(market));
    if (!is_market(market)) return g_config.default_market;
    return market;
}

static std::string normalize_symbol(std::string symbol) {
    symbol = upper(trim(symbol));
    symbol.erase(std::remove(symbol.begin(), symbol.end(), '\0'), symbol.end());
    if (symbol.find('/') != std::string::npos || symbol.find('\\') != std::string::npos || symbol.find("..") != std::string::npos) {
        return "";
    }
    return symbol;
}

static int clamp_int(int value, int low, int high) {
    return std::max(low, std::min(high, value));
}

static int64_t safe_stoll(const std::string& s, int64_t fallback = 0) {
    try {
        if (trim(s).empty()) return fallback;
        return std::stoll(trim(s));
    } catch (...) {
        return fallback;
    }
}

static double safe_stod(const std::string& s, double fallback = 0.0) {
    try {
        if (trim(s).empty()) return fallback;
        return std::stod(trim(s));
    } catch (...) {
        return fallback;
    }
}

static std::string symbol_from_record(const KlineRecord& r) {
    size_t n = 0;
    while (n < sizeof(r.symbol) && r.symbol[n] != '\0') ++n;
    return std::string(r.symbol, n);
}

static std::string iso_from_ms(int64_t ms) {
    if (ms <= 0) return "";
    auto tp = std::chrono::system_clock::time_point{std::chrono::milliseconds{ms}};
    auto tt = std::chrono::system_clock::to_time_t(tp);
    std::tm gmt{};
#ifdef _WIN32
    gmtime_s(&gmt, &tt);
#else
    gmtime_r(&tt, &gmt);
#endif
    std::ostringstream os;
    os << std::put_time(&gmt, "%Y-%m-%dT%H:%M:%SZ");
    return os.str();
}

static std::string iso_from_us(int64_t us) {
    if (us <= 0) return "";
    auto tp = std::chrono::system_clock::time_point{std::chrono::microseconds{us}};
    auto tt = std::chrono::system_clock::to_time_t(tp);
    std::tm gmt{};
#ifdef _WIN32
    gmtime_s(&gmt, &tt);
#else
    gmtime_r(&tt, &gmt);
#endif
    int64_t micros = us % 1'000'000LL;
    if (micros < 0) micros += 1'000'000LL;
    std::ostringstream os;
    os << std::put_time(&gmt, "%Y-%m-%dT%H:%M:%S")
       << '.' << std::setw(6) << std::setfill('0') << micros << "Z";
    return os.str();
}

static std::string json_escape(const std::string& s) {
    std::ostringstream os;
    for (unsigned char c : s) {
        switch (c) {
            case '"': os << "\\\""; break;
            case '\\': os << "\\\\"; break;
            case '\b': os << "\\b"; break;
            case '\f': os << "\\f"; break;
            case '\n': os << "\\n"; break;
            case '\r': os << "\\r"; break;
            case '\t': os << "\\t"; break;
            default:
                if (c < 0x20) {
                    os << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(c) << std::dec;
                } else {
                    os << static_cast<char>(c);
                }
        }
    }
    return os.str();
}

static std::string json_str(const std::string& s) {
    return "\"" + json_escape(s) + "\"";
}

static std::string url_decode(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (c == '+') {
            out.push_back(' ');
        } else if (c == '%' && i + 2 < s.size()) {
            auto hexval = [](char ch) -> int {
                if ('0' <= ch && ch <= '9') return ch - '0';
                if ('a' <= ch && ch <= 'f') return ch - 'a' + 10;
                if ('A' <= ch && ch <= 'F') return ch - 'A' + 10;
                return -1;
            };
            int hi = hexval(s[i + 1]);
            int lo = hexval(s[i + 2]);
            if (hi >= 0 && lo >= 0) {
                out.push_back(static_cast<char>((hi << 4) | lo));
                i += 2;
            } else {
                out.push_back(c);
            }
        } else {
            out.push_back(c);
        }
    }
    return out;
}

static std::unordered_map<std::string, std::string> parse_query(const std::string& target) {
    std::unordered_map<std::string, std::string> q;
    auto pos = target.find('?');
    if (pos == std::string::npos) return q;
    std::string query = target.substr(pos + 1);
    std::stringstream ss(query);
    std::string item;
    while (std::getline(ss, item, '&')) {
        auto eq = item.find('=');
        std::string key = url_decode(eq == std::string::npos ? item : item.substr(0, eq));
        std::string val = eq == std::string::npos ? "" : url_decode(item.substr(eq + 1));
        q[key] = val;
    }
    return q;
}

static std::string path_only(const std::string& target) {
    auto pos = target.find('?');
    return pos == std::string::npos ? target : target.substr(0, pos);
}

static fs::path daily_bin_dir() {
    return g_config.base_dir / "daily_bin";
}

static fs::path log_dir() {
    return g_config.base_dir / "logs";
}

static fs::path timing_csv_path() {
    fs::path micro = log_dir() / "live_symbol_delays_microseconds_batched.csv";
    if (fs::exists(micro)) return micro;
    return log_dir() / "live_symbol_delays.csv";
}

static fs::path market_dir(const std::string& market) {
    return daily_bin_dir() / normalize_market(market);
}

static fs::path symbol_dir(const std::string& market, const std::string& symbol) {
    return market_dir(market) / normalize_symbol(symbol);
}

static std::string exe_dir_string(const char* argv0) {
#ifdef _WIN32
    wchar_t buf[MAX_PATH];
    DWORD n = GetModuleFileNameW(nullptr, buf, MAX_PATH);
    if (n > 0 && n < MAX_PATH) {
        fs::path p(buf);
        return p.parent_path().string();
    }
#endif
    try {
        fs::path p(argv0 ? argv0 : ".");
        if (p.has_parent_path()) return fs::absolute(p).parent_path().string();
    } catch (...) {}
    return fs::current_path().string();
}

// =============================================================================
// CONFIG FILE
// =============================================================================

static bool parse_bool(std::string s, bool fallback) {
    s = lower(trim(s));
    if (s == "1" || s == "true" || s == "yes" || s == "on") return true;
    if (s == "0" || s == "false" || s == "no" || s == "off") return false;
    return fallback;
}

static void apply_config_key_value(const std::string& key_raw, const std::string& value_raw) {
    std::string key = lower(trim(key_raw));
    std::string value = trim(value_raw);
    if (key == "base_dir") g_config.base_dir = value;
    else if (key == "host") g_config.host = value;
    else if (key == "port") g_config.port = clamp_int(static_cast<int>(safe_stoll(value, g_config.port)), 1, 65535);
    else if (key == "auto_open_browser") g_config.auto_open_browser = parse_bool(value, g_config.auto_open_browser);
    else if (key == "log_requests") g_config.log_requests = parse_bool(value, g_config.log_requests);
    else if (key == "default_market") g_config.default_market = normalize_market(value);
    else if (key == "default_symbol") g_config.default_symbol = normalize_symbol(value);
    else if (key == "default_bars") g_config.default_bars = clamp_int(static_cast<int>(safe_stoll(value, g_config.default_bars)), 1, MAX_BARS);
    else if (key == "default_timing_rows") g_config.default_timing_rows = clamp_int(static_cast<int>(safe_stoll(value, g_config.default_timing_rows)), 1, MAX_TIMING_ROWS);
    else if (key == "default_refresh_seconds") g_config.default_refresh_seconds = clamp_int(static_cast<int>(safe_stoll(value, g_config.default_refresh_seconds)), 1, 300);
}

static void read_config_file(const fs::path& path) {
    std::ifstream f(path);
    if (!f.is_open()) return;
    std::string line;
    while (std::getline(f, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') continue;
        auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        apply_config_key_value(line.substr(0, eq), line.substr(eq + 1));
    }
}

static void parse_args(int argc, char** argv, const std::string& exe_dir) {
    fs::path default_config = fs::path(exe_dir) / "dashboard_config.txt";
    read_config_file(default_config);

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) return "";
            return argv[++i];
        };
        if (a == "--config") read_config_file(next());
        else if (a == "--base-dir") g_config.base_dir = next();
        else if (a == "--host") g_config.host = next();
        else if (a == "--port") g_config.port = clamp_int(static_cast<int>(safe_stoll(next(), g_config.port)), 1, 65535);
        else if (a == "--market") g_config.default_market = normalize_market(next());
        else if (a == "--symbol") g_config.default_symbol = normalize_symbol(next());
        else if (a == "--bars") g_config.default_bars = clamp_int(static_cast<int>(safe_stoll(next(), g_config.default_bars)), 1, MAX_BARS);
        else if (a == "--timing-rows") g_config.default_timing_rows = clamp_int(static_cast<int>(safe_stoll(next(), g_config.default_timing_rows)), 1, MAX_TIMING_ROWS);
        else if (a == "--refresh") g_config.default_refresh_seconds = clamp_int(static_cast<int>(safe_stoll(next(), g_config.default_refresh_seconds)), 1, 300);
        else if (a == "--no-browser") g_config.auto_open_browser = false;
        else if (a == "--quiet-requests") g_config.log_requests = false;
        else if (a == "--log-requests") g_config.log_requests = true;
    }

    g_config.default_market = normalize_market(g_config.default_market);
    g_config.default_symbol = normalize_symbol(g_config.default_symbol.empty() ? "BTCUSDT" : g_config.default_symbol);
}



static int count_bin_files_limited(const fs::path& base, int limit = 3) {
    std::error_code ec;
    fs::path root = base / "daily_bin";
    if (!fs::exists(root, ec) || !fs::is_directory(root, ec)) return 0;

    int count = 0;
    for (const auto& market : MARKETS) {
        fs::path mdir = root / market;
        if (!fs::exists(mdir, ec) || !fs::is_directory(mdir, ec)) continue;

        try {
            for (const auto& sym_entry : fs::directory_iterator(mdir)) {
                if (!sym_entry.is_directory()) continue;
                for (const auto& child : fs::directory_iterator(sym_entry.path())) {
                    if (child.is_regular_file() && child.path().extension() == ".bin") {
                        if (++count >= limit) return count;
                    }
                }
            }
        } catch (...) {
            // Ignore transient filesystem errors while collector is writing.
        }
    }
    return count;
}

static bool has_daily_bin_dir(const fs::path& p) {
    std::error_code ec;
    return fs::exists(p / "daily_bin", ec) && fs::is_directory(p / "daily_bin", ec);
}

static bool looks_like_base_dir_with_data(const fs::path& p) {
    return has_daily_bin_dir(p) && count_bin_files_limited(p, 1) > 0;
}

static void resolve_base_dir_after_args(const std::string& exe_dir) {
    // Prefer a data_dump that actually contains .bin files. The previous version
    // accepted the first folder that merely had a daily_bin directory, which can
    // accidentally select an empty data_dump near the dashboard exe.
    std::vector<fs::path> candidates;
    candidates.push_back(g_config.base_dir);
    candidates.push_back(fs::current_path() / g_config.base_dir);
    candidates.push_back(fs::path(exe_dir) / g_config.base_dir);
    candidates.push_back(fs::current_path() / "data_dump");
    candidates.push_back(fs::path(exe_dir) / "data_dump");

    fs::path cur = fs::current_path();
    for (int i = 0; i < 8; ++i) {
        candidates.push_back(cur / "data_dump");
        if (!cur.has_parent_path() || cur.parent_path() == cur) break;
        cur = cur.parent_path();
    }

    fs::path ex = fs::path(exe_dir);
    for (int i = 0; i < 8; ++i) {
        candidates.push_back(ex / "data_dump");
        if (!ex.has_parent_path() || ex.parent_path() == ex) break;
        ex = ex.parent_path();
    }

    auto canonical_or_absolute = [](const fs::path& p) {
        std::error_code ec;
        fs::path c = fs::weakly_canonical(p, ec);
        if (!ec) return c;
        c = fs::absolute(p, ec);
        return ec ? p : c;
    };

    std::set<std::string> seen;
    std::vector<fs::path> unique;
    for (const auto& c : candidates) {
        fs::path normalized = canonical_or_absolute(c);
        std::string key = normalized.string();
        if (seen.insert(key).second) unique.push_back(normalized);
    }

    std::cout << "[DASHBOARD] base_dir candidates:\n";
    for (const auto& c : unique) {
        std::cout << "  " << c.string()
                  << " daily_bin=" << (has_daily_bin_dir(c) ? "yes" : "no")
                  << " bin_files_seen=" << count_bin_files_limited(c, 3)
                  << "\n";
    }

    for (const auto& c : unique) {
        if (looks_like_base_dir_with_data(c)) {
            g_config.base_dir = c;
            std::cout << "[DASHBOARD] selected BASE_DIR with .bin data: " << g_config.base_dir.string() << "\n";
            return;
        }
    }

    for (const auto& c : unique) {
        if (has_daily_bin_dir(c)) {
            g_config.base_dir = c;
            std::cout << "[DASHBOARD] selected BASE_DIR with daily_bin only: " << g_config.base_dir.string() << "\n";
            return;
        }
    }

    std::cout << "[DASHBOARD] WARNING: no valid BASE_DIR found. Using configured BASE_DIR: "
              << g_config.base_dir.string() << "\n";
}

// =============================================================================
// .BIN READER
// =============================================================================

static int64_t validate_binary_size(const fs::path& path) {
    std::error_code ec;
    auto size = fs::file_size(path, ec);
    if (ec || size == 0) return 0;
    if (size % sizeof(KlineRecord) != 0) {
        throw std::runtime_error("bad file size not divisible by 128: " + path.string());
    }
    return static_cast<int64_t>(size / sizeof(KlineRecord));
}

static std::vector<KlineRecord> read_records(const fs::path& path) {
    int64_t rows = validate_binary_size(path);
    if (rows <= 0) return {};
    std::vector<KlineRecord> out(static_cast<size_t>(rows));
    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) throw std::runtime_error("could not open " + path.string());
    f.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(out.size() * sizeof(KlineRecord)));
    if (!f) throw std::runtime_error("short read " + path.string());
    return out;
}

static std::optional<FileEdge> read_file_edge(const fs::path& path) {
    int64_t rows = validate_binary_size(path);
    if (rows <= 0) return FileEdge{path, 0, std::nullopt, std::nullopt, std::nullopt, std::nullopt};

    std::ifstream f(path, std::ios::binary);
    if (!f.is_open()) return std::nullopt;
    KlineRecord first{};
    KlineRecord last{};
    f.read(reinterpret_cast<char*>(&first), sizeof(first));
    f.seekg((rows - 1) * static_cast<int64_t>(sizeof(KlineRecord)), std::ios::beg);
    f.read(reinterpret_cast<char*>(&last), sizeof(last));
    if (!f) return std::nullopt;

    return FileEdge{path, rows, first.open_time, last.open_time, first.close_time, last.close_time};
}

static std::string day_from_filename(const fs::path& path, const std::string& market, const std::string& symbol) {
    std::string name = path.filename().string();
    std::string prefix = normalize_symbol(symbol) + "_" + normalize_market(market) + "_" + INTERVAL + "_";
    if (name.rfind(prefix, 0) != 0 || name.size() < prefix.size() + 14) return name;
    return name.substr(prefix.size(), 10);
}

static std::vector<fs::path> daily_files_for_symbol(const std::string& market, const std::string& symbol) {
    std::vector<fs::path> files;
    fs::path root = symbol_dir(market, symbol);
    if (!fs::exists(root) || !fs::is_directory(root)) return files;
    std::string sym = normalize_symbol(symbol);
    std::string prefix = sym + "_" + normalize_market(market) + "_" + INTERVAL + "_";
    for (const auto& entry : fs::directory_iterator(root)) {
        if (!entry.is_regular_file()) continue;
        std::string name = entry.path().filename().string();
        if (name.rfind(prefix, 0) == 0 && name.size() >= prefix.size() + 14 && name.ends_with(".bin")) {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end(), [&](const fs::path& a, const fs::path& b) {
        return day_from_filename(a, market, symbol) < day_from_filename(b, market, symbol);
    });
    return files;
}

static std::vector<std::string> discover_symbols_for_market(const std::string& market) {
    std::vector<std::string> symbols;
    fs::path root = market_dir(market);
    if (!fs::exists(root) || !fs::is_directory(root)) return symbols;
    for (const auto& entry : fs::directory_iterator(root)) {
        if (!entry.is_directory()) continue;
        std::string sym = upper(entry.path().filename().string());
        if (sym.empty()) continue;
        bool has_bin = false;
        for (const auto& child : fs::directory_iterator(entry.path())) {
            if (child.is_regular_file() && child.path().extension() == ".bin") {
                has_bin = true;
                break;
            }
        }
        if (has_bin) symbols.push_back(sym);
    }
    std::sort(symbols.begin(), symbols.end());
    symbols.erase(std::unique(symbols.begin(), symbols.end()), symbols.end());
    return symbols;
}

static std::map<std::string, int> market_counts() {
    std::map<std::string, int> counts;
    for (const auto& m : MARKETS) counts[m] = static_cast<int>(discover_symbols_for_market(m).size());
    return counts;
}

static std::vector<KlineRecord> dedupe_sort_rows(const std::vector<KlineRecord>& rows) {
    std::map<int64_t, KlineRecord> by_open;
    for (const auto& r : rows) by_open[r.open_time] = r;
    std::vector<KlineRecord> out;
    out.reserve(by_open.size());
    for (const auto& [_, r] : by_open) out.push_back(r);
    return out;
}

static std::pair<std::vector<KlineRecord>, std::vector<std::string>> read_latest_rows(const std::string& market, const std::string& symbol, int bars) {
    bars = clamp_int(bars, 1, MAX_BARS);
    std::vector<std::string> skipped;
    std::vector<KlineRecord> rows;
    auto files = daily_files_for_symbol(market, symbol);
    int64_t loaded = 0;
    for (auto it = files.rbegin(); it != files.rend(); ++it) {
        try {
            auto arr = read_records(*it);
            if (arr.empty()) continue;
            loaded += static_cast<int64_t>(arr.size());
            rows.insert(rows.end(), arr.begin(), arr.end());
            if (loaded >= bars + 2000) break;
        } catch (const std::exception& e) {
            skipped.push_back(it->string() + ": " + e.what());
        }
    }
    rows = dedupe_sort_rows(rows);
    if (static_cast<int>(rows.size()) > bars) {
        rows.erase(rows.begin(), rows.end() - bars);
    }
    return {rows, skipped};
}


static std::pair<std::vector<KlineRecord>, std::vector<std::string>> read_rows_after_close(
    const std::string& market,
    const std::string& symbol,
    int64_t after_close_ms,
    int max_new_rows
) {
    max_new_rows = clamp_int(max_new_rows, 1, MAX_BARS);
    std::vector<std::string> skipped;
    std::vector<KlineRecord> rows;
    auto files = daily_files_for_symbol(market, symbol);

    for (auto it = files.rbegin(); it != files.rend(); ++it) {
        try {
            auto edge = read_file_edge(*it);
            if (!edge || edge->rows <= 0) continue;
            if (edge->last_close_time_ms && *edge->last_close_time_ms <= after_close_ms) continue;

            auto arr = read_records(*it);
            for (const auto& r : arr) {
                if (r.close_time > after_close_ms) rows.push_back(r);
            }

            // Once we read a file whose first close is before/equal to our cursor,
            // older files cannot contain newer rows.
            if (edge->first_close_time_ms && *edge->first_close_time_ms <= after_close_ms) break;
            if (static_cast<int>(rows.size()) >= max_new_rows + 1000) break;
        } catch (const std::exception& e) {
            skipped.push_back(it->string() + ": " + e.what());
        }
    }

    rows = dedupe_sort_rows(rows);
    rows.erase(std::remove_if(rows.begin(), rows.end(), [&](const KlineRecord& r) {
        return r.close_time <= after_close_ms;
    }), rows.end());

    if (static_cast<int>(rows.size()) > max_new_rows) {
        rows.erase(rows.begin(), rows.end() - max_new_rows);
    }
    return {rows, skipped};
}


static std::pair<std::vector<KlineRecord>, std::vector<std::string>> read_rows_close_range(
    const std::string& market,
    const std::string& symbol,
    int64_t start_close_ms,
    int64_t end_close_ms,
    int max_rows
) {
    max_rows = clamp_int(max_rows, 1, MAX_BARS);
    if (end_close_ms < start_close_ms) return {{}, {}};

    std::vector<std::string> skipped;
    std::vector<KlineRecord> rows;
    auto files = daily_files_for_symbol(market, symbol);

    for (const auto& file : files) {
        try {
            auto edge = read_file_edge(file);
            if (!edge || edge->rows <= 0) continue;
            if (edge->last_close_time_ms && *edge->last_close_time_ms < start_close_ms) continue;
            if (edge->first_close_time_ms && *edge->first_close_time_ms > end_close_ms) break;

            auto arr = read_records(file);
            for (const auto& r : arr) {
                if (r.close_time >= start_close_ms && r.close_time <= end_close_ms) {
                    rows.push_back(r);
                }
            }
            if (static_cast<int>(rows.size()) >= max_rows) break;
        } catch (const std::exception& e) {
            skipped.push_back(file.string() + ": " + e.what());
        }
    }

    rows = dedupe_sort_rows(rows);
    rows.erase(std::remove_if(rows.begin(), rows.end(), [&](const KlineRecord& r) {
        return r.close_time < start_close_ms || r.close_time > end_close_ms;
    }), rows.end());
    if (static_cast<int>(rows.size()) > max_rows) rows.resize(static_cast<size_t>(max_rows));
    return {rows, skipped};
}

static std::pair<int64_t, int64_t> gap_info(const std::vector<KlineRecord>& rows) {
    int64_t missing = 0;
    int64_t gaps = 0;
    if (rows.size() <= 1) return {0, 0};
    for (size_t i = 1; i < rows.size(); ++i) {
        int64_t diff = rows[i].open_time - rows[i - 1].open_time;
        if (diff > INTERVAL_MS) {
            int64_t miss = (diff / INTERVAL_MS) - 1;
            if (miss > 0) {
                missing += miss;
                ++gaps;
            }
        }
    }
    return {gaps, missing};
}

// =============================================================================
// CSV TIMING READER
// =============================================================================

static std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> fields;
    std::string cur;
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (c == '"') {
            if (in_quotes && i + 1 < line.size() && line[i + 1] == '"') {
                cur.push_back('"');
                ++i;
            } else {
                in_quotes = !in_quotes;
            }
        } else if (c == ',' && !in_quotes) {
            fields.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    fields.push_back(cur);
    return fields;
}

static int64_t csv_i64(const std::vector<std::string>& f, const std::unordered_map<std::string, size_t>& h, const std::string& name, int64_t fallback = 0) {
    auto it = h.find(lower(name));
    if (it == h.end() || it->second >= f.size()) return fallback;
    return safe_stoll(f[it->second], fallback);
}

static std::string csv_str(const std::vector<std::string>& f, const std::unordered_map<std::string, size_t>& h, const std::string& name) {
    auto it = h.find(lower(name));
    if (it == h.end() || it->second >= f.size()) return "";
    return trim(f[it->second]);
}

static int64_t csv_us_or_ms(const std::vector<std::string>& f, const std::unordered_map<std::string, size_t>& h, const std::string& us_col, const std::string& ms_col) {
    int64_t us = csv_i64(f, h, us_col, 0);
    if (us != 0) return us;
    int64_t ms = csv_i64(f, h, ms_col, 0);
    return ms == 0 ? 0 : ms * 1000LL;
}


struct TimingCacheState {
    std::mutex mu;
    fs::path path;
    uintmax_t file_size = 0;
    std::streampos offset = 0;
    bool header_loaded = false;
    std::unordered_map<std::string, size_t> header;
    std::deque<TimingRow> rows;
};

static TimingCacheState g_timing_cache;

static std::optional<TimingRow> parse_timing_row_from_fields(
    const std::vector<std::string>& fields,
    const std::unordered_map<std::string, size_t>& h
) {
    std::string row_market = normalize_market(csv_str(fields, h, "market"));
    std::string row_symbol = normalize_symbol(csv_str(fields, h, "symbol"));
    if (row_market.empty() || row_symbol.empty()) return std::nullopt;

    TimingRow r;
    r.market = row_market;
    r.symbol = row_symbol;
    r.scheduled_close_us = csv_us_or_ms(fields, h, "scheduled_close_us", "scheduled_close_ms");
    r.exchange_event_us = csv_us_or_ms(fields, h, "exchange_event_us", "exchange_event_ms");
    r.received_us = csv_us_or_ms(fields, h, "received_us", "received_ms");
    r.saved_us = csv_us_or_ms(fields, h, "saved_us", "saved_ms");
    r.scheduled_close_to_receive_us = csv_i64(fields, h, "scheduled_close_to_receive_us", csv_i64(fields, h, "scheduled_close_to_receive_ms", 0) * 1000LL);
    r.exchange_event_to_receive_us = csv_i64(fields, h, "exchange_event_to_receive_us", csv_i64(fields, h, "exchange_event_to_receive_ms", 0) * 1000LL);
    r.receive_to_saved_us = csv_i64(fields, h, "receive_to_saved_us", csv_i64(fields, h, "receive_to_saved_ms", 0) * 1000LL);
    r.write_call_us = csv_i64(fields, h, "write_call_us", csv_i64(fields, h, "write_call_ms", 0) * 1000LL);
    r.scheduled_close_to_saved_us = csv_i64(fields, h, "scheduled_close_to_saved_us", csv_i64(fields, h, "scheduled_close_to_saved_ms", 0) * 1000LL);
    r.rows_written = csv_i64(fields, h, "rows_written", 0);
    return r;
}

static void reset_timing_cache_locked(TimingCacheState& c, const fs::path& path) {
    c.path = path;
    c.file_size = 0;
    c.offset = 0;
    c.header_loaded = false;
    c.header.clear();
    c.rows.clear();
}

static void update_timing_cache_locked() {
    fs::path path = timing_csv_path();
    std::error_code ec;
    if (!fs::exists(path, ec)) {
        reset_timing_cache_locked(g_timing_cache, path);
        return;
    }

    uintmax_t size = fs::file_size(path, ec);
    if (ec) return;

    if (g_timing_cache.path != path || size < g_timing_cache.file_size) {
        reset_timing_cache_locked(g_timing_cache, path);
    }
    if (size == g_timing_cache.file_size && g_timing_cache.header_loaded) return;

    std::ifstream f(path);
    if (!f.is_open()) return;

    if (!g_timing_cache.header_loaded) {
        std::string header_line;
        if (!std::getline(f, header_line)) return;
        auto header_fields = split_csv_line(header_line);
        for (size_t i = 0; i < header_fields.size(); ++i) {
            g_timing_cache.header[lower(trim(header_fields[i]))] = i;
        }
        g_timing_cache.header_loaded = true;
        g_timing_cache.offset = f.tellg();
    } else {
        f.seekg(g_timing_cache.offset);
    }

    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        auto fields = split_csv_line(line);
        auto row = parse_timing_row_from_fields(fields, g_timing_cache.header);
        if (!row) continue;
        g_timing_cache.rows.push_back(*row);
        while (g_timing_cache.rows.size() > static_cast<size_t>(MAX_TIMING_ROWS)) {
            g_timing_cache.rows.pop_front();
        }
    }

    if (f.eof()) {
        g_timing_cache.offset = static_cast<std::streampos>(size);
        g_timing_cache.file_size = size;
    } else {
        auto pos = f.tellg();
        if (pos >= 0) g_timing_cache.offset = pos;
        g_timing_cache.file_size = size;
    }
}

static std::vector<TimingRow> read_timing_rows(const std::string& market_raw, const std::string& symbol_raw, int max_rows) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    max_rows = clamp_int(max_rows, 1, MAX_TIMING_ROWS);

    std::lock_guard<std::mutex> lk(g_timing_cache.mu);
    update_timing_cache_locked();

    std::deque<TimingRow> out;
    for (const auto& r : g_timing_cache.rows) {
        if (r.market != market || r.symbol != symbol) continue;
        out.push_back(r);
        while (static_cast<int>(out.size()) > max_rows) out.pop_front();
    }
    return std::vector<TimingRow>(out.begin(), out.end());
}

static std::vector<TimingRow> read_timing_rows_since(
    const std::string& market_raw,
    const std::string& symbol_raw,
    int64_t after_received_us,
    int max_rows
) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    max_rows = clamp_int(max_rows, 1, MAX_TIMING_ROWS);

    std::lock_guard<std::mutex> lk(g_timing_cache.mu);
    update_timing_cache_locked();

    std::vector<TimingRow> out;
    for (const auto& r : g_timing_cache.rows) {
        if (r.market != market || r.symbol != symbol) continue;
        if (r.received_us <= after_received_us) continue;
        out.push_back(r);
        if (static_cast<int>(out.size()) > max_rows) {
            out.erase(out.begin(), out.begin() + (out.size() - max_rows));
        }
    }
    return out;
}

// =============================================================================
// JSON PAYLOADS
// =============================================================================

static std::string api_markets_json() {
    auto counts = market_counts();
    std::ostringstream os;
    os << "{\"markets\":[\"spot\",\"um\",\"cm\"],";
    os << "\"default_market\":" << json_str(g_config.default_market) << ',';
    os << "\"default_symbol\":" << json_str(g_config.default_symbol) << ',';
    os << "\"default_bars\":" << g_config.default_bars << ',';
    os << "\"default_timing_rows\":" << g_config.default_timing_rows << ',';
    os << "\"default_refresh_seconds\":" << g_config.default_refresh_seconds << ',';
    os << "\"base_dir\":" << json_str(g_config.base_dir.string()) << ',';
    os << "\"daily_bin_dir\":" << json_str(daily_bin_dir().string()) << ',';
    os << "\"base_dir_exists\":" << (fs::exists(g_config.base_dir) ? "true" : "false") << ',';
    os << "\"daily_bin_dir_exists\":" << (fs::exists(daily_bin_dir()) ? "true" : "false") << ',';
    os << "\"timing_csv_path\":" << json_str(timing_csv_path().string()) << ',';
    os << "\"dtype_itemsize\":" << sizeof(KlineRecord) << ',';
    os << "\"counts\":{";
    bool first = true;
    for (const auto& m : MARKETS) {
        if (!first) os << ',';
        first = false;
        os << json_str(m) << ':' << counts[m];
    }
    os << "}}";
    return os.str();
}

static std::string api_symbols_json(const std::string& market_raw) {
    std::string market = normalize_market(market_raw);
    auto symbols = discover_symbols_for_market(market);
    std::ostringstream os;
    os << "{\"market\":" << json_str(market) << ",\"count\":" << symbols.size() << ",\"symbols\":[";
    for (size_t i = 0; i < symbols.size(); ++i) {
        if (i) os << ',';
        os << json_str(symbols[i]);
    }
    os << "]}";
    return os.str();
}

static std::string api_summary_json(const std::string& market_raw) {
    std::string market = normalize_market(market_raw);
    auto symbols = discover_symbols_for_market(market);
    std::ostringstream os;
    os << "{\"market\":" << json_str(market) << ",\"rows\":[";
    bool first_row = true;
    for (const auto& sym : symbols) {
        auto files = daily_files_for_symbol(market, sym);
        int64_t total_rows = 0;
        int readable = 0;
        int corrupt = 0;
        std::optional<int64_t> first_close;
        std::optional<int64_t> last_close;
        for (const auto& p : files) {
            try {
                auto edge = read_file_edge(p);
                if (!edge || edge->rows <= 0) continue;
                ++readable;
                total_rows += edge->rows;
                if (edge->first_close_time_ms) first_close = first_close ? std::min(*first_close, *edge->first_close_time_ms) : edge->first_close_time_ms;
                if (edge->last_close_time_ms) last_close = last_close ? std::max(*last_close, *edge->last_close_time_ms) : edge->last_close_time_ms;
            } catch (...) {
                ++corrupt;
            }
        }
        if (!first_row) os << ',';
        first_row = false;
        os << "{\"symbol\":" << json_str(sym)
           << ",\"files\":" << readable
           << ",\"rows\":" << total_rows
           << ",\"first_close_utc\":" << json_str(first_close ? iso_from_ms(*first_close) : "-")
           << ",\"last_close_utc\":" << json_str(last_close ? iso_from_ms(*last_close) : "-")
           << ",\"corrupt_files\":" << corrupt << "}";
    }
    os << "],\"count\":" << symbols.size() << "}";
    return os.str();
}

static std::string api_range_json(const std::string& market_raw, const std::string& symbol_raw) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    auto files = daily_files_for_symbol(market, symbol);
    int64_t total_rows = 0;
    int readable = 0;
    int corrupt = 0;
    std::optional<int64_t> first_open;
    std::optional<int64_t> last_open;
    std::optional<int64_t> first_close;
    std::optional<int64_t> last_close;
    for (const auto& p : files) {
        try {
            auto edge = read_file_edge(p);
            if (!edge || edge->rows <= 0) continue;
            ++readable;
            total_rows += edge->rows;
            if (edge->first_open_time_ms) first_open = first_open ? std::min(*first_open, *edge->first_open_time_ms) : edge->first_open_time_ms;
            if (edge->last_open_time_ms) last_open = last_open ? std::max(*last_open, *edge->last_open_time_ms) : edge->last_open_time_ms;
            if (edge->first_close_time_ms) first_close = first_close ? std::min(*first_close, *edge->first_close_time_ms) : edge->first_close_time_ms;
            if (edge->last_close_time_ms) last_close = last_close ? std::max(*last_close, *edge->last_close_time_ms) : edge->last_close_time_ms;
        } catch (...) {
            ++corrupt;
        }
    }
    bool has = readable > 0 && last_close.has_value();
    std::ostringstream os;
    os << "{\"market\":" << json_str(market)
       << ",\"symbol\":" << json_str(symbol)
       << ",\"interval\":" << json_str(INTERVAL)
       << ",\"has_data\":" << (has ? "true" : "false")
       << ",\"file_count\":" << files.size()
       << ",\"readable_file_count\":" << readable
       << ",\"row_count\":" << total_rows
       << ",\"corrupt_files\":" << corrupt
       << ",\"first_open_time_ms\":" << (first_open ? std::to_string(*first_open) : "null")
       << ",\"last_open_time_ms\":" << (last_open ? std::to_string(*last_open) : "null")
       << ",\"first_close_time_ms\":" << (first_close ? std::to_string(*first_close) : "null")
       << ",\"last_close_time_ms\":" << (last_close ? std::to_string(*last_close) : "null")
       << ",\"first_close_utc\":" << json_str(first_close ? iso_from_ms(*first_close) : "")
       << ",\"last_close_utc\":" << json_str(last_close ? iso_from_ms(*last_close) : "")
       << "}";
    return os.str();
}


static void write_kline_record_json(std::ostringstream& os, const std::string& symbol, const KlineRecord& r) {
    os << "{\"symbol\":" << json_str(symbol_from_record(r).empty() ? symbol : symbol_from_record(r))
       << ",\"open_time_ms\":" << r.open_time
       << ",\"close_time_ms\":" << r.close_time
       << ",\"open_time_utc\":" << json_str(iso_from_ms(r.open_time))
       << ",\"close_time_utc\":" << json_str(iso_from_ms(r.close_time))
       << ",\"open\":" << r.open
       << ",\"high\":" << r.high
       << ",\"low\":" << r.low
       << ",\"close\":" << r.close
       << ",\"volume\":" << r.volume
       << ",\"quote_volume\":" << r.quote_volume
       << ",\"trades\":" << r.trades
       << ",\"taker_base_vol\":" << r.taker_base_vol
       << ",\"taker_quote_vol\":" << r.taker_quote_vol
       << ",\"maker_base_vol\":" << r.maker_base_vol
       << ",\"maker_quote_vol\":" << r.maker_quote_vol
       << "}";
}

static std::string api_ohlcv_json(const std::string& market_raw, const std::string& symbol_raw, int bars) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    auto [rows, skipped] = read_latest_rows(market, symbol, bars);
    auto [gap_count, missing_rows] = gap_info(rows);
    std::ostringstream os;
    os << "{\"market\":" << json_str(market)
       << ",\"symbol\":" << json_str(symbol)
       << ",\"interval\":" << json_str(INTERVAL)
       << ",\"requested_bars\":" << clamp_int(bars, 1, MAX_BARS)
       << ",\"row_count\":" << rows.size()
       << ",\"gap_count\":" << gap_count
       << ",\"missing_rows_plotted\":" << missing_rows;
    if (!rows.empty()) {
        os << ",\"first_close_time_ms\":" << rows.front().close_time
           << ",\"latest_close_time_ms\":" << rows.back().close_time
           << ",\"first_close_utc\":" << json_str(iso_from_ms(rows.front().close_time))
           << ",\"latest_close_utc\":" << json_str(iso_from_ms(rows.back().close_time));
    } else {
        os << ",\"first_close_time_ms\":null,\"latest_close_time_ms\":null,\"first_close_utc\":\"\",\"latest_close_utc\":\"\"";
    }
    os << ",\"skipped_files\":[";
    for (size_t i = 0; i < skipped.size(); ++i) {
        if (i) os << ',';
        os << json_str(skipped[i]);
    }
    os << "],\"rows\":[";
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) os << ',';
        const auto& r = rows[i];
        os << "{\"symbol\":" << json_str(symbol_from_record(r))
           << ",\"open_time_ms\":" << r.open_time
           << ",\"close_time_ms\":" << r.close_time
           << ",\"open_time_utc\":" << json_str(iso_from_ms(r.open_time))
           << ",\"close_time_utc\":" << json_str(iso_from_ms(r.close_time))
           << ",\"open\":" << r.open
           << ",\"high\":" << r.high
           << ",\"low\":" << r.low
           << ",\"close\":" << r.close
           << ",\"volume\":" << r.volume
           << ",\"quote_volume\":" << r.quote_volume
           << ",\"trades\":" << r.trades
           << ",\"taker_base_vol\":" << r.taker_base_vol
           << ",\"taker_quote_vol\":" << r.taker_quote_vol
           << ",\"maker_base_vol\":" << r.maker_base_vol
           << ",\"maker_quote_vol\":" << r.maker_quote_vol
           << "}";
    }
    os << "]}";
    return os.str();
}


static std::string api_ohlcv_since_json(
    const std::string& market_raw,
    const std::string& symbol_raw,
    int64_t after_close_ms,
    int max_new_rows
) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    max_new_rows = clamp_int(max_new_rows, 1, MAX_BARS);
    auto [rows, skipped] = read_rows_after_close(market, symbol, after_close_ms, max_new_rows);

    std::ostringstream os;
    os << "{\"market\":" << json_str(market)
       << ",\"symbol\":" << json_str(symbol)
       << ",\"interval\":" << json_str(INTERVAL)
       << ",\"after_close_time_ms\":" << after_close_ms
       << ",\"max_new_rows\":" << max_new_rows
       << ",\"row_count\":" << rows.size();
    if (!rows.empty()) {
        os << ",\"latest_close_time_ms\":" << rows.back().close_time
           << ",\"latest_close_utc\":" << json_str(iso_from_ms(rows.back().close_time));
    } else {
        os << ",\"latest_close_time_ms\":null,\"latest_close_utc\":\"\"";
    }
    os << ",\"skipped_files\":[";
    for (size_t i = 0; i < skipped.size(); ++i) {
        if (i) os << ',';
        os << json_str(skipped[i]);
    }
    os << "],\"rows\":[";
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) os << ',';
        write_kline_record_json(os, symbol, rows[i]);
    }
    os << "]}";
    return os.str();
}


static std::string api_ohlcv_range_json(
    const std::string& market_raw,
    const std::string& symbol_raw,
    int64_t start_close_ms,
    int64_t end_close_ms,
    int max_rows
) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    max_rows = clamp_int(max_rows, 1, MAX_BARS);
    auto [rows, skipped] = read_rows_close_range(market, symbol, start_close_ms, end_close_ms, max_rows);

    std::ostringstream os;
    os << "{\"market\":" << json_str(market)
       << ",\"symbol\":" << json_str(symbol)
       << ",\"interval\":" << json_str(INTERVAL)
       << ",\"start_close_ms\":" << start_close_ms
       << ",\"end_close_ms\":" << end_close_ms
       << ",\"max_rows\":" << max_rows
       << ",\"row_count\":" << rows.size();
    os << ",\"skipped_files\":[";
    for (size_t i = 0; i < skipped.size(); ++i) {
        if (i) os << ',';
        os << json_str(skipped[i]);
    }
    os << "],\"rows\":[";
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) os << ',';
        write_kline_record_json(os, symbol, rows[i]);
    }
    os << "]}";
    return os.str();
}

static std::string api_timings_json(const std::string& market_raw, const std::string& symbol_raw, int rows_requested) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    auto rows = read_timing_rows(market, symbol, rows_requested);
    std::ostringstream os;
    os << "{\"market\":" << json_str(market)
       << ",\"symbol\":" << json_str(symbol)
       << ",\"timing_csv_path\":" << json_str(timing_csv_path().string())
       << ",\"requested_rows\":" << clamp_int(rows_requested, 1, MAX_TIMING_ROWS)
       << ",\"row_count\":" << rows.size()
       << ",\"rows\":[";
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) os << ',';
        const auto& r = rows[i];
        os << "{\"market\":" << json_str(r.market)
           << ",\"symbol\":" << json_str(r.symbol)
           << ",\"scheduled_close_us\":" << r.scheduled_close_us
           << ",\"scheduled_close_utc\":" << json_str(iso_from_us(r.scheduled_close_us))
           << ",\"exchange_event_us\":" << r.exchange_event_us
           << ",\"exchange_event_utc\":" << json_str(iso_from_us(r.exchange_event_us))
           << ",\"received_us\":" << r.received_us
           << ",\"received_utc\":" << json_str(iso_from_us(r.received_us))
           << ",\"saved_us\":" << r.saved_us
           << ",\"saved_utc\":" << json_str(iso_from_us(r.saved_us))
           << ",\"scheduled_close_to_receive_us\":" << r.scheduled_close_to_receive_us
           << ",\"exchange_event_to_receive_us\":" << r.exchange_event_to_receive_us
           << ",\"receive_to_saved_us\":" << r.receive_to_saved_us
           << ",\"write_call_us\":" << r.write_call_us
           << ",\"scheduled_close_to_saved_us\":" << r.scheduled_close_to_saved_us
           << ",\"rows_written\":" << r.rows_written
           << "}";
    }
    os << "]}";
    return os.str();
}


static std::string api_timings_since_json(
    const std::string& market_raw,
    const std::string& symbol_raw,
    int64_t after_received_us,
    int rows_requested
) {
    std::string market = normalize_market(market_raw);
    std::string symbol = normalize_symbol(symbol_raw);
    auto rows = read_timing_rows_since(market, symbol, after_received_us, rows_requested);
    std::ostringstream os;
    os << "{\"market\":" << json_str(market)
       << ",\"symbol\":" << json_str(symbol)
       << ",\"timing_csv_path\":" << json_str(timing_csv_path().string())
       << ",\"after_received_us\":" << after_received_us
       << ",\"requested_rows\":" << clamp_int(rows_requested, 1, MAX_TIMING_ROWS)
       << ",\"row_count\":" << rows.size()
       << ",\"rows\":[";
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) os << ',';
        const auto& r = rows[i];
        os << "{\"market\":" << json_str(r.market)
           << ",\"symbol\":" << json_str(r.symbol)
           << ",\"scheduled_close_us\":" << r.scheduled_close_us
           << ",\"scheduled_close_utc\":" << json_str(iso_from_us(r.scheduled_close_us))
           << ",\"exchange_event_us\":" << r.exchange_event_us
           << ",\"exchange_event_utc\":" << json_str(iso_from_us(r.exchange_event_us))
           << ",\"received_us\":" << r.received_us
           << ",\"received_utc\":" << json_str(iso_from_us(r.received_us))
           << ",\"saved_us\":" << r.saved_us
           << ",\"saved_utc\":" << json_str(iso_from_us(r.saved_us))
           << ",\"scheduled_close_to_receive_us\":" << r.scheduled_close_to_receive_us
           << ",\"exchange_event_to_receive_us\":" << r.exchange_event_to_receive_us
           << ",\"receive_to_saved_us\":" << r.receive_to_saved_us
           << ",\"write_call_us\":" << r.write_call_us
           << ",\"scheduled_close_to_saved_us\":" << r.scheduled_close_to_saved_us
           << ",\"rows_written\":" << r.rows_written
           << "}";
    }
    os << "]}";
    return os.str();
}


static std::string api_debug_json() {
    std::ostringstream os;
    auto counts = market_counts();
    os << "{\"base_dir\":" << json_str(g_config.base_dir.string())
       << ",\"current_path\":" << json_str(fs::current_path().string())
       << ",\"daily_bin_dir\":" << json_str(daily_bin_dir().string())
       << ",\"base_dir_exists\":" << (fs::exists(g_config.base_dir) ? "true" : "false")
       << ",\"daily_bin_dir_exists\":" << (fs::exists(daily_bin_dir()) ? "true" : "false")
       << ",\"bin_files_seen\":" << count_bin_files_limited(g_config.base_dir, 1000000)
       << ",\"timing_csv_path\":" << json_str(timing_csv_path().string())
       << ",\"timing_csv_exists\":" << (fs::exists(timing_csv_path()) ? "true" : "false")
       << ",\"markets\":{\"spot\":" << counts["spot"]
       << ",\"um\":" << counts["um"]
       << ",\"cm\":" << counts["cm"] << "}}";
    return os.str();
}

// =============================================================================
// HTML
// =============================================================================

static const char* INDEX_HTML = R"HTML(<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Binance C++ .bin + Timing Plotter</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root { color-scheme: dark; }
    body { margin:0; font-family:Segoe UI, Arial, sans-serif; background:#0b1020; color:#e5e7eb; }
    header { padding:18px 22px; background:#111827; border-bottom:1px solid #263244; }
    h1 { margin:0 auto; max-width:1700px; font-size:22px; }
    .caption { color:#9ca3af; font-size:13px; margin:5px auto 0; max-width:1700px; }
    .wrap { padding:18px 22px; display:grid; gap:16px; max-width:1700px; margin:0 auto; }
    .panel { background:#111827; border:1px solid #263244; border-radius:14px; padding:14px; box-shadow:0 10px 22px rgba(0,0,0,.25); }
    .controls { display:grid; grid-template-columns:120px minmax(200px,1fr) 110px 110px 135px 135px 130px 110px; gap:12px; align-items:end; }
    label { display:grid; gap:6px; font-size:12px; color:#b9c0cc; }
    select,input,button { border:1px solid #374151; background:#0b1020; color:#f9fafb; border-radius:10px; padding:9px 10px; font-size:14px; }
    button { cursor:pointer; background:#1f2937; }
    button:hover { background:#273449; }
    .status { display:flex; flex-wrap:wrap; gap:10px; font-size:13px; color:#cbd5e1; margin-top:12px; }
    .pill { padding:6px 9px; border-radius:999px; background:#172033; border:1px solid #263244; }
    .ok { color:#86efac; } .bad { color:#fca5a5; }
    #ohlcvChart { width:100%; height:58vh; min-height:420px; }
    #timingChart { width:100%; height:42vh; min-height:340px; }
    table { width:100%; border-collapse:collapse; font-size:12px; table-layout:fixed; }
    th,td { text-align:left; padding:8px; border-bottom:1px solid #243044; overflow-wrap:anywhere; vertical-align:top; }
    th { color:#93c5fd; position:sticky; top:0; background:#111827; z-index:1; }
    .scroll { height:280px; overflow:auto; resize:vertical; border:1px solid rgba(38,50,68,.55); border-radius:10px; }
    @media (max-width:1200px) { .controls { grid-template-columns:1fr 1fr; } }
  </style>
</head>
<body>
<header>
  <h1>Binance C++ .bin OHLCV + Live Timing Plotter</h1>
  <div class="caption">Reads collector daily .bin files and logs/live_symbol_delays_microseconds.csv from your selected BASE_DIR.</div>
</header>
<main class="wrap">
  <section class="panel">
    <div class="controls">
      <label>Market <select id="market"></select></label>
      <label>Symbol <select id="symbol"></select></label>
      <label>OHLCV rows <input id="bars" type="number" min="1" max="250000" value="500"></label>
      <label>Timing rows <input id="timingRows" type="number" min="1" max="250000" value="5000"></label>
      <label>Plot type <select id="plotType"><option value="candles">Candles + volume</option><option value="close">Close line</option></select></label>
      <label>Refresh sec <input id="refresh" type="number" min="1" max="300" value="3"></label>
      <button id="plotBtn">Plot now</button>
      <button id="pauseBtn">Pause</button>
    </div>
    <div class="status">
      <span class="pill" id="rootStatus">Loading...</span>
      <span class="pill" id="rangeStatus">Range: -</span>
      <span class="pill" id="latestStatus">Latest: -</span>
      <span class="pill" id="timingStatus">Timing: -</span>
      <span class="pill" id="gapStatus">Gaps: -</span>
    </div>
  </section>

  <section class="panel"><div id="ohlcvChart"></div></section>
  <section class="panel"><h3>Live timing delays, microseconds</h3><div id="timingChart"></div></section>
  <section class="panel"><h3>Symbols in selected market</h3><div class="scroll"><table id="summaryTable"></table></div></section>
</main>
<script>
const $ = id => document.getElementById(id);
let pollHandle = null;
let paused = false;
let lastSeenCloseMs = null;
let lastTimingReceivedUs = null;
let chartReady = false;
let timingReady = false;
let ohlcvRowsCache = [];
let ohlcvCloseSet = new Set();
let gapRepairInFlight = false;
let lastGapRepairCheckMs = 0;
function qs(p){ return new URLSearchParams(p).toString(); }
function fmtInt(n){ return Number(n || 0).toLocaleString(); }
async function fetchJSON(url){ const r = await fetch(url,{cache:'no-store'}); if(!r.ok) throw new Error(await r.text()); return await r.json(); }
function renderTable(id, rows){
  const el=$(id); if(!rows || !rows.length){ el.innerHTML='<tr><td>No rows</td></tr>'; return; }
  const cols=Object.keys(rows[0]);
  el.innerHTML='<thead><tr>'+cols.map(c=>`<th>${c}</th>`).join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+cols.map(c=>`<td>${r[c] ?? ''}</td>`).join('')+'</tr>').join('')+'</tbody>';
}
function baseLayout(){ return {paper_bgcolor:'#111827',plot_bgcolor:'#0b1020',font:{color:'#e5e7eb'},margin:{l:65,r:30,t:35,b:45},hovermode:'x unified',legend:{orientation:'h'},xaxis:{gridcolor:'#233047'}}; }

function rowCloseKey(r){ return String(r.close_time_ms); }
function resetOHLCVCache(rows){
  ohlcvRowsCache = (rows || []).slice().sort((a,b)=>a.close_time_ms-b.close_time_ms);
  ohlcvCloseSet = new Set(ohlcvRowsCache.map(rowCloseKey));
}
function mergeOHLCVRows(rows){
  let changed = false;
  for(const r of (rows || [])){
    const key = rowCloseKey(r);
    if(!ohlcvCloseSet.has(key)){
      ohlcvRowsCache.push(r);
      ohlcvCloseSet.add(key);
      changed = true;
    }
  }
  if(changed){
    ohlcvRowsCache.sort((a,b)=>a.close_time_ms-b.close_time_ms);
    const maxBars = ohlcvMax();
    while(ohlcvRowsCache.length > maxBars){
      const old = ohlcvRowsCache.shift();
      if(old) ohlcvCloseSet.delete(rowCloseKey(old));
    }
    lastSeenCloseMs = ohlcvRowsCache.length ? ohlcvRowsCache[ohlcvRowsCache.length-1].close_time_ms : lastSeenCloseMs;
  }
  return changed;
}
function currentGapRanges(){
  const ranges = [];
  if(ohlcvRowsCache.length < 2) return ranges;
  for(let i=1;i<ohlcvRowsCache.length;i++){
    const prev = ohlcvRowsCache[i-1].close_time_ms;
    const cur = ohlcvRowsCache[i].close_time_ms;
    if(cur - prev > 60000){
      ranges.push({start: prev + 60000, end: cur - 60000, missing: Math.floor((cur-prev)/60000)-1});
    }
  }
  return ranges;
}
async function renderOHLCVCache(statusText){
  const rows = ohlcvRowsCache;
  const market=$('market').value, symbol=$('symbol').value, plotType=$('plotType').value;
  if(!rows.length){ Plotly.purge('ohlcvChart'); chartReady=false; return; }
  const x=rows.map(r=>new Date(r.open_time_ms)); const close=rows.map(r=>r.close); const volume=rows.map(r=>r.volume);
  let traces=[]; let layout=baseLayout(); layout.xaxis.title='UTC open time'; layout.xaxis.rangeslider={visible:false};
  if(plotType==='candles'){
    traces.push({x,open:rows.map(r=>r.open),high:rows.map(r=>r.high),low:rows.map(r=>r.low),close,type:'candlestick',name:`${market} ${symbol}`,yaxis:'y'});
    traces.push({x,y:volume,type:'bar',name:'volume',yaxis:'y2',opacity:.35});
    layout.yaxis={title:'Price',gridcolor:'#233047',domain:[.28,1]}; layout.yaxis2={title:'Volume',gridcolor:'#233047',domain:[0,.20]};
  } else {
    traces.push({x,y:close,type:'scatter',mode:'lines',name:`${market} ${symbol} close`}); layout.yaxis={title:'Close',gridcolor:'#233047'};
  }
  await Plotly.react('ohlcvChart',traces,layout,{responsive:true});
  chartReady=true;
  lastSeenCloseMs=rows[rows.length-1].close_time_ms;
  const gaps=currentGapRanges();
  const missing=gaps.reduce((a,g)=>a+g.missing,0);
  $('latestStatus').textContent=statusText || `Latest plotted: ${rows[rows.length-1].close_time_utc} | rows ${fmtInt(rows.length)}`;
  $('gapStatus').textContent=`Gaps: ${fmtInt(gaps.length)} ranges, missing rows ${fmtInt(missing)}`;
}

async function loadMarkets(){
  const data = await fetchJSON('/api/markets');
  $('bars').value = data.default_bars || 500;
  $('timingRows').value = data.default_timing_rows || 5000;
  $('refresh').value = data.default_refresh_seconds || 3;
  const sel=$('market'); sel.innerHTML='';
  data.markets.forEach(m=>{ const o=document.createElement('option'); o.value=m; o.textContent=`${m} (${data.counts[m] || 0})`; sel.appendChild(o); });
  if(data.default_market) sel.value=data.default_market;
  window.dashboardBaseDir = data.base_dir || '';
  window.dashboardDailyBinDir = data.daily_bin_dir || '';
  const okText = data.daily_bin_dir_exists ? 'OK' : 'NOT FOUND';
  $('rootStatus').textContent=`BASE_DIR: ${data.base_dir} | daily_bin: ${okText} | CSV: ${data.timing_csv_path}`;
  window.defaultSymbol = data.default_symbol || '';
}
async function loadSymbols(){
  const market=$('market').value; const data=await fetchJSON('/api/symbols?'+qs({market}));
  const sel=$('symbol'); sel.innerHTML='';
  data.symbols.forEach(sym=>{ const o=document.createElement('option'); o.value=sym; o.textContent=sym; sel.appendChild(o); });
  if(window.defaultSymbol && data.symbols.includes(window.defaultSymbol)) sel.value=window.defaultSymbol;
  if(!data.symbols.length){ $('rangeStatus').innerHTML=`<span class="bad">No symbols found for ${market}. Check BASE_DIR/daily_bin path: ${window.dashboardDailyBinDir || ''}</span>`; Plotly.purge('ohlcvChart'); Plotly.purge('timingChart'); renderTable('summaryTable',[]); return; }
  await loadSummary(); await loadRangeAndPlot();
}
async function loadSummary(){ const data=await fetchJSON('/api/summary?'+qs({market:$('market').value})); renderTable('summaryTable',data.rows || []); }
async function loadRangeAndPlot(){
  const market=$('market').value, symbol=$('symbol').value; if(!market || !symbol) return;
  const range=await fetchJSON('/api/range?'+qs({market,symbol}));
  if(!range.has_data){ $('rangeStatus').innerHTML=`<span class="bad">No data for ${market} ${symbol}</span>`; return; }
  $('rangeStatus').textContent=`Range: ${range.first_close_utc} → ${range.last_close_utc} | files ${fmtInt(range.readable_file_count)} | rows ${fmtInt(range.row_count)}`;
  await plotAll();
}
function ohlcvMax(){ return Math.max(1, Number($('bars').value || 500)); }
function timingMax(){ return Math.max(1, Number($('timingRows').value || 5000)); }
async function plotOHLCV(){
  const market=$('market').value, symbol=$('symbol').value, bars=ohlcvMax();
  const data=await fetchJSON('/api/ohlcv?'+qs({market,symbol,bars})); const rows=data.rows || [];
  if(!rows.length){ $('latestStatus').innerHTML='<span class="bad">No OHLCV rows loaded</span>'; Plotly.purge('ohlcvChart'); chartReady=false; resetOHLCVCache([]); return; }
  resetOHLCVCache(rows);
  await renderOHLCVCache(`Latest plotted: ${data.latest_close_utc} | rows ${fmtInt(data.row_count)}`);
}
async function appendOHLCV(){
  if(!chartReady || !lastSeenCloseMs){ await plotOHLCV(); return; }
  const market=$('market').value, symbol=$('symbol').value, plotType=$('plotType').value;
  const data=await fetchJSON('/api/ohlcv_since?'+qs({market,symbol,after_close_ms:lastSeenCloseMs,max_rows:Math.min(1000, ohlcvMax())}));
  const rows=(data.rows || []).filter(r=>r.close_time_ms>lastSeenCloseMs && !ohlcvCloseSet.has(rowCloseKey(r)));
  if(rows.length){
    mergeOHLCVRows(rows);
    const x=rows.map(r=>new Date(r.open_time_ms));
    const maxBars=ohlcvMax();
    if(plotType==='candles'){
      await Plotly.extendTraces('ohlcvChart',{x:[x],open:[rows.map(r=>r.open)],high:[rows.map(r=>r.high)],low:[rows.map(r=>r.low)],close:[rows.map(r=>r.close)]},[0],maxBars);
      await Plotly.extendTraces('ohlcvChart',{x:[x],y:[rows.map(r=>r.volume)]},[1],maxBars);
    } else {
      await Plotly.extendTraces('ohlcvChart',{x:[x],y:[rows.map(r=>r.close)]},[0],maxBars);
    }
    lastSeenCloseMs=ohlcvRowsCache[ohlcvRowsCache.length-1].close_time_ms;
    $('latestStatus').textContent=`Latest appended: ${rows[rows.length-1].close_time_utc} | new bars ${fmtInt(rows.length)}`;
  }
  await repairMissingOHLCV();
}
async function repairMissingOHLCV(){
  if(gapRepairInFlight || !chartReady || ohlcvRowsCache.length < 2) return;
  // Avoid pounding the server if a historical gap is genuinely not filled yet.
  const now = Date.now();
  const minMs = Math.max(3000, Number($('refresh').value || 3) * 1000);
  if(now - lastGapRepairCheckMs < minMs) return;
  lastGapRepairCheckMs = now;
  const gaps = currentGapRanges();
  if(!gaps.length) return;
  gapRepairInFlight = true;
  try{
    const market=$('market').value, symbol=$('symbol').value;
    let repaired = 0;
    // Check a few ranges per poll. This fills backfilled candles without reloading the full chart.
    for(const g of gaps.slice(0, 5)){
      const data = await fetchJSON('/api/ohlcv_range?'+qs({market,symbol,start_close_ms:g.start,end_close_ms:g.end,max_rows:Math.min(2000,g.missing)}));
      const newRows = (data.rows || []).filter(r=>!ohlcvCloseSet.has(rowCloseKey(r)));
      if(mergeOHLCVRows(newRows)) repaired += newRows.length;
    }
    if(repaired > 0){
      await renderOHLCVCache(`Repaired missing bars: ${fmtInt(repaired)} | latest ${new Date(lastSeenCloseMs).toISOString()}`);
    } else {
      const remaining=currentGapRanges();
      const missing=remaining.reduce((a,g)=>a+g.missing,0);
      $('gapStatus').textContent=`Gaps: ${fmtInt(remaining.length)} ranges, missing rows ${fmtInt(missing)}`;
    }
  } finally {
    gapRepairInFlight = false;
  }
}
async function plotTimings(){
  const market=$('market').value, symbol=$('symbol').value, rowsReq=timingMax();
  const data=await fetchJSON('/api/timings?'+qs({market,symbol,rows:rowsReq})); const rows=data.rows || [];
  if(!rows.length){ $('timingStatus').innerHTML=`<span class="bad">No timing rows for ${market} ${symbol}</span>`; Plotly.purge('timingChart'); timingReady=false; return; }
  const x=rows.map(r=>new Date(Math.floor((r.received_us || r.scheduled_close_us)/1000)));
  const cols=['scheduled_close_to_receive_us','exchange_event_to_receive_us','receive_to_saved_us','write_call_us','scheduled_close_to_saved_us'];
  const traces=cols.map(c=>({x,y:rows.map(r=>r[c]),type:'scatter',mode:'lines',name:c}));
  let layout=baseLayout(); layout.xaxis.title='UTC received time'; layout.yaxis={title:'Delay (microseconds)',gridcolor:'#233047'};
  await Plotly.react('timingChart',traces,layout,{responsive:true});
  timingReady=true;
  const last=rows[rows.length-1];
  lastTimingReceivedUs=last.received_us || last.scheduled_close_us || lastTimingReceivedUs;
  $('timingStatus').textContent=`Timing rows: ${fmtInt(data.row_count)} | latest receive→saved ${fmtInt(last.receive_to_saved_us)} µs | close→receive ${fmtInt(last.scheduled_close_to_receive_us)} µs`;
}
async function appendTimings(){
  if(!timingReady || !lastTimingReceivedUs){ await plotTimings(); return; }
  const market=$('market').value, symbol=$('symbol').value;
  const data=await fetchJSON('/api/timings_since?'+qs({market,symbol,after_received_us:lastTimingReceivedUs,rows:Math.min(5000,timingMax())}));
  const rows=(data.rows || []).filter(r=>(r.received_us || r.scheduled_close_us)>lastTimingReceivedUs);
  if(!rows.length) return;
  const cols=['scheduled_close_to_receive_us','exchange_event_to_receive_us','receive_to_saved_us','write_call_us','scheduled_close_to_saved_us'];
  const x=rows.map(r=>new Date(Math.floor((r.received_us || r.scheduled_close_us)/1000)));
  const update={x:cols.map(()=>x), y:cols.map(c=>rows.map(r=>r[c]))};
  await Plotly.extendTraces('timingChart',update,[0,1,2,3,4],timingMax());
  const last=rows[rows.length-1];
  lastTimingReceivedUs=last.received_us || last.scheduled_close_us || lastTimingReceivedUs;
  $('timingStatus').textContent=`Timing appended: ${fmtInt(rows.length)} | latest receive→saved ${fmtInt(last.receive_to_saved_us)} µs | close→receive ${fmtInt(last.scheduled_close_to_receive_us)} µs`;
}
async function plotAll(){ try{ await plotOHLCV(); await plotTimings(); } catch(err){ console.error(err); $('latestStatus').innerHTML=`<span class="bad">${err.message}</span>`; } }
async function pollLatest(){ if(paused) return; const market=$('market').value, symbol=$('symbol').value; if(!market || !symbol) return; try{ await appendOHLCV(); await appendTimings(); } catch(e){ console.warn(e); } }
function resetPolling(){ if(pollHandle) clearInterval(pollHandle); pollHandle=setInterval(pollLatest, Math.max(1,Number($('refresh').value || 3))*1000); }
$('market').addEventListener('change',async()=>{lastSeenCloseMs=null; lastTimingReceivedUs=null; chartReady=false; timingReady=false; resetOHLCVCache([]); await loadSymbols();});
$('symbol').addEventListener('change',async()=>{lastSeenCloseMs=null; lastTimingReceivedUs=null; chartReady=false; timingReady=false; resetOHLCVCache([]); await loadRangeAndPlot();});
$('plotBtn').addEventListener('click',plotAll); $('bars').addEventListener('change',plotOHLCV); $('timingRows').addEventListener('change',plotTimings); $('plotType').addEventListener('change',plotOHLCV); $('refresh').addEventListener('change',resetPolling);
$('pauseBtn').addEventListener('click',()=>{paused=!paused; $('pauseBtn').textContent=paused?'Resume':'Pause';});
(async function init(){ await loadMarkets(); await loadSymbols(); resetPolling(); })();
</script>
</body>
</html>)HTML";

// =============================================================================
// HTTP SERVER
// =============================================================================

struct SocketRuntime {
    SocketRuntime() {
#ifdef _WIN32
        WSADATA wsa{};
        if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) throw std::runtime_error("WSAStartup failed");
#endif
    }
    ~SocketRuntime() {
#ifdef _WIN32
        WSACleanup();
#endif
    }
};

static void close_socket(socket_t s) {
#ifdef _WIN32
    closesocket(s);
#else
    close(s);
#endif
}

static bool send_all(socket_t s, const std::string& data) {
    const char* p = data.data();
    size_t left = data.size();
    while (left > 0) {
#ifdef _WIN32
        int n = send(s, p, static_cast<int>(std::min<size_t>(left, 16 * 1024)), 0);
#else
        ssize_t n = send(s, p, std::min<size_t>(left, 16 * 1024), 0);
#endif
        if (n <= 0) return false;
        p += n;
        left -= static_cast<size_t>(n);
    }
    return true;
}

static std::string http_response(const std::string& body, const std::string& type = "application/json", int status = 200, const std::string& status_text = "OK") {
    std::ostringstream os;
    os << "HTTP/1.1 " << status << ' ' << status_text << "\r\n"
       << "Content-Type: " << type << "; charset=utf-8\r\n"
       << "Content-Length: " << body.size() << "\r\n"
       << "Cache-Control: no-store\r\n"
       << "Connection: close\r\n\r\n"
       << body;
    return os.str();
}

static std::string handle_target(const std::string& target) {
    std::string path = path_only(target);
    auto q = parse_query(target);
    try {
        if (path == "/" || path == "/index.html") return http_response(INDEX_HTML, "text/html");
        if (path == "/api/markets") return http_response(api_markets_json());
        if (path == "/api/debug") return http_response(api_debug_json());
        if (path == "/api/symbols") return http_response(api_symbols_json(q.count("market") ? q["market"] : g_config.default_market));
        if (path == "/api/summary") return http_response(api_summary_json(q.count("market") ? q["market"] : g_config.default_market));
        if (path == "/api/range") return http_response(api_range_json(q.count("market") ? q["market"] : g_config.default_market, q.count("symbol") ? q["symbol"] : g_config.default_symbol));
        if (path == "/api/ohlcv") return http_response(api_ohlcv_json(q.count("market") ? q["market"] : g_config.default_market, q.count("symbol") ? q["symbol"] : g_config.default_symbol, q.count("bars") ? static_cast<int>(safe_stoll(q["bars"], g_config.default_bars)) : g_config.default_bars));
        if (path == "/api/ohlcv_since") return http_response(api_ohlcv_since_json(q.count("market") ? q["market"] : g_config.default_market, q.count("symbol") ? q["symbol"] : g_config.default_symbol, q.count("after_close_ms") ? safe_stoll(q["after_close_ms"], 0) : 0, q.count("max_rows") ? static_cast<int>(safe_stoll(q["max_rows"], 1000)) : 1000));
        if (path == "/api/ohlcv_range") return http_response(api_ohlcv_range_json(q.count("market") ? q["market"] : g_config.default_market, q.count("symbol") ? q["symbol"] : g_config.default_symbol, q.count("start_close_ms") ? safe_stoll(q["start_close_ms"], 0) : 0, q.count("end_close_ms") ? safe_stoll(q["end_close_ms"], 0) : 0, q.count("max_rows") ? static_cast<int>(safe_stoll(q["max_rows"], 1000)) : 1000));
        if (path == "/api/timings") return http_response(api_timings_json(q.count("market") ? q["market"] : g_config.default_market, q.count("symbol") ? q["symbol"] : g_config.default_symbol, q.count("rows") ? static_cast<int>(safe_stoll(q["rows"], g_config.default_timing_rows)) : g_config.default_timing_rows));
        if (path == "/api/timings_since") return http_response(api_timings_since_json(q.count("market") ? q["market"] : g_config.default_market, q.count("symbol") ? q["symbol"] : g_config.default_symbol, q.count("after_received_us") ? safe_stoll(q["after_received_us"], 0) : 0, q.count("rows") ? static_cast<int>(safe_stoll(q["rows"], 5000)) : 5000));
        return http_response("{\"error\":\"not found\"}", "application/json", 404, "Not Found");
    } catch (const std::exception& e) {
        return http_response("{\"error\":" + json_str(e.what()) + "}", "application/json", 500, "Internal Server Error");
    }
}

static void handle_client(socket_t client) {
    std::string req;
    char buf[4096];
    for (;;) {
#ifdef _WIN32
        int n = recv(client, buf, sizeof(buf), 0);
#else
        ssize_t n = recv(client, buf, sizeof(buf), 0);
#endif
        if (n <= 0) break;
        req.append(buf, buf + n);
        if (req.find("\r\n\r\n") != std::string::npos || req.size() > 64 * 1024) break;
    }

    std::string target = "/";
    std::istringstream is(req);
    std::string method;
    std::string version;
    is >> method >> target >> version;
    if (g_config.log_requests) {
        std::cout << "[HTTP] " << method << " " << target << "\n";
    }

    if (method != "GET") {
        send_all(client, http_response("{\"error\":\"GET only\"}", "application/json", 405, "Method Not Allowed"));
    } else {
        std::string response = handle_target(target);
        if (g_config.log_requests) {
            std::cout << "[HTTP] response_bytes=" << response.size() << " target=" << target << "\n";
        }
        send_all(client, response);
    }
    close_socket(client);
}

static socket_t make_listen_socket(const std::string& host, int port) {
    socket_t s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == invalid_socket_value) throw std::runtime_error("socket failed");

    int yes = 1;
#ifdef _WIN32
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&yes), sizeof(yes));
#else
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        close_socket(s);
        throw std::runtime_error("bad host: " + host);
    }
    if (bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_socket(s);
        throw std::runtime_error("bind failed on " + host + ":" + std::to_string(port));
    }
    if (listen(s, SOMAXCONN) != 0) {
        close_socket(s);
        throw std::runtime_error("listen failed");
    }
    return s;
}

static void open_browser(const std::string& url) {
    if (!g_config.auto_open_browser) return;
    std::thread([url] {
        std::this_thread::sleep_for(700ms);
#ifdef _WIN32
        ShellExecuteA(nullptr, "open", url.c_str(), nullptr, nullptr, SW_SHOWNORMAL);
#elif defined(__APPLE__)
        std::string cmd = "open '" + url + "'";
        std::system(cmd.c_str());
#else
        std::string cmd = "xdg-open '" + url + "' >/dev/null 2>&1 &";
        std::system(cmd.c_str());
#endif
    }).detach();
}

static void run_server() {
    SocketRuntime runtime;
    socket_t listener = make_listen_socket(g_config.host, g_config.port);
    std::string url = "http://" + g_config.host + ":" + std::to_string(g_config.port);
    std::cout << "Dashboard running at: " << url << "\n";
    std::cout << "BASE_DIR: " << g_config.base_dir << "\n";
    std::cout << "OHLCV:    " << daily_bin_dir() << "\n";
    std::cout << "Timing:   " << timing_csv_path() << "\n";
    std::cout << "Debug:    " << url << "/api/debug\n";
    std::cout << "Request logging: " << (g_config.log_requests ? "on" : "off") << "\n";
    std::cout << "Press Ctrl+C or close this console to stop.\n";
    open_browser(url);

    while (!g_stop.load()) {
        sockaddr_in client_addr{};
#ifdef _WIN32
        int len = sizeof(client_addr);
        socket_t client = accept(listener, reinterpret_cast<sockaddr*>(&client_addr), &len);
#else
        socklen_t len = sizeof(client_addr);
        socket_t client = accept(listener, reinterpret_cast<sockaddr*>(&client_addr), &len);
#endif
        if (client == invalid_socket_value) continue;
        std::thread(handle_client, client).detach();
    }
    close_socket(listener);
}

int main(int argc, char** argv) {
    try {
        std::string exe_dir = exe_dir_string(argc > 0 ? argv[0] : nullptr);
        parse_args(argc, argv, exe_dir);
        resolve_base_dir_after_args(exe_dir);
        run_server();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
}
