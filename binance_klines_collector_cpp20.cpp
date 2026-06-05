





// binance_klines_collector_cpp20.cpp
// C++20 port of the uploaded Python Binance klines collector.
// Rebuilt from Pasted text(74).txt with the Python timestamp normalization,
// complete-bin precheck behavior, live delay CSV metrics, and live supervisor.
//
// External dependencies, easiest with vcpkg:
//   vcpkg install curl openssl boost-beast boost-interprocess nlohmann-json pugixml libzip
//
// Build with the included CMakeLists.txt in this folder, or compile manually linking
// CURL, OpenSSL, Boost.System, pugixml, libzip, and platform socket libraries.
//
// Notes:
// - Edit the CONFIG section before running, especially BASE_DIR and RUN_MODE.
// - This uses C++ threads instead of Python multiprocessing for RUN_MODE="all".
// - Binary .bin record layout intentionally matches the Python numpy dtype: 128 bytes.
// - REST/bulk/gap-fill dedupe + sort daily files; live websocket append writes closed candles only.
// - Added live delay CSV logging: exchange minute boundary -> receive, receive -> .bin saved.
// - LIVE DELAY FINAL VERSION: writes one CSV row per closed live kline for every symbol.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <cmath>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/interprocess/sync/file_lock.hpp>

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include <pugixml.hpp>
#include <zip.h>
#include <openssl/err.h>

namespace fs = std::filesystem;
namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = boost::asio::ssl;
using tcp = boost::asio::ip::tcp;
using json = nlohmann::json;
using namespace std::chrono_literals;

// =============================================================================
// CONFIG - edit these values instead of command-line args, matching the Python file.
// =============================================================================

enum class Market { spot, um, cm };
enum class WriteMode { dedupe_sort, append_live };

enum class RunMode { bulk, rest72, live, all, validate, symbols };

static fs::path BASE_DIR = "data_dump";
static fs::path BULK_CSV_DIR = BASE_DIR / "bulk_csv";
static fs::path DAILY_BIN_DIR = BASE_DIR / "daily_bin";
static fs::path LOG_DIR = BASE_DIR / "logs";
static fs::path SYMBOLS_OUTPUT_DIR = BASE_DIR / "symbols_output";
static fs::path LOG_PATH = LOG_DIR / "klines_collector_cpp.log";

// One CSV containing timing records for every live symbol.
static fs::path LIVE_DELAY_CSV_PATH = LOG_DIR / "live_symbol_delays_microseconds_batched.csv";
static bool LIVE_DELAY_LOG_ENABLED = true;

// Timing CSV rows are cached in RAM by worker threads and written by one
// dedicated writer thread at second 30 of every minute. This removes the
// per-message CSV file open/write/close from the live hot path.
static bool LIVE_DELAY_BATCH_CSV_WRITES = true;
static int LIVE_DELAY_CSV_FLUSH_SECOND = 30;
static size_t LIVE_DELAY_CSV_BATCH_FORCE_FLUSH_ROWS = 50'000;

static RunMode RUN_MODE = RunMode::live; //RunMode::all;

static std::vector<std::string> SYMBOLS_SPOT_BULK = {};
static std::vector<std::string> SYMBOLS_FUTURES_UM_BULK = {"AAVEUSDT", "BTCUSDT", "ETHUSDT", "LTCUSDT", "SOLUSDT", "XRPUSDT"};
static std::vector<std::string> SYMBOLS_FUTURES_CM_BULK = {"AAVEUSD_PERP", "BTCUSD_PERP"};

static std::vector<std::string> SYMBOLS_SPOT_LIVE_REST = {};
static std::vector<std::string> SYMBOLS_FUTURES_UM_LIVE_REST = {"AAVEUSDT", "BTCUSDT", "ETHUSDT", "LTCUSDT", "SOLUSDT", "XRPUSDT"};
static std::vector<std::string> SYMBOLS_FUTURES_CM_LIVE_REST = {"AAVEUSD_PERP", "BTCUSD_PERP"};

static bool AUTO_FIND_SYMBOLS_SPOT = true;
static bool AUTO_FIND_SYMBOLS_UM = true;
static bool AUTO_FIND_SYMBOLS_CM = true;
static bool AUTO_VALIDATE_LIVE_SYMBOLS_WITH_KLINE = false;
static int AUTO_VALIDATE_MAX_WORKERS = 25;
static int AUTO_VALIDATE_MAX_AGE_MINUTES = 180;

static constexpr const char* INTERVAL = "1m";
static constexpr int64_t INTERVAL_MS = 60'000;
// REST lookback is capped to 3 chunks of 999 one-minute candles per symbol.
// 999 * 3 = 2997 rows = about 49h57m.
static int REST_LOOKBACK_ROWS = 999 * 3;
static int REST_MAX_CHUNKS_PER_SYMBOL = 3;
static int REST_SYMBOL_THREADS = 24;

// REST-only global WEIGHT limiter. This is separate from websocket live streaming.
// It prevents REST backfill/gap-fill/exchangeInfo requests from flooding Binance
// while live websocket collection keeps running. Kline requests consume weight
// based on the LIMIT used. Reconnect gap-fill calculates the exact missing rows,
// then charges only the matching request weight.
static bool REST_RATE_LIMIT_ENABLED = true;
static int REST_MAX_WEIGHT_PER_MINUTE = 1000;
static bool REST_RATE_LIMIT_SPREAD_REQUESTS = false;

// Smart REST lookback: before requesting REST rows, check the latest .bin candle
// already saved for that symbol. If it is only missing the current partial day,
// request only that missing range instead of always requesting 999*3 rows.
static bool REST_LOOKBACK_ONLY_MISSING_FROM_DISK = true;

static int BULK_DOWNLOAD_THREADS = 40;
static int BULK_PLAN_THREADS = 10;
static bool BULK_DEBUG_LOG_EVERY_CSV_CHECK = false;
static bool BULK_DEBUG_LOG_REMOTE_SUMMARY = false;

static double RUN_ALL_START_REST_AND_BULK_AFTER_LIVE_S = 1.0;
static double RUN_ALL_MONITOR_SLEEP_S = 5.0;
static bool RUN_ALL_RESTART_LIVE_IF_THREAD_EXITS = true;
static int MAX_SYMBOLS_PER_STREAM = 100;

// LIVE PIPELINE PARALLELISM
// Websocket threads only receive raw messages and push them into a queue.
// Worker threads parse JSON and convert klines. .bin writes are done by a
// dedicated short-batch writer thread to reduce open/lock/write overhead.
// 0 = auto-detect hardware cores minus 2, leaving CPU for websocket receive
// threads, the .bin writer, the CSV writer, and the OS.
static unsigned int LIVE_PROCESS_WORKERS = 0;

// Backpressure limit for raw websocket jobs waiting to be processed.
// If this fills, websocket read threads will block until workers catch up.
static size_t LIVE_QUEUE_MAX = 100'000;

// Live .bin batching. Workers enqueue parsed closed klines; one writer thread
// flushes them every 20,000 microseconds. This intentionally adds up to about
// 20ms of local save latency, but greatly reduces filesystem contention when
// thousands of symbols close at the same minute.
static bool LIVE_BIN_BATCH_WRITES = true;
static int LIVE_BIN_BATCH_FLUSH_US = 1'000;
static size_t LIVE_BIN_BATCH_FORCE_FLUSH_ROWS = 50000;

// Triple-buffered live .bin writer stage. One buffer is being filled by
// processing workers, up to LIVE_BIN_WRITER_THREADS buffers can be written
// concurrently, and additional ready batches wait in a queue.
// 3 gives: filling / saving / waiting with three writer workers.
static unsigned int LIVE_BIN_WRITER_THREADS = 4;
static size_t LIVE_BIN_READY_BATCH_QUEUE_MAX = 4;

static int STREAM_RECONNECT_MAX_BACKOFF_S = 120;
static int STREAM_RECV_TIMEOUT_S = 600;
static int WS_OPEN_TIMEOUT_S = 30;
static int WS_CLOSE_TIMEOUT_S = 10;
static std::optional<double> WS_CLIENT_PING_INTERVAL_S = std::nullopt;
static std::optional<double> WS_CLIENT_PING_TIMEOUT_S = std::nullopt;
static int WS_UNSOLICITED_PONG_INTERVAL_S = 300;

static bool LOG_RECOVERABLE_NETWORK_TRACEBACKS = false;
static double NETWORK_FAILURE_RETRY_MIN_S = 1.0;
static double NETWORK_FAILURE_RETRY_MAX_S = 15.0;
static double NETWORK_FAILURE_BACKOFF_MAX_S = 30.0;

static int HTTP_TIMEOUT_S = 30;
static int HTTP_RETRIES = 8;
static int BULK_DOWNLOAD_CONNECT_TIMEOUT_S = 10;
static int BULK_DOWNLOAD_READ_TIMEOUT_S = 180;
static int BULK_DOWNLOAD_RETRIES = 12;
static size_t BULK_DOWNLOAD_CHUNK_SIZE = 2 * 1024 * 1024;
static double BULK_DOWNLOAD_BACKOFF_MAX_S = 180.0;

static std::chrono::sys_days BULK_START_DATE{std::chrono::year{2026}/5/18};
static std::chrono::sys_days BULK_END_DATE{std::chrono::year{2099}/12/31};
static bool BULK_IMPORT_TO_BINS = true;
static bool BULK_DELETE_ZIPS_AFTER_EXTRACT = true;
static bool BULK_SKIP_IMPORT_IF_DAILY_BINS_HAVE_1440_ROWS = true;
static int BULK_EXPECTED_ROWS_PER_DAY = 1440;
static int BULK_BIN_COMPLETENESS_WORKERS = 1;
static std::optional<std::chrono::sys_days> VALIDATE_DATE = std::nullopt;

static std::string SPOT_REST_KLINES = "https://api.binance.com/api/v3/klines";
static std::string UM_REST_KLINES = "https://fapi.binance.com/fapi/v1/klines";
static std::string CM_REST_KLINES = "https://dapi.binance.com/dapi/v1/klines";
static std::string SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo";
static std::string UM_EXCHANGE_INFO = "https://fapi.binance.com/fapi/v1/exchangeInfo";
static std::string CM_EXCHANGE_INFO = "https://dapi.binance.com/dapi/v1/exchangeInfo";

static std::vector<std::string> SPOT_WS_BASE_FALLBACKS = {
    "wss://stream.binance.com:443/stream?streams=",
    "wss://stream.binance.com:9443/stream?streams=",
    "wss://data-stream.binance.vision/stream?streams="
};
static std::vector<std::string> UM_WS_BASE_FALLBACKS = {
    "wss://fstream.binance.com/market/stream?streams="
};
static std::vector<std::string> CM_WS_BASE_FALLBACKS = {
    "wss://dstream.binance.com/stream?streams="
};
static bool DNS_FORCE_IPV4 = true;
static std::string S3_LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";

// =============================================================================
// LOGGING
// =============================================================================

class Logger {
    std::mutex mu_;
    std::ofstream file_;
public:
    void open(const fs::path& path) {
        fs::create_directories(path.parent_path());
        file_.open(path, std::ios::app);
    }
    template <typename... Args>
    void log(const char* level, Args&&... args) {
        std::ostringstream msg;
        (msg << ... << std::forward<Args>(args));
        auto now = std::chrono::system_clock::now();
        auto tt = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
        std::tm gmt{};
#if defined(_WIN32)
        gmtime_s(&gmt, &tt);
#else
        gmtime_r(&tt, &gmt);
#endif
        std::ostringstream line;
        line << std::put_time(&gmt, "%Y-%m-%dT%H:%M:%S")
             << '.' << std::setw(3) << std::setfill('0') << ms << "Z [" << level << "] ["
             << std::this_thread::get_id() << "] " << msg.str();
        std::lock_guard<std::mutex> lk(mu_);
        std::cout << line.str() << std::endl;
        if (file_.is_open()) file_ << line.str() << std::endl;
    }
};

static Logger logger;

#define LOG_INFO(...) logger.log("INFO", __VA_ARGS__)
#define LOG_WARN(...) logger.log("WARNING", __VA_ARGS__)
#define LOG_ERR(...) logger.log("ERROR", __VA_ARGS__)
#define LOG_DBG(...) logger.log("DEBUG", __VA_ARGS__)

// =============================================================================
// STRING, DATE, MARKET HELPERS
// =============================================================================

static std::string upper(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::toupper(c)); });
    return s;
}
static std::string lower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c){ return static_cast<char>(std::tolower(c)); });
    return s;
}
static std::string trim(std::string s) {
    auto is_space = [](unsigned char c){ return std::isspace(c); };
    s.erase(s.begin(), std::find_if_not(s.begin(), s.end(), is_space));
    s.erase(std::find_if_not(s.rbegin(), s.rend(), is_space).base(), s.end());
    return s;
}
static bool is_valid_binance_symbol_name(std::string_view s) {
    // Keep paths Windows-safe and Binance-like. This prevents corrupted or
    // non-ASCII S3 prefixes from becoming filesystem paths such as
    // "Õ©üÕ«ë...USDT".
    if (s.empty() || s.size() > 40) return false;
    bool has_alnum = false;
    for (unsigned char c : s) {
        if (std::isalnum(c)) { has_alnum = true; continue; }
        if (c == '_') continue; // COIN-M contracts such as BTCUSD_PERP.
        return false;
    }
    return has_alnum;
}

static std::vector<std::string> clean_symbol_list(const std::vector<std::string>& symbols) {
    std::unordered_set<std::string> seen;
    std::vector<std::string> out;
    for (auto s : symbols) {
        s = upper(trim(s));
        if (s.empty()) continue;
        if (!is_valid_binance_symbol_name(s)) {
            LOG_WARN("[SYMBOLS] skipping invalid/non-ASCII symbol=", s);
            continue;
        }
        if (seen.count(s)) continue;
        seen.insert(s);
        out.push_back(s);
    }
    return out;
}
static std::string join(const std::vector<std::string>& xs, std::string_view sep) {
    std::ostringstream os;
    for (size_t i=0;i<xs.size();++i) { if (i) os << sep; os << xs[i]; }
    return os.str();
}

static int64_t utc_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
static int64_t utc_now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
static int64_t floor_minute_ms(int64_t ms) { return ms - (ms % INTERVAL_MS); }
static int64_t last_closed_minute_open_ms(std::optional<int64_t> now_ms = std::nullopt) {
    int64_t now = now_ms.value_or(utc_now_ms());
    return floor_minute_ms(now) - INTERVAL_MS;
}
static std::chrono::sys_days ms_to_day(int64_t ms) {
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> tp{std::chrono::milliseconds{ms}};
    return std::chrono::floor<std::chrono::days>(tp);
}
static int64_t day_start_ms(std::chrono::sys_days d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d.time_since_epoch()).count();
}
static std::string date_to_string(std::chrono::sys_days d) {
    std::chrono::year_month_day ymd{d};
    std::ostringstream os;
    os << int(ymd.year()) << '-' << std::setw(2) << std::setfill('0') << unsigned(ymd.month())
       << '-' << std::setw(2) << std::setfill('0') << unsigned(ymd.day());
    return os.str();
}
static std::string month_to_string(std::chrono::sys_days d) {
    std::chrono::year_month_day ymd{d};
    std::ostringstream os;
    os << int(ymd.year()) << '-' << std::setw(2) << std::setfill('0') << unsigned(ymd.month());
    return os.str();
}
static std::string ms_to_utc_string(int64_t ms) {
    auto tp = std::chrono::system_clock::time_point{std::chrono::milliseconds{ms}};
    auto tt = std::chrono::system_clock::to_time_t(tp);
    std::tm gmt{};
#if defined(_WIN32)
    gmtime_s(&gmt, &tt);
#else
    gmtime_r(&tt, &gmt);
#endif
    std::ostringstream os;
    os << std::put_time(&gmt, "%Y-%m-%dT%H:%M:%SZ");
    return os.str();
}
static std::string us_to_utc_string(int64_t us) {
    auto tp = std::chrono::system_clock::time_point{std::chrono::microseconds{us}};
    auto tt = std::chrono::system_clock::to_time_t(tp);
    std::tm gmt{};
#if defined(_WIN32)
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
static std::chrono::sys_days month_start(std::chrono::sys_days d) {
    std::chrono::year_month_day ymd{d};
    return std::chrono::sys_days{ymd.year()/ymd.month()/1};
}
static std::chrono::sys_days add_month(std::chrono::sys_days d) {
    std::chrono::year_month_day ymd{d};
    auto ym = std::chrono::year_month{ymd.year(), ymd.month()} + std::chrono::months{1};
    return std::chrono::sys_days{ym/1};
}
static std::chrono::sys_days month_end(std::chrono::sys_days d) {
    std::chrono::year_month_day ymd{d};
    return std::chrono::sys_days{std::chrono::year_month_day_last{ymd.year()/ymd.month()/std::chrono::last}};
}
static std::vector<std::chrono::sys_days> date_range_days(std::chrono::sys_days start, std::chrono::sys_days end) {
    std::vector<std::chrono::sys_days> out;
    for (auto d=start; d<=end; d += std::chrono::days{1}) out.push_back(d);
    return out;
}

static std::string market_name(Market m) {
    switch (m) { case Market::spot: return "spot"; case Market::um: return "um"; case Market::cm: return "cm"; }
    return "unknown";
}
static std::string market_label(Market m) {
    switch (m) { case Market::spot: return "SPOT"; case Market::um: return "USD-M"; case Market::cm: return "COIN-M"; }
    return "UNKNOWN";
}
static std::string market_rest_url(Market m) {
    switch (m) { case Market::spot: return SPOT_REST_KLINES; case Market::um: return UM_REST_KLINES; case Market::cm: return CM_REST_KLINES; }
    throw std::invalid_argument("unknown market");
}
static std::string market_exchange_info_url(Market m) {
    switch (m) { case Market::spot: return SPOT_EXCHANGE_INFO; case Market::um: return UM_EXCHANGE_INFO; case Market::cm: return CM_EXCHANGE_INFO; }
    throw std::invalid_argument("unknown market");
}
static std::vector<std::string> market_ws_bases(Market m) {
    switch (m) { case Market::spot: return SPOT_WS_BASE_FALLBACKS; case Market::um: return UM_WS_BASE_FALLBACKS; case Market::cm: return CM_WS_BASE_FALLBACKS; }
    throw std::invalid_argument("unknown market");
}
static int rest_limit(Market m) {
    (void)m;
    return 999;
}
static std::string vision_market_path(Market m) {
    switch (m) { case Market::spot: return "spot"; case Market::um: return "futures/um"; case Market::cm: return "futures/cm"; }
    throw std::invalid_argument("unknown market");
}

// =============================================================================
// LIVE DELAY CSV LOGGING
// =============================================================================

struct LiveDelayRecord {
    std::string market;
    std::string symbol;

    int64_t kline_open_time_ms = 0;
    int64_t kline_close_time_ms = 0;

    // Binance kline timestamps are milliseconds. For microsecond delay math,
    // these are also stored as microseconds by multiplying ms by 1000.
    int64_t scheduled_close_ms = 0;
    int64_t scheduled_close_us = 0;

    // Binance websocket event time field "E" is milliseconds, if present.
    int64_t exchange_event_ms = 0;
    int64_t exchange_event_us = 0;

    // Local machine timestamps. The *_us values are the high-resolution ones.
    int64_t received_ms = 0;
    int64_t received_us = 0;
    int64_t save_started_ms = 0;
    int64_t save_started_us = 0;
    int64_t saved_ms = 0;
    int64_t saved_us = 0;

    // Millisecond delay columns kept for compatibility/readability.
    int64_t scheduled_close_to_receive_ms = 0;
    int64_t exchange_event_to_receive_ms = 0;
    int64_t receive_to_saved_ms = 0;
    int64_t write_call_ms = 0;
    int64_t scheduled_close_to_saved_ms = 0;

    // Microsecond delay columns.
    int64_t scheduled_close_to_receive_us = 0;
    int64_t exchange_event_to_receive_us = 0;
    int64_t receive_to_saved_us = 0;
    int64_t write_call_us = 0;
    int64_t scheduled_close_to_saved_us = 0;

    int rows_written = 0;
};

static std::mutex live_delay_csv_mu;

static std::string csv_escape(std::string s) {
    bool needs_quotes = s.find_first_of(",\"\n\r") != std::string::npos;
    if (!needs_quotes) return s;

    std::string out;
    out.reserve(s.size() + 2);
    out.push_back('"');

    for (char c : s) {
        if (c == '"') out += "\"\"";
        else out.push_back(c);
    }

    out.push_back('"');
    return out;
}

static void write_live_delay_csv_header(std::ofstream& f) {
    f
        << "market,"
        << "symbol,"
        << "kline_open_time_utc,"
        << "kline_open_time_ms,"
        << "kline_close_time_ms,"
        << "scheduled_close_ms,"
        << "scheduled_close_us,"
        << "scheduled_close_utc,"
        << "exchange_event_ms,"
        << "exchange_event_us,"
        << "exchange_event_utc,"
        << "received_ms,"
        << "received_us,"
        << "received_utc_us,"
        << "save_started_ms,"
        << "save_started_us,"
        << "saved_ms,"
        << "saved_us,"
        << "saved_utc_us,"
        << "scheduled_close_to_receive_ms,"
        << "scheduled_close_to_receive_us,"
        << "exchange_event_to_receive_ms,"
        << "exchange_event_to_receive_us,"
        << "receive_to_saved_ms,"
        << "receive_to_saved_us,"
        << "write_call_ms,"
        << "write_call_us,"
        << "scheduled_close_to_saved_ms,"
        << "scheduled_close_to_saved_us,"
        << "rows_written"
        << '\n';
}

static void write_live_delay_csv_row(std::ofstream& f, const LiveDelayRecord& r) {
    auto utc_ms_or_empty = [](int64_t ms) -> std::string {
        return ms > 0 ? ms_to_utc_string(ms) : "";
    };
    auto utc_us_or_empty = [](int64_t us) -> std::string {
        return us > 0 ? us_to_utc_string(us) : "";
    };

    f
        << csv_escape(r.market) << ','
        << csv_escape(r.symbol) << ','
        << csv_escape(utc_ms_or_empty(r.kline_open_time_ms)) << ','
        << r.kline_open_time_ms << ','
        << r.kline_close_time_ms << ','
        << r.scheduled_close_ms << ','
        << r.scheduled_close_us << ','
        << csv_escape(utc_us_or_empty(r.scheduled_close_us)) << ','
        << r.exchange_event_ms << ','
        << r.exchange_event_us << ','
        << csv_escape(utc_us_or_empty(r.exchange_event_us)) << ','
        << r.received_ms << ','
        << r.received_us << ','
        << csv_escape(utc_us_or_empty(r.received_us)) << ','
        << r.save_started_ms << ','
        << r.save_started_us << ','
        << r.saved_ms << ','
        << r.saved_us << ','
        << csv_escape(utc_us_or_empty(r.saved_us)) << ','
        << r.scheduled_close_to_receive_ms << ','
        << r.scheduled_close_to_receive_us << ','
        << r.exchange_event_to_receive_ms << ','
        << r.exchange_event_to_receive_us << ','
        << r.receive_to_saved_ms << ','
        << r.receive_to_saved_us << ','
        << r.write_call_ms << ','
        << r.write_call_us << ','
        << r.scheduled_close_to_saved_ms << ','
        << r.scheduled_close_to_saved_us << ','
        << r.rows_written
        << '\n';
}

static std::mutex live_delay_pending_mu;
static std::condition_variable live_delay_pending_cv;
static std::vector<LiveDelayRecord> live_delay_pending_records;

static int64_t next_delay_csv_flush_us(int flush_second) {
    constexpr int64_t US_PER_SECOND = 1'000'000LL;
    constexpr int64_t US_PER_MINUTE = 60LL * US_PER_SECOND;

    flush_second = std::clamp(flush_second, 0, 59);

    int64_t now_us = utc_now_us();
    int64_t minute_start_us = (now_us / US_PER_MINUTE) * US_PER_MINUTE;
    int64_t flush_us = minute_start_us + static_cast<int64_t>(flush_second) * US_PER_SECOND;

    if (now_us >= flush_us) flush_us += US_PER_MINUTE;
    return flush_us;
}

static void cache_live_delay_record(LiveDelayRecord&& r) {
    if (!LIVE_DELAY_LOG_ENABLED) return;

    bool force_flush = false;
    {
        std::lock_guard<std::mutex> lk(live_delay_pending_mu);
        live_delay_pending_records.push_back(std::move(r));
        force_flush = live_delay_pending_records.size() >= LIVE_DELAY_CSV_BATCH_FORCE_FLUSH_ROWS;
    }

    if (force_flush) live_delay_pending_cv.notify_one();
}

static void append_live_delay_csv_batch(const std::vector<LiveDelayRecord>& records) {
    if (!LIVE_DELAY_LOG_ENABLED || records.empty()) return;

    // Only the dedicated CSV writer thread calls this during live mode, but the
    // mutex keeps it safe if this helper is reused by a non-live path later.
    std::lock_guard<std::mutex> lk(live_delay_csv_mu);

    fs::create_directories(LIVE_DELAY_CSV_PATH.parent_path());

    bool need_header = true;
    std::error_code ec;
    if (fs::exists(LIVE_DELAY_CSV_PATH, ec) && fs::file_size(LIVE_DELAY_CSV_PATH, ec) > 0) {
        need_header = false;
    }

    std::ofstream f(LIVE_DELAY_CSV_PATH, std::ios::app);
    if (!f.is_open()) {
        LOG_WARN("[LIVE DELAY CSV] could not open ", LIVE_DELAY_CSV_PATH.string());
        return;
    }

    if (need_header) write_live_delay_csv_header(f);
    for (const auto& r : records) write_live_delay_csv_row(f, r);
}

// =============================================================================
// KLINE RECORD LAYOUT - matches Python numpy dtype exactly.
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
static_assert(sizeof(KlineRecord) == 128, "KlineRecord must match Python KLINE_DTYPE.itemsize == 128");

struct Kline {
    std::string symbol;
    int64_t open_time = 0;
    int64_t close_time = 0;
    double open = 0.0;
    double high = 0.0;
    double low = 0.0;
    double close = 0.0;
    double volume = 0.0;
    double quote_volume = 0.0;
    int64_t trades = 0;
    double taker_base_vol = 0.0;
    double taker_quote_vol = 0.0;
    double maker_base_vol = 0.0;
    double maker_quote_vol = 0.0;
};

static bool is_finite_kline(const Kline& k) {
    return std::isfinite(k.open) && std::isfinite(k.high) && std::isfinite(k.low) &&
           std::isfinite(k.close) && std::isfinite(k.volume);
}
static KlineRecord to_record(const Kline& k) {
    KlineRecord r{};
    std::string sym = upper(k.symbol);
    std::strncpy(r.symbol, sym.c_str(), sizeof(r.symbol));
    r.open_time = k.open_time;
    r.close_time = k.close_time;
    r.open = k.open;
    r.high = k.high;
    r.low = k.low;
    r.close = k.close;
    r.volume = k.volume;
    r.quote_volume = k.quote_volume;
    r.trades = k.trades;
    r.taker_base_vol = k.taker_base_vol;
    r.taker_quote_vol = k.taker_quote_vol;
    r.maker_base_vol = k.maker_base_vol;
    r.maker_quote_vol = k.maker_quote_vol;
    return r;
}
static size_t bounded_strlen24(const char* p) {
    size_t n = 0;
    while (n < 24 && p[n] != '\0') ++n;
    return n;
}
static Kline from_record(const KlineRecord& r) {
    Kline k;
    k.symbol = std::string(r.symbol, bounded_strlen24(r.symbol));
    k.open_time = r.open_time;
    k.close_time = r.close_time;
    k.open = r.open;
    k.high = r.high;
    k.low = r.low;
    k.close = r.close;
    k.volume = r.volume;
    k.quote_volume = r.quote_volume;
    k.trades = r.trades;
    k.taker_base_vol = r.taker_base_vol;
    k.taker_quote_vol = r.taker_quote_vol;
    k.maker_base_vol = r.maker_base_vol;
    k.maker_quote_vol = r.maker_quote_vol;
    return k;
}

static std::vector<Kline> normalize_klines(std::vector<Kline> xs, const std::string& symbol) {
    std::vector<Kline> out;
    out.reserve(xs.size());
    for (auto& k : xs) {
        k.symbol = upper(symbol.empty() ? k.symbol : symbol);
        if (k.open_time <= 0 || k.close_time <= 0) continue;
        if (k.open_time % INTERVAL_MS != 0) continue;
        if (!is_finite_kline(k)) continue;
        k.maker_base_vol = k.volume - k.taker_base_vol;
        k.maker_quote_vol = k.quote_volume - k.taker_quote_vol;
        out.push_back(k);
    }
    return out;
}

// =============================================================================
// FILE LOCKING + BIN PATHS
// =============================================================================

class FileLockGuard {
    fs::path lock_path_;
    std::optional<boost::interprocess::file_lock> lock_;
public:
    explicit FileLockGuard(const fs::path& target) {
        std::error_code ec;
        fs::create_directories(target.parent_path(), ec);
        if (ec) throw std::runtime_error("create lock directory failed for " + target.parent_path().string() + ": " + ec.message());
        lock_path_ = fs::path(target.string() + ".lock");
        { std::ofstream touch(lock_path_, std::ios::app | std::ios::binary); }
        lock_.emplace(lock_path_.string().c_str());
        lock_->lock();
    }
    ~FileLockGuard() {
        try { if (lock_) lock_->unlock(); } catch (...) {}
    }
    FileLockGuard(const FileLockGuard&) = delete;
    FileLockGuard& operator=(const FileLockGuard&) = delete;
};

static fs::path daily_bin_path(Market market, const std::string& symbol, std::chrono::sys_days day) {
    std::string sym = upper(symbol);
    if (!is_valid_binance_symbol_name(sym)) {
        throw std::runtime_error("invalid Binance symbol for .bin path: " + sym);
    }
    return DAILY_BIN_DIR / market_name(market) / sym / (sym + "_" + market_name(market) + "_" + INTERVAL + "_" + date_to_string(day) + ".bin");
}
static fs::path bulk_csv_path(Market market, const std::string& symbol, const std::string& timeperiod, std::chrono::sys_days d) {
    std::string sym = upper(symbol);
    if (!is_valid_binance_symbol_name(sym)) {
        throw std::runtime_error("invalid Binance symbol for bulk CSV path: " + sym);
    }
    std::string suffix = (timeperiod == "monthly") ? month_to_string(d) : date_to_string(d);
    return BULK_CSV_DIR / market_name(market) / timeperiod / "klines" / sym / INTERVAL / (sym + "-" + INTERVAL + "-" + suffix + ".csv");
}
static std::vector<KlineRecord> read_bin_file(const fs::path& path) {
    std::vector<KlineRecord> records;
    if (!fs::exists(path) || fs::file_size(path) == 0) return records;
    auto sz = fs::file_size(path);
    if (sz % sizeof(KlineRecord) != 0) {
        LOG_WARN("bad .bin size not multiple of record size: ", path.string(), " size=", sz);
        return records;
    }
    records.resize(sz / sizeof(KlineRecord));
    std::ifstream f(path, std::ios::binary);
    f.read(reinterpret_cast<char*>(records.data()), static_cast<std::streamsize>(sz));
    return records;
}
static void atomic_write_records(const fs::path& path, const std::vector<KlineRecord>& records) {
    fs::create_directories(path.parent_path());
    fs::path tmp = path;
    tmp += ".tmp." + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) + "." + std::to_string(utc_now_ms());
    {
        std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
        if (!records.empty()) f.write(reinterpret_cast<const char*>(records.data()), static_cast<std::streamsize>(records.size() * sizeof(KlineRecord)));
    }
#if defined(_WIN32)
    std::error_code ec;
    fs::remove(path, ec); // std::filesystem::rename is not overwrite-atomic on Windows.
    fs::rename(tmp, path, ec);
    if (ec) throw std::runtime_error("rename failed: " + ec.message());
#else
    fs::rename(tmp, path);
#endif
}
static void append_records(const fs::path& path, const std::vector<KlineRecord>& records) {
    if (records.empty()) return;
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);
    if (ec) {
        throw std::runtime_error("create_directories failed for " + path.parent_path().string() + ": " + ec.message());
    }
    std::ofstream f(path, std::ios::binary | std::ios::app);
    if (!f.is_open()) {
        throw std::runtime_error("open append failed for " + path.string());
    }
    f.write(reinterpret_cast<const char*>(records.data()), static_cast<std::streamsize>(records.size() * sizeof(KlineRecord)));
    if (!f.good()) {
        throw std::runtime_error("write append failed for " + path.string());
    }
}

static std::vector<Kline> clean_one_day(std::vector<Kline> rows, std::chrono::sys_days day) {
    int64_t start = day_start_ms(day);
    int64_t end = start + 24LL * 60 * 60 * 1000;
    std::vector<Kline> filtered;
    for (auto& k : rows) if (k.open_time >= start && k.open_time < end) filtered.push_back(k);
    std::stable_sort(filtered.begin(), filtered.end(), [](const Kline& a, const Kline& b){ return a.open_time < b.open_time; });
    std::vector<Kline> deduped;
    for (const auto& k : filtered) {
        if (!deduped.empty() && deduped.back().open_time == k.open_time) deduped.back() = k;
        else deduped.push_back(k);
    }
    if (deduped.size() > 1440) {
        LOG_WARN(date_to_string(day), " had ", deduped.size(), " rows after dedupe; trimming to last 1440");
        deduped.erase(deduped.begin(), deduped.end() - 1440);
    }
    return deduped;
}

static int write_klines_daily(Market market, const std::string& symbol, std::vector<Kline> rows, WriteMode mode, const std::string& source) {
    rows = normalize_klines(std::move(rows), symbol);
    if (rows.empty()) return 0;

    std::map<std::chrono::sys_days, std::vector<Kline>> by_day;
    for (auto& k : rows) by_day[ms_to_day(k.open_time)].push_back(k);

    int written = 0;
    for (auto& [day, day_rows] : by_day) {
        auto path = daily_bin_path(market, symbol, day);
        std::vector<KlineRecord> new_records;
        new_records.reserve(day_rows.size());
        for (auto& k : day_rows) new_records.push_back(to_record(k));

        FileLockGuard lock(path);
        if (mode == WriteMode::append_live) {
            append_records(path, new_records);
            written += static_cast<int>(new_records.size());
            continue;
        }

        auto existing_records = read_bin_file(path);
        std::vector<Kline> combined;
        combined.reserve(existing_records.size() + day_rows.size());
        for (const auto& r : existing_records) combined.push_back(from_record(r));
        for (const auto& k : day_rows) combined.push_back(k);
        auto clean = clean_one_day(std::move(combined), day);
        std::vector<KlineRecord> out_records;
        out_records.reserve(clean.size());
        for (const auto& k : clean) out_records.push_back(to_record(k));
        atomic_write_records(path, out_records);
        written += static_cast<int>(new_records.size());
        LOG_DBG("[", market_name(market), " ", symbol, " ", source, "] day=", date_to_string(day), " existing=", existing_records.size(), " incoming=", new_records.size(), " after=", out_records.size());
    }
    return written;
}

static std::optional<int64_t> read_latest_open_time_ms(Market market, const std::string& symbol, int lookback_days = 7) {
    auto today = ms_to_day(utc_now_ms());
    std::optional<int64_t> latest;
    for (int offset=0; offset<lookback_days; ++offset) {
        auto d = today - std::chrono::days{offset};
        auto path = daily_bin_path(market, symbol, d);
        if (!fs::exists(path)) continue;
        try {
            FileLockGuard lock(path);
            auto records = read_bin_file(path);
            for (auto& r : records) latest = latest ? std::max(*latest, r.open_time) : r.open_time;
        } catch (const std::exception& e) {
            LOG_WARN("read_latest_open_time_ms failed ", path.string(), ": ", e.what());
        }
    }
    return latest;
}

// =============================================================================
// HTTP HELPERS WITH CURL
// =============================================================================

struct HttpResponse {
    long status = 0;
    std::string body;
    std::string retry_after;
};

static size_t curl_write_string(char* ptr, size_t size, size_t nmemb, void* userdata) {
    auto* s = static_cast<std::string*>(userdata);
    s->append(ptr, size * nmemb);
    return size * nmemb;
}
static size_t curl_header_callback(char* buffer, size_t size, size_t nitems, void* userdata) {
    auto* resp = static_cast<HttpResponse*>(userdata);
    std::string h(buffer, size * nitems);
    auto pos = h.find(':');
    if (pos != std::string::npos) {
        auto key = lower(trim(h.substr(0, pos)));
        auto val = trim(h.substr(pos+1));
        if (key == "retry-after") resp->retry_after = val;
    }
    return size * nitems;
}
static std::string url_encode(const std::string& x) {
    CURL* c = curl_easy_init();
    char* enc = curl_easy_escape(c, x.c_str(), static_cast<int>(x.size()));
    std::string out = enc ? enc : "";
    if (enc) curl_free(enc);
    curl_easy_cleanup(c);
    return out;
}
static std::string build_url(const std::string& base, const std::map<std::string, std::string>& params) {
    if (params.empty()) return base;
    std::ostringstream qs;
    bool first = true;
    for (auto& [k, v] : params) {
        if (!first) qs << '&';
        first = false;
        qs << url_encode(k) << '=' << url_encode(v);
    }
    return base + (base.find('?') == std::string::npos ? "?" : "&") + qs.str();
}
static const std::string& curl_ca_bundle_path() {
    static const std::string path = []{
        const char* env_names[] = {"CURL_CA_BUNDLE", "SSL_CERT_FILE"};
        for (const char* name : env_names) {
            if (const char* value = std::getenv(name)) {
                std::error_code ec;
                fs::path p(value);
                if (!value[0] || !fs::exists(p, ec) || fs::file_size(p, ec) == 0) continue;
                return std::string(value);
            }
        }
        const fs::path candidates[] = {
            R"(C:\msys64\ucrt64\etc\ssl\certs\ca-bundle.crt)",
            R"(C:\msys64\ucrt64\etc\ssl\cert.pem)",
            R"(C:\msys64\usr\ssl\certs\ca-bundle.crt)",
            R"(C:\msys64\usr\ssl\cert.pem)"
        };
        for (const auto& p : candidates) {
            std::error_code ec;
            if (fs::exists(p, ec) && fs::file_size(p, ec) > 0) return p.string();
        }
        return std::string{};
    }();
    return path;
}
static void configure_curl_tls(CURL* curl) {
    const auto& ca_path = curl_ca_bundle_path();
    if (!ca_path.empty()) curl_easy_setopt(curl, CURLOPT_CAINFO, ca_path.c_str());
}
static HttpResponse http_get(const std::string& url, long timeout_s = HTTP_TIMEOUT_S) {
    CURL* curl = curl_easy_init();
    if (!curl) throw std::runtime_error("curl_easy_init failed");
    HttpResponse resp;
    configure_curl_tls(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, timeout_s);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "klines-only-collector-cpp/1.0");
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_string);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp.body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, curl_header_callback);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &resp);
    CURLcode rc = curl_easy_perform(curl);
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &resp.status);
    curl_easy_cleanup(curl);
    if (rc != CURLE_OK) throw std::runtime_error(std::string("curl error: ") + curl_easy_strerror(rc));
    return resp;
}

static bool should_rate_limit_rest_url(const std::string& full_url) {
    std::string u = lower(full_url);
    return u.find("https://api.binance.com/") == 0 ||
           u.find("https://fapi.binance.com/") == 0 ||
           u.find("https://dapi.binance.com/") == 0;
}

static int kline_request_weight(Market market, int limit) {
    limit = std::max(1, limit);

    // Spot /api/v3/klines has fixed request weight 2.
    if (market == Market::spot) return 2;

    // USD-M and COIN-M futures kline request weight depends on LIMIT.
    // Keep LIMIT <= 999 to avoid the >1000 weight bucket.
    if (limit < 100) return 1;
    if (limit < 500) return 2;
    if (limit <= 1000) return 5;
    return 10;
}

static void rest_global_rate_limit_wait(const std::string& full_url, const std::string& label, int request_weight = 1) {
    if (!REST_RATE_LIMIT_ENABLED) return;
    if (!should_rate_limit_rest_url(full_url)) return; // Do not slow S3/Binance Vision downloads.

    using clock = std::chrono::steady_clock;
    static std::mutex mu;
    static std::deque<std::pair<clock::time_point, int>> recent_weight_events;
    static clock::time_point next_allowed = clock::now();

    const int max_weight_per_minute = std::max(1, REST_MAX_WEIGHT_PER_MINUTE);
    const int weight = std::clamp(request_weight, 1, max_weight_per_minute);
    const auto window = std::chrono::minutes(1);

    // Spread requests approximately by request weight. Examples with 300 weight/min:
    // weight 1 -> ~200ms spacing, weight 5 -> ~1000ms spacing, weight 10 -> ~2000ms spacing.
    const auto spacing = std::chrono::milliseconds(
        std::max<int64_t>(1, (60'000LL * weight + max_weight_per_minute - 1) / max_weight_per_minute)
    );

    std::unique_lock<std::mutex> lk(mu);
    for (;;) {
        auto now = clock::now();
        while (!recent_weight_events.empty() && now - recent_weight_events.front().first >= window) {
            recent_weight_events.pop_front();
        }

        int used_weight = 0;
        for (const auto& [tp, w] : recent_weight_events) {
            (void)tp;
            used_weight += w;
        }

        auto wait_until = now;
        bool must_wait = false;

        if (used_weight + weight > max_weight_per_minute) {
            auto limited_until = recent_weight_events.empty()
                ? now + std::chrono::milliseconds(100)
                : recent_weight_events.front().first + window;
            if (limited_until > wait_until) wait_until = limited_until;
            must_wait = true;
        }

        if (REST_RATE_LIMIT_SPREAD_REQUESTS && now < next_allowed) {
            if (next_allowed > wait_until) wait_until = next_allowed;
            must_wait = true;
        }

        if (!must_wait) {
            recent_weight_events.push_back({now, weight});
            if (REST_RATE_LIMIT_SPREAD_REQUESTS) next_allowed = now + spacing;
            return;
        }

        LOG_DBG("[REST WEIGHT LIMIT] waiting label=", label,
                " weight=", weight,
                " used_weight=", used_weight,
                "/", max_weight_per_minute);

        lk.unlock();
        std::this_thread::sleep_until(wait_until);
        lk.lock();
    }
}

static std::string safe_get_text(const std::string& url, const std::map<std::string,std::string>& params = {}, int retries = HTTP_RETRIES, const std::string& label = "request_text", int request_weight = 1) {
    double delay = 1.0;
    auto full = build_url(url, params);
    for (int attempt=1; attempt<=retries; ++attempt) {
        try {
            rest_global_rate_limit_wait(full, label, request_weight);
            auto resp = http_get(full, HTTP_TIMEOUT_S);
            if (resp.status == 200) return resp.body;
            if (resp.status == 418 || resp.status == 429 || resp.status == 500 || resp.status == 502 || resp.status == 503 || resp.status == 504) {
                double sleep_s = delay;
                if (!resp.retry_after.empty()) sleep_s = std::max(sleep_s, std::stod(resp.retry_after));
                LOG_WARN("[", label, "] HTTP ", resp.status, " retrying in ", sleep_s, "s url=", full);
                std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(sleep_s*1000)));
                delay = std::min(delay * 2.0, 60.0);
                continue;
            }
            throw std::runtime_error("HTTP " + std::to_string(resp.status) + " body=" + resp.body.substr(0, 300));
        } catch (const std::exception& e) {
            LOG_WARN("[", label, "] network/error attempt ", attempt, "/", retries, ": ", e.what());
            if (attempt == retries) throw;
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>((delay + 0.2) * 1000)));
            delay = std::min(delay * 2.0, 60.0);
        }
    }
    throw std::runtime_error(label + " failed after retries");
}
static json safe_get_json(const std::string& url, const std::map<std::string,std::string>& params = {}, int retries = HTTP_RETRIES, const std::string& label = "request", int request_weight = 1) {
    return json::parse(safe_get_text(url, params, retries, label, request_weight));
}

static bool safe_download(const std::string& url, const fs::path& out_path, const std::string& label, int retries = BULK_DOWNLOAD_RETRIES) {
    std::error_code dir_ec;
    fs::create_directories(out_path.parent_path(), dir_ec);
    if (dir_ec) {
        LOG_WARN("[", label, "] cannot create directory for download path: ", out_path.parent_path().string(), " error=", dir_ec.message());
        return false;
    }
    if (fs::exists(out_path) && fs::file_size(out_path) > 0) return true;
    fs::path part = out_path;
    part += ".part";
    double delay = 1.0;
    for (int attempt=1; attempt<=retries; ++attempt) {
        try {
            CURL* curl = curl_easy_init();
            if (!curl) throw std::runtime_error("curl_easy_init failed");
            FILE* fp = std::fopen(part.string().c_str(), "wb");
            if (!fp) throw std::runtime_error("cannot open temp file: " + part.string());
            configure_curl_tls(curl);
            curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
            curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
            curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, BULK_DOWNLOAD_CONNECT_TIMEOUT_S);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, BULK_DOWNLOAD_READ_TIMEOUT_S);
            curl_easy_setopt(curl, CURLOPT_USERAGENT, "klines-only-collector-cpp/1.0");
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, fp);
            CURLcode rc = curl_easy_perform(curl);
            long status = 0;
            curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
            curl_easy_cleanup(curl);
            std::fclose(fp);
            if (rc != CURLE_OK) throw std::runtime_error(std::string("curl error: ") + curl_easy_strerror(rc));
            if (status == 404) { fs::remove(part); return false; }
            if (status != 200) throw std::runtime_error("HTTP " + std::to_string(status));
            fs::rename(part, out_path);
            return true;
        } catch (const std::exception& e) {
            LOG_WARN("[", label, "] download attempt ", attempt, "/", retries, " failed: ", e.what());
            std::error_code ec; fs::remove(part, ec);
            if (attempt == retries) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(delay * 1000)));
            delay = std::min(delay * 2.0, BULK_DOWNLOAD_BACKOFF_MAX_S);
        }
    }
    return false;
}

// =============================================================================
// KLINE PARSERS
// =============================================================================

static double parse_double_safe(const json& v, double fallback = 0.0) {
    if (v.is_number_float() || v.is_number_integer()) return v.get<double>();
    if (v.is_string()) return std::stod(v.get<std::string>());
    return fallback;
}
static int64_t parse_int64_safe(const json& v, int64_t fallback = 0) {
    if (v.is_number_integer()) return v.get<int64_t>();
    if (v.is_number_float()) return static_cast<int64_t>(v.get<double>());
    if (v.is_string()) return std::stoll(v.get<std::string>());
    return fallback;
}
static std::vector<Kline> rest_json_to_klines(const std::string& symbol, const json& rows) {
    std::vector<Kline> out;
    if (!rows.is_array()) return out;
    for (auto& r : rows) {
        if (!r.is_array() || r.size() < 11) continue;
        Kline k;
        k.symbol = upper(symbol);
        k.open_time = parse_int64_safe(r[0]);
        k.open = parse_double_safe(r[1]);
        k.high = parse_double_safe(r[2]);
        k.low = parse_double_safe(r[3]);
        k.close = parse_double_safe(r[4]);
        k.volume = parse_double_safe(r[5]);
        k.close_time = parse_int64_safe(r[6]);
        k.quote_volume = parse_double_safe(r[7]);
        k.trades = parse_int64_safe(r[8]);
        k.taker_base_vol = parse_double_safe(r[9]);
        k.taker_quote_vol = parse_double_safe(r[10]);
        k.maker_base_vol = k.volume - k.taker_base_vol;
        k.maker_quote_vol = k.quote_volume - k.taker_quote_vol;
        out.push_back(k);
    }
    return normalize_klines(std::move(out), symbol);
}
static std::optional<Kline> ws_kline_to_kline(const json& k) {
    try {
        Kline out;
        out.symbol = upper(k.value("s", ""));
        out.open_time = parse_int64_safe(k.at("t"));
        out.close_time = parse_int64_safe(k.at("T"));
        out.open = parse_double_safe(k.at("o"));
        out.high = parse_double_safe(k.at("h"));
        out.low = parse_double_safe(k.at("l"));
        out.close = parse_double_safe(k.at("c"));
        out.volume = parse_double_safe(k.at("v"));
        out.quote_volume = parse_double_safe(k.at("q"));
        out.trades = parse_int64_safe(k.at("n"));
        out.taker_base_vol = parse_double_safe(k.at("V"));
        out.taker_quote_vol = parse_double_safe(k.at("Q"));
        out.maker_base_vol = out.volume - out.taker_base_vol;
        out.maker_quote_vol = out.quote_volume - out.taker_quote_vol;
        if (out.symbol.empty() || !is_valid_binance_symbol_name(out.symbol) || out.open_time % INTERVAL_MS != 0 || !is_finite_kline(out)) return std::nullopt;
        return out;
    } catch (...) {
        return std::nullopt;
    }
}
static std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> fields;
    std::string cur;
    bool in_quotes = false;
    for (char c : line) {
        if (c == '"') { in_quotes = !in_quotes; continue; }
        if (c == ',' && !in_quotes) { fields.push_back(cur); cur.clear(); }
        else cur.push_back(c);
    }
    fields.push_back(cur);
    return fields;
}

static int64_t portable_timegm_utc(std::tm* tm) {
#if defined(_WIN32)
    return static_cast<int64_t>(_mkgmtime(tm));
#else
    return static_cast<int64_t>(timegm(tm));
#endif
}

static std::optional<int64_t> parse_iso_utc_to_ms(std::string s) {
    // Small fallback equivalent to the Python pd.to_datetime(..., utc=True) path.
    // Binance Vision normally uses numeric timestamps, but this makes the CSV
    // reader tolerant of ISO-like timestamps too.
    s = trim(s);
    if (s.empty()) return std::nullopt;
    if (!s.empty() && (s.back() == 'Z' || s.back() == 'z')) s.pop_back();
    auto dot = s.find('.');
    if (dot != std::string::npos) s = s.substr(0, dot);
    std::replace(s.begin(), s.end(), 'T', ' ');

    std::tm tm{};
    std::istringstream is{s};
    is >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    if (!is.fail()) return portable_timegm_utc(&tm) * 1000LL;

    tm = {};
    std::istringstream is2{s};
    is2 >> std::get_time(&tm, "%Y-%m-%d");
    if (!is2.fail()) return portable_timegm_utc(&tm) * 1000LL;
    return std::nullopt;
}

static std::optional<int64_t> to_epoch_ms_token(const std::string& token) {
    // Faithful C++ equivalent of the Python to_epoch_ms_series():
    //   >1e14              microseconds -> milliseconds
    //   1e11..1e14         already milliseconds
    //   1e9..1e11          seconds -> milliseconds
    // plus a defensive nanosecond branch for very large values.
    std::string t = trim(token);
    if (t.empty()) return std::nullopt;
    try {
        size_t pos = 0;
        long double v = std::stold(t, &pos);
        // Accept numeric strings with whitespace after the number only.
        while (pos < t.size() && std::isspace(static_cast<unsigned char>(t[pos]))) ++pos;
        if (pos == t.size()) {
            long double av = std::fabsl(v);
            long double ms = 0;
            if (av > 1.0e17L) ms = std::floor(v / 1'000'000.0L); // ns -> ms, defensive
            else if (av > 1.0e14L) ms = std::floor(v / 1'000.0L); // us -> ms
            else if (av > 1.0e11L) ms = v;                       // ms
            else if (av > 1.0e9L)  ms = v * 1000.0L;              // s -> ms
            else return std::nullopt;
            return static_cast<int64_t>(std::llround(ms));
        }
    } catch (...) {
        // Fall through to ISO parser.
    }
    return parse_iso_utc_to_ms(t);
}

static std::optional<Kline> csv_row_to_kline(const std::string& symbol, const std::vector<std::string>& f) {
    if (f.size() < 11) return std::nullopt;
    try {
        auto open_ms = to_epoch_ms_token(f[0]);
        auto close_ms = to_epoch_ms_token(f[6]);
        if (!open_ms || !close_ms) return std::nullopt; // header row or malformed timestamp

        Kline k;
        k.symbol = upper(symbol);
        k.open_time = *open_ms;
        k.open = std::stod(trim(f[1]));
        k.high = std::stod(trim(f[2]));
        k.low = std::stod(trim(f[3]));
        k.close = std::stod(trim(f[4]));
        k.volume = std::stod(trim(f[5]));
        k.close_time = *close_ms;
        k.quote_volume = std::stod(trim(f[7]));
        k.trades = std::stoll(trim(f[8]));
        k.taker_base_vol = std::stod(trim(f[9]));
        k.taker_quote_vol = std::stod(trim(f[10]));
        k.maker_base_vol = k.volume - k.taker_base_vol;
        k.maker_quote_vol = k.quote_volume - k.taker_quote_vol;
        if (k.open_time % INTERVAL_MS != 0 || !is_finite_kline(k)) return std::nullopt;
        return k;
    } catch (...) {
        return std::nullopt; // header row or malformed row
    }
}
static std::vector<Kline> read_bulk_csv_to_klines(const fs::path& csv_path, const std::string& symbol) {
    std::ifstream f(csv_path);
    std::vector<Kline> out;
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        auto fields = split_csv_line(line);
        auto k = csv_row_to_kline(symbol, fields);
        if (k) out.push_back(*k);
    }
    return normalize_klines(std::move(out), symbol);
}

// =============================================================================
// SYMBOL DISCOVERY
// =============================================================================

static std::vector<std::string> fetch_exchangeinfo_live_symbols(Market market) {
    json payload = safe_get_json(market_exchange_info_url(market), {}, HTTP_RETRIES, "exchangeInfo " + market_name(market));
    std::set<std::string> out;
    if (payload.contains("symbols") && payload["symbols"].is_array()) {
        for (auto& item : payload["symbols"]) {
            if (!item.is_object()) continue;
            std::string sym = upper(item.value("symbol", ""));
            if (sym.empty() || !is_valid_binance_symbol_name(sym)) continue;
            std::string status = upper(item.value("status", item.value("contractStatus", "")));
            std::string contract_status = upper(item.value("contractStatus", ""));
            bool trading = status == "TRADING" || contract_status == "TRADING";
            if (!trading) continue;
            if (market == Market::spot && item.contains("isSpotTradingAllowed") && item["isSpotTradingAllowed"].is_boolean() && !item["isSpotTradingAllowed"].get<bool>()) continue;
            out.insert(sym);
        }
    }
    LOG_INFO("[SYMBOLS ", market_name(market), " live/rest] exchangeInfo trading symbols=", out.size());
    return std::vector<std::string>(out.begin(), out.end());
}

static std::tuple<std::string,bool,std::string> latest_closed_kline_is_recent(Market market, const std::string& symbol) {
    try {
        int64_t end_ms = last_closed_minute_open_ms();
        int64_t start_ms = end_ms - AUTO_VALIDATE_MAX_AGE_MINUTES * INTERVAL_MS;
        json data = safe_get_json(market_rest_url(market), {
            {"symbol", upper(symbol)}, {"interval", INTERVAL}, {"startTime", std::to_string(start_ms)},
            {"endTime", std::to_string(end_ms + INTERVAL_MS - 1)}, {"limit", "1"}
        }, HTTP_RETRIES, "live-check " + market_name(market) + " " + symbol);
        if (!data.is_array() || data.empty()) return {upper(symbol), false, "no_rows"};
        int64_t open_ms = parse_int64_safe(data.back().at(0));
        int64_t age_min = (end_ms - open_ms) / INTERVAL_MS;
        if (age_min <= AUTO_VALIDATE_MAX_AGE_MINUTES) return {upper(symbol), true, "age_min=" + std::to_string(age_min)};
        return {upper(symbol), false, "stale_age_min=" + std::to_string(age_min)};
    } catch (const std::exception& e) {
        return {upper(symbol), false, std::string("error=") + e.what()};
    }
}

static void write_symbol_list(const std::string& name, const std::vector<std::string>& symbols) {
    fs::create_directories(SYMBOLS_OUTPUT_DIR);
    auto path = SYMBOLS_OUTPUT_DIR / name;
    std::ofstream f(path);
    auto clean = clean_symbol_list(symbols);
    for (auto& s : clean) f << s << '\n';
    LOG_INFO("[SYMBOLS] saved ", clean.size(), " -> ", path.string());
}

// Forward declaration: bulk discovery uses S3 XML functions below.
static std::vector<std::string> discover_vision_symbols(Market market);

static std::pair<std::vector<std::string>, std::vector<std::string>> resolve_market_symbols(Market market, bool auto_find, const std::vector<std::string>& bulk_predefined, const std::vector<std::string>& live_predefined) {
    if (!auto_find) {
        auto bulk = clean_symbol_list(bulk_predefined);
        auto live = clean_symbol_list(live_predefined);
        LOG_INFO("[SYMBOLS ", market_name(market), "] using predefined bulk=", bulk.size(), " live/rest=", live.size());
        return {bulk, live};
    }
    auto bulk = discover_vision_symbols(market);
    auto live = fetch_exchangeinfo_live_symbols(market);
    if (AUTO_VALIDATE_LIVE_SYMBOLS_WITH_KLINE) {
        std::vector<std::string> filtered;
        std::mutex mu;
        std::vector<std::thread> threads;
        std::atomic<size_t> idx{0};
        int workers = std::min<int>(AUTO_VALIDATE_MAX_WORKERS, live.size());
        for (int w=0; w<workers; ++w) {
            threads.emplace_back([&]{
                for (;;) {
                    size_t i = idx.fetch_add(1);
                    if (i >= live.size()) break;
                    auto [sym, ok, detail] = latest_closed_kline_is_recent(market, live[i]);
                    if (ok) { std::lock_guard<std::mutex> lk(mu); filtered.push_back(sym); }
                }
            });
        }
        for (auto& t : threads) t.join();
        std::sort(filtered.begin(), filtered.end());
        live = std::move(filtered);
    }
    return {bulk, live};
}

struct SymbolSets {
    std::vector<std::string> spot_bulk, um_bulk, cm_bulk;
    std::vector<std::string> spot_live_rest, um_live_rest, cm_live_rest;
};

static SymbolSets resolve_symbol_sets() {
    auto [spot_bulk, spot_live] = resolve_market_symbols(Market::spot, AUTO_FIND_SYMBOLS_SPOT, SYMBOLS_SPOT_BULK, SYMBOLS_SPOT_LIVE_REST);
    auto [um_bulk, um_live] = resolve_market_symbols(Market::um, AUTO_FIND_SYMBOLS_UM, SYMBOLS_FUTURES_UM_BULK, SYMBOLS_FUTURES_UM_LIVE_REST);
    auto [cm_bulk, cm_live] = resolve_market_symbols(Market::cm, AUTO_FIND_SYMBOLS_CM, SYMBOLS_FUTURES_CM_BULK, SYMBOLS_FUTURES_CM_LIVE_REST);
    write_symbol_list("spot_bulk_symbols.txt", spot_bulk);
    write_symbol_list("um_bulk_symbols.txt", um_bulk);
    write_symbol_list("cm_bulk_symbols.txt", cm_bulk);
    write_symbol_list("spot_live_rest_symbols.txt", spot_live);
    write_symbol_list("um_live_rest_symbols.txt", um_live);
    write_symbol_list("cm_live_rest_symbols.txt", cm_live);
    auto futures_bulk = um_bulk; futures_bulk.insert(futures_bulk.end(), cm_bulk.begin(), cm_bulk.end());
    auto futures_live = um_live; futures_live.insert(futures_live.end(), cm_live.begin(), cm_live.end());
    write_symbol_list("combined_futures_bulk_symbols.txt", clean_symbol_list(futures_bulk));
    write_symbol_list("combined_futures_live_rest_symbols.txt", clean_symbol_list(futures_live));
    return {spot_bulk, um_bulk, cm_bulk, spot_live, um_live, cm_live};
}

// =============================================================================
// REST LOOKBACK - 3 x 999 rows per symbol
// =============================================================================

static std::vector<Kline> fetch_rest_klines_range(Market market, const std::string& symbol, int64_t start_ms, int64_t end_ms, int max_chunks = REST_MAX_CHUNKS_PER_SYMBOL) {
    std::string sym = upper(symbol);
    std::string url = market_rest_url(market);
    int limit = rest_limit(market);
    max_chunks = std::max(1, max_chunks);

    std::vector<Kline> rows;
    int64_t cursor = floor_minute_ms(start_ms);
    end_ms = floor_minute_ms(end_ms);
    int chunks_done = 0;

    while (cursor <= end_ms && chunks_done < max_chunks) {
        int64_t remaining_rows = ((end_ms - cursor) / INTERVAL_MS) + 1;
        int request_limit = static_cast<int>(std::min<int64_t>(limit, remaining_rows));
        request_limit = std::max(1, request_limit);

        int request_weight = kline_request_weight(market, request_limit);
        std::string rest_label = "REST " + market_name(market) + " " + sym +
                                 " rows=" + std::to_string(request_limit) +
                                 " weight=" + std::to_string(request_weight);

        json data = safe_get_json(url, {
            {"symbol", sym}, {"interval", INTERVAL}, {"startTime", std::to_string(cursor)},
            {"endTime", std::to_string(end_ms + INTERVAL_MS - 1)}, {"limit", std::to_string(request_limit)}
        }, HTTP_RETRIES, rest_label, request_weight);

        if (!data.is_array() || data.empty()) break;

        auto batch = rest_json_to_klines(sym, data);
        rows.insert(rows.end(), batch.begin(), batch.end());
        ++chunks_done;

        int64_t last_open = parse_int64_safe(data.back().at(0));
        int64_t next_cursor = last_open + INTERVAL_MS;
        if (static_cast<int>(data.size()) < request_limit || next_cursor <= cursor) break;
        cursor = next_cursor;
        // No fixed sleep here: the global REST weight limiter controls pacing,
        // and disabling REST_RATE_LIMIT_SPREAD_REQUESTS allows parallel calls up to the bucket limit.
    }

    rows = normalize_klines(std::move(rows), sym);
    std::vector<Kline> filtered;
    for (auto& k : rows) if (k.open_time >= start_ms && k.open_time <= end_ms) filtered.push_back(k);
    std::stable_sort(filtered.begin(), filtered.end(), [](const Kline& a, const Kline& b){ return a.open_time < b.open_time; });
    std::vector<Kline> deduped;
    for (auto& k : filtered) {
        if (!deduped.empty() && deduped.back().open_time == k.open_time) deduped.back() = k;
        else deduped.push_back(k);
    }
    return deduped;
}

static std::pair<std::string,int> update_rest_lookback_for_symbol(Market market, const std::string& symbol, int rows_back = REST_LOOKBACK_ROWS) {
    rows_back = std::max(1, rows_back);
    rows_back = std::min(rows_back, rest_limit(market) * std::max(1, REST_MAX_CHUNKS_PER_SYMBOL));

    std::string sym = upper(symbol);
    int64_t end_ms = last_closed_minute_open_ms();
    int64_t lookback_start_ms = end_ms - static_cast<int64_t>(rows_back - 1) * INTERVAL_MS;
    int64_t start_ms = lookback_start_ms;
    std::optional<int64_t> disk_latest;

    if (REST_LOOKBACK_ONLY_MISSING_FROM_DISK) {
        disk_latest = read_latest_open_time_ms(market, sym, 7);
        if (disk_latest) {
            // If the latest saved candle is inside or after the configured lookback window,
            // only request the truly missing candles after it. This turns many startup
            // backfills into one small REST call instead of always 3 x 999 rows.
            if (*disk_latest >= lookback_start_ms) {
                start_ms = *disk_latest + INTERVAL_MS;
            }
        }
    }

    if (start_ms > end_ms) {
        LOG_INFO("[REST ", market_name(market), " ", sym,
                 "] skip already up to date disk_latest=",
                 (disk_latest ? ms_to_utc_string(*disk_latest) : std::string("none")),
                 " end=", ms_to_utc_string(end_ms));
        return {sym, 0};
    }

    int64_t rows_needed = ((end_ms - start_ms) / INTERVAL_MS) + 1;
    int max_chunks = static_cast<int>((rows_needed + rest_limit(market) - 1) / rest_limit(market));
    max_chunks = std::clamp(max_chunks, 1, REST_MAX_CHUNKS_PER_SYMBOL);

    auto rows = fetch_rest_klines_range(market, sym, start_ms, end_ms, max_chunks);
    int written = write_klines_daily(market, sym, rows, WriteMode::dedupe_sort,
                                     "rest_missing_" + std::to_string(rows_needed) + "rows");

    LOG_INFO("[REST ", market_name(market), " ", sym,
             "] lookback_rows=", rows_back,
             " missing_rows=", rows_needed,
             " max_chunks=", max_chunks,
             " limit=", rest_limit(market),
             " disk_latest=", (disk_latest ? ms_to_utc_string(*disk_latest) : std::string("none")),
             " start=", ms_to_utc_string(start_ms),
             " end=", ms_to_utc_string(end_ms),
             " fetched=", rows.size(),
             " wrote=", written);

    return {sym, static_cast<int>(rows.size())};
}

static void update_last_72h_rest_all_symbols(const std::vector<std::string>& spot, const std::vector<std::string>& um, const std::vector<std::string>& cm, int max_workers = REST_SYMBOL_THREADS) {
    std::vector<std::pair<Market,std::string>> jobs;
    for (auto& s : clean_symbol_list(spot)) jobs.push_back({Market::spot, s});
    for (auto& s : clean_symbol_list(um)) jobs.push_back({Market::um, s});
    for (auto& s : clean_symbol_list(cm)) jobs.push_back({Market::cm, s});
    if (jobs.empty()) { LOG_INFO("[REST72] no symbols"); return; }
    LOG_INFO("[REST72] starting ", jobs.size(), " jobs max_workers=", max_workers);
    std::atomic<size_t> idx{0};
    std::vector<std::thread> workers;
    int n = std::min<int>(max_workers, jobs.size());
    for (int w=0; w<n; ++w) {
        workers.emplace_back([&]{
            for (;;) {
                size_t i = idx.fetch_add(1);
                if (i >= jobs.size()) break;
                try {
                    auto [m, s] = jobs[i];
                    auto [sym, rows] = update_rest_lookback_for_symbol(m, s);
                    LOG_INFO("[REST72] done ", market_name(m), " ", sym, " rows=", rows);
                } catch (const std::exception& e) {
                    LOG_ERR("[REST72] failed: ", e.what());
                }
            }
        });
    }
    for (auto& t : workers) t.join();
}

// =============================================================================
// BULK BINANCE VISION DOWNLOAD + IMPORT
// =============================================================================

struct BulkArchiveJob {
    Market market;
    std::string symbol;
    std::string timeperiod; // monthly or daily
    std::chrono::sys_days d;
    std::string url;
    fs::path csv_path;
    std::string remote_key;
    bool local_csv_exists = false;

    std::string archive_label() const {
        return symbol + "-" + INTERVAL + "-" + (timeperiod == "monthly" ? month_to_string(d) : date_to_string(d));
    }
    bool needs_download() const { return !local_csv_exists; }
};

static std::pair<std::vector<std::string>, std::vector<std::string>> s3_list_keys_and_prefixes(const std::string& prefix, int max_pages = 500) {
    std::vector<std::string> keys, prefixes;
    std::optional<std::string> marker;
    for (int page=0; page<max_pages; ++page) {
        std::map<std::string,std::string> params{{"delimiter","/"},{"prefix",prefix}};
        if (marker) params["marker"] = *marker;
        std::string xml = safe_get_text(S3_LIST_URL, params, HTTP_RETRIES, "S3 " + prefix);
        pugi::xml_document doc;
        auto ok = doc.load_string(xml.c_str());
        if (!ok) throw std::runtime_error("cannot parse S3 XML");
        std::vector<std::string> page_keys;
        for (pugi::xml_node c: doc.child("ListBucketResult").children("Contents")) {
            std::string k = c.child("Key").text().as_string();
            if (!k.empty()) { keys.push_back(k); page_keys.push_back(k); }
        }
        for (pugi::xml_node cp: doc.child("ListBucketResult").children("CommonPrefixes")) {
            std::string p = cp.child("Prefix").text().as_string();
            if (!p.empty()) prefixes.push_back(p);
        }
        std::string truncated = lower(doc.child("ListBucketResult").child("IsTruncated").text().as_string());
        if (truncated != "true") break;
        std::string next = doc.child("ListBucketResult").child("NextMarker").text().as_string();
        if (next.empty() && !page_keys.empty()) next = page_keys.back();
        if (next.empty()) break;
        marker = next;
    }
    return {keys, prefixes};
}
static std::vector<std::string> s3_list_keys(const std::string& prefix, int max_pages = 500) {
    return s3_list_keys_and_prefixes(prefix, max_pages).first;
}
static std::string vision_symbol_root_prefix(Market market, const std::string& timeperiod) {
    return "data/" + vision_market_path(market) + "/" + timeperiod + "/klines/";
}
static std::string vision_prefix(Market market, const std::string& symbol, const std::string& timeperiod) {
    return "data/" + vision_market_path(market) + "/" + timeperiod + "/klines/" + upper(symbol) + "/" + INTERVAL + "/";
}
static std::string vision_base_url(Market market, const std::string& timeperiod) {
    return "https://data.binance.vision/data/" + vision_market_path(market) + "/" + timeperiod + "/klines";
}
static std::vector<std::string> discover_vision_symbols(Market market) {
    std::set<std::string> symbols;
    for (auto timeperiod : {std::string("daily"), std::string("monthly")}) {
        auto root_prefix = vision_symbol_root_prefix(market, timeperiod);
        auto [keys, prefixes] = s3_list_keys_and_prefixes(root_prefix);
        (void)keys;
        for (auto& p : prefixes) {
            std::string rest = p.rfind(root_prefix, 0) == 0 ? p.substr(root_prefix.size()) : p;
            auto slash = rest.find('/');
            std::string sym = slash == std::string::npos ? rest : rest.substr(0, slash);
            sym = upper(trim(sym));
            if (!sym.empty() && is_valid_binance_symbol_name(sym)) symbols.insert(sym);
        }
    }
    LOG_INFO("[SYMBOLS ", market_name(market), " bulk] discovered ", symbols.size(), " Binance Vision symbols");
    return std::vector<std::string>(symbols.begin(), symbols.end());
}
static std::pair<std::set<std::chrono::sys_days>, std::set<std::chrono::sys_days>> list_available_archives(Market market, const std::string& symbol) {
    std::string sym = upper(symbol);
    std::regex re_month("^" + sym + "-" + INTERVAL + "-([0-9]{4})-([0-9]{2})\\.zip$", std::regex::icase);
    std::regex re_day("^" + sym + "-" + INTERVAL + "-([0-9]{4})-([0-9]{2})-([0-9]{2})\\.zip$", std::regex::icase);
    std::set<std::chrono::sys_days> months, days;
    auto monthly_keys = s3_list_keys(vision_prefix(market, sym, "monthly"));
    auto daily_keys = s3_list_keys(vision_prefix(market, sym, "daily"));
    std::smatch m;
    for (auto& key : monthly_keys) {
        auto base = fs::path(key).filename().string();
        if (base.ends_with(".checksum")) continue;
        if (std::regex_match(base, m, re_month)) {
            int y = std::stoi(m[1]); unsigned mon = static_cast<unsigned>(std::stoi(m[2]));
            months.insert(std::chrono::sys_days{std::chrono::year{y}/std::chrono::month{mon}/1});
        }
    }
    for (auto& key : daily_keys) {
        auto base = fs::path(key).filename().string();
        if (base.ends_with(".checksum")) continue;
        if (std::regex_match(base, m, re_day)) {
            int y = std::stoi(m[1]); unsigned mon = static_cast<unsigned>(std::stoi(m[2])); unsigned da = static_cast<unsigned>(std::stoi(m[3]));
            std::chrono::year_month_day ymd{std::chrono::year{y}, std::chrono::month{mon}, std::chrono::day{da}};
            if (ymd.ok()) days.insert(std::chrono::sys_days{ymd});
        }
    }
    return {months, days};
}
static std::vector<BulkArchiveJob> plan_bulk_jobs_for_symbol(Market market, const std::string& symbol, std::chrono::sys_days start_date, std::chrono::sys_days end_date) {
    std::string sym = upper(symbol);
    if (!is_valid_binance_symbol_name(sym)) {
        LOG_WARN("[BULK PLAN ", market_name(market), "] skipping invalid/non-ASCII symbol=", sym);
        return {};
    }
    auto [months_avail, days_avail] = list_available_archives(market, sym);
    std::vector<BulkArchiveJob> jobs;
    if (months_avail.empty() && days_avail.empty()) {
        LOG_WARN("[BULK PLAN ", market_name(market), " ", sym, "] no remote archives");
        return jobs;
    }
    std::vector<std::chrono::sys_days> wanted_months;
    for (auto m = month_start(start_date); m <= month_start(end_date); m = add_month(m)) wanted_months.push_back(m);
    std::set<std::chrono::sys_days> months_to_use;
    for (auto m : wanted_months) if (months_avail.count(m)) months_to_use.insert(m);
    std::set<std::pair<int,int>> months_covered;
    for (auto m : months_to_use) {
        auto ymd = std::chrono::year_month_day{m};
        months_covered.insert({int(ymd.year()), int(unsigned(ymd.month()))});
    }
    auto add_job = [&](const std::string& timeperiod, std::chrono::sys_days d) {
        std::string suffix = timeperiod == "monthly" ? month_to_string(d) : date_to_string(d);
        std::string zip_name = sym + "-" + INTERVAL + "-" + suffix + ".zip";
        fs::path csv_path = bulk_csv_path(market, sym, timeperiod, d);
        bool local = fs::exists(csv_path) && fs::file_size(csv_path) > 0;
        std::string remote_key = "data/" + vision_market_path(market) + "/" + timeperiod + "/klines/" + sym + "/" + INTERVAL + "/" + zip_name;
        std::string url = vision_base_url(market, timeperiod) + "/" + sym + "/" + INTERVAL + "/" + url_encode(zip_name);
        jobs.push_back({market, sym, timeperiod, d, url, csv_path, remote_key, local});
    };
    for (auto m : months_to_use) add_job("monthly", m);
    int remote_daily_in_range = 0;
    for (auto d : date_range_days(start_date, end_date)) {
        if (!days_avail.count(d)) continue;
        ++remote_daily_in_range;
        auto ymd = std::chrono::year_month_day{d};
        if (!months_covered.count({int(ymd.year()), int(unsigned(ymd.month()))})) add_job("daily", d);
    }
    int need = 0; for (auto& j : jobs) if (j.needs_download()) ++need;
    LOG_INFO("[BULK PLAN ", market_name(market), " ", sym, "] planned_total=", jobs.size(), " need_download=", need, " already_have_csv=", jobs.size()-need);
    return jobs;
}
static bool extract_first_csv(const fs::path& zip_path, const fs::path& csv_path) {
    fs::create_directories(csv_path.parent_path());
    int err = 0;
    zip_t* za = zip_open(zip_path.string().c_str(), ZIP_RDONLY, &err);
    if (!za) return false;
    zip_int64_t n = zip_get_num_entries(za, 0);
    for (zip_uint64_t i=0; i<static_cast<zip_uint64_t>(n); ++i) {
        const char* name = zip_get_name(za, i, 0);
        if (!name) continue;
        std::string nm(name);
        if (lower(nm).ends_with(".csv")) {
            zip_file_t* zf = zip_fopen_index(za, i, 0);
            if (!zf) { zip_close(za); return false; }
            std::ofstream out(csv_path, std::ios::binary | std::ios::trunc);
            std::vector<char> buf(1024 * 1024);
            zip_int64_t r = 0;
            while ((r = zip_fread(zf, buf.data(), buf.size())) > 0) out.write(buf.data(), static_cast<std::streamsize>(r));
            zip_fclose(zf);
            zip_close(za);
            return fs::exists(csv_path) && fs::file_size(csv_path) > 0;
        }
    }
    zip_close(za);
    return false;
}
static std::optional<fs::path> download_and_extract_archive(const BulkArchiveJob& job, bool delete_zip=true) {
    if (fs::exists(job.csv_path) && fs::file_size(job.csv_path) > 0) return job.csv_path;
    fs::create_directories(job.csv_path.parent_path());
    auto zip_path = job.csv_path;
    zip_path.replace_extension(".zip");
    LOG_INFO("[BULK DOWNLOAD START ", market_name(job.market), " ", job.symbol, "] ", job.archive_label(), " url=", job.url);
    bool ok = safe_download(job.url, zip_path, "BULK " + market_name(job.market) + " " + job.symbol);
    if (!ok) return std::nullopt;
    bool extracted = extract_first_csv(zip_path, job.csv_path);
    if (delete_zip) { std::error_code ec; fs::remove(zip_path, ec); }
    if (!extracted) return std::nullopt;
    return job.csv_path;
}
static std::vector<std::chrono::sys_days> bulk_job_expected_days(const BulkArchiveJob& job, std::chrono::sys_days start_date, std::chrono::sys_days end_date) {
    if (job.timeperiod == "daily") return (start_date <= job.d && job.d <= end_date) ? std::vector<std::chrono::sys_days>{job.d} : std::vector<std::chrono::sys_days>{};
    auto start = std::max(start_date, job.d);
    auto end = std::min(end_date, month_end(job.d));
    if (start > end) return {};
    return date_range_days(start, end);
}
static bool daily_bin_complete(Market market, const std::string& symbol, std::chrono::sys_days day) {
    auto path = daily_bin_path(market, symbol, day);
    if (!fs::exists(path)) return false;
    auto sz = fs::file_size(path);
    return sz == static_cast<uintmax_t>(BULK_EXPECTED_ROWS_PER_DAY) * sizeof(KlineRecord);
}
static int import_bulk_csv_to_daily_bins(const BulkArchiveJob& job, std::chrono::sys_days start_date, std::chrono::sys_days end_date, const std::vector<std::chrono::sys_days>& only_days = {}) {
    if (!fs::exists(job.csv_path) || fs::file_size(job.csv_path) == 0) return 0;
    std::set<std::chrono::sys_days> only(only_days.begin(), only_days.end());
    auto rows = read_bulk_csv_to_klines(job.csv_path, job.symbol);
    int64_t start_ms = day_start_ms(start_date);
    int64_t end_ms = day_start_ms(end_date + std::chrono::days{1}) - 1;
    std::vector<Kline> filtered;
    for (auto& k : rows) {
        if (k.open_time < start_ms || k.open_time > end_ms) continue;
        if (!only.empty() && !only.count(ms_to_day(k.open_time))) continue;
        filtered.push_back(k);
    }
    LOG_INFO("[BULK IMPORT START ", market_name(job.market), " ", job.symbol, "] csv=", job.csv_path.string(), " rows=", filtered.size());
    if (filtered.empty()) return 0;
    int written = write_klines_daily(job.market, job.symbol, filtered, WriteMode::dedupe_sort, "bulk_csv");
    LOG_INFO("[BULK IMPORT DONE ", market_name(job.market), " ", job.symbol, "] rows_imported=", filtered.size(), " write_attempts=", written);
    return written;
}
static void download_bulk_historical_klines(const std::vector<std::string>& spot_symbols, const std::vector<std::string>& um_symbols, const std::vector<std::string>& cm_symbols, std::chrono::sys_days start_date, std::chrono::sys_days end_date, bool import_to_bins=true, bool delete_zip=true) {
    std::vector<std::pair<Market,std::string>> plan_items;
    for (auto& s : clean_symbol_list(spot_symbols)) plan_items.push_back({Market::spot, s});
    for (auto& s : clean_symbol_list(um_symbols)) plan_items.push_back({Market::um, s});
    for (auto& s : clean_symbol_list(cm_symbols)) plan_items.push_back({Market::cm, s});
    if (plan_items.empty()) { LOG_INFO("[BULK] no symbols"); return; }

    LOG_INFO("[BULK] planning ", plan_items.size(), " symbols ", date_to_string(start_date), " -> ", date_to_string(end_date));
    std::vector<BulkArchiveJob> all_jobs;
    std::mutex jobs_mu;
    std::atomic<size_t> idx{0};
    int plan_workers = std::min<int>(BULK_PLAN_THREADS, plan_items.size());
    std::vector<std::thread> planners;
    for (int w=0; w<plan_workers; ++w) {
        planners.emplace_back([&]{
            for (;;) {
                size_t i = idx.fetch_add(1);
                if (i >= plan_items.size()) break;
                try {
                    auto [m, s] = plan_items[i];
                    auto jobs = plan_bulk_jobs_for_symbol(m, s, start_date, end_date);
                    std::lock_guard<std::mutex> lk(jobs_mu);
                    all_jobs.insert(all_jobs.end(), jobs.begin(), jobs.end());
                } catch (const std::exception& e) {
                    LOG_ERR("bulk plan failed: ", e.what());
                }
            }
        });
    }
    for (auto& t : planners) t.join();
    if (all_jobs.empty()) { LOG_INFO("[BULK] no remote archives found"); return; }

    std::vector<BulkArchiveJob> completed;
    std::vector<BulkArchiveJob> download_jobs;
    for (auto& j : all_jobs) (j.needs_download() ? download_jobs : completed).push_back(j);
    LOG_INFO("[BULK] planned_total_csvs=", all_jobs.size(), " need_download=", download_jobs.size(), " already_have_csv=", completed.size());

    std::atomic<size_t> dli{0};
    std::mutex completed_mu;
    int dl_workers = std::min<int>(BULK_DOWNLOAD_THREADS, download_jobs.size());
    std::vector<std::thread> downloaders;
    for (int w=0; w<dl_workers; ++w) {
        downloaders.emplace_back([&]{
            for (;;) {
                size_t i = dli.fetch_add(1);
                if (i >= download_jobs.size()) break;
                auto& job = download_jobs[i];
                try {
                    auto csv = download_and_extract_archive(job, delete_zip);
                    if (csv) { std::lock_guard<std::mutex> lk(completed_mu); completed.push_back(job); }
                } catch (const std::exception& e) {
                    LOG_ERR("bulk download failed ", job.symbol, ": ", e.what());
                }
            }
        });
    }
    for (auto& t : downloaders) t.join();

    if (!import_to_bins) { LOG_WARN("[BULK IMPORT] disabled"); return; }
    int imported = 0;
    for (auto& job : completed) {
        try {
            std::vector<std::chrono::sys_days> needed;
            if (BULK_SKIP_IMPORT_IF_DAILY_BINS_HAVE_1440_ROWS) {
                for (auto d : bulk_job_expected_days(job, start_date, end_date)) if (!daily_bin_complete(job.market, job.symbol, d)) needed.push_back(d);
                if (needed.empty()) continue;
            } else {
                needed = bulk_job_expected_days(job, start_date, end_date);
            }
            imported += import_bulk_csv_to_daily_bins(job, start_date, end_date, needed);
        } catch (const std::exception& e) {
            LOG_ERR("bulk import failed ", job.csv_path.string(), ": ", e.what());
        }
    }
    LOG_INFO("[BULK IMPORT] done row_write_attempts=", imported);
}

// =============================================================================
// LIVE MULTIPLEX STREAMS + RECONNECT GAP FILL
// =============================================================================

static std::vector<std::vector<std::string>> chunk_symbols(const std::vector<std::string>& symbols, int size = MAX_SYMBOLS_PER_STREAM) {
    auto clean = clean_symbol_list(symbols);
    std::vector<std::vector<std::string>> out;
    for (size_t i=0; i<clean.size(); i += size) out.emplace_back(clean.begin()+i, clean.begin()+std::min(clean.size(), i+size));
    return out;
}
static std::string ws_url_streams(const std::vector<std::string>& symbols) {
    std::vector<std::string> streams;
    for (auto& s : symbols) streams.push_back(lower(s) + "@kline_" + INTERVAL);
    return join(streams, "/");
}
struct ParsedWsBase { std::string host; std::string port; std::string target_prefix; };
static ParsedWsBase parse_ws_base(const std::string& base) {
    std::regex re(R"(^wss://([^/:]+)(?::([0-9]+))?(/.*)$)");
    std::smatch m;
    if (!std::regex_match(base, m, re)) throw std::runtime_error("bad ws base: " + base);
    return {m[1], m[2].matched ? m[2].str() : "443", m[3]};
}
static std::string ws_target(const std::string& ws_base, const std::vector<std::string>& symbols) {
    auto p = parse_ws_base(ws_base);
    return p.target_prefix + ws_url_streams(symbols);
}

static std::pair<std::string,int> gap_fill_symbol(Market market, const std::string& symbol, int64_t start_ms, int64_t end_ms) {
    if (start_ms > end_ms) return {upper(symbol), 0};
    auto rows = fetch_rest_klines_range(market, symbol, start_ms, end_ms);
    int written = write_klines_daily(market, symbol, rows, WriteMode::dedupe_sort, "stream_gap_fill");
    LOG_INFO("[GAP ", market_name(market), " ", symbol, "] ", ms_to_utc_string(start_ms), " -> ", ms_to_utc_string(end_ms), " fetched=", rows.size(), " wrote=", written);
    return {upper(symbol), static_cast<int>(rows.size())};
}
static void gap_fill_after_reconnect(
    Market market,
    const std::vector<std::string>& symbols,
    int64_t gap_since_ms,
    std::unordered_map<std::string,int64_t>& last_seen_open_ms
) {
    auto clean = clean_symbol_list(symbols);
    if (clean.empty()) {
        LOG_INFO("[GAP ", market_name(market), "] no symbols");
        return;
    }

    int64_t now_ms = utc_now_ms();
    int64_t end_ms = last_closed_minute_open_ms(now_ms);

    // Do NOT subtract one minute globally.
    // The old "- INTERVAL_MS" caused constant rows=1 refetches.
    int64_t fallback_start_ms = floor_minute_ms(gap_since_ms);

    if (fallback_start_ms < 0) fallback_start_ms = 0;

    std::vector<std::tuple<std::string,int64_t,int64_t>> jobs;
    jobs.reserve(clean.size());

    for (auto& sym : clean) {
        sym = upper(sym);

        std::optional<int64_t> mem_latest;
        auto it = last_seen_open_ms.find(sym);
        if (it != last_seen_open_ms.end()) {
            mem_latest = it->second;
        }

        int64_t start_ms = fallback_start_ms;

        // If we already saw/saved this symbol's latest candle in live mode,
        // start from the next candle after that.
        if (mem_latest) {
            start_ms = *mem_latest + INTERVAL_MS;
        }

        start_ms = floor_minute_ms(start_ms);

        if (start_ms <= end_ms) {
            int64_t rows_needed = ((end_ms - start_ms) / INTERVAL_MS) + 1;

            jobs.emplace_back(sym, start_ms, end_ms);

            LOG_INFO("[GAP CHECK ", market_name(market), " ", sym,
                     "] QUEUE start=", ms_to_utc_string(start_ms),
                     " end=", ms_to_utc_string(end_ms),
                     " rows=", rows_needed,
                     " mem_latest=",
                     (mem_latest ? ms_to_utc_string(*mem_latest) : std::string("none")));
        } else {
            LOG_DBG("[GAP CHECK ", market_name(market), " ", sym,
                    "] SKIP already up to date start=", ms_to_utc_string(start_ms),
                    " end=", ms_to_utc_string(end_ms),
                    " mem_latest=",
                    (mem_latest ? ms_to_utc_string(*mem_latest) : std::string("none")));
        }
    }

    if (jobs.empty()) {
        LOG_INFO("[GAP ", market_name(market), "] no symbol gaps after latest-seen check");
        return;
    }

    LOG_INFO("[GAP ", market_name(market), "] REST backfilling symbols=", jobs.size(),
             " max_weight_per_minute=", REST_MAX_WEIGHT_PER_MINUTE);

    std::atomic<size_t> idx{0};
    std::mutex mu;
    std::vector<std::thread> workers;

    int n = std::min<int>(REST_SYMBOL_THREADS, jobs.size());

    for (int w = 0; w < n; ++w) {
        workers.emplace_back([&] {
            for (;;) {
                size_t i = idx.fetch_add(1);
                if (i >= jobs.size()) break;

                auto [sym, st, en] = jobs[i];

                try {
                    int64_t rows_needed = ((en - st) / INTERVAL_MS) + 1;
                    int max_chunks = static_cast<int>(
                        (rows_needed + rest_limit(market) - 1) / rest_limit(market)
                    );
                    max_chunks = std::max(1, max_chunks);

                    auto rows = fetch_rest_klines_range(market, sym, st, en, max_chunks);

                    write_klines_daily(
                        market,
                        sym,
                        rows,
                        WriteMode::dedupe_sort,
                        "stream_gap_fill"
                    );

                    if (!rows.empty()) {
                        int64_t latest = std::max_element(
                            rows.begin(),
                            rows.end(),
                            [](auto& a, auto& b) {
                                return a.open_time < b.open_time;
                            }
                        )->open_time;

                        std::lock_guard<std::mutex> lk(mu);
                        last_seen_open_ms[sym] = std::max(last_seen_open_ms[sym], latest);
                    }
                } catch (const std::exception& e) {
                    LOG_ERR("gap fill failed ", market_name(market), " ", sym, ": ", e.what());
                }
            }
        });
    }

    for (auto& t : workers) t.join();
}


static std::atomic<bool> stop_requested{false};

// Never-close mode:
// Some launchers/consoles can deliver SIGINT/SIGTERM even when you did not
// intentionally request a stop. In this build those signals are ignored so the
// live collector keeps reconnecting/running until the process is forcibly killed
// or the machine shuts down.
static constexpr bool STOP_ON_OS_SIGNAL = false;

static void handle_signal_stop(int) {
    if constexpr (STOP_ON_OS_SIGNAL) {
        stop_requested.store(true);
    }
}

static void live_delay_csv_writer_loop(std::shared_ptr<std::atomic<bool>> writer_stop) {
    // LOG_INFO("[LIVE DELAY CSV] batch writer started path=", LIVE_DELAY_CSV_PATH.string(),
    //          " flush_second=", LIVE_DELAY_CSV_FLUSH_SECOND,
    //          " force_flush_rows=", LIVE_DELAY_CSV_BATCH_FORCE_FLUSH_ROWS);

    while (!stop_requested.load() && !(writer_stop && writer_stop->load())) {
        int64_t flush_us = next_delay_csv_flush_us(LIVE_DELAY_CSV_FLUSH_SECOND);
        auto flush_tp = std::chrono::system_clock::time_point{std::chrono::microseconds{flush_us}};

        std::vector<LiveDelayRecord> batch;
        {
            std::unique_lock<std::mutex> lk(live_delay_pending_mu);
            live_delay_pending_cv.wait_until(lk, flush_tp, [&] {
                return stop_requested.load() ||
                       (writer_stop && writer_stop->load()) ||
                       live_delay_pending_records.size() >= LIVE_DELAY_CSV_BATCH_FORCE_FLUSH_ROWS;
            });

            if (!live_delay_pending_records.empty()) {
                batch.swap(live_delay_pending_records);
            }
        }

        if (!batch.empty()) {
            append_live_delay_csv_batch(batch);
            LOG_INFO("[LIVE DELAY CSV] flushed rows=", batch.size());
        }
    }

    // Final flush when this live run is shutting down/restarting.
    std::vector<LiveDelayRecord> final_batch;
    {
        std::lock_guard<std::mutex> lk(live_delay_pending_mu);
        final_batch.swap(live_delay_pending_records);
    }

    if (!final_batch.empty()) {
        append_live_delay_csv_batch(final_batch);
        LOG_INFO("[LIVE DELAY CSV] final flush rows=", final_batch.size());
    }

    LOG_INFO("[LIVE DELAY CSV] batch writer stopped");
}

// =============================================================================
// LIVE MESSAGE PROCESSING PIPELINE
// =============================================================================

struct LiveStreamState {
    std::mutex mu;
    std::unordered_map<std::string, int64_t> last_seen_open_ms;
};

struct LiveRawMessageJob {
    Market market;
    std::string raw_json;
    int64_t received_us = 0;
    std::shared_ptr<LiveStreamState> stream_state;
};

static void update_last_seen_open_from_worker(const std::shared_ptr<LiveStreamState>& state, const std::string& symbol, int64_t open_time_ms);

struct LiveBinWriteJob {
    Market market;
    Kline row;
    int64_t received_us = 0;
    int64_t received_ms = 0;
    int64_t scheduled_close_ms = 0;
    int64_t scheduled_close_us = 0;
    int64_t exchange_event_ms = 0;
    int64_t exchange_event_us = 0;
    std::shared_ptr<LiveStreamState> stream_state;
};

class BlockingLiveBinWriteQueue {
    std::mutex mu_;
    std::condition_variable cv_not_empty_;
    std::condition_variable cv_not_full_;

    // Hot fill buffer: processing workers append parsed closed klines here.
    std::vector<LiveBinWriteJob> filling_;

    // Ready batches: writer threads pop whole batches from here and write them.
    std::queue<std::vector<LiveBinWriteJob>> ready_;

    bool shutdown_ = false;

    void rotate_filling_to_ready_locked() {
        if (filling_.empty()) return;
        ready_.push(std::move(filling_));
        filling_ = std::vector<LiveBinWriteJob>{};
        filling_.reserve(std::min<size_t>(LIVE_BIN_BATCH_FORCE_FLUSH_ROWS, 16'384));
    }

public:
    BlockingLiveBinWriteQueue() {
        filling_.reserve(std::min<size_t>(LIVE_BIN_BATCH_FORCE_FLUSH_ROWS, 16'384));
    }

    BlockingLiveBinWriteQueue(const BlockingLiveBinWriteQueue&) = delete;
    BlockingLiveBinWriteQueue& operator=(const BlockingLiveBinWriteQueue&) = delete;

    bool push(LiveBinWriteJob job) {
        std::unique_lock<std::mutex> lk(mu_);

        // Backpressure only when too many complete batches are already waiting.
        // This keeps memory bounded but still lets the current fill batch accept work.
        cv_not_full_.wait(lk, [&] {
            return shutdown_ || ready_.size() < LIVE_BIN_READY_BATCH_QUEUE_MAX;
        });
        if (shutdown_) return false;

        filling_.push_back(std::move(job));

        if (filling_.size() >= LIVE_BIN_BATCH_FORCE_FLUSH_ROWS) {
            rotate_filling_to_ready_locked();
            lk.unlock();
            cv_not_empty_.notify_one();
        }

        return true;
    }

    std::vector<LiveBinWriteJob> pop_batch_for(std::chrono::microseconds wait_for) {
        std::unique_lock<std::mutex> lk(mu_);

        // Wait for either a ready batch, a forced flush, shutdown, or timeout.
        bool woke = cv_not_empty_.wait_for(lk, wait_for, [&] {
            return shutdown_ || !ready_.empty() || filling_.size() >= LIVE_BIN_BATCH_FORCE_FLUSH_ROWS;
        });

        if (ready_.empty() && !filling_.empty()) {
            // Time-based flush: writer thread turns current fill buffer into a ready batch.
            // Producers can immediately start filling a fresh vector while this batch is written.
            rotate_filling_to_ready_locked();
        }

        std::vector<LiveBinWriteJob> batch;
        if (!ready_.empty()) {
            batch = std::move(ready_.front());
            ready_.pop();
            cv_not_full_.notify_all();
        }

        (void)woke;
        return batch;
    }

    bool is_shutdown_and_empty() const {
        auto* self = const_cast<BlockingLiveBinWriteQueue*>(this);
        std::lock_guard<std::mutex> lk(self->mu_);
        return self->shutdown_ && self->ready_.empty() && self->filling_.empty();
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            shutdown_ = true;
            rotate_filling_to_ready_locked();
        }
        cv_not_empty_.notify_all();
        cv_not_full_.notify_all();
    }

    size_t ready_batch_count() const {
        auto* self = const_cast<BlockingLiveBinWriteQueue*>(this);
        std::lock_guard<std::mutex> lk(self->mu_);
        return self->ready_.size();
    }

    size_t filling_size() const {
        auto* self = const_cast<BlockingLiveBinWriteQueue*>(this);
        std::lock_guard<std::mutex> lk(self->mu_);
        return self->filling_.size();
    }
};

static std::shared_ptr<BlockingLiveBinWriteQueue> live_bin_queue;

static void build_and_cache_delay_record_after_save(
    const LiveBinWriteJob& job,
    int rows_written,
    int64_t save_started_us,
    int64_t saved_us
) {
    if (!LIVE_DELAY_LOG_ENABLED) return;

    LiveDelayRecord delay_record;
    delay_record.market = market_name(job.market);
    delay_record.symbol = job.row.symbol;
    delay_record.kline_open_time_ms = job.row.open_time;
    delay_record.kline_close_time_ms = job.row.close_time;
    delay_record.scheduled_close_ms = job.scheduled_close_ms;
    delay_record.scheduled_close_us = job.scheduled_close_us;
    delay_record.exchange_event_ms = job.exchange_event_ms;
    delay_record.exchange_event_us = job.exchange_event_us;
    delay_record.received_ms = job.received_ms;
    delay_record.received_us = job.received_us;
    delay_record.save_started_ms = save_started_us / 1000;
    delay_record.save_started_us = save_started_us;
    delay_record.saved_ms = saved_us / 1000;
    delay_record.saved_us = saved_us;

    delay_record.scheduled_close_to_receive_us = job.received_us - job.scheduled_close_us;
    delay_record.exchange_event_to_receive_us =
        job.exchange_event_us > 0 ? job.received_us - job.exchange_event_us : 0;
    delay_record.receive_to_saved_us = saved_us - job.received_us;
    delay_record.write_call_us = saved_us - save_started_us;
    delay_record.scheduled_close_to_saved_us = saved_us - job.scheduled_close_us;

    delay_record.scheduled_close_to_receive_ms = delay_record.scheduled_close_to_receive_us / 1000;
    delay_record.exchange_event_to_receive_ms = delay_record.exchange_event_to_receive_us / 1000;
    delay_record.receive_to_saved_ms = delay_record.receive_to_saved_us / 1000;
    delay_record.write_call_ms = delay_record.write_call_us / 1000;
    delay_record.scheduled_close_to_saved_ms = delay_record.scheduled_close_to_saved_us / 1000;

    delay_record.rows_written = rows_written;

    if (LIVE_DELAY_BATCH_CSV_WRITES) {
        cache_live_delay_record(std::move(delay_record));
    } else {
        append_live_delay_csv_batch(std::vector<LiveDelayRecord>{delay_record});
    }
}

static void write_live_bin_batch(std::vector<LiveBinWriteJob>& batch) {
    if (batch.empty()) return;

    struct GroupKey {
        Market market;
        std::string symbol;
        std::chrono::sys_days day;

        bool operator<(const GroupKey& other) const {
            if (market != other.market) return static_cast<int>(market) < static_cast<int>(other.market);
            if (symbol != other.symbol) return symbol < other.symbol;
            return day < other.day;
        }
    };

    std::map<GroupKey, std::vector<size_t>> groups;
    for (size_t i = 0; i < batch.size(); ++i) {
        const auto& job = batch[i];
        if (!is_valid_binance_symbol_name(job.row.symbol)) continue;
        groups[{job.market, upper(job.row.symbol), ms_to_day(job.row.open_time)}].push_back(i);
    }

    for (const auto& [key, indexes] : groups) {
        int64_t save_started_us = utc_now_us();
        int rows_written = 0;
        try {
            auto path = daily_bin_path(key.market, key.symbol, key.day);
            std::vector<KlineRecord> records;
            records.reserve(indexes.size());
            for (size_t idx : indexes) {
                Kline fixed = batch[idx].row;
                fixed.symbol = key.symbol;
                fixed.maker_base_vol = fixed.volume - fixed.taker_base_vol;
                fixed.maker_quote_vol = fixed.quote_volume - fixed.taker_quote_vol;
                records.push_back(to_record(fixed));
            }

            FileLockGuard lock(path);
            append_records(path, records);
            rows_written = static_cast<int>(records.size());
        } catch (const std::exception& e) {
            LOG_ERR("[LIVE BIN BATCH] failed ", market_name(key.market), " ", key.symbol,
                    " day=", date_to_string(key.day), " rows=", indexes.size(), ": ", e.what());
        }

        int64_t saved_us = utc_now_us();
        for (size_t idx : indexes) {
            build_and_cache_delay_record_after_save(batch[idx], rows_written > 0 ? 1 : 0, save_started_us, saved_us);
            if (rows_written > 0) {
                update_last_seen_open_from_worker(batch[idx].stream_state, batch[idx].row.symbol, batch[idx].row.open_time);
            }
        }
    }
}

static void live_bin_writer_loop(int writer_id, std::shared_ptr<BlockingLiveBinWriteQueue> bin_queue) {
    LOG_INFO("[LIVE BIN WRITER ", writer_id, "] started batch_flush_us=", LIVE_BIN_BATCH_FLUSH_US,
             " force_flush_rows=", LIVE_BIN_BATCH_FORCE_FLUSH_ROWS,
             " ready_batch_queue_max=", LIVE_BIN_READY_BATCH_QUEUE_MAX);

    const auto flush_wait = std::chrono::microseconds(std::max(1, LIVE_BIN_BATCH_FLUSH_US));
    while (!stop_requested.load()) {
        auto batch = bin_queue->pop_batch_for(flush_wait);
        if (!batch.empty()) write_live_bin_batch(batch);
        if (bin_queue->is_shutdown_and_empty()) break;
    }

    for (;;) {
        auto batch = bin_queue->pop_batch_for(std::chrono::microseconds(0));
        if (batch.empty()) break;
        write_live_bin_batch(batch);
    }

    LOG_INFO("[LIVE BIN WRITER ", writer_id, "] stopped");
}

class BlockingLiveQueue {
    std::mutex mu_;
    std::condition_variable cv_not_empty_;
    std::condition_variable cv_not_full_;
    std::queue<LiveRawMessageJob> q_;
    size_t max_size_ = 0;
    bool shutdown_ = false;

public:
    explicit BlockingLiveQueue(size_t max_size) : max_size_(std::max<size_t>(1, max_size)) {}

    BlockingLiveQueue(const BlockingLiveQueue&) = delete;
    BlockingLiveQueue& operator=(const BlockingLiveQueue&) = delete;

    bool push(LiveRawMessageJob job) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_full_.wait(lk, [&]{ return shutdown_ || q_.size() < max_size_; });
        if (shutdown_) return false;
        q_.push(std::move(job));
        cv_not_empty_.notify_one();
        return true;
    }

    bool pop(LiveRawMessageJob& out) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_empty_.wait(lk, [&]{ return shutdown_ || !q_.empty(); });
        if (q_.empty()) return false;
        out = std::move(q_.front());
        q_.pop();
        cv_not_full_.notify_one();
        return true;
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            shutdown_ = true;
        }
        cv_not_empty_.notify_all();
        cv_not_full_.notify_all();
    }

    size_t size() const {
        // Approximate queue size for logging/debugging only.
        auto* self = const_cast<BlockingLiveQueue*>(this);
        std::lock_guard<std::mutex> lk(self->mu_);
        return self->q_.size();
    }
};

static unsigned int resolved_live_process_workers() {
    if (LIVE_PROCESS_WORKERS > 0) return std::max(1u, LIVE_PROCESS_WORKERS);
    unsigned int cores = std::thread::hardware_concurrency();
    if (cores == 0) return 2u;
    if (cores <= 2) return 1u;
    return cores - 2u;
}

static void merge_last_seen_snapshot_back(
    const std::shared_ptr<LiveStreamState>& state,
    const std::unordered_map<std::string,int64_t>& snapshot
) {
    if (!state) return;
    std::lock_guard<std::mutex> lk(state->mu);
    for (const auto& [sym, open_ms] : snapshot) {
        auto& slot = state->last_seen_open_ms[sym];
        slot = std::max(slot, open_ms);
    }
}

static std::unordered_map<std::string,int64_t> copy_last_seen_snapshot(
    const std::shared_ptr<LiveStreamState>& state
) {
    if (!state) return {};
    std::lock_guard<std::mutex> lk(state->mu);
    return state->last_seen_open_ms;
}

static void update_last_seen_open_from_worker(
    const std::shared_ptr<LiveStreamState>& state,
    const std::string& symbol,
    int64_t open_time_ms
) {
    if (!state) return;
    std::lock_guard<std::mutex> lk(state->mu);
    auto& slot = state->last_seen_open_ms[upper(symbol)];
    slot = std::max(slot, open_time_ms);
}

static void process_live_raw_message_job(LiveRawMessageJob&& job) {
    json msg;
    try {
        msg = json::parse(job.raw_json);
    } catch (...) {
        return;
    }

    json data = msg.contains("data") ? msg["data"] : msg;
    if (!data.is_object()) return;
    if (!data.contains("k") || !data["k"].is_object()) return;

    auto& k = data["k"];
    if (!k.value("x", false)) return; // closed candles only

    int64_t received_us = job.received_us;
    int64_t received_ms = received_us / 1000;

    // Binance websocket event time, field "E", if present.
    int64_t exchange_event_ms = 0;
    try {
        if (data.contains("E")) exchange_event_ms = parse_int64_safe(data["E"]);
    } catch (...) {
        exchange_event_ms = 0;
    }

    auto row = ws_kline_to_kline(k);
    if (!row) return;

    // For 1m klines, open_time is exactly on the minute.
    // The candle should be complete at open_time + 60,000ms.
    int64_t scheduled_close_ms = row->open_time + INTERVAL_MS;
    int64_t scheduled_close_us = scheduled_close_ms * 1000LL;
    int64_t exchange_event_us = exchange_event_ms > 0 ? exchange_event_ms * 1000LL : 0;

    LiveBinWriteJob bin_job;
    bin_job.market = job.market;
    bin_job.row = *row;
    bin_job.received_us = received_us;
    bin_job.received_ms = received_ms;
    bin_job.scheduled_close_ms = scheduled_close_ms;
    bin_job.scheduled_close_us = scheduled_close_us;
    bin_job.exchange_event_ms = exchange_event_ms;
    bin_job.exchange_event_us = exchange_event_us;
    bin_job.stream_state = job.stream_state;

    if (LIVE_BIN_BATCH_WRITES) {
        if (!live_bin_queue || !live_bin_queue->push(std::move(bin_job))) {
            throw std::runtime_error("live .bin write queue has shut down");
        }
    } else {
        std::vector<LiveBinWriteJob> one;
        one.push_back(std::move(bin_job));
        write_live_bin_batch(one);
    }
}

static void live_processing_worker_loop(int worker_id, std::shared_ptr<BlockingLiveQueue> queue) {
    LOG_INFO("[LIVE WORKER ", worker_id, "] started");
    LiveRawMessageJob job;
    while (queue->pop(job)) {
        try {
            process_live_raw_message_job(std::move(job));
        } catch (const std::exception& e) {
            LOG_ERR("[LIVE WORKER ", worker_id, "] job failed: ", e.what());
        } catch (...) {
            LOG_ERR("[LIVE WORKER ", worker_id, "] job failed with unknown exception");
        }
    }
    LOG_INFO("[LIVE WORKER ", worker_id, "] stopped");
}

static void live_multiplex_loop(Market market, std::vector<std::string> symbols, std::shared_ptr<BlockingLiveQueue> live_queue) {
    symbols = clean_symbol_list(symbols);
    if (symbols.empty()) return;
    std::string label = "LIVE " + market_name(market) + " " + symbols.front() + ".." + symbols.back() + " n=" + std::to_string(symbols.size());
    auto bases = market_ws_bases(market);
    size_t base_index = 0;
    double backoff = 1.0;
    bool first_connect = true;
    std::optional<int64_t> pending_gap_since_ms;
    auto stream_state = std::make_shared<LiveStreamState>();

    while (!stop_requested.load()) {
        int64_t connect_attempt_ms = utc_now_ms();
        int64_t connection_open_ms = 0;
        int64_t last_ws_message_ms = 0;
        try {
            auto base = bases[base_index % bases.size()];
            auto p = parse_ws_base(base);
            std::string target = ws_target(base, symbols);
            LOG_INFO("[", label, "] connecting base=", base);

            net::io_context ioc;
            ssl::context ctx{ssl::context::tlsv12_client};
            if (!curl_ca_bundle_path().empty()) ctx.load_verify_file(curl_ca_bundle_path());
            ctx.set_default_verify_paths();
            ctx.set_verify_mode(ssl::verify_peer);
            tcp::resolver resolver{ioc};
            websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws{ioc, ctx};
            if(!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), p.host.c_str())) {
                throw std::runtime_error("SNI setup failed");
            }
            auto const results = DNS_FORCE_IPV4 ? resolver.resolve(tcp::v4(), p.host, p.port)
                                           : resolver.resolve(p.host, p.port);
            beast::get_lowest_layer(ws).expires_after(std::chrono::seconds(WS_OPEN_TIMEOUT_S));
            beast::get_lowest_layer(ws).connect(results);
            ws.next_layer().handshake(ssl::stream_base::client);
            beast::get_lowest_layer(ws).expires_never();
            ws.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
            ws.set_option(websocket::stream_base::decorator([](websocket::request_type& req){
                req.set(beast::http::field::user_agent, "klines-only-collector-cpp/1.0");
            }));
            ws.handshake(p.host, target);
            connection_open_ms = utc_now_ms();
            last_ws_message_ms = connection_open_ms;
            LOG_INFO("[", label, "] connected at=", ms_to_utc_string(connection_open_ms));

            if (!first_connect) {
                int64_t gap_ms = pending_gap_since_ms.value_or(connection_open_ms);
                auto last_seen_snapshot = copy_last_seen_snapshot(stream_state);
                gap_fill_after_reconnect(market, symbols, gap_ms, last_seen_snapshot);
                merge_last_seen_snapshot_back(stream_state, last_seen_snapshot);
                pending_gap_since_ms.reset();
            }
            first_connect = false;
            backoff = 1.0;

            while (!stop_requested.load()) {
                beast::flat_buffer buffer;
                beast::get_lowest_layer(ws).expires_after(std::chrono::seconds(STREAM_RECV_TIMEOUT_S));
                ws.read(buffer);
                int64_t received_us = utc_now_us();
                last_ws_message_ms = received_us / 1000;
                auto raw = beast::buffers_to_string(buffer.data());

                // Keep the websocket thread light: detect server shutdown, then enqueue
                // the raw message. JSON parsing, kline conversion, .bin writes, and timing
                // CSV writes happen on the live worker pool using all available CPU cores.
                if (raw.find("serverShutdown") != std::string::npos || raw.find("!serverShutdown") != std::string::npos) {
                    pending_gap_since_ms = last_ws_message_ms;
                    throw std::runtime_error("serverShutdown event");
                }

                LiveRawMessageJob job;
                job.market = market;
                job.raw_json = std::move(raw);
                job.received_us = received_us;
                job.stream_state = stream_state;

                if (!live_queue->push(std::move(job))) {
                    throw std::runtime_error("live processing queue has shut down");
                }
            }
        } catch (const std::exception& e) {
            if (!pending_gap_since_ms) pending_gap_since_ms = last_ws_message_ms ? last_ws_message_ms : (connection_open_ms ? connection_open_ms : connect_attempt_ms);
            double sleep_s = backoff;
            LOG_WARN("[", label, "] disconnected/reconnect trigger: ", e.what(), "; gap_from=", ms_to_utc_string(*pending_gap_since_ms), "; reconnect_sleep=", sleep_s, "s");
            std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int>(sleep_s * 1000)));
            backoff = std::min(backoff * 2.0, static_cast<double>(STREAM_RECONNECT_MAX_BACKOFF_S));
            if (bases.size() > 1) base_index = (base_index + 1) % bases.size();
        }
    }
}

static void log_live_market_stream_plan(const std::string& market, size_t symbol_count, size_t connection_count) {
    if (symbol_count == 0) {
        LOG_WARN("[LIVE] ", market, " stream disabled: no symbols configured in its live/rest symbol list");
    } else {
        LOG_INFO("[LIVE] ", market, " stream enabled: symbols=", symbol_count,
                 " multiplex_connections=", connection_count,
                 " max_symbols_per_stream=", MAX_SYMBOLS_PER_STREAM);
    }
}

static void start_live_streams(const std::vector<std::string>& spot, const std::vector<std::string>& um, const std::vector<std::string>& cm) {
    auto spot_clean = clean_symbol_list(spot);
    auto um_clean = clean_symbol_list(um);
    auto cm_clean = clean_symbol_list(cm);

    auto spot_chunks = chunk_symbols(spot_clean);
    auto um_chunks = chunk_symbols(um_clean);
    auto cm_chunks = chunk_symbols(cm_clean);

    log_live_market_stream_plan("spot", spot_clean.size(), spot_chunks.size());
    log_live_market_stream_plan("um", um_clean.size(), um_chunks.size());
    log_live_market_stream_plan("cm", cm_clean.size(), cm_chunks.size());

    const size_t total_connections = spot_chunks.size() + um_chunks.size() + cm_chunks.size();

    if (total_connections == 0) {
        LOG_ERR("[LIVE] all live streams disabled because all live/rest symbol lists are empty. "
                "No websocket connections or processing workers will be started. "
                "The process will stay alive and re-check via the live supervisor.");

        // Return to the live supervisor. The supervisor already sleeps/retries and,
        // in RUN_MODE=all, prevents main() from falling back to PowerShell.
        return;
    }

    auto live_queue = std::make_shared<BlockingLiveQueue>(LIVE_QUEUE_MAX);
    live_bin_queue = std::make_shared<BlockingLiveBinWriteQueue>();
    unsigned int worker_count = resolved_live_process_workers();

    std::vector<std::thread> live_bin_writers;
    if (LIVE_BIN_BATCH_WRITES) {
        unsigned int bin_writer_count = std::max(1u, LIVE_BIN_WRITER_THREADS);
        live_bin_writers.reserve(bin_writer_count);
        for (unsigned int i = 0; i < bin_writer_count; ++i) {
            live_bin_writers.emplace_back(live_bin_writer_loop, static_cast<int>(i), live_bin_queue);
        }
    }

    std::shared_ptr<std::atomic<bool>> delay_csv_writer_stop = std::make_shared<std::atomic<bool>>(false);
    std::thread delay_csv_writer;
    if (LIVE_DELAY_LOG_ENABLED && LIVE_DELAY_BATCH_CSV_WRITES) {
        delay_csv_writer = std::thread(live_delay_csv_writer_loop, delay_csv_writer_stop);
    }

    std::vector<std::thread> processing_workers;
    processing_workers.reserve(worker_count);
    for (unsigned int i = 0; i < worker_count; ++i) {
        processing_workers.emplace_back(live_processing_worker_loop, static_cast<int>(i), live_queue);
    }

    std::vector<std::thread> stream_threads;
    stream_threads.reserve(total_connections);

    for (auto& c : spot_chunks) stream_threads.emplace_back(live_multiplex_loop, Market::spot, c, live_queue);
    for (auto& c : um_chunks) stream_threads.emplace_back(live_multiplex_loop, Market::um, c, live_queue);
    for (auto& c : cm_chunks) stream_threads.emplace_back(live_multiplex_loop, Market::cm, c, live_queue);

    LOG_INFO("[LIVE] started ", stream_threads.size(), " multiplex connections and ",
             worker_count, " processing workers queue_max=", LIVE_QUEUE_MAX,
             " bin_batch_us=", (LIVE_BIN_BATCH_WRITES ? LIVE_BIN_BATCH_FLUSH_US : 0),
             " bin_writers=", (LIVE_BIN_BATCH_WRITES ? std::max(1u, LIVE_BIN_WRITER_THREADS) : 0),
             " disabled_markets=",
             (spot_clean.empty() ? "spot " : ""),
             (um_clean.empty() ? "um " : ""),
             (cm_clean.empty() ? "cm" : ""));

    for (auto& t : stream_threads) t.join();

    // If all websocket threads ever exit, shut down the worker queue cleanly so
    // the live supervisor can decide whether to restart start_live_streams().
    live_queue->shutdown();
    for (auto& t : processing_workers) t.join();

    if (live_bin_queue) live_bin_queue->shutdown();
    for (auto& t : live_bin_writers) {
        if (t.joinable()) t.join();
    }
    live_bin_queue.reset();

    if (delay_csv_writer.joinable()) {
        delay_csv_writer_stop->store(true);
        live_delay_pending_cv.notify_one();
        delay_csv_writer.join();
    } else {
        // Non-batched mode: flush anything that may have been cached before exit.
        std::vector<LiveDelayRecord> final_batch;
        {
            std::lock_guard<std::mutex> lk(live_delay_pending_mu);
            final_batch.swap(live_delay_pending_records);
        }
        append_live_delay_csv_batch(final_batch);
    }
}

// =============================================================================
// VALIDATION
// =============================================================================

static std::vector<Kline> read_daily_bin(Market market, const std::string& symbol, std::chrono::sys_days d) {
    auto path = daily_bin_path(market, symbol, d);
    FileLockGuard lock(path);
    auto records = read_bin_file(path);
    std::vector<Kline> rows;
    rows.reserve(records.size());
    for (auto& r : records) rows.push_back(from_record(r));
    return rows;
}
static std::map<std::string,std::string> validate_daily_bin(Market market, const std::string& symbol, std::chrono::sys_days d) {
    std::map<std::string,std::string> result;
    auto path = daily_bin_path(market, symbol, d);
    result["market"] = market_name(market);
    result["symbol"] = upper(symbol);
    result["date"] = date_to_string(d);
    result["path"] = path.string();
    if (!fs::exists(path)) { result["exists"] = "false"; result["rows"] = "0"; return result; }
    result["exists"] = "true";
    auto rows = read_daily_bin(market, symbol, d);
    result["rows"] = std::to_string(rows.size());
    std::set<int64_t> opens;
    for (auto& k : rows) opens.insert(k.open_time);
    result["unique_open_times"] = std::to_string(opens.size());
    result["duplicates"] = std::to_string(static_cast<int>(rows.size()) - static_cast<int>(opens.size()));
    result["complete_1440"] = (opens.size() == 1440 ? "true" : "false");
    return result;
}

// =============================================================================
// RUN MODE SUPERVISION
// =============================================================================

static void sleep_interruptible_ms(int64_t total_ms) {
    const int64_t step_ms = 250;
    int64_t slept = 0;
    while (!stop_requested.load() && slept < total_ms) {
        int64_t chunk = std::min<int64_t>(step_ms, total_ms - slept);
        std::this_thread::sleep_for(std::chrono::milliseconds(chunk));
        slept += chunk;
    }
}

static size_t live_symbol_total(const SymbolSets& symbols) {
    return clean_symbol_list(symbols.spot_live_rest).size()
         + clean_symbol_list(symbols.um_live_rest).size()
         + clean_symbol_list(symbols.cm_live_rest).size();
}

static void live_supervisor_forever(SymbolSets symbols) {
    int restart_count = 0;

    while (!stop_requested.load()) {
        size_t total_live_symbols = live_symbol_total(symbols);

        if (total_live_symbols == 0) {
            LOG_ERR("[THREAD LIVE] no live/rest symbols are configured. The program will NOT close; "
                    "it will keep checking every ", RUN_ALL_MONITOR_SLEEP_S,
                    "s. Add symbols to SYMBOLS_*_LIVE_REST or enable AUTO_FIND_SYMBOLS_*.");
            sleep_interruptible_ms(static_cast<int64_t>(RUN_ALL_MONITOR_SLEEP_S * 1000));
            continue;
        }

        try {
            LOG_INFO("[THREAD LIVE] supervisor start attempt=", restart_count + 1,
                     " live_symbols=", total_live_symbols,
                     " spot=", clean_symbol_list(symbols.spot_live_rest).size(),
                     " um=", clean_symbol_list(symbols.um_live_rest).size(),
                     " cm=", clean_symbol_list(symbols.cm_live_rest).size());

            // start_live_streams joins its worker threads. Under normal operation it never returns.
            // If it does return for any reason, the supervisor below restarts it instead of allowing
            // main() to return to PowerShell.
            start_live_streams(symbols.spot_live_rest, symbols.um_live_rest, symbols.cm_live_rest);

            LOG_ERR("[THREAD LIVE] start_live_streams returned. This should not happen in continuous mode.");
        } catch (const std::exception& e) {
            LOG_ERR("[THREAD LIVE] crashed: ", e.what());
        } catch (...) {
            LOG_ERR("[THREAD LIVE] crashed with unknown exception");
        }

        ++restart_count;

        if (stop_requested.load()) break;

        if (!RUN_ALL_RESTART_LIVE_IF_THREAD_EXITS) {
            LOG_ERR("[THREAD LIVE] RUN_ALL_RESTART_LIVE_IF_THREAD_EXITS=false. "
                    "Keeping the process alive anyway; set it true to automatically restart live streams.");
            while (!stop_requested.load()) {
                sleep_interruptible_ms(static_cast<int64_t>(RUN_ALL_MONITOR_SLEEP_S * 1000));
            }
            break;
        }

        LOG_WARN("[THREAD LIVE] restarting live supervisor after exit count=", restart_count,
                 " sleep_s=", RUN_ALL_MONITOR_SLEEP_S);
        sleep_interruptible_ms(static_cast<int64_t>(RUN_ALL_MONITOR_SLEEP_S * 1000));
    }

    LOG_WARN("[THREAD LIVE] supervisor stopped because stop_requested=true. "
             "In never-stop mode this should only happen if STOP_ON_OS_SIGNAL=true or another code path sets stop_requested.");
}

static void run_all_three_threads(const SymbolSets& symbols) {
    LOG_INFO("[ALL] starting live forever in the main thread; REST72 and BULK run in a background coordinator");

    SymbolSets symbols_copy = symbols;

    std::thread rest_bulk_coordinator([symbols_copy]() mutable {
        sleep_interruptible_ms(static_cast<int64_t>(RUN_ALL_START_REST_AND_BULK_AFTER_LIVE_S * 1000));
        if (stop_requested.load()) return;

        LOG_INFO("[ALL] starting REST72 and BULK worker threads");

        std::thread rest_thread([symbols_copy]{
            try {
                update_last_72h_rest_all_symbols(
                    symbols_copy.spot_live_rest,
                    symbols_copy.um_live_rest,
                    symbols_copy.cm_live_rest
                );
                LOG_INFO("[THREAD REST72] finished normally");
            } catch (const std::exception& e) {
                LOG_ERR("[THREAD REST72] crashed: ", e.what());
            } catch (...) {
                LOG_ERR("[THREAD REST72] crashed with unknown exception");
            }
        });

        std::thread bulk_thread([symbols_copy]{
            try {
                download_bulk_historical_klines(
                    symbols_copy.spot_bulk,
                    symbols_copy.um_bulk,
                    symbols_copy.cm_bulk,
                    BULK_START_DATE,
                    BULK_END_DATE,
                    BULK_IMPORT_TO_BINS,
                    BULK_DELETE_ZIPS_AFTER_EXTRACT
                );
                LOG_INFO("[THREAD BULK] finished normally");
            } catch (const std::exception& e) {
                LOG_ERR("[THREAD BULK] crashed: ", e.what());
            } catch (...) {
                LOG_ERR("[THREAD BULK] crashed with unknown exception");
            }
        });

        rest_thread.join();
        bulk_thread.join();

        LOG_INFO("[ALL] REST72 and BULK completed. LIVE remains running forever in the main thread. "
                 "Only Ctrl+C/terminate should stop collection.");
    });

    // The live supervisor is intentionally run on the main thread so main_impl() cannot return
    // just because REST72/BULK finished or because a live worker returned unexpectedly.
    rest_bulk_coordinator.detach();
    live_supervisor_forever(symbols_copy);
}

static void main_impl() {
    fs::create_directories(LOG_DIR);
    fs::create_directories(SYMBOLS_OUTPUT_DIR);
    logger.open(LOG_PATH);
    std::signal(SIGINT, handle_signal_stop);
    std::signal(SIGTERM, handle_signal_stop);
    if constexpr (!STOP_ON_OS_SIGNAL) {
        LOG_WARN("[SIGNALS] SIGINT/SIGTERM are ignored in this never-stop build. "
                 "Use Task Manager, Stop-Process, closing the terminal, or kill the process to end it.");
    }

    if (LIVE_DELAY_LOG_ENABLED) {
        fs::create_directories(LIVE_DELAY_CSV_PATH.parent_path());

        std::error_code ec;
        bool need_header = !fs::exists(LIVE_DELAY_CSV_PATH, ec) ||
                           fs::file_size(LIVE_DELAY_CSV_PATH, ec) == 0;

        if (need_header) {
            std::ofstream f(LIVE_DELAY_CSV_PATH, std::ios::app);
            if (f.is_open()) write_live_delay_csv_header(f);
        }

        LOG_INFO("[LIVE DELAY CSV] writing to ", LIVE_DELAY_CSV_PATH.string());
    }

    curl_global_init(CURL_GLOBAL_DEFAULT);
    LOG_INFO("Starting C++ Binance klines collector run_mode=", static_cast<int>(RUN_MODE));
    LOG_INFO("[REST RATE LIMIT] enabled=", (REST_RATE_LIMIT_ENABLED ? "true" : "false"),
             " max_weight_per_minute=", REST_MAX_WEIGHT_PER_MINUTE,
             " spread_requests=", (REST_RATE_LIMIT_SPREAD_REQUESTS ? "true" : "false"));
    LOG_INFO("[REST LOOKBACK] rows=", REST_LOOKBACK_ROWS,
             " max_chunks_per_symbol=", REST_MAX_CHUNKS_PER_SYMBOL,
             " rest_limit=", rest_limit(Market::um),
             " only_missing_from_disk=", (REST_LOOKBACK_ONLY_MISSING_FROM_DISK ? "true" : "false"));

    auto symbols = resolve_symbol_sets();

    switch (RUN_MODE) {
        case RunMode::symbols:
            LOG_INFO("[SYMBOLS] done");
            break;
        case RunMode::bulk:
            download_bulk_historical_klines(symbols.spot_bulk, symbols.um_bulk, symbols.cm_bulk, BULK_START_DATE, BULK_END_DATE, BULK_IMPORT_TO_BINS, BULK_DELETE_ZIPS_AFTER_EXTRACT);
            break;
        case RunMode::rest72:
            update_last_72h_rest_all_symbols(symbols.spot_live_rest, symbols.um_live_rest, symbols.cm_live_rest);
            break;
        case RunMode::live:
            start_live_streams(symbols.spot_live_rest, symbols.um_live_rest, symbols.cm_live_rest);
            break;
        case RunMode::all:
            run_all_three_threads(symbols);
            break;
        case RunMode::validate: {
            auto d = VALIDATE_DATE.value_or(ms_to_day(utc_now_ms()));
            std::vector<std::tuple<Market,std::string>> jobs;
            for (auto& s : symbols.spot_live_rest) jobs.push_back({Market::spot, s});
            for (auto& s : symbols.um_live_rest) jobs.push_back({Market::um, s});
            for (auto& s : symbols.cm_live_rest) jobs.push_back({Market::cm, s});
            for (auto& [m,s] : jobs) {
                auto r = validate_daily_bin(m, s, d);
                LOG_INFO("[VALIDATE] ", r["market"], " ", r["symbol"], " date=", r["date"], " exists=", r["exists"], " rows=", r["rows"], " complete_1440=", r["complete_1440"]);
            }
            break;
        }
    }
    curl_global_cleanup();
}

int main() {
    try {
        main_impl();
        return 0;
    } catch (const std::exception& e) {
        LOG_ERR("fatal: ", e.what());
        return 1;
    }
}

