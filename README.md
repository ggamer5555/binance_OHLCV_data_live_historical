# Binance C++20 Kline Collector + Local Dashboard

A C++20 toolkit for collecting Binance 1-minute OHLCV/kline market data, storing it in compact daily binary files, recording live stream latency metrics, and visualizing the results in a local browser dashboard.



<img width="1321" height="814" alt="btc1" src="https://github.com/user-attachments/assets/73308d9b-43e8-4eee-affb-f7a28d97f5a9" />


<img width="1294" height="776" alt="image" src="https://github.com/user-attachments/assets/3b10c26a-c0e9-4e91-a5b1-d540eed29bed" />



The repository contains two main programs:

| File | Purpose |
|---|---|
| `binance_klines_collector_cpp20.cpp` | Collects Binance spot, USD-M futures, and COIN-M futures 1-minute klines from bulk archives, REST APIs, and live WebSocket streams. |
| `binance_plot_dashboard_server.cpp` | Runs a lightweight local HTTP dashboard that reads the collector output and plots OHLCV candles plus live timing/latency data. |

---

## What this project does

This project is designed to build a local, appendable market-data store for Binance 1-minute klines.

The collector can:

- Download historical kline CSV archives from Binance Vision.
- Import historical CSV data into daily `.bin` files.
- Fetch recent candles from Binance REST endpoints.
- Subscribe to Binance WebSocket kline streams for live updates.
- Save only closed 1-minute candles.
- Deduplicate and sort historical/REST data by timestamp.
- Append live candles efficiently using a batched writer.
- Track live stream timing in microseconds, including:
  - scheduled candle close → local receive time
  - exchange event time → local receive time
  - receive time → file saved time
  - total scheduled close → saved time
- Recover gaps after WebSocket reconnects by backfilling missing candles through REST.
- Validate daily binary files for row counts, duplicates, and expected 1,440 candles per day.

The dashboard can:

- Discover available markets and symbols from the collector output directory.
- Read the same 128-byte binary kline record format used by the collector.
- Plot recent OHLCV data as candlesticks or close-price lines.
- Display volume bars.
- Show missing-row/gap information.
- Plot live latency/timing metrics from the CSV log.
- Auto-refresh when new data arrives.
- Serve everything through a small built-in local HTTP server.

---

## Supported markets

The code is structured around three Binance market groups:

| Market code | Meaning |
|---|---|
| `spot` | Binance spot markets |
| `um` | USD-M futures |
| `cm` | COIN-M futures |

---

## Data format

The collector writes one binary file per market, symbol, interval, and UTC day.

Default output layout:

```text
data_dump/
  bulk_csv/
  daily_bin/
    spot/
      BTCUSDT/
        BTCUSDT_spot_1m_2026-05-28.bin
    um/
      BTCUSDT/
        BTCUSDT_um_1m_2026-05-28.bin
    cm/
      BTCUSD_PERP/
        BTCUSD_PERP_cm_1m_2026-05-28.bin
  logs/
    klines_collector_cpp.log
    live_symbol_delays_microseconds_batched.csv
  symbols_output/
```

Each `.bin` row is a packed 128-byte record:

| Field | Type | Description |
|---|---|---|
| `symbol` | `char[24]` | Symbol name, uppercased |
| `open_time` | `int64_t` | Kline open timestamp in milliseconds |
| `close_time` | `int64_t` | Kline close timestamp in milliseconds |
| `open` | `double` | Open price |
| `high` | `double` | High price |
| `low` | `double` | Low price |
| `close` | `double` | Close price |
| `volume` | `double` | Base asset volume |
| `quote_volume` | `double` | Quote asset volume |
| `trades` | `int64_t` | Number of trades |
| `taker_base_vol` | `double` | Taker buy base volume |
| `taker_quote_vol` | `double` | Taker buy quote volume |
| `maker_base_vol` | `double` | Maker base volume, calculated from total minus taker base |
| `maker_quote_vol` | `double` | Maker quote volume, calculated from total minus taker quote |

This fixed layout is intended to match a Python/NumPy dtype with an item size of 128 bytes.

---

## Collector features

### Historical bulk import

The collector can download daily/monthly Binance Vision archives, extract the CSV data, parse the kline rows, and import them into daily `.bin` files.

Historical imports are deduplicated and sorted before writing.

### REST lookback updates

The `rest72` mode fetches recent klines for configured symbols, using a configurable lookback window. By default, the code is set up around a 72-hour recent-history update.

### Live WebSocket collection

The live mode connects to Binance multiplex WebSocket streams and listens for closed `1m` kline events.

The live pipeline is split into separate stages:

1. WebSocket receiver threads read raw messages.
2. Worker threads parse JSON and convert closed candles into kline records.
3. A dedicated binary writer batches `.bin` writes.
4. A dedicated CSV writer batches timing metric writes.

This design keeps WebSocket reads lightweight and reduces filesystem contention when many symbols close at the same minute.

### Reconnect and gap recovery

If a WebSocket disconnects or Binance sends a shutdown signal, the collector records the gap start time. After reconnecting, it backfills the missing candles with REST calls before continuing live collection.

### Validation mode

Validation checks daily `.bin` files for:

- whether the file exists
- total rows
- unique open timestamps
- duplicate count
- whether the file has 1,440 unique 1-minute candles

---

## Collector run modes

The collector has these run modes configured in the source:

| Mode | Description |
|---|---|
| `symbols` | Resolve/write configured or auto-discovered symbol lists. |
| `bulk` | Download and import historical Binance Vision data. |
| `rest72` | Fetch recent REST klines for the configured symbols. |
| `live` | Start live WebSocket kline collection. |
| `all` | Run live collection continuously while REST and bulk jobs run in the background. |
| `validate` | Validate daily binary files for the configured symbols. |

The current source sets `RUN_MODE = RunMode::all`, so the collector is intended to run live continuously while also coordinating recent REST updates and bulk historical imports.

---

## Dashboard features

The dashboard is a standalone C++20 program with no C++ third-party library requirement. It uses operating-system sockets directly and loads Plotly in the browser.

Default dashboard settings:

| Setting | Default |
|---|---|
| Host | `127.0.0.1` |
| Port | `8060` |
| Base directory | `data_dump` |
| Default market | `spot` |
| Default symbol | `BTCUSDT` |
| Default OHLCV rows | `500` |
| Default timing rows | `5000` |
| Refresh interval | `3` seconds |

When started, the dashboard opens:

```text
http://127.0.0.1:8060
```

It reads:

```text
data_dump/daily_bin/
data_dump/logs/live_symbol_delays_microseconds_batched.csv
```

The web UI includes:

- market selector
- symbol selector
- OHLCV row count
- timing row count
- candle/close-line plot type
- refresh interval
- pause/resume button
- OHLCV chart
- live timing chart
- symbol summary table

---

## Dashboard API endpoints

The dashboard serves a small JSON API:

| Endpoint | Description |
|---|---|
| `/` or `/index.html` | Dashboard HTML UI |
| `/api/markets` | Available market list and symbol counts |
| `/api/symbols?market=um` | Symbols available for a market |
| `/api/summary?market=um` | Per-symbol file count, row count, and date range |
| `/api/range?market=um&symbol=BTCUSDT` | First/last timestamp range for a symbol |
| `/api/ohlcv?market=um&symbol=BTCUSDT&bars=500` | Latest OHLCV rows for plotting |
| `/api/timings?market=um&symbol=BTCUSDT&rows=5000` | Live timing/latency rows for plotting |

---

## Dependencies

### Collector

The collector depends on:

- C++20 compiler
- libcurl
- OpenSSL
- Boost.Asio / Boost.Beast
- Boost.Interprocess
- nlohmann-json
- pugixml
- libzip

With vcpkg:

```bash
vcpkg install curl openssl boost-beast boost-interprocess nlohmann-json pugixml libzip
```

### Dashboard

The dashboard source uses only the C++ standard library plus platform socket APIs:

- Windows: Winsock2
- Linux/macOS: POSIX sockets

The browser UI loads Plotly from a CDN.

---

## Build notes

### Collector

The collector must be linked with its external dependencies. If using CMake, link the equivalent of:

- CURL
- OpenSSL
- Boost.System
- Boost.Beast / Boost.Asio headers
- Boost.Interprocess headers
- nlohmann-json
- pugixml
- libzip
- platform socket libraries where required

Example manual build commands will depend on your compiler, operating system, and package manager.

### Dashboard

Linux/macOS example:

```bash
g++ -std=c++20 -O2 -pthread binance_plot_dashboard_server.cpp -o dashboard
```

Windows MinGW example:

```bash
g++ -std=c++20 -O2 binance_plot_dashboard_server.cpp -o dashboard.exe -lws2_32
```

---

## Configuration

### Collector configuration

The collector is configured directly inside the `CONFIG` section of `binance_klines_collector_cpp20.cpp`.

Important values to review before running:

- `BASE_DIR`
- `RUN_MODE`
- `SYMBOLS_SPOT_BULK`
- `SYMBOLS_FUTURES_UM_BULK`
- `SYMBOLS_FUTURES_CM_BULK`
- `SYMBOLS_SPOT_LIVE_REST`
- `SYMBOLS_FUTURES_UM_LIVE_REST`
- `SYMBOLS_FUTURES_CM_LIVE_REST`
- `AUTO_FIND_SYMBOLS_SPOT`
- `AUTO_FIND_SYMBOLS_UM`
- `AUTO_FIND_SYMBOLS_CM`
- `BULK_START_DATE`
- `BULK_END_DATE`
- `LIVE_DELAY_LOG_ENABLED`

### Dashboard configuration

The dashboard supports a config file named:

```text
dashboard_config.txt
```

Example:

```text
base_dir=data_dump
host=127.0.0.1
port=8060
auto_open_browser=true
default_market=um
default_symbol=BTCUSDT
default_bars=500
default_timing_rows=5000
default_refresh_seconds=3
```

It also supports command-line arguments:

```bash
./dashboard --base-dir data_dump --host 127.0.0.1 --port 8060 --market um --symbol BTCUSDT --bars 1000 --timing-rows 5000 --refresh 3
```

Use `--no-browser` to prevent the dashboard from opening a browser automatically.

---

## Typical workflow

1. Edit the collector configuration.
2. Build the collector.
3. Run the collector to create or update `data_dump/`.
4. Build the dashboard.
5. Start the dashboard and open `http://127.0.0.1:8060`.
6. Select a market and symbol to inspect OHLCV and live latency metrics.

Example:

```bash
# Run collector
./binance_klines_collector

# In another terminal, run dashboard
./dashboard --base-dir data_dump
```

---

## Notes and limitations

- The collector is configured in source code rather than through command-line flags.
- Live mode is designed to keep running and reconnecting.
- In the uploaded version, the collector uses a never-stop signal mode, so normal `SIGINT`/`SIGTERM` may be ignored depending on the build configuration.
- Binance rate limits and API availability still apply.
- The collector stores UTC timestamps.
- The dashboard assumes the collector's `.bin` files use the exact 128-byte record layout.
- The dashboard is intended for local use and binds to `127.0.0.1` by default.

---

## Project status

This codebase is suitable for a local market-data collection and monitoring workflow. It is especially useful if you want:

- fast local binary storage for 1-minute Binance klines
- a combined historical + live data collector
- reconnect-aware WebSocket ingestion
- latency/timing measurements for live candles
- a lightweight browser dashboard without a separate web framework

---

## Disclaimer

This software is for data collection, research, and visualization. It does not place trades and does not provide financial advice.
