# MarketObserver

**MarketObserver** is a high-performance, asynchronous market surveillance system designed to track real-time stock data, aggregate it into custom timeframes, and detect statistical anomalies.

---

## 🛠️ Developer Guide

### 1. Core Calculation Functions

The mathematical core of the application is located in `src/analysis/core/`. This strictly separates the "how" (math) from the "when" (orchestration).

#### **How to Add or Change Calculations:**
1.  **Navigate to**: `src/analysis/core/`
2.  **Modify or Create**:
    *   `financial.go`: For financial indicators (e.g., Price Changes, Moving Averages).
    *   `statistics.go`: For statistical methods (e.g., Standard Deviation, Z-Score).
    *   *Example*: To add an RSI calculation, create a function `CalculateRSI(prices []float64) float64` in `financial.go`.
3.  **Integrate**:
    *   Go to `src/analysis/analysis_facade.go`.
    *   Call your new function within the `Analyze` or `CalculateStats` methods to expose it to the rest of the application.

#### **How to Remove Calculations:**
1.  Remove the function call from `src/analysis/analysis_facade.go`.
2.  Delete the function definition from `src/analysis/core/*.go`.

---

### 2. Data Sources

Data sources are modular and implement the `IDataSource` interface. They are managed by the `MultiSourceManager`.

#### **How to Add a New Data Source:**
1.  **Implement Interface**: Create a new package (e.g., `src/data_source/alpha_vantage/`) and implement the `IDataSource` interface defined in `src/interfaces/data_source.go`.
    *   Must implement: `FetchInitialData`, `FetchUpdateData`, `Start(ctx, chan, wg)`, `Stop`, `Name`, `IsRealTime`, `UpdateSymbols`.
2.  **Configuration**: Add necessary config fields to `src/models/config.go` and `config/default.yaml`.
3.  **Registration**:
    *   Open `cmd/test/setup.go` (or `setupSources` function).
    *   Initialize your new source instance.
    *   Add it to the slice passed to `datasource.NewMultiSourceManager`.

#### **How to Remove a Data Source:**
1.  **Unregister**: Remove the initialization code from `cmd/test/setup.go`.
2.  **Cleanup**: Delete the source's package directory from `src/data_source/`.

---

### 3. Time Frame Respect (Minimal 5m Aggregation)

The system is architected around a **5-minute base resolution**.

*   **Ingestion**: All data sources must provide data in 5-minute snapshots (or be resampled to 5m before entering the system).
*   **Aggregation**: Higher timeframes (15m, 1h, 4h) are strictly built by aggregating these 5-minute Base Candles.
*   **Why?**: This ensures consistency. We never fetch "1-hour candles" directly; we build them from twelve 5-minute candles.
*   **Logic**:
    *   See `src/analysis/resampler.go` for the aggregation logic.
    *   The `Resample` function groups 5m candles into buckets based on the target window (e.g., `1h`).

---

## 🚀 Standard Documentation

### Features
- **Multi-Source Architecture**: Ingest data from Yahoo Finance, Interactive Brokers, or any custom source simultaneously using the `MultiSourceManager`.
- **High Performance**:
    - **Go Concurrency**: Heavy use of Goroutines and Channels for non-blocking I/O.
    - **Context-Managed Lifecycle**: Graceful shutdown and signal propagation using `context.Context`.
    - **Lock-Free Hot Paths**: Optimized data ingestion loops minimize mutex contention.
- **Dynamic Analysis**: Real-time calculation of internal statistics and anomalies.
- **gRPC Control Plane**: Dynamic management of sources (Start/Stop/Add/Remove) via gRPC.

### Architecture
- **`cmd/test/`**: Application entry point and setup.
- **`src/data_source/`**: Data ingestion layer.
    - `YahooFinanceSource`: Polling-based source example.
    - `MultiSourceManager`: Fan-in aggregator for all sources.
- **`src/analysis/`**: Business logic.
    - `core/`: Pure functions for math/stats.
    - `Facade`: Orchestrator.
- **`src/storage/`**: Database persistence (SQLite/PostgreSQL).

### Running the App
```bash
# Navigate to the command directory
cd cmd/test

# Run with default config
go run main.go setup.go bootstrap.go servers.go core_processing.go --config ../../config/default.yaml
```

### Configuration
Managed via `config/default.yaml`.
```yaml
data_source:
  update_interval_seconds: 300 # 5 minutes
  yahoo:
    enabled: true
    symbols: ["AAPL", "NVDA", "MSFT"]
```
