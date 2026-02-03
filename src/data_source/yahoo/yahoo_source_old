package yahoo

import (
	"context"
	"encoding/json"
	"fmt"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"market-observer/src/models"
	"market-observer/src/utils"
)

type YahooFinanceSource struct {
	Config           *models.MConfig
	SourceConfig     models.MSourceConfig // Store specific source config (Generic settings)
	symbols          atomic.Value         // Stores []string safely
	Network          interfaces.INetworkManager
	Logger           *logger.Logger
	HttpClient       *http.Client
	MarketScheduler  *utils.MarketScheduler
	LastTimestamps   map[string]int64
	lastTimestampsMu sync.RWMutex
	cancelFunc       context.CancelFunc // To support Stop()
	ctx              context.Context    // Lifecycle context for Push safety
	outputChan       chan<- map[string][]models.MStockPrice
	isRunning        atomic.Bool
	mu               sync.Mutex
}

// -----------------------------------------------------------------------------

func (s *YahooFinanceSource) Name() string {
	return s.SourceConfig.Name
}

// -----------------------------------------------------------------------------

// IsRealTime returns false because Yahoo Finance matches the polling interval model
func (s *YahooFinanceSource) IsRealTime() bool {
	return false
}

// -----------------------------------------------------------------------------

func NewYahooFinanceSource(cfg *models.MConfig, sourceCfg models.MSourceConfig, netMgr interfaces.INetworkManager) *YahooFinanceSource {
	s := &YahooFinanceSource{
		Config:         cfg,
		SourceConfig:   sourceCfg,
		Network:        netMgr,
		Logger:         logger.NewLogger(nil, "YahooFinanceSource-"+sourceCfg.Name), // Unique Logger Name
		LastTimestamps: make(map[string]int64),
		HttpClient: &http.Client{
			Timeout: time.Duration(cfg.Network.RequestTimeout) * time.Second,
		},
		MarketScheduler: utils.NewMarketScheduler(sourceCfg.Symbols, logger.NewLogger(nil, "MarketScheduler-"+sourceCfg.Name)),
	}
	s.symbols.Store(sourceCfg.Symbols)
	return s
}

// -----------------------------------------------------------------------------

// FetchInitialData fetches historical data
func (s *YahooFinanceSource) FetchInitialData() (map[string][]models.MStockPrice, error) {
	rangeStr := fmt.Sprintf("%dd", s.Config.DataSource.DataRetentionDays)
	data, err := s.fetchBatch(s.getSymbols(), func(symbol string) ([]models.MStockPrice, error) {
		return s.fetchSymbolData(symbol, rangeStr, true)
	})

	if err != nil {
		return nil, err
	}

	// Update last timestamps
	for symbol, prices := range data {
		if len(prices) > 0 {
			lastPt := prices[len(prices)-1]
			s.lastTimestampsMu.Lock()
			s.LastTimestamps[symbol] = lastPt.Timestamp
			s.lastTimestampsMu.Unlock()
		}
	}

	return data, nil
}

// -----------------------------------------------------------------------------

// FetchUpdateData fetches latest updates
func (s *YahooFinanceSource) FetchUpdateData() (map[string][]models.MStockPrice, error) {
	return s.fetchBatch(s.getSymbols(), func(symbol string) ([]models.MStockPrice, error) {
		return s.fetchSymbolData(symbol, "1d", false)
	})
}

// -----------------------------------------------------------------------------

// fetchBatch processes symbols concurrently
func (s *YahooFinanceSource) fetchBatch(
	symbols []string,
	fetchFunc func(string) ([]models.MStockPrice, error),
) (map[string][]models.MStockPrice, error) {
	if len(symbols) == 0 {
		return make(map[string][]models.MStockPrice), nil
	}

	results := make(map[string][]models.MStockPrice)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make([]error, 0, len(symbols))
	var errorsMu sync.Mutex

	// Semaphore for concurrency limit (matches Python's asyncioSemaphore)
	sem := make(chan struct{}, s.Config.Network.ConcurrentRequests)

	for _, symbol := range symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Small delay to avoid rate limiting (matches Python's await asyncioSleep(0.01))
			time.Sleep(10 * time.Millisecond)

			data, err := fetchFunc(sym)
			if err != nil {
				s.Logger.Info("Error fetching symbol %s: %v", sym, err)
				errorsMu.Lock()
				errors = append(errors, err)
				errorsMu.Unlock()
				return
			}

			if data != nil {
				mu.Lock()
				results[sym] = data
				mu.Unlock()
			}
		}(symbol)
	}

	wg.Wait()

	s.Logger.Info("YahooFinance: Fetched %d/%d symbols successfully", len(results), len(symbols))

	// Return errors if all failed, otherwise return results
	if len(results) == 0 && len(errors) > 0 {
		return nil, fmt.Errorf("all fetches failed: %v", errors[0])
	}

	return results, nil
}

// -----------------------------------------------------------------------------

// fetchSymbolData fetches and parses data for a symbol (matches Python's _fetch_yahoo_data)
func (s *YahooFinanceSource) fetchSymbolData(symbol, rangeStr string, isInitial bool) ([]models.MStockPrice, error) {
	params := map[string]string{
		"interval":       "5m",
		"range":          rangeStr,
		"includePrePost": "false",
	}

	url := fmt.Sprintf("https://query1.finance.yahoo.com/v8/finance/chart/%s", symbol)

	respBytes, err := s.Network.Get(url, params)
	if err != nil {
		return nil, fmt.Errorf("network error for %s: %w", symbol, err)
	}

	// Parse the response
	return s.parseChartResponse(symbol, respBytes)
}

// -----------------------------------------------------------------------------

type YahooChartResponse struct {
	Chart struct {
		Result []struct {
			Meta struct {
				Currency             string  `json:"currency"`
				Symbol               string  `json:"symbol"`
				ExchangeName         string  `json:"exchangeName"`
				InstrumentType       string  `json:"instrumentType"`
				FirstTradeDate       int64   `json:"firstTradeDate"`
				RegularMarketTime    int64   `json:"regularMarketTime"`
				Gmtoffset            int     `json:"gmtoffset"`
				Timezone             string  `json:"timezone"`
				ExchangeTimezoneName string  `json:"exchangeTimezoneName"`
				RegularMarketPrice   float64 `json:"regularMarketPrice"`
				ChartPreviousClose   float64 `json:"chartPreviousClose"`
				PriceHint            int     `json:"priceHint"`
				CurrentTradingPeriod struct {
					Pre struct {
						Timezone  string `json:"timezone"`
						Start     int64  `json:"start"`
						End       int64  `json:"end"`
						Gmtoffset int    `json:"gmtoffset"`
					} `json:"pre"`
					Regular struct {
						Timezone  string `json:"timezone"`
						Start     int64  `json:"start"`
						End       int64  `json:"end"`
						Gmtoffset int    `json:"gmtoffset"`
					} `json:"regular"`
					Post struct {
						Timezone  string `json:"timezone"`
						Start     int64  `json:"start"`
						End       int64  `json:"end"`
						Gmtoffset int    `json:"gmtoffset"`
					} `json:"post"`
				} `json:"currentTradingPeriod"`
				DataGranularity string   `json:"dataGranularity"`
				Range           string   `json:"range"`
				ValidRanges     []string `json:"validRanges"`
			} `json:"meta"`
			Timestamp  []int64 `json:"timestamp"`
			Indicators struct {
				Quote []struct {
					High   []*float64 `json:"high"`   // Use pointers to handle null
					Low    []*float64 `json:"low"`    // Use pointers to handle null
					Open   []*float64 `json:"open"`   // Use pointers to handle null
					Close  []*float64 `json:"close"`  // Use pointers to handle null
					Volume []*float64 `json:"volume"` // Use pointers to handle null
				} `json:"quote"`
			} `json:"indicators"`
		} `json:"result"`
		Error *struct {
			Code        string `json:"code"`
			Description string `json:"description"`
		} `json:"error"`
	} `json:"chart"`
}

// -----------------------------------------------------------------------------

func (s *YahooFinanceSource) parseChartResponse(symbol string, data []byte) ([]models.MStockPrice, error) {
	var resp YahooChartResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("json unmarshal failed: %w", err)
	}

	if resp.Chart.Error != nil {
		return nil, fmt.Errorf("yahoo api error: %s - %s", resp.Chart.Error.Code, resp.Chart.Error.Description)
	}

	if len(resp.Chart.Result) == 0 {
		return nil, fmt.Errorf("no result in response for %s", symbol)
	}

	result := resp.Chart.Result[0]
	if len(result.Timestamp) == 0 {
		return nil, fmt.Errorf("no timestamps in response for %s", symbol)
	}

	meta := result.Meta
	indicators := result.Indicators.Quote
	if len(indicators) == 0 {
		return nil, fmt.Errorf("no quote data in response for %s", symbol)
	}

	quote := indicators[0]

	// 1. Validation: Alignment check (matches Python)
	if len(result.Timestamp) != len(quote.Close) ||
		len(result.Timestamp) != len(quote.Open) ||
		len(result.Timestamp) != len(quote.High) ||
		len(result.Timestamp) != len(quote.Low) ||
		len(result.Timestamp) != len(quote.Volume) {
		s.Logger.Info("Data alignment error for %s: Mismatched array lengths", symbol)
		return nil, fmt.Errorf("data alignment error for %s", symbol)
	}

	// 2. Build time series with sorting
	type dataPoint struct {
		timestamp int64
		open      float64
		high      float64
		low       float64
		close     float64
		volume    float64
	}

	var points []dataPoint
	validPoints := 0

	// Create and sort data points (matches Python's zip and sort)
	for i := 0; i < len(result.Timestamp); i++ {
		ts := result.Timestamp[i]

		// Handle null values (pointers can be nil)
		var open, high, low, closeVal, volume float64
		var isValid bool = true

		if quote.Open[i] != nil {
			open = *quote.Open[i]
		} else {
			isValid = false
		}
		if quote.High[i] != nil {
			high = *quote.High[i]
		} else {
			isValid = false
		}
		if quote.Low[i] != nil {
			low = *quote.Low[i]
		} else {
			isValid = false
		}
		if quote.Close[i] != nil {
			closeVal = *quote.Close[i]
		} else {
			isValid = false
		}
		if quote.Volume[i] != nil {
			volume = *quote.Volume[i]
		} else {
			isValid = false
		}

		// 3. Data Cleaning (matches Python)
		if !isValid {
			s.Logger.Info("Invalid OHLCV data received for %s at index %d", symbol, i)
			continue
		}

		if closeVal <= 0 || volume < 0 {
			s.Logger.Info("Skipping invalid point for %s: close=%f, volume=%f", symbol, closeVal, volume)
			continue
		}

		points = append(points, dataPoint{
			timestamp: ts,
			open:      open,
			high:      high,
			low:       low,
			close:     closeVal,
			volume:    volume,
		})
		validPoints++
	}

	// Sort by timestamp (matches Python's sorted())
	sort.Slice(points, func(i, j int) bool {
		return points[i].timestamp < points[j].timestamp
	})

	if len(points) == 0 {
		return nil, fmt.Errorf("no valid data points for %s", symbol)
	}

	// 4. Calculate time series with percentage changes
	var timeSeries []models.MStockPrice
	var prevClose, prevVolume float64

	// Initialize with chartPreviousClose if available
	if meta.ChartPreviousClose > 0 {
		prevClose = meta.ChartPreviousClose
	} else if len(points) > 0 {
		prevClose = points[0].close
	}

	if len(points) > 0 {
		prevVolume = points[0].volume
	}

	for _, point := range points {
		pricePct := 0.0
		volPct := 0.0

		if prevClose > 0 {
			pricePct = (point.close - prevClose) / prevClose
		}

		if prevVolume > 0 {
			volPct = (point.volume - prevVolume) / prevVolume
		}

		item := models.MStockPrice{
			Symbol:              symbol,
			Timestamp:           point.timestamp,
			Price:               point.close, // Map Close to Price
			Volume:              point.volume,
			PricePercentChange:  pricePct,
			VolumePercentChange: volPct,
			CreatedAt:           time.Now().UTC(),
		}

		timeSeries = append(timeSeries, item)
		prevClose = point.close
		prevVolume = point.volume
	}

	// Logging (matches Python's debug log)
	startTs := timeSeries[0].Timestamp
	endTs := timeSeries[len(timeSeries)-1].Timestamp
	s.Logger.Info("Fetched %s: %d valid points [%d -> %d]", symbol, validPoints, startTs, endTs)

	return timeSeries, nil
}

// -----------------------------------------------------------------------------

// Start begins the data fetching loop
func (s *YahooFinanceSource) Start(parentCtx context.Context, outputChan chan<- map[string][]models.MStockPrice, wg *sync.WaitGroup) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning.Load() {
		return fmt.Errorf("source %s is already running", s.Name())
	}

	// Derive a context so we can stop just this source via Stop()
	ctx, cancel := context.WithCancel(parentCtx)
	s.cancelFunc = cancel
	s.ctx = ctx
	s.outputChan = outputChan
	s.isRunning.Store(true)

	wg.Add(1)
	go s.runLoop(ctx, outputChan, wg)
	s.Logger.Info("Started YahooFinanceSource: %s", s.Name())
	return nil
}

// -----------------------------------------------------------------------------

// Stop signals the run loop to exit
func (s *YahooFinanceSource) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning.Load() {
		return fmt.Errorf("source %s is not running", s.Name())
	}

	if s.cancelFunc != nil {
		s.cancelFunc()
	}
	s.isRunning.Store(false)
	s.Logger.Info("Stopped YahooFinanceSource: %s", s.Name())
	return nil
}

// -----------------------------------------------------------------------------

// PushToDataSourceManager sends data to the manager's channel safely
func (s *YahooFinanceSource) PushToDataSourceManager(data map[string][]models.MStockPrice) error {
	if s.outputChan == nil {
		return fmt.Errorf("output channel is nil")
	}

	select {
	case s.outputChan <- data:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// -----------------------------------------------------------------------------

// runLoop is the main loop that fetches data periodically
func (s *YahooFinanceSource) runLoop(ctx context.Context, outputChan chan<- map[string][]models.MStockPrice, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(time.Duration(s.Config.DataSource.UpdateIntervalSeconds) * time.Second)
	defer ticker.Stop()

	// 1. Thread-Local State Optimization
	// We copy the shared map to a local map to avoid locking in the hot path.
	// Since this goroutine is the ONLY writer to LastTimestamps while running,
	// and we synchronized on Start(), this is safe.
	localTimestamps := make(map[string]int64)

	s.lastTimestampsMu.RLock()
	for k, v := range s.LastTimestamps {
		localTimestamps[k] = v
	}
	s.lastTimestampsMu.RUnlock()

	// Helper to sync back on exit (e.g. for potential restart)
	defer func() {
		s.lastTimestampsMu.Lock()
		for k, v := range localTimestamps {
			// Update only if greater (to be safe if mixed access ever happens)
			if v > s.LastTimestamps[k] {
				s.LastTimestamps[k] = v
			}
		}
		s.lastTimestampsMu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Market status check
			anyMarketOpen := s.MarketScheduler.AnyMarketOpen()
			if !anyMarketOpen {
				s.Logger.Info("All markets are closed. Pausing for 60 minutes...")
				// Interruptible Sleep
				select {
				case <-time.After(60 * time.Minute):
					// Woke up after sleep
				case <-ctx.Done():
					return // Stop signal received during sleep
				}
				continue
			}

			// Fetch data
			data, err := s.FetchUpdateData()
			if err != nil {
				s.Logger.Info("Error fetching updates: %v", err)
				continue
			}

			// Check for fresh data (Dedup)
			validData := make(map[string][]models.MStockPrice)
			for symbol, prices := range data {
				var newPrices []models.MStockPrice

				// Lock-Free Read from Local State
				lastTs := localTimestamps[symbol]

				for _, p := range prices {
					if lastTs == 0 || p.Timestamp > lastTs {
						newPrices = append(newPrices, p)
					}
				}

				if len(newPrices) > 0 {
					validData[symbol] = newPrices

					// Lock-Free Update to Local State
					lastP := newPrices[len(newPrices)-1]
					if lastP.Timestamp > localTimestamps[symbol] {
						localTimestamps[symbol] = lastP.Timestamp
					}
				}
			}

			// Push to channel if we have new data
			if len(validData) > 0 {
				if err := s.PushToDataSourceManager(validData); err != nil {
					return // Stop if push failed (e.g. context cancelled)
				}
			}
		}
	}
}

// -----------------------------------------------------------------------------

func (s *YahooFinanceSource) UpdateSymbols(symbols []string) error {
	// Atomic swap
	s.symbols.Store(symbols)
	s.Logger.Info("Updated symbol list. New count: %d", len(symbols))

	// Also update MarketScheduler
	s.MarketScheduler.UpdateSymbols(symbols)

	return nil
}

// -----------------------------------------------------------------------------

func (s *YahooFinanceSource) getSymbols() []string {
	return s.symbols.Load().([]string)
}
