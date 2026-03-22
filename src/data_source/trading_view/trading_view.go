package trading_view

import (
	"context"
	"sync"
	"time"

	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"

	tv "github.com/VictorVictini/tradingview-lib"
)

type TradingViewSource struct {
	Config       *models.MConfig
	SourceConfig models.MSourceConfig
	Logger       *logger.Logger
	api          *tv.API
	symbols      []string
}

// -----------------------------------------------------------------------------

func NewTradingViewSource(cfg *models.MConfig, sourceCfg models.MSourceConfig, logger *logger.Logger) interfaces.IDataSource {
	return &TradingViewSource{
		Config:       cfg,
		SourceConfig: sourceCfg,
		Logger:       logger,
		symbols:      sourceCfg.Symbols,
	}
}

// -----------------------------------------------------------------------------

func (s *TradingViewSource) Name() string {
	return s.SourceConfig.Name
}

// -----------------------------------------------------------------------------

func (s *TradingViewSource) IsRealTime() bool {
	return true
}

// -----------------------------------------------------------------------------

func (s *TradingViewSource) UpdateSymbols(symbols []string) error {
	if s.api != nil {
		s.api.RemoveRealtimeSymbols(s.symbols)
		s.api.AddRealtimeSymbols(symbols)
	}
	s.symbols = symbols
	return nil
}

// -----------------------------------------------------------------------------

func (s *TradingViewSource) FetchInitialData() (map[string][]models.MStockPrice, error) {
	s.Logger.Info("TradingView FetchInitialData: Legacy function not needed for push sourcing.")
	return make(map[string][]models.MStockPrice), nil
}

// -----------------------------------------------------------------------------

func (s *TradingViewSource) FetchUpdateData() (map[string][]models.MStockPrice, error) {
	return nil, nil // Handled by push streaming
}

// -----------------------------------------------------------------------------

func (s *TradingViewSource) Start(ctx context.Context, outputChan chan<- map[string][]models.MStockPrice, wg *sync.WaitGroup) error {
	// Setup API config and channels
	api := &tv.API{}
	api.Channels.Read = make(chan map[string]interface{}, 1000)
	api.Channels.Error = make(chan error, 100)
	s.api = api

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := s.api.OpenConnection(nil)
		if err != nil {
			s.Logger.Error("TradingView connection error: %v", err)
		}
	}()

	// Wait briefly for connection (in lieu of explicit connect ack from lib)
	go func() {
		time.Sleep(2 * time.Second)
		if len(s.symbols) > 0 {
			s.api.AddRealtimeSymbols(s.symbols)
			s.Logger.Info("[%s] Connected to TradingView & subscribed to: %v", s.Name(), s.symbols)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				s.Stop()
				return
			case err := <-s.api.Channels.Error:
				s.Logger.Error("TradingView Channel Error: %v", err)
			case data := <-s.api.Channels.Read:
				if data["type"] == "realtime" {
					sym, ok := data["symbol"].(string)
					if !ok || sym == "" {
						continue
					}
					price, _ := data["current_price"].(float64)
					volume, _ := data["volume"].(float64)

					// Timestamp fallback if missing or wrong type
					tsFloat, ok := data["timestamp"].(float64)
					var ts int64
					if !ok {
						ts = time.Now().UTC().Unix()
					} else {
						ts = int64(tsFloat)
					}

					tick := models.MStockPrice{
						Symbol:    sym,
						Price:     price,
						Volume:    volume,
						Timestamp: ts,
					}

					dataMap := make(map[string][]models.MStockPrice)
					dataMap[sym] = []models.MStockPrice{tick}
					outputChan <- dataMap
				}
			}
		}
	}()

	return nil
}

// -----------------------------------------------------------------------------

func (s *TradingViewSource) Stop() error {
	if s.api != nil && len(s.symbols) > 0 {
		s.api.RemoveRealtimeSymbols(s.symbols)
	}
	s.Logger.Info("[%s] Connection stopped", s.Name())
	return nil
}
