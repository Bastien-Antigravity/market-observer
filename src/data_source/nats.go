package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"market-observer/src/interfaces"
	"market-observer/src/models"

	"github.com/nats-io/nats.go"
)

// -----------------------------------------------------------------------------

type NATSDataSource struct {
	Config     *models.MConfig
	SourceConf models.MSourceConfig
	Logger     interfaces.Logger
	conn       *nats.Conn
	sub        *nats.Subscription
	symbols    []string
	isRunning  bool
	ctx        context.Context
}

// -----------------------------------------------------------------------------

func NewNATSDataSource(cfg *models.MConfig, srcConf models.MSourceConfig, logger interfaces.Logger) interfaces.IDataSource {
	return &NATSDataSource{
		Config:     cfg,
		SourceConf: srcConf,
		Logger:     logger,
		symbols:    srcConf.Symbols,
	}
}

// -----------------------------------------------------------------------------

func (ns *NATSDataSource) Name() string {
	return ns.SourceConf.Name
}

// -----------------------------------------------------------------------------

func (ns *NATSDataSource) IsRealTime() bool {
	return true
}

func (ns *NATSDataSource) Type() string {
	return "nats"
}

func (ns *NATSDataSource) IsRunning() bool {
	return ns.isRunning
}

// -----------------------------------------------------------------------------

func (ns *NATSDataSource) UpdateSymbols(symbols []string) error {
	ns.symbols = symbols
	return nil
}

// -----------------------------------------------------------------------------

func (ns *NATSDataSource) FetchInitialData() (map[string][]models.MStockPrice, error) {
	ns.Logger.Info("NATS FetchInitialData: Not applicable for push stream. Returning empty initial data.")
	return make(map[string][]models.MStockPrice), nil
}

// -----------------------------------------------------------------------------

func (ns *NATSDataSource) FetchUpdateData() (map[string][]models.MStockPrice, error) {
	return nil, nil // Handled by push streaming
}

// -----------------------------------------------------------------------------

func (ns *NATSDataSource) Start(ctx context.Context, outputChan chan<- map[string][]models.MStockPrice, wg *sync.WaitGroup) error {
	ns.ctx = ctx
	if len(ns.Config.Nats.Servers) == 0 {
		return fmt.Errorf("no nats servers configured")
	}
	url := ns.Config.Nats.Servers[0]

	// Use client ID if provided
	opts := []nats.Option{}
	if ns.Config.Nats.ClientID != "" {
		opts = append(opts, nats.Name(ns.Config.Nats.ClientID))
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %v", err)
	}
	ns.conn = nc

	ns.conn = nc

	// Don't call wg.Add(1) here; the caller (MultiSourceManager) already does it for the primary loop or cancellation listener
	subject := ns.Config.Nats.Subject
	if subject == "" {
		subject = "tick.raw" // Safety fallback
	}

	ns.Logger.Info(fmt.Sprintf("[%s] Connected to NATS and subscribing to %s", ns.Name(), subject))

	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		// Attempt to parse as single tick or array of ticks
		var ticks []models.MStockPrice

		// Try array first
		if err := json.Unmarshal(m.Data, &ticks); err != nil {
			// Fallback to single object
			var singleTick models.MStockPrice
			if err2 := json.Unmarshal(m.Data, &singleTick); err2 != nil {
				ns.Logger.Error(fmt.Sprintf("Failed to parse NATS message: arrayErr=%v, singleErr=%v", err, err2))
				return
			}
			ticks = []models.MStockPrice{singleTick}
		}

		dataMap := make(map[string][]models.MStockPrice)
		now := time.Now().UTC().Unix()
		for _, t := range ticks {
			if t.Timestamp == 0 {
				t.Timestamp = now
			}
			dataMap[t.Symbol] = append(dataMap[t.Symbol], t)
		}

		// Context-aware send to avoid blocking during shutdown
		select {
		case outputChan <- dataMap:
		case <-ns.ctx.Done():
			return
		}
	})
	if err != nil {
		ns.conn.Close()
		ns.conn = nil
		wg.Done()
		return err
	}
	ns.sub = sub
	ns.isRunning = true

	// Listen for cancellation
	go func() {
		defer wg.Done()
		<-ctx.Done()
		ns.Stop()
	}()

	return nil
}

// -----------------------------------------------------------------------------

func (ns *NATSDataSource) Stop() error {
	ns.isRunning = false
	if ns.sub != nil {
		ns.sub.Unsubscribe()
	}
	if ns.conn != nil {
		ns.conn.Close()
		ns.conn = nil
	}
	ns.Logger.Info(fmt.Sprintf("[%s] Connection stopped", ns.Name()))
	return nil
}
