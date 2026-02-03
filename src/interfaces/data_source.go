package interfaces

import (
	"context"
	"market-observer/src/models"
	"sync"
)

// -----------------------------------------------------------------------------
// IDataSource interface for fetching stock data from external sources.
// -----------------------------------------------------------------------------

type IDataSource interface {

	// Name returns the unique identifier of the source
	Name() string

	// -----------------------------------------------------------------------------

	// FetchInitialData retrieves historical data (e.g. 7 days) for all validation symbols.
	FetchInitialData() (map[string][]models.MStockPrice, error)

	// -----------------------------------------------------------------------------

	// FetchUpdateData for 5-minute updates
	FetchUpdateData() (map[string][]models.MStockPrice, error)

	// -----------------------------------------------------------------------------

	// IsRealTime returns true if the source provides real-time data
	IsRealTime() bool

	// -----------------------------------------------------------------------------

	// UpdateSymbols updates the list of symbols being monitored
	UpdateSymbols(symbols []string) error

	// -----------------------------------------------------------------------------

	// Start begins the data fetching process
	// ctx: controls the lifecycle (cancellation stops the source)
	// outputChan: channel to push data to
	// wg: WaitGroup to signal when the source has fully stopped
	Start(ctx context.Context, outputChan chan<- map[string][]models.MStockPrice, wg *sync.WaitGroup) error

	// -----------------------------------------------------------------------------

	// Stop terminates the data fetching process (legacy/manual stop)
	// Ideally, cancelling the context passed to Start should be enough.
	Stop() error
}
