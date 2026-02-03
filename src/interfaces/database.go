package interfaces

import "market-observer/src/models"

// -----------------------------------------------------------------------------
// IDatabase defines the contract for storage operations.
// -----------------------------------------------------------------------------

type IDatabase interface {

	// -----------------------------------------------------------------------------

	// Initialize sets up the database schema and tables.
	Initialize() error

	// -----------------------------------------------------------------------------

	// SaveStockPricesBulk inserts a batch of raw stock prices.
	SaveStockPricesBulk(prices []models.MStockPrice) error

	// -----------------------------------------------------------------------------
	// SaveAggregations for saving calculated stats (Postgres/SQLite)
	SaveAggregations(aggs map[string]map[string][]models.MAggregation) error

	// -----------------------------------------------------------------------------
	// SaveIntermediateStats for saving rolling stats (Postgres/SQLite)
	SaveIntermediateStats(stats []models.MIntermediateStats) error

	// -----------------------------------------------------------------------------

	// CleanupOldData removes data older than the retention policy.
	CleanupOldData() error

	// -----------------------------------------------------------------------------

	// Close the database connection
	Close() error
}
