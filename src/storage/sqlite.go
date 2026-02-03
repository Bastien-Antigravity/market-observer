package storage

import (
	"database/sql"
	"fmt"
	"log"
	"market-observer/src/logger"
	"market-observer/src/models"
	"time"

	_ "modernc.org/sqlite"
)

// SQLite batch constants
const (
	sqliteMaxVars   = 32000
	paramsPerRow    = 6
	sqliteBatchSize = sqliteMaxVars / paramsPerRow // ~5333 rows
)

// -----------------------------------------------------------------------------

type AsyncSQLiteDB struct {
	Config *models.MConfig
	DB     *sql.DB
	Logger *logger.Logger
}

// -----------------------------------------------------------------------------

func NewAsyncSQLiteDB(cfg *models.MConfig, log *logger.Logger) (*AsyncSQLiteDB, error) {
	return &AsyncSQLiteDB{
		Config: cfg,
		Logger: log,
	}, nil
}

// -----------------------------------------------------------------------------

func (d *AsyncSQLiteDB) Initialize() error {
	dsn := d.Config.Storage.DBPath

	// Open DB
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	d.DB = db

	// PRAGMA optimizations
	if _, err := db.Exec("PRAGMA journal_mode = WAL;"); err != nil {
		d.Logger.Warning("Failed to set WAL mode: %v", err)
	}
	if _, err := db.Exec("PRAGMA synchronous = NORMAL;"); err != nil {
		d.Logger.Warning("Failed to set synchronous mode: %v", err)
	}

	// Recreate Tables
	return d.recreateTables()
}

// -----------------------------------------------------------------------------

func (d *AsyncSQLiteDB) recreateTables() error {
	// Drop stock_prices
	if _, err := d.DB.Exec("DROP TABLE IF EXISTS stock_prices"); err != nil {
		return fmt.Errorf("failed to drop stock_prices: %w", err)
	}

	// Create stock_prices
	// SQLite types: INTEGER for int64, REAL for float64, TEXT for string
	query := `
		CREATE TABLE stock_prices (
			symbol TEXT,
			timestamp INTEGER,
			price REAL,
			volume REAL,
			price_percent_change REAL,
			volume_percent_change REAL,
			PRIMARY KEY (symbol, timestamp)
		);
	`
	if _, err := d.DB.Exec(query); err != nil {
		return fmt.Errorf("failed to create stock_prices: %w", err)
	}

	for _, w := range d.Config.WindowsAgg {
		// Aggregations
		aggTable := fmt.Sprintf("aggregations_%s", w)
		if _, err := d.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", aggTable)); err != nil {
			return fmt.Errorf("failed to drop %s: %w", aggTable, err)
		}

		query = fmt.Sprintf(`
			CREATE TABLE %s (
				symbol TEXT,
				start_time INTEGER,
				end_time INTEGER,
				open REAL,
				high REAL,
				low REAL,
				close REAL,
				volume REAL,
				price_percent_change REAL,
				volume_percent_change REAL,
				PRIMARY KEY (symbol, start_time)
			);
		`, aggTable)
		if _, err := d.DB.Exec(query); err != nil {
			return fmt.Errorf("failed to create %s: %w", aggTable, err)
		}

		// Intermediate Stats
		statsTable := fmt.Sprintf("intermediate_stats_%s", w)
		if _, err := d.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", statsTable)); err != nil {
			return fmt.Errorf("failed to drop %s: %w", statsTable, err)
		}

		query = fmt.Sprintf(`
			CREATE TABLE %s (
				symbol TEXT,
				window_name TEXT,
				avg_volume_history REAL,
				std_volume_history REAL,
				data_points_history INTEGER,
				last_history_timestamp INTEGER,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (symbol, window_name)
			);
		`, statsTable)
		if _, err := d.DB.Exec(query); err != nil {
			return fmt.Errorf("failed to create %s: %w", statsTable, err)
		}
	}

	return nil
}

// -----------------------------------------------------------------------------

func (d *AsyncSQLiteDB) SaveStockPricesBulk(prices []models.MStockPrice) error {
	if len(prices) == 0 {
		return nil
	}

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT INTO stock_prices (symbol, timestamp, price, volume, price_percent_change, volume_percent_change)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range prices {
		_, err := stmt.Exec(p.Symbol, p.Timestamp, p.Price, p.Volume, p.PricePercentChange, p.VolumePercentChange)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

// -----------------------------------------------------------------------------

func (d *AsyncSQLiteDB) SaveAggregations(aggs map[string]map[string][]models.MAggregation) error {
	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, wMap := range aggs {
		for w, items := range wMap {
			if len(items) == 0 {
				continue
			}
			tableName := fmt.Sprintf("aggregations_%s", w)

			query := fmt.Sprintf(`
				INSERT INTO %s (symbol, start_time, end_time, open, high, low, close, volume, price_percent_change, volume_percent_change)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, tableName)

			stmt, err := tx.Prepare(query)
			if err != nil {
				return err
			}
			defer stmt.Close()

			for _, agg := range items {
				_, err = stmt.Exec(agg.Symbol, agg.StartTime, agg.EndTime, agg.Open, agg.High, agg.Low, agg.Close, agg.Volume, agg.PricePercentChange, agg.VolumePercentChange)
				if err != nil {
					return err
				}
			}
		}
	}

	return tx.Commit()
}

// -----------------------------------------------------------------------------

func (d *AsyncSQLiteDB) SaveIntermediateStats(stats []models.MIntermediateStats) error {
	if len(stats) == 0 {
		return nil
	}

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Group by window
	byWindow := make(map[string][]models.MIntermediateStats)
	for _, s := range stats {
		byWindow[s.WindowName] = append(byWindow[s.WindowName], s)
	}

	for w, list := range byWindow {
		tableName := fmt.Sprintf("intermediate_stats_%s", w)

		query := fmt.Sprintf(`
			INSERT INTO %s (symbol, window_name, avg_volume_history, std_volume_history, data_points_history, last_history_timestamp, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (symbol, window_name) DO UPDATE SET
				avg_volume_history = excluded.avg_volume_history,
				std_volume_history = excluded.std_volume_history,
				data_points_history = excluded.data_points_history,
				last_history_timestamp = excluded.last_history_timestamp,
				updated_at = excluded.updated_at
		`, tableName)

		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, s := range list {
			_, err = stmt.Exec(s.Symbol, s.WindowName, s.AvgVolumeHistory, s.StdVolumeHistory, s.DataPointsHistory, s.LastHistoryTimestamp, time.Now().UTC())
			if err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// -----------------------------------------------------------------------------

func (d *AsyncSQLiteDB) CleanupOldData() error {
	retentionDays := d.Config.DataSource.DataRetentionDays
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Unix()

	log.Printf("Cleaning up data older than %d days (timestamp < %d)...", retentionDays, cutoff)

	// Clean stock_prices
	if _, err := d.DB.Exec("DELETE FROM stock_prices WHERE timestamp < ?", cutoff); err != nil {
		d.Logger.Error("Cleanup stock_prices error: %v", err)
	}

	// Clean aggregation tables
	for _, w := range d.Config.WindowsAgg {
		tableName := fmt.Sprintf("aggregations_%s", w)
		if _, err := d.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE end_time < ?", tableName), cutoff); err != nil {
			d.Logger.Error("Cleanup %s error: %v", tableName, err)
		}
	}

	d.Logger.Info("Cleanup completed")
	return nil
}

// -----------------------------------------------------------------------------

func (d *AsyncSQLiteDB) Close() error {
	if d.DB != nil {
		return d.DB.Close()
	}
	return nil
}
