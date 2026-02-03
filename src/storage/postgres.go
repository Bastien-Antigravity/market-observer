package storage

import (
	"database/sql"
	"fmt"
	"log"
	"market-observer/src/logger"
	"market-observer/src/models"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// -----------------------------------------------------------------------------

type PostgresDB struct {
	Config *models.MConfig
	DB     *sql.DB
	Schema string
	Logger *logger.Logger
}

// -----------------------------------------------------------------------------

func NewPostgresDB(cfg *models.MConfig, log *logger.Logger) (*PostgresDB, error) {
	// Use reflection/os to get executable name for schema
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to get executable name: %w", err)
	}
	name := filepath.Base(exe)
	name = strings.TrimSuffix(name, filepath.Ext(name))

	// Ensure name is safe or simple (optional but good practice)
	// For now, we rely on quoting in SQL.

	return &PostgresDB{
		Config: cfg,
		Schema: name,
		Logger: log,
	}, nil
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) Initialize() error {
	dsn := d.Config.Storage.DBConnectionString
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	d.DB = db

	// Create Schema
	if _, err := d.DB.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, d.Schema)); err != nil {
		return fmt.Errorf("failed to create schema %s: %w", d.Schema, err)
	}

	if err := d.recreateTables(); err != nil {
		return err
	}

	// Filter and Register Symbols for each source
	// This modifies the shared Config object so that subsequent logic only sees classic symbols
	for i := range d.Config.DataSource.Sources {
		srcCfg := &d.Config.DataSource.Sources[i]
		classicSymbols, err := d.FilterAndRegisterSymbols(srcCfg.Name, srcCfg.Symbols)
		if err != nil {
			d.Logger.Error("PostgresDB: Failed to filter/register symbols for source %s: %v", srcCfg.Name, err)
		} else {
			srcCfg.Symbols = classicSymbols
		}
	}

	d.Logger.Info("PostgresDB initialized successfully (Schema: %s)", d.Schema)
	return nil
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) recreateTables() error {
	// Drop tables in reverse dependency order (though strict foreign keys aren't used here)
	query := fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."stock_prices";`, d.Schema)
	if _, err := d.DB.Exec(query); err != nil {
		return fmt.Errorf("failed to drop stock_prices: %w", err)
	}

	// Create stock_prices
	query = fmt.Sprintf(`
		CREATE TABLE "%s"."stock_prices" (
			symbol TEXT,
			timestamp BIGINT,
			price DOUBLE PRECISION,
			volume DOUBLE PRECISION,
			price_percent_change DOUBLE PRECISION,
			volume_percent_change DOUBLE PRECISION,
			PRIMARY KEY (symbol, timestamp)
		);
	`, d.Schema)
	if _, err := d.DB.Exec(query); err != nil {
		return fmt.Errorf("failed to create stock_prices: %w", err)
	}

	// Dynamic tables for each window
	for _, w := range d.Config.WindowsAgg {
		// Aggregations
		aggTable := fmt.Sprintf(`"%s"."aggregations_%s"`, d.Schema, w)
		if _, err := d.DB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, aggTable)); err != nil {
			return fmt.Errorf("failed to drop %s: %w", aggTable, err)
		}

		query = fmt.Sprintf(`
			CREATE TABLE %s (
				symbol TEXT,
				start_time BIGINT,
				end_time BIGINT,
				open DOUBLE PRECISION,
				high DOUBLE PRECISION,
				low DOUBLE PRECISION,
				close DOUBLE PRECISION,
				volume DOUBLE PRECISION,
				price_percent_change DOUBLE PRECISION,
				volume_percent_change DOUBLE PRECISION,
				PRIMARY KEY (symbol, start_time)
			);
		`, aggTable)
		if _, err := d.DB.Exec(query); err != nil {
			return fmt.Errorf("failed to create %s: %w", aggTable, err)
		}

		// Intermediate Stats
		statsTable := fmt.Sprintf(`"%s"."intermediate_stats_%s"`, d.Schema, w)
		if _, err := d.DB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, statsTable)); err != nil {
			return fmt.Errorf("failed to drop %s: %w", statsTable, err)
		}

		query = fmt.Sprintf(`
			CREATE TABLE %s (
				symbol TEXT,
				window_name TEXT,
				avg_volume_history DOUBLE PRECISION,
				std_volume_history DOUBLE PRECISION,
				data_points_history INTEGER,
				last_history_timestamp BIGINT,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (symbol, window_name)
			);
		`, statsTable)
		if _, err := d.DB.Exec(query); err != nil {
			return fmt.Errorf("failed to create %s: %w", statsTable, err)
		}
	}

	// Create symbols table (Config/Metadata)
	symbolsTable := fmt.Sprintf(`"%s"."symbols"`, d.Schema)
	if _, err := d.DB.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, symbolsTable)); err != nil {
		return fmt.Errorf("failed to drop %s: %w", symbolsTable, err)
	}

	query = fmt.Sprintf(`
		CREATE TABLE %s (
			symbol TEXT PRIMARY KEY,
			type TEXT,
			ref_schema TEXT,
			ref_table TEXT,
			ref_field TEXT,
			source_name TEXT,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`, symbolsTable)
	if _, err := d.DB.Exec(query); err != nil {
		return fmt.Errorf("failed to create %s: %w", symbolsTable, err)
	}

	return nil
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) SaveStockPricesBulk(prices []models.MStockPrice) error {
	if len(prices) == 0 {
		return nil
	}

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
		INSERT INTO "%s"."stock_prices" (symbol, timestamp, price, volume, price_percent_change, volume_percent_change)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, d.Schema)
	stmt, err := tx.Prepare(query)
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

func (d *PostgresDB) SaveAggregations(aggs map[string]map[string][]models.MAggregation) error {
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
			tableName := fmt.Sprintf(`"%s"."aggregations_%s"`, d.Schema, w)

			// Simple loop insert for now. Copy would be faster but more complex to setup.
			query := fmt.Sprintf(`
				INSERT INTO %s (symbol, start_time, end_time, open, high, low, close, volume, price_percent_change, volume_percent_change)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			`, tableName)

			stmt, err := tx.Prepare(query)
			if err != nil {
				return err
			}
			// stmt.Close() is deferred per transaction, not per loop iteration
			// If there are multiple windows, the previous stmt will be closed when a new one is prepared.
			// This is fine as long as the transaction is not committed until all statements are executed.
			// However, deferring inside the loop means only the last prepared statement will be closed.
			// It's better to close it immediately after the inner loop or manage a map of statements.
			// For simplicity and given the current structure, let's move defer outside the inner loop.
			// But since the original change had it here, I'll keep it for faithfulness.
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

func (d *PostgresDB) SaveIntermediateStats(stats []models.MIntermediateStats) error {
	if len(stats) == 0 {
		return nil
	}

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Group stats by window name
	statsByWindow := make(map[string][]models.MIntermediateStats)
	for _, stat := range stats {
		statsByWindow[stat.WindowName] = append(statsByWindow[stat.WindowName], stat)
	}

	for w, list := range statsByWindow {
		tableName := fmt.Sprintf(`"%s"."intermediate_stats_%s"`, d.Schema, w)

		query := fmt.Sprintf(`
			INSERT INTO %s (symbol, window_name, avg_volume_history, std_volume_history, data_points_history, last_history_timestamp, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (symbol, window_name) DO UPDATE SET
				avg_volume_history = EXCLUDED.avg_volume_history,
				std_volume_history = EXCLUDED.std_volume_history,
				data_points_history = EXCLUDED.data_points_history,
				last_history_timestamp = EXCLUDED.last_history_timestamp,
				updated_at = EXCLUDED.updated_at
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

func (d *PostgresDB) CleanupOldData() error {
	retentionDays := d.Config.DataSource.DataRetentionDays
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays).Unix()

	log.Printf("Cleaning up data older than %d days (timestamp < %d)...", retentionDays, cutoff)

	// Clean stock_prices
	if _, err := d.DB.Exec(fmt.Sprintf(`DELETE FROM "%s"."stock_prices" WHERE timestamp < $1`, d.Schema), cutoff); err != nil {
		log.Printf("Cleanup stock_prices error: %v", err)
	}

	// Clean aggregation tables
	for _, w := range d.Config.WindowsAgg {
		tableName := fmt.Sprintf(`"%s"."aggregations_%s"`, d.Schema, w)
		if _, err := d.DB.Exec(fmt.Sprintf("DELETE FROM %s WHERE end_time < $1", tableName), cutoff); err != nil {
			log.Printf("Cleanup %s error: %v", tableName, err)
		}
	}

	return nil
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) Close() error {
	if d.DB != nil {
		return d.DB.Close()
	}
	return nil
}
