package storage

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"market-observer/src/interfaces"
	"market-observer/src/models"

	_ "github.com/lib/pq"
)

// -----------------------------------------------------------------------------

type PostgresDB struct {
	Config *models.MConfig
	DB     *sql.DB
	Schema string
	Logger interfaces.Logger
}

// -----------------------------------------------------------------------------

func NewPostgresDB(cfg *models.MConfig, log interfaces.Logger) (*PostgresDB, error) {
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

	// Append sslmode if not already defined in the connection string
	if strings.Contains(dsn, "postgresql://") && !strings.Contains(dsn, "sslmode=") {
		sslMode := "disable"
		if d.Config.Storage.EnableSSL {
			sslMode = "require"
		}

		separator := "?"
		if strings.Contains(dsn, "?") {
			separator = "&"
		}
		dsn = fmt.Sprintf("%s%ssslmode=%s", dsn, separator, sslMode)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	if err := db.Ping(); err != nil {
		return err
	}

	d.DB = db

	// Check Reset flag
	if d.Config.Storage.Reset {
		d.Logger.Warning(fmt.Sprintf("Reset flag is true: Dropping schema %s CASCADE", d.Schema))
		if _, err := d.DB.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, d.Schema)); err != nil {
			return fmt.Errorf("failed to drop schema %s: %w", d.Schema, err)
		}
	}

	// Create Schema
	if _, err := d.DB.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, d.Schema)); err != nil {
		return fmt.Errorf("failed to create schema %s: %w", d.Schema, err)
	}

	if err := d.createTables(); err != nil {
		return err
	}

	// Filter and Register Symbols for each source
	// This modifies the shared Config object so that subsequent logic only sees classic symbols
	for i := range d.Config.DataSource.Sources {
		srcCfg := &d.Config.DataSource.Sources[i]
		classicSymbols, err := d.FilterAndRegisterSymbols(srcCfg.Name, srcCfg.Symbols)
		if err != nil {
			d.Logger.Error(fmt.Sprintf("PostgresDB: Failed to filter/register symbols for source %s: %v", srcCfg.Name, err))
		} else {
			srcCfg.Symbols = classicSymbols
		}
	}

	d.Logger.Info(fmt.Sprintf("PostgresDB initialized successfully (Schema: %s)", d.Schema))
	return nil
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) createTables() error {
	// Create stock_prices_tick
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS "%s"."stock_prices_tick" (
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
		return fmt.Errorf("failed to create stock_prices_tick: %w", err)
	}

	// TimescaleDB Integration
	// Note: We use 86400 seconds (1 day) chunk intervals for integer timestamps.
	hyperQueryTick := fmt.Sprintf(`SELECT create_hypertable('"%s"."stock_prices_tick"', 'timestamp', chunk_time_interval => 86400, if_not_exists => TRUE);`, d.Schema)
	if _, err := d.DB.Exec(hyperQueryTick); err != nil {
		return fmt.Errorf("TimescaleDB hypertable creation failed (ensure TimescaleDB is installed and enabled!): %w", err)
	}

	dropAfterSeconds := d.Config.DataSource.DataRetentionDays * 86400
	retentionTick := fmt.Sprintf(`SELECT add_retention_policy('"%s"."stock_prices_tick"', drop_after => %d, if_not_exists => TRUE);`, d.Schema, dropAfterSeconds)
	if _, err := d.DB.Exec(retentionTick); err != nil {
		return fmt.Errorf("TimescaleDB retention policy failed for stock_prices_tick: %w", err)
	}

	// Dynamic tables for each window
	for _, w := range d.Config.WindowsAgg {
		// Aggregations
		aggTable := fmt.Sprintf(`"%s"."aggregations_%s"`, d.Schema, w)

		query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
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

		hyperQueryAgg := fmt.Sprintf(`SELECT create_hypertable('%s', 'start_time', chunk_time_interval => 86400, if_not_exists => TRUE);`, aggTable)
		if _, err := d.DB.Exec(hyperQueryAgg); err != nil {
			return fmt.Errorf("TimescaleDB hypertable creation failed for %s: %w", aggTable, err)
		}

		dropAfterSeconds := d.Config.DataSource.DataRetentionDays * 86400
		retentionAgg := fmt.Sprintf(`SELECT add_retention_policy('%s', drop_after => %d, if_not_exists => TRUE);`, aggTable, dropAfterSeconds)
		if _, err := d.DB.Exec(retentionAgg); err != nil {
			return fmt.Errorf("TimescaleDB retention policy failed for %s: %w", aggTable, err)
		}

		// Intermediate Stats
		statsTable := fmt.Sprintf(`"%s"."intermediate_stats_%s"`, d.Schema, w)

		query = fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
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

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
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
	pMode := d.Config.Storage.PostgresMode
	if pMode == "aggregated" {
		return nil // Operating in aggregated mode, bypass raw tick insert
	}

	if len(prices) == 0 {
		return nil
	}

	tx, err := d.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
		INSERT INTO "%s"."stock_prices_tick" (symbol, timestamp, price, volume, price_percent_change, volume_percent_change)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING
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
	pMode := d.Config.Storage.PostgresMode
	if pMode == "tick" {
		return nil // Operating in tick-only mode, bypass aggregated insert
	}

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

			query := fmt.Sprintf(`
				INSERT INTO %s (symbol, start_time, end_time, open, high, low, close, volume, price_percent_change, volume_percent_change)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT DO NOTHING
			`, tableName)

			stmt, err := tx.Prepare(query)
			if err != nil {
				return err
			}

			for _, agg := range items {
				_, err = stmt.Exec(agg.Symbol, agg.StartTime, agg.EndTime, agg.Open, agg.High, agg.Low, agg.Close, agg.Volume, agg.PricePercentChange, agg.VolumePercentChange)
				if err != nil {
					stmt.Close()
					return err
				}
			}
			stmt.Close() // close immediately after use, not deferred
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

		for _, s := range list {
			_, err = stmt.Exec(s.Symbol, s.WindowName, s.AvgVolumeHistory, s.StdVolumeHistory, s.DataPointsHistory, s.LastHistoryTimestamp, time.Now().UTC())
			if err != nil {
				stmt.Close()
				return err
			}
		}
		stmt.Close() // close immediately after use, not deferred
	}

	return tx.Commit()
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) CleanupOldData() error {
	// TimescaleDB's native retention policies handle this securely and automatically.
	// Because Initialize() enforces that TimescaleDB must be active, manual deletes are not needed.
	return nil
}

// -----------------------------------------------------------------------------

func (d *PostgresDB) Close() error {
	if d.DB != nil {
		return d.DB.Close()
	}
	return nil
}
