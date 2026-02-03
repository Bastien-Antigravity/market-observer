package main

import (
	"fmt"
	"os"

	"market-observer/src/analysis"
	datasource "market-observer/src/data_source"
	"market-observer/src/data_source/yahoo"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
	"market-observer/src/network"
	"market-observer/src/storage"
)

// -----------------------------------------------------------------------------

// setupDatabase initializes the database connection based on config
func setupDatabase(config *models.MConfig, appLogger *logger.Logger) (interfaces.IDatabase, error) {
	var db interfaces.IDatabase
	var err error

	switch config.Storage.DBType {
	case "postgres":
		pgLogger := logger.NewLogger(config, "PostgresDB")
		db, err = storage.NewPostgresDB(config, pgLogger)
	default:
		// Default to SQLite
		sqliteLogger := logger.NewLogger(config, "SQLiteDB")
		db, err = storage.NewAsyncSQLiteDB(config, sqliteLogger)
	}

	if err != nil {
		appLogger.Critical("Failed to init db: %v", err)
		return nil, err
	}
	if err := db.Initialize(); err != nil {
		appLogger.Critical("Failed to migrate db: %v", err)
		return nil, err
	}
	return db, nil
}

// -----------------------------------------------------------------------------

// setupNetwork initializes the network manager
func setupNetwork(config *models.MConfig) interfaces.INetworkManager {
	networkLogger := logger.NewLogger(config, "NetworkManager")
	return network.NewAsyncNetworkManager(config, networkLogger)
}

// -----------------------------------------------------------------------------

// setupDataSources initializes data sources and wraps them in a manager
func setupDataSources(config *models.MConfig, appLogger *logger.Logger, networkManage interfaces.INetworkManager) (interfaces.IDataSource, *datasource.MultiSourceManager, error) {
	var sources []interfaces.IDataSource
	appLogger.Info("Initializing data sources...")

	// Symbol Filtering
	for _, srcCfg := range config.DataSource.Sources {
		if len(srcCfg.Symbols) > 0 {
			if srcCfg.Type == "yahoo" {
				s := yahoo.NewYahooFinanceSource(config, srcCfg, networkManage)
				sources = append(sources, s)
				appLogger.Info("Added source: %s with %d symbols (IsRealTime: %v)", srcCfg.Name, len(srcCfg.Symbols), s.IsRealTime())
			} else {
				appLogger.Warning("Unknown source type in config: %s", srcCfg.Name)
			}
		} else {
			appLogger.Info("Source %s: No classic symbols to fetch from provider.", srcCfg.Name)
		}
	}

	if len(sources) == 0 {
		appLogger.Critical("No valid data sources initialized. Exiting.")
		return nil, nil, fmt.Errorf("no valid data sources")
	}

	// Verify all sources have compatible IsRealTime settings
	isRealTimeRef := sources[0].IsRealTime()
	for i, s := range sources {
		if s.IsRealTime() != isRealTimeRef {
			appLogger.Critical("Source incompatibility detected! Source %d starts with %v but ref is %v", i, s.IsRealTime(), isRealTimeRef)
			os.Exit(1)
		}
	}

	// Always use MultiSourceManager
	appLogger.Info("Initializing MultiSourceManager for %d sources.", len(sources))
	multiSource := datasource.NewMultiSourceManager(sources, appLogger)
	return multiSource, multiSource, nil
}

// -----------------------------------------------------------------------------

// setupAnalysis initializes the analysis facade
func setupAnalysis(config *models.MConfig) *analysis.AnalysisFacade {
	analysisLogger := logger.NewLogger(config, "Analysis")
	return analysis.NewAnalysisFacade(config, analysisLogger)
}
