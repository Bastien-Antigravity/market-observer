package main

import (
	"fmt"
	"os"

	"market-observer/src/analysis"
	datasource "market-observer/src/data_source"
	"market-observer/src/data_source/trading_view"
	"market-observer/src/data_source/yahoo"
	"market-observer/src/interfaces"
	"market-observer/src/models"
	"market-observer/src/network"
	"market-observer/src/storage"
)

// -----------------------------------------------------------------------------

// setupDatabase initializes the database connection based on config
func setupDatabase(config *models.MConfig, appLogger interfaces.Logger) (interfaces.IDatabase, error) {
	var db interfaces.IDatabase
	var err error

	switch config.Storage.DBType {
	case "postgres":
		db, err = storage.NewPostgresDB(config, appLogger)
	default:
		// Default to SQLite
		db, err = storage.NewAsyncSQLiteDB(config, appLogger)
	}

	if err != nil {
		appLogger.Critical(fmt.Sprintf("Failed to init db: %v", err))
		return nil, err
	}
	if err := db.Initialize(); err != nil {
		appLogger.Critical(fmt.Sprintf("Failed to migrate db: %v", err))
		return nil, err
	}
	return db, nil
}

// -----------------------------------------------------------------------------

// setupNetwork initializes the network manager
func setupNetwork(config *models.MConfig, log interfaces.Logger) interfaces.INetworkManager {
	return network.NewAsyncNetworkManager(config, log)
}

// -----------------------------------------------------------------------------

// setupDataSources initializes data sources and wraps them in a manager
func setupDataSources(config *models.MConfig, appLogger interfaces.Logger, networkManage interfaces.INetworkManager) (interfaces.IDataSource, *datasource.MultiSourceManager, error) {
	var sources []interfaces.IDataSource
	appLogger.Info("Initializing data sources...")

	// Symbol Filtering
	for _, srcCfg := range config.DataSource.Sources {
		if !srcCfg.Enabled {
			continue
		}

		var s interfaces.IDataSource

		switch srcCfg.Type {
		case "yahoo":
			if len(srcCfg.Symbols) > 0 {
				s = yahoo.NewYahooFinanceSource(config, srcCfg, networkManage, appLogger)
			} else {
				appLogger.Info(fmt.Sprintf("Source %s: No classic symbols to fetch from provider.", srcCfg.Name))
				continue
			}
		case "nats":
			s = datasource.NewNATSDataSource(config, srcCfg, appLogger)
		case "trading_view":
			s = trading_view.NewTradingViewSource(config, srcCfg, appLogger)
		default:
			appLogger.Warning(fmt.Sprintf("Unknown source type in config: %s", srcCfg.Name))
			continue
		}

		sources = append(sources, s)
		appLogger.Info(fmt.Sprintf("Added source: %s with %d symbols (IsRealTime: %v)", srcCfg.Name, len(srcCfg.Symbols), s.IsRealTime()))
	}

	if len(sources) == 0 {
		appLogger.Critical("No valid data sources initialized. Exiting.")
		return nil, nil, fmt.Errorf("no valid data sources")
	}

	// Verify all sources have compatible IsRealTime settings
	isRealTimeRef := sources[0].IsRealTime()
	for i, s := range sources {
		if s.IsRealTime() != isRealTimeRef {
			appLogger.Critical(fmt.Sprintf("Source incompatibility detected! Source %d starts with %v but ref is %v", i, s.IsRealTime(), isRealTimeRef))
			os.Exit(1)
		}
	}

	// Always use MultiSourceManager
	appLogger.Info(fmt.Sprintf("Initializing MultiSourceManager for %d sources.", len(sources)))
	multiSource := datasource.NewMultiSourceManager(sources, appLogger)
	return multiSource, multiSource, nil
}

// -----------------------------------------------------------------------------

// setupAnalysis initializes the analysis facade
func setupAnalysis(config *models.MConfig, log interfaces.Logger) *analysis.AnalysisFacade {
	return analysis.NewAnalysisFacade(config, log)
}
