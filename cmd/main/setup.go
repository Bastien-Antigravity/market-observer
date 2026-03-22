package main

import (
	"os"
	"strings"

	"market-observer/src/analysis"
	datasource "market-observer/src/data_source"
	"market-observer/src/data_source/trading_view"
	"market-observer/src/data_source/yahoo"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
	"market-observer/src/network"
	"market-observer/src/storage"
)

// setupDatabase initializes the database connection
func setupDatabase(config *models.MConfig, appLogger *logger.Logger) (interfaces.IDatabase, error) {
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
		appLogger.Critical("Failed to init db: %v", err)
		return nil, err
	}
	if err := db.Initialize(); err != nil {
		appLogger.Critical("Failed to migrate db: %v", err)
		return nil, err
	}
	return db, nil
}

// setupNetwork initializes the network manager
func setupNetwork(config *models.MConfig, appLogger *logger.Logger) interfaces.INetworkManager {
	return network.NewAsyncNetworkManager(config, appLogger)
}

// setupDataSources initializes the single data source requested by cmd/main
func setupDataSources(config *models.MConfig, appLogger *logger.Logger, networkManage interfaces.INetworkManager) (interfaces.IDataSource, *datasource.MultiSourceManager, error) {
	// Validate Enabled Sources (Strictly 1 Allowed)
	enabledCount := 0
	var activeSourceConfig *models.MSourceConfig

	for i := range config.DataSource.Sources {
		if config.DataSource.Sources[i].Enabled {
			enabledCount++
			activeSourceConfig = &config.DataSource.Sources[i]
		}
	}

	if enabledCount == 0 {
		appLogger.Critical("No data sources enabled in config. Please enable exactly one.")
		os.Exit(1)
	}
	if enabledCount > 1 {
		appLogger.Critical("Multiple data sources enabled (%d). Please enable exactly one for this mode.", enabledCount)
		os.Exit(1)
	}

	// Factory Pattern based on Type
	var source interfaces.IDataSource
	sourceType := strings.ToLower(activeSourceConfig.Type)
	if sourceType == "" {
		sourceType = "yahoo" // Default
	}

	switch sourceType {
	case "yahoo":
		source = yahoo.NewYahooFinanceSource(config, *activeSourceConfig, networkManage)
	case "nats":
		source = datasource.NewNATSDataSource(config, *activeSourceConfig, appLogger)
	case "trading_view":
		source = trading_view.NewTradingViewSource(config, *activeSourceConfig, appLogger)
	default:
		appLogger.Critical("Unknown data source type: %s for source: %s", sourceType, activeSourceConfig.Name)
		os.Exit(1)
	}

	appLogger.Info("Initialized Data Source: %s (Type: %s)", activeSourceConfig.Name, sourceType)

	// Wrap in MultiSourceManager for Control Services
	multiSource := datasource.NewMultiSourceManager([]interfaces.IDataSource{source}, appLogger)
	return source, multiSource, nil
}

// setupAnalysis initializes the analysis facade
func setupAnalysis(config *models.MConfig, appLogger *logger.Logger) *analysis.AnalysisFacade {
	return analysis.NewAnalysisFacade(config, appLogger)
}
