package main

import (
	"flag"
	"fmt"
	"os"

	"market-observer/src/config"
	"market-observer/src/logger"
	"market-observer/src/server"
	"market-observer/src/utils"
)

func main() {
	// Parse command line flags
	configPath := flag.String("config", "../../config/default.yaml", "path to config file")
	flag.Parse()

	// Load config from YAML file
	conf, err := config.NewConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	appLogger := logger.NewLogger(conf, conf.Name)

	// Setup Database
	db, err := setupDatabase(conf.MConfig, appLogger)
	if err != nil {
		os.Exit(1)
	}

	// Setup Network
	networkManager := setupNetwork(conf.MConfig, appLogger)

	// Setup Data Source
	source, multiSource, err := setupDataSources(conf.MConfig, appLogger, networkManager)
	if err != nil {
		os.Exit(1)
	}

	// Setup Analysis
	analyzer := setupAnalysis(conf.MConfig, appLogger)

	// Setup WebSocket Server (FastAPI)
	srv := server.NewFastAPIServer(conf.MConfig, appLogger)
	go func() {
		if err := srv.Start(); err != nil {
			appLogger.Error("Server failed: %v", err)
		}
	}()

	// Setup Control Servers (gRPC and REST)
	startServers(conf, *configPath, appLogger, multiSource, networkManager)

	// Setup Memory Manager
	maxPoints := utils.CalculateMaxDataPoints(conf.DataSource.DataRetentionDays)
	memManager := utils.NewMemoryManager(512, maxPoints)

	// Perform Initial Load
	intermediateStats, err := performInitialLoad(source, db, analyzer, memManager, conf.MConfig, appLogger, srv)
	if err != nil {
		appLogger.Warning("Failed to perform initial load fully: %v", err)
	}

	// Start Data Processing Loop
	runDataLoop(source, db, analyzer, memManager, conf.MConfig, appLogger, srv, intermediateStats)
}
