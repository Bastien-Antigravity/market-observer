package main

import (
	"context"
	"flag"
	"fmt"
	"market-observer/src/config"
	"market-observer/src/helpers"
	"market-observer/src/logger"
	"market-observer/src/models"
	"market-observer/src/server"
	"market-observer/src/utils"
	"os"
	"sync"
)

func main() {
	// 1. Parse command line flags
	configPath := flag.String("config", "../../config/default.yaml", "path to config file")
	flag.Parse()

	// 2. Load config
	conf, err := config.NewConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// 3. Setup Logger
	appLogger := logger.NewLogger(conf, conf.Name)

	// 4. Setup Components
	db, err := setupDatabase(conf.MConfig, appLogger)
	if err != nil {
		os.Exit(1)
	}

	networkManager := setupNetwork(conf.MConfig)
	source, multiSource, err := setupDataSources(conf.MConfig, appLogger, networkManager)
	if err != nil {
		os.Exit(1)
	}

	analyzer := setupAnalysis(conf.MConfig)
	srv := server.NewFastAPIServer(conf.MConfig, appLogger)

	// 5. Memory Manager
	maxPoints := utils.CalculateMaxDataPoints(conf.DataSource.DataRetentionDays)
	memLimit := helpers.GetRecommendedMemoryLimit()
	appLogger.Info("Memory Limit set to: %d MB", memLimit)
	memManager := utils.NewMemoryManager(memLimit, maxPoints)

	// 6. Bootstrap (Initial Load)
	initialPayload, intermediateStats, err := performInitialLoad(source, db, analyzer, memManager, conf.MConfig, appLogger)
	if err != nil {
		appLogger.Warning("Bootstrap completed with warnings: %v", err)
	}

	// 7. Update Server State with Initial Data
	srv.UpdateAllDatas(initialPayload)

	// 8. Start Servers
	startServers(srv, multiSource, conf, *configPath, appLogger, networkManager)

	// 9. Run Main Processing Loop
	appLogger.Info("Starting Main Data Loop...")

	// Lifecycle Management
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure cleanup

	var wg sync.WaitGroup
	updatesChan := make(chan map[string][]models.MStockPrice, 500)

	// Start Sources (Context-Based Direct Push)
	if err := multiSource.Start(ctx, updatesChan, &wg); err != nil {
		appLogger.Critical("Failed to start data sources: %v", err)
	}

	// Wait for cleanup on exit
	defer func() {
		appLogger.Info("Waiting for sources to stop...")
		cancel()  // Signal stop
		wg.Wait() // Wait for sources
		close(updatesChan)
		appLogger.Info("Shutdown complete.")
	}()

	// Run Loop (Blocking)
	runDataLoop(updatesChan, db, analyzer, memManager, srv, conf.MConfig, appLogger, intermediateStats)
}
