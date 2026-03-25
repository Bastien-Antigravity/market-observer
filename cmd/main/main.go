package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"

	"market-observer/src/config"
	"market-observer/src/helpers"
	"market-observer/src/models"
	"market-observer/src/server"
	"market-observer/src/utils"

	distributed_config "github.com/Bastien-Antigravity/distributed-config"
	"github.com/Bastien-Antigravity/flexible-logger/src/profiles"
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

	// 3. Setup Logger (using HighPerfLogger for main app)
	dConfig := distributed_config.New("standalone")
	appLogger := profiles.NewHighPerfLogger(conf.Name, dConfig)
	defer appLogger.Close()

	// 4. Setup Components
	db, err := setupDatabase(conf.MConfig, appLogger)
	if err != nil {
		os.Exit(1)
	}

	networkManager := setupNetwork(conf.MConfig, appLogger)
	source, multiSource, err := setupDataSources(conf.MConfig, appLogger, networkManager)
	if err != nil {
		os.Exit(1)
	}

	analyzer := setupAnalysis(conf.MConfig, appLogger)
	srv := server.NewFastAPIServer(conf.MConfig, appLogger)

	// 5. Memory Manager
	maxPoints := utils.CalculateMaxDataPoints(conf.DataSource.DataRetentionDays)
	memLimit := helpers.GetRecommendedMemoryLimit()
	appLogger.Info(fmt.Sprintf("Memory Limit set to: %d MB", memLimit))
	memManager := utils.NewMemoryManager(memLimit, maxPoints, appLogger)

	// 6. Bootstrap (Initial Load)
	initialPayload, intermediateStats, err := performInitialLoad(source, db, analyzer, memManager, conf.MConfig, appLogger)
	if err != nil {
		appLogger.Warning(fmt.Sprintf("Bootstrap completed with warnings: %v", err))
	}

	// 7. Update Server State with Initial Data
	srv.UpdateAllDatas(initialPayload)

	// 8. Start Servers
	startServers(srv, multiSource, conf, *configPath, appLogger, networkManager)

	// 9. Run Main Processing Loop
	appLogger.Info("Starting Main Data Loop...")

	// Lifecycle Management
	ctx, cancel := context.WithCancel(context.Background())
	// Moved cancel to the deferred anonymous function below to avoid double-call logic
	// defer cancel() // Ensure cleanup

	var wg sync.WaitGroup
	updatesChan := make(chan map[string][]models.MStockPrice, 500)

	// Start Sources (Context-Based Direct Push)
	if err := multiSource.Start(ctx, updatesChan, &wg); err != nil {
		appLogger.Critical(fmt.Sprintf("Failed to start data sources: %v", err))
	}

	// Wait for cleanup on exit
	defer func() {
		appLogger.Info("Initiating shutdown...")
		cancel() // Signal all components (sources, etc.) to stop

		appLogger.Info("Waiting for background processes to exit...")
		wg.Wait() // Wait for sources to confirm stoppage

		appLogger.Info("Shutdown complete.")
	}()

	// Run Loop (Blocking)
	runDataLoop(updatesChan, db, analyzer, memManager, srv, conf.MConfig, appLogger, intermediateStats)
}
