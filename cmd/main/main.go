package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"market-observer/src/analysis"
	"market-observer/src/config"
	"market-observer/src/data_source/yahoo"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
	"market-observer/src/network"
	"market-observer/src/server"
	"market-observer/src/storage"
	"market-observer/src/utils"
)

// -----------------------------------------------------------------------------

func main() {
	// Parse command line flags
	configPath := flag.String("config", "../../config/default.yaml", "path to config file")
	flag.Parse()

	// Load config from YAML file
	config, err := config.NewConfig(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Setup logger
	appLogger := logger.NewLogger(config, config.Name)

	// 2. Setup Components
	var db interfaces.IDatabase

	switch config.Storage.DBType {
	case "postgres":
		db, err = storage.NewPostgresDB(config.MConfig, appLogger)
	default:
		// Default to SQLite
		db, err = storage.NewAsyncSQLiteDB(config.MConfig, appLogger)
	}

	if err != nil {
		appLogger.Critical("Failed to init db: %v", err)
	}
	if err := db.Initialize(); err != nil {
		appLogger.Critical("Failed to migrate db: %v", err)
	}

	// 3. Setup Components
	var networkManage interfaces.INetworkManager = network.NewAsyncNetworkManager(config.MConfig, appLogger)

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
		source = yahoo.NewYahooFinanceSource(config.MConfig, *activeSourceConfig, networkManage)
	default:
		appLogger.Critical("Unknown data source type: %s for source: %s", sourceType, activeSourceConfig.Name)
		os.Exit(1)
	}

	appLogger.Info("Initialized Data Source: %s (Type: %s)", activeSourceConfig.Name, sourceType)
	var analyzer *analysis.AnalysisFacade = analysis.NewAnalysisFacade(config.MConfig, appLogger)
	var srv interfaces.IDataExchanger = server.NewFastAPIServer(config.MConfig, appLogger)

	// 4. Memory Manager
	maxPoints := utils.CalculateMaxDataPoints(config.DataSource.DataRetentionDays)
	memManager := utils.NewMemoryManager(512, maxPoints) // Hardcoded 512MB as config removed it

	// 5. Initial Data Load
	appLogger.Info("Fetching initial data...")
	initialData, err := source.FetchInitialData()
	if err != nil {
		appLogger.Warning("Initial fetch failed: %v", err)
	}

	// 6. Populate Memory Manager with initial data
	for sym, dataList := range initialData {
		for _, p := range dataList {
			memManager.AddDataPoint(sym, p)
		}
	}

	// 7. Initial Processing and Aggregation
	intermediateStats := make(map[string]map[string]models.MIntermediateStats)
	// Accumulate aggregations for server state (Strongly Typed)
	initialAggsForServer := make(map[string]map[string][]models.MAggregation)

	// Process per window
	wStatsMap := analyzer.CalculateStatsForWindows(initialData, config.WindowsAgg)

	for _, w := range config.WindowsAgg {
		// Save stats
		var statsList []models.MIntermediateStats

		// Extract stats for this window from the bulk result
		for sym := range initialData {
			if symStats, ok := wStatsMap[sym]; ok {
				if s, ok := symStats[w]; ok {
					statsList = append(statsList, s)
					if intermediateStats[s.Symbol] == nil {
						intermediateStats[s.Symbol] = make(map[string]models.MIntermediateStats)
					}
					intermediateStats[s.Symbol][w] = s
				}
			}
		}

		if len(statsList) > 0 {
			db.SaveIntermediateStats(statsList)
		}

		// 8. Initial Aggregation
		// Extract stats for this window
		currentWindowStats := make(map[string]models.MIntermediateStats)
		for sym, wMap := range intermediateStats {
			if s, ok := wMap[w]; ok {
				currentWindowStats[sym] = s
			}
		}

		aggs := analyzer.AggregateHistorical(initialData, w, currentWindowStats)

		// Save Aggs & Buffer for Server
		aggMap := make(map[string]map[string][]models.MAggregation)
		for sym, innerMap := range aggs {
			if aggMap[sym] == nil {
				aggMap[sym] = make(map[string][]models.MAggregation)
			}
			if initialAggsForServer[sym] == nil {
				initialAggsForServer[sym] = make(map[string][]models.MAggregation)
			}

			if candles, ok := innerMap[w]; ok {
				aggMap[sym][w] = append(aggMap[sym][w], candles...)

				// Capture Latest Candle for server state
				if len(candles) > 0 {
					latest := candles[len(candles)-1]
					initialAggsForServer[sym][w] = []models.MAggregation{latest}
				}
			}
		}
		db.SaveAggregations(aggMap)
	}

	// 9. Save Raw Data (Bulk)
	var allRaw []models.MStockPrice
	for _, list := range initialData {
		allRaw = append(allRaw, list...)
	}
	db.SaveStockPricesBulk(allRaw)

	appLogger.Info("Initialization complete.")

	// -------------------------------------------------------------------------
	// Send Initial Data to Server State
	// -------------------------------------------------------------------------
	initialRawMap := make(map[string]interface{})
	for sym, list := range initialData {
		if len(list) > 0 {
			initialRawMap[sym] = list[len(list)-1]
		}
	}

	initialPayload := map[string]interface{}{
		"type":               "INITIAL", // Mark as INITIAL
		"raw_data":           initialRawMap,
		"aggregations":       initialAggsForServer,
		"timestamp":          time.Now().Unix(),
		"processing_metrics": map[string]interface{}{},
	}
	srv.UpdateAllDatas(initialPayload)
	// -------------------------------------------------------------------------

	// 10. Start Server (FastAPIServer with ported endpoints)
	go func() {
		if err := srv.Start(); err != nil {
			appLogger.Error("Server failed: %v", err)
		}
	}()

	// 12. Main Loop (Push Model)
	// Create context for cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wrapWg := &sync.WaitGroup{}
	updatesChan := make(chan map[string][]models.MStockPrice, 100)

	// Start Source
	if err := source.Start(ctx, updatesChan, wrapWg); err != nil {
		appLogger.Critical("Failed to start source: %v", err)
		return
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	appLogger.Info("Starting data loop (Push Model)...")

	for {
		select {
		case updates, ok := <-updatesChan:
			if !ok {
				appLogger.Info("Data source closed channel.")
				return
			}

			startProcess := time.Now()
			appLogger.Info("Received update for %d symbols", len(updates))

			// Process Updates
			var newRaw []models.MStockPrice

			for sym, data := range updates {
				newRaw = append(newRaw, data...)
				// Update Memory Manager
				for _, p := range data {
					memManager.AddDataPoint(sym, p)
				}
			}
			db.SaveStockPricesBulk(newRaw)

			// Aggregate Realtime
			accumulatedAggs := make(map[string]map[string][]models.MAggregation)

			totalWindows := 0
			for _, w := range config.WindowsAgg {
				currentWindowStats := make(map[string]models.MIntermediateStats)
				for sym, wMap := range intermediateStats {
					if s, ok := wMap[w]; ok {
						currentWindowStats[sym] = s
					}
				}

				wAggs := analyzer.AggregateRealTime(updates, w, currentWindowStats)
				totalWindows += len(wAggs)

				// Save
				aggMap := make(map[string]map[string][]models.MAggregation)
				for sym, innerMap := range wAggs {
					if aggMap[sym] == nil {
						aggMap[sym] = make(map[string][]models.MAggregation)
					}
					if candle, ok := innerMap[w]; ok {
						aggMap[sym][w] = append(aggMap[sym][w], candle)
					}
				}
				db.SaveAggregations(aggMap)

				// Accumulate for Broadcast
				for sym, innerMap := range wAggs {
					if _, ok := accumulatedAggs[sym]; !ok {
						accumulatedAggs[sym] = make(map[string][]models.MAggregation)
					}

					if candle, ok := innerMap[w]; ok {
						accumulatedAggs[sym][w] = []models.MAggregation{candle}
					}
				}
			}

			elapsed := time.Since(startProcess).Seconds()

			// Broadcast
			rawInterfaceMap := make(map[string]interface{})
			for k, v := range updates {
				rawInterfaceMap[k] = v
			}

			payload := map[string]interface{}{
				"type":         "UPDATE",
				"raw_data":     rawInterfaceMap,
				"aggregations": accumulatedAggs,
				"timestamp":    time.Now().Unix(),
				"processing_metrics": models.MProcessingMetrics{
					AggregationTimeSeconds: elapsed,
					ValidSymbols:           len(updates),
					WindowsProcessed:       totalWindows,
				},
			}

			srv.UpdateAllDatas(payload)
			srv.Broadcast(payload)

			// Cleanup
			db.CleanupOldData()

		case <-quit:
			appLogger.Info("Shutting down...")
			cancel()      // Signal source to stop
			wrapWg.Wait() // Wait for source to close
			return
		}
	}
}
