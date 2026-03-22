package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"market-observer/src/analysis"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
	"market-observer/src/utils"
)

// runDataLoop runs the data ingestion bounds
func runDataLoop(
	source interfaces.IDataSource,
	db interfaces.IDatabase,
	analyzer *analysis.AnalysisFacade,
	memManager *utils.MemoryManager,
	config *models.MConfig,
	appLogger *logger.Logger,
	srv interfaces.IDataExchanger,
	intermediateStats map[string]map[string]models.MIntermediateStats,
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wrapWg := &sync.WaitGroup{}
	updatesChan := make(chan map[string][]models.MStockPrice, 100)

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

			var newRaw []models.MStockPrice

			for sym, data := range updates {
				newRaw = append(newRaw, data...)
				for _, p := range data {
					memManager.AddDataPoint(sym, p)
				}
			}
			db.SaveStockPricesBulk(newRaw)

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

			db.CleanupOldData()

		case <-quit:
			appLogger.Info("Shutting down...")
			cancel()
			wrapWg.Wait()
			return
		}
	}
}
