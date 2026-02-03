package main

import (
	"market-observer/src/analysis"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
	"market-observer/src/utils"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// -----------------------------------------------------------------------------

// runDataLoop handles the main data processing loop (direct push model)
func runDataLoop(
	updatesChan <-chan map[string][]models.MStockPrice,
	db interfaces.IDatabase,
	analyzer *analysis.AnalysisFacade,
	memManager *utils.MemoryManager,
	srv interfaces.IDataExchanger,
	config *models.MConfig,
	appLogger *logger.Logger,
	intermediateStats map[string]map[string]models.MIntermediateStats, // State carried over
) {

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

			startProcess := time.Now().UTC()
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

			// Construct map with FULL history for Updated Symbols
			// This ensures AggregateRealTime has enough data points to calculate Correlation/Anomaly
			fullHistoryMap := make(map[string][]models.MStockPrice)
			for sym := range updates {
				// Get full history from RingBuffer
				if buffer := memManager.GetBuffer(sym); buffer != nil {
					fullHistoryMap[sym] = buffer.GetAll()
				}
			}

			// Aggregate Realtime using FULL history

			accumulatedAggs := make(map[string]map[string][]models.MAggregation)

			for _, w := range config.WindowsAgg {
				currentWindowStats := make(map[string]models.MIntermediateStats)
				for sym, wMap := range intermediateStats {
					if s, ok := wMap[w]; ok {
						currentWindowStats[sym] = s
					}
				}

				// Pass fullHistoryMap instead of updates
				wAggs := analyzer.AggregateRealTime(fullHistoryMap, w, currentWindowStats)

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
				"aggregations": accumulatedAggs, // Only new candles
				"timestamp":    time.Now().UTC().Unix(),
				"processing_metrics": models.MProcessingMetrics{
					AggregationTimeSeconds: elapsed,
					ValidSymbols:           len(updates),
					WindowsProcessed:       len(config.WindowsAgg),
				},
			}

			srv.UpdateAllDatas(payload)
			srv.Broadcast(payload)

			// Cleanup
			db.CleanupOldData()

		case <-quit:
			appLogger.Info("Shutting down...")
			return
		}
	}
}
