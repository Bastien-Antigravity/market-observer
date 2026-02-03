package main

import (
	"market-observer/src/analysis"
	"market-observer/src/interfaces"
	"market-observer/src/logger"
	"market-observer/src/models"
	"market-observer/src/utils"
	"time"
)

// performInitialLoad fetches initial data and prepares the system state
func performInitialLoad(
	source interfaces.IDataSource,
	db interfaces.IDatabase,
	analyzer *analysis.AnalysisFacade,
	memManager *utils.MemoryManager,
	config *models.MConfig,
	appLogger *logger.Logger,
) (map[string]interface{}, map[string]map[string]models.MIntermediateStats, error) {

	appLogger.Info("Fetching initial data...")
	initialData, err := source.FetchInitialData()
	if err != nil {
		appLogger.Warning("Initial fetch failed: %v", err)
		// We continue even if fail, potentially? Or return error.
		// Original code warned but continued.
	}

	// Populate Memory Manager with initial data
	for sym, dataList := range initialData {
		for _, p := range dataList {
			memManager.AddDataPoint(sym, p)
		}
	}

	// Initial Processing and Aggregation
	intermediateStats := make(map[string]map[string]models.MIntermediateStats)
	initialAggsForServer := make(map[string]map[string][]models.MAggregation)
	initialValidSymbols := len(initialData)

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

		// Initial Aggregation
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

	// Save Raw Data (Bulk)
	var allRaw []models.MStockPrice
	for _, list := range initialData {
		allRaw = append(allRaw, list...)
	}
	db.SaveStockPricesBulk(allRaw)

	appLogger.Info("Initialization complete.")

	// Construct Initial Payload
	initialRawMap := make(map[string]interface{})
	for sym, list := range initialData {
		if len(list) > 0 {
			initialRawMap[sym] = list[len(list)-1]
		}
	}

	initialPayload := map[string]interface{}{
		"type":         "INITIAL", // Mark as INITIAL
		"raw_data":     initialRawMap,
		"aggregations": initialAggsForServer,
		"timestamp":    time.Now().UTC().Unix(),
		"processing_metrics": models.MProcessingMetrics{
			AggregationTimeSeconds: 0,
			ValidSymbols:           initialValidSymbols,
			WindowsProcessed:       len(config.WindowsAgg),
		},
	}

	return initialPayload, intermediateStats, nil
}
