package analysis

import (
	"market-observer/src/logger"
	"sort"
	"time"

	"market-observer/src/analysis/core"
	"market-observer/src/models"
)

type AnalysisFacade struct {
	Config            *models.MConfig
	WindowsSecondsMap map[string]int64 // Need to add this to config
	Logger            *logger.Logger
}

// -----------------------------------------------------------------------------

func NewAnalysisFacade(cfg *models.MConfig, log *logger.Logger) *AnalysisFacade {
	// Initialize window mapping (should come from config)
	windowsMap := make(map[string]int64)
	for _, window := range cfg.WindowsAgg {
		if dur, err := time.ParseDuration(window); err == nil {
			windowsMap[window] = int64(dur.Seconds())
		}
	}

	return &AnalysisFacade{
		Config:            cfg,
		WindowsSecondsMap: windowsMap,
		Logger:            log,
	}
}

// -----------------------------------------------------------------------------

// AggregateRealTime aggregates real-time data for the current aligned window.
// It uses full history to calculate changes relative to the previous aligned window.
func (a *AnalysisFacade) AggregateRealTime(
	data map[string][]models.MStockPrice,
	windowName string,
	intermediateStats map[string]models.MIntermediateStats,
) map[string]map[string]models.MAggregation {

	results := make(map[string]map[string]models.MAggregation)

	windowSeconds, ok := a.WindowsSecondsMap[windowName]
	if !ok {
		a.Logger.Error("Invalid window name %s", windowName)
		return results
	}

	for symbol, prices := range data {
		if len(prices) == 0 {
			continue
		}

		// Sort by timestamp
		sort.Slice(prices, func(i, j int) bool {
			return prices[i].Timestamp < prices[j].Timestamp
		})

		// 1. Identify the Current Aligned Window based on the LATEST data point
		lastPt := prices[len(prices)-1]
		currentWStart := lastPt.Timestamp - (lastPt.Timestamp % windowSeconds)
		currentWEnd := currentWStart + windowSeconds
		prevWStart := currentWStart - windowSeconds

		// 2. Partition data into Current and Previous windows
		var currentSubset []models.MStockPrice
		var prevSubset []models.MStockPrice

		for _, p := range prices {
			if p.Timestamp >= currentWStart && p.Timestamp < currentWEnd {
				currentSubset = append(currentSubset, p)
			} else if p.Timestamp >= prevWStart && p.Timestamp < currentWStart {
				prevSubset = append(prevSubset, p)
			}
		}

		if len(currentSubset) == 0 {
			continue
		}

		// 3. Process Current Window
		pricesArr := make([]float64, len(currentSubset))
		volsArr := make([]float64, len(currentSubset))
		timestamps := make([]int64, len(currentSubset))
		totalVol := 0.0

		for i, p := range currentSubset {
			pricesArr[i] = p.Price
			volsArr[i] = p.Volume
			timestamps[i] = p.Timestamp
			totalVol += p.Volume
		}

		ohlcv := core.ComputeOHLCV(pricesArr, volsArr)
		corr := core.CalculateCorrelation(pricesArr, volsArr)

		// 4. Stats & Anomaly
		avgVol := 1.0
		if stat, ok := intermediateStats[symbol]; ok {
			avgVol = stat.AvgVolumeHistory
		}
		anomaly := core.CalculateAnomalyRatio(totalVol, avgVol)

		// 5. Calculate Changes vs Previous Window
		pctChange := 0.0
		volPct := 0.0

		if len(prevSubset) > 0 {
			// Calculate Prev Window Close & Volume
			// (Optimization: We don't need full OHLCV, just Close and Sum Volume)
			prevClose := prevSubset[len(prevSubset)-1].Price
			prevVolTotal := 0.0
			for _, p := range prevSubset {
				prevVolTotal += p.Volume
			}

			pctChange = core.CalculateChangePercent(ohlcv["close"], prevClose)
			volPct = core.CalculateChangePercent(ohlcv["volume"], prevVolTotal)

		} else {
			// Fallback if no previous window in memory (start of day/buffer)
			// Compare to Open of current window ?? Or just 0.
			pctChange = core.CalculateChangePercent(ohlcv["close"], ohlcv["open"])
		}

		// 6. Construct Aggregation
		agg := models.MAggregation{
			Symbol:                 symbol,
			WindowName:             windowName,
			Open:                   ohlcv["open"],
			High:                   ohlcv["high"],
			Low:                    ohlcv["low"],
			Close:                  ohlcv["close"],
			Volume:                 ohlcv["volume"],
			AvgPrice:               ohlcv["avg_price"],
			PricePercentChange:     pctChange,
			VolumePercentChange:    volPct,
			PriceVolumeCorrelation: corr,
			VolumeAnomalyRatio:     anomaly,
			StartTime:              currentWStart,
			EndTime:                currentWEnd,
			DataPoints:             len(currentSubset),
		}

		results[symbol] = map[string]models.MAggregation{
			windowName: agg,
		}
	}

	return results
}

// -----------------------------------------------------------------------------

// AggregateHistorical aggregates entire history (matching Python's aggregate_initial)
func (a *AnalysisFacade) AggregateHistorical(
	data map[string][]models.MStockPrice,
	windowName string,
	intermediateStats map[string]models.MIntermediateStats,
) map[string]map[string][]models.MAggregation {

	results := make(map[string]map[string][]models.MAggregation)

	windowSeconds, ok := a.WindowsSecondsMap[windowName]
	if !ok {
		return results
	}

	for symbol, prices := range data {
		if len(prices) == 0 {
			continue
		}

		// Sort by timestamp
		sort.Slice(prices, func(i, j int) bool {
			return prices[i].Timestamp < prices[j].Timestamp
		})

		// Resample into windows
		windows := make(map[int64][]models.MStockPrice)
		for _, p := range prices {
			wStart := p.Timestamp - (p.Timestamp % windowSeconds)
			windows[wStart] = append(windows[wStart], p)
		}

		// Get window starts sorted
		var windowStarts []int64
		for wStart := range windows {
			windowStarts = append(windowStarts, wStart)
		}
		sort.Slice(windowStarts, func(i, j int) bool {
			return windowStarts[i] < windowStarts[j]
		})

		var candles []models.MAggregation
		avgVol := 1.0
		if stat, ok := intermediateStats[symbol]; ok {
			avgVol = stat.AvgVolumeHistory
		}

		var prevClose, prevVolume float64
		prevCloseSet := false

		for _, wStart := range windowStarts {
			subset := windows[wStart]
			if len(subset) == 0 {
				continue
			}

			// Prepare arrays
			pricesArr := make([]float64, len(subset))
			volsArr := make([]float64, len(subset))

			totalVol := 0.0
			for i, p := range subset {
				pricesArr[i] = p.Price
				volsArr[i] = p.Volume
				totalVol += p.Volume
			}

			// Calculate metrics
			ohlcv := core.ComputeOHLCV(pricesArr, volsArr)
			corr := core.CalculateCorrelation(pricesArr, volsArr)
			anomaly := core.CalculateAnomalyRatio(totalVol, avgVol)

			// Calculate changes from previous window
			pctChange := 0.0
			volChange := 0.0
			if prevCloseSet {
				pctChange = core.CalculateChangePercent(ohlcv["close"], prevClose)
				volChange = core.CalculateChangePercent(ohlcv["volume"], prevVolume)
			}

			// Create candle
			candle := models.MAggregation{
				Symbol:                 symbol,
				WindowName:             windowName,
				Open:                   ohlcv["open"],
				High:                   ohlcv["high"],
				Low:                    ohlcv["low"],
				Close:                  ohlcv["close"],
				Volume:                 ohlcv["volume"],
				AvgPrice:               ohlcv["avg_price"],
				PricePercentChange:     pctChange,
				VolumePercentChange:    volChange, // Different from real-time!
				PriceVolumeCorrelation: corr,
				VolumeAnomalyRatio:     anomaly,
				StartTime:              wStart,
				EndTime:                wStart + windowSeconds,
				DataPoints:             len(subset),
			}

			candles = append(candles, candle)
			prevClose = ohlcv["close"]
			prevVolume = ohlcv["volume"]
			prevCloseSet = true
		}

		if len(candles) > 0 {
			results[symbol] = map[string][]models.MAggregation{
				windowName: candles,
			}
		}
	}

	return results
}

// -----------------------------------------------------------------------------

// CalculateStatsForWindows calculates stats for multiple windows (matching Python)
func (a *AnalysisFacade) CalculateStatsForWindows(
	data map[string][]models.MStockPrice,
	windowNames []string,
) map[string]map[string]models.MIntermediateStats {

	results := make(map[string]map[string]models.MIntermediateStats)

	// Filter valid windows
	targetWindows := make([]string, 0)
	for _, wn := range windowNames {
		if _, ok := a.WindowsSecondsMap[wn]; ok {
			targetWindows = append(targetWindows, wn)
		}
	}

	for symbol, prices := range data {
		if len(prices) == 0 {
			continue
		}

		// Sort by timestamp
		sort.Slice(prices, func(i, j int) bool {
			return prices[i].Timestamp < prices[j].Timestamp
		})

		symbolStats := make(map[string]models.MIntermediateStats)

		for _, windowName := range targetWindows {
			windowSeconds := a.WindowsSecondsMap[windowName]

			// Resample into windows
			windows := make(map[int64]float64)
			for _, p := range prices {
				wStart := p.Timestamp - (p.Timestamp % windowSeconds)
				windows[wStart] += p.Volume
			}

			// Collect volumes
			var vols []float64
			for _, vol := range windows {
				vols = append(vols, vol)
			}

			if len(vols) == 0 {
				continue
			}

			// Calculate stats
			mean, std := core.CalculateMeanStd(vols)

			symbolStats[windowName] = models.MIntermediateStats{
				Symbol:               symbol,
				WindowName:           windowName,
				AvgVolumeHistory:     mean,
				StdVolumeHistory:     std,
				DataPointsHistory:    len(vols),
				LastHistoryTimestamp: prices[len(prices)-1].Timestamp,
			}
		}

		if len(symbolStats) > 0 {
			results[symbol] = symbolStats
		}
	}

	return results
}

// -----------------------------------------------------------------------------

// Helper method matching Python's convert_to_numpy_matrix
func ConvertToMatrix(data []models.MStockPrice) [][]float64 {
	if len(data) == 0 {
		return [][]float64{}
	}

	// Sort by timestamp
	sort.Slice(data, func(i, j int) bool {
		return data[i].Timestamp < data[j].Timestamp
	})

	matrix := make([][]float64, len(data))
	for i, p := range data {
		matrix[i] = []float64{float64(p.Timestamp), p.Price, p.Volume}
	}

	return matrix
}
