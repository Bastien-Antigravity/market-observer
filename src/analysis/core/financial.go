package core

import "math"

// -----------------------------------------------------------------------------

// ComputeOHLCV calculates OHLCV and AvgPrice from price/volume arrays.
func ComputeOHLCV(prices []float64, volumes []float64) map[string]float64 {
	if len(prices) == 0 {
		return map[string]float64{
			"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "avg_price": 0,
		}
	}

	open := prices[0]
	closePrice := prices[len(prices)-1]
	high := -1.0
	low := math.MaxFloat64
	totalVol := 0.0
	sumPrice := 0.0

	for i, p := range prices {
		v := volumes[i]
		if p > high {
			high = p
		}
		if p < low {
			low = p
		}
		totalVol += v
		sumPrice += p
	}

	avgPrice := 0.0
	if len(prices) > 0 {
		avgPrice = sumPrice / float64(len(prices))
	}

	return map[string]float64{
		"open":      open,
		"high":      high,
		"low":       low,
		"close":     closePrice,
		"volume":    totalVol,
		"avg_price": avgPrice,
	}
}

// -----------------------------------------------------------------------------

// CalculateChangePercent calculates percentage change.
func CalculateChangePercent(current, previous float64) float64 {
	if previous == 0 {
		return 0.0
	}
	return (current - previous) / previous
}

// -----------------------------------------------------------------------------

// CalculateAnomalyRatio computes volume anomaly
func CalculateAnomalyRatio(currentVol, avgVol float64) float64 {
	if avgVol <= 0 {
		// Match Python logic: return 1.0 if currentVol == 0, else return currentVol
		if currentVol == 0 {
			return 1.0
		}
		return currentVol
	}
	return currentVol / avgVol
}
