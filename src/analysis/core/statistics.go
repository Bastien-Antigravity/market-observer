package core

import "math"

// -----------------------------------------------------------------------------

// CalculateMeanStd computes mean and standard deviation.
func CalculateMeanStd(data []float64) (float64, float64) {
	if len(data) == 0 {
		return 0, 0
	}

	// Calculate mean
	sum := 0.0
	for _, v := range data {
		sum += v
	}
	mean := sum / float64(len(data))

	// For single element, return std = 0 (matching Python)
	if len(data) == 1 {
		return mean, 0
	}

	// Calculate standard deviation with N denominator (population std)
	varianceSum := 0.0
	for _, v := range data {
		varianceSum += (v - mean) * (v - mean)
	}
	std := math.Sqrt(varianceSum / float64(len(data)))
	return mean, std
}

// -----------------------------------------------------------------------------

// CalculateCorrelation computes Pearson correlation coefficient.
func CalculateCorrelation(x, y []float64) float64 {
	// Length check matching Python
	if len(x) != len(y) || len(x) < 2 {
		return 0
	}

	n := float64(len(x))

	// Calculate means first for zero variance check
	_, stdX := CalculateMeanStd(x)
	_, stdY := CalculateMeanStd(y)

	// Zero variance check matching Python
	if stdX == 0 || stdY == 0 {
		return 0
	}

	// Calculate correlation
	sumX, sumY, sumXY, sumX2, sumY2 := 0.0, 0.0, 0.0, 0.0, 0.0
	for i := 0; i < len(x); i++ {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
	}

	numerator := (n * sumXY) - (sumX * sumY)
	denominator := math.Sqrt(((n * sumX2) - (sumX * sumX)) * ((n * sumY2) - (sumY * sumY)))

	if denominator == 0 {
		return 0
	}

	result := numerator / denominator

	if math.IsNaN(result) {
		return 0
	}

	return result
}

// -----------------------------------------------------------------------------

// CalculateZScore calculates Z-Score (Standard Score).
func CalculateZScore(value, mean, std float64) float64 {
	if std == 0 {
		return 0.0
	}
	return (value - mean) / std
}
