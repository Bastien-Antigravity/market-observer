package utils

import "math"

// -----------------------------------------------------------------------------

// Constants for data retention and memory management.
// Assuming standard trading week of ~5 days * 6.5 hours * 60 minutes = 1950 points.
// Rounded up to 2000 for safety.

// Constants and helper functions for data retention and memory management.
// Assuming standard trading day of 6.5 hours * 60 minutes = 390 points.
// Rounded up to 400 for safety.
const (
	DefaultRetentionDays = 7
)

// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

// CalculateMaxDataPoints calculates max data points based on retention days.
// approx 400 points per day (covering 6.5h market hours)
func CalculateMaxDataPoints(days int) int {
	return int(math.Ceil(float64(days) * 400))
}
