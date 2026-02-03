package models

import "time"

// MIntermediateStats stores running statistics for a window.
type MIntermediateStats struct {
	Symbol               string
	WindowName           string
	AvgVolumeHistory     float64
	StdVolumeHistory     float64
	DataPointsHistory    int
	LastHistoryTimestamp int64
	UpdatedAt            time.Time
}
