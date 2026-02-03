package models

// MProcessingMetrics represents the performance metrics for the data processing pipeline.
type MProcessingMetrics struct {
	AggregationTimeSeconds float64 `json:"aggregation_time_seconds"`
	ValidSymbols           int     `json:"valid_symbols"`
	WindowsProcessed       int     `json:"windows_processed"`
}
