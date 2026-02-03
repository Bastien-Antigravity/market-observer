package models

import "time"

// MAggregation represents a calculated candle for a specific time window.
type MAggregation struct {
	Symbol                 string    `json:"symbol"`
	WindowName             string    `json:"window_name"` // e.g., "5m", "1h"
	Open                   float64   `json:"open"`
	High                   float64   `json:"high"`
	Low                    float64   `json:"low"`
	Close                  float64   `json:"close"`
	Volume                 float64   `json:"volume"`
	AvgPrice               float64   `json:"avg_price"`
	PricePercentChange     float64   `json:"price_percent_change"`
	VolumePercentChange    float64   `json:"volume_percent_change"`
	PriceVolumeCorrelation float64   `json:"price_volume_correlation"`
	VolumeAnomalyRatio     float64   `json:"volume_anomaly_ratio"`
	StartTime              int64     `json:"start_time"`
	EndTime                int64     `json:"end_time"`
	DataPoints             int       `json:"data_points"`
	CreatedAt              time.Time `json:"created_at"`
}
