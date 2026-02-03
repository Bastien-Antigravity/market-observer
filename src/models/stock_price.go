package models

import "time"

// MStockPrice represents the stored stock data.
type MStockPrice struct {
	Symbol              string    `json:"symbol"`
	Price               float64   `json:"price"`
	PricePercentChange  float64   `json:"price_percent_change"`
	Volume              float64   `json:"volume"`
	VolumePercentChange float64   `json:"volume_percent_change"`
	Timestamp           int64     `json:"timestamp"`
	FetchedAt           int64     `json:"fetched_at"`
	CreatedAt           time.Time `json:"created_at"`
}
