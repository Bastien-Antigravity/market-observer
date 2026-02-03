package models

// -----------------------------------------------------------------------------
// Server State Structure (Matches Python exactly)
// -----------------------------------------------------------------------------

type MLatestData struct {
	Type              string                               `json:"type"` // "INITIAL" or "UPDATE"
	RawData           map[string]MStockPrice               `json:"raw_data"`
	Aggregations      map[string]map[string][]MAggregation `json:"aggregations"`
	Timestamp         int64                                `json:"timestamp"`
	ProcessingMetrics MProcessingMetrics                   `json:"processing_metrics"`
}

// -----------------------------------------------------------------------------
// SubscribeCommand for client messages
// -----------------------------------------------------------------------------

type MSubscribeCommand struct {
	Command    string   `json:"command"`
	ClientType string   `json:"clientType"`
	Symbols    []string `json:"symbols"`
	Timeframe  string   `json:"timeframe"`
}
