package server

import (
	"encoding/json"
	"market-observer/src/models"
)

// -----------------------------------------------------------------------------

func safeProcessingMetrics(data map[string]interface{}, key string) models.MProcessingMetrics {
	if val, ok := data[key]; ok {
		if m, ok := val.(models.MProcessingMetrics); ok {
			return m
		}
		// Try map conversion if it comes as generic map (e.g. from JSON)
		if m, ok := val.(map[string]interface{}); ok {
			return models.MProcessingMetrics{
				AggregationTimeSeconds: safeFloat64(m, "aggregation_time_seconds"),
				ValidSymbols:           int(safeInt64(m, "valid_symbols")),
				WindowsProcessed:       int(safeInt64(m, "windows_processed")),
			}
		}
	}
	return models.MProcessingMetrics{}
}

// -----------------------------------------------------------------------------

func safeStockPriceMap(data map[string]interface{}, key string) map[string]models.MStockPrice {
	result := make(map[string]models.MStockPrice)
	if val, ok := data[key]; ok {
		// If it's already the right type
		if m, ok := val.(map[string]models.MStockPrice); ok {
			return m
		}

		// If it needs conversion (e.g. from JSON interface{})
		if m, ok := val.(map[string]interface{}); ok {
			for k, v := range m {
				if sp, ok := v.(models.MStockPrice); ok {
					result[k] = sp
				} else if spMap, ok := v.(map[string]interface{}); ok {
					// Bruteforce manual mapping or json roundtrip
					jsonBytes, _ := json.Marshal(spMap)
					var sp models.MStockPrice
					if err := json.Unmarshal(jsonBytes, &sp); err == nil {
						result[k] = sp
					}
				}
			}
		}
	}
	return result
}

// -----------------------------------------------------------------------------

func safeAggregationsMap(data map[string]interface{}, key string) map[string]map[string][]models.MAggregation {
	result := make(map[string]map[string][]models.MAggregation)
	if val, ok := data[key]; ok {
		if m, ok := val.(map[string]map[string][]models.MAggregation); ok {
			return m
		}

		// Fallback for generic structure
		if m, ok := val.(map[string]interface{}); ok {
			for sym, windows := range m {
				result[sym] = make(map[string][]models.MAggregation)
				if wMap, ok := windows.(map[string]interface{}); ok {
					for wName, wData := range wMap {
						// wData should be a slice
						if slice, ok := wData.([]models.MAggregation); ok {
							result[sym][wName] = slice
						} else if slice, ok := wData.([]interface{}); ok {
							// Convert []interface{} to []MAggregation
							var aggSlice []models.MAggregation
							jsonBytes, _ := json.Marshal(slice)
							if err := json.Unmarshal(jsonBytes, &aggSlice); err == nil {
								result[sym][wName] = aggSlice
							}
						}
					}
				}
			}
		}
	}
	return result
}

// -----------------------------------------------------------------------------

func safeFloat64(data map[string]interface{}, key string) float64 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case int:
			return float64(v)
		case int64:
			return float64(v)
		}
	}
	return 0.0
}

// -----------------------------------------------------------------------------

func safeInt64(data map[string]interface{}, key string) int64 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		}
	}
	return 0
}

// -----------------------------------------------------------------------------

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
