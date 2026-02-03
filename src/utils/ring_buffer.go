package utils

import (
	"market-observer/src/models"
)

// -----------------------------------------------------------------------------
// RingBuffer is a fixed-size circular buffer with structured data.
// True ring buffer - no resizing allowed!
// -----------------------------------------------------------------------------

type RingBuffer struct {
	// Data storage as 2D slice (rows x features)
	data     [][models.RB_NUM_FEATURES]float64
	capacity int
	index    int // Next write position
	size     int // Current number of elements
}

// -----------------------------------------------------------------------------

// NewRingBuffer creates a new buffer with fixed capacity
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 1000 // Default reasonable size
	}

	return &RingBuffer{
		data:     make([][5]float64, capacity),
		capacity: capacity,
		index:    0,
		size:     0,
	}
}

// -----------------------------------------------------------------------------

// Append adds a structured data point (Strict Type)
func (rb *RingBuffer) Append(point models.MStockPrice) {
	rb.data[rb.index] = [models.RB_NUM_FEATURES]float64{
		float64(point.Timestamp),
		point.Price,
		point.Volume,
		point.PricePercentChange,
		point.VolumePercentChange,
	}

	rb.index = (rb.index + 1) % rb.capacity

	// Update size (never exceeds capacity)
	if rb.size < rb.capacity {
		rb.size++
	}
}

// -----------------------------------------------------------------------------

// GetLatest returns n latest records as StockPrice
func (rb *RingBuffer) GetLatest(n int) []models.MStockPrice {
	if rb.size == 0 || n <= 0 {
		return []models.MStockPrice{}
	}

	// Calculate how many to return
	count := n
	if n > rb.size {
		count = rb.size
	}

	result := make([]models.MStockPrice, count)

	// Calculate starting index (latest data is at index-1)
	startIdx := (rb.index - count + rb.capacity) % rb.capacity

	for i := 0; i < count; i++ {
		idx := (startIdx + i) % rb.capacity
		row := rb.data[idx]

		result[i] = models.MStockPrice{
			Timestamp:           int64(row[models.RB_IDX_TIMESTAMP]),
			Price:               row[models.RB_IDX_PRICE],
			Volume:              row[models.RB_IDX_VOLUME],
			PricePercentChange:  row[models.RB_IDX_PRICE_PCT],
			VolumePercentChange: row[models.RB_IDX_VOL_PCT],
		}
	}

	return result
}

// -----------------------------------------------------------------------------

// GetAll returns all data in insertion order (oldest to newest)
func (rb *RingBuffer) GetAll() []models.MStockPrice {
	if rb.size == 0 {
		return []models.MStockPrice{}
	}

	result := make([]models.MStockPrice, rb.size)

	// Calculate start index (oldest element)
	var startIdx int
	if rb.size == rb.capacity {
		// Buffer is full, oldest is at current index (wrap-around)
		startIdx = rb.index
	} else {
		// Buffer not full, oldest is at index 0
		startIdx = 0
	}

	// Extract in order
	for i := 0; i < rb.size; i++ {
		idx := (startIdx + i) % rb.capacity
		row := rb.data[idx]

		result[i] = models.MStockPrice{
			Timestamp:           int64(row[models.RB_IDX_TIMESTAMP]),
			Price:               row[models.RB_IDX_PRICE],
			Volume:              row[models.RB_IDX_VOLUME],
			PricePercentChange:  row[models.RB_IDX_PRICE_PCT],
			VolumePercentChange: row[models.RB_IDX_VOL_PCT],
		}
	}

	return result
}

// -----------------------------------------------------------------------------

// GetSnapshot returns data as 2D array
func (rb *RingBuffer) GetSnapshot() [][5]float64 {
	if rb.size == 0 {
		return [][5]float64{}
	}

	result := make([][5]float64, rb.size)

	// Calculate start index
	var startIdx int
	if rb.size == rb.capacity {
		startIdx = rb.index
	} else {
		startIdx = 0
	}

	for i := 0; i < rb.size; i++ {
		idx := (startIdx + i) % rb.capacity
		result[i] = rb.data[idx]
	}

	return result
}

// -----------------------------------------------------------------------------

// Size returns current number of elements
func (rb *RingBuffer) Size() int {
	return rb.size
}

// -----------------------------------------------------------------------------

// Capacity returns buffer capacity (fixed)
func (rb *RingBuffer) Capacity() int {
	return rb.capacity
}

// -----------------------------------------------------------------------------

// Resize changes the capacity of the buffer
// If newCapacity < size, oldest data is dropped
func (rb *RingBuffer) Resize(newCapacity int) {
	if newCapacity <= 0 {
		return
	}
	if newCapacity == rb.capacity {
		return
	}

	// Create new buffer
	newData := make([][5]float64, newCapacity)

	// Copy existing data
	// If expanding: copy all
	// If shrinking: copy only what fits (newest)

	count := rb.size
	if count > newCapacity {
		count = newCapacity
	}

	// Get all data first

	// But internally we store float arrays.

	// Efficient way: use GetSnapshot or manual copy
	// To avoid circular dependency or conversion overhead, let's just use manual extraction

	// Extract latest 'count' items from OLD buffer
	// Start index of latest 'count' items:
	// tail index is rb.index
	// start = (rb.index - count + rb.capacity) % rb.capacity

	startIdx := (rb.index - count + rb.capacity) % rb.capacity

	for i := 0; i < count; i++ {
		idx := (startIdx + i) % rb.capacity
		newData[i] = rb.data[idx]
	}

	rb.data = newData
	rb.capacity = newCapacity
	rb.size = count
	rb.index = count % newCapacity
}

// -----------------------------------------------------------------------------

// IsFull returns whether buffer is full
func (rb *RingBuffer) IsFull() bool {
	return rb.size == rb.capacity
}

// -----------------------------------------------------------------------------

// Clear resets the buffer
func (rb *RingBuffer) Clear() {
	rb.index = 0
	rb.size = 0
}

// -----------------------------------------------------------------------------
// Helper function
// -----------------------------------------------------------------------------

func getFloat(data map[string]interface{}, key string, defaultValue float64) float64 {
	if val, ok := data[key]; ok {
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int:
			return float64(v)
		case int64:
			return float64(v)
		default:
			return defaultValue
		}
	}
	return defaultValue
}
