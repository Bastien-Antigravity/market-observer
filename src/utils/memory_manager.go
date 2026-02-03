package utils

import (
	"runtime"
	"runtime/debug"
	"sync"

	"market-observer/src/logger"
	"market-observer/src/models"
)

// -----------------------------------------------------------------------------
// MemoryManager manages in-memory data buffers for symbols.
// -----------------------------------------------------------------------------

type MemoryManager struct {
	DataStreams   map[string]*RingBuffer // Changed to RingBuffer
	MaxMemoryMB   int
	MaxDataPoints int
	Logger        *logger.Logger
	mu            sync.RWMutex // Add mutex for thread safety
}

// -----------------------------------------------------------------------------

func NewMemoryManager(maxMemoryMB, maxDataPoints int) *MemoryManager {
	return &MemoryManager{
		DataStreams:   make(map[string]*RingBuffer), // Changed type
		MaxMemoryMB:   maxMemoryMB,
		MaxDataPoints: maxDataPoints,
		Logger:        logger.NewLogger(nil, "MemoryManager"),
	}
}

// -----------------------------------------------------------------------------

// AddDataPoint adds a data point to the buffer for a symbol.
// Changed to accept models.MStockPrice (Strict Type)
func (mm *MemoryManager) AddDataPoint(symbol string, data models.MStockPrice) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if _, ok := mm.DataStreams[symbol]; !ok {
		mm.DataStreams[symbol] = NewRingBuffer(mm.MaxDataPoints)
	}

	mm.DataStreams[symbol].Append(data)

	// Periodic memory check
	if mm.DataStreams[symbol].Size()%100 == 0 {
		mm.CheckMemoryLimits()
	}
}

// -----------------------------------------------------------------------------

// AddStockDataPoint adds a structured data point (Wrapper - kept for backwards compt if needed, but redirects)
func (mm *MemoryManager) AddStockDataPoint(symbol string, data models.MStockPrice) {
	mm.AddDataPoint(symbol, data)
}

// -----------------------------------------------------------------------------

// GetLatestData returns data with flexible parameters (matches Python API)
func (mm *MemoryManager) GetLatestData(symbol string, allData bool) interface{} {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// If symbol is empty, return data for all symbols (matches Python's None check)
	if symbol == "" {
		return mm.getAllSymbolsData(allData)
	}

	// Get data for specific symbol
	return mm.getSymbolData(symbol, allData)
}

// -----------------------------------------------------------------------------

// getAllSymbolsData returns data for all symbols
func (mm *MemoryManager) getAllSymbolsData(allData bool) interface{} {
	if allData {
		// Return FULL state for ALL symbols
		result := make(map[string][]models.MStockPrice)
		for sym, buffer := range mm.DataStreams {
			if buffer.Size() == 0 {
				continue
			}

			// Get full history directly
			history := buffer.GetAll()
			result[sym] = history
		}
		return result
	} else {
		// Return latest point for ALL symbols (Snapshot only)
		result := make(map[string]models.MStockPrice)
		for sym, buffer := range mm.DataStreams {
			if buffer.Size() == 0 {
				continue
			}

			latest := buffer.GetLatest(1)
			if len(latest) > 0 {
				result[sym] = latest[0]
			}
		}
		return result
	}
}

// -----------------------------------------------------------------------------

// getSymbolData returns data for specific symbol
func (mm *MemoryManager) getSymbolData(symbol string, allData bool) interface{} {
	buffer, ok := mm.DataStreams[symbol]
	if !ok || buffer.Size() == 0 {
		return nil
	}

	if allData {
		// Single symbol full history
		return buffer.GetAll()
	} else {
		// Just latest point
		latest := buffer.GetLatest(1)
		if len(latest) > 0 {
			return latest[0]
		}
		return nil
	}
}

// -----------------------------------------------------------------------------

// GetLatestArrays returns all data as structured arrays (matches Python)
func (mm *MemoryManager) GetLatestArrays(symbol string) [][5]float64 {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	buffer, ok := mm.DataStreams[symbol]
	if !ok || buffer.Size() == 0 {
		return [][5]float64{}
	}

	// Get snapshot as 2D array
	snapshot := buffer.GetSnapshot()

	// Convert to [][5]float64 if needed
	// (Assuming GetSnapshot returns [][5]float64)
	return snapshot
}

// -----------------------------------------------------------------------------

// CheckMemoryLimits checks and enforces memory limits (matches Python logic)
func (mm *MemoryManager) CheckMemoryLimits() {
	currentMemory := mm.GetProcessMemoryMB()

	if currentMemory > float64(mm.MaxMemoryMB) {
		mm.Logger.Info("Memory usage %.1fMB exceeds limit %dMB. Cleaning up.",
			currentMemory, mm.MaxMemoryMB)

		// Reduce data retention by half to free memory (matches Python)
		mm.mu.Lock()
		for symbol := range mm.DataStreams {
			buffer := mm.DataStreams[symbol]
			if buffer.Capacity() > 100 {
				newCapacity := buffer.Capacity() / 2
				if newCapacity < 50 {
					newCapacity = 50
				}
				buffer.Resize(newCapacity)
			}
		}
		mm.mu.Unlock()

		// Force garbage collection
		runtime.GC()
		debug.FreeOSMemory()
	}
}

// -----------------------------------------------------------------------------

// GetProcessMemoryMB gets current process memory usage in MB
func (mm *MemoryManager) GetProcessMemoryMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Use HeapAlloc for process memory (closest to psutil's rss)
	return float64(m.HeapAlloc) / 1024 / 1024

	// Alternative: estimate like Python's fallback
	// totalItems := 0
	// for _, buffer := range mm.DataStreams {
	//     totalItems += buffer.Capacity()
	// }
	// return float64(totalItems*5*8) / 1024 / 1024 // 5 features * 8 bytes
}

// -----------------------------------------------------------------------------

// Cleanup clears all data (matches Python)
func (mm *MemoryManager) Cleanup() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.DataStreams = make(map[string]*RingBuffer)
	runtime.GC()
	debug.FreeOSMemory()
}

// -----------------------------------------------------------------------------

// GetBuffer returns the ring buffer for a symbol (convenience method)
func (mm *MemoryManager) GetBuffer(symbol string) *RingBuffer {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	return mm.DataStreams[symbol]
}

// -----------------------------------------------------------------------------

// HasSymbol checks if symbol exists
func (mm *MemoryManager) HasSymbol(symbol string) bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	_, ok := mm.DataStreams[symbol]
	return ok
}

// -----------------------------------------------------------------------------

// SymbolCount returns number of symbols with data
func (mm *MemoryManager) SymbolCount() int {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	return len(mm.DataStreams)
}
