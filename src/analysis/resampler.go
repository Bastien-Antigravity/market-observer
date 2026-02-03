package analysis

import (
	"sort"
)

// TimeSeriesResampler handles time-based resampling calculations.
type TimeSeriesResampler struct{}

// -----------------------------------------------------------------------------

// ResampleIndices returns window groupings for timestamps (matching Python implementation)
// Returns: []struct { Indices []int, StartTime int64, EndTime int64 }
func (r *TimeSeriesResampler) ResampleIndices(timestamps []int64, windowSeconds int64) []struct {
	Indices   []int
	StartTime int64
	EndTime   int64
} {
	if len(timestamps) == 0 {
		return []struct {
			Indices   []int
			StartTime int64
			EndTime   int64
		}{}
	}

	// Ensure timestamps are sorted (matching Python assumption)
	sortedTimestamps := make([]int64, len(timestamps))
	copy(sortedTimestamps, timestamps)
	sort.Slice(sortedTimestamps, func(i, j int) bool {
		return sortedTimestamps[i] < sortedTimestamps[j]
	})

	// Find min and max timestamps (matching Python)
	minTs := sortedTimestamps[0]
	maxTs := sortedTimestamps[len(sortedTimestamps)-1]

	// Create window boundaries (matching Python's np.arange)
	var windowStarts []int64
	for start := minTs; start <= maxTs+windowSeconds; start += windowSeconds {
		windowStarts = append(windowStarts, start)
	}

	var results []struct {
		Indices   []int
		StartTime int64
		EndTime   int64
	}

	// For each window, find indices (matching Python's searchsorted)
	for i := 0; i < len(windowStarts)-1; i++ {
		windowStart := windowStarts[i]
		windowEnd := windowStarts[i+1]

		// Find start index (left side search)
		startIdx := sort.Search(len(sortedTimestamps), func(j int) bool {
			return sortedTimestamps[j] >= windowStart
		})

		// Find end index (left side search)
		endIdx := sort.Search(len(sortedTimestamps), func(j int) bool {
			return sortedTimestamps[j] >= windowEnd
		})

		if startIdx < endIdx {
			// Create indices slice (matching Python's np.arange)
			indices := make([]int, endIdx-startIdx)
			for idx := startIdx; idx < endIdx; idx++ {
				indices[idx-startIdx] = idx
			}

			results = append(results, struct {
				Indices   []int
				StartTime int64
				EndTime   int64
			}{
				Indices:   indices,
				StartTime: windowStart,
				EndTime:   windowEnd,
			})
		}
	}

	return results
}

// -----------------------------------------------------------------------------

// ResampleData returns actual data groupings (convenience function)
func ResampleData[T any](
	r *TimeSeriesResampler,
	timestamps []int64,
	data []T,
	windowSeconds int64,
) []struct {
	Data      []T
	StartTime int64
	EndTime   int64
} {
	indicesList := r.ResampleIndices(timestamps, windowSeconds)

	var results []struct {
		Data      []T
		StartTime int64
		EndTime   int64
	}

	for _, indicesGroup := range indicesList {
		// Extract data using indices
		dataSlice := make([]T, len(indicesGroup.Indices))
		for i, idx := range indicesGroup.Indices {
			if idx < len(data) {
				dataSlice[i] = data[idx]
			}
		}

		results = append(results, struct {
			Data      []T
			StartTime int64
			EndTime   int64
		}{
			Data:      dataSlice,
			StartTime: indicesGroup.StartTime,
			EndTime:   indicesGroup.EndTime,
		})
	}

	return results
}

// -----------------------------------------------------------------------------

// ResampleMultiData returns groupings for multiple data arrays (for price, volume, etc.)
func (r *TimeSeriesResampler) ResampleMultiData(
	timestamps []int64,
	windowSeconds int64,
	dataArrays ...[]float64,
) []struct {
	DataArrays [][]float64
	StartTime  int64
	EndTime    int64
} {
	indicesList := r.ResampleIndices(timestamps, windowSeconds)

	var results []struct {
		DataArrays [][]float64
		StartTime  int64
		EndTime    int64
	}

	for _, indicesGroup := range indicesList {
		// For each data array, extract the window slice
		windowData := make([][]float64, len(dataArrays))

		for arrIdx, dataArray := range dataArrays {
			windowSlice := make([]float64, len(indicesGroup.Indices))
			for i, idx := range indicesGroup.Indices {
				if idx < len(dataArray) {
					windowSlice[i] = dataArray[idx]
				}
			}
			windowData[arrIdx] = windowSlice
		}

		results = append(results, struct {
			DataArrays [][]float64
			StartTime  int64
			EndTime    int64
		}{
			DataArrays: windowData,
			StartTime:  indicesGroup.StartTime,
			EndTime:    indicesGroup.EndTime,
		})
	}

	return results
}

// -----------------------------------------------------------------------------

// Helper function matching Python's searchsorted behavior
func SearchSorted(arr []int64, value int64, side string) int {
	if side == "left" {
		return sort.Search(len(arr), func(i int) bool {
			return arr[i] >= value
		})
	} else { // "right"
		return sort.Search(len(arr), func(i int) bool {
			return arr[i] > value
		})
	}
}

// -----------------------------------------------------------------------------

// Simple Helper for window boundaries (keeping backward compatibility)
func CalculateWindowBoundaries(ts int64, window int64) (int64, int64) {
	start := ts - (ts % window)
	return start, start + window
}
