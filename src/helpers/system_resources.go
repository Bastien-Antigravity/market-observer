package helpers

import "fmt"

// GetRecommendedMemoryLimit calculates a safe memory limit for the application.
// Default policy: 75% of Total RAM.
// Fallback: 512MB.
func GetRecommendedMemoryLimit() int {
	// Call OS-specific implementation
	totalMB := GetTotalSystemMemoryMB()
	if totalMB == 0 {
		fmt.Println("Warning: Could not determine system memory. Defaulting to 512MB.")
		return 512
	}

	// Use 75% of available RAM
	limit := int(float64(totalMB) * 0.75)

	// Ensure at least 512MB if system has > 512MB, otherwise use total
	if limit < 512 {
		if totalMB < 512 {
			return totalMB // Very low memory system
		}
		return 512
	}

	return limit
}
