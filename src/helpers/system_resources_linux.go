//go:build linux

package helpers

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// GetTotalSystemMemoryMB returns the total physical memory in MB.
func GetTotalSystemMemoryMB() int {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.Atoi(fields[1])
				if err == nil {
					return kb / 1024
				}
			}
		}
	}
	return 0
}
