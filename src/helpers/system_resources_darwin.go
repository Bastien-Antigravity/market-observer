//go:build darwin

package helpers

import (
	"os/exec"
	"strconv"
	"strings"
)

// GetTotalSystemMemoryMB returns the total physical memory in MB.
func GetTotalSystemMemoryMB() int {
	cmd := exec.Command("sysctl", "-n", "hw.memsize")
	out, err := cmd.Output()
	if err != nil {
		return 0
	}

	bytesStr := strings.TrimSpace(string(out))
	bytes, err := strconv.ParseInt(bytesStr, 10, 64)
	if err != nil {
		return 0
	}

	return int(bytes / 1024 / 1024)
}
