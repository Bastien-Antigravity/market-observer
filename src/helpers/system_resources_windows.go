//go:build windows

package helpers

import (
	"syscall"
	"unsafe"
)

// MEMORYSTATUSEX structure for GlobalMemoryStatusEx
type MEMORYSTATUSEX struct {
	dwLength                uint32
	dwMemoryLoad            uint32
	ullTotalPhys            uint64
	ullAvailPhys            uint64
	ullTotalPageFile        uint64
	ullAvailPageFile        uint64
	ullTotalVirtual         uint64
	ullAvailVirtual         uint64
	ullAvailExtendedVirtual uint64
}

// GetTotalSystemMemoryMB returns the total physical memory in MB.
func GetTotalSystemMemoryMB() int {
	kernel32, err := syscall.LoadDLL("kernel32.dll")
	if err != nil {
		return 0
	}
	defer kernel32.Release()

	proc, err := kernel32.FindProc("GlobalMemoryStatusEx")
	if err != nil {
		return 0
	}

	var memStatus MEMORYSTATUSEX
	memStatus.dwLength = uint32(unsafe.Sizeof(memStatus))

	ret, _, _ := proc.Call(uintptr(unsafe.Pointer(&memStatus)))
	if ret == 0 {
		return 0
	}

	return int(memStatus.ullTotalPhys / 1024 / 1024)
}
