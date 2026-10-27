//go:build darwin

package db

import (
	"os"
	"syscall"
	"unsafe"
)

// proc_taskinfo ABI from Apple's public sys/proc_info.h. proc_info call 2,
// flavor PROC_PIDTASKINFO (4). Unlike mincore, pti_resident_size is task RSS.
func processResidentBytes() (int64, error) {
	var info struct {
		Virtual, Resident, TotalUser, TotalSystem, ThreadsUser, ThreadsSystem uint64
		Details                                                               [12]int32
	}
	n, _, errno := syscall.Syscall6(syscall.SYS_PROC_INFO, 2, uintptr(os.Getpid()), 4, 0, uintptr(unsafe.Pointer(&info)), unsafe.Sizeof(info))
	if errno != 0 {
		return 0, errno
	}
	if n != unsafe.Sizeof(info) {
		return 0, ErrCorrupt
	}
	return int64(info.Resident), nil
}
