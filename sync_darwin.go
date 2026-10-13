//go:build darwin

package db

import (
	"os"
	"syscall"
)

const fBarrierFsync = 85 // F_BARRIERFSYNC from <sys/fcntl.h>

// syncFull forces the drive to flush its cache (F_FULLFSYNC via os.File.Sync).
func syncFull(f fileLike) error { return f.Sync() }

// syncBarrier orders writes and flushes to the drive without waiting for the
// drive cache to reach stable media (F_BARRIERFSYNC).
func syncBarrier(f fileLike) error {
	if _, ok := f.(*os.File); !ok {
		return f.Sync()
	}
	_, _, e := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), fBarrierFsync, 0)
	if e != 0 {
		return e
	}
	return nil
}

// syncOS is fsync(2): data reaches the drive, survives an OS crash but not a
// power loss on macOS. This is what SQLite does by default.
func syncOS(f fileLike) error {
	if _, ok := f.(*os.File); !ok {
		return f.Sync()
	}
	return syscall.Fsync(int(f.Fd()))
}
