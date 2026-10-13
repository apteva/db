package db

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// A stable lock inode protects recovery and the data-file rename window. The
// data inode's own lock is retained for older engines, but cannot protect swaps.
func lockDirectory(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("lock database directory: %w", err)
	}
	return f, nil
}
