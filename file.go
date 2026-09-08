package aptevadb

import (
	"io"
	"os"
)

// fileLike is what the engine needs from a file. Tests substitute a
// fault-injecting implementation through openFile.
type fileLike interface {
	io.ReaderAt
	io.WriterAt
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
	Sync() error
	Close() error
	Fd() uintptr
}

var openFile = func(path string) (fileLike, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
}
