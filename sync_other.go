//go:build !darwin

package db

func syncFull(f fileLike) error    { return f.Sync() }
func syncBarrier(f fileLike) error { return f.Sync() }
func syncOS(f fileLike) error      { return f.Sync() }
