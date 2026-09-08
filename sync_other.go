//go:build !darwin

package aptevadb

func syncFull(f fileLike) error    { return f.Sync() }
func syncBarrier(f fileLike) error { return f.Sync() }
func syncOS(f fileLike) error      { return f.Sync() }
