package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestDirectoryLockProtectsRecoveryAndSwapWindow(t *testing.T) {
	dir := t.TempDir()
	database, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	temporary := filepath.Join(dir, "compact.tmp")
	if err = os.Mkdir(temporary, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(temporary, "active-copy")
	os.WriteFile(marker, []byte("keep"), 0600)
	data := filepath.Join(dir, "data")
	old := filepath.Join(dir, "data.old")
	if err = os.Rename(data, old); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir, Options{}); !errors.Is(err, ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatal(err)
	}
	if _, err = os.Stat(marker); err != nil {
		t.Fatal("second open destroyed active copy", err)
	}
	if _, err = os.Stat(data); !os.IsNotExist(err) {
		t.Fatal("second open restored another owner's old file", err)
	}
	if err = os.Rename(old, data); err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(temporary)
	if err = database.Compact(bg); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(dir, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatal("directory ownership lost during swap", err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := Open(dir, Options{})
	if err != nil {
		t.Fatal("directory lock not released", err)
	}
	other.Close()
}

func TestOpenRefusesTruncatedDataWithoutOverwriting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data")
	original := []byte("not a database")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(original) {
		t.Fatal("open overwrote a short data file", err)
	}
}

func TestRejectedOpenPreservesRecoveryArtifacts(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "data")
	os.WriteFile(data, []byte("invalid data"), 0600)
	tmp := filepath.Join(dir, "compact.tmp")
	os.Mkdir(tmp, 0700)
	marker := filepath.Join(tmp, "keep")
	os.WriteFile(marker, []byte("keep"), 0600)
	fallback := filepath.Join(dir, "data.old")
	os.WriteFile(fallback, []byte("keep old"), 0600)
	if _, err := Open(dir, Options{}); err == nil {
		t.Fatal("invalid directory opened")
	}
	for _, path := range []string{marker, fallback} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("rejected open removed recovery artifacts", path, err)
		}
	}
}

func TestCompactionSwapFailureRequiresReopen(t *testing.T) {
	for _, trip := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(trip), func(t *testing.T) {
			dir := t.TempDir()
			database, err := Open(dir, Options{Sync: true, SyncMode: SyncOS})
			if err != nil {
				t.Fatal(err)
			}
			if err = database.Upsert([]byte("acknowledged"), []byte("safe")); err != nil {
				t.Fatal(err)
			}
			originalRename, originalSync := renameCompactionFile, syncCompactionDirectory
			t.Cleanup(func() { renameCompactionFile = originalRename; syncCompactionDirectory = originalSync })
			calls := 0
			renameCompactionFile = func(a, b string) error {
				calls++
				if calls == trip {
					return syscall.ENOSPC
				}
				return originalRename(a, b)
			}
			syncCompactionDirectory = func(path string) error {
				calls++
				if calls == trip {
					return syscall.ENOSPC
				}
				return originalSync(path)
			}
			if err = database.Compact(bg); !errors.Is(err, syscall.ENOSPC) {
				t.Fatal(err)
			}
			if err = database.Health(); !errors.Is(err, syscall.ENOSPC) {
				t.Fatal(err)
			}
			if _, _, err = database.Find([]byte("acknowledged"), nil); !errors.Is(err, syscall.ENOSPC) {
				t.Fatal(err)
			}
			database.Stats() // must not inspect the released mapping after a failed rename
			database.Close()
			renameCompactionFile = originalRename
			syncCompactionDirectory = originalSync
			reopened, err := Open(dir, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			value, found, err := reopened.Find([]byte("acknowledged"), nil)
			if err != nil || !found || string(value) != "safe" {
				t.Fatal(string(value), found, err)
			}
		})
	}
}
