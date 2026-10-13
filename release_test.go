package db

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// TestFormatRefusal: a file of another format version is identified
// without opening it and refused by Open with ErrFormat naming both
// versions.
func TestFormatRefusal(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	db.Upsert([]byte("k"), []byte("v"))
	db.Close()
	if v, err := FormatVersionOf(dir); err != nil || v != FormatVersion {
		t.Fatalf("FormatVersionOf = %d, %v", v, err)
	}
	// rewrite both meta pages as format 99, with valid checksums
	f, err := os.OpenFile(filepath.Join(dir, "data"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	tab := crc32.MakeTable(crc32.Castagnoli)
	buf := make([]byte, PageSize)
	for slot := int64(0); slot < 2; slot++ {
		if _, err := f.ReadAt(buf, slot*PageSize); err != nil {
			t.Fatal(err)
		}
		if string(buf[:8]) != metaMagic {
			continue
		}
		le.PutUint32(buf[8:], 99)
		le.PutUint32(buf[52:], crc32.Checksum(buf[:52], tab))
		if _, err := f.WriteAt(buf, slot*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()
	if v, err := FormatVersionOf(dir); err != nil || v != 99 {
		t.Fatalf("FormatVersionOf after rewrite = %d, %v", v, err)
	}
	_, err = Open(dir, Options{})
	if !errors.Is(err, ErrFormat) {
		t.Fatalf("Open: %v", err)
	}
	if want := fmt.Sprintf("file is format 99, this build reads %d", FormatVersion); err == nil || !contains(err.Error(), want) {
		t.Fatalf("error should name both versions: %v", err)
	}
	// the refusal released the lock: a good file in another dir still opens
	db2, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	db2.Close()
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestOpenRestoresInterruptedSwap: a crash between the two renames of a
// compaction swap leaves the original as data.old and no data; Open puts
// it back instead of starting an empty database.
func TestOpenRestoresInterruptedSwap(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		db.Upsert([]byte(fmt.Sprintf("k%04d", i)), []byte("v"))
	}
	db.Close()
	if err := os.Rename(filepath.Join(dir, "data"), filepath.Join(dir, "data.old")); err != nil {
		t.Fatal(err)
	}
	os.MkdirAll(filepath.Join(dir, "compact.tmp"), 0o755)
	os.WriteFile(filepath.Join(dir, "compact.tmp", "data"), []byte("partial copy"), 0o644)
	db, err = Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	n := 0
	db.Scan(nil, nil, func(k, v []byte) bool { n++; return true })
	if n != 1000 {
		t.Fatalf("%d keys after the interrupted swap, want 1000", n)
	}
	if _, err := os.Stat(filepath.Join(dir, "data.old")); !os.IsNotExist(err) {
		t.Fatal("data.old still present")
	}
	if _, err := os.Stat(filepath.Join(dir, "compact.tmp")); !os.IsNotExist(err) {
		t.Fatal("compact.tmp still present")
	}
}
