package db

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
)

type checkpointWriteCounter struct {
	fileLike
	calls int
	short bool
}

func (f *checkpointWriteCounter) WriteAt(b []byte, off int64) (int, error) {
	f.calls++
	if f.short {
		n, e := f.fileLike.WriteAt(b[:len(b)-1], off)
		return n, e
	}
	return f.fileLike.WriteAt(b, off)
}
func TestCheckpointGroupsPagesWithoutMutatingCaptures(t *testing.T) {
	for _, n := range []int{2, 140} {
		f, e := os.CreateTemp(t.TempDir(), "capture")
		if e != nil {
			t.Fatal(e)
		}
		counter := &checkpointWriteCounter{fileLike: f}
		d := &DB{pager: &pager{f: counter}}
		pages := make([]ioPage, n)
		original := make(map[uint32][]byte)
		for i := range pages {
			p := make(page, PageSize)
			initPage(p, pageLeaf, 0)
			p.insertLeafCell(0, []byte{byte(i)}, bytes.Repeat([]byte{byte(i + 1)}, 100), false)
			id := uint32(2 + i)
			if i >= 70 {
				id += 7
			}
			pages[n-1-i] = ioPage{id: id, data: p}
			original[id] = append([]byte(nil), p...)
		}
		if e = d.writeCheckpointPages(pages); e != nil {
			t.Fatal(e)
		}
		if n > 4 && counter.calls > 6 {
			t.Fatalf("%d pages required %d writes", n, counter.calls)
		}
		for _, p := range pages {
			if !bytes.Equal(p.data, original[p.id]) {
				t.Fatal("capture was mutated")
			}
			got := make(page, PageSize)
			if _, e = f.ReadAt(got, int64(p.id)*PageSize); e != nil {
				t.Fatal(e)
			}
			if got.crc() != pageCrc(got) {
				t.Fatal("invalid checksum")
			}
			got.setCrc(p.data.crc())
			if !bytes.Equal(got, p.data) {
				t.Fatal("changed page contents")
			}
		}
		counter.short = true
		if e = d.writeCheckpointPages(pages); !errors.Is(e, io.ErrShortWrite) {
			t.Fatalf("short write: %v", e)
		}
		f.Close()
	}
}
