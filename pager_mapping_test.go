package db

import (
	"bytes"
	"os"
	"testing"
	"unsafe"
)

func TestFileSegmentsKeepAddressesAcrossGrowth(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "segments")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(3 * minMapBytes); err != nil {
		t.Fatal(err)
	}
	p, err := newPager(f, 8, false)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	p.hwm = 1
	p.growTo(1)
	if err = p.ensureMapped(1); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{17}, PageSize)
	if _, err = f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	old := p.mappedPage(0)
	address := unsafe.SliceData(old)
	const pagesPerMap = minMapBytes / PageSize
	if err = p.ensureMapped(2*pagesPerMap + 1); err != nil {
		t.Fatal(err)
	}
	if len(p.maps) != 3 || unsafe.SliceData(p.mappedPage(0)) != address || !bytes.Equal(old, data) {
		t.Fatal("growth moved an existing segment")
	}
	for _, id := range []uint32{pagesPerMap - 1, pagesPerMap, pagesPerMap + 1, 2 * pagesPerMap} {
		value := bytes.Repeat([]byte{byte(id)}, PageSize)
		if _, err = f.WriteAt(value, int64(id)*PageSize); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(p.mappedPage(id), value) {
			t.Fatalf("incorrect mapped offset for %d", id)
		}
	}
}

func TestRecycledPagesKeepContentsAndLiveMappings(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "reclaim")
	if err != nil {
		t.Fatal(err)
	}
	const count = 128
	if err = f.Truncate(count * PageSize); err != nil {
		t.Fatal(err)
	}
	p, err := newPager(f, 8, false)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	p.hwm = count
	p.growTo(count)
	if err = p.ensureMapped(count); err != nil {
		t.Fatal(err)
	}
	for id := uint32(0); id < count; id++ {
		value := bytes.Repeat([]byte{byte(id)}, PageSize)
		if _, err = f.WriteAt(value, int64(id)*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	live := p.mappedPage(17)
	address := unsafe.SliceData(live)
	var ids []uint32
	for id := uint32(32); id < 96; id++ {
		ids = append(ids, id)
	}
	p.releaseIDs(ids)
	p.discardFreePages() // repeat reclamation must skip already discarded ranges
	if unsafe.SliceData(p.mappedPage(17)) != address || !bytes.Equal(live, bytes.Repeat([]byte{17}, PageSize)) {
		t.Fatal("live mapping changed")
	}
	for _, id := range ids {
		if p.mappedPage(id)[0] != byte(id) {
			t.Fatalf("reclaimed page %d changed contents", id)
		}
	}
	id := p.allocID()
	if p.discarded[id/64]&(uint64(1)<<(id%64)) != 0 {
		t.Fatal("reused id remained discarded")
	}
}
