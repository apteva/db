package db

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestRetiringCopyScanPreservesConcurrentViews(t *testing.T) {
	d, err := Open(t.TempDir(), Options{CacheBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	const rows = 8000
	value := bytes.Repeat([]byte{37}, 1024)
	key := func(i int) []byte { return []byte(fmt.Sprintf("key-%05d", i)) }
	for i := 0; i < rows; i++ {
		if err = d.Upsert(key(i), value); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	s, err := d.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	errs := make(chan error, 6)
	var readers sync.WaitGroup
	for r := 0; r < 6; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			var buf []byte
			for i := 0; i < rows; i++ {
				v, found, e := s.Find(key((i*97+r)%rows), buf)
				if e != nil || !found || !bytes.Equal(v, value) {
					errs <- fmt.Errorf("reader %d row %d: found=%v err=%v", r, i, found, e)
					return
				}
				buf = v[:0]
			}
		}(r)
	}
	seen := 0
	var callbackErr error
	err = d.scanWithRetirement(view{d, s}, d.def.pfx, d.def.end, func(k, v []byte) bool {
		if !bytes.Equal(v, value) {
			callbackErr = fmt.Errorf("copy row %d changed", seen)
			return false
		}
		if seen%1000 == 0 {
			callbackErr = d.Upsert(key(seen), []byte("new value"))
		}
		seen++
		return callbackErr == nil
	}, true)
	readers.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if err != nil || callbackErr != nil || seen != rows {
		t.Fatalf("copy: rows=%d err=%v callback=%v", seen, err, callbackErr)
	}
	if err = d.Verify(nil); err != nil {
		t.Fatal(err)
	}
}
