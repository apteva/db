package db

import (
	"fmt"
	"sync"
	"testing"
)

// Stats must synchronize with the WAL flusher, which runs outside db.mu.
func TestStatsConcurrentWALFlush(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 1 << 20})
	var wg sync.WaitGroup
	errs := make(chan error, 1)
	start := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		val := make([]byte, 2048)
		b := db.NewBatch()
		for i := 0; i < 8000; i++ {
			if err := b.Upsert([]byte(fmt.Sprint(i)), val); err != nil {
				errs <- err
				return
			}
			if b.Len() == 100 {
				if err := b.Commit(); err != nil {
					errs <- err
					return
				}
			}
		}
		if err := b.Commit(); err != nil {
			errs <- err
		}
	}()
	close(start)
	for i := 0; i < 8000; i++ {
		_ = db.Stats()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
