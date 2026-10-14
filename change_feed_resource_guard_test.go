package db

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The identical source is built against public alpha.4 and current code.
func TestChangeFeedBaselineProbe(t *testing.T) {
	if os.Getenv("APTEVA_FEED_BASELINE") == "" {
		t.Skip("standalone paired resource probe")
	}
	batchSize := 1000
	if os.Getenv("APTEVA_FEED_SINGLE") == "1" {
		batchSize = 1
	}
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var docs [100][]byte
	for i := range docs {
		docs[i], err = EncodeJSON(nil, []byte(fmt.Sprintf(`{"tenant":%d,"seq":%d,"padding":"%s"}`, i, i, strings.Repeat("x", 100))))
		if err != nil {
			t.Fatal(err)
		}
	}
	keys := make([][8]byte, batchSize)
	batch := d.NewBatch()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	start := time.Now()
	var commitNS int64
	const total = 20000
	for base := 0; base < total; base += batchSize {
		for i := range keys {
			binary.BigEndian.PutUint64(keys[i][:], uint64(base+i))
			if err = batch.Upsert(keys[i][:], docs[(base+i)%100]); err != nil {
				t.Fatal(err)
			}
		}
		s := time.Now()
		if err = batch.Commit(); err != nil {
			t.Fatal(err)
		}
		commitNS += time.Since(s).Nanoseconds()
	}
	seconds := time.Since(start).Seconds()
	runtime.ReadMemStats(&after)
	out := map[string]any{"version": Version, "mode": "BaselineOff", "rows": total, "batch_size": batchSize, "seconds": seconds, "rows_per_second": float64(total) / seconds, "write_ns_per_row": float64(commitNS) / total, "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "heap_alloc_bytes": after.HeapAlloc, "heap_sys_bytes": after.HeapSys}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("FEED_PROBE", string(b))
}
