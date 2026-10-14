package db

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type feedBenchMode struct {
	name                          string
	subscribers                   int
	filter                        string
	values, project, slow, unique bool
	enabled                       bool
}

var feedBenchModes = []feedBenchMode{
	{name: "Off"},
	{name: "Capture", enabled: true},
	{name: "Metadata1", enabled: true, subscribers: 1},
	{name: "Metadata10", enabled: true, subscribers: 10},
	{name: "Metadata100", enabled: true, subscribers: 100},
	{name: "Filtered100", enabled: true, subscribers: 100, filter: `{"tenant":0}`},
	{name: "Tenants100", enabled: true, subscribers: 100, unique: true},
	{name: "Values10", enabled: true, subscribers: 10, values: true},
	{name: "Projection100", enabled: true, subscribers: 100, values: true, project: true, filter: `{"tenant":0}`},
	{name: "Slow100", enabled: true, subscribers: 100, slow: true},
}

type feedBenchRun struct {
	db        *DB
	batch     *Batch
	docs      [100][]byte
	keys      [][8]byte
	mode      feedBenchMode
	cancel    context.CancelFunc
	feeds     []*ChangeCursor
	acks      chan struct{}
	errs      chan error
	wg        sync.WaitGroup
	count     int
	commitNS  int64
	delivered int64
}

func newFeedBench(dir string, m feedBenchMode, batchSize, padding int) (*feedBenchRun, error) {
	opts := Options{}
	if m.enabled {
		opts.ChangeFeed = &ChangeFeedOptions{}
	}
	d, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &feedBenchRun{db: d, batch: d.NewBatch(), mode: m, cancel: cancel, keys: make([][8]byte, batchSize), acks: make(chan struct{}, m.subscribers), errs: make(chan error, m.subscribers+1)}
	for i := range r.docs {
		r.docs[i], err = EncodeJSON(nil, []byte(fmt.Sprintf(`{"tenant":%d,"seq":%d,"padding":"%s"}`, i, i, strings.Repeat("x", padding))))
		if err != nil {
			r.close()
			return nil, err
		}
	}
	for i := 0; i < m.subscribers; i++ {
		o := WatchOptions{IncludeValues: m.values, Filter: []byte(m.filter), HeartbeatInterval: time.Hour}
		if m.unique {
			o.Filter = []byte(fmt.Sprintf(`{"tenant":%d}`, i%100))
		}
		if m.project {
			o.Fields = []string{"seq"}
		}
		c, err := d.Watch(ctx, o)
		if err != nil {
			r.close()
			return nil, err
		}
		r.feeds = append(r.feeds, c)
		if m.slow {
			continue
		}
		r.wg.Add(1)
		go func(c *ChangeCursor) {
			defer r.wg.Done()
			for c.Next() {
				change := c.Change()
				if change.Operation == ChangePut && m.values {
					var doc map[string]any
					if err := change.Decode(&doc); err != nil {
						r.errs <- err
						return
					}
					if m.project && len(doc) != 1 {
						r.errs <- fmt.Errorf("bad projection: %v", doc)
						return
					}
				}
				if change.EndOfCommit {
					select {
					case r.acks <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
			}
			if ctx.Err() == nil {
				r.errs <- c.Err()
			}
		}(c)
	}
	return r, nil
}
func (r *feedBenchRun) runBatch() error {
	for i := range r.keys {
		binary.BigEndian.PutUint64(r.keys[i][:], uint64(r.count+i))
		if err := r.batch.Upsert(r.keys[i][:], r.docs[(r.count+i)%100]); err != nil {
			return err
		}
	}
	r.count += len(r.keys)
	start := time.Now()
	if err := r.batch.Commit(); err != nil {
		return err
	}
	r.commitNS += time.Since(start).Nanoseconds()
	if !r.mode.slow && r.mode.subscribers > 0 {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		for i := 0; i < r.mode.subscribers; i++ {
			select {
			case <-r.acks:
			case err := <-r.errs:
				return fmt.Errorf("subscriber: %v", err)
			case <-timer.C:
				return fmt.Errorf("subscriber timeout")
			}
		}
	}

	return nil
}
func (r *feedBenchRun) close() {
	r.cancel()
	for _, c := range r.feeds {
		c.Close()
	}
	r.wg.Wait()
	r.db.Close()
}
func BenchmarkNativeChangeFeeds(b *testing.B) {
	for _, m := range feedBenchModes {
		b.Run(m.name, func(b *testing.B) {
			const batchSize = 1000
			r, err := newFeedBench(b.TempDir(), m, batchSize, 100)
			if err != nil {
				b.Fatal(err)
			}
			defer r.close()
			b.ReportAllocs()
			for b.Loop() {
				if err = r.runBatch(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(r.commitNS)/float64(b.N)/batchSize, "write-ns/row")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/batchSize, "end-to-end-ns/row")
			stats := r.db.ChangeFeedStats()
			b.ReportMetric(float64(stats.CacheBytes), "shared-cache-B")
			b.ReportMetric(float64(stats.PeakReaderBytes), "peak-reader-B")
		})
	}
}

// The resource probe runs once per fresh child process. The runner uses wait4
// for per-process peak RSS; Go heap metrics here are separate from RSS.
func TestChangeFeedResourceProbe(t *testing.T) {
	mode := os.Getenv("APTEVA_FEED_PROBE")
	if mode == "" {
		t.Skip("standalone resource/latency benchmark")
	}
	var m feedBenchMode
	found := false
	for _, candidate := range feedBenchModes {
		if candidate.name == mode {
			m = candidate
			found = true
		}
	}
	if !found {
		t.Fatal(mode)
	}
	batchSize := 1000
	if os.Getenv("APTEVA_FEED_SINGLE") == "1" {
		batchSize = 1
	}
	// Unique tenant filters require every subscriber to see a match per batch.
	if batchSize == 1 && m.unique {
		t.Skip("use batched tenant probe")
	}
	r, err := newFeedBench(t.TempDir(), m, batchSize, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	total := 20_000
	if m.subscribers > 0 && !m.slow {
		total = 10_000
	}
	if batchSize == 1 {
		total = 20_000
	}
	if batchSize == 1 && m.filter != "" {
		total = 1000 // shared selective readers acknowledge matching commits only
		m.filter = ""
		t.Skip("selective filters require batched fixture")
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	latencies := make([]int64, 0, total/batchSize)
	start := time.Now()
	for i := 0; i < total/batchSize; i++ {
		s := time.Now()
		if err = r.runBatch(); err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, time.Since(s).Nanoseconds())
	}
	seconds := time.Since(start).Seconds()
	runtime.ReadMemStats(&after)
	out := map[string]any{"version": Version, "mode": mode, "rows": total, "batch_size": batchSize, "seconds": seconds, "rows_per_second": float64(total) / seconds, "write_ns_per_row": float64(r.commitNS) / float64(total), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "heap_alloc_bytes": after.HeapAlloc, "heap_sys_bytes": after.HeapSys, "commit_to_last_subscriber_ns": latencies, "feed_stats": r.db.ChangeFeedStats()}
	b, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("FEED_PROBE", string(b))
}
