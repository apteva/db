// Command bench runs identical insert/find workloads against the engine,
// Pebble and SQLite (C via cgo, and pure Go) and prints a comparison table.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"

	aptevadb "github.com/apteva/db"
)

type kv interface {
	Upsert(k, v []byte) error
	Find(k, dst []byte) ([]byte, bool, error)
	// Iterate walks every key in order and reports how many it saw.
	Iterate() (int, error)
	// NewReader returns a handle safe to use from another goroutine.
	NewReader() (reader, error)
	// Settle brings the store to its steady read layout: a full compaction
	// for Pebble, a checkpoint for the engine and for SQLite.
	Settle() error
	Close() error
}

type reader interface {
	Find(k, dst []byte) ([]byte, bool, error)
	Close() error
}

type opener func(dir string) (kv, error)

var (
	flagN                   = flag.Int("n", 1_000_000, "keys per workload")
	flagVal                 = flag.Int("vsize", 100, "value size in bytes")
	flagEngines             = flag.String("engines", "ours,pebble,sqlite-c,sqlite-go", "comma list")
	flagWork                = flag.String("workloads", "random,seq", "comma list of random,seq")
	flagDir                 = flag.String("dir", "", "scratch dir (default: temp)")
	flagPar                 = flag.Int("par", runtime.GOMAXPROCS(0), "goroutines for the parallel find phase")
	flagNoPar               = flag.Bool("nopar", false, "skip parallel find phase")
	flagCache               = flag.Int64("cachemb", 512, "block/page cache size in MB for Pebble and SQLite")
	flagOursMB              = flag.Int64("ourscachemb", 0, "page cache limit in MB for our engine (0: keep everything resident)")
	flagDurable             = flag.Bool("durable", false, "durable-insert mode: single writer, then 10 and 50 concurrent writers; no find phases")
	flagAgg                 = flag.Bool("agg", false, "aggregation mode: load N documents, run the pipelines (engines: ours, pebble, sqlite-c)")
	flagJoin                = flag.Bool("join", false, "join mode: users and orders collections, $lookup pipelines against SQL joins (engines: ours, ours-par, sqlite-c)")
	flagComputedCollections = flag.Bool("computedcollections", false, "native maintained collections versus on-demand queries, Pebble application maintenance and SQLite triggers")
	flagComputed            = flag.Bool("computed", false, "stored computed fields versus application writes/read-time derivation, Pebble and SQLite generated columns")
	flagReps                = flag.Int("reps", 1, "run each engine this many times on fresh directories and keep the best of each phase")
)

type result struct {
	engine, workload, phase string
	ops                     int
	d                       time.Duration
}

func main() {
	flag.Parse()
	dir := *flagDir
	if dir == "" {
		d, err := os.MkdirTemp("", "kvbench")
		if err != nil {
			panic(err)
		}
		dir = d
		defer os.RemoveAll(dir)
	}
	if *flagAgg {
		runAggBench(dir, *flagN, strings.Split(*flagEngines, ","))
		return
	}
	if *flagComputedCollections {
		runComputedCollectionsBench(dir, *flagN, strings.Split(*flagEngines, ","))
		return
	}
	if *flagComputed {
		runComputedBench(dir, *flagN, strings.Split(*flagEngines, ","))
		return
	}
	if *flagJoin {
		runJoinBench(dir, *flagN, strings.Split(*flagEngines, ","))
		return
	}
	openers := map[string]opener{
		"ours":                  func(d string) (kv, error) { return openOurs(d, false) },
		"ours-compact":          func(d string) (kv, error) { return openOursCompact(d) },
		"ours-coll4":            func(d string) (kv, error) { return openOursColl(d, 4) },
		"ours-memtable":         func(d string) (kv, error) { return openOursMemtable(d) },
		"pebble-coll4":          func(d string) (kv, error) { return openPebbleColl(d, 4) },
		"sqlite-c-tables4":      func(d string) (kv, error) { return openSQLiteTables(d, 4) },
		"ours-sync":             func(d string) (kv, error) { return openOursMode(d, aptevadb.SyncFull) },
		"ours-sync-barrier":     func(d string) (kv, error) { return openOursMode(d, aptevadb.SyncBarrier) },
		"ours-sync-batch1000":   func(d string) (kv, error) { return openOursBatch(d, aptevadb.SyncFull, 1000) },
		"ours-batch1000":        func(d string) (kv, error) { return openOursBatch(d, aptevadb.SyncFull, -1000) },
		"pebble-sync-batch1000": func(d string) (kv, error) { return openPebble(d, true, 1000) },
		"sqlite-c-sync-txn1000": func(d string) (kv, error) { return openSQLiteC(d, true, 1000) },
		"ours-sync-fsync":       func(d string) (kv, error) { return openOursMode(d, aptevadb.SyncOS) },
		"pebble":                func(d string) (kv, error) { return openPebble(d, false, 0) },
		"pebble-sync":           func(d string) (kv, error) { return openPebble(d, true, 0) },
		"pebble-batch1000":      func(d string) (kv, error) { return openPebble(d, false, 1000) },
		"sqlite-c":              func(d string) (kv, error) { return openSQLiteC(d, false, 1) },
		"sqlite-c-sync":         func(d string) (kv, error) { return openSQLiteC(d, true, 1) },
		"sqlite-c-sync-fsync":   func(d string) (kv, error) { return openSQLiteCFsync(d) },
		"sqlite-c-txn1000":      func(d string) (kv, error) { return openSQLiteC(d, false, 1000) },
		"sqlite-c-bulk":         func(d string) (kv, error) { return openSQLiteC(d, false, *flagN) },
		"sqlite-c-bulk-mmap":    func(d string) (kv, error) { return openSQLiteCMmap(d, *flagN) },
		"sqlite-go":             func(d string) (kv, error) { return openSQLiteGo(d, false, 1) },
		"sqlite-go-sync":        func(d string) (kv, error) { return openSQLiteGo(d, true, 1) },
		"sqlite-go-txn1000":     func(d string) (kv, error) { return openSQLiteGo(d, false, 1000) },
		"sqlite-go-bulk":        func(d string) (kv, error) { return openSQLiteGo(d, false, *flagN) },
	}
	if *flagResources {
		runResourceBench(dir, openers)
		return
	}
	var results []result
	var sizes = map[string]int64{}
	for _, wl := range strings.Split(*flagWork, ",") {
		keys, vals, findOrder, missKeys := genWorkload(wl, *flagN, *flagVal)
		for _, name := range strings.Split(*flagEngines, ",") {
			op, ok := openers[name]
			if !ok {
				fmt.Fprintf(os.Stderr, "unknown engine %q\n", name)
				os.Exit(2)
			}
			for rep := 0; rep < *flagReps; rep++ {
				edir := filepath.Join(dir, name+"-"+wl)
				os.RemoveAll(edir)
				os.MkdirAll(edir, 0o755)
				runtime.GC()
				db, err := op(edir)
				if err != nil {
					panic(name + ": " + err.Error())
				}
				rec := func(phase string, ops int, d time.Duration) {
					// keep the best of the repetitions per phase
					for i := range results {
						r := &results[i]
						if r.engine == name && r.workload == wl && r.phase == phase {
							if d < r.d {
								r.d = d
							}
							fmt.Fprintf(os.Stderr, "%-18s %-6s %-14s %8.0f ns/op  (rep %d)\n", name, wl, phase, float64(d)/float64(ops), rep+1)
							return
						}
					}
					results = append(results, result{name, wl, phase, ops, d})
					fmt.Fprintf(os.Stderr, "%-18s %-6s %-14s %8.0f ns/op  %10.0f ops/s\n", name, wl, phase, float64(d)/float64(ops), float64(ops)/d.Seconds())
				}
				n := len(findOrder)
				ks := *flagN

				start := time.Now()
				for i := 0; i < ks; i++ {
					if err := db.Upsert(keys[i*16:(i+1)*16], vals[i*(*flagVal):(i+1)*(*flagVal)]); err != nil {
						panic(err)
					}
				}
				rec("insert", ks, time.Since(start))

				if *flagDurable {
					for _, par := range []int{10, 50} {
						extra := make([]byte, ks*16)
						rand.New(rand.NewSource(int64(par))).Read(extra)
						target := db
						switch e := db.(type) {
						case *sqliteKV, *oursBatch:
							target = &lockedKV{kv: db}
						case *peb:
							if e.bsize > 0 {
								target = &lockedKV{kv: db}
							}
						}
						var wg sync.WaitGroup
						start = time.Now()
						for g := 0; g < par; g++ {
							wg.Add(1)
							go func(g int) {
								defer wg.Done()
								for i := g; i < ks; i += par {
									if err := target.Upsert(extra[i*16:(i+1)*16], vals[i*(*flagVal):(i+1)*(*flagVal)]); err != nil {
										panic(err)
									}
								}
							}(g)
						}
						wg.Wait()
						rec(fmt.Sprintf("insert-par%d", par), ks, time.Since(start))
					}
					if err := db.Close(); err != nil {
						panic(err)
					}
					sizes[name+"/"+wl] = dirSize(edir)
					if *flagDir == "" {
						if err := os.RemoveAll(edir); err != nil {
							panic(err)
						}
					}
					continue
				}

				start = time.Now()
				if err := db.Settle(); err != nil {
					panic(err)
				}
				rec("settle", ks, time.Since(start))

				var buf []byte
				start = time.Now()
				for _, i := range findOrder {
					v, found, err := db.Find(keys[i*16:(i+1)*16], buf)
					if err != nil {
						panic(err)
					}
					if !found || len(v) != *flagVal || v[0] != byte(i) {
						panic(fmt.Sprintf("%s: bad value for key %d found=%v len=%d", name, i, found, len(v)))
					}
					buf = v
				}
				rec("find-hit", n, time.Since(start))

				start = time.Now()
				for i := 0; i < n; i++ {
					_, found, err := db.Find(missKeys[i*16:(i+1)*16], buf)
					if err != nil {
						panic(err)
					}
					if found {
						panic("found a missing key")
					}
				}
				rec("find-miss", n, time.Since(start))

				start = time.Now()
				seen, err := db.Iterate()
				if err != nil {
					panic(err)
				}
				if seen != ks {
					panic(fmt.Sprintf("%s: iterated %d keys of %d", name, seen, ks))
				}
				rec("scan", ks, time.Since(start))

				if !*flagNoPar {
					par := *flagPar
					var wg sync.WaitGroup
					start = time.Now()
					readers := make([]reader, par)
					for g := range readers {
						r, err := db.NewReader()
						if err != nil {
							panic(err)
						}
						readers[g] = r
					}
					start = time.Now()
					for g := 0; g < par; g++ {
						wg.Add(1)
						go func(g int, rd reader) {
							defer wg.Done()
							var b []byte
							for j := g; j < n; j += par {
								i := findOrder[j]
								v, found, err := rd.Find(keys[i*16:(i+1)*16], b)
								if err != nil || !found {
									panic(fmt.Sprintf("parallel find failed: %v found=%v", err, found))
								}
								b = v
							}
						}(g, readers[g])
					}
					wg.Wait()
					rec(fmt.Sprintf("find-hit-par%d", par), n, time.Since(start))
					for _, r := range readers {
						r.Close()
					}
				}

				if err := db.Close(); err != nil {
					panic(err)
				}
				sizes[name+"/"+wl] = dirSize(edir)
				if *flagDir == "" {
					if err := os.RemoveAll(edir); err != nil {
						panic(err)
					}
				}
			}
		}
	}

	fmt.Println()
	fmt.Printf("keys=%d  key=16B  value=%dB  cpu=%s  GOMAXPROCS=%d\n\n", *flagN, *flagVal, runtime.GOARCH, runtime.GOMAXPROCS(0))
	phases := []string{}
	seen := map[string]bool{}
	for _, r := range results {
		if !seen[r.phase] {
			seen[r.phase] = true
			phases = append(phases, r.phase)
		}
	}
	for _, wl := range strings.Split(*flagWork, ",") {
		fmt.Printf("workload=%s (ns/op, lower is better)\n", wl)
		fmt.Printf("%-18s", "engine")
		for _, p := range phases {
			fmt.Printf(" %14s", p)
		}
		fmt.Printf(" %10s\n", "disk")
		for _, name := range strings.Split(*flagEngines, ",") {
			fmt.Printf("%-18s", name)
			for _, p := range phases {
				for _, r := range results {
					if r.engine == name && r.workload == wl && r.phase == p {
						fmt.Printf(" %14.0f", float64(r.d)/float64(r.ops))
					}
				}
			}
			fmt.Printf(" %8.0fMB\n", float64(sizes[name+"/"+wl])/(1<<20))
		}
		fmt.Println()
	}
}

// genWorkload builds N 16-byte keys, N values, a shuffled lookup order and N
// keys guaranteed absent. Values start with the low byte of the key index so
// lookups can be spot-checked.
func genWorkload(wl string, n, vsize int) (keys, vals []byte, order []int, miss []byte) {
	rng := rand.New(rand.NewSource(42))
	keys = make([]byte, n*16)
	if wl == "seq" {
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(keys[i*16:], uint64(i))
			binary.BigEndian.PutUint64(keys[i*16+8:], uint64(i)*0x9E3779B97F4A7C15)
		}
	} else {
		rng.Read(keys)
	}
	vals = make([]byte, n*vsize)
	rng.Read(vals)
	for i := 0; i < n; i++ {
		vals[i*vsize] = byte(i)
	}
	order = rng.Perm(n)
	miss = make([]byte, n*16)
	rng.Read(miss)
	if wl == "seq" {
		// missing keys share the counter prefix of a live key so they land in
		// the same leaves as hits, but carry a different second half
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(miss[i*16:], uint64(rng.Intn(n)))
		}
	}
	return
}

func dirSize(dir string) int64 {
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// ---- our engine

type ours struct{ db *aptevadb.DB }

func openOurs(dir string, sync bool) (kv, error) {
	db, err := aptevadb.Open(dir, aptevadb.Options{Sync: sync, CacheBytes: *flagOursMB << 20})
	if err != nil {
		return nil, err
	}
	return &ours{db}, nil
}

// openOursMemtable buffers inserts in a 64MB memtable applied in key order.
func openOursMemtable(dir string) (kv, error) {
	db, err := aptevadb.Open(dir, aptevadb.Options{MemtableBytes: 64 << 20})
	if err != nil {
		return nil, err
	}
	return &ours{db}, nil
}

// oursCompact settles with a checkpoint followed by a full Compact, so the
// disk column shows the packed file and the settle column its cost.
type oursCompact struct{ *ours }

func openOursCompact(dir string) (kv, error) {
	k, err := openOurs(dir, false)
	if err != nil {
		return nil, err
	}
	return &oursCompact{k.(*ours)}, nil
}

func (o *oursCompact) Settle() error {
	if err := o.db.Checkpoint(); err != nil {
		return err
	}
	return o.db.Compact(context.Background())
}
func (o *oursCompact) NewReader() (reader, error) { return o.db.NewReader() }

func openOursMode(dir string, mode aptevadb.SyncMode) (kv, error) {
	db, err := aptevadb.Open(dir, aptevadb.Options{Sync: true, SyncMode: mode, CacheBytes: *flagOursMB << 20})
	if err != nil {
		return nil, err
	}
	return &ours{db}, nil
}

// oursBatch aggregates n inserts into one Batch.Commit (n < 0: no sync).
type oursBatch struct {
	db *aptevadb.DB
	b  *aptevadb.Batch
	n  int
}

func openOursBatch(dir string, mode aptevadb.SyncMode, n int) (kv, error) {
	sync := n > 0
	if n < 0 {
		n = -n
	}
	db, err := aptevadb.Open(dir, aptevadb.Options{Sync: sync, SyncMode: mode})
	if err != nil {
		return nil, err
	}
	return &oursBatch{db: db, b: db.NewBatch(), n: n}, nil
}

func (o *oursBatch) Upsert(k, v []byte) error {
	if err := o.b.Upsert(k, v); err != nil {
		return err
	}
	if o.b.Len() >= o.n {
		return o.b.Commit()
	}
	return nil
}
func (o *oursBatch) Find(k, dst []byte) ([]byte, bool, error) {
	if err := o.b.Commit(); err != nil {
		return nil, false, err
	}
	return o.db.Find(k, dst)
}
func (o *oursBatch) NewReader() (reader, error) {
	if err := o.b.Commit(); err != nil {
		return nil, err
	}
	return o.db.NewReader()
}
func (o *oursBatch) Settle() error {
	if err := o.b.Commit(); err != nil {
		return err
	}
	return o.db.Checkpoint()
}
func (o *oursBatch) Close() error {
	if err := o.b.Commit(); err != nil {
		return err
	}
	return o.db.Close()
}

// lockedKV serialises writers for engines with one writer per connection.
type lockedKV struct {
	kv
	mu sync.Mutex
}

func (l *lockedKV) Upsert(k, v []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.kv.Upsert(k, v)
}
func (o *ours) Iterate() (int, error) {
	cur := o.db.NewCursor()
	defer cur.Close()
	n := 0
	for ok := cur.First(); ok; ok = cur.Next() {
		n++
	}
	return n, cur.Err()
}
func (o *oursColl) Iterate() (int, error) {
	n := 0
	for _, c := range o.colls {
		cur := c.NewCursor()
		defer cur.Close()
		for ok := cur.First(); ok; ok = cur.Next() {
			n++
		}
		if err := cur.Err(); err != nil {
			return n, err
		}
	}
	return n, nil
}
func (o *oursBatch) Iterate() (int, error) {
	if err := o.b.Commit(); err != nil {
		return 0, err
	}
	return (&ours{o.db}).Iterate()
}
func (p *peb) Iterate() (int, error) {
	if err := p.flushBatch(); err != nil {
		return 0, err
	}
	it, err := p.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n, it.Error()
}
func (o *ours) Upsert(k, v []byte) error                 { return o.db.Upsert(k, v) }
func (o *ours) Find(k, dst []byte) ([]byte, bool, error) { return o.db.Find(k, dst) }
func (o *ours) Close() error                             { return o.db.Close() }
func (o *ours) NewReader() (reader, error)               { return o.db.NewReader() }
func (o *ours) Settle() error {
	// a checkpoint, then a full compaction: the packed layout Pebble's
	// settle (a full compaction) reaches too
	if err := o.db.Checkpoint(); err != nil {
		return err
	}
	return o.db.Compact(context.Background())
}

// oursColl spreads keys over n named collections by their last byte, the
// way an application keeps several tables in one file: every key still
// carries exactly one tag byte.
type oursColl struct {
	db    *aptevadb.DB
	colls []*aptevadb.Collection
}

func openOursColl(dir string, n int) (kv, error) {
	db, err := aptevadb.Open(dir, aptevadb.Options{CacheBytes: *flagOursMB << 20})
	if err != nil {
		return nil, err
	}
	o := &oursColl{db: db}
	for i := 0; i < n; i++ {
		c, err := db.Collection(fmt.Sprintf("t%d", i))
		if err != nil {
			return nil, err
		}
		o.colls = append(o.colls, c)
	}
	return o, nil
}

func (o *oursColl) pick(k []byte) *aptevadb.Collection { return o.colls[int(k[len(k)-1])%len(o.colls)] }
func (o *oursColl) Upsert(k, v []byte) error           { return o.pick(k).Upsert(k, v) }
func (o *oursColl) Find(k, dst []byte) ([]byte, bool, error) {
	return o.pick(k).Find(k, dst)
}
func (o *oursColl) Close() error { return o.db.Close() }
func (o *oursColl) NewReader() (reader, error) {
	r := &oursCollReader{}
	for _, c := range o.colls {
		h, err := c.NewReader()
		if err != nil {
			r.Close()
			return nil, err
		}
		r.handles = append(r.handles, h)
	}
	return r, nil
}
func (o *oursColl) Settle() error { return o.db.Checkpoint() }

type oursCollReader struct{ handles []*aptevadb.Reader }

func (r *oursCollReader) Find(k, dst []byte) ([]byte, bool, error) {
	return r.handles[int(k[len(k)-1])%len(r.handles)].Find(k, dst)
}
func (r *oursCollReader) Close() error {
	for _, h := range r.handles {
		if err := h.Close(); err != nil {
			return err
		}
	}
	return nil
}

// pebColl is Pebble with a one-byte table prefix on every key, the usual
// way to keep several tables in one LSM.
type pebColl struct {
	*peb
	n   int
	buf []byte
}

func openPebbleColl(dir string, n int) (kv, error) {
	k, err := openPebble(dir, false, 0)
	if err != nil {
		return nil, err
	}
	return &pebColl{peb: k.(*peb), n: n}, nil
}

func (p *pebColl) tag(k []byte) []byte {
	p.buf = append(append(p.buf[:0], byte(int(k[len(k)-1])%p.n)), k...)
	return p.buf
}
func (p *pebColl) Upsert(k, v []byte) error { return p.peb.Upsert(p.tag(k), v) }
func (p *pebColl) Find(k, dst []byte) ([]byte, bool, error) {
	return p.peb.Find(p.tag(k), dst)
}
func (p *pebColl) NewReader() (reader, error) {
	r, err := p.peb.NewReader()
	if err != nil {
		return nil, err
	}
	return &pebCollReader{r, p.n, nil}, nil
}

type pebCollReader struct {
	reader
	n   int
	buf []byte
}

func (r *pebCollReader) Find(k, dst []byte) ([]byte, bool, error) {
	r.buf = append(append(r.buf[:0], byte(int(k[len(k)-1])%r.n)), k...)
	return r.reader.Find(r.buf, dst)
}

// sharedReader wraps engines whose handle is already goroutine-safe.
type sharedReader struct{ kv }

func (sharedReader) Close() error { return nil }

// ---- pebble

type peb struct {
	db    *pebble.DB
	wo    *pebble.WriteOptions
	batch *pebble.Batch
	bsize int
	inb   int
}

func openPebble(dir string, sync bool, batch int) (kv, error) {
	opts := &pebble.Options{CacheSize: *flagCache << 20, MemTableSize: 64 << 20, Logger: quiet{}}
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	p := &peb{db: db, wo: pebble.NoSync, bsize: batch}
	if sync {
		p.wo = pebble.Sync
	}
	return p, nil
}

func (p *peb) Upsert(k, v []byte) error {
	if p.bsize == 0 {
		return p.db.Set(k, v, p.wo)
	}
	if p.batch == nil {
		p.batch = p.db.NewBatch()
	}
	if err := p.batch.Set(k, v, nil); err != nil {
		return err
	}
	p.inb++
	if p.inb >= p.bsize {
		return p.flushBatch()
	}
	return nil
}

func (p *peb) flushBatch() error {
	if p.batch == nil {
		return nil
	}
	err := p.batch.Commit(p.wo)
	p.batch.Close()
	p.batch, p.inb = nil, 0
	return err
}

func (p *peb) Find(k, dst []byte) ([]byte, bool, error) {
	if err := p.flushBatch(); err != nil {
		return nil, false, err
	}
	v, closer, err := p.db.Get(k)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return dst[:0], false, nil
		}
		return nil, false, err
	}
	dst = append(dst[:0], v...)
	closer.Close()
	return dst, true, nil
}

func (p *peb) Close() error {
	if err := p.flushBatch(); err != nil {
		return err
	}
	return p.db.Close()
}

func (p *peb) Settle() error {
	if err := p.flushBatch(); err != nil {
		return err
	}
	if err := p.db.Flush(); err != nil {
		return err
	}
	return p.db.Compact(context.Background(), []byte{0}, bytes.Repeat([]byte{0xff}, 17), false)
}

func (p *peb) NewReader() (reader, error) {
	if err := p.flushBatch(); err != nil {
		return nil, err
	}
	return sharedReader{p}, nil
}

func ctx() context.Context { return context.Background() }

type quiet struct{}

func (quiet) Infof(string, ...interface{})  {}
func (quiet) Errorf(string, ...interface{}) {}
func (quiet) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

var _ = sort.Ints
