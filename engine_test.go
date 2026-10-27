package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

func randKV(rng *rand.Rand, n int) map[string][]byte {
	m := make(map[string][]byte, n)
	for len(m) < n {
		k := make([]byte, 1+rng.Intn(40))
		rng.Read(k)
		v := make([]byte, rng.Intn(300))
		rng.Read(v)
		m[string(k)] = v
	}
	return m
}

func check(t *testing.T, db *DB, m map[string][]byte) {
	t.Helper()
	var buf []byte
	for k, want := range m {
		got, ok, err := db.Find([]byte(k), buf)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || !bytes.Equal(got, want) {
			t.Fatalf("key %x: ok=%v got %d bytes want %d bytes", k, ok, len(got), len(want))
		}
		buf = got
	}
}

func insertAll(t *testing.T, db *DB, m map[string][]byte) {
	t.Helper()
	for k, v := range m {
		if err := db.Upsert([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRandomInsertFindUpdateReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	m := randKV(rng, 200_000)
	insertAll(t, db, m)
	check(t, db, m)

	// misses
	for i := 0; i < 10_000; i++ {
		k := make([]byte, 1+rng.Intn(40))
		rng.Read(k)
		if _, ok := m[string(k)]; ok {
			continue
		}
		if _, found, _ := db.Find(k, nil); found {
			t.Fatalf("found absent key %x", k)
		}
	}

	// updates with different sizes, exercising remove/compact paths
	i := 0
	for k := range m {
		if i%2 == 0 {
			v := make([]byte, rng.Intn(600))
			rng.Read(v)
			m[k] = v
			if err := db.Upsert([]byte(k), v); err != nil {
				t.Fatal(err)
			}
		}
		i++
	}
	check(t, db, m)
	st := db.Stats()
	t.Logf("height=%d resident=%d hwm=%d free=%d", st.Height, st.ResidentPages, st.HighWaterPages, st.FreePages)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	check(t, db, m)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSequentialAndReverse(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		db, err := Open(t.TempDir(), Options{})
		if err != nil {
			t.Fatal(err)
		}
		const n = 100_000
		m := make(map[string][]byte, n)
		for i := 0; i < n; i++ {
			j := i
			if reverse {
				j = n - 1 - i
			}
			k := make([]byte, 8)
			binary.BigEndian.PutUint64(k, uint64(j))
			v := bytes.Repeat([]byte{byte(j)}, 50)
			m[string(k)] = v
			if err := db.Upsert(k, v); err != nil {
				t.Fatal(err)
			}
		}
		check(t, db, m)
		db.Close()
	}
}

func TestLogReplayAfterCrash(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(2))
	a := randKV(rng, 30_000)
	insertAll(t, db, a)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	b := randKV(rng, 20_000)
	insertAll(t, db, b)
	// a few deletes must replay too
	deleted := 0
	for k := range a {
		if deleted == 500 {
			break
		}
		if err := db.Delete([]byte(k)); err != nil {
			t.Fatal(err)
		}
		delete(a, k)
		delete(b, k)
		deleted++
	}
	if err := db.FlushLog(); err != nil {
		t.Fatal(err)
	}
	db.crash()

	// torn tail: garbage after the last intact record must be ignored
	gens, _ := walGens(dir)
	f, _ := os.OpenFile(walPath(dir, gens[len(gens)-1]), os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{9, 9, 9, 9, 1, 2, 3})
	f.Close()

	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	all := map[string][]byte{}
	for k, v := range a {
		all[k] = v
	}
	for k, v := range b {
		all[k] = v
	}
	check(t, db, all)
	// keep writing after recovery, then crash again without any checkpoint
	c := randKV(rng, 10_000)
	insertAll(t, db, c)
	for k, v := range c {
		all[k] = v
	}
	db.FlushLog()
	db.crash()
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	check(t, db, all)
	db.Close()
}

func TestSyncMode(t *testing.T) {
	for _, mode := range []SyncMode{SyncFull, SyncBarrier, SyncOS} {
		dir := t.TempDir()
		db, err := Open(dir, Options{Sync: true, SyncMode: mode})
		if err != nil {
			t.Fatal(err)
		}
		m := randKV(rand.New(rand.NewSource(3)), 100)
		insertAll(t, db, m)
		db.crash()
		db, err = Open(dir, Options{})
		if err != nil {
			t.Fatal(err)
		}
		check(t, db, m)
		db.Close()
	}
}

func TestPageReuseAcrossCheckpoints(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(4))
	m := randKV(rng, 50_000)
	insertAll(t, db, m)
	db.Checkpoint()
	base := db.Stats()
	for round := 0; round < 6; round++ {
		for k := range m {
			v := make([]byte, rng.Intn(300))
			rng.Read(v)
			m[k] = v
			db.Upsert([]byte(k), v)
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
	}
	check(t, db, m)
	st := db.Stats()
	t.Logf("after 6 rewrite rounds: hwm %d -> %d, free=%d resident=%d relocations=%d", base.HighWaterPages, st.HighWaterPages, st.FreePages, st.ResidentPages, st.Relocations)
	// the base load packs leaves tightly (sorted apply); random-size
	// rewrites settle near the fill random inserts reach, about a third
	// more pages, and every freed page must have been reused or truncated
	if st.HighWaterPages > base.HighWaterPages*7/5 || st.FreePages > 64 {
		t.Fatalf("copy-on-write slack not reclaimed: hwm %d -> %d, free=%d", base.HighWaterPages, st.HighWaterPages, st.FreePages)
	}
	db.Close()
}

func TestConcurrentReadersWithWriter(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	const n = 50_000
	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i*7919%n))
		return k
	}
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var buf []byte
			for i := 0; i < 20_000; i++ {
				j := rng.Intn(n)
				got, ok, err := db.Find(key(j), buf)
				if err != nil {
					t.Error(err)
					return
				}
				if ok && !bytes.Equal(got, key(j)) {
					t.Errorf("bad value for %d", j)
					return
				}
				buf = got
			}
		}(int64(r))
	}
	for i := 0; i < n; i++ {
		k := key(i)
		if err := db.Upsert(k, k); err != nil {
			t.Fatal(err)
		}
		if i%10_000 == 0 {
			db.Checkpoint()
		}
	}
	wg.Wait()
	db.Close()
}

func TestLimits(t *testing.T) {
	db, _ := Open(t.TempDir(), Options{})
	defer db.Close()
	if err := db.Upsert(nil, []byte("x")); err != ErrKeySize {
		t.Fatal("expected ErrKeySize")
	}
	if err := db.Upsert(make([]byte, 100), make([]byte, MaxCellSize)); err != nil {
		t.Fatalf("a value of MaxCellSize must spill to overflow pages, got %v", err)
	}
	if v, ok, _ := db.Find(make([]byte, 100), nil); !ok || len(v) != MaxCellSize {
		t.Fatal("spilled value does not read back")
	}
	big := make([]byte, MaxCellSize-leafCellHdr-100)
	for i := 0; i < 2000; i++ {
		k := make([]byte, 100)
		binary.BigEndian.PutUint64(k, uint64(i))
		big[0] = byte(i)
		if err := db.Upsert(k, big); err != nil {
			t.Fatal(err)
		}
	}
	k := make([]byte, 100)
	binary.BigEndian.PutUint64(k, 1234)
	got, ok, _ := db.Find(k, nil)
	if !ok || got[0] != byte(1234&0xff) || len(got) != len(big) {
		t.Fatal("big cell roundtrip failed")
	}
}

func TestBatchAtomicity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{Sync: true, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(11))
	base := randKV(rng, 5_000)
	insertAll(t, db, base)

	// a committed batch is visible and survives a crash
	b := db.NewBatch()
	committed := randKV(rng, 2_000)
	for k, v := range committed {
		if err := b.Upsert([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
	for k := range committed {
		if _, ok, _ := db.Find([]byte(k), nil); ok {
			t.Fatalf("key visible before Commit")
		}
		break
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	all := map[string][]byte{}
	for k, v := range base {
		all[k] = v
	}
	for k, v := range committed {
		all[k] = v
	}
	check(t, db, all)

	// a batch whose last record never reached the log must vanish entirely
	torn := randKV(rng, 500)
	for k := range torn {
		if _, dup := all[k]; dup {
			delete(torn, k)
		}
	}
	b = db.NewBatch()
	for k, v := range torn {
		b.Upsert([]byte(k), v)
	}
	// batches may delete too
	b = db.NewBatch()
	for k := range committed {
		b.Delete([]byte(k))
		delete(all, k)
		if b.Len() == 300 {
			break
		}
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	for k := range torn {
		delete(all, k)
	}
	b = db.NewBatch()
	for k, v := range torn {
		b.Upsert([]byte(k), v)
	}
	// simulate the crash: write all but the tail of the batch to the log
	last := b.buf[b.lastOff:]
	last[8] &^= flagMore
	cut := b.buf[:len(b.buf)-5]
	db.wal.appendRaw(cut)
	db.wal.flush()
	db.crash()

	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	check(t, db, all)
	for k := range torn {
		if _, ok, _ := db.Find([]byte(k), nil); ok {
			t.Fatalf("torn batch leaked key %x", k)
		}
	}
	// the log tail was truncated back to the last complete batch, so new
	// writes and another recovery still work
	more := randKV(rng, 300)
	insertAll(t, db, more)
	db.FlushLog()
	db.crash()
	for k, v := range more {
		all[k] = v
	}
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	check(t, db, all)
	db.Close()
}

func fileSize(t *testing.T, dir string) int64 {
	t.Helper()
	st, err := os.Stat(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	return st.Size()
}

func TestEvictionKeepsResidentBounded(t *testing.T) {
	dir := t.TempDir()
	const limit = 64
	opts := Options{CacheBytes: limit * PageSize, SyncMode: SyncOS}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(21))
	m := randKV(rng, 100_000)
	insertAll(t, db, m) // far more pages than the limit; dirty pressure checkpoints
	check(t, db, m)
	st := db.Stats()
	t.Logf("hwm=%d resident=%d dirty=%d evictions=%d loads=%d", st.HighWaterPages, st.ResidentPages, st.DirtyPages, st.Evictions, st.Loads)
	if st.Evictions == 0 || st.HighWaterPages < 4*limit {
		t.Fatalf("eviction did not engage: %+v", st)
	}
	// allowance: eviction slack, pinned dirty/in-flight pages, and internal pages (never evicted)
	if allowed := limit + limit/16 + st.DirtyPages + st.InFlightPages + st.HighWaterPages/50 + 8; st.ResidentPages > allowed {
		t.Fatalf("resident pages %d exceed limit %d (allowed %d)", st.ResidentPages, limit, allowed)
	}
	// updates with size changes and a scan while evicting
	i := 0
	for k := range m {
		if i%3 == 0 {
			v := make([]byte, rng.Intn(400))
			rng.Read(v)
			m[k] = v
			if err := db.Upsert([]byte(k), v); err != nil {
				t.Fatal(err)
			}
		}
		i++
	}
	n := 0
	db.Scan(nil, nil, func(k, v []byte) bool {
		n++
		return bytes.Equal(v, m[string(k)])
	})
	if n != len(m) {
		t.Fatalf("scan under eviction saw %d of %d keys", n, len(m))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	check(t, db, m)
	db.Close()
}

func TestEvictionWithConcurrentReaders(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheBytes: 32 * PageSize, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	const n = 60_000
	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i*7919%n))
		return k
	}
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var buf []byte
			for i := 0; i < 30_000; i++ {
				j := rng.Intn(n)
				got, ok, err := db.Find(key(j), buf)
				if err != nil {
					t.Error(err)
					return
				}
				if ok && !bytes.Equal(got, key(j)) {
					t.Errorf("bad value for %d", j)
					return
				}
				buf = got
			}
		}(int64(r))
	}
	for i := 0; i < n; i++ {
		k := key(i)
		v := append(append([]byte{}, k...), make([]byte, 120)...)
		if err := db.Upsert(k, v[:8]); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	for i := 0; i < n; i += 97 {
		got, ok, _ := db.Find(key(i), nil)
		if !ok || !bytes.Equal(got, key(i)) {
			t.Fatalf("missing key %d after concurrent run", i)
		}
	}
	db.Close()
}

func TestCompactShrinksFile(t *testing.T) {
	for _, cache := range []int64{0, 40 * PageSize} {
		dir := t.TempDir()
		opts := Options{CacheBytes: cache, SyncMode: SyncOS}
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(31))
		m := randKV(rng, 100_000)
		insertAll(t, db, m)
		db.Checkpoint()
		// rewrite half the values so the copy-on-write checkpoint leaves holes
		i := 0
		for k := range m {
			if i%2 == 0 {
				v := make([]byte, rng.Intn(300))
				rng.Read(v)
				m[k] = v
				db.Upsert([]byte(k), v)
			}
			i++
		}
		db.Checkpoint()
		before := fileSize(t, dir)
		hwmBefore := db.Stats().HighWaterPages
		start := time.Now()
		if err := db.Compact(bg); err != nil {
			t.Fatal(err)
		}
		after := fileSize(t, dir)
		st := db.Stats()
		t.Logf("cache=%d: %d -> %d bytes (%.0f%%), pages %d -> %d, free=%d, %v", cache, before, after,
			100*float64(after)/float64(before), hwmBefore, st.HighWaterPages, st.FreePages, time.Since(start).Round(time.Millisecond))
		// balancing packs leaves and the checkpoint vacuum reclaims copy-on-write
		// holes, so Compact only removes the remaining split slack
		if after >= before*95/100 {
			t.Fatalf("compaction did not shrink the file: %d -> %d", before, after)
		}
		if st.FreePages > 1 { // the fresh file's empty initial root is replaced copy-on-write
			t.Fatalf("compacted file still has %d free pages", st.FreePages)
		}
		check(t, db, m)
		if _, err := os.Stat(filepath.Join(dir, "compact.tmp")); !os.IsNotExist(err) {
			t.Fatal("temporary compaction directory left behind")
		}
		// the database keeps working: new writes, log replay after a crash, reopen
		more := randKV(rng, 5_000)
		insertAll(t, db, more)
		for k, v := range more {
			m[k] = v
		}
		db.FlushLog()
		db.crash()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		check(t, db, m)
		db.Close()
	}
}

func TestMemtableFindScanRecoverCompact(t *testing.T) {
	dir := t.TempDir()
	opts := Options{CacheBytes: 48 * PageSize, MemtableBytes: 200 << 10, SyncMode: SyncOS}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(41))
	m := randKV(rng, 60_000)
	insertAll(t, db, m) // many applies with partial checkpoints along the way
	st := db.Stats()
	if st.MemtableEntries == 0 {
		t.Fatalf("memtable not in use: %+v", st)
	}
	t.Logf("after load: memtable=%d entries/%d bytes, hwm=%d resident=%d dirty=%d", st.MemtableEntries, st.MemtableBytes, st.HighWaterPages, st.ResidentPages, st.DirtyPages)
	check(t, db, m)

	// updates land in the memtable and must shadow the tree in finds and scans
	i := 0
	for k := range m {
		if i%5 == 0 {
			v := make([]byte, rng.Intn(200))
			rng.Read(v)
			m[k] = v
			db.Upsert([]byte(k), v)
		}
		i++
	}
	check(t, db, m)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var seen []string
	err = db.Scan(nil, nil, func(k, v []byte) bool {
		if !bytes.Equal(v, m[string(k)]) {
			t.Fatalf("scan value mismatch for %x", k)
		}
		seen = append(seen, string(k))
		return true
	})
	if err != nil || !reflect.DeepEqual(seen, keys) {
		t.Fatalf("merged scan wrong: err=%v got %d keys want %d", err, len(seen), len(keys))
	}
	lo, hi := keys[100], keys[40_000]
	seen = seen[:0]
	db.Scan([]byte(lo), []byte(hi), func(k, v []byte) bool { seen = append(seen, string(k)); return true })
	if !reflect.DeepEqual(seen, keys[100:40_000]) {
		t.Fatalf("merged range scan wrong: %d keys", len(seen))
	}

	// crash with entries only in the log: replay must rebuild the memtable
	db.FlushLog()
	db.crash()
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	check(t, db, m)

	// compaction must fold the memtable in
	more := randKV(rng, 3_000)
	insertAll(t, db, more)
	for k, v := range more {
		m[k] = v
	}
	if err := db.Compact(bg); err != nil {
		t.Fatal(err)
	}
	if st := db.Stats(); st.MemtableEntries != 0 {
		t.Fatalf("memtable not applied by Compact: %d entries", st.MemtableEntries)
	}
	check(t, db, m)
	db.Close()
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	check(t, db, m)
	db.Close()
}

func TestMemtableWithSharding(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheBytes: 256 * PageSize, MemtableBytes: 1 << 20, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	orders := loadOrders(t, db, 40_000) // part of these still sit in the memtable
	if db.Stats().MemtableEntries == 0 {
		t.Fatal("expected buffered entries")
	}
	var want float64
	var n int64
	for _, o := range orders {
		if o.region == "east" {
			want += o.amount
			n++
		}
	}
	for _, par := range []int{1, 8} {
		rows, err := db.AggregateParallel(bg, []byte(`[{"$match": {"region": "east"}}, {"$group": {"_id": null, "total": {"$sum": "$amount"}, "n": {"$sum": 1}}}]`), par)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0]["n"].(int64) != n || !near(rows[0]["total"].(float64), want) {
			t.Fatalf("shards=%d: %#v want n=%d total=%v", par, rows, n, want)
		}
	}
}

// leafFill walks every leaf and reports used bytes over leaf capacity.
func leafFill(t *testing.T, db *DB) (fill float64, leaves int) {
	t.Helper()
	db.mu.RLock()
	defer db.mu.RUnlock()
	used := 0
	var walk func(id uint32)
	walk = func(id uint32) {
		n, err := db.pager.get(id)
		if err != nil {
			t.Fatal(err)
		}
		p := n.data
		if p.typ() == pageLeaf {
			used += PageSize - p.freeSpace() - p.garbage()
			leaves++
			return
		}
		for i := 0; i <= p.numCells(); i++ {
			walk(p.child(i))
		}
	}
	walk(db.root)
	return float64(used) / float64(leaves*PageSize), leaves
}

func TestLeafFillWithBalancing(t *testing.T) {
	// random inserts straight into the tree
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(51))
	m := map[string][]byte{}
	for len(m) < 200_000 {
		k := make([]byte, 16)
		rng.Read(k)
		v := make([]byte, 100)
		rng.Read(v)
		m[string(k)] = v
		if err := db.Upsert(k, v); err != nil {
			t.Fatal(err)
		}
	}
	fill, leaves := leafFill(t, db)
	st := db.Stats()
	t.Logf("random: fill=%.1f%% leaves=%d balances=%d", 100*fill, leaves, st.Balances)
	if fill < 0.74 || st.Balances == 0 {
		t.Fatalf("balancing not effective: fill=%.2f balances=%d", fill, st.Balances)
	}
	check(t, db, m)
	db.Close()

	// the same keys applied in sorted batches through the memtable
	db, err = Open(t.TempDir(), Options{CacheBytes: 256 * PageSize, MemtableBytes: 2 << 20, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range m {
		if err := db.Upsert([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
	db.Checkpoint()
	fill, leaves = leafFill(t, db)
	t.Logf("sorted batches: fill=%.1f%% leaves=%d balances=%d", 100*fill, leaves, db.Stats().Balances)
	if fill < 0.72 {
		t.Fatalf("sorted apply should pack leaves: fill=%.2f", fill)
	}
	check(t, db, m)
	db.Close()
}

func TestDeleteEverywhere(t *testing.T) {
	for _, opts := range []Options{{}, {CacheBytes: 64 * PageSize, MemtableBytes: 64 << 10, SyncMode: SyncOS}} {
		dir := t.TempDir()
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(61))
		m := randKV(rng, 40_000)
		insertAll(t, db, m)
		// delete every third key, some through batches, some through the tree directly
		i := 0
		b := db.NewBatch()
		for k := range m {
			if i%3 == 0 {
				if i%2 == 0 {
					if err := db.Delete([]byte(k)); err != nil {
						t.Fatal(err)
					}
				} else {
					b.Delete([]byte(k))
				}
				delete(m, k)
			}
			i++
		}
		if err := b.Commit(); err != nil {
			t.Fatal(err)
		}
		check(t, db, m)
		n := 0
		db.Scan(nil, nil, func(k, v []byte) bool {
			if _, ok := m[string(k)]; !ok {
				t.Fatalf("deleted key %x still scanned", k)
			}
			n++
			return true
		})
		if n != len(m) {
			t.Fatalf("scan saw %d keys, want %d", n, len(m))
		}
		// delete absent keys, re-insert deleted ones, crash, reopen
		db.Delete([]byte("no such key"))
		back := randKV(rng, 2_000)
		insertAll(t, db, back)
		for k, v := range back {
			m[k] = v
		}
		db.Checkpoint()
		db.FlushLog()
		db.crash()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		check(t, db, m)
		if err := db.Verify(bg); err != nil {
			t.Fatal(err)
		}
		// delete everything: the tree collapses back to an empty root leaf
		for k := range m {
			db.Delete([]byte(k))
		}
		db.Checkpoint()
		n = 0
		db.Scan(nil, nil, func(k, v []byte) bool { n++; return true })
		st := db.Stats()
		if n != 0 || st.Height != 1 {
			t.Fatalf("after deleting all: scanned %d, height %d", n, st.Height)
		}
		if err := db.Compact(bg); err != nil {
			t.Fatal(err)
		}
		if got := fileSize(t, dir); got > 8*PageSize {
			t.Fatalf("empty database still %d bytes on disk", got)
		}
		db.Close()
	}
}

func TestChecksumDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{CacheBytes: 32 * PageSize, MemtableBytes: -1, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	m := randKV(rand.New(rand.NewSource(71)), 20_000)
	insertAll(t, db, m)
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
	// find a real leaf: walk down the leftmost path
	db.mu.RLock()
	n, _ := db.pager.get(db.root)
	for n.data.typ() == pageInternal {
		n, _ = db.pager.get(n.data.child(0))
	}
	victim := int64(n.id)
	db.mu.RUnlock()
	f, _ := os.OpenFile(filepath.Join(dir, "data"), os.O_RDWR, 0)
	f.WriteAt([]byte{0xff}, victim*PageSize+PageSize/2)
	f.Close()
	if err := db.Verify(bg); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Verify did not detect the corrupt page: %v", err)
	}
	// Pages are checksummed on their first load. Reset verification and
	// explicitly evict the victim: random lookups do not guarantee that it
	// leaves the clock cache, and resident pages intentionally skip rechecking.
	db.mu.Lock()
	db.pager.loadMu.Lock()
	db.pager.verified[victim/64].Store(db.pager.verified[victim/64].Load() &^ (1 << (uint32(victim) % 64)))
	if db.pager.pages[victim].Swap(nil) != nil {
		db.pager.resident.Add(-1)
	}
	db.pager.loadMu.Unlock()
	db.mu.Unlock()
	// Once the page is evicted, reading it again must fail too.
	corrupt := false
	err = db.Scan(nil, nil, func(k, v []byte) bool { return true })
	corrupt = errors.Is(err, ErrCorrupt)
	if !corrupt {
		t.Fatalf("scan through the corrupt page did not fail: %v", err)
	}
	db.Close()
}

func TestLockAndClosedHandles(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open should fail with ErrLocked, got %v", err)
	}
	db.Upsert([]byte("a"), []byte("1"))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.Find([]byte("a"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Find after Close: %v", err)
	}
	if err := db.Upsert([]byte("a"), []byte("2")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Insert after Close: %v", err)
	}
	if err := db.Scan(nil, nil, func(k, v []byte) bool { return true }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Scan after Close: %v", err)
	}
	if _, err := db.Aggregate(bg, []byte(`[{"$count": "n"}]`)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Aggregate after Close: %v", err)
	}
	// the lock is released: reopen works and the data is there
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	v, ok, _ := db.Find([]byte("a"), nil)
	if !ok || string(v) != "1" {
		t.Fatal("data lost across close/reopen")
	}
	db.Close()
}

func TestFormatVersionCheck(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, Options{})
	db.Close()
	f, _ := os.OpenFile(filepath.Join(dir, "data"), os.O_RDWR, 0)
	buf := make([]byte, PageSize)
	f.ReadAt(buf, 0)
	le.PutUint32(buf[8:], 99)
	le.PutUint32(buf[52:], crc32.Checksum(buf[:52], crcTable))
	f.WriteAt(buf, 0)
	f.ReadAt(buf, PageSize)
	le.PutUint32(buf[8:], 99)
	le.PutUint32(buf[52:], crc32.Checksum(buf[:52], crcTable))
	f.WriteAt(buf, PageSize)
	f.Close()
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrFormat) {
		t.Fatalf("expected ErrFormat, got %v", err)
	}
}

func TestLargeValuesOverflow(t *testing.T) {
	for _, opts := range []Options{{}, {CacheBytes: 128 * PageSize, MemtableBytes: 256 << 10, SyncMode: SyncOS}} {
		dir := t.TempDir()
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(81))
		m := map[string][]byte{}
		sizes := []int{3_000, 5_000, 20_000, 100_000, 1_000_000}
		for i := 0; i < 300; i++ {
			k := []byte(fmt.Sprintf("big-%05d", rng.Intn(1000)))
			v := make([]byte, sizes[rng.Intn(len(sizes))])
			rng.Read(v)
			m[string(k)] = v
			if err := db.Upsert(k, v); err != nil {
				t.Fatal(err)
			}
		}
		small := randKV(rng, 5_000) // mixed with ordinary records
		insertAll(t, db, small)
		for k, v := range small {
			m[k] = v
		}
		check(t, db, m)
		// overwrite big with small and small with big, delete some big ones
		i := 0
		for k := range m {
			switch i % 4 {
			case 0:
				v := []byte("tiny")
				m[k] = v
				db.Upsert([]byte(k), v)
			case 1:
				v := make([]byte, 40_000)
				rng.Read(v)
				m[k] = v
				db.Upsert([]byte(k), v)
			case 2:
				db.Delete([]byte(k))
				delete(m, k)
			}
			i++
		}
		check(t, db, m)
		n := 0
		db.Scan(nil, nil, func(k, v []byte) bool {
			if !bytes.Equal(v, m[string(k)]) {
				t.Fatalf("scan: wrong value for %s (%d bytes)", k, len(v))
			}
			n++
			return true
		})
		if n != len(m) {
			t.Fatalf("scan saw %d of %d", n, len(m))
		}
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := db.Verify(bg); err != nil {
			t.Fatal(err)
		}
		// large values still in the log only, then crash
		late := map[string][]byte{}
		for i := 0; i < 20; i++ {
			k := fmt.Sprintf("late-%d", i)
			v := make([]byte, 50_000+i)
			rng.Read(v)
			late[k] = v
			db.Upsert([]byte(k), v)
		}
		db.FlushLog()
		db.crash()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range late {
			m[k] = v
		}
		check(t, db, m)
		if err := db.Compact(bg); err != nil {
			t.Fatal(err)
		}
		check(t, db, m)
		if err := db.Verify(bg); err != nil {
			t.Fatal(err)
		}
		if err := db.Upsert([]byte("x"), make([]byte, MaxValueSize+1)); !errors.Is(err, ErrValueSize) {
			t.Fatalf("expected ErrValueSize, got %v", err)
		}
		db.Close()
	}
}

func TestFreelistPersistedAndFallback(t *testing.T) {
	dir := t.TempDir()
	// a cache limit bounds the vacuum per checkpoint, so free pages survive a close
	opts := Options{CacheBytes: 256 * PageSize, MemtableBytes: -1, SyncMode: SyncOS}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(91))
	m := randKV(rng, 120_000)
	insertAll(t, db, m)
	db.Checkpoint()
	full := db.Stats().HighWaterPages
	i := 0
	for k := range m {
		if i%10 != 0 {
			db.Delete([]byte(k))
			delete(m, k)
		}
		i++
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	st := db.Stats()
	t.Logf("reopen: hwm %d -> %d, free=%d loads=%d", full, st.HighWaterPages, st.FreePages, st.Loads)
	// each checkpoint vacuums a bounded slice of the tail; a slow run (the
	// race detector) may fit enough checkpoints to finish the vacuum, in
	// which case the file is compact and an empty free list is right
	if st.FreePages == 0 && st.HighWaterPages > full/5 {
		t.Fatal("free list not restored")
	}
	if int(st.Loads) > st.HighWaterPages/10 {
		t.Fatalf("open read %d pages of %d: the persisted free list was not used", st.Loads, st.HighWaterPages)
	}
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
	check(t, db, m)
	// freed pages are reused and the vacuum keeps shrinking the file
	more := randKV(rng, 20_000)
	insertAll(t, db, more)
	for k, v := range more {
		m[k] = v
	}
	for i := 0; i < 20; i++ {
		db.Checkpoint()
	}
	if after := db.Stats().HighWaterPages; after > full/2 {
		t.Fatalf("file did not shrink after deleting 90%%: hwm %d -> %d", full, after)
	}
	db.Close()

	// corrupt the persisted list: open must fall back to a full walk
	f, _ := os.OpenFile(filepath.Join(dir, "data"), os.O_RDWR, 0)
	buf := make([]byte, PageSize)
	f.ReadAt(buf, 0)
	m0 := meta{le.Uint64(buf[16:]), le.Uint32(buf[24:]), le.Uint32(buf[28:]), le.Uint64(buf[32:]), le.Uint64(buf[40:]), le.Uint32(buf[48:])}
	f.ReadAt(buf, PageSize)
	m1 := meta{le.Uint64(buf[16:]), le.Uint32(buf[24:]), le.Uint32(buf[28:]), le.Uint64(buf[32:]), le.Uint64(buf[40:]), le.Uint32(buf[48:])}
	head := m0.freeHead
	if m1.checkpoint > m0.checkpoint {
		head = m1.freeHead
	}
	if head == 0 {
		t.Fatal("expected a persisted free list")
	}
	f.WriteAt([]byte{0xAA}, int64(head)*PageSize+hdrSize+2)
	f.Close()
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
	check(t, db, m)
	if int(db.Stats().Loads) < db.Stats().HighWaterPages/2 {
		t.Fatal("expected a full walk after a corrupt free list")
	}
	db.Close()
}

// TestLeafFillThroughMemtable: random keys buffered in the default memtable
// and applied in key order still pack leaves well, thanks to splits at the
// insertion point of a sorted run.
func TestLeafFillThroughMemtable(t *testing.T) {
	db, err := Open(t.TempDir(), Options{MemtableBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(53))
	m := map[string][]byte{}
	for len(m) < 200_000 {
		k := make([]byte, 16)
		rng.Read(k)
		v := make([]byte, 100)
		rng.Read(v)
		m[string(k)] = v
		if err := db.Upsert(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	fill, leaves := leafFill(t, db)
	t.Logf("memtable path: fill=%.1f%% leaves=%d", fill*100, leaves)
	if fill < 0.75 {
		t.Fatalf("leaves only %.0f%% full after sorted applies", fill*100)
	}
	check(t, db, m)
	db.Close()
}
