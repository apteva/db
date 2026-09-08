package aptevadb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Crash coverage of the maintenance operations: an index build, index and
// collection drops, a compaction and the log trim at close are each
// interrupted at trip points spread over every write they make, the file
// layer then loses a random suffix of unsynced writes, and the reopened
// database must be consistent: verified, indexes either whole or absent
// with no stray entries, documents intact, the operation repeatable.

// crashLab installs the fault layer for one test.
type crashLab struct {
	t    *testing.T
	dir  string
	ctl  *faultCtl
	orig func(string) (fileLike, error)
}

func newCrashLab(t *testing.T, seed int64) *crashLab {
	l := &crashLab{t: t, dir: t.TempDir(), ctl: &faultCtl{rng: rand.New(rand.NewSource(seed))}, orig: openFile}
	openFile = l.ctl.open
	t.Cleanup(func() {
		l.ctl.release()
		openFile = l.orig
	})
	return l
}

// arm makes the trip-th write from now fail, and every write after it;
// 0 disarms.
func (l *crashLab) arm(trip int) {
	l.ctl.mu.Lock()
	l.ctl.writes, l.ctl.tripped, l.ctl.trip = 0, false, trip
	l.ctl.mu.Unlock()
}

func (l *crashLab) writes() int {
	l.ctl.mu.Lock()
	defer l.ctl.mu.Unlock()
	return l.ctl.writes
}

// crash loses a random suffix of unsynced writes, drops the handles and
// brings the file layer back.
func (l *crashLab) crash(db *DB) {
	l.ctl.crash()
	db.crash()
	l.arm(0)
}

func (l *crashLab) open(opts Options) *DB {
	db, err := Open(l.dir, opts)
	if err != nil {
		l.t.Fatalf("open: %v", err)
	}
	return db
}

// expectedFailure reports whether err is what an interrupted operation
// may return: the trip itself, a database that has since refused writes,
// or nothing at all when the trip fell after the operation's last write.
func expectedFailure(err error) bool {
	return err == nil || errors.Is(err, errTripped) || errors.Is(err, ErrClosed) || isFailed(err)
}

// sweep runs body at trip points spread over an operation's writes and
// insists that the operation was interrupted in most rounds: a sweep that
// never trips would test nothing.
func (l *crashLab) sweep(what string, total, n int, body func(trip int) error) {
	l.t.Helper()
	if total == 0 {
		l.t.Fatalf("%s: the dry run made no writes", what)
	}
	tripped := 0
	pts := tripPoints(total, n)
	for _, trip := range pts {
		if err := body(trip); err != nil {
			tripped++
		}
	}
	l.t.Logf("%s: %d writes, %d trip points, %d interrupted", what, total, len(pts), tripped)
	if tripped < len(pts)/2 {
		l.t.Fatalf("%s: only %d of %d rounds were interrupted", what, tripped, len(pts))
	}
}

// tripPoints spreads up to n trip points over 1..total.
func tripPoints(total, n int) []int {
	if total <= 0 {
		return nil
	}
	if total <= n {
		pts := make([]int, total)
		for i := range pts {
			pts[i] = i + 1
		}
		return pts
	}
	pts := make([]int, n)
	for i := range pts {
		pts[i] = 1 + i*(total-1)/(n-1)
	}
	return pts
}

var crashOpts = []Options{
	{Sync: true, SyncMode: SyncFull, LogFlushInterval: time.Hour},
	{Sync: true, SyncMode: SyncFull, LogFlushInterval: time.Hour, CacheBytes: 64 * PageSize, MemtableBytes: 128 << 10},
}

func loadCrashDocs(t *testing.T, c *Collection, n int) {
	b := c.NewBatch()
	for i := 0; i < n; i++ {
		if err := b.InsertJSON([]byte(fmt.Sprintf("d%05d", i)), []byte(fmt.Sprintf(`{"g":%d,"tags":[%d,%d],"pad":"%032d"}`, i%17, i%5, i%7, i))); err != nil {
			t.Fatal(err)
		}
		if b.Len() >= 500 {
			if err := b.Commit(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
}

// checkConsistent verifies the tree, the indexes, and that no index entry
// belongs to an index the catalog does not know.
func checkConsistent(t *testing.T, db *DB, where string) {
	t.Helper()
	if err := db.Verify(bg); err != nil {
		t.Fatalf("%s: verify: %v", where, err)
	}
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatalf("%s: verify indexes: %v", where, err)
	}
	known := map[uint32]bool{}
	for _, d := range db.indexDefs() {
		known[d.ID] = true
	}
	lo := []byte{reservedPrefix, 'i'}
	stray := 0
	db.scan(view{db, nil}, lo, prefixEnd(lo), func(k, _ []byte) bool {
		if len(k) >= 6 && !known[binary.BigEndian.Uint32(k[2:])] {
			stray++
		}
		return true
	})
	if stray > 0 {
		t.Fatalf("%s: %d index entries without an index", where, stray)
	}
}

// countDocs counts the documents of c whose g field is what the load wrote.
func countDocs(t *testing.T, c *Collection, where string) int {
	t.Helper()
	n := 0
	err := c.Scan(nil, nil, func(k, doc []byte) bool {
		var i int
		fmt.Sscanf(string(k), "d%05d", &i)
		if g := docField(doc, "g"); g.k != kInt || g.i != int64(i%17) {
			t.Fatalf("%s: document %s damaged", where, k)
		}
		n++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCrashDuringIndexBuild(t *testing.T) {
	for oi, opts := range crashOpts {
		lab := newCrashLab(t, int64(31+oi))
		db := lab.open(opts)
		c, _ := db.Collection("c")
		loadCrashDocs(t, c, 3000)
		if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		// a dry run counts the writes a build makes
		lab.arm(0)
		if err := c.CreateIndex(bg, "probe", "g"); err != nil {
			t.Fatal(err)
		}
		total := lab.writes()
		if err := c.DropIndex(bg, "probe"); err != nil {
			t.Fatal(err)
		}
		db.Checkpoint()
		want, _ := c.Aggregate(bg, []byte(`[{"$match":{"g":3}},{"$count":"n"}]`))
		round := 0
		lab.sweep(fmt.Sprintf("opts %d index build", oi), total, 20, func(trip int) error {
			round++
			name := fmt.Sprintf("ix%02d", round)
			lab.arm(trip)
			buildErr := c.CreateIndex(bg, name, "g")
			if !expectedFailure(buildErr) {
				t.Fatalf("opts %d trip %d/%d: %v", oi, trip, total, buildErr)
			}
			lab.crash(db)
			db = lab.open(opts)
			where := fmt.Sprintf("opts %d trip %d/%d", oi, trip, total)
			checkConsistent(t, db, where)
			c, _ = db.Collection("c")
			if n := countDocs(t, c, where); n != 3000 {
				t.Fatalf("%s: %d documents", where, n)
			}
			// the index is whole or absent; either way the query is right
			// and a build can be run again
			if _, present := c.Indexes()[name]; !present {
				if err := c.CreateIndex(bg, name, "g"); err != nil {
					t.Fatalf("%s: rebuild: %v", where, err)
				}
			}
			before := db.Stats().IndexScans
			got, err := c.Aggregate(bg, []byte(`[{"$match":{"g":3}},{"$count":"n"}]`))
			if err != nil || got[0]["n"] != want[0]["n"] {
				t.Fatalf("%s: query %v %v, want %v", where, got, err, want)
			}
			if db.Stats().IndexScans == before {
				t.Fatalf("%s: index not used", where)
			}
			checkConsistent(t, db, where+" after rebuild")
			if err := c.DropIndex(bg, name); err != nil {
				t.Fatalf("%s: drop: %v", where, err)
			}
			db.Checkpoint()
			return buildErr
		})
		db.Close()
	}
}

func TestCrashDuringDrops(t *testing.T) {
	for oi, opts := range crashOpts {
		lab := newCrashLab(t, int64(41+oi))
		db := lab.open(opts)
		c, _ := db.Collection("c")
		keep, _ := db.Collection("keep")
		loadCrashDocs(t, c, 3000)
		loadCrashDocs(t, keep, 500)
		build := func(c *Collection) {
			for _, ix := range [][2]string{{"by_g", "g"}, {"by_tag", "tags"}} {
				if err := c.CreateIndex(bg, ix[0], ix[1]); err != nil && !errors.Is(err, ErrIndexExists) {
					t.Fatal(err)
				}
			}
		}
		build(c)
		build(keep)
		db.Checkpoint()
		// dry runs for the write counts
		lab.arm(0)
		if err := c.DropIndex(bg, "by_g"); err != nil {
			t.Fatal(err)
		}
		dropIndexWrites := lab.writes()
		build(c)
		lab.arm(0)
		if err := db.DropCollection(bg, "c"); err != nil {
			t.Fatal(err)
		}
		dropCollWrites := lab.writes()
		c, _ = db.Collection("c")
		loadCrashDocs(t, c, 3000)
		build(c)
		db.Checkpoint()

		lab.sweep(fmt.Sprintf("opts %d drop index", oi), dropIndexWrites, 12, func(trip int) error {
			where := fmt.Sprintf("opts %d drop index trip %d/%d", oi, trip, dropIndexWrites)
			lab.arm(trip)
			err := c.DropIndex(bg, "by_g")
			if !expectedFailure(err) {
				t.Fatalf("%s: %v", where, err)
			}
			lab.crash(db)
			db = lab.open(opts)
			checkConsistent(t, db, where)
			c, _ = db.Collection("c")
			keep, _ = db.Collection("keep")
			if n := countDocs(t, c, where); n != 3000 {
				t.Fatalf("%s: %d documents", where, n)
			}
			if got := keep.Indexes(); len(got) != 2 {
				t.Fatalf("%s: keep lost indexes: %v", where, got)
			}
			// gone or whole: dropping again ends with it gone
			if err := c.DropIndex(bg, "by_g"); err != nil && !errors.Is(err, ErrNoSuchIndex) {
				t.Fatalf("%s: drop again: %v", where, err)
			}
			checkConsistent(t, db, where+" after second drop")
			build(c)
			db.Checkpoint()
			return err
		})
		lab.sweep(fmt.Sprintf("opts %d drop collection", oi), dropCollWrites, 16, func(trip int) error {
			where := fmt.Sprintf("opts %d drop collection trip %d/%d", oi, trip, dropCollWrites)
			lab.arm(trip)
			err := db.DropCollection(bg, "c")
			if !expectedFailure(err) {
				t.Fatalf("%s: %v", where, err)
			}
			lab.crash(db)
			db = lab.open(opts)
			checkConsistent(t, db, where)
			keep, _ = db.Collection("keep")
			if n := countDocs(t, keep, where); n != 500 {
				t.Fatalf("%s: keep has %d documents", where, n)
			}
			listed := false
			for _, n := range db.Collections() {
				if n == "c" {
					listed = true
				}
			}
			if listed {
				// partial: finish the job
				if err := db.DropCollection(bg, "c"); err != nil {
					t.Fatalf("%s: drop again: %v", where, err)
				}
			}
			for _, n := range db.Collections() {
				if n == "c" {
					t.Fatalf("%s: collection still listed", where)
				}
			}
			checkConsistent(t, db, where+" after second drop")
			// nothing of it remains in the tree, and the name is reusable
			c, _ = db.Collection("c")
			if n := countDocs(t, c, where); n != 0 {
				t.Fatalf("%s: %d keys survived the drop", where, n)
			}
			loadCrashDocs(t, c, 3000)
			build(c)
			db.Checkpoint()
			return err
		})
		db.Close()
	}
}

func TestCrashDuringCompact(t *testing.T) {
	for oi, opts := range crashOpts {
		lab := newCrashLab(t, int64(51+oi))
		db := lab.open(opts)
		c, _ := db.Collection("c")
		loadCrashDocs(t, c, 3000)
		if err := c.CreateIndex(bg, "by_g", "g"); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3000; i += 3 {
			c.Delete([]byte(fmt.Sprintf("d%05d", i)))
		}
		db.Checkpoint()
		lab.arm(0)
		if err := db.Compact(bg); err != nil {
			t.Fatal(err)
		}
		total := lab.writes()
		lab.sweep(fmt.Sprintf("opts %d compact", oi), total, 20, func(trip int) error {
			where := fmt.Sprintf("opts %d compact trip %d/%d", oi, trip, total)
			// some writes between compactions, so each has a log delta to catch up
			for i := 1; i < 3000; i += 300 {
				c.InsertJSON([]byte(fmt.Sprintf("d%05d", i)), []byte(fmt.Sprintf(`{"g":%d,"tags":[1],"pad":"x"}`, i%17)))
			}
			lab.arm(trip)
			err := db.Compact(bg)
			if !expectedFailure(err) {
				t.Fatalf("%s: %v", where, err)
			}
			lab.crash(db)
			db = lab.open(opts)
			checkConsistent(t, db, where)
			c, _ = db.Collection("c")
			if n := countDocs(t, c, where); n != 2000 {
				t.Fatalf("%s: %d documents, want 2000", where, n)
			}
			for _, leftover := range []string{"compact.tmp", "data.old"} {
				if _, err := os.Stat(filepath.Join(lab.dir, leftover)); !os.IsNotExist(err) {
					t.Fatalf("%s: %s left behind", where, leftover)
				}
			}
			if err := db.Compact(bg); err != nil {
				t.Fatalf("%s: compact again: %v", where, err)
			}
			checkConsistent(t, db, where+" after compact")
			return err
		})
		db.Close()
	}
}

func TestCrashDuringLogTrimAtClose(t *testing.T) {
	for oi, opts := range crashOpts {
		lab := newCrashLab(t, int64(61+oi))
		db := lab.open(opts)
		c, _ := db.Collection("c")
		// enough log for a trim: past logTrimBytes
		b := c.NewBatch()
		pad := make([]byte, 300)
		for i := range pad {
			pad[i] = 'x'
		}
		for i := 0; i < 20_000; i++ {
			b.InsertJSON([]byte(fmt.Sprintf("d%05d", i)), []byte(fmt.Sprintf(`{"g":%d,"tags":[0],"pad":"%s"}`, i%17, pad)))
			if b.Len() >= 1000 {
				if err := b.Commit(); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := b.Commit(); err != nil {
			t.Fatal(err)
		}
		if size := db.wal.size(); size < logTrimBytes {
			t.Fatalf("log only %d bytes, trim needs %d", size, logTrimBytes)
		}
		// dry run of a close to count its writes, then reopen and refill
		lab.arm(0)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		total := lab.writes()
		lab.sweep(fmt.Sprintf("opts %d close", oi), total, 12, func(trip int) error {
			where := fmt.Sprintf("opts %d close trip %d/%d", oi, trip, total)
			db = lab.open(opts)
			c, _ = db.Collection("c")
			b := c.NewBatch()
			for i := 0; i < 20_000; i++ {
				b.InsertJSON([]byte(fmt.Sprintf("d%05d", i)), []byte(fmt.Sprintf(`{"g":%d,"tags":[1],"pad":"%s"}`, i%17, pad)))
				if b.Len() >= 1000 {
					if err := b.Commit(); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := b.Commit(); err != nil {
				t.Fatal(err)
			}
			lab.arm(trip)
			err := db.Close()
			if !expectedFailure(err) {
				t.Fatalf("%s: %v", where, err)
			}
			lab.crash(db)
			db = lab.open(opts)
			checkConsistent(t, db, where)
			c, _ = db.Collection("c")
			if n := countDocs(t, c, where); n != 20_000 {
				t.Fatalf("%s: %d documents", where, n)
			}
			lab.arm(0)
			if err := db.Close(); err != nil {
				t.Fatalf("%s: clean close: %v", where, err)
			}
			if gens, _ := walGens(lab.dir); len(gens) != 1 {
				t.Fatalf("%s: %d log generations after a clean close: %v", where, len(gens), gens)
			}
			return err
		})
	}
}
