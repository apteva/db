package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func feedDB(t *testing.T, o Options) *DB {
	t.Helper()
	if o.ChangeFeed == nil {
		o.ChangeFeed = &ChangeFeedOptions{}
	}
	d, err := Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !d.closed {
			d.Close()
		}
	})
	return d
}
func feedWatch(t *testing.T, d *DB, o WatchOptions) *ChangeCursor {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	c, err := d.Watch(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func nextChange(t *testing.T, c *ChangeCursor) Change {
	t.Helper()
	if !c.Next() {
		t.Fatal("missing change", c.Err())
	}
	return c.Change().Clone()
}

func TestChangeFeedBasic(t *testing.T) {
	for _, syncWrite := range []bool{false, true} {
		t.Run(fmt.Sprint(syncWrite), func(t *testing.T) {
			d := feedDB(t, Options{Sync: syncWrite, SyncMode: SyncOS})
			a := mustCollection(t, d, "a")
			b := mustCollection(t, d, "b")
			cur := feedWatch(t, d, WatchOptions{IncludeValues: true})
			anchor := cur.ResumeToken()
			batch := a.NewBatch()
			mustCompat(t, batch.Upsert([]byte("x"), []byte("first")))
			mustCompat(t, batch.Upsert([]byte("x"), []byte("final")))
			mustCompat(t, batch.With(b).Upsert([]byte("y"), []byte("second")))
			mustCompat(t, batch.Commit())
			one := nextChange(t, cur)
			two := nextChange(t, cur)
			if one.Collection != "a" || string(one.Key) != "x" || string(one.Value) != "final" || one.EndOfCommit || two.Collection != "b" || !two.EndOfCommit || one.CommitToken != two.CommitToken {
				t.Fatal(one, two)
			}
			if cur.ResumeToken() != two.CommitToken {
				t.Fatal("bad watermark")
			}
			mustCompat(t, a.Delete([]byte("x")))
			three := nextChange(t, cur)
			if three.Operation != ChangeDelete || three.HasValue || !three.EndOfCommit {
				t.Fatal(three)
			}
			replay := feedWatch(t, d, WatchOptions{After: anchor, IncludeValues: true})
			for _, want := range []Change{one, two, three} {
				got := nextChange(t, replay)
				if got.Operation != want.Operation || got.Collection != want.Collection || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || got.EndOfCommit != want.EndOfCommit {
					t.Fatal(got, want)
				}
			}
			mustCompat(t, cur.Close())
			mustCompat(t, replay.Close())
			stats := d.ChangeFeedStats()
			if stats.Watchers != 0 || stats.ReaderBytes != 0 {
				t.Fatal(stats)
			}
		})
	}
}
func TestChangeFeedFiltersProjectionAndProgress(t *testing.T) {
	d := feedDB(t, Options{})
	orders := mustCollection(t, d, "orders")
	opts := WatchOptions{Collections: []string{"orders"}, Operations: []ChangeOp{ChangePut}, Filter: []byte(`{"$and":[{"tenant":"a"},{"$or":[{"n":{"$gte":9007199254740993}},{"nested.name":{"$prefix":"ab.["}}]}]}`), IncludeValues: true, Fields: []string{"n"}, HeartbeatInterval: 5 * time.Millisecond}
	c := feedWatch(t, d, opts)
	same := feedWatch(t, d, opts)
	mustCompat(t, orders.InsertJSON([]byte("low"), []byte(`{"tenant":"a","n":9007199254740992}`)))
	mustCompat(t, orders.InsertJSON([]byte("hit"), []byte(`{"tenant":"a","n":9007199254740993,"unused":"long"}`)))
	for _, cur := range []*ChangeCursor{c, same} {
		r := nextChange(t, cur)
		if r.Operation == ChangeProgress {
			r = nextChange(t, cur)
		}
		if string(r.Key) != "hit" || !r.EndOfCommit {
			t.Fatal(r)
		}
		var doc map[string]any
		mustCompat(t, r.Decode(&doc))
		if len(doc) != 1 || doc["n"] != int64(9007199254740993) {
			t.Fatal(doc)
		}
	}
	if s := d.ChangeFeedStats(); s.FilterGroups != 1 {
		t.Fatal(s)
	}
	mustCompat(t, orders.Delete([]byte("hit")))
	progress := nextChange(t, c)
	if progress.Operation != ChangeProgress || !progress.EndOfCommit {
		t.Fatal(progress)
	}
	if progress.CommitToken == "" || progress.CommitToken != c.ResumeToken() {
		t.Fatal("progress did not advance")
	}
}
func TestChangeFeedReopenCompactAndDisable(t *testing.T) {
	d := feedDB(t, Options{Sync: true, SyncMode: SyncOS})
	c := feedWatch(t, d, WatchOptions{IncludeValues: true})
	anchor := c.ResumeToken()
	mustCompat(t, d.Upsert([]byte("a"), []byte("one")))
	first := nextChange(t, c)
	mustCompat(t, c.Close())
	mustCompat(t, d.Checkpoint())
	mustCompat(t, d.Compact(bg))
	mustCompat(t, d.Close())
	var err error
	d, err = Open(d.dir, Options{Sync: true, SyncMode: SyncOS})
	mustCompat(t, err)
	defer d.Close()
	c = feedWatch(t, d, WatchOptions{After: anchor, IncludeValues: true})
	got := nextChange(t, c)
	if string(got.Value) != "one" || got.CommitToken != first.CommitToken {
		t.Fatal(got)
	}
	mustCompat(t, d.Upsert([]byte("b"), []byte("two")))
	got = nextChange(t, c)
	if string(got.Key) != "b" {
		t.Fatal(got)
	}
	token := got.CommitToken
	mustCompat(t, c.Close())
	mustCompat(t, d.Close())
	d, err = Open(d.dir, Options{ChangeFeed: &ChangeFeedOptions{Disabled: true}})
	mustCompat(t, err)
	if _, err = d.Watch(bg, WatchOptions{}); !errors.Is(err, ErrChangeFeedDisabled) {
		t.Fatal(err)
	}
	mustCompat(t, d.Upsert([]byte("c"), []byte("three")))
	mustCompat(t, d.Close())
	d, err = Open(d.dir, Options{ChangeFeed: &ChangeFeedOptions{}})
	mustCompat(t, err)
	if _, err = d.Watch(bg, WatchOptions{After: token}); !errors.Is(err, ErrResumeInvalid) {
		t.Fatal(err)
	}
}
func TestChangeFeedComputedMembershipAndSchema(t *testing.T) {
	d := feedDB(t, Options{})
	source := mustCollection(t, d, "orders")
	mustCompat(t, source.InsertJSON([]byte("a"), []byte(`{"status":"paid","amount":10}`)))
	all := feedWatch(t, d, WatchOptions{})
	view, err := d.CreateComputedCollection(bg, "paid", "orders", []byte(`[{"$match":{"status":"paid"}},{"$project":{"amount":1}}]`))
	mustCompat(t, err)
	r := nextChange(t, all)
	if r.Operation != ChangeReset || r.Collection != "paid" {
		t.Fatal(r)
	}
	c, err := view.Watch(bg, WatchOptions{IncludeValues: true})
	mustCompat(t, err)
	defer c.Close()
	mustCompat(t, source.ReplaceJSON([]byte("a"), []byte(`{"status":"cancelled","amount":10}`)))
	r = nextChange(t, c)
	if r.Operation != ChangeDelete || string(r.Key) != "a" {
		t.Fatal(r)
	}
	mustCompat(t, source.ReplaceJSON([]byte("a"), []byte(`{"status":"paid","amount":20}`)))
	r = nextChange(t, c)
	if r.Operation != ChangePut {
		t.Fatal(r)
	}
	mustCompat(t, d.DropCollection(bg, "paid"))
	r = nextChange(t, c)
	if r.Operation != ChangeDrop {
		t.Fatal(r)
	}
}
func TestChangeFeedConcurrentCloseAndRollback(t *testing.T) {
	d := feedDB(t, Options{})
	c := feedWatch(t, d, WatchOptions{})
	tx, err := d.Begin(bg)
	mustCompat(t, err)
	mustCompat(t, tx.Upsert([]byte("discard"), []byte("x")))
	tx.Rollback()
	mustCompat(t, d.Upsert([]byte("keep"), []byte("x")))
	r := nextChange(t, c)
	if string(r.Key) != "keep" {
		t.Fatal(r)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); c.Next() }()
	time.Sleep(time.Millisecond)
	mustCompat(t, c.Close())
	wg.Wait()
	if s := d.ChangeFeedStats(); s.ReaderBytes != 0 || s.Watchers != 0 {
		t.Fatal(s)
	}
}

func TestChangeFeedSnapshotHandoff(t *testing.T) {
	d := feedDB(t, Options{})
	mustCompat(t, d.Upsert([]byte("a"), []byte("before")))
	s, c, err := d.SnapshotAndWatch(bg, WatchOptions{IncludeValues: true})
	mustCompat(t, err)
	defer s.Close()
	defer c.Close()
	mustCompat(t, d.Upsert([]byte("a"), []byte("after")))
	v, ok, err := s.Find([]byte("a"), nil)
	mustCompat(t, err)
	if !ok || string(v) != "before" {
		t.Fatal(string(v), ok)
	}
	r := nextChange(t, c)
	if string(r.Value) != "after" {
		t.Fatal(r)
	}
}
func TestChangeFeedHistoryExpiryAndFreshHandoff(t *testing.T) {
	d := feedDB(t, Options{ChangeFeed: &ChangeFeedOptions{RetentionBytes: 4096, CacheBytes: 1024, RetentionAge: 100 * time.Millisecond}})
	c := feedWatch(t, d, WatchOptions{})
	token := c.ResumeToken()
	mustCompat(t, c.Close())
	for i := 0; i < 100; i++ {
		mustCompat(t, d.Upsert([]byte(fmt.Sprint(i)), make([]byte, 200)))
	}
	if _, err := d.Watch(bg, WatchOptions{After: token}); !errors.Is(err, ErrHistoryExpired) {
		t.Fatal(err)
	}
	mustCompat(t, d.wal.rotate())
	time.Sleep(150 * time.Millisecond)
	mustCompat(t, d.Checkpoint())
	s, live, err := d.SnapshotAndWatch(bg, WatchOptions{})
	mustCompat(t, err)
	s.Close()
	live.Close()
	fresh := feedWatch(t, d, WatchOptions{})
	mustCompat(t, d.Upsert([]byte("fresh"), []byte("ok")))
	r := nextChange(t, fresh)
	if string(r.Key) != "fresh" {
		t.Fatal(r)
	}
}
func TestChangeFeedCrashTornTailAndTokenReuse(t *testing.T) {
	d := feedDB(t, Options{Sync: true, SyncMode: SyncOS})
	c := feedWatch(t, d, WatchOptions{})
	mustCompat(t, d.Upsert([]byte("kept"), []byte("a")))
	kept := nextChange(t, c)
	mustCompat(t, c.Close())
	mustCompat(t, d.Checkpoint())
	c = feedWatch(t, d, WatchOptions{})
	mustCompat(t, d.Upsert([]byte("lost"), []byte("b")))
	lost := nextChange(t, c)
	mustCompat(t, c.Close())
	gen, end := d.wal.position()
	d.crash()
	// Tear the final marker, leaving its user record intact. The group must not
	// recover without its final marker, and its issued token must never alias.
	mustCompat(t, os.Truncate(walPath(d.dir, gen), int64(end-1)))
	var err error
	d, err = Open(d.dir, Options{Sync: true, SyncMode: SyncOS})
	mustCompat(t, err)
	defer d.Close()
	_, found, err := d.Find([]byte("lost"), nil)
	mustCompat(t, err)
	if found {
		t.Fatal("torn feed group recovered")
	}
	c = feedWatch(t, d, WatchOptions{After: kept.CommitToken})
	mustCompat(t, d.Upsert([]byte("lost"), []byte("b")))
	again := nextChange(t, c)
	if again.CommitToken == lost.CommitToken {
		t.Fatal("token reused after recovery")
	}
	if _, err = d.Watch(bg, WatchOptions{After: lost.CommitToken}); !errors.Is(err, ErrResumeInvalid) {
		t.Fatal(err)
	}
}
func TestChangeFeedBudgetAndSharedFilters(t *testing.T) {
	d := feedDB(t, Options{ChangeFeed: &ChangeFeedOptions{CacheBytes: 256 << 10, MaxWatchBytes: 1 << 20}})
	opts := WatchOptions{Filter: []byte(`{"n":{"$gte":1}}`), IncludeValues: true, Fields: []string{"n"}}
	cursors := make([]*ChangeCursor, 100)
	for i := range cursors {
		cursors[i] = feedWatch(t, d, opts)
	}
	data := []byte(`{"n":2,"padding":"` + strings.Repeat("a", 80<<10) + `"}`)
	mustCompat(t, d.UpsertJSON([]byte("x"), data))
	for _, c := range cursors {
		r := nextChange(t, c)
		if len(r.Value) > 128 {
			t.Fatal("projection retained omitted payload")
		}
		c.Close()
	}
	stats := d.ChangeFeedStats()
	if stats.ReaderBytes != 0 || stats.CacheBytes > 256<<10 || stats.PeakReaderBytes > 1<<20 || stats.Watchers != 0 {
		t.Fatal(stats)
	}
	if _, err := d.Watch(bg, WatchOptions{Filter: []byte(`{"n":9223372036854775808}`)}); err == nil {
		t.Fatal("accepted overflowing integer filter")
	}
	c := feedWatch(t, d, WatchOptions{IncludeValues: true, MaxMemoryBytes: 8192})
	mustCompat(t, d.UpsertJSON([]byte("large"), data))
	if c.Next() || !errors.Is(c.Err(), ErrLimit) {
		t.Fatal(c.Err())
	}
	if d.Health() != nil {
		t.Fatal("reader limit affected DB health")
	}
}
func TestChangeFeedHistoricalValuesBackupAndCaptureLimit(t *testing.T) {
	d := feedDB(t, Options{ChangeFeed: &ChangeFeedOptions{CacheBytes: 1024}})
	c := feedWatch(t, d, WatchOptions{IncludeValues: true})
	token := c.ResumeToken()
	c.Close()
	for _, s := range []string{"one", "two", "three"} {
		mustCompat(t, d.Upsert([]byte("k"), []byte(s)))
	}
	c = feedWatch(t, d, WatchOptions{After: token, IncludeValues: true})
	for _, want := range []string{"one", "two", "three"} {
		r := nextChange(t, c)
		if string(r.Value) != want {
			t.Fatal(r, want)
		}
	}
	c.Close()
	backup := filepath.Join(t.TempDir(), "backup")
	mustCompat(t, d.Backup(bg, backup))
	copy, err := Open(backup, Options{})
	mustCompat(t, err)
	defer copy.Close()
	if _, err = copy.Watch(bg, WatchOptions{After: token}); !errors.Is(err, ErrResumeInvalid) {
		t.Fatal(err)
	}
	small := feedDB(t, Options{ChangeFeed: &ChangeFeedOptions{MaxCaptureBytes: 128}})
	if err = small.Upsert([]byte("a"), []byte("b")); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	_, ok, err := small.Find([]byte("a"), nil)
	mustCompat(t, err)
	if ok {
		t.Fatal("capture limit partly wrote")
	}
}
func TestChangeFeedDurabilityFence(t *testing.T) {
	d := feedDB(t, Options{Sync: true, SyncMode: SyncOS})
	c := feedWatch(t, d, WatchOptions{})
	orig := d.wal.syncFn
	started := make(chan struct{})
	release := make(chan struct{})
	d.wal.syncFn = func(f fileLike) error { close(started); <-release; return orig(f) }
	wrote := make(chan error, 1)
	go func() { wrote <- d.Upsert([]byte("a"), []byte("b")) }()
	<-started
	read := make(chan bool, 1)
	go func() { read <- c.Next() }()
	select {
	case <-read:
		t.Fatal("event before durability fence")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	mustCompat(t, <-wrote)
	if !<-read {
		t.Fatal(c.Err())
	}
	d.wal.syncFn = orig
	c.Close()
}

func TestFormat9ChangeFeedUpgrade(t *testing.T) {
	dir := t.TempDir()
	files, err := filepath.Glob("testdata/format9/*")
	mustCompat(t, err)
	for _, name := range files {
		base := filepath.Base(name)
		if base != "data" && !strings.HasPrefix(base, "wal-") {
			continue
		}
		data, err := os.ReadFile(name)
		mustCompat(t, err)
		mustCompat(t, os.WriteFile(filepath.Join(dir, base), data, 0600))
	}
	version, err := FormatVersionOf(dir)
	mustCompat(t, err)
	if version != 9 {
		t.Fatal(version)
	}
	d, err := Open(dir, Options{ChangeFeed: &ChangeFeedOptions{}})
	mustCompat(t, err)
	defer d.Close()
	data, err := os.ReadFile(filepath.Join(dir, "data"))
	mustCompat(t, err)
	for _, off := range []int{0, PageSize} {
		if le.Uint32(data[off+8:]) != 10 {
			t.Fatal("old writer could reopen upgraded metadata")
		}
	}
	source, err := d.OpenCollection("source")
	mustCompat(t, err)
	cur := feedWatch(t, d, WatchOptions{Collections: []string{"source", "present"}, IncludeValues: true})
	mustCompat(t, source.UpsertJSON([]byte("b"), []byte(`{"v":false}`)))
	first := nextChange(t, cur)
	second := nextChange(t, cur)
	if first.Collection != "source" || second.Collection != "present" || first.CommitToken != second.CommitToken || !second.EndOfCommit {
		t.Fatal(first, second)
	}
	var row map[string]any
	mustCompat(t, second.Decode(&row))
	if row["n"] != int64(2) {
		t.Fatal(row)
	}
	mustCompat(t, d.Verify(bg))
}

func FuzzChangeMarker(f *testing.F) {
	table := crc32.MakeTable(crc32.Castagnoli)
	feed := &changeFeed{db: &DB{wal: &wal{crcTab: table}}}
	p := make([]byte, changeHeaderBytes+4)
	copy(p, changeMagic)
	le.PutUint64(p[48:], 10)
	p = le.AppendUint32(p, uint32(len(p)+23))
	p = append(p, changeFooter...)
	enc := encodeRecord(nil, nil, p, false, false, table)
	enc[8] = flagFeed
	le.PutUint32(enc[4:], crc32.Checksum(enc[8:], table))
	f.Add(enc)
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 64<<10 {
			return
		}
		_, _ = feed.parseMarker(b, lsn{1, 1 << 32})
	})
}

func TestChangeFeedStress(t *testing.T) {
	durationText := os.Getenv("CHANGE_FEED_STRESS")
	if durationText == "" {
		t.Skip("set CHANGE_FEED_STRESS=2m")
	}
	duration, err := time.ParseDuration(durationText)
	mustCompat(t, err)
	d := feedDB(t, Options{})
	source := mustCollection(t, d, "source")
	view, err := d.CreateComputedCollection(bg, "counts", "source", []byte(`[{"$group":{"_id":null,"n":{"$count":{}}}}]`))
	mustCompat(t, err)
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	cursors := []*ChangeCursor{}
	for i := 0; i < 10; i++ {
		cur, err := d.Watch(ctx, WatchOptions{Collections: []string{"source"}, IncludeValues: true})
		mustCompat(t, err)
		cursors = append(cursors, cur)
		wg.Add(1)
		go func(c *ChangeCursor) {
			defer wg.Done()
			var last, commitMax int64 = -1, -1
			var prior lsn
			for c.Next() {
				r := c.Change()
				if r.Operation == ChangePut {
					var row map[string]any
					if err := r.Decode(&row); err != nil {
						errs <- err
						return
					}
					n := row["seq"].(int64)
					if n <= last {
						errs <- fmt.Errorf("old value delivered after newer commit: %d <= %d", n, last)
						return
					}
					commitMax = max(commitMax, n)
				}
				if r.EndOfCommit {
					at, _, err := c.f.decodeToken(r.CommitToken)
					if err != nil {
						errs <- err
						return
					}
					if prior.gen != 0 && !lsnLess(prior, at) {
						errs <- fmt.Errorf("unordered commit token")
						return
					}
					prior = at
					last = commitMax
				}
			}
			if ctx.Err() == nil {
				errs <- c.Err()
			}
		}(cur)
	}
	for i := 0; i < 100; i++ {
		cur, err := d.Watch(ctx, WatchOptions{})
		mustCompat(t, err)
		cursors = append(cursors, cur)
	}
	defer func() {
		cancel()
		for _, c := range cursors {
			c.Close()
		}
		wg.Wait()
	}()
	rng := rand.New(rand.NewSource(51))
	present := map[int]bool{}
	sequence := 0
	round := 0
	start := time.Now()
	for time.Since(start) < duration {
		select {
		case err := <-errs:
			t.Fatal(err)
		default:
		}
		batch := source.NewBatch()
		for i := 0; i < 100; i++ {
			key := rng.Intn(500)
			k := []byte(fmt.Sprintf("%03d", key))
			if rng.Intn(5) == 0 {
				mustCompat(t, batch.Delete(k))
				delete(present, key)
			} else {
				doc, err := EncodeJSON(nil, []byte(fmt.Sprintf(`{"seq":%d}`, sequence)))
				mustCompat(t, err)
				mustCompat(t, batch.Upsert(k, doc))
				present[key] = true
			}
			sequence++
		}
		mustCompat(t, batch.Commit())
		round++
		if round%500 == 0 {
			mustCompat(t, d.wal.rotate())
			s, watch, err := d.SnapshotAndWatch(bg, WatchOptions{})
			mustCompat(t, err)
			s.Close()
			watch.Close()
			var row map[string]any
			k, err := ComputedGroupKey(nil)
			mustCompat(t, err)
			ok, err := view.FindDocument(k, &row)
			mustCompat(t, err)
			if !ok || row["n"] != int64(len(present)) {
				t.Fatal(row, len(present))
			}
		}
		stats := d.ChangeFeedStats()
		if stats.CacheBytes > d.feed.cfg.Options.CacheBytes || stats.ReaderBytes+stats.PlanBytes > d.feed.cfg.Options.MaxWatchBytes {
			t.Fatal(stats)
		}
	}
	cancel()
	for _, c := range cursors {
		c.Close()
	}
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if stats := d.ChangeFeedStats(); stats.Watchers != 0 || stats.ReaderBytes != 0 || stats.PlanBytes != 0 {
		t.Fatal(stats)
	}
	t.Logf("%d logical writes, %d commits, 10 readers and 100 idle subscribers", sequence, round)
}

func TestChangeFeedLargeDocumentColdProjection(t *testing.T) {
	d := feedDB(t, Options{ChangeFeed: &ChangeFeedOptions{CacheBytes: 1024}})
	c := feedWatch(t, d, WatchOptions{IncludeValues: true, Fields: []string{"padding"}})
	doc := []byte(`{"padding":"` + strings.Repeat("x", MaxDocumentSize-128) + `"}`)
	mustCompat(t, d.UpsertJSON([]byte("large"), doc))
	r := nextChange(t, c)
	var got map[string]any
	mustCompat(t, r.Decode(&got))
	if len(got["padding"].(string)) != MaxDocumentSize-128 {
		t.Fatal("large value truncated")
	}
	c.Close()
	if s := d.ChangeFeedStats(); s.ReaderBytes != 0 {
		t.Fatal(s)
	}
}

// Delivery projections must be budgeted even when their metadata match plan
// was first installed by a cursor with no projection.
func TestChangeFeedProjectionPlanBudgets(t *testing.T) {
	d := feedDB(t, Options{ChangeFeed: &ChangeFeedOptions{MaxWatchBytes: 4096}})
	metadata := feedWatch(t, d, WatchOptions{})
	before := d.ChangeFeedStats()
	fields := make([]string, 64)
	for i := range fields {
		fields[i] = fmt.Sprintf("field%03d", i)
	}
	if c, err := d.Watch(context.Background(), WatchOptions{IncludeValues: true, Fields: fields}); !errors.Is(err, ErrLimit) {
		if c != nil {
			c.Close()
		}
		t.Fatal(err)
	}
	if after := d.ChangeFeedStats(); after.PlanBytes != before.PlanBytes || after.Watchers != before.Watchers {
		t.Fatal(before, after)
	}
	small := feedWatch(t, d, WatchOptions{IncludeValues: true, Fields: []string{"a"}})
	if d.ChangeFeedStats().PlanBytes <= before.PlanBytes {
		t.Fatal("projection plan uncharged")
	}
	small.Close()
	if d.ChangeFeedStats().PlanBytes != before.PlanBytes {
		t.Fatal("projection budget not released")
	}
	if c, err := d.Watch(context.Background(), WatchOptions{MaxMemoryBytes: 256}); !errors.Is(err, ErrLimit) {
		if c != nil {
			c.Close()
		}
		t.Fatal(err)
	}
	metadata.Close()
	if s := d.ChangeFeedStats(); s.ReaderBytes != 0 || s.PlanBytes != 0 || s.Watchers != 0 {
		t.Fatal(s)
	}
}
