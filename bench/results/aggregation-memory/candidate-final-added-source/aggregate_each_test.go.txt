package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

func aggregateFixture(t *testing.T, n int) *Collection {
	t.Helper()
	d, err := Open(t.TempDir(), Options{CacheBytes: 8 << 20})
	mustCompat(t, err)
	t.Cleanup(func() { d.Close() })
	c, err := d.Collection("rows")
	mustCompat(t, err)
	for i := 0; i < n; i++ {
		var b DocBuilder
		b.Int("g", int64(i%1000))
		b.Int("i", int64(i))
		v := int64(math.MaxInt64)
		if i%3 == 2 {
			v = -v
		}
		b.Int("v", v)
		if i%3 != 0 {
			b.Int("nullable", 0)
		}
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%08d", i)), b.Bytes()))
	}
	return c
}
func eachRows(t *testing.T, c *Collection, p string, o AggregateOptions) []map[string]any {
	t.Helper()
	var rows []map[string]any
	mustCompat(t, c.AggregateEach(bg, []byte(p), o, func(r map[string]any) bool { rows = append(rows, r); return true }))
	return rows
}
func canonicalRows(rows []map[string]any) {
	sort.SliceStable(rows, func(i, j int) bool { return fmt.Sprintf("%#v", rows[i]) < fmt.Sprintf("%#v", rows[j]) })
}
func TestAggregateEachOrderedIndex(t *testing.T) {
	c := aggregateFixture(t, 2000)
	mustCompat(t, c.CreateCompoundIndex(bg, "gi", []IndexField{{Field: "g"}, {Field: "i", Desc: true}}))
	p := `[{"$sort":{"g":1,"i":-1}},{"$group":{"_id":"$g","sum":{"$sum":"$v"},"avg":{"$avg":"$v"},"n":{"$sum":1},"nn":{"$countNonNull":"$nullable"},"first":{"$first":"$i"},"last":{"$last":"$i"}}},{"$limit":2}]`
	want, err := referenceAggregate(c, p)
	mustCompat(t, err)
	var st AggregateStats
	got := eachRows(t, c, p, AggregateOptions{MaxMemoryBytes: 4096, Stats: &st})
	if !reflect.DeepEqual(want, got) || st.Strategy != "ordered-index" || st.Index != "gi" || st.InputRows >= 2000 || st.PeakMemoryBytes > 4096 {
		t.Fatalf("got=%v want=%v stats=%+v", got, want, st)
	}
}
func TestAggregateEachIndexNumericAndNullTies(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 8; i++ {
		var b DocBuilder
		if i < 4 {
			if i%2 == 0 {
				b.Int("g", 1)
			} else {
				b.Float("g", 1)
			}
		} else if i%2 == 0 {
			b.Null("g")
		}
		b.Int("i", int64(i))
		mustCompat(t, c.Insert([]byte(fmt.Sprint(i)), b.Bytes()))
	}
	mustCompat(t, c.CreateCompoundIndex(bg, "g", []IndexField{{Field: "g"}}))
	p := `[{"$sort":{"g":1}},{"$group":{"_id":"$g","n":{"$sum":1},"first":{"$first":"$i"},"last":{"$last":"$i"}}}]`
	want, err := referenceAggregate(c, p)
	mustCompat(t, err)
	got := eachRows(t, c, p, AggregateOptions{MaxMemoryBytes: 8192})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}
func TestAggregateEachSpillExactState(t *testing.T) {
	c := aggregateFixture(t, 6000)
	p := `[{"$group":{"_id":"$g","sum":{"$sum":"$v"},"avg":{"$avg":"$v"},"n":{"$sum":1},"nn":{"$countNonNull":"$nullable"},"min":{"$min":"$i"},"max":{"$max":"$i"},"first":{"$first":"$i"},"last":{"$last":"$i"}}},{"$sort":{"_id":1}}]`
	want, err := referenceAggregate(c, p)
	mustCompat(t, err)
	dir := t.TempDir()
	var st AggregateStats
	got := eachRows(t, c, p, AggregateOptions{AllowDiskUse: true, TempDir: dir, MaxMemoryBytes: 128 << 10, MaxDiskBytes: 32 << 20, Stats: &st})
	if !reflect.DeepEqual(got, want) || st.SpillRuns == 0 || st.MergePasses == 0 || st.PeakMemoryBytes > 128<<10 || st.PeakDiskBytes > 32<<20 {
		t.Fatalf("equality=%v stats=%+v", reflect.DeepEqual(got, want), st)
	}
	names, err := os.ReadDir(dir)
	mustCompat(t, err)
	if len(names) != 0 {
		t.Fatal("temporary files leaked", names)
	}
}
func TestAggregateEachSpillCleanup(t *testing.T) {
	c := aggregateFixture(t, 3000)
	p := []byte(`[{"$group":{"_id":"$g","first":{"$first":"$i"}}}]`)
	for _, mode := range []string{"quota", "cancel", "stop", "memory", "io"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			o := AggregateOptions{AllowDiskUse: true, TempDir: dir, MaxMemoryBytes: 64 << 10}
			ctx, cancel := context.WithCancel(bg)
			defer cancel()
			var want error
			switch mode {
			case "quota":
				o.MaxDiskBytes = 1024
				want = ErrDiskLimit
			case "memory":
				o.AllowDiskUse = false
				want = ErrLimit
			case "io":
				f := filepath.Join(dir, "file")
				mustCompat(t, os.WriteFile(f, nil, 0600))
				o.TempDir = f
			}
			seen := 0
			err := c.AggregateEach(ctx, p, o, func(map[string]any) bool {
				seen++
				if mode == "cancel" {
					cancel()
				}
				return mode != "stop"
			})
			switch mode {
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case "stop":
				if err != nil || seen != 1 {
					t.Fatal(err, seen)
				}
			case "io":
				if err == nil {
					t.Fatal("expected I/O error")
				}
			default:
				if !errors.Is(err, want) {
					t.Fatal(err, want)
				}
			}
			names, e := os.ReadDir(dir)
			mustCompat(t, e)
			if mode == "io" {
				if len(names) != 1 {
					t.Fatal(names)
				}
			} else if len(names) != 0 {
				t.Fatal("temporary files leaked", names)
			}
			if c.db.Stats().Snapshots != 0 {
				t.Fatal("snapshot leaked")
			}
		})
	}
}
func TestAggregateEachChainedBlockingOperators(t *testing.T) {
	c := aggregateFixture(t, 3000)
	p := `[{"$sort":{"i":-1}},{"$group":{"_id":"$g","n":{"$sum":1},"first":{"$first":"$i"}}},{"$project":{"n":1}},{"$group":{"_id":"$n","groups":{"$sum":1}}},{"$sort":{"groups":-1}}]`
	want, err := referenceAggregate(c, p)
	mustCompat(t, err)
	got := eachRows(t, c, p, AggregateOptions{AllowDiskUse: true, TempDir: t.TempDir(), MaxMemoryBytes: 128 << 10, MaxDiskBytes: 32 << 20})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got=%v want=%v", got, want)
	}
}
func TestAggregateEachSnapshotAndReentrantCallback(t *testing.T) {
	c := aggregateFixture(t, 100)
	mustCompat(t, c.CreateCompoundIndex(bg, "g", []IndexField{{Field: "g"}}))
	seen := 0
	mustCompat(t, c.AggregateEach(bg, []byte(`[{"$group":{"_id":"$g","n":{"$sum":1}}}]`), AggregateOptions{}, func(r map[string]any) bool {
		seen++
		if seen == 1 {
			mustCompat(t, c.UpsertJSON([]byte("00000099"), []byte(`{"g":0}`)))
		}
		if r["n"] != int64(1) {
			t.Fatal(r)
		}
		return true
	}))
	if seen != 100 {
		t.Fatal(seen)
	}
}

func referenceAggregate(c *Collection, pipeline string) ([]map[string]any, error) {
	p, err := ParsePipeline([]byte(pipeline))
	if err != nil {
		return nil, err
	}
	return p.Run(bg, c.Source(nil, nil))
}

func TestAggregateSpillWideCancellationAndCollectorBudget(t *testing.T) {
	c := aggregateFixture(t, 0)
	values := []int64{math.MaxInt64, math.MaxInt64, -math.MaxInt64, -math.MaxInt64, 3}
	for round, v := range values {
		for g := 0; g < 600; g++ {
			var b DocBuilder
			b.Int("g", int64(g))
			b.Int("v", v)
			b.Int("i", int64(round))
			mustCompat(t, c.Insert([]byte(fmt.Sprintf("%02d-%05d", round, g)), b.Bytes()))
		}
	}
	p := `[{"$group":{"_id":"$g","sum":{"$sum":"$v"},"avg":{"$avg":"$v"},"first":{"$first":"$i"},"last":{"$last":"$i"}}}]`
	seen := 0
	var st AggregateStats
	mustCompat(t, c.AggregateEach(bg, []byte(p), AggregateOptions{AllowDiskUse: true, MaxMemoryBytes: 32 << 10, Stats: &st}, func(r map[string]any) bool {
		seen++
		if r["sum"] != int64(3) || r["avg"] != 0.6 || r["first"] != int64(0) || r["last"] != int64(4) {
			t.Fatal(r)
		}
		return true
	}))
	if seen != 600 || st.SpillRuns == 0 || st.PeakMemoryBytes > 32<<10 {
		t.Fatal(seen, st)
	}
	if _, err := c.AggregateWithOptions(bg, []byte(p), AggregateOptions{AllowDiskUse: true, MaxMemoryBytes: 32 << 10}); !errors.Is(err, ErrLimit) {
		t.Fatal("collector must remain bounded", err)
	}
	rows, err := c.AggregateWithOptions(bg, []byte(p), AggregateOptions{MaxMemoryBytes: 2 << 20})
	mustCompat(t, err)
	if len(rows) != 600 {
		t.Fatal(len(rows))
	}
}
func TestAggregationTempLeaseCleanup(t *testing.T) {
	root := t.TempDir()
	d := &aggregateDisk{ctx: bg, options: AggregateOptions{AllowDiskUse: true, TempDir: root}, stats: &AggregateStats{}, budget: &memoryBudget{max: 1 << 20}, stateLimit: 1 << 18}
	_, f, err := d.create()
	mustCompat(t, err)
	mustCompat(t, f.Close())
	removed, err := CleanupAggregationTemp(bg, root)
	mustCompat(t, err)
	if removed != 0 {
		t.Fatal("removed live execution")
	}
	// Closing only the lease models process death. The next cleanup must remove
	// that directory without touching unrelated files in the custom temp root.
	mustCompat(t, d.lease.Close())
	d.lease = nil
	unrelated := filepath.Join(root, "keep")
	mustCompat(t, os.WriteFile(unrelated, []byte("keep"), 0600))
	removed, err = CleanupAggregationTemp(bg, root)
	mustCompat(t, err)
	if removed != 1 {
		t.Fatal(removed)
	}
	data, err := os.ReadFile(unrelated)
	mustCompat(t, err)
	if string(data) != "keep" {
		t.Fatal(data)
	}
}
func TestAggregateSpillRecordCorruption(t *testing.T) {
	d := &aggregateDisk{ctx: bg, options: AggregateOptions{AllowDiskUse: true, TempDir: t.TempDir()}, stats: &AggregateStats{}, budget: &memoryBudget{max: 1 << 20}, stateLimit: 1 << 18}
	defer d.close()
	run, f, err := d.create()
	mustCompat(t, err)
	mustCompat(t, d.writeRecord(f, run, []byte("record")))
	mustCompat(t, f.Close())
	mustCompat(t, os.WriteFile(run.path, []byte{255, 255, 255, 255, 0, 0, 0, 0}, 0600))
	r, err := d.reader(run)
	mustCompat(t, err)
	defer r.close()
	if err = r.next(true); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestAggregateEachCompoundEqualityAndMixedOrder(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 200; i++ {
		var b DocBuilder
		b.Int("tenant", int64(i%2))
		b.Int("a", int64(i%5))
		b.Int("b", int64(i%7))
		b.Int("i", int64(i))
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%06d", i)), b.Bytes()))
	}
	mustCompat(t, c.CreateCompoundIndex(bg, "mixed", []IndexField{{Field: "tenant"}, {Field: "a", Desc: true}, {Field: "b"}, {Field: "i"}}))
	p := `[{"$match":{"tenant":1}},{"$sort":{"a":-1,"b":1,"i":1}},{"$group":{"_id":{"a":"$b","b":"$a"},"first":{"$first":"$i"},"last":{"$last":"$i"}}}]`
	want, err := referenceAggregate(c, p)
	mustCompat(t, err)
	var st AggregateStats
	got := eachRows(t, c, p, AggregateOptions{MaxMemoryBytes: 8192, Stats: &st})
	if !reflect.DeepEqual(got, want) || st.Strategy != "ordered-index" || st.Index != "mixed" {
		t.Fatalf("same=%v stats=%+v", reflect.DeepEqual(got, want), st)
	}
	p = `[{"$match":{"tenant":1}},{"$group":{"_id":{"a":"$b","b":"$a"},"n":{"$sum":1}}}]`
	want, err = referenceAggregate(c, p)
	mustCompat(t, err)
	got = eachRows(t, c, p, AggregateOptions{MaxMemoryBytes: 8192, Stats: &st})
	canonicalRows(want)
	canonicalRows(got)
	if !reflect.DeepEqual(got, want) || st.Strategy != "ordered-index" {
		t.Fatal(got, want, st)
	}
}

func TestAggregateEachOrderedTypedTieSpill(t *testing.T) {
	c := aggregateFixture(t, 0)
	fields := make([]IndexField, 8)
	for j := range fields {
		fields[j].Field = fmt.Sprintf("f%d", j)
	}
	for i := 0; i < 256; i++ {
		var b DocBuilder
		for j, f := range fields {
			if i&(1<<j) != 0 {
				b.Null(f.Field)
			}
		}
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%04d", i)), b.Bytes()))
	}
	mustCompat(t, c.CreateCompoundIndex(bg, "null_tuple", fields))
	p := `[{"$sort":{"f0":1,"f1":1,"f2":1,"f3":1,"f4":1,"f5":1,"f6":1,"f7":1}},{"$group":{"_id":{"a":"$f0","b":"$f1","c":"$f2","d":"$f3","e":"$f4","f":"$f5","g":"$f6","h":"$f7"},"n":{"$sum":1}}}]`
	var st AggregateStats
	seen := 0
	mustCompat(t, c.AggregateEach(bg, []byte(p), AggregateOptions{AllowDiskUse: true, MaxMemoryBytes: 32 << 10, Stats: &st}, func(row map[string]any) bool {
		seen++
		if row["n"] != int64(1) {
			t.Fatal(row)
		}
		return true
	}))
	if seen != 256 || st.Strategy != "ordered-index+spill" || st.SpillRuns == 0 || st.PeakMemoryBytes > 32<<10 {
		t.Fatal(seen, st)
	}
}
func TestPipelineSourceBoundedExecution(t *testing.T) {
	c := aggregateFixture(t, 2500)
	p, err := ParsePipeline([]byte(`[{"$group":{"_id":"$g","n":{"$sum":1}}},{"$sort":{"_id":1}},{"$limit":3}]`))
	mustCompat(t, err)
	var rows []map[string]any
	mustCompat(t, p.RunEach(bg, c.Source(nil, nil), AggregateOptions{AllowDiskUse: true, TempDir: t.TempDir(), MaxMemoryBytes: 64 << 10}, func(r map[string]any) bool { rows = append(rows, r); return true }))
	if len(rows) != 3 || rows[0]["_id"] != int64(0) {
		t.Fatal(rows)
	}
}

func TestAggregateSpillConcurrentSnapshotWrites(t *testing.T) {
	c := aggregateFixture(t, 6000)
	mustCompat(t, c.db.Checkpoint())
	s, err := c.db.Snapshot()
	mustCompat(t, err)
	defer s.Close()
	sc, err := s.Collection("rows")
	mustCompat(t, err)
	p := `[{"$group":{"_id":"$g","sum":{"$sum":"$v"},"n":{"$sum":1}}},{"$sort":{"_id":1}}]`
	want, err := referenceAggregate(sc, p)
	mustCompat(t, err)
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 5000; i++ {
			select {
			case <-ctx.Done():
				done <- nil
				return
			default:
			}
			var b DocBuilder
			b.Int("g", 0)
			b.Int("v", -1)
			if err := c.Upsert([]byte(fmt.Sprintf("%08d", i%6000)), b.Bytes()); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for i := 0; i < 3; i++ {
		var got []map[string]any
		mustCompat(t, sc.AggregateEach(bg, []byte(p), AggregateOptions{AllowDiskUse: true, MaxMemoryBytes: 128 << 10}, func(r map[string]any) bool { got = append(got, r); return true }))
		if !reflect.DeepEqual(got, want) {
			t.Fatal("snapshot aggregation changed under writes")
		}
	}
	cancel()
	mustCompat(t, <-done)
	s.Close()
	mustCompat(t, c.db.Checkpoint())
	mustCompat(t, c.db.Verify(bg))
}
