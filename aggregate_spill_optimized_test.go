package db

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestGroupVisitorNullableOwnershipAndCancellation(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 6000; i++ {
		var b DocBuilder
		b.String("g", strings.Repeat("x", i%23)+fmt.Sprint(i%337))
		if i%3 != 0 {
			b.String("v", "")
		}
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%08d", i)), b.Bytes()))
	}
	p := `[{"$group":{"_id":{"g":"$g"},"nn":{"$countNonNull":"$v"}}}]`
	want, e := referenceAggregate(c, p)
	mustCompat(t, e)
	var got []map[string]any
	var st AggregateStats
	mustCompat(t, c.AggregateGroups(bg, []byte(p), AggregateOptions{MaxMemoryBytes: 64 << 10, AllowDiskUse: true, Stats: &st}, func(v GroupView) error {
		n, ok := v.Int64("nn")
		if !ok || v.Kind("nn") != "int" {
			t.Fatal(v.Any("nn"))
		}
		got = append(got, map[string]any{"_id": map[string]any{"g": v.ID(0)}, "nn": n})
		return nil
	}))
	canonicalRows(want)
	canonicalRows(got)
	if !reflect.DeepEqual(want, got) || st.SpillPartitions < 2 || st.SpillRuns >= st.SpillPartitions || st.PeakMemoryBytes > 64<<10 {
		t.Fatalf("parity=%v stats=%+v", reflect.DeepEqual(want, got), st)
	}
	for _, spill := range []bool{false, true} {
		ctx, cancel := context.WithCancel(bg)
		seen := 0
		err := c.AggregateGroups(ctx, []byte(p), AggregateOptions{MaxMemoryBytes: 8 << 20, AllowDiskUse: spill}, func(v GroupView) error { seen++; cancel(); return nil })
		cancel()
		if !errors.Is(err, context.Canceled) || seen != 1 {
			t.Fatal(err, seen)
		}
	}
}
func TestGroupVisitorRejectsOmittedOverflow(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 5000; i++ {
		var b DocBuilder
		b.Int("g", int64(i%1000))
		v := int64(1)
		if i%1000 == 999 {
			v = math.MaxInt64
		}
		b.Int("v", v)
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%08d", i)), b.Bytes()))
	}
	overflow := errors.New("checked integer overflow")
	err := c.AggregateGroups(bg, []byte(`[{"$group":{"_id":"$g","sum":{"$sum":"$v"}}}]`), AggregateOptions{MaxMemoryBytes: 64 << 10, AllowDiskUse: true}, func(v GroupView) error {
		if _, ok := v.Int64("sum"); !ok {
			_, _, exact := v.IntegerSum("sum")
			if !exact {
				t.Fatal("lost exact integer state")
			}
			return overflow
		}
		return nil
	})
	if err != overflow {
		t.Fatal(err)
	}
	if _, err = c.AggregateWithOptions(bg, []byte(`[{"$group":{"_id":"$g","sum":{"$sum":"$v"}}},{"$limit":1}]`), AggregateOptions{MaxMemoryBytes: 2 << 20}); err != nil {
		t.Fatal(err)
	}
}

// A visitor error comes back unwrapped, in memory and after a spill.
func TestGroupVisitorErrorIdentity(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 5000; i++ {
		var b DocBuilder
		b.Int("g", int64(i))
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%08d", i)), b.Bytes()))
	}
	p := `[{"$group":{"_id":"$g","n":{"$sum":1}}}]`
	pp, err := ParsePipeline([]byte(p))
	mustCompat(t, err)
	stop := errors.New("stop")
	for _, spill := range []bool{false, true} {
		opts := AggregateOptions{MaxMemoryBytes: 8 << 20}
		if spill {
			opts = AggregateOptions{MaxMemoryBytes: 64 << 10, AllowDiskUse: true}
		}
		visit := func(GroupView) error { return stop }
		if err := c.AggregateGroups(bg, []byte(p), opts, visit); err != stop {
			t.Fatalf("collection spill=%v: %v", spill, err)
		}
		if err := pp.RunGroups(bg, c.Source(nil, nil), opts, visit); err != stop {
			t.Fatalf("source spill=%v: %v", spill, err)
		}
	}
}
func TestSpillWideStringExtremaAndSkew(t *testing.T) {
	c := aggregateFixture(t, 0)
	for i := 0; i < 4000; i++ {
		var b DocBuilder
		g := i % 300
		if i > 2000 {
			g = 0
		}
		b.String("g", fmt.Sprintf("%03d", g)+strings.Repeat("k", i%3*1000))
		b.String("v", fmt.Sprintf("%08d", i)+strings.Repeat("z", i%5*400))
		mustCompat(t, c.Insert([]byte(fmt.Sprintf("%08d", i)), b.Bytes()))
	}
	p := `[{"$group":{"_id":"$g","first":{"$first":"$v"},"last":{"$last":"$v"},"lo":{"$min":"$v"},"hi":{"$max":"$v"},"n":{"$count":{}}}}]`
	want, e := referenceAggregate(c, p)
	mustCompat(t, e)
	got := eachRows(t, c, p, AggregateOptions{MaxMemoryBytes: 128 << 10, AllowDiskUse: true, MaxDiskBytes: 64 << 20})
	canonicalRows(want)
	canonicalRows(got)
	if !reflect.DeepEqual(want, got) {
		t.Fatal("variable state/skew mismatch")
	}
}
func TestSpillDescriptorBudget(t *testing.T) {
	if os.Getenv("APTEVADB_SPILL_FD_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestSpillDescriptorBudget$", "-test.timeout=90s")
		cmd.Env = append(os.Environ(), "APTEVADB_SPILL_FD_CHILD=1")
		b, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("descriptor child: %v\n%s", e, b)
		}
		return
	}
	var lim syscall.Rlimit
	mustCompat(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim))
	lim.Cur = min(lim.Max, 96)
	mustCompat(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim))
	var held []*os.File
	defer func() {
		for _, f := range held {
			f.Close()
		}
	}()
	for i := 0; i < 48; i++ {
		f, e := os.Open(os.DevNull)
		mustCompat(t, e)
		held = append(held, f)
	}
	ctx, cancel := context.WithTimeout(bg, 30*time.Second)
	defer cancel()
	root := t.TempDir()
	const workers = 3
	var ready sync.WaitGroup
	ready.Add(workers)
	gate := make(chan struct{})
	go func() { ready.Wait(); close(gate) }()
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func() {
			p, e := ParsePipeline([]byte(`[{"$group":{"_id":"$g","n":{"$count":{}},"sum":{"$sum":"$v"},"lo":{"$min":"$v"},"hi":{"$max":"$v"},"avg":{"$avg":"$v"}}}]`))
			if e != nil {
				errs <- e
				return
			}
			p.SetLimits(Limits{MaxGroups: 200000, MaxBytes: 12 << 20})
			src := func(y func([]byte, []byte) bool) error {
				var b DocBuilder
				announced := false
				defer func() {
					if !announced {
						ready.Done()
					}
				}()
				for i := 0; i < 100000; i++ {
					if i == 50000 {
						announced = true
						ready.Done()
						select {
						case <-gate:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					b.Reset()
					b.Int("g", int64(i))
					b.Int("v", int64(i))
					if !y(nil, b.Bytes()) {
						break
					}
				}
				return nil
			}
			count := 0
			var st AggregateStats
			e = p.RunGroups(ctx, src, AggregateOptions{MaxMemoryBytes: 12 << 20, AllowDiskUse: true, MaxDiskBytes: 1 << 30, TempDir: root, Stats: &st}, func(v GroupView) error {
				k, ok := v.Int64("_id")
				sum, sok := v.Int64("sum")
				if !ok || !sok || k != sum {
					return fmt.Errorf("bad concurrent group")
				}
				count++
				return nil
			})
			if e == nil && (count != 100000 || st.SpillPartitions < 32 || st.MaxPartitionDepth != 0 || st.PeakMemoryBytes > 12<<20) {
				e = fmt.Errorf("count=%d stats=%+v", count, st)
			}
			if e != nil {
				cancel()
			}
			errs <- e
		}()
	}
	for i := 0; i < workers; i++ {
		mustCompat(t, <-errs)
	}
	files, e := os.ReadDir(root)
	mustCompat(t, e)
	if len(files) != 0 {
		t.Fatal("scratch leak", files)
	}
}
