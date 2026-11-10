package db

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestComputedMetricsOnline(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	d, err := Open(path, DurableOptions())
	mustCompat(t, err)
	defer func() {
		if d != nil {
			d.Close()
		}
	}()
	calls, err := d.Collection("calls")
	mustCompat(t, err)
	sales, err := d.Collection("sales")
	mustCompat(t, err)
	put := func(c *Collection, key, day, centre, caller string) error {
		var b DocBuilder
		b.String("day", day)
		b.String("centre", centre)
		b.Bool("eligible", true)
		if caller != "" {
			b.String("caller", caller)
		}
		return c.Upsert([]byte(key), b.Bytes())
	}
	mustCompat(t, put(calls, "a", "Mon", "A", "X"))
	mustCompat(t, put(calls, "aa", "Mon", "A", "X"))
	mustCompat(t, put(calls, "b", "Tue", "A", "X"))
	mustCompat(t, put(calls, "c", "Tue", "B", "X"))
	mustCompat(t, put(sales, "sale", "Mon", "A", ""))
	inputs := []ComputedCollectionInput{
		{Source: "calls", Pipeline: []byte(`[{"$group":{"_id":{"day":"$day","centre":"$centre"},"calls":{"$count":{}},"unique_callers":{"$countDistinct":"$caller","where":{"$and":[{"eligible":true},{"caller":{"$ne":null}}]}}}}]`)},
		{Source: "sales", Pipeline: []byte(`[{"$group":{"_id":{"day":"$day","centre":"$centre"},"sales":{"$count":{}}}}]`)},
	}
	status, err := d.BeginComputedCollection(ctx, "metrics", inputs)
	mustCompat(t, err)
	if status.Ready {
		t.Fatal(status)
	}
	status, err = d.AdvanceComputedCollection(ctx, "metrics", 1)
	mustCompat(t, err)
	if status.Scanned["calls"] != 1 || status.Ready {
		t.Fatal(status)
	}
	old, err := d.Snapshot()
	mustCompat(t, err)
	if _, err = old.Collection("metrics"); !errors.Is(err, ErrNoSuchCollection) {
		t.Fatal(err)
	}
	old.Close()
	if _, err = d.OpenCollection("metrics"); !errors.Is(err, ErrComputedCollectionBuilding) {
		t.Fatal(err)
	}
	if err = d.DropCollection(ctx, "sales"); !errors.Is(err, ErrComputedCollectionDependency) {
		t.Fatal(err)
	}
	// A behind-cursor insert and a change ahead of the cursor both land
	// during the build. The prefix key aa must be visited after key a.
	mustCompat(t, put(calls, "A", "Tue", "B", "Y"))
	mustCompat(t, put(calls, "b", "Tue", "A", "Z"))
	mustCompat(t, put(calls, "b", "Tue", "A", "X"))
	var export bytes.Buffer
	mustCompat(t, d.Export(ctx, &export))
	restore := filepath.Join(t.TempDir(), "restored")
	mustCompat(t, Import(ctx, &export, restore))
	restored, err := Open(restore, DurableOptions())
	mustCompat(t, err)
	restoredView, err := restored.OpenCollection("metrics")
	mustCompat(t, err)
	count, err := restoredView.CountDocuments(ctx, nil, CountOptions{})
	mustCompat(t, err)
	if count != 3 {
		t.Fatal(count)
	}
	mustCompat(t, restored.Close())
	mustCompat(t, d.Close())
	d = nil
	d, err = Open(path, DurableOptions())
	mustCompat(t, err)
	calls, err = d.OpenCollection("calls")
	mustCompat(t, err)
	sales, err = d.OpenCollection("sales")
	mustCompat(t, err)
	status, err = d.BeginComputedCollection(ctx, "metrics", inputs)
	mustCompat(t, err)
	if status.Scanned["calls"] != 1 || status.Ready {
		t.Fatal(status)
	}
	snap, err := d.Snapshot()
	mustCompat(t, err)
	for !status.Ready {
		status, err = d.AdvanceComputedCollection(ctx, "metrics", 1)
		mustCompat(t, err)
	}
	if _, err = snap.Collection("metrics"); !errors.Is(err, ErrNoSuchCollection) {
		t.Fatal(err)
	}
	snap.Close()
	view, err := d.OpenCollection("metrics")
	mustCompat(t, err)
	info, err := view.ComputedCollection()
	mustCompat(t, err)
	if len(info.Inputs) != 2 {
		t.Fatal(info)
	}
	assertTotals := func(want [3]int64, wantByCentre map[string]int) {
		t.Helper()
		var got [3]int64
		byCentre := map[string]map[string]bool{}
		all := map[string]bool{}
		s, err := d.Snapshot()
		mustCompat(t, err)
		defer s.Close()
		c, err := s.Collection("metrics")
		mustCompat(t, err)
		var scanErr error
		mustCompat(t, c.Scan(nil, nil, func(key, doc []byte) bool {
			got[0] += docField(doc, "calls").i
			got[1] += docField(doc, "sales").i
			centre := string(docField(doc, "centre").s)
			if byCentre[centre] == nil {
				byCentre[centre] = map[string]bool{}
			}
			scanErr = c.ScanComputedDistinct(ctx, key, "unique_callers", func(id []byte) bool {
				all[string(id)], byCentre[centre][string(id)] = true, true
				return true
			})
			return scanErr == nil
		}))
		mustCompat(t, scanErr)
		got[2] = int64(len(all))
		counts := map[string]int{}
		for c, set := range byCentre {
			counts[c] = len(set)
		}
		if got != want || !reflect.DeepEqual(counts, wantByCentre) {
			t.Fatalf("got %v %v, want %v %v", got, counts, want, wantByCentre)
		}
	}
	assertTotals([3]int64{5, 1, 2}, map[string]int{"A": 1, "B": 2})
	mustCompat(t, calls.Delete([]byte("a")))
	key, err := ComputedGroupTupleKey("A", "Mon")
	mustCompat(t, err)
	if r := computedRow(t, view.Find, string(key)); r["unique_callers"] != int64(1) {
		t.Fatal(r)
	}
	mustCompat(t, calls.Delete([]byte("aa")))
	if r := computedRow(t, view.Find, string(key)); r["unique_callers"] != int64(0) {
		t.Fatal(r)
	}
	mustCompat(t, put(calls, "b", "Tue", "B", "X"))
	assertTotals([3]int64{3, 1, 2}, map[string]int{"A": 0, "B": 2})
	batch := calls.NewBatch()
	mustCompat(t, batch.Delete([]byte("b")))
	mustCompat(t, batch.With(sales).Insert([]byte("sale"), []byte("unused")))
	if err := batch.Commit(); !errors.Is(err, ErrKeyExists) {
		t.Fatal(err)
	}
	assertTotals([3]int64{3, 1, 2}, map[string]int{"A": 0, "B": 2})
	var wg sync.WaitGroup
	for i := byte(0); i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := put(calls, string([]byte{'q', i}), "Tue", "B", "W"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	assertTotals([3]int64{19, 1, 3}, map[string]int{"A": 0, "B": 3})
	mustCompat(t, d.DropCollection(ctx, "metrics"))
	mustCompat(t, d.DropCollection(ctx, "calls"))
	mustCompat(t, d.DropCollection(ctx, "sales"))
}
