package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

const ccGroup = `[{"$match":{"paid":true}},{"$group":{"_id":"$customer","total":{"$sum":"$amount"},"n":{"$count":{}}}}]`

func ccCreate(t *testing.T, db *DB, name, source, pipeline string) *Collection {
	t.Helper()
	c, err := db.CreateComputedCollection(bg, name, source, []byte(pipeline))
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func ccGroupRow(t *testing.T, c *Collection, id any) map[string]any {
	t.Helper()
	k, err := ComputedGroupKey(id)
	if err != nil {
		t.Fatal(err)
	}
	return computedRow(t, c.Find, string(k))
}
func ccAbsent(t *testing.T, c *Collection, id any) {
	t.Helper()
	k, err := ComputedGroupKey(id)
	if err != nil {
		t.Fatal(err)
	}
	_, ok, err := c.Find(k, nil)
	if err != nil || ok {
		t.Fatalf("unexpected group %v: %v %v", id, ok, err)
	}
}
func ccPut(t *testing.T, c *Collection, key string, customer, amount int64, paid bool) {
	t.Helper()
	var b DocBuilder
	b.Int("customer", customer)
	b.Int("amount", amount)
	b.Bool("paid", paid)
	if err := c.Upsert([]byte(key), b.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func TestComputedCollectionsBackfillAndProjection(t *testing.T) {
	for _, mem := range []int{0, 256 << 10} {
		t.Run(fmt.Sprint(mem), func(t *testing.T) {
			db := computedDB(t, Options{MemtableBytes: int64(mem), CacheBytes: 32 * PageSize})
			src, _ := db.Collection("orders")
			other, _ := db.Collection("other")
			for i := 0; i < 2400; i++ {
				key := fmt.Sprintf("%05d", i)
				ccPut(t, src, key, int64(i%13), int64(i), i%3 != 0)
				if err := other.Upsert([]byte(key), []byte("opaque")); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 400; i++ {
				if i%2 == 0 {
					if err := src.Delete([]byte(fmt.Sprintf("%05d", i))); err != nil {
						t.Fatal(err)
					}
				} else {
					ccPut(t, src, fmt.Sprintf("%05d", i), 99, -1, true)
				}
			}
			for i := 2400; i < 2600; i++ {
				ccPut(t, src, fmt.Sprintf("%05d", i), 7, 3, true)
			}
			projected := ccCreate(t, db, "paid", "orders", `[{"$match":{"paid":true}},{"$project":{"customer":1,"amount":1}}]`)
			copied := ccCreate(t, db, "copy", "orders", `[]`)
			if err := src.Scan(nil, nil, func(k, v []byte) bool {
				want, _ := DecodeDocument(v)
				row := computedRow(t, copied.Find, string(k))
				if !reflect.DeepEqual(row, want) {
					t.Fatalf("copy %s", k)
				}
				got, ok, err := projected.Find(k, nil)
				if err != nil {
					t.Fatal(err)
				}
				if ok != want["paid"].(bool) {
					t.Fatalf("filter %s", k)
				}
				if ok {
					p, _ := DecodeDocument(got)
					if len(p) != 2 || p["customer"] != want["customer"] || p["amount"] != want["amount"] {
						t.Fatal(p, want)
					}
				}
				return true
			}); err != nil {
				t.Fatal(err)
			}
			ccPut(t, src, "00201", 22, 34, false)
			if _, ok, _ := projected.Find([]byte("00201"), nil); ok {
				t.Fatal("filter removal")
			}
			ccPut(t, src, "00201", 23, 35, true)
			if computedRow(t, projected.Find, "00201")["amount"] != int64(35) {
				t.Fatal("filter entry")
			}
			if err := projected.CreateIndex(bg, "amount", "amount"); err != nil {
				t.Fatal(err)
			}
			ccPut(t, src, "00201", 23, 36, true)
			if err := db.VerifyIndexes(bg); err != nil {
				t.Fatal(err)
			}
			if err := src.Delete([]byte("00201")); err != nil {
				t.Fatal(err)
			}
			if _, ok, _ := projected.Find([]byte("00201"), nil); ok {
				t.Fatal("delete propagation")
			}
			if err := db.Verify(bg); err != nil {
				t.Fatal(err)
			}
		})
	}
	db := computedDB(t, Options{})
	if err := db.UpsertJSON([]byte("a"), []byte(`{"_id":1,"a":2,"b":3}`)); err != nil {
		t.Fatal(err)
	}
	inc := ccCreate(t, db, "inc", "", `[{"$project":{"a":1}}]`)
	exc := ccCreate(t, db, "exc", "", `[{"$project":{"b":0}}]`)
	noid := ccCreate(t, db, "noid", "", `[{"$project":{"a":1,"_id":0}}]`)
	for _, c := range []*Collection{inc, exc} {
		if !reflect.DeepEqual(computedRow(t, c.Find, "a"), map[string]any{"_id": int64(1), "a": int64(2)}) {
			t.Fatal(c.name)
		}
	}
	if !reflect.DeepEqual(computedRow(t, noid.Find, "a"), map[string]any{"a": int64(2)}) {
		t.Fatal("_id exclusion")
	}
}

func TestComputedCollectionsBatchAtomicity(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 256 << 10})
	src, _ := db.Collection("orders")
	if err := src.DefineComputedFields(bg, []byte(`{"amount":{"$multiply":["$qty","$price"]}}`)); err != nil {
		t.Fatal(err)
	}
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	rows := ccCreate(t, db, "paid", "orders", `[{"$match":{"paid":true}}]`)
	if err := dst.CreateIndex(bg, "total", "total"); err != nil {
		t.Fatal(err)
	}
	put := func(b *Batch, k, doc string) {
		if err := b.UpsertJSON([]byte(k), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	b := db.NewBatch()
	b.Upsert([]byte("raw"), []byte("opaque"))
	s := b.With(src)
	put(s, "a", `{"customer":1,"qty":3,"price":7,"paid":true}`)
	put(s, "b", `{"customer":1,"qty":2,"price":8,"paid":true}`)
	put(s, "a", `{"customer":2,"qty":4,"price":9,"paid":true}`)
	s.Delete([]byte("b"))
	put(s, "b", `{"customer":2,"qty":-4,"price":9,"paid":true}`)
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	ccAbsent(t, dst, 1)
	r := ccGroupRow(t, dst, 2)
	if r["total"] != int64(0) || r["n"] != int64(2) {
		t.Fatal(r)
	}
	if computedRow(t, rows.Find, "a")["amount"] != int64(36) {
		t.Fatal("computed fields ran after collection maintenance")
	}
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	b = db.NewBatch()
	b.Upsert([]byte("absent"), []byte("raw"))
	s = b.With(src)
	s.Delete([]byte("a"))
	put(s, "c", `{"customer":9,"qty":3,"price":8,"paid":true}`)
	put(s, "bad", `{"customer":[],"qty":1,"price":1,"paid":true}`)
	if err := b.Commit(); !errors.Is(err, ErrComputedCollection) {
		t.Fatal(err)
	}
	if _, ok, _ := db.Find([]byte("absent"), nil); ok {
		t.Fatal("partial mixed batch")
	}
	if _, ok, _ := src.Find([]byte("a"), nil); !ok {
		t.Fatal("partial delete")
	}
	ccAbsent(t, dst, 9)
	if !reflect.DeepEqual(ccGroupRow(t, dst, 2), r) {
		t.Fatal("partial summary")
	}
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	s = src.NewBatch()
	s.Delete([]byte("a"))
	s.Delete([]byte("b"))
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	ccAbsent(t, dst, 2)
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}

func TestComputedCollectionsExactTotals(t *testing.T) {
	db := computedDB(t, Options{})
	src, _ := db.Collection("orders")
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	ccPut(t, src, "a", 1, math.MaxInt64, true)
	if err := src.UpsertJSON([]byte("b"), []byte(`{"customer":1,"amount":1,"paid":true}`)); !errors.Is(err, ErrComputedCollection) {
		t.Fatal("overflow", err)
	}
	if _, ok, _ := src.Find([]byte("b"), nil); ok {
		t.Fatal("overflow wrote source")
	}
	// An intermediate total exceeds int64, while the final batch total is valid.
	b := src.NewBatch()
	b.UpsertJSON([]byte("b"), []byte(`{"customer":1,"amount":1,"paid":true}`))
	b.Delete([]byte("a"))
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if ccGroupRow(t, dst, 1)["total"] != int64(1) {
		t.Fatal("wide intermediate")
	}
	ccPut(t, src, "min", 2, math.MinInt64, true)
	if err := src.Delete([]byte("min")); err != nil {
		t.Fatal(err)
	}
	ccAbsent(t, dst, 2)
	ccPut(t, src, "c", 9007199254740992, 5, true)
	ccPut(t, src, "d", 9007199254740993, 6, true)
	if ccGroupRow(t, dst, int64(9007199254740992))["total"] != int64(5) || ccGroupRow(t, dst, int64(9007199254740993))["total"] != int64(6) {
		t.Fatal("integer key rounded")
	}
	for i, doc := range []string{`{"paid":true}`, `{"customer":null,"paid":true,"amount":null}`, `{"customer":1.0,"amount":2,"paid":true}`, `{"customer":"1","amount":4,"paid":true}`, `{"customer":true,"amount":8,"paid":true}`} {
		if err := src.UpsertJSON([]byte(fmt.Sprint("scalar", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if ccGroupRow(t, dst, nil)["n"] != int64(2) || ccGroupRow(t, dst, 1)["total"] != int64(3) || ccGroupRow(t, dst, "1")["total"] != int64(4) || ccGroupRow(t, dst, true)["total"] != int64(8) {
		t.Fatal("typed groups")
	}
	if err := src.UpsertJSON([]byte("float"), []byte(`{"customer":1,"amount":1.5,"paid":true}`)); !errors.Is(err, ErrComputedCollection) {
		t.Fatal(err)
	}
	for _, v := range []any{struct{}{}, uint64(2), []any{}, json.Number("9223372036854775808"), math.Inf(1), math.NaN(), strings.Repeat("x", MaxKeySize)} {
		if _, err := ComputedGroupKey(v); !errors.Is(err, ErrComputedCollection) {
			t.Fatal(v, err)
		}
	}
	k, _ := ComputedGroupKey(int64(1))
	f, _ := ComputedGroupKey(1.0)
	if !bytes.Equal(k, f) {
		t.Fatal("integral float normalization")
	}
}

func TestComputedCollectionsDefinitionsAndReadOnly(t *testing.T) {
	db := computedDB(t, Options{})
	src, _ := db.Collection("orders")
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	for _, p := range []string{`{}`, `[{"$sort":{"a":1}}]`, `[{"$group":{"_id":"$a","x":{"$avg":"$b"}}}]`, `[{"$group":{"_id":{},"x":{"$sum":1}}}]`, `[{"$group":{"_id":null,"x":{"$count":1}}}]`, `[{"$group":{"_id":null,"x":{"$sum":1.2}}}]`, `[{"$project":{"a":2}}]`, `[{"$project":{"a.b":1}}]`, `[{"$project":{"a":1}},{"$match":{"a":1}}]`, `[{"$match":{"a":[1]}}]`, `[{"$match":{"a":9223372036854775808}}]`, `[{"$match":{"a":1,"a":2}}]`, `[{"$group":{"_id":null,"_id":"$a"}}]`} {
		if _, err := db.CreateComputedCollection(bg, "bad", "orders", []byte(p)); !errors.Is(err, ErrComputedCollection) {
			t.Fatal(p, err)
		}
		if _, ok := db.lookupCollection("bad"); ok {
			t.Fatal("failed catalog published")
		}
	}
	if _, err := db.CreateComputedCollection(bg, "totals", "orders", []byte(`[]`)); !errors.Is(err, ErrComputedCollectionExists) {
		t.Fatal(err)
	}
	if _, err := db.CreateComputedCollection(bg, "chain", "totals", []byte(`[]`)); !errors.Is(err, ErrComputedCollection) {
		t.Fatal(err)
	}
	if _, err := db.CreateComputedCollection(bg, "unknown", "missing", []byte(`[]`)); !errors.Is(err, ErrNoSuchCollection) {
		t.Fatal(err)
	}
	for _, err := range []error{dst.UpsertJSON([]byte("x"), []byte(`{}`)), dst.Delete([]byte("x")), dst.NewBatch().UpsertJSON([]byte("x"), []byte(`{}`)), dst.DefineComputedFields(bg, []byte(`{}`))} {
		if !errors.Is(err, ErrComputedCollectionReadOnly) {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if _, err := db.CreateComputedCollection(ctx, "cancel", "orders", []byte(`[]`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := db.DropCollection(bg, "orders"); !errors.Is(err, ErrComputedCollectionDependency) {
		t.Fatal(err)
	}
	if err := src.Upsert([]byte("bad"), []byte("opaque")); !errors.Is(err, ErrComputedCollection) {
		t.Fatal(err)
	}
	other, _ := db.Collection("other")
	if err := other.Upsert([]byte("bad"), []byte("opaque")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateComputedCollection(bg, "invalid", "other", []byte(`[]`)); !errors.Is(err, ErrComputedCollection) {
		t.Fatal(err)
	}
	if _, ok := db.lookupCollection("invalid"); ok {
		t.Fatal("bad backfill published")
	}
	info, err := dst.ComputedCollection()
	if err != nil || info.Source != "orders" {
		t.Fatal(info, err)
	}
	info.Pipeline[0] = 'x'
	if _, err := dst.ComputedCollection(); err != nil {
		t.Fatal(err)
	}
	if info, err := src.ComputedCollection(); err != nil || info != nil {
		t.Fatal(info, err)
	}
}

func TestComputedCollectionsSnapshotDropAndRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	src, _ := db.Collection("orders")
	ccPut(t, src, "a", 1, 5, true)
	before, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	if _, err := before.Collection("totals"); err != ErrNoSuchCollection {
		t.Fatalf("future collection visible: %v", err)
	}
	before.Close()
	if err := dst.CreateIndex(bg, "total", "total"); err != nil {
		t.Fatal(err)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sd, _ := snap.Collection("totals")
	ss, _ := snap.Collection("orders")
	ccPut(t, src, "a", 2, 7, true)
	if ccGroupRow(t, sd, 1)["total"] != int64(5) || computedRow(t, ss.Find, "a")["customer"] != int64(1) {
		t.Fatal("snapshot was not atomic")
	}
	if err := db.DropCollection(bg, "totals"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection(bg, "orders"); err != nil {
		t.Fatal(err)
	}
	if info, err := sd.ComputedCollection(); err != nil || info.Source != "orders" {
		t.Fatal(info, err)
	}
	if ccGroupRow(t, sd, 1)["total"] != int64(5) {
		t.Fatal("held snapshot lost output")
	}
	snap.Close()
	src, _ = db.Collection("orders")
	ccPut(t, src, "new", 1, 11, true)
	dst = ccCreate(t, db, "totals", "orders", ccGroup)
	if ccGroupRow(t, dst, 1)["total"] != int64(11) {
		t.Fatal("stale counters")
	}
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	if err := dst.CreateIndex(bg, "total", "total"); err != nil {
		t.Fatal(err)
	}
	backup := t.TempDir()
	if err := db.Backup(bg, backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(bg); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, backup} {
		db, err = Open(path, Options{})
		if err != nil {
			t.Fatal(err)
		}
		src, _ = db.Collection("orders")
		dst, _ = db.Collection("totals")
		if info, err := dst.ComputedCollection(); err != nil || info.Source != "orders" {
			t.Fatal(info, err)
		}
		ccPut(t, src, "next", 1, -11, true)
		r := ccGroupRow(t, dst, 1)
		if r["total"] != int64(0) || r["n"] != int64(2) {
			t.Fatal(r)
		}
		src.Delete([]byte("new"))
		src.Delete([]byte("next"))
		ccAbsent(t, dst, 1)
		if err := db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}

func TestComputedCollectionsRandomizedModel(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 256 << 10, CacheBytes: 16 * PageSize})
	src, _ := db.Collection("orders")
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	rows := ccCreate(t, db, "paid", "orders", `[{"$match":{"paid":true}},{"$project":{"amount":1}}]`)
	type order struct {
		customer, amount int64
		paid             bool
	}
	model := map[int]order{}
	rng := rand.New(rand.NewSource(47523))
	for round := 0; round < 100; round++ {
		b := src.NewBatch()
		for j := 0; j < 100; j++ {
			id := rng.Intn(400)
			key := []byte(fmt.Sprint(id))
			if rng.Intn(4) == 0 {
				if err := b.Delete(key); err != nil {
					t.Fatal(err)
				}
				delete(model, id)
			} else {
				o := order{int64(rng.Intn(20)), int64(rng.Intn(1000) - 500), rng.Intn(3) != 0}
				doc := fmt.Sprintf(`{"customer":%d,"amount":%d,"paid":%t}`, o.customer, o.amount, o.paid)
				if err := b.UpsertJSON(key, []byte(doc)); err != nil {
					t.Fatal(err)
				}
				model[id] = o
			}
		}
		if err := b.Commit(); err != nil {
			t.Fatal(err)
		}
		totals := map[int64][2]int64{}
		for id, o := range model {
			v, ok, err := rows.Find([]byte(fmt.Sprint(id)), nil)
			if err != nil || ok != o.paid {
				t.Fatal(id, ok, err)
			}
			if ok {
				r, _ := DecodeDocument(v)
				if len(r) != 1 || r["amount"] != o.amount {
					t.Fatal(r, o)
				}
				x := totals[o.customer]
				x[0] += o.amount
				x[1]++
				totals[o.customer] = x
			}
		}
		seen := 0
		if err := dst.Scan(nil, nil, func(k, v []byte) bool {
			r, _ := DecodeDocument(v)
			id := r["_id"].(int64)
			want, ok := totals[id]
			if !ok || r["total"] != want[0] || r["n"] != want[1] {
				t.Fatalf("round %d: %v vs %v", round, r, want)
			}
			seen++
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if seen != len(totals) {
			t.Fatal(seen, len(totals))
		}
		if round%10 == 0 {
			if err := db.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
}

func TestComputedCollectionsConcurrent(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 256 << 10})
	src, _ := db.Collection("orders")
	ccPut(t, src, "a", 1, 0, true)
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for writer := 0; writer < 4; writer++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 120; i++ {
				key := []byte(fmt.Sprint(w))
				doc := []byte(fmt.Sprintf(`{"customer":1,"amount":%d,"paid":true}`, i))
				if err := src.UpsertJSON(key, doc); err != nil {
					errs <- err
					return
				}
			}
		}(writer)
	}
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 80; i++ {
				s, err := db.Snapshot()
				if err != nil {
					errs <- err
					return
				}
				sc, _ := s.Collection("orders")
				dc, _ := s.Collection("totals")
				var total, count int64
				err = sc.Scan(nil, nil, func(k, v []byte) bool { total += docField(v, "amount").i; count++; return true })
				key, _ := ComputedGroupKey(1)
				v, ok, e := dc.Find(key, nil)
				if err == nil {
					err = e
				}
				if err == nil && (!ok || docField(v, "total").i != total || docField(v, "n").i != count) {
					err = fmt.Errorf("inconsistent snapshot: %d %d", total, count)
				}
				s.Close()
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	r := ccGroupRow(t, dst, 1)
	if r["total"] != int64(4*119) || r["n"] != int64(5) {
		t.Fatal(r)
	}
}

func TestComputedCollectionsCrashRecovery(t *testing.T) {
	if dir := os.Getenv("APTEVADB_CC_CRASH_DIR"); dir != "" {
		db, err := Open(dir, Options{Sync: true, SyncMode: SyncOS})
		if err != nil {
			t.Fatal(err)
		}
		src, _ := db.Collection("orders")
		ccPut(t, src, "a", 1, 5, true)
		ccCreate(t, db, "totals", "orders", ccGroup)
		if os.Getenv("APTEVADB_CC_CRASH_CREATE") != "1" {
			b := src.NewBatch()
			b.Delete([]byte("a"))
			b.UpsertJSON([]byte("b"), []byte(`{"customer":2,"amount":17,"paid":true}`))
			if err := b.Commit(); err != nil {
				t.Fatal(err)
			}
		}
		os.Exit(0)
	}
	for _, mode := range []string{"create", "write", "torn-write", "torn-create"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestComputedCollectionsCrashRecovery$")
			cmd.Env = append(os.Environ(), "APTEVADB_CC_CRASH_DIR="+dir)
			if strings.Contains(mode, "create") {
				cmd.Env = append(cmd.Env, "APTEVADB_CC_CRASH_CREATE=1")
			}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatal(err, string(out))
			}
			if strings.HasPrefix(mode, "torn") {
				paths, _ := filepath.Glob(filepath.Join(dir, "wal-*"))
				f, err := os.OpenFile(paths[len(paths)-1], os.O_RDWR, 0)
				if err != nil {
					t.Fatal(err)
				}
				st, _ := f.Stat()
				if err := f.Truncate(st.Size() - 1); err != nil {
					t.Fatal(err)
				}
				f.Close()
			}
			db, err := Open(dir, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			src, _ := db.Collection("orders")
			dst, exists := db.lookupCollection("totals")
			if mode == "torn-create" {
				if exists {
					t.Fatal("partial creation recovered")
				}
				ccPut(t, src, "next", 1, 3, true)
				return
			}
			if !exists {
				t.Fatal("catalog lost")
			}
			if mode == "write" {
				ccAbsent(t, dst, 1)
				if ccGroupRow(t, dst, 2)["total"] != int64(17) {
					t.Fatal("recovered output")
				}
			} else {
				if ccGroupRow(t, dst, 1)["total"] != int64(5) {
					t.Fatal("torn source/summary")
				}
				ccAbsent(t, dst, 2)
			}
			ccPut(t, src, "next", 3, 11, true)
			if ccGroupRow(t, dst, 3)["total"] != int64(11) {
				t.Fatal("definition not loaded")
			}
		})
	}
}

func TestComputedCollectionsLegacy4Upgrade(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	f, err := os.OpenFile(filepath.Join(dir, "data"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, PageSize)
	for slot := int64(0); slot < 2; slot++ {
		if _, err := f.ReadAt(page, slot*PageSize); err != nil {
			t.Fatal(err)
		}
		le.PutUint32(page[8:], 4)
		le.PutUint32(page[52:], crc32.Checksum(page[:52], crcTable))
		if _, err := f.WriteAt(page, slot*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	f, _ = os.Open(filepath.Join(dir, "data"))
	defer f.Close()
	for slot := int64(0); slot < 2; slot++ {
		f.ReadAt(page, slot*PageSize)
		if le.Uint32(page[8:]) != FormatVersion {
			t.Fatal("older writer fallback", slot)
		}
	}
}

func TestComputedCollectionsIOFailure(t *testing.T) {
	for _, op := range []string{"create", "write", "drop"} {
		for trip := 1; trip <= 6; trip++ {
			t.Run(fmt.Sprintf("%s/%d", op, trip), func(t *testing.T) {
				lab := newCrashLab(t, int64(7800+trip))
				db := lab.open(Options{Sync: true, SyncMode: SyncOS})
				src, _ := db.Collection("orders")
				ccPut(t, src, "a", 1, 5, true)
				if op != "create" {
					ccCreate(t, db, "totals", "orders", ccGroup)
				}
				lab.arm(trip)
				var err error
				switch op {
				case "create":
					_, err = db.CreateComputedCollection(bg, "totals", "orders", []byte(ccGroup))
				case "write":
					err = src.UpsertJSON([]byte("b"), []byte(`{"customer":1,"amount":7,"paid":true}`))
				case "drop":
					err = db.DropCollection(bg, "totals")
				}
				if !expectedFailure(err) {
					t.Fatal(err)
				}
				ack := err == nil
				lab.crash(db)
				db = lab.open(Options{Sync: true, SyncMode: SyncOS})
				defer db.Close()
				dst, exists := db.lookupCollection("totals")
				if ack && ((op == "drop") == exists) {
					t.Fatal("acknowledged catalog state lost")
				}
				src, _ = db.Collection("orders")
				if exists {
					var total, count int64
					src.Scan(nil, nil, func(k, v []byte) bool { total += docField(v, "amount").i; count++; return true })
					r := ccGroupRow(t, dst, 1)
					if r["total"] != total || r["n"] != count {
						t.Fatal("partial transaction after I/O fault", r, total, count)
					}
					ccPut(t, src, "next", 1, 3, true)
					if ccGroupRow(t, dst, 1)["total"] != total+3 {
						t.Fatal("maintenance missing")
					}
				}
			})
		}
	}
}

func TestComputedCollectionsCreateWriteRaceAndLimits(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 256 << 10})
	src, _ := db.Collection("orders")
	ccPut(t, src, "a", 1, 0, true)
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		for i := 0; i < 1000; i++ {
			if err := src.UpsertJSON([]byte("a"), []byte(fmt.Sprintf(`{"customer":1,"amount":%d,"paid":true}`, i))); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	<-started
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ccGroupRow(t, dst, 1)["total"] != int64(999) {
		t.Fatal("creation raced past source write")
	}
	for i := 1; i < 64; i++ {
		ccCreate(t, db, fmt.Sprint("dep", i), "orders", `[{"$group":{"_id":null,"n":{"$sum":1}}}]`)
	}
	if _, err := db.CreateComputedCollection(bg, "too-many", "orders", []byte(`[]`)); !errors.Is(err, ErrComputedCollection) {
		t.Fatal(err)
	}
	if err := db.DropCollection(bg, "dep1"); err != nil {
		t.Fatal(err)
	}
	ccCreate(t, db, "replacement", "orders", `[]`)
	ccPut(t, src, "a", 1, 1000, true)
	if ccGroupRow(t, dst, 1)["total"] != int64(1000) {
		t.Fatal("detached wrong dependent")
	}
}

func TestComputedCollectionsBackfillStringOwnership(t *testing.T) {
	db := computedDB(t, Options{CacheBytes: 16 * PageSize, MemtableBytes: 256 << 10})
	src, _ := db.Collection("orders")
	b := src.NewBatch()
	for i := 0; i < 3000; i++ {
		doc := fmt.Sprintf(`{"customer":"%s%04d","amount":%d,"paid":true}`, strings.Repeat("x", 128), i%57, i)
		if err := b.UpsertJSON([]byte(fmt.Sprintf("%04d", i)), []byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	dst := ccCreate(t, db, "totals", "orders", ccGroup)
	for group := 0; group < 57; group++ {
		var total, n int64
		for i := group; i < 3000; i += 57 {
			total += int64(i)
			n++
		}
		id := fmt.Sprintf("%s%04d", strings.Repeat("x", 128), group)
		r := ccGroupRow(t, dst, id)
		if r["_id"] != id || r["total"] != total || r["n"] != n {
			t.Fatal("backfill borrowed a reused page buffer", r, id, total, n)
		}
	}
}

func TestComputedCollectionsMaximumSourceKeys(t *testing.T) {
	db := computedDB(t, Options{})
	src, _ := db.Collection("orders")
	key := []byte(strings.Repeat("k", MaxKeySize))
	if err := src.UpsertJSON(key, []byte(`{"customer":1,"amount":5,"paid":true}`)); err != nil {
		t.Fatal(err)
	}
	copy := ccCreate(t, db, "copy", "orders", `[]`)
	totals := ccCreate(t, db, "totals", "orders", ccGroup)
	// Exercise three-byte collection prefixes as well as the one-byte form.
	for db.colls.nextID < 254 {
		if _, err := db.Collection(fmt.Sprint("unused", db.colls.nextID)); err != nil {
			t.Fatal(err)
		}
	}
	escaped := ccCreate(t, db, "escaped", "orders", `[]`)
	b := src.NewBatch()
	if err := b.UpsertJSON(key, []byte(`{"customer":2,"amount":7,"paid":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*Collection{copy, escaped} {
		if computedRow(t, c.Find, string(key))["amount"] != int64(7) {
			t.Fatal("maximum key lost", c.name)
		}
	}
	ccAbsent(t, totals, 1)
	if ccGroupRow(t, totals, 2)["total"] != int64(7) {
		t.Fatal("group update with maximum source key")
	}
	for _, name := range []string{"copy", "escaped", "totals", "orders"} {
		if err := db.DropCollection(bg, name); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Verify(bg); err != nil {
		t.Fatal(err)
	}
}
