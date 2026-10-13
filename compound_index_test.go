package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func compatDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(t.TempDir(), DurableOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}
func mustCompat(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestExactIndexNumericOrderAgainstRationalModel(t *testing.T) {
	values := []value{{k: kInt, i: math.MinInt64}, {k: kInt, i: math.MaxInt64}, {k: kInt, i: 1 << 53}, {k: kInt, i: 1<<53 + 1}, {k: kInt, i: -(1 << 53) - 1}, {k: kFloat, f: math.SmallestNonzeroFloat64}, {k: kFloat, f: -math.SmallestNonzeroFloat64}, {k: kFloat, f: 0x1p63}, {k: kFloat, f: -0x1p63}, {k: kFloat, f: math.Copysign(0, -1)}, {k: kInt, i: 0}, {k: kFloat, f: .5}, {k: kInt, i: 1}, {k: kFloat, f: 1}}
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 200; i++ {
		values = append(values, value{k: kInt, i: int64(rng.Uint64())}, value{k: kFloat, f: math.Float64frombits(rng.Uint64())})
	}
	rational := func(v value) *big.Rat {
		if v.k == kInt {
			return new(big.Rat).SetInt64(v.i)
		}
		return new(big.Rat).SetFloat64(v.f)
	}
	for _, a := range values {
		if a.k == kFloat && (math.IsNaN(a.f) || math.IsInf(a.f, 0)) {
			continue
		}
		for _, b := range values {
			if b.k == kFloat && (math.IsNaN(b.f) || math.IsInf(b.f, 0)) {
				continue
			}
			want := rational(a).Cmp(rational(b))
			got := compareValues(a, b)
			encoded := bytes.Compare(encodeExactIndexValue(nil, a), encodeExactIndexValue(nil, b))
			if got != want || (encoded < 0) != (want < 0) || (encoded > 0) != (want > 0) {
				t.Fatalf("%v/%v: comparison=%d encoding=%d want=%d", a.any(), b.any(), got, encoded, want)
			}
		}
	}
}

func TestCompoundCursorOrderingBoundsSnapshots(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("rows")
	mustCompat(t, err)
	fields := []IndexField{{Field: "tenant"}, {Field: "n", Desc: true}}
	mustCompat(t, c.CreateCompoundIndex(bg, "tenant_n", fields))
	keys := [][]byte{[]byte("a"), []byte("a\x00"), []byte("a\x00\x00"), []byte("b"), []byte("c")}
	ns := []int64{1<<53 + 1, 1<<53 + 1, 1<<53 + 1, 1 << 53, math.MaxInt64}
	for i, key := range keys {
		mustCompat(t, c.UpsertDocument(key, map[string]any{"tenant": "a", "n": ns[i]}))
	}
	mustCompat(t, c.UpsertDocument([]byte("other"), map[string]any{"tenant": "b", "n": int64(1)}))
	want := []string{"c", "a", "a\x00", "a\x00\x00", "b"}
	cursor, err := c.NewIndexCursor(bg, "tenant_n", IndexCursorOptions{Prefix: []any{"a"}})
	mustCompat(t, err)
	defer cursor.Close()
	read := func(first func() bool, next func() bool) []string {
		var out []string
		for ok := first(); ok; ok = next() {
			out = append(out, string(cursor.Key()))
		}
		mustCompat(t, cursor.Err())
		return out
	}
	if got := read(cursor.First, cursor.Next); !reflect.DeepEqual(got, want) {
		t.Fatalf("forward=%q want=%q", got, want)
	}
	reverse := append([]string(nil), want...)
	for i, j := 0, len(reverse)-1; i < j; i, j = i+1, j-1 {
		reverse[i], reverse[j] = reverse[j], reverse[i]
	}
	if got := read(cursor.Last, cursor.Prev); !reflect.DeepEqual(got, reverse) {
		t.Fatalf("reverse=%q", got)
	}
	if !cursor.Seek([]any{"a", json.Number("9007199254740993")}) || string(cursor.Key()) != "a" {
		t.Fatal("tuple seek failed")
	}
	position := append([]byte(nil), cursor.Position()...)
	if !cursor.SeekAfter(position) || string(cursor.Key()) != "a\x00" {
		t.Fatal("position seek failed")
	}
	mustCompat(t, c.Delete([]byte("a")))
	mustCompat(t, c.DropIndex(bg, "tenant_n"))
	if got := read(cursor.First, cursor.Next); !reflect.DeepEqual(got, want) {
		t.Fatal("cursor lost its snapshot")
	}
	mustCompat(t, cursor.Close())
	mustCompat(t, c.CreateCompoundIndex(bg, "tenant_n", fields))
	bounded, err := c.NewIndexCursor(bg, "tenant_n", IndexCursorOptions{Lower: []any{"a", int64(1<<53 + 1)}, Upper: []any{"a", int64(1 << 53)}, UpperInclusive: true})
	mustCompat(t, err)
	var boundedKeys []string
	for ok := bounded.First(); ok; ok = bounded.Next() {
		boundedKeys = append(boundedKeys, string(bounded.Key()))
	}
	mustCompat(t, bounded.Err())
	bounded.Close()
	if !reflect.DeepEqual(boundedKeys, []string{"a\x00", "a\x00\x00", "b"}) {
		t.Fatalf("bounds=%q", boundedKeys)
	}
	mustCompat(t, d.VerifyIndexes(bg))
	mustCompat(t, d.Verify(bg))
	ctx, cancel := context.WithCancel(bg)
	cancelled, err := c.NewIndexCursor(ctx, "tenant_n", IndexCursorOptions{})
	mustCompat(t, err)
	cancel()
	if cancelled.First() || !errors.Is(cancelled.Err(), context.Canceled) {
		t.Fatal("cursor ignored cancellation")
	}
	cancelled.Close()
}

func TestCompoundUniqueNullsSparseAndAtomicSwaps(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("users")
	mustCompat(t, err)
	mustCompat(t, c.CreateCompoundIndex(bg, "email", []IndexField{{Field: "tenant"}, {Field: "email"}}, IndexOptions{Unique: true, NullsDistinct: true}))
	for _, key := range []string{"null1", "null2", "missing"} {
		doc := map[string]any{"tenant": "a"}
		if key != "missing" {
			doc["email"] = nil
		}
		mustCompat(t, c.InsertDocument([]byte(key), doc))
	}
	mustCompat(t, c.InsertDocument([]byte("one"), map[string]any{"tenant": "a", "email": "x"}))
	mustCompat(t, c.InsertDocument([]byte("two"), map[string]any{"tenant": "a", "email": "y"}))
	if err := c.InsertDocument([]byte("dup"), map[string]any{"tenant": "a", "email": "x"}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatal(err)
	}
	mustCompat(t, c.InsertDocument([]byte("different"), map[string]any{"tenant": "b", "email": "x"}))
	b := c.NewBatch()
	mustCompat(t, b.UpsertJSON([]byte("one"), []byte(`{"tenant":"a","email":"y"}`)))
	mustCompat(t, b.UpsertJSON([]byte("two"), []byte(`{"tenant":"a","email":"x"}`)))
	mustCompat(t, b.Commit())
	if err := c.UpsertDocument([]byte("one"), map[string]any{"tenant": "a", "email": []any{"z"}}); !errors.Is(err, ErrIndexValue) {
		t.Fatal(err)
	}
	var got map[string]any
	_, err = c.FindDocument([]byte("one"), &got)
	mustCompat(t, err)
	if got["email"] != "y" {
		t.Fatal("rejected write changed data")
	}
	mustCompat(t, d.VerifyIndexes(bg))
	sparse, err := d.Collection("sparse")
	mustCompat(t, err)
	mustCompat(t, sparse.CreateCompoundIndex(bg, "n", []IndexField{{Field: "n"}}, IndexOptions{Unique: true, Sparse: true}))
	mustCompat(t, sparse.InsertJSON([]byte("a"), []byte(`{}`)))
	mustCompat(t, sparse.InsertJSON([]byte("b"), []byte(`{}`)))
	mustCompat(t, sparse.InsertJSON([]byte("c"), []byte(`{"n":null}`)))
	if err := sparse.InsertJSON([]byte("d"), []byte(`{"n":null}`)); !errors.Is(err, ErrDuplicateKey) {
		t.Fatal(err)
	}
	if _, err = sparse.ExplainQuery(bg, nil, QueryOptions{Sort: []IndexField{{Field: "n"}}, Hint: "n"}); !errors.Is(err, ErrPipeline) {
		t.Fatal("incomplete sparse plan accepted", err)
	}
	mustCompat(t, d.VerifyIndexes(bg))
}

func TestLogicalPrefixQueriesPlansAndMemory(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("rows")
	mustCompat(t, err)
	for i := 0; i < 100; i++ {
		tenant := "a"
		if i%2 == 0 {
			tenant = "b"
		}
		mustCompat(t, c.UpsertDocument([]byte(fmt.Sprintf("%03d", i)), map[string]any{"tenant": tenant, "n": int64(i), "s": "a.[%" + fmt.Sprint(i)}))
	}
	mustCompat(t, c.CreateCompoundIndex(bg, "tenant_n", []IndexField{{Field: "tenant"}, {Field: "n", Desc: true}}))
	filter := []byte(`{"$and":[{"tenant":"a"},{"$or":[{"n":{"$gte":90}},{"n":1}]},{"s":{"$prefix":"a.[%"}}]}`)
	opts := QueryOptions{Sort: []IndexField{{Field: "n", Desc: true}}, Limit: 3, MaxMemoryBytes: 256, Stats: &QueryStats{}}
	plan, err := c.ExplainQuery(bg, filter, opts)
	mustCompat(t, err)
	if plan.Access != "index-order" || plan.SortInMemory {
		t.Fatalf("plan=%+v", plan)
	}
	var got []int64
	mustCompat(t, c.Query(bg, filter, opts, func(_, raw []byte) bool {
		m, e := DecodeDocument(raw)
		mustCompat(t, e)
		got = append(got, m["n"].(int64))
		return true
	}))
	if !reflect.DeepEqual(got, []int64{99, 97, 95}) || opts.Stats.Returned != 3 || opts.Stats.PeakMemoryBytes != 0 {
		t.Fatalf("query=%v stats=%+v", got, opts.Stats)
	}
	for _, parallel := range []bool{false, true} {
		p, err := ParsePipeline(append(append([]byte(`[{"$match":`), filter...), []byte(`},{"$count":"n"}]`)...))
		mustCompat(t, err)
		var rows []map[string]any
		if parallel {
			rows, err = c.RunParallel(bg, p, 4)
		} else {
			rows, err = c.Run(bg, p)
		}
		mustCompat(t, err)
		if len(rows) != 1 || rows[0]["n"] != int64(6) {
			t.Fatal(rows)
		}
	}
	mustCompat(t, c.DropIndex(bg, "tenant_n"))
	if err := c.Query(bg, nil, QueryOptions{Sort: []IndexField{{Field: "n", Desc: true}}, MaxMemoryBytes: 128}, func(_, _ []byte) bool { return true }); !errors.Is(err, ErrLimit) {
		t.Fatal("unbounded sort ignored budget", err)
	}
	var page []int64
	stats := QueryStats{}
	mustCompat(t, c.Query(bg, nil, QueryOptions{Sort: []IndexField{{Field: "n", Desc: true}}, Limit: 2, MaxMemoryBytes: 4096, Stats: &stats}, func(_, raw []byte) bool {
		m, _ := DecodeDocument(raw)
		page = append(page, m["n"].(int64))
		return true
	}))
	if !reflect.DeepEqual(page, []int64{99, 98}) || stats.PeakMemoryBytes > 4096 {
		t.Fatal(page, stats)
	}
	p, err := ParsePipeline([]byte(`[{"$group":{"_id":"$n","n":{"$count":{}}}}]`))
	mustCompat(t, err)
	p.SetLimits(Limits{MaxBytes: 1024})
	if _, err = c.RunParallel(bg, p, 8); !errors.Is(err, ErrLimit) {
		t.Fatal("parallel aggregation ignored shared budget", err)
	}
	for _, invalid := range []string{`{"$or":[]}`, `{"$and":[1]}`, `{"s":{"$prefix":1}}`, `{"n":9223372036854775808}`, `{} {}`} {
		if err := c.Query(bg, []byte(invalid), QueryOptions{}, func(_, _ []byte) bool { return true }); !errors.Is(err, ErrPipeline) {
			t.Fatal(invalid, err)
		}
	}
}

func TestExtendedDocumentsComputedAndPersistence(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("wide")
	mustCompat(t, err)
	mustCompat(t, c.DefineComputedFields(bg, []byte(`{"copy":{"$upper":"$s"}}`)))
	projected, err := d.CreateComputedCollection(bg, "projected", "wide", []byte(`[{"$project":{"s":1,"copy":1}}]`))
	mustCompat(t, err)
	wide := map[string]any{"s": strings.Repeat("a", 100000), "nested": map[string]any{"s": strings.Repeat("x", 80000)}, "array": []any{strings.Repeat("y", 90000)}}
	for i := 0; i < 300; i++ {
		wide[fmt.Sprintf("f%03d", i)] = int64(i)
	}
	mustCompat(t, c.InsertDocument([]byte(strings.Repeat("k", 2048)), wide))
	raw, found, err := projected.Find([]byte(strings.Repeat("k", 2048)), nil)
	mustCompat(t, err)
	if !found {
		t.Fatal("derived row missing")
	}
	decoded, err := DecodeDocument(raw)
	mustCompat(t, err)
	if decoded["copy"] != strings.Repeat("A", 100000) {
		t.Fatal("extended computed field lost data")
	}
	many := make([]any, 70000)
	for i := range many {
		many[i] = int64(i)
	}
	encoded, err := Encode(map[string]any{"a": many})
	mustCompat(t, err)
	m, err := DecodeDocument(encoded)
	mustCompat(t, err)
	if len(m["a"].([]any)) != len(many) {
		t.Fatal("extended array count")
	}
	if _, err = Encode(map[string]any{"s": strings.Repeat("x", MaxDocumentSize)}); !errors.Is(err, ErrDocument) {
		t.Fatal("oversized document accepted", err)
	}
	mustCompat(t, d.VerifyIndexes(bg))
	mustCompat(t, d.Verify(bg))
	var export bytes.Buffer
	mustCompat(t, d.Export(bg, &export))
	destination := t.TempDir() + "/import"
	mustCompat(t, Import(bg, &export, destination))
	copy, err := Open(destination, DurableOptions())
	mustCompat(t, err)
	defer copy.Close()
	mustCompat(t, copy.VerifyIndexes(bg))
}

func TestCompoundSortAgainstIndependentModel(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("rows")
	mustCompat(t, err)
	rng := rand.New(rand.NewSource(7))
	type row struct {
		key  string
		a, b int64
	}
	rows := make([]row, 500)
	for i := range rows {
		rows[i] = row{fmt.Sprintf("%03d", i), rng.Int63n(7), rng.Int63n(13)}
		mustCompat(t, c.InsertDocument([]byte(rows[i].key), map[string]any{"a": rows[i].a, "b": rows[i].b}))
	}
	for _, ad := range []bool{false, true} {
		for _, bd := range []bool{false, true} {
			fields := []IndexField{{Field: "a", Desc: ad}, {Field: "b", Desc: bd}}
			name := fmt.Sprint(ad, bd)
			mustCompat(t, c.CreateCompoundIndex(bg, name, fields))
			model := append([]row(nil), rows...)
			sort.Slice(model, func(i, j int) bool {
				a, b := model[i], model[j]
				if a.a != b.a {
					if ad {
						return a.a > b.a
					}
					return a.a < b.a
				}
				if a.b != b.b {
					if bd {
						return a.b > b.b
					}
					return a.b < b.b
				}
				return a.key < b.key
			})
			var got []string
			mustCompat(t, c.Query(bg, nil, QueryOptions{Sort: fields, Skip: 13, Limit: 21, Hint: name, MaxMemoryBytes: 128}, func(key, _ []byte) bool { got = append(got, string(key)); return true }))
			want := []string{}
			for _, r := range model[13:34] {
				want = append(want, r.key)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatal(name, got, want)
			}
		}
	}
	mustCompat(t, d.VerifyIndexes(bg))
}

func TestCompoundRangesBothDirectionsAndPrefixes(t *testing.T) {
	for _, desc := range []bool{false, true} {
		t.Run(fmt.Sprint(desc), func(t *testing.T) {
			d := compatDB(t)
			c, err := d.Collection("rows")
			mustCompat(t, err)
			source := []any{nil, false, true, int64(-9007199254740993), int64(-1), int64(0), .5, int64(9007199254740992), int64(9007199254740993), "a", "a\x00", "a\x00b", "a.%_[", "a.%_[z", "b"}
			for i, v := range source {
				mustCompat(t, c.InsertDocument([]byte(fmt.Sprintf("%03d", i)), map[string]any{"v": v}))
			}
			mustCompat(t, c.InsertJSON([]byte("missing"), []byte(`{}`)))
			mustCompat(t, c.CreateCompoundIndex(bg, "v", []IndexField{{Field: "v", Desc: desc}}))
			filters := []string{`{"v":null}`, `{"v":9007199254740993}`, `{"v":{"$gt":9007199254740992}}`, `{"v":{"$gte":9007199254740992,"$lte":9007199254740993}}`, `{"v":{"$lt":9007199254740993}}`, `{"v":{"$lte":9007199254740993}}`, `{"v":{"$prefix":"a"}}`, `{"v":{"$prefix":"a\u0000"}}`, `{"v":{"$prefix":"a.%_["}}`, `{"v":{"$prefix":""}}`}
			for _, filter := range filters {
				opts := QueryOptions{Sort: []IndexField{{Field: "v", Desc: desc}}, Hint: "v"}
				var got, want []string
				mustCompat(t, c.Query(bg, []byte(filter), opts, func(k, _ []byte) bool { got = append(got, string(k)); return true }))
				opts.Hint = "$natural"
				mustCompat(t, c.Query(bg, []byte(filter), opts, func(k, _ []byte) bool { want = append(want, string(k)); return true }))
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: index=%q scan=%q", filter, got, want)
				}
			}
			mustCompat(t, d.VerifyIndexes(bg))
		})
	}
}

func TestLargeKeysSplitRecoveryAndIndexLimitAtomicity(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("rows")
	mustCompat(t, err)
	for i := 0; i < 64; i++ {
		key := []byte(fmt.Sprintf("%04d", i) + strings.Repeat("x", MaxKeySize-4))
		mustCompat(t, c.Insert(key, bytes.Repeat([]byte{'v'}, 6000)))
	}
	mustCompat(t, d.Checkpoint())
	mustCompat(t, d.Verify(bg))
	mustCompat(t, d.Compact(bg))
	count := 0
	mustCompat(t, c.Scan(nil, nil, func(k, v []byte) bool {
		if len(k) != MaxKeySize || len(v) != 6000 {
			t.Fatal("large key/value damaged")
		}
		count++
		return true
	}))
	if count != 64 {
		t.Fatal(count)
	}
	if err := c.Insert(bytes.Repeat([]byte{'x'}, MaxKeySize+1), nil); !errors.Is(err, ErrKeySize) {
		t.Fatal(err)
	}
	indexed, err := d.Collection("indexed")
	mustCompat(t, err)
	mustCompat(t, indexed.CreateCompoundIndex(bg, "v", []IndexField{{Field: "v"}}))
	if err := indexed.InsertDocument([]byte("long"), map[string]any{"v": strings.Repeat("x", MaxKeySize)}); !errors.Is(err, ErrKeySize) {
		t.Fatal(err)
	}
	if _, found, err := indexed.Find([]byte("long"), nil); err != nil || found {
		t.Fatal("oversized index write was partial", found, err)
	}
	mustCompat(t, d.VerifyIndexes(bg))
}

func TestCompoundExportImportAndReopen(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("rows")
	mustCompat(t, err)
	fields := []IndexField{{Field: "tenant"}, {Field: "v", Desc: true}}
	mustCompat(t, c.CreateCompoundIndex(bg, "tenant_v", fields, IndexOptions{Unique: true, NullsDistinct: true}))
	for _, doc := range []struct {
		key string
		v   any
	}{{"a", int64(9007199254740992)}, {"b", int64(9007199254740993)}, {"c", nil}, {"d", nil}} {
		mustCompat(t, c.InsertDocument([]byte(doc.key), map[string]any{"tenant": "a", "v": doc.v}))
	}
	var data bytes.Buffer
	mustCompat(t, d.Export(bg, &data))
	target := t.TempDir() + "/restored"
	mustCompat(t, Import(bg, &data, target))
	restored, err := Open(target, DurableOptions())
	mustCompat(t, err)
	check := func() {
		cc, err := restored.OpenCollection("rows")
		mustCompat(t, err)
		indexes := cc.ListIndexes()
		if len(indexes) != 1 || !indexes[0].NullsDistinct || !reflect.DeepEqual(indexes[0].Fields, fields) {
			t.Fatal(indexes)
		}
		var keys []string
		mustCompat(t, cc.Query(bg, []byte(`{"tenant":"a"}`), QueryOptions{Sort: []IndexField{{Field: "v", Desc: true}}}, func(k, _ []byte) bool { keys = append(keys, string(k)); return true }))
		if !reflect.DeepEqual(keys, []string{"b", "a", "c", "d"}) {
			t.Fatal(keys)
		}
		mustCompat(t, restored.VerifyIndexes(bg))
	}
	check()
	mustCompat(t, restored.Close())
	restored, err = Open(target, DurableOptions())
	mustCompat(t, err)
	defer restored.Close()
	check()
}
