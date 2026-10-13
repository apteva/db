package db

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestIntegerAggregationOverflow(t *testing.T) {
	cases := [][]int64{{math.MaxInt64, math.MaxInt64}, {math.MinInt64, math.MinInt64}, {math.MaxInt64, math.MaxInt64, -math.MaxInt64, -math.MaxInt64, 3}, {math.MinInt64, math.MinInt64, math.MaxInt64, math.MaxInt64, 5}, {9007199254740993, -9007199254740992}}
	for _, inputs := range cases {
		t.Run(fmt.Sprint(inputs), func(t *testing.T) {
			d, err := Open(t.TempDir(), Options{})
			mustCompat(t, err)
			defer d.Close()
			// Spread inputs across many pages so merges exercise wide partial sums.
			for i := 0; i < 1600; i++ {
				row := map[string]any{"pad": string(make([]byte, 100))}
				if i%300 == 0 && i/300 < len(inputs) {
					row["v"] = inputs[i/300]
				}
				mustCompat(t, d.def.InsertDocument([]byte(fmt.Sprintf("%06d", i)), row))
			}
			mustCompat(t, d.Checkpoint())
			total := new(big.Int)
			for _, x := range inputs {
				total.Add(total, big.NewInt(x))
			}
			var wantSum any
			if total.IsInt64() {
				wantSum = total.Int64()
			} else {
				wantSum, _ = new(big.Float).SetInt(total).Float64()
			}
			wantAvg, _ := new(big.Rat).SetFrac(total, big.NewInt(int64(len(inputs)))).Float64()
			for _, query := range []string{`[{"$group":{"_id":null,"sum":{"$sum":"$v"},"avg":{"$avg":"$v"}}}]`, `[{"$project":{"v":1}},{"$group":{"_id":null,"sum":{"$sum":"$v"},"avg":{"$avg":"$v"}}}]`} {
				for _, shards := range []int{0, 1, 2, 4, 10} {
					var rows []map[string]any
					if shards == 0 {
						rows, err = d.Aggregate(bg, []byte(query))
					} else {
						rows, err = d.AggregateParallel(bg, []byte(query), shards)
					}
					mustCompat(t, err)
					if len(rows) != 1 || !reflect.DeepEqual(rows[0]["sum"], wantSum) || rows[0]["avg"] != wantAvg {
						t.Fatalf("shards=%d got=%v want sum=%v avg=%v", shards, rows, wantSum, wantAvg)
					}
				}
			}
		})
	}
}

func TestWideAggregationModel(t *testing.T) {
	rng := rand.New(rand.NewSource(31))
	for trial := 0; trial < 300; trial++ {
		var pieces [7]accState
		total := new(big.Int)
		for i := 0; i < 200; i++ {
			x := int64(rng.Uint64())
			pieces[i%len(pieces)].addInteger(x)
			total.Add(total, big.NewInt(x))
		}
		var merged computedWide
		for _, piece := range pieces {
			merged.merge(piece.integerSum())
		}
		want, _ := new(big.Float).SetInt(total).Float64()
		if merged.fitsInt64() != total.IsInt64() || merged.float64() != want {
			t.Fatalf("wide result differs: %v want %v", merged.float64(), want)
		}
	}
	// Floating input must not expose a previously overflowed integer subtotal.
	st := accState{hasFloat: true, fsum: 0.5, count: 3}
	st.addInteger(math.MaxInt64)
	st.addInteger(math.MaxInt64)
	if got := accValue(&accSpec{op: accSum}, &st); got.k != kFloat || got.f < 1e19 {
		t.Fatal(got)
	}
	for i := 0; i < 10000; i++ {
		w := computedWide{lo: rng.Uint64(), hi: rng.Uint64()}
		if i < 100 {
			w.hi = uint64(1) << uint(i%64)
		}
		x := new(big.Int).Lsh(new(big.Int).SetUint64(w.hi), 64)
		x.Add(x, new(big.Int).SetUint64(w.lo))
		if int64(w.hi) < 0 {
			x.Sub(x, new(big.Int).Lsh(big.NewInt(1), 128))
		}
		want, _ := new(big.Float).SetPrec(128).SetInt(x).Float64()
		if w.float64() != want {
			t.Fatalf("rounded %x:%x got %.17g want %.17g", w.hi, w.lo, w.float64(), want)
		}
	}
}

func TestFormat8AggregationUpgrade(t *testing.T) {
	dir := t.TempDir()
	files, err := filepath.Glob("testdata/format8/*")
	mustCompat(t, err)
	for _, name := range files {
		if filepath.Base(name) != "data" && !strings.HasPrefix(filepath.Base(name), "wal-") {
			continue
		}
		b, err := os.ReadFile(name)
		mustCompat(t, err)
		mustCompat(t, os.WriteFile(filepath.Join(dir, filepath.Base(name)), b, 0600))
	}
	v, err := FormatVersionOf(dir)
	mustCompat(t, err)
	if v != 8 {
		t.Fatal(v)
	}
	d, err := Open(dir, Options{})
	mustCompat(t, err)
	defer d.Close()
	v, err = FormatVersionOf(dir)
	mustCompat(t, err)
	if v != FormatVersion {
		t.Fatal(v)
	}
	data, err := os.ReadFile(filepath.Join(dir, "data"))
	mustCompat(t, err)
	for _, offset := range []int{0, PageSize} {
		if le.Uint32(data[offset+8:]) != FormatVersion {
			t.Fatal("old-format metadata remains after upgrade")
		}
	}
	c, err := d.OpenCollection("source")
	mustCompat(t, err)
	rows, err := c.Aggregate(bg, []byte(`[{"$group":{"_id":null,"sum":{"$sum":"$v"},"avg":{"$avg":"$v"},"n":{"$countNonNull":"$v"}}}]`))
	mustCompat(t, err)
	if rows[0]["sum"] != float64(math.MaxInt64)*2 || rows[0]["avg"] != float64(math.MaxInt64) || rows[0]["n"] != int64(2) {
		t.Fatal(rows)
	}
	_, err = d.CreateComputedCollection(bg, "present", "source", []byte(`[{"$group":{"_id":null,"n":{"$countNonNull":"$v"}}}]`))
	mustCompat(t, err)
	mustCompat(t, d.Verify(bg))
}

func TestCountNonNull(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableBytes: -1})
	mustCompat(t, err)
	defer d.Close()
	c := mustCollection(t, d, "counts")
	rows := []map[string]any{{}, {"v": nil}, {"v": 0}, {"v": false}, {"v": ""}, {"v": []any{}}, {"v": map[string]any{}}, {"v": 2.5}, {"nested": map[string]any{"v": 7}}}
	for i, row := range rows {
		mustCompat(t, c.InsertDocument([]byte(fmt.Sprint(i)), row))
	}
	query := []byte(`[{"$group":{"_id":null,"rows":{"$count":{}},"present":{"$countNonNull":"$v"},"nested":{"$countNonNull":"$nested.v"}}}]`)
	want := []map[string]any{{"_id": nil, "rows": int64(9), "present": int64(6), "nested": int64(1)}}
	for _, prefix := range []string{"", `{"$project":{"v":1,"nested":1}},`} {
		q := append([]byte("["+prefix), query[1:]...)
		got, err := c.Aggregate(bg, q)
		mustCompat(t, err)
		if !reflect.DeepEqual(got, want) {
			t.Fatal(got)
		}
		got, err = c.AggregateParallel(bg, q, 4)
		mustCompat(t, err)
		if !reflect.DeepEqual(got, want) {
			t.Fatal(got)
		}
	}
	view, err := d.CreateComputedCollection(bg, "present", "counts", query)
	mustCompat(t, err)
	check := func(n int64) {
		var row map[string]any
		key, err := ComputedGroupKey(nil)
		mustCompat(t, err)
		ok, err := view.FindDocument(key, &row)
		mustCompat(t, err)
		if !ok || row["present"] != n {
			t.Fatal(row)
		}
	}
	check(6)
	mustCompat(t, c.UpsertDocument([]byte("0"), map[string]any{"v": "yes"}))
	check(7)
	mustCompat(t, c.UpsertDocument([]byte("2"), map[string]any{"v": nil}))
	check(6)
	mustCompat(t, c.Delete([]byte("3")))
	check(5)
	mustCompat(t, d.Checkpoint())
	mustCompat(t, d.Compact(bg))
	dir := d.dir
	mustCompat(t, d.Close())
	d, err = Open(dir, Options{})
	mustCompat(t, err)
	defer d.Close()
	view, err = d.OpenCollection("present")
	mustCompat(t, err)
	check(5)
	for _, operand := range []string{`null`, `{}`, `1`, `"v"`, `"$"`} {
		if _, err := ParsePipeline([]byte(`[{"$group":{"_id":null,"n":{"$countNonNull":` + operand + `}}}]`)); err == nil {
			t.Fatal("accepted invalid count operand", operand)
		}
	}
}
