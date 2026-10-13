package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestTopRowsMatchesStableFullSort(t *testing.T) {
	rng := rand.New(rand.NewSource(73))
	values := []any{nil, false, true, int64(-2), int64(0), int64(1), float64(1), 2.5, "", "a", "z", map[string]any{"x": int64(2)}, []any{int64(1), "x"}}
	rows := make([]map[string]any, 700)
	for i := range rows {
		row := map[string]any{"position": i, "b": rng.Intn(5), "nested": map[string]any{"x": rng.Intn(8)}}
		if i%9 != 0 {
			row["a"] = values[rng.Intn(len(values))]
		}
		rows[i] = row
	}
	for _, keys := range [][]sortKey{
		{{"a", false}}, {{"a", true}}, {{"a", false}, {"b", true}}, {{"b", true}, {"nested.x", false}}, {{"missing", true}},
	} {
		full := append([]map[string]any(nil), rows...)
		full, err := applyRows(&stage{kind: stSort, sortKeys: keys}, full, DefaultLimits, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, keep := range []int{0, 1, 10, 100, 700, 900} {
			for _, skip := range []int{0, 3, 650, 900} {
				got, ok, err := topRows(bg, rows, keys, keep, skip)
				if err != nil || !ok {
					t.Fatalf("selection failed: %v %v", ok, err)
				}
				want := full[:min(keep, len(full))]
				want = want[min(skip, len(want)):]
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("keys=%v keep=%d skip=%d: stable selection differs", keys, keep, skip)
				}
			}
		}
	}
}

func TestAggregateTopKMatchesUnfusedPipeline(t *testing.T) {
	database, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var b, sub DocBuilder
	var a ArrayBuilder
	var key [8]byte
	for i := 0; i < 2400; i++ {
		b.Reset()
		b.String("customer", fmt.Sprintf("c%04d", i%600))
		b.Int("id", int64(i))
		b.Int("amount", int64(i%7))
		b.String("region", fmt.Sprintf("r%d", i%9))
		if i%6 == 0 {
			b.Null("payload")
		} else if i%6 == 1 {
			b.Bool("payload", true)
		} else if i%6 == 2 {
			b.Float("payload", 2.5)
		} else if i%6 == 3 {
			b.String("payload", "x")
		} else if i%6 == 4 {
			sub.Reset()
			sub.Int("z", int64(i%11))
			sub.String("a", "nested")
			b.Doc("payload", sub.Bytes())
		} else {
			a.Reset()
			a.Int(int64(i % 13))
			a.String("array")
			b.Array("payload", a.Bytes())
		}
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err := database.Upsert(key[:], b.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	queries := []string{
		`[{"$group":{"_id":"$customer","n":{"$sum":1},"total":{"$sum":"$amount"}}},{"$sort":{"total":-1}},{"$limit":10}]`,
		`[{"$group":{"_id":"$customer","n":{"$count":{}}}},{"$sort":{"n":-1}},{"$skip":7},{"$limit":10}]`,
		`[{"$group":{"_id":"$customer","avg":{"$avg":"$amount"},"first":{"$first":"$id"},"last":{"$last":"$id"}}},{"$sort":{"avg":1,"_id":-1}},{"$limit":17}]`,
		`[{"$group":{"_id":{"r":"$region","c":"$customer"},"mx":{"$max":"$payload"},"mn":{"$min":"$payload"}}},{"$sort":{"_id.r":1,"_id.c":-1}},{"$limit":19}]`,
		`[{"$group":{"_id":{"r":"$region","c":"$customer"},"n":{"$sum":1}}},{"$sort":{"_id":1}},{"$limit":11}]`,
		`[{"$group":{"_id":"$payload","n":{"$sum":1}}},{"$sort":{"_id":1}},{"$limit":11}]`,
		`[{"$group":{"_id":"$customer","mx":{"$max":"$payload"}}},{"$sort":{"mx":1}},{"$limit":13}]`,
		`[{"$group":{"_id":"$customer","avg":{"$avg":"$nonexistent"}}},{"$sort":{"avg":-1}},{"$limit":12}]`,
		`[{"$group":{"_id":null,"n":{"$count":{}}}},{"$sort":{"n":-1}},{"$limit":1}]`,
		`[{"$group":{"_id":"$customer","n":{"$count":{}}}},{"$sort":{"n":-1}},{"$skip":1000},{"$limit":11}]`,
		`[{"$group":{"_id":"$customer","n":{"$count":{}}}},{"$sort":{"n":-1}},{"$limit":0}]`,
		`[{"$sort":{"amount":-1,"customer":1}},{"$skip":7},{"$limit":11},{"$project":{"id":1}}]`,
		`[{"$sort":{"payload":1}},{"$limit":37}]`,
		`[{"$group":{"_id":"$customer","n":{"$count":{}}}},{"$sort":{"n":-1}},{"$skip":9223372036854775807},{"$limit":11}]`,
	}
	for _, query := range queries {
		p, err := ParsePipeline([]byte(query))
		if err != nil {
			t.Fatal(err)
		}
		reference := *p
		reference.stages = nil
		for _, s := range p.stages {
			reference.stages = append(reference.stages, s)
			if s.kind == stSort {
				reference.stages = append(reference.stages, stage{kind: stMatch})
			}
		}
		want, err := reference.Run(bg, database.Source(nil, nil))
		if err != nil {
			t.Fatal(err)
		}
		for _, workers := range []int{1, 2, 7} {
			got, err := database.def.RunParallel(bg, p, workers)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("workers=%d query=%s\ngot=%v\nwant=%v", workers, query, got, want)
			}
		}
	}
	// A zero output limit must not skip grouping limits or cancellation.
	p, err := ParsePipeline([]byte(queries[10]))
	if err != nil {
		t.Fatal(err)
	}
	p.SetLimits(Limits{MaxGroups: 2, MaxRows: 100})
	if _, err := p.Run(bg, database.Source(nil, nil)); !errors.Is(err, ErrLimit) {
		t.Fatalf("group limit bypassed: %v", err)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if _, err := database.def.RunParallel(ctx, p, 7); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
}

func TestTopRowsNaNFallsBack(t *testing.T) {
	rows := []map[string]any{{"v": 1.0}, {"v": math.NaN()}, {"v": 2.0}}
	_, ok, err := topRows(bg, rows, []sortKey{{"v", true}}, 1, 0)
	if ok || err != nil {
		t.Fatalf("NaN must use original stable sort: %v %v", ok, err)
	}
}
