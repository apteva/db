package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func BenchmarkAggregationNumerics(b *testing.B) {
	const n = 100_000
	d, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	var docs [2][]byte
	for i, nullable := range []string{"null", "0"} {
		docs[i], err = EncodeJSON(nil, []byte(`{"i":1,"f":1.25,"huge":9223372036854775807,"nullable":`+nullable+`}`))
		if err != nil {
			b.Fatal(err)
		}
	}
	batch := d.NewBatch()
	var key [8]byte
	for i := 0; i < n; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		batch.Upsert(key[:], docs[i%2])
		if (i+1)%1000 == 0 {
			if err := batch.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := d.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct {
		name, accumulators string
		want               map[string]any
	}{
		{"Integers", `"sum":{"$sum":"$i"},"avg":{"$avg":"$i"}`, map[string]any{"_id": nil, "sum": int64(n), "avg": float64(1)}},
		{"Floats", `"sum":{"$sum":"$f"},"avg":{"$avg":"$f"}`, map[string]any{"_id": nil, "sum": float64(n) * 1.25, "avg": 1.25}},
		{"WideIntegers", `"sum":{"$sum":"$huge"},"avg":{"$avg":"$huge"}`, map[string]any{"_id": nil, "sum": float64(math.MaxInt64) * n, "avg": float64(math.MaxInt64)}},
		{"RowCount", `"n":{"$count":{}}`, map[string]any{"_id": nil, "n": int64(n)}},
		{"NonNullCount", `"n":{"$countNonNull":"$nullable"}`, map[string]any{"_id": nil, "n": int64(n / 2)}},
	} {
		for _, shards := range []int{1, 10} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, shards), func(b *testing.B) {
				query := []byte(`[{"$group":{"_id":null,` + tc.accumulators + `}}]`)
				b.ReportAllocs()
				for b.Loop() {
					rows, err := d.AggregateParallel(context.Background(), query, shards)
					if err != nil || len(rows) != 1 || !reflect.DeepEqual(rows[0], tc.want) {
						b.Fatal(rows, err)
					}
				}
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/row")
			})
		}
	}
}
