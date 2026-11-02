package db

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

// Aggregates narrow a leading $match through exact compound and descending
// indexes as queries do, and return what a full scan returns.
func TestAggregateUsesCompoundIndexes(t *testing.T) {
	d := compatDB(t)
	plain, err := d.Collection("plain")
	mustCompat(t, err)
	indexed, err := d.Collection("indexed")
	mustCompat(t, err)
	b := d.NewBatch()
	for i := 0; i < 40000; i++ {
		doc := map[string]any{"tenant": fmt.Sprintf("t%d", i%7), "status": fmt.Sprintf("s%d", i%3), "amount": i % 97}
		switch i % 50 {
		case 0: // no day
		case 1:
			doc["day"] = nil
		case 2:
			doc["day"] = i % 30 // a number sorts apart from strings
		default:
			doc["day"] = fmt.Sprintf("2026-10-%02d", i%30+1)
		}
		key := []byte(fmt.Sprintf("k%06d", i))
		raw, err := json.Marshal(doc)
		mustCompat(t, err)
		mustCompat(t, b.With(plain).InsertJSON(key, raw))
		mustCompat(t, b.With(indexed).InsertJSON(key, raw))
		if i%5000 == 4999 {
			mustCompat(t, b.Commit())
			b = d.NewBatch()
		}
	}
	mustCompat(t, indexed.CreateCompoundIndex(bg, "tenant_day", []IndexField{{Field: "tenant"}, {Field: "day"}}))
	mustCompat(t, indexed.CreateCompoundIndex(bg, "day_desc", []IndexField{{Field: "day", Desc: true}}))
	group := `{"$group":{"_id":"$status","n":{"$sum":1},"v":{"$sum":"$amount"}}}`
	cases := []struct {
		match string
		index string // "" when a scan is expected
	}{
		{`{"tenant":"t3","day":"2026-10-05"}`, "tenant_day"},
		{`{"$and":[{"tenant":"t3"},{"day":{"$ne":null}},{"day":{"$gte":"2026-10-03"}},{"day":{"$lt":"2026-10-06"}}]}`, "tenant_day"},
		{`{"tenant":"t3","day":{"$gt":20}}`, "tenant_day"},
		{`{"tenant":"t1","day":null}`, "tenant_day"},
		{`{"day":{"$lte":"2026-10-02"}}`, "day_desc"},
		{`{"day":{"$gte":"2026-10-29"},"status":"s1"}`, "day_desc"},
		{`{"tenant":{"$in":["t1","t2"]},"day":"2026-10-07"}`, "tenant_day"},
		{`{"tenant":{"$in":["t3"]},"day":"2026-10-05"}`, "tenant_day"},
		{`{"$and":[{"tenant":{"$ne":null}},{"tenant":{"$in":["t3","t1","t3"]}},{"$and":[{"day":{"$ne":null}},{"day":{"$gte":"2026-10-03"}}]},{"$and":[{"day":{"$ne":null}},{"day":{"$lt":"2026-10-06"}}]}]}`, "tenant_day"},
		{`{"tenant":"t3","day":{"$in":[2,2.0,12]}}`, "tenant_day"},
		{`{"tenant":"t1","day":{"$in":[null,"2026-10-03"]}}`, "tenant_day"},
		{`{"tenant":{"$in":["t0","t1","t2","t3","t4","t5","t6"]},"day":{"$in":["2026-10-01","2026-10-02","2026-10-03","2026-10-04","2026-10-05","2026-10-06","2026-10-07","2026-10-08","2026-10-09","2026-10-10"]}}`, ""}, // 70 tuples exceed maxInRanges
		{`{"tenant":"t3"}`, ""}, // more than an eighth of the collection
		{`{"status":"s2"}`, ""},
	}
	run := func(c *Collection, pipe []byte, mode int) ([]map[string]any, string) {
		t.Helper()
		var rows []map[string]any
		var stats AggregateStats
		var err error
		switch mode {
		case 0:
			rows, err = c.Aggregate(bg, pipe)
		case 1:
			rows, err = c.AggregateParallel(bg, pipe, 4)
		default:
			err = c.AggregateEach(bg, pipe, AggregateOptions{AllowDiskUse: true, Stats: &stats}, func(r map[string]any) bool {
				rows = append(rows, r)
				return true
			})
		}
		mustCompat(t, err)
		sort.Slice(rows, func(i, j int) bool { return fmt.Sprint(rows[i]["_id"]) < fmt.Sprint(rows[j]["_id"]) })
		return rows, stats.Index
	}
	for _, tc := range cases {
		pipe := []byte(`[{"$match":` + tc.match + `},` + group + `]`)
		want, _ := run(plain, pipe, 0)
		if len(want) == 0 {
			t.Fatalf("%s: no rows", tc.match)
		}
		for mode := 0; mode < 3; mode++ {
			before := d.Stats().IndexScans
			got, index := run(indexed, pipe, mode)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s mode %d:\n got %v\nwant %v", tc.match, mode, got, want)
			}
			used := d.Stats().IndexScans > before
			if used != (tc.index != "") {
				t.Fatalf("%s mode %d: index used = %v", tc.match, mode, used)
			}
			if mode == 2 && index != tc.index {
				t.Fatalf("%s: index %q, want %q", tc.match, index, tc.index)
			}
		}
	}
}

// A sparse compound index covers only filters requiring each of its fields.
func TestAggregateSkipsIncompleteSparseCompoundIndex(t *testing.T) {
	d := compatDB(t)
	c, err := d.Collection("events")
	mustCompat(t, err)
	b := d.NewBatch()
	for i := 0; i < 20000; i++ {
		doc := map[string]any{"tenant": fmt.Sprintf("t%d", i%40)}
		if i%2 == 0 {
			doc["day"] = fmt.Sprintf("d%d", i%7)
		}
		raw, err := json.Marshal(doc)
		mustCompat(t, err)
		mustCompat(t, b.With(c).InsertJSON([]byte(fmt.Sprintf("k%06d", i)), raw))
	}
	mustCompat(t, b.Commit())
	mustCompat(t, c.CreateCompoundIndex(bg, "sparse", []IndexField{{Field: "tenant"}, {Field: "day"}}, IndexOptions{Sparse: true}))
	count := func(match string) (int64, bool) {
		before := d.Stats().IndexScans
		rows, err := c.Aggregate(bg, []byte(`[{"$match":`+match+`},{"$count":"n"}]`))
		mustCompat(t, err)
		return rows[0]["n"].(int64), d.Stats().IndexScans > before
	}
	if n, used := count(`{"tenant":"t3"}`); n != 500 || used {
		t.Fatalf("tenant only: n=%d indexed=%v", n, used)
	}
	if n, used := count(`{"tenant":"t4","day":"d4"}`); n == 0 || !used {
		t.Fatalf("tenant and day: n=%d indexed=%v", n, used)
	}
	if n, used := count(`{"tenant":{"$in":["t4","t6"]},"day":{"$in":["d4","d6"]}}`); n == 0 || !used {
		t.Fatalf("tenant and day lists: n=%d indexed=%v", n, used)
	}
	// a listed null matches documents without the field, which a sparse index omits
	if n, used := count(`{"tenant":"t5","day":{"$in":["d4",null]}}`); n != 500 || used {
		t.Fatalf("tenant and day or null: n=%d indexed=%v", n, used)
	}
}
