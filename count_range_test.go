package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func countRangeIndexes(t *testing.T, c *Collection) {
	t.Helper()
	ctx := context.Background()
	for name, fields := range map[string][]IndexField{
		"scope_at":      {{Field: "scope"}, {Field: "at"}},
		"scope_at_desc": {{Field: "region"}, {Field: "at", Desc: true}},
		"two_then_at":   {{Field: "tenant"}, {Field: "scope"}, {Field: "n"}},
		"n_only":        {{Field: "n", Desc: true}},
	} {
		if err := c.CreateCompoundIndex(ctx, name, fields); err != nil {
			t.Fatal(err)
		}
	}
}

// Every index-only count must equal evaluating the documents, on the same
// snapshot, through the engine's own predicates.
func TestCountRangesMatchDocumentEvaluation(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("events")
	if err != nil {
		t.Fatal(err)
	}
	countRangeIndexes(t, c)
	rng := rand.New(rand.NewSource(11))
	strs := []any{nil, "", "a", "a\x00", "a\x00b", "ab", "b", "é", "zz"}
	nums := []any{nil, -1.5, int64(-1), 0, 0.0, 1, 1.5, int64(1) << 53, (int64(1) << 53) + 1, float64(int64(1) << 53), math.MaxInt64, "1", "x", true, false}
	pick := func(v []any) any { return v[rng.Intn(len(v))] }
	insert := func(i int) {
		doc := map[string]any{"pad": "p"}
		for field, values := range map[string][]any{"scope": strs, "region": strs, "tenant": strs, "at": nums, "n": nums} {
			if rng.Intn(10) > 0 { // otherwise missing
				doc[field] = pick(values)
			}
		}
		raw, _ := json.Marshal(doc)
		if err := c.InsertJSON([]byte(fmt.Sprintf("k%05d", i)), raw); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3000; i++ {
		insert(i)
	}
	term := func(field string, values []any) any {
		switch rng.Intn(7) {
		case 0:
			return map[string]any{field: map[string]any{"$eq": pick(values)}}
		case 1, 2:
			n := 1 + rng.Intn(4)
			list := make([]any, n)
			for i := range list {
				list[i] = pick(values)
			}
			return map[string]any{field: map[string]any{"$in": list}}
		case 3:
			return map[string]any{field: map[string]any{"$ne": nil}}
		default:
			return map[string]any{"$and": []any{map[string]any{field: map[string]any{"$ne": nil}}, map[string]any{field: map[string]any{[]string{"$gt", "$gte", "$lt", "$lte"}[rng.Intn(4)]: pick(values)}}}}
		}
	}
	covered := 0
	check := func(rounds int) {
		t.Helper()
		for i := 0; i < rounds; i++ {
			filter := []any{}
			switch rng.Intn(5) {
			case 0:
				filter = append(filter, term("scope", strs), term("at", nums))
			case 1:
				filter = append(filter, term("region", strs), term("at", nums), term("at", nums))
			case 2:
				filter = append(filter, term("tenant", strs), term("scope", strs), term("n", nums))
			case 3:
				filter = append(filter, term("n", nums), term("n", nums))
			default:
				filter = append(filter, term("scope", strs), term("scope", strs), term("at", nums))
			}
			raw, _ := json.Marshal(map[string]any{"$and": filter})
			var stats QueryStats
			got, err := c.CountDocuments(context.Background(), raw, CountOptions{Stats: &stats})
			if err != nil {
				t.Fatalf("%s: %v", raw, err)
			}
			want, err := c.CountDocuments(context.Background(), raw, CountOptions{Hint: "$natural"})
			if err != nil {
				t.Fatalf("%s: %v", raw, err)
			}
			if got != want {
				t.Fatalf("%s: plan %+v counted %d, documents %d", raw, stats.Plan, got, want)
			}
			if stats.Plan.IndexOnly {
				covered++
				if stats.DocumentFetches != 0 || stats.IndexEntriesExamined != got {
					t.Fatalf("%s: index-only read documents: %+v, count %d", raw, stats, got)
				}
			}
		}
	}
	check(2000)
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	check(2000)
	// Buffered writes over a checkpoint take the merge scanner.
	for i := 3000; i < 3300; i++ {
		insert(i)
	}
	for i := 0; i < 200; i++ {
		if err := c.Delete([]byte(fmt.Sprintf("k%05d", i*7))); err != nil {
			t.Fatal(err)
		}
	}
	check(2000)
	if covered < 1000 {
		t.Fatalf("only %d filters used the index-only path", covered)
	}
}

func TestCountRangesPlan(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("events")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.CreateCompoundIndex(ctx, "by_scope_at", []IndexField{{Field: "scope"}, {Field: "at"}}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2000; i++ {
		raw := fmt.Sprintf(`{"scope":"S%d","at":%d,"other":%d}`, i%4, i, i%2)
		if err := c.InsertJSON([]byte(fmt.Sprintf("k%05d", i)), []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	const in = `{"$and":[{"scope":{"$ne":null}},{"scope":{"$in":["S1","S2","S1"]}},{"at":{"$ne":null}},{"at":{"$gte":100}},{"at":{"$lt":900}}]}`
	var stats QueryStats
	n, err := c.CountDocuments(ctx, []byte(in), CountOptions{Stats: &stats})
	if err != nil || n != 400 {
		t.Fatal(n, err)
	}
	if !stats.Plan.IndexOnly || stats.Plan.Ranges != 2 || stats.DocumentFetches != 0 || stats.IndexEntriesExamined != 400 {
		t.Fatalf("%+v", stats)
	}
	// Skip and limit cross the ranges.
	n, err = c.CountDocuments(ctx, []byte(in), CountOptions{Skip: 150, Limit: 100, Stats: &stats})
	if err != nil || n != 100 {
		t.Fatal(n, err)
	}
	n, err = c.CountDocuments(ctx, []byte(in), CountOptions{Skip: 350, Limit: 100})
	if err != nil || n != 50 {
		t.Fatal(n, err)
	}
	// A filter the index cannot prove reads documents, narrowed by the $in.
	residual := `{"$and":[{"scope":{"$in":["S1","S2"]}},{"at":{"$gte":100}},{"other":{"$eq":1}}]}`
	if n, err = c.CountDocuments(ctx, []byte(residual), CountOptions{Stats: &stats}); err != nil {
		t.Fatal(err)
	}
	want, err := c.CountDocuments(ctx, []byte(residual), CountOptions{Hint: "$natural"})
	if err != nil || n != want {
		t.Fatal(n, want, err)
	}
	if stats.Plan.IndexOnly || stats.Plan.Index != "by_scope_at" || stats.DocumentFetches >= 2000 {
		t.Fatalf("%+v", stats)
	}
	// $in on a null matches missing, and $ne null removes it.
	if err := c.InsertJSON([]byte("missing"), []byte(`{"at":5}`)); err != nil {
		t.Fatal(err)
	}
	for filter, want := range map[string]int64{
		`{"scope":{"$in":[null]}}`:                                               1,
		`{"$and":[{"scope":{"$ne":null}},{"scope":{"$in":[null,"S1"]}}]}`:        500,
		`{"$and":[{"scope":{"$in":[null,"S1"]}},{"at":{"$lt":2}}]}`:              1,
		`{"$and":[{"scope":{"$eq":"S1"}},{"scope":{"$eq":"S2"}}]}`:               0,
		`{"$and":[{"scope":{"$eq":"S1"}},{"at":{"$gte":1}},{"at":{"$lt":"x"}}]}`: 0,
	} {
		n, err := c.CountDocuments(ctx, []byte(filter), CountOptions{Stats: &stats})
		if err != nil || n != want || !stats.Plan.IndexOnly {
			t.Fatalf("%s: %d want %d, %v, %+v", filter, n, want, err, stats.Plan)
		}
	}
	// $ne null alone cannot bind a field, and more ranges than the bound fall
	// back to documents.
	values := []string{}
	for i := 0; i <= maxInRanges; i++ {
		values = append(values, fmt.Sprintf("%q", fmt.Sprintf("S%d", i)))
	}
	for _, filter := range []string{
		`{"scope":{"$ne":null}}`,
		`{"$and":[{"scope":{"$eq":"S1"}},{"at":{"$ne":null}}]}`,
		`{"scope":{"$in":[` + join(values) + `]}}`,
		`{"$or":[{"scope":{"$eq":"S1"}},{"at":{"$lt":5}}]}`,
	} {
		n, err := c.CountDocuments(ctx, []byte(filter), CountOptions{Stats: &stats})
		if err != nil || stats.Plan.IndexOnly {
			t.Fatalf("%s: %d %v %+v", filter, n, err, stats.Plan)
		}
		want, err := c.CountDocuments(ctx, []byte(filter), CountOptions{Hint: "$natural"})
		if err != nil || n != want {
			t.Fatalf("%s: %d want %d, %v", filter, n, want, err)
		}
	}
}

func join(values []string) string {
	out := ""
	for i, v := range values {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}

// Sparse indexes omit documents that lack a field, so they can only prove a
// count that requires a value for every field.
func TestCountRangesSparse(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("events")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := c.CreateCompoundIndex(ctx, "sparse", []IndexField{{Field: "a"}, {Field: "b"}}, IndexOptions{Sparse: true}); err != nil {
		t.Fatal(err)
	}
	for i, raw := range []string{`{"a":"x","b":1}`, `{"a":"x"}`, `{"b":1}`, `{"a":"y","b":2}`, `{"a":null,"b":2}`} {
		if err := c.InsertJSON([]byte(fmt.Sprintf("k%d", i)), []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	for _, filter := range []string{
		`{"a":{"$in":["x","y"]}}`,
		`{"$and":[{"a":{"$in":["x","y"]}},{"b":{"$gte":1}}]}`,
		`{"$and":[{"a":{"$in":["x",null]}},{"b":{"$gte":1}}]}`,
		`{"a":{"$eq":"x"}}`,
	} {
		var stats QueryStats
		got, err := c.CountDocuments(ctx, []byte(filter), CountOptions{Stats: &stats})
		if err != nil {
			t.Fatal(err)
		}
		want, err := c.CountDocuments(ctx, []byte(filter), CountOptions{Hint: "$natural"})
		if err != nil || got != want {
			t.Fatalf("%s: %d want %d, %v (%+v)", filter, got, want, err, stats.Plan)
		}
	}
}
