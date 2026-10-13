package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestDocumentRoundTrip(t *testing.T) {
	src := []byte(`{"name":"ada","age":36,"score":9.5,"ok":true,"none":null}`)
	enc, err := EncodeJSON(nil, src)
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeDocument(enc)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"name": "ada", "age": int64(36), "score": 9.5, "ok": true, "none": nil}
	if !reflect.DeepEqual(m, want) {
		t.Fatalf("got %#v", m)
	}
	nested, err := EncodeJSON(nil, []byte(`{"a":{"b":1},"l":[1,"x"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if v := pathValue(docField(nested, "a"), "b"); v.k != kInt || v.i != 1 {
		t.Fatalf("nested path lookup: %+v", v)
	}
	if docField(enc, "age").i != 36 || docField(enc, "missing").k != kMissing {
		t.Fatal("field lookup broken")
	}
}

func TestScanOrderAndRanges(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// empty database
	n := 0
	db.Scan(nil, nil, func(k, v []byte) bool { n++; return true })
	if n != 0 {
		t.Fatal("empty scan returned rows")
	}
	rng := rand.New(rand.NewSource(5))
	m := randKV(rng, 60_000)
	insertAll(t, db, m)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var got []string
	var prev []byte
	err = db.Scan(nil, nil, func(k, v []byte) bool {
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatalf("scan out of order")
		}
		if !bytes.Equal(v, m[string(k)]) {
			t.Fatalf("scan value mismatch for %x", k)
		}
		prev = append(prev[:0], k...)
		got = append(got, string(k))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, keys) {
		t.Fatalf("full scan returned %d keys, want %d", len(got), len(keys))
	}

	// half-open range and early stop
	lo, hi := keys[1000], keys[5000]
	got = got[:0]
	db.Scan([]byte(lo), []byte(hi), func(k, v []byte) bool { got = append(got, string(k)); return true })
	if !reflect.DeepEqual(got, keys[1000:5000]) {
		t.Fatalf("range scan returned %d keys, want 4000", len(got))
	}
	got = got[:0]
	db.Scan([]byte(lo), nil, func(k, v []byte) bool { got = append(got, string(k)); return len(got) < 7 })
	if !reflect.DeepEqual(got, keys[1000:1007]) {
		t.Fatalf("early stop broken: %d keys", len(got))
	}
}

func TestScanWithConcurrentWriter(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	key := func(i int) []byte {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		return k
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200_000; i++ {
			db.Upsert(key(i*7919%200_000), []byte("v"))
		}
	}()
	for r := 0; r < 20; r++ {
		var prev []byte
		db.Scan(nil, nil, func(k, v []byte) bool {
			if prev != nil && bytes.Compare(prev, k) >= 0 {
				t.Errorf("scan out of order under writes")
				return false
			}
			prev = append(prev[:0], k...)
			return true
		})
	}
	wg.Wait()
}

type order struct {
	region, status, customer string
	amount                   float64
	qty                      int64
}

func loadOrders(t *testing.T, db *DB, n int) []order {
	t.Helper()
	rng := rand.New(rand.NewSource(8))
	regions := []string{"north", "south", "east", "west"}
	statuses := []string{"new", "shipped", "returned"}
	orders := make([]order, n)
	var b DocBuilder
	for i := range orders {
		o := order{
			region:   regions[rng.Intn(len(regions))],
			status:   statuses[rng.Intn(len(statuses))],
			customer: string(rune('a' + rng.Intn(26))),
			amount:   math.Round(rng.Float64()*10000) / 10,
			qty:      int64(1 + rng.Intn(9)),
		}
		orders[i] = o
		b.Reset()
		b.Int("id", int64(i))
		b.String("region", o.region)
		b.String("status", o.status)
		b.Float("amount", o.amount)
		b.Int("qty", o.qty)
		b.String("customer", o.customer)
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		if err := db.Upsert(k, b.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	return orders
}

func near(a, b float64) bool { return math.Abs(a-b) <= 1e-6*math.Max(1, math.Abs(a)) }

func TestAggregate(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	orders := loadOrders(t, db, 20_000)

	// expected: per-region totals over shipped orders
	type acc struct {
		sum  float64
		n    int64
		maxQ int64
		minA float64
	}
	exp := map[string]*acc{}
	for _, o := range orders {
		if o.status != "shipped" {
			continue
		}
		a := exp[o.region]
		if a == nil {
			a = &acc{minA: math.Inf(1)}
			exp[o.region] = a
		}
		a.sum += o.amount
		a.n++
		a.maxQ = max(a.maxQ, o.qty)
		a.minA = math.Min(a.minA, o.amount)
	}

	rows, err := db.Aggregate(bg, []byte(`[
		{"$match": {"status": "shipped"}},
		{"$group": {"_id": "$region", "total": {"$sum": "$amount"}, "n": {"$sum": 1},
		            "avg": {"$avg": "$amount"}, "maxq": {"$max": "$qty"}, "mina": {"$min": "$amount"}, "cnt": {"$count": {}}}},
		{"$sort": {"total": -1}}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(exp) {
		t.Fatalf("got %d groups, want %d", len(rows), len(exp))
	}
	prev := math.Inf(1)
	for _, r := range rows {
		a := exp[r["_id"].(string)]
		if a == nil || !near(r["total"].(float64), a.sum) || r["n"].(int64) != a.n || r["cnt"].(int64) != a.n ||
			!near(r["avg"].(float64), a.sum/float64(a.n)) || r["maxq"].(int64) != a.maxQ || !near(r["mina"].(float64), a.minA) {
			t.Fatalf("bad group row %#v (want %+v)", r, a)
		}
		if r["total"].(float64) > prev {
			t.Fatal("not sorted descending")
		}
		prev = r["total"].(float64)
	}

	// null _id, range predicate, $in, $limit after group, $project
	var n int64
	var sum float64
	for _, o := range orders {
		if o.amount > 500 && (o.region == "north" || o.region == "east") {
			n++
			sum += o.amount
		}
	}
	rows, err = db.Aggregate(bg, []byte(`[
		{"$match": {"amount": {"$gt": 500}, "region": {"$in": ["north", "east"]}}},
		{"$group": {"_id": null, "n": {"$sum": 1}, "sum": {"$sum": "$amount"}}},
		{"$project": {"_id": 0, "n": 1, "sum": 1}}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0]["n"].(int64) != n || !near(rows[0]["sum"].(float64), sum) {
		t.Fatalf("null-id group wrong: %#v want n=%d sum=%v", rows, n, sum)
	}
	if _, has := rows[0]["_id"]; has {
		t.Fatal("$project did not drop _id")
	}

	// composite _id and $count stage
	rows, err = db.Aggregate(bg, []byte(`[
		{"$group": {"_id": {"r": "$region", "s": "$status"}, "n": {"$sum": 1}}},
		{"$count": "groups"}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	if rows[0]["groups"].(int64) != 12 {
		t.Fatalf("composite groups: %#v", rows)
	}

	// no $group: streaming match + skip + limit, then sort on rows
	rows, err = db.Aggregate(bg, []byte(`[
		{"$match": {"customer": "q", "qty": {"$gte": 8}}},
		{"$skip": 2}, {"$limit": 5},
		{"$sort": {"amount": 1}}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	var want []order
	for _, o := range orders {
		if o.customer == "q" && o.qty >= 8 {
			want = append(want, o)
		}
	}
	want = want[2:7]
	if len(rows) != 5 {
		t.Fatalf("got %d rows", len(rows))
	}
	sort.Slice(want, func(i, j int) bool { return want[i].amount < want[j].amount })
	for i, r := range rows {
		if !near(r["amount"].(float64), want[i].amount) || r["region"].(string) != want[i].region {
			t.Fatalf("row %d = %#v, want %+v", i, r, want[i])
		}
	}

	// $group after a materialising stage takes the row path
	rows, err = db.Aggregate(bg, []byte(`[
		{"$sort": {"id": 1}}, {"$limit": 100},
		{"$group": {"_id": "$status", "first": {"$first": "$id"}, "last": {"$last": "$id"}}},
		{"$sort": {"_id": 1}}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		st := r["_id"].(string)
		var first, last int64 = -1, -1
		for i := 0; i < 100; i++ {
			if orders[i].status == st {
				if first < 0 {
					first = int64(i)
				}
				last = int64(i)
			}
		}
		if r["first"].(int64) != first || r["last"].(int64) != last {
			t.Fatalf("first/last wrong for %s: %#v", st, r)
		}
	}

	// bad pipelines are rejected
	for _, bad := range []string{`[{"$group": {}}]`, `[{"$foo": 1}]`, `[{"$match": 1}]`, `[{"$sort": {"a": 2}}]`} {
		if _, err := db.Aggregate(bg, []byte(bad)); err == nil {
			t.Fatalf("accepted bad pipeline %s", bad)
		}
	}
}

func TestInsertFindJSON(t *testing.T) {
	db, _ := Open(t.TempDir(), Options{})
	defer db.Close()
	if err := db.UpsertJSON([]byte("u1"), []byte(`{"name":"ada","age":36}`)); err != nil {
		t.Fatal(err)
	}
	out, ok, err := db.FindJSON([]byte("u1"))
	if err != nil || !ok || string(out) != `{"age":36,"name":"ada"}` {
		t.Fatalf("got %s ok=%v err=%v", out, ok, err)
	}
}

func TestShardedAggregateMatchesSerial(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	loadOrders(t, db, 30_000)
	db.Checkpoint() // shard bounds come from the tree, not the memtable
	bounds, err := db.ShardBounds(8)
	if err != nil {
		t.Fatal(err)
	}
	if len(bounds) == 0 {
		t.Fatal("expected shard bounds for a multi-level tree")
	}
	shards, _ := db.Shards(8)
	total := 0
	for _, s := range shards {
		s(func(k, v []byte) bool { total++; return true })
	}
	if total != 30_000 {
		t.Fatalf("shards cover %d keys, want 30000", total)
	}
	canon := func(rows []map[string]any) map[string]map[string]any {
		out := map[string]map[string]any{}
		for _, r := range rows {
			out[fmt.Sprint(r["_id"])] = r
		}
		return out
	}
	for _, pipe := range []string{
		`[{"$group": {"_id": "$region", "total": {"$sum": "$amount"}, "n": {"$sum": 1}, "avg": {"$avg": "$amount"},
		   "mx": {"$max": "$qty"}, "mn": {"$min": "$amount"}, "first": {"$first": "$id"}, "last": {"$last": "$id"}}}]`,
		`[{"$match": {"amount": {"$gt": 500}}}, {"$group": {"_id": null, "n": {"$sum": 1}, "avg": {"$avg": "$amount"}, "first": {"$first": "$id"}, "last": {"$last": "$id"}}}]`,
		`[{"$group": {"_id": {"r": "$region", "s": "$status"}, "n": {"$count": {}}}}, {"$sort": {"n": -1}}, {"$limit": 4}]`,
		`[{"$match": {"customer": "q"}}, {"$skip": 3}, {"$limit": 10}, {"$group": {"_id": "$status", "n": {"$sum": 1}}}]`,
		`[{"$match": {"qty": 9}}, {"$sort": {"amount": -1}}, {"$limit": 5}]`,
	} {
		p, err := ParsePipeline([]byte(pipe))
		if err != nil {
			t.Fatal(err)
		}
		serial, err := p.Run(bg, db.Source(nil, nil))
		if err != nil {
			t.Fatal(err)
		}
		par, err := p.RunSharded(bg, shards)
		if err != nil {
			t.Fatal(err)
		}
		if len(serial) != len(par) {
			t.Fatalf("%s: %d rows serial vs %d sharded", pipe, len(serial), len(par))
		}
		cs, cp := canon(serial), canon(par)
		for id, r := range cs {
			q := cp[id]
			if q == nil {
				t.Fatalf("%s: group %s missing in sharded result", pipe, id)
			}
			for k, v := range r {
				if k == "_id" {
					continue
				}
				a, b := valueOf(v), valueOf(q[k])
				if a.isNum() && b.isNum() {
					if !near(a.num(), b.num()) {
						t.Fatalf("%s: group %s field %s: %v vs %v", pipe, id, k, v, q[k])
					}
				} else if !reflect.DeepEqual(v, q[k]) {
					t.Fatalf("%s: group %s field %s: %v vs %v", pipe, id, k, v, q[k])
				}
			}
		}
	}
}

func TestNestedDocumentsArraysAndLimits(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	docs := []string{
		`{"name":"ada","addr":{"city":"Paris","geo":{"lat":48.8}},"tags":["math","cs"],"scores":[9,7],"n":1}`,
		`{"name":"bob","addr":{"city":"Rome"},"tags":["cs"],"scores":[5],"n":2}`,
		`{"name":"cyd","addr":{"city":"Paris"},"tags":[],"scores":[],"n":3}`,
		`{"name":"dee","tags":"solo","n":4}`,
		`{"name":"eve","addr":{"city":"Oslo"},"tags":["math"],"scores":[10,10,10],"n":5}`,
	}
	for i, d := range docs {
		if err := db.UpsertJSON([]byte(fmt.Sprintf("u%d", i)), []byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	// round trip keeps nesting
	out, ok, err := db.FindJSON([]byte("u0"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	var back map[string]any
	json.Unmarshal(out, &back)
	if back["addr"].(map[string]any)["geo"].(map[string]any)["lat"] != 48.8 || len(back["tags"].([]any)) != 2 {
		t.Fatalf("nested round trip lost data: %s", out)
	}
	// dotted path match, streaming group
	rows, err := db.Aggregate(bg, []byte(`[{"$match": {"addr.city": "Paris"}}, {"$group": {"_id": null, "n": {"$sum": 1}, "names": {"$min": "$name"}}}]`))
	if err != nil || rows[0]["n"].(int64) != 2 || rows[0]["names"].(string) != "ada" {
		t.Fatalf("dotted match: %v %v", rows, err)
	}
	// array containment and $in on arrays
	rows, err = db.Aggregate(bg, []byte(`[{"$match": {"tags": "math"}}, {"$count": "n"}]`))
	if err != nil || rows[0]["n"].(int64) != 2 {
		t.Fatalf("array containment: %v %v", rows, err)
	}
	rows, err = db.Aggregate(bg, []byte(`[{"$match": {"tags": {"$in": ["solo", "cs"]}}}, {"$count": "n"}]`))
	if err != nil || rows[0]["n"].(int64) != 3 {
		t.Fatalf("$in over arrays: %v %v", rows, err)
	}
	rows, err = db.Aggregate(bg, []byte(`[{"$match": {"scores": {"$gte": 9}}}, {"$sort": {"name": 1}}]`))
	if err != nil || len(rows) != 2 || rows[0]["name"] != "ada" || rows[1]["name"] != "eve" {
		t.Fatalf("range over array elements: %v %v", rows, err)
	}
	// $unwind + group, and preserve option
	rows, err = db.Aggregate(bg, []byte(`[{"$unwind": "$tags"}, {"$group": {"_id": "$tags", "n": {"$sum": 1}}}, {"$sort": {"n": -1, "_id": 1}}]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0]["_id"] != "cs" || rows[0]["n"].(int64) != 2 || rows[1]["_id"] != "math" || rows[2]["_id"] != "solo" {
		t.Fatalf("unwind+group+multikey sort: %v", rows)
	}
	rows, err = db.Aggregate(bg, []byte(`[{"$unwind": {"path": "$scores", "preserveNullAndEmptyArrays": true}}, {"$count": "n"}]`))
	if err != nil || rows[0]["n"].(int64) != 8 { // 2+1+1(empty kept)+1(missing kept)+3
		t.Fatalf("unwind preserve: %v %v", rows, err)
	}
	// multi-key sort order is the key order given
	rows, err = db.Aggregate(bg, []byte(`[{"$unwind": "$scores"}, {"$sort": {"scores": -1, "name": 1}}, {"$limit": 3}, {"$project": {"name": 1, "scores": 1, "_id": 0}}]`))
	if err != nil || len(rows) != 3 || rows[0]["name"] != "eve" || rows[2]["name"] != "eve" {
		t.Fatalf("multi-key sort: %v %v", rows, err)
	}
	// group by a nested field and project a nested path away
	rows, err = db.Aggregate(bg, []byte(`[{"$group": {"_id": "$addr.city", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]`))
	if err != nil || len(rows) != 4 || rows[0]["_id"] != nil || rows[1]["_id"] != "Oslo" {
		t.Fatalf("group by nested path: %v %v", rows, err)
	}
	// limits
	p, _ := ParsePipeline([]byte(`[{"$group": {"_id": "$name", "n": {"$sum": 1}}}]`))
	p.SetLimits(Limits{MaxGroups: 2, MaxRows: 100})
	if _, err := p.Run(bg, db.Source(nil, nil)); !errors.Is(err, ErrLimit) {
		t.Fatalf("group limit not enforced: %v", err)
	}
	p, _ = ParsePipeline([]byte(`[{"$match": {"n": {"$gt": 0}}}]`))
	p.SetLimits(Limits{MaxGroups: 10, MaxRows: 3})
	if _, err := p.Run(bg, db.Source(nil, nil)); !errors.Is(err, ErrLimit) {
		t.Fatalf("row limit not enforced: %v", err)
	}
	// nesting depth is bounded
	deep := "1"
	for i := 0; i < 20; i++ {
		deep = `{"a":` + deep + `}`
	}
	if _, err := EncodeJSON(nil, []byte(deep)); err == nil {
		t.Fatal("expected a nesting error")
	}
}
