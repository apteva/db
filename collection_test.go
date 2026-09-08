package aptevadb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestCollectionsBasics(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	users, err := db.Collection("users")
	if err != nil {
		t.Fatal(err)
	}
	orders, err := db.Collection("orders")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := db.Collection("users"); again != users {
		t.Fatal("Collection should return the same handle")
	}
	// the same key in three collections holds three values
	db.Insert([]byte("k"), []byte("default"))
	users.Insert([]byte("k"), []byte("user"))
	orders.Insert([]byte("k"), []byte("order"))
	for _, c := range []struct {
		get  func([]byte, []byte) ([]byte, bool, error)
		want string
	}{{db.Find, "default"}, {users.Find, "user"}, {orders.Find, "order"}} {
		v, ok, err := c.get([]byte("k"), nil)
		if err != nil || !ok || string(v) != c.want {
			t.Fatalf("got %q %v %v, want %q", v, ok, err, c.want)
		}
	}
	// scans and cursors stay inside their collection
	for i := 0; i < 100; i++ {
		users.Insert([]byte(fmt.Sprintf("u%03d", i)), []byte("x"))
		orders.Insert([]byte(fmt.Sprintf("o%03d", i)), []byte("y"))
	}
	n := 0
	users.Scan(nil, nil, func(k, v []byte) bool {
		if k[0] != 'u' && string(k) != "k" {
			t.Fatalf("foreign key %q in users scan", k)
		}
		n++
		return true
	})
	if n != 101 {
		t.Fatalf("users scan saw %d keys, want 101", n)
	}
	cur := orders.NewCursor()
	n = 0
	for ok := cur.First(); ok; ok = cur.Next() {
		n++
	}
	if n != 101 {
		t.Fatalf("orders cursor saw %d keys, want 101", n)
	}
	if !cur.Last() || string(cur.Key()) != "o099" {
		t.Fatalf("Last = %q", cur.Key())
	}
	n = 0
	for ok := cur.Last(); ok; ok = cur.Prev() {
		n++
	}
	if n != 101 {
		t.Fatalf("orders cursor backwards saw %d keys, want 101", n)
	}
	if cur.Seek([]byte("o050")); string(cur.Key()) != "o050" {
		t.Fatalf("Seek = %q", cur.Key())
	}
	if got := db.Collections(); !reflect.DeepEqual(got, []string{"orders", "users"}) {
		t.Fatalf("Collections = %v", got)
	}
	if st := db.Stats(); st.Collections != 2 {
		t.Fatalf("Stats.Collections = %d", st.Collections)
	}
	// keys with any leading byte live happily in a named collection
	if err := users.Insert([]byte{0xff, 0xff}, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := users.Insert([]byte{0x00}, []byte("lo")); err != nil {
		t.Fatal(err)
	}
	if v, ok, _ := users.Find([]byte{0xff, 0xff}, nil); !ok || string(v) != "hi" {
		t.Fatal("0xff key lost")
	}
	if v, ok, _ := orders.Find([]byte{0xff, 0xff}, nil); ok {
		t.Fatalf("0xff key leaked into orders: %q", v)
	}
	// survives reopen
	db.Close()
	db, err = Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := db.Collections(); !reflect.DeepEqual(got, []string{"orders", "users"}) {
		t.Fatalf("after reopen Collections = %v", got)
	}
	users, _ = db.Collection("users")
	if v, ok, _ := users.Find([]byte("k"), nil); !ok || string(v) != "user" {
		t.Fatal("users.k lost across reopen")
	}
	orders, _ = db.Collection("orders")
	if v, ok, _ := orders.Find([]byte("k"), nil); !ok || string(v) != "order" {
		t.Fatal("orders.k lost across reopen")
	}
	// a third collection gets a fresh id, not a recycled one
	third, _ := db.Collection("third")
	if third.id != 3 {
		t.Fatalf("third id = %d", third.id)
	}
}

func TestDropCollection(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, _ := db.Collection("tmp")
	keep, _ := db.Collection("keep")
	for i := 0; i < 5000; i++ {
		c.InsertJSON([]byte(fmt.Sprint(i)), []byte(fmt.Sprintf(`{"n":%d}`, i%10)))
		keep.Insert([]byte(fmt.Sprint(i)), []byte("k"))
	}
	if err := c.CreateIndex(bg, "by_n", "n"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection(bg, "tmp"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection(bg, "tmp"); err != ErrNoSuchCollection {
		t.Fatalf("second drop: %v", err)
	}
	if err := c.Insert([]byte("x"), []byte("y")); err != ErrNoSuchCollection {
		t.Fatalf("write to dropped collection: %v", err)
	}
	if got := db.Collections(); !reflect.DeepEqual(got, []string{"keep"}) {
		t.Fatalf("Collections = %v", got)
	}
	if len(db.indexDefs()) != 0 {
		t.Fatal("index survived the drop")
	}
	// nothing of it remains in the tree
	n := 0
	db.scan(view{db, nil}, nil, nil, func(k, _ []byte) bool { n++; return true })
	if n != 5000+1 { // keep's rows and its catalog entry
		t.Fatalf("%d keys after drop, want 5001", n)
	}
	if v, ok, _ := keep.Find([]byte("4999"), nil); !ok || string(v) != "k" {
		t.Fatal("keep lost data")
	}
	// the name can be reused; the new collection is empty
	c2, _ := db.Collection("tmp")
	if _, ok, _ := c2.Find([]byte("1"), nil); ok {
		t.Fatal("recreated collection is not empty")
	}
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}

func TestCrossCollectionBatch(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	users, _ := db.Collection("users")
	orders, _ := db.Collection("orders")
	if err := orders.CreateIndex(bg, "by_user", "user"); err != nil {
		t.Fatal(err)
	}
	b := users.NewBatch()
	b.InsertJSON([]byte("u1"), []byte(`{"name":"ann","orders":0}`))
	ob := b.With(orders)
	ob.InsertJSON([]byte("o1"), []byte(`{"user":"u1","amount":10}`))
	ob.InsertJSON([]byte("o2"), []byte(`{"user":"u1","amount":20}`))
	b.InsertJSON([]byte("u1"), []byte(`{"name":"ann","orders":2}`)) // later write wins
	if b.Len() != 4 {
		t.Fatalf("Len = %d", b.Len())
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if j, _, _ := users.FindJSON([]byte("u1")); !strings.Contains(string(j), `"orders":2`) {
		t.Fatalf("user = %s", j)
	}
	rows, err := orders.Aggregate(bg, []byte(`[{"$match":{"user":"u1"}},{"$group":{"_id":null,"t":{"$sum":"$amount"}}}]`))
	if err != nil || len(rows) != 1 || rows[0]["t"] != int64(30) {
		t.Fatalf("rows = %v err = %v", rows, err)
	}
	if db.Stats().IndexScans != 1 {
		t.Fatal("the $match on user should have used the index")
	}
	// all or nothing across a crash: the batch is one log record group
	db.Close()
	db, err = Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	orders, _ = db.Collection("orders")
	if _, ok, _ := orders.Find([]byte("o2"), nil); !ok {
		t.Fatal("o2 lost")
	}
}

func TestIndexesArePerCollection(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	a, _ := db.Collection("a")
	bc, _ := db.Collection("b")
	for i := 0; i < 1000; i++ {
		a.InsertJSON([]byte(fmt.Sprint(i)), []byte(fmt.Sprintf(`{"g":%d,"v":%d}`, i%7, i)))
		bc.InsertJSON([]byte(fmt.Sprint(i)), []byte(fmt.Sprintf(`{"g":%d,"v":%d}`, i%7, -i)))
	}
	if err := a.CreateIndex(bg, "by_g", "g"); err != nil {
		t.Fatal(err)
	}
	if err := bc.CreateIndex(bg, "by_g", "g"); err != nil { // same name, other collection
		t.Fatal(err)
	}
	if err := a.CreateIndex(bg, "by_g", "v"); err != ErrIndexExists {
		t.Fatalf("duplicate name: %v", err)
	}
	if got := a.Indexes(); len(got) != 1 || got["by_g"] != "g" {
		t.Fatalf("a.Indexes = %v", got)
	}
	if got := db.Indexes(); len(got) != 0 {
		t.Fatalf("default Indexes = %v", got)
	}
	q := []byte(`[{"$match":{"g":3}},{"$group":{"_id":null,"s":{"$sum":"$v"},"n":{"$sum":1}}}]`)
	ra, _ := a.Aggregate(bg, q)
	rb, _ := bc.Aggregate(bg, q)
	if ra[0]["n"] != int64(143) || rb[0]["n"] != int64(143) {
		t.Fatalf("counts %v %v", ra, rb)
	}
	if ra[0]["s"].(int64) != -rb[0]["s"].(int64) {
		t.Fatalf("sums %v %v", ra, rb)
	}
	if db.Stats().IndexScans != 2 {
		t.Fatalf("IndexScans = %d", db.Stats().IndexScans)
	}
	// maintenance follows the collection: updating a doc in b touches b's index only
	bc.InsertJSON([]byte("5"), []byte(`{"g":3,"v":100}`))
	a.Delete([]byte("3"))
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	if err := a.DropIndex(bg, "by_g"); err != nil {
		t.Fatal(err)
	}
	if err := bc.DropIndex(bg, "by_g"); err != nil {
		t.Fatal(err)
	}
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotCollections(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS, MemtableBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, _ := db.Collection("c")
	for i := 0; i < 100; i++ {
		c.InsertJSON([]byte(fmt.Sprintf("%03d", i)), []byte(`{"v":1}`))
	}
	s, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc, err := s.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Collection("nope"); err != ErrNoSuchCollection {
		t.Fatalf("missing collection: %v", err)
	}
	for i := 0; i < 100; i++ {
		c.InsertJSON([]byte(fmt.Sprintf("%03d", i)), []byte(`{"v":2}`))
		c.InsertJSON([]byte(fmt.Sprintf("n%03d", i)), []byte(`{"v":2}`))
	}
	rows, err := sc.Aggregate(bg, []byte(`[{"$group":{"_id":null,"s":{"$sum":"$v"},"n":{"$sum":1}}}]`))
	if err != nil || rows[0]["s"] != int64(100) || rows[0]["n"] != int64(100) {
		t.Fatalf("snapshot rows %v err %v", rows, err)
	}
	live, _ := c.Aggregate(bg, []byte(`[{"$group":{"_id":null,"s":{"$sum":"$v"},"n":{"$sum":1}}}]`))
	if live[0]["s"] != int64(400) || live[0]["n"] != int64(200) {
		t.Fatalf("live rows %v", live)
	}
	if err := sc.Insert([]byte("x"), []byte("y")); err == nil {
		t.Fatal("snapshot collection accepted a write")
	}
	cur := sc.NewCursor()
	n := 0
	for ok := cur.First(); ok; ok = cur.Next() {
		n++
	}
	if n != 100 {
		t.Fatalf("snapshot cursor saw %d, want 100", n)
	}
}

// naive join reference -------------------------------------------------

type ju struct {
	id      int64
	country string
	tags    []int64
}

type jo struct {
	id     int64
	user   int64
	amount float64
	status string
	tag    int64
}

func loadJoinData(t *testing.T, db *DB, nu, no int, seed int64) (*Collection, *Collection, []ju, []jo) {
	rng := rand.New(rand.NewSource(seed))
	users, _ := db.Collection("users")
	orders, _ := db.Collection("orders")
	countries := []string{"FR", "DE", "US", "JP"}
	us := make([]ju, nu)
	ub := users.NewBatch()
	for i := range us {
		us[i] = ju{int64(i), countries[rng.Intn(4)], []int64{int64(rng.Intn(5)), int64(rng.Intn(5))}}
		if err := ub.InsertJSON([]byte(fmt.Sprintf("u%06d", i)), []byte(fmt.Sprintf(`{"id":%d,"country":%q,"tags":[%d,%d]}`, i, us[i].country, us[i].tags[0], us[i].tags[1]))); err != nil {
			t.Fatal(err)
		}
	}
	if err := ub.Commit(); err != nil {
		t.Fatal(err)
	}
	os := make([]jo, no)
	ob := orders.NewBatch()
	for i := range os {
		// some orders point at users that do not exist
		os[i] = jo{int64(i), int64(rng.Intn(nu + nu/10)), float64(rng.Intn(10000)) / 100, []string{"new", "shipped"}[rng.Intn(2)], int64(rng.Intn(5))}
		if err := ob.InsertJSON([]byte(fmt.Sprintf("o%06d", i)), []byte(fmt.Sprintf(`{"id":%d,"user":%d,"amount":%g,"status":%q,"tag":%d}`, i, os[i].user, os[i].amount, os[i].status, os[i].tag))); err != nil {
			t.Fatal(err)
		}
	}
	if err := ob.Commit(); err != nil {
		t.Fatal(err)
	}
	// a non-document in each collection must be invisible
	users.Insert([]byte("raw"), []byte{1, 2, 3})
	orders.Insert([]byte("raw"), []byte{4, 5, 6})
	return users, orders, us, os
}

type joinRow struct {
	id   string
	vals []float64
}

func canonRows(rows []map[string]any) []joinRow {
	var out []joinRow
	for _, r := range rows {
		id := ""
		if v, ok := r["_id"]; ok && v != nil {
			id = fmt.Sprint(v)
		}
		names := make([]string, 0, len(r))
		for k := range r {
			if k != "_id" {
				names = append(names, k)
			}
		}
		sort.Strings(names)
		var vals []float64
		for _, k := range names {
			switch x := r[k].(type) {
			case int64:
				vals = append(vals, float64(x))
			case float64:
				vals = append(vals, x)
			}
		}
		out = append(out, joinRow{id, vals})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func sameRows(a, b []joinRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].id != b[i].id || len(a[i].vals) != len(b[i].vals) {
			return false
		}
		for j := range a[i].vals {
			d := a[i].vals[j] - b[i].vals[j]
			if d > 1e-6 || d < -1e-6 {
				return false
			}
		}
	}
	return true
}

func TestLookupMatchesNaiveJoin(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS, MemtableBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	users, orders, us, os := loadJoinData(t, db, 500, 4000, 7)
	byID := map[int64]ju{}
	for _, u := range us {
		byID[u.id] = u
	}

	// 1. orders joined with users, grouped by country: sum, count
	q1 := `[{"$lookup":{"from":"users","localField":"user","foreignField":"id","as":"u"}},{"$unwind":"$u"},
	        {"$group":{"_id":"$u.country","n":{"$sum":1},"total":{"$sum":"$amount"}}}]`
	want1 := map[string][]float64{}
	for _, o := range os {
		if u, ok := byID[o.user]; ok {
			w := want1[u.country]
			if w == nil {
				w = []float64{0, 0}
			}
			w[0]++
			w[1] += o.amount
			want1[u.country] = w
		}
	}
	// 2. filtered on both sides, global count and avg
	q2 := `[{"$match":{"status":"shipped"}},{"$lookup":{"from":"users","localField":"user","foreignField":"id","as":"u"}},{"$unwind":"$u"},
	        {"$match":{"u.country":"FR","amount":{"$gt":20}}},{"$group":{"_id":null,"n":{"$sum":1},"avg":{"$avg":"$amount"}}}]`
	n2, s2 := 0.0, 0.0
	for _, o := range os {
		if u, ok := byID[o.user]; ok && o.status == "shipped" && u.country == "FR" && o.amount > 20 {
			n2++
			s2 += o.amount
		}
	}
	// 3. left join: orders without a user
	q3 := `[{"$lookup":{"from":"users","localField":"user","foreignField":"id","as":"u"}},{"$unwind":{"path":"$u","preserveNullAndEmptyArrays":true}},
	        {"$match":{"u":null}},{"$count":"orphans"}]`
	orphans := 0
	for _, o := range os {
		if _, ok := byID[o.user]; !ok {
			orphans++
		}
	}
	// 4. array on the foreign side: orders whose tag is one of the user's tags
	q4 := `[{"$lookup":{"from":"users","localField":"tag","foreignField":"tags","as":"u"}},{"$unwind":"$u"},{"$count":"pairs"}]`
	pairs := 0
	for _, o := range os {
		for _, u := range us {
			if u.tags[0] == o.tag || u.tags[1] == o.tag {
				pairs++
			}
		}
	}
	// 5. users to orders, without unwind (materialised), top spenders via $unwind + $group
	q5 := `[{"$match":{"country":"DE"}},{"$lookup":{"from":"orders","localField":"id","foreignField":"user","as":"os"}},
	        {"$unwind":"$os"},{"$group":{"_id":"$id","spent":{"$sum":"$os.amount"}}},{"$sort":{"spent":-1,"_id":1}},{"$limit":5}]`
	spent := map[int64]float64{}
	for _, o := range os {
		if u, ok := byID[o.user]; ok && u.country == "DE" {
			spent[o.user] += o.amount
		}
	}
	type kv struct {
		id int64
		v  float64
	}
	var top []kv
	for id, v := range spent {
		top = append(top, kv{id, v})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].v != top[j].v {
			return top[i].v > top[j].v
		}
		return top[i].id < top[j].id
	})
	if len(top) > 5 {
		top = top[:5]
	}
	// 6. materialised lookup kept as an array, then $project
	q6 := `[{"$match":{"id":{"$in":[1,2,3]}}},{"$lookup":{"from":"orders","localField":"id","foreignField":"user","as":"os"}},{"$project":{"id":1,"os":1}}]`

	check := func(label string, run func(q string) ([]map[string]any, error)) {
		t.Helper()
		rows, err := run(q1)
		if err != nil {
			t.Fatalf("%s q1: %v", label, err)
		}
		got := map[string][]float64{}
		for _, r := range rows {
			got[fmt.Sprint(r["_id"])] = []float64{float64(r["n"].(int64)), r["total"].(float64)}
		}
		if len(got) != len(want1) {
			t.Fatalf("%s q1: %v want %v", label, got, want1)
		}
		for k, w := range want1 {
			g := got[k]
			if g == nil || g[0] != w[0] || g[1]-w[1] > 1e-6 || w[1]-g[1] > 1e-6 {
				t.Fatalf("%s q1 %s: %v want %v", label, k, g, w)
			}
		}
		rows, err = run(q2)
		if err != nil {
			t.Fatalf("%s q2: %v", label, err)
		}
		if float64(rows[0]["n"].(int64)) != n2 || (n2 > 0 && rows[0]["avg"].(float64)-s2/n2 > 1e-9) {
			t.Fatalf("%s q2: %v want n=%v avg=%v", label, rows, n2, s2/n2)
		}
		rows, err = run(q3)
		if err != nil {
			t.Fatalf("%s q3: %v", label, err)
		}
		if rows[0]["orphans"] != int64(orphans) {
			t.Fatalf("%s q3: %v want %d", label, rows, orphans)
		}
		rows, err = run(q4)
		if err != nil {
			t.Fatalf("%s q4: %v", label, err)
		}
		if rows[0]["pairs"] != int64(pairs) {
			t.Fatalf("%s q4: %v want %d", label, rows, pairs)
		}
		rows, err = run(q5)
		if err != nil {
			t.Fatalf("%s q5: %v", label, err)
		}
		if len(rows) != len(top) {
			t.Fatalf("%s q5: %d rows want %d", label, len(rows), len(top))
		}
		for i, r := range rows {
			if r["_id"] != top[i].id || r["spent"].(float64)-top[i].v > 1e-6 || top[i].v-r["spent"].(float64) > 1e-6 {
				t.Fatalf("%s q5 row %d: %v want %v", label, i, r, top[i])
			}
		}
		rows, err = run(q6)
		if err != nil {
			t.Fatalf("%s q6: %v", label, err)
		}
		if len(rows) != 3 {
			t.Fatalf("%s q6: %d rows", label, len(rows))
		}
		for _, r := range rows {
			id := r["id"].(int64)
			list := r["os"].([]any)
			n := 0
			for _, o := range os {
				if o.user == id {
					n++
				}
			}
			if len(list) != n {
				t.Fatalf("%s q6 user %d: %d orders want %d", label, id, len(list), n)
			}
			if len(r) != 2 {
				t.Fatalf("%s q6: projection kept %v", label, r)
			}
		}
	}
	// q1-q4 run from orders, q5/q6 from users: route by the "from" side
	route := func(u, o *Collection, shards int) func(q string) ([]map[string]any, error) {
		return func(q string) ([]map[string]any, error) {
			c := o
			if strings.Contains(q, `"from":"orders"`) {
				c = u
			}
			if shards > 1 {
				return c.AggregateParallel(bg, []byte(q), shards)
			}
			return c.Aggregate(bg, []byte(q))
		}
	}
	runFrom := route(users, orders, 1)
	check("hash", runFrom)
	check("hash-parallel", route(users, orders, 4))
	// now with indexes: selective queries probe them, wide ones still hash
	if err := users.CreateIndex(bg, "by_id", "id"); err != nil {
		t.Fatal(err)
	}
	if err := users.CreateIndex(bg, "by_country", "country"); err != nil {
		t.Fatal(err)
	}
	if err := users.CreateIndex(bg, "by_tags", "tags"); err != nil {
		t.Fatal(err)
	}
	if err := orders.CreateIndex(bg, "by_user", "user"); err != nil {
		t.Fatal(err)
	}
	before := db.Stats().IndexScans
	check("indexed", runFrom)
	if db.Stats().IndexScans == before {
		t.Fatal("no query used an index")
	}
	check("indexed-parallel", route(users, orders, 4))
	// under a snapshot, with the live data changed
	s, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 100; i++ {
		orders.InsertJSON([]byte(fmt.Sprintf("o%06d", i)), []byte(`{"id":-1,"user":0,"amount":1e9,"status":"new","tag":0}`))
	}
	su, _ := s.Collection("users")
	so, _ := s.Collection("orders")
	check("snapshot", route(su, so, 1))
	check("snapshot-parallel", route(su, so, 4))
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}

func TestLookupErrors(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c, _ := db.Collection("c")
	c.InsertJSON([]byte("1"), []byte(`{"a":1}`))
	q := `[{"$lookup":{"from":"missing","localField":"a","foreignField":"a","as":"x"}}]`
	if _, err := c.Aggregate(bg, []byte(q)); !errors.Is(err, ErrNoSuchCollection) {
		t.Fatalf("missing from: %v", err)
	}
	p, _ := ParsePipeline([]byte(`[{"$lookup":{"from":"c","localField":"a","foreignField":"a","as":"x"}}]`))
	if _, err := p.Run(bg, c.Source(nil, nil)); !errors.Is(err, ErrPipeline) {
		t.Fatalf("Run without a collection: %v", err)
	}
	for _, bad := range []string{
		`[{"$lookup":{"from":"c","localField":"a","as":"x"}}]`,
		`[{"$lookup":{"from":"c","pipeline":[],"as":"x"}}]`,
		`[{"$lookup":"c"}]`,
	} {
		if _, err := ParsePipeline([]byte(bad)); !errors.Is(err, ErrPipeline) {
			t.Fatalf("%s parsed: %v", bad, err)
		}
	}
	// self join through the default collection ("" is allowed as from)
	db.InsertJSON([]byte("1"), []byte(`{"a":1,"b":1}`))
	db.InsertJSON([]byte("2"), []byte(`{"a":1,"b":2}`))
	rows, err := db.Aggregate(bg, []byte(`[{"$lookup":{"from":"","localField":"a","foreignField":"a","as":"m"}},{"$unwind":"$m"},{"$count":"n"}]`))
	if err != nil || rows[0]["n"] != int64(4) {
		t.Fatalf("self join: %v %v", rows, err)
	}
	// a table larger than the row limit is refused
	p, _ = ParsePipeline([]byte(`[{"$lookup":{"from":"","localField":"a","foreignField":"a","as":"m"}},{"$unwind":"$m"},{"$count":"n"}]`))
	p.SetLimits(Limits{MaxGroups: 10, MaxRows: 1})
	if _, err := db.def.Run(bg, p); !errors.Is(err, ErrLimit) {
		t.Fatalf("limit: %v", err)
	}
}

func TestCollectionKeysSortInsideTheirRange(t *testing.T) {
	// the three-byte tag form must order after every single-byte one and
	// split back into the same id and key
	for _, id := range []uint32{0, 1, 253, 254, 255, 1000, 65535} {
		pfx := collPrefix(id)
		k := append(append([]byte(nil), pfx...), "key"...)
		gid, uk, ok := splitKey(k)
		if !ok || gid != id || string(uk) != "key" {
			t.Fatalf("id %d: split = %d %q %v", id, gid, uk, ok)
		}
		if end := prefixEnd(pfx); end != nil && bytes.Compare(k, end) >= 0 {
			t.Fatalf("id %d: key %x not below end %x", id, k, end)
		}
	}
	if bytes.Compare(collPrefix(253), collPrefix(254)) >= 0 || bytes.Compare(collPrefix(254), collPrefix(255)) >= 0 {
		t.Fatal("prefixes do not order by id")
	}
	if _, _, ok := splitKey([]byte{0, 'c'}); ok {
		t.Fatal("reserved key split as user key")
	}
	// many collections: ids past the single-byte range work end to end
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	var last *Collection
	for i := 0; i < 300; i++ {
		c, err := db.Collection(fmt.Sprintf("c%03d", i))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.InsertJSON([]byte("k"), []byte(fmt.Sprintf(`{"i":%d}`, i))); err != nil {
			t.Fatal(err)
		}
		last = c
	}
	if last.id != 300 || len(last.pfx) != 3 {
		t.Fatalf("collection 300: id %d prefix %x", last.id, last.pfx)
	}
	db.Close()
	db, err = Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if len(db.Collections()) != 300 {
		t.Fatalf("%d collections after reopen", len(db.Collections()))
	}
	for i := 0; i < 300; i++ {
		c, _ := db.Collection(fmt.Sprintf("c%03d", i))
		j, ok, err := c.FindJSON([]byte("k"))
		if err != nil || !ok {
			t.Fatalf("c%03d: %v %v", i, ok, err)
		}
		var m map[string]any
		json.Unmarshal(j, &m)
		if m["i"] != float64(i) {
			t.Fatalf("c%03d holds %s", i, j)
		}
		n := 0
		c.Scan(nil, nil, func(k, v []byte) bool { n++; return true })
		if n != 1 {
			t.Fatalf("c%03d scan saw %d keys", i, n)
		}
	}
}

func TestApproxCount(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sizes := map[string]int{"tiny": 3, "small": 900, "mid": 40_000, "big": 300_000}
	for name, n := range sizes {
		c, _ := db.Collection(name)
		b := c.NewBatch()
		for i := 0; i < n; i++ {
			b.InsertJSON([]byte(fmt.Sprintf("%08d", i)), []byte(`{"v":1,"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`))
			if b.Len() >= 5000 {
				if err := b.Commit(); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := b.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	db.Checkpoint()
	for name, n := range sizes {
		c, _ := db.Collection(name)
		got, err := c.approxCount()
		if err != nil {
			t.Fatal(err)
		}
		lo, hi := n/2, n*2
		if n < 1000 {
			lo, hi = 0, n+2000 // small ranges share leaves with neighbours
		}
		if got < lo || got > hi {
			t.Fatalf("%s: estimate %d for %d keys", name, got, n)
		}
		t.Logf("%s: %d keys, estimate %d", name, n, got)
	}
}
