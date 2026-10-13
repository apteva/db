package db

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func TestSecondaryIndexes(t *testing.T) {
	for _, opts := range []Options{{SyncMode: SyncOS}, {CacheBytes: 128 * PageSize, MemtableBytes: 256 << 10, SyncMode: SyncOS}} {
		dir := t.TempDir()
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		rng := rand.New(rand.NewSource(171))
		regions := []string{"north", "south", "east", "west"}
		type doc struct {
			region string
			amount float64
			tags   []string
		}
		docs := map[string]doc{}
		put := func(k string, d doc) {
			docs[k] = d
			tags := ""
			for i, tg := range d.tags {
				if i > 0 {
					tags += ","
				}
				tags += fmt.Sprintf("%q", tg)
			}
			j := fmt.Sprintf(`{"region":%q,"amount":%g,"tags":[%s],"addr":{"city":%q}}`, d.region, d.amount, tags, d.region+"-city")
			if err := db.UpsertJSON([]byte(k), []byte(j)); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 20_000; i++ {
			put(fmt.Sprintf("d%05d", i), doc{regions[rng.Intn(4)], float64(rng.Intn(100000)) / 100, []string{"t" + fmt.Sprint(rng.Intn(50)), "t" + fmt.Sprint(rng.Intn(50))}})
		}
		// keys may start with any byte; the reserved space is internal
		if err := db.Upsert([]byte{0, 'x'}, []byte("zero-byte key")); err != nil {
			t.Fatal(err)
		}
		if v, ok, _ := db.Find([]byte{0, 'x'}, nil); !ok || string(v) != "zero-byte key" {
			t.Fatal("zero-byte key not stored")
		}
		db.Delete([]byte{0, 'x'})
		db.Checkpoint() // the planner sizes the collection from the tree
		if err := db.CreateIndex(bg, "by_region", "region"); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateIndex(bg, "by_amount", "amount"); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateIndex(bg, "by_tag", "tags"); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateIndex(bg, "by_city", "addr.city"); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateIndex(bg, "by_region", "region"); !errors.Is(err, ErrIndexExists) {
			t.Fatal("duplicate index accepted")
		}
		if err := db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		// writes after creation maintain the entries: updates, deletes, batches
		i := 0
		nonDoc := map[string]bool{}
		for k, d := range docs {
			switch i % 5 {
			case 0:
				d.region = regions[(i/5)%4]
				d.amount = float64(rng.Intn(1000))
				d.tags = []string{"t1"}
				put(k, d)
			case 1:
				db.Delete([]byte(k))
				delete(docs, k)
			case 2:
				db.Upsert([]byte(k), []byte("not a document")) // loses its entries, stays a user key
				delete(docs, k)
				nonDoc[k] = true
			}
			i++
		}
		b := db.NewBatch()
		b.Delete([]byte("d00001"))
		delete(docs, "d00001")
		delete(nonDoc, "d00001")
		b.UpsertJSON([]byte("d00001"), []byte(`{"region":"east","amount":5,"tags":["t7"]}`)) // same key twice in one batch
		docs["d00001"] = doc{"east", 5, []string{"t7"}}
		if err := b.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		// queries through the index equal full scans
		count := func(pipe string) (int64, uint64) {
			before := db.Stats().IndexScans
			rows, err := db.Aggregate(bg, []byte(pipe))
			if err != nil {
				t.Fatalf("%s: %v", pipe, err)
			}
			n, _ := rows[0]["n"].(int64)
			return n, db.Stats().IndexScans - before
		}
		expect := func(f func(doc) bool) int64 {
			var n int64
			for _, d := range docs {
				if f(d) {
					n++
				}
			}
			return n
		}
		cases := []struct {
			pipe string
			f    func(doc) bool
		}{
			{`[{"$match": {"region": "east"}}, {"$count": "n"}]`, func(d doc) bool { return d.region == "east" }},
			{`[{"$match": {"region": {"$in": ["east", "west"]}}}, {"$count": "n"}]`, func(d doc) bool { return d.region == "east" || d.region == "west" }},
			{`[{"$match": {"amount": {"$gte": 900}}}, {"$count": "n"}]`, func(d doc) bool { return d.amount >= 900 }},
			{`[{"$match": {"amount": {"$lt": 10}}}, {"$count": "n"}]`, func(d doc) bool { return d.amount < 10 }},
			{`[{"$match": {"amount": {"$gt": 999.5}}}, {"$count": "n"}]`, func(d doc) bool { return d.amount > 999.5 }},
			{`[{"$match": {"tags": "t7"}}, {"$count": "n"}]`, func(d doc) bool {
				for _, tg := range d.tags {
					if tg == "t7" {
						return true
					}
				}
				return false
			}},
			{`[{"$match": {"addr.city": "north-city"}}, {"$count": "n"}]`, func(d doc) bool { return d.region == "north" }},
			{`[{"$match": {"region": "east", "amount": {"$lt": 50}}}, {"$count": "n"}]`, func(d doc) bool { return d.region == "east" && d.amount < 50 }},
		}
		for _, c := range cases {
			got, used := count(c.pipe)
			want := expect(c.f)
			if got != want {
				t.Fatalf("%s: got %d want %d", c.pipe, got, want)
			}
			// the planner reads through an index only while it names at
			// most an eighth of the collection (or indexCountMin documents);
			// a scan is faster past that
			if selective := want <= max(indexCountMin, int64(len(docs))/8); used != 1 && selective {
				t.Fatalf("%s: index not used", c.pipe)
			} else if used != 0 && !selective {
				t.Fatalf("%s: index used for %d of %d documents", c.pipe, want, len(docs))
			}
		}
		if _, used := count(`[{"$match": {"region": {"$ne": "east"}}}, {"$count": "n"}]`); used != 0 {
			t.Fatal("$ne should not use an index")
		}
		// snapshots see a consistent index; reopen keeps the catalog; drop removes entries
		s, _ := db.Snapshot()
		put("d99999", doc{"east", 1, nil})
		rows, _ := s.Aggregate(bg, []byte(`[{"$match": {"region": "east"}}, {"$count": "n"}]`))
		live, _ := db.Aggregate(bg, []byte(`[{"$match": {"region": "east"}}, {"$count": "n"}]`))
		if rows[0]["n"].(int64)+1 != live[0]["n"].(int64) {
			t.Fatalf("snapshot index view: %v vs %v", rows, live)
		}
		s.Close()
		db.Checkpoint()
		db.Close()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		if len(db.Indexes()) != 4 {
			t.Fatalf("catalog lost: %v", db.Indexes())
		}
		if err := db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		if err := db.Compact(bg); err != nil {
			t.Fatal(err)
		}
		if err := db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		if err := db.DropIndex(bg, "by_tag"); err != nil {
			t.Fatal(err)
		}
		n := 0
		db.Scan(nil, nil, func(k, v []byte) bool { n++; return true })
		if want := len(docs) + len(nonDoc); n != want { // docs already holds d99999
			t.Fatalf("public scan sees %d keys, want %d user keys and no index entries", n, want)
		}
		if err := db.DropIndex(bg, "by_tag"); !errors.Is(err, ErrNoSuchIndex) {
			t.Fatal("dropping twice should fail")
		}
		c := db.NewCursor()
		defer c.Close()
		if !c.First() || c.Key()[0] != 'd' {
			t.Fatalf("cursor first key %q: must be a user key, never an index entry", c.Key())
		}
		db.Close()
	}
}

func TestCreateIndexWithConcurrentWrites(t *testing.T) {
	db, err := Open(t.TempDir(), Options{CacheBytes: 128 * PageSize, MemtableBytes: 256 << 10, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 30_000; i++ {
		db.UpsertJSON([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf(`{"v":%d}`, i%100)))
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(3))
		for {
			select {
			case <-stop:
				return
			default:
			}
			i := rng.Intn(30_000)
			if rng.Intn(4) == 0 {
				db.Delete([]byte(fmt.Sprintf("k%05d", i)))
			} else {
				db.UpsertJSON([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf(`{"v":%d}`, rng.Intn(100))))
			}
		}
	}()
	if err := db.CreateIndex(bg, "by_v", "v"); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Aggregate(bg, []byte(`[{"$match": {"v": 42}}, {"$count": "n"}]`))
	if err != nil {
		t.Fatal(err)
	}
	var want int64
	db.Scan(nil, nil, func(k, v []byte) bool {
		if docField(v, "v").i == 42 {
			want++
		}
		return true
	})
	if rows[0]["n"].(int64) != want {
		t.Fatalf("indexed count %d, scan count %d", rows[0]["n"], want)
	}
}
