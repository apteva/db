package aptevadb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
)

// TestOrphanEntriesSwept: entries of an index whose catalog entry never
// made it (a build interrupted by a crash) are removed at the next open,
// and the id they used is safe to reuse.
func TestOrphanEntriesSwept(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := db.Collection("c")
	for i := 0; i < 2000; i++ {
		c.InsertJSON([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf(`{"g":%d}`, i%13)))
	}
	if err := c.CreateIndex(bg, "real", "g"); err != nil {
		t.Fatal(err)
	}
	// plant entries under the id the next index would get, as a crashed
	// build leaves them, plus some under a higher id
	orphan := db.idx.nextID
	b := db.NewBatch()
	b.internal = true
	for i := 0; i < 500; i++ {
		b.add(indexEntryKey(orphan, value{k: kInt, i: int64(i)}, []byte(fmt.Sprintf("k%05d", i))), nil, false)
		b.add(indexEntryKey(orphan+7, value{k: kString, s: []byte("x")}, []byte(fmt.Sprintf("k%05d", i))), nil, false)
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	n := 0
	lo := []byte{reservedPrefix, 'i'}
	db.scan(view{db, nil}, lo, prefixEnd(lo), func(k, _ []byte) bool {
		if binary.BigEndian.Uint32(k[2:]) != 1 {
			n++
		}
		return true
	})
	if n != 0 {
		t.Fatalf("%d orphan entries survived the open", n)
	}
	c, _ = db.Collection("c")
	if err := c.CreateIndex(bg, "second", "g"); err != nil {
		t.Fatal(err)
	}
	if err := db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	rows, err := c.Aggregate(bg, []byte(`[{"$match":{"g":3}},{"$count":"n"}]`))
	if err != nil || rows[0]["n"] != int64(154) {
		t.Fatalf("rows %v err %v", rows, err)
	}
}

// TestBulkBuildUnderWrites: a build with concurrent inserts, updates and
// deletes of both indexed and unrelated keys ends verified, with a
// compaction refused meanwhile and allowed afterwards.
func TestBulkBuildUnderWrites(t *testing.T) {
	for _, opts := range []Options{{SyncMode: SyncOS}, {CacheBytes: 256 * PageSize, MemtableBytes: 512 << 10, SyncMode: SyncOS}} {
		dir := t.TempDir()
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		c, _ := db.Collection("c")
		rng := rand.New(rand.NewSource(11))
		for i := 0; i < 60_000; i++ {
			c.InsertJSON([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf(`{"v":%d,"tags":[%d,%d]}`, rng.Intn(5000), rng.Intn(20), rng.Intn(20))))
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var compactErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(12))
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := []byte(fmt.Sprintf("k%06d", r.Intn(60_000)))
				switch i % 3 {
				case 0:
					c.InsertJSON(k, []byte(fmt.Sprintf(`{"v":%d,"tags":[%d]}`, r.Intn(5000), r.Intn(20))))
				case 1:
					c.Delete(k)
				default:
					c.InsertJSON([]byte(fmt.Sprintf("n%06d", i)), []byte(`{"v":1}`))
				}
				if i == 200 {
					// refused while the build runs (its snapshot, or its unlogged inserts)
					if err := db.Compact(bg); err != nil && !errors.Is(err, ErrIndexBuilding) && !errors.Is(err, ErrSnapshotsOpen) {
						compactErr = err
					}
				}
			}
		}()
		if err := c.CreateIndex(bg, "by_v", "v"); err != nil {
			t.Fatal(err)
		}
		if err := c.CreateIndex(bg, "by_tag", "tags"); err != nil {
			t.Fatal(err)
		}
		close(stop)
		wg.Wait()
		if compactErr != nil {
			t.Fatal(compactErr)
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
		// the index answers what a scan answers
		want, _ := c.Aggregate(bg, []byte(`[{"$match":{"v":{"$lt":100}}},{"$count":"n"}]`))
		var n int64
		c.Scan(nil, nil, func(_, doc []byte) bool {
			if v := docField(doc, "v"); v.k == kInt && v.i < 100 {
				n++
			}
			return true
		})
		if want[0]["n"] != n {
			t.Fatalf("index says %v, scan says %d", want, n)
		}
		db.Close()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		if len(db.Indexes()) != 0 {
			t.Fatal("default collection has indexes")
		}
		c, _ = db.Collection("c")
		if got := c.Indexes(); len(got) != 2 {
			t.Fatalf("indexes after reopen: %v", got)
		}
		db.Close()
	}
}

// TestReorderMatchesWritten: a reordered join gives the same rows as the
// pipeline run as written, whichever side is selective.
func TestReorderMatchesWritten(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	users, orders, _, _ := loadJoinData(t, db, 2000, 30_000, 21)
	for _, ix := range []struct {
		c           *Collection
		name, field string
	}{{users, "by_id", "id"}, {users, "by_country", "country"}, {users, "by_tags", "tags"}, {orders, "by_user", "user"}, {orders, "by_status", "status"}} {
		if err := ix.c.CreateIndex(bg, ix.name, ix.field); err != nil {
			t.Fatal(err)
		}
	}
	queries := []string{
		// orders side unselective, users side selective: reordered
		`[{"$match":{"status":"shipped"}},{"$lookup":{"from":"users","localField":"user","foreignField":"id","as":"u"}},{"$unwind":"$u"},{"$match":{"u.country":"FR"}},{"$group":{"_id":null,"n":{"$sum":1},"avg":{"$avg":"$amount"}}}]`,
		`[{"$lookup":{"from":"users","localField":"user","foreignField":"id","as":"u"}},{"$unwind":"$u"},{"$match":{"u.country":{"$in":["FR","DE"]},"amount":{"$gt":50}}},{"$group":{"_id":"$u.country","n":{"$sum":1},"total":{"$sum":"$amount"},"maxTag":{"$max":"$tag"}}},{"$sort":{"_id":1}}]`,
		`[{"$lookup":{"from":"users","localField":"user","foreignField":"id","as":"u"}},{"$unwind":"$u"},{"$match":{"u.country":"JP"}},{"$group":{"_id":{"c":"$u.country","s":"$status"},"n":{"$sum":1},"first":{"$first":"$id"}}},{"$sort":{"n":-1,"_id":1}}]`,
		// array on the foreign side, matched through the users tags index
		`[{"$lookup":{"from":"users","localField":"tag","foreignField":"tags","as":"u"}},{"$unwind":"$u"},{"$match":{"u.country":"US"}},{"$group":{"_id":"$tag","n":{"$sum":1}}},{"$sort":{"_id":1}}]`,
		// left join and $count never reorder; they must still agree
		`[{"$match":{"status":"new"}},{"$lookup":{"from":"users","localField":"user","foreignField":"id","as":"u"}},{"$unwind":{"path":"$u","preserveNullAndEmptyArrays":true}},{"$match":{"u.country":"FR"}},{"$count":"n"}]`,
	}
	for _, q := range queries {
		db.noReorder = true
		want, err := orders.Aggregate(bg, []byte(q))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		db.noReorder = false
		got, err := orders.Aggregate(bg, []byte(q))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if !sameRows(canonRows(want), canonRows(got)) {
			t.Fatalf("%s:\nwritten:   %v\nreordered: %v", q, want, got)
		}
		got1, err := orders.AggregateParallel(bg, []byte(q), 1)
		if err != nil || !sameRows(canonRows(want), canonRows(got1)) {
			t.Fatalf("%s single core: %v %v", q, got1, err)
		}
	}
	// the first query does reorder: users' country index is the source
	db.noReorder = false
	p, _ := ParsePipeline([]byte(queries[0]))
	pl := p.plan()
	if _, from, ok := orders.reorder(p, &pl); !ok || from.name != "users" {
		t.Fatalf("query 1 not reordered (ok=%v)", ok)
	}
	// group keys of the reordered pipeline keep their names
	rows, _ := orders.Aggregate(bg, []byte(queries[1]))
	names := []string{}
	for k := range rows[0] {
		names = append(names, k)
	}
	sort.Strings(names)
	if fmt.Sprint(names) != "[_id maxTag n total]" {
		t.Fatalf("row names %v", names)
	}
}
