package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
)

func TestUniqueIndexTransactionsAndRecovery(t *testing.T) {
	for _, opts := range []Options{{Sync: true, SyncMode: SyncOS}, {Sync: true, SyncMode: SyncOS, CacheBytes: 64 * PageSize, MemtableBytes: 128 << 10}} {
		dir := t.TempDir()
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		c := mustCollection(t, db, "users")
		c.InsertJSON([]byte("a"), []byte(`{"email":"a","tags":["x","x"]}`))
		c.InsertJSON([]byte("b"), []byte(`{"email":"b","tags":["y"]}`))
		if err = c.CreateUniqueIndex(bg, "email", "email"); err != nil {
			t.Fatal(err)
		}
		if err = c.CreateUniqueIndex(bg, "tags", "tags"); err != nil {
			t.Fatal(err)
		}
		if err = c.UpsertJSON([]byte("c"), []byte(`{"email":"a"}`)); !errors.Is(err, ErrDuplicateKey) {
			t.Fatal(err)
		}
		if _, ok, _ := c.Find([]byte("c"), nil); ok {
			t.Fatal("duplicate leaked")
		}
		b := c.NewBatch()
		b.UpsertJSON([]byte("a"), []byte(`{"email":"b","tags":["y"]}`))
		b.UpsertJSON([]byte("b"), []byte(`{"email":"a","tags":["x"]}`))
		if err = b.Commit(); err != nil {
			t.Fatal("swap", err)
		}
		b.UpsertJSON([]byte("c"), []byte(`{"email":"z"}`))
		b.UpsertJSON([]byte("d"), []byte(`{"email":"z"}`))
		if err = b.Commit(); !errors.Is(err, ErrDuplicateKey) {
			t.Fatal(err)
		}
		if _, ok, _ := c.Find([]byte("c"), nil); ok {
			t.Fatal("batch partially applied")
		}
		b.UpsertJSON([]byte("c"), []byte(`{"email":"a"}`))
		b.Delete([]byte("b"))
		if err = b.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		db.crash()
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		c = mustCollection(t, db, "users")
		if info := c.ListIndexes(); len(info) != 2 || !info[0].Unique {
			t.Fatal(info)
		}
		if err = c.InsertJSON([]byte("d"), []byte(`{"email":"a"}`)); !errors.Is(err, ErrDuplicateKey) {
			t.Fatal("recovered uniqueness", err)
		}
		if err = db.Compact(bg); err != nil {
			t.Fatal(err)
		}
		backup := t.TempDir()
		if err = db.Backup(bg, backup); err != nil {
			t.Fatal(err)
		}
		db.Close()
		restored, err := Open(backup, opts)
		if err != nil {
			t.Fatal(err)
		}
		c = mustCollection(t, restored, "users")
		if err = c.InsertJSON([]byte("d"), []byte(`{"email":"a"}`)); !errors.Is(err, ErrDuplicateKey) {
			t.Fatal("backup uniqueness", err)
		}
		restored.Close()
	}
}
func TestUniqueIndexBuildFailsIntact(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.InsertJSON([]byte("a"), []byte(`{"v":1}`))
	db.InsertJSON([]byte("b"), []byte(`{"v":1}`))
	if err = db.CreateUniqueIndex(bg, "v", "v"); !errors.Is(err, ErrDuplicateKey) {
		t.Fatal(err)
	}
	if len(db.Indexes()) != 0 {
		t.Fatal("failed build visible")
	}
	if err = db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}
func TestUniqueIndexConcurrentWriters(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.CreateUniqueIndex(bg, "v", "v"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wins := make(chan bool, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := db.InsertJSON([]byte(fmt.Sprint(i)), []byte(`{"v":1}`))
			wins <- err == nil
			if err != nil && !errors.Is(err, ErrDuplicateKey) {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	n := 0
	for win := range wins {
		if win {
			n++
		}
	}
	if n != 1 {
		t.Fatal(n)
	}
}

func TestUniqueAndLegacySignedZero(t *testing.T) {
	database, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var builder DocBuilder
	builder.Float("v", math.Copysign(0, -1))
	negative := append([]byte(nil), builder.Bytes()...)
	if err = database.Insert([]byte("a"), negative); err != nil {
		t.Fatal(err)
	}
	if err = database.CreateUniqueIndex(bg, "v", "v"); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertJSON([]byte("b"), []byte(`{"v":0}`)); !errors.Is(err, ErrDuplicateKey) {
		t.Fatal(err)
	}
	if err = database.DropIndex(bg, "v"); err != nil {
		t.Fatal(err)
	}
	if err = database.CreateIndex(bg, "v", "v"); err != nil {
		t.Fatal(err)
	}
	d := database.indexDefs()[0]
	// Simulate a persisted format-6 index rather than inserting an old key into
	// a new exact index. The legacy definition and backfill must agree.
	exactKey := exactEntryKey(d, value{k: kInt, i: 0}, []byte("a"))
	d.Encoding = 0
	database.idx.mu.Lock()
	database.idx.defs[0] = d
	database.idx.mu.Unlock()
	b := database.NewBatch()
	b.internal = true
	b.add(exactKey, nil, true)
	meta, _ := json.Marshal(d)
	b.add(catalogKey(0, d.Name), meta, false)
	b.add(indexEntryKey(d.ID, value{k: kInt, i: 0}, []byte("a")), nil, true)
	b.add(legacyZeroEntryKey(d.ID, []byte("a")), nil, false)
	if err = b.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = database.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	for _, filter := range []string{`{"v":0}`, `{"v":{"$gte":0}}`, `{"v":{"$lte":0}}`} {
		p, _ := ParsePipeline([]byte(`[{"$match":` + filter + `}]`))
		pl := p.plan()
		rs, ok := indexRangesFor(d, pl.preds[0])
		if !ok {
			t.Fatal(filter)
		}
		n := 0
		for _, r := range rs {
			database.scan(view{database, nil}, r.lo, r.hi, func(_, _ []byte) bool { n++; return true })
		}
		if n != 1 {
			t.Fatal(filter, n)
		}
	}
	if err = database.ReplaceJSON([]byte("a"), []byte(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	if err = database.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}

func TestUniqueLargeIntegers(t *testing.T) {
	d, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err = d.CreateUniqueIndex(bg, "v", "v"); err != nil {
		t.Fatal(err)
	}
	for i, n := range []int64{1 << 53, (1 << 53) + 1, math.MaxInt64 - 1, math.MaxInt64} {
		var b DocBuilder
		b.Int("v", n)
		if err = d.Insert([]byte(fmt.Sprint(i)), b.Bytes()); err != nil {
			t.Fatal(n, err)
		}
	}
	var b DocBuilder
	b.Int("v", (1<<53)+1)
	if err = d.Insert([]byte("duplicate"), b.Bytes()); !errors.Is(err, ErrDuplicateKey) {
		t.Fatal(err)
	}
	if err = d.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}
