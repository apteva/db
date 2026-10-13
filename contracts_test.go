package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func mustCollection(t *testing.T, db *DB, name string) *Collection {
	t.Helper()
	c, err := db.Collection(name)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestInsertAndReplaceContracts(t *testing.T) {
	for _, opts := range []Options{{}, {CacheBytes: 64 * PageSize, MemtableBytes: 128 << 10}} {
		db, err := Open(t.TempDir(), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err = db.Insert([]byte("k"), []byte("a")); err != nil {
			t.Fatal(err)
		}
		if err = db.Insert([]byte("k"), []byte("b")); !errors.Is(err, ErrKeyExists) {
			t.Fatal(err)
		}
		if err = db.Replace([]byte("missing"), []byte("b")); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err = db.Replace([]byte("k"), []byte("b")); err != nil {
			t.Fatal(err)
		}
		b := db.NewBatch()
		b.Insert([]byte("x"), []byte("1"))
		b.Insert([]byte("k"), []byte("2"))
		if err = b.Commit(); !errors.Is(err, ErrKeyExists) {
			t.Fatal(err)
		}
		if _, ok, _ := db.Find([]byte("x"), nil); ok {
			t.Fatal("failed batch partially applied")
		}
		b.Delete([]byte("k"))
		b.Insert([]byte("k"), []byte("3"))
		b.Replace([]byte("k"), []byte("4"))
		if err = b.Commit(); err != nil {
			t.Fatal(err)
		}
		v, _, _ := db.Find([]byte("k"), nil)
		if string(v) != "4" {
			t.Fatal(string(v))
		}
		var wg sync.WaitGroup
		winners := make(chan bool, 32)
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := db.Insert([]byte("race"), nil)
				winners <- err == nil
				if err != nil && !errors.Is(err, ErrKeyExists) {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		close(winners)
		n := 0
		for won := range winners {
			if won {
				n++
			}
		}
		if n != 1 {
			t.Fatal(n)
		}
	}
}

func TestScanAndCursorSnapshotContract(t *testing.T) {
	for _, opts := range []Options{{}, {CacheBytes: 64 * PageSize, MemtableBytes: 128 << 10}} {
		db, err := Open(t.TempDir(), opts)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		c := mustCollection(t, db, "rows")
		b := c.NewBatch()
		for i := 0; i < 3000; i++ {
			b.Upsert([]byte(fmt.Sprintf("%04d", i)), bytes.Repeat([]byte("a"), 100))
		}
		if err = b.Commit(); err != nil {
			t.Fatal(err)
		}
		cur := c.NewCursor()
		defer cur.Close()
		n := 0
		err = c.Scan(nil, nil, func(k, v []byte) bool {
			if n == 0 {
				bb := c.NewBatch()
				for i := 0; i < 3000; i++ {
					bb.Upsert([]byte(fmt.Sprintf("%04d", i)), []byte("b"))
				}
				bb.Upsert([]byte("new"), nil)
				if err := bb.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if len(v) != 100 {
				t.Fatalf("scan mixed versions at %s", k)
			}
			n++
			return true
		})
		if err != nil || n != 3000 {
			t.Fatal(n, err)
		}
		if err = db.DropCollection(bg, "rows"); err != nil {
			t.Fatal(err)
		}
		n = 0
		for ok := cur.First(); ok; ok = cur.Next() {
			if len(cur.Value()) != 100 {
				t.Fatal("cursor mixed versions")
			}
			n++
		}
		if cur.Err() != nil || n != 3000 {
			t.Fatal(n, cur.Err())
		}
		cur.Close()
		if db.Stats().Snapshots != 0 {
			t.Fatal("cursor leaked its view")
		}
		if _, _, err = c.Find([]byte("0000"), nil); !errors.Is(err, ErrNoSuchCollection) {
			t.Fatal(err)
		}
	}
}
func TestCancelledDropHasNoEffects(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := mustCollection(t, db, "rows")
	c.UpsertJSON([]byte("a"), []byte(`{"v":1}`))
	c.CreateIndex(bg, "v", "v")
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if err = c.DropIndex(ctx, "v"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = db.DropCollection(ctx, "rows"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(c.Indexes()) != 1 {
		t.Fatal("index lost")
	}
	if _, ok, err := c.Find([]byte("a"), nil); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err = db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}

func TestBatchExpansionLimitAndOptions(t *testing.T) {
	database, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.CreateIndex(bg, "v", "v"); err != nil {
		t.Fatal(err)
	}
	b := database.NewBatch()
	b.limit = 150
	if err = b.InsertJSON([]byte("a"), []byte(`{"v":[1,2,3,4,5,6,7,8]}`)); err != nil {
		t.Fatal(err)
	}
	if err = b.Commit(); !errors.Is(err, ErrTransactionTooLarge) {
		t.Fatal(err)
	}
	if _, ok, _ := database.Find([]byte("a"), nil); ok {
		t.Fatal("oversize expansion partially applied")
	}
	if err = database.DropIndex(bg, "v"); err != nil {
		t.Fatal(err)
	}
	if err = database.InsertJSON([]byte("a"), []byte(`{"v":"`+strings.Repeat("x", MaxKeySize)+`"}`)); err != nil {
		t.Fatal(err)
	}
	if err = database.CreateIndex(bg, "long", "v"); !errors.Is(err, ErrKeySize) {
		t.Fatal("oversize index key accepted", err)
	}
	for _, o := range []Options{{LogFlushInterval: -1}, {CacheBytes: -1}, {MemtableBytes: -2}, {SyncMode: 99}, {Parallelism: -1}} {
		if _, err = Open(t.TempDir(), o); !errors.Is(err, ErrOptions) {
			t.Fatal(o, err)
		}
	}
}
