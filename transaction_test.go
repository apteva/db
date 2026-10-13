package db

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestTransactionAtomicReadModifyWrite(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	accounts := mustCollection(t, db, "accounts")
	events := mustCollection(t, db, "events")
	accounts.Insert([]byte("balance"), []byte("0"))
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				for {
					err := db.Update(bg, func(tx *Tx) error {
						c, _ := tx.Collection("accounts")
						e, _ := tx.Collection("events")
						v, _, err := c.Find([]byte("balance"), nil)
						if err != nil {
							return err
						}
						var n int
						fmt.Sscan(string(v), &n)
						if err = c.Replace([]byte("balance"), []byte(fmt.Sprint(n+1))); err != nil {
							return err
						}
						return e.Insert([]byte(fmt.Sprintf("%d-%d", w, i)), nil)
					})
					if errors.Is(err, ErrConflict) {
						continue
					}
					if err != nil {
						t.Error(err)
					}
					break
				}
			}
		}(w)
	}
	wg.Wait()
	v, _, err := accounts.Find([]byte("balance"), nil)
	if err != nil || string(v) != "320" {
		t.Fatal(string(v), err)
	}
	n := 0
	events.Scan(nil, nil, func(_, _ []byte) bool { n++; return true })
	if n != 320 {
		t.Fatal(n)
	}
	if db.Stats().Snapshots != 0 {
		t.Fatal("leaked transactions")
	}
}
func TestTransactionReadYourWritesAndRollback(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.Upsert([]byte("a"), []byte("old"))
	db.Upsert([]byte("c"), nil)
	tx, err := db.Begin(bg)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	tx.Replace([]byte("a"), []byte("new"))
	tx.Insert([]byte("b"), []byte("second"))
	tx.Delete([]byte("c"))
	v, ok, err := tx.Find([]byte("a"), nil)
	if err != nil || !ok || string(v) != "new" {
		t.Fatal(string(v), ok, err)
	}
	keys := ""
	err = tx.Scan(nil, nil, func(k, _ []byte) bool { keys += string(k); return true })
	if err != nil || keys != "ab" {
		t.Fatal(keys, err)
	}
	tx.Rollback()
	v, _, _ = db.Find([]byte("a"), nil)
	if string(v) != "old" {
		t.Fatal("rollback wrote")
	}
	ctx, cancel := context.WithCancel(bg)
	tx, _ = db.Begin(ctx)
	tx.Insert([]byte("x"), nil)
	cancel()
	if err = tx.Commit(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	tx, _ = db.Begin(bg)
	tx.Insert([]byte("x"), nil)
	db.Upsert([]byte("outside"), nil)
	if err = tx.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, ok, _ = db.Find([]byte("x"), nil); ok {
		t.Fatal("failed tx wrote")
	}
}
func TestTransactionComputedReadYourWrites(t *testing.T) {
	db, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	src := mustCollection(t, db, "orders")
	if err = src.DefineComputedFields(bg, []byte(`{"total":{"$multiply":["$qty","$price"]}}`)); err != nil {
		t.Fatal(err)
	}
	_, err = db.CreateComputedCollection(bg, "totals", "orders", []byte(`[{"$group":{"_id":"$customer","total":{"$sum":"$total"}}}]`))
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := db.Begin(bg)
	defer tx.Rollback()
	s, _ := tx.Collection("orders")
	d, _ := tx.Collection("totals")
	if err = s.UpsertJSON([]byte("a"), []byte(`{"customer":1,"qty":2,"price":5}`)); err != nil {
		t.Fatal(err)
	}
	v, _, err := s.Find([]byte("a"), nil)
	if err != nil || docField(v, "total").i != 10 {
		t.Fatal(err)
	}
	n := 0
	err = d.Scan(nil, nil, func(_, v []byte) bool {
		if docField(v, "total").i != 10 {
			t.Error("derived stale")
		}
		n++
		return true
	})
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
