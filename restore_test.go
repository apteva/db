package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreMixedDatabase(t *testing.T) {
	src, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	orders := mustCollection(t, src, "orders")
	orders.DefineComputedFields(bg, []byte(`{"total":{"$multiply":["$qty","$price"]}}`))
	orders.InsertJSON([]byte("a"), []byte(`{"customer":1,"qty":2,"price":5}`))
	orders.CreateUniqueIndex(bg, "customer", "customer")
	_, err = src.CreateComputedCollection(bg, "totals", "orders", []byte(`[{"$group":{"_id":"$customer","total":{"$sum":"$total"}}}]`))
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err = src.Backup(bg, backup); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "restored")
	if err = Restore(bg, backup, dst); err != nil {
		t.Fatal(err)
	}
	if err = Restore(bg, backup, dst); !errors.Is(err, ErrDestinationExists) {
		t.Fatal(err)
	}
	got, err := Open(dst, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	c := mustCollection(t, got, "orders")
	if err = c.InsertJSON([]byte("b"), []byte(`{"customer":1,"qty":1,"price":1}`)); !errors.Is(err, ErrDuplicateKey) {
		t.Fatal(err)
	}
	if info, err := c.ComputedFields(); err != nil || len(info) != 1 {
		t.Fatal(info, err)
	}
	if err = got.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	incomplete := t.TempDir()
	os.WriteFile(filepath.Join(incomplete, restoreMarker), nil, 0600)
	if _, err = Open(incomplete, Options{}); !errors.Is(err, ErrRestoreIncomplete) {
		t.Fatal(err)
	}
}
