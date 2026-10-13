package db

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestLogicalExportImport(t *testing.T) {
	original, err := Open(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	c := mustCollection(t, original, "orders")
	c.DefineComputedFields(bg, []byte(`{"total":{"$multiply":["$qty","$price"]}}`))
	c.InsertJSON([]byte{0, 255}, []byte(`{"customer":1,"qty":2,"price":5}`))
	c.CreateUniqueIndex(bg, "customer", "customer")
	_, err = original.CreateComputedCollection(bg, "totals", "orders", []byte(`[{"$group":{"_id":"$customer","total":{"$sum":"$total"}}}]`))
	if err != nil {
		t.Fatal(err)
	}
	original.Insert([]byte{255, 0}, []byte{0, 255, 1})
	var output bytes.Buffer
	if err = original.Export(bg, &output); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "imported")
	if err = Import(bg, bytes.NewReader(output.Bytes()), target); err != nil {
		t.Fatal(err)
	}
	imported, err := Open(target, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Close()
	raw, ok, err := imported.Find([]byte{255, 0}, nil)
	if err != nil || !ok || !bytes.Equal(raw, []byte{0, 255, 1}) {
		t.Fatal(raw, ok, err)
	}
	c = mustCollection(t, imported, "orders")
	if err = c.InsertJSON([]byte("duplicate"), []byte(`{"customer":1,"qty":1,"price":2}`)); !errors.Is(err, ErrDuplicateKey) {
		t.Fatal(err)
	}
	totals := mustCollection(t, imported, "totals")
	key, _ := ComputedGroupKey(1)
	raw, ok, err = totals.Find(key, nil)
	if err != nil || !ok || docField(raw, "total").i != 10 {
		t.Fatal(ok, err)
	}
	broken := filepath.Join(t.TempDir(), "broken")
	if err = Import(bg, bytes.NewReader(output.Bytes()[:output.Len()/2]), broken); err == nil {
		t.Fatal("truncated import accepted")
	}
	if _, err = Open(broken, Options{}); !errors.Is(err, ErrRestoreIncomplete) {
		t.Fatal(err)
	}
}
