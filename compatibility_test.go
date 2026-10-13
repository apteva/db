package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestActualFormat5FixtureUpgrade(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"data", "wal-00000001"} {
		b, err := os.ReadFile(filepath.Join("testdata", "format5", name))
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	v, err := FormatVersionOf(dir)
	if err != nil || v != 5 {
		t.Fatal(v, err)
	}
	d, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	v, err = FormatVersionOf(dir)
	if err != nil || v != 6 {
		t.Fatal(v, err)
	}
	orders := mustCollection(t, d, "orders")
	row, ok, err := orders.Find([]byte("a"), nil)
	if err != nil || !ok || docField(row, "total").i != 10 {
		t.Fatal(ok, err)
	}
	if err = orders.CreateUniqueIndex(bg, "unique_customer", "customer"); err != nil {
		t.Fatal(err)
	}
	if err = d.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	totals := mustCollection(t, d, "totals")
	key, err := ComputedGroupKey(1)
	if err != nil {
		t.Fatal(err)
	}
	row, ok, err = totals.Find(key, nil)
	if err != nil || !ok || docField(row, "total").i != 10 {
		t.Fatal(ok, err)
	}
}
