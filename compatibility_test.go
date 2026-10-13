package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCompatibilityExactLookupLegacyAndNative(t *testing.T) {
	for _, mode := range []string{"hash", "legacy", "exact"} {
		t.Run(mode, func(t *testing.T) {
			d := compatDB(t)
			parents, err := d.Collection("parents")
			mustCompat(t, err)
			foreign, err := d.Collection("foreign")
			mustCompat(t, err)
			values := []any{int64(9007199254740992), int64(9007199254740993), float64(9007199254740992), int64(0)}
			for i, v := range values {
				mustCompat(t, parents.InsertDocument([]byte(fmt.Sprint(i)), map[string]any{"name": fmt.Sprint(i), "fk": v}))
				if i == 3 {
					var b DocBuilder
					b.Float("fk", math.Copysign(0, -1))
					mustCompat(t, foreign.Insert([]byte(fmt.Sprint(i)), b.Bytes()))
				} else {
					mustCompat(t, foreign.InsertDocument([]byte(fmt.Sprint(i)), map[string]any{"fk": v}))
				}
			}
			for i := 0; i < 300; i++ {
				mustCompat(t, foreign.InsertJSON([]byte(fmt.Sprint(i+10)), []byte(`{"fk":1}`)))
			}
			if mode == "exact" {
				mustCompat(t, foreign.CreateIndex(bg, "fk", "fk"))
			}
			if mode == "legacy" {
				def := indexDef{ID: 1, Coll: foreign.id, Name: "fk", Field: "fk"}
				meta, _ := json.Marshal(def)
				b := d.NewBatch()
				b.internal = true
				mustCompat(t, b.add(catalogKey(foreign.id, "fk"), meta, false))
				mustCompat(t, foreign.Scan(nil, nil, func(pk, doc []byte) bool {
					indexedValues(doc, "fk", func(v value) {
						key := indexEntryKey(def.ID, v, pk)
						if v.isNum() && v.num() == 0 {
							key = legacyZeroEntryKey(def.ID, pk)
						}
						mustCompat(t, b.add(key, nil, false))
					})
					return true
				}))
				mustCompat(t, b.Commit())
				d.idx.mu.Lock()
				d.idx.defs = append(d.idx.defs, def)
				d.idx.nextID = 2
				d.idx.mu.Unlock()
			}
			p, err := ParsePipeline([]byte(`[{"$lookup":{"from":"foreign","localField":"fk","foreignField":"fk","as":"joined"}},{"$unwind":"$joined"},{"$group":{"_id":"$name","n":{"$count":{}}}}]`))
			mustCompat(t, err)
			got, err := parents.RunParallel(bg, p, 3)
			mustCompat(t, err)
			counts := map[string]int64{}
			for _, row := range got {
				counts[row["_id"].(string)] = row["n"].(int64)
			}
			if !reflect.DeepEqual(counts, map[string]int64{"0": 2, "1": 1, "2": 2, "3": 1}) {
				t.Fatal(counts)
			}
		})
	}
}

func TestCompatibilityExactIntegerFilter(t *testing.T) {
	d, err := Open(t.TempDir(), DurableOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err := d.Collection("numbers")
	if err != nil {
		t.Fatal(err)
	}
	for _, indexed := range []bool{false, true} {
		if indexed {
			if err := c.CreateIndex(context.Background(), "n", "n"); err != nil {
				t.Fatal(err)
			}
		}
		for _, doc := range []struct{ key, json string }{{"a", `{"n":9007199254740992}`}, {"b", `{"n":9007199254740993}`}} {
			if err := c.UpsertJSON([]byte(doc.key), []byte(doc.json)); err != nil {
				t.Fatal(err)
			}
		}
		var keys []string
		err := c.Query(context.Background(), []byte(`{"n":9007199254740993}`), QueryOptions{}, func(k, _ []byte) bool { keys = append(keys, string(k)); return true })
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(keys, []string{"b"}) {
			t.Errorf("indexed=%v: got %v, want [b]", indexed, keys)
		}
	}
}

func TestCompatibilityExactTypedMapDecode(t *testing.T) {
	encoded, err := EncodeJSON(nil, []byte(`{"n":9007199254740993,"nested":{"n":9007199254740993},"arr":[9007199254740993]}`))
	mustCompat(t, err)
	var got map[string]any
	mustCompat(t, Decode(encoded, &got))
	if got["n"] != int64(9007199254740993) || got["nested"].(map[string]any)["n"] != int64(9007199254740993) || got["arr"].([]any)[0] != int64(9007199254740993) {
		t.Fatal(got)
	}
	var typed struct {
		N any `json:"n"`
	}
	mustCompat(t, Decode(encoded, &typed))
	if typed.N != json.Number("9007199254740993") {
		t.Fatal(typed)
	}
}

func TestCompatibilityLargeDocumentRoundTrip(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, DurableOptions())
	if err != nil {
		t.Fatal(err)
	}
	c, err := d.Collection("docs")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"a": strings.Repeat("a", 40000), "b": strings.Repeat("b", 40000), "z": int64(7)}
	if err = c.InsertDocument([]byte("large"), want); err != nil {
		t.Fatal(err)
	}
	if err = d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = Open(dir, DurableOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	c, err = d.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	raw, found, err := c.Find([]byte("large"), nil)
	if err != nil || !found {
		t.Fatalf("find: %v %v", found, err)
	}
	got, err := DecodeDocument(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("large document did not round trip")
	}
}

func TestCompatibilityRecoverWrappedAlphaDirectory(t *testing.T) {
	var b DocBuilder
	b.String("a", strings.Repeat("a", 40000))
	b.String("b", strings.Repeat("b", 40000))
	b.Int("z", 7)
	wide := b.Bytes()
	n, dir, ok := docDir(wide)
	if !ok {
		t.Fatal("wide encode")
	}
	legacy := append([]byte(nil), wide[:docContentEnd(wide, n, dir)]...)
	for i := 0; i < n; i++ {
		legacy = le.AppendUint16(legacy, uint16(docOffset(dir, n, i)))
	}
	legacy = append(legacy, byte(n))
	got, err := DecodeDocument(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got["z"] != int64(7) || got["b"] != strings.Repeat("b", 40000) {
		t.Fatal("legacy directory reconstruction lost data")
	}
	legacy[len(legacy)-3] ^= 1
	if _, err := DecodeDocument(legacy); err == nil {
		t.Fatal("guessed past a corrupted legacy offset")
	}
}

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
	if err != nil || v != FormatVersion {
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
