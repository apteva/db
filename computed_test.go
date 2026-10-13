package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func computedDB(t *testing.T, opts Options) *DB {
	t.Helper()
	db, err := Open(t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func computedRow(t *testing.T, find func([]byte, []byte) ([]byte, bool, error), key string) map[string]any {
	t.Helper()
	v, ok, err := find([]byte(key), nil)
	if err != nil || !ok {
		t.Fatalf("find %s: %v %v", key, ok, err)
	}
	row, err := DecodeDocument(v)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestComputedFieldsExpressions(t *testing.T) {
	db := computedDB(t, Options{})
	defs := []byte(`{
 "total":{"$multiply":["$qty","$price"]},
 "net":{"$subtract":["$total",{"$ifNull":["$discount",0]}]},
 "email_key":{"$lower":"$email"},
 "label":{"$concat":[{"$upper":"$addr.city"},"!"]},
 "ratio":{"$divide":["$net",2]},
 "constant":{"$literal":"$literal"},
 "large":{"$add":[9007199254740993,1]},
 "fallback":{"$ifNull":["$missing",true]},
 "absent":"$missing",
 "null":{"$ifNull":["$nil",false]}
 }`)
	if err := db.DefineComputedFields(bg, defs); err != nil {
		t.Fatal(err)
	}
	if err := db.UpsertJSON([]byte("k"), []byte(`{"qty":3,"price":7,"discount":2,"email":"AÄ@EXAMPLE.COM","addr":{"city":"Paris"},"total":-1,"net":-9,"nil":null}`)); err != nil {
		t.Fatal(err)
	}
	row := computedRow(t, db.Find, "k")
	for k, want := range map[string]any{"total": int64(21), "net": int64(19), "email_key": "aÄ@example.com", "label": "PARIS!", "ratio": float64(9.5), "constant": "$literal", "large": int64(9007199254740994), "fallback": true, "absent": nil, "null": false} {
		if !reflect.DeepEqual(row[k], want) {
			t.Errorf("%s=%#v want %#v", k, row[k], want)
		}
	}
	reader, err := db.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if got := computedRow(t, reader.Find, "k")["total"]; got != int64(21) {
		t.Fatal(got)
	}
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	if err := db.UpsertJSON([]byte("k"), []byte(`{"qty":2,"price":5}`)); err != nil {
		t.Fatal(err)
	}
	if computedRow(t, snap.Find, "k")["net"] != int64(19) {
		t.Fatal("snapshot changed")
	}
	row = computedRow(t, db.Find, "k")
	if row["total"] != int64(10) || row["net"] != int64(10) || row["email_key"] != nil {
		t.Fatal(row)
	}
	returned, err := db.ComputedFields()
	if err != nil {
		t.Fatal(err)
	}
	returned["total"][0] = 'X'
	delete(returned, "net")
	again, err := db.ComputedFields()
	if err != nil || !json.Valid(again["total"]) || len(again) != 10 {
		t.Fatalf("mutable definitions: %v %v", again, err)
	}
	if err := db.DefineComputedFields(bg, []byte("{}")); !errors.Is(err, ErrComputedNonEmpty) {
		t.Fatal(err)
	}
}

func TestComputedFieldsValidation(t *testing.T) {
	invalid := []string{
		`{`, `{"x":`, `{"x":1,}`,
		`[]`, `null`, `{"x":1,"x":2}`, `{"x":{"$add":[1,{"$add":[2,3]," $add":4}]}}`,
		`{"x":"$x"}`, `{"x":"$y","y":"$x"}`, `{"x.y":1}`, `{"$x":1}`, `{"":1}`,
		`{"x":{"$random":1}}`, `{"x":{"$add":[1]}}`, `{"x":{"$lower":["$s"]}}`,
		`{"x":{"$literal":{}}}`, `{"x":"$a..b"}`, `{"x":1,"y":"$x.foo"}`,
		`{"x":9223372036854775808}`, `{"x":-9223372036854775809}`, `{"x":1e400}`, `{"x":1}{}`,
	}
	deep := "1"
	for i := 0; i < 35; i++ {
		deep = `{"$ifNull":[` + deep + `,0]}`
	}
	invalid = append(invalid, `{"x":`+deep+`}`)
	fields := map[string]int{}
	for i := 0; i < 65; i++ {
		fields[fmt.Sprintf("f%d", i)] = i
	}
	many, _ := json.Marshal(fields)
	invalid = append(invalid, string(many))
	for _, defs := range invalid {
		if _, err := compileComputedFields([]byte(defs)); !errors.Is(err, ErrComputedField) {
			t.Errorf("accepted %s: %v", defs, err)
		}
	}
	db := computedDB(t, Options{})
	if err := db.DefineComputedFields(bg, []byte(`{"total":{"$multiply":["$a","$b"]}}`)); err != nil {
		t.Fatal(err)
	}
	var d DocBuilder
	d.Int("a", 3)
	d.Int("b", 4)
	good := append([]byte(nil), d.Bytes()...)
	if err := db.Upsert([]byte("k"), good); err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{[]byte("opaque"), {1}, append([]byte(nil), good...)}
	cases[2][len(cases[2])-3] = 1 // corrupt first directory offset
	d.Reset()
	d.Int("a", 1)
	d.Int("a", 2)
	cases = append(cases, append([]byte(nil), d.Bytes()...))
	d.Reset()
	d.Float("unused", math.NaN())
	cases = append(cases, append([]byte(nil), d.Bytes()...))
	d.Reset()
	d.Int("a", math.MaxInt64)
	d.Int("b", 2)
	cases = append(cases, append([]byte(nil), d.Bytes()...))
	d.Reset()
	d.Int("a", math.MinInt64)
	d.Int("b", -1)
	cases = append(cases, append([]byte(nil), d.Bytes()...))
	d.Reset()
	d.String("a", "3")
	d.Int("b", 4)
	cases = append(cases, append([]byte(nil), d.Bytes()...))
	d.Reset()
	d.Float("a", math.MaxFloat64)
	d.Float("b", 2)
	cases = append(cases, append([]byte(nil), d.Bytes()...))
	for _, bad := range cases {
		if err := db.Upsert([]byte("k"), bad); !errors.Is(err, ErrComputedField) {
			t.Errorf("accepted invalid input: %v", err)
		}
		if got := computedRow(t, db.Find, "k")["total"]; got != int64(12) {
			t.Fatal("failed write changed value", got)
		}
	}
	for _, defs := range []string{`{"x":{"$divide":[1,0]}}`, `{"x":{"$add":[9223372036854775807,1]}}`, `{"x":{"$subtract":[-9223372036854775808,1]}}`, `{"x":"$a"}`} {
		p, err := compileComputedFields([]byte(defs))
		if err != nil {
			t.Fatal(err)
		}
		d.Reset()
		var nested DocBuilder
		nested.Int("b", 1)
		d.Doc("a", nested.Bytes())
		doc := d.Bytes()
		if _, err = p.apply(doc); !errors.Is(err, ErrComputedField) {
			t.Fatal(defs, err)
		}
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if err := db.DefineComputedFields(ctx, []byte("{}")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestComputedFieldsBatchIndexes(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 256 << 10, CacheBytes: 128 * PageSize})
	c, err := db.Collection("orders")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.DefineComputedFields(bg, []byte(`{"total":{"$multiply":["$qty","$price"]},"key":{"$lower":"$email"}}`)); err != nil {
		t.Fatal(err)
	}
	if err = c.CreateIndex(bg, "by_total", "total"); err != nil {
		t.Fatal(err)
	}
	if err = c.CreateIndex(bg, "by_key", "key"); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatch()
	orders := b.With(c)
	if err = b.Upsert([]byte("opaque"), []byte("arbitrary bytes")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if err = orders.UpsertJSON([]byte(fmt.Sprint(i)), []byte(fmt.Sprintf(`{"qty":%d,"price":10,"email":"A%d@B"}`, i, i))); err != nil {
			t.Fatal(err)
		}
	}
	orders.Delete([]byte("1"))
	orders.UpsertJSON([]byte("1"), []byte(`{"qty":99,"price":10,"email":"X@Y"}`))
	// Commit via a non-default handle must still transform all collections.
	if err = orders.Commit(); err != nil {
		t.Fatal(err)
	}
	if computedRow(t, c.Find, "1")["total"] != int64(990) {
		t.Fatal("duplicate key order")
	}
	if err = db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	before := db.Stats().IndexScans
	rows, err := c.Aggregate(bg, []byte(`[{"$match":{"key":"x@y"}},{"$count":"n"}]`))
	if err != nil || len(rows) != 1 || rows[0]["n"] != int64(1) || db.Stats().IndexScans == before {
		t.Fatal(rows, err, "computed index not used")
	}
	b = db.NewBatch()
	b.Upsert([]byte("must-not-exist"), []byte("plain"))
	b.With(c).Delete([]byte("1"))
	b.With(c).UpsertJSON([]byte("2"), []byte(`{"qty":3,"price":8}`))
	b.With(c).UpsertJSON([]byte("bad"), []byte(`{"qty":"bad","price":4}`))
	if err = b.Commit(); !errors.Is(err, ErrComputedField) {
		t.Fatal(err)
	}
	if _, ok, _ := db.Find([]byte("must-not-exist"), nil); ok {
		t.Fatal("partial cross-collection batch")
	}
	if b.Len() != 0 || computedRow(t, c.Find, "1")["total"] != int64(990) || computedRow(t, c.Find, "2")["total"] != int64(20) {
		t.Fatal("partial batch")
	}
	if err = db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
	if err = c.Delete([]byte("1")); err != nil {
		t.Fatal(err)
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = db.VerifyIndexes(bg); err != nil {
		t.Fatal(err)
	}
}

func TestComputedFieldsEmptyAndSnapshots(t *testing.T) {
	db := computedDB(t, Options{})
	c, _ := db.Collection("empty")
	other, _ := db.Collection("other")
	other.Upsert([]byte("k"), []byte("v"))
	for i := 0; i < 1000; i++ {
		c.Upsert([]byte(fmt.Sprint(i)), []byte("v"))
	}
	if err := c.DefineComputedFields(bg, []byte(`{"x":1}`)); !errors.Is(err, ErrComputedNonEmpty) {
		t.Fatal(err)
	}
	db.Checkpoint()
	if err := c.DefineComputedFields(bg, []byte(`{"x":1}`)); !errors.Is(err, ErrComputedNonEmpty) {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if err := c.Delete([]byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.DefineComputedFields(bg, []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	s, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sc, err := s.Collection("empty")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DefineComputedFields(bg, []byte(`{"y":2}`)); err != nil {
		t.Fatal(err)
	}
	rules, err := sc.ComputedFields()
	if err != nil || string(rules["x"]) != "1" || len(rules) != 1 {
		t.Fatal(rules, err)
	}
	if err = sc.DefineComputedFields(bg, []byte("{}")); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
	if err = db.NewBatch().With(sc).Upsert([]byte("k"), []byte{0}); !errors.Is(err, ErrReadOnly) {
		t.Fatal(err)
	}
	if err = db.DropCollection(bg, "empty"); err != nil {
		t.Fatal(err)
	}
	rules, err = sc.ComputedFields()
	if err != nil || string(rules["x"]) != "1" {
		t.Fatal(rules, err)
	}
	if err := c.DefineComputedFields(bg, []byte("{}")); !errors.Is(err, ErrNoSuchCollection) {
		t.Fatal(err)
	}
	c, _ = db.Collection("empty")
	rules, err = c.ComputedFields()
	if err != nil || len(rules) != 0 {
		t.Fatal(rules, err)
	}
	if err = c.Upsert([]byte("opaque"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err = db.DefineComputedFields(bg, []byte(`{"x":1}`)); err != nil {
		t.Fatal("other collection interfered", err)
	}
	if err = db.DefineComputedFields(bg, []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func TestComputedFieldsPersistence(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{Sync: true, SyncMode: SyncOS})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := db.Collection("orders")
	defs := []byte(`{"total":{"$multiply":["$qty","$price"]}}`)
	if err = c.DefineComputedFields(bg, defs); err != nil {
		t.Fatal(err)
	}
	c.CreateIndex(bg, "total", "total")
	for i := 0; i < 300; i++ {
		if err = c.UpsertJSON([]byte(fmt.Sprint(i)), []byte(`{"qty":3,"price":7}`)); err != nil {
			t.Fatal(err)
		}
	}
	backup := t.TempDir()
	if err = db.Backup(bg, backup); err != nil {
		t.Fatal(err)
	}
	if err = db.Compact(bg); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{dir, backup} {
		db, err = Open(d, Options{})
		if err != nil {
			t.Fatal(err)
		}
		c, _ = db.Collection("orders")
		rules, err := c.ComputedFields()
		if err != nil || len(rules) != 1 {
			t.Fatal(rules, err)
		}
		if computedRow(t, c.Find, "2")["total"] != int64(21) {
			t.Fatal("stored value missing")
		}
		if err = c.UpsertJSON([]byte("new"), []byte(`{"qty":5,"price":6}`)); err != nil {
			t.Fatal(err)
		}
		if computedRow(t, c.Find, "new")["total"] != int64(30) {
			t.Fatal("rules missing after reopen")
		}
		if err = db.VerifyIndexes(bg); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestComputedFieldsCrashRecovery(t *testing.T) {
	if dir := os.Getenv("APTEVADB_COMPUTED_CRASH_DIR"); dir != "" {
		db, err := Open(dir, Options{Sync: true, SyncMode: SyncOS})
		if err != nil {
			t.Fatal(err)
		}
		if err = db.DefineComputedFields(bg, []byte(`{"x":{"$add":["$a",1]}}`)); err != nil {
			t.Fatal(err)
		}
		b := db.NewBatch()
		b.UpsertJSON([]byte("a"), []byte(`{"a":9007199254740993}`))
		b.UpsertJSON([]byte("b"), []byte(`{"a":7}`))
		if err = b.Commit(); err != nil {
			t.Fatal(err)
		}
		// Deliberately exit without Close/checkpoint: recover the schema and
		// the already-generated document bytes directly from the durable WAL.
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestComputedFieldsCrashRecovery$")
	cmd.Env = append(os.Environ(), "APTEVADB_COMPUTED_CRASH_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(out))
	}
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if computedRow(t, db.Find, "a")["x"] != int64(9007199254740994) || computedRow(t, db.Find, "b")["x"] != int64(8) {
		t.Fatal("WAL did not preserve generated results")
	}
	if err = db.UpsertJSON([]byte("c"), []byte(`{"a":2}`)); err != nil {
		t.Fatal(err)
	}
	if computedRow(t, db.Find, "c")["x"] != int64(3) {
		t.Fatal("schema not recovered")
	}
}

func TestComputedFieldsLegacyUpgrade(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Upsert([]byte("old"), []byte("opaque")); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "data"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	page := make([]byte, PageSize)
	for slot := int64(0); slot < 2; slot++ {
		if _, err = f.ReadAt(page, slot*PageSize); err != nil {
			t.Fatal(err)
		}
		if string(page[:8]) != metaMagic {
			continue
		}
		le.PutUint32(page[8:], 3)
		le.PutUint32(page[52:], crc32.Checksum(page[:52], crcTable))
		if _, err = f.WriteAt(page, slot*PageSize); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()
	db, err = Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	version, err := FormatVersionOf(dir)
	if err != nil || version != FormatVersion {
		t.Fatal("upgrade was not published before Open returned", version, err)
	}
	f, err = os.Open(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	for slot := int64(0); slot < 2; slot++ {
		if _, err = f.ReadAt(page, slot*PageSize); err != nil {
			t.Fatal(err)
		}
		if le.Uint32(page[8:]) != FormatVersion || le.Uint32(page[52:]) != crc32.Checksum(page[:52], crcTable) {
			t.Fatal("older-format fallback remains", slot)
		}
	}
	f.Close()
	v, ok, err := db.Find([]byte("old"), nil)
	if err != nil || !ok || !bytes.Equal(v, []byte("opaque")) {
		t.Fatal(v, ok, err)
	}
	c, _ := db.Collection("new")
	if err = c.DefineComputedFields(bg, []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
}

func TestComputedFieldsConcurrentReaders(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 256 << 10})
	if err := db.DefineComputedFields(bg, []byte(`{"x":{"$multiply":["$a",3]}}`)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 32; i++ {
		db.UpsertJSON([]byte(fmt.Sprint(i)), []byte(`{"a":0}`))
	}
	var wg sync.WaitGroup
	failures := make(chan error, 17)
	start := make(chan struct{})
	for g := 0; g < 16; g++ {
		reader, err := db.NewReader()
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(g int, r *Reader) {
			defer wg.Done()
			defer r.Close()
			<-start
			for i := 0; i < 800; i++ {
				doc, ok, err := r.Find([]byte(fmt.Sprint((i+g)%32)), nil)
				if err != nil || !ok {
					failures <- fmt.Errorf("find: %v %v", ok, err)
					return
				}
				a, x := docField(doc, "a"), docField(doc, "x")
				if a.k != kInt || x.k != kInt || x.i != 3*a.i {
					failures <- fmt.Errorf("inconsistent computed document")
					return
				}
			}
		}(g, reader)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1600; i++ {
			if err := db.UpsertJSON([]byte(fmt.Sprint(i%32)), []byte(fmt.Sprintf(`{"a":%d}`, i))); err != nil {
				failures <- err
				return
			}
			if i%200 == 0 {
				if err := db.Checkpoint(); err != nil {
					failures <- err
					return
				}
			}
		}
	}()
	close(start)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestComputedFieldsSchemaWriteRace(t *testing.T) {
	db := computedDB(t, Options{})
	for i := 0; i < 100; i++ {
		c, _ := db.Collection(fmt.Sprint(i))
		ready := make(chan struct{})
		result := make(chan error, 1)
		go func() { <-ready; result <- c.DefineComputedFields(bg, []byte(`{"x":1}`)) }()
		close(ready)
		err := c.UpsertJSON([]byte("k"), []byte("{}"))
		if err != nil {
			t.Fatal(err)
		}
		schemaErr := <-result
		row := computedRow(t, c.Find, "k")
		if schemaErr == nil && row["x"] != int64(1) {
			t.Fatal("schema published with uncomputed live value")
		}
		if schemaErr != nil && !errors.Is(schemaErr, ErrComputedNonEmpty) {
			t.Fatal(schemaErr)
		}
	}
}

func BenchmarkComputedFieldsTransform(b *testing.B) {
	p, err := compileComputedFields([]byte(`{"total":{"$multiply":["$qty","$price"]},"email_key":{"$lower":"$email"}}`))
	if err != nil {
		b.Fatal(err)
	}
	var d DocBuilder
	d.Int("qty", 3)
	d.Int("price", 7)
	d.String("email", "A@B.COM")
	doc := d.Bytes()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = p.apply(doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkComputedFieldsTransformReuse(b *testing.B) {
	p, err := compileComputedFields([]byte(`{"total":{"$multiply":["$qty","$price"]},"email_key":{"$lower":"$email"}}`))
	if err != nil {
		b.Fatal(err)
	}
	var d DocBuilder
	d.Int("qty", 3)
	d.Int("price", 7)
	d.String("email", "A@B.COM")
	doc := d.Bytes()
	var dst []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst, err = p.applyInto(doc, dst)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestComputedFieldsBatchBufferOwnership(t *testing.T) {
	db := computedDB(t, Options{MemtableBytes: 256 << 10})
	a, _ := db.Collection("a")
	z, _ := db.Collection("z")
	if err := a.DefineComputedFields(bg, []byte(`{"x":{"$lower":"$s"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := z.DefineComputedFields(bg, []byte(`{"x":{"$concat":["$s","$s","$s"]}}`)); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatch()
	for i := 0; i < 200; i++ {
		s := strings.Repeat("A", 1+i%100)
		if err := b.With(a).UpsertJSON([]byte(fmt.Sprint(i)), []byte(fmt.Sprintf(`{"s":%q}`, s))); err != nil {
			t.Fatal(err)
		}
		if err := b.With(z).UpsertJSON([]byte(fmt.Sprint(i)), []byte(fmt.Sprintf(`{"s":%q}`, s))); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if computedRow(t, a.Find, fmt.Sprint(i))["x"] != strings.Repeat("a", 1+i%100) {
			t.Fatal("reused temporary buffer leaked into storage", i)
		}
		if computedRow(t, z.Find, fmt.Sprint(i))["x"] != strings.Repeat("A", 3*(1+i%100)) {
			t.Fatal("cross-collection result was overwritten", i)
		}
	}
}

func TestComputedFieldsDocumentLimits(t *testing.T) {
	// JSON canonicalization escapes HTML characters. A definition accepted
	// now must also fit the parser's bound when loaded from the catalog.
	large := []byte(`{"x":{"$literal":"` + strings.Repeat("<", 12000) + `"}}`)
	if _, err := compileComputedFields(large); !errors.Is(err, ErrComputedField) {
		t.Fatal("accepted unreopenable schema", err)
	}
	p, err := compileComputedFields([]byte(`{"x":{"$concat":["$s","$s"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	var d DocBuilder
	d.String("s", strings.Repeat("x", 33000))
	if _, err = p.apply(d.Bytes()); !errors.Is(err, ErrComputedField) {
		t.Fatal(err)
	}
	d.Reset()
	for i := 0; i < 255; i++ {
		d.Null(fmt.Sprint(i))
	}
	if _, err = p.apply(d.Bytes()); !errors.Is(err, ErrComputedField) {
		t.Fatal(err)
	}
}

func TestComputedFieldsBatchReuse(t *testing.T) {
	db := computedDB(t, Options{})
	c, _ := db.Collection("computed")
	if err := c.DefineComputedFields(bg, []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	b := db.NewBatch()
	bound := b.With(c)
	for i := 0; i < 10; i++ {
		b.Upsert([]byte("plain"), []byte("v"))
		for j := 0; j < 100; j++ {
			b.With(c).UpsertJSON([]byte(fmt.Sprint(j)), []byte("{}"))
		}
		if len(b.views) != 1 {
			t.Fatal("unbounded collection tracking", len(b.views))
		}
		if err := bound.Commit(); err != nil {
			t.Fatal(err)
		}
		// Reuse a bound handle after reset; schema still applies when commit
		// is invoked through an unrelated wrapper.
		bound.UpsertJSON([]byte("new"), []byte("{}"))
		if err := b.Commit(); err != nil {
			t.Fatal(err)
		}
		if computedRow(t, c.Find, "new")["x"] != int64(1) {
			t.Fatal("bypassed rules")
		}
	}
	if err := db.DropCollection(bg, "computed"); err != nil {
		t.Fatal(err)
	}
	b.Upsert([]byte("plain"), []byte("new"))
	if err := b.Commit(); err != nil {
		t.Fatal("previous batch's dropped handle leaked", err)
	}
}

func TestComputedFieldsTornBatch(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestComputedFieldsCrashRecovery$")
	cmd.Env = append(os.Environ(), "APTEVADB_COMPUTED_CRASH_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(err, string(out))
	}
	logs, err := filepath.Glob(filepath.Join(dir, "wal-*"))
	if err != nil || len(logs) == 0 {
		t.Fatal(logs, err)
	}
	// Tear the last byte of the last record in the two-document chain.
	path := logs[len(logs)-1]
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Truncate(path, st.Size()-1); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, key := range []string{"a", "b"} {
		if _, ok, err := db.Find([]byte(key), nil); err != nil || ok {
			t.Fatal("partial computed batch recovered", key, ok, err)
		}
	}
	defs, err := db.ComputedFields()
	if err != nil || len(defs) != 1 {
		t.Fatal("previous schema lost", defs, err)
	}
	if err = db.UpsertJSON([]byte("c"), []byte(`{"a":9}`)); err != nil {
		t.Fatal(err)
	}
	if computedRow(t, db.Find, "c")["x"] != int64(10) {
		t.Fatal("recovered schema not applied")
	}
}

func TestComputedFieldsSchemaIOFailure(t *testing.T) {
	for trip := 1; trip <= 6; trip++ {
		t.Run(fmt.Sprint(trip), func(t *testing.T) {
			lab := newCrashLab(t, int64(1000+trip))
			db := lab.open(Options{Sync: true, SyncMode: SyncOS})
			lab.arm(trip)
			err := db.DefineComputedFields(bg, []byte(`{"x":1}`))
			if !expectedFailure(err) {
				t.Fatal(err)
			}
			acknowledged := err == nil
			lab.crash(db)
			db = lab.open(Options{Sync: true, SyncMode: SyncOS})
			defer db.Close()
			defs, err := db.ComputedFields()
			if err != nil {
				t.Fatal(err)
			}
			if acknowledged && len(defs) != 1 {
				t.Fatal("acknowledged schema lost")
			}
			if err = db.UpsertJSON([]byte("k"), []byte("{}")); err != nil {
				t.Fatal(err)
			}
			row := computedRow(t, db.Find, "k")
			if len(defs) == 1 && row["x"] != int64(1) {
				t.Fatal("recovered rule/value disagree")
			}
			if len(defs) == 0 && len(row) != 0 {
				t.Fatal(row)
			}
		})
	}
}
