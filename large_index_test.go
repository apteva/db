package db

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestActualFormat7FixtureUpgrade(t *testing.T) {
	dir := t.TempDir()
	files, err := filepath.Glob("testdata/format7/*")
	mustCompat(t, err)
	for _, name := range files {
		if filepath.Base(name) != "data" && !strings.HasPrefix(filepath.Base(name), "wal-") {
			continue
		}
		b, err := os.ReadFile(name)
		mustCompat(t, err)
		mustCompat(t, os.WriteFile(filepath.Join(dir, filepath.Base(name)), b, 0600))
	}
	version, err := FormatVersionOf(dir)
	mustCompat(t, err)
	if version != 7 {
		t.Fatal(version)
	}
	d, err := Open(dir, Options{SyncMode: SyncOS})
	mustCompat(t, err)
	defer d.Close()
	data, err := os.ReadFile(filepath.Join(dir, "data"))
	mustCompat(t, err)
	for _, off := range []int{0, PageSize} {
		if le.Uint32(data[off+8:]) != FormatVersion {
			t.Fatal("old meta slot after upgrade")
		}
	}
	c, err := d.OpenCollection("rows")
	mustCompat(t, err)
	doc, found, err := c.Find([]byte("saved"), nil)
	mustCompat(t, err)
	if !found || docField(doc, "n").i != 9007199254740993 || len(docField(doc, "blob").s) != 80000 {
		t.Fatal("format-7 fixture lost data")
	}
	cur, err := c.NewIndexCursor(bg, "tenant_n", IndexCursorOptions{})
	mustCompat(t, err)
	if !cur.First() || string(cur.Key()) != "saved" {
		t.Fatal("old exact index")
	}
	mustCompat(t, cur.Close())
	mustCompat(t, c.CreateCompoundIndex(bg, "wide", []IndexField{{Field: "wide"}}))
	mustCompat(t, c.InsertDocument(bytes.Repeat([]byte{'k'}, 1500), map[string]any{"wide": strings.Repeat("v", 3000), "email": nil}))
	mustCompat(t, d.Verify(bg))
	mustCompat(t, d.VerifyIndexes(bg))
}

func TestLargeCombinedIndexEntries(t *testing.T) {
	for _, kind := range []string{"ordinary", "compound", "descending", "unique"} {
		for _, backfill := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/backfill=%v", kind, backfill), func(t *testing.T) {
				dir := t.TempDir()
				d, err := Open(dir, Options{SyncMode: SyncOS, MemtableBytes: 128 << 10})
				mustCompat(t, err)
				defer func() { d.Close() }()
				c, err := d.Collection("large")
				mustCompat(t, err)
				create := func() {
					switch kind {
					case "ordinary":
						mustCompat(t, c.CreateIndex(bg, "v", "v"))
					case "unique":
						mustCompat(t, c.CreateUniqueIndex(bg, "v", "v"))
					default:
						mustCompat(t, c.CreateCompoundIndex(bg, "v", []IndexField{{Field: "v", Desc: kind == "descending"}}))
					}
				}
				if !backfill {
					create()
				}
				keys := make([][]byte, 240)
				values := make([]string, len(keys))
				rng := rand.New(rand.NewSource(73))
				for i := range keys {
					pklen, vlen := 1500, 3000
					if i%3 == 0 {
						pklen, vlen = 3500, 4600
					}
					keys[i] = []byte(fmt.Sprintf("%04d", i) + strings.Repeat("k", pklen-4))
					values[i] = fmt.Sprintf("%04d", i) + strings.Repeat("v", vlen-4)
					// Exercise zero escaping within the encoded limit.
					if i%7 == 0 {
						keys[i][30] = 0
						values[i] = values[i][:20] + "\x00" + values[i][21:]
					}
				}
				for _, i := range rng.Perm(len(keys)) {
					mustCompat(t, c.InsertDocument(keys[i], map[string]any{"v": values[i]}))
				}
				if backfill {
					create()
				}
				check := func() {
					mustCompat(t, d.Verify(bg))
					mustCompat(t, d.VerifyIndexes(bg))
					cur, err := c.NewIndexCursor(bg, "v", IndexCursorOptions{})
					mustCompat(t, err)
					defer cur.Close()
					order := make([]int, len(keys))
					for i := range order {
						order[i] = i
					}
					sort.Slice(order, func(i, j int) bool {
						if kind == "descending" {
							return values[order[i]] > values[order[j]]
						}
						return values[order[i]] < values[order[j]]
					})
					n := 0
					for ok := cur.First(); ok; ok = cur.Next() {
						if n >= len(order) || !bytes.Equal(cur.Key(), keys[order[n]]) {
							t.Fatalf("wrong cursor row %d", n)
						}
						n++
					}
					mustCompat(t, cur.Err())
					if n != len(keys) {
						t.Fatal(n)
					}
					// Repeated exclusive seeks after unrelated traversal, using long positions.
					if !cur.Seek([]any{values[50]}) {
						t.Fatal("long tuple seek")
					}
					position := append([]byte(nil), cur.Position()...)
					if !cur.Seek([]any{values[150]}) {
						t.Fatal("long tuple seek")
					}
					if kind == "descending" {
						if !cur.SeekAfter(position) || !bytes.Equal(cur.Key(), keys[49]) {
							t.Fatal("long resume")
						}
					} else {
						if !cur.SeekAfter(position) || !bytes.Equal(cur.Key(), keys[51]) {
							t.Fatal("long resume")
						}
					}
					mustCompat(t, c.Query(bg, []byte(`{}`), QueryOptions{Sort: []IndexField{{Field: "v", Desc: kind == "descending"}}, Limit: 10}, func(k, doc []byte) bool { return true }))
				}
				check()
				mustCompat(t, d.Checkpoint())
				mustCompat(t, d.Close())
				d, err = Open(dir, Options{SyncMode: SyncOS})
				mustCompat(t, err)
				c, err = d.OpenCollection("large")
				mustCompat(t, err)
				check()
				mustCompat(t, d.Compact(bg))
				check()
				for _, i := range rng.Perm(len(keys)) {
					mustCompat(t, c.Delete(keys[i]))
					mustCompat(t, c.InsertDocument(keys[i], map[string]any{"v": values[i]}))
				}
				mustCompat(t, d.VerifyIndexes(bg))
				check()
				// Reject the escaped physical size atomically, on both insert and replacement.
				huge := strings.Repeat("\x00", MaxIndexKeySize/2)
				if err := c.InsertDocument([]byte("oversized"), map[string]any{"v": huge}); !errors.Is(err, ErrKeySize) {
					t.Fatal(err)
				}
				if err := c.UpsertDocument(keys[0], map[string]any{"v": huge}); !errors.Is(err, ErrKeySize) {
					t.Fatal(err)
				}
				check()
			})
		}
	}
}

func TestWideTreeSplitWithMixedCells(t *testing.T) {
	d, err := Open(t.TempDir(), Options{SyncMode: SyncOS})
	mustCompat(t, err)
	defer d.Close()
	rng := rand.New(rand.NewSource(91))
	model := make(map[string][]byte)
	keys := make([][]byte, 3000)
	for i := range keys {
		size := []int{20, 100, 3000, 4500, MaxIndexKeySize}[i%5]
		keys[i] = []byte("\x00\x04" + fmt.Sprintf("%08d", i) + strings.Repeat("x", size-10))
	}
	for pass := 0; pass < 3; pass++ {
		for step, i := range rng.Perm(len(keys)) {
			b := &Batch{coll: d.def, batchState: &batchState{db: d, internal: true, lastOff: -1, limit: MaxTransactionBytes}}
			if pass == 2 && i%3 == 0 {
				mustCompat(t, b.Delete(keys[i]))
				delete(model, string(keys[i]))
			} else {
				v := bytes.Repeat([]byte{byte(i)}, []int{0, 8, 50, 5000}[rng.Intn(4)])
				mustCompat(t, b.Upsert(keys[i], v))
				model[string(keys[i])] = v
			}
			mustCompat(t, b.Commit())
			if step%250 == 0 {
				mustCompat(t, d.Verify(bg))
			}
		}
		mustCompat(t, d.Checkpoint())
		mustCompat(t, d.Verify(bg))
		snap, err := d.Snapshot()
		mustCompat(t, err)
		fake := &Collection{db: d, snap: snap, state: d.def.state, pfx: []byte{0, 4}, end: []byte{0, 5}}
		cur := fake.NewCursor()
		ordered := make([]string, 0, len(model))
		for k := range model {
			ordered = append(ordered, k)
		}
		sort.Strings(ordered)
		n := 0
		for ok := cur.First(); ok; ok = cur.Next() {
			if n >= len(ordered) || string(cur.ikey) != ordered[n] || !bytes.Equal(cur.Value(), model[ordered[n]]) {
				t.Fatalf("mixed tree pass=%d row=%d", pass, n)
			}
			n++
		}
		mustCompat(t, cur.Err())
		cur.Close()
		snap.Close()
		if n != len(ordered) {
			t.Fatal(n, len(ordered))
		}
	}
	mustCompat(t, d.Compact(bg))
	mustCompat(t, d.Verify(bg))
}

func TestLargeIndexWALRecoveryAndBoundary(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, Options{SyncMode: SyncOS, MemtableBytes: 1 << 20})
	mustCompat(t, err)
	defer func() { d.Close() }()
	mustCompat(t, d.CreateCompoundIndex(bg, "v", []IndexField{{Field: "v"}}))
	mustCompat(t, d.Checkpoint())
	key := bytes.Repeat([]byte{'k'}, 1500)
	// Entry: 6-byte index prefix, string tag + terminator (3), escaped key
	// terminator and length (4), plus the unescaped ASCII payloads.
	v := strings.Repeat("v", MaxIndexKeySize-len(key)-13)
	mustCompat(t, d.def.InsertDocument(key, map[string]any{"v": v}))
	if err := d.def.InsertDocument([]byte("short"), map[string]any{"v": strings.Repeat("x", MaxIndexKeySize)}); !errors.Is(err, ErrKeySize) {
		t.Fatal(err)
	}
	mustCompat(t, d.FlushLog())
	d.crash()
	d, err = Open(dir, Options{SyncMode: SyncOS})
	mustCompat(t, err)
	mustCompat(t, d.Verify(bg))
	mustCompat(t, d.VerifyIndexes(bg))
	cur, err := d.def.NewIndexCursor(bg, "v", IndexCursorOptions{})
	mustCompat(t, err)
	defer cur.Close()
	if !cur.First() || !bytes.Equal(cur.Key(), key) || len(cur.Position()) != MaxIndexKeySize {
		t.Fatalf("boundary recovery: len=%d error=%v", len(cur.Position()), cur.Err())
	}
}
