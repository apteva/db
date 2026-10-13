package db

import (
	"encoding/json"
	"strings"
	"testing"
)

// FuzzDocument: any input either fails to encode or round-trips; decoding
// arbitrary bytes never panics.
func FuzzDocument(f *testing.F) {
	f.Add([]byte(`{"a":1,"b":"x","c":null,"d":true,"e":2.5,"n":{"x":[1,"y",{"z":false}]}}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"big":9007199254740993}`))
	f.Add([]byte(`{"s":"` + strings.Repeat("x", 80000) + `"}`))
	f.Add([]byte(`[1,2]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if _, err := DecodeDocument(data); err == nil {
			walkDoc(data, func(name []byte, v value) bool { return true })
		}
		enc, err := EncodeJSON(nil, data)
		if err != nil {
			return
		}
		m, err := DecodeDocument(enc)
		if err != nil {
			t.Fatalf("encoded document does not decode: %v", err)
		}
		var want map[string]any
		dec := json.NewDecoder(bytesReader(data))
		dec.UseNumber()
		if dec.Decode(&want) != nil {
			return
		}
		if len(m) != len(want) {
			t.Fatalf("round trip changed field count: %d vs %d", len(m), len(want))
		}
		vals := make([]value, 1)
		hints := make([]int, 1)
		for k := range want {
			if strings.Contains(k, ".") {
				continue // a dotted name is read as a path
			}
			extractFields(enc, splitPaths([]string{k}), vals, hints)
			if vals[0].k == kMissing {
				t.Fatalf("field %q missing after encode", k)
			}
		}
	})
}

// FuzzPipeline: parsing never panics, and a parsed pipeline runs over a
// small document set without panicking.
func FuzzPipeline(f *testing.F) {
	f.Add([]byte(`[{"$match":{"a":{"$gt":1}}},{"$group":{"_id":"$b","n":{"$sum":1},"m":{"$max":"$a"}}},{"$sort":{"n":-1,"_id":1}},{"$limit":2}]`))
	f.Add([]byte(`[{"$unwind":"$tags"},{"$group":{"_id":"$tags","n":{"$count":{}}}}]`))
	f.Add([]byte(`[{"$project":{"a":1}},{"$count":"x"}]`))
	var b DocBuilder
	var docs [][]byte
	for i := 0; i < 20; i++ {
		b.Reset()
		b.Int("a", int64(i%5))
		b.String("b", string(rune('a'+i%3)))
		b.Float("f", float64(i)/3)
		docs = append(docs, append([]byte(nil), b.Bytes()...))
	}
	src := Source(func(yield func(k, v []byte) bool) error {
		for i, d := range docs {
			if !yield([]byte{byte(i)}, d) {
				break
			}
		}
		return nil
	})
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := ParsePipeline(data)
		if err != nil {
			return
		}
		p.Run(bg, src)
		p.RunSharded(bg, []Source{src, src})
	})
}

// FuzzPage: random insert/remove/compact sequences keep a page consistent.
func FuzzPage(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	f.Fuzz(func(t *testing.T, ops []byte) {
		p := make(page, PageSize)
		initPage(p, pageLeaf, 0)
		scratch := make(page, PageSize)
		var keys []string
		for i := 0; i+1 < len(ops); i += 2 {
			switch ops[i] % 3 {
			case 0, 1:
				k := []byte{ops[i+1], byte(i)}
				v := make([]byte, int(ops[i+1])*3)
				idx, found := p.searchLeaf(k)
				if found {
					p.removeCell(idx)
				}
				if p.freeSpace() < leafCellHdr+len(k)+len(v)+slotSize {
					if p.garbage() > 0 {
						p.compact(scratch)
					}
					if p.freeSpace() < leafCellHdr+len(k)+len(v)+slotSize {
						continue
					}
				}
				p.insertLeafCell(idx, k, v, false)
				if !found {
					keys = append(keys, string(k))
				}
			case 2:
				if n := p.numCells(); n > 0 {
					idx := int(ops[i+1]) % n
					k := string(p.leafKey(idx))
					p.removeCell(idx)
					for j, kk := range keys {
						if kk == k {
							keys = append(keys[:j], keys[j+1:]...)
							break
						}
					}
				}
			}
		}
		if err := verifyLeaf(p); err != nil {
			t.Fatal(err)
		}
		if p.numCells() != len(keys) {
			t.Fatalf("page has %d cells, model %d", p.numCells(), len(keys))
		}
	})
}
