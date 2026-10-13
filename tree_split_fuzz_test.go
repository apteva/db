package db

import (
	"bytes"
	"testing"
)

func FuzzWidePageSplit(f *testing.F) {
	f.Add([]byte{255, 255, 255, 7})
	f.Add([]byte{255, 1, 128, 254, 129, 0, 255, 2})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) < 3 {
			return
		}
		for _, internal := range []bool{false, true} {
			typ, level := pageLeaf, byte(0)
			if internal {
				typ, level = pageInternal, 1
			}
			p, r := make(page, PageSize), make(page, PageSize)
			initPage(p, typ, level)
			initPage(r, typ, level)
			p.setLeftChild(123)
			var keys [][]byte
			for i, b := range input[:min(len(input), 200)] {
				// Unique sorted keys; vary full physical sizes through the new limit.
				key := append([]byte{byte(i)}, bytes.Repeat([]byte{'x'}, 1+int(b)*(MaxIndexKeySize-2)/255)...)
				hdr := leafCellHdr
				if internal {
					hdr = internalCellHdr
				}
				need := hdr + len(key) + slotSize
				if p.freeSpace() >= need {
					if internal {
						p.insertInternalCell(p.numCells(), key, uint32(i+124))
					} else {
						p.insertLeafCell(p.numCells(), key, nil, false)
					}
					keys = append(keys, key)
					continue
				}
				if p.numCells() < 2 {
					t.Fatal("half-page cell bound failed")
				}
				// Put the new key between existing keys, exercising every insertion side.
				at := int(b) % len(keys)
				key[0] = keys[at][0]
				key[1] = 'w'
				if bytes.Compare(key, keys[at]) >= 0 {
					t.Fatal("probe order")
				}
				d := &DB{}
				sep := d.splitPageInsert(p, r, key, nil, false, 999)
				keys = append(keys, nil)
				copy(keys[at+1:], keys[at:])
				keys[at] = key
				got := [][]byte{}
				for j := 0; j < p.numCells(); j++ {
					if internal {
						got = append(got, p.internalKey(j))
					} else {
						got = append(got, p.leafKey(j))
					}
				}
				if internal {
					got = append(got, sep)
				}
				for j := 0; j < r.numCells(); j++ {
					if internal {
						got = append(got, r.internalKey(j))
					} else {
						got = append(got, r.leafKey(j))
					}
				}
				if len(got) != len(keys) || p.freeSpace() < 0 || r.freeSpace() < 0 || p.numCells() == 0 || r.numCells() == 0 {
					t.Fatal("invalid split sizes")
				}
				for j := range keys {
					if !bytes.Equal(keys[j], got[j]) {
						t.Fatal("split lost/reordered key", j)
					}
				}
				if !internal && !bytes.Equal(sep, r.leafKey(0)) {
					t.Fatal("leaf separator")
				}
				if internal {
					if p.leftChild() != 123 {
						t.Fatal("left child")
					}
					for j := 0; j < len(got); j++ {
						want := uint32(got[j][0]) + 124
						if j == at {
							want = 999
						}
						var actual uint32
						if j < p.numCells() {
							actual = p.internalChild(j)
						} else if j == p.numCells() {
							actual = r.leftChild()
						} else {
							actual = r.internalChild(j - p.numCells() - 1)
						}
						if actual != want {
							t.Fatal("split lost child", j, actual, want)
						}
					}
				}
				break
			}
		}
	})
}
