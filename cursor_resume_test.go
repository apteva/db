package db

import (
	"bytes"
	"fmt"
	"testing"
)

func TestIndexCursorResumeReusedPosition(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			opts := Options{SyncMode: SyncOS, MemtableBytes: -1}
			if pending {
				opts.MemtableBytes = 128 << 10
			}
			d, err := Open(t.TempDir(), opts)
			mustCompat(t, err)
			defer d.Close()
			c, err := d.Collection("resume")
			mustCompat(t, err)
			mustCompat(t, c.CreateCompoundIndex(bg, "n", []IndexField{{Field: "n"}}))
			for i := 0; i < 1500; i++ {
				mustCompat(t, c.InsertDocument([]byte(fmt.Sprintf("%04d", i)), map[string]any{"n": i}))
			}
			mustCompat(t, d.Checkpoint())
			if pending {
				for i := 5; i < 1500; i += 11 {
					mustCompat(t, c.Delete([]byte(fmt.Sprintf("%04d", i))))
				}
			}
			cur, err := c.NewIndexCursor(bg, "n", IndexCursorOptions{})
			mustCompat(t, err)
			defer cur.Close()
			var positions [][]byte
			for ok := cur.First(); ok; ok = cur.Next() {
				positions = append(positions, append([]byte(nil), cur.Position()...))
			}
			mustCompat(t, cur.Err())
			for _, pair := range [][2]int{{20, 80}, {80, 20}, {10, 1200}, {1200, 10}} {
				for _, forward := range []bool{true, false} {
					// Leave a live leaf cache at an unrelated row, including the same leaf.
					if !cur.SeekAfter(positions[pair[1]]) {
						t.Fatal("initial seek")
					}
					fresh, err := c.NewIndexCursor(bg, "n", IndexCursorOptions{})
					mustCompat(t, err)
					var got, want bool
					if forward {
						got = cur.SeekAfter(positions[pair[0]])
						want = fresh.SeekAfter(positions[pair[0]])
					} else {
						got = cur.SeekBefore(positions[pair[0]])
						want = fresh.SeekBefore(positions[pair[0]])
					}
					if got != want || !bytes.Equal(cur.Key(), fresh.Key()) {
						t.Fatalf("from %d after traversal to %d forward=%v: reused=%q fresh=%q", pair[0], pair[1], forward, cur.Key(), fresh.Key())
					}
					mustCompat(t, cur.Err())
					mustCompat(t, fresh.Err())
					fresh.Close()
				}
			}
		})
	}
}

func TestCursorResumeAfterCollectionExhaustion(t *testing.T) {
	d, err := Open(t.TempDir(), Options{MemtableBytes: -1})
	mustCompat(t, err)
	defer d.Close()
	left := mustCollection(t, d, "left")
	right := mustCollection(t, d, "right")
	for _, c := range []*Collection{left, right} {
		for i := 0; i < 50; i++ {
			mustCompat(t, c.Upsert([]byte(fmt.Sprintf("%04d", i)), []byte("v")))
		}
	}
	for _, c := range []*Collection{left, right} {
		for _, forward := range []bool{true, false} {
			cur := c.NewCursor()
			ok := cur.First()
			if !forward {
				ok = cur.Last()
			}
			var position []byte
			for ok {
				position = append(position[:0], cur.ikey...)
				if forward {
					ok = cur.Next()
				} else {
					ok = cur.Prev()
				}
			}
			mustCompat(t, cur.Err())
			// Resume in the opposite direction at exactly the last valid key.
			if !cur.move(position, !forward, false) {
				t.Fatalf("%s forward=%v failed to resume", c.Name(), forward)
			}
			want := "0048"
			if !forward {
				want = "0001"
			}
			if string(cur.Key()) != want {
				t.Fatalf("%s forward=%v resumed at %s, want %s", c.Name(), forward, cur.Key(), want)
			}
			cur.Close()
		}
	}
}
