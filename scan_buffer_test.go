package db

import (
	"bytes"
	"fmt"
	"testing"
)

// Cursors return private value buffers. Reusing them must not change a frozen
// memtable or a tree page; scans must still read the original mixed-size values.
func TestIterationValueIsolation(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint(buffered), func(t *testing.T) {
			opts := Options{MemtableBytes: -1, CacheBytes: 32 * PageSize}
			if buffered {
				opts.MemtableBytes = 4 << 20
			}
			d, err := Open(t.TempDir(), opts)
			mustCompat(t, err)
			defer d.Close()
			want := make(map[string][]byte)
			var keys []string
			for i := 0; i < 256; i++ {
				k := fmt.Sprintf("%04d", i)
				v := bytes.Repeat([]byte{byte(i)}, []int{0, 1, 100, 6000, 80000}[i%5])
				mustCompat(t, d.Upsert([]byte(k), v))
				want[k] = v
				keys = append(keys, k)
			}
			mustCompat(t, d.Checkpoint())
			if buffered {
				for i := 0; i < len(keys); i += 7 {
					v := bytes.Repeat([]byte{7}, 100+i)
					mustCompat(t, d.Upsert([]byte(keys[i]), v))
					want[keys[i]] = v
				}
			}
			s, err := d.Snapshot()
			mustCompat(t, err)
			defer s.Close()
			for pass := 0; pass < 2; pass++ {
				n := 0
				mustCompat(t, s.Scan([]byte(keys[3]), []byte(keys[253]), func(k, v []byte) bool {
					if string(k) != keys[n+3] || !bytes.Equal(v, want[string(k)]) {
						t.Fatalf("scan pass %d row %d differs", pass, n)
					}
					n++
					return true
				}))
				if n != 250 {
					t.Fatalf("scan returned %d rows", n)
				}
				cur := s.NewCursor()
				for _, forward := range []bool{true, false} {
					n = 0
					ok := cur.First()
					if !forward {
						ok = cur.Last()
					}
					for ok {
						i := n
						if !forward {
							i = len(keys) - 1 - n
						}
						if string(cur.Key()) != keys[i] || !bytes.Equal(cur.Value(), want[keys[i]]) {
							t.Fatalf("cursor pass %d forward=%v row %d differs", pass, forward, n)
						}
						clear(cur.Value())
						n++
						if forward {
							ok = cur.Next()
						} else {
							ok = cur.Prev()
						}
					}
					mustCompat(t, cur.Err())
					if n != len(keys) {
						t.Fatalf("cursor returned %d rows", n)
					}
				}
				cur.Close()
			}
		})
	}
}
