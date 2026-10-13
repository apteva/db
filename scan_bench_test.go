package db

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func BenchmarkRawIteration(b *testing.B) {
	const rows = 100_000
	d, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	var key [8]byte
	var val [100]byte
	batch := d.NewBatch()
	for i := 0; i < rows; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		binary.BigEndian.PutUint64(val[:], uint64(i))
		batch.Upsert(key[:], val[:])
		if (i+1)%1000 == 0 {
			if err := batch.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := d.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"Scan", "Cursor"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				n := 0
				check := func(k, v []byte) bool {
					if len(k) != 8 || len(v) != 100 || binary.BigEndian.Uint64(k) != uint64(n) || binary.BigEndian.Uint64(v) != uint64(n) {
						b.Fatalf("incorrect row %d", n)
					}
					n++
					return true
				}
				if mode == "Scan" {
					if err := d.Scan(nil, nil, check); err != nil {
						b.Fatal(err)
					}
				} else {
					cur := d.NewCursor()
					for ok := cur.First(); ok; ok = cur.Next() {
						check(cur.Key(), cur.Value())
					}
					if err := cur.Err(); err != nil {
						b.Fatal(err)
					}
					cur.Close()
				}
				if n != rows {
					b.Fatalf("got %d rows, want %d", n, rows)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/rows, "ns/row")
		})
	}
}

func BenchmarkRawRange(b *testing.B) {
	const rows = 10_000
	d, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	var key [8]byte
	var val [100]byte
	batch := d.NewBatch()
	for i := 0; i < rows; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		binary.BigEndian.PutUint64(val[:], uint64(i))
		batch.Upsert(key[:], val[:])
		if (i+1)%1000 == 0 {
			if err := batch.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := d.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{1, 8, 128} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.ReportAllocs()
			var lo, hi [8]byte
			probe := 0
			for b.Loop() {
				first := probe * 7919 % (rows - size)
				probe++
				binary.BigEndian.PutUint64(lo[:], uint64(first))
				binary.BigEndian.PutUint64(hi[:], uint64(first+size))
				n := 0
				if err := d.Scan(lo[:], hi[:], func(k, v []byte) bool {
					if binary.BigEndian.Uint64(k) != uint64(first+n) || binary.BigEndian.Uint64(v) != uint64(first+n) {
						b.Fatal("incorrect range row")
					}
					n++
					return true
				}); err != nil {
					b.Fatal(err)
				}
				if n != size {
					b.Fatalf("got %d rows, want %d", n, size)
				}
			}
		})
	}
}
