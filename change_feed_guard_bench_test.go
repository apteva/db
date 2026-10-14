package db

import (
	"encoding/binary"
	"testing"
)

// Copied unchanged into the released alpha.4 source for paired disabled checks.
func BenchmarkChangeFeedDisabledGuard(b *testing.B) {
	const rows = 100_000
	d, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	var key [8]byte
	var value [100]byte
	batch := d.NewBatch()
	for i := 0; i < rows; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		if err = batch.Upsert(key[:], value[:]); err != nil {
			b.Fatal(err)
		}
		if (i+1)%1000 == 0 {
			if err = batch.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err = d.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	b.Run("Upsert", func(b *testing.B) {
		b.ReportAllocs()
		probe := 0
		for b.Loop() {
			probe = (probe + 7919) % rows
			binary.BigEndian.PutUint64(key[:], uint64(probe))
			if err = d.Upsert(key[:], value[:]); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Find", func(b *testing.B) {
		b.ReportAllocs()
		var dst [100]byte
		probe := 0
		for b.Loop() {
			probe = (probe + 7919) % rows
			binary.BigEndian.PutUint64(key[:], uint64(probe))
			v, ok, err := d.Find(key[:], dst[:0])
			if err != nil || !ok || len(v) != 100 {
				b.Fatal(ok, err)
			}
		}
	})
	b.Run("Scan", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n := 0
			if err = d.Scan(nil, nil, func(k, v []byte) bool { n++; return true }); err != nil || n != rows {
				b.Fatal(n, err)
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/rows, "ns/row")
	})
}
