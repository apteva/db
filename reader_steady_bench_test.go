package db

import (
	"sync/atomic"
	"testing"
)

// Sustained point reads complement the short parallel phase in the comparison
// harness, where a burst of background CPU work can dominate the whole sample.
func BenchmarkReaderSteady(b *testing.B) {
	const rows = 1_000_000
	keys, vals := benchKeys(rows)
	d, err := Open(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer d.Close()
	batch := d.NewBatch()
	for i := 0; i < rows; i++ {
		batch.Upsert(keys[i*16:(i+1)*16], vals[i*100:(i+1)*100])
		if (i+1)%1000 == 0 {
			if err := batch.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := d.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	find := func(r *Reader, state *uint64, buf []byte) []byte {
		*state = *state*6364136223846793005 + 1
		i := int(*state>>32) % rows
		v, found, err := r.Find(keys[i*16:(i+1)*16], buf)
		if err != nil || !found || len(v) != 100 || v[0] != vals[i*100] {
			b.Fatalf("incorrect point read %d: found=%v error=%v", i, found, err)
		}
		return v
	}
	b.Run("Single", func(b *testing.B) {
		r, err := d.NewReader()
		if err != nil {
			b.Fatal(err)
		}
		defer r.Close()
		state := uint64(1)
		var buf []byte
		b.ReportAllocs()
		for b.Loop() {
			buf = find(r, &state, buf)
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		var seeds atomic.Uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			r, err := d.NewReader()
			if err != nil {
				b.Error(err)
				return
			}
			defer r.Close()
			state := seeds.Add(1)
			var buf []byte
			for pb.Next() {
				buf = find(r, &state, buf)
			}
		})
	})
}
