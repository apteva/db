package aptevadb

import (
	"math/rand"
	"testing"
	"time"
)

func benchKeys(n int) ([]byte, []byte) {
	rng := rand.New(rand.NewSource(7))
	keys := make([]byte, n*16)
	rng.Read(keys)
	vals := make([]byte, n*100)
	rng.Read(vals)
	return keys, vals
}

// BenchmarkInsertRandom1M reports ns per insert over one 1M-key load per iteration.
func BenchmarkInsertRandom1M(b *testing.B) {
	keys, vals := benchKeys(1_000_000)
	var total time.Duration
	for b.Loop() {
		db, _ := Open(b.TempDir(), Options{})
		start := time.Now()
		for i := 0; i < 1_000_000; i++ {
			db.Insert(keys[i*16:(i+1)*16], vals[i*100:(i+1)*100])
		}
		total += time.Since(start)
		db.Close()
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/1e6, "ns/insert")
}

// BenchmarkInsertRandom1MDirect is the same load with the memtable off.
func BenchmarkInsertRandom1MDirect(b *testing.B) {
	keys, vals := benchKeys(1_000_000)
	var total time.Duration
	for b.Loop() {
		db, _ := Open(b.TempDir(), Options{MemtableBytes: -1})
		start := time.Now()
		for i := 0; i < 1_000_000; i++ {
			db.Insert(keys[i*16:(i+1)*16], vals[i*100:(i+1)*100])
		}
		total += time.Since(start)
		db.Close()
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N)/1e6, "ns/insert")
}

func BenchmarkFindRandom1M(b *testing.B) {
	keys, vals := benchKeys(1_000_000)
	db, _ := Open(b.TempDir(), Options{})
	for i := 0; i < 1_000_000; i++ {
		db.Insert(keys[i*16:(i+1)*16], vals[i*100:(i+1)*100])
	}
	rng := rand.New(rand.NewSource(9))
	var buf []byte
	b.ResetTimer()
	for b.Loop() {
		i := rng.Intn(1_000_000)
		buf, _, _ = db.Find(keys[i*16:(i+1)*16], buf)
	}
	b.StopTimer()
	db.Close()
}
