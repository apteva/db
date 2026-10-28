package main

import (
	"encoding/json"
	"flag"
	"fmt"
	db "github.com/apteva/db"
	"os"
	"runtime"
	"syscall"
	"time"
)

var n = flag.Int("n", 1000000, "rows")
var numeric = flag.Bool("numeric", false, "numeric aggregates")
var typed = flag.Bool("typed", false, "borrowed visitor")
var mem = flag.Int64("memory", 12<<20, "execution bytes")

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func sec(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
func main() {
	flag.Parse()
	dir, e := os.MkdirTemp("", "aptevadb-spill-micro-")
	must(e)
	defer os.RemoveAll(dir)
	group := `{"count":{"$count":{}}}`
	if *numeric {
		group = `{"count":{"$count":{}},"sum_i":{"$sum":"$i"},"min_i":{"$min":"$i"},"max_i":{"$max":"$i"},"avg_n":{"$avg":"$n"}}`
	}
	pipeline := `[{"$group":{"_id":{"i":"$i"},` + group[1:] + `}]`
	p, e := db.ParsePipeline([]byte(pipeline))
	must(e)
	p.SetLimits(db.Limits{MaxGroups: 2000000, MaxBytes: *mem})
	src := func(y func([]byte, []byte) bool) error {
		var b db.DocBuilder
		for i := 0; i < *n; i++ {
			b.Reset()
			b.Int("i", int64(i))
			b.Float("n", float64(i))
			if !y(nil, b.Bytes()) {
				break
			}
		}
		return nil
	}
	seen := make([]uint64, (*n+63)/64)
	total := 0
	var checksum int64
	check := func(key, count, sum, min, max int64, avg float64) error {
		if key < 0 || key >= int64(*n) || count != 1 || seen[key/64]&(1<<(key%64)) != 0 {
			return fmt.Errorf("bad group %d", key)
		}
		seen[key/64] |= 1 << (key % 64)
		total++
		checksum += key
		if *numeric && (sum != key || min != key || max != key || avg != float64(key)) {
			return fmt.Errorf("bad numeric %d", key)
		}
		return nil
	}
	var st db.AggregateStats
	var a, b runtime.MemStats
	var u, v syscall.Rusage
	runtime.ReadMemStats(&a)
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &u))
	start := time.Now()
	e = execute(p, src, db.AggregateOptions{AllowDiskUse: true, MaxMemoryBytes: *mem, MaxDiskBytes: 1 << 30, TempDir: dir, Stats: &st}, *typed, check)
	elapsed := time.Since(start).Seconds()
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &v))
	runtime.ReadMemStats(&b)
	must(e)
	if total != *n || checksum != int64(*n)*int64(*n-1)/2 {
		panic("bad totals")
	}
	files, e := os.ReadDir(dir)
	must(e)
	if len(files) != 0 {
		panic("scratch leak")
	}
	must(json.NewEncoder(os.Stdout).Encode(map[string]any{"rows": *n, "numeric": *numeric, "typed": *typed, "wall": elapsed, "user": sec(v.Utime) - sec(u.Utime), "system": sec(v.Stime) - sec(u.Stime), "allocated": b.TotalAlloc - a.TotalAlloc, "objects": b.Mallocs - a.Mallocs, "gc": b.NumGC - a.NumGC, "peak_rss": v.Maxrss, "stats": st, "go": runtime.Version()}))
}
