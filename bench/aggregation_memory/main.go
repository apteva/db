package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	db "github.com/apteva/db"
	"hash/crc32"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var n = flag.Int("n", 300000, "rows")
var groups = flag.Int("groups", 100000, "distinct groups")
var mode = flag.String("mode", "ordered-all", "ordered-all, ordered-limit, unordered, crud, cold")
var api = flag.String("api", "legacy", "legacy, stream or parallel")
var budget = flag.Int64("memorymb", 512, "working memory")
var spill = flag.Bool("spill", false, "allow spill")
var dir = flag.String("dir", "", "fresh fixture")
var pad = flag.Int("pad", 128, "document padding")

type phase struct {
	Name                     string
	CPU, Wall                float64
	RSS, PeakRSS             int64
	Footprint, PeakFootprint uint64
	Allocated                uint64
	Faults                   int64
}

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func rss() int64 {
	out, e := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	must(e)
	v, e := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	must(e)
	return v * 1024
}
func seconds(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
func usage() syscall.Rusage {
	var u syscall.Rusage
	must(syscall.Getrusage(syscall.RUSAGE_SELF, &u))
	return u
}
func measure(name string, fn func() error) (phase, error) {
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	u := usage()
	start := time.Now()
	e := fn()
	wall := time.Since(start).Seconds()
	v := usage()
	runtime.ReadMemStats(&b)
	f, pf := resourceFootprint()
	p := phase{Name: name, CPU: seconds(v.Utime) + seconds(v.Stime) - seconds(u.Utime) - seconds(u.Stime), Wall: wall, RSS: rss(), PeakRSS: v.Maxrss, Footprint: f, PeakFootprint: pf, Allocated: b.TotalAlloc - a.TotalAlloc, Faults: v.Minflt - u.Minflt}
	fmt.Fprintf(os.Stderr, "%s wall=%.4f cpu=%.4f rss=%.1fMiB\n", name, p.Wall, p.CPU, float64(p.RSS)/(1<<20))
	return p, e
}
func main() {
	flag.Parse()
	if *dir == "" || *n < 1 || *groups < 1 || *groups > *n {
		panic("invalid fixture")
	}
	ctx := context.Background()
	var phases []phase
	d, e := openDatabase(*dir, *mode == "cold")
	must(e)
	c, e := d.Collection("rows")
	must(e)
	padding := strings.Repeat("x", *pad)
	checksums := make([]uint32, *n)
	loadOrder := rand.New(rand.NewSource(1)).Perm(*n)
	seenGroups := make([]uint64, (*groups+63)/64)
	record := func(name string, fn func() error) error {
		p, e := measure(name, fn)
		phases = append(phases, p)
		return e
	}
	must(record("load", func() error {
		var b db.DocBuilder
		for j := 0; j < *n; j++ {
			i := j
			if *mode == "crud-random" {
				i = loadOrder[j]
			}
			b.Reset()
			b.Int("g", int64(i%*groups))
			b.Int("i", int64(i))
			b.Int("v", 1)
			b.String("pad", padding)
			checksums[i] = crc32.ChecksumIEEE(b.Bytes())
			if e := c.Upsert([]byte(fmt.Sprintf("%09d", i)), b.Bytes()); e != nil {
				return e
			}
		}
		return nil
	}))
	if strings.HasPrefix(*mode, "ordered-") {
		must(record("index", func() error { return c.CreateCompoundIndex(ctx, "g", []db.IndexField{{Field: "g"}}) }))
	}
	must(record("settle", func() error {
		if e := d.Checkpoint(); e != nil {
			return e
		}
		return d.Compact(ctx)
	}))
	runtime.GC()
	var st any
	var resultErr error
	var seen, sum int64
	if strings.HasPrefix(*mode, "crud") || *mode == "cold" {
		order := rand.New(rand.NewSource(42)).Perm(*n)
		var buf []byte
		hit := func() error {
			for _, i := range order {
				v, ok, e := c.Find([]byte(fmt.Sprintf("%09d", i)), buf)
				if e != nil {
					return e
				}
				if !ok || crc32.ChecksumIEEE(v) != checksums[i] {
					return fmt.Errorf("missing row %d", i)
				}
				buf = v
			}
			return nil
		}
		must(record("point-hits", hit))
		must(record("parallel-hits-10", func() error {
			var wg sync.WaitGroup
			fail := make(chan error, 10)
			for worker := 0; worker < 10; worker++ {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					r, e := c.NewReader()
					if e != nil {
						fail <- e
						return
					}
					defer r.Close()
					var buf []byte
					for j := worker; j < len(order); j += 10 {
						i := order[j]
						v, ok, e := r.Find([]byte(fmt.Sprintf("%09d", i)), buf)
						if e != nil {
							fail <- e
							return
						}
						if !ok || crc32.ChecksumIEEE(v) != checksums[i] {
							fail <- fmt.Errorf("bad parallel value %d", i)
							return
						}
						buf = v
					}
				}(worker)
			}
			wg.Wait()
			close(fail)
			for e := range fail {
				if e != nil {
					return e
				}
			}
			return nil
		}))
		must(record("scan", func() error {
			count := 0
			e := c.Scan(nil, nil, func(k, v []byte) bool {
				count++
				i, e := strconv.Atoi(string(k))
				if e != nil || i < 0 || i >= *n || crc32.ChecksumIEEE(v) != checksums[i] {
					panic("invalid scanned value")
				}
				return true
			})
			if e == nil && count != *n {
				return fmt.Errorf("bad scan count %d", count)
			}
			return e
		}))
		if *mode == "cold" {
			must(record("trim", func() error { var e error; st, e = trim(d); return e }))
			must(record("point-hits-after-trim", hit))
			must(record("retrim", func() error { var e error; st, e = trim(d); return e }))
		} else {
			must(record("updates", func() error {
				for i := 0; i < *n/10; i++ {
					var b db.DocBuilder
					b.Int("g", int64(i%*groups))
					b.Int("i", int64(i))
					b.Int("v", 2)
					b.String("pad", padding)
					checksums[i] = crc32.ChecksumIEEE(b.Bytes())
					if e := c.Upsert([]byte(fmt.Sprintf("%09d", i)), b.Bytes()); e != nil {
						return e
					}
				}
				if e := d.Checkpoint(); e != nil {
					return e
				}
				return d.Compact(ctx)
			}))
			must(record("point-hits-after-update", hit))
		}
	} else {
		pipeline := `[{"$group":{"_id":"$g","n":{"$sum":1},"sum":{"$sum":"$v"}}}]`
		if strings.HasPrefix(*mode, "ordered-") {
			pipeline = `[{"$sort":{"g":1}},{"$group":{"_id":"$g","n":{"$sum":1},"sum":{"$sum":"$v"}}}]`
			if *mode == "ordered-limit" {
				pipeline = strings.TrimSuffix(pipeline, "]") + `,{"$limit":10}]`
			}
		}
		p, e := db.ParsePipeline([]byte(pipeline))
		must(e)
		p.SetLimits(db.Limits{MaxGroups: 1 << 22, MaxRows: 1 << 22, MaxBytes: *budget << 20})
		resultErr = record("aggregate", func() error {
			var e error
			st, e = execute(c, p, *api, *budget<<20, *spill, func(row map[string]any) bool {
				g, ok := row["_id"].(int64)
				if !ok || g < 0 || g >= int64(*groups) {
					panic(fmt.Sprint("invalid group", row))
				}
				count := int64((*n + *groups - 1 - int(g)) / *groups)
				if row["n"] != count || row["sum"] != count {
					panic(fmt.Sprint("invalid aggregate", row, count))
				}
				if seenGroups[g/64]&(uint64(1)<<(g%64)) != 0 {
					panic("duplicate output group")
				}
				seenGroups[g/64] |= uint64(1) << (g % 64)
				if strings.HasPrefix(*mode, "ordered-") && g != seen {
					panic("wrong ordered output")
				}
				seen++
				sum += count
				return true
			})
			return e
		})
		if resultErr == nil {
			want := int64(*groups)
			if *mode == "ordered-limit" {
				want = min(want, 10)
			}
			if seen != want {
				panic(fmt.Sprintf("got %d groups want %d", seen, want))
			}
			if *mode != "ordered-limit" && sum != int64(*n) {
				panic("sum mismatch")
			}
		}
	}
	var errorText string
	if resultErr != nil {
		errorText = resultErr.Error()
	}
	must(record("close", d.Close))
	out := map[string]any{"version": db.Version, "mode": *mode, "api": *api, "rows": *n, "groups": *groups, "memory_mb": *budget, "spill": *spill, "phases": phases, "stats": st, "output_groups": seen, "output_count": sum, "error": errorText, "budget_error": errors.Is(resultErr, db.ErrLimit), "background_note": "Fresh process; one GC after fixture maintenance before measurement; lifetime peaks include fixture work."}
	must(json.NewEncoder(os.Stdout).Encode(out))
}
