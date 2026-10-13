package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var flagResources = flag.Bool("resources", false, "JSON CPU/RSS/allocation comparison; exactly one engine and workload per process")

type resourcePhase struct {
	Phase         string  `json:"phase"`
	Ops           int     `json:"ops"`
	WallSeconds   float64 `json:"wall_seconds"`
	UserSeconds   float64 `json:"user_seconds"`
	SystemSeconds float64 `json:"system_seconds"`
	CPUNsPerOp    float64 `json:"cpu_ns_per_op"`
	WallNsPerOp   float64 `json:"wall_ns_per_op"`
	AverageCores  float64 `json:"average_cores"`
	RSSBytes      int64   `json:"rss_bytes"`
	PeakRSSBytes  int64   `json:"process_peak_rss_bytes_so_far"`
	Footprint     uint64  `json:"memory_footprint_bytes"`
	PeakFootprint uint64  `json:"process_peak_memory_footprint_bytes_so_far"`
	GoHeapBytes   uint64  `json:"go_heap_bytes"`
	GoAllocated   uint64  `json:"go_allocated_bytes"`
	GoAllocs      uint64  `json:"go_allocations"`
	GCCycles      uint32  `json:"gc_cycles"`
}

func resourceUsage() unix.Rusage {
	var r unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &r); err != nil {
		panic(err)
	}
	return r
}

func resourceSeconds(t unix.Timeval) float64 {
	return float64(t.Sec) + float64(t.Usec)/1e6
}

func resourcePeak(r unix.Rusage) int64 {
	// macOS reports bytes; Linux reports KiB. This is a process lifetime
	// high-water mark, never a phase-local peak or Go-only heap size.
	if runtime.GOOS == "darwin" {
		return r.Maxrss
	}
	return r.Maxrss * 1024
}

func resourceRSS() int64 {
	// Run outside measured phases. ps includes Go, native SQLite, and
	// resident file mappings, unlike runtime.MemStats. Child CPU is not
	// included in RUSAGE_SELF.
	b, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		panic(err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		panic(err)
	}
	return n * 1024
}

func measureResource(phase string, ops int, fn func() error) resourcePhase {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	u0 := resourceUsage()
	start := time.Now()
	if err := fn(); err != nil {
		panic(phase + ": " + err.Error())
	}
	d := time.Since(start).Seconds()
	u1 := resourceUsage()
	runtime.ReadMemStats(&after)
	user := resourceSeconds(u1.Utime) - resourceSeconds(u0.Utime)
	system := resourceSeconds(u1.Stime) - resourceSeconds(u0.Stime)
	footprint, peakFootprint := resourceFootprint()
	return resourcePhase{
		Phase: phase, Ops: ops, WallSeconds: d, UserSeconds: user, SystemSeconds: system,
		CPUNsPerOp: (user + system) * 1e9 / float64(ops), WallNsPerOp: d * 1e9 / float64(ops),
		AverageCores: (user + system) / d, RSSBytes: resourceRSS(), PeakRSSBytes: resourcePeak(u1),
		Footprint: footprint, PeakFootprint: peakFootprint,
		GoHeapBytes: after.HeapAlloc, GoAllocated: after.TotalAlloc - before.TotalAlloc,
		GoAllocs: after.Mallocs - before.Mallocs, GCCycles: after.NumGC - before.NumGC,
	}
}

func runResourceBench(dir string, openers map[string]opener) {
	name, workload := *flagEngines, *flagWork
	if strings.Contains(name, ",") || strings.Contains(workload, ",") || *flagReps != 1 || *flagDurable {
		panic("resources requires one engine, one workload, reps=1 and durable=false; repeat in separate processes")
	}
	if (workload != "random" && workload != "seq") || *flagN < 1 || *flagVal < 1 || *flagPar < 1 {
		panic("invalid resource workload, count, value size or readers")
	}
	op, ok := openers[name]
	if !ok {
		panic("unknown resource engine " + name)
	}
	baseline := resourceRSS()
	baselineFootprint, _ := resourceFootprint()
	keys, vals, order, misses := genWorkload(workload, *flagN, *flagVal)
	runtime.GC()
	fixtureRSS := resourceRSS()
	fixtureFootprint, _ := resourceFootprint()
	edir, err := os.MkdirTemp(dir, name+"-resources-")
	if err != nil {
		panic(err)
	}
	var db kv
	var phases []resourcePhase
	record := func(phase string, ops int, fn func() error) {
		r := measureResource(phase, ops, fn)
		phases = append(phases, r)
		fmt.Fprintf(os.Stderr, "%s %s %-18s wall=%.3fs cpu=%.3fs rss=%.1fMiB\n", name, workload, phase,
			r.WallSeconds, r.UserSeconds+r.SystemSeconds, float64(r.RSSBytes)/(1<<20))
	}
	record("open", 1, func() error { db, err = op(edir); return err })
	n, vs := *flagN, *flagVal
	record("insert", n, func() error {
		for i := 0; i < n; i++ {
			if err := db.Upsert(keys[i*16:(i+1)*16], vals[i*vs:(i+1)*vs]); err != nil {
				return err
			}
		}
		return nil
	})
	record("settle", n, db.Settle)
	var buf []byte
	check := func(rd reader, i int, dst []byte) ([]byte, error) {
		v, found, err := rd.Find(keys[i*16:(i+1)*16], dst)
		if err != nil {
			return nil, err
		}
		if !found || len(v) != vs || v[0] != byte(i) {
			return nil, fmt.Errorf("bad value at %d, found=%v len=%d", i, found, len(v))
		}
		return v, nil
	}
	record("find-hit", n, func() error {
		for _, i := range order {
			buf, err = check(db, i, buf)
			if err != nil {
				return err
			}
		}
		return nil
	})
	record("find-miss", n, func() error {
		for i := 0; i < n; i++ {
			_, found, err := db.Find(misses[i*16:(i+1)*16], buf)
			if err != nil {
				return err
			}
			if found {
				return fmt.Errorf("unexpected hit at %d", i)
			}
		}
		return nil
	})
	record("scan", n, func() error {
		seen, err := db.Iterate()
		if err != nil {
			return err
		}
		if seen != n {
			return fmt.Errorf("scan returned %d of %d", seen, n)
		}
		return nil
	})
	if !*flagNoPar {
		par := *flagPar
		readers := make([]reader, par)
		// Include connection creation in parallel CPU/wall cost; capture RSS
		// before closing them, including SQLite's per-connection page caches.
		record(fmt.Sprintf("find-hit-par%d", par), n, func() error {
			for g := range readers {
				r, err := db.NewReader()
				if err != nil {
					return err
				}
				readers[g] = r
			}
			var wg sync.WaitGroup
			errs := make([]error, par)
			for g, rd := range readers {
				wg.Add(1)
				go func(g int, rd reader) {
					defer wg.Done()
					var b []byte
					for j := g; j < n; j += par {
						b, errs[g] = check(rd, order[j], b)
						if errs[g] != nil {
							return
						}
					}
				}(g, rd)
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					return err
				}
			}
			return nil
		})
		record("readers-close", par, func() error {
			for _, r := range readers {
				if err := r.Close(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	record("close", 1, db.Close)
	runtime.KeepAlive(keys)
	runtime.KeepAlive(vals)
	runtime.KeepAlive(order)
	runtime.KeepAlive(misses)
	footprint, peakFootprint := resourceFootprint()
	output := map[string]any{
		"engine": name, "workload": workload, "n": n, "key_bytes": 16, "value_bytes": vs,
		"readers": *flagPar, "gomaxprocs": runtime.GOMAXPROCS(0), "go_version": runtime.Version(),
		"cache_mb": *flagCache, "ours_cache_mb": *flagOursMB,
		"baseline_rss_bytes": baseline, "fixture_rss_bytes": fixtureRSS,
		"baseline_memory_footprint_bytes": baselineFootprint, "fixture_memory_footprint_bytes": fixtureFootprint,
		"memory_footprint_bytes": footprint, "process_peak_memory_footprint_bytes": peakFootprint,
		"fixture_payload_bytes": n * (16 + vs + 8 + 16),
		"phases":                phases, "disk_bytes": dirSize(edir), "process_peak_rss_bytes": resourcePeak(resourceUsage()),
		"notes": "RSS/peak include fixtures, native allocations and resident mappings; macOS memory footprint additionally accounts for compressed memory, but differs in treatment of file-backed pages. Go allocation counters exclude C allocations and anonymous mapped storage (including AptevaDB page/WAL buffers). Peaks are cumulative. Async/non-fsync writes. Parallel read includes reader setup. Settle is explicit engine maintenance.",
	}
	if err := json.NewEncoder(os.Stdout).Encode(output); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(filepath.Clean(edir)); err != nil {
		panic(err)
	}
}
