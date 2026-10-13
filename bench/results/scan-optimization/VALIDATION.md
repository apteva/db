# Scan optimization validation

Unreleased `main` implementation at `4b5405287229d2c06215b275851237268d9955af`. The published alpha.3 tag is unchanged. Format 8 and the public APIs remain compatible.

Sequential cursor steps avoid arbitrary-seek position comparisons, advance the known leaf slot when no buffered entries participate, and reuse one private value buffer. Arbitrary resume seeks still invalidate unrelated cached positions. Crossing a collection boundary clears the cache, and empty boundary values preserve buffer reuse.

Callback scans copy contiguous inline keys/values together, use three-word cell references, and call the callback directly when the buffered merge is empty. The final implementation preserves the original narrow-range end checks and releases the reader lock before callbacks.

## Measurement conditions

Apple M1 Pro, 16 GiB RAM, darwin/arm64, Go 1.25.1, GOMAXPROCS=10. Standard workloads use 1,000,000 16-byte keys and 100-byte values; asynchronous writes, native default resident cache, Pebble/SQLite 512 MiB configured caches (SQLite per connection). Reads follow native checkpoints, Pebble compactions and SQLite settling. These are steady read-layout workloads, not concurrent write/read throughput tests.

Seven paired native processes use the identical comparison harness linked against alpha.3 and the final library, alternating execution order. The API benchmarks use an identical copied harness for alpha.2, alpha.3 and final, three fresh processes per library, rotating order, one-second measurements and model-checked keys/values. Ranges return 1, 8 or 128 rows; sustained point reads use 1,000,000 random keys. Aggregate/join harnesses retain their existing best-of-five inner-query method, with every outer process retained.

No local builds, tests or soak processes ran during reported benchmark processes. Other user apps, services, builds and tests remained active; every process has a background CPU snapshot. This machine was not isolated. Min/max and every trial are retained, including severe outliers. Measurements do not establish universal regression-free performance.

## Final native cursor walks

Median ns/operation; scan means a counted `Cursor.Next` walk. Other phases are ns/key. Standard output rounds to whole nanoseconds.

| Workload/phase | alpha.3 | Final | Change | alpha.3 range | Final range |
|---|---:|---:|---:|---:|---:|
| random/insert | 777 | 844 | +8.6% | 740–1,205 | 751–1,318 |
| random/settle | 1,432 | 1,172 | -18.2% | 952–2,688 | 878–2,436 |
| random/find-hit | 661 | 714 | +8.0% | 645–922 | 662–1,791 |
| random/find-miss | 475 | 474 | -0.2% | 403–521 | 435–642 |
| random/scan | 49 | 38 | -22.4% | 48–53 | 38–39 |
| random/find-hit-par10 | 109 | 91 | -16.5% | 88–212 | 84–186 |
| seq/insert | 196 | 193 | -1.5% | 184–206 | 188–336 |
| seq/settle | 878 | 751 | -14.5% | 753–935 | 672–1,844 |
| seq/find-hit | 988 | 1,026 | +3.8% | 950–1,662 | 938–1,435 |
| seq/find-miss | 924 | 892 | -3.5% | 876–1,499 | 860–1,534 |
| seq/scan | 50 | 39 | -22.0% | 49–51 | 38–50 |
| seq/find-hit-par10 | 166 | 171 | +3.0% | 124–185 | 129–305 |

Cursor scan time falls from 49 to 38 ns/row for random-built trees and 50 to 39 ns/row for sequential-built trees (about 22%). Random insert and hit medians are 8.6% and 8.0% slower; sequential hits and parallel hits are 3.8% and 3.0% slower. Ranges overlap and background outliers are large. Those slower medians are disclosed rather than attributed definitively to either the engine or CPU activity. No point-read or ordinary insert algorithm was changed.

## Final API measurements

Iteration values are ns/row over 100,000 rows; ranges are ns/query; point reads are ns/read. These include validation overhead and are different operation mixes from the standard counted walks.

| API | alpha.2 median | alpha.3 median | Final median | Final range |
|---|---:|---:|---:|---:|
| RawIteration/Scan | 23.54 | 24.77 | 23.82 | 17.43–25.95 |
| RawIteration/Cursor | 50.7 | 50.87 | 40.78 | 39.37–45.42 |
| RawRange/1 | 639.7 | 636.6 | 592.4 | 562.1–927.2 |
| RawRange/8 | 1,289 | 1,771 | 1,344 | 1,150–3,594 |
| RawRange/128 | 9,913 | 10,436 | 10,314 | 9,006–24,407 |
| ReaderSteady/Single | 763.8 | 797.2 | 869.5 | 791.5–928.3 |
| ReaderSteady/Parallel | 107 | 124.8 | 154.8 | 103.2–517.1 |

The callback-scan final median is only modestly better in this noisy series; its range is 17.43–25.95 ns/row. Sustained single/parallel reader medians are slower too, with very large parallel outliers. Small-range and point-read timings need a quiet workstation before making tighter claims.

Go allocations per complete 100,000-row callback scan fall from 78,608 to 74,512 bytes and 28 to 27 allocations. Cursor walks fall from 99,608 to 99,352 bytes and 1,551 to 1,549 allocations. These are Go allocation counts, not peak resident memory or total C/mmap memory.

## Final four-engine comparison

One fresh final process, 1,000,000 rows per workload; ns/row for counted cursor walks. This table is a single-trial comparison. Three earlier candidate four-engine processes are also retained.

| Engine | Random-built scan | Sequential-built scan |
|---|---:|---:|
| ours | 41 | 38 |
| pebble | 56 | 54 |
| sqlite-c | 128 | 115 |
| sqlite-go | 105 | 91 |

## Wider feature validation

The first candidate ran three paired outer trials of compatibility, 1,500-byte primary keys with 3,000-byte indexed values, aggregation, joins, computed fields and computed collections against the existing comparison engines. All completed their correctness checks. The final source then ran every one of those modes again; all passed. Its compatibility/wide-index measurements are in `validation.json`; complete aggregate/join/computed tables are in the raw stdout files. Short single runs are checks, not evidence that every query got faster.

The binary-search end-bound candidate was discarded to keep narrow-range comparisons unchanged. Its favorable bulk-scan results are preliminary, not substituted for the final measurements. An intermediate run additionally checked exhausted-cursor cache invalidation. All preliminary, intermediate and final observations are retained.

## Correctness and build checks

- Full local `go test -race ./...`: passed, 364.394 s. Local `CURSOR_STRESS=2m SOAK=2m go test -race -run 'TestCursorStress|TestSoak' -timeout 10m .`: passed, 248.765 s. These were before the final empty-value buffer-retention guard; all cursor/scan/isolation race tests were rerun after that adjustment and passed, 15.080 s.
- Root vet/build and benchmark-module vet/build: passed.
- New regressions cover forward/reverse resume at the last valid position after collection exhaustion, mixed-size inline/overflow values, cursor value-buffer isolation, frozen buffered values, bounded scans and repeated snapshot reads.
- Exact source commit [CI run 38441188141](https://github.com/apteva/db/actions/runs/38441188141): Linux and macOS vet/build/unit tests and full race suites passed; 45-second document, pipeline, page and wide-page-split fuzz runs passed.

## Evidence and reproduction

128 fresh benchmark processes are retained in `raw/`, including six early screening processes. `validation.json` retains parsed samples, medians and ranges. `raw/finalized-source.json` fingerprints the measured runtime/test source; the files match the implementation commit. Binaries were built from that source before commit stamping; the source was then verified unchanged. Raw runners/commands/build paths are sanitized; binary hashes and complete outputs are retained.

To rerun API measurements on this source:

```sh
GOMAXPROCS=10 go test -run '^$' -bench 'BenchmarkRaw(Iteration|Range)|BenchmarkReaderSteady' -benchtime=1s -count=3 .
cd bench
go build -o /tmp/apteva-scan-bench .
/tmp/apteva-scan-bench -n 1000000 -vsize 100 -engines ours,pebble,sqlite-c,sqlite-go -workloads random,seq -par 10 -reps 1
```

For paired libraries, archive each baseline commit, copy the current root benchmark test files into each archive, and compile `go test -c` binaries before measurement. For the standard harness, build identical benchmark-module sources with only their `github.com/apteva/db` replace target changed. Run serial fresh processes/directories, rotate order and retain all outputs. Avoid compiler/test CPU load during measurements.
