# Native memory optimization validation

Three serial fresh-process trials for each engine, fixture and insertion order. The unchanged alpha.6 binary is interleaved with the candidate, Pebble and SQLite C. All successful trials are retained; medians and ranges are available in the accompanying JSON/CSV. CPU is process user plus system time, including engine, GC and maintenance threads. Memory is process lifetime peak RSS and macOS physical footprint, including workload fixtures and native/mapped allocations.

Hardware: Apple M1 Pro, ten logical CPUs, 16 GiB RAM; Go 1.25.1. See dataset metadata for OS version, binary SHA256, working-tree patch, dependency versions, and per-run background activity. These are embedded raw key/value operations with binary 16-byte keys and random full-value payloads. Every phase validates results. Read hits/misses and ten-reader hits each make two passes; full-value scans make five. Writes use per-row atomic upserts and asynchronous/non-fsync durability.

All engines receive a 64 MiB cache option. This is not an equal total-process memory cap: SQLite has a cache per connection, Pebble also has a 64 MiB memtable, and the candidate derives a 32 MiB native memtable budget including metadata and replaced records. Before the change, AptevaDB derived 32 MiB of logical payload and allowed its active buffer to grow to four budgets. The candidate permits a two-budget active burst while another table applies, then uses backpressure. Individual acknowledged write loops and subsequent maintenance are preserved separately in raw phase results; operation summaries include maintenance to account for completed engine work.

The SQLite C binding bundles SQLite 3.32.2 and stores BLOBs in a WITHOUT ROWID table. This is a version-specific result, not a claim about the latest SQLite. Pebble uses Bloom filters and its default compression. No cold OS-cache claim is made. File-backed memory and compressed memory are charged differently by RSS and macOS physical footprint; report both. Go allocation counts exclude native memory. Diagnostic/profiling runs are separate and are not included in this matrix.

## Results and remaining gaps

All 48 trials passed their value/count checks. These results support a substantial memory improvement, but do not meet an all-metrics/no-regression acceptance criterion.

- 100,000 × 1024 B (random): peak RSS 654.8 → 278.5 MiB (-57.5%), versus Pebble 268.9 MiB. Physical footprint 227.7 versus Pebble 255.6 MiB.
  Remaining positive median change: Point read hit, CPU +5.1% versus the unchanged build.
  Remaining positive median change: Point read hit, elapsed time +5.2% versus the unchanged build.
- 250,000 × 1024 B (random): peak RSS 1544.8 → 631.7 MiB (-59.1%), versus Pebble 541.8 MiB. Physical footprint 435.1 versus Pebble 528.6 MiB.
  Remaining positive median change: Load + maintenance, elapsed time +0.7% versus the unchanged build.
- 1,000,000 × 100 B (random): peak RSS 922.4 → 342.4 MiB (-62.9%), versus Pebble 379.5 MiB. Physical footprint 300.2 versus Pebble 372.5 MiB.
  Remaining positive median change: Load + maintenance, elapsed time +2.3% versus the unchanged build.
- 1,000,000 × 100 B (seq): peak RSS 805.4 → 357.0 MiB (-55.7%), versus Pebble 326.4 MiB. Physical footprint 252.8 versus Pebble 359.8 MiB.

Three repetitions are insufficient to establish statistical significance for small differences. Keep the complete ranges; do not interpret a lower CPU median as proof of lower elapsed time. Peak RSS remains above Pebble on the sequential million-row and both 1-KiB cases. Physical footprint is below Pebble in all four cases.

The macOS full suite passed (213.758 s), final ownership/cache/compaction race checks passed (63.778 s), and final Linux arm64 ownership/cache/compaction checks passed (26.412 s). The earlier memory implementation also passed the full race and Linux suites; see validation.json for their exact scope and later fixes. Vet and Linux/Darwin amd64 cross-builds passed. No release or new tag has been published.

## Peak process memory

### 100,000 rows × 1024 B, random insertion

| Engine | Peak RSS MiB (median; range) | Peak footprint MiB (median; range) |
|---|---:|---:|
| AptevaDB before | 654.8; 642.8–690.3 | 409.2; 402.2–447.1 |
| AptevaDB candidate | 278.5; 274.7–279.5 | 227.7; 224.1–231.8 |
| Pebble 2.1.7 | 268.9; 267.4–268.9 | 255.6; 254.2–255.6 |
| SQLite C 3.32.2 | 1008.5; 982.9–1012.6 | 1022.4; 1022.3–1022.5 |

| Operation | Before CPU s/M | Candidate CPU s/M | Pebble CPU s/M | SQLite CPU s/M | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 6.219 | 5.053 | 8.845 | 43.943 | -18.7% |
| Point read hit | 0.648 | 0.681 | 8.886 | 15.583 | +5.1% |
| Point read miss | 0.282 | 0.242 | 5.431 | 14.892 | -14.2% |
| Full value scan | 0.084 | 0.081 | 0.541 | 2.155 | -3.8% |
| 10-reader point hits | 1.359 | 0.713 | 25.165 | 204.972 | -47.5% |
| Update + maintenance | 28.402 | 19.127 | 63.123 | 45.253 | -32.7% |
| 80% read / 20% update + maintenance | 3.718 | 2.721 | 12.110 | 22.875 | -26.8% |
| Delete + maintenance | 23.738 | 17.417 | 32.971 | 35.003 | -26.6% |
| Scan after delete | 0.085 | 0.083 | 0.526 | 2.226 | -1.6% |

| Operation | Before wall ms | Candidate wall ms | Pebble wall ms | SQLite wall ms | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 876.72 | 853.04 | 1638.58 | 4341.93 | -2.7% |
| Point read hit | 128.97 | 135.63 | 1774.26 | 3094.57 | +5.2% |
| Point read miss | 56.27 | 48.30 | 1084.78 | 2951.52 | -14.2% |
| Full value scan | 42.08 | 40.42 | 270.03 | 1086.10 | -4.0% |
| 10-reader point hits | 76.77 | 22.82 | 953.56 | 5348.85 | -70.3% |
| Update + maintenance | 429.64 | 378.09 | 1249.85 | 460.58 | -12.0% |
| 80% read / 20% update + maintenance | 501.94 | 464.11 | 1745.40 | 2299.69 | -7.5% |
| Delete + maintenance | 356.63 | 331.71 | 649.81 | 347.53 | -7.0% |
| Scan after delete | 37.94 | 37.27 | 236.38 | 1003.32 | -1.8% |

Negative change means an improvement. Throughput, average cores, allocations, every observation, and raw acknowledged-write/maintenance phases are retained in the data files. Wall-time totals use the recorded operation counts (two point-read passes and five scan passes), not one million operations.

### 250,000 rows × 1024 B, random insertion

| Engine | Peak RSS MiB (median; range) | Peak footprint MiB (median; range) |
|---|---:|---:|
| AptevaDB before | 1544.8; 1411.5–1549.0 | 949.4; 936.7–962.4 |
| AptevaDB candidate | 631.7; 625.2–644.1 | 435.1; 428.7–447.9 |
| Pebble 2.1.7 | 541.8; 540.9–556.9 | 528.6; 527.8–543.8 |
| SQLite C 3.32.2 | 1182.8; 1165.2–1199.1 | 1222.4; 1222.4–1222.7 |

| Operation | Before CPU s/M | Candidate CPU s/M | Pebble CPU s/M | SQLite CPU s/M | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 7.521 | 6.159 | 15.167 | 47.909 | -18.1% |
| Point read hit | 0.761 | 0.686 | 8.074 | 19.705 | -9.9% |
| Point read miss | 0.355 | 0.266 | 7.801 | 18.961 | -25.3% |
| Full value scan | 0.085 | 0.075 | 0.520 | 2.176 | -11.3% |
| 10-reader point hits | 1.784 | 0.795 | 22.358 | 227.787 | -55.4% |
| Update + maintenance | 34.550 | 17.784 | 43.874 | 49.851 | -48.5% |
| 80% read / 20% update + maintenance | 5.332 | 3.605 | 12.645 | 26.738 | -32.4% |
| Delete + maintenance | 30.231 | 16.950 | 32.897 | 38.527 | -43.9% |
| Scan after delete | 0.085 | 0.084 | 0.530 | 2.263 | -1.8% |

| Operation | Before wall ms | Candidate wall ms | Pebble wall ms | SQLite wall ms | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 2264.54 | 2280.92 | 6290.80 | 11834.63 | +0.7% |
| Point read hit | 378.94 | 341.41 | 4016.97 | 9816.98 | -9.9% |
| Point read miss | 177.35 | 132.59 | 3894.10 | 9384.15 | -25.2% |
| Full value scan | 105.80 | 93.74 | 649.01 | 2728.88 | -11.4% |
| 10-reader point hits | 275.37 | 52.08 | 1984.72 | 13935.39 | -81.1% |
| Update + maintenance | 1075.57 | 775.75 | 2161.03 | 1240.30 | -27.9% |
| 80% read / 20% update + maintenance | 1582.50 | 1341.44 | 4366.66 | 6747.65 | -15.2% |
| Delete + maintenance | 954.09 | 755.75 | 1588.22 | 954.77 | -20.8% |
| Scan after delete | 95.69 | 93.98 | 594.80 | 2547.77 | -1.8% |

Negative change means an improvement. Throughput, average cores, allocations, every observation, and raw acknowledged-write/maintenance phases are retained in the data files. Wall-time totals use the recorded operation counts (two point-read passes and five scan passes), not one million operations.

### 1,000,000 rows × 100 B, random insertion

| Engine | Peak RSS MiB (median; range) | Peak footprint MiB (median; range) |
|---|---:|---:|
| AptevaDB before | 922.4; 918.1–929.3 | 607.0; 596.7–616.7 |
| AptevaDB candidate | 342.4; 342.2–344.4 | 300.2; 299.3–301.1 |
| Pebble 2.1.7 | 379.5; 354.1–385.7 | 372.5; 372.5–373.0 |
| SQLite C 3.32.2 | 1172.8; 1172.6–1207.4 | 1164.4; 1164.3–1165.0 |

| Operation | Before CPU s/M | Candidate CPU s/M | Pebble CPU s/M | SQLite CPU s/M | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 2.144 | 1.706 | 3.686 | 18.490 | -20.4% |
| Point read hit | 0.667 | 0.635 | 7.882 | 3.445 | -4.8% |
| Point read miss | 0.419 | 0.341 | 7.525 | 2.986 | -18.7% |
| Full value scan | 0.019 | 0.019 | 0.099 | 0.397 | -3.9% |
| 10-reader point hits | 1.531 | 0.735 | 21.893 | 16.524 | -52.0% |
| Update + maintenance | 5.287 | 3.151 | 8.678 | 14.470 | -40.4% |
| 80% read / 20% update + maintenance | 1.338 | 1.275 | 8.602 | 5.703 | -4.7% |
| Delete + maintenance | 4.646 | 3.015 | 7.097 | 13.004 | -35.1% |
| Scan after delete | 0.019 | 0.018 | 0.099 | 0.406 | -4.9% |

| Operation | Before wall ms | Candidate wall ms | Pebble wall ms | SQLite wall ms | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 1876.84 | 1920.68 | 4071.36 | 18453.98 | +2.3% |
| Point read hit | 1329.53 | 1266.29 | 15782.97 | 6901.60 | -4.8% |
| Point read miss | 834.37 | 680.21 | 15068.98 | 5976.22 | -18.5% |
| Full value scan | 96.54 | 92.78 | 492.54 | 1983.90 | -3.9% |
| 10-reader point hits | 863.35 | 163.55 | 7823.85 | 4231.49 | -81.1% |
| Update + maintenance | 647.26 | 517.56 | 1412.19 | 1441.50 | -20.0% |
| 80% read / 20% update + maintenance | 1468.50 | 1380.40 | 9233.69 | 5678.16 | -6.0% |
| Delete + maintenance | 548.87 | 471.81 | 1036.23 | 1304.54 | -14.0% |
| Scan after delete | 87.06 | 82.59 | 446.56 | 1827.16 | -5.1% |

Negative change means an improvement. Throughput, average cores, allocations, every observation, and raw acknowledged-write/maintenance phases are retained in the data files. Wall-time totals use the recorded operation counts (two point-read passes and five scan passes), not one million operations.

### 1,000,000 rows × 100 B, seq insertion

| Engine | Peak RSS MiB (median; range) | Peak footprint MiB (median; range) |
|---|---:|---:|
| AptevaDB before | 805.4; 774.9–854.1 | 535.3; 503.8–571.4 |
| AptevaDB candidate | 357.0; 353.8–358.3 | 252.8; 243.3–281.3 |
| Pebble 2.1.7 | 326.4; 326.2–372.1 | 359.8; 358.8–360.3 |
| SQLite C 3.32.2 | 1172.3; 1158.2–1174.3 | 1164.2; 1164.2–1164.2 |

| Operation | Before CPU s/M | Candidate CPU s/M | Pebble CPU s/M | SQLite CPU s/M | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 1.008 | 0.668 | 1.998 | 9.059 | -33.7% |
| Point read hit | 0.914 | 0.890 | 7.509 | 3.464 | -2.6% |
| Point read miss | 0.794 | 0.748 | 7.398 | 3.077 | -5.8% |
| Full value scan | 0.020 | 0.019 | 0.100 | 0.389 | -2.3% |
| 10-reader point hits | 1.887 | 1.040 | 21.330 | 16.170 | -44.9% |
| Update + maintenance | 5.532 | 3.526 | 8.119 | 14.718 | -36.3% |
| 80% read / 20% update + maintenance | 1.594 | 1.467 | 8.178 | 5.927 | -7.9% |
| Delete + maintenance | 4.721 | 3.072 | 6.618 | 13.255 | -34.9% |
| Scan after delete | 0.020 | 0.019 | 0.100 | 0.397 | -7.0% |

| Operation | Before wall ms | Candidate wall ms | Pebble wall ms | SQLite wall ms | Candidate versus before |
|---|---:|---:|---:|---:|---:|
| Load + maintenance | 938.07 | 784.59 | 2381.64 | 9010.26 | -16.4% |
| Point read hit | 1821.17 | 1773.28 | 14991.58 | 6914.17 | -2.6% |
| Point read miss | 1581.33 | 1492.28 | 14866.87 | 6128.36 | -5.6% |
| Full value scan | 97.95 | 95.74 | 502.33 | 1943.48 | -2.3% |
| 10-reader point hits | 995.97 | 228.34 | 8025.70 | 4072.30 | -77.1% |
| Update + maintenance | 712.93 | 607.45 | 1306.95 | 1463.58 | -14.8% |
| 80% read / 20% update + maintenance | 1704.07 | 1614.84 | 8809.51 | 5913.61 | -5.2% |
| Delete + maintenance | 575.42 | 467.37 | 1005.63 | 1335.29 | -18.8% |
| Scan after delete | 90.31 | 84.01 | 448.53 | 1781.05 | -7.0% |

Negative change means an improvement. Throughput, average cores, allocations, every observation, and raw acknowledged-write/maintenance phases are retained in the data files. Wall-time totals use the recorded operation counts (two point-read passes and five scan passes), not one million operations.

## Implementation

Stable file segments avoid duplicate mappings when the file grows. Writable pages, metadata descriptors and memtable data use explicitly owned native storage; snapshots/cursors and checkpoint I/O keep references until retirement. Both cache modes stream compaction output. Checkpoint vectors/scratch storage are reused. Verified clean reads avoid descriptor allocations and read-side CLOCK writes. Old file-page translations are reclaimed only when live views/I/O no longer need them, preserving file contents and current warm data. Small WAL buffers remain on the Go heap; larger bursts use reclaimable storage.

Testing exposed a compaction WAL replay/deletion race; the fix pins the replay range and serializes compactions and rotations. Catch-up runs without monopolizing the old checkpointer, then uses a brief memory-only catch-up/final swap. Bounded buffers can still apply backpressure when ingestion outruns checkpoint progress. This is not a hard process-memory limit or a guarantee of zero write latency.

## Reproduction

```sh
cd bench
go build -o /tmp/apteva-memory-candidate .
python3 run_memory_comparison.py --binary /tmp/apteva-memory-candidate --baseline-binary /path/to/unchanged-alpha6-benchmark --out /tmp/apteva-memory-results
python3 summarize_large_cpu.py /tmp/apteva-memory-results
python3 report_memory_comparison.py /tmp/apteva-memory-results
```

The original benchmark binary is identified by SHA256 in baseline-origin.json and matches the prior CPU benchmark archive. The candidate is the recorded working tree based on 65cedd3, not a newly published release. Source patches/copies and compiler/module metadata accompany raw measurements. Compilers/tests/profilers and the temporary Linux test VM were stopped before measurement.

