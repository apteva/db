# AptevaDB memory and throughput follow-up

This unreleased candidate batches checkpoint writes, retains tree descent between scan leaves, bounds ordinary WAL backlog at 4 MiB, releases burst log storage at replay barriers, and retires consumed old-file translations during private compaction. The default memtable budget is CacheBytes/4, with two active budgets allowed while another table applies. Writable-page checkpoint pressure starts at 1/2 of the cache target. Explicit memtable budgets remain configurable. The on-disk format remains 10.

Startup cleanup also checkpoints synchronously before workers start. This fixes a recovery deadlock exposed by the interrupted-index-build tests; waiting writers recheck closure and failure before continuing.

## Method

48 serial fresh-process fixtures: four datasets/insertion orders, four engine/build entries, three trials each, interleaved in a seeded order. The candidate, original alpha.6, Pebble 2.1.7 and C SQLite 3.32.2 all run in this new cohort. Earlier candidates and exploratory tuning runs are not pooled. All rows, scans and expected values are validated.

Apple M1 Pro, ten logical CPUs, 16 GiB, Go 1.25.1. The runner retains per-trial process/background activity, timestamps, binary hashes and source patches. The source archive and provenance file identify the final source. No agent builds, tests, profiling or Linux VM run during measurement. System storage/indexing work and user applications remained active; these are measurements under recorded contention, not an idle-machine performance guarantee. Small median differences should not be treated as statistically established.


Fixtures stay alive in every process and are included in RSS and physical footprint. CPU is process user plus system time across engine and maintenance threads. RSS includes resident file mappings; macOS physical footprint accounts for shared/compressed pages differently. Cache options are 64 MiB, not equal total-memory caps. Pebble additionally uses a 64 MiB memtable, default compression and Bloom filters. SQLite uses a WITHOUT ROWID BLOB table, a cache per connection, ten connections for parallel reads, and WAL with synchronous OFF. Writes are per-row atomic asynchronous upserts. Load/update/delete/mixed totals include subsequent checkpoint and full compaction; raw write loops and maintenance remain separately available in runs.json. Reads repeat two full hit/miss/ten-reader passes; scans consume full values over five passes. No cold OS-cache claim.

One incomplete SQLite fixture was stopped by the disk-space guard and removed before completing its insert phase. Its partial measurements are excluded. The 24 completed million-row trials were preserved; only the incomplete fixture was retried. The guard reserve was increased before resumption; completed child measurement phases are unchanged. See disk-guard-resume.json.

## Peak process memory

| Dataset / order | Original RSS MiB | Candidate RSS MiB | Pebble RSS MiB | SQLite RSS MiB | Candidate vs Pebble |
|---|---:|---:|---:|---:|---:|
| 100,000 × 1024 B / random | 714.4 | 260.3 | 257.4 | 1012.5 | +1.1% |
| 250,000 × 1024 B / random | 1531.3 | 597.2 | 541.7 | 1147.9 | +10.3% |
| 1,000,000 × 100 B / random | 886.0 | 321.4 | 321.7 | 1024.8 | -0.1% |
| 1,000,000 × 100 B / seq | 902.5 | 317.2 | 319.7 | 1026.0 | -0.8% |

| Dataset / order | Original footprint MiB | Candidate footprint MiB | Pebble footprint MiB | SQLite footprint MiB |
|---|---:|---:|---:|---:|
| 100,000 × 1024 B / random | 476.9 | 221.6 | 249.1 | 1022.5 |
| 250,000 × 1024 B / random | 1043.3 | 377.8 | 555.6 | 1187.8 |
| 1,000,000 × 100 B / random | 614.7 | 256.0 | 371.1 | 1164.1 |
| 1,000,000 × 100 B / seq | 708.5 | 216.8 | 329.7 | 1164.3 |

All observations and min/max ranges are retained in cpu-summary.json and dataset runs.json. These are process lifetime peaks, not Go heap limits.

## CPU and elapsed time versus original alpha.6

### 100,000 × 1024 B / random

| Operation | Original CPU s/M | Candidate CPU s/M | CPU change | Original wall ms | Candidate wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 8.105 | 7.051 | -13.0% | 1076.86 | 2024.57 | +88.0% |
| Point read hit | 0.763 | 0.781 | +2.3% | 153.28 | 161.50 | +5.4% |
| Point read miss | 0.327 | 0.328 | +0.3% | 65.28 | 72.27 | +10.7% |
| Full value scan | 0.096 | 0.079 | -17.5% | 45.24 | 39.47 | -12.7% |
| 10-reader point hits | 1.599 | 0.753 | -52.9% | 87.85 | 34.21 | -61.1% |
| Update + maintenance | 53.629 | 24.001 | -55.2% | 879.69 | 734.73 | -16.5% |
| 80% read / 20% update + maintenance | 6.030 | 4.264 | -29.3% | 992.98 | 1023.37 | +3.1% |
| Delete + maintenance | 46.537 | 20.328 | -56.3% | 668.79 | 626.26 | -6.4% |
| Scan after delete | 0.094 | 0.075 | -20.1% | 43.99 | 33.75 | -23.3% |
### 250,000 × 1024 B / random

| Operation | Original CPU s/M | Candidate CPU s/M | CPU change | Original wall ms | Candidate wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 11.081 | 9.701 | -12.5% | 3368.36 | 4951.07 | +47.0% |
| Point read hit | 0.861 | 0.781 | -9.3% | 429.91 | 390.27 | -9.2% |
| Point read miss | 0.421 | 0.333 | -20.9% | 210.40 | 166.55 | -20.8% |
| Full value scan | 0.090 | 0.072 | -20.4% | 112.94 | 89.60 | -20.7% |
| 10-reader point hits | 2.627 | 0.829 | -68.5% | 349.94 | 55.31 | -84.2% |
| Update + maintenance | 54.676 | 27.837 | -49.1% | 2155.88 | 1430.22 | -33.7% |
| 80% read / 20% update + maintenance | 7.576 | 5.235 | -30.9% | 2349.01 | 2038.92 | -13.2% |
| Delete + maintenance | 45.215 | 19.309 | -57.3% | 1537.42 | 937.36 | -39.0% |
| Scan after delete | 0.087 | 0.072 | -17.7% | 98.13 | 80.97 | -17.5% |
### 1,000,000 × 100 B / random

| Operation | Original CPU s/M | Candidate CPU s/M | CPU change | Original wall ms | Candidate wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 3.522 | 2.626 | -25.4% | 4224.38 | 3720.91 | -11.9% |
| Point read hit | 0.788 | 0.742 | -5.8% | 1576.35 | 1487.58 | -5.6% |
| Point read miss | 0.530 | 0.453 | -14.7% | 1068.18 | 918.93 | -14.0% |
| Full value scan | 0.021 | 0.019 | -10.6% | 104.92 | 93.54 | -10.9% |
| 10-reader point hits | 1.885 | 0.760 | -59.7% | 1145.22 | 246.70 | -78.5% |
| Update + maintenance | 11.709 | 6.698 | -42.8% | 1777.03 | 1254.22 | -29.4% |
| 80% read / 20% update + maintenance | 2.005 | 1.851 | -7.7% | 2662.05 | 2120.76 | -20.3% |
| Delete + maintenance | 9.238 | 5.920 | -35.9% | 1233.23 | 1308.41 | +6.1% |
| Scan after delete | 0.021 | 0.019 | -7.4% | 93.02 | 85.98 | -7.6% |
### 1,000,000 × 100 B / seq

| Operation | Original CPU s/M | Candidate CPU s/M | CPU change | Original wall ms | Candidate wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 2.114 | 1.348 | -36.2% | 2751.48 | 1828.72 | -33.5% |
| Point read hit | 1.124 | 1.122 | -0.2% | 2342.41 | 2378.51 | +1.5% |
| Point read miss | 0.965 | 0.867 | -10.1% | 1959.69 | 1751.79 | -10.6% |
| Full value scan | 0.021 | 0.019 | -11.4% | 110.84 | 94.39 | -14.8% |
| 10-reader point hits | 2.127 | 1.077 | -49.4% | 1229.04 | 294.07 | -76.1% |
| Update + maintenance | 10.485 | 8.582 | -18.1% | 1746.90 | 1452.52 | -16.9% |
| 80% read / 20% update + maintenance | 2.981 | 2.111 | -29.2% | 5124.88 | 2328.41 | -54.6% |
| Delete + maintenance | 11.298 | 6.507 | -42.4% | 2021.71 | 1292.36 | -36.1% |
| Scan after delete | 0.030 | 0.024 | -17.9% | 142.30 | 110.25 | -22.5% |

Negative changes mean improvement. CPU is normalized per million logical operations; elapsed uses actual operation counts. Comparator CPU/throughput results are also available for every operation in cpu-summary.json/csv.

## Remaining gaps

- 100,000 × 1024 B / random: peak RSS median 1.1% above Pebble.
- 250,000 × 1024 B / random: peak RSS median 10.3% above Pebble.
- 100,000 × 1024 B / random, Load + maintenance: CPU -13.0%, elapsed +88.0% versus original alpha.6.
- 100,000 × 1024 B / random, Point read hit: CPU +2.3%, elapsed +5.4% versus original alpha.6.
- 100,000 × 1024 B / random, Point read miss: CPU +0.3%, elapsed +10.7% versus original alpha.6.
- 100,000 × 1024 B / random, 80% read / 20% update + maintenance: CPU -29.3%, elapsed +3.1% versus original alpha.6.
- 250,000 × 1024 B / random, Load + maintenance: CPU -12.5%, elapsed +47.0% versus original alpha.6.
- 1,000,000 × 100 B / random, Delete + maintenance: CPU -35.9%, elapsed +6.1% versus original alpha.6.
- 1,000,000 × 100 B / seq, Point read hit: CPU -0.2%, elapsed +1.5% versus original alpha.6.

The complete matrix is retained regardless of outcomes. See validation.json for checks and their exact source scope. No release was published.

## Reproduction

```sh
cd bench
go build -o /tmp/apteva-memory-followup .
python3 run_memory_comparison.py --binary /tmp/apteva-memory-followup --baseline-binary /path/to/original-alpha6-benchmark --out /tmp/apteva-memory-followup-results
python3 summarize_large_cpu.py /tmp/apteva-memory-followup-results
python3 report_memory_followup.py /tmp/apteva-memory-followup-results --dirty-fraction 1/2
```

