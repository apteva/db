# AptevaDB memory optimization: final validation

Generic memory reclamation substantially reduces process peaks. The final code also warms the replacement file after releasing the old mapping, correcting the first-read fault cost found in the initial candidate. This is an unreleased working-tree change; the on-disk format stays 10.

## Method and provenance

The first matrix contains 48 serial fresh-process trials: unchanged alpha.6, the memory candidate, Pebble and SQLite C, three trials per dataset/order. After fixing post-compaction warm-up, a second matrix repeats all four dataset/order combinations for the final native candidate and unchanged alpha.6, again interleaved with three fresh processes each (24 trials). Competitor columns come from the completed first matrix. The two candidate versions and baseline cohorts are kept separate; they are not pooled or cherry-picked. This is not a claim that the final code was measured simultaneously with the competitors.

Apple M1 Pro, ten logical CPUs, 16 GiB; Go 1.25.1. OS/compiler/dependency versions, source patches/archive hashes, binary hashes, timestamps and per-run background activity are retained. Compilers, tests, profiling and the temporary Linux VM were stopped during measurement. All 72 reportable trials validate values, scans and row counts. Diagnostic/tuning trials are separate.

Each process includes identical retained workload fixtures. CPU means process user plus system time across all engine/GC/maintenance threads. RSS and macOS physical footprint include heap, native/mapped memory and fixtures. These metrics charge file-backed and compressed memory differently; neither is a Go allocation counter. Read phases repeat two hit/miss/ten-reader passes; scans consume full values in five passes. Raw 16-byte binary keys and random 100-byte/1-KiB payloads are used. Writes are per-row atomic asynchronous upserts; totals include the subsequent checkpoint/full compaction. A lower CPU median is not necessarily a lower elapsed-time median.

All engines get a 64 MiB cache option, which is not a total-process cap. Native defaults to a 32 MiB memtable budget accounting for native record/metadata storage and overwritten values, with an active burst of up to two budgets during apply. Before the change the budget counted only logical payload and allowed four active budgets. Pebble also has a 64 MiB memtable and default compression/Bloom filters. SQLite has a cache per connection, ten connections for parallel reads, WAL/synchronous OFF, and a WITHOUT ROWID BLOB table. The C binding bundles SQLite 3.32.2; these are version-specific results. There is no cold OS-cache claim or equal total-memory-cap claim.

## Peak process memory

| Dataset / insertion | Alpha.6 RSS MiB | Final RSS MiB | Pebble RSS MiB | SQLite RSS MiB | Native change |
|---|---:|---:|---:|---:|---:|
| 100,000 × 1024 B / random | 678.7 | 280.0 | 268.9 | 1008.5 | -58.7% |
| 250,000 × 1024 B / random | 1583.4 | 627.2 | 541.8 | 1182.8 | -60.4% |
| 1,000,000 × 100 B / random | 936.6 | 346.2 | 379.5 | 1172.8 | -63.0% |
| 1,000,000 × 100 B / seq | 819.2 | 358.6 | 326.4 | 1172.3 | -56.2% |

| Dataset / insertion | Alpha.6 footprint MiB | Final footprint MiB | Pebble footprint MiB | SQLite footprint MiB |
|---|---:|---:|---:|---:|
| 100,000 × 1024 B / random | 444.8 | 235.6 | 255.6 | 1022.4 |
| 250,000 × 1024 B / random | 1056.1 | 430.4 | 528.6 | 1222.4 |
| 1,000,000 × 100 B / random | 619.5 | 301.5 | 372.5 | 1164.4 |
| 1,000,000 × 100 B / seq | 536.5 | 256.9 | 359.8 | 1164.2 |

These are medians. Every observation and min/max range is retained in cpu-summary.json and dataset runs.json. Three repetitions do not establish statistical significance for small differences or a universal lead.

## CPU and elapsed time versus unchanged alpha.6

### 100,000 × 1024 B / random

| Operation | Before CPU s/M | Final CPU s/M | CPU change | Before wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 6.451 | 5.128 | -20.5% | 809.27 | 898.45 | +11.0% |
| Point read hit | 0.706 | 0.615 | -12.8% | 140.53 | 122.61 | -12.8% |
| Point read miss | 0.319 | 0.241 | -24.3% | 59.23 | 48.10 | -18.8% |
| Full value scan | 0.085 | 0.077 | -9.8% | 42.41 | 38.12 | -10.1% |
| 10-reader point hits | 1.320 | 0.705 | -46.6% | 76.26 | 23.16 | -69.6% |
| Update + maintenance | 30.419 | 18.729 | -38.4% | 515.88 | 369.57 | -28.4% |
| 80% read / 20% update + maintenance | 3.873 | 2.845 | -26.5% | 543.54 | 488.62 | -10.1% |
| Delete + maintenance | 24.743 | 17.661 | -28.6% | 371.26 | 327.97 | -11.7% |
| Scan after delete | 0.084 | 0.077 | -7.3% | 37.38 | 34.67 | -7.2% |
### 250,000 × 1024 B / random

| Operation | Before CPU s/M | Final CPU s/M | CPU change | Before wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 7.764 | 6.423 | -17.3% | 2249.01 | 2351.48 | +4.6% |
| Point read hit | 0.770 | 0.675 | -12.3% | 374.85 | 336.26 | -10.3% |
| Point read miss | 0.358 | 0.269 | -24.9% | 178.05 | 133.95 | -24.8% |
| Full value scan | 0.085 | 0.075 | -11.6% | 105.95 | 93.69 | -11.6% |
| 10-reader point hits | 1.770 | 0.788 | -55.5% | 276.19 | 53.11 | -80.8% |
| Update + maintenance | 35.811 | 18.264 | -49.0% | 1132.77 | 806.48 | -28.8% |
| 80% read / 20% update + maintenance | 5.375 | 3.604 | -32.9% | 1628.58 | 1320.36 | -18.9% |
| Delete + maintenance | 31.636 | 16.171 | -48.9% | 1024.92 | 711.55 | -30.6% |
| Scan after delete | 0.083 | 0.076 | -8.3% | 93.04 | 85.32 | -8.3% |
### 1,000,000 × 100 B / random

| Operation | Before CPU s/M | Final CPU s/M | CPU change | Before wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 2.144 | 1.738 | -19.0% | 1927.65 | 1973.22 | +2.4% |
| Point read hit | 0.662 | 0.627 | -5.3% | 1319.59 | 1250.66 | -5.2% |
| Point read miss | 0.400 | 0.337 | -15.8% | 799.37 | 672.67 | -15.8% |
| Full value scan | 0.019 | 0.018 | -4.0% | 95.76 | 92.00 | -3.9% |
| 10-reader point hits | 1.461 | 0.744 | -49.0% | 839.40 | 163.07 | -80.6% |
| Update + maintenance | 5.007 | 3.232 | -35.4% | 632.51 | 520.24 | -17.8% |
| 80% read / 20% update + maintenance | 1.340 | 1.215 | -9.3% | 1422.66 | 1369.83 | -3.7% |
| Delete + maintenance | 4.671 | 2.959 | -36.7% | 546.02 | 459.46 | -15.9% |
| Scan after delete | 0.019 | 0.019 | -2.9% | 86.76 | 84.28 | -2.9% |
### 1,000,000 × 100 B / seq

| Operation | Before CPU s/M | Final CPU s/M | CPU change | Before wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 1.024 | 0.689 | -32.7% | 999.98 | 854.04 | -14.6% |
| Point read hit | 0.917 | 0.876 | -4.4% | 1827.19 | 1746.03 | -4.4% |
| Point read miss | 0.778 | 0.719 | -7.6% | 1544.25 | 1432.34 | -7.2% |
| Full value scan | 0.019 | 0.018 | -5.2% | 96.72 | 91.71 | -5.2% |
| 10-reader point hits | 1.801 | 1.041 | -42.2% | 963.05 | 224.62 | -76.7% |
| Update + maintenance | 5.404 | 3.445 | -36.3% | 681.52 | 562.93 | -17.4% |
| 80% read / 20% update + maintenance | 1.574 | 1.383 | -12.1% | 1664.80 | 1521.49 | -8.6% |
| Delete + maintenance | 4.923 | 3.128 | -36.5% | 602.70 | 491.95 | -18.4% |
| Scan after delete | 0.020 | 0.019 | -6.1% | 89.24 | 83.80 | -6.1% |

Negative changes mean improvement. CPU is normalized per million logical operations; elapsed times use the actual recorded operation counts. Raw acknowledged write loops and maintenance phases remain separate in runs.json.

## Remaining gaps

- 100,000 × 1024 B / random: native peak RSS is 4.1% above Pebble in these cohorts.
- 250,000 × 1024 B / random: native peak RSS is 15.8% above Pebble in these cohorts.
- 1,000,000 × 100 B / seq: native peak RSS is 9.9% above Pebble in these cohorts.
- 100,000 × 1024 B / random, Load + maintenance: elapsed-time median +11.0%.
- 250,000 × 1024 B / random, Load + maintenance: elapsed-time median +4.6%.
- 1,000,000 × 100 B / random, Load + maintenance: elapsed-time median +2.4%.

An across-the-board win is not established. Shared mapped-file residency, private caches, ingestion backpressure and current OS memory pressure affect these comparisons. Configured cache sizes do not imply the same working set or a hard RSS bound.

## Implementation and checks

Stable file segments eliminate duplicate growth mappings. Explicitly owned native page/descriptor/memtable storage releases retired buffers without waiting for heap GC; snapshots, scans, cursors and checkpoint I/O retain their own lifetimes. Both cache modes stream compaction; checkpoint vectors/scratch storage are reused and WAL buffers are lazy/reclaimable. Verified clean reads avoid descriptor allocation and read-side CLOCK reference writes. Obsolete file translations are reclaimed without changing file contents or live addresses. New mappings are warmed only after the old file closes, so the two trees need not be resident together.

Checkpoint completion sweeps newly clean descriptors; statistics separately expose resident in-flight I/O pages without double-counting dirty pages. Tests pause I/O to verify ownership/accounting before and after completion. WAL replay pins, fixed flushed replay boundaries and serialized compactions/rotations prevent required generations from being deleted during copying. Bounded ingestion can still apply backpressure when it outruns I/O.

Full macOS, targeted race, Linux, vet and cross-build evidence is retained with exact source-stage scope in validation.json. No release/new tag was published.

## Reproduction

```sh
cd bench
go build -o /tmp/apteva-memory-final .
python3 run_memory_comparison.py --binary /tmp/apteva-memory-final --baseline-binary /path/to/unchanged-alpha6-benchmark --engines ours --out /tmp/apteva-memory-final-results
python3 summarize_large_cpu.py /tmp/apteva-memory-final-results
python3 report_memory_final.py /tmp/apteva-memory-final-results --comparators /path/to/completed-four-engine-matrix
```

