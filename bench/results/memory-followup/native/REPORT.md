# AptevaDB: final memory and performance validation

The selected code retains the previous candidate's buffering policy: a CacheBytes/2 memtable budget accounting for record/metadata/obsolete storage, up to two active budgets while a table applies, and writable-page pressure at CacheBytes/4. Smaller default tables and larger dirty-page batches remain separate rejected experiments. Explicit options retain their existing meaning.

Structural changes batch contiguous checkpoint I/O, preserve immutable capture buffers, reuse descent between scan leaves, bound ordinary WAL backlog, release oversized log storage at replay barriers, and retire consumed file translations during private compaction. A WAL boundary fix flushes a complete prefix before buffer growth. Snapshot-retired IDs require both reader completion and a durable metadata barrier before reuse, including retirements during checkpoint I/O. The on-disk format remains 10.

## Cohorts and method

The final native cohort has 36 serial fresh-process trials: four dataset/order combinations, three repetitions, and three interleaved native builds. Original alpha.6 is the engine at commit 65cedd31a494bb561485cfc07c3dd0aeed06be06. Previous is the saved final warmed memory candidate from October 15, which matches the starting point of this optimization request. Final contains the validated changes and both recovery/WAL fixes. All three use the identical current Go benchmark harness, compiler and cache/fixture options.

Pebble and SQLite columns come from the preceding completed 48-trial October 27 matrix, with three repetitions of each competitor per dataset/order. Its intermediate native candidate used different tuning and is excluded from final native results. Native versions and cohorts are never pooled. Competitor values are reference comparisons from that preceding cohort; they were not measured simultaneously with the final native cohort.

Apple M1 Pro, ten logical CPUs, 16 GiB; Go 1.25.1. Background applications and system storage/indexing activity were present and recorded per trial. No agent builds, tests, profiling or VM ran alongside measurement. Retain all samples and min/max ranges; small median differences under contention are not statistical guarantees. Every completed trial validates values and row counts. Source archives, hashes, build information and validation scope are retained.

Fixtures remain alive and are included in process lifetime RSS/physical-footprint peaks. CPU is process user plus system time across threads. Cache settings are 64 MiB, not total-process caps. Pebble additionally uses a 64 MiB memtable, compression and Bloom filters; SQLite C 3.32.2 uses WITHOUT ROWID BLOB rows, WAL with synchronous OFF, and a cache per connection with ten parallel read connections. Reads repeat two hit/miss/ten-reader passes, and five full-value scan passes. Writes are per-row atomic asynchronous upserts. Load/update/delete/mixed operation totals include the subsequent checkpoint and full compaction. Raw loops, verification and maintenance remain individually recorded. No cold OS-cache claim.

macOS tracks resident memory and physical footprint with different ledgers; footprint also accounts for compressed internal memory. Both metrics are reported. See [Apple's kernel accounting](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/task.c#L1139).

## Peak process memory

| Dataset / insertion | Alpha.6 RSS MiB | Previous RSS MiB | Final RSS MiB | Pebble RSS MiB | SQLite RSS MiB | Final vs previous |
|---|---:|---:|---:|---:|---:|---:|
| 100,000 × 1024 B / random | 700.7 | 269.2 | 261.7 | 257.4 | 1012.5 | -2.8% |
| 250,000 × 1024 B / random | 1468.3 | 595.1 | 599.5 | 541.7 | 1147.9 | +0.7% |
| 1,000,000 × 100 B / random | 869.6 | 339.7 | 337.2 | 321.7 | 1024.8 | -0.7% |
| 1,000,000 × 100 B / seq | 786.1 | 334.4 | 331.5 | 319.7 | 1026.0 | -0.9% |

| Dataset / insertion | Alpha.6 footprint MiB | Previous footprint MiB | Final footprint MiB | Pebble footprint MiB | SQLite footprint MiB |
|---|---:|---:|---:|---:|---:|
| 100,000 × 1024 B / random | 465.1 | 229.7 | 225.4 | 249.1 | 1022.5 |
| 250,000 × 1024 B / random | 908.7 | 423.7 | 424.2 | 555.6 | 1187.8 |
| 1,000,000 × 100 B / random | 615.1 | 293.6 | 293.0 | 371.1 | 1164.1 |
| 1,000,000 × 100 B / seq | 502.0 | 268.3 | 270.1 | 329.7 | 1164.3 |

### Worst observed RSS across the three repetitions

| Dataset / insertion | Previous maximum MiB | Final maximum MiB | Change |
|---|---:|---:|---:|
| 100,000 × 1024 B / random | 272.5 | 261.9 | -3.9% |
| 250,000 × 1024 B / random | 651.4 | 599.5 | -8.0% |
| 1,000,000 × 100 B / random | 344.1 | 341.1 | -0.9% |
| 1,000,000 × 100 B / seq | 348.1 | 337.8 | -3.0% |

These maxima describe the observed three-trial ranges, not a worst-case guarantee. Median comparisons above remain the primary summaries.

## CPU and elapsed time versus the previous memory candidate

### 100,000 × 1024 B / random

| Operation | Previous CPU s/M | Final CPU s/M | CPU change | Previous wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 7.946 | 5.179 | -34.8% | 1413.59 | 1063.19 | -24.8% |
| Point read hit | 0.776 | 0.686 | -11.6% | 159.25 | 137.40 | -13.7% |
| Point read miss | 0.293 | 0.274 | -6.6% | 58.49 | 54.77 | -6.4% |
| Full value scan | 0.081 | 0.076 | -5.8% | 40.45 | 37.88 | -6.4% |
| 10-reader point hits | 0.758 | 0.771 | +1.6% | 24.75 | 23.68 | -4.3% |
| Update + maintenance | 25.852 | 21.292 | -17.6% | 628.76 | 439.47 | -30.1% |
| 80% read / 20% update + maintenance | 3.429 | 2.949 | -14.0% | 714.95 | 561.08 | -21.5% |
| Delete + maintenance | 21.447 | 18.791 | -12.4% | 467.94 | 421.60 | -9.9% |
| Scan after delete | 0.081 | 0.075 | -7.8% | 36.37 | 33.49 | -7.9% |
### 250,000 × 1024 B / random

| Operation | Previous CPU s/M | Final CPU s/M | CPU change | Previous wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 9.922 | 7.613 | -23.3% | 4558.65 | 3795.18 | -16.7% |
| Point read hit | 0.834 | 0.830 | -0.4% | 415.92 | 414.44 | -0.4% |
| Point read miss | 0.335 | 0.374 | +11.8% | 167.48 | 196.06 | +17.1% |
| Full value scan | 0.080 | 0.076 | -5.4% | 108.09 | 94.62 | -12.5% |
| 10-reader point hits | 0.901 | 0.852 | -5.5% | 70.36 | 66.10 | -6.1% |
| Update + maintenance | 25.343 | 22.820 | -10.0% | 1168.46 | 1169.52 | +0.1% |
| 80% read / 20% update + maintenance | 4.388 | 4.449 | +1.4% | 1692.07 | 2017.88 | +19.3% |
| Delete + maintenance | 20.810 | 19.248 | -7.5% | 932.83 | 1185.00 | +27.0% |
| Scan after delete | 0.082 | 0.080 | -2.4% | 92.24 | 90.29 | -2.1% |
### 1,000,000 × 100 B / random

| Operation | Previous CPU s/M | Final CPU s/M | CPU change | Previous wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 2.075 | 1.814 | -12.6% | 2197.60 | 2005.12 | -8.8% |
| Point read hit | 0.730 | 0.730 | -0.0% | 1457.57 | 1458.84 | +0.1% |
| Point read miss | 0.441 | 0.425 | -3.7% | 882.91 | 848.59 | -3.9% |
| Full value scan | 0.019 | 0.019 | -2.8% | 95.82 | 93.04 | -2.9% |
| 10-reader point hits | 0.773 | 0.760 | -1.7% | 203.02 | 203.01 | -0.0% |
| Update + maintenance | 4.182 | 3.920 | -6.3% | 871.63 | 681.80 | -21.8% |
| 80% read / 20% update + maintenance | 1.510 | 1.345 | -10.9% | 1842.79 | 1865.73 | +1.2% |
| Delete + maintenance | 3.642 | 3.367 | -7.6% | 641.17 | 669.44 | +4.4% |
| Scan after delete | 0.020 | 0.019 | -5.1% | 89.23 | 84.09 | -5.8% |
### 1,000,000 × 100 B / seq

| Operation | Previous CPU s/M | Final CPU s/M | CPU change | Previous wall ms | Final wall ms | Wall change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 0.998 | 0.802 | -19.6% | 1213.40 | 1011.29 | -16.7% |
| Point read hit | 1.072 | 1.053 | -1.8% | 2247.28 | 2360.44 | +5.0% |
| Point read miss | 0.855 | 0.869 | +1.6% | 1716.05 | 1743.61 | +1.6% |
| Full value scan | 0.020 | 0.019 | -5.2% | 98.40 | 93.37 | -5.1% |
| 10-reader point hits | 1.074 | 1.107 | +3.1% | 285.13 | 352.00 | +23.5% |
| Update + maintenance | 4.857 | 4.282 | -11.8% | 864.52 | 746.89 | -13.6% |
| 80% read / 20% update + maintenance | 1.851 | 1.745 | -5.7% | 2025.16 | 2055.90 | +1.5% |
| Delete + maintenance | 4.168 | 3.739 | -10.3% | 687.26 | 712.23 | +3.6% |
| Scan after delete | 0.019 | 0.019 | +0.2% | 86.51 | 87.31 | +0.9% |

## Load plus maintenance versus original alpha.6

| Dataset / insertion | Alpha.6 ms | Previous ms | Final ms | Final vs alpha.6 |
|---|---:|---:|---:|---:|
| 100,000 × 1024 B / random | 1097.52 | 1413.59 | 1063.19 | -3.1% |
| 250,000 × 1024 B / random | 3673.79 | 4558.65 | 3795.18 | +3.3% |
| 1,000,000 × 100 B / random | 2782.80 | 2197.60 | 2005.12 | -27.9% |
| 1,000,000 × 100 B / seq | 1603.68 | 1213.40 | 1011.29 | -36.9% |

## Remaining positive median differences

- 100,000 × 1024 B / random: final peak RSS 1.7% above Pebble.
- 250,000 × 1024 B / random: final peak RSS 0.7% above previous.
- 250,000 × 1024 B / random: final peak RSS 10.7% above Pebble.
- 250,000 × 1024 B / random: final peak physical footprint 0.1% above previous.
- 1,000,000 × 100 B / random: final peak RSS 4.8% above Pebble.
- 1,000,000 × 100 B / seq: final peak RSS 3.7% above Pebble.
- 1,000,000 × 100 B / seq: final peak physical footprint 0.7% above previous.
- 100,000 × 1024 B / random, 10-reader point hits: CPU +1.6%, elapsed -4.3% versus previous.
- 250,000 × 1024 B / random, Point read miss: CPU +11.8%, elapsed +17.1% versus previous.
- 250,000 × 1024 B / random, Update + maintenance: CPU -10.0%, elapsed +0.1% versus previous.
- 250,000 × 1024 B / random, 80% read / 20% update + maintenance: CPU +1.4%, elapsed +19.3% versus previous.
- 250,000 × 1024 B / random, Delete + maintenance: CPU -7.5%, elapsed +27.0% versus previous.
- 1,000,000 × 100 B / random, Point read hit: CPU -0.0%, elapsed +0.1% versus previous.
- 1,000,000 × 100 B / random, 80% read / 20% update + maintenance: CPU -10.9%, elapsed +1.2% versus previous.
- 1,000,000 × 100 B / random, Delete + maintenance: CPU -7.6%, elapsed +4.4% versus previous.
- 1,000,000 × 100 B / seq, Point read hit: CPU -1.8%, elapsed +5.0% versus previous.
- 1,000,000 × 100 B / seq, Point read miss: CPU +1.6%, elapsed +1.6% versus previous.
- 1,000,000 × 100 B / seq, 10-reader point hits: CPU +3.1%, elapsed +23.5% versus previous.
- 1,000,000 × 100 B / seq, 80% read / 20% update + maintenance: CPU -5.7%, elapsed +1.5% versus previous.
- 1,000,000 × 100 B / seq, Delete + maintenance: CPU -10.3%, elapsed +3.6% versus previous.
- 1,000,000 × 100 B / seq, Scan after delete: CPU +0.2%, elapsed +0.9% versus previous.

performance-gates.json enumerates every comparison, including original-alpha.6 CPU/elapsed deltas. These are strict comparisons of observed medians, not significance tests. All source cohorts, samples and ranges remain available. See validation.json for functional/race/recovery checks. Changes remain unreleased.


## Additional read-cache experiment

A separate complete-key routing-map prototype was tested in three interleaved fresh processes per build with 250,000 random 1-KiB values. Diagnostics confirmed the corrected shortcut was active. Its hit-read CPU improved only 2.0%, while peak RSS increased by 95.0 MiB (15.9%) and load CPU rose 6.1%. Misses improved 65.9%, but this fails the memory/no-regression requirement. The map remains outside the selected implementation. See the [rejected experiment and raw samples](readcache-experiment/REPORT.md). The initial invalidated prototype is preserved separately and excluded from these comparisons.

