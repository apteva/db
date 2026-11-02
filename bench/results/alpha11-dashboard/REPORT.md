# Alpha.11 dashboard performance

Source: the alpha.11 working tree on top of `42ee96b`. Baseline: alpha.10
(`42ee96b`, with its harness). Apple M1 Pro, 10 cores, 16 GiB, macOS, Go 1.25.1.
Five interleaved fresh-process trials per workload. Medians are given with
[min–max] ranges, and each benchmark keeps the best of 3 runs per query inside
its process. The machine was busy: the one-minute load average sampled before
each process ranged from 6.8 to 14.3 (median 10.3), and every sample is in the
raw outputs. Raw outputs: `raw-trials.tar.gz`. Per-query medians and ranges:
`summary.json`. Runner: `run_trials.py`.

## Dashboard workload (`-dashboard -n 1000000`)

1,000,000 call records (100 campaigns, 50,000 callers, RFC 3339 UTC
timestamps over 300 days) and 200,000 prospects keyed `p0000000…`. 2% of calls
name no prospect. SQLite runs the equivalent SQL: `FILTER` clauses,
`COUNT(DISTINCT)`, `date()`/`strftime()` for UTC buckets, a deterministic Go
function for Paris buckets (SQLite has no IANA zones), and a `JOIN` on the
prospects' `TEXT PRIMARY KEY` (`WITHOUT ROWID`). Pebble runs the engine's
executor over its iterator and has no joins. Every engine's results are
compared group by group with the first engine's before timings are printed.

Milliseconds, median [min–max]; lower is better. `ours` is one core and
`ours-par` is 10 shards.

| query | ours | ours-par | pebble | sqlite-c | sqlite-go |
|---|---|---|---|---|---|
| 3 filtered metrics by campaign | 123 [122–131] | 24.5 [21.5–26.8] | 178 [175–223] | 731 [708–795] | 937 [901–962] |
| 6-metric dashboard by campaign | 416 [392–531] | 189 [177–194] | 468 [457–476] | 1270 [1173–1538] | 2059 [1982–2076] |
| daily UTC buckets × campaign | 157 [154–169] | 39.0 [35.6–48.2] | 216 [211–227] | 1042 [947–1093] | 1449 [1357–1490] |
| daily Paris buckets × campaign | 158 [156–163] | 35.3 [32.6–38.4] | 216 [213–236] | 1445 [1411–1496] | 1581 [1552–1674] |
| weekly Paris revenue (Monday) | 121 [120–123] | 24.0 [21.4–34.2] | 174 [173–196] | 856 [851–862] | 792 [755–830] |
| hourly UTC buckets | 95.5 [92.6–96.2] | 18.1 [16.1–26.2] | 150 [148–152] | 366 [362–372] | 769 [763–783] |
| distinct callers by campaign | 314 [281–335] | 177 [172–198] | 358 [349–377] | 995 [977–1007] | 1918 [1855–2088] |
| distinct answered callers by campaign | 199 [198–224] | 86.8 [83.8–96.1] | 258 [248–273] | 849 [833–1024] | 1356 [1312–1558] |
| key join: calls by prospect city | 508 [499–589] | 78.1 [71.6–93.6] | — | 791 [746–806] | 1571 [1511–1861] |
| key join, one campaign (1%) | 42.0 [41.3–42.4] | 7.8 [6.9–8.4] | — | 45.1 [44.4–49.3] | 77.6 [76.1–83.3] |
| load (s) | 0.91 [0.82–0.98] | 0.88 [0.77–1.02] | 1.68 [1.56–3.31] | 1.88 [1.74–1.91] | 1.51 [1.42–1.66] |
| on-disk size (MiB) | 196.3 | 196.3 | 156.5 [156.5–160.5] | 157.6 | 157.6 |

AptevaDB has the lowest median on every query on one core, and on all cores.
Its worst trial also beats every other engine's best trial on every query on
one core, except the 6-metric dashboard against Pebble (531 against 457).
The narrowest lead is the selective key join on one core: 42.0 ms against
sqlite-c's 45.1 ms, a 7% margin that a quieter or faster machine may not
reproduce. The scan over all calls dominates that query, not the 10,000
point reads.

On-disk size is the one measured dimension where AptevaDB loses: 196 MiB
against about 157 MiB for Pebble and SQLite, because documents carry field
names and a directory that SQLite's column rows and Pebble's compressed
blocks do not. No compression work was done for this release.

### What changed to get there

The first run of the harness at 1M lost the selective key join (55.8 ms
against 44.0 ms), and the whole-collection key join failed with ErrLimit.
That join's hash table, charged at about 700 bytes a row, needed about
140 MB for 200,000 prospects against the 64 MiB query budget. Three changes
fixed both:

- A key join always reads records by key. One tree descent per probe beats
  hashing the foreign collection, needs no memory, and drops the pre-pass
  that a hash join uses to size its table.
- A checkpointed source is scanned in place even when the join reads, with
  each point read made under the reader lock the scan already holds.
- Index-probe and key joins hand the consumer a reused row aliasing the read
  buffer instead of copying each match's fields. The copy was 46% of the
  selective join's CPU.

Measured on the first harness run at 100k calls, the same changes took the
selective join from 11.1 to 3.7 ms (sqlite-c 4.0 ms). The whole-collection
join went from 27.7 ms with the table to 33.1 ms with point reads, still
ahead of sqlite-c's 58.9 ms.

The old way to count distinct values, two `$group` stages, was dropped from
the comparison: its SQL is the same `COUNT(DISTINCT)` as the
`$countDistinct` row. At 100k it was 4.4× slower than `$countDistinct`
(75 against 17 ms). At 1M it exceeded the 64 MiB default budget, with about
906,000 intermediate groups, where `$countDistinct` completes in 314 ms.

## Regression check against alpha.10 (`-agg -n 50000`, `-join -n 100000`)

Same five-trial interleaving, `ours` and `ours-par` only. At 1M and 200k
documents, `-agg` exceeds the 64 MiB default budget on the customer grouping
in both builds, so it ran at the documented 50k.

- `-agg`: every query median is within 0.4 ms of alpha.10, and the ranges
  overlap. The largest shift is the 100k-group customer grouping on 10 shards,
  4.4 against 4.8 ms, which is faster.
- `-join`: every query median is within ±1.9 ms of alpha.10, and the ranges
  overlap. The largest shift is the indexed `orders JOIN users, group by
  country` on one core: 20.5 [18.2–23.7] against 18.6 [18.0–20.1]. Index-probe
  joins gained the row-copy removal; hash joins are unchanged in code.

The load was high and uneven, so this is no zero-regression claim. Both sets
of numbers are kept in `summary.json`.

## Not measured here

- The 60-minute soak.
- Spill (`AllowDiskUse`) timings for `$countDistinct` and filtered
  accumulators. Correctness under spill is tested; speed is not benchmarked.
- Linux.
