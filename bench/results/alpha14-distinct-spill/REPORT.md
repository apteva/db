# alpha.14: exact distinct counts for one oversized group

Reported failure: eight filtered `$countDistinct` metrics with `_id: null` over
64,084 rows and 24,443 distinct callers returned `ErrLimit: aggregation
partition depth exhausted` at a 16 MiB budget (8 MiB state limit). Six metrics
passed. Spilled partial states are partitioned by group key, and one key can
never be divided, so `partitionGroup` recursed until depth 32.

`TestGlobalDistinctSpillIsExact` reproduces the shape and compares every
accumulator with the in-memory reference. On alpha.13 all of its cases fail
with "partition depth exhausted"; on alpha.14 they pass. Stats per case, in
order: global 16 MiB, global 2 MiB, grouped by 3 keys at 2 MiB, one large
group among 125 small ones at 2 MiB (`test-stats.txt`):

| Case | Peak memory | Spilled | Distinct slices | Partition depth |
|---|---:|---:|---:|---:|
| global, 16 MiB | 10.4 MB | 6.6 MB | 2 | 0 |
| global, 2 MiB | 1.3 MB | 6.6 MB | 13 | 0 |
| 3 groups, 2 MiB | 1.1 MB | 6.6 MB | 30 | 0 |
| skewed, 2 MiB | 1.6 MB | 13.0 MB | 11 | 1 |

The Database app's `TestGlobalFilteredDistinctCountsSpill` (same data through
`aggregate`, 6/7/8 metrics, default 16 MiB work budget) fails on alpha.13 and
passes against alpha.14 with exact counts.
