# Alpha.12: aggregations use compound indexes

Before alpha.12, the planner for an aggregation's leading `$match` considered
single-field ascending indexes only. A `(tenant, day)` index was ignored, so a
dashboard over one tenant and one day scanned the whole collection. Query
already used the index.

## Engine

`repro.go.txt` (module file `go.mod.txt`) loads 328,120 records of about 1 KB:
4 scopes × 26 days, 3,155 per scope and day. It then times one
`$match {scope_id, day}` + `$group` aggregate (`AggregateParallel`, 8 shards,
median of 7) as indexes are added. Apple M-series, 10 cores.

| Indexes | alpha.11 | alpha.12 |
|---|---:|---:|
| none | 12.8 ms | 14.0 ms (same scan) |
| compound (scope_id, day) | 14.2 ms | 0.78 ms |
| + single-field scope_id | 14.0 ms | 0.96 ms |
| + single-field day | 4.6 ms | 1.01 ms |
| Query with the same filter | 2.2 ms | 2.2 ms |

In alpha.11 only the single-field `day` index helped. The `scope_id` index names a
quarter of the collection, above the one-eighth threshold, so it was not used.
Alpha.12 counts compound candidates first, so the 3,155-entry tuple bounds the
counting of the broader single-field candidates.

## Through the Database app

The Database app's `BenchmarkScopedDayAggregates` uses the same layout with
328,000 records of about 600 bytes and a `(scope_id, day)` index. It runs one
grouped aggregate, and a read batch of 14 conditional aggregates. Both test
binaries were interleaved, 6 runs each:

| | alpha.11 | alpha.12 |
|---|---|---|
| aggregate | 19.4–28.9 ms (median ≈ 20.3) | 1.10–2.96 ms (median ≈ 1.16) |
| batch of 14 | 274–441 ms (median ≈ 282) | 15.0–18.2 ms (median ≈ 15.6) |

Results are identical: the index only narrows which records are read, and every
predicate is still checked on each record (`TestAggregateUsesCompoundIndexes`
compares each pipeline with an unindexed copy in Aggregate, AggregateParallel and
AggregateEach).
