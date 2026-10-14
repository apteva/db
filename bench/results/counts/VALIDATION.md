# Alpha.6 native filtered count validation

2026-10-14, Apple M1 Pro, 10 logical CPUs, Go 1.25.1, macOS arm64. Baseline is
published alpha.5 (`2dbc4d428c82e281f1489dd450004a6d33b3adfc`), not a simulated
count implementation. The engine module retains zero external dependencies.

## Reported reproduction

Each engine gets 100,000 rows, 20 buckets (`b00` through `b19`), rank equal to
row number, and a 100-byte payload. Filter `{"bucket":"b00"}` matches exactly
5,000 documents. Apteva uses scalar `(bucket ASC, rank ASC)`. SQLite's prepared
`SELECT COUNT(*) ... WHERE bucket=?` uses its covering `(bucket,rank)` index.
Pebble has no native document-count API: its reference counts entries of an
application-maintained scalar secondary index through a bounded iterator and
snapshot; the reference does not parse JSON or evaluate documents.

Apteva is checkpointed, SQLite commits its load, and Pebble flushes/compacts before
reads. Five warmups precede timing. Setup, writes, fixture creation and compilation
are excluded. Six serial count processes (three fresh fixtures for each version)
ran with one-second Go benchmark samples, alternating version order. All samples
are retained; the table uses medians of the three samples for each listed version.
No HTTP, adapter or MCP is involved, and no agent test/compiler ran concurrently
with these timed processes. macOS/storage/UI background activity remained active;
CPU snapshots are retained in the local output directory rather than published.

| Operation | Median ms/count | Allocated B/op | Allocs/op |
|---|---:|---:|---:|
| alpha.5 Query + count pipeline | 3.024039 | 1,876,167 | 15,113 |
| alpha.5 document-loading index cursor | 1.941252 | 82,648 | 5,043 |
| alpha.6 Query + count pipeline | 3.028079 | 1,876,169 | 15,113 |
| alpha.6 document-loading index cursor | 1.674548 | 82,648 | 5,043 |
| **alpha.6 CountDocuments, covering** | **0.006355** | **2,488** | **32** |
| alpha.6 CountDocuments, forced document scan | 9.207460 | 72,256 | 50 |
| alpha.6 CountDocuments, residual payload prefix | 2.679777 | 86,041 | 5,085 |
| SQLite C covering count (alpha.6 rounds) | 0.125922 | 32 | 2 |
| SQLite Go covering count (alpha.6 rounds) | 0.228491 | 832 | 8 |
| Pebble application covering-index count (alpha.6 rounds) | 0.226573 | 216 | 14 |

For this checkpointed reproduction the new API is about **476× faster** than
alpha.5 Query/pipeline, **20× faster** than SQLite C, and **36× faster** than the
SQLite Go/Pebble references. Allocated bytes per call fall about 754× against the
old Query/pipeline. SQLite and Pebble still allocate fewer Go bytes than the new
API. This is a count-specific comparison, not a universal database ranking.
The tests assert **zero document fetches**, `Plan.IndexOnly=true`, and 5,000
logical index entries examined on every timed covering call. Retained bound
memory is 24 bytes; this excludes query parsing/plans, snapshots, scan scratch,
page caches and caller memory. B/op is per-operation Go allocation, not RSS or
process footprint. Checkpointed counts use bounded leaf slot totals; buffered
index counts merge entries and have different costs. Existing Query/pipeline
callers must adopt CountDocuments to obtain this improvement.

## Existing-path guards

All twelve original paired count/guard processes are retained. Existing code for
raw writes, point reads, scans and cursors is unchanged by this release. Query
adds document-fetch instrumentation and its paired median is +0.13% here.

| Guard median | alpha.5 | alpha.6 | Change |
|---|---:|---:|---:|
| Buffered random upsert, ns/op | 729.8 | 588.9 | -19.3% |
| Point Find after timed writes, ns/op | 569.2 | 638.8 | +12.2% |
| Raw Scan with row validation, ns/row | 21.41 | 18.24 | -14.8% |
| Raw Cursor with row validation, ns/row | 39.46 | 39.76 | +0.8% |
| Raw range 1, ns/op | 945.2 | 604.2 | -36.1% |
| Raw range 8, ns/op | 1,373 | 1,203 | -12.4% |
| Raw range 128, ns/op | 9,580 | 9,421 | -1.7% |

The post-write Find guard includes an uncontrolled number of preceding timed
upserts and possible background checkpoint work. Its short samples varied from
398 to 713 ns. A cursor sample also reached 104 ns/row during background activity.
We retained these results and added six serial processes with **isolated reads**,
three fresh fixtures per version, alternating order, and three-second samples.
These do not execute the timed Upsert phase first:

| Isolated guard median | alpha.5 | alpha.6 |
|---|---:|---:|
| Point Find, ns/op | 416.9 | 403.2 |
| Raw Cursor, ns/row | 39.33 | 39.35 |

Isolated Find improved 3.3% and cursor throughput was effectively flat (+0.05%).
These observations do not prove all workloads have no regression; every raw
sample is included in `summary.json`, including the slower original guard.

## Correctness and release checks

- Compare counts with forced document evaluation: empty/full matches, multiple
  prefix fields, mixed directions, exact values above 2^53, numeric equivalence,
  missing/null fields, repeated and contradictory equalities and nested AND/OR.
- Sparse trailing-field omissions and nullable predicates fall back; multikey
  arrays with duplicate/multiple matching entries count each document once.
- Residual predicates, literal prefixes, raw non-doc values, hints, skip/limit,
  invalid filters/options, budgets, cancellation and snapshot cleanup are tested.
- Test checkpointed and buffered insert/update/tombstone views, checkpoint/reopen
  recovery, concurrent writes and index drop/recreation against a held snapshot.
- Native default collection and snapshot APIs, authenticated Go HTTP driver,
  capped server budgets, errors and leased read-only snapshot counts are tested.
- Local vet, short suite and full race suite passed; the final leaf-slot count
  regressions also passed under race. Exact release commit Linux/macOS CI and
  local 2-minute cursor/change-feed stress and soak gate tagging. The release
  workflow repeats race/stress/soak on Linux/macOS before publishing four archives.
- Post-publication verification records will be added under `release-alpha6`:
  all archive checksums, public module/sumdb, CLI install, embedded count and
  downloaded daemon/public Go driver count plus snapshot/watch compatibility.

## Reproduce

```sh
python3 bench/results/counts/build.py
python3 bench/results/counts/run.py /tmp/apteva-count-results
python3 bench/results/counts/guard_reads.py /tmp/apteva-count-results
python3 bench/results/counts/summarize.py /tmp/apteva-count-results
```

`bench/count/count_test.go` is the fixture and workload source. `build.py` archives
alpha.5 and copies the same fixture to it, removing only unavailable new-API
cases/statistics. All binaries are built before timed runs; run without other
agent tests/compilers. Scripts use `/private/tmp` binary paths (macOS); Linux
users can substitute `/tmp`. Counts are asynchronous-loaded embedded reads;
this benchmark makes no durable-write or parallel-count throughput claim.
