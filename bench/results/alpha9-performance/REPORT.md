# Aggregation, spill and index-build performance: validation against alpha.8

2026-10-28. **Pre-publication benchmark evidence; file format remains 10.**

Bounded (spill) aggregation of one million groups takes less than half of
alpha.8's time and writes about a sixth of its temporary bytes. In-memory
aggregation is faster on every measured query, and the parallel 100,000-group
query now scales with cores. Key-value, batched and durable write paths are
unchanged within noise. Peak RSS of the numeric spill workload rose by 5 MiB.
**These measurements were taken on a busy workstation and do not establish zero
regressions everywhere.**

## Implementation

- Spill aggregation: adaptive partition fanout with budgeted I/O buffers, a
  compact temporary record codec, arena-backed reconstruction and compact COUNT
  state. Temporary spill records changed; no persistent format changed.
- `AggregateGroups`/`RunGroups` with a borrowed typed `GroupView`, which skips
  per-group result maps ([usage](../../../USAGE.md#typed-group-visitor-alpha9)).
- In-memory aggregation:
  - filter fields are extracted first, and the rest only for documents that pass;
  - each document's directory is decoded once, and field names are checked before decoding values;
  - float-vs-number and string-equality predicates have fast paths.
- Sharded `$group` above 8,192 groups merges by key hash on all workers, with
  per-query key arenas and per-slab budget charges.
- Index build skips a checkpoint with nothing buffered, checkpoints skip the page
  sync when they write no pages, released one-page buffers are reused, and build
  entries sort without reflection.
- Durability fix: creating a log generation fsyncs the directory.
- Hang fix, found by the release soak after measurement: checkpoints sized the
  persisted free list before allocating its pages, which take their ids from
  that list, so a list just above a multiple of 4,092 ids panicked; since
  alpha.7 the panic deadlocked the checkpointer. The fix does not touch any
  measured path except free-list capture during checkpoints.

## Method and identities

- **Baseline:** the alpha.8 engine, a `git archive` of `2b19d13`. That is the
  `v0.2.0-alpha.8` tag (`4f57eb9`) plus one evidence-only commit; the engine
  source is identical. Only the candidate's benchmark harness (`bench/main.go`,
  `bench/agg.go`, `bench/spill_groups`) was copied in.
- **Candidate:** the release working tree. Engine sources are hashed in
  [candidate-source-sha256.json](candidate-source-sha256.json). After
  measurement, documentation and CI changed, and so did `checkpoint.go` (the
  hang fix above) plus its test, so that file's hash differs in the release.
- **Records:** binaries and the raw archive are identified in
  [source-identity.json](source-identity.json), with build metadata in
  [build-info.txt](build-info.txt).

**Harness parity fix.** The batched AptevaDB settle step now checkpoints *and*
compacts, as the Pebble batch settle already did. Both builds use the fixed
harness, so batch disk sizes compare like for like. Earlier batch disk figures
for AptevaDB were uncompacted files.

**Host.** MacBookPro18,1 (Apple M1 Pro, 10 cores, 16 GiB), Darwin 27.0.0
arm64, Go 1.25.1, GOMAXPROCS=10.

**Background load.** Other applications and agent sessions were active. The
1-minute load average before each trial was 9.3 to 35.6, median 13.7, and every
trial records its load average and top processes. No significance is claimed.

**Trials.** 48 fresh processes: 3 trials of each suite and build, with the
order of the two builds alternating each trial. All exited successfully.

| Suite | Fixture and settings |
|---|---|
| Key-value | 1,000,000 keys, 16-byte keys, 100-byte random values, async writes, unbounded cache |
| Batched | the same keys in 1,000-record batches/transactions |
| Aggregation | 50,000 order documents; each query is the best of 5 inside the process |
| Durable | 1,000 inserts with full sync (F_FULLFSYNC on every engine): 1, 10 and 50 writers |
| Spill | `bench/spill_groups`: 1,000,000 distinct groups, 12 MiB execution budget, disk use allowed; COUNT and numeric (COUNT, SUM/MIN/MAX, AVG) pipelines; owned maps (`AggregateEach`), and on the candidate also the typed visitor |

Pebble and SQLite figures come from the candidate harness. In the aggregation
suite, Pebble runs AptevaDB's executor over a Pebble iterator, so its numbers
improve with this release too. That row measures storage, not Pebble's own
aggregation.

The raw output of every trial is in [raw-trials.tar.gz](raw-trials.tar.gz),
with all medians, ranges and values in [summary.json](summary.json) and the
runner in [harness/](harness/).

## Results

Medians with [min–max] over three trials, alpha.8 → candidate.

### Spill aggregation, 1,000,000 groups, 12 MiB budget

| Pipeline | Wall s | User CPU s | System CPU s | Go allocations MiB | GC cycles | Spilled MB | Merge passes | Peak RSS MiB |
|---|---|---|---|---|---|---|---|---|
| COUNT, owned maps | 1.83 [1.75–2.07] → 0.80 [0.78–1.33] (−56%) | 1.97 → 0.95 | 0.47 → 0.10 | 1,868 → 1,002 | 776 → 324 | 192 → 29 | 17 → 1 | 18 → 19 |
| Numeric, owned maps | 3.06 [2.96–3.69] → 1.13 [1.12–1.18] (−63%) | 2.63 → 1.18 | 0.88 → 0.21 | 3,360 → 1,994 | 1,090 → 472 | 560 → 99 | 17 → 1 | 19 → 24 |
| COUNT, typed visitor | — → 0.52 [0.50–0.56] | — → 0.53 | — → 0.04 | — → 354 | — → 114 | — → 29 | — → 1 | — → 19 |
| Numeric, typed visitor | — → 0.86 [0.84–0.87] | — → 0.83 | — → 0.17 | — → 1,314 | — → 330 | — → 99 | — → 1 | — → 24 |

The typed visitor does not exist in alpha.8; compared with alpha.8's owned-map
run, it is 72% faster on both pipelines. Numeric peak RSS rises 5 MiB because of
the larger budgeted I/O buffers. Accounted execution memory stays inside the
unchanged 12 MiB budget.

### In-memory aggregation, 50,000 documents (ms)

| Query | AptevaDB 1 core | AptevaDB all cores | Pebble (AptevaDB executor) | SQLite C |
|---|---|---|---|---|
| group by region: sum, count | 4.4 → 3.9 (−11%) | 1.0 → 1.1 [0.8–1.3] | 5.7 | 13.2 |
| filter + group + sort + limit | 5.9 → 2.7 (−54%) | 1.3 → 0.6 (−54%) | 4.7 | 5.7 |
| range filter, count + avg | 3.0 → 2.0 (−33%) | 0.7 → 0.5 (−29%) | 4.0 | 2.3 |
| group by customer (100k groups), top 10 | 10.8 → 9.0 (−17%) | 9.4 → 4.5 (−52%) | 11.3 | 19.7 |
| build two indexes (s) | 0.08 → 0.07 | — | — | 0.04 |
| load (s) | 0.03 → 0.03 | — | 0.15 | 0.05 |

The all-cores `group by region` figure is unchanged within its range. Index
build remains slower than SQLite here. AptevaDB runs F_FULLFSYNC checkpoints,
while this SQLite configuration uses `synchronous=OFF`, so the comparison is not
like for like. The durability policy is unchanged.

### Key-value, batched and durable writes

Within noise of alpha.8 apart from two cells. The full matrix is in
[summary.json](summary.json).

- **Sequential lookups with 10 readers: 147 → 212 ns/op.** The ranges overlap
  (136–205 vs 175–254), and no candidate change touches the read path (reader,
  lookup, tree, scan, cursor or page reads). We record the cell without
  explaining it.
- **Random find-miss: 463 → 499 ns.** The ranges overlap (436–528 vs 494–514).

AptevaDB vs Pebble and SQLite C, candidate medians:

| | AptevaDB | Pebble | SQLite C |
|---|---|---|---|
| random insert, ns/op | 778 | 1,748 | 19,182 |
| random find-hit, ns/op | 718 | 2,415 | 2,994 |
| random find-hit, 10 readers, ns/op | 102 | 636 | 2,923 |
| scan, ns/key | 38 | 57 | 135 |
| batched random disk size, MB | 128 | 189 | 134 |
| durable insert, 1 / 10 / 50 writers, ms | 4.32 / 0.47 / 0.10 | 4.28 / 0.84 / 0.17 | 4.27 / 4.32 / 4.43 |

Single-writer durable inserts are bound by the device flush for every engine.

## Validation

`go vet`, the full race suite, and the release workflow's 60-second cursor, soak
and change-feed stress run all passed on the candidate
([output](validation-race-soak.txt)). That run preceded one later `wal.go`
change: deleting checkpointed log generations no longer fsyncs the directory,
for the reason given in DURABILITY.md. The unit tests and the CI fuzz smoke ran
on the final tree. The 60-minute soak then hung on macOS; the cause was the
free-list panic above, and the fix was validated by a deterministic regression
test, the unit and race suites and a repeated soak before tagging. The tag's
release workflow repeats the race, stress and soak
gates on Linux and macOS before publishing.

New tests cover the result order, `$first`/`$last` and limits of a sharded merge
above the partitioned threshold, against the serial run. The spill tests cover
typed-visitor ownership, cancellation, overflow reporting, skew, wide strings
and descriptor budgets.
