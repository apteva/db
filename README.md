# AptevaDB

A Go document and key-value database with named collections, atomic transactions,
scalar and compound indexes with mixed directions, snapshots, aggregation, stored computed fields and computed
collections. Use it embedded, or run the HTTP server and connect with the Go driver.

**Public alpha: v0.2.0-alpha.10.** Go 1.25+, Linux and macOS, amd64 and arm64.
Windows, replication and SQL are not supported. The module has no external
runtime dependencies. The file format is 10; opening formats 3–9 upgrades them
permanently, so keep a backup before upgrading.

```sh
go get github.com/apteva/db@v0.2.0-alpha.10
go install github.com/apteva/db/cmd/aptevadbctl@v0.2.0-alpha.10
go install github.com/apteva/db/cmd/aptevadb-server@v0.2.0-alpha.10
```

```go
import (
    "context"
    db "github.com/apteva/db"
)

database, err := db.Open("./data", db.DurableOptions())
if err != nil { return err }
defer database.Close()
users, err := database.Collection("users")
if err != nil { return err }
err = users.InsertJSON([]byte("u1"), []byte(`{"name":"Ada","age":30}`))
if err != nil { return err }
err = users.CreateUniqueIndex(context.Background(), "name", "name")
if err != nil { return err }
var user struct { Name string; Age int }
found, err := users.FindOne(context.Background(), []byte(`{"age":{"$gte":18}}`), &user)
```

Run `go run ./examples/embedded` for a complete example with error handling.
The root package is the embedded Go API; `client` is the remote Go driver.
See [USAGE.md](USAGE.md) for transactions, streaming queries and operation
contracts, [OPERATIONS.md](OPERATIONS.md) for backup/import/recovery and limits,
[SERVER.md](SERVER.md) for the server/driver and HTTP protocol,
[DURABILITY.md](DURABILITY.md), [FORMAT.md](FORMAT.md), and [CHANGELOG.md](CHANGELOG.md).

**Durability:** `DurableOptions()` enables full synchronous flushing.
`Options{}` deliberately uses asynchronous writes for compatibility and benchmark
comparisons; recent acknowledged writes can be lost after a crash. The server
and maintenance CLI use the durable profile.

One process owns a database directory. Within it, DB and collection handles
support concurrent use; writes serialize. Transactions, batches and cursors
belong to one goroutine. Close snapshots/cursors/transactions promptly: they
retain older pages and prevent compaction while held.

Alpha.5 adds [native change feeds](CHANGE_FEEDS.md): opt-in capture,
filtered/projected subscriptions, replay and snapshot handoff for embedded apps
and the HTTP Go client. Opening older databases performs a one-way format-10
migration; back up with the original binary first. The
[change-feed validation report](bench/results/change-feeds/VALIDATION.md) includes
all trials, CPU/RSS results and measured capture/fan-out costs.

The [alpha.4 aggregation and release report](bench/results/alpha4/VALIDATION.md)
records exact integer accumulation/parallel merges, non-null field counting,
format-8 migration, public Go/client/CLI checks and all 24 benchmark processes.
Serial integer SUM/AVG remained essentially flat; background activity affected
parallel timings. All archive hashes and the downloaded daemon were verified.

The [scan optimization report](bench/results/scan-optimization/VALIDATION.md)
records roughly 22% lower cursor-scan time on `main`, 128 retained benchmark
processes and Linux/macOS validation. Some other medians were slower on the busy
workstation; the report includes all samples and does not claim universal improvement.

The [alpha.3 cursor and index validation report](bench/results/alpha3/VALIDATION.md)
includes the two reported regressions, 8,160-byte encoded entries, matched-harness
alpha.2 comparisons and large-entry results against Pebble and both SQLite bindings.
Raw scans and some short write samples were slower; all measurements are retained.

The [alpha.2 compatibility/speed report](bench/results/alpha2/VALIDATION.md) includes
three repeats against Pebble/SQLite and paired alpha.1 comparisons. The
[alpha.1 validation report](bench/results/public-alpha/VALIDATION.md)
retains 30 resource and 24 feature trials against Pebble and both SQLite bindings.
Other historical results under `bench/results/` describe their original builds. The separate
`bench` module compares SQLite and Pebble without adding engine dependencies.
KV comparisons use `Upsert` (matching the other engines' replacement behavior);
`Insert` now performs a duplicate-key check. See [BENCHMARKS.md](BENCHMARKS.md).

Alpha.10 returns aggregation errors unchanged; its
[publication verification](bench/results/release-alpha10/RELEASE.md) records
Linux/macOS CI and soak, public Go installation and all four archive checksums.

Alpha.9 adds the [typed group visitor](USAGE.md#typed-group-visitor-alpha9) and
faster in-memory and spill aggregation; the [comparison with alpha.8](bench/results/alpha9-performance/REPORT.md)
retains 48 interleaved trials and the remaining tradeoffs.
The [publication verification](bench/results/alpha9-performance/release-alpha9/RELEASE.md)
confirms Linux/macOS CI and soak, public Go installation, all four archive
checksums, the downloaded server/driver and alpha.8 directory compatibility.

Alpha.8 adds [bounded native aggregation](USAGE.md#bounded-native-aggregation-alpha8)
and [resident-memory controls](OPERATIONS.md#resident-memory-controls-alpha8).
The [comparison with alpha.7](bench/results/aggregation-memory/REPORT.md) records
110 final trials, correctness checks and remaining performance tradeoffs.
The [publication verification](bench/results/aggregation-memory/release-alpha8/RELEASE.md)
confirms Linux/macOS CI, public Go installation, all four archive checksums,
the downloaded server/driver and alpha.7 directory compatibility.

## Stored computed fields

Define deterministic scalar fields on an **empty collection**, then write source
fields normally. The database stores the derived values with each document:

```go
err := orders.DefineComputedFields(ctx, []byte(`{
  "total": {"$multiply": ["$qty", "$price"]},
  "email_key": {"$lower": "$email"},
  "net": {"$subtract": ["$total", {"$ifNull": ["$discount", 0]}]}
}`))
if err != nil { return err }
err = orders.InsertJSON([]byte("o1"), []byte(`{
  "qty": 3, "price": 700, "discount": 100, "email": "A@EXAMPLE.COM"
}`)) // stores total=2100, net=2000, email_key="a@example.com"
if err != nil { return err }
err = orders.CreateIndex(ctx, "by_email_key", "email_key")
```

`DB.DefineComputedFields` addresses the default collection.
`Collection.ComputedFields`, `DB.ComputedFields` and `Snapshot.ComputedFields`
return detached definition maps; snapshot collection handles return the rules
at that snapshot. Reads, cursors, aggregation and persistent readers use the
stored values. Supplied values for computed output names are overwritten.
Binary `Insert`, JSON writes and batches across collections all enforce the
same rules. An evaluation error aborts the entire batch before any log records
or index entries are written. Recovery replays stored bytes without evaluating
expressions again. Ordinary indexes work on computed outputs.

Definitions map top-level output names to expressions. A string `"$a.b"` reads
a source path; an exact computed output name may reference another definition.
Dependencies are sorted automatically and cycles are rejected. Use `$literal`
to store a string beginning with `$`. Supported version-one operators:

| Operator | Arguments / result |
|---|---|
| `$add`, `$multiply` | 2–16 numeric expressions |
| `$subtract` | Two numeric expressions |
| `$divide` | Two numeric expressions; floating result, zero divisor rejected |
| `$lower`, `$upper` | One string expression; ASCII casing, other bytes preserved |
| `$concat` | 1–16 string expressions |
| `$ifNull` | `[input, fallback]`; evaluates fallback only for missing/null input |
| `$literal` | A null, boolean, string or finite numeric scalar |

Missing and null inputs propagate null. Other type mismatches fail with
`ErrComputedField`; integer-only arithmetic preserves int64 precision and
rejects overflow, and floating arithmetic rejects non-finite results. Integer
literals outside int64 are rejected. Explicit floating literals use float64.
Configured collections accept validated binary documents up to 16 MiB,
with unique field names, at most 4,096 total fields and nesting up to 16 levels.
Definitions allow at most 64 outputs, 64 KiB of input/canonical JSON, 1,024 expression nodes
and expression depth 32. Outputs are scalars, not nested document/array fields.

Installing, replacing or removing definitions requires an empty collection
(`ErrComputedNonEmpty` otherwise); `{}` removes them. There is no automatic
backfill yet. Deleted documents do not prevent a schema change, even before a
checkpoint. Dropping a collection drops its rules too.

**Format migration:** opening a format-3 through format-9 database upgrades both
metadata slots durably to format 10 before returning. Older binaries then refuse
to open it; keep a backup if you need to use one. Format 10 adds optional WAL
commit markers for change capture; page/document layouts and logical export
version 2 remain unchanged. Other format versions are refused.

Benchmark the feature with `cd bench` and
`GOTOOLCHAIN=local GOWORK=off go run . -computed -n 100000 -reps 3`.
The harness retains every trial and checks every derived result. It compares
native fields, application materialization and read-time computation with
Pebble and SQLite stored generated columns, using non-durable 1,000-row
batches/transactions. SQL uses typed columns; the other stores use binary
documents. Index build and query phases are reported separately. This workload
measures feature costs and query benefits, not a universal database ranking.

## Stored computed collections

Create a collection derived from an existing source. Creation atomically
backfills current documents; future source upserts, deletes and cross-collection
batches maintain the stored results and their indexes in the same transaction.
Computed fields on the source are evaluated first.

```go
totals, err := database.CreateComputedCollection(ctx, "paid_totals", "orders", []byte(`[
  {"$match": {"paid": true}},
  {"$group": {"_id": "$customer", "total": {"$sum": "$amount"}, "n": {"$count": {}}}}
]`))
if err != nil { return err }
key, err := db.ComputedGroupKey(int64(42))
if err != nil { return err }
doc, found, err := totals.Find(key, nil) // current total for customer 42
```

The first version supports up to 16 stages: leading `$match` stages followed
by an optional final `$project` or `$group`. An empty pipeline copies the source.
Filtered/projected collections retain the source primary keys. Projection uses
top-level 0/1 or boolean flags with the same semantics as ordinary aggregation.
Filters support scalar comparisons and `$in`, including nested source paths.

Groups use `_id: null` or one `"$field.path"` reference to a scalar. Outputs
support exact int64 `$sum` (a field or integer constant), `$count: {}`,
and `$countNonNull: "$field.path"`;
`$sum: 1` also counts rows. Missing/null sum inputs contribute zero; other
non-integer inputs fail. Checked 128-bit intermediate arithmetic permits a
batch to exceed int64 temporarily if its final totals fit. Zero-sum groups
remain present until their last matching member is deleted. Missing and null
IDs share a key; integral floats normalize to int64. `ComputedGroupKey` accepts
nil, bool, int, int64, finite float64, string and valid scalar `json.Number`.
Integer IDs remain distinct above 2^53. Encoded IDs are limited to 4,026 bytes.

Computed collections are read-only: direct inserts, deletes and computed-field
definitions return `ErrComputedCollectionReadOnly`. Their ordinary indexes,
cursors, aggregation, snapshots and persistent readers work normally.
`Collection.ComputedCollection()` returns a detached definition, or nil for an
ordinary collection; a snapshot handle reads the definition at that snapshot.

The destination must be a new named collection; the source must already exist
(use `""` for the default collection). There are at most 64 dependents per source.
Chaining, joins, array expansion, window stages, composite IDs, averages, extrema
and floating-point sums are outside this version. Definitions are immutable;
drop and recreate to change them. Dropping a source with dependents returns
`ErrComputedCollectionDependency`; drop dependents first. Dropping a computed
collection removes its rows, membership counters, indexes and definition atomically.

Creation and drop block readers and writers while staging a single transaction, bounded
at 256 MiB (`ErrComputedCollectionLimit`). Context cancellation and validation
failures before logging leave the database unchanged. Sources must contain
validated binary documents, including documents that do not match a filter.
A source-write validation or aggregate overflow error aborts the entire batch
before logging. WAL recovery replays stored results without recomputing; backup
and compaction preserve definitions, results and hidden membership counters.

Benchmark with `cd bench` and
`GOTOOLCHAIN=local GOWORK=off go run . -computedcollections -n 250000 -reps 3`.
It retains all trials and checks every summary against an independent integer
model, including group moves, filter changes and deletion. Comparisons include
on-demand queries, Pebble application-maintained summaries and SQLite triggers,
with 1,000-row batches/transactions, WAL on and sync off. On-demand point queries
use a source index where supported; stored point queries use primary keys.
Benchmarks report write costs, all-group queries and 10/32-reader throughput.

## Design

* **B+tree over fixed-size 16KB slotted pages** (`page.go`). Each slot carries the
  cell offset, the key length and an 8-byte big-endian key prefix, so binary search
  runs over the compact slot array and touches the cell only on a prefix tie.
* **Mapped reads, private writes** (`pager.go`). The data file uses stable
  read-only 64 MiB segments. Growing it adds segments without duplicating old
  mappings. Verified clean reads use mapped bytes directly, even after a page
  descriptor is evicted. Dirty pages stay resident; with `Options.CacheBytes`
  set, partial checkpoints begin at one quarter of the page target.
* **Buffer ownership** (`page_buffers.go`, `node_buffers.go`, `memtable_buffers.go`,
  `wal_buffer.go`). Writable pages, page descriptors and memtable records/metadata
  use owned native buffers. Snapshots, cursors and checkpoint I/O retain explicit
  references. Retired buffers return to the OS without waiting for Go GC. Small
  internal pages have separate buffers so they cannot pin mostly empty leaf
  arenas. Normal WAL flush buffers stay on the Go heap; bursts above 2 MiB use
  reclaimable mappings. Both cache modes stream compaction/backup output and
  reuse checkpoint vectors/scratch storage. Unneeded old file-page mappings are
  reclaimed after copies or checkpoint/snapshot retirement, preserving file
  contents and all live views. These allocations sit outside Go's heap; measure
  process RSS and physical footprint as well as `runtime.MemStats`.
  `CacheBytes` is a logical page target, not a hard total-memory limit.
* **Sibling balancing** (`tree.go`, `balanceLeaf`). A leaf that overflows
  first sheds cells to a neighbour with room and rewrites the separator; it splits
  only when both neighbours are full. Leaves end up ~87% full after random inserts
  instead of ~69%.
* **Memtable** (`memtable.go`). With a cache limit, inserts are buffered
  in memory (they are already in the log) and applied to the tree in key
  order once `Options.MemtableBytes` is reached, so each leaf write carries
  many records. The memtable is a small in-memory B+tree (wide nodes,
  contiguous arrays) with a keyed hash table for point reads: a scan copies out only the
  entries of its range, so probes stay cheap while writers run. Applying a
  sorted batch reuses the leaf reached for the previous key and splits an
  overflowing leaf at the insertion point, keeping the left part full. If dirty pages fill
  the cache mid-apply, a partial checkpoint writes them out but leaves the log
  alone, so replay after a crash rebuilds the memtable. Default budget: one half
  of `CacheBytes` with a cache limit; without one, inserts go straight to
  the tree, which is faster on this design (a random insert costs about
  1µs, a skip-list insert with its allocations about as much).
* **Checkpoint vacuum**. Page ids are handed out lowest-first, and every full
  checkpoint moves the highest-numbered live pages into low free slots and
  truncates the free tail, so copy-on-write holes never accumulate in the file.
* **Compact** (`DB.Compact`) rewrites the tree in key order into a fresh file and
  swaps it in, reclaiming the remaining split slack. It copies a snapshot while writes continue, briefly locking to swap the file.
* **Write-ahead log with group commit** (`wal.go`). Logical records
  `[len][crc32c][klen][key][value]`. With `Options.Sync` every Insert waits for an
  fsync that concurrent inserts share: while one fsync runs, arriving writers
  wait on a condition; when it finishes, every writer it covered returns at
  once and one of the others leads the next, which covers everything appended
  meanwhile. Once concurrency has shown itself, a leader pauses 150µs before
  its fsync so the writers just released can join the group instead of each
  starting the next fsync alone. Without `Sync`, records reach the OS every
  10ms or every 1MB. A log generation made entirely dead by a full checkpoint
  is dropped at close and after compaction.
* **Copy-on-write checkpoints** (`db.go`, `tree.go`). Dirty pages are
  written to fresh page ids, parents are patched bottom-up, then one of two meta
  pages is flipped. The data file is therefore never torn; the log is truncated
  under a generation number so a crash between the two steps replays correctly.
  Freed ids are reused one checkpoint later; the free list is rebuilt at open by
  walking internal pages only.
* **Append optimization**: inserting past the last key of the tree starts a fresh
  leaf instead of splitting in the middle, so sequential loads pack pages tightly.
* **Concurrency**: one writer, many readers. Ordinary `Find` uses a
  `sync.RWMutex` and appends into a caller-provided buffer. For sustained
  parallel point reads, create one `Reader` per worker with `DB.NewReader`,
  `Collection.NewReader`, or `Snapshot.NewReader`, reuse it for `Find` calls,
  then `Close` it. Persistent handles distribute read counters across padded
  lanes. Writers exclude both ordinary and handle reads; no lock is held
  between handle calls. A live handle sees current values, while a snapshot
  handle retains its snapshot's view. Close all handles when finished so
  writes can skip lane coordination. Short keys and reused value buffers
  avoid allocations on successful point reads.
* **Cursors** step within the leaf reached last, validated by a tree generation
  counter, and descend from the root only at leaf boundaries or after a
  change, so ordered iteration costs tens of nanoseconds per key.

* **Batches** (`db.go`, `Batch`): many inserts, one log write, one flush.
  Log records carry a continuation flag, so a batch replays all-or-nothing.
* **Documents and aggregation** (`doc.go`, `scan.go`, `aggregate.go`). JSON objects are stored in a typed binary encoding
  and read back with `InsertJSON`/`FindJSON`. `Scan` walks the tree in key order through one consistent snapshot,
  holding the reader lock one leaf at a time. `Aggregate` runs a MongoDB-style
  pipeline: `$match` (`$eq $ne $gt $gte $lt $lte $in`), `$group` (`_id` null,
  `"$field"` or composite; `$sum $avg $min $max $count $countNonNull $first $last`), `$sort`,
  `$skip`, `$limit`, `$project`, `$count`. Leading `$match`/`$skip`/`$limit`
  stages and a following `$group` run as one streaming pass that extracts every
  referenced field from each document in a single walk. `AggregateParallel`
  splits the tree into key ranges at internal separators (`ShardBounds`), runs
  a `$match`/`$group` prefix on one goroutine per range and merges the
  accumulators. Nested documents and arrays are supported, with dotted paths
  and `$unwind` for array expansion.

* **Delete** removes a key through the same log, memtable and tree paths;
  emptied leaves are unlinked, leaves under a quarter full merge into a
  neighbour, and a root with one child collapses.
* **Large values** beyond a quarter page live in overflow page chains; the leaf
  cell keeps an 8-byte pointer. Values up to 1GB.
* **Persisted free list** so opening a large file reads only the list and the
  root path; a corrupt list falls back to a full walk.
* **Page checksums** (crc32c) on every page, verified on first read; `Verify`
  walks the whole file checking checksums, key order, separators, overflow
  chains and free-list consistency.
* **Safety**: one process per database (`flock`), a format version in the meta
  page, closed handles refuse calls, and an I/O error during a checkpoint puts
  the handle into a failed state that refuses further writes.
* **Cursor** with `Seek`, `First`, `Last`, `Next`, `Prev`, merging the memtable.
* **Backup** writes a compact copy into another directory.
* **Documents** may nest objects and arrays (16 levels); field references in
  pipelines may be dotted paths; `$unwind` expands arrays; an array field matches
  a predicate when any element does; `$sort` takes several keys in order;
  `Limits` cap groups and rows so a query cannot exhaust memory.
* **Crash testing**: `TestCrashRecoveryWithFaultInjection` runs hundreds of
  rounds through a file layer that trips at a random write, then loses a random
  suffix of unsynced writes and may tear the first lost one, per inode. After
  each recovery every acknowledged write must be present, an in-flight batch
  must be all-or-nothing, and `Verify` must pass. `CRASH_ROUNDS=300 go test
  -run Crash .` runs a longer soak. Fuzz targets cover documents, the
  pipeline parser and page operations (`go test -run '^$' -fuzz '^FuzzDocument$' -fuzztime 30s .`).
* **Tooling**: `cmd/aptevadbctl` with `stats`, `verify`, `get`, `dump`, `agg`,
  `compact`, `backup`, `restore`, `export`, `import`, `collections`, `version`
  and `format`.
* **Background checkpoints** (`checkpoint.go`). A checkpoint folds the
  frozen memtable into the tree in small locked chunks, then captures the dirty
  set under the lock in memory only (final page ids, patched parents, free list,
  meta page), and does every write and fsync outside the lock. A writer touching
  a captured page copies it first. Log rotation, file truncation, old-log
  deletion and the directory sync all happen off the lock; the log's bulk writes
  run on the flusher goroutine with a second buffer. Flow control: a writer waits
  when the active memtable reaches twice its budget while the previous table is applying.
  The budget includes native record/metadata storage and replaced values.
* **Non-blocking compaction and backup** copy a snapshot while writers continue,
  replay the log delta into the copy in rounds, and hold the lock only for the
  last few records and two renames; the copy's meta points back into the shared
  log so nothing is flushed under the lock. The new mappings are warmed only after
  the old file has closed, preserving read performance without retaining two trees.
  With 64MB cache and 16MB memtable,
  the longest single write wait during checkpoints and compactions measured 4 to
  6 ms in historical runs; see BENCHMARKS.md for current validation.
* **Snapshots** (`snapshot.go`). `Snapshot()` pins the root and memtable;
  a page modified afterwards is copied first and its old version kept in the
  snapshot's overlay; page ids it may reference are quarantined, not reused.
  Zero cost while no snapshot is open. Snapshots offer `Find`, `Scan`, cursors
  and aggregation.
* **Secondary indexes** (`index.go`). Entries live under a reserved prefix
  in the same tree, maintained in the same batch as the document, multikey for
  arrays, dotted paths. Indexes belong to a collection. Building one reads the
  documents from a snapshot, sorts the entries and inserts them in order
  without log records (leaves fill completely, one descent per leaf), then
  checkpoints before the catalog entry is logged; keys written meanwhile are
  left to the normal maintenance, a crash before the catalog entry leaves
  entries the next open sweeps, and compaction is refused during the build.
  The planner collects the primary keys an index names for the predicates of
  a leading `$match` (equality, `$in`, ranges) in one pass over the entries,
  takes the most selective candidate, and reads through it only while it
  names at most an eighth of the collection, then re-checks every predicate
  on the fetched document. Unique index creation instead stages and validates
  the whole index before publishing it atomically. `VerifyIndexes` cross-checks
  entries, documents and unique constraints.
* **Execution**. `Aggregate` runs on every core by default: a `$match`/`$group`
  (or `$count`) pipeline without a window is split into key-range shards of
  the tree, or into shards of an index's primary keys, and the per-shard
  groups are merged pairwise in parallel rounds; `AggregateParallel(ctx, p, 1)` is the single-core run and
  `Options.Parallelism` sets the default. A
  collection range with nothing buffered is read leaf by leaf in place under
  the reader lock, without copying, when no step of the pass uses the
  database itself.
* **Collections** (`collection.go`). A collection is a key range of the
  one tree: every user key carries a one-byte collection tag (three bytes past
  253 collections), so a named collection's keys are exactly as long as the
  default collection's and the slot prefix keeps its seven key bytes. The
  catalog is a reserved key per collection, written to the log before the
  collection is visible. `Collection` offers the whole API (`Insert`, `Find`,
  `Scan`, cursors, batches, indexes, aggregation, snapshots); the `DB` methods
  address the default collection. A batch spans collections through
  `Batch.With`, and commits as one log record group. `DropCollection` removes
  indexes, keys and catalog entry in one atomic WAL group. Staging is capped at
  256 MiB; cancellation or a staging-limit error leaves the collection intact.
* **`$lookup` joins** (`lookup.go`). The equality form
  (`from`/`localField`/`foreignField`/`as`), read through the same snapshot as
  the source. A `$lookup` unwound on its own field joins in the streaming pass,
  with following `$match` stages and a `$group` or `$count`: one output record
  per match, or one with the foreign fields missing under
  `preserveNullAndEmptyArrays` (left join, anti-join through `{"as": null}`).
  Otherwise the join runs on materialised rows and `as` holds the matched
  documents. The join is a hash join over the foreign collection (numbers keyed
  by their bits, count-only when nothing but existence is needed, filtered to
  the outer keys after a bounded pre-pass when the outer side is small), or
  index probes when the foreign field is indexed and the expected number of
  probes costs less than hashing the collection (its size is estimated from the
  tree shape); a run that exceeds its probe budget switches to the table.
  Null and a missing field match each other; an array on either side matches
  by any element, each document once. Parallel runs build one table and share
  it across shards. Consecutive probes reuse the index leaf reached last time
  (a sorted outer side walks the index in order), validated by a tree
  generation counter. An inner join followed by a `$group` is turned around
  when the foreign side is the selective one (its `$match` stages can use an
  index) and this side's join field is indexed: the pipeline then starts from
  the foreign collection and probes back, with identical results.

## Run

```bash
go test -race .
(cd bench && go build -o ../aptevadb-bench .) && ./aptevadb-bench -n 1000000 -workloads random,seq
```

Aggregation benchmark (ours, Pebble driven by our executor, SQLite SQL):

```bash
./aptevadb-bench -agg -n 1000000 -engines ours,ours-par,pebble,pebble-par,sqlite-c
```

Join benchmark: 100k users and 1M orders in two collections, seven `$lookup`
pipelines against the equivalent SQL joins, results cross-checked:

```bash
./aptevadb-bench -join -n 1000000 -engines ours,ours-par,sqlite-c
```

Several collections in one file (keys spread over four collections, four
tables for SQLite, a one-byte key prefix for Pebble):

```bash
./aptevadb-bench -n 1000000 -engines ours,ours-coll4,pebble,pebble-coll4,sqlite-c,sqlite-c-tables4
```

Durable-insert benchmark with 1, 10 and 50 writers:

```bash
./aptevadb-bench -durable -n 3000 -engines ours-sync,ours-sync-batch1000,pebble-sync,sqlite-c-sync
```

Memory-constrained run (64MB cache for every engine) and compaction:

```bash
./aptevadb-bench -n 1000000 -cachemb 64 -ourscachemb 64 -engines ours,pebble,sqlite-c-bulk -workloads random
./aptevadb-bench -n 1000000 -nopar -engines ours,ours-compact -workloads random
```

Engines: `ours`, `ours-compact`, `ours-sync`, `ours-sync-batch1000`, `ours-sync-barrier`, `ours-sync-fsync`, `pebble`, `pebble-sync`, `pebble-batch1000`,
`sqlite-c` (C SQLite via crawshaw.io/sqlite, one autocommit per insert),
`sqlite-c-txn1000`, `sqlite-c-bulk` (one transaction), `sqlite-c-sync`,
and the pure-Go `sqlite-go*` equivalents via zombiezen.com/go/sqlite (modernc).

SQLite runs a `WITHOUT ROWID` table in WAL mode with a 512MB page cache.
Pebble runs with a 512MB block cache, 64MB memtable and bloom filters.
The `settle` phase is a full compaction for Pebble, a checkpoint followed by
`Compact` for the engine, and a checkpoint for SQLite, so `find` numbers and
the `disk` column reflect each engine's packed layout. `-reps N` runs each
engine N times on fresh directories and keeps the best of every phase.

## Not built yet

Composite indexes, index-only scans, join reordering beyond the one shape
described above, the `let` and `pipeline` forms of `$lookup`, per-collection
statistics, buffered (Bε) internal nodes, a runtime page-size option,
Windows support, and any SQL layer.

The [alpha.6 count validation report](bench/results/counts/VALIDATION.md) records
all fresh-fixture count comparisons and paired read/write/scan guards.
Filtered counts use `collection.CountDocuments(ctx, filter, db.CountOptions{})`.
Scalar equality prefixes of exact compound indexes count without loading
matching documents when all predicates are covered; other filters retain
snapshot-consistent document evaluation. See [USAGE.md](USAGE.md#filtered-document-counts)
for limits, null/sparse/multikey semantics, execution statistics and driver use.
