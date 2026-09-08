# aptevadb — a from-scratch Go key-value engine, benchmarked against SQLite and Pebble

Started as two operations, `Insert` and `Find`; now an embedded key-value and
document store with collections, deletes, cross-collection batches, cursors,
large values, secondary indexes, MongoDB-style aggregation with `$lookup`
joins, snapshots, crash-safe checkpoints and a fault-injected recovery test.

```go
import "github.com/apteva/aptevadb"

db, _ := aptevadb.Open(dir, aptevadb.Options{})
users, _ := db.Collection("users")
orders, _ := db.Collection("orders")
users.InsertJSON([]byte("u1"), []byte(`{"id":1,"country":"FR"}`))
b := orders.NewBatch()                       // one atomic batch, two collections
b.InsertJSON([]byte("o1"), []byte(`{"user_id":1,"amount":42}`))
b.With(users).InsertJSON([]byte("u1"), []byte(`{"id":1,"country":"FR","orders":1}`))
b.Commit()
orders.CreateIndex(ctx, "by_user", "user_id")
rows, _ := orders.Aggregate(ctx, []byte(`[
  {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "id", "as": "u"}},
  {"$unwind": "$u"},
  {"$group": {"_id": "$u.country", "total": {"$sum": "$amount"}}}]`))
```

Version 0.1.0, on-disk format 3. The engine has no dependencies. See
[FORMAT.md](FORMAT.md) for the file format and its compatibility policy,
[DURABILITY.md](DURABILITY.md) for what survives which crash, and
[CHANGELOG.md](CHANGELOG.md). CI runs vet, the tests, the race detector and
a fuzz smoke test on Linux and macOS; benchmarks were taken on macOS on
arm64. Windows is not supported.

Layout: the root package is the engine; `cmd/aptevadbctl` inspects and
maintains a database; `bench/` is a separate module holding the benchmark
harness, so its dependencies (Pebble and two SQLite bindings) stay out of
the engine's graph.

## Design

* **B+tree over fixed-size 16KB slotted pages** (`page.go`). Each slot carries the
  cell offset, the key length and an 8-byte big-endian key prefix, so binary search
  runs over the compact slot array and touches the cell only on a prefix tie.
* **Mapped reads, private writes** (`pager.go`). The data file is mapped
  read-only (the mapping doubles as the file grows). A clean page is a view into
  the OS page cache: loading it costs a small struct and a page fault, no copy and
  no syscall, and the cache is shared across threads and processes. The writer
  copies a page into a private buffer before modifying it; a checkpoint writes the
  buffer and hands the page back to the mapping. With `Options.CacheBytes` set,
  clean leaf pages are evicted by a CLOCK sweep (internal pages stay resident so
  checkpoints can always reach dirty leaves) and a checkpoint runs once dirty pages
  reach three quarters of the limit.
* **Sibling balancing** (`tree.go`, `balanceLeaf`). A leaf that overflows
  first sheds cells to a neighbour with room and rewrites the separator; it splits
  only when both neighbours are full. Leaves end up ~87% full after random inserts
  instead of ~69%.
* **Memtable** (`memtable.go`). With a cache limit, inserts are buffered
  in memory (they are already in the log) and applied to the tree in key
  order once `Options.MemtableBytes` is reached, so each leaf write carries
  many records. The memtable is a small in-memory B+tree (wide nodes,
  contiguous arrays) with a map for point reads: a scan copies out only the
  entries of its range, so probes stay cheap while writers run. Applying a
  sorted batch reuses the leaf reached for the previous key and splits an
  overflowing leaf at the insertion point, keeping the left part full. If dirty pages fill
  the cache mid-apply, a partial checkpoint writes them out but leaves the log
  alone, so replay after a crash rebuilds the memtable. Default budget: half
  of `CacheBytes` with a cache limit; without one, inserts go straight to
  the tree, which is faster on this design (a random insert costs about
  1µs, a skip-list insert with its allocations about as much).
* **Checkpoint vacuum**. Page ids are handed out lowest-first, and every full
  checkpoint moves the highest-numbered live pages into low free slots and
  truncates the free tail, so copy-on-write holes never accumulate in the file.
* **Compact** (`DB.Compact`) rewrites the tree in key order into a fresh file and
  swaps it in, reclaiming the remaining split slack. The database is locked while
  it runs.
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
* **Concurrency**: one writer, many readers, under a `sync.RWMutex`. Readers never
  allocate; `Find` appends the value into a caller-provided buffer.
* **Cursors** step within the leaf reached last, validated by a tree generation
  counter, and descend from the root only at leaf boundaries or after a
  change, so ordered iteration costs tens of nanoseconds per key.

* **Batches** (`db.go`, `Batch`): many inserts, one log write, one flush.
  Log records carry a continuation flag, so a batch replays all-or-nothing.
* **Documents and aggregation** (`doc.go`, `scan.go`, `aggregate.go`). Flat JSON objects are stored in a typed binary encoding
  and read back with `InsertJSON`/`FindJSON`. `Scan` walks the tree in key order,
  holding the reader lock one leaf at a time. `Aggregate` runs a MongoDB-style
  pipeline: `$match` (`$eq $ne $gt $gte $lt $lte $in`), `$group` (`_id` null,
  `"$field"` or composite; `$sum $avg $min $max $count $first $last`), `$sort`,
  `$skip`, `$limit`, `$project`, `$count`. Leading `$match`/`$skip`/`$limit`
  stages and a following `$group` run as one streaming pass that extracts every
  referenced field from each document in a single walk. `AggregateParallel`
  splits the tree into key ranges at internal separators (`ShardBounds`), runs
  a `$match`/`$group` prefix on one goroutine per range and merges the
  accumulators, which SQLite cannot do for a single query. Nested documents and
  arrays are not supported yet.

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
  -run Crash ./engine` runs a longer soak. Fuzz targets cover documents, the
  pipeline parser and page operations (`go test -fuzz FuzzDocument ./engine`).
* **Tooling**: `cmd/aptevadbctl` with `stats`, `verify`, `get`, `dump`, `agg`,
  `compact`, `backup`.
* **Background checkpoints** (`checkpoint.go`). A checkpoint folds the
  frozen memtable into the tree in small locked chunks, then captures the dirty
  set under the lock in memory only (final page ids, patched parents, free list,
  meta page), and does every write and fsync outside the lock. A writer touching
  a captured page copies it first. Log rotation, file truncation, old-log
  deletion and the directory sync all happen off the lock; the log's bulk writes
  run on the flusher goroutine with a second buffer. Flow control: a writer waits
  only when the active memtable reaches four times its budget.
* **Non-blocking compaction and backup** copy a snapshot while writers continue,
  replay the log delta into the copy in rounds, and hold the lock only for the
  last few records and two renames; the copy's meta points back into the shared
  log so nothing is flushed under the lock. With 64MB cache and 16MB memtable,
  the longest single write wait during checkpoints and compactions measured 4 to
  6 ms.
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
  on the fetched document. `VerifyIndexes` cross-checks entries and
  documents.
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
  indexes, keys and catalog entry in bounded batches.
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

Read transactions that span several statements with a write set, composite
and unique indexes, index-only scans, join reordering beyond the one shape
described above, the `let` and `pipeline` forms of `$lookup`, per-collection
statistics, buffered (Bε) internal nodes, a runtime page-size option,
Windows support, and any SQL layer.
