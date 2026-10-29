# Changelog

All notable changes to this module. The format follows Keep a Changelog;
versions follow semantic versioning. Before 1.0, a minor version may change
the API or the on-disk format; both are listed here when it happens, with
the migration.

## [0.2.0-alpha.10] - 2026-10-29

- Fix: aggregation returns errors unchanged again. Since alpha.8 every error
  from Aggregate*, RunEach, AggregateGroups and RunGroups was wrapped by
  errors.Join with a nil cleanup error, so an error returned by a GroupView
  callback compared unequal to itself with == (errors.Is still matched).
  A cleanup failure is still joined onto the result. No API, format or
  performance change.

## [0.2.0-alpha.9] - 2026-10-28

- Add AggregateGroups/RunGroups with a borrowed, typed GroupView for pipelines
  ending in $group. It skips per-group result maps, reports integer SUM overflow
  instead of rounding it, and exposes the exact signed 128-bit subtotal.
- Speed up bounded (spill) aggregation: adaptive partition fanout with budgeted
  I/O buffers, a compact temporary record codec, arena-backed reconstruction and
  compact COUNT state. Temporary spill records changed; nothing persistent did.
- Speed up in-memory aggregation: extract filter fields first and skip the rest
  for rejected documents, decode each document's directory once, check field
  names before decoding values, and add float-vs-number and string-equality
  predicate fast paths (NaN ordering unchanged).
- Sharded $group with more than 8,192 groups merges shard results by key hash
  on all workers instead of pairwise rounds; group keys share per-query arenas
  and the memory budget is charged per slab. Row order, $first/$last and limits
  match the serial run.
- Index builds skip the initial checkpoint when nothing is buffered, checkpoints
  skip the page sync when they write no pages, released one-page buffers are
  reused instead of unmapped, and build entries sort without reflection.
- Fix: creating a log generation now fsyncs the database directory. Previously
  a power loss right after a log rotation could lose the new file's directory
  entry and, with Sync, records already acknowledged in it. DURABILITY.md now
  states that deleting checkpointed generations is not fsynced, and why.
- Fix: a checkpoint whose free list sat just above a multiple of 4,092 ids
  panicked while writing the list, because each page of the list takes its id
  from the free ids it records (present since 0.1.0). Since alpha.7 the panic
  deadlocked the checkpointer under the writer lock instead of crashing, so
  the database hung; the 60-minute macOS soak timed out on alpha.8 this way.
  The list now grows until its remaining ids fit, and a checkpoint panic crashes.
- Benchmark harness: the batched AptevaDB settle step now compacts, matching the
  Pebble batch settle, so disk sizes compare like for like.
- Retain 48 interleaved fresh-process trials against alpha.8. One-million-group
  spill: COUNT 56% and numeric 63% faster, 82–85% fewer spill bytes, one merge
  pass instead of 17; numeric spill peak RSS rose 5 MiB. In-memory aggregation is
  11–54% faster on one core; KV, batched and durable writes are unchanged within
  noise. The workstation was busy; no zero-regression claim is made.
- Format remains 10. No migration.

## [0.2.0-alpha.8] - 2026-10-27

- Add native AggregateEach/RunEach and collecting WithOptions APIs, with proven
  compound-index streaming groups, early group limits and explicit strategy stats.
- Add opt-in bounded hash-partition aggregation and stable external sorting;
  preserve typed/exact integer state, one snapshot, cancellation, shared disk
  quotas, execution leases and automatic abandoned scratch cleanup.
- Account for private page/descriptor/memtable/WAL allocations and aggregation
  reservations, including leased and temporary native stores. Expose MemoryStats,
  opt-in cold mapped-page reclamation/TrimMemory and managed-buffer admission
  with pre-WAL pressure errors or cancellable waiting for retained views.
- Preserve default cache policy and file format 10. New streaming/options APIs
  are embedded Go features; no new HTTP streaming endpoint. Existing format-10
  databases require no new migration.
- Retain 110 final benchmark trials against alpha.7, including one-million-document
  guards. Ordered groups reduce CPU/peak memory and reclamation reduces idle RSS;
  opt-in disk spill trades speed for bounded execution. Some guard medians remain
  slower; the report does not claim zero regressions or a universal advantage.

## [0.2.0-alpha.7] - 2026-10-27

- Reduce retained native memory with stable file segments, explicit ownership of
  writable pages/descriptors and memtable record/metadata storage, and reclamation
  of old mapped file versions. Preserve snapshot/cursor and checkpoint I/O views.
- Stream compaction in both cache modes, reuse checkpoint vectors and scratch
  buffers, and allocate reclaimable WAL buffers lazily. Verified clean reads
  avoid descriptor allocation and read-side CLOCK reference writes. Warm the new
  compacted mapping after closing the old file in both cache modes.
- With a cache target, the default memtable budget remains `CacheBytes/2`,
  but now accounts for native metadata and replaced records. Writers wait at
  two active-table budgets while another table applies (previously four budgets).
  Dirty-page checkpoint pressure begins at `CacheBytes/4` (previously `3/4`).
  These targets do not cap total process memory or versions retained by snapshots.
- Pin the WAL replay range through compaction and serialize concurrent copies
  and log rotations, preventing background checkpoints from deleting required
  generations.
- Expose checkpoint-pinned `InFlightPages` in statistics without double-counting
  dirty pages; cache bounds include pages that I/O cannot evict.
- Batch contiguous checkpoint pages with immutable staging copies while retaining
  the data-sync/meta-sync durability order. Reuse tree descent between scan leaves.
- Bound ordinary WAL backlog at 4 MiB, release oversized burst buffers at replay
  barriers, and report allocated `WALBufferBytes`. Large atomic batches remain supported.
- Retire consumed file translations during private compaction copies, preserving
  concurrent views and warming the replacement only after closing the old file.
- Drain startup cleanup synchronously before checkpoint workers start, avoiding
  a recovery deadlock after interrupted index builds with small memtable budgets.
  Writers also recheck closure/failure after waiting for buffer space.
- Flush a complete WAL prefix before a new record crosses the ordinary buffer
  limit, avoiding a double-sized allocation immediately before flushing.
- Keep snapshot-retired page IDs pending until metadata that excludes them is
  durable, including when retirement happens during checkpoint I/O. This prevents
  recovered tree corruption after an interrupted checkpoint.
- The on-disk format remains 10 and existing databases need no migration.

## [0.2.0-alpha.6] - 2026-10-14

- Add `CountDocuments(ctx, filter, CountOptions)` for embedded/default and
  snapshot collections, plus the HTTP Go driver and read-only snapshot leases.
- Count scalar compound equality prefixes directly from bounded index leaf
  slots when every predicate is covered. Merge buffered snapshot index entries
  when necessary; perform zero document fetches on either covering path.
- Preserve exact integer equality, mixed index directions, null/missing and
  sparse semantics. Residual predicates, logical OR and multikey indexes use
  snapshot-consistent document evaluation without sorting/materializing results.
- Report `Plan.IndexOnly`, `DocumentFetches` and `IndexEntriesExamined`, with
  cancellation, skip/limit, bounded retained query memory and explicit errors.
- Add correctness, checkpoint/recovery, concurrent snapshot/catalog and remote
  API regressions, plus paired alpha.5 and SQLite/Pebble count benchmarks.
- On-disk format remains 10; no new migration. Existing Query/aggregation APIs
  retain their behavior. Adapters must adopt CountDocuments for covering counts.

## [0.2.0-alpha.5] - 2026-10-14

- Add opt-in persistent native change feeds with filtered/projected Go and HTTP
  cursors, atomic snapshot handoff, computed-collection events, bounded replay,
  database-bound resume tokens and explicit history/resource errors.
- Reuse original WAL values and add final commit markers instead of a second
  value journal. Share hot metadata, match plans, replay data and small projections;
  keep filtering outside the writer lock and avoid per-subscriber queues.
- Add separate HTTP watch capacity/connections, heartbeat/write-idle handling,
  leased read-only snapshots, close/cancellation cleanup and daemon capture flags.
- Format 10 adds WAL-only commit markers and a persistent capture configuration.
  Formats 3–9 upgrade both metadata slots synchronously. Keep an original-binary
  backup; older binaries refuse the upgrade. Export/import remains version 2.
- Charge compiled match/projection plans against global and per-cursor budgets;
  own option strings to avoid retaining oversized caller buffers. Cancel live
  watches before daemon shutdown waits for HTTP handlers.
- Fence synchronous WAL rotation against in-flight fsyncs and unsynced appends.
  Publish collection creation atomically with its catalog; defer index-build
  touch bookkeeping until staging limits have passed.

## [0.2.0-alpha.4] - 2026-10-13

- Fix integer SUM/AVG overflow in serial aggregation and parallel shard merges.
  Accumulate integer inputs exactly in signed 128 bits. SUM returns int64 when
  the final total fits, otherwise float64; AVG returns float64. Cancellation
  after intermediate overflow recovers exact integer totals. Float results
  use IEEE-754 precision; maintained integer sums still reject final overflow.
- Add `$countNonNull: "$field.path"` group accumulators, including maintained
  computed collections. Count present non-null values of every type, including
  zero/false/empty strings and containers; exclude missing and null. Existing
  row-count operations retain their behavior.
- Format 9 protects persisted non-null-count definitions from older writers.
  Opening formats 3–8 upgrades both metadata slots durably before writes.
  Back up with the original binary before this one-way upgrade. Page, document,
  WAL and export layouts are unchanged; logical export/import stays version 2.

- Speed up sequential cursors by reusing private value buffers and advancing
  known cached slots without repeated key comparisons. Arbitrary saved-position
  seeks retain cache validation; crossing a collection boundary clears the cache.
- Copy inline scan keys and values together, use smaller cell references, and
  bypass the memtable merge when empty. Preserve narrow-range bound checks,
  snapshot consistency and reentrant callbacks.
- Add mixed-size value isolation, exhausted-cursor resume regressions, and
  correctness-checked scan/range/sustained-reader benchmarks. Clarify that raw
  scan callback buffers are read-only and borrowed.

## [0.2.0-alpha.3] - 2026-10-13

- Fix reused cursor `SeekAfter`/`SeekBefore` after traversal to unrelated saved
  positions. Random exclusive seeks discard the cached leaf position; sequential
  reads retain the cache. Snapshot and binary-key ordering remain unchanged.
- Raise complete encoded index entries from 4,032 to 8,160 bytes, independent
  of the 4,032-byte user-key limit. A 1,500-byte primary key and a 3,000-byte
  indexed ASCII string now fit, including encoding overhead. NUL escaping and
  every compound component count toward the encoded limit.
- Retain existing leaf/internal cuts when the incoming cell fits; otherwise
  include its size when choosing the split, so mixed small/large cells cannot
  overflow an insertion half. Ordinary key validation keeps its fast path. Empty wide index values
  stay inline; tree traversal supports 32 levels for reduced fanout.
- Format 8 permanently upgrades formats 3–7 before writes. Older binaries refuse
  the new format because their split algorithms assume smaller cells. Back up
  using the original binary first. Logical export/import remains version 2.

## [0.2.0-alpha.2] - 2026-10-13

- Preserve integers in typed map decoding; fix exact JSON integer filtering and
  mixed int64/float64 comparisons, including
  large integers in scans, indexes and hash/index lookup joins.
- Extended document lengths/offsets: up to 16 MiB, 4,096 fields, large strings,
  nested values and arrays. Small documents retain their compact encoding.
  Valid alpha.1 documents with wrapped offset directories can be read safely;
  corrupted payloads are refused rather than guessed.
- Native scalar compound indexes with mixed ASC/DESC components; exact numeric
  ordering and escaped primary-key tie ordering. Snapshot index cursors support
  bounds, prefix ranges, reverse traversal and seek positions.
- Logical $and/$or and case-sensitive literal $prefix filters, QueryOptions.Sort,
  index hints, ExplainQuery and examined/returned/working-memory statistics.
- Optional NullsDistinct and Sparse unique constraints; atomic updates/swaps,
  schema export/import, snapshots and crash replay preserve the options.
- 4,032-byte user/index keys; expanded document support in computed fields and
  projected collections. The server/driver frame limit is now 32 MiB.
- Configurable query working-memory budgets; sorted pages use a bounded heap
  when an ordered index cannot serve them. Parallel aggregations share a budget.
- Format 7 permanently upgrades formats 3–6 before new writes; existing legacy
  index encodings remain readable and are rechecked exactly. Rebuild an old
  index to use ordered cursors. Logical export version 2; Import accepts 1/2.

## [0.2.0-alpha.1] - 2026-10-13

- Module/repository renamed to `github.com/apteva/db`; Go package `db`.
- Breaking API: Insert creates only; Upsert retains previous replacement behavior.
  Replace requires an existing key. Batch conditions are atomic and ordered.
- Consistent scan/cursor/query catalogs and rows; cursors now require Close.
- Atomic collection/index deletion with cancellation and staging limits.
- Serializable optimistic read/write transactions with read-your-writes,
  computed outputs and explicit ErrConflict (no implicit callback retries).
- Native unique indexes, including atomic swaps and multikey constraints.
- Format 6 refuses old writers that cannot enforce unique constraints; automatic
  upgrades from 3–5 and a real format-5 compatibility fixture.
- Typed document helpers, streaming filters and safe document updates.
- Verified restore, lossless JSON-lines export/import, durable CLI defaults.
- Stable directory locking protects recovery and compaction rename windows.
- Concurrent JSON encoding fixed; short writes detected; WAL I/O failures sticky.
- HTTP v1 server with authentication/TLS, deadlines, bounded requests/transactions;
  pooled Go client with streaming results and explicit uncertain-write errors.
- Linux/macOS CI, race/fuzz/soak gates and binary releases with SHA-256 checksums.


- Explicit lifetime accounting for writable page arenas, checkpoint captures
  and snapshot overlays, with unused storage returned to the OS. Large WAL
  mappings release superseded buffers immediately; record encoding grows once.
  Leaf balancing reuses scratch storage. Backup and compaction stream completed
  pages in bounded batches and avoid faulting clean leaves during maintenance.
  Bounded-cache mode retains its existing Go allocation and compaction policy
  after parallel-read regression checks. These buffer changes preserve the storage layout.
- CPU, process RSS and macOS compressed-memory footprint benchmarks with fresh
  per-engine processes, retained repetitions and paired regression comparisons.

- Native stored computed fields per collection, deterministic dependencies,
  arithmetic/string/null operators, validation before WAL append, ordinary
  indexes on outputs, snapshot introspection and persisted definitions.
- Computed-field benchmarks against application materialization/read-time
  derivation, Pebble and both SQLite bindings, retaining every repetition.
- Computed-field catalogs were introduced in format 4; the current format is 6.
- Computed-field definition installation/change/removal require an empty collection.

- Native stored computed collections: atomic backfill, filtered/projected rows
  and incremental scalar groups with exact integer sums/counts. Source writes,
  dependent results, hidden membership counters and indexes commit/recover
  together. Direct writes and chains are rejected; source drop requires removing
  dependents first. Creation/drop block reads and writes and stage at most 256 MiB.
- On-disk format 5 adds computed-collection catalogs and counters. Formats 3/4
  upgrade both metadata slots synchronously before Open returns; older engines
  refuse format 5. Feature benchmarks compare on-demand queries, Pebble
  application materialization and SQLite triggers, retaining every trial.

## [0.1.0] - 2026-09-08

Initial development version. On-disk format 3 (see FORMAT.md). Tests, the race
detector and fuzz targets run in CI on Linux and macOS; Windows is not
supported.

### Added

- Key-value store: `Insert`, `Delete`, `Find`, `Scan`, cursors, batches
  atomic across collections (`Batch.With`), values up to 1 GiB.
- Collections as key ranges of one tree, with a catalog; `Collection`
  carries the whole API, the `DB` methods address the default collection.
- Documents: a binary encoding with `InsertJSON`, `FindJSON`, `DocBuilder`,
  `EncodeJSON`, `DecodeDocument`.
- Aggregation: MongoDB-style pipelines with `$match`, `$group`, `$sort`,
  `$skip`, `$limit`, `$project`, `$count`, `$unwind` and the equality form
  of `$lookup`; streaming execution, parallel execution over key-range
  shards by default (`Options.Parallelism`), memory limits.
- Secondary indexes per collection, multikey, dotted paths, built in bulk
  from a snapshot while writes continue; a planner that uses them for a
  leading `$match` and for join probes, with one join reordering.
- Snapshots with consistent reads, scans, cursors and aggregation.
- Write-ahead log with group commit and three sync modes; copy-on-write
  checkpoints running in the background; non-blocking `Compact` and
  `Backup`; a page cache limit with a memtable; page checksums; `Verify`
  and `VerifyIndexes`.
- `aptevadbctl`: stats, verify, collections, get, dump, agg, compact,
  backup, format, version.
- Documents: FORMAT.md, DURABILITY.md.
- Tests: a fault-injection crash test over random writes and recovery,
  crash coverage of index builds, drops, compaction and the log trim, fuzz
  targets, a cursor stress test (`CURSOR_STRESS=<duration>`), and a soak
  test (`SOAK=<duration>`) with crash-recovery phases;
  CI runs vet, tests, race and fuzz on Linux and macOS, and the soak
  nightly.

### API decisions taken for this release

These renames happened before the first tag and are recorded so code
written against the pre-release `engine` package can be updated:

- Package `engine` became the root package `aptevadb`; module path
  `github.com/apteva/aptevadb`.
- Long operations take a `context.Context`: `Aggregate`,
  `AggregateParallel`, `Run`, `RunParallel`, `Pipeline.Run`,
  `Pipeline.RunSharded`, `CreateIndex`, `DropIndex`, `DropCollection`,
  `Compact`, `Backup`, `Verify`, `VerifyIndexes`. A nil context is treated
  as `context.Background()`.
- `Aggregate` runs on `Options.Parallelism` goroutines (default
  `runtime.GOMAXPROCS(0)`) instead of always on every core;
  `AggregateParallel(ctx, p, 1)` runs on the calling goroutine.
- `Options.FlushInterval` became `LogFlushInterval`;
  `Options.CheckpointBytes` became `CheckpointLogBytes`; `DB.Flush` became
  `FlushLog`.
- `ErrTooManyColls` became `ErrTooManyCollections`; writes through a
  snapshot's collection return `ErrReadOnly`.
- Error strings are prefixed `aptevadb:`; every error the package returns
  for a caller's condition is a sentinel usable with `errors.Is`.
- `Version` and `FormatVersion` constants; `FormatVersionOf(dir)`.

### Fixed before release

- `Open` restores a database whose compaction swap was interrupted between
  its two renames; previously the original file was deleted and an empty
  database created.
- A cursor could record a stale leaf against a fresh tree generation when a
  writer changed the tree between its descent and the moment it cached the
  position, and then read a page that had moved or been truncated away (a
  bus error on the mapping). Found by the soak test; the generation is now
  taken inside the locked descent. A cursor stress test covers the path.

### Known limitations

- One writer at a time; readers never block.
- No transactions across statements, no unique or composite indexes, no
  index-only scans, no range deletes.
- `$lookup` supports only the `localField`/`foreignField` form; joins run
  in the order written except for the one reordering documented in the
  README.
- Aggregation and joins run in memory within `Limits`.
