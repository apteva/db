# Operations

Supported: local Linux/macOS filesystems with reliable locking, mmap and fsync,
on amd64/arm64. One process exclusively opens a database directory. Do not share
an open directory between servers, containers or a network filesystem.

The server and CLI select full synchronous durability. Embedded Options{} is
asynchronous. Acknowledged full-sync writes survive power loss only when the OS,
filesystem and device honor the flush primitive; see DURABILITY.md.

## Backup, restore and logical export

An embedded application's `database.Backup(ctx, destination)` captures a consistent
backup while writers continue. The destination must be absent or empty. Never
copy individual live data/WAL files. Restore/import targets must not exist.

```sh
aptevadbctl backup ./data ./backup-2026-10-13
aptevadbctl verify ./backup-2026-10-13
aptevadbctl restore ./backup-2026-10-13 ./restored
aptevadbctl verify ./restored
aptevadbctl export ./data ./database.jsonl
aptevadbctl import ./imported ./database.jsonl
```

CLI maintenance requires exclusive access: stop the standalone server first.
For online backups, embed the engine/server and call the DB Backup API from the
owning process. Remote HTTP clients do not choose server filesystem paths.

Restore verifies data and indexes before and after copying. Export is a versioned
lossless JSON-lines logical format: binary keys/values use base64; collection
schemas, computed definitions and index constraints are included. Import rebuilds
computed outputs from source documents and recreates indexes. It rejects duplicate
keys and truncated streams. Export/import currently treat input as trusted local
backup data; there is no untrusted-upload API.

Interrupted restore/import leaves `.restore-incomplete`; Open refuses that
location. Keep the original backup, remove only that incomplete destination, and
restore/import into a fresh directory. Existing directories are never replaced.

## Recovery and upgrades

On startup the engine validates metadata, replays complete checksummed WAL groups,
ignores/truncates a torn tail and completes interrupted compaction swaps. It does
not repair arbitrary corruption. Keep verified backups on separate storage and
regularly restore one into a disposable directory to check your recovery process.

`aptevadbctl format ./data` reads metadata without opening or upgrading.
Open upgrades formats 3–9 to 10 synchronously and permanently. Back up with the
original binary before upgrading; retain that binary for the original backup.
Older binaries refuse the upgraded directory. Format 9 adds non-null counts in maintained definitions; format 8 permits wider index cells; format 7 introduced exact/compound index encodings and extended documents;
see FORMAT.md and the real historical fixture under testdata/format5.

Short writes, disk-full errors and failed flushes are fatal to the current handle.
Health() reports the failure; later writes are refused. Close, resolve the I/O
problem, reopen and verify. A write that returned an I/O error may have reached
storage: check application keys before retrying. The remote driver explicitly
reports ErrCommitUnknown for transport failures and never retries writes itself.

## Limits and monitoring

| Resource | Alpha contract |
|---|---|
| User key | 1–4,032 bytes |
| Binary document | up to 16 MiB, 4,096 fields, 16 nesting levels; strings/nested values use extended lengths when needed |
| Opaque value | up to 1 GiB |
| Index entry | up to 8,160 encoded bytes (`MaxIndexKeySize`), including the index prefix, all components, escaped primary key, terminators and length suffix |
| Index name | 1–4,026 UTF-8 bytes |
| Named collections | IDs 1–65,535; IDs are not reused while a DB remains open |
| Computed fields | define on empty collections; scalar outputs; checked arithmetic |
| Computed collections | no chaining; scalar groups with integer sums/counts; 256 MiB staging |
| Transaction write/maintenance staging | 256 MiB, including computed/index expansion |
| Aggregation | default 1,048,576 groups/rows and 64 MiB retained working-memory budget; configurable per pipeline |
| Default server | 32 MiB request/response frames; 32 concurrent requests; 64 transactions |
| Default server transaction | expires after 60 seconds, releasing its snapshot |
| Default server aggregation | at most 10,000 groups/rows and 64 MiB query working memory |

CacheBytes is a logical page-cache target, **not** a total resident-memory limit.
Memtables, WAL buffers, metadata, retained snapshots, query results, mmap pages and
Go allocation overhead add memory. A limited cache does not currently guarantee
lower total memory than Pebble. Measure process RSS and OS footprint, not just Go
heap statistics. Avoid claiming a universal speed/memory advantage from one run.

Stats() and `aptevadbctl stats` expose pages, loads/writes/evictions, memtables,
snapshots, quarantined pages, stalls, indexes and lock durations. Health() exposes
fatal write state. `verify` checks tree structure plus secondary indexes. Compact
reclaims space, but is refused while snapshots or an index build are open. Plan
free space for a second data file during compaction/backup/restore.

Directory ownership is protected by a persistent `LOCK` file before recovery or
compaction cleanup starts, and remains held across data-file swaps. Do not remove
that file while the database is open. The data-file lock is retained as well.

QueryOptions.MaxMemoryBytes and Limits.MaxBytes account for retained execution data,
including row maps, group slabs, index candidates, join tables and sort buffers.
They exclude page caches, pinned snapshot versions, the current input document,
request/response framing and caller-owned values, and are not hard process-RSS
limits. Ordered compound-index pages stream; fallback sorts with Limit retain
only Skip+Limit candidates. Raise a pipeline budget explicitly or reduce workers
for large-cardinality parallel groups. Close snapshots and cursors promptly.

Logical exports now write version 2, preserving compound directions and nullable
constraints; Import still reads version 1. Older binaries reject version-2 exports
instead of silently losing new index contracts.


Native capture settings persist once enabled; nil options preserve them. Use
CHANGE_FEEDS.md for history windows, snapshot leases and limits. Logical expiry
can precede segment deletion; the recovery tail is retained. Backups/restores
start a fresh change-stream identity and do not carry its history. Format-10
upgrades are permanent and older binaries cannot open the directory.

## Resident-memory controls (alpha.8)

```go
opts := db.DurableOptions()
opts.CacheBytes = 64 << 20
opts.MappedCacheBytes = 32 << 20
opts.ManagedMemoryBytes = 256 << 20
handle, err := db.Open("./data", opts)
// ...
stats, err := handle.MemoryStats(ctx)
reclaimedEstimate, err := handle.TrimMemory(ctx, 32 << 20)
```

Both new targets default to zero (disabled). MappedCacheBytes enables recency
tracking and a sweep once per second. TrimMemory establishes tracking on its
first call if disabled; subsequent sweeps reclaim cold clean file translations
in 1 MiB chunks while preserving mapping addresses and contents. Recent chunks
survive a sweep, so a hot working set may exceed the soft target. Refaulting a
reclaimed page costs work on a later read. TrimMemory's return and
ReclaimedMappedBytes estimate resident file-backed bytes remapped, rather than
measure a process-RSS delta. Reclamation does not force garbage collection.

MemoryStats separates process RSS from allocated private page buffers,
descriptors, memtable arenas and WAL stores, including retired stores leased by
snapshots/cursors and temporary native compaction/backup buffers. Active,
immutable and retained memtables are separate. SnapshotPinnedBytes and
CheckpointPinnedBytes are overlapping subsets of those categories; PinnedBytes
counts their union and other leased memtables once. They are not additional
allocated memory. QueryWorkingBytes covers registered aggregation budgets.
MetadataEstimateBytes estimates Go bookkeeping separately.

MappedPageCacheBytes comes from mincore and describes OS file page-cache
residency; it is not process mapped RSS. MappedResidentEstimateBytes excludes
explicitly reclaimed chunks until they are read again, and remains an estimate.
MappedVirtualBytes is address space. None of those values should be summed with
ProcessResidentBytes. Diagnostics perform residency syscalls on explicit request,
not on ordinary reads.

ManagedMemoryBytes is a soft pre-write admission target for owned native buffers
and registered aggregation reservations. It is not a hard process RSS cap;
metadata, snapshots' bookkeeping, Go/caller allocations and mapped file pages
are separate, and one atomic operation, checkpoint or in-flight copy can overshoot.
Above target, admission attempts checkpoint/reclamation before logging a write.
An ordinary write returns ErrMemoryPressure if retained storage still prevents
progress, avoiding a deadlock when its caller owns the retaining snapshot.
Batch.CommitContext with a cancellable context may wait for leases to close;
cancellation/pressure rejection occurs before entering the WAL. Close snapshots
and cursors promptly and configure a target large enough for ordinary engine
buffers plus workload bursts. Aggregations can return ErrMemoryPressure if the
shared managed target is exhausted. Do not treat either option as an OS memory
limit or a promise to outperform another engine on every workload.
