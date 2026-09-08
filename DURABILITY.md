# Durability contract

What a caller can rely on after a crash, by kind of crash and by setting.
"Acknowledged" means the call (Insert, Delete, Batch.Commit) returned nil.

## Model

Every write goes to the log before it goes anywhere else: appended to an
in-memory log buffer under the writer lock, then applied to the memtable
(with a cache limit) or to the tree's in-memory pages. Log records reach the
operating system when the buffer holds 1 MiB, every `LogFlushInterval`
(10 ms by default), and on `FlushLog`, `Checkpoint` and `Close`. Nothing in
the data file is ever overwritten in place: a checkpoint writes modified
pages to fresh page ids, then flips one of two meta pages. A crash therefore
never tears the data file; recovery reads the current meta page and replays
the log from the position it names.

## What survives a crash

| Crash | `Sync` off | `Sync` on, `SyncFull` | `Sync` on, `SyncBarrier` | `Sync` on, `SyncOS` |
|---|---|---|---|---|
| Process dies, OS keeps running | acknowledged writes older than about 10 ms (or 1 MiB of log) survive; newer ones may be lost | everything acknowledged survives | everything acknowledged survives | everything acknowledged survives |
| Kernel panic or power loss | whatever the OS and drive had written; typically the last seconds are lost | everything acknowledged survives (`F_FULLFSYNC` on macOS, `fsync` on Linux) | writes stay ordered; acknowledged writes still in the drive's cache can be lost | as SQLite's default: `fsync` on macOS does not flush the drive cache, so acknowledged writes in it can be lost |

With `Sync` on, a call returns only after an fsync that covers its record.
Concurrent writers share fsyncs (group commit): one fsync in flight, the
others wait for it and the next covers them all. What is lost is only what
was not yet acknowledged, and it is lost as whole records: replay stops at
the first record with a bad checksum or a missing tail, and the log is
truncated there.

## Atomicity

- One `Insert` or `Delete` is atomic: it is in the file entirely or not at
  all.
- A `Batch` is all-or-nothing, across collections and including the index
  entries it maintains: its records are chained in the log and replay
  applies a chain only when its last record is present.
- Readers see a consistent state at every point: a `Find` never sees half a
  batch, a `Snapshot` sees the database as of one instant, and a `Scan`
  sees each leaf atomically (it may see batches committed after it started
  in later leaves).
- Index entries are maintained in the same batch as the document, so an
  index never disagrees with the documents after recovery; `VerifyIndexes`
  checks that.

## Memtable and checkpoints

With a cache limit, writes are buffered in a memtable before reaching the
tree. The memtable holds nothing the log does not: the meta page's log
position never moves past the first record still in the active memtable,
so replay rebuilds it. A checkpoint applies the memtable, writes the pages
it changed, fsyncs the data file, flips the meta page and fsyncs again;
only then is older log space released. A crash at any point in that
sequence leaves either the old meta page or the new one valid, and the log
covers the difference.

The directory is fsynced after every file rename, log rotation and log
generation deletion, so the set of files recovery finds is itself durable.

## Operations with their own rules

- **`Close`** runs a full checkpoint, drops the dead log generation and
  releases the lock. A database closed cleanly opens without replay. A
  process that exits without `Close` replays at the next open; nothing
  acknowledged under the table above is lost.
- **`Checkpoint`** applies the memtable and makes every write so far
  durable in the data file, whatever the sync setting. It is the way to get
  durability on demand with `Sync` off.
- **`FlushLog`** hands log records to the operating system without an
  fsync: it protects against a process crash, not against power loss.
- **`CreateIndex`** inserts the existing documents' entries without log
  records, then runs a checkpoint before the catalog entry is logged, so an
  index is either complete and durable or absent. A crash before the
  catalog entry leaves entries with no owner, which the next open removes.
  Writes made during the build are logged and maintained normally.
  `Compact` is refused while a build runs.
- **`DropIndex` and `DropCollection`** delete in bounded batches. A crash
  midway leaves a partial deletion: the index or collection is still
  listed, with whatever survived, and dropping it again finishes the job.
  `DropCollection` removes the indexes first, then the keys, then the
  catalog entry.
- **`Compact`** copies a snapshot into `compact.tmp`, replays the log delta
  into the copy, checkpoints the copy, then swaps files with two renames
  and an fsync of the directory. Until the swap, the original is
  untouched; the log is shared, so a crash at any point leaves a database
  that opens: `Open` discards `compact.tmp` and, if the crash fell between
  the two renames, renames the original back. `Compact` is refused while
  snapshots are open or an index is being built.
- **`Backup`** writes a compact, self-contained copy of the database as of
  one instant, taken from a snapshot while writers continue. The copy opens
  on its own; it holds every collection and index. A cancelled or failed
  backup removes its directory.
- **`Snapshot`** pins the tree and the buffered records as of its creation
  until `Close`; page ids a snapshot may reference are not reused
  meanwhile. Snapshots are in-memory only and do not survive a restart.

## Corruption

Every page carries a crc32c checked the first time it is read after open;
a mismatch is `ErrCorrupt` and the read fails rather than returning bad
data. `Verify` walks the whole tree and the free list. Log records are
checked on replay. The engine does not repair corruption; restore from a
backup.

## What the engine does not promise

- Durability across a drive that lies about flushes; the `SyncFull` row
  assumes the drive honours a full flush.
- Anything about files copied while the database is open by another
  process: use `Backup`.
- Protection against two processes opening the same directory on a file
  system whose `flock` does not work (some network file systems).
- Windows, where the file mapping, locking and fsync code has not been
  built.
