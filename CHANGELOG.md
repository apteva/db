# Changelog

All notable changes to this module. The format follows Keep a Changelog;
versions follow semantic versioning. Before 1.0, a minor version may change
the API or the on-disk format; both are listed here when it happens, with
the migration.

## [0.1.0] - 2026-09-08

First tagged release. On-disk format 3 (see FORMAT.md). Tested on macOS on
arm64; the Linux sync path exists but has not run; Windows is not
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

### Known limitations

- One writer at a time; readers never block.
- No transactions across statements, no unique or composite indexes, no
  index-only scans, no range deletes.
- `$lookup` supports only the `localField`/`foreignField` form; joins run
  in the order written except for the one reordering documented in the
  README.
- Aggregation and joins run in memory within `Limits`.
