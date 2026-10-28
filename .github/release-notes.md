AptevaDB alpha.9 makes aggregation faster, in memory and when it spills to disk, adds a typed group visitor and fixes a log-rotation durability gap and a checkpoint hang.

- **Faster bounded (spill) aggregation.** Partition fanout adapts with budgeted I/O buffers, temporary records use a compact codec, and reconstruction uses arenas. Temporary spill records changed; no persistent format changed.
- **`AggregateGroups`/`RunGroups`** stream completed groups as a borrowed, typed `GroupView` without building result maps. An integer SUM that overflowed int64 is reported, never rounded, and `IntegerSum` returns its exact signed 128-bit value.
- **Faster in-memory aggregation.**
  - Filter fields are extracted first, and the rest only for documents that pass.
  - Each document's directory is decoded once, and field names are checked before decoding values.
  - Float and string-equality predicates have fast paths.
  - Sharded `$group` above 8,192 groups merges by key hash on all cores; row order, `$first`/`$last` and limits match the serial run.
- **Faster index builds.** A checkpoint with nothing buffered is skipped, empty checkpoints skip their page sync, page buffers are reused, and entries sort without reflection.
- **Durability fix.** Creating a log generation now fsyncs the database directory. Before, a power loss right after a log rotation could lose the new file's directory entry and, with `Sync`, records already acknowledged in it.
- **Hang fix.** A checkpoint whose free list sat just above a multiple of 4,092 ids panicked while writing the list, a bug present since 0.1.0. Since alpha.7 that panic deadlocked the database instead of crashing it, which is how the 60-minute macOS soak timed out on alpha.8. Long-running databases with many freed pages on alpha.7 or alpha.8 should upgrade.

**Results.** The [benchmark report](https://github.com/apteva/db/blob/v0.2.0-alpha.9/bench/results/alpha9-performance/REPORT.md) compares this release with alpha.8 in 48 interleaved fresh processes. All raw trials are kept.

- **Spill, one million groups, 12 MiB budget:** 56% faster for COUNT and 63% for numeric aggregates, with 82–85% fewer spill bytes and one merge pass instead of 17. The typed visitor takes 0.52 s and 0.86 s.
- **In-memory aggregation:** 11–54% faster on one core. The parallel 100,000-group query is 52% faster.
- **Key-value, batched and durable writes:** unchanged within noise.
- **Trade-off:** the numeric spill workload's peak RSS rose by 5 MiB.

The workstation was busy (load 9–36), so these results do not establish zero regressions everywhere. The benchmark harness's batched settle step now compacts, as its Pebble counterpart did, so disk sizes compare like for like. See [usage](https://github.com/apteva/db/blob/v0.2.0-alpha.9/USAGE.md#typed-group-visitor-alpha9) for the visitor contract.

**Format remains 10; alpha.5–alpha.8 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
