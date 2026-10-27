AptevaDB alpha.7 improves memory ownership, checkpoint I/O and scanning, and fixes WAL buffering and interrupted-checkpoint recovery.

- Stable file mappings and explicitly owned page, descriptor and memtable buffers release retired storage without waiting for Go GC. Snapshots, cursors and checkpoint captures retain the buffers they need.
- Stream compaction, batch contiguous checkpoint writes using immutable staging copies, and reuse tree descent between scan leaves. Keep the existing data-sync/metadata-sync durability order.
- Bound ordinary WAL backlog at 4 MiB, flush complete records before buffer growth, and release oversized burst buffers at replay barriers. Large records and atomic batches remain supported.
- Fix recovery deadlock during startup cleanup with bounded memtables. Keep snapshot-retired page IDs unavailable for reuse until both readers finish and the replacement metadata is durable, including retirements during checkpoint I/O.
- Add `Stats.InFlightPages` and `Stats.WALBufferBytes`. `CacheBytes` remains a logical page-cache target, not a total-process memory limit.

The [benchmark report](https://github.com/apteva/db/blob/v0.2.0-alpha.7/bench/results/memory-followup/native/REPORT.md) retains every sample, source hashes and limitations. In four tested dataset/order combinations, peak RSS was 58–63% lower than original alpha.6. Compared with the preceding memory-optimization candidate, bulk-load elapsed time improved 9–25% and CPU use fell 13–35%. Physical footprint was 10–24% below Pebble reference runs; RSS remained 2–11% above them. The largest random load remained 3.3% slower than original alpha.6, and some other timing regressions remain. Background activity was present; native and competitor cohorts are reported separately. The complete-key routing-map experiment was rejected and is excluded from this release.

**Format remains 10; alpha.5/alpha.6 directories need no new migration.** Opening formats 3–9 still permanently upgrades them to format 10; back up with the original binary first. This remains a public prerelease alpha.

```sh
go get github.com/apteva/db@v0.2.0-alpha.7
go install github.com/apteva/db/cmd/aptevadbctl@v0.2.0-alpha.7
go install github.com/apteva/db/cmd/aptevadb-server@v0.2.0-alpha.7
```

Linux/macOS amd64/arm64 CLI/server archives include the usage and operations guides. Verify downloads against `SHA256SUMS`.

Post-publication [verification evidence](https://github.com/apteva/db/tree/main/bench/results/memory-followup/release-alpha7) confirms all four archive checksums, public Go proxy/sumdb and CLI installation, embedded and HTTP-driver checks, bounded large-value/snapshot/compaction recovery, and alpha.6 directory compatibility. Source and release CI passed. A duplicate tag CI run exposed pre-existing test-harness issues (an unlocked diagnostic counter read and a 100-ms RPC fixture timeout); test-only fixes and repeated race checks are documented with the evidence. The published engine and tag are unchanged.
