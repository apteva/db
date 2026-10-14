AptevaDB alpha.5 adds native real-time change feeds for embedded Go applications and the HTTP Go driver.

- Opt-in persistent capture with collection/operation/key-prefix filters, exact document predicates, projected values, and database-bound resume tokens. Filters reuse Query's logical/range/literal-prefix language.
- Atomic `SnapshotAndWatch` hands off initial state to committed changes without a gap. Source and maintained computed-collection events share one commit; subscribe to a computed collection for live membership updates.
- Capture reuses original WAL values and appends a final commit marker. Shared hot metadata, matching plans, replay payloads and small projections reduce subscriber duplication. History, compiled plans, reader memory and watcher counts have explicit limits and expiry/errors.
- HTTP watches use separate capacity and connections, coalesced progress heartbeats, bounded frames, read-only snapshot leases and write-idle deadlines. Client/server closure cancels streams; the daemon drains idle watches promptly on shutdown.
- Fix synchronous WAL rotation fencing, atomic collection catalog publication and rejected-write index bookkeeping. Add torn-commit/replay, large-document, computed-event, filtering, memory-accounting and concurrency regressions.

Capture is disabled on a fresh database unless enabled. Stored capture settings survive reopening, including CLI opens; disabling/re-enabling creates a new history identity. Delivery is at least once within retained history. Put includes insertion/replacement; filtered deletes have no after-image. Reset/Drop invalidations pass selected collection scope. See [CHANGE_FEEDS.md](https://github.com/apteva/db/blob/v0.2.0-alpha.5/CHANGE_FEEDS.md) for these contracts, limits and examples.

**Format 10 permanently upgrades formats 3–9. Back up with the original binary first; older binaries refuse the upgraded directory.** Format 10 adds persistent capture settings and WAL-only final commit markers. Page/document layouts and logical export/import version 2 remain unchanged. Backups/restores start a new feed identity and do not preserve old replay tokens.

The [validation report](https://github.com/apteva/db/blob/v0.2.0-alpha.5/bench/results/change-feeds/VALIDATION.md) retains 291 benchmark processes against alpha.4, Pebble and C/pure-Go SQLite. On an M1 Pro with async 1,000-row commits, 100 subscriptions sharing a 1% filter measured 944K source rows/s; projected delivery measured 904K/s with about 20–21 MiB process peak RSS. Capture adds measurable write cost and batching matters. Background CPU activity affected some read timings; this is not a claim of universal improvement or HTTP throughput.

```sh
go get github.com/apteva/db@v0.2.0-alpha.5
go install github.com/apteva/db/cmd/aptevadbctl@v0.2.0-alpha.5
go install github.com/apteva/db/cmd/aptevadb-server@v0.2.0-alpha.5
```

Linux/macOS amd64/arm64 CLI/server archives include the change-feed guide; verify them using SHA256SUMS. This remains a public prerelease alpha. See README, USAGE, FORMAT and OPERATIONS for other contracts and limitations.
