AptevaDB's first public alpha under `github.com/apteva/db`.

- Embedded Go document/key-value engine and optional authenticated HTTP v1 server.
- Go remote driver in `github.com/apteva/db/client` with pooled connections,
  streaming filters, explicit transaction conflicts and no automatic write retries.
- Create-only Insert, explicit Upsert/Replace, consistent scans/cursors/catalogs,
  and atomic collection/index drops across cancellation and crash recovery.
- Optimistic serializable transactions, native unique indexes, stored computed
  fields/collections, aggregation, verified backup/restore and logical export/import.
- Durable server/CLI defaults; concurrent JSON encoding and sticky WAL I/O failures fixed.

Install the Go module at this tag, or download the CLI/server archive for Linux or
macOS on amd64/arm64. Check each archive against SHA256SUMS before use. See README,
USAGE, OPERATIONS, SERVER and DURABILITY in the archive/repository.

This is an alpha. Format 6 upgrades formats 3–5 permanently: back up with the
original binary before upgrading. Old Insert callers must migrate to Upsert for
replacement; cursors must now be closed. Transactions use database-wide conflict
validation. Drops and staged atomic work have a 256 MiB limit; CacheBytes is not a
total-process-memory cap. The server uses one full-access token and has no
replication or per-user permissions. Benchmarks do not establish a universal
speed or memory advantage over other engines.

Release verification passed unit/vet/build, race and fuzz checks on Linux/macOS,
plus the release stress/soak runs. All four downloaded archives matched SHA256SUMS;
the native commands, tagged-module embedded/remote CRUD, Go command installations
and daemon shutdown were verified.

[Completed benchmark report](https://github.com/apteva/db/blob/main/bench/results/public-alpha/VALIDATION.md):
30 resource trials and 24 feature trials, all successful. Raw observations and
settings are in the repository and validation evidence attachment. The library
uses the tagged source; subsequent fixture corrections affect benchmarks only.
The bounded-cache memory result remains worse than Pebble. See the
[current README](https://github.com/apteva/db/blob/main/README.md) and
[usage contracts](https://github.com/apteva/db/blob/main/USAGE.md) for corrected
examples and design notes.
