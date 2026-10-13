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
