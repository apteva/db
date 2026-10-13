AptevaDB alpha.2 fixes the first-user precision/document bugs and adds generic
Database-app engine capabilities.

- Exact integer filtering, mixed numeric comparisons and integer-preserving map
  decoding. Hash/index joins also preserve large integers and signed zero.
- Extended documents up to 16 MiB/4,096 fields, large strings/nested values/arrays;
  compact small-document encodings remain unchanged. Valid alpha.1 wrapped root
  offset directories are reconstructed safely; corrupted payloads are refused.
- Native scalar compound indexes with mixed ASC/DESC directions, exact numeric
  ordering and snapshot index cursors with prefix/range/seek/reverse traversal.
- Logical $and/$or, literal $prefix, ordered query pages, index hints, ExplainQuery
  and query statistics. Fallback pages use bounded selection heaps.
- Optional NullsDistinct/Sparse uniqueness, configurable shared query-memory
  budgets, 4,032-byte keys/index entries, and 32 MiB server/driver frames.
- Embedded/remote API parity for compound indexes, filters, ordering, plans and
  budgets. Schemas, patch-style upserts, permissions and pagination signing
  remain adapter responsibilities.

Format 7 permanently upgrades formats 3–6 before writes. Back up with the original
binary first. Existing legacy indexes stay readable and are checked exactly;
rebuild under a new name to use ordered cursors. Logical exports write version 2;
Import reads versions 1 and 2. Map decoding now preserves int64 values directly.

Download the Linux/macOS amd64/arm64 CLI/server archive and verify SHA256SUMS, or
install github.com/apteva/db@v0.2.0-alpha.2. See README/USAGE/OPERATIONS/SERVER for
contracts and examples. This remains an alpha: one owning process, database-wide
transaction conflicts, 256 MiB atomic staging, one full-access server token, no
replication/per-user permissions. Query budgets cover accounted execution storage,
not total RSS, page caches, snapshot versions or framing. Benchmark results report
observations and do not establish a universal speed/memory lead.
