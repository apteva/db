AptevaDB alpha.20 adds resumable online builds for computed rollups over
multiple sources, with per-measure filters and exact distinct memberships.
A build advances in bounded transactions, including committed progress.
Writes maintain the scanned prefixes and readers see the view when the
last input finishes. Source edits maintain group totals and identity
reference counts in the same transaction.

Use BeginComputedCollection and AdvanceComputedCollection to build, and
ScanComputedDistinct on a snapshot-bound collection to merge identities
over filtered groups. The existing synchronous computed API is unchanged.

File format remains 10. Databases containing online computed definitions
require alpha.20 or later.
