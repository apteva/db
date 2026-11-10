AptevaDB alpha.21 adds MergeComputedDistinct for bulk unions of stored
exact distinct memberships on a read snapshot. Adjacent bucket ranges
reuse B-tree leaf reads, and each output retains an independent exact
union. A single bucket can return its saved cardinality directly.
The operation supports working-memory limits and optional diagnostics.

File format remains 10. Existing computed collections and membership
records can be queried in place.
