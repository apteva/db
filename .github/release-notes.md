AptevaDB alpha.12 lets aggregations use compound indexes.

- **Aggregations use compound indexes.** Until now, the leading `$match` of `Aggregate`, `AggregateParallel`, `AggregateEach`, `RunEach` and `AggregateGroups` used only single-field ascending indexes. So `{"tenant":"acme","day":"2026-10-05"}` scanned the whole collection even with a `(tenant, day)` index, although `Query` used it. Exact compound and descending indexes now apply when the filter binds an equality prefix of their fields, optionally with a range on the next field, the same bounds `Query` uses. A sparse compound index applies only when the filter requires a non-null value for each component. On 328k records with 3.2k matching a tenant and day, a grouped aggregate went from about 14 ms to under 1 ms.
- Results are unchanged: the index only narrows which records are read, and every predicate is still checked on each one.

See [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.12/USAGE.md#compound-indexes-and-ordered-pages) for the rules.

**Format remains 10; alpha.5–alpha.11 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
