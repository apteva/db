AptevaDB alpha.13 lets aggregations use `$in` lists on compound indexes.

- **`$in` on a compound index's leading fields.** Alpha.12 used a compound index for aggregations only when its leading fields were matched with `$eq`. So `{"tenant":{"$in":["acme"]},"day":{"$gte":"2026-10-25","$lt":"2026-10-26"}}` scanned the whole collection with a `(tenant, day)` index, while the same filter with `$eq` read only the matching records. Each combination of listed values now reads one index range, up to 64 combinations, in key order and without duplicates. A sparse compound index accepts an `$in` list that lists no null.
- Results are unchanged: the index only narrows which records are read, and every predicate is still checked on each one.

See [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.13/USAGE.md#compound-indexes-and-ordered-pages) for the rules.

**Format remains 10; alpha.5–alpha.12 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
