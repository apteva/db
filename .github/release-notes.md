AptevaDB alpha.15 adds `$like`, a multi-field text search filter.

- **`$like` matches a SQL LIKE pattern over several fields joined by a separator**, e.g. `{"$like":{"fields":["resource_key","opportunity_key","commercial_id"],"separator":" ","pattern":"%23 op%"}}`. Semantics are SQLite's default LIKE over `coalesce(a,'') || ' ' || coalesce(b,'')`: `%` and `_` are wildcards (`_` is one UTF-8 character), ASCII letters fold case, there is no escape character, and missing or null fields read as empty.
- **It filters wherever filters apply**: Query, CountDocuments, `$match`, accumulator `where`, change feeds and computed collections, nested in `$and`/`$or`.
- **It never selects an index.** Indexed conjuncts still choose the access path, so a search bounded by an equality and a date range only reads that range. At 200k documents a search inside a centre + 90-day range adds about 0.6 ms to a count; an unbounded search costs about 0.4 µs per document.

See [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.15/USAGE.md#multi-field-like-alpha15) for the rules.

**Format remains 10; alpha.5–alpha.14 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
