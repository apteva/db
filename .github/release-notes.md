AptevaDB alpha.18 adds a literal substring search to `$like`.

- **`literal` replaces `pattern` when wildcards are not wanted.** `{"$like":{"fields":["label"],"literal":"100%"}}` matches a real percent sign, and `A_B` a real underscore. ASCII letters compare case-insensitively, and missing and null fields read as the empty string. An empty literal matches every string.
- **Like a pattern, it never selects an index.** The other conjuncts choose the access path. It works in queries, counts and aggregation `$match`.
- **Existing `pattern` filters are unchanged.**

**Format remains 10; alpha.5–alpha.17 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
