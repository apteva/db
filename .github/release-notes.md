AptevaDB alpha.17 narrows filters through an index for more of the collection.

- **The cutoff follows the real collection size.** A filter that needs documents reads them by key while the index names at most a quarter of the collection. This replaces the eighth stated for alpha.16, which was applied to an estimate that can be several times off in either direction: on 125,865 real documents it read 45,489, so narrowing stopped near 5,700 entries. Past 4,096 entries the size is now counted from an index with one entry per document, reading no documents. A collection without such an index still uses the estimate.
- **Only `$in` scopes of several values change.** An equality or a one-value `$in` already read through the index with no cutoff.
- **Results are unchanged.**

A `$in` scope and `$like` count on those real documents, best of 5, warm:

| Entries in scope | alpha.16 | alpha.17 |
|---:|---:|---:|
| 4,567 | 10.7 ms | 12.1 ms |
| 6,116 | 65.4 ms | 14.6 ms |
| 13,693 | 78.8 ms | 32.2 ms |
| 25,737 | 99.7 ms | 59.3 ms |
| 31,803 | 109.7 ms | 111.2 ms |
| 62,537 | 158.4 ms | 160.3 ms |

The first row reads by key on both, so that difference is noise. Cold reads were not measured.

**Format remains 10; alpha.5–alpha.16 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
