AptevaDB alpha.16 counts more filters from index entries alone, without fetching documents.

- **`CountDocuments` covers `$in` prefixes and bounded ranges.** Equality or `$in` on the leading fields of a compound index, a following range, and `$ne null` where those prove it, count index entries only. `$in` lists expand to at most 64 ranges, and each document counts once. At 200k wide documents a two-value `$in` with a date range drops from 75 ms and 200,000 document fetches to 0.011 ms and none.
- **Filters that still need documents read fewer.** Counts and sorted queries narrow through a leading `$in` when the index names at most an eighth of the collection, as unsorted queries already did. A two-value `$in`, date range and `$like` count drops from 81 ms to 15 ms.
- **Results are unchanged.** Index counts are compared with document evaluation over thousands of randomised filters, including nulls, mixed types, large integers and buffered writes.

See [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.16/USAGE.md#counting-with-an-index-alpha16) for the rules.

**Format remains 10; alpha.5–alpha.15 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
