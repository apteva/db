AptevaDB alpha.14 makes distinct counts exact when one group's distinct sets outgrow the memory budget.

- **Distinct counts no longer fail with "aggregation partition depth exhausted".** Spilled group state is partitioned by group key. A group with `_id: null` has a single key, so its state could not be split, and alpha.13 repartitioned it until the depth limit. Eight filtered `$countDistinct` metrics over 64,084 rows and 24,443 distinct callers failed at a 16 MiB budget; six fit. Such a group's spilled states are now read once per disjoint slice of the distinct values, and the slice counts add up to the exact total. Slices start from the spilled size and double when one does not fit, up to 1024, reported as `AggregateStats.DistinctSlices`. The other accumulators of that group are merged on the first pass.
- **Merging distinct sets charges only new members.** Partial states from parallel workers overlap; each incoming set used to be charged in full.
- Results are exact, as before: no estimate is ever used.

See [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.14/USAGE.md#dashboard-aggregation-alpha11) for the rules.

**Format remains 10; alpha.5–alpha.13 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
