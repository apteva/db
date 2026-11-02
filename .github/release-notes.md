AptevaDB alpha.11 adds four aggregation features for reporting dashboards and fixes a wrong-count bug in alpha.9 and alpha.10.

- **Fix: a lone `$count` with `AllowDiskUse` returned wrong counts.** A `$group` whose only accumulator is `$count` or `$countNonNull` returned wrong counts, mostly 0 and without an error, when run through `AggregateEach`, `RunEach`, `AggregateWithOptions` or `AggregateGroups` with `AllowDiskUse: true`, whether or not it spilled. Alpha.9 and alpha.10 are affected; alpha.8, plain `Aggregate`/`Run` and runs without `AllowDiskUse` are not. Users of those calls should upgrade.
- **Filtered accumulators.** `{"$count":{},"where":{"status":"answered"}}`, or any accumulator with a `where` filter in `$match` syntax, gives several filtered metrics in one pass.
- **`$dateTrunc` with time zones.** Hour, day, week, month and year buckets in any IANA zone, with `startOfWeek` and epoch-number support. Daylight-saving days are 23 or 25 hours long.
- **`$countDistinct`.** Exact distinct counts per group, which spill with `AllowDiskUse`.
- **`$lookup` on the record key.** `"foreignField":"$key"` joins on the foreign collection's record key, without an index or table.

See [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.11/USAGE.md#dashboard-aggregation-alpha11) for semantics and the [performance report](https://github.com/apteva/db/blob/v0.2.0-alpha.11/bench/results/alpha11-dashboard/REPORT.md) for the comparison with SQLite and Pebble.

**Format remains 10; alpha.5–alpha.10 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
