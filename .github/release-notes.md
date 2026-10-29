AptevaDB alpha.10 fixes one error-handling bug in aggregation. There are no API, format or performance changes.

- **Aggregation errors come back unchanged.** Since alpha.8 every error returned by the aggregation entry points was wrapped by `errors.Join`, so an error returned from an `AggregateGroups`/`RunGroups` callback did not compare equal to the original with `==`, contrary to [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.10/USAGE.md#typed-group-visitor-alpha9). `errors.Is` matched in both versions and still does. If temporary-file cleanup fails, its error is still joined onto the result.

Alpha.10 also contains everything in [alpha.9](https://github.com/apteva/db/releases/tag/v0.2.0-alpha.9), including the checkpoint hang fix; alpha.7 and alpha.8 users should upgrade.

**Format remains 10; alpha.5–alpha.9 directories need no migration.** Opening formats 3–9 still permanently upgrades them to format 10, so back up with the original binary first. This is a public prerelease alpha.
