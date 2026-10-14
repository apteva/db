AptevaDB alpha.6 adds native filtered document counts for embedded Go applications and the HTTP Go driver.

- `Collection.CountDocuments(ctx, filter, db.CountOptions{})` counts scalar compound equality prefixes without loading documents when every predicate is covered. Checkpointed indexes count bounded leaf slots directly; buffered snapshot writes use the index merge scanner.
- Preserve exact integer equality, mixed index directions, null/missing and sparse semantics. Residual predicates, logical OR and multikey indexes fall back to document evaluation, with no matching result array or sort retained.
- Counts use one snapshot and support cancellation, skip/limit, memory budgets, index hints and explicit errors. `QueryStats.Plan.IndexOnly`, `DocumentFetches` and `IndexEntriesExamined` expose the execution path. The Go driver also counts through leased read-only snapshots.
- Add native/driver correctness, concurrency, checkpoint/reopen and budget regressions, plus fresh-fixture count comparisons and paired alpha.5 guards. See the [validation report](https://github.com/apteva/db/blob/v0.2.0-alpha.6/bench/results/counts/VALIDATION.md) for all samples and the reproduction.

On three fresh 100,000-row fixtures with 5,000 matches, the covering count median was **0.006355 ms**, versus **3.024039 ms** for alpha.5 Query/count and **0.125922 ms** for SQLite C: about **476×** and **20×** faster for this checkpointed case, with zero document fetches. All samples, allocation counts and slower guard observations are retained in the validation report. Background activity remained present; this is not a universal database ranking.

Adapters must adopt `CountDocuments` to use this path; existing Query/aggregation behavior is unchanged. `COUNT(column)` remains the `$countNonNull` accumulator. See [USAGE.md](https://github.com/apteva/db/blob/v0.2.0-alpha.6/USAGE.md#filtered-document-counts) for coverage, hints, memory exclusions and examples.

**Format remains 10; alpha.5 directories need no new migration.** Opening formats 3–9 still permanently upgrades them to format 10; back up with the original binary first. This remains a public prerelease alpha.

```sh
go get github.com/apteva/db@v0.2.0-alpha.6
go install github.com/apteva/db/cmd/aptevadbctl@v0.2.0-alpha.6
go install github.com/apteva/db/cmd/aptevadb-server@v0.2.0-alpha.6
```

Linux/macOS amd64/arm64 CLI/server archives include the updated usage and server guides; verify them using SHA256SUMS.
