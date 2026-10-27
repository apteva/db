AptevaDB alpha.8 adds bounded native aggregation and resident-memory controls.

- `AggregateEach`/`RunEach` stream owned results from one snapshot. Proven compound-index group order supports equality prefixes, mixed directions and early limits; explicit ordered prefixes also accelerate the existing materializing API.
- Opt-in disk-backed hash aggregation and stable external sorts preserve typed accumulator state, exact signed-128 integer subtotals, snapshots, cancellation and shared memory/disk budgets. Private scratch leases support cleanup after cancellation, errors and process crashes.
- `MemoryStats` separates private page/descriptor/memtable/WAL buffers, leased/pinned storage, aggregation reservations, mapped residency estimates and process RSS. `MappedCacheBytes`/`TrimMemory` reclaim cold file translations; `ManagedMemoryBytes` adds soft pre-WAL admission/backpressure. Both targets default to disabled.
- Correct the pre-existing test-harness diagnostic race and isolate the short watch RPC timeout fixture, without changing those production contracts.

The [benchmark report](https://github.com/apteva/db/blob/v0.2.0-alpha.8/bench/results/aggregation-memory/REPORT.md) retains 110 final fresh-process comparisons with alpha.7, including one-million-document guards. Ordered aggregation reduced median CPU by 73%, peak RSS by 32% and peak physical footprint by 32%. Cold-page reclamation reduced idle RSS from 201 to 43 MiB; lifetime peaks stayed similar. At a 16 MiB budget, disk-backed aggregation completed a workload alpha.7 rejected. Versus a 128 MiB RAM execution, spill reduced peak RSS by 30% but took 3.5 times as long. Some guard medians remained slower, with overlapping timing ranges on the busy workstation; these results do not establish zero regressions everywhere.

New aggregation options/streaming are embedded Go APIs; no new HTTP streaming endpoint is included. Query budgets and managed/mapped targets are not hard process RSS limits. Unsorted group order is unspecified; float partial merges may change IEEE-754 rounding. Integer accumulation remains exact. See [usage](https://github.com/apteva/db/blob/v0.2.0-alpha.8/USAGE.md) and [operations](https://github.com/apteva/db/blob/v0.2.0-alpha.8/OPERATIONS.md) for the full contracts.

**Format remains 10; existing alpha.5–alpha.7 directories need no new migration.** Opening formats 3–9 still permanently upgrades them to format 10; back up with the original binary first. This is a public prerelease alpha.

```sh
go get github.com/apteva/db@v0.2.0-alpha.8
go install github.com/apteva/db/cmd/aptevadbctl@v0.2.0-alpha.8
go install github.com/apteva/db/cmd/aptevadb-server@v0.2.0-alpha.8
```

Linux/macOS amd64/arm64 CLI/server archives include the usage and operations guides. Verify downloads against `SHA256SUMS`.
