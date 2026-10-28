# Alpha.9 publication verification

Published public prerelease: [v0.2.0-alpha.9](https://github.com/apteva/db/releases/tag/v0.2.0-alpha.9).
Tag commit: `8d6daf262403b0c669323fc02e3f9f3674d845d7`. Format remains 10.

## Gates before the tag

- The first release candidate, `545190a`, passed CI. Its 60-minute soak passed
  on Linux, but on macOS it ran into the job timeout. The scheduled soak of the
  alpha.8 engine had been cancelled the same way that morning.
- A local 20-minute soak reproduced the hang. The goroutine dump showed a checkpoint panic
  in free-list capture (`slice bounds out of range [4092:4091]`). This bug has
  been present since 0.1.0. Since alpha.7, the deferred lock in `runCheckpoint` deadlocked
  on it. `8d6daf2` fixes both and adds a regression test that fails
  on the old code.
- On `8d6daf2`:
  - [Source CI](https://github.com/apteva/db/actions/runs/40771991774) passed on Linux
    and macOS: vet, builds, unit tests, race checks, change-feed stress and fuzz smoke.
  - The [60-minute soak](https://github.com/apteva/db/actions/runs/40772012639) passed
    on Linux and macOS.
  - A local 20-minute macOS soak passed in 1,202 seconds ([output](local-soak.txt)).
  - The local unit suite passed, and so did the two-minute race-detector stress
    runs (cursor, soak and change feed).

## Publication

- The [release workflow](https://github.com/apteva/db/actions/runs/40783947471)
  passed: race/stress/soak verification on both OSes, all four archive builds,
  and prerelease publication.
- A fresh consumer resolved the tag through proxy.golang.org and sum.golang.org.
  It used no workspace and no replace, and a fresh module cache. The proxy
  reports origin commit `8d6daf2`. The embedded and HTTP Go APIs built, and
  `go install` of the CLI reports 0.2.0-alpha.9.
- The public module passed these native API checks: covering counts, watch,
  projection and replay; bounded buffers and snapshot retention; exact signed-128
  spill, disk quota and cleanup; ordered snapshot aggregation; memory accounting
  and Verify.
- New for alpha.9: the typed group visitor spilled 3,000 groups under a 64 KiB
  budget. It reported the overflowed int64 sum as not representable, and returned
  the exact 128-bit value from `IntegerSum`. It stopped on the first callback error
  and left no temporary files.
- All four Linux/macOS amd64/arm64 archives match SHA256SUMS. The build metadata
  of all eight binaries matches the archive's OS and architecture, with CGO=0,
  the clean tag commit and Go 1.25.14. This does not claim runtime execution on
  every architecture.
- The downloaded macOS ARM64 CLI and server ran the public Go HTTP driver checks:
  snapshot count, filtered/projected watch, leased snapshot reads and resume. The server
  exited 0 on SIGTERM. Unlike alpha.8, idle-watch shutdown time was not measured.
- Public alpha.8 created a format-10 directory with large values. Public alpha.9
  opened it, verified it, updated it and reopened it. No migration is needed.

## Known issue

When a callback stops `AggregateGroups`/`RunGroups` with an error, alpha.9 does
not return that error unchanged, as USAGE.md says. It returns it wrapped by
`errors.Join`, so `errors.Is(err, yours)` is true but `err == yours` is false.
The same wrapping applies to every error returned by the aggregation entry points
since alpha.8 (`aggregate_each.go` joins the result with a nil cleanup error).
Compare with `errors.Is` until it is fixed.

## Records

[Raw verification output](release-verification.txt), [module identity](public-module.json),
[checksums](SHA256SUMS), [binary build metadata](archive-build-metadata.txt),
[release metadata](release-metadata.json) and [CI runs](ci-runs.json) are kept
next to the [consumer](public-consumer-source.go.txt) and
[alpha.8 fixture](alpha8-fixture-source.go.txt) programs. The first run of the
typed-visitor check failed because its own fixture reused keys. That run was
corrected and is omitted. Downloaded executables, databases and module caches
stay outside the repository.

The [performance report](../REPORT.md) has benchmark results and tradeoffs.
Measurements preceded the checkpoint fix, which changed only free-list capture
and the checkpoint timing note.
