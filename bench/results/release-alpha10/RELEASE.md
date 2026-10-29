# Alpha.10 publication verification

Published public prerelease: [v0.2.0-alpha.10](https://github.com/apteva/db/releases/tag/v0.2.0-alpha.10).
Tag commit: `420f38e09b30a69b3eae0f9bf81f6e7a5d3beb2b`. Format remains 10.

Alpha.10 changes one thing: aggregation entry points return errors unchanged,
fixing the known issue recorded for alpha.9. No API, format or performance change.

## Gates before the tag

- The first [source CI](https://github.com/apteva/db/actions/runs/40835507214) attempt
  on `420f38e` failed one Ubuntu unit test, `TestChecksumDetectsCorruption`
  ("Verify did not detect the corrupt page"). macOS unit tests, both race-detector
  jobs and fuzz smoke passed. The test passed 200/200 runs locally on macOS, and
  alpha.9's Ubuntu CI passed it. Rerunning the failed job passed (attempt 2), so it
  was treated as intermittent. It is not explained: an occasional missed corrupt
  page in `Verify` on Linux remains possible and should be investigated.
- The tag was pushed after the rerun passed.

## Publication

- [Tag CI](https://github.com/apteva/db/actions/runs/40839616917) passed on Linux
  and macOS.
- The [release workflow](https://github.com/apteva/db/actions/runs/40839616865)
  passed and published the prerelease with all four archives.
- The [60-minute soak](https://github.com/apteva/db/actions/runs/40835507999) on
  `420f38e` passed on Linux and macOS (each job about 80 minutes including setup).
- A fresh consumer resolved the tag through proxy.golang.org and sum.golang.org,
  with no workspace, no replace and a fresh module cache. The proxy reports origin
  commit `420f38e` ([module identity](public-module.json)).
- The consumer ([source](public-consumer-source.go.txt)) stopped `AggregateGroups`
  with a callback error and got that exact error back (`err == stop`); see its
  [output](public-consumer-output.txt).
- All four Linux/macOS amd64/arm64 archives match [SHA256SUMS](SHA256SUMS). The
  build metadata of all eight binaries matches the archive's OS and architecture,
  with CGO=0 and the clean tag commit ([metadata](archive-build-metadata.txt)).
  The downloaded macOS ARM64 CLI reports `0.2.0-alpha.10 (format 10)`. Other
  architectures were not executed.
- Downstream: Apteva Database 0.5.3 pins alpha.10 and passed its full suite, race
  tests and packaged-binary checks on all three backends.

## Records

[Release metadata](release-metadata.json) and [CI runs](ci-runs.json) are kept
next to the files above. Downloaded executables and module caches stay outside
the repository.
