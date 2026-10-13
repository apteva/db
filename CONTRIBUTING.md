# Contributing

Use Go 1.25 or newer. The root engine/server/client use only the standard library;
the benchmark harness has a separate module containing comparison dependencies.

```sh
go vet ./...
go test -short ./...
go test -race ./...
CURSOR_STRESS=2m SOAK=2m go test -race -run 'TestCursorStress|TestSoak' -timeout 10m .
cd bench
go vet ./...
go build .
```

Test Linux and macOS before releasing. Preserve old-format fixtures, add an
explicit format migration for incompatible changes, and never infer correctness
from performance measurements. Benchmarks must state build SHA, hardware, caches,
sync settings, fixture size, repetitions and background load; retain every trial.
Use Upsert for comparisons with engines whose Set operation replaces values.
Keep performance benchmarks separate from compiler/test/soak CPU activity.

Release: update Version and CHANGELOG together, complete CI and soak checks on the
release commit, then push its v-prefixed tag. The release workflow validates the
version, runs tests/race checks and publishes four CLI/server archives plus SHA256SUMS.
Pre-release tags are marked prerelease on GitHub. Until 1.0, API changes may occur;
record the old/new behavior and migration in the changelog.
