#!/usr/bin/env bash
set -euo pipefail
: "${RELEASE_TAG:?release tag is required}"
: "${RELEASE_GOOS:?target OS is required}"
: "${RELEASE_GOARCH:?target architecture is required}"
if [[ ! "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.-]+)?$ ]]; then exit 2; fi
case "$RELEASE_GOOS" in linux|darwin) ;; *) exit 2 ;; esac
case "$RELEASE_GOARCH" in amd64|arm64) ;; *) exit 2 ;; esac
version=$(go run ./cmd/aptevadbctl version | awk '{print $2}')
test "v${version}" = "$RELEASE_TAG"
stage="dist/package-${RELEASE_GOOS}-${RELEASE_GOARCH}"
mkdir -p "$stage"
for command in aptevadbctl aptevadb-server; do
  env CGO_ENABLED=0 GOOS="$RELEASE_GOOS" GOARCH="$RELEASE_GOARCH" go build -trimpath -ldflags='-s -w' -o "$stage/$command" "./cmd/$command"
done
cp LICENSE README.md USAGE.md OPERATIONS.md SERVER.md DURABILITY.md FORMAT.md CHANGELOG.md CHANGE_FEEDS.md "$stage/"
archive="dist/aptevadb_${version}_${RELEASE_GOOS}_${RELEASE_GOARCH}.tar.gz"
tar -czf "$archive" -C "$stage" .
