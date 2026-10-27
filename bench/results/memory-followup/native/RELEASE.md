# Alpha.7 release provenance

The retained source snapshot and benchmarks describe the implementation before the alpha.7 version bump. Every measured Go source hash matches the release source after normalizing only `db.Version` from `0.2.0-alpha.7` back to `0.2.0-alpha.6`. No other engine code changed after the recorded full-suite, targeted race, recovery and performance validation.

The release adds version/install instructions, changelog and release notes. See the repository workflows for Linux/macOS full-suite/race/stress release gates. The complete routing-map experiment stays inside evidence archives and is not compiled into the engine. The on-disk format remains 10.

The benchmark report retains its pre-release status wording as historical evidence. The alpha.7 release notes identify the published version and preserve the remaining performance gaps.
