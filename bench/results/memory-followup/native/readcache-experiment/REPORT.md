# Rejected point-read routing experiment

An isolated clone adds a complete Go map from internal keys to leaf IDs/slots after compaction and log trimming. The memtable overlay is checked first; any tree generation change invalidates the shortcut. Snapshots retain normal tree descent. This is an experimental ceiling probe, not production code.

Three interleaved fresh processes per build, 250,000 random 1-KiB values, 64-MiB engine cache, two read passes, five value scans and ten readers. The comparison build is the selected final implementation, not original alpha.6. Harness profile `prior-memory-candidate` identifies that selected build here; `bounded64` identifies the experimental map build. Binary hashes identify both unambiguously. All fixture checks passed. Background activity and raw samples are retained.

Diagnostic status, collected outside timed phases, confirms the routing map was active throughout the initial read phases in all three prototype repetitions. No builds or tests ran alongside trials.

| Operation | Selected CPU s | Probe CPU s | CPU change | Selected elapsed ms | Probe elapsed ms | Elapsed change |
|---|---:|---:|---:|---:|---:|---:|
| Load + maintenance | 2.4017 | 2.5472 | +6.1% | 3745.17 | 3737.60 | -0.2% |
| Point hit | 0.3775 | 0.3701 | -2.0% | 376.29 | 369.34 | -1.8% |
| Point miss | 0.1614 | 0.0551 | -65.9% | 161.07 | 55.01 | -65.8% |
| Ten-reader hit | 0.4211 | 0.3928 | -6.7% | 63.26 | 63.41 | +0.2% |
| Full-value scan | 0.0912 | 0.0919 | +0.7% | 91.29 | 91.33 | +0.0% |

Peak RSS median increased from 599.0 to 694.0 MiB (+15.9%, +95.0 MiB). Physical footprint increased from 424.0 to 452.9 MiB. Misses benefit, but hit-read gains do not justify the extra allocation/build cost or provide evidence of enough headroom to pay for file-page reclamation. The complete routing map is rejected and is absent from the main implementation. Three-trial medians under background load are not significance guarantees.

The earlier `2026-10-27-readcache-probe` cohort and `2026-10-27-readcache-active-check` diagnostic exposed construction before log trimming. A later checkpoint invalidated that first prototype, so its timing results cannot establish shortcut performance. They are preserved as invalid experiments, not pooled here.

See source-snapshot.tar.gz and source-provenance.json for the measured prototype. The source diff includes outside-phase diagnostic instrumentation. The selected implementation retains its independent complete validation.
