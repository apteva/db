#!/usr/bin/env python3
"""Report a fresh four-engine cohort without mixing candidate revisions."""
import argparse
import json
import pathlib

p = argparse.ArgumentParser()
p.add_argument('root')
p.add_argument('--dirty-fraction', default='1/4')
a = p.parse_args()
root = pathlib.Path(a.root)
data = json.loads((root / 'cpu-summary.json').read_text())
assert len(data) == 16 and all(x['trials'] == 3 for x in data)
cases = sorted({(x['rows'], x['value_bytes'], x['workload']) for x in data})
def find(c, engine):
    return next(x for x in data if (x['rows'], x['value_bytes'], x['workload']) == c and x['engine'] == engine)
def med(x, metric):
    return x[metric]['median']
def op(x, name, metric):
    return x['operations'][name]['measures'][metric]['median']
def delta(new, old):
    return (new / old - 1) * 100

lines = ['# AptevaDB memory and throughput follow-up', '',
    f'This unreleased candidate batches checkpoint writes, retains tree descent between scan leaves, bounds ordinary WAL backlog at 4 MiB, releases burst log storage at replay barriers, and retires consumed old-file translations during private compaction. The default memtable budget is CacheBytes/4, with two active budgets allowed while another table applies. Writable-page checkpoint pressure starts at {a.dirty_fraction} of the cache target. Explicit memtable budgets remain configurable. The on-disk format remains 10.', '',
    'Startup cleanup also checkpoints synchronously before workers start. This fixes a recovery deadlock exposed by the interrupted-index-build tests; waiting writers recheck closure and failure before continuing.', '',
    '## Method', '',
    '48 serial fresh-process fixtures: four datasets/insertion orders, four engine/build entries, three trials each, interleaved in a seeded order. The candidate, original alpha.6, Pebble 2.1.7 and C SQLite 3.32.2 all run in this new cohort. Earlier candidates and exploratory tuning runs are not pooled. All rows, scans and expected values are validated.', '',
    'Apple M1 Pro, ten logical CPUs, 16 GiB, Go 1.25.1. The runner retains per-trial process/background activity, timestamps, binary hashes and source patches. The source archive and provenance file identify the final source. No agent builds, tests, profiling or Linux VM run during measurement. System storage/indexing work and user applications remained active; these are measurements under recorded contention, not an idle-machine performance guarantee. Small median differences should not be treated as statistically established.', '',
    'Runner pauses followed the exit of trials 12 and 17 in the million-row dataset, to allow separate tuning probes. No measured processes overlapped those probes or builds. Their outer wrapper durations include the pauses; operation summaries use only the child process phase counters and times, which were already complete. See serial-tuning-pause.json. The exact final source was restored before each resumption.', '',
    'Fixtures stay alive in every process and are included in RSS and physical footprint. CPU is process user plus system time across engine and maintenance threads. RSS includes resident file mappings; macOS physical footprint accounts for shared/compressed pages differently. Cache options are 64 MiB, not equal total-memory caps. Pebble additionally uses a 64 MiB memtable, default compression and Bloom filters. SQLite uses a WITHOUT ROWID BLOB table, a cache per connection, ten connections for parallel reads, and WAL with synchronous OFF. Writes are per-row atomic asynchronous upserts. Load/update/delete/mixed totals include subsequent checkpoint and full compaction; raw write loops and maintenance remain separately available in runs.json. Reads repeat two full hit/miss/ten-reader passes; scans consume full values over five passes. No cold OS-cache claim.', '',
    '## Peak process memory', '',
    '| Dataset / order | Original RSS MiB | Candidate RSS MiB | Pebble RSS MiB | SQLite RSS MiB | Candidate vs Pebble |',
    '|---|---:|---:|---:|---:|---:|']
gaps = []
if not (root / 'serial-tuning-pause.json').exists():
    lines = [line for line in lines if not line.startswith('Runner pauses followed')]
if (root / 'disk-guard-resume.json').exists():
    at = lines.index('## Peak process memory')
    lines[at:at] = ['One incomplete SQLite fixture was stopped by the disk-space guard and removed before completing its insert phase. Its partial measurements are excluded. The 24 completed million-row trials were preserved; only the incomplete fixture was retried. The guard reserve was increased before resumption; completed child measurement phases are unchanged. See disk-guard-resume.json.', '']
for c in cases:
    xs = [find(c, e) for e in ('ours-before', 'ours', 'pebble', 'sqlite-c')]
    values = [med(x, 'process_peak_rss_bytes') / 2**20 for x in xs]
    change = delta(values[1], values[2])
    lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | ' + ' | '.join(f'{v:.1f}' for v in values) + f' | {change:+.1f}% |')
    if change > 0:
        gaps.append(f'- {c[0]:,} × {c[1]} B / {c[2]}: peak RSS median {change:.1f}% above Pebble.')
lines += ['', '| Dataset / order | Original footprint MiB | Candidate footprint MiB | Pebble footprint MiB | SQLite footprint MiB |', '|---|---:|---:|---:|---:|']
for c in cases:
    xs = [find(c, e) for e in ('ours-before', 'ours', 'pebble', 'sqlite-c')]
    lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | ' + ' | '.join(f"{med(x, 'process_peak_memory_footprint_bytes') / 2**20:.1f}" for x in xs) + ' |')
    change = delta(med(xs[1], 'process_peak_memory_footprint_bytes'), med(xs[2], 'process_peak_memory_footprint_bytes'))
    if change > 0:
        gaps.append(f'- {c[0]:,} × {c[1]} B / {c[2]}: physical footprint median {change:.1f}% above Pebble.')
lines += ['', 'All observations and min/max ranges are retained in cpu-summary.json and dataset runs.json. These are process lifetime peaks, not Go heap limits.', '', '## CPU and elapsed time versus original alpha.6', '']
for c in cases:
    old, new = find(c, 'ours-before'), find(c, 'ours')
    lines += [f'### {c[0]:,} × {c[1]} B / {c[2]}', '', '| Operation | Original CPU s/M | Candidate CPU s/M | CPU change | Original wall ms | Candidate wall ms | Wall change |', '|---|---:|---:|---:|---:|---:|---:|']
    for name in new['operations']:
        oc, nc = (op(x, name, 'cpu_seconds_per_million_ops') for x in (old, new))
        ow, nw = (1000 * op(x, name, 'wall_seconds') for x in (old, new))
        dc, dw = delta(nc, oc), delta(nw, ow)
        lines.append(f'| {name} | {oc:.3f} | {nc:.3f} | {dc:+.1f}% | {ow:.2f} | {nw:.2f} | {dw:+.1f}% |')
        if dc > 0 or dw > 0:
            gaps.append(f'- {c[0]:,} × {c[1]} B / {c[2]}, {name}: CPU {dc:+.1f}%, elapsed {dw:+.1f}% versus original alpha.6.')
lines += ['', 'Negative changes mean improvement. CPU is normalized per million logical operations; elapsed uses actual operation counts. Comparator CPU/throughput results are also available for every operation in cpu-summary.json/csv.', '', '## Remaining gaps', '']
lines += gaps or ['No positive median CPU/elapsed differences versus original alpha.6, or peak memory differences versus Pebble, in this cohort. This does not establish an across-the-board guarantee for other workloads.']
lines += ['', 'The complete matrix is retained regardless of outcomes. See validation.json for checks and their exact source scope. No release was published.', '', '## Reproduction', '', '```sh', 'cd bench', 'go build -o /tmp/apteva-memory-followup .', 'python3 run_memory_comparison.py --binary /tmp/apteva-memory-followup --baseline-binary /path/to/original-alpha6-benchmark --out /tmp/apteva-memory-followup-results', 'python3 summarize_large_cpu.py /tmp/apteva-memory-followup-results', f'python3 report_memory_followup.py /tmp/apteva-memory-followup-results --dirty-fraction {a.dirty_fraction}', '```', '']
(root / 'REPORT.md').write_text('\n'.join(lines) + '\n')
print(root / 'REPORT.md')
