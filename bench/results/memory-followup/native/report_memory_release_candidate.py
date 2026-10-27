#!/usr/bin/env python3
"""Keep final/prior/original native cohorts separate from retained competitors."""
import argparse
import json
import pathlib

p = argparse.ArgumentParser()
p.add_argument('root')
p.add_argument('--comparators', required=True)
a = p.parse_args()
root, comp_root = pathlib.Path(a.root), pathlib.Path(a.comparators)
native = json.loads((root / 'cpu-summary.json').read_text())
comparators = json.loads((comp_root / 'cpu-summary.json').read_text())
assert len(native) == 12 and all(x['trials'] == 3 for x in native)
assert {x['engine'] for x in native} == {'ours', 'ours-before', 'ours-prior'}
assert len(comparators) == 16 and all(x['trials'] == 3 for x in comparators)
cases = sorted({(x['rows'], x['value_bytes'], x['workload']) for x in native})
def find(c, e):
    data = native if e.startswith('ours') else comparators
    return next(x for x in data if (x['rows'], x['value_bytes'], x['workload']) == c and x['engine'] == e)
def med(x, key): return x[key]['median']
def op(x, name, key): return x['operations'][name]['measures'][key]['median']
def delta(n, o): return (n / o - 1) * 100

lines = ['# AptevaDB: final memory and performance validation', '',
 'The selected code retains the previous candidate\'s buffering policy: a CacheBytes/2 memtable budget accounting for record/metadata/obsolete storage, up to two active budgets while a table applies, and writable-page pressure at CacheBytes/4. Smaller default tables and larger dirty-page batches remain separate rejected experiments. Explicit options retain their existing meaning.', '',
 'Structural changes batch contiguous checkpoint I/O, preserve immutable capture buffers, reuse descent between scan leaves, bound ordinary WAL backlog, release oversized log storage at replay barriers, and retire consumed file translations during private compaction. A WAL boundary fix flushes a complete prefix before buffer growth. Snapshot-retired IDs require both reader completion and a durable metadata barrier before reuse, including retirements during checkpoint I/O. The on-disk format remains 10.', '',
 '## Cohorts and method', '',
 'The final native cohort has 36 serial fresh-process trials: four dataset/order combinations, three repetitions, and three interleaved native builds. Original alpha.6 is the engine at commit 65cedd31a494bb561485cfc07c3dd0aeed06be06. Previous is the saved final warmed memory candidate from October 15, which matches the starting point of this optimization request. Final contains the validated changes and both recovery/WAL fixes. All three use the identical current Go benchmark harness, compiler and cache/fixture options.', '',
 'Pebble and SQLite columns come from the preceding completed 48-trial October 27 matrix, with three repetitions of each competitor per dataset/order. Its intermediate native candidate used different tuning and is excluded from final native results. Native versions and cohorts are never pooled. Competitor values are reference comparisons from that preceding cohort; they were not measured simultaneously with the final native cohort.', '',
 'Apple M1 Pro, ten logical CPUs, 16 GiB; Go 1.25.1. Background applications and system storage/indexing activity were present and recorded per trial. No agent builds, tests, profiling or VM ran alongside measurement. Retain all samples and min/max ranges; small median differences under contention are not statistical guarantees. Every completed trial validates values and row counts. Source archives, hashes, build information and validation scope are retained.', '',
 'Fixtures remain alive and are included in process lifetime RSS/physical-footprint peaks. CPU is process user plus system time across threads. Cache settings are 64 MiB, not total-process caps. Pebble additionally uses a 64 MiB memtable, compression and Bloom filters; SQLite C 3.32.2 uses WITHOUT ROWID BLOB rows, WAL with synchronous OFF, and a cache per connection with ten parallel read connections. Reads repeat two hit/miss/ten-reader passes, and five full-value scan passes. Writes are per-row atomic asynchronous upserts. Load/update/delete/mixed operation totals include the subsequent checkpoint and full compaction. Raw loops, verification and maintenance remain individually recorded. No cold OS-cache claim.', '',
 'macOS tracks resident memory and physical footprint with different ledgers; footprint also accounts for compressed internal memory. Both metrics are reported. See [Apple\'s kernel accounting](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/task.c#L1139).', '',
 '## Peak process memory', '',
 '| Dataset / insertion | Alpha.6 RSS MiB | Previous RSS MiB | Final RSS MiB | Pebble RSS MiB | SQLite RSS MiB | Final vs previous |',
 '|---|---:|---:|---:|---:|---:|---:|']
checks, gaps = [], []
for c in cases:
    xs = [find(c, e) for e in ('ours-before', 'ours-prior', 'ours', 'pebble', 'sqlite-c')]
    vs = [med(x, 'process_peak_rss_bytes') / 2**20 for x in xs]
    lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | ' + ' | '.join(f'{v:.1f}' for v in vs) + f' | {delta(vs[2], vs[1]):+.1f}% |')
    for key, label in [('process_peak_rss_bytes', 'RSS'), ('process_peak_memory_footprint_bytes', 'physical footprint')]:
        for index, baseline in [(1, 'previous'), (3, 'Pebble')]:
            d = delta(med(xs[2], key), med(xs[index], key))
            checks.append({'case': c, 'metric': label, 'baseline': baseline, 'change_percent': d, 'non_regressing_median': d <= 0})
            if d > 0: gaps.append(f'- {c[0]:,} × {c[1]} B / {c[2]}: final peak {label} {d:.1f}% above {baseline}.')
lines += ['', '| Dataset / insertion | Alpha.6 footprint MiB | Previous footprint MiB | Final footprint MiB | Pebble footprint MiB | SQLite footprint MiB |', '|---|---:|---:|---:|---:|---:|']
for c in cases:
    xs = [find(c, e) for e in ('ours-before', 'ours-prior', 'ours', 'pebble', 'sqlite-c')]
    lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | ' + ' | '.join(f"{med(x, 'process_peak_memory_footprint_bytes') / 2**20:.1f}" for x in xs) + ' |')
lines += ['', '### Worst observed RSS across the three repetitions', '', '| Dataset / insertion | Previous maximum MiB | Final maximum MiB | Change |', '|---|---:|---:|---:|']
for c in cases:
    old, new = (find(c, e)['process_peak_rss_bytes']['max'] / 2**20 for e in ('ours-prior', 'ours'))
    lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | {old:.1f} | {new:.1f} | {delta(new, old):+.1f}% |')
lines += ['', 'These maxima describe the observed three-trial ranges, not a worst-case guarantee. Median comparisons above remain the primary summaries.', '', '## CPU and elapsed time versus the previous memory candidate', '']
for c in cases:
    old, new = find(c, 'ours-prior'), find(c, 'ours')
    lines += [f'### {c[0]:,} × {c[1]} B / {c[2]}', '', '| Operation | Previous CPU s/M | Final CPU s/M | CPU change | Previous wall ms | Final wall ms | Wall change |', '|---|---:|---:|---:|---:|---:|---:|']
    for name in new['operations']:
        oc, nc = (op(x, name, 'cpu_seconds_per_million_ops') for x in (old, new))
        ow, nw = (op(x, name, 'wall_seconds') * 1000 for x in (old, new))
        dc, dw = delta(nc, oc), delta(nw, ow)
        lines.append(f'| {name} | {oc:.3f} | {nc:.3f} | {dc:+.1f}% | {ow:.2f} | {nw:.2f} | {dw:+.1f}% |')
        for baseline in ('ours-prior', 'ours-before'):
            for key in ('cpu_seconds_per_million_ops', 'wall_seconds'):
                d = delta(op(new, name, key), op(find(c, baseline), name, key))
                checks.append({'case': c, 'operation': name, 'metric': key, 'baseline': baseline, 'change_percent': d, 'non_regressing_median': d <= 0})
        if dc > 0 or dw > 0: gaps.append(f'- {c[0]:,} × {c[1]} B / {c[2]}, {name}: CPU {dc:+.1f}%, elapsed {dw:+.1f}% versus previous.')
lines += ['', '## Load plus maintenance versus original alpha.6', '', '| Dataset / insertion | Alpha.6 ms | Previous ms | Final ms | Final vs alpha.6 |', '|---|---:|---:|---:|---:|']
for c in cases:
    vs = [1000 * op(find(c, e), 'Load + maintenance', 'wall_seconds') for e in ('ours-before', 'ours-prior', 'ours')]
    lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | ' + ' | '.join(f'{v:.2f}' for v in vs) + f' | {delta(vs[2], vs[0]):+.1f}% |')
lines += ['', '## Remaining positive median differences', '']
lines += gaps or ['No positive median differences in these comparisons. This does not establish a universal lead for other workloads or operating conditions.']
lines += ['', 'performance-gates.json enumerates every comparison, including original-alpha.6 CPU/elapsed deltas. These are strict comparisons of observed medians, not significance tests. All source cohorts, samples and ranges remain available. See validation.json for functional/race/recovery checks. Changes remain unreleased.', '']
lines += ['', '## Additional read-cache experiment', '',
 'A separate complete-key routing-map prototype was tested in three interleaved fresh processes per build with 250,000 random 1-KiB values. Diagnostics confirmed the corrected shortcut was active. Its hit-read CPU improved only 2.0%, while peak RSS increased by 95.0 MiB (15.9%) and load CPU rose 6.1%. Misses improved 65.9%, but this fails the memory/no-regression requirement. The map remains outside the selected implementation. See the [rejected experiment and raw samples](readcache-experiment/REPORT.md). The initial invalidated prototype is preserved separately and excluded from these comparisons.', '']
(root / 'REPORT.md').write_text('\n'.join(lines) + '\n')
(root / 'performance-gates.json').write_text(json.dumps({'checks': checks, 'all_non_regressing_medians': all(x['non_regressing_median'] for x in checks), 'statistical_significance_claimed': False}, indent=2) + '\n')
print(root / 'REPORT.md')
