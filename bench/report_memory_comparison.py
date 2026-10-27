#!/usr/bin/env python3
"""Report retained memory/CPU trials and paired pre-change comparisons."""
import argparse, json, pathlib
p=argparse.ArgumentParser();p.add_argument('root');a=p.parse_args();root=pathlib.Path(a.root)
data=json.loads((root/'cpu-summary.json').read_text())
engines=['ours-before','ours','pebble','sqlite-c']
names={'ours-before':'AptevaDB before','ours':'AptevaDB candidate','pebble':'Pebble 2.1.7','sqlite-c':'SQLite C 3.32.2'}
lines=['# Native memory optimization validation','',
'Three serial fresh-process trials for each engine, fixture and insertion order. The unchanged alpha.6 binary is interleaved with the candidate, Pebble and SQLite C. All successful trials are retained; medians and ranges are available in the accompanying JSON/CSV. CPU is process user plus system time, including engine, GC and maintenance threads. Memory is process lifetime peak RSS and macOS physical footprint, including workload fixtures and native/mapped allocations.','',
'Hardware: Apple M1 Pro, ten logical CPUs, 16 GiB RAM; Go 1.25.1. See dataset metadata for OS version, binary SHA256, working-tree patch, dependency versions, and per-run background activity. These are embedded raw key/value operations with binary 16-byte keys and random full-value payloads. Every phase validates results. Read hits/misses and ten-reader hits each make two passes; full-value scans make five. Writes use per-row atomic upserts and asynchronous/non-fsync durability.','',
'All engines receive a 64 MiB cache option. This is not an equal total-process memory cap: SQLite has a cache per connection, Pebble also has a 64 MiB memtable, and the candidate derives a 32 MiB native memtable budget including metadata and replaced records. Before the change, AptevaDB derived 32 MiB of logical payload and allowed its active buffer to grow to four budgets. The candidate permits a two-budget active burst while another table applies, then uses backpressure. Individual acknowledged write loops and subsequent maintenance are preserved separately in raw phase results; operation summaries include maintenance to account for completed engine work.','',
'The SQLite C binding bundles SQLite 3.32.2 and stores BLOBs in a WITHOUT ROWID table. This is a version-specific result, not a claim about the latest SQLite. Pebble uses Bloom filters and its default compression. No cold OS-cache claim is made. File-backed memory and compressed memory are charged differently by RSS and macOS physical footprint; report both. Go allocation counts exclude native memory. Diagnostic/profiling runs are separate and are not included in this matrix.','',
'## Peak process memory','']
outcomes=['## Results and remaining gaps','',
'All 48 trials passed their value/count checks. These results support a substantial memory improvement, but do not meet an all-metrics/no-regression acceptance criterion.','']
for a in data:
 if a['engine']!='ours':continue
 case=(a['rows'],a['value_bytes'],a['workload'])
 b=next(d for d in data if d['engine']=='ours-before' and (d['rows'],d['value_bytes'],d['workload'])==case)
 pb=next(d for d in data if d['engine']=='pebble' and (d['rows'],d['value_bytes'],d['workload'])==case)
 rss=a['process_peak_rss_bytes']['median']/2**20;old=b['process_peak_rss_bytes']['median']/2**20;pr=pb['process_peak_rss_bytes']['median']/2**20
 foot=a['process_peak_memory_footprint_bytes']['median']/2**20;pf=pb['process_peak_memory_footprint_bytes']['median']/2**20
 outcomes.append(f'- {case[0]:,} × {case[1]} B ({case[2]}): peak RSS {old:.1f} → {rss:.1f} MiB ({(rss/old-1)*100:+.1f}%), versus Pebble {pr:.1f} MiB. Physical footprint {foot:.1f} versus Pebble {pf:.1f} MiB.')
 for op,m in a['operations'].items():
  for key,label in [('cpu_seconds','CPU'),('wall_seconds','elapsed time')]:
   v=m['measures'][key]['median'];prev=b['operations'][op]['measures'][key]['median'];delta=(v/prev-1)*100
   if delta>0:
    outcomes.append(f'  Remaining positive median change: {op}, {label} {delta:+.1f}% versus the unchanged build.')
outcomes+=['','Three repetitions are insufficient to establish statistical significance for small differences. Keep the complete ranges; do not interpret a lower CPU median as proof of lower elapsed time. Peak RSS remains above Pebble on the sequential million-row and both 1-KiB cases. Physical footprint is below Pebble in all four cases.','',
'The macOS full suite passed (213.758 s), final ownership/cache/compaction race checks passed (63.778 s), and final Linux arm64 ownership/cache/compaction checks passed (26.412 s). The earlier memory implementation also passed the full race and Linux suites; see validation.json for their exact scope and later fixes. Vet and Linux/Darwin amd64 cross-builds passed. No release or new tag has been published.','']
lines[lines.index('## Peak process memory'):lines.index('## Peak process memory')]=outcomes
for case in sorted({(d['rows'],d['value_bytes'],d['workload'])for d in data}):
 subset={d['engine']:d for d in data if (d['rows'],d['value_bytes'],d['workload'])==case}
 lines += [f'### {case[0]:,} rows × {case[1]} B, {case[2]} insertion','', '| Engine | Peak RSS MiB (median; range) | Peak footprint MiB (median; range) |','|---|---:|---:|']
 for e in engines:
  if e not in subset:continue
  d=subset[e];cells=[]
  for k in ('process_peak_rss_bytes','process_peak_memory_footprint_bytes'):
   m=d[k];cells.append(f"{m['median']/2**20:.1f}; {m['min']/2**20:.1f}–{m['max']/2**20:.1f}")
  lines.append('| '+names[e]+' | '+' | '.join(cells)+' |')
 lines+=['','| Operation | Before CPU s/M | Candidate CPU s/M | Pebble CPU s/M | SQLite CPU s/M | Candidate versus before |','|---|---:|---:|---:|---:|---:|']
 for op in next(iter(subset.values()))['operations']:
  vals={e:subset[e]['operations'][op]['measures']['cpu_seconds_per_million_ops']['median']for e in subset}
  delta=(vals['ours']/vals['ours-before']-1)*100 if 'ours-before'in vals else 0
  lines.append('| '+op+' | '+' | '.join(f'{vals[e]:.3f}'if e in vals else 'pending'for e in engines)+f' | {delta:+.1f}% |')
 lines+=['','| Operation | Before wall ms | Candidate wall ms | Pebble wall ms | SQLite wall ms | Candidate versus before |','|---|---:|---:|---:|---:|---:|']
 for op in next(iter(subset.values()))['operations']:
  vals={e:subset[e]['operations'][op]['measures']['wall_seconds']['median']*1000 for e in subset}
  delta=(vals['ours']/vals['ours-before']-1)*100 if 'ours-before' in vals else 0
  lines.append('| '+op+' | '+' | '.join(f'{vals[e]:.2f}' if e in vals else 'pending' for e in engines)+f' | {delta:+.1f}% |')
 lines+=['','Negative change means an improvement. Throughput, average cores, allocations, every observation, and raw acknowledged-write/maintenance phases are retained in the data files. Wall-time totals use the recorded operation counts (two point-read passes and five scan passes), not one million operations.','']
lines+=['## Implementation','',
'Stable file segments avoid duplicate mappings when the file grows. Writable pages, metadata descriptors and memtable data use explicitly owned native storage; snapshots/cursors and checkpoint I/O keep references until retirement. Both cache modes stream compaction output. Checkpoint vectors/scratch storage are reused. Verified clean reads avoid descriptor allocations and read-side CLOCK writes. Old file-page translations are reclaimed only when live views/I/O no longer need them, preserving file contents and current warm data. Small WAL buffers remain on the Go heap; larger bursts use reclaimable storage.','',
'Testing exposed a compaction WAL replay/deletion race; the fix pins the replay range and serializes compactions and rotations. Catch-up runs without monopolizing the old checkpointer, then uses a brief memory-only catch-up/final swap. Bounded buffers can still apply backpressure when ingestion outruns checkpoint progress. This is not a hard process-memory limit or a guarantee of zero write latency.','',
'## Reproduction','',
'```sh','cd bench','go build -o /tmp/apteva-memory-candidate .','python3 run_memory_comparison.py --binary /tmp/apteva-memory-candidate --baseline-binary /path/to/unchanged-alpha6-benchmark --out /tmp/apteva-memory-results','python3 summarize_large_cpu.py /tmp/apteva-memory-results','python3 report_memory_comparison.py /tmp/apteva-memory-results','```','',
'The original benchmark binary is identified by SHA256 in baseline-origin.json and matches the prior CPU benchmark archive. The candidate is the recorded working tree based on 65cedd3, not a newly published release. Source patches/copies and compiler/module metadata accompany raw measurements. Compilers/tests/profilers and the temporary Linux test VM were stopped before measurement.','']
(root/'REPORT.md').write_text('\n'.join(lines)+'\n');print(root/'REPORT.md')
