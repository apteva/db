#!/usr/bin/env python3
"""Report final native repeats without pooling different candidate versions."""
import argparse,json,pathlib
p=argparse.ArgumentParser();p.add_argument('root');p.add_argument('--comparators',required=True);a=p.parse_args()
root=pathlib.Path(a.root);other=pathlib.Path(a.comparators)
final=json.loads((root/'cpu-summary.json').read_text());comparators=json.loads((other/'cpu-summary.json').read_text())
assert sum(x['trials'] for x in final)==24 and all(x['trials']==3 for x in final)
assert sum(x['trials'] for x in comparators)==48 and all(x['trials']==3 for x in comparators)
def case(x):return (x['rows'],x['value_bytes'],x['workload'])
def median(x,key):return x[key]['median']
def operation(x,op,key):return x['operations'][op]['measures'][key]['median']
def find(data,c,e):return next(x for x in data if case(x)==c and x['engine']==e)
def change(v,b):return (v/b-1)*100
cases=sorted({case(x) for x in final});names={'ours-before':'Unchanged alpha.6','ours':'Final AptevaDB candidate','pebble':'Pebble 2.1.7','sqlite-c':'SQLite C 3.32.2'}
lines=['# AptevaDB memory optimization: final validation','',
'Generic memory reclamation substantially reduces process peaks. The final code also warms the replacement file after releasing the old mapping, correcting the first-read fault cost found in the initial candidate. This is an unreleased working-tree change; the on-disk format stays 10.','',
'## Method and provenance','',
'The first matrix contains 48 serial fresh-process trials: unchanged alpha.6, the memory candidate, Pebble and SQLite C, three trials per dataset/order. After fixing post-compaction warm-up, a second matrix repeats all four dataset/order combinations for the final native candidate and unchanged alpha.6, again interleaved with three fresh processes each (24 trials). Competitor columns come from the completed first matrix. The two candidate versions and baseline cohorts are kept separate; they are not pooled or cherry-picked. This is not a claim that the final code was measured simultaneously with the competitors.','',
'Apple M1 Pro, ten logical CPUs, 16 GiB; Go 1.25.1. OS/compiler/dependency versions, source patches/archive hashes, binary hashes, timestamps and per-run background activity are retained. Compilers, tests, profiling and the temporary Linux VM were stopped during measurement. All 72 reportable trials validate values, scans and row counts. Diagnostic/tuning trials are separate.','',
'Each process includes identical retained workload fixtures. CPU means process user plus system time across all engine/GC/maintenance threads. RSS and macOS physical footprint include heap, native/mapped memory and fixtures. These metrics charge file-backed and compressed memory differently; neither is a Go allocation counter. Read phases repeat two hit/miss/ten-reader passes; scans consume full values in five passes. Raw 16-byte binary keys and random 100-byte/1-KiB payloads are used. Writes are per-row atomic asynchronous upserts; totals include the subsequent checkpoint/full compaction. A lower CPU median is not necessarily a lower elapsed-time median.','',
'All engines get a 64 MiB cache option, which is not a total-process cap. Native defaults to a 32 MiB memtable budget accounting for native record/metadata storage and overwritten values, with an active burst of up to two budgets during apply. Before the change the budget counted only logical payload and allowed four active budgets. Pebble also has a 64 MiB memtable and default compression/Bloom filters. SQLite has a cache per connection, ten connections for parallel reads, WAL/synchronous OFF, and a WITHOUT ROWID BLOB table. The C binding bundles SQLite 3.32.2; these are version-specific results. There is no cold OS-cache claim or equal total-memory-cap claim.','',
'## Peak process memory','',
'| Dataset / insertion | Alpha.6 RSS MiB | Final RSS MiB | Pebble RSS MiB | SQLite RSS MiB | Native change |','|---|---:|---:|---:|---:|---:|']
for c in cases:
 new=find(final,c,'ours');old=find(final,c,'ours-before');pb=find(comparators,c,'pebble');sq=find(comparators,c,'sqlite-c')
 vals=[median(x,'process_peak_rss_bytes')/2**20 for x in [old,new,pb,sq]]
 lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | '+' | '.join(f'{v:.1f}' for v in vals)+f' | {change(vals[1],vals[0]):+.1f}% |')
lines+=['','| Dataset / insertion | Alpha.6 footprint MiB | Final footprint MiB | Pebble footprint MiB | SQLite footprint MiB |','|---|---:|---:|---:|---:|']
for c in cases:
 xs=[find(final,c,'ours-before'),find(final,c,'ours'),find(comparators,c,'pebble'),find(comparators,c,'sqlite-c')]
 lines.append(f'| {c[0]:,} × {c[1]} B / {c[2]} | '+' | '.join(f"{median(x,'process_peak_memory_footprint_bytes')/2**20:.1f}" for x in xs)+' |')
lines+=['','These are medians. Every observation and min/max range is retained in cpu-summary.json and dataset runs.json. Three repetitions do not establish statistical significance for small differences or a universal lead.','', '## CPU and elapsed time versus unchanged alpha.6','']
regress=[]
for c in cases:
 new=find(final,c,'ours');old=find(final,c,'ours-before')
 lines +=[f'### {c[0]:,} × {c[1]} B / {c[2]}','', '| Operation | Before CPU s/M | Final CPU s/M | CPU change | Before wall ms | Final wall ms | Wall change |','|---|---:|---:|---:|---:|---:|---:|']
 for op in new['operations']:
  oc=operation(old,op,'cpu_seconds_per_million_ops');nc=operation(new,op,'cpu_seconds_per_million_ops');ow=operation(old,op,'wall_seconds')*1000;nw=operation(new,op,'wall_seconds')*1000
  dc=change(nc,oc);dw=change(nw,ow)
  lines.append(f'| {op} | {oc:.3f} | {nc:.3f} | {dc:+.1f}% | {ow:.2f} | {nw:.2f} | {dw:+.1f}% |')
  if dc>0:regress.append(f'- {c[0]:,} × {c[1]} B / {c[2]}, {op}: CPU median {dc:+.1f}%.')
  if dw>0:regress.append(f'- {c[0]:,} × {c[1]} B / {c[2]}, {op}: elapsed-time median {dw:+.1f}%.')
lines+=['','Negative changes mean improvement. CPU is normalized per million logical operations; elapsed times use the actual recorded operation counts. Raw acknowledged write loops and maintenance phases remain separate in runs.json.','', '## Remaining gaps','']
for c in cases:
 n=find(final,c,'ours');pb=find(comparators,c,'pebble')
 for key,label in [('process_peak_rss_bytes','RSS'),('process_peak_memory_footprint_bytes','physical footprint')]:
  delta=change(median(n,key),median(pb,key))
  if delta>0:lines.append(f'- {c[0]:,} × {c[1]} B / {c[2]}: native peak {label} is {delta:.1f}% above Pebble in these cohorts.')
lines+=regress or ['No positive CPU/elapsed medians versus the unchanged build in this native cohort.']
lines+=['','An across-the-board win is not established. Shared mapped-file residency, private caches, ingestion backpressure and current OS memory pressure affect these comparisons. Configured cache sizes do not imply the same working set or a hard RSS bound.','', '## Implementation and checks','',
'Stable file segments eliminate duplicate growth mappings. Explicitly owned native page/descriptor/memtable storage releases retired buffers without waiting for heap GC; snapshots, scans, cursors and checkpoint I/O retain their own lifetimes. Both cache modes stream compaction; checkpoint vectors/scratch storage are reused and WAL buffers are lazy/reclaimable. Verified clean reads avoid descriptor allocation and read-side CLOCK reference writes. Obsolete file translations are reclaimed without changing file contents or live addresses. New mappings are warmed only after the old file closes, so the two trees need not be resident together.','',
'Checkpoint completion sweeps newly clean descriptors; statistics separately expose resident in-flight I/O pages without double-counting dirty pages. Tests pause I/O to verify ownership/accounting before and after completion. WAL replay pins, fixed flushed replay boundaries and serialized compactions/rotations prevent required generations from being deleted during copying. Bounded ingestion can still apply backpressure when it outruns I/O.','',
'Full macOS, targeted race, Linux, vet and cross-build evidence is retained with exact source-stage scope in validation.json. No release/new tag was published.','', '## Reproduction','',chr(96)*3+'sh','cd bench','go build -o /tmp/apteva-memory-final .','python3 run_memory_comparison.py --binary /tmp/apteva-memory-final --baseline-binary /path/to/unchanged-alpha6-benchmark --engines ours --out /tmp/apteva-memory-final-results','python3 summarize_large_cpu.py /tmp/apteva-memory-final-results','python3 report_memory_final.py /tmp/apteva-memory-final-results --comparators /path/to/completed-four-engine-matrix',chr(96)*3,'']
(root/'REPORT.md').write_text('\n'.join(lines)+'\n');print(root/'REPORT.md')
