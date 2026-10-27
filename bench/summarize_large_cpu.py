#!/usr/bin/env python3
"""Aggregate CPU/wall by logical operation; include engine maintenance cost."""
import argparse,csv,json,pathlib,statistics
p=argparse.ArgumentParser();p.add_argument('root');args=p.parse_args();root=pathlib.Path(args.root)
rows=[];groups={}
for path in root.glob('*/runs.json'):
 for r in json.loads(path.read_text()):
  engine={'alpha6-before-memory':'ours-before','prior-memory-candidate':'ours-prior'}.get(r['profile'],r['engine'])
  groups.setdefault((r['n'],r['value_bytes'],r['workload'],engine),[]).append(r)
operations={
 'Load + maintenance':('insert','settle'),
 'Point read hit':('find-hit',), 'Point read miss':('find-miss',),
 'Full value scan':('scan-values',),'10-reader point hits':('find-hit-par10',),
 'Update + maintenance':('update','settle-update'),
 '80% read / 20% update + maintenance':('mixed-80read-20update','settle-mixed'),
 'Delete + maintenance':('delete','settle-delete'),
 'Scan after delete':('scan-after-delete',),
}
summary=[]
for (n,size,work,engine),trials in sorted(groups.items()):
 entry={'rows':n,'value_bytes':size,'workload':work,'engine':engine,'trials':len(trials),'operations':{}}
 for metric in ('process_peak_rss_bytes','process_peak_memory_footprint_bytes','disk_bytes','fixture_rss_bytes'):
  values=[r[metric] for r in trials];entry[metric]={'median':statistics.median(values),'min':min(values),'max':max(values)}
 for op,names in operations.items():
  samples=[]
  for r in trials:
   phases={x['phase']:x for x in r['phases']};count=phases[names[0]]['ops']
   cpu=sum(phases[name]['user_seconds']+phases[name]['system_seconds'] for name in names)
   wall=sum(phases[name]['wall_seconds'] for name in names)
   samples.append({'repetition':r['repetition'],'ops':count,'cpu_seconds':cpu,'wall_seconds':wall,
    'cpu_seconds_per_million_ops':cpu*1e6/count,'ops_per_second':count/wall,'average_cores':cpu/wall,
    'go_allocated_bytes_per_op':sum(phases[name]['go_allocated_bytes'] for name in names)/count})
  measures={k:{'median':statistics.median(s[k] for s in samples),'min':min(s[k] for s in samples),'max':max(s[k] for s in samples)} for k in samples[0] if k!='repetition'}
  entry['operations'][op]={'samples':samples,'measures':measures}
  rows.append({'dataset_rows':n,'value_bytes':size,'workload':work,'engine':engine,'operation':op,'trials':len(trials),
   'cpu_s_per_million':measures['cpu_seconds_per_million_ops']['median'],'ops_per_second':measures['ops_per_second']['median'],
   'average_cores':measures['average_cores']['median'],'peak_rss_mib':entry['process_peak_rss_bytes']['median']/2**20,
   'peak_footprint_mib':entry['process_peak_memory_footprint_bytes']['median']/2**20})
 summary.append(entry)
(root/'cpu-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
if rows:
 with (root/'cpu-summary.csv').open('w')as f:
  writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
 for r in rows:
  if r['workload']=='random' and r['operation'] in ('Load + maintenance','Point read hit','10-reader point hits','Full value scan','80% read / 20% update + maintenance'):
   print(r['dataset_rows'],r['value_bytes'],r['engine'],r['operation'],round(r['cpu_s_per_million'],3),'CPU s/M',round(r['ops_per_second']),'ops/s',flush=True)
