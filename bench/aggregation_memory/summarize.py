#!/usr/bin/env python3
"""Emit medians and full ranges from run.py's retained results."""
import argparse,csv,json,statistics
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('directory');a=p.parse_args();root=Path(a.directory)
runs=json.loads((root/'runs.json').read_text());rows=[]
for case in sorted({r['case'] for r in runs}):
 cohort=[r for r in runs if r['case']==case]
 for phase in sorted({p['Name'] for r in cohort for p in r['phases']}):
  samples={b:[p for r in cohort if r['build']==b for p in r['phases'] if p['Name']==phase] for b in ['alpha7','current']}
  for metric in ['CPU','Wall','RSS','PeakRSS','Footprint','PeakFootprint','Allocated']:
   vals={b:[p[metric] for p in ps] for b,ps in samples.items()}
   if not all(vals.values()):continue
   med={b:statistics.median(v) for b,v in vals.items()}
   comparable=all(not r['error'] for r in cohort)
   rows.append({'case':case,'phase':phase,'metric':metric,'alpha7_median':med['alpha7'],'alpha7_min':min(vals['alpha7']),'alpha7_max':max(vals['alpha7']),'current_median':med['current'],'current_min':min(vals['current']),'current_max':max(vals['current']),'delta_percent':100*(med['current']/med['alpha7']-1) if med['alpha7'] and comparable else '', 'comparison_note':'successful work' if comparable else 'baseline ErrLimit; unequal completed work', 'n_alpha7':len(vals['alpha7']),'n_current':len(vals['current'])})
with (root/'summary.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
print('| Case | CPU s, alpha.7 → candidate | Wall s | Peak RSS MiB |')
print('|---|---:|---:|---:|')
for case in sorted({r['case'] for r in runs}):
 phase='aggregate' if any(x['case']==case and x['phase']=='aggregate' for x in rows) else 'point-hits'
 def at(metric,ph=phase):return next(x for x in rows if x['case']==case and x['phase']==ph and x['metric']==metric)
 def fmt(x,scale=1):
  text=f"{x['alpha7_median']/scale:.4f} → {x['current_median']/scale:.4f}"
  return text+(f" ({x['delta_percent']:+.1f}%)" if x['delta_percent']!='' else ' (baseline failed)')
 if case=='spill-same-budget':
  print('|',case,'| ErrLimit → completes | ErrLimit → completes |',fmt(at('PeakRSS','close'),2**20),'|');continue
 print('|',case,'|',fmt(at('CPU')),'|',fmt(at('Wall')),'|',fmt(at('PeakRSS','close'),2**20),'|')
print('\nAll phase medians/ranges: summary.csv. Expected baseline budget failure is not successful work.')
