import json,glob,re,statistics,math
from pathlib import Path
ROOT=Path(__file__).resolve().parent;O=ROOT/'verified';runs={r['name']:r for r in json.loads((O/'runs.json').read_text())}
def median(x):return statistics.median(x)
def pctile(x,p):
 a=sorted(x);pos=(len(a)-1)*p;lo=int(pos);hi=min(lo+1,len(a)-1);return a[lo]+(a[hi]-a[lo])*(pos-lo)
def probe(path):return json.loads(next(l[11:] for l in path.read_text().splitlines() if l.startswith('FEED_PROBE ')))
summary={'feeds':{},'guards':{},'kv':{},'resources':{}}
for size in [1000,1]:
 for mode in ['Off','Capture','Metadata1','Metadata10','Metadata100','Filtered100','Tenants100','Values10','Projection100','Slow100']:
  paths=list(O.glob(f'feed-{mode}-batch{size}-*.txt'))
  if not paths:continue
  vals=[probe(p) for p in paths]; recs=[runs[p.stem] for p in paths];lat=[v for x in vals for v in x['commit_to_last_subscriber_ns']]
  summary['feeds'][mode+'-'+str(size)]={'rows_s':median([x['rows_per_second'] for x in vals]),'rows_s_range':[min(x['rows_per_second'] for x in vals),max(x['rows_per_second'] for x in vals)],'writer_ns_row':median([x['write_ns_per_row'] for x in vals]),'cpu_process_ns_row':median([(r['user_seconds']+r['system_seconds'])*1e9/x['rows'] for r,x in zip(recs,vals)]),'peak_rss_mb':median([r['peak_rss_bytes']/2**20 for r in recs]),'peak_reader_bytes':max(x['feed_stats']['PeakReaderBytes'] for x in vals),'plan_bytes':median([x['feed_stats']['PlanBytes'] for x in vals]),'alloc_bytes_row':median([x['allocated_bytes']/x['rows'] for x in vals]),'allocs_row':median([x['allocations']/x['rows'] for x in vals]),'p50_batch_ms':pctile(lat,.5)/1e6,'p99_batch_ms':pctile(lat,.99)/1e6,'latency_samples':len(lat)}
for op in ['Upsert','Find','Scan']:
 d={}
 for version in ['alpha4','current']:
  vals=[];allocs=[]
  for p in O.glob(f'guard-{version}-*.txt'):
   m=re.search('/'+op+r'-10\s+\d+\s+([\d.]+) ns/op(.*)',p.read_text())
   vals.append(float(m[1]));a=re.search(r'(\d+) B/op\s+(\d+) allocs/op',m[2]);allocs.append([int(a[1]),int(a[2])])
  d[version]={'median_ns':median(vals),'range_ns':[min(vals),max(vals)],'bytes_op':median([x[0] for x in allocs]),'allocs_op':median([x[1] for x in allocs])}
 d['change_pct']=(d['current']['median_ns']/d['alpha4']['median_ns']-1)*100;summary['guards'][op]=d
for p in O.glob('kv-*.txt'):
 version=p.stem.split('-')[1]
 for wl,phase,ns in re.findall(r'^ours\s+(random|seq)\s+(\S+)\s+(\d+) ns/op',p.read_text(),re.M):summary['kv'].setdefault(wl+'/'+phase,{}).setdefault(version,[]).append(int(ns))
for k,vers in summary['kv'].items():
 for v,vals in vers.items():vers[v]={'median_ns':median(vals),'range_ns':[min(vals),max(vals)]}
 vers['change_pct']=(vers['current']['median_ns']/vers['alpha4']['median_ns']-1)*100
for engine in ['ours','pebble','sqlite-c','sqlite-go']:
 basedir=O if engine=='ours' else ROOT
 vals=[json.loads(next(l for l in p.read_text().splitlines() if l.startswith('{'))) for p in basedir.glob(f'resources-{engine}-*.txt')]
 d={'source':json.loads((basedir/'metadata.json').read_text())['runtime_commit'],'peak_rss_mb':median([x['process_peak_rss_bytes']/2**20 for x in vals]),'peak_footprint_mb':median([x['process_peak_memory_footprint_bytes']/2**20 for x in vals]),'phases':{}}
 for phase in ['insert','settle','find-hit','find-miss','scan','find-hit-par10']:
  x=[next(p for p in v['phases'] if p['phase']==phase) for v in vals]
  d['phases'][phase]={'wall_ns_op':median([p['wall_ns_per_op'] for p in x]),'cpu_ns_op':median([p['cpu_ns_per_op'] for p in x]),'rss_mb':median([p['rss_bytes']/2**20 for p in x])}
 summary['resources'][engine]=d
(O/'summary.json').write_text(json.dumps(summary,indent=2));print(json.dumps(summary,indent=2))
