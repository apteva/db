#!/usr/bin/env python3
import json,pathlib,re,statistics,sys
root=pathlib.Path(sys.argv[1]); result={}
for p in sorted(root.glob('*-count.txt'))+sorted(root.glob('*-guards.txt'))+sorted(root.glob('*-isolated-reads.txt')):
    version=p.name.split('-')[1]
    for line in p.read_text().splitlines():
        if not line.startswith('Benchmark'): continue
        fields=line.split(); name=re.sub(r'-\d+$','',fields[0]); vals={}
        if 'isolated-reads' in p.name: name='Isolated/'+name
        for i in range(2,len(fields)-1,2): vals[fields[i+1]]=float(fields[i])
        result.setdefault(version,{}).setdefault(name,[]).append({'file':p.name,**vals})
report={}
for version,benches in result.items():
    report[version]={}
    for name,samples in benches.items():
        units={u for s in samples for u in s if u!='file'}
        report[version][name]={'samples':samples,'median':{u:statistics.median(s[u] for s in samples if u in s) for u in units}}
(root/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
for version,benches in report.items():
    for name,rec in benches.items(): print(version,name,rec['median'])
