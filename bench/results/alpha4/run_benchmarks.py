import subprocess,pathlib,json,time,platform,hashlib,tempfile
out=pathlib.Path(__file__).parent
runs=[]
commands=[]
for trial in range(1,4):
 for version in (['alpha3','alpha4'] if trial%2 else ['alpha4','alpha3']):
  commands.append((f"numerics-{version}-{trial}",[str(out/(version+'-numerics.test')),'-test.run=^$','-test.bench=^BenchmarkAggregationNumerics$/^(Integers|Floats|RowCount)$/','-test.benchtime=500ms','-test.count=1']))
  commands.append((f"agg-{version}-{trial}",[str(out/(version+'-bench')),'-agg','-n','100000','-engines','ours,ours-par,pebble,sqlite-c','-reps','5']))
  commands.append((f"kv-{version}-{trial}",[str(out/(version+'-bench')),'-n','1000000','-vsize','100','-engines','ours','-par','10','-reps','1']))
commands.append(('new-feature',[str(out/'alpha4-numerics.test'),'-test.run=^$','-test.bench=^BenchmarkAggregationNumerics$/^(WideIntegers|NonNullCount)$/','-test.benchtime=1s','-test.count=3']))
for mode,n,eng in [('join',50000,'ours,ours-par,sqlite-c'),('computedcollections',20000,'ours,pebble,sqlite-c'),('computed',50000,'ours,pebble,sqlite-c'),('compat',50000,'ours,pebble,sqlite-c,sqlite-go'),('wideindex',10000,'ours,pebble,sqlite-c,sqlite-go')]:
 commands.append((mode,[str(out/'alpha4-bench'),'-'+mode,'-n',str(n),'-engines',eng,'-reps','3']))
meta={'platform':platform.platform(),'hardware':subprocess.check_output(['sysctl','-n','machdep.cpu.brand_string','hw.ncpu','hw.memsize'],text=True),'go':subprocess.check_output(['go','version'],text=True),'source':(out/'source.txt').read_text().strip(),'baseline':json.loads((out/'baseline-source.json').read_text())['baseline']}
(out/'metadata.json').write_text(json.dumps(meta,indent=2))
for name,cmd in commands:
 print('RUN',name,flush=True)
 record={'name':name,'command':cmd,'start':time.time(),'load_before':subprocess.check_output(['uptime'],text=True).strip()}
 with tempfile.TemporaryDirectory(prefix='apteva-alpha4-bench-') as tmp:
  if not name.startswith('numerics-') and name!='new-feature': cmd=cmd+['-dir',tmp]; record['command']=cmd
  result=subprocess.run(cmd,capture_output=True,text=True)
 record.update({'seconds':time.time()-record['start'],'exit':result.returncode,'load_after':subprocess.check_output(['uptime'],text=True).strip()})
 (out/(name+'.txt')).write_text(result.stdout+result.stderr)
 runs.append(record); (out/'runs.json').write_text(json.dumps(runs,indent=2))
 if result.returncode: raise SystemExit('FAIL '+name+' '+result.stdout+result.stderr)
print('All benchmarks passed',flush=True)
