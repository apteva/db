from pathlib import Path
import json,os,re,secrets,signal,subprocess,tempfile,time,urllib.request

ROOT=Path(__file__).resolve().parent
binary=ROOT.parent/'release-download/native-archive/aptevadb-server'
env={**os.environ,'APTEVADB_TOKEN':secrets.token_hex(32),'GOWORK':'off'}
with (ROOT/'daemon.log').open('w') as log:
 process=subprocess.Popen([str(binary),'-dir',tempfile.mkdtemp(prefix='daemon-data-',dir=ROOT),'-listen','127.0.0.1:0'],env=env,stdout=log,stderr=log)
 try:
  url=None
  for _ in range(100):
   if process.poll() is not None:raise RuntimeError('server exited early')
   sockets=subprocess.run(['lsof','-nP','-a','-p',str(process.pid),'-iTCP','-sTCP:LISTEN','-Fn'],capture_output=True,text=True)
   match=re.search(r'^n(127\.0\.0\.1:\d+)$',sockets.stdout,re.M)
   if match:url='http://'+match.group(1);break
   time.sleep(.05)
  if url is None:raise RuntimeError('listener unavailable')
  request=urllib.request.Request(url+'/v1/health',headers={'Authorization':'Bearer '+env['APTEVADB_TOKEN']})
  with urllib.request.urlopen(request,timeout=10) as response:health=json.load(response)
  assert health['version']=='0.2.0-alpha.3' and health['format']==8,health
  env['SMOKE_URL']=url
  run=subprocess.run(['go','run','.','remote'],cwd=ROOT,env=env,capture_output=True,text=True,timeout=120)
  print(run.stdout,end='')
  if run.returncode:raise RuntimeError(run.stderr)
 finally:
  process.send_signal(signal.SIGTERM)
  code=process.wait(timeout=45)
  assert code==0,code
print('downloaded daemon health/version and graceful shutdown PASS')
