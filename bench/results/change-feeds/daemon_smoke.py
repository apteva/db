import os,sys,subprocess,socket,time,json,urllib.request,hashlib,signal
from pathlib import Path
root=Path(__file__).resolve().parent
token='local-native-feed-smoke-fixture'
s=socket.socket();s.bind(('127.0.0.1',0));port=s.getsockname()[1];s.close();url=f'http://127.0.0.1:{port}'
def request(path,body=None):
 return urllib.request.urlopen(urllib.request.Request(url+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'}),timeout=10)
with (root/'daemon-smoke-server.txt').open('w') as log:
 p=subprocess.Popen([str(root/'aptevadb-server'),'-dir',str(root/'daemon-smoke-db'),'-listen',f'127.0.0.1:{port}','-change-feed'],env={**os.environ,'APTEVADB_TOKEN':token},stdout=log,stderr=log)
 streams=[]
 try:
  for i in range(100):
   try:
    with request('/v1/health') as h: health=json.load(h)
    break
   except Exception:time.sleep(.05)
  else:raise RuntimeError('startup failed')
  first=request('/v1/watch',{'op':'watch','watch':{}});streams.append(first);ready=json.loads(first.readline());assert ready['watch_ready']
  with request('/v1/rpc',{'op':'upsert','key':'YQ==','value':'cmF3'}) as h:assert not json.load(h).get('error')
  event=json.loads(first.readline())['change'];assert event['operation']=='put' and event['key']=='YQ==' and event['end_of_commit'];saved=event['commit_token'];first.close()
  replay=request('/v1/watch',{'op':'watch','watch':{'after':saved}});streams.append(replay);assert json.loads(replay.readline())['watch_ready']
  with request('/v1/rpc',{'op':'upsert','key':'Yg==','value':'cmF3Mg=='}) as h:assert not json.load(h).get('error')
  event=json.loads(replay.readline())['change'];assert event['key']=='Yg==' and event['end_of_commit']
  start=time.monotonic();p.send_signal(signal.SIGTERM);code=p.wait(timeout=10);elapsed=time.monotonic()-start;assert code==0;assert replay.read()==b''
  print(json.dumps({'health':health,'watch_replay':True,'idle_watch_shutdown_seconds':elapsed,'exit_code':code}))
 finally:
  for h in streams:h.close()
  if p.poll() is None:p.kill();p.wait()
old=Path('/Users/marcoschwartz/Documents/aptevadb-benchmarks/2026-10-13-alpha4-aggregation/public-bin/aptevadbctl')
def hashes():return {str(x.relative_to(root/'daemon-smoke-db')):hashlib.sha256(x.read_bytes()).hexdigest() for x in (root/'daemon-smoke-db').glob('*') if x.is_file() and x.name!='LOCK'}
before=hashes();probe=subprocess.run([str(old),'verify',str(root/'daemon-smoke-db')],capture_output=True,text=True);assert probe.returncode!=0 and before==hashes()
print('OLD BINARY REFUSAL',probe.returncode,probe.stdout.strip(),probe.stderr.strip(),'data/WAL hashes unchanged')
subprocess.run([str(root/'aptevadbctl'),'verify',str(root/'daemon-smoke-db')],check=True)
