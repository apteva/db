from pathlib import Path
import subprocess,json,hashlib,tarfile,platform
out=Path(__file__).parent;dl=out/'release-download';dl.mkdir(exist_ok=True)
subprocess.run(['gh','release','download','v0.2.0-alpha.4','--repo','apteva/db','--dir',str(dl),'--pattern','*.tar.gz','--pattern','SHA256SUMS'],check=True)
records=[]
for line in (dl/'SHA256SUMS').read_text().splitlines():
 expected,name=line.split();name=name.lstrip('*');path=dl/name
 actual=hashlib.sha256(path.read_bytes()).hexdigest();assert actual==expected,(name,expected,actual);records.append({'archive':name,'sha256':actual})
assert len(records)==4,records
native=dl/'native-archive';native.mkdir(exist_ok=True)
with tarfile.open(dl/'aptevadb_0.2.0-alpha.4_darwin_arm64.tar.gz') as t:t.extractall(native,filter='data')
outputs=[]
for name,flag in [('aptevadbctl','version'),('aptevadb-server','-version')]:
 p=subprocess.run([str(native/name),flag],capture_output=True,text=True,check=True);assert '0.2.0-alpha.4' in p.stdout and 'format 9' in p.stdout,p.stdout;outputs.append(p.stdout)
p=subprocess.run([str(native/'aptevadbctl'),'verify',str(out/'public-smoke-db')],capture_output=True,text=True,check=True);outputs.append(p.stdout)
p=subprocess.run(['python3',str(out/'daemon_smoke.py')],capture_output=True,text=True,check=True);outputs.append(p.stdout+p.stderr)
(out/'release-archive-smoke.txt').write_text(''.join(outputs));(out/'release-checksums.json').write_text(json.dumps(records,indent=2));print(''.join(outputs));print('All four archive SHA256 hashes verified')
