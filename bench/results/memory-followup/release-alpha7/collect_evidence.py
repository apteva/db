from pathlib import Path
import json,subprocess
root=Path(__file__).resolve().parent
def get(args):return subprocess.check_output(args,text=True)
for name,run in [('source-ci',40501997556),('release-ci',40502212771)]:
 data=json.loads(get(['gh','run','view',str(run),'--repo','apteva/db','--json','status,conclusion,headSha,url,jobs']))
 assert data['conclusion']=='success',data
 assert data['headSha']=='4124a0f592168c6109e06d864d7c9535f4486001'
 (root/(name+'.json')).write_text(json.dumps(data,indent=2)+'\n')
 with (root/(name+'.log')).open('w') as f:subprocess.run(['gh','run','view',str(run),'--repo','apteva/db','--log'],stdout=f,check=True)
r=json.loads(get(['gh','api','repos/apteva/db/releases/tags/v0.2.0-alpha.7']))
assert r['tag_name']=='v0.2.0-alpha.7' and r['prerelease'] and not r['draft']
assert len(r['assets'])==5
(root/'release-metadata.json').write_text(json.dumps(r,indent=2)+'\n')
print('Successful source/release CI and public release metadata captured.')
