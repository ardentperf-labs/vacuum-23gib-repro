#!/usr/bin/env python3
"""Read progress plus the most recent complete guest iostat sample."""
import json,pathlib,subprocess
r=pathlib.Path(__file__).resolve().parent
s=json.loads((r/'state.json').read_text())
code=r'''import json,pathlib
r=pathlib.Path('/home/fioadmin/results')
logs=list(pathlib.Path('/home/fioadmin').glob('*test.log'))
print('\n'.join(max(logs,key=lambda p:p.stat().st_mtime).read_text().splitlines()[-2:]))
samples=list(r.glob('run-*/iostat.json'))
if not samples:
 print('Fixture initialization; no fio stage has started yet')
 raise SystemExit(0)
p=max(samples,key=lambda p:p.stat().st_mtime)
device=pathlib.Path(json.loads((r/'config.json').read_text())['device']).name
for line in reversed(p.read_text().splitlines()):
 if f'"disk_device": "{device}"' in line:
  d=json.loads(line.strip().rstrip(','));print(p.parent.name,{k:d[k] for k in ['r/s','rkB/s','r_await','rareq-sz','aqu-sz','w/s']});break
print('complete', (r/'complete.json').exists())
'''
subprocess.run(['ssh','-i',str(r/'ssh-key'),f"fioadmin@{s['ip']}",'python3 -'],input=code,text=True,check=True)
