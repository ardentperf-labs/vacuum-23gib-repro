#!/usr/bin/env python3
"""Capture continuous PostgreSQL logs without modifying experiment scripts."""
import argparse, datetime as dt, importlib.util, json, pathlib, subprocess, sys, threading, time
p=argparse.ArgumentParser(); p.add_argument('stack',type=pathlib.Path); a=p.parse_args(); root=a.stack.resolve(); sd=root/'.state'
read=lambda f:json.loads(f.read_text()) if f.exists() else {}
while True:
 try:
  s=read(sd/'test-state.json')
  if s.get('phase') in ('preflight','vacuum','replay_drain'): break
  if s.get('phase') in ('complete','failed'): sys.exit(0)
 except Exception: pass
 time.sleep(1)
state=read(sd/'state.json'); kc=state['kubeconfigPath']; out=pathlib.Path(s['output'])/'monitoring'; out.mkdir(parents=True,exist_ok=True)
base=['kubectl','--kubeconfig',kc]; pods=json.loads(subprocess.check_output(base+['get','pods','-n','repro','-l','cnpg.io/cluster=hdd-pg','-o','json'],text=True))['items']
started=dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'); stop=threading.Event(); processes=[]; failures=[]
def follow(role,name):
 with (out/(role+'-postgres-live.log')).open('a') as log, (out/(role+'-follower.err')).open('a') as err:
  while not stop.is_set():
   proc=subprocess.Popen(base+['logs','-n','repro',name,'-c','postgres','--timestamps=true','--follow=true','--since-time='+started],stdout=log,stderr=err); processes.append(proc)
   while proc.poll() is None and not stop.wait(1): pass
   if proc.poll() is None:
    proc.terminate()
    try:proc.wait(timeout=15)
    except subprocess.TimeoutExpired:proc.kill();proc.wait()
   elif not stop.is_set(): failures.append({'role':role,'exit':proc.returncode,'time':dt.datetime.now(dt.timezone.utc).isoformat()})
   stop.wait(3)
threads=[]
for pod in pods:
 role=pod['metadata']['labels'].get('cnpg.io/instanceRole')
 if role in ('primary','replica'):
  th=threading.Thread(target=follow,args=(role,pod['metadata']['name']),daemon=True); th.start(); threads.append(th)
try:
 while True:
  s=read(sd/'test-state.json')
  if s.get('phase') in ('complete','failed'): break
  time.sleep(2)
finally:
 stop.set()
 for th in threads:th.join(timeout=20)
sys.path.insert(0,str(root)); spec=importlib.util.spec_from_file_location('vacuum_runner',root/'03-test.py'); mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
timing=read(out.parent/'timing.json'); events=[]; files={}
for role in ('primary','replica'):
 f=out/(role+'-postgres-live.log')
 if f.exists():
  files[role]={'path':str(f),'bytes':f.stat().st_size}
  events.extend(mod.parse_replication_log(f.read_text(errors='replace'),role))
start=timing.get('vacuumStart'); end=timing.get('replayEnd') or timing.get('observationEnd')
if start and end:
 stamp=lambda x:dt.datetime.fromisoformat(x.replace('Z','+00:00'))
 events=[e for e in events if e.get('time') and stamp(start)<=stamp(e['time'])<=stamp(end)]
summary={'startedAt':started,'finishedAt':dt.datetime.now(dt.timezone.utc).isoformat(),'testState':s,'measurementWindow':{'start':start,'end':end},'logFiles':files,'replicationEvents':{'total':len(events),'events':events},'followerRestarts':failures}
(out/'monitor-summary.json').write_text(json.dumps(summary,indent=2)+'\n'); print(json.dumps(summary))
