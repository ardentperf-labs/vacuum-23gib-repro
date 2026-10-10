#!/usr/bin/env python3
"""Read-only compact progress from the durable measurement samples."""
from pathlib import Path
import json,datetime
root=Path(__file__).resolve().parent.parent
state=json.loads((root/'run/.state/test-state.json').read_text())
rows=[json.loads(x) for x in (root/'run/results/client/sessions.jsonl').read_text().splitlines() if x.strip()]
good=[x for x in rows if x.get('sampleStatus')=='ok']; b=good[-1]; a=min(good,key=lambda x:abs(x['sampleEpoch']-(b['sampleEpoch']-60)))
result={'nowUtc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'phase':state['phase'],'clientSampleUtc':b['observedAtUtc'],'connections':b['benchSessions'],'committed':b['committedTransactions'],'recentCounterTps':(b['committedTransactions']-a['committedTransactions'])/(b['sampleEpoch']-a['sampleEpoch']) if b['sampleEpoch']>a['sampleEpoch'] else None,'activeClients':b.get('activeSessions'),'syncRepWaits':sum(x.get('waitEvent')=='SyncRep' for x in b.get('backends',[])),'oldestActiveSeconds':b.get('maxActiveXactAgeSeconds'),'samplerErrors':len(rows)-len(good)}
p=root/'run/results/samples.jsonl'
if p.exists():
 samples=[json.loads(x) for x in p.read_text().splitlines() if x.strip()];current=samples[-1];prior=min(samples,key=lambda x:abs(x['primary']['epoch']-(current['primary']['epoch']-60)))
 def lsn(s):a,b=s.split('/');return int(a,16)*2**32+int(b,16)
 result['diskSampleUtc']=current['primary']['ts'];progress=current['primary'].get('vacuumProgress') or {};result['vacuumProgress']=progress.get('phase');result['heapScannedPercent']=100*progress.get('heapBlksScanned',0)/progress['heapBlksTotal'] if progress.get('heapBlksTotal') else None
 result['replayBacklogGiB']=max(0,lsn(current['primary']['flushLsn'])-lsn(current['replica']['replayLsn']))/2**30
 receiver=current['replica'].get('receiver') or {};result['flushBacklogMiB']=max(0,lsn(current['primary']['flushLsn'])-lsn(receiver['flushedLsn']))/2**20 if receiver.get('flushedLsn') else None
 result['recentDataReads']={}
 for role in ('primary','replica'):
  cur,old=current[role],prior[role];dev=cur['devices']['data']['majorMinor'];dc,do=cur['disks'][dev],old['disks'][dev];seconds=cur['epoch']-old['epoch']
  if seconds>0:result['recentDataReads'][role]={'iops':(dc['r']-do['r'])/seconds,'MiBps':(dc['rs']-do['rs'])*512/2**20/seconds}
print(json.dumps(result,indent=2))
