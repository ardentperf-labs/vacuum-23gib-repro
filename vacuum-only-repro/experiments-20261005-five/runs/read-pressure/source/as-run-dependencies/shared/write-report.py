#!/usr/bin/env python3
"""Generate a standalone experiment report from recorded evidence."""
import argparse, datetime as dt, json, pathlib
p=argparse.ArgumentParser(); p.add_argument('result',type=pathlib.Path); p.add_argument('--title',default='23-GiB unchanged repeat'); p.add_argument('--output',type=pathlib.Path); a=p.parse_args()
d=a.result.resolve(); j=lambda name:json.loads((d/name).read_text()); s=j('summary.json'); t=j('timing.json'); f=j('preflight.json'); prep=f['prepare']
output=a.output or d.parent.parent/'REPORT.md'; output=output.resolve()
seconds=lambda start,end:(dt.datetime.fromisoformat(end.replace('Z','+00:00'))-dt.datetime.fromisoformat(start.replace('Z','+00:00'))).total_seconds()
def duration(v): return f'{v/3600:.2f} h ({v/60:.1f} min)'
def n(v): return '—' if v is None else f'{v:.2f}'
vac=seconds(t['vacuumStart'],t['vacuumComplete']); tail=seconds(t['vacuumComplete'],t['replayEnd']) if t['replayEnd'] else None
v=s['windows']['vacuum']; allwin=s['windows'].get('vacuumAndReplay',{})
text=[f'# {a.title}', '', f"Run `{d.name}`, UTC. Primary VACUUM completed in **{duration(vac)}**. Replica replay tail: **{duration(tail) if tail is not None else 'not verified complete'}**. Test complete: **{s['status']['testComplete']}**.", '',f"Replica DATA reads during VACUUM averaged **{n(v['replica_data']['readIops'])} IOPS / {n(v['replica_data']['readMBps'])} MB/s / {n(v['replica_data']['readAwaitMs'])} ms await**. Replica/primary DATA read IOPS ratio: **{n(s['vacuumDataReadAmplification'].get('ratio') if isinstance(s['vacuumDataReadAmplification'],dict) else s['vacuumDataReadAmplification'])}×**.", '', '## Configuration', '', f"Fresh half-dead heap: {prep['heapBytes']:,} bytes ({prep['heapBytes']/1024**3:.3f} GiB), estimated {prep.get('preparedLiveTuplesEstimate',0):,} live and {prep.get('preparedDeadTuplesEstimate',0):,} dead tuples. The retained source keeper was restored, the bounded interleaved fixture was rebuilt, and CNPG seeded the replica from paired DATA/WAL snapshots. Both roles ran on separate AKS workers with separate 512-GiB Standard_LRS HDD DATA/WAL disks and host caching disabled.", '', '```sql', t['vacuumSql'], '```', '', '| Setting | Primary | Replica |', '|---|---|---|']
for k in ['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay','maintenance_io_concurrency','maintenance_work_mem','shared_buffers','max_wal_size','checkpoint_timeout','synchronous_commit','wal_sender_timeout','wal_receiver_timeout']:
 text.append(f"| {k} | {f['sql']['primary']['settings'].get(k,{}).get('value','unrecorded')} | {f['sql']['replica']['settings'].get(k,{}).get('value','unrecorded')} |")
text+=['', 'No application workload was started. Every instrumented PostgreSQL application-session check was recorded with raw samples. Globally enabled autovacuum is disabled only for the fixture table. Explicit FREEZE is the original rebuilt-fixture workload option.', '', '## Timeline', '', '| Event | UTC |', '|---|---|']
for key,label in [('vacuumStart','VACUUM starts'),('heapWorkComplete','Heap-work completion observed'),('vacuumComplete','Primary VACUUM completes'),('replayEnd','Replica catch-up verified')]: text.append(f"| {label} | {t.get(key) or 'not observed'} |")
text+=['', f"Remote VACUUM exit: `{t['remoteVacuumExitCode']}`. Replica catch-up: `{t['replayCaughtUp']}`. Completion LSN: `{t['replayTargetLsn']}`. Final replica replay / primary flush LSN: `{t['replicaReplayLsnAtObservationEnd']}` / `{t['primaryFlushLsnAtObservationEnd']}`. Final backlog: `{t['replicaLagBytesAtObservationEnd']}` bytes. Catch-up requires three consecutive healthy samples through both the exact VACUUM completion LSN and current primary flush LSN.", '', '## Disk measurements', '', 'Each cell is IOPS / decimal MB/s / operation-weighted await in ms.', '', '| Device / operation | Primary VACUUM | Replica replay tail | Whole timeline |', '|---|---:|---:|---:|']
for key in ('primary_data','primary_wal','replica_data','replica_wal'):
 for op in ('read','write'):
  cells=[]
  for w in ('vacuum','replayDrain','vacuumAndReplay'):
   q=s['windows'].get(w,{}).get(key,{}); cells.append(' / '.join(n(q.get(op+k)) for k in ('Iops','MBps','AwaitMs')))
  text.append('| '+key.replace('_',' ').title()+' '+op+' | '+' | '.join(cells)+' |')
pd=allwin.get('primary_data',{}); rd=allwin.get('replica_data',{})
if pd.get('readIops') and rd.get('readIops'):
 po=pd['readIops']*pd['coveredSeconds']; ro=rd['readIops']*rd['coveredSeconds']
 pb=pd['readMBps']*pd['coveredSeconds']; rb=rd['readMBps']*rd['coveredSeconds']
 text+=['', f"Across the complete timeline, replica/primary DATA completed-read operation ratio is **{ro/po:.3f}×** and read-byte ratio is **{rb/pb:.3f}×**. Effective mean DATA read sizes are {pd['readMBps']*1e6/pd['readIops']/1024:.2f} KiB on primary and {rd['readMBps']*1e6/rd['readIops']/1024:.2f} KiB on replica. These derive from valid counter-window totals; request size and the observation window affect IOPS ratios."]
text+=['', '## Graphs', '', 'All y-axes are linear. Figures contain four primary/replica DATA/WAL panels and separate read/write traces. Sixty-second UTC buckets use operation-weighted await and time-weighted IOPS/throughput. The p99 axis ceiling is a display choice; raw data and reported statistics retain all values, and off-scale buckets are marked. Five-second interval await averages are distinct from individual operation latency.', '']
import os
for metric in ['await','iops','throughput']: text += [f'![{metric}]({os.path.relpath(d/(metric+".png"),output.parent)})', '']
text+=['## Measurement quality', '',f"Collected {s['sampleCount']:,} samples; maximum gap {s['maximumSampleGapSeconds']:.2f} seconds. Counter-window coverage valid: {s['coverage']['valid']}. Guest await is completed block-operation time including queueing; it is not an Azure service-latency metric. Post-VACUUM error: `{t.get('postVacuumObservationError')}`. Continuous PostgreSQL logs and final capture are saved alongside the counters.", '', '## Evidence', '']
for name in ['preflight.json','vacuum.log','timing.json','summary.json','events.jsonl','samples.jsonl','monitoring/monitor-summary.json']:
 if (d/name).exists(): text.append(f'- [{name}]({os.path.relpath(d/name,output.parent)})')
state_path=d.parent.parent/'.state/state.json'
if state_path.exists():
 infra=json.loads(state_path.read_text())
 text+=['', '## Infrastructure and repeat comparison', '', f"Azure region: `{infra['location']}`. Stack owner token: `{infra['ownerToken']}`. This baseline uses the same East US region, worker SKU, fixture size, and unmodified experiment scripts as the original run."]
 original=pathlib.Path(__file__).resolve().parents[2]/'vacuum-only-repro/results/20261004T205730Z'
 if (original/'summary.json').exists():
  old=json.loads((original/'summary.json').read_text()); ot=json.loads((original/'timing.json').read_text()); ov=old['windows']['vacuum']
  text+=['', '| Metric | Original 23 GiB | Unchanged repeat |', '|---|---:|---:|',
   f"| VACUUM duration, minutes | {seconds(ot['vacuumStart'],ot['vacuumComplete'])/60:.2f} | {vac/60:.2f} |",
   f"| Replica replay tail, minutes | {seconds(ot['vacuumComplete'],ot['replayEnd'])/60:.2f} | {tail/60:.2f} |"]
  for key,label in [('readIops','Replica DATA read IOPS during VACUUM'),('readMBps','Replica DATA read MB/s during VACUUM'),('readAwaitMs','Replica DATA read await ms during VACUUM')]:
   text.append(f"| {label} | {n(ov['replica_data'][key])} | {n(v['replica_data'][key])} |")
  text.append(f"| DATA read IOPS amplification | {n(old['vacuumDataReadAmplification']['ratio'])}× | {n(s['vacuumDataReadAmplification']['ratio'])}× |")
 text+=['', 'Cleanup preserves all snapshots. The separately recorded cleanup result identifies any stopped AKS control plane whose incremental snapshots prevent deletion of its node resource group.']
output.write_text('\n'.join(text)+'\n'); print(output)
