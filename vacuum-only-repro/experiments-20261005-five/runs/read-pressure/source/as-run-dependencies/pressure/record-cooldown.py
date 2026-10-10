#!/usr/bin/env python3
"""Enforce a five-minute no-reader cooldown for the fresh final-run DATA fixture."""
from __future__ import annotations
import datetime as dt
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import sys
import time

HERE=Path(__file__).resolve().parents[1]; STATE=HERE/'.state'; DATA='/var/lib/postgresql/data'; PGDATA=DATA+'/pgdata'
REQUIRED=300; INTERVAL=30; NAMES=['pressure','pressure.1','pressure.2','pressure.3']; FILE_BYTES=1024**3
spec=importlib.util.spec_from_file_location('final_read_pressure_cooldown',HERE/'read_pressure.py')
rp=importlib.util.module_from_spec(spec); assert spec and spec.loader; spec.loader.exec_module(rp)

def stamp(): return dt.datetime.now(dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
def parse_stamp(s): return dt.datetime.fromisoformat(s.replace('Z','+00:00'))
def save(path,obj):
 path.parent.mkdir(mode=0o700,parents=True,exist_ok=True); tmp=path.with_suffix(path.suffix+'.tmp'); tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+'\n'); os.chmod(tmp,0o600); os.replace(tmp,path)
def require(ok,msg):
 if not ok: raise RuntimeError(msg)

REMOTE_FIXTURE=r'''set -eu
token=$1; mount=$2; pgdata=$3; auxname=$4
case "$token" in ''|*[!0-9a-f]*) exit 81 ;; esac
[ "${#token}" -eq 32 ] && [ "$mount" = /var/lib/postgresql/data ] && [ "$pgdata" = "$mount/pgdata" ]
[ "$auxname" = ".repro-read-pressure-aux-$token" ]
real_mount=$(realpath -e "$mount"); real_pgdata=$(realpath -e "$pgdata"); dir="$mount/$auxname"
[ "$real_mount" = "$mount" ] && [ "$real_pgdata" = "$pgdata" ]
[ -d "$dir" ] && [ ! -L "$dir" ] && [ "$(realpath -e "$dir")" = "$dir" ]
case "$dir/" in "$real_pgdata/"*) exit 82 ;; esac
[ -f "$dir/.owner.json" ] && [ ! -L "$dir/.owner.json" ]
mount_dev=$(stat -c %d "$mount"); pgdata_dev=$(stat -c %d "$pgdata"); aux_dev=$(stat -c %d "$dir")
[ "$mount_dev" = "$pgdata_dev" ] && [ "$mount_dev" = "$aux_dev" ]
printf 'mountStDev|%s\n' "$mount_dev"
printf 'fixtureManifestBegin\n'; cat "$dir/.owner.json"; printf '\nfixtureManifestEnd\n'
printf 'fixtureInventoryBegin\n'
find "$dir" -mindepth 1 -maxdepth 1 -printf '%f|%y|%s|%b|%i|%D\n' | sort -V
printf 'fixtureInventoryEnd\n'
printf 'filesystem|%s\n' "$(df -B1 "$mount" | tail -1)"
printf 'meminfo|%s\n' "$(grep -E '^(MemTotal|MemFree|Cached|Dirty|Writeback):' /proc/meminfo | tr '\n' ';')"
printf 'ioPressure|%s\n' "$(tr '\n' ';' </proc/pressure/io)"
'''

def read_fixture(base,ns,pod,token,manifest):
 out=rp.run(base+['exec','-i','-c','postgres','-n',ns,pod,'--','sh','-s','--',token,DATA,PGDATA,f'.repro-read-pressure-aux-{token}'],input=REMOTE_FIXTURE,retryable=False)
 chunk=out.split('fixtureManifestBegin\n',1)
 require(len(chunk)==2 and '\nfixtureManifestEnd\n' in chunk[1],'remote owner manifest missing')
 remote_manifest=json.loads(chunk[1].split('\nfixtureManifestEnd\n',1)[0])
 require(remote_manifest==manifest,'remote fixture manifest differs from local completion artifact')
 chunk=out.split('fixtureInventoryBegin\n',1)
 require(len(chunk)==2 and '\nfixtureInventoryEnd\n' in chunk[1],'remote fixture inventory missing')
 expected=set(NAMES+['.owner.json']); observed={}
 for line in chunk[1].split('\nfixtureInventoryEnd\n',1)[0].splitlines():
  f=line.split('|'); require(len(f)==6,f'malformed fixture row: {line!r}')
  name,kind,size,blocks,inode,dev=f; require(name in expected and name not in observed,'unexpected/duplicate fixture file')
  require(kind=='f','fixture contains a non-regular entry')
  size,blocks,inode,dev=map(int,(size,blocks,inode,dev)); allocated=blocks*512
  if name=='.owner.json': require(size<=65536 and dev==manifest['dataMountStDev'],'owner marker size/device invalid')
  else: require(size==FILE_BYTES and allocated>=size and dev==manifest['dataMountStDev'],'auxiliary data file size/allocation/device mismatch')
  observed[name]={'bytes':size,'allocatedBytes':allocated,'inode':inode,'stDev':dev}
 require(set(observed)==expected,'auxiliary fixture has a missing/unexpected entry')
 recorded={x['name']:{'bytes':x['bytes'],'allocatedBytes':x['allocatedBytes'],'inode':x['inode'],'stDev':x['stDev']} for x in manifest['generatedFiles']}
 require({k:v for k,v in observed.items() if k!='.owner.json'}==recorded,'live auxiliary file stats differ from generation manifest')
 require(sum(observed[n]['bytes'] for n in NAMES)==4*FILE_BYTES,'live fixture total is not four GiB')
 require(observed['.owner.json']['stDev']==observed['pressure']['stDev']==int(out.split('mountStDev|',1)[1].splitlines()[0]),'mount, marker, and data files are not on the same filesystem')
 return {'raw':out,'files':observed}

COMMON_PROGRESS_FIELDS=("'inRecovery',pg_is_in_recovery(),"
 "'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'),"
 "'walLevel',current_setting('wal_level'),'fullPageWrites',current_setting('full_page_writes'),"
 "'recoveryPrefetch',current_setting('recovery_prefetch'),'vacuumCostDelay',current_setting('vacuum_cost_delay')")
def guc_query(): return f"SELECT json_build_object({COMMON_PROGRESS_FIELDS})::text;"
def progress_query(recovery=False):
 lsn_sql=("'receiveLsn',pg_last_wal_receive_lsn(),'replayLsn',pg_last_wal_replay_lsn()" if recovery else
          "'flush',pg_current_wal_flush_lsn(),'senders',(SELECT json_agg(json_build_object('state',state,'sent',sent_lsn,'flush',flush_lsn,'replay',replay_lsn)) FROM pg_stat_replication)")
 return f"SELECT json_build_object('sampledAt',clock_timestamp(),{lsn_sql},{COMMON_PROGRESS_FIELDS})::text;"
def progress(base,ns,pod,db,recovery=False):
 return rp.test.sql(base,ns,pod,db,progress_query(recovery),retryable=True)
def lsn(s): return rp.test.lsn(s) if s else None
def replication_check(primary,replica):
 senders=primary.get('senders') or []
 senders_ok=(len(senders)==1 and senders[0].get('state')=='streaming' and
             senders[0].get('sent')==primary.get('flush') and senders[0].get('flush')==primary.get('flush') and
             senders[0].get('replay')==primary.get('flush') and replica.get('receiveLsn')==primary.get('flush') and
             replica.get('replayLsn')==primary.get('flush'))
 return bool(senders_ok)

def main():
 if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]: print(__doc__); return 0
 infra=json.loads((STATE/'state.json').read_text()); prep=json.loads((STATE/'prepare.json').read_text()); pointer=json.loads((STATE/'auxiliary-random-fill-final.json').read_text())
 token=infra.get('ownerToken'); require(infra.get('status')=='ready' and prep.get('status')=='complete' and pointer.get('status')=='complete' and pointer.get('ownerToken')==token,'stack/preparation ownership state is not complete')
 artifact=Path(pointer['manifestArtifact']).resolve(); require(artifact.is_file(),'generated fixture manifest artifact missing')
 manifest=json.loads(artifact.read_text()); generated=parse_stamp(manifest['generationCompletedAtHost'])
 require(manifest.get('kind')=='read-pressure-token-owned-generated-fixture' and manifest.get('copyStatus')=='complete' and manifest.get('ownerToken')==token and manifest.get('dataClaim')=='hdd-pg-2' and str(manifest.get('dataLun'))=='0' and manifest.get('totalBytes')==4*FILE_BYTES and manifest.get('fileNames')==NAMES and manifest.get('destinationRelativeDirectory')==f'.repro-read-pressure-aux-{token}','generated fixture manifest has wrong identity or geometry')
 expected_backup=json.loads((STATE/'reset-audit/snapshot-seed-verification.json').read_text())
 require(manifest.get('freshSeedBackupName')==expected_backup.get('backupName'),'fixture is not tied to the latest fresh paired snapshot seed')
 outdir=HERE/'results'/('final-auxiliary-cooldown-'+dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')); outdir.mkdir(mode=0o700,parents=True,exist_ok=False)
 rp.configure_retry_log(outdir/'read-only-retries.jsonl')
 cfg=Path(infra.get('kubeconfigPath',STATE/'kubeconfig')).resolve(); require(cfg.is_file(),'owned kubeconfig missing')
 base=['kubectl','--kubeconfig',str(cfg)]; ns,cluster=prep.get('cluster','repro/hdd-pg').split('/',1)
 lock=(STATE/'test.lock').open('a')
 try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError as e: lock.close(); raise RuntimeError('another calibration/test/teardown holds the stack lock') from e
 try:
  nsobj=json.loads(rp.run(base+['get','namespace',ns,'-o','json'],retryable=True)); require(nsobj.get('metadata',{}).get('labels',{}).get('repro.stack.token')==token,'namespace owner token mismatch')
  pods=rp.find_pods(base,ns,cluster); primary=pods['primary']; replica=pods['replica']
  require(primary['metadata']['name']=='hdd-pg-1' and replica['metadata']['name']=='hdd-pg-2','expected primary/replica identities missing')
  require(all(p.get('status',{}).get('phase')=='Running' and any(c.get('type')=='Ready' and c.get('status')=='True' for c in p.get('status',{}).get('conditions',[])) for p in (primary,replica)),'PostgreSQL pods are not both Ready')
  layout=rp.test.storage(base,ns,pods); data=layout['roles']['replica']['data']
  require(data.get('claim')=='hdd-pg-2' and data.get('pv')==manifest.get('dataPv') and data.get('volumeHandle')==manifest.get('azureDiskId') and data.get('node')==manifest.get('replicaNode') and str(data.get('lun'))=='0' and data.get('path')==DATA,'standby DATA PVC/node/LUN differs from generated fixture')
  qbefore=rp.test.sample(base,ns,'repro','final-cooldown',replica,layout['roles']['replica'])
  data_dev=qbefore['devices']['data']; queue=data_dev['queue']
  require(data_dev.get('majorMinor')==manifest.get('dataMajorMinor') and queue.get('nrRequests')==316 and queue.get('scsiDeviceQueueDepth')==316 and '[none]' in queue.get('scheduler',''),'standby DATA device/queue is not the restored default')
  reader_selector=['get','pods','-n',ns,'-l',f'repro.stack.token={token},repro.experiment=read-pressure','-o','json']
  readers=json.loads(rp.run(base+reader_selector,retryable=True))['items']; require(not readers,'token-owned reader pods exist at cooldown start')
  fixture=read_fixture(base,ns,replica['metadata']['name'],token,manifest)
  guc_sql=guc_query()
  defaults={}
  for name,pod in [('primary',primary),('replica',replica)]:
   row=rp.test.sql(base,ns,pod['metadata']['name'],'repro',guc_sql,retryable=True)
   require(row.get('inRecovery')==(name=='replica') and row.get('appSessions')==0 and row.get('walLevel')=='logical' and row.get('fullPageWrites')=='on' and row.get('recoveryPrefetch')=='try' and row.get('vacuumCostDelay')=='0',f'{name} app-session/default GUC check failed: {row}')
   defaults[name]=row
  p0=progress(base,ns,primary['metadata']['name'],'repro'); r0=progress(base,ns,replica['metadata']['name'],'repro',True)
  require(replication_check(p0,r0),f'standby is not caught up at cooldown start: primary={p0}, replica={r0}')
  started=stamp(); start_mono=time.monotonic(); points=[]; disk_rows=[]; reader_checks=0
  def record_point():
   pp=progress(base,ns,primary['metadata']['name'],'repro'); rr=progress(base,ns,replica['metadata']['name'],'repro',True)
   disk=rp.test.sample(base,ns,'repro','final-cooldown',replica,layout['roles']['replica'])
   require(disk.get('appSessions')==0 and disk['devices']['data'].get('pv')==data['pv'] and disk['devices']['data'].get('majorMinor')==manifest['dataMajorMinor'] and disk['devices']['data']['queue']==queue,'DATA disk mapping/default queue changed during cooldown')
   for role,row,recovery in [('primary',pp,False),('replica',rr,True)]:
    require(row.get('inRecovery')==recovery and row.get('appSessions')==0 and row.get('walLevel')=='logical' and row.get('fullPageWrites')=='on' and row.get('recoveryPrefetch')=='try' and row.get('vacuumCostDelay')=='0',f'{role} session/default GUC changed during cooldown: {row}')
   pt={'capturedAtHost':stamp(),'primary':pp,'replica':rr,'diskSample':disk,'caughtUp':replication_check(pp,rr)}
   points.append(pt); disk_rows.append(disk)
  record_point()
  while True:
   elapsed=time.monotonic()-start_mono; wall=(dt.datetime.now(dt.timezone.utc)-generated).total_seconds()
   remaining=max(REQUIRED-elapsed,REQUIRED-wall)
   if remaining<=0: break
   time.sleep(min(INTERVAL,remaining))
   readers=json.loads(rp.run(base+reader_selector,retryable=True))['items']; reader_checks+=1
   require(not readers,'token-owned reader pod appeared during cooldown')
   record_point()
  readers=json.loads(rp.run(base+reader_selector,retryable=True))['items']; reader_checks+=1; require(not readers,'token-owned reader pod exists at cooldown end')
  elapsed=time.monotonic()-start_mono; ended=stamp(); wall_age=(dt.datetime.now(dt.timezone.utc)-generated).total_seconds()
  require(elapsed>=REQUIRED and wall_age>=REQUIRED,'both measured and wall-clock cooldown gates must be at least 300 seconds')
  finalp=progress(base,ns,primary['metadata']['name'],'repro'); finalr=progress(base,ns,replica['metadata']['name'],'repro',True)
  caught_start=points[0]['caughtUp']; caught_end=replication_check(finalp,finalr)
  require(caught_start and caught_end,'replica must be caught up at both cooldown boundaries; no replay activity/advancement is required during idle cooldown')
  proof={'status':'complete','ownerToken':token,'createdAtHost':stamp(),'resultDirectory':str(outdir),'fixtureManifestPath':str(artifact),'fixtureGenerationCompletedAtHost':manifest['generationCompletedAtHost'],'cooldownStartedAtHost':started,'cooldownEndedAtHost':ended,'requiredCooldownSeconds':REQUIRED,'measuredCooldownSeconds':elapsed,'wallAgeSinceGenerationSecondsAtEnd':wall_age,'sampleIntervalSeconds':INTERVAL,'readerCountStart':0,'readerCountEnd':len(readers),'readerPodChecksDuringWait':reader_checks,'readerPodsDuringCooldown':False,'dropCaches':False,'cooldownDoesNotProvePageCacheEviction':True,'idleCaughtUpRequiredAtBoundaries':True,'replayProgressOrPositiveLagRequired':False,'primaryReplicaDefaults':defaults,'primaryProgressAtStart':p0,'replicaProgressAtStart':r0,'primaryProgressAtEnd':finalp,'replicaProgressAtEnd':finalr,'caughtUpAtStart':caught_start,'caughtUpAtEnd':caught_end,'replayLsnDeltaBytes':(lsn(finalr.get('replayLsn'))-lsn(r0.get('replayLsn'))) if finalr.get('replayLsn') and r0.get('replayLsn') else None,'backlogAtStartBytes':max(0,lsn(p0['flush'])-lsn(r0['replayLsn'])),'backlogAtEndBytes':max(0,lsn(finalp['flush'])-lsn(finalr['replayLsn'])),'dataPv':data['pv'],'azureDiskId':data['volumeHandle'],'replicaNode':data['node'],'dataLun':str(data['lun']),'dataMajorMinor':data_dev['majorMinor'],'dataQueueDefault':queue,'remoteFixtureInventory':fixture['raw'],'remoteGeneratedFileStats':fixture['files'],'diskSampleBefore':qbefore,'diskSampleAfter':disk_rows[-1],'diskSamples':disk_rows,'progressPoints':points,'retryLogPath':str(outdir/'read-only-retries.jsonl')}
  save(outdir/'cooldown-proof.json',proof); save(STATE/'auxiliary-random-cooldown-final-proof.json',proof)
  print(json.dumps({'status':'complete','measuredCooldownSeconds':elapsed,'wallAgeSinceGenerationSeconds':wall_age,'caughtUpAtStart':caught_start,'caughtUpAtEnd':caught_end,'backlogAtStartBytes':proof['backlogAtStartBytes'],'backlogAtEndBytes':proof['backlogAtEndBytes'],'artifact':str(outdir/'cooldown-proof.json')},indent=2),flush=True)
  return 0
 finally: lock.close()

if __name__=='__main__':
 try: raise SystemExit(main())
 except Exception as exc: print(f'ERROR: {type(exc).__name__}: {exc}',file=sys.stderr,flush=True); raise SystemExit(2)
