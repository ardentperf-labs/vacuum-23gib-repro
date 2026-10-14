#!/usr/bin/env python3
"""Run detached VACUUM and sample separate PostgreSQL DATA/WAL disks."""
import argparse, base64, fcntl, concurrent.futures as cf, datetime as dt, hashlib, json, os, re, shlex, subprocess, sys, tarfile, threading, time, uuid
from collections import deque
from io import BytesIO
from pathlib import Path
HERE=Path(__file__).resolve().parent
STATE=HERE/".state"
CLIENT_DIR=HERE.parent/"client"
RUN_CONTEXT={}
GIB=1024**3
import plot

CLIENT_SETUP_SQL = """CREATE TABLE public.pgbench_client_rows (
    client_id integer NOT NULL,
    update_count bigint NOT NULL
) WITH (autovacuum_enabled = false);
INSERT INTO public.pgbench_client_rows (client_id, update_count)
SELECT g.client_id, 0 FROM generate_series(0, 9) AS g(client_id);
GRANT SELECT, UPDATE ON public.pgbench_client_rows TO app;
"""
CLIENT_TRANSACTION_SQL = """UPDATE public.pgbench_client_rows
SET update_count = update_count + 1
WHERE client_id = :client_id;
"""
BENCH_TPS=5
BENCH_CLIENTS=10
BENCH_WARMUP_SECONDS=300
BENCH_POST_RECOVERY_SECONDS=300

GUC_SQL="""SELECT json_build_object(
 'version',current_setting('server_version'),'recovery',pg_is_in_recovery(),
 'systemId',(SELECT system_identifier::text FROM pg_control_system()),
 'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'),
 'settings',(SELECT json_object_agg(name,json_build_object('value',current_setting(name),'setting',setting,'unit',unit,'source',source,'pendingRestart',pending_restart))
  FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay','maintenance_io_concurrency',
  'maintenance_work_mem','shared_buffers','max_wal_size','checkpoint_timeout','checkpoint_completion_target','track_io_timing','track_wal_io_timing','autovacuum',
  'synchronous_commit','synchronous_standby_names','wal_sender_timeout','wal_receiver_timeout','max_wal_senders','max_replication_slots','wal_compression','wal_buffers','wal_writer_delay','wal_writer_flush_after','statement_timeout'])),
 'defaultTablespaceIoConcurrencyOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) AS opt(value) WHERE t.spcname='pg_default' AND value LIKE 'maintenance_io_concurrency=%'),
 'table',(SELECT json_build_object('oid',c.oid,'relfilenode',c.relfilenode,'heapBytes',pg_relation_size(c.oid),
  'totalBytes',pg_total_relation_size(c.oid),'tablespaceOid',c.reltablespace,'reloptions',c.reloptions,
  'tablespaceOverrides',(SELECT json_agg(x.relname) FROM pg_class x WHERE
   (x.oid=c.oid OR x.oid IN (SELECT i.indexrelid FROM pg_index i WHERE i.indrelid=c.oid)) AND x.reltablespace<>0),
  'liveEstimate',s.n_live_tup,'deadEstimate',s.n_dead_tup)
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_stat_all_tables s ON s.relid=c.oid
  WHERE n.nspname='public' AND c.relname='vacuum_target'))::text;"""
WAL_SQL="""SELECT json_build_object('flushLsn',pg_current_wal_flush_lsn(),'walBytes',wal_bytes::text,
 'walRecords',wal_records,'walFpi',wal_fpi,'statsReset',stats_reset)::text FROM pg_stat_wal;"""

REMOTE=r'''set -eu
 data=$(printenv PGDATA 2>/dev/null || true); [ -n "$data" ] || data=/var/lib/postgresql/data/pgdata
data=$(readlink -f "$data"); wal=$(readlink -f "$data/pg_wal")
ts=$(date -u +%s.%N); iso=$(date -u +'%Y-%m-%dT%H:%M:%S.%NZ')
printf '__TIME__\n%s\n%s\n' "$ts" "$iso"
for k in DATA WAL; do
 if [ "$k" = DATA ]; then p=$data; lun=$DATA_LUN; lp=$DATA_LUN_PATH; else p=$wal; lun=$WAL_LUN; lp=$WAL_LUN_PATH; fi
 set -- $(findmnt -n -r -T "$p" -o MAJ:MIN,SOURCE,TARGET,FSTYPE); mm=$1; src=$2; target=$3; fs=$4
 [ -n "$mm" ] && [ -n "$src" ] && [ -n "$target" ] && [ -n "$fs" ] || { echo "$k findmnt returned an incomplete mount mapping" >&2; exit 1; }
 sys=$(readlink -f "/sys/dev/block/$mm")
 while [ -f "$sys/partition" ]; do sys=$(dirname "$sys"); done
 diskmm=$(cat "$sys/dev"); disk=$(basename "$sys"); link=; seen=0
 for x in "$lp" "/dev/disk/azure/scsi1/lun$lun" "/dev/disk/azure/data/by-lun/$lun"; do
  [ -n "$x" ] && [ -e "$x" ] || continue
  seen=1
  name=$(basename "$(readlink -f "$x")"); q=$(readlink -f "/sys/class/block/$name")
  while [ -f "$q/partition" ]; do q=$(dirname "$q"); done
  [ "$(cat "$q/dev")" = "$diskmm" ] || continue; link=$x; break
 done
 [ "$seen" = 0 ] || [ -n "$link" ] || { echo "visible Azure LUN link does not map to mounted disk $diskmm" >&2; exit 1; }
 sysdev=$(readlink -f "/sys/class/block/$disk/device" 2>/dev/null || true)
 driver_chain=''; storvsc_path=''; nvme_bound=0; ancestor=$sysdev
 while [ -n "$ancestor" ] && [ "$ancestor" != /sys ] && [ "$ancestor" != / ]; do
  if [ -L "$ancestor/driver" ]; then
   driver_path=$(readlink -f "$ancestor/driver"); driver=$(basename "$driver_path")
   [ -n "$driver_chain" ] && driver_chain="$driver_chain,"
   driver_chain="$driver_chain$(basename "$ancestor"):$driver"
   [ "$driver" = hv_storvsc ] && storvsc_path=$ancestor
   [ "$driver" = nvme ] && nvme_bound=1
  fi
  ancestor=$(dirname "$ancestor")
 done
 controller_type=other; controller=; controller_driver=; controller_path=''
 case "$disk" in
  nvme[0-9]*n[0-9]*)
   controller=${disk%n[0-9]*}; controller_path=$(readlink -f "/sys/class/nvme/$controller" 2>/dev/null || true)
   [ -n "$controller_path" ] && [ "$nvme_bound" = 1 ] || { echo "NVMe controller/driver missing for $disk" >&2; exit 1; }
   controller_type=nvme; controller_driver=nvme
   ;;
  *)
   if [ -n "$storvsc_path" ]; then controller_type=storvsc; controller=hv_storvsc; controller_driver=hv_storvsc; controller_path=$storvsc_path; fi
   ;;
 esac
 q="/sys/class/block/$disk/queue"
 attr() { [ -r "$1" ] && cat "$1" || true; }
 scsi_hctl=; scsi_depth=''
 scsi_device=$(readlink -f "/sys/class/block/$disk/device" 2>/dev/null || true)
 if [ -n "$scsi_device" ] && [ -r "$scsi_device/queue_depth" ]; then
  candidate=$(basename "$scsi_device")
  case "$candidate" in *:*:*:*) scsi_hctl=$candidate; scsi_depth=$(cat "$scsi_device/queue_depth");; esac
 fi
 mq_count=0; mq_map=''
 for mq in "/sys/class/block/$disk"/mq/*; do
  [ -d "$mq" ] || continue
  mq_count=$((mq_count+1)); qid=$(basename "$mq")
  [ -n "$mq_map" ] && mq_map="$mq_map;"
  mq_map="$mq_map$qid~$(attr "$mq/cpu_list")~$(attr "$mq/nr_tags")~$(attr "$mq/nr_reserved_tags")"
 done
 printf '__DEV_%s__\n%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
  "$k" "$p" "$mm" "$diskmm" "$src" "$target" "$fs" "$lun" "$link" "$disk" \
  "$(attr "$q/max_sectors_kb")" "$(attr "$q/max_hw_sectors_kb")" "$(attr "$q/nr_requests")" \
  "$(stat -Lc %d "$p")" "$(findmnt -n -r -T "$p" -o MAJ:MIN | tr -d '[:space:]')" "$sysdev" \
  "$controller_type" "$controller" "$controller_driver" "$controller_path" "${driver_chain:-none}" \
  "$scsi_hctl" "$scsi_depth" "$(attr "$q/scheduler")" "$(attr "$q/read_ahead_kb")" \
  "$(attr "$q/rotational")" "$(attr "$q/nomerges")" "$(attr "$q/wbt_lat_usec")" \
  "$(attr "$q/rq_affinity")" "$(attr "$q/io_poll")" "$(attr "$q/io_poll_delay")" "$mq_count" "$mq_map"
done
printf '__DISKSTATS__\n'; cat /proc/diskstats
printf '__SQL__\n'
psql -XAt -F '|' -U postgres -d "$DB_NAME" -c "SELECT CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'primary' END,
 coalesce(pg_last_wal_replay_lsn()::text,''),CASE WHEN pg_is_in_recovery() THEN '' ELSE pg_current_wal_flush_lsn()::text END,
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend' AND application_name IS DISTINCT FROM '$BENCH_APP'),
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend' AND application_name='$BENCH_APP'),
 (SELECT coalesce(sum(update_count),0) FROM public.pgbench_client_rows),
 coalesce((SELECT json_agg(json_build_object('pid',pid,'state',state,'waitEventType',wait_event_type,'waitEvent',wait_event,
  'xactAgeSeconds',extract(epoch FROM clock_timestamp()-xact_start),
  'queryAgeSeconds',extract(epoch FROM clock_timestamp()-query_start)) ORDER BY pid)::text
  FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name='$BENCH_APP' AND backend_type='client backend'),'[]'),
 coalesce((SELECT json_build_object('phase',v.phase,'heapBlksTotal',v.heap_blks_total,'heapBlksScanned',v.heap_blks_scanned,
 'heapBlksVacuumed',v.heap_blks_vacuumed,'indexVacuumCount',v.index_vacuum_count,'numDeadTuples',v.num_dead_tuples,
 'backendState',a.state,'waitEventType',a.wait_event_type,'waitEvent',a.wait_event)::text
 FROM pg_stat_activity a LEFT JOIN pg_stat_progress_vacuum v ON v.pid=a.pid
 WHERE a.application_name='$VAC_APP' AND a.datname=current_database() AND a.usename='postgres' AND a.backend_type='client backend' LIMIT 1),''),
 coalesce((SELECT json_agg(json_build_object('state',state,'syncState',sync_state,'sentLsn',sent_lsn,
 'writeLsn',write_lsn,'flushLsn',flush_lsn,'replayLsn',replay_lsn,'writeLag',write_lag,'flushLag',flush_lag,'replayLag',replay_lag))::text FROM pg_stat_replication),'[]'),
 coalesce((SELECT json_build_object('status',status,'writtenLsn',written_lsn,'flushedLsn',flushed_lsn,
 'latestEndLsn',latest_end_lsn,'lastMsgReceiptTime',last_msg_receipt_time)::text FROM pg_stat_wal_receiver LIMIT 1),''),
 coalesce((SELECT json_build_object('walBytes',wal_bytes::text,'walRecords',wal_records,'walFpi',wal_fpi,'statsReset',stats_reset)::text FROM pg_stat_wal),'{}');"
'''

def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")
def save(path,obj):
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(path.suffix+".tmp")
    tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+"\n"); os.replace(tmp,path)
def append(path,obj):
    with path.open("a") as f: f.write(json.dumps(obj,sort_keys=True)+"\n")
def event(event_path,event_name,/,**data):
    append(event_path,{"time":stamp(),"event":event_name,**data})
def cmd(argv,timeout=30,input=None):
    p=subprocess.run(argv,input=input,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if p.returncode: raise RuntimeError(f"{' '.join(map(str,argv))} failed ({p.returncode}): {(p.stderr or p.stdout)[-1500:]}")
    return p.stdout
def sql(base,ns,pod,db,query,timeout=45):
    out=cmd(base+["exec","-i","-c","postgres","-n",ns,pod,"--","psql","-X","-A","-t","-v","ON_ERROR_STOP=1","-U","postgres","-d",db],timeout,query+"\n")
    try: return json.loads(out.strip().splitlines()[-1])
    except (IndexError,json.JSONDecodeError) as e: raise RuntimeError(f"bad SQL JSON from {pod}: {out[-400:]}") from e
def lsn(s):
    a,b=s.split("/"); return int(a,16)*(1<<32)+int(b,16)
def minimum_fixture_heap(expected):
    if not expected: return 100*GIB
    expected=int(expected)
    return max(GIB,min(100*GIB,int(expected*.97)))
def replica_reached_target(primary,replica,target,expected_bench_sessions=0):
    if (primary.get("role")!="primary" or replica.get("role")!="replica" or
        primary.get("appSessions") or replica.get("appSessions") or
        primary.get("benchSessions")!=expected_bench_sessions or replica.get("benchSessions")!=0 or not target): return False
    try:
        goal=lsn(target); flushed=lsn(primary["flushLsn"]); replayed=lsn(replica["replayLsn"])
        return flushed>=goal and replayed>=goal
    except (KeyError,TypeError,ValueError): return False
def replica_reached_vacuum_target(primary,replica,target,expected_bench_sessions=BENCH_CLIENTS):
    if (primary.get("role")!="primary" or replica.get("role")!="replica" or primary.get("appSessions") or replica.get("appSessions") or
        primary.get("benchSessions")!=expected_bench_sessions or replica.get("benchSessions")!=0 or not target): return False
    try:
        goal=lsn(target); flushed=lsn(primary["flushLsn"]); replayed=lsn(replica["replayLsn"])
        sender=any(x.get("state")=="streaming" and x.get("syncState") in ("sync","quorum") for x in primary.get("senders",[]))
        receiver=(replica.get("receiver") or {}).get("status")=="streaming"
        return flushed>=goal and replayed>=goal and sender and receiver
    except (KeyError,TypeError,ValueError): return False
def measurement_complete(exit_code,vacuum_marker,caught,failure,client_complete,intervention_complete):
    return bool(exit_code==0 and vacuum_marker and caught and client_complete and intervention_complete and not failure)
def next_catchup_streak(streak,qualifies):
    return streak+1 if qualifies else 0
def pod_claims(pod):
    c=next(x for x in pod["spec"]["containers"] if x["name"]=="postgres")
    volumes={v["name"]:v.get("persistentVolumeClaim",{}).get("claimName") for v in pod["spec"]["volumes"]}
    return [{"path":m["mountPath"],"claim":volumes[m["name"]]} for m in c["volumeMounts"] if volumes.get(m["name"])]

def completed_cnpg_jobs(items,cluster):
    jobs={}
    for job in items:
        meta=job.get("metadata",{}); labels=meta.get("labels",{}); owners=meta.get("ownerReferences",[])
        job_role=labels.get("cnpg.io/jobRole"); instance=labels.get("cnpg.io/instanceName")
        valid=(job.get("status",{}).get("succeeded",0)>0 and not job.get("status",{}).get("active",0)
            and labels.get("cnpg.io/cluster")==cluster and job_role in ("snapshot-recovery","join")
            and instance in (cluster+"-1",cluster+"-2")
            and any(o.get("kind")=="Cluster" and o.get("name")==cluster for o in owners))
        name=meta.get("name")
        if not valid or not name or name in jobs: raise RuntimeError(f"unexpected/active Job {name or '<unnamed>'}")
        jobs[name]={"uid":meta.get("uid"),"jobRole":job_role,"instanceName":instance}
    return jobs

def ready_cluster_instance_pods(items,cluster,completed_jobs):
    pods={}; completed_job_pods=[]
    for pod in items:
        meta=pod.get("metadata",{}); labels=meta.get("labels",{}); owners=meta.get("ownerReferences",[])
        if labels.get("cnpg.io/cluster")!=cluster: raise RuntimeError(f"unexpected pod outside cluster {cluster}: {meta.get('name')}")
        job_owners=[o for o in owners if o.get("kind")=="Job" and o.get("name") in completed_jobs]
        if job_owners:
            if len(job_owners)!=1: raise RuntimeError(f"pod {meta.get('name')} has ambiguous completed Job ownership")
            owner=job_owners[0]; expected=completed_jobs[owner["name"]]
            if owner.get("uid") and expected.get("uid") and owner["uid"]!=expected["uid"]:
                raise RuntimeError(f"pod {meta.get('name')} owner UID does not match completed CNPG Job")
            if pod.get("status",{}).get("phase")!="Succeeded":
                raise RuntimeError(f"CNPG Job pod {meta.get('name')} is not completed")
            if labels.get("cnpg.io/jobRole") not in (None,expected["jobRole"]):
                raise RuntimeError(f"CNPG Job pod {meta.get('name')} role differs from its Job")
            if labels.get("cnpg.io/instanceName") not in (None,expected["instanceName"]):
                raise RuntimeError(f"CNPG Job pod {meta.get('name')} instance differs from its Job")
            completed_job_pods.append(meta.get("name"))
            continue
        role=labels.get("cnpg.io/instanceRole")
        if role not in ("primary","replica") or role in pods:
            raise RuntimeError(f"unexpected or duplicate active application pod {meta.get('name')}")
        ready=any(c.get("type")=="Ready" and c.get("status")=="True" for c in pod.get("status",{}).get("conditions",[]))
        if not ready or pod.get("status",{}).get("phase")!="Running":
            raise RuntimeError(f"invalid {role} pod {meta.get('name')}: expected Running/Ready")
        pods[role]=pod
    if set(pods)!={"primary","replica"}:
        raise RuntimeError("expected exactly two ready PostgreSQL instance pods and only completed CNPG Job pods")
    return pods,completed_job_pods

def attachment_lun(metadata,path=""):
    lun=next((v for k,v in metadata.items() if k.lower()=="lun"),None)
    if lun is None:
        hit=re.search(r"(?:lun|by-lun/)(\d+)",path,re.I); lun=hit.group(1) if hit else None
    if lun is None or not str(lun).isdigit(): raise RuntimeError("Azure CSI attachment has no numeric LUN")
    return str(lun)

def storage(base,ns,pods):
    pvcs=json.loads(cmd(base+["get","pvc","-n",ns,"-o","json"]))["items"]
    pvs={x["metadata"]["name"]:x for x in json.loads(cmd(base+["get","pv","-o","json"]))["items"]}
    sc=json.loads(cmd(base+["get","storageclass","azure-standard-hdd","-o","json"]))
    vas=json.loads(cmd(base+["get","volumeattachments","-o","json"]))["items"]
    if sc.get("provisioner")!="disk.csi.azure.com" or sc.get("parameters",{}).get("skuName")!="Standard_LRS" or sc.get("parameters",{}).get("cachingMode")!="None":
        raise RuntimeError("expected no host caching on Standard_LRS Azure Disk storage")
    pvc={x["metadata"]["name"]:x for x in pvcs}; result={"storageClass":sc,"roles":{},"claims":{}}
    for role,pod in pods.items():
        found={}
        for m in pod_claims(pod):
            kind="wal" if "/wal" in m["path"] else "data" if "/data" in m["path"] else None
            c=pvc.get(m["claim"]); pv=pvs.get((c or {}).get("spec",{}).get("volumeName"))
            cs=(pv or {}).get("spec",{}).get("csi",{}); attrs=cs.get("volumeAttributes",{})
            if not kind or kind in found or not c or c.get("status",{}).get("phase")!="Bound": raise RuntimeError(f"invalid PVC mount {m}")
            if c["spec"].get("storageClassName")!="azure-standard-hdd" or c["spec"].get("resources",{}).get("requests",{}).get("storage")!="512Gi":
                raise RuntimeError(f"{m['claim']} is not 512Gi azure-standard-hdd")
            if not pv or cs.get("driver")!="disk.csi.azure.com" or attrs.get("skuName")!="Standard_LRS" or attrs.get("cachingMode")!="None" or pv["spec"].get("capacity",{}).get("storage")!="512Gi":
                raise RuntimeError(f"{m['claim']} is not uncached 512Gi Standard_LRS")
            va=[v for v in vas if v.get("spec",{}).get("source",{}).get("persistentVolumeName")==pv["metadata"]["name"] and
                v.get("spec",{}).get("nodeName")==pod["spec"].get("nodeName") and v.get("status",{}).get("attached")]
            if len(va)!=1: raise RuntimeError(f"{m['claim']} has no unique CSI attachment on its node")
            att=va[0].get("status",{}).get("attachmentMetadata",{}); path=att.get("devicePath","")
            try: lun=attachment_lun(att,path)
            except RuntimeError as e: raise RuntimeError(f"{m['claim']} has no CSI LUN") from e
            found[kind]={"claim":m["claim"],"path":m["path"],"pv":pv["metadata"]["name"],"volumeHandle":cs.get("volumeHandle"),
                "node":pod["spec"].get("nodeName"),"lun":str(lun),"attachmentDevicePath":path if path.startswith("/dev/") else ""}
            result["claims"][m["claim"]]=found[kind]
        if set(found)!={"data","wal"} or found["data"]["claim"]==found["wal"]["claim"]: raise RuntimeError(f"{role} DATA/WAL are not separate")
        result["roles"][role]=found
    if len(result["claims"])!=4: raise RuntimeError("expected four distinct disk claims")
    return result

def parse_sample(out,pod,mapping):
    sec={}; key=None
    for line in out.splitlines():
        if line.startswith("__") and line.endswith("__"): key=line[2:-2]; sec[key]=[]
        elif key: sec[key].append(line)
    try:
        disks={}
        for line in sec["DISKSTATS"]:
            f=line.split()
            if len(f)>=14:
                mm=f"{f[0]}:{f[1]}"; disks[mm]={"name":f[2],"r":int(f[3]),"rs":int(f[5]),"rm":int(f[6]),"w":int(f[7]),"ws":int(f[9]),"wm":int(f[10])}
        devices={}
        for kind in ("data","wal"):
            f=sec["DEV_"+kind.upper()][0].split("|")
            if len(f)!=32: raise ValueError(f"bad disk mapping row: expected 32 fields, found {len(f)}")
            (path,mountmm,diskmm,src,target,fs,lun,link,name,maxsec,maxhw,nreq,statdev,findmm,sysdev,
             controller_type,controller,controller_driver,controller_path,driver_chain,scsi_hctl,scsi_depth,
             scheduler,read_ahead,rotational,nomerges,wbt_lat,rq_affinity,io_poll,io_poll_delay,mq_count,mq_map)=f
            if f"{os.major(int(statdev))}:{os.minor(int(statdev))}"!=mountmm or findmm.strip()!=mountmm: raise ValueError("stat/findmnt major:minor mismatch")
            expected=mapping[kind]
            if lun!=expected["lun"]: raise ValueError("CSI LUN mapping changed")
            if target!=expected["path"]: raise ValueError("mounted path does not match the DATA/WAL PVC mount")
            hctl=re.search(r"/(\d+:\d+:\d+):(\d+)$",sysdev)
            if name.startswith("sd") and hctl and hctl.group(2)!=lun: raise ValueError(f"SCSI sysfs LUN {hctl.group(2)} differs from CSI LUN {lun}")
            if diskmm not in disks or disks[diskmm]["name"]!=name: raise ValueError("sysfs disk missing from /proc/diskstats")
            if controller_type not in ("nvme","storvsc") or not controller_driver or not controller_path:
                raise ValueError(f"unsupported or incomplete {kind.upper()} controller mapping: {controller_type}/{controller_driver}")
            blk_mq=[]
            if mq_map:
                for item in mq_map.split(";"):
                    parts=item.split("~",3)
                    if len(parts)!=4: raise ValueError("bad blk-mq mapping row")
                    qid,cpu_list,tags,reserved=parts
                    blk_mq.append({"queueId":qid,"cpuList":cpu_list,
                        "nrTags":int(tags) if tags.isdigit() else None,
                        "nrReservedTags":int(reserved) if reserved.isdigit() else None})
            if int(mq_count)!=len(blk_mq) or not blk_mq: raise ValueError("blk-mq hardware queue mapping is missing or inconsistent")
            queue={"maxSectorsKb":int(maxsec),"maxHwSectorsKb":int(maxhw),"nrRequests":int(nreq),
                "scheduler":scheduler,"readAheadKb":int(read_ahead) if read_ahead.isdigit() else None,
                "rotational":int(rotational) if rotational.isdigit() else None,
                "nomerges":int(nomerges) if nomerges.isdigit() else None,
                "wbtLatUsec":int(wbt_lat) if wbt_lat.isdigit() else None,
                "rqAffinity":int(rq_affinity) if rq_affinity.isdigit() else None,
                "ioPoll":int(io_poll) if io_poll.isdigit() else None,
                "ioPollDelay":int(io_poll_delay) if io_poll_delay.isdigit() else None,
                "blkMq":blk_mq}
            devices[kind]={"path":path,"claim":expected["claim"],"pv":expected["pv"],"node":expected["node"],"majorMinor":diskmm,
                "mountMajorMinor":mountmm,"source":src,"mount":target,"expectedMount":expected["path"],"filesystem":fs,"lun":int(lun),"attachmentDevicePath":expected["attachmentDevicePath"],
                "lunPath":link,"lunPathVisible":bool(link),"sysfsDevicePath":sysdev,"device":name,
                "controller":{"type":controller_type,"name":controller,"driver":controller_driver,"path":controller_path,"driverChain":driver_chain},
                "scsi":{"hctl":scsi_hctl or None,"queueDepth":int(scsi_depth) if scsi_depth.isdigit() else None},"queue":queue}
        if devices["data"]["majorMinor"]==devices["wal"]["majorMinor"]: raise ValueError("DATA/WAL share a block device")
        f=sec["SQL"][0].split("|",10)
        return {"epoch":float(sec["TIME"][0]),"ts":sec["TIME"][1],"pod":pod["metadata"]["name"],"role":f[0],
            "replayLsn":f[1],"flushLsn":f[2],"appSessions":int(f[3]),"benchSessions":int(f[4]),
            "committedTransactions":int(f[5]),"clientBackends":json.loads(f[6]),
            "vacuumProgress":json.loads(f[7]) if f[7] else None,
            "senders":json.loads(f[8]),"receiver":json.loads(f[9]) if f[9] else None,
            "walStats":json.loads(f[10]),"devices":devices,"disks":disks}
    except (KeyError,IndexError,ValueError,json.JSONDecodeError) as e: raise RuntimeError(f"invalid sample from {pod['metadata']['name']}: {e}") from e

def sample(base,ns,db,app,bench_app,pod,mapping,expected_bench_sessions=BENCH_CLIENTS):
    env={"DB_NAME":db,"VAC_APP":app,"BENCH_APP":bench_app}
    for k in ("data","wal"): env[k.upper()+"_LUN"]=mapping[k]["lun"]; env[k.upper()+"_LUN_PATH"]=mapping[k]["attachmentDevicePath"]
    prefix=" ".join(f"{k}={shlex.quote(v)}" for k,v in env.items())
    out=cmd(base+["exec","-c","postgres","-n",ns,pod["metadata"]["name"],"--","sh","-c",prefix+"\n"+REMOTE],30)
    row=parse_sample(out,pod,mapping)
    expected=expected_bench_sessions if pod["metadata"]["labels"].get("cnpg.io/instanceRole")=="primary" else 0
    if (row["role"]!=pod["metadata"]["labels"].get("cnpg.io/instanceRole") or row["appSessions"] or
        row["benchSessions"]!=expected): raise RuntimeError("role, unrelated app session, or pgbench session count changed")
    return row

def both(base,ns,db,app,bench_app,pods,layout,expected_bench_sessions=BENCH_CLIENTS):
    with cf.ThreadPoolExecutor(2) as pool:
        f={r:pool.submit(sample,base,ns,db,app,bench_app,p,layout["roles"][r],expected_bench_sessions) for r,p in pods.items()}
        return {r:f[r].result() for r in pods}


def same_map(first, rows):
    for role in ("primary","replica"):
        for dev in ("data","wal"):
            a,b=first[role]["devices"][dev],rows[role]["devices"][dev]
            keys=("path","claim","pv","node","majorMinor","mountMajorMinor","source","mount","filesystem","expectedMount","lun","lunPath","sysfsDevicePath","device","controller","scsi","queue")
            if any(a[k]!=b[k] for k in keys): raise RuntimeError(f"{role} {dev} disk/PVC/LUN mapping changed")

def check_pods(base,ns,cluster,pods):
    jobs=completed_cnpg_jobs(json.loads(cmd(base+["get","jobs","-n",ns,"-o","json"]))["items"],cluster)
    items=json.loads(cmd(base+["get","pods","-n",ns,"-l","cnpg.io/cluster="+cluster,"-o","json"]))["items"]
    current,_=ready_cluster_instance_pods(items,cluster,jobs)
    for role,old in pods.items():
        p=current[role]; meta=p.get("metadata",{}); status=p.get("status",{})
        if meta.get("uid")!=old["metadata"].get("uid"): raise RuntimeError("PostgreSQL pod identity changed")
        if meta.get("labels",{}).get("cnpg.io/instanceRole")!=role: raise RuntimeError(f"PostgreSQL {role} role changed")
        if status.get("phase")!="Running" or not any(c.get("type")=="Ready" and c.get("status")=="True" for c in status.get("conditions",[])):
            raise RuntimeError(f"PostgreSQL {role} pod is not Running/Ready")

def vacuum_spec(prepared):
    options=["VERBOSE"]
    if prepared.get("fixtureTargetHeapBytes") is not None: options.append("FREEZE")
    return {"options":options,"sql":f"VACUUM ({', '.join(options)}) public.vacuum_target;"}

VACUUM_WORKDIR_TOKEN="__VACUUM_WORK_DIRECTORY__"

def vacuum_query(statement):
    # COPY TO PROGRAM runs in PostgreSQL's working directory. The remote
    # launcher expands this token to the absolute controller temp directory.
    start_program=f"cat > {VACUUM_WORKDIR_TOKEN}/vacuum-start.marker"
    complete_program=f"cat > {VACUUM_WORKDIR_TOKEN}/vacuum-complete.marker"
    return f"""COPY (SELECT 'VACUUM_START|'||to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')||'|'||pg_current_wal_flush_lsn()||'|'||wal_bytes||'|'||wal_records||'|'||wal_fpi FROM pg_stat_wal) TO PROGRAM '{start_program}';
{statement}
COPY (SELECT 'VACUUM_COMPLETE|'||to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')||'|'||pg_current_wal_flush_lsn()||'|'||wal_bytes||'|'||wal_records||'|'||wal_fpi FROM pg_stat_wal) TO PROGRAM '{complete_program}';
"""

def launch_vacuum(base,ns,pod,db,app,safety_seconds=86400,statement=None):
    work="vacuum-"+uuid.uuid4().hex; q=shlex.quote
    statement=statement or vacuum_spec({})["sql"]
    query=vacuum_query(statement)
    wrapper=f'''#!/bin/sh
d=$1
PGAPPNAME={q(app)} psql -X -A -t -v ON_ERROR_STOP=1 -U postgres -d {q(db)} -f "$d/vacuum.sql" >"$d/vacuum.log" 2>&1 &
client=$!
sh -c 'end=$(( $(date +%s) + {int(safety_seconds)} )); while [ ! -r "$1/status" ] && [ "$(date +%s)" -lt "$end" ]; do sleep 5; done; if [ ! -r "$1/status" ]; then PGAPPNAME=vacuum-guard psql -XAt -U postgres -d {q(db)} -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name='\''{app}'\'' AND usename='\''postgres'\'';" >/dev/null 2>&1; fi' sh "$d" >/dev/null 2>&1 &
guard=$!
if wait "$client"; then rc=0; else rc=$?; fi
printf '%s\\n' "$rc" >"$d/status.tmp"; mv "$d/status.tmp" "$d/status"
kill "$guard" >/dev/null 2>&1 || true
'''
    remote=f'''set -eu
 base=$(printenv TMPDIR 2>/dev/null || true); [ -n "$base" ] && [ -w "$base" ] || base=/tmp
d="$base/{work}"; mkdir -m 700 "$d"
marker_dir=$(printf '%s' "$d" | sed 's/[\\\\&@]/\\\\&/g')
printf %s {q(query)} | sed "s@{VACUUM_WORKDIR_TOKEN}@$marker_dir@g" >"$d/vacuum.sql"
grep -F "$d/vacuum-start.marker" "$d/vacuum.sql" >/dev/null
grep -F "$d/vacuum-complete.marker" "$d/vacuum.sql" >/dev/null
printf %s {q(wrapper)} >"$d/run.sh"; chmod 700 "$d/run.sh"
nohup /bin/sh "$d/run.sh" "$d" </dev/null >/dev/null 2>&1 &
printf '%s\\n%s\\n' "$d" "$!"
'''
    out=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",remote],30).strip().splitlines()
    return out[-2],int(out[-1])

def remote_status(base,ns,pod,work):
    f=shlex.quote(work+"/status")
    return cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",f"if [ -r {f} ]; then cat {f}; else echo RUNNING; fi"],20).strip()

def cancel_owned(base,ns,pod,db,app):
    q="SELECT coalesce(json_agg(json_build_object('pid',pid,'cancelSent',pg_cancel_backend(pid))),'[]'::json)::text FROM pg_stat_activity WHERE application_name='%s' AND usename='postgres';"%app
    return sql(base,ns,pod,db,q)

def fetch_log(base,ns,pod,work,dest):
    f=shlex.quote(work+"/vacuum.log")
    log=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c","cat "+f],90); markers=[]
    for kind in ("start","complete"):
        path=shlex.quote(work+f"/vacuum-{kind}.marker")
        try: marker=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c","cat "+path],20).strip()
        except RuntimeError: marker=""
        if marker: markers.append(marker)
    dest.write_text(log+(("\n" if log and not log.endswith("\n") else "")+"\n".join(markers)+("\n" if markers else "")))
    marks={}
    for line in log.splitlines():
        x=line.strip().split("|")
        if len(x)==6 and x[0] in ("VACUUM_START","VACUUM_COMPLETE"):
            marks[x[0].lower().replace("vacuum_","")]={"time":x[1],"flushLsn":x[2],"walBytes":int(x[3]),"walRecords":int(x[4]),"walFpi":int(x[5])}
    return marks

def parse_replication_log(text,role):
    needles=(("sender_timeout","terminating walsender process due to replication timeout"),
      ("receiver_timeout","terminating walreceiver due to timeout"),
      ("receiver_admin_termination","terminating connection due to administrator command"),
      ("receiver_disconnect","could not receive data from WAL stream"),
      ("receiver_connected","started streaming WAL from primary"))
    found=[]
    for line in text.splitlines():
        try: row=json.loads(line[line.find("{"):]) if "{" in line else {}
        except json.JSONDecodeError: continue
        if not isinstance(row,dict): continue
        rec=row.get("record") or {}
        if not isinstance(rec,dict): continue
        message=rec.get("message","")
        kind=next((name for name,needle in needles if needle in message),None)
        if not kind: continue
        event_time=row.get("ts") or (line[:line.find("{")].strip().split() or [None])[0]
        log_time=rec.get("log_time","")
        if not event_time and isinstance(log_time,str) and log_time.endswith(" UTC"):
            event_time=log_time[:-4].replace(" ","T",1)+"Z"
        if not event_time: continue
        item={"time":event_time,"event":kind,"role":role,"message":message}
        for src,dst in (("session_id","sessionId"),("process_id","processId"),("application_name","applicationName"),("backend_type","backendType")):
            if rec.get(src) is not None: item[dst]=rec[src]
        found.append(item)
    return found

def capture_replication_logs(base,ns,pods,start,out,mark):
    events=[]; errors=[]; raw=out/"raw"; raw.mkdir(parents=True,exist_ok=True)
    for role,pod in pods.items():
        dest=raw/f"{role}-postgres.log"
        try:
            log=cmd(base+["logs","-n",ns,pod["metadata"]["name"],"-c","postgres","--since-time="+start],120)
            dest.write_text(log)
            if not log.strip(): raise RuntimeError("kubectl returned no PostgreSQL log lines")
            events.extend(parse_replication_log(log,role))
        except Exception as e:
            err={"role":role,"error":str(e)}; errors.append(err)
            mark("replication_log_capture_failed",**err)
            events.append({"time":stamp(),"event":"replication_log_capture_failed","role":role,"message":str(e)})
            try: dest.write_text("")
            except OSError: pass
    events.sort(key=lambda x:x["time"] or "")
    try: save(out/"replication-events.json",events)
    except Exception as e: mark("replication_events_save_failed",error=str(e))
    if errors:
        try: save(raw/"replication-log-errors.json",errors)
        except Exception as e: mark("replication_log_errors_save_failed",error=str(e))
    return events

def setup_client_fixture(base,ns,pod,db):
    setup=(CLIENT_DIR/"setup.sql").read_text()
    cmd(base+["exec","-i","-c","postgres","-n",ns,pod,"--","psql","-X","-v","ON_ERROR_STOP=1","-U","postgres","-d",db],90,setup)
    q="""SELECT json_build_object('persistence',c.relpersistence,'rows',(SELECT count(*) FROM public.pgbench_client_rows),
      'minId',(SELECT min(client_id) FROM public.pgbench_client_rows),'maxId',(SELECT max(client_id) FROM public.pgbench_client_rows),
      'autovacuumDisabled','autovacuum_enabled=false'=ANY(c.reloptions),'hasIndex',c.relhasindex,
      'appCanSelectUpdate',has_table_privilege('app',c.oid,'SELECT') AND has_table_privilege('app',c.oid,'UPDATE'))::text
      FROM pg_class c WHERE c.oid='public.pgbench_client_rows'::regclass;"""
    actual=sql(base,ns,pod,db,q)
    expected={"persistence":"p","rows":10,"minId":0,"maxId":9,"autovacuumDisabled":True,"hasIndex":False,"appCanSelectUpdate":True}
    if actual!=expected: raise RuntimeError(f"pgbench fixture differs from the ten-row logged writer: {actual}")
    return actual

def check_pgbench_runtime(base,ns,pod):
    out=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c","command -v pgbench && pgbench --version"],30)
    lines=[x.strip() for x in out.splitlines() if x.strip()]
    if len(lines)<2 or not (lines[0]=="pgbench" or lines[0].endswith("/pgbench")) or not re.fullmatch(r"pgbench \(PostgreSQL\) 16\.15(?: \(Debian [^)]+\))?",lines[1]):
        raise RuntimeError(f"primary image lacks verified PostgreSQL 16.15 pgbench: {out.strip()!r}")
    return {"path":lines[0],"version":lines[1]}

def verify_client_auth(base,ns,pod,db,credential):
    q="SELECT current_user||'|'||current_database()||'|'||current_setting('synchronous_commit')||'|'||current_setting('statement_timeout')"
    out=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","env","PGPASSFILE="+credential["pgpassFilePath"],
        "PGAPPNAME=pgbench-auth-preflight","psql","-X","-A","-t","-v","ON_ERROR_STOP=1","-h","127.0.0.1",
        "-p",str(credential["port"]),"-U","app","-d",db,"-c",q],30).strip()
    if out!=f"app|{db}|on|0": raise RuntimeError(f"client auth expected app|{db}|on|0, got {out!r}")

def client_activity_query(app):
    app="'"+app.replace("'","''")+"'"
    return f"""SELECT json_build_object('observedAtUtc',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
      'benchSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend'),
      'unrelatedAppSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name IS DISTINCT FROM {app} AND backend_type='client backend'),
      'committedTransactions',(SELECT coalesce(sum(update_count),0) FROM public.pgbench_client_rows),
      'activeSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active'),
      'waitingSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active' AND wait_event IS NOT NULL),
      'backends',coalesce((SELECT json_agg(json_build_object('pid',pid,'state',state,'waitEventType',wait_event_type,'waitEvent',wait_event,
        'backendStartUtc',to_char(backend_start AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'xactStartUtc',to_char(xact_start AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'queryStartUtc',to_char(query_start AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
        'xactAgeSeconds',extract(epoch FROM clock_timestamp()-xact_start),'queryAgeSeconds',extract(epoch FROM clock_timestamp()-query_start)) ORDER BY pid)
        FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend'),'[]'::json),
      'maxActiveXactAgeSeconds',(SELECT max(extract(epoch FROM clock_timestamp()-xact_start)) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active'),
      'maxActiveQueryAgeSeconds',(SELECT max(extract(epoch FROM clock_timestamp()-query_start)) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active'))::text;"""

class ClientMonitor:
    def __init__(self,base,ns,pod,db,bench_app,path,interval=1.0):
        self.base,self.ns,self.pod,self.db,self.bench_app=base,ns,pod,db,bench_app
        self.path,self.interval=path,interval
        self.stop_event=threading.Event(); self.lock=threading.Lock(); self.latest_row=None; self.history=deque(maxlen=120)
        self.thread=threading.Thread(target=self._run,name="pgbench-activity-sampler",daemon=True)
    def start(self): self.thread.start()
    def latest(self):
        with self.lock: return dict(self.latest_row) if self.latest_row else None
    def _run(self):
        query=client_activity_query(self.bench_app)
        while not self.stop_event.is_set():
            requested=time.time(); begun=time.monotonic()
            try:
                remote="PGAPPNAME=pgbench-session-monitor psql -XAt -v ON_ERROR_STOP=1 -U postgres -d "+shlex.quote(self.db)+" -c "+shlex.quote(query)
                output=cmd(self.base+["exec","-c","postgres","-n",self.ns,self.pod,"--","sh","-c",remote],20)
                row=json.loads(output.strip().splitlines()[-1])
                row.update({"sampleEpoch":time.time(),"sampleRequestedEpoch":requested,"sampleCommandSeconds":time.monotonic()-begun,"sampleStatus":"ok"})
            except Exception as e:
                row={"sampleEpoch":time.time(),"sampleRequestedEpoch":requested,"sampleCommandSeconds":time.monotonic()-begun,
                     "sampleStatus":"error","error":str(e)[:500]}
            append(self.path,row)
            with self.lock:
                self.latest_row=row
                self.history.append(row)
            self.stop_event.wait(max(0,self.interval-(time.monotonic()-begun)))
    def progress_health(self,expected_sessions=10,min_tps=4.0,window_seconds=60.0,max_pending_age_seconds=60.0):
        now=time.time()
        with self.lock: rows=list(self.history)
        good=[r for r in rows if r.get("sampleStatus")=="ok" and now-float(r.get("sampleEpoch",0))<=window_seconds+5]
        latest=good[-1] if good else None
        result={"healthy":False,"checkedAtEpoch":now,"windowSeconds":window_seconds,"minimumTps":min_tps,
            "sampleCount":len(good),"transactionsPerSecond":None,"latest":latest}
        if not latest or now-float(latest.get("sampleEpoch",0))>5: result["reason"]="latest client sample is stale or missing"; return result
        cutoff=float(latest["sampleEpoch"])-window_seconds
        start=next((r for r in good if float(r["sampleEpoch"])<=cutoff),None)
        if not start:
            result["reason"]="client history does not yet cover a full rolling window"; return result
        elapsed=float(latest["sampleEpoch"])-float(start["sampleEpoch"])
        delta=int(latest["committedTransactions"])-int(start["committedTransactions"])
        rate=delta/elapsed if elapsed>0 else 0.0
        window=[r for r in good if float(r["sampleEpoch"])>=float(start["sampleEpoch"])]
        placement_ok=all(r.get("benchSessions")==expected_sessions and r.get("unrelatedAppSessions")==0 for r in window)
        prolonged=[]
        for r in window:
            for backend in r.get("backends") or []:
                if backend.get("state")=="active" and max(float(backend.get("queryAgeSeconds") or 0),float(backend.get("xactAgeSeconds") or 0))>=max_pending_age_seconds:
                    prolonged.append({"sampleEpoch":r.get("sampleEpoch"),"pid":backend.get("pid"),
                        "queryAgeSeconds":backend.get("queryAgeSeconds"),"xactAgeSeconds":backend.get("xactAgeSeconds"),
                        "waitEventType":backend.get("waitEventType"),"waitEvent":backend.get("waitEvent")})
        result.update({"startEpoch":start["sampleEpoch"],"endEpoch":latest["sampleEpoch"],"elapsedSeconds":elapsed,
            "committedTransactions":delta,"transactionsPerSecond":rate,"sessionPlacementHealthy":placement_ok,
            "prolongedPendingCount":len(prolonged),"prolongedPending":prolonged[:20]})
        result["healthy"]=bool(elapsed>=window_seconds-5 and rate>=min_tps and placement_ok and not prolonged)
        result["reason"]="healthy" if result["healthy"] else "low write progress, changed session placement, or prolonged pending transaction"
        return result
    def stop(self,timeout=25):
        self.stop_event.set(); self.thread.join(timeout)
        return not self.thread.is_alive()


def launch_pgbench(base,ns,pod,db,bench_app,credential,duration_seconds,target_tps=5,clients=10):
    work="pgbench-"+uuid.uuid4().hex; q=shlex.quote
    txn=(CLIENT_DIR/"transaction.sql").read_text()
    wrapper=f'''#!/bin/sh
set -u
d=$1
PGAPPNAME={q(bench_app)} PGPASSFILE={q(credential['pgpassFilePath'])} pgbench --host=127.0.0.1 --port={int(credential['port'])} \
  --no-vacuum --protocol=simple --client={int(clients)} --jobs={int(clients)} \
  --rate={float(target_tps):g} --time={int(duration_seconds)} --max-tries=1 --random-seed=42 --log --log-prefix="$d/pgbench-log" \
  --progress=5 --progress-timestamp --username=app --file="$d/transaction.sql" {q(db)} >"$d/pgbench.out" 2>&1 &
client=$!
printf '%s\\n' "$client" >"$d/pgbench.pid"
if wait "$client"; then rc=0; else rc=$?; fi
printf '%s\\n' "$rc" >"$d/status.tmp"; mv "$d/status.tmp" "$d/status"
exit 0
'''
    remote=f'''set -eu
base=$(printenv TMPDIR 2>/dev/null || true); [ -n "$base" ] && [ -w "$base" ] || base=/tmp
d="$base/{work}"; mkdir -m 700 "$d"
printf %s {q(txn)} >"$d/transaction.sql"
printf %s {q(wrapper)} >"$d/run.sh"; chmod 700 "$d/run.sh"
nohup /bin/sh "$d/run.sh" "$d" </dev/null >/dev/null 2>&1 &
wrapper=$!
end=$(( $(date +%s) + 20 ))
while [ ! -s "$d/pgbench.pid" ] && [ "$(date +%s)" -lt "$end" ]; do sleep 0.1; done
[ -s "$d/pgbench.pid" ] || {{ echo 'pgbench did not start' >&2; exit 1; }}
cat "$d/pgbench.pid" | tr -d '\\n'; printf '\\n%s\\n%s\\n' "$d" "$wrapper"
'''
    out=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",remote],30).strip().splitlines()
    if len(out)<3 or not out[-3].isdigit() or not out[-1].isdigit(): raise RuntimeError(f"could not identify pgbench process: {out}")
    return {"workDirectory":out[-2],"wrapperPid":int(out[-1]),"pgbenchPid":int(out[-3]),"status":"RUNNING","applicationName":bench_app,
            "database":db,"clients":clients,"targetTps":target_tps,"durationLimitSeconds":int(duration_seconds),
            "host":"127.0.0.1","port":int(credential["port"]),"credentialFilePath":credential["pgpassFilePath"],
            "noVacuum":True,"latencyLimit":None,"maxTries":1}

def remote_client_status(base,ns,pod,client):
    f=shlex.quote(client["workDirectory"]+"/status")
    return cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",f"if [ -r {f} ]; then cat {f}; else echo RUNNING; fi"],20).strip()

def signal_pgbench_stop(base,ns,pod,client):
    pid=shlex.quote(client["workDirectory"]+"/pgbench.pid")
    # PostgreSQL 16.15 pgbench uses SIGALRM for its duration handler. This sets
    # timer_exceeded so it stops scheduling and drains active transactions; its
    # source has no SIGINT handler, and background jobs may inherit SIGINT-ignore.
    script=f'if [ -r {pid} ]; then p=$(cat {pid}); kill -ALRM "$p" 2>/dev/null || true; fi; echo graceful_stop_requested'
    return cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",script],20).strip()

def wait_for_warmup(base,ns,pod,client,monitor,seconds,target_tps=5.0,window_seconds=60.0):
    ready_deadline=time.monotonic()+90; ready=None; next_status=0
    while time.monotonic()<ready_deadline:
        if time.monotonic()>=next_status:
            if remote_client_status(base,ns,pod,client)!="RUNNING": raise RuntimeError("pgbench exited before ten persistent app sessions were established")
            next_status=time.monotonic()+5
        row=monitor.latest()
        if row and row.get("sampleStatus")=="ok" and row.get("benchSessions")==client["clients"]:
            ready=row; break
        time.sleep(.25)
    if not ready: raise RuntimeError("pgbench did not establish exactly ten authenticated persistent app sessions")
    warmup_start=float(ready["sampleEpoch"]); previous_count=int(ready["committedTransactions"])
    bucket_start=warmup_start; bucket_count=previous_count; windows=[]; start_mono=time.monotonic()
    deadline=start_mono+seconds+window_seconds+5; needed_windows=int((seconds+window_seconds-1)//window_seconds); last_row=ready
    next_status=time.monotonic()
    while time.monotonic()<deadline and (time.monotonic()-start_mono<seconds or len(windows)<needed_windows):
        if time.monotonic()>=next_status:
            if remote_client_status(base,ns,pod,client)!="RUNNING": raise RuntimeError("pgbench exited during its five-minute pre-VACUUM warm-up")
            next_status=time.monotonic()+10
        row=monitor.latest()
        if not row or time.time()-float(row.get("sampleEpoch",0))>10: raise RuntimeError("one-second pgbench activity sampler stopped updating during warm-up")
        if row.get("sampleStatus")=="ok":
            if row.get("unrelatedAppSessions")!=0: raise RuntimeError(f"unrelated app sessions appeared during warm-up: {row.get('unrelatedAppSessions')}")
            if row.get("benchSessions")!=client["clients"]: raise RuntimeError(f"pgbench persistent sessions changed during warm-up: {row.get('benchSessions')}")
            last_row=row; now=float(row["sampleEpoch"])
            if now-bucket_start>=window_seconds:
                count=int(row["committedTransactions"]); elapsed=now-bucket_start; rate=(count-bucket_count)/elapsed if elapsed else 0.0
                windows.append({"startEpoch":bucket_start,"endEpoch":now,"transactions":count-bucket_count,"seconds":elapsed,"tps":rate})
                if not target_tps*.8<=rate<=target_tps*1.2: raise RuntimeError(f"pgbench warm-up did not sustain the {target_tps:g} TPS offer: {rate:.3f} TPS")
                bucket_start=now; bucket_count=count
        time.sleep(.25)
    total=int(last_row["committedTransactions"])-previous_count; elapsed=max(0,float(last_row["sampleEpoch"])-warmup_start)
    if len(windows)<needed_windows or elapsed<seconds: raise RuntimeError(f"pgbench warm-up did not cover {seconds:g}s in {needed_windows} healthy windows: observed {elapsed:.1f}s/{len(windows)} windows")
    return {"startEpoch":warmup_start,"endEpoch":float(last_row["sampleEpoch"]),"seconds":elapsed,"transactions":total,
            "averageTps":total/elapsed if elapsed else None,"healthyWindows":windows,"targetTps":target_tps,
            "persistentSessions":client["clients"],"unrelatedAppSessions":0}

def pre_vacuum_checkpoint(base,ns,primary,replica,db,client,monitor,catchup_hours,interval,mark):
    before=sql(base,ns,primary,db,WAL_SQL)
    start=stamp()
    mark("pre_vacuum_checkpoint_started",time=start,walBefore=before,clientStillRunning=True)
    cmd(base+["exec","-i","-c","postgres","-n",ns,primary,"--","psql","-X","-v","ON_ERROR_STOP=1","-U","postgres","-d",db],600,"CHECKPOINT;\n")
    after=sql(base,ns,primary,db,WAL_SQL)
    target=after.get("flushLsn")
    if not target: raise RuntimeError("pre-VACUUM checkpoint returned no primary flush LSN")
    complete=stamp()
    mark("pre_vacuum_checkpoint_completed",time=complete,targetLsn=target,walBefore=before,walAfter=after,
         clientStillRunning=True)
    deadline=time.monotonic()+catchup_hours*3600
    next_status=0
    while time.monotonic()<deadline:
        if time.monotonic()>=next_status:
            if remote_client_status(base,ns,primary,client)!="RUNNING":
                raise RuntimeError("pgbench exited while waiting for pre-VACUUM checkpoint replay")
            next_status=time.monotonic()+10
        activity=monitor.latest()
        if (not activity or activity.get("sampleStatus")!="ok" or time.time()-float(activity.get("sampleEpoch",0))>10 or
            activity.get("benchSessions")!=client["clients"] or activity.get("unrelatedAppSessions")!=0):
            raise RuntimeError(f"pgbench sessions/sampler unhealthy while waiting for checkpoint replay: {activity}")
        replay=sql(base,ns,replica,db,"SELECT json_build_object('recovery',pg_is_in_recovery(),'replayLsn',pg_last_wal_replay_lsn())::text;")
        if not replay["recovery"]: raise RuntimeError("replica role changed during pre-VACUUM checkpoint replay")
        if replay["replayLsn"] and lsn(replay["replayLsn"])>=lsn(target):
            primary_now=sql(base,ns,primary,db,WAL_SQL)
            reached=stamp()
            result={"startedAtUtc":start,"checkpointCompletedAtUtc":complete,"replayReachedAtUtc":reached,
                "walBefore":before,"walAfter":after,"targetLsn":target,"replicaReplayLsn":replay["replayLsn"],
                "primaryFlushLsnAtReplay":primary_now["flushLsn"],"clientStillRunning":True}
            mark("pre_vacuum_checkpoint_replay_complete",time=reached,targetLsn=target,
                 replicaReplayLsn=replay["replayLsn"],primaryFlushLsn=primary_now["flushLsn"],clientStillRunning=True)
            return result
        time.sleep(interval)
    raise RuntimeError(f"replica did not replay pre-VACUUM checkpoint LSN {target} within {catchup_hours:g}h")

def stop_pgbench(base,ns,pod,client,timeout=900):
    result={"requestedAtUtc":stamp(),"signal":"SIGALRM","signalSemantics":"pgbench duration handler stops scheduling and drains active transactions",
            "requestResult":signal_pgbench_stop(base,ns,pod,client)}
    end=time.monotonic()+timeout; status="RUNNING"
    while time.monotonic()<end:
        status=remote_client_status(base,ns,pod,client)
        if status!="RUNNING": break
        time.sleep(2)
    result.update({"finishedAtUtc":stamp(),"status":status,"exitCode":int(status) if status.isdecimal() else None,
                   "waitSeconds":max(0,timeout-(end-time.monotonic()))})
    if status=="RUNNING": raise RuntimeError(f"pgbench did not drain gracefully within {timeout}s; sessions/queries were left untouched")
    if status!="0": raise RuntimeError(f"pgbench exited with status {status}")
    return result

def fetch_client_logs(base,ns,pod,client,dest):
    work=shlex.quote(client["workDirectory"])
    argv=base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",f"tar -C {work} -czf - ."]
    proc=subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=180)
    if proc.returncode: raise RuntimeError(f"pgbench log archive fetch failed: {proc.stderr.decode(errors='replace')[-1000:]}")
    dest.mkdir(parents=True,exist_ok=True); copied=[]
    with tarfile.open(fileobj=BytesIO(proc.stdout),mode="r:gz") as archive:
        for member in archive.getmembers():
            name=Path(member.name).name
            if not member.isfile() or not (name in {"pgbench.out","status","pgbench.pid","transaction.sql"} or name.startswith("pgbench-log.")):
                continue
            stream=archive.extractfile(member)
            if stream is None: continue
            target=dest/name; target.write_bytes(stream.read()); copied.append(target)
    if not any(x.name=="pgbench.out" for x in copied): raise RuntimeError("pgbench stdout log missing from remote run")
    return copied

def summarize_pgbench_logs(paths):
    rows=[]; errors=[]
    for path in paths:
        if not path.name.startswith("pgbench-log."): continue
        for line_no,line in enumerate(path.read_text(errors="replace").splitlines(),1):
            f=line.split()
            if len(f)<7: errors.append({"file":path.name,"line":line_no,"text":line[:240]}); continue
            try:
                latency_us=int(f[2]); schedule_lag_us=int(f[6])
                rows.append({"clientId":int(f[0]),"transactionNumber":int(f[1]),"latencyMs":latency_us/1000,
                    "scheduleLagMs":schedule_lag_us/1000,"executionMs":max(0,latency_us-schedule_lag_us)/1000,
                    "completionEpoch":float(f[4])+int(f[5])/1e6})
            except (ValueError,IndexError): errors.append({"file":path.name,"line":line_no,"text":line[:240]})
    latency=[x["latencyMs"] for x in rows]; lag=[x["scheduleLagMs"] for x in rows]; execution=[x["executionMs"] for x in rows]
    def distribution(values):
        if not values: return {"count":0,"mean":None,"p50":None,"p95":None,"p99":None,"max":None}
        values=sorted(values)
        def percentile(p): return values[min(len(values)-1,max(0,int((len(values)-1)*p)))]
        return {"count":len(values),"mean":sum(values)/len(values),"p50":percentile(.50),"p95":percentile(.95),"p99":percentile(.99),"max":values[-1]}
    return {"transactionLogRows":len(rows),"completedTransactions":None,"latencyMs":distribution(latency),"scheduleLagMs":distribution(lag),
            "executionMs":distribution(execution),"malformedLogRows":errors,
            "latencySource":"pgbench per-transaction completion logs; latency includes Poisson schedule lag",
            "executionSource":"pgbench logged latency minus schedule_lag; log rows are completion records"}

def wait_for_bench_drain(monitor,timeout=120):
    end=time.monotonic()+timeout; last=None
    while time.monotonic()<end:
        row=monitor.latest()
        if row and row.get("sampleStatus")=="ok":
            last=row
            if row.get("benchSessions")==0 and row.get("unrelatedAppSessions")==0: return row
        time.sleep(.25)
    raise RuntimeError(f"pgbench sessions did not drain within {timeout}s; latest sample={last}")

def capture_pgbench_results(active):
    if active.get("captured"): return active.get("summary")
    client=active["client"]; base=active["base"]; ns=active["namespace"]; pod=active["pod"]
    result_dir=Path(active["outputDirectory"])/"client"/"raw"
    paths=fetch_client_logs(base,ns,pod,client,result_dir)
    stdout=(result_dir/"pgbench.out").read_text(errors="replace")
    summary=summarize_pgbench_logs(paths)
    processed=re.search(r"number of transactions actually processed:\s*(\d+)",stdout)
    failed=re.search(r"number of failed transactions:\s*(\d+)",stdout)
    summary["completedTransactions"]=int(processed.group(1)) if processed else None
    summary["loggedCompletedTransactions"]=summary["transactionLogRows"]
    summary["reportedFailedTransactions"]=int(failed.group(1)) if failed else None
    summary["failedTransactions"]=summary["reportedFailedTransactions"]
    drained=active.get("drainedSample") or {}
    summary["committedTransactions"]=drained.get("committedTransactions")
    summary["committedUpdateCountSource"]="sum(update_count) from the ten logged client rows after all pgbench sessions drained"
    stop_result=active.get("stopResult") or {}
    before=stop_result.get("serverActivityBeforeStopRequest") or {}
    backends=before.get("backends") or []
    summary["pendingTransactionsAtStopRequest"]=sum(1 for x in backends if x.get("xactAgeSeconds") is not None)
    summary["inFlightActiveSessionsAtStopRequest"]=sum(1 for x in backends if x.get("state")=="active")
    summary["waitEventsAtStopRequest"]=[{"pid":x.get("pid"),"state":x.get("state"),"waitEventType":x.get("waitEventType"),"waitEvent":x.get("waitEvent"),
        "xactAgeSeconds":x.get("xactAgeSeconds"),"queryAgeSeconds":x.get("queryAgeSeconds")} for x in backends]
    summary["unfinishedAtStopSemantics"]="pgbench transaction logs record completed transactions; one-second pg_stat_activity samples preserve pending states, waits, and ages through graceful stop and session drain"
    summary["pgbenchExitCode"]=int(remote_client_status(base,ns,pod,client))
    summary["progressOutput"]="client/raw/pgbench.out"
    summary["rawTransactionLogs"]=[f"client/raw/{p.name}" for p in paths if p.name.startswith("pgbench-log.")]
    summary["validationErrors"]=pgbench_summary_errors(summary)
    save(Path(active["outputDirectory"])/"client-summary.json",summary)
    active.update({"captured":True,"summary":summary,"rawFiles":summary["rawTransactionLogs"]})
    save(Path(active["outputDirectory"])/"client-process.json",{**client,"status":"complete","summaryFile":"client-summary.json"})
    return summary

def pgbench_summary_errors(summary):
    errors=[]
    native=summary.get("completedTransactions")
    logged=summary.get("loggedCompletedTransactions")
    committed=summary.get("committedTransactions")
    failed=summary.get("failedTransactions")
    if native is None: errors.append("pgbench stdout has no native completed-transaction count")
    if failed is None: errors.append("pgbench stdout has no native failed-transaction count")
    elif failed: errors.append(f"pgbench reported {failed} failed transactions")
    if summary.get("malformedLogRows"): errors.append(f"pgbench transaction log has {len(summary['malformedLogRows'])} malformed or failed rows")
    if native is not None and logged is not None and native!=logged:
        errors.append(f"pgbench native completed count {native} differs from logged completed rows {logged}")
    if committed is None: errors.append("server committed-update counter is missing after session drain")
    elif native is not None:
        difference=committed-native
        possible_inflight=max(summary.get("pendingTransactionsAtStopRequest",0),summary.get("inFlightActiveSessionsAtStopRequest",0))
        summary["commitLogReconciliation"]={"serverCommittedUpdates":committed,"pgbenchCompletedTransactions":native,
            "difference":difference,"possibleInflightAtStopRequest":possible_inflight,
            "explainedByInFlightGracefulDrain":0<difference<=possible_inflight}
        if difference<0 or difference>possible_inflight:
            errors.append(f"server committed-update counter {committed} cannot be reconciled with pgbench completed count {native} and {possible_inflight} in-flight clients at stop")
    return errors

def live_vacuum_start_marker(base,ns,pod,work):
    script="cat "+shlex.quote(work+"/vacuum-start.marker")+" 2>/dev/null || true"
    line=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",script],20).strip()
    if not line: return None
    f=line.split("|")
    if len(f)!=6 or f[0]!="VACUUM_START": raise RuntimeError(f"bad VACUUM_START marker: {line!r}")
    return {"time":f[1],"flushLsn":f[2],"walBytes":int(f[3]),"walRecords":int(f[4]),"walFpi":int(f[5])}

def sigterm_replica_walreceiver(base,ns,pod,db):
    query="""WITH r AS (
      SELECT w.pid,a.backend_type,a.application_name,a.backend_start,w.status,w.receive_start_lsn
      FROM pg_stat_wal_receiver w JOIN pg_stat_activity a ON a.pid=w.pid
      WHERE a.backend_type='walreceiver'), n AS (SELECT count(*) AS c FROM r)
      SELECT json_build_object('inRecovery',pg_is_in_recovery(),'receiverCount',n.c,
       'receivers',coalesce((SELECT json_agg(json_build_object('pid',r.pid,'backendType',r.backend_type,'applicationName',r.application_name,
          'backendStart',r.backend_start,'status',r.status,'receiveStartLsn',r.receive_start_lsn)) FROM r),'[]'::json))::text FROM n;"""
    result=sql(base,ns,pod,db,query)
    receivers=result.get("receivers") or []
    if (result.get("inRecovery") is not True or result.get("receiverCount")!=1 or len(receivers)!=1 or
        not isinstance(receivers[0].get("pid"),int) or receivers[0].get("backendType")!="walreceiver" or
        receivers[0].get("status")!="streaming"):
        raise RuntimeError(f"refusing to signal unless exactly one verified replica walreceiver exists: {result}")
    pid=receivers[0]["pid"]
    # The SQL PID is only a candidate. Verify its Linux process identity, re-read
    # the full SQL receiver row, then verify the stable Linux identity again
    # before sending SIGTERM. PostgreSQL can update the walreceiver process title
    # with its streaming LSN between reads, so the two command-line hashes are
    # retained for diagnostics but deliberately are not required to match.
    remote=r'''set -eu
expected_pid=$1
db=$2
case "$expected_pid" in ''|*[!0-9]*) echo 'REFUSED: receiver PID is not numeric' >&2; exit 41;; esac
query="WITH r AS (SELECT w.pid,a.backend_type,a.backend_start,w.status,w.receive_start_lsn FROM pg_stat_wal_receiver w JOIN pg_stat_activity a ON a.pid=w.pid WHERE a.backend_type='walreceiver'), n AS (SELECT count(*) AS c FROM r) SELECT CASE WHEN pg_is_in_recovery() THEN 't' ELSE 'f' END,n.c::text,coalesce((SELECT pid::text FROM r LIMIT 1),''),coalesce((SELECT backend_type FROM r LIMIT 1),''),coalesce((SELECT status FROM r LIMIT 1),''),coalesce((SELECT backend_start::text FROM r LIMIT 1),''),coalesce((SELECT receive_start_lsn::text FROM r LIMIT 1),'') FROM n"
read_receiver() {
    psql -XAt -F '|' -v ON_ERROR_STOP=1 -U postgres -d "$db" -c "$query"
}
refuse() { echo "REFUSED: $*" >&2; exit 41; }
first=$(read_receiver) || refuse 'could not query replica receiver identity'
IFS='|' read -r recovery count pid backend_type status backend_start receive_start_lsn <<EOF
$first
EOF
[ "$recovery" = t ] || refuse 'pod is no longer in recovery'
[ "$count" = 1 ] || refuse "receiver count changed to $count"
[ "$pid" = "$expected_pid" ] || refuse "SQL receiver PID changed from $expected_pid to $pid"
[ "$backend_type" = walreceiver ] || refuse "backend type is $backend_type"
[ "$status" = streaming ] || refuse "receiver status is $status"
read_proc_identity() {
    p="$1"
    [ -r "/proc/$p/status" ] && [ -r "/proc/$p/stat" ] && [ -r "/proc/$p/cmdline" ] || refuse "PID $p has no readable proc identity"
    uid_fields=$(awk '$1 == "Uid:" {print $2, $3, $4, $5; exit}' "/proc/$p/status")
    set -- $uid_fields
    [ "$#" = 4 ] && [ "$1" = "$2" ] && [ "$2" = "$3" ] && [ "$3" = "$4" ] || refuse "PID $p has unexpected real/effective/saved/filesystem UIDs"
    proc_uid=$1
    [ "$proc_uid" = "$(id -u)" ] || refuse "PID $p UID $proc_uid differs from postgres UID $(id -u)"
    stat_rest=$(sed 's/^.*) //' "/proc/$p/stat")
    start_ticks=$(printf '%s\n' "$stat_rest" | awk '{print $20}')
    case "$start_ticks" in ''|*[!0-9]*) refuse "PID $p start ticks are unavailable";; esac
    executable=$(readlink -f "/proc/$p/exe")
    case "$executable" in */postgres) :;; *) refuse "PID $p executable is not postgres: $executable";; esac
    cmdline=$(tr '\000' ' ' < "/proc/$p/cmdline")
    case "$cmdline" in *walreceiver*) :;; *) refuse "PID $p command line is not a walreceiver";; esac
    cmdline_sha=$(printf '%s' "$cmdline" | sha256sum | awk '{print $1}')
    cmdline_b64=$(printf '%s' "$cmdline" | base64 | tr -d '\r\n')
    printf '%s|%s|%s|%s|%s|%s\n' "$p" "$proc_uid" "$start_ticks" "$executable" "$cmdline_sha" "$cmdline_b64"
}
before=$(read_proc_identity "$pid")
second=$(read_receiver) || refuse 'could not recheck replica receiver identity'
[ "$second" = "$first" ] || refuse 'replica receiver SQL identity changed during process verification'
after=$(read_proc_identity "$pid")
before_key=$(printf '%s\n' "$before" | awk -F '|' '{print $1"|"$2"|"$3"|"$4}')
after_key=$(printf '%s\n' "$after" | awk -F '|' '{print $1"|"$2"|"$3"|"$4}')
[ "$after_key" = "$before_key" ] || refuse 'receiver process PID/starttime/UID/executable changed before signal'
requested_at=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
kill -TERM "$pid" || refuse "kill -TERM failed for verified receiver PID $pid"
accepted_at=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
printf 'RECEIVER|%s\nPROCESS_BEFORE|%s\nPROCESS_AFTER|%s\nSIGNAL|SIGTERM|%s|%s\n' "$first" "$before" "$after" "$requested_at" "$accepted_at"
'''
    output=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",remote,"walreceiver-sigterm",str(pid),db],30)
    rows={}
    for line in output.splitlines():
        fields=line.split("|")
        if fields and fields[0] in ("RECEIVER","PROCESS_BEFORE","PROCESS_AFTER","SIGNAL"):
            rows[fields[0]]=fields[1:]
    receiver_row=rows.get("RECEIVER",[])
    before_row=rows.get("PROCESS_BEFORE",[])
    after_row=rows.get("PROCESS_AFTER",[])
    signal_row=rows.get("SIGNAL",[])
    if len(receiver_row)!=7 or len(before_row)!=6 or len(after_row)!=6 or len(signal_row)!=3 or signal_row[0]!="SIGTERM":
        raise RuntimeError(f"walreceiver OS-signal verification returned an incomplete receipt: {output!r}")
    before_identity,after_identity=verified_proc_identity_pair(before_row,after_row,pid)
    return {"inRecovery":True,"receiverCount":1,"receivers":receivers,"pid":pid,"signalSent":True,
        "signal":"SIGTERM","mechanism":"kill -TERM after repeated SQL identity checks, stable /proc PID/starttime/UID/executable checks, and two walreceiver command-line type checks",
        "receiverRecheck":{"inRecovery":receiver_row[0]=="t","count":int(receiver_row[1]),"pid":int(receiver_row[2]),
            "backendType":receiver_row[3],"status":receiver_row[4],"backendStart":receiver_row[5],"receiveStartLsn":receiver_row[6]},
        "processIdentity":{**after_identity,"commandLineBefore":before_identity["commandLine"],
            "commandLineSha256Before":before_identity["commandLineSha256"],
            "commandLineAfter":after_identity["commandLine"],"commandLineSha256After":after_identity["commandLineSha256"]},
        "signalRequestedAtUtc":signal_row[1],"signalAcceptedAtUtc":signal_row[2]}

def parse_proc_identity(fields,expected_pid):
    if len(fields)!=6:
        raise RuntimeError("walreceiver process identity record is incomplete")
    try:
        pid,uid,start_ticks=map(int,fields[:3])
        command_line=base64.b64decode(fields[5],validate=True).decode()
    except Exception as exc:
        raise RuntimeError("walreceiver process identity fields are invalid") from exc
    executable,command_sha=fields[3],fields[4]
    if (pid!=expected_pid or uid<0 or start_ticks<0 or not executable.endswith("/postgres") or
        "walreceiver" not in command_line.lower() or hashlib.sha256(command_line.encode()).hexdigest()!=command_sha):
        raise RuntimeError("walreceiver process identity failed PID, executable, command-line or hash validation")
    return {"pid":pid,"uid":uid,"startTicks":start_ticks,"executable":executable,
        "commandLineSha256":command_sha,"commandLine":command_line}

def verified_proc_identity_pair(before_fields,after_fields,expected_pid):
    before=parse_proc_identity(before_fields,expected_pid)
    after=parse_proc_identity(after_fields,expected_pid)
    stable_keys=("pid","uid","startTicks","executable")
    if any(before[key]!=after[key] for key in stable_keys):
        raise RuntimeError("walreceiver process stable PID/UID/starttime/executable identity changed before signal")
    return before,after

def execute_intervention(base,ns,replica,db,mode,marker,scheduled_epoch,after_seconds):
    scheduled=dt.datetime.fromtimestamp(scheduled_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z")
    if mode=="sigterm":
        try:
            signal_result=sigterm_replica_walreceiver(base,ns,replica,db); status="sent"; error=None
        except Exception as exc:
            signal_result={"signal":"SIGTERM","signalSent":False,"error":str(exc)}; status="failed"; error=str(exc)
        executed_at=signal_result.get("signalRequestedAtUtc") if signal_result.get("signalSent") is True else None
        result={"mode":"sigterm","status":status,"scheduledAtUtc":scheduled,"executedAtUtc":executed_at,"vacuumStartMarker":marker,
            "afterVacuumStartSeconds":after_seconds,"signalResult":signal_result,
            "causalNote":"SIGTERM of the replica walreceiver simulates a stream disconnect; it does not itself cause a primary wal_sender_timeout."}
        return result,error
    receiver_error=None
    try: receiver=sql(base,ns,replica,db,"SELECT json_build_object('observedAtUtc',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),'inRecovery',pg_is_in_recovery(),'receiver',coalesce((SELECT json_build_object('pid',pid,'status',status,'writtenLsn',written_lsn,'flushedLsn',flushed_lsn,'latestEndLsn',latest_end_lsn) FROM pg_stat_wal_receiver LIMIT 1),'{}'::json))::text;")
    except Exception as exc: receiver={}; receiver_error=str(exc)
    result={"mode":"control","status":"failed" if receiver_error else "marked","scheduledAtUtc":scheduled,"executedAtUtc":receiver.get("observedAtUtc"),"vacuumStartMarker":marker,
        "afterVacuumStartSeconds":after_seconds,"signalResult":None,"receiverAtControlMarker":receiver,"receiverSampleError":receiver_error,
        "causalNote":"Control marker records the matched +600-second point; no backend is signaled."}
    return result,receiver_error

class InterventionScheduler:
    """Run the one-shot intervention at a marker-derived monotonic deadline."""
    def __init__(self,mode,marker,scheduled_epoch,after_seconds,action,*,wall_clock=time.time,monotonic=time.monotonic):
        self.mode=mode; self.marker=marker; self.scheduled_epoch=scheduled_epoch; self.after_seconds=after_seconds
        self.action=action; self.wall_clock=wall_clock; self.monotonic=monotonic
        self._lock=threading.Lock(); self._cancel=threading.Event(); self._done=threading.Event()
        self._result=None; self._error=None; self._started=False; self._joined=False; self._thread=None
        self.started_at_utc=None; self.deadline_monotonic=None; self.initial_wait_seconds=None

    def start(self):
        with self._lock:
            if self._started: raise RuntimeError("intervention scheduler may only be started once")
            self._started=True
            now_mono=self.monotonic(); now_wall=self.wall_clock()
            self.initial_wait_seconds=max(0.0,self.scheduled_epoch-now_wall)
            self.deadline_monotonic=now_mono+self.initial_wait_seconds
            self.started_at_utc=dt.datetime.fromtimestamp(now_wall,dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")
            self._thread=threading.Thread(target=self._run,name="walreceiver-intervention",daemon=True)
            self._thread.start()
        return self

    def _finish(self,result,error):
        with self._lock:
            self._result=result; self._error=error; self._done.set()

    def _run(self):
        try:
            while True:
                remaining=self.deadline_monotonic-self.monotonic()
                if remaining<=0: break
                if self._cancel.wait(remaining):
                    scheduled=dt.datetime.fromtimestamp(self.scheduled_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z")
                    self._finish({"mode":self.mode,"status":"cancelled_before_dispatch","scheduledAtUtc":scheduled,
                        "executedAtUtc":None,"vacuumStartMarker":self.marker,"afterVacuumStartSeconds":self.after_seconds,
                        "signalResult":None,"schedulerStartedAtUtc":self.started_at_utc},
                        "intervention scheduler was cancelled before dispatch")
                    return
            if self._cancel.is_set():
                scheduled=dt.datetime.fromtimestamp(self.scheduled_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z")
                self._finish({"mode":self.mode,"status":"cancelled_before_dispatch","scheduledAtUtc":scheduled,
                    "executedAtUtc":None,"vacuumStartMarker":self.marker,"afterVacuumStartSeconds":self.after_seconds,
                    "signalResult":None,"schedulerStartedAtUtc":self.started_at_utc},
                    "intervention scheduler was cancelled before dispatch")
                return
            dispatch_epoch=self.wall_clock()
            result,error=self.action()
            result=dict(result)
            result["schedulerStartedAtUtc"]=self.started_at_utc
            result["schedulerDispatchStartedAtUtc"]=dt.datetime.fromtimestamp(dispatch_epoch,dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")
            result["schedulerDispatchLatenessSeconds"]=max(0.0,dispatch_epoch-self.scheduled_epoch)
            self._finish(result,error)
        except Exception as exc:
            scheduled=dt.datetime.fromtimestamp(self.scheduled_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z")
            self._finish({"mode":self.mode,"status":"failed","scheduledAtUtc":scheduled,"executedAtUtc":None,
                "vacuumStartMarker":self.marker,"afterVacuumStartSeconds":self.after_seconds,"signalResult":None,
                "schedulerStartedAtUtc":self.started_at_utc,"schedulerError":str(exc)},str(exc))

    def ready_result(self):
        if not self._done.is_set(): return None
        with self._lock: return self._result,self._error

    def cancel(self):
        self._cancel.set()

    def join_once(self,timeout=None):
        with self._lock:
            if not self._started: return True
            if self._joined: return self._thread is None or not self._thread.is_alive()
            self._joined=True
            thread=self._thread
        thread.join(timeout)
        return not thread.is_alive()

def app_secret_credentials(base,ns,cluster,uid,db,pod):
    name=cluster+"-app"; secret=json.loads(cmd(base+["get","secret",name,"-n",ns,"-o","json"],30))
    meta=secret.get("metadata") or {}; owners=meta.get("ownerReferences") or []
    if secret.get("type")!="kubernetes.io/basic-auth" or not any(o.get("kind")=="Cluster" and o.get("name")==cluster and (not uid or o.get("uid")==uid) for o in owners):
        raise RuntimeError("CNPG app Secret ownership/type check failed")
    def val(key):
        try: return base64.b64decode(secret["data"][key],validate=True).decode()
        except Exception: raise RuntimeError(f"CNPG app Secret has invalid {key}") from None
    values={k:val(k) for k in ("username","dbname","port","password")}
    if values["username"]!="app" or values["dbname"]!=db or not values["port"].isdigit() or not values["password"] or any(c in values["password"] for c in "\0\n\r"):
        raise RuntimeError("CNPG app Secret does not identify the expected client credentials")
    secret_name=name
    directory="/controller/tmp/pgbench-auth-"+uuid.uuid4().hex; path=directory+"/pgpass"
    esc=lambda x:str(x).replace("\\","\\\\").replace(":","\\:")
    content=":".join(esc(x) for x in ("127.0.0.1",values["port"],db,"app",values["password"]))+"\n"
    sh=f"set -eu; umask 077; d={shlex.quote(directory)}; p={shlex.quote(path)}; mkdir -m 700 \"$d\"; cat >\"$p\"; chmod 600 \"$p\"; printf '%s\\n' \"$p\""
    try:
        proc=subprocess.run(base+["exec","-i","-c","postgres","-n",ns,pod,"--","sh","-c",sh],input=content,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
    finally:
        content=""; values["password"]=""
    if proc.returncode or proc.stdout.strip()!=path:
        try: cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c","rm -rf -- "+shlex.quote(directory)],20)
        except Exception: pass
        raise RuntimeError("private pgpass install failed verification")
    return {"secretName":secret_name,"username":"app","database":db,"port":int(values["port"]),"pgpassFilePath":path,
        "pgpassDirectory":directory,"pgpassMode":"0600","directoryMode":"0700"}

def remove_pgpass(base,ns,pod,credential):
    d=shlex.quote(credential["pgpassDirectory"]); p=shlex.quote(credential["pgpassFilePath"])
    cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c",f"set -eu; d={d}; p={p}; if [ -d \"$d\" ]; then rm -f -- \"$p\"; rmdir -- \"$d\"; fi"],30)


def cleanup_pre_vacuum_failure():
    ctx=RUN_CONTEXT
    if not ctx or ctx.get("vacuumStarted"): return []
    errors=[]; base=ctx.get("base"); ns=ctx.get("namespace"); pod=ctx.get("primary")
    client=ctx.get("client"); monitor=ctx.get("monitor"); credential=ctx.get("credential"); out=ctx.get("out")
    if client and base and ns and pod:
        try:
            status=remote_client_status(base,ns,pod,client)
            stop_result=stop_pgbench(base,ns,pod,client,900) if status=="RUNNING" else {"status":status,"alreadyExited":True,"finishedAtUtc":stamp()}
            drained=None; end=time.monotonic()+120
            while monitor and time.monotonic()<end:
                sample=monitor.latest()
                if sample and sample.get("sampleStatus")=="ok" and sample.get("benchSessions")==0: drained=sample; break
                time.sleep(.25)
            if drained:
                active={"client":client,"base":base,"namespace":ns,"pod":pod,"outputDirectory":str(out),
                    "stopResult":stop_result,"drainedSample":drained}
                capture_pgbench_results(active)
            save(Path(out)/"client-cleanup-after-pre-vacuum-failure.json",{"stop":stop_result,"drained":drained})
        except Exception as exc: errors.append(f"pre-VACUUM pgbench cleanup failed: {exc}")
    if monitor:
        try:
            if not monitor.stop(): errors.append("pre-VACUUM client sampler did not stop")
        except Exception as exc: errors.append(f"pre-VACUUM sampler cleanup failed: {exc}")
    if credential and base and ns and pod:
        try: remove_pgpass(base,ns,pod,credential)
        except Exception as exc: errors.append(f"pre-VACUUM pgpass cleanup failed: {exc}")
    if client and base and ns and pod:
        try: cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c","rm -rf -- "+shlex.quote(client["workDirectory"])],30)
        except Exception as exc: errors.append(f"pre-VACUUM pgbench temporary cleanup failed: {exc}")
    return errors

def run(a):
    RUN_CONTEXT.clear()
    out=Path(a.out).resolve(); out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()): raise RuntimeError(f"output directory not empty: {out}")
    infra=json.loads((STATE/"state.json").read_text()); prep=json.loads((STATE/"prepare.json").read_text())
    if infra.get("status")!="ready" or prep.get("status")!="complete": raise RuntimeError("stack not ready/prepared")
    cfg=Path(a.kubeconfig or infra.get("kubeconfigPath") or STATE/"kubeconfig").resolve()
    if not cfg.is_file(): raise RuntimeError(f"kubeconfig missing: {cfg}")
    pair=prep.get("cluster","repro/hdd-pg").split("/"); ns,name=pair if len(pair)==2 else ("repro","hdd-pg")
    base=[a.kubectl,"--kubeconfig",str(cfg)]; raw=out/"samples.jsonl"; events=out/"events.jsonl"; statefile=STATE/"test-state.json"
    token=infra.get("ownerToken")
    if not token: raise RuntimeError("infra ownership token missing")
    ns_obj=json.loads(cmd(base+["get","namespace",ns,"-o","json"]))
    cluster_obj=json.loads(cmd(base+["get","cluster",name,"-n",ns,"-o","json"]))
    if ns_obj.get("metadata",{}).get("labels",{}).get("repro.stack.token")!=token or cluster_obj.get("metadata",{}).get("labels",{}).get("repro.stack.token")!=token:
        raise RuntimeError("namespace/Cluster ownership token mismatch")
    if cluster_obj.get("spec",{}).get("instances")!=2: raise RuntimeError("expected a two-instance CNPG cluster")
    def state(phase,**x): save(statefile,{"runId":out.name,"phase":phase,"output":str(out),"updatedAt":stamp(),**x})
    def mark(event_name,/,**x): event(events,event_name,**x)
    state("preflight")
    jobs=completed_cnpg_jobs(json.loads(cmd(base+["get","jobs","-n",ns,"-o","json"]))["items"],name)
    items=json.loads(cmd(base+["get","pods","-n",ns,"-l","cnpg.io/cluster="+name,"-o","json"]))["items"]
    pods,completed_job_pods=ready_cluster_instance_pods(items,name,jobs)
    if pods["primary"]["spec"].get("nodeName")==pods["replica"]["spec"].get("nodeName"): raise RuntimeError("pods must use separate nodes")
    layout=storage(base,ns,pods); fixture={}
    candidates=((STATE/"prepared.json"),) if prep.get("sourceKind")=="fresh-initdb-seed" else (HERE/"snapshot.json",STATE/"snapshot.json")
    for f in candidates:
        if f.exists(): fixture=json.loads(f.read_text()); break
    for k in ("systemIdentifier","heapBytes","tableBytes"):
        if prep.get(k) is not None: fixture[k]=prep[k]
    meta={}
    for role,pod in pods.items():
        m=sql(base,ns,pod["metadata"]["name"],a.database,GUC_SQL); meta[role]=m
        if not m["version"].startswith("16.15") or m["recovery"]!=(role=="replica"): raise RuntimeError(f"{role}: version/role mismatch")
        if m["appSessions"] or (fixture.get("systemIdentifier") and m["systemId"]!=str(fixture["systemIdentifier"])): raise RuntimeError(f"{role}: app session/system ID mismatch")
        settings={k:v["value"] for k,v in m["settings"].items()}
        expected={"wal_level":"logical","full_page_writes":"on","recovery_prefetch":"try","vacuum_cost_delay":"0",
          "maintenance_io_concurrency":"10","track_io_timing":"on","track_wal_io_timing":"on","shared_buffers":"256MB",
          "autovacuum":"on","max_wal_size":"1GB","checkpoint_timeout":"5min","statement_timeout":"0"}
        for k,v in expected.items():
            if str(settings.get(k))!=v: raise RuntimeError(f"{role}: expected {k}={v}, found {settings.get(k)}")
        t=m["table"]
        minimum_heap=minimum_fixture_heap(fixture.get("heapBytes"))
        if not t or int(t["heapBytes"])<minimum_heap or "autovacuum_enabled=false" not in (t["reloptions"] or []) or t["tablespaceOverrides"] or m["defaultTablespaceIoConcurrencyOverride"]:
            raise RuntimeError(f"{role}: table size/autovacuum/tablespace gate failed")
        for key,actual in (("heapBytes",t["heapBytes"]),("tableBytes",t["totalBytes"])):
            if fixture.get(key) and abs(int(actual)-int(fixture[key]))>int(fixture[key])*.03: raise RuntimeError(f"{role}: {key} differs from prepared source by over 3%")
    if meta["primary"]["systemId"]!=meta["replica"]["systemId"] or abs(int(meta["primary"]["table"]["heapBytes"])-int(meta["replica"]["table"]["heapBytes"]))>64*1024**2:
        raise RuntimeError("primary/replica system ID or heap differs")
    client_fixture=setup_client_fixture(base,ns,pods["primary"]["metadata"]["name"],a.database)
    pgbench_runtime=check_pgbench_runtime(base,ns,pods["primary"]["metadata"]["name"])
    bench_prefix=a.client_app_name
    bench_app=f"{bench_prefix}{a.intervention_mode}-{uuid.uuid4().hex[:10]}"
    credential=app_secret_credentials(base,ns,name,cluster_obj["metadata"].get("uid"),a.database,pods["primary"]["metadata"]["name"])
    RUN_CONTEXT.update({"base":base,"namespace":ns,"primary":pods["primary"]["metadata"]["name"],
        "database":a.database,"credential":credential,"out":out,"client":None,"monitor":None,"vacuumStarted":False})
    verify_client_auth(base,ns,pods["primary"]["metadata"]["name"],a.database,credential)
    save(out/"preflight.json",{"prepare":prep,"pods":{r:{"name":p["metadata"]["name"],"node":p["spec"]["nodeName"],"uid":p["metadata"]["uid"],"claims":pod_claims(p)} for r,p in pods.items()},
        "sql":meta,"storage":layout,"completedCnpgBootstrapJobs":sorted(jobs),"completedCnpgBootstrapJobPods":completed_job_pods,
        "fixture":fixture,"clientCreated":True,"clientFixture":client_fixture})
    mark("preflight_complete",systemId=meta["primary"]["systemId"],heapBytes=meta["primary"]["table"]["heapBytes"],allowedJobs=sorted(jobs),allowedJobPods=completed_job_pods)

    client_fixture_meta={"tool":"pgbench","version":pgbench_runtime["version"],"path":pgbench_runtime["path"],
        "targetTps":BENCH_TPS,"clients":BENCH_CLIENTS,"warmupSeconds":BENCH_WARMUP_SECONDS,
        "postRecoveryHealthySeconds":BENCH_POST_RECOVERY_SECONDS,"transactionFile":"client/transaction.sql",
        "transactionSha256":hashlib.sha256((CLIENT_DIR/"transaction.sql").read_bytes()).hexdigest(),
        "setupFile":"client/setup.sql","setupSha256":hashlib.sha256((CLIENT_DIR/"setup.sql").read_bytes()).hexdigest(),
        "fixture":client_fixture,"applicationName":bench_app,
        "applicationNamePrefix":bench_prefix,"database":a.database,"user":"app","synchronousCommit":"on",
        "statementTimeoutSeconds":0,"noVacuum":True,"latencyLimit":None,"maxTries":1,"protocol":"simple","randomSeed":42,
        "credential":{"secretName":credential["secretName"],"username":"app","database":a.database,"port":credential["port"],
           "pgpassFilePath":credential["pgpassFilePath"],"pgpassMode":"0600","directoryMode":"0700","transport":"TCP"},
        "stopSignal":"SIGALRM","stopSignalSemantics":"pgbench duration handler drains active transactions"}
    save(out/"client-config.json",client_fixture_meta)
    mark("pgbench_runtime_verified",**pgbench_runtime)
    mark("client_fixture_created",fixture=client_fixture)

    primary,replica=pods["primary"]["metadata"]["name"],pods["replica"]["metadata"]["name"]
    wal_before=sql(base,ns,primary,a.database,WAL_SQL)
    cmd(base+["exec","-i","-c","postgres","-n",ns,primary,"--","psql","-X","-v","ON_ERROR_STOP=1","-U","postgres","-d",a.database],600,"CHECKPOINT;\n")
    wal_cp=sql(base,ns,primary,a.database,WAL_SQL); target=wal_cp["flushLsn"]
    mark("checkpoint_complete",targetLsn=target,before=wal_before,after=wal_cp)
    end=time.monotonic()+a.catchup_hours*3600
    while time.monotonic()<end:
        r=sql(base,ns,replica,a.database,"SELECT json_build_object('recovery',pg_is_in_recovery(),'replayLsn',pg_last_wal_replay_lsn())::text;")
        if not r["recovery"]: raise RuntimeError("replica role changed during checkpoint replay")
        if r["replayLsn"] and lsn(r["replayLsn"])>=lsn(target): break
        time.sleep(a.interval)
    else: raise RuntimeError(f"replica did not replay checkpoint LSN {target}")
    client_dir=out/"client"; client_dir.mkdir(parents=True,exist_ok=True)
    monitor=ClientMonitor(base,ns,primary,a.database,bench_app,client_dir/"sessions.jsonl",1.0); monitor.start()
    RUN_CONTEXT["monitor"]=monitor
    client_duration=int(max((a.safety_hours+2*a.catchup_hours+2)*3600,a.minimum_observation_seconds+7200))
    client=launch_pgbench(base,ns,primary,a.database,bench_app,credential,client_duration,BENCH_TPS,BENCH_CLIENTS)
    RUN_CONTEXT["client"]=client
    save(out/"client-process.json",client)
    mark("client_started",applicationName=bench_app,applicationNamePrefix=bench_prefix,pgbenchPid=client["pgbenchPid"],
        workDirectory=client["workDirectory"],targetTps=BENCH_TPS,clients=BENCH_CLIENTS,durationLimitSeconds=client_duration)
    state("client_warmup",applicationName=bench_app,targetTps=BENCH_TPS,clients=BENCH_CLIENTS)
    warmup=wait_for_warmup(base,ns,primary,client,monitor,BENCH_WARMUP_SECONDS,BENCH_TPS)
    save(out/"client-warmup.json",warmup); mark("client_warmup_complete",**warmup)
    checkpoint_result=pre_vacuum_checkpoint(base,ns,primary,replica,a.database,client,monitor,a.catchup_hours,a.interval,mark)
    wal_start=sql(base,ns,primary,a.database,WAL_SQL); app="vacuum-only-"+uuid.uuid4().hex
    first=both(base,ns,a.database,app,bench_app,pods,layout)
    if first["primary"]["benchSessions"]!=BENCH_CLIENTS or first["replica"]["benchSessions"]!=0:
        raise RuntimeError("pgbench sessions were not confined to the primary before VACUUM")
    rows=[first]; append(raw,{"seq":0,"phase":"pre_vacuum_initial",**first})
    mark("pre_vacuum_initial_sample",sampleTime=first["primary"]["ts"],targetLsn=checkpoint_result["targetLsn"],
        primaryFlushLsn=first["primary"]["flushLsn"],replicaReplayLsn=first["replica"]["replayLsn"],pgbenchSessions=BENCH_CLIENTS,walCounters=wal_start)
    vacuum=vacuum_spec(prep)
    vacuum_launch_epoch=time.time()
    work,pid=launch_vacuum(base,ns,primary,a.database,app,int(a.safety_hours*3600),vacuum["sql"])
    RUN_CONTEXT.update({"vacuumStarted":True,"vacuumWork":work,"vacuumApplicationName":app})
    save(out/"vacuum-process.json",{"applicationName":app,"pod":primary,"pid":pid,"workDirectory":work,"status":"RUNNING",**vacuum})
    state("vacuum",applicationName=app,remotePid=pid); mark("observation_started",applicationName=app,interval=a.interval,vacuumOptions=vacuum["options"],vacuumSql=vacuum["sql"])
    seq=1; status="RUNNING"; failure=None; first_progress=last_progress=first_null=heap_complete=None; phase=None; last_progress_row=None
    intervention_complete=False; intervention=None; vacuum_marker=None; vacuum_start_epoch=None; minimum_end_epoch=None; intervention_due_epoch=None
    intervention_scheduler=None
    deadline=time.monotonic()+a.safety_hours*3600
    def schedule_intervention():
        nonlocal intervention_scheduler
        if intervention_scheduler is not None or not vacuum_marker or intervention_due_epoch is None: return
        action=lambda: execute_intervention(base,ns,replica,a.database,a.intervention_mode,
            vacuum_marker,intervention_due_epoch,a.intervention_after_seconds)
        intervention_scheduler=InterventionScheduler(a.intervention_mode,vacuum_marker,intervention_due_epoch,
            a.intervention_after_seconds,action).start()
        mark("walreceiver_intervention_scheduled",mode=a.intervention_mode,scheduledAtUtc=
            dt.datetime.fromtimestamp(intervention_due_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z"),
            schedulerStartedAtUtc=intervention_scheduler.started_at_utc,
            initialWaitSeconds=intervention_scheduler.initial_wait_seconds,
            deadlineSource="remote VACUUM_START marker epoch converted once to local monotonic deadline")
    def harvest_intervention():
        nonlocal intervention_complete,intervention,failure
        if intervention_complete or intervention_scheduler is None: return
        ready=intervention_scheduler.ready_result()
        if ready is None: return
        intervention,intervention_error=ready
        intervention_complete=True
        save(out/"walreceiver-intervention.json",intervention)
        mark("walreceiver_intervention",**intervention)
        if intervention_error:
            failure=failure or f"walreceiver intervention failed: {intervention_error}"
            mark("walreceiver_intervention_failed",mode=a.intervention_mode,error=intervention_error,
                 scheduledAtUtc=intervention.get("scheduledAtUtc"))
    try:
        next_at=time.monotonic()
        while (status=="RUNNING" or not intervention_complete) and time.monotonic()<deadline:
            time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
            harvest_intervention()
            try:
                check_pods(base,ns,name,pods)
                row=both(base,ns,a.database,app,bench_app,pods,layout); same_map(first,row)
            except Exception as sample_error:
                mark("sample_attempt_failed",phase="vacuum",error=str(sample_error))
                try: status=remote_status(base,ns,primary,work)
                except Exception as status_error: mark("vacuum_status_read_failed",error=str(status_error))
                continue
            rows.append(row); append(raw,{"seq":seq,"phase":"vacuum",**row}); seq+=1
            if not vacuum_marker:
                try: vacuum_marker=live_vacuum_start_marker(base,ns,primary,work)
                except Exception as marker_error:
                    mark("vacuum_start_marker_poll_failed",error=str(marker_error))
                    try: status=remote_status(base,ns,primary,work)
                    except Exception: pass
                    continue
                if vacuum_marker:
                    vacuum_start_epoch=dt.datetime.fromisoformat(vacuum_marker["time"].replace("Z","+00:00")).timestamp()
                    intervention_due_epoch=vacuum_start_epoch+a.intervention_after_seconds
                    minimum_end_epoch=vacuum_start_epoch+a.minimum_observation_seconds
                    mark("vacuum_start_marker_observed",marker=vacuum_marker,source="remote psql marker written immediately before VACUUM statement",
                        interventionDueUtc=dt.datetime.fromtimestamp(intervention_due_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z"),
                        minimumObservationEndUtc=dt.datetime.fromtimestamp(minimum_end_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z"))
                    schedule_intervention()
                    harvest_intervention()
            p=row["primary"]["vacuumProgress"]; ph=p.get("phase") if p else None
            if ph:
                first_progress=first_progress or row["primary"]["ts"]; last_progress=row["primary"]["ts"]; last_progress_row=p
                if ph!=phase: mark("vacuum_progress_phase",time=row["primary"]["ts"],phase=ph,progress=p); phase=ph
            elif first_progress and p and p.get("backendState"):
                if not first_null:
                    first_null=row["primary"]["ts"]
                    mark("vacuum_progress_first_null",time=first_null,backendState=p.get("backendState"),progress=p)
                if (not heap_complete and last_progress_row and
                    int(last_progress_row.get("heapBlksTotal") or 0)>0 and
                    int(last_progress_row.get("heapBlksScanned") or 0)>=int(last_progress_row.get("heapBlksTotal") or 1)):
                    heap_complete=row["primary"]["ts"]
                    mark("heap_work_complete_observed",time=heap_complete,progress=p,
                         lastProgress=last_progress_row,replication={"senders":row["primary"]["senders"],
                         "receiver":row["replica"]["receiver"],"replayLsn":row["replica"]["replayLsn"],
                         "flushLsn":row["primary"]["flushLsn"]})
            if status=="RUNNING":
                try: status=remote_status(base,ns,primary,work)
                except Exception as status_error: mark("vacuum_status_read_failed",error=str(status_error))
        if status=="RUNNING":
            failure=f"VACUUM exceeded {a.safety_hours:g}h"; mark("safety_bound_cancel",result=cancel_owned(base,ns,primary,a.database,app))
            stop=time.monotonic()+180
            while status=="RUNNING" and time.monotonic()<stop: time.sleep(3); status=remote_status(base,ns,primary,work)
    except Exception as e:
        failure=str(e); mark("observation_failed",error=failure)
        mark("vacuum_left_running_after_observation_error",applicationName=app,reason="avoid injecting an early query cancel into the WAL/client observation window")
    # A transient sampler or marker error must not shorten the client window or
    # cancel VACUUM. Keep observing until VACUUM exits, the one-shot marker is
    # handled, and the required post-start observation duration has elapsed.
    fallback_end_epoch=minimum_end_epoch or (vacuum_launch_epoch+a.minimum_observation_seconds+60)
    next_fallback=time.monotonic()
    while time.monotonic()<deadline and (status=="RUNNING" or (vacuum_marker and not intervention_complete) or time.time()<fallback_end_epoch):
        time.sleep(max(0,next_fallback-time.monotonic())); next_fallback+=a.interval
        harvest_intervention()
        if not vacuum_marker:
            try:
                vacuum_marker=live_vacuum_start_marker(base,ns,primary,work)
                if vacuum_marker:
                    vacuum_start_epoch=dt.datetime.fromisoformat(vacuum_marker["time"].replace("Z","+00:00")).timestamp()
                    intervention_due_epoch=vacuum_start_epoch+a.intervention_after_seconds
                    minimum_end_epoch=vacuum_start_epoch+a.minimum_observation_seconds
                    fallback_end_epoch=minimum_end_epoch
                    mark("vacuum_start_marker_observed",marker=vacuum_marker,source="remote psql COPY marker written immediately before VACUUM statement")
                    schedule_intervention()
            except Exception as e: mark("vacuum_start_marker_poll_failed",error=str(e))
        harvest_intervention()
        try:
            check_pods(base,ns,name,pods)
            row=both(base,ns,a.database,app,bench_app,pods,layout); same_map(first,row)
            rows.append(row); append(raw,{"seq":seq,"phase":"minimum_observation_hold",**row}); seq+=1
        except Exception as e: mark("sample_attempt_failed",phase="minimum_observation_hold",error=str(e))
        try: status=remote_status(base,ns,primary,work)
        except Exception as e: mark("vacuum_status_read_failed",error=str(e))
        harvest_intervention()
    if intervention_scheduler is not None:
        if (intervention_scheduler.ready_result() is None and intervention_scheduler.deadline_monotonic is not None
                and intervention_scheduler.deadline_monotonic>time.monotonic()):
            intervention_scheduler.cancel()
        joined=intervention_scheduler.join_once(timeout=75)
        if not joined:
            intervention_scheduler.cancel()
            failure=failure or "walreceiver intervention worker did not finish before the 75s join bound"
            mark("walreceiver_intervention_worker_join_timeout")
        harvest_intervention()
    rc=int(status) if status.isdecimal() else None
    save(out/"vacuum-process.json",{"applicationName":app,"pod":primary,"pid":pid,"workDirectory":work,"status":status,"remoteExitCode":rc})
    marks={}
    try: marks=fetch_log(base,ns,primary,work,out/"vacuum.log")
    except Exception as e: failure=failure or f"VACUUM log fetch failed: {e}"
    start,complete=marks.get("start"),marks.get("complete")
    if start: mark("vacuum_started",marker=start)
    if complete: mark("vacuum_completed",marker=complete)
    if not (rc==0 and start and complete): failure=failure or f"VACUUM not confirmed complete (exit={rc})"
    wal_exit=wal_delta=None
    if rc==0:
        try:
            wal_exit=sql(base,ns,primary,a.database,WAL_SQL)
            if wal_start["statsReset"]==wal_exit["statsReset"]:
                wal_delta={k:int(wal_exit[k])-int(wal_start[k]) for k in ("walBytes","walRecords","walFpi")}
                if min(wal_delta.values())<0: wal_delta=None
        except Exception as e: mark("wal_counter_read_failed",error=str(e))
    caught=False; client_target_caught=False; post_recovery_healthy=False; post_recovery_event_written=False; replay_end=None
    replay=flush=None; tail_error=None; drain_started=None; stable=0; healthy_since=None; post_recovery_seconds=0
    client_stop_result=None; client_summary=None; final_caught=False; final_replay_end=None; final_target=None; final_drain_started=None
    client_complete=False; client_stopped=False
    minimum_end_epoch=minimum_end_epoch or (time.time()+a.minimum_observation_seconds)
    if complete and rc==0 and intervention_complete:
        target=complete.get("flushLsn")
        if not target:
            failure=failure or "VACUUM completion marker has no flush LSN"; tail_error=failure
        else:
            state("replay_drain_client_active",targetLsn=target,timeoutHours=a.catchup_hours,
                  minimumObservationEndUtc=dt.datetime.fromtimestamp(minimum_end_epoch,dt.timezone.utc).isoformat().replace("+00:00","Z"))
            mark("replay_drain_started",targetLsn=target,timeoutHours=a.catchup_hours,clientStillRunning=True)
            drain_started=time.monotonic(); max_wait=max(a.catchup_hours*3600,minimum_end_epoch-time.time()+BENCH_POST_RECOVERY_SECONDS+300)
            stop_at=drain_started+max_wait; next_at=drain_started
            try:
                while time.monotonic()<stop_at:
                    now=time.time(); top_client_health=monitor.progress_health(BENCH_CLIENTS,4.0)
                    tail_ok=healthy_since is not None and time.monotonic()-healthy_since>=BENCH_POST_RECOVERY_SECONDS and top_client_health["healthy"]
                    if now>=minimum_end_epoch and tail_ok:
                        caught=True; client_target_caught=True; post_recovery_healthy=True
                        post_recovery_seconds=time.monotonic()-healthy_since; replay_end=rows[-1]["primary"]["ts"]
                        if not post_recovery_event_written:
                            event_time=(top_client_health.get("latest") or {}).get("observedAtUtc") or rows[-1]["primary"]["ts"]
                            mark("client_post_recovery_healthy_complete",time=event_time,targetLsn=target,
                                healthySeconds=post_recovery_seconds,clientHealth=top_client_health)
                            post_recovery_event_written=True
                        break
                    time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
                    if time.monotonic()>=stop_at: break
                    try:
                        check_pods(base,ns,name,pods)
                        row=both(base,ns,a.database,app,bench_app,pods,layout); same_map(first,row)
                    except Exception as sample_error:
                        mark("sample_attempt_failed",phase="replay_drain_client_active",error=str(sample_error))
                        continue
                    if row["primary"]["benchSessions"]!=BENCH_CLIENTS or row["replica"]["benchSessions"]!=0:
                        raise RuntimeError("pgbench persistent-session placement changed during replica recovery")
                    rows.append(row); append(raw,{"seq":seq,"phase":"replay_drain_client_active",**row}); seq+=1
                    replay=row["replica"]["replayLsn"]; flush=row["primary"]["flushLsn"]
                    client_health=monitor.progress_health(BENCH_CLIENTS,4.0)
                    append(out/"client-recovery-health.jsonl",{"time":row["primary"]["ts"],"replicaTargetHealthy":replica_reached_vacuum_target(
                        row["primary"],row["replica"],target,BENCH_CLIENTS),**client_health})
                    healthy=replica_reached_vacuum_target(row["primary"],row["replica"],target,BENCH_CLIENTS) and client_health["healthy"]
                    stable=next_catchup_streak(stable,healthy)
                    if stable>=3 and healthy_since is None:
                        client_target_caught=True; healthy_since=time.monotonic(); replay_end=row["primary"]["ts"]
                        mark("replica_reached_vacuum_target_with_client",time=replay_end,targetLsn=target,
                             primaryFlushLsn=flush,replicaReplayLsn=replay,consecutiveHealthySamples=stable,
                             clientSessions=row["primary"]["benchSessions"])
                    if healthy_since is not None and not healthy:
                        mark("client_post_recovery_health_reset",time=row["primary"]["ts"],targetLsn=target,
                             primaryFlushLsn=flush,replicaReplayLsn=replay,healthySeconds=time.monotonic()-healthy_since,
                             clientHealth=client_health)
                        healthy_since=None; stable=0; post_recovery_healthy=False; post_recovery_seconds=0; post_recovery_event_written=False
                    elif healthy_since is not None and time.monotonic()-healthy_since>=BENCH_POST_RECOVERY_SECONDS:
                        post_recovery_healthy=True; post_recovery_seconds=time.monotonic()-healthy_since
                if not client_target_caught:
                    failure=failure or f"replica did not replay VACUUM completion target {target} with the client active within {a.catchup_hours:g}h"
                    tail_error=failure; mark("replay_drain_timeout",targetLsn=target,timeoutHours=a.catchup_hours,
                        replayLsn=replay,primaryFlushLsn=flush,clientStillRunning=True)
                elif not post_recovery_healthy:
                    failure=failure or f"replica did not remain healthy for {BENCH_POST_RECOVERY_SECONDS}s after reaching VACUUM LSN {target}"
                    tail_error=failure; mark("client_post_recovery_health_timeout",targetLsn=target,timeoutHours=a.catchup_hours)
                if time.time()<minimum_end_epoch:
                    failure=failure or "minimum client observation interval ended before its required wall-clock boundary"
                    tail_error=failure
            except Exception as e:
                tail_error=str(e); failure=failure or f"client-active replay observation failed: {e}"
                mark("replay_drain_failed",error=tail_error,targetLsn=target)

    # Gracefully stop only after the minimum window and recovery tail; never cancel
    # an in-flight client transaction to manufacture an apparent stall.
    try:
        if remote_client_status(base,ns,primary,client)=="RUNNING":
            activity_before_stop=monitor.latest()
            client_stop_result=stop_pgbench(base,ns,primary,client,900)
            client_stop_result["serverActivityBeforeStopRequest"]=activity_before_stop
        else:
            observed_exit=stamp()
            client_stop_result={"requestedAtUtc":observed_exit,"status":remote_client_status(base,ns,primary,client),
                "alreadyExited":True,"finishedAtUtc":observed_exit,
                "stopBoundarySemantics":"pgbench had already exited; timestamp records observation of the completed client boundary"}
        drain_limit=time.monotonic()+120; drained=None
        while time.monotonic()<drain_limit:
            candidate=monitor.latest()
            if candidate and candidate.get("sampleStatus")=="ok" and candidate.get("benchSessions")==0:
                drained=candidate; break
            time.sleep(.25)
        if drained is None: raise RuntimeError("pgbench sessions did not drain after its graceful stop")
        client_stop_result["serverActivityAfterDrain"]=drained; save(out/"client-stop.json",client_stop_result)
        mark("client_sessions_drained",time=drained.get("observedAtUtc"),sample=drained)
        active={"client":client,"base":base,"namespace":ns,"pod":primary,"outputDirectory":str(out),
            "stopResult":client_stop_result,"drainedSample":drained}
        client_summary=capture_pgbench_results(active); client_stopped=True
        if client_summary.get("validationErrors"):
            for error in client_summary["validationErrors"]: mark("pgbench_validation_failed",error=error)
            failure=failure or "; ".join(client_summary["validationErrors"])
        mark("pgbench_logs_captured",completedTransactions=client_summary.get("completedTransactions"),
             loggedCompletedTransactions=client_summary.get("loggedCompletedTransactions"),
             committedTransactions=client_summary.get("committedTransactions"),failedTransactions=client_summary.get("failedTransactions"))
    except Exception as e:
        failure=failure or f"pgbench graceful stop/artifact capture failed: {e}"; tail_error=tail_error or str(e)
        mark("client_stop_or_capture_failed",error=str(e))

    if client_stopped:
        try:
            final_target=sql(base,ns,primary,a.database,WAL_SQL)["flushLsn"]
            state("final_replay_drain",targetLsn=final_target,timeoutHours=a.catchup_hours)
            mark("final_replay_drain_started",targetLsn=final_target,clientStopped=True)
            final_drain_started=time.monotonic(); stop_at=final_drain_started+a.catchup_hours*3600; next_at=final_drain_started; final_stable=0
            while time.monotonic()<stop_at:
                time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
                if time.monotonic()>=stop_at: break
                try:
                    check_pods(base,ns,name,pods)
                    row=both(base,ns,a.database,app,bench_app,pods,layout,0); same_map(first,row)
                except Exception as sample_error:
                    mark("sample_attempt_failed",phase="final_replay_drain",error=str(sample_error))
                    continue
                rows.append(row); append(raw,{"seq":seq,"phase":"final_replay_drain",**row}); seq+=1
                replay=row["replica"]["replayLsn"]; flush=row["primary"]["flushLsn"]
                final_stable=next_catchup_streak(final_stable,replica_reached_target(row["primary"],row["replica"],final_target,0))
                if final_stable>=3:
                    final_caught=True; caught=True; final_replay_end=row["primary"]["ts"]
                    mark("replica_caught_up_after_client_stop",time=final_replay_end,targetLsn=final_target,
                         primaryFlushLsn=flush,replicaReplayLsn=replay,consecutiveHealthySamples=final_stable)
                    break
            if not final_caught:
                failure=failure or f"replica did not catch up to final client WAL target {final_target} within {a.catchup_hours:g}h"
                tail_error=tail_error or failure; mark("final_replay_drain_timeout",targetLsn=final_target,timeoutHours=a.catchup_hours,
                    replayLsn=replay,primaryFlushLsn=flush)
        except Exception as e:
            failure=failure or f"final replay observation failed: {e}"; tail_error=tail_error or str(e)
            mark("final_replay_drain_failed",error=str(e),targetLsn=final_target)

    if not monitor.stop():
        failure=failure or "one-second pgbench activity sampler did not stop"; mark("client_sampler_stop_failed")
    try:
        remove_pgpass(base,ns,primary,credential)
        config=json.loads((out/"client-config.json").read_text()); config["credentialFileRemoved"]=True; config["credentialCleanupAtUtc"]=stamp(); save(out/"client-config.json",config)
    except Exception as e:
        failure=failure or f"private pgpass cleanup failed: {e}"; mark("pgpass_cleanup_failed",error=str(e))
    try: cmd(base+["exec","-c","postgres","-n",ns,primary,"--","sh","-c","rm -rf -- "+shlex.quote(client["workDirectory"])],30)
    except Exception as e: mark("pgbench_remote_temp_cleanup_failed",error=str(e))
    RUN_CONTEXT.clear()
    drain_seconds=(time.monotonic()-drain_started) if drain_started is not None else 0
    final_drain_seconds=(time.monotonic()-final_drain_started) if final_drain_started is not None else 0
    minimum_observed=bool(vacuum_start_epoch and minimum_end_epoch and time.time()>=minimum_end_epoch)
    client_complete=bool(client_stopped and client_summary and not client_summary.get("validationErrors") and minimum_observed and post_recovery_healthy)
    intervention_complete=bool(intervention_complete and intervention and intervention.get("status") not in ("failed","cancelled_before_dispatch") and
        (a.intervention_mode=="control" or (intervention.get("signalResult") or {}).get("signalSent") is True))
    observed=rows[-1]["primary"]["ts"]; lag=max(0,lsn(flush)-lsn(replay)) if flush and replay else None
    timing={"vacuumStart":start["time"] if start else None,"vacuumStartMarkerObserved":vacuum_marker,
      "vacuumComplete":complete["time"] if complete else None,
      "heapWorkStart":start["time"] if start and first_progress else None,"heapWorkFirstProgress":first_progress,"heapWorkLastProgress":last_progress,
      "heapWorkComplete":heap_complete,"heapWorkCompleteLowerBound":last_progress if heap_complete else None,"heapWorkCompleteUpperBound":heap_complete,
      "heapWorkCompletionObservedByProgress":bool(heap_complete),"replayEnd":final_replay_end or replay_end,"observationEnd":observed,"replayCaughtUp":caught,
      "replayTargetLsn":complete["flushLsn"] if complete else None,"replicaReplayLsnAtObservationEnd":replay,
      "primaryFlushLsnAtObservationEnd":flush,"replicaLagBytesAtObservationEnd":lag,"replayDrainSeconds":drain_seconds,
      "finalReplayTargetLsn":final_target,"finalReplayCaughtUp":final_caught,"finalReplayDrainSeconds":final_drain_seconds,
      "intervention":intervention,"interventionMode":a.intervention_mode,"interventionAfterSeconds":a.intervention_after_seconds,
      "minimumObservationSeconds":a.minimum_observation_seconds,"minimumObservationEndEpoch":minimum_end_epoch,
      "minimumObservationSatisfied":minimum_observed,"clientWarmup":warmup,"clientStop":client_stop_result,
      "clientSummary":client_summary,"clientSessionSamples":"client/sessions.jsonl",
      "clientSessionAgeAndWaitSource":"primary pg_stat_activity xact/query ages, wait events, and commit counter sampled once per second",
      "clientTargetCaughtWithSessions":client_target_caught,"clientPostRecoveryHealthy":post_recovery_healthy,
      "clientPostRecoveryHealthySeconds":post_recovery_seconds,"clientRecoveryHealthSamples":"client-recovery-health.jsonl",
      "clientRecoveryHealthRule":">=4 committed updates/s over rolling 60s, ten sessions remain connected, no unrelated app sessions, and no active query/xact older than 60s",
      "remoteVacuumExitCode":rc,"walBeforeVacuum":wal_start,"walAtVacuumStart":start,"walAtVacuumComplete":complete,
      "vacuumOptions":vacuum["options"],"vacuumSql":vacuum["sql"],
      "walCountersAfterBackendExit":wal_exit,"walCounterDeltaFromBeforeVacuum":wal_delta,
      "walCounterDeltaScope":"cluster pg_stat_wal counters; includes background WAL, not physical disk writes",
      "checkpoint":{"before":wal_before,"after":wal_cp,"targetLsn":wal_cp["flushLsn"]},"postVacuumObservationError":tail_error}
    measurement_ok=measurement_complete(rc,complete,caught,failure,client_complete,intervention_complete)
    timing["testComplete"]=False
    save(out/"timing.json",timing)
    if start:
        timing["replicationEvents"]=capture_replication_logs(base,ns,pods,start["time"],out,mark)
        save(out/"timing.json",timing)
    test_complete=False
    if complete:
        try:
            report=plot.analyze(rows,timing,a.interval)
            report["status"]={"vacuumExitCode":rc,"vacuumComplete":bool(rc==0 and complete),"error":failure,
                "testComplete":measurement_ok,
                "vacuumWindowCoverageValid":report.get("coverage",{}).get("windows",{}).get("vacuum",{}).get("valid",False),
                "replayCaughtUp":caught,"replicaLagBytesAtObservationEnd":lag,"replayDrainSeconds":drain_seconds,
                "clientComplete":client_complete,"interventionComplete":intervention_complete,
                "minimumObservationSatisfied":minimum_observed,"clientPostRecoveryHealthy":post_recovery_healthy}
            report["walDeltaVacuumMarkers"]={k:int(complete[k])-int(start[k]) for k in ("walBytes","walRecords","walFpi")} if start and complete else None
            report["walCounterDeltaFromBeforeVacuum"]=wal_delta
            plot.render(out,rows,timing,report); report.pop("_series",None); save(out/"summary.json",report)
            test_complete=measurement_ok
        except Exception as e:
            mark("summary_failed",error=str(e)); failure=failure or str(e); test_complete=False
    timing["testComplete"]=test_complete
    try: save(out/"timing.json",timing)
    except Exception as e:
        failure=failure or f"final timing write failed: {e}"; test_complete=False
        try: mark("artifact_failed",artifact="timing.json",error=str(e))
        except Exception: pass
    mark("observation_finished",observationEnd=observed,replayCaughtUp=caught,
        testComplete=test_complete,replicaLagBytes=lag,error=failure)
    state("complete" if test_complete else "failed",error=failure,replayCaughtUp=caught,testComplete=test_complete)
    if rc==0 and complete and work.startswith(("/controller/tmp/vacuum-","/tmp/vacuum-")):
        try: cmd(base+["exec","-c","postgres","-n",ns,primary,"--","sh","-c","rm -rf -- "+shlex.quote(work)],20)
        except Exception as e: mark("remote_temp_cleanup_failed",error=str(e))
    return 0 if not failure else 2

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--kubeconfig"); p.add_argument("--kubectl",default="kubectl")
    p.add_argument("--out",default=str(HERE/"results"/dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")))
    p.add_argument("--database",default="repro"); p.add_argument("--interval",type=float,default=5)
    p.add_argument("--intervention-mode",choices=("sigterm","control"),default="sigterm")
    p.add_argument("--intervention-after-seconds",type=float,default=600)
    p.add_argument("--minimum-observation-seconds",type=float,default=8400)
    p.add_argument("--client-app-name",default="vacuum-pgbench-",help="application_name prefix used by the external collector")
    p.add_argument("--safety-hours",type=float,default=24); p.add_argument("--catchup-hours",type=float,default=24)
    a=p.parse_args()
    if (not 2<=a.interval<=30 or min(a.safety_hours,a.catchup_hours)<=0 or
        a.intervention_after_seconds<0 or a.intervention_after_seconds>=a.safety_hours*3600 or a.minimum_observation_seconds<8400 or
        not re.fullmatch(r"[A-Za-z0-9_.-]{1,32}",a.client_app_name)):
        p.error("interval 2..30s; time bounds positive; intervention must precede the safety bound; minimum observation at least 8400s; app-name prefix 1..32 safe characters")
    STATE.mkdir(exist_ok=True)
    lock=(STATE/"test.lock").open("a")
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        print("ERROR: measurement or teardown already running",file=sys.stderr); return 2
    try: return run(a)
    except Exception as e:
        print(f"ERROR: {e}",file=sys.stderr)
        cleanup_errors=cleanup_pre_vacuum_failure()
        if RUN_CONTEXT.get("vacuumStarted"):
            # Leave the detached VACUUM/client intact after launch. Automatic
            # cleanup here would shorten the required hang window or cancel work.
            print("VACUUM had started; left its process and client workload untouched for operator/controller recovery.",file=sys.stderr)
        save(STATE/"test-state.json",{"phase":"failed","updatedAt":stamp(),"error":str(e),"cleanupErrors":cleanup_errors,
            "vacuumLeftRunning":bool(RUN_CONTEXT.get("vacuumStarted"))})
        return 2

if __name__=="__main__": raise SystemExit(main())
