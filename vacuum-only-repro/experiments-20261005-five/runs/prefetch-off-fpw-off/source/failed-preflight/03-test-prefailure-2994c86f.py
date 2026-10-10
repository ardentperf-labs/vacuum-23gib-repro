#!/usr/bin/env python3
"""Run detached VACUUM and sample separate PostgreSQL DATA/WAL disks."""
import argparse, fcntl, concurrent.futures as cf, datetime as dt, json, os, re, shlex, subprocess, sys, time, uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
STATE=HERE/".state"
GIB=1024**3
import plot

GUC_SQL="""SELECT json_build_object(
 'version',current_setting('server_version'),'recovery',pg_is_in_recovery(),
 'systemId',(SELECT system_identifier::text FROM pg_control_system()),
 'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'),
 'settings',(SELECT json_object_agg(name,json_build_object('value',current_setting(name),'setting',setting,'unit',unit,'source',source,'sourceFile',sourcefile,'pendingRestart',pending_restart))
  FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','wal_log_hints','vacuum_cost_delay','maintenance_io_concurrency',
  'maintenance_work_mem','shared_buffers','max_wal_size','checkpoint_timeout','checkpoint_completion_target','track_io_timing','track_wal_io_timing','autovacuum',
  'synchronous_commit','synchronous_standby_names','wal_sender_timeout','wal_receiver_timeout','max_wal_senders','max_replication_slots','wal_compression','wal_buffers','wal_writer_delay','wal_writer_flush_after','statement_timeout'])),
 'defaultTablespaceIoConcurrencyOverride',EXISTS(SELECT 1 FROM pg_tablespace t,unnest(t.spcoptions) AS opt(value) WHERE t.spcname='pg_default' AND value LIKE 'maintenance_io_concurrency=%'),
 'dataChecksumVersion',(SELECT data_checksum_version FROM pg_control_init()),
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
 set -- $(findmnt -n -T "$p" -o MAJ:MIN,SOURCE,TARGET,FSTYPE); mm=$1; src=$2; target=$3; fs=$4
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
  q="/sys/class/block/$disk/queue"
 printf '__DEV_%s__\n%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' "$k" "$p" "$mm" "$diskmm" "$src" "$target" "$fs" "$lun" "$link" "$disk" "$(cat "$q/max_sectors_kb")" "$(cat "$q/max_hw_sectors_kb")" "$(cat "$q/nr_requests")" "$(stat -Lc %d "$p")" "$(findmnt -n -T "$p" -o MAJ:MIN)" "$sysdev"
done
printf '__DISKSTATS__\n'; cat /proc/diskstats
printf '__SQL__\n'
psql -XAt -F '|' -U postgres -d "$DB_NAME" -c "SELECT CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'primary' END,
 coalesce(pg_last_wal_replay_lsn()::text,''),CASE WHEN pg_is_in_recovery() THEN '' ELSE pg_current_wal_flush_lsn()::text END,
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend'),
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
def event(path,name,**data):
    append(path,{"time":stamp(),"event":name,**data})
def cmd(argv,timeout=30,input=None):
    p=subprocess.run(argv,input=input,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if p.returncode: raise RuntimeError(f"{' '.join(map(str,argv))} failed ({p.returncode}): {(p.stderr or p.stdout)[-1500:]}")
    return p.stdout
def sql(base,ns,pod,db,query,timeout=45):
    out=cmd(base+["exec","-i","-c","postgres","-n",ns,pod,"--","psql","-X","-A","-t","-v","ON_ERROR_STOP=1","-U","postgres","-d",db],timeout,query+"\n")
    try: return json.loads(out.strip().splitlines()[-1])
    except (IndexError,json.JSONDecodeError) as e: raise RuntimeError(f"bad SQL JSON from {pod}: {out[-400:]}") from e
def recovery_prefetch_stats(base,ns,pod,db):
    try:
        exists=sql(base,ns,pod,db,"SELECT json_build_object('available',to_regclass('pg_catalog.pg_stat_recovery_prefetch') IS NOT NULL)::text;")
        if not exists.get("available"):
            return {"available":False,"reason":"pg_stat_recovery_prefetch view is unavailable"}
        stats=sql(base,ns,pod,db,"SELECT COALESCE((SELECT to_jsonb(s) FROM pg_catalog.pg_stat_recovery_prefetch s LIMIT 1),'{}'::jsonb)::text;")
        return {"available":True,"stats":stats}
    except Exception as e:
        return {"available":False,"error":str(e)}
def recovery_prefetch_counter_delta(before,after):
    if not before.get("available") or not after.get("available"):
        return {"available":False,"reason":before.get("reason") or before.get("error") or after.get("reason") or after.get("error") or "statistics unavailable"}
    b=before.get("stats",{}); a=after.get("stats",{})
    if b.get("stats_reset")!=a.get("stats_reset"):
        return {"available":False,"reason":"pg_stat_recovery_prefetch reset during measurement","beforeStatsReset":b.get("stats_reset"),"afterStatsReset":a.get("stats_reset")}
    counter_names=("prefetch","hit","skip_init","skip_new","skip_fpw","skip_rep")
    gauge_names=("wal_distance","block_distance","io_depth")
    counters={key:a[key]-b[key] for key in counter_names
        if key in a and key in b and isinstance(a[key],(int,float)) and not isinstance(a[key],bool)
        and isinstance(b[key],(int,float)) and not isinstance(b[key],bool)}
    gauges={key:{"before":b.get(key),"after":a.get(key)} for key in gauge_names if key in a or key in b}
    if any(v<0 for v in counters.values()):
        return {"available":False,"reason":"one or more recovery-prefetch counters decreased","counterDelta":counters,"gauges":gauges,"statsReset":a.get("stats_reset")}
    return {"available":True,"statsReset":a.get("stats_reset"),"counterDelta":counters,"gauges":gauges}
def whole_timeline_data_read_metrics(report):
    window=report.get("windows",{}).get("vacuumAndReplay",{})
    primary=window.get("primary_data",{}); replica=window.get("replica_data",{})
    def device(values):
        iops=values.get("readIops"); mbps=values.get("readMBps"); seconds=values.get("coveredSeconds")
        byte_per_op=(mbps*1_000_000/iops) if iops and mbps is not None else None
        read_bytes=(mbps*1_000_000*seconds) if mbps is not None and seconds is not None else None
        return {"readIops":iops,"readMBps":mbps,"coveredSeconds":seconds,
            "effectiveBytesPerReadOperation":byte_per_op,"estimatedReadBytesDecimal":read_bytes}
    p=device(primary); r=device(replica)
    return {"window":"vacuumAndReplay","basis":"time-weighted guest block-counter read throughput and IOPS over each DATA device's covered seconds",
        "units":{"readMBps":"decimal MB/s","effectiveBytesPerReadOperation":"decimal bytes/read","estimatedReadBytesDecimal":"decimal bytes"},
        "primary":p,"replica":r,
        "replicaToPrimaryReadOperationRatio":(r["readIops"]/p["readIops"] if p["readIops"] else None),
        "replicaToPrimaryReadByteRatio":(r["estimatedReadBytesDecimal"]/p["estimatedReadBytesDecimal"] if p["estimatedReadBytesDecimal"] else None)}
def lsn(s):
    a,b=s.split("/"); return int(a,16)*(1<<32)+int(b,16)
def minimum_fixture_heap(expected):
    if not expected: return 100*GIB
    expected=int(expected)
    return max(GIB,min(100*GIB,int(expected*.97)))
def replica_reached_target(primary,replica,target):
    if (primary.get("role")!="primary" or replica.get("role")!="replica" or
        primary.get("appSessions") or replica.get("appSessions") or not target): return False
    try:
        goal=lsn(target); flushed=lsn(primary["flushLsn"]); replayed=lsn(replica["replayLsn"])
        return flushed>=goal and replayed>=goal and replayed>=flushed
    except (KeyError,TypeError,ValueError): return False
def measurement_complete(exit_code,vacuum_marker,caught,failure):
    return bool(exit_code==0 and vacuum_marker and caught and not failure)
def next_catchup_streak(streak,qualifies):
    return streak+1 if qualifies else 0
def pod_claims(pod):
    c=next(x for x in pod["spec"]["containers"] if x["name"]=="postgres")
    volumes={v["name"]:v.get("persistentVolumeClaim",{}).get("claimName") for v in pod["spec"]["volumes"]}
    return [{"path":m["mountPath"],"claim":volumes[m["name"]]} for m in c["volumeMounts"] if volumes.get(m["name"])]

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
        raise RuntimeError("expected uncached Standard_LRS Azure Disk storage")
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
            if len(f)!=15: raise ValueError("bad disk mapping row")
            path,mountmm,diskmm,src,target,fs,lun,link,name,maxsec,maxhw,nreq,statdev,findmm,sysdev=f
            if f"{os.major(int(statdev))}:{os.minor(int(statdev))}"!=mountmm or findmm.strip()!=mountmm: raise ValueError("stat/findmnt major:minor mismatch")
            expected=mapping[kind]
            if lun!=expected["lun"]: raise ValueError("CSI LUN mapping changed")
            if target!=expected["path"]: raise ValueError("mounted path does not match the DATA/WAL PVC mount")
            hctl=re.search(r"/(\d+:\d+:\d+):(\d+)$",sysdev)
            if name.startswith("sd") and hctl and hctl.group(2)!=lun: raise ValueError(f"SCSI sysfs LUN {hctl.group(2)} differs from CSI LUN {lun}")
            if diskmm not in disks or disks[diskmm]["name"]!=name: raise ValueError("sysfs disk missing from /proc/diskstats")
            devices[kind]={"path":path,"claim":expected["claim"],"pv":expected["pv"],"node":expected["node"],"majorMinor":diskmm,
                "mountMajorMinor":mountmm,"source":src,"mount":target,"expectedMount":expected["path"],"filesystem":fs,"lun":int(lun),"attachmentDevicePath":expected["attachmentDevicePath"],
                "lunPath":link,"lunPathVisible":bool(link),"sysfsDevicePath":sysdev,"device":name,
                "queue":{"maxSectorsKb":int(maxsec),"maxHwSectorsKb":int(maxhw),"nrRequests":int(nreq)}}
        if devices["data"]["majorMinor"]==devices["wal"]["majorMinor"]: raise ValueError("DATA/WAL share a block device")
        f=sec["SQL"][0].split("|",7)
        return {"epoch":float(sec["TIME"][0]),"ts":sec["TIME"][1],"pod":pod["metadata"]["name"],"role":f[0],
            "replayLsn":f[1],"flushLsn":f[2],"appSessions":int(f[3]),"vacuumProgress":json.loads(f[4]) if f[4] else None,
            "senders":json.loads(f[5]),"receiver":json.loads(f[6]) if f[6] else None,"walStats":json.loads(f[7]),"devices":devices,"disks":disks}
    except (KeyError,IndexError,ValueError,json.JSONDecodeError) as e: raise RuntimeError(f"invalid sample from {pod['metadata']['name']}: {e}") from e

def sample(base,ns,db,app,pod,mapping):
    env={"DB_NAME":db,"VAC_APP":app}
    for k in ("data","wal"): env[k.upper()+"_LUN"]=mapping[k]["lun"]; env[k.upper()+"_LUN_PATH"]=mapping[k]["attachmentDevicePath"]
    prefix=" ".join(f"{k}={shlex.quote(v)}" for k,v in env.items())
    out=cmd(base+["exec","-c","postgres","-n",ns,pod["metadata"]["name"],"--","sh","-c",prefix+"\n"+REMOTE],30)
    row=parse_sample(out,pod,mapping)
    if row["role"]!=pod["metadata"]["labels"].get("cnpg.io/instanceRole") or row["appSessions"]: raise RuntimeError("role mismatch or app client session detected")
    return row

def both(base,ns,db,app,pods,layout):
    with cf.ThreadPoolExecutor(2) as pool:
        f={r:pool.submit(sample,base,ns,db,app,p,layout["roles"][r]) for r,p in pods.items()}
        return {r:f[r].result() for r in pods}


def same_map(first, rows):
    for role in ("primary","replica"):
        for dev in ("data","wal"):
            a,b=first[role]["devices"][dev],rows[role]["devices"][dev]
            keys=("path","claim","pv","node","majorMinor","mountMajorMinor","source","mount","filesystem","expectedMount","lun","lunPath","sysfsDevicePath","device","queue")
            if any(a[k]!=b[k] for k in keys): raise RuntimeError(f"{role} {dev} disk/PVC/LUN mapping changed")

def check_pods(base,ns,cluster,pods):
    items=json.loads(cmd(base+["get","pods","-n",ns,"-l","cnpg.io/cluster="+cluster,"-o","json"]))["items"]
    current={p["metadata"]["name"]:p for p in items}
    if len(items)!=2: raise RuntimeError("PostgreSQL pod count changed")
    for role,old in pods.items():
        p=current.get(old["metadata"]["name"],{}); meta=p.get("metadata",{}); status=p.get("status",{})
        if meta.get("uid")!=old["metadata"].get("uid"): raise RuntimeError("PostgreSQL pod identity changed")
        if meta.get("labels",{}).get("cnpg.io/instanceRole")!=role: raise RuntimeError(f"PostgreSQL {role} role changed")
        if status.get("phase")!="Running" or not any(c.get("type")=="Ready" and c.get("status")=="True" for c in status.get("conditions",[])):
            raise RuntimeError(f"PostgreSQL {role} pod is not Running/Ready")

def vacuum_spec(prepared):
    options=["VERBOSE"]
    if prepared.get("fixtureTargetHeapBytes") is not None: options.append("FREEZE")
    return {"options":options,"sql":f"VACUUM ({', '.join(options)}) public.vacuum_target;"}

def launch_vacuum(base,ns,pod,db,app,safety_seconds=86400,statement=None):
    work="vacuum-"+uuid.uuid4().hex; q=shlex.quote
    statement=statement or vacuum_spec({})["sql"]
    query="""SELECT 'VACUUM_START|'||to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')||'|'||pg_current_wal_flush_lsn()||'|'||wal_bytes||'|'||wal_records||'|'||wal_fpi FROM pg_stat_wal;
"""+statement+"""
SELECT 'VACUUM_COMPLETE|'||to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')||'|'||pg_current_wal_flush_lsn()||'|'||wal_bytes||'|'||wal_records||'|'||wal_fpi FROM pg_stat_wal;
"""
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
printf %s {q(query)} >"$d/vacuum.sql"; printf %s {q(wrapper)} >"$d/run.sh"; chmod 700 "$d/run.sh"
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
    log=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c","cat "+f],90); dest.write_text(log); marks={}
    for line in log.splitlines():
        x=line.strip().split("|")
        if len(x)==6 and x[0] in ("VACUUM_START","VACUUM_COMPLETE"):
            marks[x[0].lower().replace("vacuum_","")]={"time":x[1],"flushLsn":x[2],"walBytes":int(x[3]),"walRecords":int(x[4]),"walFpi":int(x[5])}
    return marks

def parse_replication_log(text,role):
    needles=(("sender_timeout","terminating walsender process due to replication timeout"),
      ("receiver_timeout","terminating walreceiver due to timeout"),
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

def cluster_spec_defaulting(cluster):
    annotations=cluster.get("metadata",{}).get("annotations",{})
    raw=annotations.get("kubectl.kubernetes.io/last-applied-configuration")
    if not isinstance(raw,str) or not raw:
        raise RuntimeError("missing last-applied CNPG Cluster manifest for treatment-default attribution")
    try: submitted=json.loads(raw).get("spec",{}).get("postgresql",{}).get("parameters",{})
    except (json.JSONDecodeError,AttributeError,TypeError) as e:
        raise RuntimeError(f"invalid last-applied CNPG Cluster manifest: {e}") from e
    live=cluster.get("spec",{}).get("postgresql",{}).get("parameters",{})
    if "full_page_writes" in submitted or "recovery_prefetch" in submitted:
        raise RuntimeError("the user-applied Cluster manifest must omit full_page_writes and recovery_prefetch treatment overrides")
    if live.get("full_page_writes","on")!="on" or live.get("recovery_prefetch","try")!="try":
        raise RuntimeError("unexpected live CNPG spec value for defaulted full_page_writes/recovery_prefetch")
    return {"submittedParameters":submitted,"liveParameters":live,
        "interpretation":"CNPG may inject the default full_page_writes=on into the live Cluster CR. The pinned user-applied manifest omits both experiment GUCs; role treatment is proven from effective pg_settings values and active local postgresql.auto.conf source files."}

def run(a):
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
    cluster_spec_treatment=cluster_spec_defaulting(cluster_obj)
    measurement_image=prep.get("measurementImage")
    if not isinstance(measurement_image,str) or "@sha256:" not in measurement_image:
        raise RuntimeError("prepared measurement image must be pinned by digest")
    if cluster_obj.get("spec",{}).get("imageName")!=measurement_image:
        raise RuntimeError("Cluster imageName differs from prepared measurement image")
    def state(phase,**x): save(statefile,{"runId":out.name,"phase":phase,"output":str(out),"updatedAt":stamp(),**x})
    def mark(name,**x): event(events,name,**x)
    state("preflight")
    items=json.loads(cmd(base+["get","pods","-n",ns,"-l","cnpg.io/cluster="+name,"-o","json"]))["items"]; pods={}
    for p in items:
        role=p.get("metadata",{}).get("labels",{}).get("cnpg.io/instanceRole")
        ready=any(c.get("type")=="Ready" and c.get("status")=="True" for c in p.get("status",{}).get("conditions",[]))
        if role in ("primary","replica"):
            if role in pods or not ready or p.get("status",{}).get("phase")!="Running": raise RuntimeError(f"invalid {role} pod")
            pods[role]=p
    if len(items)!=2 or set(pods)!={"primary","replica"}: raise RuntimeError("expected exactly two ready PostgreSQL pods")
    expected_digest=measurement_image.rsplit("@",1)[1]
    for role,pod in pods.items():
        postgres_spec=next((c for c in pod.get("spec",{}).get("containers",[]) if c.get("name")=="postgres"),{})
        postgres_status=next((c for c in pod.get("status",{}).get("containerStatuses",[]) if c.get("name")=="postgres"),{})
        if postgres_spec.get("image")!=measurement_image or not str(postgres_status.get("imageID","")).endswith(expected_digest):
            raise RuntimeError(f"{role} pod does not run the pinned prepared measurement image digest")
    if pods["primary"]["spec"].get("nodeName")==pods["replica"]["spec"].get("nodeName"): raise RuntimeError("pods must use separate nodes")
    jobs=[]
    for j in json.loads(cmd(base+["get","jobs","-n",ns,"-o","json"]))["items"]:
        lab=j.get("metadata",{}).get("labels",{}); owners=j.get("metadata",{}).get("ownerReferences",[])
        ok=j.get("status",{}).get("succeeded",0)>0 and not j.get("status",{}).get("active",0) and lab.get("cnpg.io/cluster")==name and lab.get("cnpg.io/jobRole")=="snapshot-recovery" and any(o.get("kind")=="Cluster" and o.get("name")==name for o in owners)
        if not ok: raise RuntimeError(f"unexpected/active Job {j['metadata']['name']}")
        jobs.append(j["metadata"]["name"])
    layout=storage(base,ns,pods); fixture={}
    candidates=((STATE/"prepared.json"),) if prep.get("sourceKind")=="fresh-initdb-seed" else (HERE/"snapshot.json",STATE/"snapshot.json")
    for f in candidates:
        if f.exists(): fixture=json.loads(f.read_text()); break
    if prep.get("sourceKind")=="reused-prepared-paired-snapshot":
        fixture={"sourceKind":prep["sourceKind"],"systemIdentifier":prep.get("systemIdentifier"),
            "heapBytes":prep.get("heapBytes"),"tableBytes":prep.get("tableBytes"),
            "preparedLiveTuplesEstimate":prep.get("preparedLiveTuplesEstimate"),
            "preparedDeadTuplesEstimate":prep.get("preparedDeadTuplesEstimate"),
            "sourcePairRestore":prep.get("sourcePairRestore")}
    for k in ("systemIdentifier","heapBytes","tableBytes"):
        if prep.get(k) is not None: fixture[k]=prep[k]
    meta={}
    for role,pod in pods.items():
        m=sql(base,ns,pod["metadata"]["name"],a.database,GUC_SQL); meta[role]=m
        if not m["version"].startswith("16.15") or m["recovery"]!=(role=="replica"): raise RuntimeError(f"{role}: version/role mismatch")
        if m["appSessions"] or (fixture.get("systemIdentifier") and m["systemId"]!=str(fixture["systemIdentifier"])): raise RuntimeError(f"{role}: app session/system ID mismatch")
        settings={k:v["value"] for k,v in m["settings"].items()}
        expected={"wal_level":"logical","full_page_writes":"off" if role=="primary" else "on","recovery_prefetch":"try" if role=="primary" else "off","vacuum_cost_delay":"0",
          "maintenance_io_concurrency":"10","track_io_timing":"on","track_wal_io_timing":"on","shared_buffers":"256MB",
          "autovacuum":"on","max_wal_size":"1GB","checkpoint_timeout":"5min","statement_timeout":"0"}
        for k,v in expected.items():
            if str(settings.get(k))!=v: raise RuntimeError(f"{role}: expected {k}={v}, found {settings.get(k)}")
        fpw=m["settings"]["full_page_writes"]
        if not str(fpw.get("sourceFile") or "").endswith("/postgresql.auto.conf") or fpw.get("pendingRestart"):
            raise RuntimeError(f"{role} full_page_writes treatment must come from active local postgresql.auto.conf")
        if role=="replica":
            prefetch=m["settings"]["recovery_prefetch"]
            if not str(prefetch.get("sourceFile") or "").endswith("/postgresql.auto.conf") or prefetch.get("pendingRestart"):
                raise RuntimeError("replica recovery_prefetch=off must come from its local postgresql.auto.conf and be active")
        t=m["table"]
        minimum_heap=minimum_fixture_heap(fixture.get("heapBytes"))
        if not t or int(t["heapBytes"])<minimum_heap or "autovacuum_enabled=false" not in (t["reloptions"] or []) or t["tablespaceOverrides"] or m["defaultTablespaceIoConcurrencyOverride"]:
            raise RuntimeError(f"{role}: table size/autovacuum/tablespace gate failed")
        for key,actual in (("heapBytes",t["heapBytes"]),("tableBytes",t["totalBytes"])):
            if fixture.get(key) and abs(int(actual)-int(fixture[key]))>int(fixture[key])*.03: raise RuntimeError(f"{role}: {key} differs from prepared source by over 3%")
    if meta["primary"]["systemId"]!=meta["replica"]["systemId"] or abs(int(meta["primary"]["table"]["heapBytes"])-int(meta["replica"]["table"]["heapBytes"]))>64*1024**2:
        raise RuntimeError("primary/replica system ID or heap differs")
    planned_vacuum=vacuum_spec(prep)
    if prep.get("sourceKind")=="reused-prepared-paired-snapshot" and planned_vacuum!={"options":["VERBOSE","FREEZE"],"sql":"VACUUM (VERBOSE, FREEZE) public.vacuum_target;"}:
        raise RuntimeError(f"reused 23-GiB fixture must preserve original VACUUM (VERBOSE, FREEZE) invocation: {planned_vacuum}")
    save(out/"vacuum-spec.json",{"preparedSourceKind":prep.get("sourceKind"),"fixtureTargetHeapGiB":prep.get("fixtureTargetHeapGiB"),
        "fixtureTargetHeapBytes":prep.get("fixtureTargetHeapBytes"),"options":planned_vacuum["options"],"sql":planned_vacuum["sql"]})
    save(out/"preflight.json",{"prepare":prep,"pods":{r:{"name":p["metadata"]["name"],"node":p["spec"]["nodeName"],"uid":p["metadata"]["uid"],"claims":pod_claims(p)} for r,p in pods.items()},
        "sql":meta,"storage":layout,"completedCnpgBootstrapJobs":jobs,"fixture":fixture,"clusterSpecTreatment":cluster_spec_treatment,"clientCreated":False})
    mark("preflight_complete",systemId=meta["primary"]["systemId"],heapBytes=meta["primary"]["table"]["heapBytes"],allowedJobs=jobs)

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
    prefetch_stats_before={role:recovery_prefetch_stats(base,ns,pod["metadata"]["name"],a.database) for role,pod in pods.items()}
    prefetch_stats_artifact={"measurementBoundary":"after checkpoint replay and immediately before baseline sampling/VACUUM launch; counters were not reset",
        "beforeCapturedAt":stamp(),"before":{role:{"pod":pods[role]["metadata"]["name"],"uid":pods[role]["metadata"].get("uid"),**value} for role,value in prefetch_stats_before.items()},"after":{},"counterDelta":{}}
    save(out/"recovery-prefetch-stats.json",prefetch_stats_artifact)
    wal_start=sql(base,ns,primary,a.database,WAL_SQL); app="vacuum-only-"+uuid.uuid4().hex
    first=both(base,ns,a.database,app,pods,layout); rows=[first]; append(raw,{"seq":0,"phase":"baseline",**first})
    vacuum=vacuum_spec(prep)
    work,pid=launch_vacuum(base,ns,primary,a.database,app,int(a.safety_hours*3600),vacuum["sql"])
    save(out/"vacuum-process.json",{"applicationName":app,"pod":primary,"pid":pid,"workDirectory":work,"status":"RUNNING",**vacuum})
    state("vacuum",applicationName=app,remotePid=pid); mark("observation_started",applicationName=app,interval=a.interval,vacuumOptions=vacuum["options"],vacuumSql=vacuum["sql"])
    seq=1; status="RUNNING"; failure=None; first_progress=last_progress=first_null=heap_complete=None; phase=None; last_progress_row=None
    deadline=time.monotonic()+a.safety_hours*3600
    try:
        next_at=time.monotonic()
        while status=="RUNNING" and time.monotonic()<deadline:
            time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
            check_pods(base,ns,name,pods)
            row=both(base,ns,a.database,app,pods,layout); same_map(first,row); rows.append(row); append(raw,{"seq":seq,"phase":"vacuum",**row}); seq+=1
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
            status=remote_status(base,ns,primary,work)
        if status=="RUNNING":
            failure=f"VACUUM exceeded {a.safety_hours:g}h"; mark("safety_bound_cancel",result=cancel_owned(base,ns,primary,a.database,app))
            stop=time.monotonic()+180
            while status=="RUNNING" and time.monotonic()<stop: time.sleep(3); status=remote_status(base,ns,primary,work)
    except Exception as e:
        failure=str(e); mark("observation_failed",error=failure)
        try:
            mark("owned_cancel",result=cancel_owned(base,ns,primary,a.database,app)); stop=time.monotonic()+120
            while time.monotonic()<stop:
                status=remote_status(base,ns,primary,work)
                if status!="RUNNING": break
                time.sleep(3)
        except Exception as ce: mark("cancel_failed",error=str(ce))
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
    caught=False; replay_end=None; replay=flush=None; tail_error=None; drain_started=None; stable=0
    if complete and not failure:
        target=complete.get("flushLsn")
        if not target:
            failure="VACUUM completion marker has no flush LSN"; tail_error=failure
            mark("replay_drain_failed",error=failure)
        else:
            state("replay_drain",targetLsn=target,timeoutHours=a.catchup_hours)
            mark("replay_drain_started",targetLsn=target,timeoutHours=a.catchup_hours)
            drain_started=time.monotonic(); stop=drain_started+a.catchup_hours*3600; next_at=drain_started
            try:
                while not caught and time.monotonic()<stop:
                    time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
                    if time.monotonic()>=stop: break
                    check_pods(base,ns,name,pods); row=both(base,ns,a.database,app,pods,layout); same_map(first,row)
                    rows.append(row); append(raw,{"seq":seq,"phase":"replay_drain",**row}); seq+=1
                    replay=row["replica"]["replayLsn"]; flush=row["primary"]["flushLsn"]
                    if time.monotonic()>=stop: break
                    stable=next_catchup_streak(stable,replica_reached_target(row["primary"],row["replica"],target))
                    if stable>=3:
                        caught=True; replay_end=row["replica"]["ts"]
                        mark("replica_caught_up",time=replay_end,targetLsn=target,
                             primaryFlushLsn=flush,replayLsn=replay,consecutiveSamples=stable)
                if not caught:
                    failure=f"replica did not replay VACUUM completion LSN {target} within {a.catchup_hours:g}h"
                    tail_error=failure; mark("replay_drain_timeout",targetLsn=target,timeoutHours=a.catchup_hours,
                        replayLsn=replay,primaryFlushLsn=flush)
            except Exception as e:
                tail_error=str(e); failure=f"post-VACUUM replay observation failed: {e}"
                mark("replay_drain_failed",error=tail_error,targetLsn=target)
    drain_seconds=(time.monotonic()-drain_started) if drain_started is not None else 0
    postflight={}; postflight_verified=False
    prefetch_stats_after={role:recovery_prefetch_stats(base,ns,pod["metadata"]["name"],a.database) for role,pod in pods.items()}
    prefetch_stats_delta={role:recovery_prefetch_counter_delta(prefetch_stats_before.get(role,{}),prefetch_stats_after.get(role,{})) for role in pods}
    prefetch_stats_artifact["afterCapturedAt"]=stamp()
    prefetch_stats_artifact["after"]={role:{"pod":pods[role]["metadata"]["name"],"uid":pods[role]["metadata"].get("uid"),**value} for role,value in prefetch_stats_after.items()}
    prefetch_stats_artifact["counterDelta"]=prefetch_stats_delta
    save(out/"recovery-prefetch-stats.json",prefetch_stats_artifact)
    try:
        final_cluster=json.loads(cmd(base+["get","cluster",name,"-n",ns,"-o","json"]))
        final_cluster_spec_treatment=cluster_spec_defaulting(final_cluster)
        if final_cluster.get("spec",{}).get("imageName")!=measurement_image:
            raise RuntimeError("Cluster measurement image changed during run")
        final_parameters=final_cluster.get("spec",{}).get("postgresql",{}).get("parameters",{})
        final_pods=json.loads(cmd(base+["get","pods","-n",ns,"-l","cnpg.io/cluster="+name,"-o","json"]))["items"]
        final_by_role={p.get("metadata",{}).get("labels",{}).get("cnpg.io/instanceRole"):p for p in final_pods}
        if set(final_by_role)!={"primary","replica"}:
            raise RuntimeError("primary/replica roles changed by the end of measurement")
        for role,pod in final_by_role.items():
            if pod["metadata"].get("uid")!=pods[role]["metadata"].get("uid"):
                raise RuntimeError(f"{role} pod UID changed during measurement")
            image=next((c for c in pod.get("spec",{}).get("containers",[]) if c.get("name")=="postgres"),{})
            image_status=next((c for c in pod.get("status",{}).get("containerStatuses",[]) if c.get("name")=="postgres"),{})
            if image.get("image")!=measurement_image or not str(image_status.get("imageID","")).endswith(expected_digest):
                raise RuntimeError(f"{role} measurement image digest changed during run")
            current=sql(base,ns,pod["metadata"]["name"],a.database,GUC_SQL)
            settings={k:v["value"] for k,v in current["settings"].items()}
            expected_prefetch="try" if role=="primary" else "off"
            expected_fpw="off" if role=="primary" else "on"
            if settings.get("recovery_prefetch")!=expected_prefetch:
                raise RuntimeError(f"{role} recovery_prefetch drifted to {settings.get('recovery_prefetch')}")
            if settings.get("full_page_writes")!=expected_fpw:
                raise RuntimeError(f"{role} full_page_writes drifted to {settings.get('full_page_writes')}")
            fpw=current["settings"]["full_page_writes"]
            if not str(fpw.get("sourceFile") or "").endswith("/postgresql.auto.conf") or fpw.get("pendingRestart"):
                raise RuntimeError(f"{role} full_page_writes lost its active local postgresql.auto.conf source")
            if role=="replica":
                s=current["settings"]["recovery_prefetch"]
                if not str(s.get("sourceFile") or "").endswith("/postgresql.auto.conf") or s.get("pendingRestart"):
                    raise RuntimeError("replica recovery_prefetch lost its active local postgresql.auto.conf source")
            postflight[role]={"pod":pod["metadata"]["name"],"uid":pod["metadata"].get("uid"),"recovery":current["recovery"],"fullPageWrites":current["settings"]["full_page_writes"],"recoveryPrefetch":current["settings"]["recovery_prefetch"]}
        save(out/"postflight.json",{"clusterSpecFullPageWrites":final_parameters.get("full_page_writes"),"clusterSpecRecoveryPrefetch":final_parameters.get("recovery_prefetch"),"clusterSpecTreatment":final_cluster_spec_treatment,"roles":postflight,"recoveryPrefetchStatsCounterDelta":prefetch_stats_delta})
        mark("prefetch_postflight_verified",roles=postflight,clusterSpecOverride=False)
        postflight_verified=True
    except Exception as e:
        failure=failure or f"recovery_prefetch postflight verification failed: {e}"
        mark("prefetch_postflight_failed",error=str(e),roles=postflight)
    observed=rows[-1]["primary"]["ts"]; lag=max(0,lsn(flush)-lsn(replay)) if flush and replay else None
    timing={"vacuumStart":start["time"] if start else None,"vacuumComplete":complete["time"] if complete else None,
      "heapWorkStart":start["time"] if start and first_progress else None,"heapWorkFirstProgress":first_progress,"heapWorkLastProgress":last_progress,
      "heapWorkComplete":heap_complete,"heapWorkCompleteLowerBound":last_progress if heap_complete else None,"heapWorkCompleteUpperBound":heap_complete,
      "heapWorkCompletionObservedByProgress":bool(heap_complete),"replayEnd":replay_end,"observationEnd":observed,"replayCaughtUp":caught,
      "replayTargetLsn":complete["flushLsn"] if complete else None,"replicaReplayLsnAtObservationEnd":replay,
      "primaryFlushLsnAtObservationEnd":flush,"replicaLagBytesAtObservationEnd":lag,"replayDrainSeconds":drain_seconds,
      "remoteVacuumExitCode":rc,"walBeforeVacuum":wal_start,"walAtVacuumStart":start,"walAtVacuumComplete":complete,
      "vacuumOptions":vacuum["options"],"vacuumSql":vacuum["sql"],
      "walCountersAfterBackendExit":wal_exit,"walCounterDeltaFromBeforeVacuum":wal_delta,
      "walCounterDeltaScope":"cluster pg_stat_wal counters; includes background WAL, not physical disk writes",
      "checkpoint":{"before":wal_before,"after":wal_cp,"targetLsn":wal_cp["flushLsn"]},"postVacuumObservationError":tail_error}
    measurement_ok=measurement_complete(rc,complete,caught,failure)
    timing["testComplete"]=False
    save(out/"timing.json",timing)
    if start:
        timing["replicationEvents"]=capture_replication_logs(base,ns,pods,start["time"],out,mark)
        save(out/"timing.json",timing)
    test_complete=False
    if complete:
        try:
            report=plot.analyze(rows,timing,a.interval)
            report["wholeTimelineDataReadMetrics"]=whole_timeline_data_read_metrics(report)
            report["recoveryPrefetchExperiment"]={"primary":"try","replica":"off","sharedClusterOverride":False,
                "replicaSourceFile":meta["replica"]["settings"]["recovery_prefetch"].get("sourceFile"),
                "postflightVerified":postflight_verified}
            report["fullPageWritesExperiment"]={"primary":"off","replica":"on","sharedClusterOverride":False,
                "clusterSpecTreatment":cluster_spec_treatment,
                "primarySourceFile":meta["primary"]["settings"]["full_page_writes"].get("sourceFile"),
                "replicaSourceFile":meta["replica"]["settings"]["full_page_writes"].get("sourceFile"),
                "walLogHints":{"primary":meta["primary"]["settings"]["wal_log_hints"]["value"],"replica":meta["replica"]["settings"]["wal_log_hints"]["value"]},
                "dataChecksumVersion":{"primary":meta["primary"].get("dataChecksumVersion"),"replica":meta["replica"].get("dataChecksumVersion")},
                "postflightVerified":postflight_verified}
            report["recoveryPrefetchStats"]={"measurementBoundary":prefetch_stats_artifact["measurementBoundary"],
                "before":prefetch_stats_artifact["before"],"after":prefetch_stats_artifact["after"],"counterDelta":prefetch_stats_delta}
            report["status"]={"vacuumExitCode":rc,"vacuumComplete":bool(rc==0 and complete),"error":failure,
                "testComplete":measurement_ok,
                "vacuumWindowCoverageValid":report.get("coverage",{}).get("windows",{}).get("vacuum",{}).get("valid",False),
                "replayCaughtUp":caught,"replicaLagBytesAtObservationEnd":lag,"replayDrainSeconds":drain_seconds}
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
    p.add_argument("--safety-hours",type=float,default=24); p.add_argument("--catchup-hours",type=float,default=24)
    a=p.parse_args()
    if not 2<=a.interval<=30 or min(a.safety_hours,a.catchup_hours)<=0: p.error("interval 2..30s; time bounds must be positive")
    STATE.mkdir(exist_ok=True)
    lock=(STATE/"test.lock").open("a")
    try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        print("ERROR: measurement or teardown already running",file=sys.stderr); return 2
    try: return run(a)
    except Exception as e:
        print(f"ERROR: {e}",file=sys.stderr); save(STATE/"test-state.json",{"phase":"failed","updatedAt":stamp(),"error":str(e)}); return 2

if __name__=="__main__": raise SystemExit(main())
