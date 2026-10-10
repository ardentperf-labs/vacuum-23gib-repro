#!/usr/bin/env python3
"""Run detached VACUUM and sample separate PostgreSQL DATA/WAL disks."""
import argparse, base64, binascii, fcntl, concurrent.futures as cf, datetime as dt, hashlib, json, os, re, shlex, subprocess, sys, tarfile, threading, time, uuid
from io import BytesIO
from pathlib import Path
HERE=Path(__file__).resolve().parent
CLIENT_DIR=HERE.parent/"client"
STATE=HERE/".state"
GIB=1024**3
import plot
ACTIVE_CLIENT=None
ACTIVE_MONITOR=None
ACTIVE_CREDENTIAL=None
ACTIVE_CLIENT_LAUNCH_ATTEMPTED=False

GUC_SQL="""SELECT json_build_object(
 'version',current_setting('server_version'),'recovery',pg_is_in_recovery(),
 'systemId',(SELECT system_identifier::text FROM pg_control_system()),
 'dataChecksumVersion',(SELECT data_page_checksum_version FROM pg_control_init()),
 'appSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend' AND application_name IS DISTINCT FROM '__BENCH_APP__'),
 'settings',(SELECT json_object_agg(name,json_build_object('value',current_setting(name),'setting',setting,'unit',unit,'source',source,'pendingRestart',pending_restart))
  FROM pg_settings WHERE name=ANY(ARRAY['wal_level','full_page_writes','recovery_prefetch','vacuum_cost_delay','wal_log_hints','maintenance_io_concurrency',
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
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND backend_type='client backend' AND application_name IS DISTINCT FROM '$BENCH_APP'),
 coalesce((SELECT json_build_object('phase',v.phase,'heapBlksTotal',v.heap_blks_total,'heapBlksScanned',v.heap_blks_scanned,
 'heapBlksVacuumed',v.heap_blks_vacuumed,'indexVacuumCount',v.index_vacuum_count,'numDeadTuples',v.num_dead_tuples,
 'backendState',a.state,'waitEventType',a.wait_event_type,'waitEvent',a.wait_event)::text
 FROM pg_stat_activity a LEFT JOIN pg_stat_progress_vacuum v ON v.pid=a.pid
 WHERE a.application_name='$VAC_APP' AND a.datname=current_database() AND a.usename='postgres' AND a.backend_type='client backend' LIMIT 1),''),
 coalesce((SELECT json_agg(json_build_object('state',state,'syncState',sync_state,'sentLsn',sent_lsn,
 'writeLsn',write_lsn,'flushLsn',flush_lsn,'replayLsn',replay_lsn,'writeLag',write_lag,'flushLag',flush_lag,'replayLag',replay_lag))::text FROM pg_stat_replication),'[]'),
 coalesce((SELECT json_build_object('status',status,'writtenLsn',written_lsn,'flushedLsn',flushed_lsn,
 'latestEndLsn',latest_end_lsn,'lastMsgReceiptTime',last_msg_receipt_time)::text FROM pg_stat_wal_receiver LIMIT 1),''),
 coalesce((SELECT json_build_object('walBytes',wal_bytes::text,'walRecords',wal_records,'walFpi',wal_fpi,'statsReset',stats_reset)::text FROM pg_stat_wal),'{}'),
 coalesce((SELECT to_jsonb(r)::text FROM pg_stat_recovery_prefetch r LIMIT 1),'{}'),
 (SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name='$BENCH_APP' AND backend_type='client backend'),
 coalesce((SELECT json_agg(json_build_object('pid',pid,'state',state,'waitEventType',wait_event_type,'waitEvent',wait_event,
  'xactAgeSeconds',extract(epoch FROM clock_timestamp()-xact_start),
  'queryAgeSeconds',extract(epoch FROM clock_timestamp()-query_start)) ORDER BY pid)::text
  FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name='$BENCH_APP' AND backend_type='client backend'),'[]');"
'''

def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")
def save(path,obj):
    path.parent.mkdir(parents=True,exist_ok=True); tmp=path.with_suffix(path.suffix+".tmp")
    tmp.write_text(json.dumps(obj,indent=2,sort_keys=True)+"\n"); os.replace(tmp,path)
def append(path,obj):
    with path.open("a") as f: f.write(json.dumps(obj,sort_keys=True)+"\n")
def event(event_log,event_name,**data):
    append(event_log,{"time":stamp(),"event":event_name,**data})
def cmd(argv,timeout=30,input=None):
    p=subprocess.run(argv,input=input,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
    if p.returncode: raise RuntimeError(f"{' '.join(map(str,argv))} failed ({p.returncode}): {(p.stderr or p.stdout)[-1500:]}")
    return p.stdout
def sql_literal(value):
    return "'"+str(value).replace("'","''")+"'"
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
def replica_reached_target(primary,replica,target):
    if (primary.get("role")!="primary" or replica.get("role")!="replica" or
        primary.get("appSessions") or replica.get("appSessions") or not target): return False
    try:
        goal=lsn(target); flushed=lsn(primary["flushLsn"]); replayed=lsn(replica["replayLsn"])
        return flushed>=goal and replayed>=goal and replayed>=flushed
    except (KeyError,TypeError,ValueError): return False
def check_bench_sessions(rows,expected):
    if rows["primary"].get("benchSessions")!=expected or rows["replica"].get("benchSessions")!=0:
        raise RuntimeError(f"expected {expected} pgbench sessions on primary and none on replica; got primary={rows['primary'].get('benchSessions')} replica={rows['replica'].get('benchSessions')}")
    if rows["primary"].get("appSessions") or rows["replica"].get("appSessions"):
        raise RuntimeError("unrelated app sessions detected")
def replica_reached_vacuum_target(primary,replica,target,expected_bench_sessions=10):
    if (primary.get("role")!="primary" or replica.get("role")!="replica" or
        primary.get("appSessions") or replica.get("appSessions") or
        primary.get("benchSessions")!=expected_bench_sessions or replica.get("benchSessions")!=0 or not target): return False
    try:
        goal=lsn(target); flushed=lsn(primary["flushLsn"]); replayed=lsn(replica["replayLsn"])
        sender=any(x.get("state")=="streaming" and x.get("syncState") in ("sync","quorum") for x in primary.get("senders",[]))
        receiver=(replica.get("receiver") or {}).get("status")=="streaming"
        return flushed>=goal and replayed>=goal and sender and receiver
    except (KeyError,TypeError,ValueError): return False
def measurement_complete(exit_code,vacuum_marker,caught,failure,client_target_caught,post_recovery_healthy,client_final_caught):
    return bool(exit_code==0 and vacuum_marker and caught and client_target_caught and post_recovery_healthy and client_final_caught and not failure)
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

def storage(base,ns,pods,infra):
    pvcs=json.loads(cmd(base+["get","pvc","-n",ns,"-o","json"]))["items"]
    pvs={x["metadata"]["name"]:x for x in json.loads(cmd(base+["get","pv","-o","json"]))["items"]}
    sc=json.loads(cmd(base+["get","storageclass","azure-premiumv2","-o","json"]))
    vas=json.loads(cmd(base+["get","volumeattachments","-o","json"]))["items"]
    params=sc.get("parameters",{})
    if (sc.get("provisioner")!="disk.csi.azure.com" or params.get("skuName")!="PremiumV2_LRS" or
        params.get("cachingMode")!="None" or params.get("kind")!="Managed" or params.get("DiskIOPSReadWrite")!="3000" or
        params.get("DiskMBpsReadWrite")!="125" or params.get("LogicalSectorSize")!="512"):
        raise RuntimeError("expected uncached PremiumV2_LRS storage at 3000 IOPS / 125 MB/s")
    pvc={x["metadata"]["name"]:x for x in pvcs}; result={"storageClass":sc,"roles":{},"claims":{}}
    for role,pod in pods.items():
        found={}
        for m in pod_claims(pod):
            kind="wal" if "/wal" in m["path"] else "data" if "/data" in m["path"] else None
            c=pvc.get(m["claim"]); pv=pvs.get((c or {}).get("spec",{}).get("volumeName"))
            cs=(pv or {}).get("spec",{}).get("csi",{}); attrs=cs.get("volumeAttributes",{})
            if not kind or kind in found or not c or c.get("status",{}).get("phase")!="Bound": raise RuntimeError(f"invalid PVC mount {m}")
            if c["spec"].get("storageClassName")!="azure-premiumv2" or c["spec"].get("resources",{}).get("requests",{}).get("storage")!="512Gi":
                raise RuntimeError(f"{m['claim']} is not 512Gi azure-premiumv2")
            if not pv or cs.get("driver")!="disk.csi.azure.com" or attrs.get("skuName")!="PremiumV2_LRS" or attrs.get("cachingMode") not in (None,"None") or pv["spec"].get("capacity",{}).get("storage")!="512Gi":
                raise RuntimeError(f"{m['claim']} is not uncached 512Gi PremiumV2_LRS")
            disk_id=cs.get("volumeHandle")
            if not disk_id: raise RuntimeError(f"{m['claim']} has no Azure disk ID")
            disk=json.loads(cmd(["az","disk","show","--ids",disk_id,"--subscription",infra["subscriptionId"],"-o","json"],timeout=900))
            expected_zone=str(infra.get("availabilityZone", ""))
            zones=disk.get("zones") or []
            # Azure CLI/ARM responses have used both diskSizeGB and diskSizeGb.
            # Treat an explicit JSON null like an omitted field so the nested
            # creationData value can still supply the sector size.
            size_gib=disk.get("diskSizeGB")
            if size_gib is None: size_gib=disk.get("diskSizeGb")
            sector=disk.get("logicalSectorSize")
            if sector is None: sector=(disk.get("creationData") or {}).get("logicalSectorSize")
            option=(disk.get("creationData") or {}).get("createOption")
            completion=disk.get("completionPercent")
            if completion is not None: completion=float(completion)
            hydrated=(completion==100 if option in ("Copy","CopyStart") else
                      completion in (None,100) if option=="Empty" else False)
            if (disk.get("sku",{}).get("name")!="PremiumV2_LRS" or size_gib is None or int(size_gib)!=512 or
                int(disk.get("diskIOPSReadWrite",0))!=3000 or int(disk.get("diskMBpsReadWrite",0))!=125 or
                disk.get("provisioningState")!="Succeeded" or not hydrated or not disk.get("managedBy") or
                expected_zone not in [str(z) for z in zones] or str(sector)!="512"):
                raise RuntimeError(f"{m['claim']} Azure disk SKU, size, zone, performance, sector, or hydration state failed")
            va=[v for v in vas if v.get("spec",{}).get("source",{}).get("persistentVolumeName")==pv["metadata"]["name"] and
                v.get("spec",{}).get("nodeName")==pod["spec"].get("nodeName") and v.get("status",{}).get("attached")]
            if len(va)!=1: raise RuntimeError(f"{m['claim']} has no unique CSI attachment on its node")
            att=va[0].get("status",{}).get("attachmentMetadata",{}); path=att.get("devicePath","")
            try: lun=attachment_lun(att,path)
            except RuntimeError as e: raise RuntimeError(f"{m['claim']} has no CSI LUN") from e
            found[kind]={"claim":m["claim"],"path":m["path"],"pv":pv["metadata"]["name"],"volumeHandle":disk_id,
                "azureDisk":{"id":disk.get("id"),"sku":disk.get("sku",{}).get("name"),"sizeGiB":size_gib,"iops":disk.get("diskIOPSReadWrite"),
                    "throughputMBps":disk.get("diskMBpsReadWrite"),"completionPercent":completion,"createOption":option,"logicalSectorSize":sector,"zones":zones},
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
        f=sec["SQL"][0].split("|",10)
        return {"epoch":float(sec["TIME"][0]),"ts":sec["TIME"][1],"pod":pod["metadata"]["name"],"role":f[0],
            "replayLsn":f[1],"flushLsn":f[2],"appSessions":int(f[3]),"vacuumProgress":json.loads(f[4]) if f[4] else None,
            "senders":json.loads(f[5]),"receiver":json.loads(f[6]) if f[6] else None,"walStats":json.loads(f[7]),
            "recoveryPrefetchStats":json.loads(f[8]),"benchSessions":int(f[9]),
            "benchBackends":json.loads(f[10]),"devices":devices,"disks":disks}
    except (KeyError,IndexError,ValueError,json.JSONDecodeError) as e: raise RuntimeError(f"invalid sample from {pod['metadata']['name']}: {e}") from e

def sample(base,ns,db,app,bench_app,pod,mapping):
    env={"DB_NAME":db,"VAC_APP":app,"BENCH_APP":bench_app}
    for k in ("data","wal"): env[k.upper()+"_LUN"]=mapping[k]["lun"]; env[k.upper()+"_LUN_PATH"]=mapping[k]["attachmentDevicePath"]
    prefix=" ".join(f"{k}={shlex.quote(v)}" for k,v in env.items())
    out=cmd(base+["exec","-c","postgres","-n",ns,pod["metadata"]["name"],"--","sh","-c",prefix+"\n"+REMOTE],30)
    row=parse_sample(out,pod,mapping)
    if row["role"]!=pod["metadata"]["labels"].get("cnpg.io/instanceRole") or row["appSessions"]: raise RuntimeError("role mismatch or app client session detected")
    return row

def both(base,ns,db,app,bench_app,pods,layout):
    with cf.ThreadPoolExecutor(2) as pool:
        f={r:pool.submit(sample,base,ns,db,app,bench_app,p,layout["roles"][r]) for r,p in pods.items()}
        return {r:f[r].result() for r in pods}

def cnpg_secret_text(secret,name):
    encoded=(secret.get("data") or {}).get(name)
    if not isinstance(encoded,str): raise RuntimeError(f"CNPG app Secret is missing its {name} field")
    try: return base64.b64decode(encoded,validate=True).decode("utf-8")
    except (binascii.Error,UnicodeDecodeError,ValueError): raise RuntimeError(f"CNPG app Secret has an invalid {name} field") from None

def read_cnpg_app_secret(base,ns,cluster,cluster_uid,db):
    name=cluster+"-app"
    argv=base+["get","secret",name,"-n",ns,"-o","json"]
    try: proc=subprocess.run(argv,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
    except Exception as e: raise RuntimeError(f"could not read CNPG app Secret {name}: {type(e).__name__}") from None
    if proc.returncode: raise RuntimeError(f"could not read CNPG app Secret {name} (kubectl exit {proc.returncode})")
    try: secret=json.loads(proc.stdout)
    except (json.JSONDecodeError,TypeError): raise RuntimeError(f"CNPG app Secret {name} returned invalid JSON") from None
    metadata=secret.get("metadata") or {}
    owners=metadata.get("ownerReferences") or []
    owner_ok=any(o.get("kind")=="Cluster" and o.get("name")==cluster and
        (not cluster_uid or o.get("uid")==cluster_uid) for o in owners)
    labels=metadata.get("labels") or {}
    if (metadata.get("name")!=name or secret.get("type")!="kubernetes.io/basic-auth" or not owner_ok or
        (labels.get("cnpg.io/cluster") not in (None,cluster))):
        raise RuntimeError(f"CNPG app Secret {name} failed its type/Cluster ownership checks")
    username=cnpg_secret_text(secret,"username")
    database=cnpg_secret_text(secret,"dbname")
    port_text=cnpg_secret_text(secret,"port")
    password=cnpg_secret_text(secret,"password")
    if username!="app" or database!=db:
        raise RuntimeError(f"CNPG app Secret {name} must identify user app and database {db}")
    if not port_text.isdecimal() or not 1<=int(port_text)<=65535:
        raise RuntimeError(f"CNPG app Secret {name} has an invalid port field")
    if not password or any(c in password for c in ("\0","\n","\r")):
        raise RuntimeError(f"CNPG app Secret {name} has an unsupported password encoding")
    return {"secretName":name,"username":username,"database":database,"port":int(port_text),"password":password}

def pgpass_escape(value):
    if any(c in str(value) for c in ("\0","\n","\r")): raise RuntimeError("invalid character in pgpass field")
    return str(value).replace("\\","\\\\").replace(":","\\:")

def install_cnpg_app_pgpass(base,ns,pod,cluster,cluster_uid,db):
    global ACTIVE_CREDENTIAL
    secret=read_cnpg_app_secret(base,ns,cluster,cluster_uid,db)
    secret_name=secret["secretName"]
    username=secret["username"]
    database=secret["database"]
    port=secret["port"]
    password=secret.pop("password")
    # CNPG mounts /controller/tmp writable; the pinned image has a read-only /tmp.
    directory="/controller/tmp/pgbench-auth-"+uuid.uuid4().hex
    path=directory+"/pgpass"
    active={"base":base,"namespace":ns,"pod":pod,"secretName":secret_name,"directory":directory,"path":path}
    ACTIVE_CREDENTIAL=active
    pgpass_text=""
    fields=()
    try:
        fields=("127.0.0.1",str(port),database,username,password)
        pgpass_text=":".join(pgpass_escape(x) for x in fields)+"\n"
        q=shlex.quote
        script=f"set -eu\numask 077\nd={q(directory)}\np={q(path)}\nmkdir -m 700 \"$d\"\ncat >\"$p\"\nchmod 600 \"$p\"\nprintf '%s\\n' \"$p\"\n"
        argv=base+["exec","-i","-c","postgres","-n",ns,pod,"--","sh","-c",script]
        try: proc=subprocess.run(argv,input=pgpass_text,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
        except Exception as e: raise RuntimeError(f"could not install private pgpass file: {type(e).__name__}") from None
        if proc.returncode: raise RuntimeError(f"could not install private pgpass file (kubectl exit {proc.returncode})")
        if proc.stdout.strip()!=path: raise RuntimeError("private pgpass install returned an unexpected path")
        active.update({"installed":True,"username":username,"database":database,"port":port})
        return {"secretName":secret_name,"username":username,"database":database,
            "host":"127.0.0.1","port":port,"pgpassFilePath":path}
    finally:
        password=""
        pgpass_text=""
        fields=()
        secret.clear()

def remove_cnpg_app_pgpass(active):
    q=shlex.quote
    script=f"set -eu\nd={q(active['directory'])}\np={q(active['path'])}\nif [ -d \"$d\" ]; then rm -f -- \"$p\"; rmdir -- \"$d\"; fi"
    cmd(active["base"]+["exec","-c","postgres","-n",active["namespace"],active["pod"],"--","sh","-c",script],30)

def verify_client_app_auth(base,ns,pod,db,credential):
    auth_query="SELECT current_user||'|'||current_database()||'|'||current_setting('synchronous_commit')||'|'||current_setting('statement_timeout')"
    auth=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","env","PGPASSFILE="+credential["pgpassFilePath"],"PGAPPNAME=pgbench-auth-preflight",
        "psql","-X","-A","-t","-v","ON_ERROR_STOP=1","-h","127.0.0.1","-p",str(credential["port"]),"-U","app","-d",db,"-c",auth_query],30).strip()
    if auth!=f"app|{db}|on|0": raise RuntimeError(f"pgbench TCP app-role authentication/session GUC check expected app|{db}|on|0, found {auth!r}")

def client_setup(base,ns,pod,db):
    script=(CLIENT_DIR/"setup.sql").read_text()
    cmd(base+["exec","-i","-c","postgres","-n",ns,pod,"--","psql","-X","-v","ON_ERROR_STOP=1","-U","postgres","-d",db],90,script)
    q="""SELECT json_build_object(
      'persistence',c.relpersistence,'rows',(SELECT count(*) FROM public.pgbench_client_rows),
      'minId',(SELECT min(client_id) FROM public.pgbench_client_rows),
      'maxId',(SELECT max(client_id) FROM public.pgbench_client_rows),
      'autovacuumDisabled','autovacuum_enabled=false'=ANY(c.reloptions),
      'hasIndex',c.relhasindex,'appCanSelectUpdate',has_table_privilege('app',c.oid,'SELECT') AND has_table_privilege('app',c.oid,'UPDATE'))::text
      FROM pg_class c WHERE c.oid='public.pgbench_client_rows'::regclass;"""
    fixture=sql(base,ns,pod,db,q)
    expected={"persistence":"p","rows":10,"minId":0,"maxId":9,"autovacuumDisabled":True,"hasIndex":False,"appCanSelectUpdate":True}
    if fixture!=expected: raise RuntimeError(f"pgbench fixture is not the expected ten-row logged table: {fixture}")
    return fixture

def check_pgbench_runtime(base,ns,pod):
    try: out=cmd(base+["exec","-c","postgres","-n",ns,pod,"--","sh","-c","command -v pgbench && pgbench --version"],30)
    except Exception as e: raise RuntimeError(f"pgbench is absent from the primary image; do not install packages or change the PostgreSQL image; use a small dedicated client pod with the app credential mounted in memory only ({e})") from e
    lines=[x.strip() for x in out.splitlines() if x.strip()]
    version_ok=(len(lines)>=2 and re.fullmatch(
        r"pgbench \(PostgreSQL\) 16\.15(?: \(Debian 16\.15-[0-9]+\.pgdg13\+[0-9]+\))?",lines[1]) is not None)
    if len(lines)<2 or not (lines[0]=="pgbench" or lines[0].endswith("/pgbench")) or not version_ok:
        raise RuntimeError(f"primary pod has no verified PostgreSQL 16 pgbench client: {out.strip()!r}; do not install into the database image; use a small dedicated client pod")
    return {"path":lines[0],"version":lines[1]}

def client_activity_query(bench_app):
    app="'"+bench_app.replace("'","''").replace("\\","\\\\").replace("%","%%")+"'"
    return f"""SELECT json_build_object(
      'observedAtUtc',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
      'benchSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend'),
      'unrelatedAppSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name IS DISTINCT FROM {app} AND backend_type='client backend'),
      'committedTransactions',(SELECT coalesce(sum(update_count),0) FROM public.pgbench_client_rows),
      'activeSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active'),
      'waitingSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active' AND wait_event IS NOT NULL),
      'idleClientReadSessions',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='idle' AND wait_event='ClientRead'),
      'backends',coalesce((SELECT json_agg(json_build_object('pid',pid,'state',state,'waitEventType',wait_event_type,'waitEvent',wait_event,
        'xactAgeSeconds',extract(epoch FROM clock_timestamp()-xact_start),
        'queryAgeSeconds',extract(epoch FROM clock_timestamp()-query_start)) ORDER BY pid)
        FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend'),'[]'::json),
      'maxActiveXactAgeSeconds',(SELECT max(extract(epoch FROM clock_timestamp()-xact_start)) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active'),
      'maxActiveQueryAgeSeconds',(SELECT max(extract(epoch FROM clock_timestamp()-query_start)) FROM pg_stat_activity WHERE datname=current_database() AND usename='app' AND application_name={app} AND backend_type='client backend' AND state='active'))::text;"""

class ClientMonitor:
    def __init__(self,base,ns,pod,db,bench_app,path,interval=1.0):
        self.base,self.ns,self.pod,self.db,self.bench_app=base,ns,pod,db,bench_app
        self.path,self.interval=path,interval
        self.stop_event=threading.Event(); self.lock=threading.Lock(); self.latest_row=None
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
            with self.lock: self.latest_row=row
            self.stop_event.wait(max(0,self.interval-(time.monotonic()-begun)))
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
  --rate={float(target_tps):g} --time={int(duration_seconds)} --max-tries=1 --log --log-prefix="$d/pgbench-log" \
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
cat "$d/pgbench.pid" | tr -d '\\n'; printf '\\n%s\\n' "$d" "$wrapper"
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

def stop_active_client(graceful_timeout=900):
    global ACTIVE_CLIENT,ACTIVE_MONITOR,ACTIVE_CREDENTIAL,ACTIVE_CLIENT_LAUNCH_ATTEMPTED
    errors=[]
    active=ACTIVE_CLIENT
    if active and not active.get("stopped"):
        try:
            activity_before_stop=ACTIVE_MONITOR.latest() if ACTIVE_MONITOR else None
            status=remote_client_status(active["base"],active["namespace"],active["pod"],active["client"])
            if status=="RUNNING": active["stopResult"]=stop_pgbench(active["base"],active["namespace"],active["pod"],active["client"],graceful_timeout)
            elif status.isdecimal(): active["stopResult"]={"status":status,"exitCode":int(status),"alreadyExited":True,"finishedAtUtc":stamp()}
            else: raise RuntimeError(f"unrecognized pgbench status {status!r}")
            active["stopResult"]["serverActivityBeforeStopRequest"]=activity_before_stop
            active["stopped"]=True
            save(Path(active["outputDirectory"])/"client-stop.json",active["stopResult"])
        except Exception as e: errors.append(f"client stop: {e}")
    if active and active.get("stopped") and ACTIVE_MONITOR and not active.get("drainedSample"):
        try:
            drained=wait_for_bench_drain(ACTIVE_MONITOR,120)
            active["drainedSample"]=drained
            active["stopResult"]["serverActivityAfterDrain"]=drained
            save(Path(active["outputDirectory"])/"client-stop.json",active["stopResult"])
        except Exception as e: errors.append(f"client session drain: {e}")
    if active and active.get("stopped") and not active.get("captured"):
        try:
            summary=capture_pgbench_results(active)
            errors.extend(f"pgbench validation: {e}" for e in summary.get("validationErrors",[]))
        except Exception as e: errors.append(f"client logs: {e}")
    if ACTIVE_MONITOR:
        try:
            if not ACTIVE_MONITOR.stop(): errors.append("one-second client sampler did not stop within 25 seconds")
        except Exception as e: errors.append(f"client sampler: {e}")
        ACTIVE_MONITOR=None
    if active and active.get("stopped") and active.get("captured") and not active.get("remoteTempRemoved"):
        work=active["client"]["workDirectory"]
        try:
            cmd(active["base"]+["exec","-c","postgres","-n",active["namespace"],active["pod"],"--","sh","-c","rm -rf -- "+shlex.quote(work)],30)
            active["remoteTempRemoved"]=True
        except Exception as e: errors.append(f"client temporary cleanup: {e}")
    no_client_was_started=active is None and not ACTIVE_CLIENT_LAUNCH_ATTEMPTED
    client_was_captured_and_drained=bool(active and active.get("stopped") and active.get("captured") and active.get("drainedSample"))
    if ACTIVE_CREDENTIAL and (no_client_was_started or client_was_captured_and_drained):
        try:
            remove_cnpg_app_pgpass(ACTIVE_CREDENTIAL)
            ACTIVE_CREDENTIAL["removed"]=True
            out=ACTIVE_CREDENTIAL.get("outputDirectory")
            config_path=Path(out)/"client-config.json" if out else None
            if config_path and config_path.is_file():
                config=json.loads(config_path.read_text())
                config["credentialFileRemoved"]=True
                config["credentialCleanupAtUtc"]=stamp()
                save(config_path,config)
            ACTIVE_CREDENTIAL=None
            ACTIVE_CLIENT_LAUNCH_ATTEMPTED=False
        except Exception as e: errors.append(f"client credential file cleanup: {e}")
    return errors

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

def run(a):
    global ACTIVE_CLIENT,ACTIVE_MONITOR,ACTIVE_CREDENTIAL,ACTIVE_CLIENT_LAUNCH_ATTEMPTED
    out=Path(a.out).resolve(); out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()): raise RuntimeError(f"output directory not empty: {out}")
    bench_app="pgbench-baseline-"+uuid.uuid4().hex
    vacuum_app="vacuum-only-"+uuid.uuid4().hex
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
    if pods["primary"]["spec"].get("nodeName")==pods["replica"]["spec"].get("nodeName"): raise RuntimeError("pods must use separate nodes")
    expected_zone=f"{infra.get('location','').lower()}-{infra.get('availabilityZone','')}"
    node_zones={}
    for pod in pods.values():
        node=pod["spec"].get("nodeName")
        node_obj=json.loads(cmd(base+["get","node",node,"-o","json"]))
        zone=node_obj.get("metadata",{}).get("labels",{}).get("topology.kubernetes.io/zone")
        node_zones[node]=zone
        if zone!=expected_zone: raise RuntimeError(f"node {node} is in zone {zone!r}, expected {expected_zone!r}")
    jobs=[]
    for j in json.loads(cmd(base+["get","jobs","-n",ns,"-o","json"]))["items"]:
        lab=j.get("metadata",{}).get("labels",{}); owners=j.get("metadata",{}).get("ownerReferences",[])
        ok=j.get("status",{}).get("succeeded",0)>0 and not j.get("status",{}).get("active",0) and lab.get("cnpg.io/cluster")==name and lab.get("cnpg.io/jobRole")=="snapshot-recovery" and any(o.get("kind")=="Cluster" and o.get("name")==name for o in owners)
        if not ok: raise RuntimeError(f"unexpected/active Job {j['metadata']['name']}")
        jobs.append(j["metadata"]["name"])
    layout=storage(base,ns,pods,infra); fixture={}
    candidates=((STATE/"prepared.json"),) if prep.get("sourceKind")=="fresh-initdb-seed" else (HERE/"snapshot.json",STATE/"snapshot.json")
    for f in candidates:
        if f.exists(): fixture=json.loads(f.read_text()); break
    for k in ("systemIdentifier","heapBytes","tableBytes"):
        if prep.get(k) is not None: fixture[k]=prep[k]
    meta={}
    for role,pod in pods.items():
        m=sql(base,ns,pod["metadata"]["name"],a.database,GUC_SQL.replace("__BENCH_APP__",bench_app)); meta[role]=m
        if not m["version"].startswith("16.15") or m["recovery"]!=(role=="replica"): raise RuntimeError(f"{role}: version/role mismatch")
        if m["appSessions"] or (fixture.get("systemIdentifier") and m["systemId"]!=str(fixture["systemIdentifier"])): raise RuntimeError(f"{role}: app session/system ID mismatch")
        settings={k:v["value"] for k,v in m["settings"].items()}
        expected={"wal_level":"logical","full_page_writes":"on","recovery_prefetch":"try","vacuum_cost_delay":"0",
          "wal_log_hints":"on","maintenance_io_concurrency":"10","maintenance_work_mem":"64MB",
          "track_io_timing":"on","track_wal_io_timing":"on","shared_buffers":"256MB","synchronous_commit":"on",
          "autovacuum":"on","max_wal_size":"1GB","checkpoint_timeout":"5min","statement_timeout":"0"}
        for k,v in expected.items():
            if str(settings.get(k))!=v: raise RuntimeError(f"{role}: expected {k}={v}, found {settings.get(k)}")
        if role=="primary" and settings.get("synchronous_standby_names")!='ANY 1 ("hdd-pg-2")':
            raise RuntimeError(f"primary: expected synchronous_standby_names=ANY 1 (\"hdd-pg-2\"), found {settings.get('synchronous_standby_names')}")
        t=m["table"]
        minimum_heap=minimum_fixture_heap(fixture.get("heapBytes"))
        if not t or int(t["heapBytes"])<minimum_heap or "autovacuum_enabled=false" not in (t["reloptions"] or []) or t["tablespaceOverrides"] or m["defaultTablespaceIoConcurrencyOverride"]:
            raise RuntimeError(f"{role}: table size/autovacuum/tablespace gate failed")
        for key,actual in (("heapBytes",t["heapBytes"]),("tableBytes",t["totalBytes"])):
            if fixture.get(key) and abs(int(actual)-int(fixture[key]))>int(fixture[key])*.03: raise RuntimeError(f"{role}: {key} differs from prepared source by over 3%")
    if meta["primary"]["systemId"]!=meta["replica"]["systemId"] or abs(int(meta["primary"]["table"]["heapBytes"])-int(meta["replica"]["table"]["heapBytes"]))>64*1024**2:
        raise RuntimeError("primary/replica system ID or heap differs")
    save(out/"preflight.json",{"prepare":prep,"pods":{r:{"name":p["metadata"]["name"],"node":p["spec"]["nodeName"],"zone":node_zones[p["spec"]["nodeName"]],"uid":p["metadata"]["uid"],"claims":pod_claims(p)} for r,p in pods.items()},
        "sql":meta,"storage":layout,"completedCnpgBootstrapJobs":jobs,"fixture":fixture,"clientCreated":False})
    mark("preflight_complete",systemId=meta["primary"]["systemId"],heapBytes=meta["primary"]["table"]["heapBytes"],allowedJobs=jobs)

    primary,replica=pods["primary"]["metadata"]["name"],pods["replica"]["metadata"]["name"]
    pgbench_runtime=check_pgbench_runtime(base,ns,primary)
    credential=install_cnpg_app_pgpass(base,ns,primary,name,cluster_obj["metadata"].get("uid"),a.database)
    ACTIVE_CREDENTIAL["outputDirectory"]=str(out)
    verify_client_app_auth(base,ns,primary,a.database,credential)
    client_fixture=client_setup(base,ns,primary,a.database)
    client_config={"tool":"pgbench","version":pgbench_runtime["version"],"path":pgbench_runtime["path"],
        "targetTps":5,"clients":10,"warmupSeconds":300,"postRecoveryHealthySeconds":300,
        "transactionFile":"client/transaction.sql","transactionSha256":hashlib.sha256((CLIENT_DIR/"transaction.sql").read_bytes()).hexdigest(),
        "setupFile":"client/setup.sql","setupSha256":hashlib.sha256((CLIENT_DIR/"setup.sql").read_bytes()).hexdigest(),
        "fixture":client_fixture,"applicationName":bench_app,"database":a.database,"user":"app",
        "credential":{"secretName":credential["secretName"],"username":credential["username"],"database":credential["database"],
            "host":credential["host"],"port":credential["port"],"pgpassFilePath":credential["pgpassFilePath"],
            "pgpassMode":"0600","directoryMode":"0700","transport":"TCP"},
        "synchronousCommit":"on","noVacuum":True,"latencyLimit":None,"maxTries":1,"protocol":"simple",
        "stopSignal":"SIGALRM","stopSignalSemantics":"PostgreSQL 16 pgbench duration handler drains active transactions"}
    save(out/"client-config.json",client_config)
    mark("pgbench_runtime_verified",**pgbench_runtime)
    mark("client_fixture_created",fixture=client_fixture)
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
    client_duration=int((a.safety_hours+2*a.catchup_hours)*3600+300+300+1800)
    client_dir=out/"client"; client_dir.mkdir(parents=True,exist_ok=True)
    monitor=ClientMonitor(base,ns,primary,a.database,bench_app,client_dir/"sessions.jsonl",1.0)
    monitor.start(); ACTIVE_MONITOR=monitor
    ACTIVE_CLIENT_LAUNCH_ATTEMPTED=True
    client=launch_pgbench(base,ns,primary,a.database,bench_app,credential,client_duration,5,10)
    ACTIVE_CLIENT={"base":base,"namespace":ns,"pod":primary,"client":client,"outputDirectory":str(out)}
    save(out/"client-process.json",client)
    mark("client_started",applicationName=bench_app,pgbenchPid=client["pgbenchPid"],workDirectory=client["workDirectory"],targetTps=5,clients=10)
    state("client_warmup",applicationName=bench_app,targetTps=5,clients=10,warmupSeconds=300)
    warmup=wait_for_warmup(base,ns,primary,client,monitor,300,5)
    save(out/"client-warmup.json",warmup)
    mark("client_warmup_complete",**warmup)
    state("pre_vacuum_checkpoint_replay",applicationName=bench_app,targetTps=5,clients=10)
    pre_vacuum_checkpoint_result=pre_vacuum_checkpoint(
        base,ns,primary,replica,a.database,client,monitor,a.catchup_hours,a.interval,mark)
    wal_start=sql(base,ns,primary,a.database,WAL_SQL)
    first=both(base,ns,a.database,vacuum_app,bench_app,pods,layout)
    if first["primary"]["benchSessions"]!=10 or first["replica"]["benchSessions"]!=0: raise RuntimeError("pgbench session placement changed before VACUUM")
    rows=[first]; append(raw,{"seq":0,"phase":"pre_vacuum_initial",**first})
    mark("pre_vacuum_initial_sample",sampleTime=first["primary"]["ts"],targetLsn=pre_vacuum_checkpoint_result["targetLsn"],
         primaryFlushLsn=first["primary"]["flushLsn"],replicaReplayLsn=first["replica"]["replayLsn"],
         pgbenchSessions=first["primary"]["benchSessions"],walCounters=wal_start)
    vacuum=vacuum_spec(prep)
    work,pid=launch_vacuum(base,ns,primary,a.database,vacuum_app,int(a.safety_hours*3600),vacuum["sql"])
    save(out/"vacuum-process.json",{"applicationName":vacuum_app,"pod":primary,"pid":pid,"workDirectory":work,"status":"RUNNING",**vacuum})
    state("vacuum",applicationName=vacuum_app,remotePid=pid); mark("observation_started",applicationName=vacuum_app,interval=a.interval,vacuumOptions=vacuum["options"],vacuumSql=vacuum["sql"])
    seq=1; status="RUNNING"; failure=None; first_progress=last_progress=first_null=heap_complete=None; phase=None; last_progress_row=None
    deadline=time.monotonic()+a.safety_hours*3600
    try:
        next_at=time.monotonic()
        while status=="RUNNING" and time.monotonic()<deadline:
            time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
            check_pods(base,ns,name,pods)
            row=both(base,ns,a.database,vacuum_app,bench_app,pods,layout); check_bench_sessions(row,10); same_map(first,row); rows.append(row); append(raw,{"seq":seq,"phase":"vacuum",**row}); seq+=1
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
            failure=f"VACUUM exceeded {a.safety_hours:g}h"; mark("safety_bound_cancel",result=cancel_owned(base,ns,primary,a.database,vacuum_app))
            stop=time.monotonic()+180
            while status=="RUNNING" and time.monotonic()<stop: time.sleep(3); status=remote_status(base,ns,primary,work)
    except Exception as e:
        failure=str(e); mark("observation_failed",error=failure)
        try:
            mark("owned_cancel",result=cancel_owned(base,ns,primary,a.database,vacuum_app)); stop=time.monotonic()+120
            while time.monotonic()<stop:
                status=remote_status(base,ns,primary,work)
                if status!="RUNNING": break
                time.sleep(3)
        except Exception as ce: mark("cancel_failed",error=str(ce))
    rc=int(status) if status.isdecimal() else None
    save(out/"vacuum-process.json",{"applicationName":vacuum_app,"pod":primary,"pid":pid,"workDirectory":work,"status":status,"remoteExitCode":rc})
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
    active_target_caught=False; post_recovery_healthy=False; caught=False
    replay_end=None; final_replay_end=None; replay=flush=None; tail_error=None
    drain_started=None; stable=0; healthy_since=None; post_recovery_seconds=0
    final_drain_started=None; final_target=None; final_stable=0; final_caught=False
    client_stop_result=None
    if complete and not failure:
        target=complete.get("flushLsn")
        if not target:
            failure="VACUUM completion marker has no flush LSN"; tail_error=failure
            mark("replay_drain_failed",error=failure)
        else:
            state("replay_drain_client_active",targetLsn=target,timeoutHours=a.catchup_hours)
            mark("replay_drain_started",targetLsn=target,timeoutHours=a.catchup_hours,clientStillRunning=True)
            drain_started=time.monotonic(); stop=drain_started+a.catchup_hours*3600; next_at=drain_started
            try:
                while not post_recovery_healthy and time.monotonic()<stop:
                    time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
                    if time.monotonic()>=stop: break
                    check_pods(base,ns,name,pods)
                    row=both(base,ns,a.database,vacuum_app,bench_app,pods,layout)
                    check_bench_sessions(row,10); same_map(first,row)
                    rows.append(row); append(raw,{"seq":seq,"phase":"replay_drain",**row}); seq+=1
                    replay=row["replica"]["replayLsn"]; flush=row["primary"]["flushLsn"]
                    healthy=replica_reached_vacuum_target(row["primary"],row["replica"],target,10)
                    stable=next_catchup_streak(stable,healthy)
                    if stable>=3 and healthy_since is None:
                        active_target_caught=True; healthy_since=time.monotonic(); replay_end=row["primary"]["ts"]
                        mark("replica_reached_vacuum_target_with_client",time=row["primary"]["ts"],targetLsn=target,
                             primaryFlushLsn=flush,replicaReplayLsn=replay,consecutiveHealthySamples=stable,
                             clientSessions=row["primary"]["benchSessions"])
                    if healthy_since is not None and healthy:
                        elapsed=time.monotonic()-healthy_since
                        if elapsed>=300:
                            post_recovery_healthy=True; post_recovery_seconds=elapsed
                            mark("client_post_recovery_healthy_complete",time=row["primary"]["ts"],targetLsn=target,
                                 primaryFlushLsn=flush,replicaReplayLsn=replay,healthySeconds=elapsed)
                    elif healthy_since is not None and not healthy:
                        mark("client_post_recovery_health_reset",time=row["primary"]["ts"],targetLsn=target,
                             primaryFlushLsn=flush,replicaReplayLsn=replay,healthySeconds=time.monotonic()-healthy_since)
                        healthy_since=None; stable=0
                if not active_target_caught:
                    failure=f"replica did not reach VACUUM completion LSN {target} in three healthy samples within {a.catchup_hours:g}h"
                    tail_error=failure; mark("replay_drain_timeout",targetLsn=target,timeoutHours=a.catchup_hours,
                        replayLsn=replay,primaryFlushLsn=flush,clientStillRunning=True)
                elif not post_recovery_healthy:
                    failure=f"replica did not remain healthy for 300s after reaching VACUUM LSN {target}"
                    tail_error=failure; mark("client_post_recovery_health_timeout",targetLsn=target,timeoutHours=a.catchup_hours,
                        replayLsn=replay,primaryFlushLsn=flush)
            except Exception as e:
                tail_error=str(e); failure=f"client-active replay observation failed: {e}"
                mark("replay_drain_failed",error=tail_error,targetLsn=target)

    if ACTIVE_CLIENT and not ACTIVE_CLIENT.get("stopped"):
        try:
            activity_before_stop=monitor.latest()
            client_stop_result=stop_pgbench(base,ns,primary,client,900)
            client_stop_result["serverActivityBeforeStopRequest"]=activity_before_stop
            ACTIVE_CLIENT["stopResult"]=client_stop_result; ACTIVE_CLIENT["stopped"]=True
            save(out/"client-stop.json",client_stop_result)
            mark("client_stop_requested_and_drained",**client_stop_result)
            drained=wait_for_bench_drain(monitor,120)
            ACTIVE_CLIENT["drainedSample"]=drained
            client_stop_result["serverActivityAfterDrain"]=drained
            save(out/"client-stop.json",client_stop_result)
            mark("client_sessions_drained",time=drained.get("observedAtUtc"),benchSessions=drained.get("benchSessions"))
        except Exception as e:
            tail_error=tail_error or str(e); failure=failure or f"pgbench graceful stop/session drain failed: {e}"
            mark("client_stop_or_drain_failed",error=str(e))

    if ACTIVE_CLIENT and ACTIVE_CLIENT.get("stopped"):
        try:
            final_target=sql(base,ns,primary,a.database,WAL_SQL)["flushLsn"]
            state("final_replay_drain",targetLsn=final_target,timeoutHours=a.catchup_hours)
            mark("final_replay_drain_started",targetLsn=final_target,clientStopped=True)
            final_drain_started=time.monotonic(); stop=final_drain_started+a.catchup_hours*3600; next_at=final_drain_started
            while not final_caught and time.monotonic()<stop:
                time.sleep(max(0,next_at-time.monotonic())); next_at+=a.interval
                if time.monotonic()>=stop: break
                check_pods(base,ns,name,pods)
                row=both(base,ns,a.database,vacuum_app,bench_app,pods,layout)
                check_bench_sessions(row,0); same_map(first,row)
                rows.append(row); append(raw,{"seq":seq,"phase":"final_replay_drain",**row}); seq+=1
                replay=row["replica"]["replayLsn"]; flush=row["primary"]["flushLsn"]
                final_stable=next_catchup_streak(final_stable,replica_reached_target(row["primary"],row["replica"],final_target))
                if final_stable>=3:
                    final_caught=True; caught=True; final_replay_end=row["primary"]["ts"]
                    mark("replica_caught_up_after_client_stop",time=final_replay_end,targetLsn=final_target,
                         primaryFlushLsn=flush,replicaReplayLsn=replay,consecutiveHealthySamples=final_stable)
            if not final_caught:
                failure=failure or f"replica did not catch up to final client WAL target {final_target} within {a.catchup_hours:g}h"
                tail_error=tail_error or failure
                mark("final_replay_drain_timeout",targetLsn=final_target,timeoutHours=a.catchup_hours,
                     replayLsn=replay,primaryFlushLsn=flush,consecutiveHealthySamples=final_stable)
        except Exception as e:
            tail_error=tail_error or str(e); failure=failure or f"final replay observation failed: {e}"
            mark("final_replay_drain_failed",error=str(e),targetLsn=final_target)

    drain_seconds=(time.monotonic()-drain_started) if drain_started is not None else 0
    final_drain_seconds=(time.monotonic()-final_drain_started) if final_drain_started is not None else 0
    try:
        if ACTIVE_CLIENT and ACTIVE_CLIENT.get("stopped"):
            client_summary=capture_pgbench_results(ACTIVE_CLIENT)
            if client_summary.get("validationErrors"):
                for e in client_summary["validationErrors"]: mark("pgbench_validation_failed",error=e)
                failure=failure or "; ".join(client_summary["validationErrors"])
            mark("pgbench_logs_captured",completedTransactions=client_summary["completedTransactions"],
                 loggedCompletedTransactions=client_summary["loggedCompletedTransactions"],
                 committedTransactions=client_summary["committedTransactions"],
                 failedTransactions=client_summary["failedTransactions"],pgbenchExitCode=client_summary["pgbenchExitCode"])
        cleanup_errors=stop_active_client()
        if cleanup_errors:
            failure=failure or "; ".join(cleanup_errors)
            for e in cleanup_errors: mark("client_cleanup_failed",error=e)
    except Exception as e:
        failure=failure or f"client artifact capture failed: {e}"
        tail_error=tail_error or str(e); mark("client_artifact_capture_failed",error=str(e))
    observed=rows[-1]["primary"]["ts"]
    lag=max(0,lsn(flush)-lsn(replay)) if flush and replay else None
    timing={"vacuumStart":start["time"] if start else None,"vacuumComplete":complete["time"] if complete else None,
      "heapWorkStart":start["time"] if start and first_progress else None,"heapWorkFirstProgress":first_progress,"heapWorkLastProgress":last_progress,
      "heapWorkComplete":heap_complete,"heapWorkCompleteLowerBound":last_progress if heap_complete else None,"heapWorkCompleteUpperBound":heap_complete,
      "heapWorkCompletionObservedByProgress":bool(heap_complete),"vacuumTargetReachedWithClient":active_target_caught,
      "clientPostRecoveryHealthy":post_recovery_healthy,"clientPostRecoveryHealthySeconds":post_recovery_seconds,
      "clientWarmup":warmup,"clientStop":client_stop_result,"clientSummary":ACTIVE_CLIENT.get("summary") if ACTIVE_CLIENT else None,
      "clientSessionSamples":"client/sessions.jsonl","clientSessionAgeAndWaitSource":"primary pg_stat_activity xact/query ages and wait events, sampled once per second",
      "replayEnd":final_replay_end or replay_end,"observationEnd":observed,"replayCaughtUp":caught,
      "replayTargetLsn":complete["flushLsn"] if complete else None,"finalReplayTargetLsn":final_target,
      "finalReplayCaughtUp":final_caught,"replicaReplayLsnAtObservationEnd":replay,
      "primaryFlushLsnAtObservationEnd":flush,"replicaLagBytesAtObservationEnd":lag,"replayDrainSeconds":drain_seconds,
      "finalReplayDrainSeconds":final_drain_seconds,
      "remoteVacuumExitCode":rc,"walBeforeVacuum":wal_start,"walAtVacuumStart":start,"walAtVacuumComplete":complete,
      "vacuumOptions":vacuum["options"],"vacuumSql":vacuum["sql"],
      "walCountersAfterBackendExit":wal_exit,"walCounterDeltaFromBeforeVacuum":wal_delta,
      "walCounterDeltaScope":"cluster pg_stat_wal counters; includes background WAL, not physical disk writes",
      "checkpoint":{"before":wal_before,"after":wal_cp,"targetLsn":wal_cp["flushLsn"]},
      "preVacuumCheckpoint":pre_vacuum_checkpoint_result,"postVacuumObservationError":tail_error}
    measurement_ok=measurement_complete(rc,complete,caught,failure,active_target_caught,post_recovery_healthy,final_caught)
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
                "replayCaughtUp":caught,"vacuumTargetReachedWithClient":active_target_caught,
                "clientPostRecoveryHealthy":post_recovery_healthy,"finalReplayCaughtUp":final_caught,
                "replicaLagBytesAtObservationEnd":lag,"replayDrainSeconds":drain_seconds,"finalReplayDrainSeconds":final_drain_seconds}
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
    return 0 if test_complete else 2

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
    finally:
        for error in stop_active_client(): print(f"ERROR cleanup: {error}",file=sys.stderr)
        lock.close()

if __name__=="__main__": raise SystemExit(main())
