#!/usr/bin/env python3
"""Run the final 23-GiB VACUUM/replay measurement against the owned DATA fixture."""
from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback

HERE = Path(__file__).resolve().parents[1]
STATE = HERE / ".state"
ROOT = Path("/workspace")
VENV_PYTHON = ROOT / "vacuum-only-repro/.venv/bin/python"
FOLLOWER = ROOT / "experiments/20261005-four/follow-replication.py"
TOKEN = "16244c2c1353a0fc1b108ec455fd70c8"
DATA_PATH = "/var/lib/postgresql/data"
PGDATA_PATH = DATA_PATH + "/pgdata"
EXPECTED_METHOD = {
    "workers": 5, "blockBytes": 1048576, "ioDepthPerProcess": 32,
    "scheduler": "mq-deadline", "nrRequests": 1024, "asyncDepth": 512,
    "fixtureTotalBytes": 4294967296,
    "minimumWholeTimelineGuestReadAwaitMs": 1500, "maximumWholeTimelineGuestReadAwaitMs": 4000,
}
FILE_BYTES = 1024 ** 3
PINS = {
    "03-test.py": "9b577902792836255130135d3e55bad198d2aa9ff42d9c916fc6113e4c737952",
    "read_pressure.py": "4b7c1a4c041648620feeb2019b8e7f7b6db8130113f8e06f4857fe41d10303e7",
    "plot.py": "aee8da47eecbf35d9218c2671112581cacce06a6870895ebf449aa360f8ceb9d",
    "../follow-replication.py": "1db22659381e3b22b4d475ea459a2fdaf9a2c665d2bf31f5bcf66ef1f52bb26f",
    "read-pressure.c": "20f47999d3b6bd4e55aecf5a4c6d417cefef9556c402a9a504c59a4d5b027e67",
    ".state/calibrate_with_startup_retry.py": "3e8d4f1e9fa0211a61066f85838898517632675a950e7180e5dd04650fac716f",
    ".state/apply_async512_mq_deadline_1024_data_sdc8_32.sh": "1cc376f5be26b881f970c25e8c14f15cead3cf510aa4f7ae550207a8b1fe6326",
    ".state/restore_async_depth_256_data_sdc8_32.sh": "b24c18170d096bce0ba6abfa2ec6f0367403994021118073f73d0949c41fe108",
    ".state/restore_data_queue_defaults_sdc8_32.sh": "273e4c165ba6e1e3c643994cc117b5b5c5fdf0ef7ab832768b6514f7737e917c",
    ".state/record_final_auxiliary_cooldown.py": "5b9ec0df4920a259b01cc9f120ce4f98f16fb1aa43e28f4ac74af18b63011c20",
    "../write-report.py": "c3c265c6d7242da4aab74556c8f0cd3f1f7d9557c59532d584937aacedacab42",
}


def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def save(path: Path, obj):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def load(path):
    return json.loads(Path(path).read_text())


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run(argv, *, input=None, timeout=60):
    proc = subprocess.Popen(argv, stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                            start_new_session=True, close_fds=True)
    try:
        out, err = proc.communicate(input=input, timeout=timeout)
        return subprocess.CompletedProcess(argv, proc.returncode, out, err)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            out, err = proc.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            out, err = proc.communicate()
        return subprocess.CompletedProcess(argv, 124, out, (err or "") + "\ncontroller killed process group on timeout")


def output_record(path, proc):
    save(path, {"argv": list(map(str, proc.args)), "returnCode": proc.returncode,
                "stdout": proc.stdout, "stderr": proc.stderr, "completedAtHost": stamp()})


def check_pins():
    changed = {rel: {"expected": value, "actual": sha(HERE / rel)}
               for rel, value in PINS.items() if sha(HERE / rel) != value}
    if changed:
        raise RuntimeError(f"audited dependency hashes changed: {changed}")


def import_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    return mod


def parse_cpu(q):
    if q.endswith("m"):
        return float(q[:-1]) / 1000
    if q.endswith("u"):
        return float(q[:-1]) / 1_000_000
    if q.endswith("n"):
        return float(q[:-1]) / 1_000_000_000
    return float(q)


def parse_memory(q):
    scales = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
              "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4}
    for suffix, scale in scales.items():
        if q.endswith(suffix):
            return float(q[:-len(suffix)]) * scale
    return float(q)


def node_headroom(node, all_pods, pressure_cpu="25m", pressure_mem="384Mi"):
    alloc = node.get("status", {}).get("allocatable", {})
    alloc_cpu, alloc_mem = parse_cpu(alloc["cpu"]), parse_memory(alloc["memory"])
    used_cpu = used_mem = 0.0
    rows = []
    for pod in all_pods:
        if pod.get("spec", {}).get("nodeName") != node["metadata"]["name"]:
            continue
        if pod.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            continue
        regular_cpu = regular_mem = 0.0
        for c in pod.get("spec", {}).get("containers", []):
            rq = c.get("resources", {}).get("requests", {})
            regular_cpu += parse_cpu(rq.get("cpu", "0"))
            regular_mem += parse_memory(rq.get("memory", "0"))
        init_cpu = init_mem = 0.0
        for c in pod.get("spec", {}).get("initContainers", []):
            rq = c.get("resources", {}).get("requests", {})
            init_cpu = max(init_cpu, parse_cpu(rq.get("cpu", "0")))
            init_mem = max(init_mem, parse_memory(rq.get("memory", "0")))
        overhead = pod.get("spec", {}).get("overhead", {})
        cpu = max(regular_cpu, init_cpu) + parse_cpu(overhead.get("cpu", "0"))
        mem = max(regular_mem, init_mem) + parse_memory(overhead.get("memory", "0"))
        used_cpu += cpu
        used_mem += mem
        rows.append({"namespace": pod.get("metadata", {}).get("namespace"),
                     "name": pod.get("metadata", {}).get("name"),
                     "cpuRequestCores": cpu, "memoryRequestBytes": mem})
    request_cpu, request_mem = parse_cpu(pressure_cpu), parse_memory(pressure_mem)
    result = {"allocatableCpuCores": alloc_cpu, "allocatableMemoryBytes": alloc_mem,
              "scheduledContainerCpuRequestsCores": used_cpu,
              "scheduledContainerMemoryRequestsBytes": used_mem,
              "estimatedAvailableCpuRequestCores": alloc_cpu - used_cpu,
              "estimatedAvailableMemoryRequestBytes": alloc_mem - used_mem,
              "plannedReaderRequestCpuCores": request_cpu,
              "plannedReaderRequestMemoryBytes": request_mem,
              "scheduledPods": rows,
              "calculation":"sum of active pod requests using max(sum of regular containers, max init container) plus pod overhead; node allocatable already excludes node reservations"}
    if alloc_cpu - used_cpu < request_cpu or alloc_mem - used_mem < request_mem:
        raise RuntimeError(f"reader request does not fit conservative node request headroom: {result}")
    return result


def queue_suffix():
    return r'''
printf '__DATA_QUEUE_PROOF__\n'
data_disk=$(basename "$(readlink -f "/sys/dev/block/$DATA_MAJOR_MINOR")")
q="/sys/class/block/$data_disk/queue"; ios="$q/iosched"; dev="/sys/class/block/$data_disk/device"
scheduler=$(cat "$q/scheduler"); active_scheduler=$(printf '%s\n' "$scheduler" | sed -n 's/.*\[\([^]]*\)\].*/\1/p')
sysdev=$(readlink -f "$dev"); qdepth=$(cat "$dev/queue_depth"); nr=$(cat "$q/nr_requests")
async=$(cat "$ios/async_depth"); maxsec=$(cat "$q/max_sectors_kb"); maxhw=$(cat "$q/max_hw_sectors_kb")
hctx=''; for d in "/sys/class/block/$data_disk"/mq/*; do [ -d "$d" ] || continue; tags=$(cat "$d/nr_tags" 2>/dev/null || echo missing); hctx="${hctx}${d##*/}:${tags},"; done
tunables=''; for n in writes_starved fifo_batch read_expire write_expire front_merges; do [ -r "$ios/$n" ] || continue; v=$(cat "$ios/$n"); tunables="${tunables}${n}=${v},"; done
printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' "$DATA_MAJOR_MINOR" "$data_disk" "$active_scheduler" "$nr" "$async" "$qdepth" "$maxsec" "$maxhw" "$sysdev" "$hctx" "$tunables" "$scheduler"
'''


def install_runtime_hooks(test, rp, startup, cooldown, fixture, infra, prep, method, manifest, paths):
    """Redirect the audited native-AIO helper only to the token-owned sibling fixture."""
    ns, cluster = prep.get("cluster", "repro/hdd-pg").split("/", 1)
    fixture_names = list(manifest["fileNames"])
    data_dir = manifest["destinationRelativeDirectory"]
    lock = threading.Lock()
    queue_jsonl = paths["controller"] / "replica-data-queue-sysfs.jsonl"
    cpu_jsonl = paths["out"] / "reader-cpu-cgroup.jsonl"
    treatment_state = paths["treatment_state"]
    original_sample = test.sample
    original_plan = rp.relation_plan
    original_start = rp.ReaderPressure.start
    original_pod_status = rp.ReaderPressure.pod_status

    def auxiliary_plan(base, db, pod, layout, block_bytes=8192):
        data = layout["roles"]["replica"]["data"]
        if (data.get("claim") != manifest["dataClaim"] or data.get("pv") != manifest["dataPv"] or
            data.get("volumeHandle") != manifest["azureDiskId"] or data.get("node") != manifest["replicaNode"] or
            str(data.get("lun")) != "0" or data.get("path") != DATA_PATH):
            raise RuntimeError(f"live DATA mapping differs from the generated fixture: {data}")
        uid_out = rp.run(base + ["exec", "-c", "postgres", "-n", ns, pod["metadata"]["name"], "--",
                                "sh", "-c", "id -u postgres; id -g postgres"], 20, retryable=True).splitlines()
        if len(uid_out) < 2 or not all(x.isdigit() for x in uid_out[:2]):
            raise RuntimeError(f"cannot resolve postgres uid/gid: {uid_out}")
        target = f"/mnt/data/{data_dir}/{fixture_names[0]}"
        effective_block_bytes = int(method["blockBytes"])
        return {"namespace": ns, "replicaPod": pod["metadata"]["name"],
                "replicaPodUid": pod["metadata"]["uid"], "replicaNode": pod["spec"]["nodeName"],
                "dataClaim": data["claim"], "dataPv": data["pv"], "dataMountPath": DATA_PATH,
                "auxiliaryDirectoryInReplica": f"{DATA_PATH}/{data_dir}",
                "relationPath": data_dir + "/" + fixture_names[0],
                "relationFilePathInReplica": f"{DATA_PATH}/{data_dir}/{fixture_names[0]}",
                "readerPath": target, "postgresUid": int(uid_out[0]), "postgresGid": int(uid_out[1]),
                "directIoBytes": effective_block_bytes,
                "helperPlanDefaultBlockBytes": block_bytes,
                "relationSegments": [{"name": n, "bytes": int(FILE_BYTES)} for n in fixture_names],
                "blockSizeCoverage": {str(effective_block_bytes): {"requestedBlockBytes": effective_block_bytes,
                    "totalSegmentBytes": method["fixtureTotalBytes"], "eligibleSegmentCount": len(fixture_names),
                    "eligibleBytes": method["fixtureTotalBytes"], "excludedBytes": 0,
                    "excludedSegments": [], "partialTailBytesWithinEligibleSegments": 0}},
                "readOnlyMount": True, "directIo": True, "sqlClientCreated": False,
                "verifiedReadableInReplica": True, "targetKind": "token-owned generated auxiliary files outside PGDATA",
                "fixtureManifest": str(paths["fixture_manifest"]), "cooldownProof": str(paths["cooldown_proof"])}

    def sample_with_queue(base, namespace, db, app, pod, mapping):
        if (pod["metadata"]["labels"].get("cnpg.io/instanceRole") != "replica" or
            not treatment_state.get("active")):
            # The wrapper's own setup sample runs before queue treatment; delegate to the
            # original read-only sampler so default none/316 is accepted and no output files appear.
            return original_sample(base, namespace, db, app, pod, mapping)
        env = {"DB_NAME": db, "VAC_APP": app,
               "DATA_MAJOR_MINOR": manifest["dataMajorMinor"]}
        for k in ("data", "wal"):
            env[k.upper() + "_LUN"] = mapping[k]["lun"]
            env[k.upper() + "_LUN_PATH"] = mapping[k]["attachmentDevicePath"]
        prefix = " ".join(f"{k}={test.shlex.quote(v)}" for k, v in env.items())
        out = test.cmd(base + ["exec", "-c", "postgres", "-n", namespace, pod["metadata"]["name"], "--",
                       "sh", "-c", prefix + "\n" + test.REMOTE + queue_suffix()], 40, retryable=True)
        sections = out.split("__DATA_QUEUE_PROOF__\n", 1)
        if len(sections) != 2:
            raise RuntimeError("per-sample DATA queue proof is missing")
        row = test.parse_sample(sections[0], pod, mapping)
        if row["devices"]["data"].get("majorMinor") != manifest["dataMajorMinor"]:
            raise RuntimeError(f"sample DATA device {row['devices']['data'].get('majorMinor')} differs from fixture device {manifest['dataMajorMinor']}")
        f = sections[1].splitlines()[0].split("|")
        if len(f) != 12:
            raise RuntimeError(f"malformed per-sample queue proof: {sections[1][:500]!r}")
        mm, device, active, nr, async_depth, depth, maxsec, maxhw, sysdev, hctx, tunables, sched = f
        proof = {"majorMinor": mm, "device": device, "activeScheduler": active,
                 "schedulerReadback": sched, "nrRequests": int(nr), "asyncDepth": int(async_depth),
                 "scsiDeviceQueueDepth": int(depth), "maxSectorsKb": int(maxsec),
                 "maxHwSectorsKb": int(maxhw), "sysfsDevicePath": sysdev,
                 "hctxNrTags": hctx, "mqDeadlineTunables": tunables, "capturedAfterSampleAt": row["ts"]}
        if row["role"] == "replica":
            expected = (mm == "8:32" and device == "sdc" and active == "mq-deadline" and
                        int(nr) == 1024 and int(async_depth) == 512 and int(depth) == 316 and
                        int(maxsec) == 1280 and int(maxhw) == 2048 and sysdev.endswith("/1:0:0:0"))
            if not expected:
                raise RuntimeError(f"replica DATA queue departed from selected method: {proof}")
            row["replicaDataSysfsQueueProof"] = proof
        else:
            row["primaryDataSysfsQueueProof"] = proof
        with lock, queue_jsonl.open("a") as fobj:
            fobj.write(json.dumps({"role": row["role"], "sampleAt": row["ts"],
                                   "treatmentActive": True, **proof}, sort_keys=True) + "\n")
        if row["role"] != pod["metadata"]["labels"].get("cnpg.io/instanceRole") or row["appSessions"]:
            raise RuntimeError("role mismatch or app client session detected")
        return row

    def reader_start(self, workers, tag):
        result = original_start(self, workers, tag)
        pod = json.loads(rp.run(self.base + ["get", "pod", self.pod, "-n", self.ns, "-o", "json"],
                                30, retryable=True))
        spec = pod["spec"]
        readers = next(c for c in spec["containers"] if c["name"] == "readers")
        volume_claims = {v["name"]: v.get("persistentVolumeClaim", {}).get("claimName") for v in spec.get("volumes", [])}
        data_mount = next(m for m in readers.get("volumeMounts", []) if m.get("name") == "data")
        rq = readers.get("resources", {})
        request = rq.get("requests", {})
        limits = rq.get("limits", {})
        if (spec.get("nodeName") != manifest["replicaNode"] or volume_claims.get("data") != manifest["dataClaim"] or
            data_mount.get("mountPath") != "/mnt/data" or data_mount.get("readOnly") is not True or
            request.get("cpu") != "25m" or limits.get("cpu") != "500m" or limits.get("memory") != "1Gi" or
            parse_memory(request.get("memory", "0Mi")) != 384 * 1024**2):
            raise RuntimeError(f"reader pod placement/resources/mount failed: {spec}")
        record = {"capturedAtHost": stamp(), "pod": self.pod, "podUid": pod["metadata"].get("uid"),
                  "node": spec.get("nodeName"), "claim": volume_claims.get("data"),
                  "mountReadOnly": data_mount.get("readOnly"), "resources": rq,
                  "readerImage": readers.get("image"), "expectedProcesses": workers,
                  "ioDepthPerProcess": self.depth, "blockBytes": self.block_bytes,
                  "target": self.plan["readerPath"], "targetKind": self.plan["targetKind"],
                  "projectedNodeHeadroom": paths["headroom"]}
        save(paths["out"] / "reader-resource-placement.json", record)
        self._full_pressure_record = record
        self._full_cpu_sample_mono = 0.0
        sample_cpu(self, force=True)
        return result

    def sample_cpu(reader, force=False):
        now = time.monotonic()
        if not force and now - getattr(reader, "_full_cpu_sample_mono", 0.0) < 30:
            return
        reader._full_cpu_sample_mono = now
        try:
            raw = rp.run(reader.base + ["exec", "-n", reader.ns, reader.pod, "-c", "readers", "--",
                    "sh", "-c", "cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/cpu.stat"], 20, retryable=True)
            lines = raw.splitlines()
            quota, period = lines[0].split()
            values = dict(line.split() for line in lines[1:] if len(line.split()) == 2)
            item = {"capturedAtHost": stamp(), "pod": reader.pod, "cpuMaxQuota": quota,
                    "cpuMaxPeriod": int(period), "usageUsec": int(values["usage_usec"]),
                    "nrPeriods": int(values["nr_periods"]), "nrThrottled": int(values["nr_throttled"]),
                    "throttledUsec": int(values["throttled_usec"])}
        except Exception as exc:
            item = {"capturedAtHost": stamp(), "pod": getattr(reader, "pod", None), "error": str(exc)}
        with lock, cpu_jsonl.open("a") as fobj:
            fobj.write(json.dumps(item, sort_keys=True) + "\n")

    def pod_status(reader):
        status = original_pod_status(reader)
        sample_cpu(reader)
        return status

    rp.relation_plan = lambda base, namespace, db, pod, layout, block_bytes=8192: auxiliary_plan(base, db, pod, layout, block_bytes)
    test.sample = sample_with_queue
    rp.ReaderPressure.start = reader_start
    rp.ReaderPressure.pod_status = pod_status


def topology_preflight(base, test, ns, cluster, replica_node, primary_node, outpath):
    nodes = json.loads(test.cmd(base + ["get", "nodes", "-o", "json"], retryable=True))["items"]
    node_by_name = {x["metadata"]["name"]: x for x in nodes}
    pods = json.loads(test.cmd(base + ["get", "pods", "-A", "-o", "json"], retryable=True))["items"]
    def ready(p):
        return p.get("status", {}).get("phase") == "Running" and any(
            c.get("type") == "Ready" and c.get("status") == "True"
            for c in p.get("status", {}).get("conditions", []))
    operator = [p for p in pods if "cnpg-controller-manager" in p.get("metadata", {}).get("name", "")]
    dns = [p for p in pods if "coredns" in p.get("metadata", {}).get("name", "") or
           p.get("metadata", {}).get("labels", {}).get("k8s-app") == "kube-dns"]
    kon = [p for p in pods if "konnectivity-agent" in p.get("metadata", {}).get("name", "")]
    require = lambda ok, msg: (_ for _ in ()).throw(RuntimeError(msg)) if not ok else None
    require(operator and all(ready(p) and p.get("spec", {}).get("nodeName") != replica_node for p in operator),
            "CNPG operator is absent or a controller pod is on the pressure node")
    require(any(ready(p) and p.get("spec", {}).get("nodeName") == primary_node for p in dns),
            "no Ready CoreDNS pod remains on the unpressured primary worker")
    require(any(ready(p) and p.get("spec", {}).get("nodeName") == primary_node for p in kon),
            "no Ready konnectivity-agent pod remains on the unpressured primary worker")
    payload = {"capturedAtHost": stamp(), "managedControlPlaneSeparateFromWorkerNodes": True,
               "pressureNode": replica_node, "unpressuredPrimaryNode": primary_node,
               "workerProviderIds": {n: node_by_name[n].get("spec", {}).get("providerID") for n in (primary_node, replica_node)},
               "operatorPods": [{"name": p["metadata"]["name"], "namespace": p["metadata"]["namespace"],
                                 "node": p["spec"].get("nodeName"), "ready": ready(p)} for p in operator],
               "coreDnsPods": [{"name": p["metadata"]["name"], "namespace": p["metadata"]["namespace"],
                                "node": p["spec"].get("nodeName"), "ready": ready(p)} for p in dns],
               "konnectivityPods": [{"name": p["metadata"]["name"], "namespace": p["metadata"]["namespace"],
                                     "node": p["spec"].get("nodeName"), "ready": ready(p)} for p in kon]}
    require(all(payload["workerProviderIds"].values()), "worker provider ID evidence is missing")
    save(outpath, payload)
    return payload, node_by_name[replica_node], pods


def selector_cleanup(base, ns, token, control, call):
    selector = f"repro.stack.token={token},repro.experiment=read-pressure"
    rows = []
    def attempt(name, argv, timeout=90):
        p = call(argv, timeout=timeout)
        output_record(control / (name + ".json"), p)
        rows.append({"name": name, "returnCode": p.returncode, "stdout": p.stdout, "stderr": p.stderr})
        return p
    deleted = attempt("delete-owned-reader-pods", base + ["delete", "pods", "-n", ns, "-l", selector,
                    "--ignore-not-found=true", "--wait=true", "--timeout=120s"], 135)
    listed = attempt("reader-pod-final-inventory", base + ["get", "pods", "-n", ns, "-l", selector, "-o", "json"], 30)
    no_pods = False
    if listed.returncode == 0:
        try:
            no_pods = not json.loads(listed.stdout).get("items", [])
        except Exception:
            no_pods = False
    cms = None
    if deleted.returncode == 0 and no_pods:
        cms = attempt("delete-owned-reader-configmaps", base + ["delete", "configmaps", "-n", ns, "-l", selector,
                       "--ignore-not-found=true", "--wait=true", "--timeout=60s"], 75)
    cm_list = attempt("reader-configmap-final-inventory", base + ["get", "configmaps", "-n", ns, "-l", selector, "-o", "json"], 30)
    no_cms = False
    if cm_list.returncode == 0:
        try:
            no_cms = not json.loads(cm_list.stdout).get("items", [])
        except Exception:
            no_cms = False
    return {"selector": selector, "steps": rows, "readerPodsAbsent": no_pods,
            "readerConfigMapsAbsent": no_cms,
            "passed": bool(deleted.returncode == 0 and no_pods and cms and cms.returncode == 0 and cm_list.returncode == 0 and no_cms)}


def validate_queue_apply_output(stdout):
    """Require explicit readback of the selected DATA-only queue state."""
    rows = [line for line in stdout.splitlines() if line.startswith("afterAsyncDepth512|")]
    if len(rows) != 1:
        raise RuntimeError("queue apply omitted a unique afterAsyncDepth512 readback")
    fields = {}
    for item in rows[0].split("|", 1)[1].split("|"):
        if "=" in item:
            key, value = item.split("=", 1)
            fields[key] = value
    expected = {"dev": "8:32", "nr": "1024", "depth": "316", "async_depth": "512",
                "max_sectors_kb": "1280", "max_hw_sectors_kb": "2048"}
    wrong = {key: {"expected": value, "actual": fields.get(key)}
             for key, value in expected.items() if fields.get(key) != value}
    scheduler = fields.get("scheduler", "")
    sysdev = fields.get("sys", "")
    if "[mq-deadline]" not in scheduler or "[none]" in scheduler:
        wrong["scheduler"] = {"expected": "mq-deadline active", "actual": scheduler}
    if not sysdev.endswith("/1:0:0:0"):
        wrong["sys"] = {"expectedSuffix": "/1:0:0:0", "actual": sysdev}
    if wrong:
        raise RuntimeError(f"queue apply readback mismatch: {wrong}")
    return fields


def aggregate_remote(data, wal):
    reads = [v.get("readMBps") for v in (data or {}, wal or {}) if isinstance(v.get("readMBps"), (int, float))]
    writes = [v.get("writeMBps") for v in (data or {}, wal or {}) if isinstance(v.get("writeMBps"), (int, float))]
    r = sum(reads) if len(reads) == 2 else None
    w = sum(writes) if len(writes) == 2 else None
    return {"dataPlusWalReadMBps": r, "dataPlusWalWriteMBps": w,
            "aggregateRemoteReadWriteMBps": r + w if r is not None and w is not None else None}


def add_cpu_summary(out):
    path = out / "reader-cpu-cgroup.jsonl"
    rows = [json.loads(x) for x in path.read_text().splitlines() if x.strip()] if path.is_file() else []
    valid = [x for x in rows if "error" not in x]
    summary = {"path": str(path), "samples": rows, "validSampleCount": len(valid),
               "limit": "500m", "request": "25m"}
    if len(valid) >= 2:
        first, last = valid[0], valid[-1]
        summary["delta"] = {k: last[k] - first[k] for k in ("usageUsec", "nrPeriods", "nrThrottled", "throttledUsec")}
        summary["cpuMax"] = f"{last['cpuMaxQuota']} {last['cpuMaxPeriod']}"
    return summary


def patch_reports(out, queue_rows, manifest, method, fixture_proof, topo_path, headroom):
    summary_path, timing_path = out / "summary.json", out / "timing.json"
    summary, timing = load(summary_path), load(timing_path)
    full_method = {"workers": method["workers"], "blockBytes": method["blockBytes"],
        "ioDepthPerProcess": method["ioDepthPerProcess"], "scheduler": method["scheduler"],
        "nrRequests": method["nrRequests"], "asyncDepth": method["asyncDepth"],
        "target": "four generated 1-GiB O_DIRECT files in a token-owned directory outside PGDATA on the replica DATA PVC",
        "readerPath": f"/mnt/data/{manifest['destinationRelativeDirectory']}/pressure",
        "fixtureBytes": manifest["totalBytes"], "fixtureManifest": str(fixture_proof["fixtureManifestPath"]),
        "cooldownProof": str(STATE / "auxiliary-random-cooldown-final-proof.json"),
        "sameDeviceReplayAndExternalReadAwait": True}
    rp = summary.setdefault("readPressure", {})
    rp["method"] = f"{method['blockBytes']}-byte random O_DIRECT native-AIO against token-owned generated DATA files; depth={method['ioDepthPerProcess']} per process"
    rp["selectedTreatment"] = full_method
    rp["targetPlanArtifact"] = "read-pressure-start.json"
    rp["replicaDataSysfsQueueProof"] = {"path": "replica-data-queue-sysfs.jsonl",
        "rows": len(queue_rows), "allReplicaSamplesPassed": True,
        "treatment": {k: method[k] for k in ("scheduler", "nrRequests", "asyncDepth")},
        "sampledFields": ["majorMinor", "activeScheduler", "nrRequests", "asyncDepth",
                          "scsiDeviceQueueDepth", "maxSectorsKb", "maxHwSectorsKb", "sysfsDevicePath", "hctxNrTags", "mqDeadlineTunables"]}
    rp["readerCpuCgroupEvidence"] = add_cpu_summary(out)
    rp["nodeRequestHeadroom"] = headroom
    rp["topologyArtifact"] = str(topo_path)
    rp["fixturePreparationAndCooldown"] = {"manifest": str(fixture_proof["fixtureManifestPath"]),
        "generatedBytes": manifest["totalBytes"], "allocatedBytes": manifest["destinationAllocatedBytes"],
        "generationCompletedAtHost": manifest["generationCompletedAtHost"],
        "measuredNoReaderCooldownSeconds": fixture_proof["measuredCooldownSeconds"],
        "dropCaches": False, "cacheEvictionProven": False,
        "payload": manifest["payloadKind"]}
    rp["replicaDataPlusWalThroughputDuringVacuumAndReplay"] = aggregate_remote(
        summary.get("windows", {}).get("readPressureReplay", {}).get("replica_data"),
        summary.get("windows", {}).get("readPressureReplay", {}).get("replica_wal"))
    rp["replicaDataPlusWalThroughputDuringReplayDrain"] = aggregate_remote(
        summary.get("windows", {}).get("readPressureReplayTail", {}).get("replica_data"),
        summary.get("windows", {}).get("readPressureReplayTail", {}).get("replica_wal"))
    rp["externalReadPressureDisclosure"] = (
        "Replica DATA device IOPS, throughput, and operation-weighted await include PostgreSQL activity plus the deliberate 5-process, 1-MiB O_DIRECT readers of generated auxiliary files. This is not PostgreSQL-only replay amplification. Reader submit-to-completion latency is reported separately from guest block-device await.")
    rp["queueTreatmentIsExplicitAdditionalVariable"] = True
    rp["readerOnlyCalibration"] = {"operationWeightedGuestDataReadAwaitMs": method["calibration"]["operationWeightedGuestDataReadAwaitMs"],
        "dataReadMBps": method["calibration"]["dataReadMBps"], "workers": 5, "ioDepthPerProcess": 32,
        "blockBytes": 1048576, "seconds": method["calibration"]["measuredSeconds"],
        "condition": "active replay, positive backlog; same-device whole-guest metric included these external reads"}
    # Summarize the 1.5-second analysis threshold from the raw block counter series.
    plot = import_module("latencycap5_report_plot", HERE / "plot.py")
    raw_rows = [json.loads(line) for line in (out / "samples.jsonl").read_text().splitlines() if line.strip()]
    series, _, _ = plot.build(raw_rows, 5)
    tail_start = plot.stamp(timing["vacuumComplete"]).timestamp()
    tail_end = plot.stamp(timing["replayEnd"]).timestamp()
    rp["sustainedOnePointFiveSecondReadAwaitDuringReplayDrain"] = plot.sustained_read_await(
        series["replica_data"], tail_start, tail_end, threshold_ms=1500, minimum_seconds=60)
    rp["analysisBandMs"] = {"minimum": 1500, "maximum": 4000,
        "averageBasis": "operation-weighted replica DATA block read await over VACUUM plus replay"}
    rp["wholeTimelineGuestDataReadAwaitMs"] = summary["windows"]["vacuumAndReplay"]["replica_data"]["readAwaitMs"]
    timing.setdefault("readPressure", {})["method"] = rp["method"]
    timing["readPressure"]["selectedTreatment"] = full_method
    timing["readPressure"]["readerCpuCgroupEvidence"] = rp["readerCpuCgroupEvidence"]
    timing["readPressure"]["replicaDataSysfsQueueProof"] = rp["replicaDataSysfsQueueProof"]
    timing["readPressure"]["fixturePreparationAndCooldown"] = rp["fixturePreparationAndCooldown"]
    start_path = out / "read-pressure-start.json"
    if start_path.is_file():
        start = load(start_path)
        start["method"] = rp["method"]
        start["targetKind"] = full_method["target"]
        start["selectedTreatment"] = full_method
        save(start_path, start)
    pressure_path = out / "read-pressure.json"
    if pressure_path.is_file():
        pressure = load(pressure_path)
        pressure["method"] = rp["method"]
        pressure["selectedTreatment"] = full_method
        pressure["readerCpuCgroupEvidence"] = rp["readerCpuCgroupEvidence"]
        pressure["replicaDataSysfsQueueProof"] = rp["replicaDataSysfsQueueProof"]
        save(pressure_path, pressure)
    save(summary_path, summary)
    save(timing_path, timing)


def generate_report(out, method, manifest, proof, control, infra):
    writer = ROOT / "experiments/20261005-four/write-report.py"
    report_path = out / "REPORT.md"
    proc = run([str(VENV_PYTHON), str(writer), str(out), "--title",
                "23-GiB read pressure with auxiliary DATA reads", "--output", str(report_path)], timeout=120)
    output_record(control / "report-generator.json", proc)
    if proc.returncode != 0 or not report_path.is_file():
        raise RuntimeError(f"report generator failed: {proc.stderr[-1200:]}")
    report = report_path.read_text()
    report = report.replace(
        "Fresh half-dead heap: ", "Prepared half-dead heap: ")
    report = report.replace(
        "The retained source keeper was restored, the bounded interleaved fixture was rebuilt, and CNPG seeded the replica from paired DATA/WAL snapshots.",
        "The retained prepared source was restored to primary, and a fresh paired CNPG DATA/WAL VolumeSnapshot Backup seeded the standby. The PostgreSQL heap was not rebuilt with SQL. A separate four-file 4-GiB random-read fixture was generated outside PGDATA on standby DATA.")
    report = report.replace(
        "This baseline uses the same East US region, worker SKU, fixture size, and unmodified experiment scripts as the original run.",
        "This is the WestUS3 read-pressure variation on the same D2s_v4 worker size and prepared 23-GiB heap. The queue scheduler/depth and 1-MiB external read size are explicit treatment variables; see the calibration and sysfs evidence below.")
    report = report.replace("| Original 23 GiB | Unchanged repeat |", "| Prior 23 GiB run | Read-pressure variation |")
    summary = load(out / "summary.json")
    timing = load(out / "timing.json")
    pressure = summary.get("readPressure", {})
    vacuum = summary.get("windows", {}).get("vacuum", {}).get("replica_data", {})
    replay = summary.get("windows", {}).get("readPressureReplay", {}).get("replica_data", {})
    tail = summary.get("windows", {}).get("readPressureReplayTail", {}).get("replica_data", {})
    seed = load(STATE / "reset-audit/snapshot-seed-verification.json")
    snapshot_ids = {row["kind"]: row["snapshotId"] for row in seed.get("newPairedSnapshots", [])}
    if not {"data", "wal"}.issubset(snapshot_ids):
        raise RuntimeError("fresh paired snapshot IDs are absent from the verified seed artifact")
    fmt = lambda x: "unavailable" if x is None else f"{x:,.2f}"
    detail = [
        "", "## Read-pressure treatment and interpretation", "",
        f"The pressure workers read four fully allocated 1-GiB generated files at `{pressure.get('selectedTreatment', {}).get('readerPath')}`. The directory is a token-owned sibling of PGDATA on the standby DATA PVC, mounted read-only in the worker pod. Requests used {method['workers']} independent processes, depth {method['ioDepthPerProcess']}, and {method['blockBytes']:,}-byte O_DIRECT reads.",
        f"During the bounded active-replay calibration, the whole-guest replica DATA read-await was {fmt(method['calibration']['operationWeightedGuestDataReadAwaitMs'])} ms over {fmt(method['calibration']['measuredSeconds'])} seconds, at {fmt(method['calibration']['dataReadMBps'])} decimal MB/s. That calibration is a same-device aggregate that includes the 1-MiB external readers and replay; it is not PostgreSQL-only latency. The full-run DATA read-await during VACUUM was {fmt(vacuum.get('readAwaitMs'))} ms, and the pressure/replay window was {fmt(replay.get('readAwaitMs'))} ms at {fmt(replay.get('readIops'))} read IOPS / {fmt(replay.get('readMBps'))} MB/s. Replay-drain tail read-await was {fmt(tail.get('readAwaitMs'))} ms. Reader submit-to-completion latency remains a separate metric.",
        f"Across primary VACUUM plus full replica replay, operation-weighted guest DATA read await was {fmt(summary['windows']['vacuumAndReplay']['replica_data'].get('readAwaitMs'))} ms. The replay-drain evidence for sustained latency of at least 1,500 ms is in [summary.json](summary.json).",
        f"The selected standby DATA-only queue treatment was `{method['scheduler']}`, `nr_requests={method['nrRequests']}`, and `async_depth={method['asyncDepth']}` with hardware queue depth 316. Per-sample sysfs readback is in [replica-data-queue-sysfs.jsonl](replica-data-queue-sysfs.jsonl); it records the DATA disk mapping, scheduler, queue limits, hctx tags, and mq-deadline tunables. The controller restored async depth 256 under mq-deadline and then restored the default `[none]` scheduler / 316 software queue; see [cleanup-summary.json](../" + control.name + "/cleanup-summary.json).",
        f"The read fixture was generated by direct random-data writes, with `{manifest['totalBytes']:,}` logical bytes and `{manifest['destinationAllocatedBytes']:,}` allocated bytes. A sampled no-reader cooldown of {fmt(proof.get('measuredCooldownSeconds'))} seconds verified the exact file inventory, device identity, default queue and caught-up replication at both boundaries. `dropCaches=false`; the cooldown does not prove page-cache eviction. Generation and earlier O_DIRECT calibration history can affect cache/device warm-up and are recorded in [auxiliary-target-preflight.json](auxiliary-target-preflight.json).",
        f"Snapshot-seed verification tied this run to CNPG Backup `{seed['backupName']}` (UID `{seed['backupUid']}`) and fresh retained DATA/WAL snapshots: `{snapshot_ids['data']}` and `{snapshot_ids['wal']}`. The original prepared keeper snapshot and every earlier experiment snapshot remain preserved.",
        f"Pressure pod placement and CPU resource requests/limits are recorded in [reader-resource-placement.json](reader-resource-placement.json); cgroup runtime accounting is in [reader-cpu-cgroup.jsonl](reader-cpu-cgroup.jsonl). Topology evidence in [topology-preflight.json](../" + control.name + "/topology-preflight.json) records worker provider IDs, CNPG operator placement, and Ready CoreDNS/konnectivity pods on the unpressured worker. The AKS control plane is managed separately from these worker nodes.",
        "",
    ]
    report += "\n".join(detail)
    report_path.write_text(report)
    return report_path



def evaluate_full_acceptance(timing, summary, pressure, start, sample_rows, test_state, cleanup, failure, report_exists):
    workers = (pressure.get("readerProcess") or {}).get("workerSummary") or {}
    marker_workers = int(start.get("plannedWorkers", 0))
    coverage_valid = bool(summary.get("coverage", {}).get("valid"))
    sustained = summary.get("readPressure", {}).get("sustainedOnePointFiveSecondReadAwaitDuringReplayDrain", {})
    replay_samples = [x for x in sample_rows if x.get("phase") in ("vacuum", "replay_drain")]
    all_sample_qproofs = [x.get("replica", {}).get("replicaDataSysfsQueueProof") for x in sample_rows]
    qproofs = [x.get("replica", {}).get("replicaDataSysfsQueueProof") for x in replay_samples]
    qproof_ok = bool(qproofs) and all(x and x.get("activeScheduler") == "mq-deadline" and
        x.get("nrRequests") == 1024 and x.get("asyncDepth") == 512 and x.get("majorMinor") == "8:32" for x in qproofs)
    qproof_ok = qproof_ok and all(x and x.get("activeScheduler") == "mq-deadline" and
        x.get("nrRequests") == 1024 and x.get("asyncDepth") == 512 and x.get("majorMinor") == "8:32"
        for x in all_sample_qproofs)
    reader_process = pressure.get("readerProcess") or {}
    exits = reader_process.get("exitEvents", [])
    health_ok = (marker_workers == 5 and start.get("start", {}).get("readyWorkers") == 5 and
        pressure.get("plannedWorkers") == 5 and pressure.get("lostAt") is None and
        pressure.get("loadActiveUntilCatchup") is True and pressure.get("stopError") is None and
        reader_process.get("stopped") is True and workers.get("workerProcessesReported") == 5 and
        workers.get("readErrors") == 0 and workers.get("readOps", 0) > 0 and len(exits) == 5 and
        all(e.get("errors", 1) == 0 for e in exits))
    overall_await = summary.get("windows", {}).get("vacuumAndReplay", {}).get("replica_data", {}).get("readAwaitMs")
    overall_within_limit = isinstance(overall_await, (int, float)) and 1500 <= overall_await <= 4000
    successful = bool(not failure and test_state.get("phase") == "complete" and
        timing.get("testComplete") is True and timing.get("replayCaughtUp") is True and
        summary.get("status", {}).get("testComplete") is True and coverage_valid and health_ok and
        qproof_ok and overall_within_limit and sustained.get("reached") is True and cleanup.get("passed") and
        cleanup.get("monitorFinished") and report_exists)
    return {"successful": successful, "coverageValid": coverage_valid,
        "overallReadLatencyWithinUserLimit": overall_within_limit, "overallGuestDataReadAwaitMs": overall_await,
        "minimumOverallGuestDataReadAwaitMs": 1500, "maximumOverallGuestDataReadAwaitMs": 4000,
        "replayCaughtUp": timing.get("replayCaughtUp"), "testComplete": timing.get("testComplete"),
        "readerHealthAndExitTotalsValid": health_ok, "readerWorkerTotals": workers,
        "readerLostAt": pressure.get("lostAt"), "readAwaitTargetReachedInReplayDrain": sustained.get("reached"),
        "readAwaitTailEvidence": sustained, "queueProofSamples": len(qproofs),
        "queueProofValidForAllReplaySamples": qproof_ok, "cleanupPassed": cleanup.get("passed"),
        "monitorFinished": cleanup.get("monitorFinished"), "reportExists": report_exists,
        "testState": test_state}

def main():
    if "--help" in sys.argv[1:] or "-h" in sys.argv[1:]:
        print(__doc__)
        return 0
    check_pins()
    infra = load(STATE / "state.json")
    prep = load(STATE / "prepare.json")
    method = load(STATE / "selected-read-pressure-method.json")
    fixture_pointer = load(STATE / "auxiliary-random-fill-final.json")
    proof = load(STATE / "auxiliary-random-cooldown-final-proof.json")
    token = infra.get("ownerToken")
    if token != TOKEN or infra.get("status") != "ready" or prep.get("status") != "complete":
        raise RuntimeError("stack token/state or original prepared-fixture state is not ready")
    for k, v in EXPECTED_METHOD.items():
        if method.get(k) != v:
            raise RuntimeError(f"selected pressure method mismatch at {k}: {method.get(k)!r}")
    manifest_path = Path(fixture_pointer["manifestArtifact"]).resolve()
    manifest = load(manifest_path)
    if (fixture_pointer.get("status") != "complete" or fixture_pointer.get("ownerToken") != token or
        manifest.get("ownerToken") != token or manifest.get("kind") != "read-pressure-token-owned-generated-fixture" or
        manifest.get("copyStatus") != "complete" or manifest.get("dataMountPath") != DATA_PATH or
        manifest.get("pgdataPath") != PGDATA_PATH or manifest.get("fileNames") != ["pressure", "pressure.1", "pressure.2", "pressure.3"] or
        manifest.get("totalBytes") != EXPECTED_METHOD["fixtureTotalBytes"] or
        manifest.get("destinationRelativeDirectory") != f".repro-read-pressure-aux-{token}"):
        raise RuntimeError("fresh generated fixture manifest identity/layout is invalid")
    fresh_seed = load(STATE / "reset-audit/snapshot-seed-verification.json")
    if manifest.get("freshSeedBackupName") != fresh_seed.get("backupName"):
        raise RuntimeError("generated fixture is not tied to the currently verified fresh snapshot seed")
    if (proof.get("status") != "complete" or proof.get("ownerToken") != token or
        proof.get("fixtureManifestPath") != str(manifest_path) or proof.get("measuredCooldownSeconds", 0) < 300 or
        proof.get("wallAgeSinceGenerationSecondsAtEnd", 0) < 300 or proof.get("readerPodsDuringCooldown") is not False or
        not proof.get("caughtUpAtStart") or not proof.get("caughtUpAtEnd")):
        raise RuntimeError("final fixture cooldown proof is absent/invalid")
    for k in ("dataPv", "azureDiskId", "replicaNode", "dataLun", "dataMajorMinor"):
        if str(proof.get(k)) != str(manifest.get({"azureDiskId": "azureDiskId"}.get(k, k))):
            raise RuntimeError(f"cooldown proof and fixture disagree on {k}")
    argv = sys.argv[1:]
    outarg = None
    for i, v in enumerate(argv):
        if v == "--out" and i + 1 < len(argv): outarg = Path(argv[i+1]).resolve()
        elif v.startswith("--out="): outarg = Path(v.split("=", 1)[1]).resolve()
    if outarg is None or not outarg.is_relative_to(HERE / "results"):
        raise RuntimeError("a unique output directory under the owned stack results/ is required")
    expected_pairs = {"--interval": "5", "--safety-hours": "24", "--catchup-hours": "336",
                      "--read-pressure-workers": "5", "--read-pressure-depth": "32",
                      "--read-pressure-block-bytes": "1048576"}
    for key, value in expected_pairs.items():
        if key not in argv or argv[argv.index(key) + 1] != value:
            raise RuntimeError(f"full-run args must include {key} {value}")
    if "--kubeconfig" not in argv or Path(argv[argv.index("--kubeconfig") + 1]).resolve() != Path(infra["kubeconfigPath"]).resolve():
        raise RuntimeError("full run must use the exact token-owned kubeconfig")
    if "--kubectl" in argv and argv[argv.index("--kubectl") + 1] != "kubectl":
        raise RuntimeError("alternate kubectl is not allowed")

    control = HERE / "results" / ("full-aux-pressure-controller-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    control.mkdir(mode=0o700, parents=True, exist_ok=False)
    outarg.mkdir(mode=0o700, parents=True, exist_ok=False)
    cfg = Path(infra["kubeconfigPath"]).resolve()
    base = ["kubectl", "--kubeconfig", str(cfg)]
    ns, cluster = prep.get("cluster", "repro/hdd-pg").split("/", 1)
    startup = import_module("full_startup_retry_patch", STATE / "calibrate_with_startup_retry.py")
    rp = startup.rp
    sys.modules["read_pressure"] = rp
    test = import_module("full_aux_test", HERE / "03-test.py")
    cooldown = import_module("full_aux_cooldown", STATE / "record_final_auxiliary_cooldown.py")
    # Both helper instances use one retry audit; core run() configures its own global again for the same path.
    preflight_retry_log = control / "preflight-read-only-retries.jsonl"
    rp.configure_retry_log(preflight_retry_log)
    test.RETRY_LOG = preflight_retry_log
    cooldown.rp.configure_retry_log(preflight_retry_log)
    nsobj = json.loads(test.cmd(base + ["get", "namespace", ns, "-o", "json"], retryable=True))
    cobj = json.loads(test.cmd(base + ["get", "cluster", cluster, "-n", ns, "-o", "json"], retryable=True))
    if nsobj.get("metadata", {}).get("labels", {}).get("repro.stack.token") != token or cobj.get("metadata", {}).get("labels", {}).get("repro.stack.token") != token:
        raise RuntimeError("namespace/Cluster ownership token mismatch")
    if cobj.get("spec", {}).get("instances") != 2:
        raise RuntimeError("expected exactly two CNPG instances")
    pods = rp.find_pods(base, ns, cluster)
    primary, replica = pods["primary"], pods["replica"]
    if primary["spec"].get("nodeName") == replica["spec"].get("nodeName"):
        raise RuntimeError("primary and standby must be on separate workers")
    layout = test.storage(base, ns, pods)
    data = layout["roles"]["replica"]["data"]
    for k, mk in (("pv", "dataPv"), ("volumeHandle", "azureDiskId"), ("node", "replicaNode")):
        if data.get(k) != manifest.get(mk): raise RuntimeError(f"current standby DATA {k} differs from generated fixture")
    if data.get("claim") != "hdd-pg-2" or str(data.get("lun")) != "0" or data.get("path") != DATA_PATH:
        raise RuntimeError(f"standby DATA PVC/LUN/mount is out of scope: {data}")
    before = test.sample(base, ns, "repro", "full-pressure-preflight", replica, layout["roles"]["replica"])
    qbefore = before["devices"]["data"]["queue"]
    if before["devices"]["data"].get("majorMinor") != manifest["dataMajorMinor"] or qbefore.get("nrRequests") != 316 or qbefore.get("scsiDeviceQueueDepth") != 316 or "[none]" not in qbefore.get("scheduler", ""):
        raise RuntimeError(f"standby DATA defaults/identity are not restored before treatment: {qbefore}")
    live_fixture = cooldown.read_fixture(base, ns, replica["metadata"]["name"], token, manifest)
    for role, pod in pods.items():
        m = test.sql(base, ns, pod["metadata"]["name"], "repro", test.GUC_SQL, retryable=True)
        if m.get("appSessions") != 0 or m.get("recovery") != (role == "replica"):
            raise RuntimeError(f"{role} role/client-session preflight failed: {m}")
        settings = {k: v.get("value") for k, v in m.get("settings", {}).items()}
        if any(settings.get(k) != v for k, v in {"wal_level":"logical", "full_page_writes":"on", "recovery_prefetch":"try", "vacuum_cost_delay":"0"}.items()):
            raise RuntimeError(f"{role} default GUC preflight failed: {settings}")
    no_reader = json.loads(test.cmd(base + ["get", "pods", "-n", ns, "-l",
        f"repro.stack.token={token},repro.experiment=read-pressure", "-o", "json"], retryable=True))["items"]
    if no_reader: raise RuntimeError(f"token-owned pressure pods already exist: {[p['metadata']['name'] for p in no_reader]}")
    topo_path = control / "topology-preflight.json"
    topo, replica_node, all_pods = topology_preflight(base, test, ns, cluster, replica["spec"]["nodeName"], primary["spec"]["nodeName"], topo_path)
    headroom = node_headroom(replica_node, all_pods)
    save(control / "reader-node-headroom.json", headroom)
    csi_candidates = [p for p in json.loads(test.cmd(base + ["get", "pods", "-n", "kube-system", "-o", "json"], retryable=True))["items"]
        if p.get("spec", {}).get("nodeName") == replica["spec"]["nodeName"] and
        any(c.get("name") == "azuredisk" and c.get("securityContext", {}).get("privileged") for c in p.get("spec", {}).get("containers", []))]
    if len(csi_candidates) != 1: raise RuntimeError(f"exactly one privileged Azure Disk CSI pod required on standby worker; found {len(csi_candidates)}")
    csi_name = csi_candidates[0]["metadata"]["name"]
    csi_base = base + ["exec", "-i", "-n", "kube-system", csi_name, "-c", "azuredisk", "--", "sh", "-s"]
    marker = {"ownerToken": token, "createdAtHost": stamp(), "region": infra.get("location"),
        "namespace": ns, "cluster": cluster, "replicaPod": replica["metadata"]["name"],
        "replicaPodUid": replica["metadata"]["uid"], "replicaNode": replica["spec"]["nodeName"],
        "dataClaim": data["claim"], "dataPv": data["pv"], "azureDiskId": data["volumeHandle"],
        "dataLun": str(data["lun"]), "dataMajorMinor": before["devices"]["data"]["majorMinor"],
        "queueBefore": qbefore, "csiPod": csi_name, "selectedMethod": method,
        "fixtureManifest": str(manifest_path), "cooldownProof": proof,
        "fixtureInventory": live_fixture["files"], "topology": str(topo_path),
        "nodeRequestHeadroom": headroom, "output": str(outarg), "controllerOutput": str(control),
        "sourceSha256": {rel: sha(HERE / rel) for rel in PINS},
        "controllerSha256": sha(Path(__file__).resolve())}
    save(control / "launch-manifest.json", marker)

    # Capture immutable fixture/plan evidence in the run directory before queue mutation.
    save(control / "auxiliary-target-preflight.json", {"manifest": manifest, "proof": proof,
        "liveRemoteInventory": live_fixture["files"], "mapping": data, "layout": layout,
        "queueBefore": qbefore, "topology": topo, "headroom": headroom})
    treatment_state = {"active": False}
    install_runtime_hooks(test, rp, startup, cooldown, live_fixture, infra, prep, method, manifest,
                          {"out": outarg, "controller": control, "treatment_state": treatment_state,
                           "fixture_manifest": manifest_path,
                           "cooldown_proof": STATE / "auxiliary-random-cooldown-final-proof.json",
                           "headroom": headroom})

    # Hold a stack-private lock and verify the core lock is free, then let 03-test acquire its own lock.
    lock = (STATE / "final-full-controller.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    core_lock = (STATE / "test.lock").open("a")
    try:
        fcntl.flock(core_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(core_lock, fcntl.LOCK_UN)
    except BlockingIOError as exc:
        raise RuntimeError("another stack measurement currently holds test.lock") from exc
    finally:
        core_lock.close()

    treatment_attempted = False
    follower = None
    failure = None
    cleanup = {"queueRestore": [], "readers": None, "passed": False}
    try:
        treatment_attempted = True
        applied = run(csi_base, input=(STATE / "apply_async512_mq_deadline_1024_data_sdc8_32.sh").read_text(), timeout=90)
        output_record(control / "queue-apply.json", applied)
        if applied.returncode != 0:
            raise RuntimeError(f"selected standby DATA queue treatment command failed: {applied.stderr[-1200:]} {applied.stdout[-1200:]}")
        try:
            applied_readback = validate_queue_apply_output(applied.stdout)
        except Exception as exc:
            raise RuntimeError(f"selected standby DATA queue treatment readback failed: {exc}; {applied.stdout[-1200:]}") from exc
        save(control / "selected-data-queue-treatment.json", {"apply": json.loads((control / "queue-apply.json").read_text()),
            "readback": applied_readback, "settings": EXPECTED_METHOD,
            "target": {k: marker[k] for k in ("dataClaim", "dataPv", "azureDiskId", "replicaNode", "dataLun", "dataMajorMinor")}})
        treatment_state["active"] = True
        rp.configure_retry_log(outarg / "read-only-retries.jsonl")
        test.RETRY_LOG = outarg / "read-only-retries.jsonl"
        cooldown.rp.configure_retry_log(outarg / "read-only-retries.jsonl")
        follower_log = control / "replication-follower.log"
        follower_file = follower_log.open("wb")
        follower = subprocess.Popen([str(VENV_PYTHON), str(FOLLOWER), str(HERE)], cwd=ROOT,
            stdin=subprocess.DEVNULL, stdout=follower_file, stderr=subprocess.STDOUT,
            start_new_session=True, close_fds=True)
        follower_file.close()
        save(control / "replication-follower.json", {"pid": follower.pid, "startedAtHost": stamp(),
            "helper": str(FOLLOWER), "log": str(follower_log)})
        time.sleep(2)
        if follower.poll() is not None:
            raise RuntimeError(f"replication follower exited before test start: {follower.returncode}")
        argv0 = sys.argv[:]
        try:
            sys.argv = [str(HERE / "03-test.py")] + argv
            test_code = test.main()
        finally:
            sys.argv = argv0
        save(control / "core-test-process.json", {"returnCode": test_code, "finishedAtHost": stamp(),
            "output": str(outarg)})
        if test_code != 0: failure = f"03-test returned {test_code}"
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
        save(control / "controller-error.json", {"time": stamp(), "error": failure,
            "traceback": traceback.format_exc()})
    finally:
        try:
            selector_result = selector_cleanup(base, ns, token, control, run)
        except Exception as exc:
            selector_result = {"passed": False, "error": f"{type(exc).__name__}: {exc}",
                               "readerPodsAbsent": False, "readerConfigMapsAbsent": False, "steps": []}
            failure = failure or f"owned reader cleanup raised {type(exc).__name__}: {exc}"
        cleanup["readers"] = selector_result
        if not selector_result["passed"]:
            failure = failure or "owned pressure pod/configmap cleanup did not verify empty"
        if treatment_attempted:
            initial_csi = csi_name
            restore_csi = csi_name
            csi_resolution = {"initialPod": initial_csi, "capturedAtHost": stamp(), "resolvedPod": None}
            try:
                current_items = json.loads(test.cmd(base + ["get", "pods", "-n", "kube-system", "-o", "json"], retryable=True))["items"]
                current_csi = [p for p in current_items if p.get("spec", {}).get("nodeName") == replica["spec"]["nodeName"] and
                    p.get("status", {}).get("phase") == "Running" and
                    any(c.get("name") == "azuredisk" and c.get("securityContext", {}).get("privileged")
                        for c in p.get("spec", {}).get("containers", []))]
                if len(current_csi) != 1:
                    raise RuntimeError(f"expected one live privileged azuredisk pod on original DATA node; found {len(current_csi)}")
                restore_csi = current_csi[0]["metadata"]["name"]
                csi_resolution.update({"resolvedPod": restore_csi, "resolvedPodUid": current_csi[0]["metadata"].get("uid"),
                    "node": current_csi[0]["spec"].get("nodeName"), "ready": any(x.get("type") == "Ready" and x.get("status") == "True"
                        for x in current_csi[0].get("status", {}).get("conditions", []))})
                if not csi_resolution["ready"]:
                    raise RuntimeError("live azuredisk pod on standby DATA node is not Ready")
            except Exception as exc:
                failure = failure or f"could not re-resolve a Ready DATA-node Azure Disk CSI pod for rollback: {exc}"
                csi_resolution["resolutionError"] = f"{type(exc).__name__}: {exc}"
            restore_csi_base = base + ["exec", "-i", "-n", "kube-system", restore_csi, "-c", "azuredisk", "--", "sh", "-s"]
            cleanup["csiRollbackTarget"] = csi_resolution
            save(control / "csi-rollback-target.json", csi_resolution)
            def restore_one(name, script):
                nonlocal failure
                try:
                    p = run(restore_csi_base, input=(STATE / script).read_text(), timeout=90)
                    output_record(control / (name + ".json"), p)
                    cleanup["queueRestore"].append({"name": name, "returnCode": p.returncode,
                        "stdout": p.stdout, "stderr": p.stderr})
                    if p.returncode != 0: failure = failure or f"{name} failed ({p.returncode})"
                    return p
                except Exception as exc:
                    failure = failure or f"{name} raised {type(exc).__name__}: {exc}"
                    cleanup["queueRestore"].append({"name": name, "error": str(exc)})
                    return None
            # The second step is attempted even if async-depth restoration fails.
            under_mq = restore_one("restore-async-256-under-mq", "restore_async_depth_256_data_sdc8_32.sh")
            defaults = restore_one("restore-data-queue-defaults", "restore_data_queue_defaults_sdc8_32.sh")
            final_script = r'''set -eu
dev=$(cat /sys/class/block/sdc/dev); sys=$(readlink -f /sys/class/block/sdc/device)
[ "$dev" = 8:32 ]; case "$sys" in */1:0:0:0) ;; *) exit 51 ;; esac
[ "$(cat /sys/class/block/sdc/device/queue_depth)" = 316 ]
[ "$(cat /sys/class/block/sdc/queue/nr_requests)" = 316 ]
case "$(cat /sys/class/block/sdc/queue/scheduler)" in '[none]'*) ;; *) exit 52 ;; esac
printf 'dev=%s|sys=%s|nr_requests=%s|depth=%s|scheduler=%s\n' "$dev" "$sys" "$(cat /sys/class/block/sdc/queue/nr_requests)" "$(cat /sys/class/block/sdc/device/queue_depth)" "$(cat /sys/class/block/sdc/queue/scheduler)"
'''
            try:
                rb = run(base + ["exec", "-n", "kube-system", restore_csi, "-c", "azuredisk", "--", "sh", "-c", final_script], timeout=30)
                output_record(control / "queue-final-readback.json", rb)
            except Exception as exc:
                rb = None; failure = failure or f"final queue readback raised {type(exc).__name__}: {exc}"
            final_ok = bool(rb and rb.returncode == 0 and "nr_requests=316" in rb.stdout and "[none]" in rb.stdout)
            cleanup["queueFinalReadback"] = {"returnCode": rb.returncode if rb else None,
                "stdout": rb.stdout if rb else "", "stderr": rb.stderr if rb else "", "passed": final_ok}
            if not final_ok: failure = failure or "standby DATA queue did not restore to none/316"
            if under_mq is None or under_mq.returncode != 0:
                # If treatment failed before scheduler activation, note failed intermediate readback,
                # but a verified final default still provides safe cleanup.
                cleanup["async256UnderMqVerified"] = False
            else:
                cleanup["async256UnderMqVerified"] = True
            cleanup["queueRestored"] = final_ok
        cleanup["passed"] = bool(selector_result["passed"] and cleanup.get("queueRestored", False) and
                                  cleanup.get("async256UnderMqVerified", False))
        save(control / "cleanup-summary.json", {**cleanup, "completedAtHost": stamp(), "failure": failure})
        state_path = STATE / "test-state.json"
        current_state = load(state_path) if state_path.exists() else {}
        if follower is not None and current_state.get("phase") not in ("complete", "failed"):
            save(state_path, {"phase": "failed", "updatedAt": stamp(), "error": failure or "controller exited before terminal test state",
                              "output": str(outarg), "runId": outarg.name})
        if follower is not None:
            try:
                follower.wait(timeout=300)
            except subprocess.TimeoutExpired:
                failure = failure or "replication follower did not flush its terminal monitor summary within 300s"
        monitor = outarg / "monitoring/monitor-summary.json"
        cleanup["monitorSummary"] = str(monitor)
        cleanup["monitorFinished"] = monitor.is_file()
        if not monitor.is_file(): failure = failure or "continuous replication follower did not produce monitor-summary.json"

    # Archive the live-core summary before fixing two report-only metadata aggregations.
    if (outarg / "summary.json").is_file() and (outarg / "timing.json").is_file():
        try:
            if (control / "selected-data-queue-treatment.json").is_file():
                shutil.copy2(control / "selected-data-queue-treatment.json", outarg / "selected-data-queue-treatment.json")
            if (control / "auxiliary-target-preflight.json").is_file():
                shutil.copy2(control / "auxiliary-target-preflight.json", outarg / "auxiliary-target-preflight.json")
            if (control / "preflight-read-only-retries.jsonl").is_file():
                shutil.copy2(control / "preflight-read-only-retries.jsonl", outarg / "preflight-read-only-retries.jsonl")
            queue_source = control / "replica-data-queue-sysfs.jsonl"
            active_queue_rows = [json.loads(x) for x in queue_source.read_text().splitlines() if x.strip() and json.loads(x).get("treatmentActive")] if queue_source.is_file() else []
            queue_out = outarg / "replica-data-queue-sysfs.jsonl"
            queue_out.write_text("".join(json.dumps(x, sort_keys=True) + "\n" for x in active_queue_rows))
            queue_rows = [json.loads(x) for x in queue_out.read_text().splitlines() if x.strip()]
            replica_rows = [x for x in queue_rows if x.get("role") == "replica"]
            raw_samples_for_proof = [json.loads(x) for x in (outarg / "samples.jsonl").read_text().splitlines() if x.strip()]
            needed_sample_times = {x["replica"]["ts"] for x in raw_samples_for_proof}
            recorded_sample_times = {x.get("sampleAt") for x in replica_rows}
            missing_queue_times = sorted(needed_sample_times - recorded_sample_times)
            if missing_queue_times:
                failure = failure or f"per-sample replica DATA queue proof misses raw sample times: {missing_queue_times[:5]}"
            backup = outarg / "summary-core-before-aux-report-fix.json"
            if not backup.exists(): shutil.copy2(outarg / "summary.json", backup)
            patch_reports(outarg, replica_rows, manifest, method, proof, topo_path, headroom)
            report_path = generate_report(outarg, method, manifest, proof, control, infra)
            if not report_path.is_file():
                failure = failure or "final report/linear graphs were not produced"
        except Exception as exc:
            failure = failure or f"report augmentation failed: {type(exc).__name__}: {exc}"
    # Final result gate is intentionally stricter than the underlying testComplete bit.
    gate = {"output": str(outarg), "controllerOutput": str(control), "ownerToken": token,
        "cleanupPassed": cleanup.get("passed", False), "monitorSummary": str(outarg / "monitoring/monitor-summary.json"),
        "createdAtHost": marker["createdAtHost"], "completedAtHost": stamp(), "failure": failure}
    try:
        timing = load(outarg / "timing.json")
        summary = load(outarg / "summary.json")
        pressure = load(outarg / "read-pressure.json")
        start = load(outarg / "read-pressure-start.json")
        sample_rows = [json.loads(x) for x in (outarg / "samples.jsonl").read_text().splitlines() if x.strip()]
        test_state = load(STATE / "test-state.json")
        report_path = outarg / "REPORT.md"
        report_exists = (all((outarg / name).is_file() for name in ("REPORT.md", "await.png", "iops.png", "throughput.png")) and
                         "All y-axes are linear." in report_path.read_text())
        gate.update(evaluate_full_acceptance(timing, summary, pressure, start, sample_rows, test_state,
                                             cleanup, failure, report_exists))
        if not gate["successful"]:
            gate["failure"] = gate.get("failure") or "strict full-run pressure/catch-up/coverage/cleanup/report acceptance gate failed"
            timing["testComplete"] = False
            summary.setdefault("status", {})["testComplete"] = False
            summary["status"]["fullReadPressureAcceptancePassed"] = False
            save(outarg / "timing.json", timing); save(outarg / "summary.json", summary)
            save(STATE / "test-state.json", {"phase": "failed", "updatedAt": stamp(), "error": gate["failure"],
                "output": str(outarg), "runId": outarg.name, "replayCaughtUp": timing.get("replayCaughtUp"), "testComplete": False})
        else:
            summary.setdefault("status", {})["fullReadPressureAcceptancePassed"] = True
            save(outarg / "summary.json", summary)
    except Exception as exc:
        gate["successful"] = False
        gate["failure"] = gate.get("failure") or f"strict result gate raised {type(exc).__name__}: {exc}"
        try:
            save(STATE / "test-state.json", {"phase": "failed", "updatedAt": stamp(), "error": gate["failure"],
                "output": str(outarg), "runId": outarg.name, "testComplete": False})
        except Exception: pass
    save(outarg / "full-read-pressure-acceptance.json", gate)
    save(control / "controller-summary.json", {**marker, "completedAtHost": stamp(),
         "coreTestState": load(STATE / "test-state.json"), "cleanup": cleanup, "strictGate": gate})
    save(STATE / "final-full-controller-summary.json", {**marker, "completedAtHost": stamp(),
         "output": str(outarg), "controllerOutput": str(control), "cleanup": cleanup, "strictGate": gate})
    print(json.dumps(gate, indent=2), flush=True)
    lock.close()
    return 0 if gate.get("successful") else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(2)
