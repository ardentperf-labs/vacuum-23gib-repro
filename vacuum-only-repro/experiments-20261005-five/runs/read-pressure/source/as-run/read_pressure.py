#!/usr/bin/env python3
"""Run cache-bypassing, read-only worker processes against the replica DATA disk."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
STATE = HERE / ".state"
sys.path.insert(0, str(HERE))
spec = importlib.util.spec_from_file_location("vacuum_test", HERE / "03-test.py")
test = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(test)

IMAGE = "ghcr.io/cloudnative-pg/postgresql:16.15-202609281634-standard-trixie@sha256:1dd42563007500b94b943f2a1620f724efedfc9eea2d07a50c6db4582c2431fd"
DEFAULT_BLOCK_BYTES = 8192
IO_DEPTH = 32
MAX_BLOCK_BYTES = 4 * 1024 * 1024
READER_CPU_REQUEST = "25m"
READER_CPU_LIMIT = "500m"
RETRY_LOG = None


def configure_retry_log(path):
    """Share one audit log across this helper and its separately loaded test module."""
    global RETRY_LOG
    RETRY_LOG = Path(path)
    test.RETRY_LOG = RETRY_LOG


def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def save(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def run(argv, timeout=60, input=None, retryable=False):
    delays = (0.4, 0.8, 1.6, 3.2, 5.0)
    for attempt in range(len(delays) + 1):
        p = subprocess.run(argv, input=input, text=True, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE, timeout=timeout)
        if p.returncode == 0:
            return p.stdout
        error = (p.stderr or p.stdout)[-1800:]
        if not retryable or attempt >= len(delays) or not test._kubectl_transport_failure(error):
            raise RuntimeError(f"{' '.join(map(str, argv))} failed ({p.returncode}): {error}")
        delay = delays[attempt]
        if RETRY_LOG is not None:
            with RETRY_LOG.open("a") as f:
                f.write(json.dumps({"time": stamp(), "operation": "read_only_kubectl",
                                    "attempt": attempt + 1, "nextDelaySeconds": delay,
                                    "error": error, "command": list(map(str, argv[:4]))}, sort_keys=True) + "\n")
        time.sleep(delay)
    raise AssertionError("unreachable")


def relation_plan(base, ns, db, pod, layout, block_bytes=DEFAULT_BLOCK_BYTES):
    """Resolve the immutable fixture relation and read-only replica DATA claim."""
    data = layout["roles"]["replica"]["data"]
    info = test.sql(base, ns, pod["metadata"]["name"], db,
                    "SELECT json_build_object('dataDirectory',current_setting('data_directory'),"
                    "'relationPath',pg_relation_filepath('public.vacuum_target'),"
                    "'relationBytes',pg_relation_size('public.vacuum_target'))::text;", retryable=True)
    rel = info.get("relationPath")
    datadir = info.get("dataDirectory")
    mount = data["path"].rstrip("/")
    if not isinstance(rel, str) or rel.startswith("/") or ".." in Path(rel).parts:
        raise RuntimeError(f"unexpected PostgreSQL relation path: {rel!r}")
    if not isinstance(datadir, str) or not datadir.startswith(mount + "/"):
        raise RuntimeError(f"PGDATA {datadir!r} is outside the DATA claim mount {mount!r}")
    pgdata_below_mount = datadir[len(mount) + 1:]
    target = str(Path("/mnt/data") / pgdata_below_mount / rel)
    uid = run(base + ["exec", "-c", "postgres", "-n", ns, pod["metadata"]["name"], "--",
                      "sh", "-c", "id -u postgres; id -g postgres"], 20, retryable=True).splitlines()
    if len(uid) < 2 or not all(s.isdigit() for s in uid[:2]):
        raise RuntimeError(f"cannot inspect PostgreSQL file owner: {uid!r}")
    run(base + ["exec", "-c", "postgres", "-n", ns, pod["metadata"]["name"], "--",
                "sh", "-c", 'test -r "$1"', "sh", datadir + "/" + rel], 20, retryable=True)
    segment_output = run(base + ["exec", "-c", "postgres", "-n", ns, pod["metadata"]["name"], "--",
        "sh", "-c", 'd=${1%/*}; b=${1##*/}; find "$d" -maxdepth 1 -type f \\( -name "$b" -o -name "$b.[0-9]*" \\) -printf "%f|%s\\n" | sort -V',
        "sh", datadir + "/" + rel], 30, retryable=True)
    relation_segments = []
    basename = Path(rel).name
    for line in segment_output.splitlines():
        name, size = line.rsplit("|", 1)
        if not re.fullmatch(re.escape(basename) + r"(?:\.\d+)?", name):
            continue
        relation_segments.append({"name": name, "bytes": int(size)})
    relation_segments.sort(key=lambda x: (0, -1) if x["name"] == basename else (1, int(x["name"].rsplit(".", 1)[1])))
    segment_bytes = sum(x["bytes"] for x in relation_segments)
    relation_size = int(info.get("relationBytes") or 0)
    if not relation_segments or segment_bytes != relation_size:
        raise RuntimeError(f"relation segment listing does not match pg_relation_size: {segment_bytes} != {relation_size}")
    block_coverage = {}
    for size in (8192, 131072, 1048576, 4194304):
        eligible = [(x["name"], x["bytes"] // size * size) for x in relation_segments if x["bytes"] // size]
        eligible_bytes = sum(n for _, n in eligible)
        block_coverage[str(size)] = {"requestedBlockBytes": size,
            "totalSegmentBytes": segment_bytes,"eligibleSegmentCount": len(eligible),
            "eligibleBytes": eligible_bytes,"excludedBytes": segment_bytes - eligible_bytes,
            "excludedSegments":[{"name":x["name"],"bytes":x["bytes"]}
                                 for x in relation_segments if x["bytes"] < size],
            "partialTailBytesWithinEligibleSegments": sum(x["bytes"] % size for x in relation_segments if x["bytes"] >= size)}
    return {
        "namespace": ns,
        "replicaPod": pod["metadata"]["name"],
        "replicaPodUid": pod["metadata"]["uid"],
        "replicaNode": pod["spec"]["nodeName"],
        "dataClaim": data["claim"],
        "dataPv": data["pv"],
        "dataMountPath": mount,
        "relationPath": rel,
        "relationFilePathInReplica": datadir + "/" + rel,
        "readerPath": target,
        "postgresUid": int(uid[0]),
        "postgresGid": int(uid[1]),
        "directIoBytes": block_bytes,
        "relationSegments": relation_segments,
        "blockSizeCoverage": block_coverage,
        "readOnlyMount": True,
        "directIo": True,
        "sqlClientCreated": False,
        "verifiedReadableInReplica": True,
    }


class ReaderPressure:
    def __init__(self, base, ns, token, plan, outdir: Path, depth: int = IO_DEPTH,
                 block_bytes: int = DEFAULT_BLOCK_BYTES):
        global RETRY_LOG
        self.base = base
        self.ns = ns
        self.token = token
        self.short = token[:8]
        self.plan = plan
        self.outdir = outdir
        self.depth = depth
        self.block_bytes = block_bytes
        self.cm = f"read-bin-{self.short}"
        self.pod = None
        self.follower = None
        self.log_handle = None
        self.last_health_log_scan = 0.0
        self.unexpected_worker_exits = []
        self.binary = STATE / "read-pressure"
        self.cm_created = False
        self.build_binary()
        self.ensure_configmap()

    def build_binary(self):
        STATE.mkdir(mode=0o700, exist_ok=True)
        self.binary.parent.chmod(0o700)
        p = subprocess.run(["gcc", "-std=c11", "-O2", "-Wall", "-Wextra", "-Werror",
                            "-o", str(self.binary), str(HERE / "read-pressure.c")],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if p.returncode:
            raise RuntimeError(f"cannot build read-pressure worker: {p.stderr[-1200:]}")
        self.binary.chmod(0o700)

    def ensure_configmap(self):
        existing = run(self.base + ["get", "configmap", self.cm, "-n", self.ns,
                                    "-o", "json"], 30, retryable=True) if self.resource_exists("configmap", self.cm) else ""
        if existing:
            obj = json.loads(existing)
            if obj.get("metadata", {}).get("labels", {}).get("repro.stack.token") != self.token:
                raise RuntimeError("read-pressure ConfigMap exists without matching stack ownership")
            return
        raw = self.binary.read_bytes()
        obj = {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": self.cm, "namespace": self.ns,
                         "labels": {"repro.stack.token": self.token,
                                    "repro.experiment": "read-pressure"}},
            "binaryData": {"read-pressure": __import__("base64").b64encode(raw).decode("ascii")},
        }
        run(self.base + ["apply", "-f", "-"], 60, json.dumps(obj))
        self.cm_created = True

    def resource_exists(self, kind, name):
        try:
            run(self.base + ["get", kind, name, "-n", self.ns, "-o", "json"], 30, retryable=True)
            return True
        except RuntimeError as e:
            if "NotFound" in str(e) or "not found" in str(e):
                return False
            raise RuntimeError(f"cannot prove {kind}/{name} absent: {e}") from e

    def start(self, workers: int, tag: str):
        if workers < 1 or workers > 128:
            raise ValueError("reader process count must be 1..128")
        if self.block_bytes < 4096 or self.block_bytes > MAX_BLOCK_BYTES or self.block_bytes % 4096:
            raise ValueError("read size must be 4 KiB..4 MiB and a multiple of 4 KiB")
        memory_estimate = workers * self.depth * self.block_bytes + workers * 2 * 1024**2 + 128 * 1024**2
        if memory_estimate > 900 * 1024**2:
            raise ValueError(f"reader buffers/process overhead estimate exceeds 900 MiB: {memory_estimate}")
        memory_request_mib = math.ceil(memory_estimate / (128 * 1024**2)) * 128
        pod_name = f"read-pressure-{self.short}-{tag[:8]}-{uuid.uuid4().hex[:4]}"[:63]
        if self.resource_exists("pod", pod_name):
            raise RuntimeError(f"reader pod {pod_name} already exists")
        pod = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": pod_name, "namespace": self.ns,
                         "labels": {"repro.stack.token": self.token,
                                    "repro.experiment": "read-pressure",
                                    "repro.reader.count": str(workers)}},
            "spec": {
                "nodeName": self.plan["replicaNode"],
                "restartPolicy": "Never",
                "terminationGracePeriodSeconds": 30,
                "automountServiceAccountToken": False,
                "enableServiceLinks": False,
                "securityContext": {"runAsNonRoot": True, "runAsUser": self.plan["postgresUid"],
                                    "runAsGroup": self.plan["postgresGid"],
                                    "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{
                    "name": "readers", "image": IMAGE, "imagePullPolicy": "IfNotPresent",
                    "command": ["/opt/reader/read-pressure"],
                    "args": [self.plan["readerPath"], str(workers), str(self.block_bytes), str(self.depth)],
                    "resources": {"requests": {"cpu": READER_CPU_REQUEST, "memory": f"{memory_request_mib}Mi"},
                                  "limits": {"cpu": READER_CPU_LIMIT, "memory": "1Gi"}},
                    "securityContext": {"allowPrivilegeEscalation": False,
                                        "readOnlyRootFilesystem": True,
                                        "capabilities": {"drop": ["ALL"]}},
                    "volumeMounts": [
                        {"name": "data", "mountPath": "/mnt/data", "readOnly": True},
                        {"name": "binary", "mountPath": "/opt/reader", "readOnly": True},
                    ],
                }],
                "volumes": [
                    {"name": "data", "persistentVolumeClaim": {"claimName": self.plan["dataClaim"], "readOnly": True}},
                    {"name": "binary", "configMap": {"name": self.cm, "defaultMode": 365}},
                ],
            },
        }
        run(self.base + ["apply", "-f", "-"], 60, json.dumps(pod))
        self.pod = pod_name
        self.start_log_capture()
        until = time.monotonic() + 900
        last_phase = None
        while time.monotonic() < until:
            item = json.loads(run(self.base + ["get", "pod", pod_name, "-n", self.ns, "-o", "json"], 30, retryable=True))
            phase = item.get("status", {}).get("phase")
            if phase != last_phase:
                last_phase = phase
            if phase in ("Failed", "Succeeded"):
                raise RuntimeError(f"read-pressure pod terminated before start: {phase}")
            logs = self.logs_snapshot()
            if ("io_setup failed" in logs or "direct read failed" in logs or
                    "cannot open relation segments" in logs or "relation segment is smaller" in logs):
                raise RuntimeError(f"read-pressure native AIO failed: {logs[-1200:]}")
            started_message = None
            ready_workers = 0
            exited_workers = []
            for line in logs.splitlines():
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if message.get("event") == "started":
                    started_message = message
                elif message.get("event") == "worker_ready":
                    ready_workers += 1
                elif message.get("event") == "worker_exit":
                    exited_workers.append(message)
            if exited_workers:
                raise RuntimeError(f"reader child exited during startup: {exited_workers}")
            if started_message is not None:
                if (started_message.get("startedWorkers") != workers or
                    started_message.get("ioDepthPerProcess") != self.depth or
                    started_message.get("blockBytes") != self.block_bytes or
                    not started_message.get("directIo") or not started_message.get("readOnly")):
                    raise RuntimeError(f"read-pressure workers did not start as requested: {started_message}")
                if ready_workers == workers:
                    started_message["readyWorkers"] = ready_workers
                    self.started = started_message
                    return started_message
            time.sleep(2)
        raise RuntimeError(f"timed out waiting for {workers} direct readers; last phase={last_phase}")

    def start_log_capture(self):
        self.outdir.mkdir(parents=True, exist_ok=True)
        path = self.outdir / f"{self.pod}.jsonl"
        self.log_handle = path.open("a")
        self.follower = subprocess.Popen(
            self.base + ["logs", "-f", self.pod, "-n", self.ns, "-c", "readers"],
            stdout=self.log_handle, stderr=subprocess.STDOUT, text=True,
            start_new_session=True, close_fds=True)

    def logs_snapshot(self):
        if not self.pod:
            return ""
        return run(self.base + ["logs", self.pod, "-n", self.ns, "-c", "readers"], 30, retryable=True)

    def pod_status(self):
        if not self.pod:
            return None
        item = json.loads(run(self.base + ["get", "pod", self.pod, "-n", self.ns, "-o", "json"], 30, retryable=True))
        state = item.get("status", {}).get("containerStatuses", [{}])[0].get("state") or {}
        now = time.monotonic()
        if now - self.last_health_log_scan >= 30:
            logs = self.logs_snapshot()
            exits = []
            for line in logs.splitlines():
                try:
                    message = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if message.get("event") == "worker_exit":
                    exits.append(message)
            self.unexpected_worker_exits = exits
            self.last_health_log_scan = now
        return {"phase": item.get("status", {}).get("phase"),
                "containerState": state,
                "podUid": item.get("metadata", {}).get("uid"),
                "unexpectedWorkerExits": self.unexpected_worker_exits,
                "healthy": (item.get("status", {}).get("phase") == "Running" and
                            "running" in state and not self.unexpected_worker_exits)}

    def stop(self):
        if not self.pod:
            return {"pod": None, "stopped": True, "log": ""}
        pod_name = self.pod
        p = subprocess.run(self.base + ["delete", "pod", pod_name, "-n", self.ns,
                                       "--wait=false", "--grace-period=30"],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
        if p.returncode and "NotFound" not in p.stderr and "not found" not in p.stderr:
            raise RuntimeError(f"cannot stop read-pressure pod: {p.stderr[-1200:]}")
        until = time.monotonic() + 120
        gone = False
        while time.monotonic() < until:
            if not self.resource_exists("pod", pod_name):
                gone = True
                break
            time.sleep(2)
        if self.follower:
            try:
                self.follower.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(self.follower.pid, signal.SIGTERM)
                self.follower.wait(timeout=10)
            self.follower = None
        if self.log_handle:
            self.log_handle.close()
            self.log_handle = None
        path = self.outdir / f"{pod_name}.jsonl"
        lines = path.read_text().splitlines() if path.exists() else []
        workers = [json.loads(x) for x in lines if x.startswith("{")]
        exits = [x for x in workers if x.get("event") == "worker_exit"]
        read_ops = sum(x.get("readOps", 0) for x in exits)
        totals = {"workerProcessesReported": len(exits),
                  "readOps": read_ops,
                  "readBytes": sum(x.get("readBytes", 0) for x in exits),
                  "readErrors": sum(x.get("errors", 0) for x in exits),
                  "maxReadLatencyMs": max((x.get("maxReadLatencyMs", 0) for x in exits), default=0),
                  "operationWeightedMeanReadLatencyMs": (
                      sum(x.get("readOps", 0) * x.get("meanReadLatencyMs", 0) for x in exits) / read_ops
                      if read_ops else None)}
        self.pod = None
        return {"pod": pod_name, "stopped": gone, "workerSummary": totals,
                "logPath": str(path), "log": "\n".join(lines), "exitEvents": exits}

    def remove_configmap(self):
        if self.resource_exists("configmap", self.cm):
            obj = json.loads(run(self.base + ["get", "configmap", self.cm, "-n", self.ns, "-o", "json"], 30, retryable=True))
            if obj.get("metadata", {}).get("labels", {}).get("repro.stack.token") != self.token:
                raise RuntimeError("refusing to remove a ConfigMap without matching stack ownership")
            run(self.base + ["delete", "configmap", self.cm, "-n", self.ns, "--wait=true"], 60)


def disk_delta(before, after, seconds, device="data"):
    mapping = before["devices"][device]["majorMinor"]
    a, b = before["disks"][mapping], after["disks"][mapping]
    ops = b["r"] - a["r"]
    sectors = b["rs"] - a["rs"]
    ms = b["rm"] - a["rm"]
    write_ops = b["w"] - a["w"]
    write_sectors = b["ws"] - a["ws"]
    write_ms = b["wm"] - a["wm"]
    if min(ops, sectors, ms, write_ops, write_sectors, write_ms) < 0:
        raise RuntimeError(f"replica {device.upper()} diskstats counter reset during calibration")
    return {"start": before["ts"], "end": after["ts"], "seconds": seconds,
            "readOps": ops, "readIops": ops / seconds if seconds > 0 else None,
            "readSectors": sectors, "readMBps": sectors * 512 / seconds / 1e6 if seconds > 0 else None,
            "readAwaitMs": ms / ops if ops else None,
            "writeOps": write_ops, "writeIops": write_ops / seconds if seconds > 0 else None,
            "writeSectors": write_sectors,
            "writeMBps": write_sectors * 512 / seconds / 1e6 if seconds > 0 else None,
            "writeAwaitMs": write_ms / write_ops if write_ops else None}


def sample_window(base, ns, db, pod, mapping, app, seconds, interval):
    samples = []
    first = test.sample(base, ns, db, app, pod, mapping)
    samples.append(first)
    deadline = time.monotonic() + seconds
    current = first
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        time.sleep(min(interval, max(0, remaining)))
        nxt = test.sample(base, ns, db, app, pod, mapping)
        samples.append(nxt)
        current = nxt
    intervals = [disk_delta(a, b, b["epoch"] - a["epoch"]) for a, b in zip(samples, samples[1:])]
    wal_intervals = [disk_delta(a, b, b["epoch"] - a["epoch"], "wal") for a, b in zip(samples, samples[1:])]
    valid = [x for x in intervals if x["readAwaitMs"] is not None]
    weighted_ops = sum(x["readOps"] for x in valid)
    weighted_ms = sum((x["readAwaitMs"] or 0) * x["readOps"] for x in valid)
    total_ops = sum(x["readOps"] for x in intervals)
    total_sectors = sum(x["readSectors"] for x in intervals)
    seconds_total = sum(x["seconds"] for x in intervals)
    wal_read_ops = sum(x["readOps"] for x in wal_intervals)
    wal_write_ops = sum(x["writeOps"] for x in wal_intervals)
    data_write_ops = sum(x["writeOps"] for x in intervals)
    data_read_bytes = total_sectors * 512
    data_write_bytes = sum(x["writeSectors"] for x in intervals) * 512
    wal_read_bytes = sum(x["readSectors"] for x in wal_intervals) * 512
    wal_write_bytes = sum(x["writeSectors"] for x in wal_intervals) * 512
    combined_remote_bytes = data_read_bytes + data_write_bytes + wal_read_bytes + wal_write_bytes
    return {"start": first["ts"], "end": current["ts"], "intervalCount": len(intervals),
            "intervals": intervals,
            "deviceQueueGeometry": first["devices"]["data"]["queue"],
            "readIops": sum(x["readOps"] for x in intervals) / sum(x["seconds"] for x in intervals),
            "readMBps": sum(x["readSectors"] for x in intervals) * 512 /
                        sum(x["seconds"] for x in intervals) / 1e6,
            "readAwaitMs": weighted_ms / weighted_ops if weighted_ops else None,
            "averageDeviceSectorsPerReadOperation": total_sectors / total_ops if total_ops else None,
            "averageDeviceBytesPerReadOperation": total_sectors * 512 / total_ops if total_ops else None,
            "replicaDataWriteIops": data_write_ops / seconds_total if seconds_total else None,
            "replicaDataWriteMBps": data_write_bytes / seconds_total / 1e6 if seconds_total else None,
            "replicaWalReadIops": wal_read_ops / seconds_total if seconds_total else None,
            "replicaWalReadMBps": wal_read_bytes / seconds_total / 1e6 if seconds_total else None,
            "replicaWalWriteIops": wal_write_ops / seconds_total if seconds_total else None,
            "replicaWalWriteMBps": wal_write_bytes / seconds_total / 1e6 if seconds_total else None,
            "replicaCombinedRemoteReadMBps": (data_read_bytes + wal_read_bytes) / seconds_total / 1e6 if seconds_total else None,
            "replicaCombinedRemoteWriteMBps": (data_write_bytes + wal_write_bytes) / seconds_total / 1e6 if seconds_total else None,
            "replicaCombinedRemoteReadWriteMBps": combined_remote_bytes / seconds_total / 1e6 if seconds_total else None,
            "replicaWalQueueGeometry": first["devices"]["wal"]["queue"],
            "maxIntervalReadAwaitMs": max((x["readAwaitMs"] for x in valid), default=None),
            "intervalsOver2000ms": sum(1 for x in valid if x["readAwaitMs"] >= 2000),
            "samples": len(samples)}


def find_pods(base, ns, cluster):
    items = json.loads(run(base + ["get", "pods", "-n", ns, "-l", "cnpg.io/cluster=" + cluster, "-o", "json"]))["items"]
    pods = {}
    for pod in items:
        role = pod.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceRole")
        if role in ("primary", "replica"):
            pods[role] = pod
    if set(pods) != {"primary", "replica"}:
        raise RuntimeError("expected ready primary and replica before reader calibration")
    return pods


def calibrate(a):
    infra = json.loads((STATE / "state.json").read_text())
    prep = json.loads((STATE / "prepare.json").read_text())
    if infra.get("status") != "ready" or prep.get("status") != "complete":
        raise RuntimeError("stack must be ready and prepared before reader calibration")
    runid = f"calibration-{a.block_bytes}-" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = Path(a.out or HERE / "results" / runid).resolve()
    if outdir.exists() and any(outdir.iterdir()):
        raise RuntimeError(f"calibration output is not empty: {outdir}")
    outdir.mkdir(parents=True, exist_ok=True)
    configure_retry_log(outdir / "read-only-retries.jsonl")
    cfg = Path(a.kubeconfig or infra.get("kubeconfigPath") or STATE / "kubeconfig").resolve()
    if not cfg.is_file():
        raise RuntimeError(f"kubeconfig missing: {cfg}")
    base = [a.kubectl, "--kubeconfig", str(cfg)]
    ns, cluster = prep.get("cluster", "repro/hdd-pg").split("/", 1)
    token = infra.get("ownerToken")
    ns_obj = json.loads(run(base + ["get", "namespace", ns, "-o", "json"]))
    if ns_obj.get("metadata", {}).get("labels", {}).get("repro.stack.token") != token:
        raise RuntimeError("namespace token mismatch")
    pods = find_pods(base, ns, cluster)
    layout = test.storage(base, ns, pods)
    plan = relation_plan(base, ns, a.database, pods["replica"], layout, a.block_bytes)
    save(outdir / "reader-plan.json", plan)
    reader = ReaderPressure(base, ns, token, plan, outdir / "reader-logs", a.depth, a.block_bytes)
    companion_plan = (relation_plan(base, ns, a.database, pods["replica"], layout, DEFAULT_BLOCK_BYTES)
                      if a.companion_8k_workers else None)
    companion = (ReaderPressure(base, ns, token, companion_plan, outdir / "companion-8k-logs",
                                a.companion_8k_depth, DEFAULT_BLOCK_BYTES)
                 if a.companion_8k_workers else None)
    lock = (STATE / "test.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as e:
        reader.remove_configmap()
        raise RuntimeError("measurement or teardown already running") from e
    result = None
    failure = None
    cleanup_results = {}
    arms = []
    run_created = stamp()
    calibration_path = None
    try:
        mapping = layout["roles"]["replica"]
        app = "reader-calibration"
        baseline = sample_window(base, ns, a.database, pods["replica"], mapping,
                                 app, min(a.seconds, 30), a.interval)
        threshold_workers = None
        for workers in a.counts:
            start_message = reader.start(workers, f"cal-{workers}")
            companion_start = (companion.start(a.companion_8k_workers, f"cal8k-{workers}")
                               if companion else None)
            measurement = sample_window(base, ns, a.database, pods["replica"], mapping,
                                        app, a.seconds, a.interval)
            stop = reader.stop()
            companion_stop = companion.stop() if companion else None
            main_health = (stop.get("stopped") and
                           (stop.get("workerSummary") or {}).get("workerProcessesReported") == workers and
                           (stop.get("workerSummary") or {}).get("readErrors") == 0)
            companion_health = (not companion or (companion_stop.get("stopped") and
                               (companion_stop.get("workerSummary") or {}).get("workerProcessesReported") == a.companion_8k_workers and
                               (companion_stop.get("workerSummary") or {}).get("readErrors") == 0))
            workers_healthy = bool(main_health and companion_health)
            arm = {"workersRequested": workers, "workerStart": start_message,
                   "measurement": measurement, "process": stop["workerSummary"],
                   "workerProcessesHealthy": workers_healthy,
                   "readerPod": stop["pod"], "readerLog": stop["logPath"],
                   "companion8kWorkers": a.companion_8k_workers,
                   "companion8kStart": companion_start,
                   "companion8kProcess": (companion_stop or {}).get("workerSummary"),
                   "companion8kPod": (companion_stop or {}).get("pod"),
                   "companion8kLog": (companion_stop or {}).get("logPath")}
            arms.append(arm)
            save(outdir / "reader-calibration-progress.json",
                 {"createdAt": stamp(), "replicaNode": plan["replicaNode"],
                  "target": plan, "baseline": baseline, "arms": arms})
            if (workers_healthy and measurement["readAwaitMs"] is not None and
                    measurement["readAwaitMs"] >= 2000 and
                    sum(x["seconds"] for x in measurement["intervals"]) >= 60):
                threshold_workers = workers
            if threshold_workers:
                break
            time.sleep(a.cooldown)
        arm_read_ops = sum((x.get("process") or {}).get("readOps", 0) +
                           (x.get("companion8kProcess") or {}).get("readOps", 0) for x in arms)
        arm_read_bytes = sum((x.get("process") or {}).get("readBytes", 0) +
                             (x.get("companion8kProcess") or {}).get("readBytes", 0) for x in arms)
        result = {"createdAt": stamp(), "calibrationCompletedAt": None,
                  "region": infra.get("location"),
                  "workerVmSize": a.worker_vm_size, "sourceSnapshotId": prep.get("sourceSnapshotId"),
                  "fixtureTargetHeapGiB": prep.get("fixtureTargetHeapGiB"),
                  "replicaNode": plan["replicaNode"], "readerPlan": plan,
                  "companion8kReaderPlan": companion_plan,
                  "readers": {"program": "read-pressure.c", "processesAreIndependent": True,
                              "method": f"random {a.block_bytes}-byte O_DIRECT native-AIO reads across vacuum_target main-fork segments",
                              "mountReadOnly": True, "writes": False, "sqlSessions": False,
                              "containerImage": IMAGE,
                              "cpuRequest": READER_CPU_REQUEST, "cpuLimit": READER_CPU_LIMIT,
                              "ioDepthPerProcess": reader.depth,
                              "blockBytes": reader.block_bytes,
                              "maximumOutstandingReads": ((threshold_workers * reader.depth) if threshold_workers else 0) +
                                  a.companion_8k_workers * a.companion_8k_depth,
                              "companion8kWorkers": a.companion_8k_workers,
                              "companion8kDepthPerProcess": a.companion_8k_depth if companion else None},
                  "calibrationSecondsPerArm": a.seconds, "sampleIntervalSeconds": a.interval,
                  "thresholdRule": "at least 60 seconds of operation-weighted guest DATA read-await >= 2000 ms, with every requested main and companion worker still reported healthy at stop",
                  "thresholdBlockBytes": a.block_bytes, "thresholdDepthPerProcess": reader.depth,
                  "companion8kWorkers": a.companion_8k_workers,
                  "companion8kDepthPerProcess": a.companion_8k_depth if companion else None,
                  "thresholdWorkers": threshold_workers,
                  "cumulativeReaderReadOps": arm_read_ops,
                  "cumulativeReaderReadBytes": arm_read_bytes,
                  "cumulativeReaderReadGiB": arm_read_bytes / (1024 ** 3),
                  "baseline": baseline, "arms": arms,
                  "fullRunRecommended": threshold_workers is not None}
        calibration_path = outdir / "reader-calibration.json"
        result["artifactPath"] = str(calibration_path)
        return result
    except Exception as exc:
        failure = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        if reader.pod:
            try:
                stopped = reader.stop()
                cleanup_results["mainReader"] = {k: stopped.get(k) for k in ("pod", "stopped", "workerSummary", "logPath")}
            except Exception as e:
                save(outdir / "reader-stop-error.json", {"time": stamp(), "error": str(e), "pod": reader.pod})
        if companion and companion.pod:
            try:
                stopped = companion.stop()
                cleanup_results["companion8kReader"] = {k: stopped.get(k) for k in ("pod", "stopped", "workerSummary", "logPath")}
            except Exception as e:
                save(outdir / "companion-8k-stop-error.json", {"time": stamp(), "error": str(e), "pod": companion.pod})
        try:
            reader.remove_configmap()
        except Exception as e:
            save(outdir / "reader-configmap-cleanup-error.json", {"time": stamp(), "error": str(e)})
        finally:
            lock.close()
        if result is not None:
            result["calibrationCompletedAt"] = stamp()
            save(calibration_path, result)
            history_path = STATE / "read-pressure-calibrations.json"
            history = json.loads(history_path.read_text()) if history_path.exists() else []
            if not isinstance(history, list):
                raise RuntimeError(f"calibration history is not a JSON list: {history_path}")
            history.append({"artifactPath": str(calibration_path),
                            "calibrationCompletedAt": result["calibrationCompletedAt"],
                            "blockBytes": reader.block_bytes, "depthPerProcess": reader.depth,
                            "companion8kWorkers": a.companion_8k_workers,
                            "companion8kDepthPerProcess": a.companion_8k_depth if companion else None,
                            "thresholdWorkers": result["thresholdWorkers"],
                            "cumulativeReaderReadOps": result["cumulativeReaderReadOps"],
                            "cumulativeReaderReadBytes": result["cumulativeReaderReadBytes"]})
            save(history_path, history)
            print(json.dumps(result, indent=2))
        elif failure is not None:
            cleanup_time = stamp()
            arm_ops = sum((x.get("process") or {}).get("readOps", 0) +
                          (x.get("companion8kProcess") or {}).get("readOps", 0) for x in arms)
            arm_bytes = sum((x.get("process") or {}).get("readBytes", 0) +
                            (x.get("companion8kProcess") or {}).get("readBytes", 0) for x in arms)
            cleanup_ops = sum((x.get("workerSummary") or {}).get("readOps", 0)
                              for x in cleanup_results.values())
            cleanup_bytes = sum((x.get("workerSummary") or {}).get("readBytes", 0)
                                for x in cleanup_results.values())
            failure_path = outdir / "reader-calibration-failure.json"
            failed = {"status": "failed", "error": failure, "createdAt": run_created,
                      "calibrationCompletedAt": cleanup_time, "region": infra.get("location"),
                      "workerVmSize": a.worker_vm_size, "blockBytes": reader.block_bytes,
                      "depthPerProcess": reader.depth, "requestedCounts": a.counts,
                      "companion8kWorkers": a.companion_8k_workers,
                      "companion8kDepthPerProcess": a.companion_8k_depth if companion else None,
                      "cpuRequest": READER_CPU_REQUEST, "cpuLimit": READER_CPU_LIMIT,
                      "completedArms": arms, "cleanup": cleanup_results,
                      "cumulativeReaderReadOps": arm_ops + cleanup_ops,
                      "cumulativeReaderReadBytes": arm_bytes + cleanup_bytes,
                      "cumulativeReaderReadGiB": (arm_bytes + cleanup_bytes) / (1024 ** 3),
                      "artifactPath": str(failure_path)}
            save(failure_path, failed)
            history_path = STATE / "read-pressure-calibrations.json"
            history = json.loads(history_path.read_text()) if history_path.exists() else []
            if not isinstance(history, list):
                raise RuntimeError(f"calibration history is not a JSON list: {history_path}")
            history.append({"artifactPath": str(failure_path),
                            "calibrationCompletedAt": cleanup_time, "status": "failed",
                            "blockBytes": reader.block_bytes, "depthPerProcess": reader.depth,
                            "companion8kWorkers": a.companion_8k_workers,
                            "companion8kDepthPerProcess": a.companion_8k_depth if companion else None,
                            "thresholdWorkers": None,
                            "cumulativeReaderReadOps": failed["cumulativeReaderReadOps"],
                            "cumulativeReaderReadBytes": failed["cumulativeReaderReadBytes"]})
            save(history_path, history)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--kubeconfig")
    p.add_argument("--kubectl", default="kubectl")
    p.add_argument("--database", default="repro")
    p.add_argument("--counts", default="4,8,16,32,64")
    p.add_argument("--depth", type=int, default=IO_DEPTH)
    p.add_argument("--block-bytes", type=int, default=DEFAULT_BLOCK_BYTES)
    p.add_argument("--companion-8k-workers", type=int, default=0,
                   help="run this many extra 8-KiB reader processes during each calibration arm")
    p.add_argument("--companion-8k-depth", type=int, default=32)
    p.add_argument("--seconds", type=int, default=60)
    p.add_argument("--interval", type=float, default=5)
    p.add_argument("--cooldown", type=int, default=15)
    p.add_argument("--worker-vm-size", default="Standard_D2s_v4")
    p.add_argument("--out")
    a = p.parse_args()
    try:
        a.counts = [int(x) for x in a.counts.split(",") if x]
    except ValueError:
        p.error("--counts must be comma-separated positive integers")
    if not a.counts or any(x < 1 or x > 128 for x in a.counts):
        p.error("--counts must be between 1 and 128")
    if a.depth < 1 or a.depth > 64:
        p.error("--depth must be between 1 and 64")
    if a.companion_8k_workers < 0 or a.companion_8k_workers > 128:
        p.error("--companion-8k-workers must be between 0 and 128")
    if a.companion_8k_depth < 1 or a.companion_8k_depth > 64:
        p.error("--companion-8k-depth must be between 1 and 64")
    if a.block_bytes < 4096 or a.block_bytes > MAX_BLOCK_BYTES or a.block_bytes % 4096:
        p.error("--block-bytes must be a multiple of 4096 between 4096 and 4194304")
    if a.seconds < 60 or a.interval < 2 or a.cooldown < 0:
        p.error("--seconds must be >=60, --interval >=2 and --cooldown >=0")
    try:
        calibrate(a)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
