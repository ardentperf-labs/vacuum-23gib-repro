#!/usr/bin/env python3
"""Collect replica WAL-receiver, block-latency, disk, and Azure VM metrics.

Start this process after the replica pod is ready and before 03-test.py. The
process performs a mandatory BCC preflight, writes preflight.json with phase
"ready", and then stays alive until interrupted. Do not start VACUUM unless
the preflight file says ready.
"""

from __future__ import annotations

import argparse
import ast
import datetime as dt
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
METRIC_NAME = "VM Uncached IOPS Consumed Percentage"
UTC = dt.timezone.utc

SQL = r"""SELECT json_build_object(
 'sampledAt', clock_timestamp(),
 'replayLsn', pg_last_wal_replay_lsn(),
 'walReceiverWait', coalesce((SELECT json_agg(json_build_object(
   'pid',pid,'backendType',backend_type,'state',state,
   'waitEventType',wait_event_type,'waitEvent',wait_event,
   'backendStart',backend_start,'stateChange',state_change)
   ORDER BY pid)
   FROM pg_stat_activity WHERE backend_type='walreceiver'),'[]'::json),
 'walReceiver', (SELECT json_build_object(
   'pid',pid,'status',status,'receiveStartLsn',receive_start_lsn,
   'receiveStartTli',receive_start_tli,'writtenLsn',written_lsn,
   'flushedLsn',flushed_lsn,'receivedTli',received_tli,
   'latestEndLsn',latest_end_lsn,'latestEndTime',latest_end_time,
   'lastMsgSendTime',last_msg_send_time,
   'lastMsgReceiptTime',last_msg_receipt_time,
   'slotName',slot_name,'senderHost',sender_host,'senderPort',sender_port)
   FROM pg_stat_wal_receiver LIMIT 1)
)::text;"""

SQL_PRETTY = "\\pset format unaligned\n\\pset tuples_only on\n\\pset footer off\n" + SQL + "\n\\watch 1\n"

POD_RESOURCES = {
    "requests": {"cpu": "50m", "memory": "96Mi"},
    "limits": {"cpu": "500m", "memory": "512Mi"},
}


def utc_now() -> dt.datetime:
    return dt.datetime.now(UTC)


def iso(value: dt.datetime | None = None) -> str:
    return (value or utc_now()).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n")
    os.replace(tmp, path)


def append_jsonl(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, sort_keys=True, separators=(",", ":")) + "\n")
        f.flush()


def argv_string(argv: list[str]) -> str:
    return " ".join(shlex.quote(s) for s in argv)


def json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value


def parse_bcc_histogram_line(payload: str) -> tuple[str, dict[str, Any]] | None:
    """Parse a -j -D biolatency line and return its device and histogram.

    BCC's documented JSON output has appeared as Python dict repr in releases,
    including byte strings for the disk bucket. Accept JSON first, then the
    safe Python literal grammar; never evaluate executable Python.
    """
    hit = payload.find("{")
    if hit < 0:
        return None
    candidate = payload[hit:]
    try:
        histogram = json.loads(candidate)
    except json.JSONDecodeError:
        try:
            histogram = ast.literal_eval(candidate)
        except (SyntaxError, ValueError):
            return None
    if not isinstance(histogram, dict):
        return None
    device = None
    for key in ("disk", "Bucket ptr", "bucket", "device"):
        value = histogram.get(key)
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        if isinstance(value, str) and value:
            device = os.path.basename(value.strip())
            break
    if not device:
        return None
    return device, json_safe(histogram)


def parse_psql_json_line(line: str) -> dict[str, Any] | None:
    """Extract a one-line JSON query result from psql's \\watch output."""
    start = line.find("{")
    if start < 0:
        return None
    value = json.loads(line[start:].strip())
    if not isinstance(value, dict):
        raise ValueError("psql row was JSON but not an object")
    required = {"sampledAt", "walReceiverWait", "walReceiver", "replayLsn"}
    if not required.issubset(value):
        raise ValueError(f"psql JSON row is missing expected keys: {sorted(required - value.keys())}")
    return value


def run(argv: list[str], timeout: float = 60, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, input=input_text, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, timeout=timeout, check=False,
    )


def check(argv: list[str], timeout: float = 60, input_text: str | None = None) -> str:
    p = run(argv, timeout, input_text)
    if p.returncode:
        raise RuntimeError(
            f"{argv_string(argv)} exited {p.returncode}: "
            f"{(p.stderr or p.stdout)[-2000:]}"
        )
    return p.stdout


def kubectl_base(args: argparse.Namespace) -> list[str]:
    result = [args.kubectl]
    if args.kubeconfig:
        result += ["--kubeconfig", args.kubeconfig]
    return result


def discover_vmss_identity(kubectl: list[str], node: str, supplied: str | None,
                           supplied_name: str | None) -> tuple[str | None, str | None, str | None, str | None]:
    """Return VMSS-instance ID, VMSS parent ID, VMName dimension, and provider ID."""
    provider_id = None
    if node:
        out = check(kubectl + ["get", "node", node, "-o", "json"])
        provider_id = json.loads(out).get("spec", {}).get("providerID")

    resource_id = supplied or provider_id
    if resource_id and resource_id.lower().startswith("azure://"):
        resource_id = resource_id[len("azure://"):]
    if not resource_id:
        return None, None, supplied_name, provider_id

    match = re.search(
        r"^(?P<parent>.+/providers/Microsoft\.Compute/virtualMachineScaleSets/"
        r"(?P<scale>[^/]+))(?:/virtualMachines/(?P<instance>[^/]+))?$",
        resource_id, flags=re.IGNORECASE,
    )
    if match:
        dimension = supplied_name or (
            f"{match.group('scale')}_{match.group('instance')}"
            if match.group("instance") is not None else None
        )
        instance_id = resource_id if match.group("instance") is not None else None
        return instance_id, match.group("parent"), dimension, provider_id
    return resource_id, None, supplied_name, provider_id


def parse_device_rows(output: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for line in output.splitlines():
        parts = line.split("|", 8)
        if len(parts) != 9 or parts[0] not in ("data", "wal"):
            continue
        kind, major_minor, device, path, controller_type, controller, driver, controller_path, driver_chain = parts
        if kind in result:
            raise RuntimeError(f"duplicate {kind.upper()} block-device mapping")
        result[kind] = {
            "majorMinor": major_minor,
            "device": device,
            "path": path,
            "controller": {"type": controller_type, "name": controller,
                           "driver": driver, "path": controller_path,
                           "driverChain": driver_chain},
        }
    if set(result) != {"data", "wal"}:
        raise RuntimeError(f"could not resolve both DATA/WAL devices: {output[-1000:]}")
    for kind, item in result.items():
        if not re.fullmatch(r"\d+:\d+", item["majorMinor"]):
            raise RuntimeError(f"{kind.upper()} has invalid block major:minor {item['majorMinor']!r}")
        if not item["device"] or not item["path"] or item["controller"]["type"] not in ("nvme", "storvsc"):
            raise RuntimeError(f"{kind.upper()} mapping is not a supported Azure disk controller: {item}")
        if not item["controller"]["driver"] or not item["controller"]["path"]:
            raise RuntimeError(f"{kind.upper()} controller binding is incomplete: {item['controller']}")
    if (result["data"]["majorMinor"] == result["wal"]["majorMinor"]
            or result["data"]["device"] == result["wal"]["device"]
            or result["data"]["path"] == result["wal"]["path"]):
        raise RuntimeError("DATA/WAL do not resolve to distinct mounted block devices")
    return result


def parse_diskstats_rows(output: str) -> set[tuple[str, str, str]]:
    rows = set()
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 3:
            rows.add((fields[0], fields[1], fields[2]))
    return rows


def missing_diskstats_targets(devices: dict[str, dict[str, Any]], rows: set[tuple[str, str, str]]) -> list[str]:
    missing = []
    for kind, item in devices.items():
        major, minor = item["majorMinor"].split(":", 1)
        if (major, minor, item["device"]) not in rows:
            missing.append(kind)
    return missing


def bcc_preflight_ready(target_devices: set[str], observed_devices: set[str]) -> bool:
    """Accept BCC once it reports a real DATA/WAL target; final coverage checks both."""
    return bool(target_devices & observed_devices)


def discover_devices(kubectl: list[str], namespace: str, pod: str) -> dict[str, dict[str, Any]]:
    script = r'''set -eu
data=$(printenv PGDATA 2>/dev/null || true)
[ -n "$data" ] || data=/var/lib/postgresql/data/pgdata
data=$(readlink -f "$data")
wal=$(readlink -f "$data/pg_wal")
for kind in data wal; do
 if [ "$kind" = data ]; then p=$data; else p=$wal; fi
 mountmm=$(findmnt -n -r -T "$p" -o MAJ:MIN | tr -d '[:space:]')
 [ -n "$mountmm" ] || { echo "$kind mount has no MAJ:MIN from findmnt" >&2; exit 1; }
 sys=$(readlink -f "/sys/dev/block/$mountmm")
 while [ -f "$sys/partition" ]; do sys=$(dirname "$sys"); done
 diskmm=$(cat "$sys/dev")
 name=$(basename "$sys")
 device_path=$(readlink -f "$sys/device" 2>/dev/null || true)
 driver_chain=''; storvsc_path=''; nvme_bound=0; ancestor=$device_path
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
 case "$name" in
  nvme[0-9]*n[0-9]*)
   controller=${name%n[0-9]*}; controller_path=$(readlink -f "/sys/class/nvme/$controller" 2>/dev/null || true)
   [ -n "$controller_path" ] && [ "$nvme_bound" = 1 ] || { echo "NVMe controller/driver missing for $name" >&2; exit 1; }
   controller_type=nvme; controller_driver=nvme
   ;;
  *)
   if [ -n "$storvsc_path" ]; then controller_type=storvsc; controller=hv_storvsc; controller_driver=hv_storvsc; controller_path=$storvsc_path; fi
   ;;
 esac
 printf '%s|%s|%s|%s|%s|%s|%s|%s|%s\n' "$kind" "$diskmm" "$name" "$p" "$controller_type" "$controller" "$controller_driver" "$controller_path" "${driver_chain:-none}"
done
'''
    out = check(kubectl + ["exec", "-c", "postgres", "-n", namespace, pod, "--", "sh", "-c", script])
    return parse_device_rows(out)


def build_bcc_pod(name: str, namespace: str, node: str, image: str) -> dict[str, Any]:
    command = r'''set -eu
export DEBIAN_FRONTEND=noninteractive

# BCC reads the kprobe blacklist from debugfs and available_filter_functions
# from tracefs. HostPath mounts use Kubernetes' default private propagation,
# so mounting either pseudo-filesystem here does not alter the host mount table.
mount_pseudo_fs() {
 fstype=$1; target=$2
 current=$(awk -v target="$target" '$2 == target { print $3; exit }' /proc/mounts)
 if [ "$current" != "$fstype" ]; then mount -t "$fstype" "$fstype" "$target"; fi
 current=$(awk -v target="$target" '$2 == target { print $3; exit }' /proc/mounts)
 [ "$current" = "$fstype" ] || { echo "failed to mount $fstype at $target (found ${current:-no mount})" >&2; exit 70; }
}
require_readable() {
 file=$1
 [ -r "$file" ] && head -c 1 "$file" >/dev/null 2>&1 || {
  echo "required BCC kernel metadata is missing or unreadable: $file" >&2; exit 71;
 }
}

mount_pseudo_fs debugfs /sys/kernel/debug
mount_pseudo_fs tracefs /sys/kernel/tracing
# BCC prefers debugfs/tracing whenever that path exists. On kernels that
# expose an empty directory there, bind the tracefs mount over it in this
# pod's private mount namespace so BCC sees the same files either way.
if [ -d /sys/kernel/debug/tracing ] && [ ! -r /sys/kernel/debug/tracing/available_filter_functions ]; then
 mount --bind /sys/kernel/tracing /sys/kernel/debug/tracing
fi
require_readable /sys/kernel/debug/kprobes/blacklist
trace_filter=/sys/kernel/debug/tracing/available_filter_functions
if [ ! -r "$trace_filter" ]; then trace_filter=/sys/kernel/tracing/available_filter_functions; fi
require_readable "$trace_filter"
printf '__BCC_TRACEFS_PREFLIGHT__\nblacklist=%s\navailable_filter_functions=%s\n' \
 /sys/kernel/debug/kprobes/blacklist "$trace_filter"

apt-get update -qq
apt-get install -y --no-install-recommends bpfcc-tools python3-bpfcc
tool=$(command -v biolatency-bpfcc || command -v biolatency || true)
[ -n "$tool" ] || { echo "biolatency is absent after installing bpfcc-tools" >&2; exit 127; }
help=$("$tool" --help 2>&1 || true)
printf '%s\n' "$help" | grep -Eq -- '(^|[[:space:]])-j([,[:space:]]|$)|--json' || {
 echo "installed biolatency lacks JSON histogram support (-j); refusing non-machine-readable run" >&2; exit 65;
}
printf '__BCC_TOOL_START__\n'
exec "$tool" -mTD -j 1
'''
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"app.kubernetes.io/name": "az-hdd-biolatency", "az-hdd-repro/run": name},
        },
        "spec": {
            "nodeName": node,
            "restartPolicy": "Never",
            "hostPID": True,
            "hostNetwork": True,
            "dnsPolicy": "ClusterFirstWithHostNet",
            "containers": [{
                "name": "biolatency",
                "image": image,
                "imagePullPolicy": "IfNotPresent",
                "command": ["/bin/sh", "-c"],
                "args": [command],
                "resources": POD_RESOURCES,
                "securityContext": {"privileged": True, "runAsUser": 0},
                "volumeMounts": [
                    {"name": "modules", "mountPath": "/lib/modules", "readOnly": True},
                    {"name": "src", "mountPath": "/usr/src", "readOnly": True},
                    {"name": "debugfs", "mountPath": "/sys/kernel/debug", "readOnly": False},
                    {"name": "tracefs", "mountPath": "/sys/kernel/tracing", "readOnly": False},
                ],
            }],
            "volumes": [
                {"name": "modules", "hostPath": {"path": "/lib/modules", "type": "Directory"}},
                {"name": "src", "hostPath": {"path": "/usr/src", "type": "Directory"}},
                {"name": "debugfs", "hostPath": {"path": "/sys/kernel/debug", "type": "Directory"}},
                {"name": "tracefs", "hostPath": {"path": "/sys/kernel/tracing", "type": "Directory"}},
            ],
        },
    }


class Collector:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.kubectl = kubectl_base(args)
        self.meta_path = self.out / "preflight.json"
        self.coverage_path = self.out / "coverage.json"
        self.status_path = self.out / "collector-status.json"
        self.stop_event = threading.Event()
        self.fatal_event = threading.Event()
        self.fatal_errors: list[str] = []
        self.threads: list[threading.Thread] = []
        self.bcc_pod: str | None = None
        self.bcc_image_id: str | None = None
        self.bcc_logs_proc: subprocess.Popen[str] | None = None
        self.bcc_ready = threading.Event()
        self.bcc_target_devices: set[str] = set()
        self.sql_first_sample = threading.Event()
        self.disk_first_sample = threading.Event()
        self.bcc_exited: dict[str, Any] = {}
        self.bcc_samples = 0
        self.bcc_unparsed = 0
        self.bcc_seen_devices: set[str] = set()
        self.sql_ok = 0
        self.sql_errors = 0
        self.sql_unparsed = 0
        self.sql_no_receiver = 0
        self.sql_times: list[str] = []
        self.disk_ok = 0
        self.disk_errors = 0
        self.disk_times: list[str] = []
        self.azure_polls = 0
        self.azure_errors = 0
        self.azure_keys: set[str] = set()
        self.azure_timestamps: set[str] = set()
        self.azure_lock = threading.Lock()
        (self.metric_instance_id, self.metric_vmss_id,
         self.vm_name, self.provider_id) = discover_vmss_identity(
            self.kubectl, args.node, args.vm_resource_id, args.vm_name,
        )
        self.metric_resource_id: str | None = None
        self.metric_namespace: str | None = None
        self.metric_scope: str | None = None
        self.metric_discovery: dict[str, Any] = {}
        self.metric_drain: dict[str, Any] = {"attempted": False}
        self.was_ready = False
        self.attachment_verification_path: Path | None = None
        self.started_utc: dt.datetime | None = None
        self.stopped_utc: dt.datetime | None = None
        self.devices: dict[str, dict[str, str]] = {}
        self.role_pod: dict[str, Any] = {}
        self.sql_app = "replica-instrument-" + uuid.uuid4().hex[:12]

    def status(self, phase: str, **extra: Any) -> None:
        atomic_json(self.status_path, {
            "updatedAtUtc": iso(), "phase": phase,
            "replicaPod": self.args.replica_pod,
            "replicaNode": self.args.node,
            "bccPod": self.bcc_pod,
            "fatalErrors": list(self.fatal_errors),
            **extra,
        })

    def preflight(self) -> None:
        self.started_utc = utc_now()
        self.status("starting")

        raw_pod = check(self.kubectl + ["get", "pod", "-n", self.args.namespace, self.args.replica_pod, "-o", "json"])
        self.role_pod = json.loads(raw_pod)
        labels = self.role_pod.get("metadata", {}).get("labels", {})
        actual_node = self.role_pod.get("spec", {}).get("nodeName")
        if self.role_pod.get("status", {}).get("phase") != "Running":
            raise RuntimeError(f"replica pod {self.args.replica_pod} is not Running")
        if labels.get("cnpg.io/instanceRole") not in (None, "replica"):
            raise RuntimeError(f"selected pod is not labeled as replica: {labels.get('cnpg.io/instanceRole')}")
        if self.args.node and self.args.node != actual_node:
            raise RuntimeError(f"replica pod is on {actual_node}, requested node was {self.args.node}")
        self.args.node = actual_node
        verified_vm_id = self.read_verified_vm_id()
        if self.args.vm_resource_id and verified_vm_id:
            explicit_id = self.args.vm_resource_id.removeprefix("azure://").rstrip("/").casefold()
            verified_id = verified_vm_id.removeprefix("azure://").rstrip("/").casefold()
            if explicit_id != verified_id:
                raise RuntimeError(
                    "--vm-resource-id differs from verified roles.replica.vmResourceId: "
                    f"{self.args.vm_resource_id} != {verified_vm_id}"
                )
        supplied_vm_id = verified_vm_id or self.args.vm_resource_id
        (self.metric_instance_id, self.metric_vmss_id,
         self.vm_name, self.provider_id) = discover_vmss_identity(
            self.kubectl, self.args.node, supplied_vm_id, self.args.vm_name,
        )
        if verified_vm_id and self.provider_id:
            verified_id = verified_vm_id.removeprefix("azure://").rstrip("/").casefold()
            provider_id = self.provider_id.removeprefix("azure://").rstrip("/").casefold()
            if verified_id != provider_id:
                raise RuntimeError(
                    "verified replica.vmResourceId does not match replica node providerID: "
                    f"{verified_vm_id} != {self.provider_id}"
                )
        if not self.metric_instance_id and not self.metric_vmss_id:
            raise RuntimeError("could not determine replica Azure VM/VMSS resource ID")

        self.devices = discover_devices(self.kubectl, self.args.namespace, self.args.replica_pod)
        self.bcc_target_devices = {x["device"] for x in self.devices.values()}
        disk_stats = check(self.kubectl + ["exec", "-c", "postgres", "-n", self.args.namespace,
                                           self.args.replica_pod, "--", "cat", "/proc/diskstats"])
        initial_disks = parse_diskstats_rows(disk_stats)
        missing = missing_diskstats_targets(self.devices, initial_disks)
        if missing:
            details = {kind: self.devices[kind] for kind in missing}
            raise RuntimeError(f"DATA/WAL device major:minor/name is missing from replica /proc/diskstats: {details}")

        # Confirm the replica identity and that the required PostgreSQL views exist.
        query = "SELECT json_build_object('isReplica',pg_is_in_recovery(),'serverVersion',current_setting('server_version'),'activityRows',(SELECT count(*) FROM pg_stat_activity WHERE backend_type='walreceiver'),'receiverRows',(SELECT count(*) FROM pg_stat_wal_receiver))::text;"
        sql_out = check(self.kubectl + ["exec", "-c", "postgres", "-n", self.args.namespace,
                                        self.args.replica_pod, "--", "psql", "-X", "-A", "-t", "-q",
                                        "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", self.args.database,
                                        "-c", query])
        sql_row = json.loads(sql_out.strip().splitlines()[-1])
        if not sql_row.get("isReplica"):
            raise RuntimeError("selected PostgreSQL pod reports pg_is_in_recovery() = false")

        self.start_bcc()
        self.start_bcc_log_reader()
        self.wait_bcc_ready()
        metric_discovery = self.discover_metric_scope()
        self.metric_discovery = metric_discovery
        azure_preflight = self.poll_azure(preflight=True)

        self.started_utc = utc_now()
        metadata = {
            "phase": "ready", "readyAtUtc": iso(self.started_utc),
            "namespace": self.args.namespace, "replicaPod": self.args.replica_pod,
            "replicaPodUid": self.role_pod.get("metadata", {}).get("uid"),
            "replicaNode": self.args.node, "providerId": self.provider_id,
            "attachmentVerificationPath": (str(self.attachment_verification_path)
                                              if self.attachment_verification_path else None),
            "azureMetricResourceId": self.metric_resource_id,
            "azureMetricResourceScope": self.metric_scope,
            "azureMetricNamespace": self.metric_namespace,
            "azureMetricDiscovery": metric_discovery,
            "azureVmNameDimension": self.vm_name,
            "database": self.args.database, "psqlPreflight": sql_row,
            "devices": self.devices,
            "diskstatsInitialDeviceNames": sorted(row[2] for row in initial_disks),
            "diskstatsInitialTargetDevices": {
                kind: {"majorMinor": item["majorMinor"], "device": item["device"],
                       "present": (item["majorMinor"].split(":", 1)[0],
                                   item["majorMinor"].split(":", 1)[1], item["device"]) in initial_disks}
                for kind, item in self.devices.items()},
            "bccImage": self.args.bcc_image,
            "bccImageId": self.bcc_image_id,
            "bccCommand": ["biolatency", "-mTD", "-j", "1"],
            "bccPreflightSampleCount": self.bcc_samples,
            "bccPreflightDevices": sorted(self.bcc_seen_devices),
            "bccPreflightTargetDevicesObserved": {
                kind: item["device"] in self.bcc_seen_devices for kind, item in self.devices.items()},
            "bccPreflightTargetDeviceObserved": bcc_preflight_ready(self.bcc_target_devices, self.bcc_seen_devices),
            "azureMetric": METRIC_NAME,
            "azurePreflightPoll": azure_preflight,
            "sampling": {"pgStatIntervalSeconds": 1, "diskstatsIntervalSeconds": 1,
                         "azureMetricNativeInterval": "PT1M"},
        }
        metadata["phase"] = "preflight_ready"
        atomic_json(self.meta_path, metadata)
        self.status("preflight_ready", preflightPath=str(self.meta_path))

    def read_verified_vm_id(self) -> str | None:
        candidate = Path(self.args.verified_attachments) if self.args.verified_attachments else (
            HERE.parent / ".state" / "attachment-verification" / "verified.json"
        )
        self.attachment_verification_path = candidate
        if not candidate.exists():
            if self.args.require_verified_attachments:
                raise RuntimeError(f"required verified-attachments manifest is missing: {candidate}")
            return None
        manifest = json.loads(candidate.read_text(encoding="utf-8"))
        replica = (manifest.get("roles") or {}).get("replica") or {}
        vm_id = replica.get("vmResourceId")
        if not vm_id:
            raise RuntimeError(f"{candidate} has no roles.replica.vmResourceId")
        return vm_id

    def start_bcc(self) -> None:
        self.bcc_pod = "biolatency-" + uuid.uuid4().hex[:10]
        pod = build_bcc_pod(self.bcc_pod, self.args.namespace, self.args.node, self.args.bcc_image)
        check(self.kubectl + ["apply", "-f", "-"], input_text=json.dumps(pod))

        deadline = time.monotonic() + self.args.bcc_startup_timeout
        while time.monotonic() < deadline:
            info = json.loads(check(self.kubectl + ["get", "pod", "-n", self.args.namespace,
                                                       self.bcc_pod, "-o", "json"]))
            phase = info.get("status", {}).get("phase")
            cs = (info.get("status", {}).get("containerStatuses") or [{}])[0]
            self.bcc_image_id = cs.get("imageID")
            waiting = cs.get("state", {}).get("waiting", {})
            terminated = cs.get("state", {}).get("terminated", {})
            if phase == "Running" and cs.get("ready"):
                return
            if phase in ("Failed", "Succeeded") or waiting.get("reason") == "ImagePullBackOff":
                raise RuntimeError(f"BCC pod did not start: phase={phase}, waiting={waiting}, terminated={terminated}")
            time.sleep(2)
        raise TimeoutError(f"BCC pod {self.bcc_pod} was not ready in {self.args.bcc_startup_timeout}s")

    def start_bcc_log_reader(self) -> None:
        assert self.bcc_pod
        argv = self.kubectl + ["logs", "-n", self.args.namespace, self.bcc_pod,
                               "-c", "biolatency", "--follow", "--timestamps=true"]
        stderr_file = (self.out / "biolatency-kubectl.stderr.log").open("ab", buffering=0)
        self.bcc_logs_proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=stderr_file,
                                              text=True, bufsize=1)

        def read_logs() -> None:
            assert self.bcc_logs_proc and self.bcc_logs_proc.stdout
            raw_path = self.out / "biolatency.raw.log"
            decoded_path = self.out / "biolatency-histograms.jsonl"
            with raw_path.open("a", encoding="utf-8", buffering=1) as raw:
                for line in self.bcc_logs_proc.stdout:
                    raw.write(line)
                    raw.flush()
                    payload = line.strip()
                    # kubectl --timestamps prefixes each container log record.
                    hit = payload.find("{")
                    if hit >= 0:
                        parsed = parse_bcc_histogram_line(payload)
                        if parsed:
                            device, histogram = parsed
                            self.bcc_samples += 1
                            self.bcc_seen_devices.add(device)
                            observed = iso()
                            append_jsonl(decoded_path, {
                                "capturedAtUtc": observed,
                                "containerLogTimestamp": payload[:hit].strip(),
                                "device": device,
                                "histogram": json_safe(histogram),
                            })
                            if bcc_preflight_ready(self.bcc_target_devices, self.bcc_seen_devices):
                                self.bcc_ready.set()
                        else:
                            self.bcc_unparsed += 1
                            append_jsonl(self.out / "biolatency-unparsed.jsonl", {
                                "capturedAtUtc": iso(), "reason": "invalid or non-device histogram",
                                "line": payload,
                            })
            code = self.bcc_logs_proc.wait()
            self.bcc_exited.update({"exitCode": code, "atUtc": iso()})
            if not self.stop_event.is_set():
                self.fail(f"BCC log stream ended unexpectedly (exit {code})")

        t = threading.Thread(target=read_logs, name="bcc-log-reader", daemon=True)
        t.start()
        self.threads.append(t)

    def wait_bcc_ready(self) -> None:
        deadline = time.monotonic() + self.args.bcc_preflight_timeout
        while time.monotonic() < deadline:
            if self.bcc_ready.wait(0.25):
                # The pod can appear Running before BCC compiles or attaches.
                if self.bcc_logs_proc and self.bcc_logs_proc.poll() is None:
                    return
                raise RuntimeError("biolatency exited immediately after its first histogram")
            if self.bcc_logs_proc and self.bcc_logs_proc.poll() is not None:
                raise RuntimeError(f"biolatency exited before producing a histogram: {self.bcc_exited}")
            time.sleep(0.25)
        raise TimeoutError(
            "biolatency did not produce a parseable histogram for either replica DATA or WAL device; "
            f"observed={sorted(self.bcc_seen_devices)}, required={sorted(self.bcc_target_devices)}; "
            f"see {self.out / 'biolatency.raw.log'} and "
            f"{self.out / 'biolatency-kubectl.stderr.log'}"
        )

    def metric_definitions(self, resource_id: str) -> tuple[list[dict[str, Any]] | None, str, list[str], int]:
        argv = [self.args.az, "monitor", "metrics", "list-definitions",
                "--resource", resource_id, "--output", "json", "--only-show-errors"]
        p = run(argv, timeout=self.args.azure_timeout)
        try:
            rows = json.loads(p.stdout) if p.stdout.strip() else None
        except json.JSONDecodeError:
            rows = None
        return rows if isinstance(rows, list) else None, p.stderr, argv, p.returncode

    @staticmethod
    def find_metric_definition(rows: list[dict[str, Any]] | None) -> dict[str, Any] | None:
        return next((x for x in (rows or [])
                     if (x.get("name") or {}).get("value") == METRIC_NAME), None)

    def query_metric_candidate(self, resource_id: str, namespace: str, scope: str,
                               vm_name: str | None, start: dt.datetime,
                               end: dt.datetime) -> tuple[dict[str, Any] | None, str, list[str], int, str]:
        argv = [self.args.az, "monitor", "metrics", "list",
                "--resource", resource_id,
                "--metric", METRIC_NAME,
                "--namespace", namespace,
                "--interval", "PT1M",
                "--aggregation", "Average",
                "--start-time", iso(start).replace(".000Z", "Z"),
                "--end-time", iso(end).replace(".000Z", "Z"),
                "--output", "json", "--only-show-errors"]
        if scope == "vmss-filtered":
            if not vm_name:
                raise RuntimeError("refusing aggregate VMSS query without the exact replica VMName filter")
            argv += ["--filter", f"VMName eq '{vm_name}'"]
        p = run(argv, timeout=self.args.azure_timeout)
        try:
            response = json.loads(p.stdout) if p.stdout.strip() else None
        except json.JSONDecodeError:
            response = None
        return response if isinstance(response, dict) else None, p.stderr, argv, p.returncode, p.stdout

    @staticmethod
    def validate_metric_candidate(response: dict[str, Any] | None, scope: str,
                                   expected_vm_name: str | None) -> tuple[str | None, dict[str, Any]]:
        if not isinstance(response, dict):
            return "live metric query returned no JSON object", {}
        metric_rows = [x for x in response.get("value", [])
                       if (x.get("name") or {}).get("value") == METRIC_NAME]
        if not metric_rows:
            return "live metric response omitted the requested metric", {}
        series = [s for metric in metric_rows for s in metric.get("timeseries", [])]
        proof: dict[str, Any] = {
            "timespan": response.get("timespan"),
            "interval": response.get("interval"),
            "metricSeriesCount": len(series),
            "dataPointCount": sum(len(s.get("data", [])) for s in series),
        }
        if not series:
            return "live metric response has no time series to validate", proof
        if scope != "vmss-filtered":
            return None, proof
        if not expected_vm_name:
            return "VMSS fallback has no exact expected VMName", proof
        observed_names: list[str | None] = []
        for item in series:
            dimensions: dict[str, str] = {}
            for metadata in item.get("metadatavalues", []) or []:
                name = metadata.get("name") or {}
                key = name.get("value") or name.get("localizedValue")
                if key is not None:
                    dimensions[str(key).casefold()] = str(metadata.get("value", ""))
            observed = dimensions.get("vmname")
            observed_names.append(observed)
            if observed != expected_vm_name:
                proof["observedVmNames"] = observed_names
                return (
                    "VMName-filtered VMSS response included an aggregate or unexpected series; "
                    f"expected {expected_vm_name!r}, observed {observed!r}",
                    proof,
                )
        proof["observedVmNames"] = observed_names
        proof["allSeriesMatchExpectedVmName"] = True
        return None, proof

    def discover_metric_scope(self) -> dict[str, Any]:
        """Discover the metric namespace and select the exact replica VM.

        Every candidate must pass a live query. The VMSS-instance resource is
        tried first; the only fallback is its parent VMSS with a definition-
        supported, exact VMName filter and matching response-series metadata.
        """
        candidates: list[tuple[str, str, str | None]] = []
        if self.metric_instance_id and self.metric_vmss_id:
            candidates.append((self.metric_instance_id, "vmss-instance", None))
        elif self.metric_instance_id:
            candidates.append((self.metric_instance_id, "vm", None))
        if self.metric_vmss_id and self.vm_name:
            candidates.append((self.metric_vmss_id, "vmss-filtered", self.vm_name))

        failed: list[dict[str, Any]] = []
        checked_at = utc_now()
        probe_start = checked_at - dt.timedelta(minutes=10)
        for resource_id, scope, dimension in candidates:
            rows, stderr, argv, code = self.metric_definitions(resource_id)
            definition = self.find_metric_definition(rows)
            if code != 0 or not definition:
                failed.append({"scope": scope, "resourceId": resource_id,
                               "command": argv, "exitCode": code,
                               "stderr": stderr[-3000:],
                               "metricDefinitionFound": bool(definition)})
                continue
            namespace = definition.get("namespace")
            if not namespace:
                failed.append({"scope": scope, "resourceId": resource_id,
                               "error": "metric definition has no namespace"})
                continue
            dimensions = {
                str((x.get("value") or x.get("localizedValue"))
                    if isinstance(x, dict) else x).casefold()
                for x in definition.get("dimensions", [])
            }
            if scope == "vmss-filtered" and ("vmname" not in dimensions or not dimension):
                failed.append({"scope": scope, "resourceId": resource_id,
                               "error": "VMSS fallback lacks the VMName dimension or exact replica VMName",
                               "dimensions": sorted(dimensions)})
                continue

            response, query_stderr, query_argv, query_code, query_stdout = self.query_metric_candidate(
                resource_id, namespace, scope, dimension, probe_start, checked_at,
            )
            validation_error, live_proof = self.validate_metric_candidate(response, scope, dimension)
            if query_code != 0 or validation_error:
                failed.append({"scope": scope, "resourceId": resource_id,
                               "definitionCommand": argv, "definitionExitCode": code,
                               "liveQueryCommand": query_argv, "liveQueryExitCode": query_code,
                               "liveQueryStderr": query_stderr[-3000:],
                               "liveQueryStdoutTail": query_stdout[-2000:],
                               "liveValidationError": validation_error,
                               "liveValidation": live_proof})
                continue
            self.metric_resource_id = resource_id
            self.metric_namespace = namespace
            self.metric_scope = scope
            return {"selectedScope": scope, "resourceId": resource_id,
                    "namespace": namespace, "metric": METRIC_NAME,
                    "dimensions": sorted(dimensions), "vmNameFilter": dimension,
                    "liveQuery": {"command": query_argv, "exitCode": query_code,
                                  "validatedAtUtc": iso(checked_at), **live_proof},
                    "failedCandidates": failed}
        raise RuntimeError(
            "Azure metric discovery failed for the replica VM instance and "
            "VMName-filtered VMSS fallback: " + json.dumps(failed, sort_keys=True)
        )

    def azure_query(self, start: dt.datetime, end: dt.datetime) -> tuple[dict[str, Any] | None, str, list[str], int, str]:
        if not self.metric_resource_id or not self.metric_namespace:
            raise RuntimeError("Azure metric scope was not discovered before polling")
        return self.query_metric_candidate(self.metric_resource_id, self.metric_namespace,
                                           self.metric_scope, self.vm_name, start, end)

    def flatten_metrics(self, response: dict[str, Any], poll_id: str) -> int:
        count = 0
        for metric in response.get("value", []):
            metric_name = (metric.get("name") or {}).get("value")
            if metric_name != METRIC_NAME:
                continue
            for series in metric.get("timeseries", []):
                dimensions = {}
                for item in series.get("metadatavalues", []) or []:
                    name = item.get("name") or {}
                    key = name.get("value", "") or name.get("localizedValue", "")
                    dimensions[str(key)] = item.get("value")
                normalized_dimensions = {str(k).casefold(): v for k, v in dimensions.items()}
                if self.metric_scope == "vmss-filtered" and normalized_dimensions.get("vmname") != self.vm_name:
                    raise RuntimeError(
                        "VMName-filtered VMSS response did not identify the exact replica series; "
                        f"expected {self.vm_name!r}, got {dimensions!r}"
                    )
                for point in series.get("data", []):
                    timestamp = point.get("timeStamp")
                    if not timestamp:
                        continue
                    # Keep successive readings of the same one-minute bucket:
                    # Azure can revise a still-open interval between polls.
                    key = f"{metric_name}|{timestamp}|{json.dumps(dimensions, sort_keys=True)}|{poll_id}"
                    with self.azure_lock:
                        if key in self.azure_keys:
                            continue
                        self.azure_keys.add(key)
                        self.azure_timestamps.add(timestamp)
                    append_jsonl(self.out / "azure-vm-uncached-iops.jsonl", {
                        "metric": metric_name, "timeStamp": timestamp,
                        "averagePercent": point.get("average"),
                        "minimumPercent": point.get("minimum"),
                        "maximumPercent": point.get("maximum"),
                        "dimensions": dimensions, "pollId": poll_id,
                    })
                    count += 1
        return count

    def poll_azure(self, preflight: bool = False, final: bool = False) -> dict[str, Any]:
        now = utc_now()
        if preflight:
            start = now - dt.timedelta(minutes=10)
        elif final and self.started_utc:
            start = self.started_utc - dt.timedelta(minutes=2)
        elif self.started_utc:
            start = max(self.started_utc - dt.timedelta(minutes=2), now - dt.timedelta(minutes=12))
        else:
            start = now - dt.timedelta(minutes=10)
        end = now
        poll_id = uuid.uuid4().hex[:12]
        response, stderr, argv, code, stdout = self.azure_query(start, end)
        record = {
            "pollId": poll_id, "capturedAtUtc": iso(now),
            "windowStartUtc": iso(start), "windowEndUtc": iso(end),
            "resourceId": self.metric_resource_id, "resourceScope": self.metric_scope,
            "metricNamespace": self.metric_namespace, "vmNameDimension": self.vm_name,
            "metric": METRIC_NAME, "command": argv,
            "exitCode": code, "stderr": stderr[-4000:], "response": response,
        }
        append_jsonl(self.out / "azure-monitor-polls.jsonl", record)
        self.azure_polls += 1
        if code != 0 or response is None:
            self.azure_errors += 1
            append_jsonl(self.out / "azure-monitor-errors.jsonl", {
                "pollId": poll_id, "capturedAtUtc": iso(now), "exitCode": code,
                "stderr": stderr[-4000:], "stdoutTail": stdout[-2000:],
                "command": argv,
            })
            if preflight:
                raise RuntimeError(f"Azure Monitor metric preflight failed: {(stderr or stdout)[-2000:]}")
            return {"pollId": poll_id, "exitCode": code, "pointCount": 0, "error": stderr[-1000:]}
        if not any((x.get("name") or {}).get("value") == METRIC_NAME for x in response.get("value", [])):
            self.azure_errors += 1
            err = "Azure Monitor response did not contain the requested metric name"
            append_jsonl(self.out / "azure-monitor-errors.jsonl", {
                "pollId": poll_id, "capturedAtUtc": iso(now), "exitCode": 0,
                "stderr": err, "command": argv,
            })
            if preflight:
                raise RuntimeError(err)
            return {"pollId": poll_id, "exitCode": 0, "pointCount": 0, "error": err}
        points = self.flatten_metrics(response, poll_id)
        return {"pollId": poll_id, "exitCode": 0, "pointCount": points,
                "responseInterval": response.get("interval"), "timespan": response.get("timespan")}

    def fail(self, message: str) -> None:
        self.fatal_errors.append(message)
        self.fatal_event.set()
        append_jsonl(self.out / "collector-errors.jsonl", {"atUtc": iso(), "error": message})
        self.status("failed", error=message)

    def start_sql_watcher(self) -> None:
        argv = self.kubectl + ["exec", "-i", "-c", "postgres", "-n", self.args.namespace,
                               self.args.replica_pod, "--", "env", f"PGAPPNAME={self.sql_app}",
                               "psql", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1",
                               "-U", "postgres", "-d", self.args.database]
        err_file = (self.out / "pgstat-psql.stderr.log").open("ab", buffering=0)
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=err_file, text=True, bufsize=1)
        assert proc.stdin is not None and proc.stdout is not None
        proc.stdin.write(SQL_PRETTY)
        proc.stdin.flush()
        proc.stdin.close()

        def read_sql() -> None:
            raw_path = self.out / "pgstat-receiver.raw.log"
            row_path = self.out / "pgstat-receiver.jsonl"
            with raw_path.open("a", encoding="utf-8", buffering=1) as raw:
                for line in proc.stdout:
                    raw.write(line)
                    raw.flush()
                    if "{" not in line:
                        continue
                    try:
                        row = parse_psql_json_line(line)
                        if row is None:
                            continue
                    except (json.JSONDecodeError, ValueError):
                        self.sql_unparsed += 1
                        append_jsonl(self.out / "pgstat-receiver-unparsed.jsonl", {
                            "capturedAtUtc": iso(), "line": line.rstrip("\n"),
                        })
                        continue
                    sampled = row.get("sampledAt")
                    self.sql_ok += 1
                    self.sql_first_sample.set()
                    if sampled:
                        self.sql_times.append(str(sampled))
                    if not row.get("walReceiverWait"):
                        self.sql_no_receiver += 1
                    append_jsonl(row_path, {
                        "capturedAtUtc": iso(), "serverSampledAt": sampled,
                        "walReceiverWait": row.get("walReceiverWait", []),
                        "walReceiver": row.get("walReceiver"),
                        "replayLsn": row.get("replayLsn"),
                    })
            code = proc.wait()
            if not self.stop_event.is_set():
                self.sql_errors += 1
                self.fail(f"persistent pg_stat watcher ended unexpectedly (exit {code})")

        t = threading.Thread(target=read_sql, name="pgstat-watch", daemon=True)
        t.start()
        self.threads.append(t)
        self.sql_proc = proc

    def start_diskstats_watcher(self) -> None:
        script = r'''set -eu
while :; do
 ts=$(date -u +'%Y-%m-%dT%H:%M:%S.%NZ')
 printf '__DISKSTATS__\t%s\t' "$ts"
 tr '\n' ';' </proc/diskstats
 printf '\n'
 sleep 1
done
'''
        argv = self.kubectl + ["exec", "-i", "-c", "postgres", "-n", self.args.namespace,
                               self.args.replica_pod, "--", "sh", "-c", script]
        err_file = (self.out / "diskstats-kubectl.stderr.log").open("ab", buffering=0)
        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=err_file,
                                text=True, bufsize=1)
        assert proc.stdout is not None

        def read_diskstats() -> None:
            raw_path = self.out / "diskstats.raw.log"
            row_path = self.out / "diskstats.jsonl"
            with raw_path.open("a", encoding="utf-8", buffering=1) as raw:
                for line in proc.stdout:
                    raw.write(line)
                    raw.flush()
                    if not line.startswith("__DISKSTATS__\t"):
                        append_jsonl(self.out / "diskstats-unparsed.jsonl", {
                            "capturedAtUtc": iso(), "line": line.rstrip("\n"),
                        })
                        continue
                    try:
                        _, sampled, raw_rows = line.rstrip("\n").split("\t", 2)
                        disks = []
                        for raw_row in raw_rows.split(";"):
                            f = raw_row.split()
                            if len(f) < 3:
                                continue
                            disks.append({
                                "major": int(f[0]), "minor": int(f[1]), "name": f[2],
                                "counters": [int(x) for x in f[3:]], "raw": raw_row,
                            })
                        present = {(str(x["major"]), str(x["minor"]), x["name"]) for x in disks}
                        missing = missing_diskstats_targets(self.devices, present)
                        if missing:
                            raise ValueError(f"DATA/WAL block major:minor/name disappeared from /proc/diskstats: {missing}")
                        self.disk_ok += 1
                        self.disk_first_sample.set()
                        self.disk_times.append(sampled)
                        append_jsonl(row_path, {
                            "sampledAtRemote": sampled, "capturedAtUtc": iso(),
                            "devices": disks,
                        })
                    except (ValueError, TypeError) as e:
                        self.disk_errors += 1
                        append_jsonl(self.out / "diskstats-errors.jsonl", {
                            "capturedAtUtc": iso(), "error": str(e), "line": line.rstrip("\n"),
                        })
            code = proc.wait()
            if not self.stop_event.is_set():
                self.disk_errors += 1
                self.fail(f"persistent /proc/diskstats watcher ended unexpectedly (exit {code})")

        t = threading.Thread(target=read_diskstats, name="diskstats-watch", daemon=True)
        t.start()
        self.threads.append(t)
        self.disk_proc = proc

    def start_azure_poller(self) -> None:
        def poll_loop() -> None:
            next_poll = time.monotonic()
            while not self.stop_event.is_set():
                if self.stop_event.wait(max(0.0, next_poll - time.monotonic())):
                    break
                try:
                    self.poll_azure()
                except Exception as e:
                    self.azure_errors += 1
                    append_jsonl(self.out / "azure-monitor-errors.jsonl", {
                        "capturedAtUtc": iso(), "error": str(e),
                    })
                next_poll += self.args.azure_poll_seconds
                if next_poll < time.monotonic():
                    next_poll = time.monotonic() + self.args.azure_poll_seconds

        t = threading.Thread(target=poll_loop, name="azure-metrics", daemon=True)
        t.start()
        self.threads.append(t)

    def wait_live_streams(self) -> None:
        deadline = time.monotonic() + self.args.stream_preflight_timeout
        while time.monotonic() < deadline:
            if self.sql_first_sample.is_set() and self.disk_first_sample.is_set():
                return
            if self.fatal_event.is_set():
                raise RuntimeError("a required live collector failed during readiness: "
                                   + "; ".join(self.fatal_errors))
            remaining = deadline - time.monotonic()
            self.sql_first_sample.wait(min(0.2, max(0, remaining)))
        raise TimeoutError(
            "live pg_stat and /proc/diskstats streams did not each produce a first sample "
            f"within {self.args.stream_preflight_timeout}s"
        )

    def terminate_sql_session(self) -> None:
        # The application name is a per-run random token, so this cancellation can
        # only target the persistent watcher created by this process.
        query = (
            "SELECT coalesce(json_agg(json_build_object('pid',pid,'terminated',"
            "pg_terminate_backend(pid))),'[]'::json)::text FROM pg_stat_activity "
            f"WHERE application_name='{self.sql_app}' AND backend_type='client backend';"
        )
        try:
            check(self.kubectl + ["exec", "-c", "postgres", "-n", self.args.namespace,
                                  self.args.replica_pod, "--", "psql", "-X", "-A", "-t", "-q",
                                  "-U", "postgres", "-d", self.args.database, "-c", query], timeout=20)
        except Exception as e:
            append_jsonl(self.out / "collector-errors.jsonl", {
                "atUtc": iso(), "cleanupError": f"could not terminate psql watcher: {e}",
            })

    def stop_process(self, proc: subprocess.Popen[str] | None) -> None:
        if proc is None or proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    def final_metric_drain(self) -> None:
        if not self.was_ready or not self.started_utc or not self.metric_resource_id:
            self.metric_drain = {"attempted": False, "reason": "collector never reached collecting"}
            return
        if self.args.metric_drain_seconds <= 0:
            self.metric_drain = {"attempted": False, "reason": "disabled by configuration"}
            return
        cutoff = time.monotonic() + self.args.metric_drain_seconds
        stop = self.stopped_utc or utc_now()
        expected_latest = (stop - dt.timedelta(minutes=1)).replace(second=0, microsecond=0)
        self.metric_drain = {
            "attempted": True, "targetLatestTimestamp": iso(expected_latest),
            "timeoutSeconds": self.args.metric_drain_seconds,
        }
        while True:
            try:
                self.poll_azure(final=True)
            except Exception as e:
                self.azure_errors += 1
                append_jsonl(self.out / "azure-monitor-errors.jsonl", {
                    "capturedAtUtc": iso(), "finalDrainError": str(e),
                })
            latest = max((parse_time(x) for x in self.azure_timestamps), default=None)
            if latest and latest >= expected_latest:
                self.metric_drain.update({"complete": True, "latestTimestamp": iso(latest)})
                return
            remaining = cutoff - time.monotonic()
            if remaining <= 0:
                self.metric_drain.update({"complete": False,
                                          "latestTimestamp": iso(latest) if latest else None})
                return
            time.sleep(min(15, remaining))

    def summary(self) -> dict[str, Any]:
        def span(values: list[str]) -> dict[str, Any]:
            if not values:
                return {"first": None, "last": None, "intervalSeconds": None, "intervalsOver1_5s": 0}
            times = sorted(parse_time(x) for x in values)
            deltas = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
            return {
                "first": iso(times[0]), "last": iso(times[-1]),
                "intervalSeconds": {"min": min(deltas) if deltas else None,
                                    "median": sorted(deltas)[len(deltas)//2] if deltas else None,
                                    "max": max(deltas) if deltas else None},
                "intervalsOver1_5s": sum(x > 1.5 for x in deltas),
            }
        return {
            "startedAtUtc": iso(self.started_utc) if self.started_utc else None,
            "stoppedAtUtc": iso(self.stopped_utc) if self.stopped_utc else None,
            "replica": {"pod": self.args.replica_pod, "node": self.args.node,
                        "devices": self.devices},
            "pgStatActivityAndWalReceiver": {
                "targetIntervalSeconds": 1, "successfulSamples": self.sql_ok,
                "samplesWithoutWalReceiverBackend": self.sql_no_receiver,
                "streamErrors": self.sql_errors, "unparsedLines": self.sql_unparsed,
                **span(self.sql_times),
            },
            "diskstats": {"targetIntervalSeconds": 1, "successfulSamples": self.disk_ok,
                           "errors": self.disk_errors, **span(self.disk_times)},
            "biolatency": {"image": self.args.bcc_image,
                           "deviceHistogramsParsed": self.bcc_samples,
                           "unparsedLines": self.bcc_unparsed,
                           "dataDevice": self.devices.get("data", {}).get("device"),
                           "walDevice": self.devices.get("wal", {}).get("device"),
                           "dataDeviceObservedInHistogram": self.devices.get("data", {}).get("device") in self.bcc_seen_devices,
                           "walDeviceObservedInHistogram": self.devices.get("wal", {}).get("device") in self.bcc_seen_devices,
                           "exit": self.bcc_exited or None},
            "azureMonitor": {"metric": METRIC_NAME, "resourceId": self.metric_resource_id,
                             "resourceScope": self.metric_scope,
                             "metricNamespace": self.metric_namespace,
                             "vmNameDimension": self.vm_name,
                             "metricDiscovery": self.metric_discovery,
                             "nativeInterval": "PT1M", "polls": self.azure_polls,
                             "errors": self.azure_errors,
                             "uniquePoints": len(self.azure_timestamps),
                             "firstPoint": min(self.azure_timestamps) if self.azure_timestamps else None,
                             "lastPoint": max(self.azure_timestamps) if self.azure_timestamps else None},
            "fatalErrors": list(self.fatal_errors),
            "azureMetricDrain": self.metric_drain,
        }

    def run(self) -> int:
        rc = 0
        try:
            self.preflight()
            self.start_sql_watcher()
            self.start_diskstats_watcher()
            self.wait_live_streams()
            self.start_azure_poller()
            self.was_ready = True
            ready_at = iso()
            ready_metadata = json.loads(self.meta_path.read_text(encoding="utf-8"))
            ready_metadata.update({"phase": "ready", "readyAtUtc": ready_at,
                                   "livePgStatSamplesAtReady": self.sql_ok,
                                   "liveDiskstatsSamplesAtReady": self.disk_ok})
            atomic_json(self.meta_path, ready_metadata)
            self.status("collecting", startedAtUtc=iso(self.started_utc))
            print(json.dumps({"event": "READY", "preflight": str(self.meta_path),
                              "status": str(self.status_path)}, sort_keys=True), flush=True)
            while not self.stop_event.wait(1):
                if self.fatal_event.is_set():
                    raise RuntimeError("a required replica instrumentation stream stopped")
                if self.bcc_logs_proc and self.bcc_logs_proc.poll() is not None:
                    raise RuntimeError(f"biolatency stopped unexpectedly: {self.bcc_exited}")
        except Exception as e:
            if not self.fatal_errors or self.fatal_errors[-1] != str(e):
                self.fatal_errors.append(str(e))
            self.status("failed", error=str(e))
            append_jsonl(self.out / "collector-errors.jsonl", {"atUtc": iso(), "error": str(e)})
            print(f"instrumentation preflight/collection failed: {e}", file=sys.stderr, flush=True)
            rc = 2
        finally:
            self.stop_event.set()
            if getattr(self, "sql_proc", None):
                self.terminate_sql_session()
                self.stop_process(self.sql_proc)
            if getattr(self, "disk_proc", None):
                self.stop_process(self.disk_proc)
            if self.bcc_logs_proc and self.bcc_logs_proc.poll() is None:
                self.stop_process(self.bcc_logs_proc)
            if self.bcc_pod:
                try:
                    check(self.kubectl + ["delete", "pod", "-n", self.args.namespace,
                                          self.bcc_pod, "--ignore-not-found=true", "--wait=true", "--timeout=90s"], timeout=100)
                except Exception as e:
                    self.fatal_errors.append(f"BCC pod cleanup failed: {e}")
                    append_jsonl(self.out / "collector-errors.jsonl", {
                        "atUtc": iso(), "cleanupError": str(e), "bccPod": self.bcc_pod,
                    })
            for t in self.threads:
                if t.is_alive():
                    wait_for = self.args.azure_timeout + 5 if t.name == "azure-metrics" else 5
                    t.join(timeout=wait_for)
            self.stopped_utc = utc_now()
            self.final_metric_drain()
            atomic_json(self.coverage_path, self.summary())
            self.status("stopped" if not self.fatal_errors else "failed",
                        stoppedAtUtc=iso(self.stopped_utc), coveragePath=str(self.coverage_path),
                        azureMetricDrain=self.metric_drain)
        return 2 if self.fatal_errors else rc


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--kubectl", default="kubectl")
    p.add_argument("--kubeconfig")
    p.add_argument("--namespace", required=True)
    p.add_argument("--replica-pod", required=True)
    p.add_argument("--node", help="Expected replica node; inferred from the pod if omitted")
    p.add_argument("--database", default="repro")
    p.add_argument("--out", required=True, help="Local directory for timestamped raw data and coverage")
    p.add_argument("--bcc-image", default=os.environ.get("BCC_IMAGE", "ubuntu:24.04"))
    p.add_argument("--bcc-startup-timeout", type=int, default=300)
    p.add_argument("--bcc-preflight-timeout", type=int, default=240)
    p.add_argument("--stream-preflight-timeout", type=int, default=20)
    p.add_argument("--vm-resource-id", help="Optional Azure VM or VMSS instance resource ID")
    p.add_argument("--vm-name", help="Optional VMName metric dimension for a VMSS resource")
    p.add_argument("--verified-attachments", help="Defaults to ../.state/attachment-verification/verified.json")
    p.add_argument("--require-verified-attachments", action=argparse.BooleanOptionalAction, default=True,
                   help="Require replica VM ID from verified attachment manifest (default: required)")
    p.add_argument("--az", default="az", help="Azure CLI or credential-wrapper executable")
    p.add_argument("--azure-poll-seconds", type=int, default=60)
    p.add_argument("--azure-timeout", type=int, default=60)
    p.add_argument("--metric-drain-seconds", type=int, default=120,
                   help="Final Azure metric-ingestion grace period before cleanup")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.azure_poll_seconds < 30 or args.metric_drain_seconds < 0:
        print("azure-poll-seconds must be >=30 and metric-drain-seconds >=0", file=sys.stderr)
        return 2
    collector = Collector(args)

    def stop_handler(signum: int, _frame: Any) -> None:
        collector.stop_event.set()

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    return collector.run()


if __name__ == "__main__":
    raise SystemExit(main())
