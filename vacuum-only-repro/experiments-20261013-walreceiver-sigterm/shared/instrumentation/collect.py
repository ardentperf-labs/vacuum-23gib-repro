#!/usr/bin/env python3
"""Collect synchronized primary/replica PostgreSQL, block, BCC, and Azure metrics.

Requires a verified-attachments.json produced by the sibling verifier. Azure
Monitor samples retain the platform's native PT1M grain. Send SIGINT/SIGTERM
after VACUUM and client observation finish; the collector drains late metrics,
removes only its uniquely named BCC pods, and writes coverage/gap evidence.
"""

from __future__ import annotations

import argparse
import ast
import base64
import datetime as dt
import hashlib
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
from urllib.parse import urlsplit

from azure_metrics import AzureMetrics, append_jsonl

UTC = dt.timezone.utc
POD_RESOURCES = {"requests": {"cpu": "50m", "memory": "96Mi"},
                 "limits": {"cpu": "500m", "memory": "512Mi"}}


def iso(value: dt.datetime | None = None) -> str:
    return (value or dt.datetime.now(UTC)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def parse_kubectl_log_timestamp(line: str) -> tuple[str | None, dt.datetime | None]:
    """Extract the RFC3339Nano prefix emitted by kubectl --timestamps=true."""
    match = re.match(r"^(\d{4}-\d\d-\d\dT\S+)\s", line)
    if not match:
        return None, None
    raw = match.group(1)
    try:
        return raw, parse_time(raw)
    except ValueError:
        return raw, None


def archive_receipt_path_for(verified_attachments: str | Path) -> Path:
    """Find only the stack-owned `.state/wal-archive.json` beside attachments."""
    attachment_path = Path(verified_attachments).resolve()
    state_dir = next((parent for parent in list(attachment_path.parents)[:4]
                      if parent.name == ".state"), None)
    return ((state_dir / "wal-archive.json") if state_dir else
            (attachment_path.parent / "wal-archive.json"))


def bcc_stream_resume_since(last_timestamp: dt.datetime | None) -> str | None:
    if last_timestamp is None:
        return None
    value = last_timestamp - dt.timedelta(seconds=2)
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def bcc_log_line_fingerprint(container_timestamp: str, line: str) -> str:
    return hashlib.sha256((container_timestamp + "\0" + line).encode("utf-8", "replace")).hexdigest()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp")
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def destination_summary(value: Any) -> dict[str, Any]:
    """Summarize a backup destination without retaining URL credentials or path."""
    if not isinstance(value, str) or not value:
        return {"configured": False}
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        scheme = parsed.scheme or None
        path = parsed.path or ""
        digest = hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()
        return {"configured": True, "scheme": scheme, "host": host,
                "pathComponentCount": len([part for part in path.split("/") if part]),
                "destinationSha256": digest, "queryOrFragmentOmitted": bool(parsed.query or parsed.fragment)}
    except ValueError:
        return {"configured": True, "parseableUrl": False,
                "destinationSha256": hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()}


def secret_selectors(value: Any, path: str = "") -> list[dict[str, str]]:
    """Return SecretKeySelector references only; never serialize adjacent values."""
    found: list[dict[str, str]] = []
    if isinstance(value, dict):
        parent = path.rsplit(".", 1)[-1].casefold()
        if (parent in {"credentials", "azurecredentials", "s3credentials", "googlecredentials",
                       "storageaccount", "storagekey", "accesskeyid", "secretaccesskey"}
                or "secret" in parent or parent.endswith("keyselector")):
            name, key = value.get("name"), value.get("key")
            if isinstance(name, str) and isinstance(key, str):
                found.append({"path": path, "secretName": name, "secretKey": key})
        for key, child in value.items():
            if isinstance(child, (dict, list)):
                found.extend(secret_selectors(child, f"{path}.{key}" if path else str(key)))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(secret_selectors(child, f"{path}[{index}]"))
    return found


def barman_backend_summary(config: Any) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {"configured": False}
    return {"configured": True,
            "destination": destination_summary(config.get("destinationPath")),
            "serverNameConfigured": bool(config.get("serverName")),
            "azureCredentialsConfigured": isinstance(config.get("azureCredentials"), dict),
            "secretSelectors": secret_selectors(config),
            "walCompression": config.get("wal", {}).get("compression")
                if isinstance(config.get("wal"), dict) else None,
            "dataCompression": config.get("data", {}).get("compression")
                if isinstance(config.get("data"), dict) else None}


def cluster_archive_source_summary(cluster: dict[str, Any]) -> dict[str, Any]:
    spec = cluster.get("spec") or {}
    backup = spec.get("backup") or {}
    bootstrap = spec.get("bootstrap") or {}
    recovery = bootstrap.get("recovery") or {}
    external = []
    for source in spec.get("externalClusters") or []:
        if not isinstance(source, dict):
            continue
        barman = source.get("barmanObjectStore")
        external.append({"name": source.get("name"), "barmanObjectStore": barman_backend_summary(barman),
                         "secretSelectors": secret_selectors(source)})
    plugins = []
    for plugin in spec.get("plugins") or []:
        if not isinstance(plugin, dict):
            continue
        parameters = plugin.get("parameters") or {}
        plugins.append({"name": plugin.get("name"), "isWALArchiver": plugin.get("isWALArchiver"),
                        "barmanObjectName": parameters.get("barmanObjectName")
                            if isinstance(parameters, dict) else None,
                        "serverNameConfigured": bool(parameters.get("serverName"))
                            if isinstance(parameters, dict) else False})
    return {"capturedAtUtc": iso(),
            "cluster": {"name": cluster.get("metadata", {}).get("name"),
                        "namespace": cluster.get("metadata", {}).get("namespace"),
                        "uid": cluster.get("metadata", {}).get("uid"),
                        "generation": cluster.get("metadata", {}).get("generation")},
            "backup": {"configured": bool(backup), "method": backup.get("method"),
                       "online": backup.get("online"),
                       "barmanObjectStore": barman_backend_summary(backup.get("barmanObjectStore")),
                       "secretSelectors": secret_selectors(backup)},
            "bootstrap": {"recoveryConfigured": bool(recovery),
                          "recoverySourceNames": [item.get("name") for item in (recovery.get("source") or [])
                                                  if isinstance(item, dict)],
                          "pgBasebackupConfigured": bool(bootstrap.get("pg_basebackup"))},
            "externalClusters": external, "plugins": plugins,
            "rawClusterSpecWritten": False}


def optional_cnpg_list(kubectl: list[str], namespace: str, resource: str) -> dict[str, Any]:
    result = command(kubectl + ["get", resource, "-n", namespace, "-o", "json"], timeout=30)
    if result.returncode:
        return {"apiAvailable": False, "count": None, "error": "read failed; raw kubectl output omitted"}
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"apiAvailable": True, "count": None, "error": "response was not JSON"}
    items = payload.get("items") or []
    if resource == "objectstores.barmancloud.cnpg.io":
        rows = []
        for item in items:
            spec = item.get("spec") or {}
            rows.append({"name": item.get("metadata", {}).get("name"),
                         "destination": destination_summary(spec.get("configuration", {}).get("destinationPath")),
                         "secretSelectors": secret_selectors(spec),
                         "conditionTypes": [x.get("type") for x in
                                            (item.get("status", {}).get("conditions") or [])
                                            if isinstance(x, dict)]})
    else:
        rows = [{"name": item.get("metadata", {}).get("name"),
                 "phase": item.get("status", {}).get("phase"),
                 "method": item.get("spec", {}).get("method"),
                 "backupName": item.get("status", {}).get("backupName")}
                for item in items]
    return {"apiAvailable": True, "count": len(rows), "items": rows}


def safe_setup_receipts(state_dir: Path | None) -> dict[str, Any]:
    """Copy only approved, non-secret fields from the archive prep receipts."""
    if state_dir is None:
        return {"available": False, "reason": "state directory not resolved"}
    archive_path = state_dir / "wal-archive.json"
    smoke_path = state_dir / "wal-archive-smoke.json"
    if not archive_path.exists() and not smoke_path.exists():
        return {"available": False, "reason": "no archive setup receipts at collection start"}
    result: dict[str, Any] = {"available": True, "secretValuesCopied": False,
                              "ownerTokenCopied": False, "snapshotIdsCopied": False}
    archive: dict[str, Any] = {}
    if archive_path.is_file():
        try:
            raw = json.loads(archive_path.read_text(encoding="utf-8"))
            secret_ref = raw.get("secretReference") if isinstance(raw, dict) else None
            if not isinstance(secret_ref, dict):
                secret_ref = {}
            lineage = raw.get("snapshotLineage") if isinstance(raw, dict) else None
            lock_check = raw.get("sourceLockVerification") if isinstance(raw, dict) else None
            smoke_ref = raw.get("smokeReceipt") if isinstance(raw, dict) else None
            archive = {"status": raw.get("status"), "backend": raw.get("backend"),
                       "destination": destination_summary(raw.get("destinationPath")),
                       "serverName": raw.get("serverName"),
                       "postgresImage": raw.get("postgresImage"),
                       "secretReference": {k: secret_ref[k] for k in ("name", "key")
                                           if isinstance(secret_ref.get(k), str)},
                       "sourceSnapshotCount": len(raw.get("sourceSnapshotIds") or [])
                           if isinstance(raw.get("sourceSnapshotIds"), list) else None,
                       "snapshotLineagePresent": bool(lineage),
                       "sourceLockVerificationPresent": bool(lock_check),
                       "smokeReceiptConfigured": bool(smoke_ref)}
            result["archiveReceipt"] = archive
        except (OSError, json.JSONDecodeError, AttributeError, TypeError):
            result["archiveReceipt"] = {"available": False, "error": "receipt parse failed"}
    else:
        result["archiveReceipt"] = {"available": False, "reason": "file absent"}

    if smoke_path.is_file():
        try:
            raw = json.loads(smoke_path.read_text(encoding="utf-8"))
            hashes = raw.get("hashes") if isinstance(raw, dict) else {}
            if not isinstance(hashes, dict):
                hashes = {}
            allowed_hashes = ("closedPrimarySegmentSha256", "independentAzureDownloadSha256",
                              "cnpgManagerWalRestoreSha256")
            safe_hashes = {key: hashes[key] for key in allowed_hashes
                           if isinstance(hashes.get(key), str) and
                           re.fullmatch(r"[0-9a-fA-F]{64}", hashes[key])}
            checks = raw.get("checks") if isinstance(raw, dict) else {}
            if not isinstance(checks, dict):
                checks = {}
            archiver = raw.get("archiver") if isinstance(raw, dict) else {}
            if not isinstance(archiver, dict):
                archiver = {}
            safe_archiver = {key: archiver[key] for key in
                ("archived_count", "failed_count", "last_archived_wal", "last_archived_time", "stats_reset")
                if key in archiver}
            wal_name = raw.get("walName") if isinstance(raw, dict) else None
            if not isinstance(wal_name, str) or not re.fullmatch(r"[0-9A-Fa-f]{24}(?:\.partial)?", wal_name):
                wal_name = None
            result["smokeReceipt"] = {"status": raw.get("status"), "walName": wal_name,
                "archiveObject": destination_summary(raw.get("archiveObject")),
                "bytes": raw.get("bytes") if isinstance(raw.get("bytes"), int) and raw.get("bytes") >= 0 else None,
                "hashes": safe_hashes,
                "hashesAllEqual": len(safe_hashes) == 3 and len(set(safe_hashes.values())) == 1,
                "archiver": safe_archiver,
                "checks": {str(key): value for key, value in checks.items() if isinstance(value, bool)}}
        except (OSError, json.JSONDecodeError, AttributeError, TypeError):
            result["smokeReceipt"] = {"available": False, "error": "receipt parse failed"}
    else:
        result["smokeReceipt"] = {"available": False, "reason": "file absent"}
    return result


def capture_archive_source_receipt(kubectl: list[str], namespace: str, out: Path,
                                   state_dir: Path | None = None) -> dict[str, Any]:
    receipt: dict[str, Any] = {"capturedAtUtc": iso(), "namespace": namespace,
                               "safety": {"secretValuesQueried": False,
                                          "rawClusterSpecWritten": False,
                                          "commandValuesWritten": False}}
    result = command(kubectl + ["get", "cluster", "-n", namespace, "-o", "json"], timeout=30)
    if result.returncode:
        receipt["clusterRead"] = {"available": False, "error": "read failed; raw kubectl output omitted"}
    else:
        try:
            cluster = json.loads(result.stdout)
            receipt["clusterRead"] = {"available": True,
                                      "archiveSource": cluster_archive_source_summary(cluster)}
        except json.JSONDecodeError:
            receipt["clusterRead"] = {"available": False, "error": "response was not JSON"}
    receipt["resources"] = {
        "backups": optional_cnpg_list(kubectl, namespace, "backups.postgresql.cnpg.io"),
        "scheduledBackups": optional_cnpg_list(kubectl, namespace, "scheduledbackups.postgresql.cnpg.io"),
        "objectStores": optional_cnpg_list(kubectl, namespace, "objectstores.barmancloud.cnpg.io"),
    }
    receipt["setupProof"] = safe_setup_receipts(state_dir)
    atomic_json(out / "archive-source-receipt.json", receipt)
    return receipt


def command(argv: list[str], timeout: float = 60, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, input=input_text, text=True, capture_output=True, timeout=timeout, check=False)


def check(argv: list[str], timeout: float = 60, input_text: str | None = None) -> str:
    result = command(argv, timeout, input_text)
    if result.returncode:
        raise RuntimeError(f"{shlex.join(argv)} exited {result.returncode}: {(result.stderr or result.stdout)[-2500:]}")
    return result.stdout


def parse_bcc_json(payload: str) -> dict[str, Any] | None:
    start = payload.find("{")
    if start < 0:
        return None
    raw = payload[start:]
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        try:
            value = ast.literal_eval(raw)
        except (ValueError, SyntaxError):
            return None
    return value if isinstance(value, dict) else None


def parse_histogram_device(value: dict[str, Any]) -> str | None:
    for key in ("disk", "device", "bucket", "Bucket ptr"):
        item = value.get(key)
        if isinstance(item, bytes):
            item = item.decode("utf-8", "replace")
        if isinstance(item, str) and item:
            return os.path.basename(item.strip())
    return None


def operation_from_flags(value: Any) -> str:
    """Linux REQ_OP values: 0=READ and 1=WRITE; retain unknown values verbatim."""
    try:
        flags = int(value)
    except (TypeError, ValueError):
        return "unknown"
    operation = flags & 0xFF
    return {0: "read", 1: "write"}.get(operation, f"op-{operation}")


def parse_devices(output: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for line in output.splitlines():
        fields = line.split("|")
        if len(fields) != 14 or fields[0] not in ("data", "wal"):
            continue
        (kind, mount_mm, disk_mm, device, path, controller_type, controller,
         driver, controller_path, driver_chain, hctl, queue_depth, scheduler,
         nr_requests) = fields
        result[kind] = {
            "majorMinor": disk_mm, "mountMajorMinor": mount_mm,
            "device": device, "path": path,
            "controller": {"type": controller_type, "name": controller,
                           "driver": driver, "path": controller_path,
                           "driverChain": driver_chain},
            "scsi": {"hctl": hctl or None,
                     "queueDepth": int(queue_depth) if queue_depth.isdigit() else None},
            "queue": {"scheduler": scheduler,
                      "nrRequests": int(nr_requests) if nr_requests.isdigit() else None},
        }
    if set(result) != {"data", "wal"}:
        raise RuntimeError(f"Could not resolve both PostgreSQL DATA/WAL block devices: {output[-1500:]}")
    for kind, item in result.items():
        if not re.fullmatch(r"\d+:\d+", item["majorMinor"]):
            raise RuntimeError(f"{kind} has invalid MAJ:MIN {item['majorMinor']!r}")
        if item["controller"]["type"] not in ("nvme", "storvsc"):
            raise RuntimeError(f"{kind} controller is not a supported Azure controller: {item['controller']}")
        if not item["controller"]["driver"] or not item["controller"]["path"]:
            raise RuntimeError(f"{kind} controller binding is incomplete: {item['controller']}")
    if result["data"]["majorMinor"] == result["wal"]["majorMinor"]:
        raise RuntimeError("DATA and WAL resolve to the same block device")
    return result


DEVICE_DISCOVERY = r'''set -eu
data=$(printenv PGDATA 2>/dev/null || true); [ -n "$data" ] || data=/var/lib/postgresql/data/pgdata
data=$(readlink -f "$data"); wal=$(readlink -f "$data/pg_wal")
for kind in data wal; do
 if [ "$kind" = data ]; then p=$data; else p=$wal; fi
 mountmm=$(findmnt -n -r -T "$p" -o MAJ:MIN | tr -d '[:space:]')
 [ -n "$mountmm" ] || { echo "$kind mount has no MAJ:MIN" >&2; exit 1; }
 sys=$(readlink -f "/sys/dev/block/$mountmm")
 while [ -f "$sys/partition" ]; do sys=$(dirname "$sys"); done
 diskmm=$(cat "$sys/dev"); name=$(basename "$sys")
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
   [ -n "$controller_path" ] && [ "$nvme_bound" = 1 ] || { echo "NVMe binding missing for $name" >&2; exit 1; }
   controller_type=nvme; controller_driver=nvme
   ;;
  *)
   if [ -n "$storvsc_path" ]; then controller_type=storvsc; controller=hv_storvsc; controller_driver=hv_storvsc; controller_path=$storvsc_path; fi
   ;;
 esac
 hctl=''; qdepth=''; scsi_device=$(readlink -f "$sys/device" 2>/dev/null || true)
 if [ -n "$scsi_device" ] && [ -r "$scsi_device/queue_depth" ]; then
  candidate=$(basename "$scsi_device")
  case "$candidate" in *:*:*:*) hctl=$candidate; qdepth=$(cat "$scsi_device/queue_depth");; esac
 fi
 queue="/sys/class/block/$name/queue"; scheduler=''; nr_requests=''
 [ ! -r "$queue/scheduler" ] || scheduler=$(cat "$queue/scheduler")
 [ ! -r "$queue/nr_requests" ] || nr_requests=$(cat "$queue/nr_requests")
 printf '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' \
  "$kind" "$mountmm" "$diskmm" "$name" "$p" "$controller_type" "$controller" "$controller_driver" \
  "$controller_path" "${driver_chain:-none}" "$hctl" "$qdepth" "$scheduler" "$nr_requests"
done
'''


def build_bcc_pod(name: str, namespace: str, node: str, image: str,
                  data_device: str, wal_device: str) -> dict[str, Any]:
    # Device histograms identify the block device. Filtered flag histograms
    # provide read/write operation buckets for each exact DATA/WAL device.
    # If biolatency cannot attach its flag kprobe, the optional tracepoint
    # helper preserves per-device read/write latency without blocking readiness.
    tracepoint_source = Path(__file__).with_name("bcc_rw_tracepoints.py").read_bytes()
    tracepoint_b64 = base64.b64encode(tracepoint_source).decode("ascii")
    command_text = r'''set -eu
export DEBIAN_FRONTEND=noninteractive PYTHONUNBUFFERED=1
mount_pseudo_fs() {
 fstype=$1; target=$2
 current=$(awk -v target="$target" '$2 == target { print $3; exit }' /proc/mounts)
 if [ "$current" != "$fstype" ]; then mount -t "$fstype" "$fstype" "$target"; fi
 [ "$(awk -v target="$target" '$2 == target { print $3; exit }' /proc/mounts)" = "$fstype" ]
}
mount_pseudo_fs debugfs /sys/kernel/debug
mount_pseudo_fs tracefs /sys/kernel/tracing
if [ -d /sys/kernel/debug/tracing ] && [ ! -r /sys/kernel/debug/tracing/available_filter_functions ]; then
 mount --bind /sys/kernel/tracing /sys/kernel/debug/tracing
fi
apt-get update -qq
apt-get install -y --no-install-recommends bpfcc-tools python3-bpfcc
printf '%s' '__BPF_RW_TRACEPOINTS_B64__' | base64 -d >/tmp/bcc_rw_tracepoints.py
for disk in __DATA__ __WAL__; do
 [ -b "/host-dev/$disk" ] || { echo "__BCC_RW_UNAVAILABLE__ host block device missing: $disk"; continue; }
 ln -sf "/host-dev/$disk" "/dev/$disk"
done
BIO=$(command -v biolatency-bpfcc || command -v biolatency || true)
: >/tmp/bcc-device.log
: >/tmp/bcc-rw-data.log
: >/tmp/bcc-rw-wal.log
: >/tmp/bcc-rw-tracepoint.log
pdev=0; prd=0; prw=0; ptp=0; need_tracepoint=0
if [ -z "$BIO" ]; then
 echo '__BCC_UNAVAILABLE__ biolatency missing; continuing with tracepoint fallback' >>/tmp/bcc-device.log
 echo '__BCC_RW_DATA_UNAVAILABLE__ biolatency missing' >>/tmp/bcc-rw-data.log
 echo '__BCC_RW_WAL_UNAVAILABLE__ biolatency missing' >>/tmp/bcc-rw-wal.log
 need_tracepoint=1
else
 HELP=$("$BIO" --help 2>&1 || true)
 if printf '%s\n' "$HELP" | grep -Eq -- '(^|[[:space:]])-D([,[:space:]]|$)'; then
  "$BIO" -mTD -j 1 >/tmp/bcc-device.log 2>&1 & pdev=$!
 else
  echo '__BCC_UNAVAILABLE__ biolatency lacks -D; per-device histogram unavailable' >>/tmp/bcc-device.log
 fi
 if printf '%s\n' "$HELP" | grep -Eq -- '(^|[[:space:]])-F([,[:space:]]|$)'; then
  "$BIO" -mTF -j 1 -d __DATA__ >/tmp/bcc-rw-data.log 2>&1 & prd=$!
  "$BIO" -mTF -j 1 -d __WAL__ >/tmp/bcc-rw-wal.log 2>&1 & prw=$!
 else
  echo '__BCC_RW_DATA_UNAVAILABLE__ biolatency lacks -F' >>/tmp/bcc-rw-data.log
  echo '__BCC_RW_WAL_UNAVAILABLE__ biolatency lacks -F' >>/tmp/bcc-rw-wal.log
  need_tracepoint=1
 fi
fi
sleep 3
if [ "$pdev" -gt 0 ] && ! kill -0 "$pdev" 2>/dev/null; then
 echo '__BCC_UNAVAILABLE__ per-device histogram exited' >>/tmp/bcc-device.log
fi
if [ "$prd" -eq 0 ] || ! kill -0 "$prd" 2>/dev/null; then
 echo '__BCC_RW_DATA_UNAVAILABLE__ filtered flag histogram exited' >>/tmp/bcc-rw-data.log
 need_tracepoint=1
fi
if [ "$prw" -eq 0 ] || ! kill -0 "$prw" 2>/dev/null; then
 echo '__BCC_RW_WAL_UNAVAILABLE__ filtered flag histogram exited' >>/tmp/bcc-rw-wal.log
 need_tracepoint=1
fi
proc_running() {
 pid=$1
 [ "$pid" -gt 0 ] 2>/dev/null || return 1
 [ -r "/proc/$pid/stat" ] || return 1
 stat_line=$(cat "/proc/$pid/stat" 2>/dev/null) || return 1
 state=${stat_line##*) }
 state=${state%% *}
 [ -n "$state" ] && [ "$state" != Z ] && [ "$state" != X ]
}
start_tracepoint_fallback() {
 if ! command -v python3 >/dev/null 2>&1 || ! command -v flock >/dev/null 2>&1 || \
    [ ! -b "/dev/__DATA__" ] || [ ! -b "/dev/__WAL__" ]; then
  echo '__BCC_RW_TP_UNAVAILABLE__ python, flock, or verified DATA/WAL device paths unavailable' >>/tmp/bcc-rw-tracepoint.log
  return
 fi
 # -F keeps the lock attached to the exact helper PID. This makes startup and
 # delayed-failure paths mutually exclusive and leaves a PID receipt for harvest.
 flock -n -F /tmp/bcc-rw-tracepoint-live.lock sh -c '
  echo "$$" >/tmp/bcc-rw-tracepoint-live.pid
  exec python3 /tmp/bcc_rw_tracepoints.py --data-device "/dev/__DATA__" --wal-device "/dev/__WAL__" --interval 1
 ' >>/tmp/bcc-rw-tracepoint.log 2>&1 </dev/null & ptp=$!
 ready=0; attempt=0
 while [ "$attempt" -lt 8 ]; do
  if grep -q '"event": "ready"' /tmp/bcc-rw-tracepoint.log; then ready=1; break; fi
  if ! proc_running "$ptp"; then break; fi
  sleep 0.5; attempt=$((attempt + 1))
 done
 if [ "$ready" -ne 1 ] && ! proc_running "$ptp"; then
  echo '__BCC_RW_TP_UNAVAILABLE__ tracepoint helper exited before readiness' >>/tmp/bcc-rw-tracepoint.log
 elif [ "$ready" -ne 1 ]; then
  echo '__BCC_RW_TP_STARTING__ tracepoint helper launched; readiness may arrive after BCC compilation' >>/tmp/bcc-rw-tracepoint.log
 fi
}
if [ "$need_tracepoint" -eq 1 ]; then
 start_tracepoint_fallback
else
 # BCC can spend tens of seconds compiling -F before discovering that the
 # kernel lacks its probe symbol. Watch both children past the initial grace
 # period so a late failure still starts the optional tracepoint collector.
 (
  while :; do
   failed=0
   if [ "$prd" -gt 0 ] && ! proc_running "$prd"; then
    echo '__BCC_RW_DATA_UNAVAILABLE__ delayed -F process exit; starting tracepoint fallback' >>/tmp/bcc-rw-data.log
    failed=1
   fi
   if [ "$prw" -gt 0 ] && ! proc_running "$prw"; then
    echo '__BCC_RW_WAL_UNAVAILABLE__ delayed -F process exit; starting tracepoint fallback' >>/tmp/bcc-rw-wal.log
    failed=1
   fi
   if [ "$failed" -eq 1 ]; then start_tracepoint_fallback; break; fi
   sleep 1
  done
 ) &
fi
echo '__BCC_STARTED__'
tail -n +1 -F /tmp/bcc-device.log 2>/dev/null | sed -u 's/^/__BCC_DEVICE__/' & tdev=$!
tail -n +1 -F /tmp/bcc-rw-data.log 2>/dev/null | sed -u 's/^/__BCC_RW_DATA__/' & trd=$!
tail -n +1 -F /tmp/bcc-rw-wal.log 2>/dev/null | sed -u 's/^/__BCC_RW_WAL__/' & trw=$!
tail -n +1 -F /tmp/bcc-rw-tracepoint.log 2>/dev/null | sed -u 's/^/__BCC_RW_TP__/' & ttp=$!
wait || true
'''.replace("__DATA__", data_device).replace("__WAL__", wal_device).replace(
        "__BPF_RW_TRACEPOINTS_B64__", tracepoint_b64)
    return {
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace,
                     "labels": {"app.kubernetes.io/name": "wal-repro-bcc", "wal-repro/run": name}},
        "spec": {"nodeName": node, "restartPolicy": "Never", "hostPID": True, "hostNetwork": True,
                 "dnsPolicy": "ClusterFirstWithHostNet",
                 "containers": [{"name": "bcc", "image": image, "imagePullPolicy": "IfNotPresent",
                                 "command": ["/bin/sh", "-c"], "args": [command_text],
                                 "resources": POD_RESOURCES,
                                 "securityContext": {"privileged": True, "runAsUser": 0},
                                 "volumeMounts": [
                                     {"name": "host-dev", "mountPath": "/host-dev", "readOnly": True},
                                     {"name": "modules", "mountPath": "/lib/modules", "readOnly": True},
                                     {"name": "src", "mountPath": "/usr/src", "readOnly": True},
                                     {"name": "debugfs", "mountPath": "/sys/kernel/debug"},
                                     {"name": "tracefs", "mountPath": "/sys/kernel/tracing"},
                                 ]}],
                 "volumes": [
                     {"name": "host-dev", "hostPath": {"path": "/dev", "type": "Directory"}},
                     {"name": "modules", "hostPath": {"path": "/lib/modules", "type": "Directory"}},
                     {"name": "src", "hostPath": {"path": "/usr/src", "type": "Directory"}},
                     {"name": "debugfs", "hostPath": {"path": "/sys/kernel/debug", "type": "Directory"}},
                     {"name": "tracefs", "hostPath": {"path": "/sys/kernel/tracing", "type": "Directory"}},
                 ]},
    }


def sql_identifier_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def safe_command_setting_sql(setting_name: str) -> str:
    """Return command metadata without returning any part of its raw value."""
    name = sql_identifier_text(setting_name)
    return f'''(SELECT json_build_object(
   'configured', setting <> '',
   'executableBasename', CASE WHEN setting = '' THEN NULL ELSE
     regexp_replace(split_part(ltrim(setting), ' ', 1), '^.*/', '') END,
   'recognizedSubcommand', CASE
     WHEN setting ~* '(^|[[:space:]/])wal-archive([[:space:]]|$)' THEN 'wal-archive'
     WHEN setting ~* '(^|[[:space:]/])wal-restore([[:space:]]|$)' THEN 'wal-restore'
     ELSE NULL END,
   'mentionsAzure', setting ~* '(azure|blob\\.core\\.windows\\.net|storageaccount|storagekey)',
   'mentionsBarman', setting ~* 'barman',
   'mentionsPgBackRest', setting ~* 'pgbackrest',
   'mentionsS3', setting ~* '(s3://|aws|amazonaws)',
   'mentionsGcs', setting ~* '(gs://|googleapis|gcs)',
   'source', source, 'sourceFile', sourcefile, 'sourceLine', sourceline,
   'pendingRestart', pending_restart)
   FROM pg_settings WHERE name = {name})'''


def make_sql(client_app: str, has_io: bool, has_prefetch: bool, has_checkpointer: bool) -> str:
    app = sql_identifier_text(client_app)
    io_sql = "(SELECT coalesce(json_agg(to_jsonb(i)), '[]'::json) FROM pg_stat_io i)" if has_io else "NULL::json"
    prefetch_sql = "(SELECT to_jsonb(p) FROM pg_stat_recovery_prefetch p)" if has_prefetch else "NULL::json"
    checkpointer_sql = "(SELECT to_jsonb(c) FROM pg_stat_checkpointer c)" if has_checkpointer else "NULL::json"
    archive_command_sql = safe_command_setting_sql("archive_command")
    restore_command_sql = safe_command_setting_sql("restore_command")
    archive_library_sql = safe_command_setting_sql("archive_library")
    return f'''SELECT json_build_object(
 'sampledAt', clock_timestamp(),
 'role', CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'primary' END,
 'isReplica', pg_is_in_recovery(),
 'replayLsn', pg_last_wal_replay_lsn(),
 'flushLsn', CASE WHEN pg_is_in_recovery() THEN NULL ELSE pg_current_wal_flush_lsn() END,
 'walReceiverWait', coalesce((SELECT json_agg(json_build_object(
   'pid',pid,'backendType',backend_type,'state',state,'waitEventType',wait_event_type,
   'waitEvent',wait_event,'backendStart',backend_start,'stateChange',state_change)
   ORDER BY pid) FROM pg_stat_activity WHERE backend_type='walreceiver'),'[]'::json),
 'activityWaits', coalesce((SELECT json_agg(json_build_object(
   'pid',pid,'backendType',backend_type,'applicationName',application_name,'state',state,
   'waitEventType',wait_event_type,'waitEvent',wait_event,'queryStart',query_start,
   'xactStart',xact_start,'stateChange',state_change,'blockingPids',pg_blocking_pids(pid),
   'query',CASE WHEN query ~* '(password|passfile|conninfo|sharedaccesssignature|accountkey)' THEN '[credential-bearing query omitted]' ELSE left(query,512) END) ORDER BY pid)
   FROM pg_stat_activity WHERE application_name LIKE {app} || '%' OR backend_type IN
   ('walreceiver','walsender','startup','checkpointer','background writer','walwriter')),'[]'::json),
 'walReceiver', (SELECT to_jsonb(w) - 'conninfo' || jsonb_build_object(
   'conninfoRedacted', CASE WHEN w.conninfo IS NULL THEN NULL ELSE '[omitted; may contain credentials]' END)
   FROM pg_stat_wal_receiver w LIMIT 1),
 'replicationSenders', coalesce((SELECT json_agg(to_jsonb(r) ORDER BY r.pid) FROM pg_stat_replication r),'[]'::json),
 'replicationSlots', coalesce((SELECT json_agg(to_jsonb(s) ORDER BY s.slot_name) FROM pg_replication_slots s),'[]'::json),
 'vacuumProgress', (SELECT json_build_object('pid',p.pid,'phase',p.phase,'heapBlksTotal',p.heap_blks_total,
   'heapBlksScanned',p.heap_blks_scanned,'heapBlksVacuumed',p.heap_blks_vacuumed,
   'indexVacuumCount',p.index_vacuum_count,'numDeadTuples',p.num_dead_tuples,
   'maxDeadTuples',p.max_dead_tuples,'backendState',a.state,
   'waitEventType',a.wait_event_type,'waitEvent',a.wait_event)
   FROM pg_stat_progress_vacuum p JOIN pg_stat_activity a USING(pid) LIMIT 1),
 'walStats', (SELECT to_jsonb(w) FROM pg_stat_wal w),
 'archiverStats', (SELECT to_jsonb(a) FROM pg_stat_archiver a),
 'archiveConfiguration', json_build_object(
   'archiveMode', (SELECT json_build_object('setting',setting,'source',source,
     'sourceFile',sourcefile,'sourceLine',sourceline,'pendingRestart',pending_restart)
     FROM pg_settings WHERE name='archive_mode'),
   'archiveCommand', {archive_command_sql},
   'restoreCommand', {restore_command_sql},
   'archiveLibrary', {archive_library_sql}),
 'databaseStats', (SELECT to_jsonb(d) FROM pg_stat_database d WHERE d.datname=current_database()),
 'settings', (SELECT json_object_agg(name, json_build_object(
   'setting',setting,'unit',unit,'source',source,'pendingRestart',pending_restart))
   FROM pg_settings WHERE name IN ('synchronous_standby_names','synchronous_commit',
   'wal_sender_timeout','wal_receiver_timeout')),
 'bgwriterStats', (SELECT to_jsonb(b) FROM pg_stat_bgwriter b),
 'checkpointerStats', {checkpointer_sql},
 'pgStatIO', {io_sql},
 'recoveryPrefetch', {prefetch_sql}
)::text;
'''


class Target:
    def __init__(self, role: str, metadata: dict[str, Any]):
        self.role = role
        self.metadata = metadata
        self.pod = str(metadata["pod"])
        self.node = str(metadata["node"])
        self.vm_id = str(metadata["vmResourceId"]).removeprefix("azure://")
        self.pod_json: dict[str, Any] = {}
        self.devices: dict[str, dict[str, Any]] = {}
        self.has_io = False
        self.has_prefetch = False
        self.has_checkpointer = False
        self.sql_app = f"wal-repro-{role}-watch-" + uuid.uuid4().hex[:8]
        self.bcc_pod: str | None = None
        self.bcc_ready = False
        self.bcc_started_event = threading.Event()
        self.bcc_error: str | None = None
        self.bcc_gaps: list[str] = []
        self.bcc_warnings: list[str] = []
        self.bcc_tracepoint_ready = False
        self.bcc_tracepoint_error: str | None = None
        self.bcc_tracepoint_gaps: list[str] = []
        self.bcc_tracepoint_pair_counters = {
            "starts": 0, "completions": 0, "matchedCompletions": 0,
            "keyCollisions": 0, "unmatchedCompletions": 0, "mapUpdateFailures": 0,
        }
        self.bcc_stream_last_timestamp: dt.datetime | None = None
        self.bcc_stream_last_timestamp_text: str | None = None
        self.bcc_stream_seen_lines: set[str] = set()
        self.bcc_stream_reconnects = 0
        self.bcc_stream_segments = 0
        self.bcc_stream_duplicates = 0
        self.bcc_stream_timestamp_errors = 0
        self.bcc_stream_gaps: list[dict[str, Any]] = []
        self.bcc_current_log_timestamp: str | None = None
        self.bcc_current_log_segment: int | None = None
        self.sql_ok = 0
        self.sql_errors = 0
        self.disk_ok = 0
        self.disk_errors = 0
        self.host_ok = 0
        self.host_errors = 0
        self.sql_times: list[str] = []
        self.disk_times: list[str] = []
        self.host_times: list[str] = []
        self.host_pressure_resources: set[str] = set()
        self.sql_proc: subprocess.Popen[str] | None = None
        self.disk_proc: subprocess.Popen[str] | None = None
        self.bcc_proc: subprocess.Popen[str] | None = None
        self.bcc_threads: list[threading.Thread] = []
        self.bcc_samples = {"device": 0, "rw": 0,
                            "rwTracepointIntervals": 0, "rwTracepointBuckets": 0,
                            "rwTracepointCompletions": 0}
        self.bcc_unparsed = 0
        self.bcc_devices: set[str] = set()
        self.sql_first_sample = False
        self.disk_first_sample = False
        self.stream_reconnects = {"postgres": 0, "diskstats": 0}
        self.stream_gaps: list[dict[str, Any]] = []


class Collector:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.out = Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=True)
        self.kubectl = [args.kubectl] + (["--kubeconfig", args.kubeconfig] if args.kubeconfig else [])
        self.stop_event = threading.Event()
        self.ready_events: dict[str, threading.Event] = {r: threading.Event() for r in ("primary", "replica")}
        self.ready_lock = threading.Lock()
        self.fatal_errors: list[str] = []
        self.targets: dict[str, Target] = {}
        manifest = json.loads(Path(args.verified_attachments).read_text(encoding="utf-8"))
        archive_receipt_path = archive_receipt_path_for(args.verified_attachments)
        archive_receipt: dict[str, Any] | None = None
        try:
            raw_archive_receipt = json.loads(archive_receipt_path.read_text(encoding="utf-8"))
            if isinstance(raw_archive_receipt, dict):
                # The Azure collector consumes only the ready state and Azure
                # resource ID. This local receipt contains other ownership and
                # lineage fields; none are copied into metric artifacts.
                archive_receipt = {"status": raw_archive_receipt.get("status"),
                                   "subscriptionId": raw_archive_receipt.get("subscriptionId"),
                                   "accountId": raw_archive_receipt.get("accountId")}
        except (OSError, json.JSONDecodeError):
            archive_receipt = None
        azure_subscription = manifest.get("subscriptionId") or (archive_receipt or {}).get("subscriptionId")
        self.azure = AzureMetrics(manifest, self.out / "azure", args.az, args.azure_timeout,
                                  subscription=azure_subscription, archive_receipt=archive_receipt)
        roles = manifest.get("roles") or {}
        if set(roles) != {"primary", "replica"}:
            raise RuntimeError("verified-attachments must contain exactly primary and replica roles")
        for role in ("primary", "replica"):
            self.targets[role] = Target(role, roles[role])
        self.threads: list[threading.Thread] = []
        self.started = dt.datetime.now(UTC)
        self.stopped: dt.datetime | None = None
        self.was_ready = False
        self.azure_readiness_proof: dict[str, Any] | None = None
        prior = self.out.parent / "attempts" / "instrumentation-segment1"
        prior_status = None
        if prior.exists():
            try:
                prior_status = json.loads((prior / "collector-status.json").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        self.prior_segment_gap = None
        if prior_status:
            self.prior_segment_gap = {"path": str(prior), "status": prior_status.get("phase"),
                "segmentStoppedAtUtc": prior_status.get("stoppedAtUtc"),
                "recoverySegmentStartedAtUtc": iso(),
                "reason": "previous long-lived kubectl exec streams were reset by peer"}
            append_jsonl(self.out / "stream-gaps.jsonl", {"stream": "all-required",
                "capturedAtUtc": iso(), **self.prior_segment_gap})
        self.status_path = self.out / "collector-status.json"
        self.preflight_path = self.out / "preflight.json"

    def status(self, phase: str, **more: Any) -> None:
        atomic_json(self.status_path, {"updatedAtUtc": iso(), "phase": phase,
                                      "roles": {r: {"pod": t.pod, "node": t.node,
                                                    "bccPod": t.bcc_pod,
                                                    "bccReady": t.bcc_ready,
                                                    "bccError": t.bcc_error,
                                                    "bccGaps": t.bcc_gaps,
                                                    "bccWarnings": t.bcc_warnings,
                                                    "bccTracepointRWReady": t.bcc_tracepoint_ready,
                                                    "bccTracepointRWError": t.bcc_tracepoint_error,
                                                    "bccTracepointRWGaps": t.bcc_tracepoint_gaps,
                                                    "bccLogStreamReconnects": t.bcc_stream_reconnects,
                                                    "bccLogStreamSegments": t.bcc_stream_segments,
                                                    "bccLogStreamDuplicateLines": t.bcc_stream_duplicates}
                                                for r, t in self.targets.items()},
                                      "fatalErrors": list(self.fatal_errors), **more})

    def preflight_role(self, target: Target) -> None:
        raw = check(self.kubectl + ["get", "pod", "-n", self.args.namespace, target.pod, "-o", "json"])
        pod = json.loads(raw)
        target.pod_json = pod
        labels = pod.get("metadata", {}).get("labels", {})
        actual_node = pod.get("spec", {}).get("nodeName")
        if pod.get("status", {}).get("phase") != "Running":
            raise RuntimeError(f"{target.role} pod {target.pod} is not Running")
        if labels.get("cnpg.io/instanceRole") not in (None, target.role):
            raise RuntimeError(f"{target.pod} role label is {labels.get('cnpg.io/instanceRole')}, expected {target.role}")
        if actual_node != target.node:
            raise RuntimeError(f"verified {target.role} node {target.node} differs from pod node {actual_node}")
        node = json.loads(check(self.kubectl + ["get", "node", target.node, "-o", "json"]))
        provider_id = str(node.get("spec", {}).get("providerID", "")).removeprefix("azure://")
        if provider_id.rstrip("/").casefold() != target.vm_id.rstrip("/").casefold():
            raise RuntimeError(f"verified {target.role} VM resource ID does not match node providerID")
        raw_devices = check(self.kubectl + ["exec", "-c", "postgres", "-n", self.args.namespace,
                                           target.pod, "--", "sh", "-c", DEVICE_DISCOVERY])
        target.devices = parse_devices(raw_devices)
        for kind in ("data", "wal"):
            expected = (target.metadata.get("disks") or {}).get(kind) or {}
            hctl = target.devices[kind]["scsi"].get("hctl")
            if hctl and expected.get("lun") is not None and target.devices[kind]["device"].startswith("sd"):
                actual_lun = hctl.rsplit(":", 1)[-1]
                if str(actual_lun) != str(expected["lun"]):
                    raise RuntimeError(f"{target.role} {kind} SCSI sysfs LUN {actual_lun} != Azure attachment LUN {expected['lun']}")
        stats = check(self.kubectl + ["exec", "-c", "postgres", "-n", self.args.namespace,
                                     target.pod, "--", "cat", "/proc/diskstats"])
        wanted = {(tuple(x["majorMinor"].split(":")), x["device"]) for x in target.devices.values()}
        found = set()
        for line in stats.splitlines():
            fields = line.split()
            if len(fields) >= 3:
                found.add(((fields[0], fields[1]), fields[2]))
        missing = [x for x in wanted if x not in found]
        if missing:
            raise RuntimeError(f"{target.role} DATA/WAL devices missing from /proc/diskstats: {missing}")
        probe = json.loads(check(self.kubectl + ["exec", "-c", "postgres", "-n", self.args.namespace,
                                                target.pod, "--", "psql", "-X", "-A", "-t", "-q",
                                                "-U", "postgres", "-d", self.args.database, "-c",
                                                "SELECT json_build_object('isReplica',pg_is_in_recovery(),'serverVersionNum',current_setting('server_version_num')::int,'hasIO',to_regclass('pg_catalog.pg_stat_io') IS NOT NULL,'hasPrefetch',to_regclass('pg_catalog.pg_stat_recovery_prefetch') IS NOT NULL,'hasCheckpointer',to_regclass('pg_catalog.pg_stat_checkpointer') IS NOT NULL)::text;"]))
        if bool(probe.get("isReplica")) != (target.role == "replica"):
            raise RuntimeError(f"{target.role} pod's pg_is_in_recovery() disagrees with its expected role")
        target.has_io = bool(probe.get("hasIO"))
        target.has_prefetch = bool(probe.get("hasPrefetch"))
        target.has_checkpointer = bool(probe.get("hasCheckpointer"))
        target.sql_text = make_sql(self.args.client_app_name, target.has_io, target.has_prefetch,
                                   target.has_checkpointer)
        target.preflight = {"podUid": pod.get("metadata", {}).get("uid"), "node": actual_node,
                            "providerId": provider_id, "serverVersionNum": probe.get("serverVersionNum"),
                            "pgStatIOAvailable": target.has_io,
                            "recoveryPrefetchAvailable": target.has_prefetch,
                            "pgStatCheckpointerAvailable": target.has_checkpointer,
                            "devices": target.devices}

    def preflight(self) -> None:
        self.status("preflight")
        for target in self.targets.values():
            self.preflight_role(target)
        if self.targets["primary"].node == self.targets["replica"].node:
            raise RuntimeError("primary and replica are on the same Kubernetes node")
        azure_preflight = self.azure.discover()
        state_dir = (Path(self.args.kubeconfig).resolve().parent if self.args.kubeconfig else
                     Path.cwd() / ".state")
        capture_archive_source_receipt(self.kubectl, self.args.namespace, self.out, state_dir)
        # Preserve the exact attachment identities alongside node/devices.
        attachment = json.loads(Path(self.args.verified_attachments).read_text(encoding="utf-8"))
        atomic_json(self.preflight_path, {"phase": "ready", "startedAtUtc": iso(self.started),
                  "attachmentManifest": str(Path(self.args.verified_attachments).resolve()),
                  "attachmentIdentities": attachment, "roles": {r: t.preflight for r, t in self.targets.items()},
                  "azureDefinitionsInventory": azure_preflight,
                  "archiveSourceReceipt": str(self.out / "archive-source-receipt.json"),
                  "sampling": {"pgStatSeconds": 1, "diskstatsSeconds": 1, "azureNativeGrain": "PT1M"},
                  "bccHistograms": {"device": "biolatency -mTD -j 1",
                                    "rwPerDevice": "biolatency -mTF -j 1 -d DEVICE; flags decode Linux REQ_OP"}})

    def start_sql(self, target: Target) -> None:
        def consume() -> None:
            raw_path = self.out / f"{target.role}-pgstat.raw.log"
            rows_path = self.out / f"{target.role}-pgstat.jsonl"
            with raw_path.open("a", encoding="utf-8", buffering=1) as raw:
                while not self.stop_event.is_set():
                    argv = self.kubectl + ["exec", "-i", "-c", "postgres", "-n", self.args.namespace,
                                           target.pod, "--", "env", f"PGAPPNAME={target.sql_app}",
                                           "psql", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1",
                                           "-U", "postgres", "-d", self.args.database]
                    started = iso()
                    error = (self.out / f"{target.role}-pgstat-psql.stderr.log").open("ab", buffering=0)
                    try:
                        proc = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=error,
                                                text=True, bufsize=1)
                        target.sql_proc = proc
                        assert proc.stdin and proc.stdout
                        try:
                            proc.stdin.write(target.sql_text + "\n\\watch 1\n")
                            proc.stdin.close()
                        except (BrokenPipeError, OSError):
                            pass
                        for line in proc.stdout:
                            raw.write(line); raw.flush()
                            hit = line.find("{")
                            if hit < 0:
                                continue
                            try:
                                value = json.loads(line[hit:].strip())
                                if not isinstance(value, dict) or "sampledAt" not in value:
                                    continue
                            except json.JSONDecodeError:
                                append_jsonl(self.out / f"{target.role}-pgstat-unparsed.jsonl",
                                             {"capturedAtUtc": iso(), "line": line.rstrip()})
                                continue
                            captured = iso()
                            target.sql_ok += 1
                            target.sql_times.append(str(value["sampledAt"]))
                            target.sql_first_sample = True
                            self.mark_role_ready(target)
                            append_jsonl(rows_path, {"capturedAtUtc": captured, **value})
                        code = proc.wait()
                    except Exception as exc:
                        code = 255
                        append_jsonl(self.out / "stream-gaps.jsonl", {"capturedAtUtc": iso(),
                            "role": target.role, "stream": "postgres", "startedAtUtc": started,
                            "error": str(exc), "reconnecting": not self.stop_event.is_set()})
                    finally:
                        target.sql_proc = None
                        error.close()
                    if self.stop_event.is_set():
                        break
                    target.sql_errors += 1
                    target.stream_reconnects["postgres"] += 1
                    gap = {"role": target.role, "stream": "postgres", "startedAtUtc": started,
                           "reconnectingAtUtc": iso(), "exitCode": code,
                           "reason": "kubectl exec watcher stream ended; retrying"}
                    target.stream_gaps.append(gap)
                    append_jsonl(self.out / "stream-gaps.jsonl", gap)
                    self.status("collecting", role=target.role, stream="postgres",
                                reconnectCount=target.stream_reconnects["postgres"])
                    if self.stop_event.wait(min(5, 1 + target.stream_reconnects["postgres"])):
                        break

        thread = threading.Thread(target=consume, name=f"{target.role}-pgstat", daemon=True)
        thread.start(); self.threads.append(thread)

    def start_diskstats(self, target: Target) -> None:
        script = r'''set -eu
while :; do
 ts=$(date -u +'%Y-%m-%dT%H:%M:%S.%NZ')
 printf '__DISKSTATS__\t%s\t' "$ts"
 tr '\n' ';' </proc/diskstats
 printf '\n'
 cpu=$(awk '/^cpu / { printf "{\"user\":%s,\"nice\":%s,\"system\":%s,\"idle\":%s,\"iowait\":%s,\"irq\":%s,\"softirq\":%s,\"steal\":%s,\"guest\":%s,\"guestNice\":%s}",$2,$3,$4,$5,$6,$7,$8,$9,$10,$11 }' /proc/stat)
 mem=$(awk '/^(MemTotal|MemAvailable|MemFree|Buffers|Cached|SwapTotal|SwapFree|Dirty|Writeback|Slab):/ { key=$1; sub(/:$/, "", key); printf "%s\"%s\":{\"valueKiB\":%s,\"unit\":\"%s\"}",sep,key,$2,$3; sep="," }' /proc/meminfo)
 psi=''
 for resource in cpu memory io; do
  file="/proc/pressure/$resource"
  [ -r "$file" ] || continue
  rows=$(awk '{ if (NR>1) printf ","; printf "\"%s\":{",$1; for(i=2;i<=NF;i++){split($i,pair,"="); key=pair[1]; if(key=="total") key="totalUs"; if(i>2) printf ","; printf "\"%s\":%s",key,pair[2]}; printf "}" }' "$file")
  [ -z "$psi" ] || psi="$psi,"
  psi="$psi\"$resource\":{$rows}"
 done
 printf '__HOSTSTATS__\t%s\t%s\t{%s}\t{%s}\n' "$ts" "$cpu" "$mem" "$psi"
 sleep 1
done
'''
        def consume() -> None:
            raw_path = self.out / f"{target.role}-diskstats.raw.log"
            rows_path = self.out / f"{target.role}-diskstats.jsonl"
            with raw_path.open("a", encoding="utf-8", buffering=1) as raw:
                while not self.stop_event.is_set():
                    argv = self.kubectl + ["exec", "-i", "-c", "postgres", "-n", self.args.namespace,
                                           target.pod, "--", "sh", "-c", script]
                    started = iso()
                    error = (self.out / f"{target.role}-diskstats-kubectl.stderr.log").open("ab", buffering=0)
                    try:
                        proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=error, text=True, bufsize=1)
                        target.disk_proc = proc
                        assert proc.stdout
                        for line in proc.stdout:
                            raw.write(line); raw.flush()
                            if not line.startswith("__DISKSTATS__\t"):
                                if line.startswith("__HOSTSTATS__\t"):
                                    try:
                                        _, sampled, cpu, mem, pressure = line.rstrip("\n").split("\t", 4)
                                        parsed_pressure = json.loads(pressure)
                                        append_jsonl(self.out / f"{target.role}-hoststats.jsonl", {
                                            "sampledAtRemote": sampled, "capturedAtUtc": iso(),
                                            "cpu": json.loads(cpu), "meminfo": json.loads(mem),
                                            "pressure": parsed_pressure})
                                        target.host_ok += 1
                                        target.host_times.append(sampled)
                                        target.host_pressure_resources.update(parsed_pressure)
                                    except (ValueError, json.JSONDecodeError) as exc:
                                        target.host_errors += 1
                                        append_jsonl(self.out / f"{target.role}-hoststats-errors.jsonl", {
                                            "capturedAtUtc": iso(), "error": str(exc), "line": line.rstrip()})
                                    continue
                                append_jsonl(self.out / f"{target.role}-diskstats-unparsed.jsonl",
                                             {"capturedAtUtc": iso(), "line": line.rstrip()})
                                continue
                            try:
                                _, sampled, payload = line.rstrip("\n").split("\t", 2)
                                disks = []
                                for item in payload.split(";"):
                                    fields = item.split()
                                    if len(fields) >= 3:
                                        disks.append({"major": int(fields[0]), "minor": int(fields[1]),
                                                      "name": fields[2], "counters": [int(x) for x in fields[3:]],
                                                      "raw": item})
                                actual = {(str(x["major"]), str(x["minor"]), x["name"]) for x in disks}
                                missing = [(v["majorMinor"].split(":")[0], v["majorMinor"].split(":")[1], v["device"])
                                           for v in target.devices.values() if
                                           (v["majorMinor"].split(":")[0], v["majorMinor"].split(":")[1], v["device"]) not in actual]
                                if missing:
                                    raise ValueError(f"mapped PostgreSQL devices missing from diskstats: {missing}")
                                target.disk_ok += 1; target.disk_times.append(sampled)
                                target.disk_first_sample = True
                                self.mark_role_ready(target)
                                append_jsonl(rows_path, {"sampledAtRemote": sampled, "capturedAtUtc": iso(),
                                                        "devices": disks})
                            except (ValueError, TypeError) as exc:
                                target.disk_errors += 1
                                append_jsonl(self.out / f"{target.role}-diskstats-errors.jsonl",
                                             {"capturedAtUtc": iso(), "error": str(exc), "line": line.rstrip()})
                        code = proc.wait()
                    except Exception as exc:
                        code = 255
                        append_jsonl(self.out / "stream-gaps.jsonl", {"capturedAtUtc": iso(),
                            "role": target.role, "stream": "diskstats", "startedAtUtc": started,
                            "error": str(exc), "reconnecting": not self.stop_event.is_set()})
                    finally:
                        target.disk_proc = None
                        error.close()
                    if self.stop_event.is_set():
                        break
                    target.disk_errors += 1
                    target.stream_reconnects["diskstats"] += 1
                    gap = {"role": target.role, "stream": "diskstats", "startedAtUtc": started,
                           "reconnectingAtUtc": iso(), "exitCode": code,
                           "reason": "kubectl exec diskstats stream ended; retrying"}
                    target.stream_gaps.append(gap)
                    append_jsonl(self.out / "stream-gaps.jsonl", gap)
                    self.status("collecting", role=target.role, stream="diskstats",
                                reconnectCount=target.stream_reconnects["diskstats"])
                    if self.stop_event.wait(min(5, 1 + target.stream_reconnects["diskstats"])):
                        break

        thread = threading.Thread(target=consume, name=f"{target.role}-diskstats", daemon=True)
        thread.start(); self.threads.append(thread)

    def mark_role_ready(self, target: Target) -> None:
        with self.ready_lock:
            if target.sql_first_sample and target.disk_first_sample:
                self.ready_events[target.role].set()

    def handle_tracepoint_bcc_line(self, target: Target, line: str) -> None:
        payload = line.split("__BCC_RW_TP__", 1)[1].strip()
        if payload.startswith("__BCC_RW_TP_UNAVAILABLE__"):
            message = payload.split("__BCC_RW_TP_UNAVAILABLE__", 1)[1].strip()
            target.bcc_tracepoint_error = message
            if message not in target.bcc_tracepoint_gaps:
                target.bcc_tracepoint_gaps.append(message)
            gap = f"read/write BCC tracepoint fallback unavailable: {message}"
            if gap not in target.bcc_gaps:
                target.bcc_gaps.append(gap)
            append_jsonl(self.out / f"{target.role}-bcc-errors.jsonl",
                         {"capturedAtUtc": iso(), "source": "block-tracepoint-fallback",
                          "capabilityGap": gap, "line": line.rstrip()})
            return

        value = parse_bcc_json(payload)
        if value is None:
            target.bcc_unparsed += 1
            append_jsonl(self.out / f"{target.role}-bcc-unparsed.jsonl",
                         {"capturedAtUtc": iso(), "stream": "__BCC_RW_TP__", "line": line.rstrip()})
            return

        captured = value.get("capturedAtUtc") or iso()
        append_jsonl(self.out / f"{target.role}-bcc-rw-tracepoint-status.jsonl",
                     {"role": target.role, **value,
                      "containerLogTimestamp": target.bcc_current_log_timestamp,
                      "bccLogSegment": target.bcc_current_log_segment,
                      "collectorCapturedAtUtc": iso()})
        if value.get("event") == "ready":
            if target.bcc_tracepoint_error:
                warning = "tracepoint helper became ready after warning: " + target.bcc_tracepoint_error
                target.bcc_tracepoint_gaps.append(warning)
                target.bcc_tracepoint_error = None
            target.bcc_tracepoint_ready = True
            return

        if value.get("event") != "interval":
            if value.get("passed") is False:
                message = str(value.get("error") or "required block tracepoints unavailable")
                target.bcc_tracepoint_error = message
                if message not in target.bcc_tracepoint_gaps:
                    target.bcc_tracepoint_gaps.append(message)
                gap = f"read/write BCC tracepoint fallback unavailable: {message}"
                if gap not in target.bcc_gaps:
                    target.bcc_gaps.append(gap)
            return

        counters = value.get("diagnosticPairCounters") or {}
        for name in target.bcc_tracepoint_pair_counters:
            try:
                target.bcc_tracepoint_pair_counters[name] += int(counters.get(name, 0))
            except (TypeError, ValueError):
                pass
        histograms = value.get("histograms") or []
        valid_histograms = [item for item in histograms if isinstance(item, dict)]
        target.bcc_samples["rwTracepointIntervals"] += 1
        target.bcc_samples["rwTracepointBuckets"] += len(valid_histograms)
        bucket_total = 0
        interval_row = {"role": target.role, **value,
                        "scope": "block:block_io_start -> block:block_io_done; exact DATA/WAL devices",
                        "pairingKey": ["dev", "sector"],
                        "containerLogTimestamp": target.bcc_current_log_timestamp,
                        "bccLogSegment": target.bcc_current_log_segment,
                        "collectorCapturedAtUtc": iso()}
        append_jsonl(self.out / "bcc-rw-tracepoint-intervals.jsonl", interval_row)
        for item in valid_histograms:
            try:
                bucket_count = int(item.get("count", 0))
                bucket_total += bucket_count
                operation = str(item.get("operation", "unknown"))
                row = {"role": target.role, "capturedAtUtc": captured,
                       "diskKind": item.get("diskKind"), "device": item.get("device"),
                       "operation": operation, "flags": {"read": 0, "write": 1}.get(operation),
                       "histogram": {"log2Microseconds": int(item["log2Microseconds"]),
                                     "count": bucket_count},
                       "source": "block-tracepoint-fallback",
                       "scope": "block:block_io_start -> block:block_io_done; per-device read/write",
                       "pairingKey": ["dev", "sector"],
                       "containerLogTimestamp": target.bcc_current_log_timestamp,
                       "bccLogSegment": target.bcc_current_log_segment,
                       "diagnosticPairCounters": counters}
                append_jsonl(self.out / "bcc-read-write-histograms.jsonl", row)
            except (KeyError, TypeError, ValueError):
                target.bcc_unparsed += 1
                append_jsonl(self.out / f"{target.role}-bcc-unparsed.jsonl",
                             {"capturedAtUtc": iso(), "stream": "__BCC_RW_TP__",
                              "invalidHistogram": item})
        target.bcc_samples["rwTracepointCompletions"] += bucket_total

    def handle_bcc_log_line(self, target: Target, devices: dict[str, Any], line: str) -> None:
        if "__BCC_STARTED__" in line:
            target.bcc_ready = True
            if target.bcc_error and "did not announce ready" in target.bcc_error:
                target.bcc_warnings.append(target.bcc_error)
                target.bcc_error = None
            target.bcc_started_event.set()
            return
        if "__BCC_RW_TP__" in line:
            self.handle_tracepoint_bcc_line(target, line)
            return
        for marker, stream, label_kind in (
            ("__BCC_DEVICE__", "bcc-device-histograms.jsonl", "device"),
            ("__BCC_RW_DATA__", "bcc-read-write-histograms.jsonl", "read-write"),
            ("__BCC_RW_WAL__", "bcc-read-write-histograms.jsonl", "read-write"),
        ):
            if marker not in line:
                continue
            payload = line.split(marker, 1)[1].strip()
            histogram = parse_bcc_json(payload)
            if histogram is None:
                if label_kind == "read-write" and payload.startswith("ERROR:"):
                    disk_kind = "data" if "RW_DATA" in marker else "wal"
                    gap = f"{disk_kind} read/write BCC latency unavailable: {payload}"
                    if gap not in target.bcc_gaps:
                        target.bcc_gaps.append(gap)
                    append_jsonl(self.out / f"{target.role}-bcc-errors.jsonl",
                                 {"capturedAtUtc": iso(), "stream": marker,
                                  "capabilityGap": gap, "line": line.rstrip()})
                else:
                    target.bcc_unparsed += 1
                    append_jsonl(self.out / f"{target.role}-bcc-unparsed.jsonl",
                                 {"capturedAtUtc": iso(), "stream": marker, "line": line.rstrip()})
                break
            device = parse_histogram_device(histogram)
            if label_kind == "read-write":
                disk_kind = "data" if "RW_DATA" in marker else "wal"
                device = devices[disk_kind]["device"]
                flags = histogram.get("flags")
                operation = operation_from_flags(flags)
                target.bcc_samples["rw"] += 1
                row = {"capturedAtUtc": iso(), "diskKind": disk_kind,
                       "device": device, "operation": operation,
                       "flags": flags, "histogram": histogram,
                       "scope": "biolatency -F with exact -d device filter",
                       "source": "biolatency-F",
                       "containerLogTimestamp": target.bcc_current_log_timestamp,
                       "bccLogSegment": target.bcc_current_log_segment}
            else:
                target.bcc_samples["device"] += 1
                if device:
                    target.bcc_devices.add(device)
                row = {"capturedAtUtc": iso(), "device": device, "histogram": histogram,
                       "scope": "biolatency -D per-device", "source": "biolatency-D",
                       "containerLogTimestamp": target.bcc_current_log_timestamp,
                       "bccLogSegment": target.bcc_current_log_segment}
            append_jsonl(self.out / stream, {"role": target.role, **row})
            break
        if "__BCC_UNAVAILABLE__" in line or ("__BCC_RW_" in line and "UNAVAILABLE" in line):
            if line.rstrip() not in target.bcc_gaps:
                target.bcc_gaps.append(line.rstrip())
            if "__BCC_UNAVAILABLE__" in line and target.bcc_error is None:
                target.bcc_error = line.split("__BCC_UNAVAILABLE__", 1)[-1].strip()
            append_jsonl(self.out / f"{target.role}-bcc-errors.jsonl",
                         {"capturedAtUtc": iso(), "line": line.rstrip()})

    def start_bcc(self, target: Target) -> None:
        target.bcc_pod = "wal-bcc-" + target.role[:2] + "-" + uuid.uuid4().hex[:8]
        devices = target.devices
        pod = build_bcc_pod(target.bcc_pod, self.args.namespace, target.node, self.args.bcc_image,
                            devices["data"]["device"], devices["wal"]["device"])
        try:
            check(self.kubectl + ["apply", "-f", "-"], input_text=json.dumps(pod))
            deadline = time.monotonic() + self.args.bcc_startup_timeout
            while time.monotonic() < deadline:
                raw = check(self.kubectl + ["get", "pod", "-n", self.args.namespace, target.bcc_pod, "-o", "json"])
                status = json.loads(raw).get("status", {})
                phase = status.get("phase")
                cs = (status.get("containerStatuses") or [{}])[0]
                if phase == "Running" and cs.get("ready"):
                    break
                if phase in ("Failed", "Succeeded"):
                    raise RuntimeError(f"BCC pod ended during startup: {status}")
                time.sleep(2)
            else:
                raise TimeoutError(f"BCC pod {target.bcc_pod} readiness timeout")
            def consume() -> None:
                raw_path = self.out / f"{target.role}-bcc.raw.log"
                with raw_path.open("a", encoding="utf-8", buffering=1) as raw:
                    while not self.stop_event.is_set():
                        target.bcc_stream_segments += 1
                        segment = target.bcc_stream_segments
                        resume_since = bcc_stream_resume_since(target.bcc_stream_last_timestamp)
                        prior_timestamp = target.bcc_stream_last_timestamp
                        prior_timestamp_text = target.bcc_stream_last_timestamp_text
                        started = iso()
                        segment_event = {"event": "segment-started", "role": target.role,
                                         "segment": segment, "startedAtUtc": started,
                                         "resumeSinceTime": resume_since,
                                         "lastContainerTimestamp": target.bcc_stream_last_timestamp_text,
                                         "deduplicationKey": "sha256(container timestamp + complete log line)",
                                         "overlapSeconds": 2 if resume_since else 0}
                        append_jsonl(self.out / f"{target.role}-bcc-stream-events.jsonl", segment_event)
                        argv = self.kubectl + ["logs", "-n", self.args.namespace, target.bcc_pod,
                                               "-c", "bcc", "--follow", "--timestamps=true"]
                        if resume_since:
                            argv += ["--since-time", resume_since]
                        stderr_path = self.out / f"{target.role}-bcc-kubectl.stderr.log"
                        code = 255
                        error_message = None
                        proc = None
                        with stderr_path.open("ab", buffering=0) as stderr:
                            try:
                                proc = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=stderr,
                                                        text=True, bufsize=1)
                                target.bcc_proc = proc
                                if self.stop_event.is_set():
                                    self.stop_one(proc)
                                assert proc.stdout
                                timestamp_error_logged = False
                                first_record_logged = False
                                duplicate_count_segment = 0
                                for line in (() if self.stop_event.is_set() else proc.stdout):
                                    stripped = line.rstrip("\r\n")
                                    container_ts, parsed_ts = parse_kubectl_log_timestamp(stripped)
                                    if container_ts is None or parsed_ts is None:
                                        target.bcc_stream_timestamp_errors += 1
                                        if not timestamp_error_logged:
                                            timestamp_error_logged = True
                                            append_jsonl(self.out / f"{target.role}-bcc-stream-gaps.jsonl", {
                                                "capturedAtUtc": iso(), "role": target.role,
                                                "stream": "bcc-logs", "segment": segment,
                                                "gapType": "container-log-timestamp-unavailable",
                                                "reason": "cannot safely choose --since-time or deduplicate these lines",
                                                "line": stripped})
                                    else:
                                        if (target.bcc_stream_last_timestamp is None or
                                                parsed_ts > target.bcc_stream_last_timestamp or
                                                (parsed_ts == target.bcc_stream_last_timestamp and
                                                 container_ts > (target.bcc_stream_last_timestamp_text or ""))):
                                            target.bcc_stream_last_timestamp = parsed_ts
                                            target.bcc_stream_last_timestamp_text = container_ts
                                        fingerprint = bcc_log_line_fingerprint(container_ts, stripped)
                                        if fingerprint in target.bcc_stream_seen_lines:
                                            target.bcc_stream_duplicates += 1
                                            duplicate_count_segment += 1
                                            append_jsonl(self.out / f"{target.role}-bcc-stream-duplicates.jsonl", {
                                                "capturedAtUtc": iso(), "role": target.role,
                                                "segment": segment, "containerTimestamp": container_ts,
                                                "fingerprint": fingerprint})
                                            continue
                                        target.bcc_stream_seen_lines.add(fingerprint)
                                    if parsed_ts is not None and not first_record_logged:
                                        first_record_logged = True
                                        delta_seconds = ((parsed_ts - prior_timestamp).total_seconds()
                                                         if prior_timestamp else None)
                                        first_event = {"event": "first-new-record", "role": target.role,
                                                       "segment": segment,
                                                       "containerTimestamp": container_ts,
                                                       "previousContainerTimestamp": prior_timestamp_text,
                                                       "resumeSinceTime": resume_since,
                                                       "secondsAfterPreviousRecord": delta_seconds,
                                                       "deduplicatedReplayLines": duplicate_count_segment}
                                        append_jsonl(self.out / f"{target.role}-bcc-stream-events.jsonl", first_event)
                                        if delta_seconds is not None and delta_seconds > 3:
                                            gap = {"capturedAtUtc": iso(), "role": target.role,
                                                   "stream": "bcc-logs", "segment": segment,
                                                   "gapType": "timestamp-jump-after-reconnect",
                                                   "previousContainerTimestamp": prior_timestamp_text,
                                                   "firstNewContainerTimestamp": container_ts,
                                                   "secondsBetweenRecords": delta_seconds,
                                                   "interpretation": "possible lost log intervals or an idle period; diskstats and other metrics remain authoritative"}
                                            target.bcc_stream_gaps.append(gap)
                                            append_jsonl(self.out / f"{target.role}-bcc-stream-gaps.jsonl", gap)
                                    target.bcc_current_log_timestamp = container_ts
                                    target.bcc_current_log_segment = segment
                                    raw.write(line); raw.flush()
                                    self.handle_bcc_log_line(target, devices, stripped)
                                code = proc.wait()
                                if proc.stdout:
                                    proc.stdout.close()
                            except Exception as exc:
                                error_message = str(exc)
                                if proc and proc.poll() is None:
                                    proc.terminate()
                                    try:
                                        proc.wait(timeout=5)
                                    except subprocess.TimeoutExpired:
                                        proc.kill(); proc.wait()
                            finally:
                                if target.bcc_proc is proc:
                                    target.bcc_proc = None
                        if self.stop_event.is_set():
                            break

                        target.bcc_stream_reconnects += 1
                        gap = {"capturedAtUtc": iso(), "role": target.role, "stream": "bcc-logs",
                               "segment": segment, "startedAtUtc": started, "reconnecting": True,
                               "exitCode": code, "error": error_message,
                               "lastContainerTimestamp": target.bcc_stream_last_timestamp_text,
                               "resumeSinceTime": resume_since,
                               "kubectlStderrPath": str(stderr_path),
                               "deduplicationKey": "sha256(container timestamp + complete log line)",
                               "reason": "kubectl logs stream ended; resuming with overlap and exact-line deduplication"}
                        target.bcc_stream_gaps.append(gap)
                        stream_gap_label = "BCC log stream interrupted; reconnecting from last timestamp"
                        if stream_gap_label not in target.bcc_gaps:
                            target.bcc_gaps.append(stream_gap_label)
                        append_jsonl(self.out / f"{target.role}-bcc-stream-gaps.jsonl", gap)
                        append_jsonl(self.out / f"{target.role}-bcc-stream-events.jsonl", {
                            "event": "segment-ended", "role": target.role, **gap})
                        self.status("collecting", role=target.role, stream="bcc-logs",
                                    reconnectCount=target.bcc_stream_reconnects,
                                    resumeSinceTime=resume_since)
                        if self.stop_event.wait(min(5, 1 + target.bcc_stream_reconnects)):
                            break

            t = threading.Thread(target=consume, name=f"{target.role}-bcc-log", daemon=True)
            t.start(); target.bcc_threads.append(t); self.threads.append(t)
            if not target.bcc_started_event.wait(25) and target.bcc_error is None:
                target.bcc_error = "BCC processes did not announce ready within 25 seconds"
        except Exception as exc:
            target.bcc_error = str(exc)
            append_jsonl(self.out / f"{target.role}-bcc-errors.jsonl",
                         {"capturedAtUtc": iso(), "error": str(exc)})

    def azure_loop(self) -> None:
        next_poll = time.monotonic()
        while not self.stop_event.is_set():
            if self.stop_event.wait(max(0, next_poll - time.monotonic())):
                return
            try:
                result = self.azure.poll()
                append_jsonl(self.out / "azure" / "poll-summary.jsonl", result)
            except Exception as exc:
                append_jsonl(self.out / "azure" / "poll-errors.jsonl", {"capturedAtUtc": iso(), "error": str(exc)})
            next_poll += self.args.azure_poll_seconds
            if next_poll < time.monotonic():
                next_poll = time.monotonic() + self.args.azure_poll_seconds

    def wait_streams(self) -> None:
        deadline = time.monotonic() + self.args.stream_preflight_timeout
        for role, event in self.ready_events.items():
            left = deadline - time.monotonic()
            if left <= 0 or not event.wait(left):
                raise TimeoutError(f"{role} PostgreSQL/diskstats collectors did not yield a first sample")
        if self.fatal_errors:
            raise RuntimeError("required collectors failed: " + "; ".join(self.fatal_errors))

    def stop_one(self, proc: subprocess.Popen[str] | None) -> None:
        if not proc or proc.poll() is not None:
            return
        try:
            proc.terminate()
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill(); proc.wait(timeout=5)

    def stop(self) -> None:
        self.stop_event.set()
        for target in self.targets.values():
            if target.sql_proc:
                # The watcher query is idle between \watch iterations; closing
                # the kubectl stream is enough. A unique application name is
                # retained in case the client disconnect left a server session.
                self.stop_one(target.sql_proc)
                try:
                    sql = ("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name="
                           + sql_identifier_text(target.sql_app) + ";")
                    check(self.kubectl + ["exec", "-c", "postgres", "-n", self.args.namespace,
                                          target.pod, "--", "psql", "-X", "-A", "-t", "-q",
                                          "-U", "postgres", "-d", self.args.database, "-c", sql], timeout=20)
                except Exception as exc:
                    append_jsonl(self.out / "collector-errors.jsonl", {"atUtc": iso(), "role": target.role,
                                                                         "cleanupError": str(exc)})
            self.stop_one(target.disk_proc)
            self.stop_one(target.bcc_proc)
            if target.bcc_pod:
                try:
                    check(self.kubectl + ["delete", "pod", "-n", self.args.namespace,
                                          target.bcc_pod, "--ignore-not-found=true", "--wait=true",
                                          "--timeout=90s"], timeout=100)
                except Exception as exc:
                    self.fatal_errors.append(f"could not delete owned BCC pod {target.bcc_pod}: {exc}")
                    append_jsonl(self.out / "collector-errors.jsonl", {"atUtc": iso(), "role": target.role,
                                                                         "bccPod": target.bcc_pod,
                                                                         "cleanupError": str(exc)})
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=self.args.azure_timeout + 10 if thread.name == "azure-monitor" else 10)

    def summary(self) -> dict[str, Any]:
        def span(samples: list[str]) -> dict[str, Any]:
            ordered = sorted(parse_time(x) for x in samples)
            deltas = [(b - a).total_seconds() for a, b in zip(ordered, ordered[1:])]
            return {"sampleCount": len(ordered), "first": iso(ordered[0]) if ordered else None,
                    "last": iso(ordered[-1]) if ordered else None,
                    "intervalSeconds": {"min": min(deltas) if deltas else None,
                                        "median": sorted(deltas)[len(deltas)//2] if deltas else None,
                                        "max": max(deltas) if deltas else None},
                    "gapsOver1_5Seconds": sum(x > 1.5 for x in deltas)}
        return {"startedAtUtc": iso(self.started), "stoppedAtUtc": iso(self.stopped),
                "priorSegmentGap": self.prior_segment_gap,
                "streamReconnectCounts": {r: dict(t.stream_reconnects) for r, t in self.targets.items()},
                "streamGaps": [gap for t in self.targets.values() for gap in t.stream_gaps],
                "roles": {r: {"pod": t.pod, "node": t.node, "vmResourceId": t.vm_id,
                              "devices": t.devices,
                              "pgStat": {"samples": t.sql_ok, "errors": t.sql_errors,
                                         "span": span(t.sql_times),
                                         "pgStatIOAvailable": t.has_io,
                                         "recoveryPrefetchAvailable": t.has_prefetch,
                                         "pgStatCheckpointerAvailable": t.has_checkpointer},
                              "diskstats": {"samples": t.disk_ok, "errors": t.disk_errors,
                                            "span": span(t.disk_times)},
                              "hostStats": {"samples": t.host_ok, "errors": t.host_errors,
                                            "span": span(t.host_times),
                                            "pressureResources": sorted(t.host_pressure_resources)},
                              "bcc": {"pod": t.bcc_pod, "ready": t.bcc_ready, "error": t.bcc_error,
                                      "deviceHistogramSamples": t.bcc_samples["device"],
                                      "readWriteHistogramSamples": t.bcc_samples["rw"],
                                      "tracepointReadWriteReady": t.bcc_tracepoint_ready,
                                      "tracepointReadWriteError": t.bcc_tracepoint_error,
                                      "tracepointReadWriteIntervals": t.bcc_samples["rwTracepointIntervals"],
                                      "tracepointReadWriteHistogramBuckets": t.bcc_samples["rwTracepointBuckets"],
                                      "tracepointReadWriteHistogramEvents": t.bcc_samples["rwTracepointCompletions"],
                                      "tracepointPairCounters": dict(t.bcc_tracepoint_pair_counters),
                                      "tracepointGaps": t.bcc_tracepoint_gaps,
                                      "logStreamReconnects": t.bcc_stream_reconnects,
                                      "logStreamSegments": t.bcc_stream_segments,
                                      "logStreamDuplicateLines": t.bcc_stream_duplicates,
                                      "logStreamTimestampParseErrors": t.bcc_stream_timestamp_errors,
                                      "logStreamGaps": t.bcc_stream_gaps,
                                      "unparsed": t.bcc_unparsed,
                                      "gaps": t.bcc_gaps,
                                      "warnings": t.bcc_warnings,
                                      "deviceNamesObserved": sorted(t.bcc_devices),
                                      "requiredDataDevice": t.devices.get("data", {}).get("device"),
                                      "requiredWalDevice": t.devices.get("wal", {}).get("device")}}
                          for r, t in self.targets.items()},
                "azureMonitor": self.azure.summary(), "fatalErrors": self.fatal_errors,
                "azureReadiness": self.azure_readiness_proof,
                "sampling": {"pgStatSeconds": 1, "diskstatsSeconds": 1,
                             "azureNativeGrain": "PT1M"}}

    def validate_azure_readiness(self, first_poll: dict[str, Any]) -> dict[str, Any]:
        """Require exact-dimension points for the node and advertised disk gates."""
        observed = {(item.get("role"), item.get("kind")): item
                    for item in first_poll.get("targets", [])}
        proof: dict[str, Any] = {"validatedAtUtc": iso(), "pollId": first_poll.get("pollId"),
                                 "nativeInterval": first_poll.get("nativeInterval"),
                                 "requiredSeries": [], "missingSeries": []}

        def has_points(result: dict[str, Any], metric: str, vm_name: str | None = None,
                       lun: str | None = None) -> bool:
            for row in result.get("metrics", []):
                if row.get("metric") != metric or row.get("exitCode") != 0 or row.get("pointCount", 0) <= 0:
                    continue
                expected = row.get("expectedDimensions") or {}
                matched = row.get("matchedDimensions") or []
                if vm_name is not None:
                    if str(expected.get("vmname", "")).casefold() != vm_name.casefold():
                        continue
                    if not any(str(dim.get("vmname", "")).casefold() == vm_name.casefold()
                               for dim in matched):
                        continue
                if lun is not None:
                    if str(expected.get("lun", "")) != str(lun):
                        continue
                    if not any(str(dim.get("lun", "")) == str(lun) for dim in matched):
                        continue
                return True
            return False

        for target in self.azure.targets:
            if target.get("kind") != "vm":
                continue
            role = target["role"]
            result = observed.get((role, "vm"))
            if result is None:
                proof["missingSeries"].append({"role": role, "metric": "VM target", "reason": "no first-poll target"})
                continue
            vm_name = target.get("expectedVmName") if target.get("selectedScope") == "vmss-parent" else None
            available = set(target.get("availableMetrics", []))
            mandatory = ("Percentage CPU", "VM Uncached IOPS Consumed Percentage")
            for metric in mandatory:
                if metric not in available:
                    proof["missingSeries"].append({"role": role, "metric": metric,
                                                    "resourceId": target.get("selectedResourceId"),
                                                    "reason": "required VM metric is not advertised"})
                    continue
                valid = has_points(result, metric, vm_name=vm_name)
                item = {"role": role, "metric": metric, "resourceId": target.get("selectedResourceId"),
                        "scope": target.get("selectedScope"), "expectedVmName": vm_name,
                        "pointCount": sum(x.get("pointCount", 0) for x in result.get("metrics", [])
                                           if x.get("metric") == metric), "exactDimensionMatched": valid}
                (proof["requiredSeries"] if valid else proof["missingSeries"]).append(item)
            for metric in ("VM Uncached Bandwidth Consumed Percentage",):
                if metric not in available:
                    continue
                valid = has_points(result, metric, vm_name=vm_name)
                item = {"role": role, "metric": metric, "resourceId": target.get("selectedResourceId"),
                        "scope": target.get("selectedScope"), "expectedVmName": vm_name,
                        "pointCount": sum(x.get("pointCount", 0) for x in result.get("metrics", [])
                                           if x.get("metric") == metric), "exactDimensionMatched": valid}
                (proof["requiredSeries"] if valid else proof["missingSeries"]).append(item)
            for disk in target.get("diskLuns", []):
                for metric in ("Data Disk Latency", "Data Disk Queue Depth"):
                    if metric not in available:
                        continue
                    valid = has_points(result, metric, vm_name=vm_name, lun=str(disk["lun"]))
                    item = {"role": role, "metric": metric, "lun": str(disk["lun"]),
                            "resourceId": target.get("selectedResourceId"), "scope": target.get("selectedScope"),
                            "expectedVmName": vm_name,
                            "pointCount": sum(x.get("pointCount", 0) for x in result.get("metrics", [])
                                               if x.get("metric") == metric and
                                               str((x.get("expectedDimensions") or {}).get("lun", "")) == str(disk["lun"])),
                            "exactDimensionsMatched": valid}
                    (proof["requiredSeries"] if valid else proof["missingSeries"]).append(item)
        if proof["missingSeries"]:
            atomic_json(self.out / "azure" / "readiness-proof.json", proof)
            raise RuntimeError("initial Azure poll lacks required exact-dimension series: " +
                               json.dumps(proof["missingSeries"], sort_keys=True))
        proof["passed"] = True
        atomic_json(self.out / "azure" / "readiness-proof.json", proof)
        return proof

    def run(self) -> int:
        rc = 0
        try:
            self.preflight()
            for target in self.targets.values():
                self.start_sql(target)
                self.start_diskstats(target)
            # BCC is best-effort: unsupported host kernels remain a recorded
            # coverage gap and do not suppress primary disk/SQL evidence.
            for target in self.targets.values():
                self.start_bcc(target)
            self.wait_streams()
            first_azure_poll = self.azure.poll(end=dt.datetime.now(UTC),
                                               start=dt.datetime.now(UTC)-dt.timedelta(minutes=10))
            self.azure_readiness_proof = self.validate_azure_readiness(first_azure_poll)
            thread = threading.Thread(target=self.azure_loop, name="azure-monitor", daemon=True)
            thread.start(); self.threads.append(thread)
            atomic_json(self.preflight_path, {**json.loads(self.preflight_path.read_text()),
                                               "phase": "collecting", "collectingAtUtc": iso(),
                                               "azureMetricReadiness": self.azure_readiness_proof,
                                               "bcc": {r: {"pod": t.bcc_pod, "ready": t.bcc_ready,
                                                           "error": t.bcc_error, "gaps": t.bcc_gaps,
                                                           "warnings": t.bcc_warnings,
                                                           "tracepointReadWriteReady": t.bcc_tracepoint_ready,
                                                           "tracepointReadWriteError": t.bcc_tracepoint_error,
                                                           "tracepointReadWriteGaps": t.bcc_tracepoint_gaps,
                                                           "logStreamReconnects": t.bcc_stream_reconnects,
                                                           "logStreamSegments": t.bcc_stream_segments,
                                                           "logStreamDuplicateLines": t.bcc_stream_duplicates,
                                                           "logStreamTimestampParseErrors": t.bcc_stream_timestamp_errors,
                                                           "logStreamGaps": t.bcc_stream_gaps}
                                                       for r, t in self.targets.items()}})
            self.was_ready = True
            self.status("collecting", preflightPath=str(self.preflight_path))
            print(json.dumps({"event": "READY", "preflight": str(self.preflight_path)}, sort_keys=True), flush=True)
            while not self.stop_event.wait(1):
                if self.fatal_errors:
                    raise RuntimeError("required collector stopped: " + "; ".join(self.fatal_errors))
                for target in self.targets.values():
                    if target.bcc_proc and target.bcc_proc.poll() is not None:
                        target.bcc_error = f"BCC log stream exited with {target.bcc_proc.returncode}"
        except Exception as exc:
            self.fatal_errors.append(str(exc))
            self.status("failed", error=str(exc))
            append_jsonl(self.out / "collector-errors.jsonl", {"atUtc": iso(), "error": str(exc)})
            print(f"collector failed: {exc}", file=sys.stderr, flush=True)
            rc = 2
        finally:
            self.status("stopping")
            self.stop()
            self.stopped = dt.datetime.now(UTC)
            # Azure Monitor ingests at one-minute grain with delay. Re-poll all
            # resources until the newest expected complete minute appears or
            # the configured grace window expires.
            target_latest = (self.stopped - dt.timedelta(minutes=1)).replace(second=0, microsecond=0)
            drain_complete = False
            expected_metrics = self.azure_expected_metrics()
            pending_metrics = list(expected_metrics)
            drain_seconds = self.args.metric_drain_seconds if self.was_ready and expected_metrics else 0
            drain_deadline = time.monotonic() + drain_seconds
            while time.monotonic() < drain_deadline:
                try:
                    self.azure.poll(start=self.started-dt.timedelta(minutes=2), end=dt.datetime.now(UTC))
                except Exception as exc:
                    append_jsonl(self.out / "azure" / "poll-errors.jsonl", {"capturedAtUtc": iso(), "finalDrainError": str(exc)})
                latest_metrics = self.azure_latest_metrics()
                pending_metrics = [key for key, timestamp in expected_metrics.items()
                                   if latest_metrics.get(key) is None or latest_metrics[key] < target_latest]
                if expected_metrics and not pending_metrics:
                    drain_complete = True
                    break
                time.sleep(min(15, max(0, drain_deadline-time.monotonic())))
            atomic_json(self.out / "coverage.json", {**self.summary(),
                        "azureMetricDrain": {"attempted": drain_seconds > 0,
                                             "complete": drain_complete,
                                             "expectedLatestBucket": iso(target_latest),
                                             "latestBucketByMetric": {key: iso(value) for key, value in self.azure_latest_metrics().items()},
                                             "pendingMetricSeries": pending_metrics if self.was_ready else [],
                                             "timeoutSeconds": self.args.metric_drain_seconds}})
            self.status("stopped" if not self.fatal_errors else "failed",
                        stoppedAtUtc=iso(self.stopped), coveragePath=str(self.out / "coverage.json"))
        return 2 if self.fatal_errors else rc

    def azure_expected_metrics(self) -> dict[str, dt.datetime | None]:
        expected = {}
        for target in self.azure.targets:
            metrics = target.get("availableMetrics", [])
            if target["kind"] == "vm":
                for metric in (x for x in metrics if not x.startswith("Data Disk")):
                    key = f"{target['role']}|vm|{metric}"
                    expected[key] = None
                for disk in target.get("diskLuns", []):
                    for metric in (x for x in metrics if x.startswith("Data Disk")):
                        key = f"{target['role']}|vm|{metric}|lun={disk['lun']}"
                        expected[key] = None
            else:
                for metric in metrics:
                    expected[f"{target['role']}|{target['kind']}|{metric}"] = None
        return expected

    def azure_latest_metrics(self) -> dict[str, dt.datetime]:
        latest: dict[str, dt.datetime] = {}
        path = self.out / "azure" / "azure-metrics.jsonl"
        if not path.exists():
            return latest
        with path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                try:
                    row = json.loads(line)
                    stamp = ((row.get("point") or {}).get("timeStamp"))
                    if stamp:
                        parsed = parse_time(stamp)
                        key = f"{row.get('role')}|{row.get('targetKind')}|{row.get('metric')}"
                        lun = (row.get("dimensions") or {}).get("lun")
                        if row.get("targetKind") == "vm" and str(row.get("metric", "")).startswith("Data Disk"):
                            key += f"|lun={lun}"
                        latest[key] = parsed if key not in latest or parsed > latest[key] else latest[key]
                except (json.JSONDecodeError, ValueError):
                    continue
        return latest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--kubeconfig")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--verified-attachments", required=True)
    parser.add_argument("--database", default="repro")
    parser.add_argument("--client-app-name", default="hdd-repro-client",
                        help="Application name or prefix recorded by the write client")
    parser.add_argument("--out", required=True)
    parser.add_argument("--bcc-image", default=os.environ.get("BCC_IMAGE", "ubuntu:24.04"))
    parser.add_argument("--bcc-startup-timeout", type=int, default=300)
    parser.add_argument("--stream-preflight-timeout", type=int, default=45)
    parser.add_argument("--az", default="az")
    parser.add_argument("--azure-timeout", type=int, default=90)
    parser.add_argument("--azure-poll-seconds", type=int, default=60)
    parser.add_argument("--metric-drain-seconds", type=int, default=120)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.azure_poll_seconds < 30 or args.metric_drain_seconds < 0:
        print("--azure-poll-seconds must be >=30 and --metric-drain-seconds >=0", file=sys.stderr)
        return 2
    collector = Collector(args)
    signal.signal(signal.SIGINT, lambda *_: collector.stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: collector.stop_event.set())
    return collector.run()


if __name__ == "__main__":
    raise SystemExit(main())
