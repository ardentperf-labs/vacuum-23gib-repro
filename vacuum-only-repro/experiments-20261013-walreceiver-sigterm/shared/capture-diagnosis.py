#!/usr/bin/env python3
"""Read-only, credential-safe point-in-time PostgreSQL/CNPG diagnosis capture.

Captures whitelisted Cluster archive-source metadata, safe PostgreSQL archive
settings and counters, selected redacted PostgreSQL/CNPG log events, and WAL
segment/archive_status filenames and metadata. Raw command settings, Secrets,
and unfiltered logs are never written.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

UTC = dt.timezone.utc
HERE = Path(__file__).resolve().parent
INSTRUMENTATION = HERE / "instrumentation"
sys.path.insert(0, str(INSTRUMENTATION))
from collect import (  # noqa: E402
    atomic_json,
    capture_archive_source_receipt,
    safe_command_setting_sql,
)


INTERESTING = re.compile(r"(wal|archive|recovery|replication|receiver|timeline|restore|sync|timeout)", re.I)
SECRET_MARKER = re.compile(r"(password|passwd|secret|token|sas|signature|conninfo|account.?key|storage.?key)", re.I)
SECRET_ASSIGNMENT = re.compile(
    r"(?i)(password|passwd|secret|token|sas|signature|conninfo|account.?key|storage.?key)"
    r"([\"']?\s*[:=]\s*[\"']?)[^\s,;\"']+"
)
URL_QUERY = re.compile(r"(https?://[^\s\"']+?)\?[^\s\"']+", re.I)
LOG_TIME = re.compile(r"^(\d{4}-\d\d-\d\dT\S+)\s+(.*)$")


def now() -> str:
    return dt.datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def run(argv: list[str], timeout: int = 90, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, input=input_text, text=True, capture_output=True,
                          timeout=timeout, check=False)


def redact_text(value: str) -> str:
    if SECRET_MARKER.search(value):
        # Preserve the event itself where a structured field gave us one, but
        # never echo a possibly credential-bearing unstructured line.
        return "[omitted: credential-bearing log line]"
    clean = SECRET_ASSIGNMENT.sub(lambda m: m.group(1) + m.group(2) + "[REDACTED]", value)
    clean = URL_QUERY.sub(r"\1?[REDACTED]", clean)
    return clean[:2000]


def classify_error(value: str) -> str:
    value = value.casefold()
    for label, pattern in (
        ("authentication", r"auth|credential|forbidden|unauthorized|signature"),
        ("permission", r"permission|access denied|not authorized"),
        ("not-found", r"not found|no such|404|missing"),
        ("timeout", r"timeout|timed out|deadline"),
        ("network", r"connection|network|dns|resolve|unreachable|reset by peer"),
        ("storage-service", r"azure|blob|object store|barman|s3|gcs"),
    ):
        if re.search(pattern, value):
            return label
    return "other"


def safe_log_record(raw_line: str, pod: str | None = None) -> dict[str, Any] | None:
    line = raw_line.rstrip("\r\n")
    if not line:
        return None
    prefix = LOG_TIME.match(line)
    timestamp = prefix.group(1) if prefix else None
    body = prefix.group(2) if prefix else line
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError:
        if not INTERESTING.search(body):
            return None
        if SECRET_MARKER.search(body):
            return {"timestamp": timestamp, "pod": pod,
                    "message": "[omitted: credential-bearing log line]", "redacted": True}
        return {"timestamp": timestamp, "pod": pod, "message": redact_text(body), "structured": False}
    if not isinstance(decoded, dict):
        return None
    nested = decoded.get("record") if isinstance(decoded.get("record"), dict) else None
    nested_message = nested.get("message") if nested else None
    msg = (nested_message if decoded.get("logger") == "postgres" and isinstance(nested_message, str)
           else decoded.get("msg") or decoded.get("message") or decoded.get("error") or "")
    selected = " ".join(str(decoded.get(k, "")) for k in
                        ("logger", "msg", "message", "error", "walName", "wal_name"))
    if decoded.get("logger") == "postgres" and nested:
        selected += " " + str(nested_message or "")
    if not INTERESTING.search(selected):
        return None
    if SECRET_MARKER.search(json.dumps(decoded, default=str)):
        # Do not retain arbitrary structured fields. The record below is an
        # allowlist, and text is passed through the redactor independently.
        pass
    event_timestamp = timestamp or decoded.get("ts") or decoded.get("timestamp")
    record: dict[str, Any] = {"timestamp": event_timestamp,
                              "pod": pod,
                              "level": decoded.get("level") if decoded.get("level") in
                                  ("trace", "debug", "info", "warning", "error", "fatal", "panic") else None,
                              "logger": (str(decoded.get("logger"))[:120]
                                         if re.fullmatch(r"[A-Za-z0-9._/-]{1,120}", str(decoded.get("logger", "")))
                                         else None),
                              "message": redact_text(str(msg)) if msg else None,
                              "structured": True}
    if decoded.get("logger") == "postgres" and nested:
        # CNPG wraps PostgreSQL's JSON logging protocol in a top-level
        # {logger:postgres,msg:record,record:{...}} object. Only these five
        # fields are retained; query/detail/context and every other member of
        # the nested record are deliberately ignored.
        safe_nested: dict[str, Any] = {}
        if isinstance(nested_message, str):
            safe_nested["message"] = redact_text(nested_message)
        log_time = nested.get("log_time")
        if isinstance(log_time, str) and len(log_time) <= 64 and re.fullmatch(
                r"\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:\.\d+)?(?: ?(?:Z|UTC|[+-]\d\d(?::?\d\d)?))?", log_time):
            safe_nested["log_time"] = log_time
            record["postgresLogTime"] = log_time
            if not event_timestamp:
                record["timestamp"] = log_time
        process_id = nested.get("process_id")
        if isinstance(process_id, int) and not isinstance(process_id, bool) and 1 <= process_id <= 2**31 - 1:
            safe_nested["process_id"] = process_id
        severity = nested.get("error_severity")
        if isinstance(severity, str) and re.fullmatch(r"[A-Z ]{1,16}", severity):
            safe_nested["error_severity"] = severity
        sql_state = nested.get("sql_state_code")
        if isinstance(sql_state, str) and re.fullmatch(r"[0-9A-Z]{5}", sql_state):
            safe_nested["sql_state_code"] = sql_state
        if safe_nested:
            record["postgresRecord"] = safe_nested
    if isinstance(msg, str):
        # Classify both raw CNPG-wrapped records and the safe redacted event
        # rows emitted by capture_logs(), so the timeline tool can consume
        # either representation without needing the original raw log stream.
        restored = re.search(r"\brestored log file [\"']([0-9A-Fa-f]{24}(?:\.partial)?|[0-9A-Fa-f]{8}\.history)['\"] from archive\b",
                             msg, re.I)
        if restored:
            record["archiveRestoreOutcome"] = "success"
            record["walName"] = restored.group(1).upper()
        elif re.search(r"\b(?:restore command failed|could not restore file|failed to restore|archive recovery failed)\b",
                       msg, re.I):
            record["archiveRestoreOutcome"] = "failure"
    wal_name = decoded.get("walName") or decoded.get("wal_name")
    if isinstance(wal_name, str) and re.fullmatch(
            r"(?:[0-9A-Fa-f]{24}(?:\.partial)?|[0-9A-Fa-f]{8}\.history)", wal_name):
        record["walName"] = wal_name
    for key in ("timeline", "timeline_id", "fileTimeline", "clusterTimeline"):
        value = decoded.get(key)
        if isinstance(value, int) and 0 <= value <= 0xFFFFFFFF:
            record[key] = value
    for key in ("startLSN", "endLSN"):
        value = decoded.get(key)
        if isinstance(value, str) and re.fullmatch(r"[0-9A-Fa-f]+/[0-9A-Fa-f]+", value):
            record[key] = value
    error_value = decoded.get("error") or decoded.get("err")
    if error_value:
        record["errorPresent"] = True
        record["errorClass"] = classify_error(str(error_value) + " " + str(msg))
    reason = decoded.get("reason")
    if reason:
        record["reasonClass"] = classify_error(str(reason))
    return record


def safe_pg_sql() -> str:
    archive = safe_command_setting_sql("archive_command")
    restore = safe_command_setting_sql("restore_command")
    library = safe_command_setting_sql("archive_library")
    return f'''SELECT json_build_object(
 'capturedAtUtc', clock_timestamp(),
 'role', CASE WHEN pg_is_in_recovery() THEN 'replica' ELSE 'primary' END,
 'inRecovery', pg_is_in_recovery(),
 'settings', json_build_object(
   'archiveMode', (SELECT json_build_object('setting',setting,'source',source,
     'sourceFile',sourcefile,'sourceLine',sourceline,'pendingRestart',pending_restart)
     FROM pg_settings WHERE name='archive_mode'),
   'archiveCommand', {archive}, 'restoreCommand', {restore}, 'archiveLibrary', {library},
   'recoveryTargetSettings', (SELECT coalesce(json_object_agg(name,json_build_object(
     'configured',setting<>'','source',source,'sourceFile',sourcefile,'sourceLine',sourceline)), '{{}}'::json)
     FROM pg_settings WHERE name IN ('recovery_target','recovery_target_lsn','recovery_target_time',
       'recovery_target_xid','recovery_target_name','recovery_target_action')),
   'primarySlotName', (SELECT json_build_object('configured',setting<>'','source',source,
     'sourceFile',sourcefile,'sourceLine',sourceline) FROM pg_settings WHERE name='primary_slot_name')),
 'archiver', (SELECT to_jsonb(a) FROM pg_stat_archiver a),
 'receiver', (SELECT to_jsonb(w) - 'conninfo' FROM pg_stat_wal_receiver w LIMIT 1),
 'senders', coalesce((SELECT json_agg(json_build_object('applicationName',application_name,
   'state',state,'syncState',sync_state,'sentLsn',sent_lsn,'writeLsn',write_lsn,
   'flushLsn',flush_lsn,'replayLsn',replay_lsn,'replyTime',reply_time) ORDER BY application_name)
   FROM pg_stat_replication),'[]'::json),
 'slots', coalesce((SELECT json_agg(json_build_object('slotName',slot_name,'slotType',slot_type,
   'active',active,'restartLsn',restart_lsn,'confirmedFlushLsn',confirmed_flush_lsn)
   ORDER BY slot_name) FROM pg_replication_slots),'[]'::json))::text;'''


def capture_pg(kubectl: list[str], namespace: str, pod: str, database: str) -> dict[str, Any]:
    sql = safe_pg_sql()
    result = run(kubectl + ["exec", "-n", namespace, pod, "-c", "postgres", "--",
                            "psql", "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1",
                            "-U", "postgres", "-d", database, "-c", sql], timeout=45)
    if result.returncode:
        return {"pod": pod, "exitCode": result.returncode, "captured": False,
                "error": "read-only PostgreSQL query failed; output omitted"}
    try:
        row = json.loads(result.stdout.strip())
    except json.JSONDecodeError:
        return {"pod": pod, "exitCode": 0, "captured": False, "error": "response was not JSON"}
    return {"pod": pod, "exitCode": 0, "captured": True, "sample": row}


WAL_INVENTORY = r'''set -eu
data=$(psql -X -A -t -q -U postgres -d postgres -c 'show data_directory')
wal=$(readlink -f "$data/pg_wal")
printf 'WALDIR\t%s\n' "$(basename "$wal")"
find "$wal" -maxdepth 2 -type f -printf '%f\t%s\t%TY-%Tm-%TdT%TH:%TM:%TS\n' \
 | awk -F '\t' '$1 ~ /^[0-9A-F]{24}(\.history|\.partial)?$/ || $1 ~ /\.ready$/ || $1 ~ /\.done$/ {print}'
'''


def capture_wal_inventory(kubectl: list[str], namespace: str, pod: str) -> dict[str, Any]:
    result = run(kubectl + ["exec", "-n", namespace, pod, "-c", "postgres", "--", "sh", "-c", WAL_INVENTORY], timeout=60)
    if result.returncode:
        return {"pod": pod, "exitCode": result.returncode, "captured": False,
                "error": "read-only WAL inventory failed; output omitted"}
    rows = []
    wal_dir = None
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if parts[0] == "WALDIR" and len(parts) > 1:
            wal_dir = parts[1]
        elif len(parts) == 3:
            rows.append({"name": parts[0], "bytes": int(parts[1]), "mtime": parts[2]})
    return {"pod": pod, "captured": True, "walDirectoryBasename": wal_dir,
            "fileCount": len(rows), "files": rows}


def capture_logs(kubectl: list[str], argv: list[str], out: Path, source: str,
                 pod: str | None = None) -> dict[str, Any]:
    result = run(kubectl + argv, timeout=180)
    output_path = out / f"{source}-log-events-redacted.jsonl"
    rows = []
    for line in result.stdout.splitlines():
        row = safe_log_record(line, pod)
        if row:
            rows.append(row)
    with output_path.open("a", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True) + "\n")
    return {"source": source, "pod": pod, "exitCode": result.returncode,
            "filteredEventCount": len(rows), "rawOutputSaved": False,
            "stderrSaved": False,
            "error": None if result.returncode == 0 else "log read failed; raw output omitted"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--kubeconfig")
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--since", default="24h")
    parser.add_argument("--operator-namespace", default="cnpg-system")
    parser.add_argument("--operator", default="deployment/cnpg-controller-manager")
    args = parser.parse_args()
    os.umask(0o077)
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True, mode=0o700)
    kubectl = [args.kubectl] + (["--kubeconfig", args.kubeconfig] if args.kubeconfig else [])
    receipt: dict[str, Any] = {"capturedAtUtc": now(), "namespace": args.namespace,
                               "cluster": args.cluster, "safety": {
                                   "readOnly": True, "secretValuesQueried": False,
                                   "rawPostgresSettingsWritten": False,
                                   "rawLogsWritten": False, "rawKubectlOutputWritten": False}}
    capture_archive_source_receipt(kubectl, args.namespace, out)

    pod_result = run(kubectl + ["get", "pods", "-n", args.namespace, "-o", "json"], timeout=45)
    if pod_result.returncode:
        receipt["pods"] = {"captured": False, "error": "pod list failed; raw output omitted"}
        pods = []
    else:
        try:
            pod_items = json.loads(pod_result.stdout).get("items", [])
            pods = [item for item in pod_items if
                    item.get("metadata", {}).get("labels", {}).get("cnpg.io/cluster") == args.cluster]
            receipt["pods"] = {"captured": True, "count": len(pods),
                "items": [{"name": p.get("metadata", {}).get("name"),
                           "uid": p.get("metadata", {}).get("uid"),
                           "node": p.get("spec", {}).get("nodeName"),
                           "phase": p.get("status", {}).get("phase"),
                           "role": p.get("metadata", {}).get("labels", {}).get("role") or
                                  p.get("metadata", {}).get("labels", {}).get("cnpg.io/instanceRole")}
                          for p in pods]}
        except (json.JSONDecodeError, TypeError):
            pods = []
            receipt["pods"] = {"captured": False, "error": "pod response was not JSON"}

    role_captures = []
    for pod_obj in pods:
        meta = pod_obj.get("metadata", {})
        pod = meta.get("name")
        labels = meta.get("labels", {})
        role = labels.get("cnpg.io/instanceRole") or labels.get("role") or "unknown"
        pg = capture_pg(kubectl, args.namespace, pod, args.database)
        pg["role"] = role
        wal = capture_wal_inventory(kubectl, args.namespace, pod)
        wal["role"] = role
        log_capture = capture_logs(kubectl, ["logs", "-n", args.namespace, pod, "-c", "postgres",
                                             "--timestamps=true", f"--since={args.since}"], out,
                                  "postgres", pod)
        statuses = pod_obj.get("status", {}).get("containerStatuses", []) or []
        postgres_status = next((item for item in statuses if item.get("name") == "postgres"), None)
        previous_log_capture = None
        try:
            restart_count = int((postgres_status or {}).get("restartCount", 0) or 0)
        except (TypeError, ValueError):
            restart_count = 0
        if restart_count > 0:
            previous_log_capture = capture_logs(
                kubectl, ["logs", "-n", args.namespace, pod, "-c", "postgres", "--previous",
                          "--timestamps=true", f"--since={args.since}"], out,
                "postgres-previous", pod)
        role_captures.append({"postgres": pg, "walInventory": wal,
                              "logs": log_capture, "previousLogs": previous_log_capture})
    receipt["roleCaptures"] = role_captures

    operator = capture_logs(kubectl, ["logs", "-n", args.operator_namespace, args.operator,
                                      "--timestamps=true", f"--since={args.since}"],
                            out, "operator")
    receipt["operatorLogs"] = operator
    atomic_json(out / "diagnosis-capture.json", receipt)

    manifest_files = []
    for path in sorted(out.iterdir()):
        if not path.is_file() or path.name == "capture-manifest.json":
            continue
        payload = path.read_bytes()
        manifest_files.append({"path": path.name, "bytes": len(payload),
                               "sha256": hashlib.sha256(payload).hexdigest()})
    atomic_json(out / "capture-manifest.json", {"capturedAtUtc": now(),
        "safety": {"secretValuesQueried": False, "rawLogsSaved": False},
        "files": manifest_files})
    os.chmod(out, 0o700)
    for path in out.iterdir():
        if path.is_file():
            os.chmod(path, 0o600)
    print(json.dumps({"capturedAtUtc": receipt["capturedAtUtc"], "out": str(out),
                      "postgresPods": len(pods), "operatorEvents": operator["filteredEventCount"],
                      "rawLogsSaved": False}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
