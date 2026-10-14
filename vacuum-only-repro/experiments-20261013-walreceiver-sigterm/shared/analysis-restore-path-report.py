#!/usr/bin/env python3
"""Build a safe archive-restore and stream-return timeline from captured logs.

The input PostgreSQL log files are read only and are never copied into the
report. Output is restricted to restore event timestamps/WAL names and the
sender/receiver state fields needed to relate archive recovery to streaming.
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

UTC = dt.timezone.utc
HERE = Path(__file__).resolve().parent
_SPEC = importlib.util.spec_from_file_location("capture_diagnosis", HERE / "capture-diagnosis.py")
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError("cannot load the diagnosis log parser")
_CAPTURE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _CAPTURE
_SPEC.loader.exec_module(_CAPTURE)


def parse_time(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or len(value) > 80:
        return None
    normalized = value.strip()
    if normalized.endswith(" UTC"):
        normalized = normalized[:-4] + "+00:00"
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(normalized.replace(" ", "T", 1))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: dt.datetime | None) -> str | None:
    if value is None:
        return None
    rendered = value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    rendered = re.sub(r"(\.\d*?[1-9])0+Z$", r"\1Z", rendered)
    return rendered.replace(".000000Z", "Z")


def safe_pid(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if 1 <= result <= 2**31 - 1 else None


def safe_lsn(value: Any) -> str | None:
    return value if isinstance(value, str) and re.fullmatch(r"[0-9A-Fa-f]+/[0-9A-Fa-f]+", value) else None


def read_signal(intervention_path: Path | None, explicit: str | None) -> dict[str, Any]:
    if explicit:
        value = parse_time(explicit)
        return {"signalSent": value is not None, "atUtc": iso(value), "source": "--signal-at"}
    if intervention_path is None:
        return {"signalSent": False, "atUtc": None, "source": None}
    try:
        obj = json.loads(intervention_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"signalSent": False, "atUtc": None, "source": "intervention receipt unreadable"}
    signal_result = obj.get("signalResult") if isinstance(obj, dict) else None
    signal_result = signal_result if isinstance(signal_result, dict) else {}
    sent = signal_result.get("signalSent") is True
    time_value = (signal_result.get("signalAcceptedAtUtc") or obj.get("executedAtUtc")) if sent else None
    parsed = parse_time(time_value)
    process_identity = signal_result.get("processIdentity")
    process_identity = process_identity if isinstance(process_identity, dict) else {}
    raw_pid = process_identity.get("pid") or signal_result.get("pid")
    try:
        receiver_pid = int(raw_pid) if raw_pid is not None else None
    except (TypeError, ValueError):
        receiver_pid = None
    return {"mode": obj.get("mode") if obj.get("mode") in ("sigterm", "control") else None,
            "status": obj.get("status") if obj.get("status") in
                ("sent", "marked", "failed", "cancelled_before_dispatch") else None,
            "signalSent": sent and parsed is not None, "atUtc": iso(parsed),
            "signaledReceiverPid": receiver_pid if receiver_pid and receiver_pid > 0 else None,
            "source": ("walreceiver-intervention.json.signalResult.signalAcceptedAtUtc"
                       if sent and signal_result.get("signalAcceptedAtUtc") else
                       "walreceiver-intervention.json.executedAtUtc")}


def read_restore_events(paths: list[Path]) -> tuple[list[dict[str, Any]], int, int]:
    events: list[dict[str, Any]] = []
    read_files = 0
    seen: set[tuple[str, str, str | None]] = set()
    duplicates = 0
    for path in paths:
        try:
            stream = path.open("r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        read_files += 1
        with stream:
            for line in stream:
                parsed = _CAPTURE.safe_log_record(line)
                if not parsed or parsed.get("archiveRestoreOutcome") not in ("success", "failure"):
                    continue
                event_time = parse_time(parsed.get("timestamp") or parsed.get("postgresLogTime"))
                if event_time is None:
                    continue
                timestamp = iso(event_time)
                outcome = parsed["archiveRestoreOutcome"]
                wal_name = parsed.get("walName")
                key = (timestamp, outcome, wal_name)
                if key in seen:
                    duplicates += 1
                    continue
                seen.add(key)
                events.append({"timestamp": timestamp, "time": event_time,
                               "outcome": outcome, "walName": wal_name,
                               "processId": (parsed.get("postgresRecord") or {}).get("process_id"),
                               "severity": (parsed.get("postgresRecord") or {}).get("error_severity"),
                               "pod": parsed.get("pod")})
    events.sort(key=lambda item: item["time"])
    return events, read_files, duplicates


def sample_time(sample: dict[str, Any]) -> dt.datetime | None:
    return parse_time(sample.get("sampledAt") or sample.get("capturedAtUtc"))


def read_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    rows: list[dict[str, Any]] = []
    try:
        stream = path.open("r", encoding="utf-8", errors="replace")
    except OSError:
        return rows
    with stream:
        for line in stream:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def streaming_evidence(path: Path | None, signal_time: dt.datetime | None,
                       kind: str, signaled_receiver_pid: int | None = None) -> dict[str, Any]:
    rows = read_jsonl(path)
    if path is None:
        return {"available": False, "reason": "sample file not supplied"}
    if not path.is_file():
        return {"available": False, "source": path.name, "reason": "sample file unavailable",
                "sampleCount": 0}
    if signal_time is None:
        return {"available": bool(rows), "reason": "signal timestamp unavailable",
                "sampleCount": len(rows)}
    matches: list[dict[str, Any]] = []
    observed_absence = None
    pid_change = None
    for wrapper in rows:
        sample = wrapper
        when = sample_time(sample)
        if when is None or when < signal_time:
            continue
        observed_at = iso(when)
        if kind == "primary":
            senders = sample.get("replicationSenders")
            if senders is None:
                senders = sample.get("replication_senders")
            if isinstance(senders, list) and not senders and observed_absence is None:
                observed_absence = {"timestamp": observed_at, "reason": "no primary replication sender rows"}
            for sender in senders if isinstance(senders, list) else []:
                if not isinstance(sender, dict):
                    continue
                app = sender.get("application_name") or sender.get("applicationName")
                app = str(app) if isinstance(app, str) and re.fullmatch(r"[A-Za-z0-9_.:/ -]{1,64}", app) else None
                sync_state = sender.get("sync_state") or sender.get("syncState")
                sync_state = str(sync_state).casefold() if sync_state is not None else None
                if sync_state not in ("async", "potential", "sync", "quorum"):
                    sync_state = "other" if sync_state else None
                state = str(sender.get("state", "unknown")).casefold()
                if state not in ("startup", "catchup", "streaming", "backup"):
                    state = "other"
                item = {"timestamp": observed_at,
                                "applicationName": app,
                                "senderPid": safe_pid(sender.get("pid")),
                                "state": state,
                                "syncState": str(sync_state)[:32] if sync_state is not None else None,
                                "syncQuorum": (state.casefold() == "streaming" and
                                               str(sync_state or "").casefold() in ("sync", "quorum"))}
                matches.append(item)
        else:
            has_receiver_field = "walReceiver" in sample or "wal_receiver" in sample
            receiver = sample.get("walReceiver") if "walReceiver" in sample else sample.get("wal_receiver")
            if has_receiver_field and not isinstance(receiver, dict) and observed_absence is None:
                observed_absence = {"timestamp": observed_at, "reason": "pg_stat_wal_receiver returned no row"}
            if isinstance(receiver, dict):
                receiver_pid = safe_pid(receiver.get("pid"))
                status = str(receiver.get("status", "unknown")).casefold()
                if status not in ("streaming", "starting", "stopping"):
                    status = "other"
                item = {"timestamp": observed_at, "status": status, "receiverPid": receiver_pid,
                        "sameAsSignaledReceiverPid": (receiver_pid == signaled_receiver_pid
                                                       if signaled_receiver_pid is not None else None),
                        "writtenLsn": safe_lsn(receiver.get("written_lsn") or receiver.get("writtenLsn")),
                        "flushedLsn": safe_lsn(receiver.get("flushed_lsn") or receiver.get("flushedLsn"))}
                matches.append(item)
                if (signaled_receiver_pid is not None and receiver_pid is not None
                        and receiver_pid != signaled_receiver_pid and pid_change is None):
                    pid_change = {"timestamp": observed_at, "oldReceiverPid": signaled_receiver_pid,
                                  "currentReceiverPid": receiver_pid}
    matches.sort(key=lambda item: item["timestamp"] or "")
    if kind == "primary":
        catchup = next((row for row in matches if row.get("state", "").casefold() == "catchup"), None)
        first_streaming = next((row for row in matches if row.get("state", "").casefold() == "streaming"), None)
        quorum = next((row for row in matches if row["syncQuorum"]), None)
        after_absence = next((row for row in matches
                              if observed_absence and row["timestamp"] > observed_absence["timestamp"]
                              and row.get("state", "").casefold() == "streaming"), None)
        return_state = after_absence
    else:
        catchup = None
        first_streaming = next((row for row in matches if row.get("status", "").casefold() == "streaming"), None)
        quorum = None
        after_absence = next((row for row in matches
                              if observed_absence and row["timestamp"] > observed_absence["timestamp"]
                              and row.get("status", "").casefold() == "streaming"
                              and (signaled_receiver_pid is None or
                                   row.get("receiverPid") != signaled_receiver_pid)), None)
        changed_streaming = next((row for row in matches
                                  if pid_change and row["timestamp"] >= pid_change["timestamp"]
                                  and row.get("status", "").casefold() == "streaming"
                                  and row.get("receiverPid") != signaled_receiver_pid), None)
        return_state = after_absence or changed_streaming
    observed_disconnect = observed_absence or pid_change
    if kind == "replica" and return_state and observed_disconnect:
        claim = ("receiver streaming observed after sampled receiver absence"
                 if observed_absence and after_absence else
                 "receiver PID changed from SIGTERM target and streaming was sampled"
                 if pid_change and changed_streaming else
                 "disconnect observed but no later streaming return sample")
    elif kind == "replica" and first_streaming:
        claim = "absence not observed; first post-signal streaming sample only"
    elif kind == "replica":
        claim = "no post-signal streaming sample observed"
    else:
        claim = ("primary sender streaming observed after sender absence"
                 if observed_absence and after_absence else
                 "absence not observed; first post-signal sender samples only"
                 if matches else "no post-signal primary sender samples observed")
    return {"available": True, "source": path.name, "sampleCount": len(rows),
            "sampleCountAfterSignal": len(matches),
            "firstObservedAbsence": observed_absence,
            "firstReceiverPidChange": pid_change if kind == "replica" else None,
            "firstCatchup": catchup,
            "firstStreaming": first_streaming,
            "firstSynchronousQuorum": quorum,
            "returnState": return_state,
            "returnClaimStatus": claim,
            "evidence": matches[:100]}


def seconds_between(left: dict[str, Any] | None, right: dict[str, Any] | None) -> float | None:
    if not left or not right:
        return None
    a, b = parse_time(left.get("timestamp")), parse_time(right.get("timestamp"))
    return round((b - a).total_seconds(), 3) if a and b else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-log", action="append", type=Path, required=True,
                        help="Captured PostgreSQL log file; input is read but never copied to output.")
    parser.add_argument("--intervention", type=Path,
                        help="walreceiver-intervention.json receipt for the SIGTERM lane.")
    parser.add_argument("--signal-at", help="UTC timestamp override when no intervention receipt is available.")
    parser.add_argument("--primary-pgstat", type=Path,
                        help="Primary pgstat.jsonl with replicationSenders samples.")
    parser.add_argument("--replica-pgstat", type=Path,
                        help="Replica pgstat.jsonl with walReceiver samples.")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()

    postgres_logs = list(args.postgres_log)
    live_replica_log = HERE / "results" / "diagnosis" / "private" / "live-replica-postgres.log"
    live_replica_log_included = False
    if live_replica_log.is_file() and not live_replica_log.is_symlink():
        candidate = live_replica_log.resolve()
        if any(path.resolve() == candidate for path in postgres_logs):
            live_replica_log_included = True
        else:
            postgres_logs.append(live_replica_log)
            live_replica_log_included = True

    signal = read_signal(args.intervention, args.signal_at)
    signal_time = parse_time(signal.get("atUtc"))
    events, files_read, duplicate_restore_rows = read_restore_events(postgres_logs)
    after = [item for item in events if signal_time is not None and item["time"] >= signal_time]
    successes = [item for item in events if item["outcome"] == "success"]
    post_successes = [item for item in after if item["outcome"] == "success"]
    failures = [item for item in events if item["outcome"] == "failure"]
    primary = streaming_evidence(args.primary_pgstat, signal_time, "primary")
    replica = streaming_evidence(args.replica_pgstat, signal_time, "replica",
                                 signal.get("signaledReceiverPid"))
    first_restore = ({key: value for key, value in post_successes[0].items() if key != "time"}
                     if post_successes else None)
    last_restore = ({key: value for key, value in post_successes[-1].items() if key != "time"}
                    if post_successes else None)
    unique_wals = list(dict.fromkeys(item["walName"] for item in post_successes if item.get("walName")))
    report = {
        "reportVersion": 1,
        "generatedAtUtc": dt.datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "signal": signal,
        "inputCoverage": {"postgresLogFilesRequested": len(postgres_logs),
                          "postgresLogFilesRead": files_read,
                          "liveReplicaPostgresLogIncluded": live_replica_log_included,
                          "duplicateRestoreRowsSkipped": duplicate_restore_rows,
                          "rawLogContentCopied": False,
                          "pgstatInputs": {"primary": args.primary_pgstat is not None,
                                           "replica": args.replica_pgstat is not None}},
        "archiveRestore": {
            "successfulRestoreCountAll": len(successes),
            "successfulRestoreCountAfterSignal": len(post_successes),
            "failedRestoreEventCountAll": len(failures),
            "uniquePostSignalWalCount": len(unique_wals),
            "firstPostSignalWal": post_successes[0].get("walName") if post_successes else None,
            "lastPostSignalWal": post_successes[-1].get("walName") if post_successes else None,
            "firstSuccessfulRestoreAfterSignal": first_restore,
            "lastSuccessfulRestoreAfterSignal": last_restore,
            "restoreEventsAfterSignal": [{key: value for key, value in item.items() if key != "time"}
                                          for item in after[:500]],
            "timelineGap": ("signal timestamp unavailable" if signal_time is None else
                            "no successful PostgreSQL archive-restore log events observed after signal"
                            if not post_successes else None),
        },
        "streamReturn": {
            "primarySender": primary,
            "replicaReceiver": replica,
            "secondsFromLastRestoreToFirstPrimaryStreaming": seconds_between(last_restore,
                                                                               primary.get("firstStreaming")),
            "secondsFromLastRestoreToFirstPrimarySynchronousQuorum": seconds_between(
                last_restore, primary.get("firstSynchronousQuorum")),
            "secondsFromLastRestoreToFirstReplicaReceiverStreaming": seconds_between(
                last_restore, replica.get("firstStreaming")),
            "secondsFromLastRestoreToObservedReplicaReturnState": seconds_between(
                last_restore, replica.get("returnState")),
            "secondsFromReceiverDisconnectToReturnStreaming": seconds_between(
                replica.get("firstObservedAbsence") or replica.get("firstReceiverPidChange"),
                replica.get("returnState")) if (replica.get("firstObservedAbsence") or
                                                 replica.get("firstReceiverPidChange")) else None,
            "interpretation": "streaming/quorum samples are observations after SIGTERM; this report does not infer causality from timing alone",
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.out.with_name(f".{args.out.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, args.out)
    print(json.dumps({"out": str(args.out),
                      "successfulRestoreCountAfterSignal": len(post_successes),
                      "firstPostSignalWal": report["archiveRestore"]["firstPostSignalWal"],
                      "lastPostSignalWal": report["archiveRestore"]["lastPostSignalWal"],
                      "firstPrimaryStreaming": primary.get("firstStreaming"),
                      "firstPrimarySynchronousQuorum": primary.get("firstSynchronousQuorum"),
                      "receiverReturnClaimStatus": replica.get("returnClaimStatus"),
                      "secondsFromLastRestoreToFirstPrimarySynchronousQuorum": report["streamReturn"].get(
                          "secondsFromLastRestoreToFirstPrimarySynchronousQuorum")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
