#!/usr/bin/env python3
"""Compare the treatment and control from their local, completed measurement artifacts.

The final report requires validated native pgbench logs and the actual VACUUM
start/end markers. ``--preview-incomplete`` is a read-only-data preview mode for
in-progress or failed attempts; it writes only outside ``results/`` and never
pretends that missing native logs represent zero TPS.
"""
import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


HERE = Path(__file__).resolve().parent
DEFAULT_EXPERIMENT_ROOT = HERE.parent
CLIENT_SAMPLE_GAP_SECONDS = 2.5
TELEMETRY_SAMPLE_GAP_SECONDS = 10.0
TPS_BUCKET_SECONDS = 10.0
GIB = 1024 ** 3
LANES = {"sigterm": {"label": "SIGTERM treatment", "color": "#1766ac"},
         "control": {"label": "No-SIGTERM control", "color": "#cf5c24"}}
SENDER_CODES = {"absent": 0, "catchup": 1, "streaming": 2}
SENDER_LABELS = {0: "Absent", 1: "Catchup", 2: "Streaming"}
PHASE_COLORS = ["#8da0cb", "#fc8d62", "#66c2a5", "#e78ac3", "#a6d854", "#ffd92f"]


def parse_epoch(value, label="timestamp"):
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{label} is missing or invalid")
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str):
        text = value.strip()
        try:
            result = float(text)
        except ValueError:
            stamp = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
            if stamp.tzinfo is None or stamp.utcoffset() is None:
                raise ValueError(f"{label} must include a timezone")
            result = stamp.astimezone(dt.timezone.utc).timestamp()
    else:
        raise ValueError(f"{label} has unsupported type {type(value).__name__}")
    if not math.isfinite(result):
        raise ValueError(f"{label} is not finite")
    return result


def utc_text(value):
    return dt.datetime.fromtimestamp(float(value), dt.timezone.utc).isoformat(
        timespec="microseconds").replace("+00:00", "Z")


def read_json(path, *, required=True):
    if not path.is_file():
        if required:
            raise FileNotFoundError(path)
        return None
    return json.loads(path.read_text())


def read_jsonl(path):
    rows, malformed = [], []
    if not path.is_file():
        return rows, [{"line": None, "error": f"missing {path.name}"}]
    for line_number, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("JSONL row is not an object")
            rows.append(value)
        except (json.JSONDecodeError, ValueError) as exc:
            malformed.append({"line": line_number, "error": str(exc)})
    return rows, malformed


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def parse_lsn(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9A-Fa-f]{1,8}/[0-9A-Fa-f]{1,8}", value.strip()):
        raise ValueError("missing or invalid LSN")
    high, low = value.strip().split("/")
    return (int(high, 16) << 32) + int(low, 16)


def map_controller_epoch(controller_epoch, good_rows):
    if not good_rows:
        return None
    nearest = min(good_rows, key=lambda row: abs(row["controllerEpoch"] - controller_epoch))
    return controller_epoch + nearest["dbEpoch"] - nearest["controllerEpoch"]


def load_client_observations(measurement, vacuum_start, observation_end):
    rows, malformed = read_jsonl(measurement / "client" / "sessions.jsonl")
    paired, sample_errors, invalid = [], [], []
    for index, row in enumerate(rows, 1):
        try:
            controller_epoch = parse_epoch(row.get("sampleEpoch"), f"client sample {index} sampleEpoch")
        except (ValueError, TypeError, OverflowError) as exc:
            controller_epoch = None
            timestamp_error = str(exc)
        else:
            timestamp_error = None
        if row.get("sampleStatus") != "ok":
            sample_errors.append({"sourceIndex": index, "controllerEpoch": controller_epoch,
                                  "error": str(row.get("error", "sampleStatus is not ok"))})
            continue
        try:
            db_epoch = parse_epoch(row.get("observedAtUtc"), f"client sample {index} observedAtUtc")
        except (ValueError, TypeError, OverflowError) as exc:
            invalid.append({"sourceIndex": index, "reason": str(exc)})
            continue
        if controller_epoch is None:
            invalid.append({"sourceIndex": index, "reason": timestamp_error})
            continue
        paired.append({"controllerEpoch": controller_epoch, "dbEpoch": db_epoch, "row": row, "sourceIndex": index})

    paired.sort(key=lambda row: (row["dbEpoch"], row["sourceIndex"]))
    successful = []
    for sample in paired:
        row = sample["row"]
        if not vacuum_start <= sample["dbEpoch"] <= observation_end:
            continue
        backends = row.get("backends")
        reasons = []
        if row.get("benchSessions") != 10:
            reasons.append("pgbench session count is not ten")
        if row.get("unrelatedAppSessions", 0) != 0:
            reasons.append("unrelated application session was present")
        if not isinstance(backends, list) or len(backends) != 10:
            reasons.append("sample does not contain ten writer backends")
            backends = []
        pids = [backend.get("pid") for backend in backends]
        if len(pids) != 10 or len(set(pids)) != 10 or any(isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 for pid in pids):
            reasons.append("writer PID set is not ten unique positive integers")

        sync_count = sum(backend.get("state") == "active" and backend.get("waitEvent") == "SyncRep"
                         for backend in backends)
        all_sync = len(backends) == 10 and sync_count == 10 and all(
            backend.get("state") == "active" and backend.get("waitEvent") == "SyncRep" for backend in backends)
        ages = []
        for backend in backends:
            age = backend.get("xactAgeSeconds")
            if age is None:
                if backend.get("state") == "active":
                    reasons.append(f"active backend {backend.get('pid')} has no transaction age")
                continue
            try:
                age = float(age)
            except (TypeError, ValueError):
                reasons.append(f"backend {backend.get('pid')} has invalid transaction age")
                continue
            if not math.isfinite(age) or age < 0:
                reasons.append(f"backend {backend.get('pid')} has non-finite or negative transaction age")
                continue
            ages.append(age)

        valid = not reasons
        successful.append({"epoch": sample["dbEpoch"], "minute": (sample["dbEpoch"]-vacuum_start)/60,
                           "syncRepWriters": sync_count if valid else None,
                           "allTenSyncRep": 10 if valid and all_sync else 0 if valid else None,
                           "oldestUnfinishedXactAgeSeconds": max(ages, default=0.0) if valid else None,
                           "valid": valid, "invalidReasons": reasons,
                           "writerPids": sorted(pid for pid in pids if isinstance(pid, int) and not isinstance(pid, bool)) if valid else []})

    offsets = [{"controllerEpoch": sample["controllerEpoch"], "dbEpoch": sample["dbEpoch"]} for sample in paired]
    mapped_errors = []
    for error in sample_errors:
        mapped = map_controller_epoch(error["controllerEpoch"], offsets) if error["controllerEpoch"] is not None else None
        mapped_errors.append({**error, "mappedDbEpoch": mapped, "mappedDbUtc": utc_text(mapped) if mapped is not None else None})
    in_window_errors = [error for error in mapped_errors if error.get("mappedDbEpoch") is not None and
                        vacuum_start <= error["mappedDbEpoch"] <= observation_end]
    client_gaps = gap_metadata(successful, CLIENT_SAMPLE_GAP_SECONDS, in_window_errors,
                               vacuum_start, observation_end)
    source_path = measurement / "client" / "sessions.jsonl"
    meta = {"source": str((measurement / "client" / "sessions.jsonl").name), "totalRows": len(rows),
            "sourceSha256": sha256(source_path) if source_path.is_file() else None,
            "malformedJsonRows": malformed, "timestampedSuccessfulRowsInWindow": len(successful),
            "successfulRowsWithInvalidPayload": sum(not row["valid"] for row in successful),
            "sampleErrorsInWindow": in_window_errors, "untimedOrMalformedRows": invalid,
            "gapRuleSeconds": CLIENT_SAMPLE_GAP_SECONDS, "gapMetadata": client_gaps,
            "units": {"syncRepWriters": "writers (0-10)", "allTenSyncRep": "writers (0 or 10)",
                      "oldestUnfinishedXactAgeSeconds": "seconds"}}
    return successful, meta, offsets


def gap_metadata(points, max_gap, errors=(), window_start=None, window_end=None):
    gaps = []
    if points and window_start is not None and points[0]["epoch"] > window_start:
        gaps.append({"startUtc": utc_text(window_start), "endUtc": utc_text(points[0]["epoch"]),
                     "seconds": points[0]["epoch"]-window_start, "kind": "leading unobserved interval"})
    elif not points and window_start is not None and window_end is not None and window_end > window_start:
        gaps.append({"startUtc": utc_text(window_start), "endUtc": utc_text(window_end),
                     "seconds": window_end-window_start, "kind": "no successful samples in window"})
    for left, right in zip(points, points[1:]):
        elapsed = right["epoch"] - left["epoch"]
        errors_between = [error for error in errors if error.get("mappedDbEpoch") is not None and
                          left["epoch"] < error["mappedDbEpoch"] < right["epoch"]]
        if elapsed > max_gap or errors_between or not left.get("valid", True) or not right.get("valid", True):
            gaps.append({"startUtc": utc_text(left["epoch"]), "endUtc": utc_text(right["epoch"]),
                         "seconds": elapsed, "mappedSampleErrorCount": len(errors_between),
                         "invalidEndpoint": not left.get("valid", True) or not right.get("valid", True)})
    if points and window_end is not None and points[-1]["epoch"] < window_end:
        gaps.append({"startUtc": utc_text(points[-1]["epoch"]), "endUtc": utc_text(window_end),
                     "seconds": window_end-points[-1]["epoch"], "kind": "trailing unobserved interval"})
    return {"gapLimitSeconds": max_gap, "maximumSuccessfulSampleGapSeconds": max(
                (right["epoch"]-left["epoch"] for left, right in zip(points, points[1:])), default=None),
            "maskedIntervals": gaps, "maskedIntervalCount": len(gaps)}


def load_telemetry(measurement, vacuum_start, observation_end, client_offsets, event_rows):
    rows, malformed = read_jsonl(measurement / "samples.jsonl")
    points, unplaceable = [], []
    for index, row in enumerate(rows, 1):
        primary, replica = row.get("primary"), row.get("replica")
        if not isinstance(primary, dict) or not isinstance(replica, dict):
            unplaceable.append({"sourceIndex": index, "reason": "primary/replica sample missing"})
            continue
        try:
            epoch = parse_epoch(primary.get("epoch", primary.get("ts")), f"telemetry sample {index} primary time")
        except (ValueError, TypeError, OverflowError) as exc:
            unplaceable.append({"sourceIndex": index, "reason": str(exc)})
            continue
        if not vacuum_start <= epoch <= observation_end:
            continue
        invalid = []
        try:
            flush_lsn = parse_lsn(primary.get("flushLsn"))
            replay_lsn = parse_lsn(replica.get("replayLsn"))
            delta = flush_lsn - replay_lsn
            clamped = delta < 0
            backlog = max(0, delta) / GIB
        except (ValueError, TypeError, OverflowError):
            backlog, clamped = None, False
            invalid.append("primary flush LSN or replica replay LSN missing/invalid")

        senders = primary.get("senders")
        sender_state = None
        if isinstance(senders, list):
            if len(senders) == 0:
                sender_state = "absent"
            elif len(senders) == 1 and isinstance(senders[0], dict):
                state = senders[0].get("state")
                if state in ("catchup", "streaming"):
                    sender_state = state
                else:
                    invalid.append(f"unrecognized single-sender state {state!r}")
            else:
                invalid.append(f"expected zero or one primary WAL sender, found {len(senders)}")

        progress = primary.get("vacuumProgress")
        phase = progress.get("phase") if isinstance(progress, dict) else None
        points.append({"epoch": epoch, "minute": (epoch-vacuum_start)/60,
                       "replicaReplayBacklogGiB": backlog, "senderState": sender_state,
                       "senderStateCode": SENDER_CODES.get(sender_state),
                       "negativeBacklogClamped": clamped, "vacuumProgressPhase": phase,
                       "invalidReasons": invalid, "valid": True})
    points.sort(key=lambda row: row["epoch"])

    # The controller emits sample_attempt_failed. Map it onto the primary DB
    # clock using the paired PostgreSQL/controller client monitor samples.
    offset_events = []
    for row in event_rows:
        if row.get("event") != "sample_attempt_failed":
            continue
        try:
            controller_epoch = parse_epoch(row.get("time"), "sample failure event time")
        except (ValueError, TypeError, OverflowError):
            continue
        mapped = map_controller_epoch(controller_epoch, client_offsets)
        if mapped is not None and vacuum_start <= mapped <= observation_end:
            offset_events.append({"controllerEpoch": controller_epoch, "mappedDbEpoch": mapped,
                                  "mappedDbUtc": utc_text(mapped), "phase": row.get("phase"),
                                  "error": row.get("error")})

    gaps = gap_metadata(points, TELEMETRY_SAMPLE_GAP_SECONDS, offset_events,
                        vacuum_start, observation_end)
    source_path = measurement / "samples.jsonl"
    meta = {"source": "samples.jsonl", "totalRows": len(rows), "malformedJsonRows": malformed,
            "sourceSha256": sha256(source_path) if source_path.is_file() else None,
            "unplaceableRows": unplaceable, "timestampedRowsInWindow": len(points),
            "invalidBacklogSamples": sum(point["replicaReplayBacklogGiB"] is None for point in points),
            "unknownSenderStateSamples": sum(point["senderState"] is None for point in points),
            "negativeConcurrentBacklogClampedSamples": sum(point["negativeBacklogClamped"] for point in points),
            "sampleAttemptFailuresInWindow": offset_events, "gapRuleSeconds": TELEMETRY_SAMPLE_GAP_SECONDS,
            "gapMetadata": gaps, "units": {"replicaReplayBacklogGiB": "GiB (2^30 bytes)",
                "senderState": "category: absent, catchup, streaming"}}
    return points, meta


def series_with_breaks(points, key, start, gap_limit, errors=()):
    x_values, y_values = [], []
    previous = None
    for point in points:
        if previous is not None:
            gap = point["epoch"] - previous["epoch"]
            failed_between = any(error.get("mappedDbEpoch") is not None and
                                 previous["epoch"] < error["mappedDbEpoch"] < point["epoch"] for error in errors)
            endpoint_invalid = point.get(key) is None or previous.get(key) is None
            if gap > gap_limit or failed_between or endpoint_invalid:
                x_values.append((point["epoch"] - start) / 60.0)
                y_values.append(math.nan)
        value = point.get(key)
        x_values.append((point["epoch"] - start) / 60.0)
        y_values.append(value if value is not None and math.isfinite(float(value)) else math.nan)
        previous = point
    return x_values, y_values


def read_native_transactions(measurement, timing, *, preview=False):
    raw_root = measurement / "client" / "raw"
    log_files = sorted(raw_root.glob("pgbench-log.*")) if raw_root.is_dir() else []
    stdout_path = raw_root / "pgbench.out"
    summary_path = measurement / "client-summary.json"
    summary = read_json(summary_path, required=False)
    config_path = measurement / "client-config.json"
    config = read_json(config_path, required=False)
    transactions, malformed = [], []
    for path in log_files:
        for line_no, line in enumerate(path.read_text(errors="replace").splitlines(), 1):
            fields = line.split()
            try:
                if len(fields) < 7:
                    raise ValueError("fewer than seven pgbench log columns")
                client_id, transaction_id = int(fields[0]), int(fields[1])
                latency_us, completion_sec, completion_us, schedule_lag_us = (
                    int(fields[2]), float(fields[4]), int(fields[5]), int(fields[6]))
                if not math.isfinite(completion_sec) or completion_us < 0 or completion_us >= 1_000_000:
                    raise ValueError("invalid completion timestamp")
                if min(client_id, transaction_id, latency_us, schedule_lag_us) < 0 or schedule_lag_us > latency_us:
                    raise ValueError("negative IDs/durations or schedule lag exceeds latency")
                transactions.append({"clientId": client_id, "transactionNumber": transaction_id,
                                     "completedAtEpoch": completion_sec + completion_us / 1_000_000,
                                     "latencyMs": latency_us / 1000.0,
                                     "scheduleLagMs": schedule_lag_us / 1000.0,
                                     "executionMs": (latency_us - schedule_lag_us) / 1000.0,
                                     "sourceFile": path.name, "sourceLine": line_no})
            except (ValueError, OverflowError) as exc:
                malformed.append({"file": path.name, "line": line_no, "reason": str(exc)})
    transactions.sort(key=lambda item: item["completedAtEpoch"])

    stdout_text = stdout_path.read_text(errors="replace") if stdout_path.is_file() else ""
    processed_match = re.search(r"number of transactions actually processed:\s*(\d+)", stdout_text)
    failed_match = re.search(r"number of failed transactions:\s*(\d+)", stdout_text)
    native_processed = int(processed_match.group(1)) if processed_match else None
    native_failed = int(failed_match.group(1)) if failed_match else None
    summary_errors = list((summary or {}).get("validationErrors") or [])
    errors = []
    if config is None:
        errors.append("client-config.json is missing.")
        expected_clients = 10
    else:
        expected_clients = config.get("clients")
        if expected_clients != 10:
            errors.append(f"pgbench client config expected ten clients, found {expected_clients!r}.")
        if config.get("targetTps") != 5:
            errors.append(f"pgbench target offer rate is not 5 TPS: {config.get('targetTps')!r}.")
        if config.get("synchronousCommit") != "on" or config.get("statementTimeoutSeconds") != 0:
            errors.append("pgbench config does not preserve synchronous_commit=on and statement_timeout=0.")
        if config.get("protocol") != "simple" or config.get("maxTries") != 1:
            errors.append("pgbench protocol/retry settings differ from the validated simple-protocol baseline.")
    seen_transactions = set()
    seen_clients = set()
    for transaction in transactions:
        identity = (transaction["clientId"], transaction["transactionNumber"])
        if transaction["clientId"] >= (expected_clients if isinstance(expected_clients, int) else 10):
            errors.append(f"native log has out-of-range client id {transaction['clientId']}.")
            break
        if identity in seen_transactions:
            errors.append(f"native transaction identity {identity} occurs more than once.")
            break
        seen_transactions.add(identity)
        seen_clients.add(transaction["clientId"])
    if config is not None and transactions and len(seen_clients) != expected_clients:
        errors.append(f"native log rows cover {len(seen_clients)} clients; expected {expected_clients}.")
    if not log_files:
        errors.append("No native pgbench transaction log files found.")
    if malformed:
        errors.append(f"{len(malformed)} malformed native pgbench log row(s).")
    if native_processed is None:
        errors.append("pgbench.out is missing the native completed transaction count.")
    elif native_processed != len(transactions):
        errors.append(f"pgbench stdout completed {native_processed}, but logs contain {len(transactions)} valid rows.")
    if native_failed is None:
        errors.append("pgbench.out is missing the native failed transaction count.")
    elif native_failed != 0:
        errors.append(f"pgbench reported {native_failed} failed transactions.")
    if summary is None:
        errors.append("client-summary.json is missing.")
    else:
        if summary_errors:
            errors.extend(f"client-summary validation: {error}" for error in summary_errors)
        for key, expected in (("completedTransactions", native_processed),
                              ("loggedCompletedTransactions", len(transactions)),
                              ("failedTransactions", native_failed)):
            if expected is not None and summary.get(key) != expected:
                errors.append(f"client-summary.{key}={summary.get(key)!r} disagrees with validated native evidence {expected}.")
        if summary.get("pgbenchExitCode") != 0:
            errors.append(f"pgbench exit code was {summary.get('pgbenchExitCode')!r}.")
        reconciliation = summary.get("commitLogReconciliation")
        if not isinstance(reconciliation, dict):
            errors.append("server commit/log reconciliation is missing.")
        elif (reconciliation.get("difference", 0) < 0 or
              reconciliation.get("difference", 0) > reconciliation.get("possibleInflightAtStopRequest", 0)):
            errors.append("server commit counter is not reconciled with native log rows and possible in-flight work.")

    sources = []
    evidence_paths = [*log_files, *([stdout_path] if stdout_path.is_file() else []),
                      *([summary_path] if summary_path.is_file() else []),
                      *([config_path] if config_path.is_file() else [])]
    for path in evidence_paths:
        sources.append({"file": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)})
    complete = not errors
    if not complete and not preview:
        raise RuntimeError("Native pgbench log validation failed: " + "; ".join(errors))
    if not complete:
        transactions = []
    return transactions, {"valid": complete, "previewOnly": bool(not complete and preview),
                          "rawLogFileCount": len(log_files), "validLogRows": len(transactions) if complete else None,
                          "nativeStdoutCompletedTransactions": native_processed,
                          "nativeStdoutFailedTransactions": native_failed,
                          "clientSummaryValidationErrors": summary_errors,
                          "clientConfiguration": ({key: config.get(key) for key in
                              ("tool", "version", "clients", "targetTps", "synchronousCommit",
                               "statementTimeoutSeconds", "protocol", "maxTries", "noVacuum",
                               "warmupSeconds", "stopSignal", "stopSignalSemantics")} if config else None),
                          "errors": errors, "malformedRows": malformed,
                          "logSources": sources,
                          "validationRule": "All pgbench raw log rows must parse; logged completion count must match pgbench.out and client-summary.json; pgbench failures and validation errors must be zero; server commit/log reconciliation must be present and in range."}


def find_event(rows, name):
    matched = [row for row in rows if row.get("event") == name]
    return matched[-1] if matched else None


def event_vacuum_start(events):
    for row in reversed(events):
        if row.get("event") == "vacuum_start_marker_observed":
            marker = row.get("marker") or {}
            if marker.get("time"):
                return marker["time"]
    return None


def validate_recorded_pilot_signal(measurement, intervention, signal_meta, start):
    """Validate the pilot's late OS-signal receipt against the canonical record."""
    if not intervention or intervention.get("mode") != "sigterm":
        raise RuntimeError("pilot override requires a canonical sigterm intervention")
    signal_result = intervention.get("signalResult") or {}
    if signal_result.get("signalSent") is not True or signal_result.get("signal") != "SIGTERM":
        raise RuntimeError("pilot override requires canonical signalSent=true for SIGTERM")
    deviation = intervention.get("timingDeviation")
    if not isinstance(deviation, str) or not deviation.strip():
        raise RuntimeError("pilot override requires the canonical timingDeviation explanation")
    if intervention.get("afterVacuumStartSeconds") != 600:
        raise RuntimeError("pilot override requires the canonical +600 second intervention schedule")

    scheduled_value = intervention.get("scheduledAtUtc")
    executed_value = intervention.get("executedAtUtc")
    if not scheduled_value or not executed_value:
        raise RuntimeError("pilot override requires canonical scheduledAtUtc and executedAtUtc values")
    scheduled, executed = (parse_epoch(scheduled_value, "pilot scheduledAtUtc"),
                           parse_epoch(executed_value, "pilot executedAtUtc"))
    if abs(scheduled - (start + 600)) > 1.0:
        raise RuntimeError("pilot canonical intervention schedule does not match VACUUM start + 600 seconds")
    if executed <= scheduled:
        raise RuntimeError("pilot canonical SIGTERM timestamp does not show the recorded late execution")

    receipt_path = measurement / "walreceiver-os-sigterm-receipt.json"
    receipt = read_json(receipt_path, required=False)
    if receipt is None:
        raise RuntimeError("pilot override requires walreceiver-os-sigterm-receipt.json")
    if receipt.get("returnCode") != 0 or receipt.get("priorFunctionSentSignal") is not False:
        raise RuntimeError("pilot OS receipt does not prove a successful first SIGTERM")
    if receipt.get("mechanism") != signal_result.get("mechanism") or "OS kill -TERM" not in str(receipt.get("mechanism", "")):
        raise RuntimeError("pilot OS receipt mechanism does not match the canonical intervention")
    try:
        pid = int(receipt["pid"])
        ticks = int(receipt["processStartTicks"])
        uid = int(receipt["uid"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("pilot OS receipt lacks verified PID, process start ticks, or UID") from exc
    if pid <= 0 or ticks <= 0 or uid < 0 or signal_result.get("pid") != pid:
        raise RuntimeError("pilot canonical intervention PID does not match the OS receipt")

    stdout = receipt.get("stdout")
    signal_line = re.search(r"(?m)^SIGNAL_UTC=(\S+)\s*$", str(stdout))
    pid_line = re.search(r"(?m)^SIGNAL_SENT=SIGTERM PID=(\d+) START_TICKS=(\d+)\s*$", str(stdout))
    if not signal_line or not pid_line:
        raise RuntimeError("pilot OS receipt stdout lacks its timestamped SIGTERM confirmation")
    receipt_signal_epoch = parse_epoch(signal_line.group(1), "pilot OS receipt SIGNAL_UTC")
    if abs(receipt_signal_epoch - executed) > 0.01:
        raise RuntimeError("canonical executedAtUtc does not match the timestamp in the OS SIGTERM receipt")
    if int(pid_line.group(1)) != pid or int(pid_line.group(2)) != ticks:
        raise RuntimeError("pilot OS receipt stdout PID/start ticks do not match the verified receipt fields")
    if signal_meta.get("signalEpoch") is None or abs(signal_meta["signalEpoch"] - receipt_signal_epoch) > 0.01:
        raise RuntimeError("plotted intervention timestamp does not match the OS SIGTERM receipt")
    receipt_scheduled = parse_epoch(receipt.get("scheduledAtUtc"), "pilot OS receipt scheduledAtUtc")
    if abs(receipt_scheduled - scheduled) > 0.01:
        raise RuntimeError("canonical scheduledAtUtc does not match the OS receipt")
    return {"accepted": True, "timingDeviation": deviation,
            "scheduledAtUtc": utc_text(scheduled), "executedAtUtc": utc_text(executed),
            "delayAfterScheduledSeconds": executed-scheduled,
            "osReceipt": {"file": receipt_path.name, "sha256": sha256(receipt_path),
                          "returnCode": receipt.get("returnCode"), "pid": pid,
                          "processStartTicks": ticks, "uid": uid,
                          "mechanism": receipt.get("mechanism"),
                          "priorFunctionSentSignal": receipt.get("priorFunctionSentSignal"),
                          "stdoutSignalTimestampMatchesCanonical": True,
                          "pidAndStartTicksMatchStdout": True,
                          "scheduledTimeMatchesCanonical": True}}


def resolve_timeline(measurement, lane, *, preview=False, allow_recorded_test_failure=False):
    timing = read_json(measurement / "timing.json", required=False) or {}
    events, event_parse_errors = read_jsonl(measurement / "events.jsonl")
    source = {"timingFilePresent": (measurement / "timing.json").is_file(),
              "eventsMalformedRows": event_parse_errors,
              "timingSha256": sha256(measurement / "timing.json") if (measurement / "timing.json").is_file() else None,
              "eventsSha256": sha256(measurement / "events.jsonl") if (measurement / "events.jsonl").is_file() else None}
    start_value = timing.get("vacuumStart")
    if start_value is None:
        marker = timing.get("vacuumStartMarkerObserved") or {}
        start_value = marker.get("time") if isinstance(marker, dict) else None
    if start_value is None and preview:
        start_value = event_vacuum_start(events)
        source["vacuumStartFallback"] = "events.jsonl vacuum_start_marker_observed.marker.time"
    if start_value is None:
        raise RuntimeError(f"{lane}: no actual VACUUM start marker in timing.json")
    start = parse_epoch(start_value, f"{lane} actual VACUUM start")
    if timing.get("vacuumComplete"):
        vacuum_end = parse_epoch(timing["vacuumComplete"], f"{lane} actual VACUUM completion")
    else:
        vacuum_end = None
    if timing.get("observationEnd"):
        observation_end = parse_epoch(timing["observationEnd"], f"{lane} observation end")
    elif preview:
        rows, _ = read_jsonl(measurement / "samples.jsonl")
        end_values = []
        for row in rows:
            primary = row.get("primary") or {}
            try:
                end_values.append(parse_epoch(primary.get("epoch", primary.get("ts")), "preview primary sample time"))
            except (ValueError, TypeError, OverflowError):
                continue
        observation_end = max(end_values, default=start)
        source["observationEndFallback"] = "latest placeable primary epoch in samples.jsonl"
    else:
        raise RuntimeError(f"{lane}: timing.json lacks actual observationEnd")
    if observation_end < start:
        raise RuntimeError(f"{lane}: observation end precedes VACUUM start")
    if vacuum_end is not None and (vacuum_end < start or vacuum_end > observation_end):
        raise RuntimeError(f"{lane}: actual VACUUM completion is outside the measurement window")

    intervention_path = measurement / "walreceiver-intervention.json"
    intervention = read_json(intervention_path, required=False)
    signal_epoch = None
    signal_meta = {"source": "walreceiver-intervention.json", "filePresent": intervention is not None,
                   "signalSent": False, "signalTimeUsed": None, "status": (intervention or {}).get("status")}
    signal_meta["sourceSha256"] = sha256(intervention_path) if intervention_path.is_file() else None
    if intervention:
        signal_result = intervention.get("signalResult") or {}
        signal_meta["signalSent"] = signal_result.get("signalSent") is True
        if signal_result.get("signalSent") is True:
            executed = intervention.get("executedAtUtc")
            if not executed:
                raise RuntimeError(f"{lane}: walreceiver SIGTERM is marked sent but actual executedAtUtc is missing")
            signal_epoch = parse_epoch(executed, f"{lane} actual SIGTERM executedAtUtc")
            signal_meta.update({"signalTimeUsed": executed, "signalEpoch": signal_epoch,
                                "signalSourceRule": "Only canonical intervention executedAtUtc is plotted when signalResult.signalSent is exactly true."})

    pilot_override = None
    if not preview:
        if timing.get("testComplete") is not True:
            if not allow_recorded_test_failure or lane != "sigterm":
                raise RuntimeError(f"{lane}: timing.json does not mark the run testComplete=true")
            if vacuum_end is None or timing.get("remoteVacuumExitCode") != 0:
                raise RuntimeError("pilot override requires a completed VACUUM marker and zero remote exit code")
            if observation_end - start < 8400:
                raise RuntimeError("pilot override requires at least 8400 seconds of observation after VACUUM start")
            if not timing.get("clientStop"):
                raise RuntimeError("pilot override requires a recorded client stop boundary")
            if not signal_meta["signalSent"]:
                raise RuntimeError("pilot override requires canonical signalSent=true")
            pilot_override = validate_recorded_pilot_signal(measurement, intervention, signal_meta, start)
        if timing.get("remoteVacuumExitCode") != 0:
            raise RuntimeError(f"{lane}: remote VACUUM did not finish with exit code zero")
        if vacuum_end is None:
            raise RuntimeError(f"{lane}: actual VACUUM completion marker is missing")
        expected_mode = "sigterm" if lane == "sigterm" else "control"
        if timing.get("interventionMode") != expected_mode:
            raise RuntimeError(f"{lane}: intervention mode is not {expected_mode!r}")
        if lane == "sigterm" and not signal_meta["signalSent"]:
            raise RuntimeError("sigterm run is not backed by a canonical successful SIGTERM receipt")
        if lane == "control" and signal_meta["signalSent"]:
            raise RuntimeError("control run unexpectedly has a successful SIGTERM receipt")
        if not timing.get("clientStop"):
            raise RuntimeError(f"{lane}: client stop record is missing")
    timeline = {"vacuumStartUtc": utc_text(start), "vacuumStartEpoch": start,
                "vacuumCompleteUtc": utc_text(vacuum_end) if vacuum_end is not None else None,
                "vacuumCompleteEpoch": vacuum_end, "vacuumDurationSeconds": vacuum_end-start if vacuum_end is not None else None,
                "observationEndUtc": utc_text(observation_end), "observationEndEpoch": observation_end,
                "observationDurationSeconds": observation_end-start,
                "signal": signal_meta, "timingSources": source}
    if pilot_override is not None:
        timeline["recordedTestFailureOverride"] = pilot_override
        timeline["rawTimingErrors"] = timing.get("errors")
    return timing, events, timeline


def build_tps_series(transactions, start, end):
    if end <= start:
        return []
    count = math.ceil((end-start)/TPS_BUCKET_SECONDS)
    buckets = [[] for _ in range(count)]
    for transaction in transactions:
        timestamp = transaction["completedAtEpoch"]
        if start <= timestamp < end:
            index = int((timestamp-start) // TPS_BUCKET_SECONDS)
            if 0 <= index < count:
                buckets[index].append(transaction)
    result = []
    for index, bucket in enumerate(buckets):
        bucket_start = start + index * TPS_BUCKET_SECONDS
        bucket_end = min(start + (index+1)*TPS_BUCKET_SECONDS, end)
        duration = bucket_end-bucket_start
        result.append({"startUtc": utc_text(bucket_start), "endUtc": utc_text(bucket_end),
                       "minute": ((bucket_start+bucket_end)/2-start)/60,
                       "seconds": duration, "completedTransactions": len(bucket),
                       "completedTps": len(bucket)/duration if duration > 0 else None})
    return result


def phase_intervals(points, start, end, errors):
    spans = []
    for left, right in zip(points, points[1:]):
        phase = left.get("vacuumProgressPhase")
        gap = right["epoch"] - left["epoch"]
        has_error = any(error.get("mappedDbEpoch") is not None and
                        left["epoch"] < error["mappedDbEpoch"] < right["epoch"] for error in errors)
        if phase and phase == right.get("vacuumProgressPhase") and gap <= TELEMETRY_SAMPLE_GAP_SECONDS and not has_error:
            span_start, span_end = max(start, left["epoch"]), min(end, right["epoch"])
            if span_end > span_start:
                spans.append({"phase": phase, "startMinute": (span_start-start)/60,
                              "endMinute": (span_end-start)/60, "startUtc": utc_text(span_start),
                              "endUtc": utc_text(span_end), "seconds": span_end-span_start})
    return spans


def load_lane(experiment_root, lane, *, preview=False, allow_recorded_test_failure=False):
    measurement = experiment_root / lane / "results" / "measurement"
    if not measurement.is_dir():
        raise FileNotFoundError(f"missing measurement directory for {lane}: {measurement}")
    timing, events, timeline = resolve_timeline(measurement, lane, preview=preview,
                                                allow_recorded_test_failure=allow_recorded_test_failure)
    native_transactions, native_validation = read_native_transactions(measurement, timing, preview=preview)
    start, end = timeline["vacuumStartEpoch"], timeline["observationEndEpoch"]
    client_points, client_meta, client_offsets = load_client_observations(measurement, start, end)
    telemetry_points, telemetry_meta = load_telemetry(measurement, start, end, client_offsets, events)

    if not preview:
        client_start_event = find_event(events, "client_started")
        stop = (timing.get("clientStop") or {}).get("requestedAtUtc")
        if client_start_event is None or stop is None:
            raise RuntimeError(f"{lane}: cannot verify native client interval coverage")
        try:
            client_start_controller = parse_epoch(client_start_event.get("time"), f"{lane} client start event")
            client_stop_controller = parse_epoch(stop, f"{lane} client stop request")
        except (ValueError, TypeError, OverflowError) as exc:
            raise RuntimeError(f"{lane}: invalid client active-window boundary: {exc}") from exc
        client_start_db = map_controller_epoch(client_start_controller, client_offsets)
        client_stop_db = map_controller_epoch(client_stop_controller, client_offsets)
        if client_start_db is None or client_start_db > start:
            raise RuntimeError(f"{lane}: client active-window start does not cover VACUUM start")
        if client_stop_db is None or client_stop_db < end:
            raise RuntimeError(f"{lane}: client stop request precedes observation end")
        timeline["clientCoverage"] = {"clientStartMappedDbUtc": utc_text(client_start_db),
                                       "clientStopMappedDbUtc": utc_text(client_stop_db),
                                       "coversVacuumStart": client_start_db <= start,
                                       "coversObservationEnd": client_stop_db >= end,
                                       "clockMapping": "nearest paired successful client sample offset"}

    pilot_override = timeline.get("recordedTestFailureOverride")
    if pilot_override is not None:
        if not native_validation.get("valid"):
            raise RuntimeError("pilot override requires valid native pgbench logs, zero failures, and count/reconciliation agreement")
        pilot_override["nativeLogValidation"] = {"valid": native_validation["valid"],
            "rawLogFileCount": native_validation["rawLogFileCount"],
            "validLogRows": native_validation["validLogRows"],
            "nativeStdoutCompletedTransactions": native_validation["nativeStdoutCompletedTransactions"],
            "nativeStdoutFailedTransactions": native_validation["nativeStdoutFailedTransactions"],
            "reconciliationValidated": True}
        pilot_override["rawTimingErrors"] = timing.get("errors")

    tps = build_tps_series(native_transactions, start, end) if native_validation["valid"] else []
    phase_spans = phase_intervals(telemetry_points, start, timeline["vacuumCompleteEpoch"] or end,
                                  telemetry_meta["sampleAttemptFailuresInWindow"])
    series = {"nativeCompletedTps": tps,
              "clientActivity": client_points,
              "replicationTelemetry": telemetry_points,
              "vacuumProgressPhaseSpans": phase_spans}
    return {"lane": lane, "label": LANES[lane]["label"], "color": LANES[lane]["color"],
            "timeline": timeline, "nativeLogValidation": native_validation,
            "clientSampling": client_meta, "telemetrySampling": telemetry_meta,
            "series": series}


def overview(experiment_root, *, preview=False, allow_recorded_test_failure=False):
    if preview and allow_recorded_test_failure:
        raise ValueError("pilot recorded-failure override cannot be combined with preview mode")
    lanes = [load_lane(experiment_root, lane, preview=preview,
                       allow_recorded_test_failure=allow_recorded_test_failure)
             for lane in ("sigterm", "control")]
    accepted_pilot_override = any("recordedTestFailureOverride" in run["timeline"] for run in lanes)
    return {"schemaVersion": 1, "previewOnly": bool(preview),
            "allowRecordedTestFailure": bool(accepted_pilot_override),
            "experimentRoot": str(experiment_root.resolve()),
            "createdAtUtc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "comparison": {"xAxis": "minutes from each lane's actual remote VACUUM start marker",
                           "observationWindow": "actual VACUUM start through actual observationEnd",
                           "lanes": [lane["lane"] for lane in lanes],
                           "signalMarkerRule": "Plot only walreceiver-intervention.json.executedAtUtc when signalResult.signalSent is exactly true; the timestamp is the actual intervention execution time.",
                           "controlSignalRule": "The control has no SIGTERM marker; its boundary is recorded separately in metadata.",
                           "vacuumEndRule": "VACUUM completion lines use timing.json.vacuumComplete only.",
                           "nativeTpsBucketSeconds": TPS_BUCKET_SECONDS,
                           "pilotFailureOverrideRule": ("Accepted only for a non-testComplete SIGTERM treatment with a completed VACUUM marker, zero remote VACUUM exit code, at least 8400 seconds of observation, valid native pgbench counts and server reconciliation, canonical signalSent=true, and a timestamp/PID/start-ticks-matched OS SIGTERM receipt." if accepted_pilot_override else None),
                           "nativeTpsDefinition": "Validated native pgbench completed transactions divided by the exact 10-second bucket duration; zero buckets are emitted only after native log and client-active-window validation.",
                           "waitObservationRule": "Wait counts are plotted only as point markers from actual 1-second pg_stat_activity samples; no wait state is interpolated or carried between observations. Xact-age lines break across invalid samples, failed samples, and successful-sample gaps over 2.5 seconds.",
                           "replicationObservationRule": "Replay backlog and primary sender state come from same-time paired samples. Lines are broken across invalid values, controller-observed sample failures, and successful-sample gaps over 10 seconds.",
                           "units": {"completedTps": "transactions/second", "syncRepWriters": "writers (0-10)",
                                     "allTenSyncRep": "writers (0 or 10)", "oldestUnfinishedXactAgeSeconds": "seconds",
                                     "replicaReplayBacklogGiB": "GiB (2^30 bytes)", "senderState": "categorical absent/catchup/streaming"}},
            "runs": lanes}


def _sampled_line(points, key, start, gap_limit, errors=()):
    # Include invalid/unknown points in the line construction so they force a break.
    all_points = [point for point in points if start <= point["epoch"]]
    return series_with_breaks(all_points, key, start, gap_limit, errors)


def render(data, output_png):
    output_png.parent.mkdir(parents=True, exist_ok=True)
    runs = {run["lane"]: run for run in data["runs"]}
    starts = {lane: run["timeline"]["vacuumStartEpoch"] for lane, run in runs.items()}
    end_minutes = max(run["timeline"]["observationDurationSeconds"] for run in runs.values()) / 60
    fig, axes = plt.subplots(5, 1, figsize=(14, 13), sharex=True)
    ax_tps, ax_waits, ax_xact, ax_replay, ax_sender = axes
    for lane, run in runs.items():
        color, label, start = run["color"], run["label"], starts[lane]
        timeline, series = run["timeline"], run["series"]
        vacuum_duration = timeline["vacuumDurationSeconds"]
        if vacuum_duration is not None:
            ax_tps.axvspan(0, vacuum_duration/60, color=color, alpha=.055)
            marker_x = vacuum_duration/60
            for ax in axes:
                ax.axvline(marker_x, color=color, linestyle="--" if lane == "control" else "-",
                           linewidth=1.15, alpha=.8, label=f"{lane}: VACUUM complete" if ax is ax_tps else None)

        tps = series["nativeCompletedTps"]
        if tps:
            ax_tps.step([item["minute"] for item in tps], [item["completedTps"] for item in tps],
                        where="mid", color=color, linewidth=1.5, label=label)
        else:
            ax_tps.text(.5, .82 if lane == "sigterm" else .70,
                        f"{label}: native pgbench logs unvalidated (preview)",
                        transform=ax_tps.transAxes, ha="center", color=color, fontsize=8)

        client_points = series["clientActivity"]
        client_errors = run["clientSampling"]["sampleErrorsInWindow"]
        for key, line_label, marker in (("syncRepWriters", f"{lane}: waiting SyncRep", "."),
                                        ("allTenSyncRep", f"{lane}: all 10 waiting", "x")):
            observed = [point for point in client_points if point.get(key) is not None]
            ax_waits.scatter([point["minute"] for point in observed], [point[key] for point in observed],
                             color=color, marker=marker, s=8 if marker == "." else 11, alpha=.58,
                             label=line_label, zorder=3 if marker == "x" else 2)
        x, y = _sampled_line(client_points, "oldestUnfinishedXactAgeSeconds", start,
                             CLIENT_SAMPLE_GAP_SECONDS, client_errors)
        ax_xact.plot(x, y, color=color, linewidth=1.3, marker=".", markersize=2.2, label=label)

        telemetry_points = series["replicationTelemetry"]
        sample_errors = run["telemetrySampling"]["sampleAttemptFailuresInWindow"]
        x, y = _sampled_line(telemetry_points, "replicaReplayBacklogGiB", start,
                             TELEMETRY_SAMPLE_GAP_SECONDS, sample_errors)
        ax_replay.plot(x, y, color=color, linewidth=1.3, marker=".", markersize=2.2, label=label)
        x, y = _sampled_line(telemetry_points, "senderStateCode", start,
                             TELEMETRY_SAMPLE_GAP_SECONDS, sample_errors)
        ax_sender.step(x, y, where="post", color=color, linewidth=1.4, marker=".", markersize=2.5,
                       label=label)

        phase_names = sorted({item["phase"] for item in series["vacuumProgressPhaseSpans"]})
        for span in series["vacuumProgressPhaseSpans"]:
            phase_index = phase_names.index(span["phase"])
            ax_tps.axvspan(span["startMinute"], span["endMinute"],
                           color=PHASE_COLORS[phase_index % len(PHASE_COLORS)], alpha=.09)

        signal = timeline["signal"]
        if signal.get("signalEpoch") is not None:
            signal_x = (signal["signalEpoch"] - start) / 60
            if 0 <= signal_x <= end_minutes:
                for ax in axes:
                    ax.axvline(signal_x, color="#171717", linestyle=":", linewidth=1.35,
                               label="Actual replica walreceiver SIGTERM" if ax is ax_tps else None)

    sig = runs["sigterm"]["timeline"]["signal"]
    panels = [(ax_tps, "Native client completions", "Completed transactions / second"),
              (ax_waits, "Observed client writer waits", "Writers (0-10)"),
              (ax_xact, "Oldest unfinished client transaction", "Transaction age (seconds)"),
              (ax_replay, "Replica replay backlog", "Primary flush − replica replay (GiB)"),
              (ax_sender, "Primary WAL sender state", "Sender state")]
    for axis, title, ylabel in panels:
        axis.set_title(title, loc="left", fontsize=10)
        axis.set_ylabel(ylabel)
        axis.set_yscale("linear")
        axis.grid(alpha=.2)
        axis.set_xlim(0, end_minutes)
        axis.legend(fontsize=7, loc="upper left", ncol=2, framealpha=.85)
    tps_handles, tps_labels = ax_tps.get_legend_handles_labels()
    phase_names = sorted({span["phase"] for run in runs.values() for span in run["series"]["vacuumProgressPhaseSpans"]})
    custom_handles = [Patch(facecolor=PHASE_COLORS[i % len(PHASE_COLORS)], alpha=.25,
                            label=f"VACUUM phase: {phase}") for i, phase in enumerate(phase_names)]
    if sig.get("signalEpoch") is None:
        custom_handles.append(Line2D([0], [0], color="#171717", linestyle=":",
                                     label="Treatment SIGTERM not sent (no signal marker)"))
    ax_tps.legend(tps_handles + custom_handles, tps_labels + [item.get_label() for item in custom_handles],
                  ncol=min(4, max(1, len(tps_handles) + len(custom_handles))),
                  fontsize=7.5, loc="upper right", framealpha=.9)
    ax_tps.set_ylim(bottom=0)
    ax_waits.set_ylim(-.4, 10.5); ax_waits.set_yticks(range(0, 11))
    ax_xact.set_ylim(bottom=0)
    ax_replay.set_ylim(bottom=0)
    ax_sender.set_ylim(-.25, 2.25); ax_sender.set_yticks([0, 1, 2], ["Absent", "Catchup", "Streaming"])
    ax_sender.set_xlabel("Minutes from each run's actual VACUUM start")
    if data["previewOnly"]:
        fig.suptitle("PREVIEW ONLY — incomplete pair or native client logs not validated", color="#a11", fontsize=12)
    elif data.get("allowRecordedTestFailure"):
        fig.suptitle("late-signal pilot; runner failure retained", color="#8a4b08", fontsize=12)
    else:
        fig.suptitle("Replica WAL receiver SIGTERM versus control", fontsize=12)
    fig.text(.99, .002, "Control lane has no SIGTERM marker.", ha="right", va="bottom", fontsize=8, color="#444")
    fig.tight_layout(rect=(0, .018, 1, .975))
    fig.savefig(output_png, dpi=160)
    plt.close(fig)


def write_overview(data, output_png):
    for run in data["runs"]:
        # A compact indexed series is retained in JSON so the graph can be
        # audited and rebuilt without reopening the native inputs.
        for point in run["series"]["clientActivity"]:
            point.pop("writerPids", None)
    output_json = output_png.with_suffix(".json")
    output_json.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", type=Path, default=DEFAULT_EXPERIMENT_ROOT)
    parser.add_argument("--preview-incomplete", action="store_true",
                        help="allow missing/unvalidated native logs and incomplete markers; never writes inside results/")
    parser.add_argument("--allow-recorded-test-failure", action="store_true",
                        help="accept the fully evidenced late-signal pilot despite its retained runner failure; all other completion, window, client, native-log, and receipt gates remain required")
    parser.add_argument("--output-directory", type=Path,
                        help="defaults to <experiment-root>/results, or <experiment-root>/preview in preview mode")
    args = parser.parse_args(argv)
    if args.preview_incomplete and args.allow_recorded_test_failure:
        parser.error("--allow-recorded-test-failure is a final-evidence mode and cannot be combined with --preview-incomplete")
    experiment_root = args.experiment_root.resolve()
    default_output = experiment_root / ("preview" if args.preview_incomplete else "results")
    output_directory = (args.output_directory or default_output).resolve()
    results_directory = (experiment_root / "results").resolve()
    if args.preview_incomplete and (output_directory == results_directory or results_directory in output_directory.parents):
        parser.error("preview output must be outside the final results/ directory")
    if not args.preview_incomplete and output_directory != results_directory:
        parser.error("final output is fixed at <experiment-root>/results/comparison-overview.png and .json")
    data = overview(experiment_root, preview=args.preview_incomplete,
                    allow_recorded_test_failure=args.allow_recorded_test_failure)
    output_png = output_directory / "comparison-overview.png"
    output_directory.mkdir(parents=True, exist_ok=True)
    render(data, output_png)
    write_overview(data, output_png)
    print(json.dumps({"previewOnly": data["previewOnly"],
                      "allowRecordedTestFailure": data["allowRecordedTestFailure"],
                      "image": str(output_png),
                      "json": str(output_png.with_suffix('.json')),
                      "nativeLogsValidated": {run["lane"]: run["nativeLogValidation"]["valid"] for run in data["runs"]}}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
