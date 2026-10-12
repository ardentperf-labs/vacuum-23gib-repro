#!/usr/bin/env python3
"""Analyze downloaded VACUUM-run instrumentation after the Azure stack is gone.

Usage:
  python3 analyze-instrumentation.py \
    results/<run-id>

The input is read locally only. The script writes analysis.json and three PNG
plots into the run directory (or --output-dir). It does not require kubectl,
Azure CLI, credentials, or a live cluster.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
import statistics
import sys
from collections import Counter
from pathlib import Path
from typing import Any


UTC = dt.timezone.utc
METRIC_NAME = "VM Uncached IOPS Consumed Percentage"
DISKSTATS_MAX_INTERVAL_SECONDS = 1.5
SAMPLE_GAP_SECONDS = 1.5


def read_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.is_file():
        return None, None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"{path.name}: {exc}"
    if not isinstance(value, dict):
        return None, f"{path.name}: expected a JSON object"
    return value, None


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    meta: dict[str, Any] = {"exists": path.is_file(), "lines": 0, "invalidLines": 0}
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows, meta
    with path.open(encoding="utf-8", errors="replace") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            meta["lines"] += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                meta["invalidLines"] += 1
                continue
            if not isinstance(row, dict):
                meta["invalidLines"] += 1
                continue
            rows.append(row)
    meta["validRows"] = len(rows)
    return rows, meta


def parse_time(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: dt.datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z") if value else None


def finite_number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def summarize(values: list[float]) -> dict[str, Any]:
    clean = [float(x) for x in values if math.isfinite(float(x))]
    if not clean:
        return {"count": 0, "mean": None, "max": None, "min": None}
    return {"count": len(clean), "mean": statistics.fmean(clean), "max": max(clean), "min": min(clean)}


def span_summary(times: list[dt.datetime], threshold: float = SAMPLE_GAP_SECONDS) -> dict[str, Any]:
    ordered = sorted(times)
    gaps = [(a, b, (b - a).total_seconds()) for a, b in zip(ordered, ordered[1:])]
    long_gaps = [{"from": iso(a), "to": iso(b), "seconds": seconds}
                 for a, b, seconds in gaps if seconds > threshold]
    deltas = [x[2] for x in gaps]
    return {
        "sampleCount": len(ordered), "first": iso(ordered[0]) if ordered else None,
        "last": iso(ordered[-1]) if ordered else None,
        "intervalSeconds": {"min": min(deltas) if deltas else None,
                            "median": statistics.median(deltas) if deltas else None,
                            "max": max(deltas) if deltas else None},
        "gapThresholdSeconds": threshold, "gapsOverThreshold": len(long_gaps),
        "gapEvents": long_gaps,
    }


def value_counts(values: list[Any]) -> dict[str, int]:
    return dict(sorted(Counter("<null>" if x is None else str(x) for x in values).items()))


def analyze_pgstat(rows: list[dict[str, Any]]) -> dict[str, Any]:
    usable: list[tuple[dt.datetime, dict[str, Any]]] = []
    for row in rows:
        sampled = parse_time(row.get("serverSampledAt")) or parse_time(row.get("capturedAtUtc"))
        if sampled:
            usable.append((sampled, row))
    usable.sort(key=lambda item: item[0])

    wait_event_counts: Counter[str] = Counter()
    wait_type_counts: Counter[str] = Counter()
    activity_state_counts: Counter[str] = Counter()
    receiver_status_counts: Counter[str] = Counter()
    missing_activity = busy_samples = waiting_samples = missing_receiver_view = 0
    pid_changes: list[dict[str, Any]] = []
    status_changes: list[dict[str, Any]] = []
    wait_mode_changes: list[dict[str, Any]] = []
    previous_pids: tuple[int, ...] | None = None
    status_unset = object()
    previous_status: str | None | object = status_unset
    previous_mode: str | None = None
    all_pids: set[int] = set()
    gaps = span_summary([sampled for sampled, _ in usable])

    for sampled, row in usable:
        activity = row.get("walReceiverWait")
        if not isinstance(activity, list):
            activity = []
        activity = [x for x in activity if isinstance(x, dict)]
        receiver = row.get("walReceiver")
        if not isinstance(receiver, dict):
            receiver = None
            missing_receiver_view += 1

        if not activity:
            missing_activity += 1
            mode = "absent"
        else:
            no_wait = [x for x in activity if not x.get("waitEvent")]
            busy_samples += bool(no_wait)
            waiting_samples += len(no_wait) != len(activity)
            mode = "busy_no_wait_event" if no_wait else "waiting"
            for item in activity:
                activity_state_counts[str(item.get("state") or "<null>")] += 1
                if item.get("waitEvent"):
                    wait_event_counts[str(item["waitEvent"])] += 1
                    wait_type_counts[str(item.get("waitEventType") or "<null>")] += 1
                else:
                    wait_event_counts["<no_wait_event>"] += 1

        pids = tuple(sorted({int(x["pid"]) for x in activity
                             if x.get("pid") is not None and str(x.get("pid")).isdigit()}))
        all_pids.update(pids)
        if previous_pids is not None and pids != previous_pids:
            pid_changes.append({"time": iso(sampled), "from": list(previous_pids), "to": list(pids)})
        previous_pids = pids

        status = receiver.get("status") if receiver else None
        status_key = str(status) if status is not None else None
        receiver_status_counts[status_key or "<absent>"] += 1
        if previous_status is not status_unset and status_key != previous_status:
            status_changes.append({"time": iso(sampled), "from": previous_status, "to": status_key})
        previous_status = status_key
        if previous_mode is not None and mode != previous_mode:
            wait_mode_changes.append({"time": iso(sampled), "from": previous_mode, "to": mode})
        previous_mode = mode

    return {
        "targetIntervalSeconds": 1,
        "samples": gaps,
        "activityBackend": {
            "samplesWithoutWalReceiverBackend": missing_activity,
            "samplesWithWalReceiverBackend": len(usable) - missing_activity,
            "samplesBusyNoWaitEvent": int(busy_samples),
            "samplesWithNamedWaitEvent": int(waiting_samples),
            "sampleClassification": {
                "absent": "walReceiverWait is empty",
                "busy_no_wait_event": "at least one WAL receiver activity row has a null waitEvent",
                "waiting": "all WAL receiver activity rows have a named waitEvent",
            },
            "stateCounts": dict(sorted(activity_state_counts.items())),
            "waitEventCounts": dict(sorted(wait_event_counts.items())),
            "waitEventTypeCounts": dict(sorted(wait_type_counts.items())),
            "uniquePids": sorted(all_pids),
            "pidChangeCount": len(pid_changes),
            "pidChanges": pid_changes,
            "waitModeTransitions": wait_mode_changes,
        },
        "walReceiverView": {
            "samplesWithoutRow": missing_receiver_view,
            "statusSampleCounts": dict(sorted(receiver_status_counts.items())),
            "statusTransitions": status_changes,
        },
    }


def disk_device_mapping(coverage: dict[str, Any] | None,
                        instrumentation_preflight: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    devices = ((coverage or {}).get("replica") or {}).get("devices")
    if not isinstance(devices, dict):
        devices = (instrumentation_preflight or {}).get("devices")
    if not isinstance(devices, dict):
        return {}
    result: dict[str, dict[str, str]] = {}
    for kind in ("data", "wal"):
        device = devices.get(kind)
        if not isinstance(device, dict):
            continue
        major_minor = str(device.get("majorMinor") or "")
        name = str(device.get("device") or "")
        if re.fullmatch(r"\d+:\d+", major_minor) and name:
            result[kind] = {"majorMinor": major_minor, "device": name,
                            "path": str(device.get("path") or "")}
    return result


def disk_counter_sample(row: dict[str, Any], identity: dict[str, str]) -> tuple[dt.datetime, list[int]] | None:
    sampled = parse_time(row.get("sampledAtRemote")) or parse_time(row.get("capturedAtUtc"))
    if not sampled:
        return None
    for device in row.get("devices", []) or []:
        if not isinstance(device, dict):
            continue
        if (str(device.get("major")) + ":" + str(device.get("minor")) == identity["majorMinor"]
                and str(device.get("name")) == identity["device"]):
            counters = device.get("counters")
            if isinstance(counters, list) and len(counters) >= 11:
                try:
                    return sampled, [int(value) for value in counters]
                except (TypeError, ValueError):
                    return None
    return None


DISK_METRIC_NAMES = {
    "readIops": "read operations/s", "writeIops": "write operations/s",
    "readMBps": "decimal MB/s", "writeMBps": "decimal MB/s",
    "readAwaitMs": "ms per completed read", "writeAwaitMs": "ms per completed write",
    "averageQueueDepth": "average outstanding I/O",
}


def analyze_one_disk(identity: dict[str, str], rows: list[dict[str, Any]],
                     max_interval: float) -> dict[str, Any]:
    samples = [sample for row in rows if (sample := disk_counter_sample(row, identity)) is not None]
    samples.sort(key=lambda item: item[0])
    intervals: list[dict[str, Any]] = []
    invalid_gap_count = counter_reset_count = 0
    gap_events: list[dict[str, Any]] = []
    inflight_points = [{"time": iso(sampled), "inFlight": counters[8]}
                       for sampled, counters in samples]
    peak = max(inflight_points, key=lambda point: point["inFlight"], default=None)

    for (t0, before), (t1, after) in zip(samples, samples[1:]):
        seconds = (t1 - t0).total_seconds()
        if seconds <= 0:
            invalid_gap_count += 1
            continue
        if seconds > max_interval:
            invalid_gap_count += 1
            gap_events.append({"from": iso(t0), "to": iso(t1), "seconds": seconds})
            continue
        # /proc/diskstats counters: reads, sectors read, read ms, writes,
        # sectors written, write ms, in-flight, I/O ms, weighted I/O ms.
        indexes = (0, 2, 3, 4, 6, 7, 9, 10)
        delta = {index: after[index] - before[index] for index in indexes}
        if min(delta.values()) < 0:
            counter_reset_count += 1
            continue
        reads, read_sectors, read_ms = delta[0], delta[2], delta[3]
        writes, write_sectors, write_ms = delta[4], delta[6], delta[7]
        elapsed_ms = seconds * 1000.0
        interval = {
            "start": iso(t0), "end": iso(t1), "time": iso(t0 + (t1 - t0) / 2),
            "seconds": seconds,
            "readIops": reads / seconds, "writeIops": writes / seconds,
            "readMBps": read_sectors * 512 / 1e6 / seconds,
            "writeMBps": write_sectors * 512 / 1e6 / seconds,
            "readAwaitMs": read_ms / reads if reads else None,
            "writeAwaitMs": write_ms / writes if writes else None,
            "averageQueueDepth": delta[10] / elapsed_ms,
        }
        intervals.append(interval)

    metric_summary: dict[str, Any] = {}
    for name, units in DISK_METRIC_NAMES.items():
        metric_summary[name] = {"units": units, **summarize(
            [float(x[name]) for x in intervals if x.get(name) is not None])}
    sample_span = span_summary([sampled for sampled, _ in samples], max_interval)
    sample_span["gapEvents"] = gap_events
    sample_span["invalidIntervals"] = invalid_gap_count
    sample_span["counterResetIntervals"] = counter_reset_count
    return {
        "device": identity,
        "sampling": sample_span,
        "intervalCount": len(intervals),
        "inFlight": {"units": "outstanding I/Os", "sampleCount": len(inflight_points),
                     "peak": peak["inFlight"] if peak else None,
                     "peakTime": peak["time"] if peak else None,
                     "timeSeries": inflight_points},
        "metrics": metric_summary,
        "timeSeries": intervals,
    }


def analyze_diskstats(rows: list[dict[str, Any]], mapping: dict[str, dict[str, str]],
                      max_interval: float = DISKSTATS_MAX_INTERVAL_SECONDS) -> dict[str, Any]:
    devices = {kind: analyze_one_disk(identity, rows, max_interval)
               for kind, identity in mapping.items() if kind in ("data", "wal")}
    return {
        "targetIntervalSeconds": 1,
        "maximumRateIntervalSeconds": max_interval,
        "rateIntervalRule": f"Rates use monotonic device counter deltas only when the interval is positive and at most {max_interval:g} seconds; larger gaps and counter resets are excluded.",
        "counterUnits": {"sectors": "512 bytes", "await": "delta read/write milliseconds divided by completed operations",
                         "averageQueueDepth": "delta weighted I/O milliseconds divided by delta elapsed milliseconds"},
        "sourceRows": len(rows), "devices": devices,
    }


def dimensions_map(row: dict[str, Any]) -> dict[str, str]:
    dims = row.get("dimensions")
    if not isinstance(dims, dict):
        return {}
    return {str(key).casefold(): str(value) for key, value in dims.items() if value is not None}


def metric_identity(coverage: dict[str, Any] | None,
                    preflight: dict[str, Any] | None) -> dict[str, Any]:
    azure = (coverage or {}).get("azureMonitor") or {}
    preflight = preflight or {}
    return {
        "resourceId": preflight.get("azureMetricResourceId") or azure.get("resourceId"),
        "resourceScope": preflight.get("azureMetricResourceScope") or azure.get("resourceScope"),
        "vmNameDimension": preflight.get("azureVmNameDimension") or azure.get("vmNameDimension"),
        "nativeInterval": azure.get("nativeInterval") or "PT1M",
    }


def metric_point_summary(points: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "bucketCount": len(points),
        "averagePercent": summarize([x["averagePercent"] for x in points]),
        "minimumPercent": summarize([x["minimumPercent"] for x in points if x.get("minimumPercent") is not None]),
        "maximumPercent": summarize([x["maximumPercent"] for x in points if x.get("maximumPercent") is not None]),
        "points": points,
    }


def analyze_azure(rows: list[dict[str, Any]], identity: dict[str, Any], timing: dict[str, Any] | None) -> dict[str, Any]:
    scope = str(identity.get("resourceScope") or "")
    resource_id = str(identity.get("resourceId") or "")
    vm_name = str(identity.get("vmNameDimension") or "")
    if scope == "vmss-filtered":
        exact_scope_proven = bool(resource_id and vm_name)
        proof = "VMSS resource query filtered by the exact VMName dimension recorded in preflight"
    elif scope in ("vm", "vmss-instance"):
        exact_scope_proven = bool(resource_id and re.search(r"/virtualMachines/[^/]+$", resource_id, re.I))
        proof = "Azure metric request used the exact virtual-machine instance resource ID recorded in preflight"
    else:
        exact_scope_proven = False
        proof = "preflight does not identify an exact VM metric resource scope"

    candidates: list[dict[str, Any]] = []
    rejected_wrong_vm = rejected_invalid = rejected_metric = 0
    for row in rows:
        if row.get("metric") != METRIC_NAME:
            rejected_metric += 1
            continue
        timestamp = parse_time(row.get("timeStamp"))
        average = finite_number(row.get("averagePercent"))
        if not timestamp or average is None or not 0 <= average <= 100:
            rejected_invalid += 1
            continue
        dims = dimensions_map(row)
        observed_vm = dims.get("vmname")
        if vm_name and observed_vm is not None and observed_vm.casefold() != vm_name.casefold():
            rejected_wrong_vm += 1
            continue
        if scope == "vmss-filtered" and (observed_vm is None or observed_vm.casefold() != vm_name.casefold()):
            rejected_wrong_vm += 1
            continue
        if not exact_scope_proven:
            rejected_invalid += 1
            continue
        candidates.append({
            "time": iso(timestamp), "epoch": timestamp.timestamp(), "averagePercent": average,
            "minimumPercent": finite_number(row.get("minimumPercent")),
            "maximumPercent": finite_number(row.get("maximumPercent")),
            "capturedAtUtc": row.get("capturedAtUtc"), "pollId": row.get("pollId"),
            "dimensions": row.get("dimensions") or {},
        })

    # Azure can revise an open native minute between polls. Keep the latest
    # captured version for each timestamp instead of counting revisions twice.
    by_bucket: dict[str, dict[str, Any]] = {}
    duplicate_revisions = 0
    for point in candidates:
        timestamp = point["time"]
        old = by_bucket.get(timestamp)
        if old is None:
            by_bucket[timestamp] = point
            continue
        duplicate_revisions += 1
        old_capture = parse_time(old.get("capturedAtUtc"))
        new_capture = parse_time(point.get("capturedAtUtc"))
        if new_capture and (not old_capture or new_capture >= old_capture):
            by_bucket[timestamp] = point
    points = sorted(by_bucket.values(), key=lambda item: item["epoch"])
    for point in points:
        point.pop("epoch", None)

    vacuum_start = parse_time((timing or {}).get("vacuumStart"))
    vacuum_end = parse_time((timing or {}).get("vacuumComplete"))
    window_points = []
    if vacuum_start and vacuum_end and vacuum_end > vacuum_start:
        for point in points:
            bucket_start = parse_time(point["time"])
            if bucket_start and bucket_start < vacuum_end and bucket_start + dt.timedelta(minutes=1) > vacuum_start:
                window_points.append(point)

    return {
        "metric": METRIC_NAME, "units": "percent", "nativeInterval": identity.get("nativeInterval"),
        "resourceId": resource_id or None, "resourceScope": scope or None,
        "vmNameDimension": vm_name or None,
        "exactVmIdentity": {"proven": exact_scope_proven, "basis": proof,
                            "dimensionChecksApplied": bool(vm_name),
                            "validRowsWithDifferentVmNameRejected": rejected_wrong_vm},
        "inputRows": len(rows), "rejectedRows": {"wrongVm": rejected_wrong_vm,
                                                    "invalidTimeOrPercentage": rejected_invalid,
                                                    "unexpectedMetric": rejected_metric},
        "revisedDuplicateBucketsCollapsed": duplicate_revisions,
        "latestValidNativeBuckets": metric_point_summary(points),
        "vacuumWindow": {
            "start": iso(vacuum_start), "end": iso(vacuum_end),
            "selection": "native one-minute buckets whose [timeStamp, timeStamp + 1 minute) overlaps the observed VACUUM interval",
            **metric_point_summary(window_points),
        },
    }


def parse_role_queue_log(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"available": False, "source": path.name, "error": "log not found"}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    def get_int(key: str) -> int | None:
        try:
            return int(values[key])
        except (KeyError, ValueError):
            return None

    devices: dict[str, Any] = {}
    for kind in ("DATA", "WAL"):
        prefix = kind + "_"
        queues = []
        queue_ids = sorted({
            int(hit.group(1)) for key in values
            if (hit := re.fullmatch(re.escape(prefix) + r"mq_(\d+)_cpu_list", key))
        })
        for queue_id in queue_ids:
            base = f"{prefix}mq_{queue_id}_"
            queues.append({
                "queueId": queue_id, "cpuList": values.get(base + "cpu_list"),
                "nrTags": get_int(base + "nr_tags"),
                "nrReservedTags": get_int(base + "nr_reserved_tags"),
            })
        devices[kind.lower()] = {
            "device": values.get(prefix + "device"),
            "majorMinor": values.get(prefix + "major_minor"),
            "controller": {
                "type": values.get(prefix + "controller_type"),
                "name": values.get(prefix + "controller"),
                "driver": values.get(prefix + "controller_driver"),
                "model": values.get(prefix + "controller_model"),
                "firmware": values.get(prefix + "controller_firmware_rev"),
                "state": values.get(prefix + "controller_state"),
                "transport": values.get(prefix + "controller_transport"),
                "address": values.get(prefix + "controller_address"),
                "path": values.get(prefix + "controller_path"),
                "hardwareQueueCount": get_int(prefix + "controller_queue_count"),
                "submissionQueueSize": get_int(prefix + "controller_sqsize"),
            },
            "blockQueue": {
                "nrRequests": get_int(prefix + "queue_nr_requests"),
                "scheduler": values.get(prefix + "queue_scheduler"),
                "maxSectorsKb": get_int(prefix + "queue_max_sectors_kb"),
                "maxHwSectorsKb": get_int(prefix + "queue_max_hw_sectors_kb"),
                "readAheadKb": get_int(prefix + "queue_read_ahead_kb"),
                "rotational": get_int(prefix + "queue_rotational"),
                "blkMqReportedCount": get_int(prefix + "mq_count"),
                "blkMqObservedCount": len(queues),
                "blkMq": queues,
            },
        }
    data_controller = devices["data"]["controller"]
    wal_controller = devices["wal"]["controller"]
    shared_controller = bool(data_controller.get("path") and
                             data_controller.get("path") == wal_controller.get("path"))
    return {
        "available": bool(values), "source": path.name,
        "onlineVcpus": get_int("online_vcpus"), "cpuOnline": values.get("cpu_online"),
        "kernel": values.get("kernel"), "devices": devices,
        "dataAndWalShareController": shared_controller,
    }


def parse_meminfo_log(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"available": False}
    text = path.read_text(encoding="utf-8", errors="replace")
    result: dict[str, Any] = {"available": True}
    for section, marker in (("before", "=== GUEST_CACHE_BEFORE ==="),
                            ("after", "=== GUEST_CACHE_AFTER ===")):
        if marker not in text:
            continue
        body = text.split(marker, 1)[1]
        if "===" in body:
            body = body.split("===", 1)[0]
        fields: dict[str, Any] = {}
        for line in body.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                fields[key.strip()] = value.strip()
            else:
                hit = re.match(r"^([^:]+):\s*(\d+)\s*kB", line)
                if hit:
                    fields[hit.group(1)] = int(hit.group(2))
        result[section] = fields
    return result


def cache_and_node_context(run_dir: Path, coverage: dict[str, Any] | None,
                           preflight: dict[str, Any] | None,
                           instrumentation_preflight: dict[str, Any] | None) -> dict[str, Any]:
    cache_meta, _ = read_json(run_dir / "guest-cache-clearing.json")
    controller = {
        role: parse_role_queue_log(run_dir / f"{role}-controller-queues.log")
        for role in ("primary", "replica")
    }
    cache_roles = (cache_meta or {}).get("roles") or {}
    cache = {}
    for role in ("primary", "replica"):
        role_meta = cache_roles.get(role) or {}
        log_path = run_dir / f"{role}-guest-cache.log"
        cache[role] = {
            "node": role_meta.get("node"), "vmResourceId": role_meta.get("vmResourceId"),
            "guestCacheClearTimesUtc": role_meta.get("guestCacheClearTimesUtc", []),
            "guestClearExitCode": role_meta.get("guestClearExitCode"),
            "log": parse_meminfo_log(log_path),
        }
    vm_size = (cache_meta or {}).get("nodeVmSize")
    role_comparison = {
        "onlineVcpusMatch": controller["primary"].get("onlineVcpus") == controller["replica"].get("onlineVcpus"),
        "cpuOnlineMaskMatch": controller["primary"].get("cpuOnline") == controller["replica"].get("cpuOnline"),
        "kernelMatch": controller["primary"].get("kernel") == controller["replica"].get("kernel"),
        "nodeVmSize": vm_size,
        "guestCacheClearCompletedForBoth": all(cache_roles.get(role, {}).get("guestClearExitCode") == 0
                                                for role in ("primary", "replica")),
        "azureReadOnlyHostCachePurged": (cache_meta or {}).get("azureReadOnlyHostCachePurged"),
        "hostCacheNote": (cache_meta or {}).get("hostCacheNote"),
    }
    measured_at = ((coverage or {}).get("startedAtUtc") or
                   (instrumentation_preflight or {}).get("readyAtUtc") or
                   (((preflight or {}).get("sql") or {}).get("replica") or {}).get("sampledAt"))
    measured_dt = parse_time(measured_at)
    if measured_dt:
        for role in ("primary", "replica"):
            clear_times = [parse_time(x) for x in cache[role]["guestCacheClearTimesUtc"]]
            clear_times = [x for x in clear_times if x]
            latest = max(clear_times) if clear_times else None
            cache[role]["secondsFromLastGuestClearToInstrumentationStart"] = (
                (measured_dt - latest).total_seconds() if latest else None)
    return {
        "controllerAndCpuByRole": controller,
        "guestCacheByRole": cache,
        "guestCachePreparation": {key: value for key, value in (cache_meta or {}).items()
                                  if key != "roles"},
        "roleComparison": role_comparison,
        "comparisonCaveat": "Controller, guest CPU, and guest-cache preparation are reported as observed run conditions. This run alone does not establish that a controller type caused a latency difference.",
    }


def measurement_context(run_dir: Path, timing: dict[str, Any] | None,
                        summary: dict[str, Any] | None,
                        preflight: dict[str, Any] | None,
                        sample_rows: list[dict[str, Any]],
                        input_meta: dict[str, Any]) -> dict[str, Any]:
    if not timing and not summary and not preflight and not sample_rows:
        return {"available": False, "status": "missing",
                "note": "No 03-test.py measurement artifacts were found; no VACUUM result can be inferred."}
    status = (summary or {}).get("status") or {}
    return {
        "available": True,
        "status": "complete" if (timing or {}).get("testComplete") is True else
                 "incomplete" if (timing or {}).get("testComplete") is False else "unknown",
        "testComplete": (timing or {}).get("testComplete"),
        "vacuumExitCode": (timing or {}).get("remoteVacuumExitCode", status.get("vacuumExitCode")),
        "vacuumStart": (timing or {}).get("vacuumStart"),
        "vacuumComplete": (timing or {}).get("vacuumComplete"),
        "vacuumDurationSeconds": (
            (parse_time((timing or {}).get("vacuumComplete")) - parse_time((timing or {}).get("vacuumStart"))).total_seconds()
            if parse_time((timing or {}).get("vacuumComplete")) and parse_time((timing or {}).get("vacuumStart")) else None),
        "replicaCaughtUp": (timing or {}).get("replayCaughtUp", status.get("replayCaughtUp")),
        "replayEnd": (timing or {}).get("replayEnd"),
        "observationEnd": (timing or {}).get("observationEnd"),
        "error": status.get("error") or (timing or {}).get("postVacuumObservationError"),
        "measurementSampleRows": len(sample_rows),
        "measurementSamplesFile": input_meta.get("measurementSamples", {}),
        "preflightPresent": preflight is not None,
        "summaryPresent": summary is not None,
    }


def add_vacuum_window_summaries(pgstat: dict[str, Any], diskstats: dict[str, Any],
                                pg_rows: list[dict[str, Any]],
                                timing: dict[str, Any] | None) -> None:
    start = parse_time((timing or {}).get("vacuumStart"))
    end = parse_time((timing or {}).get("vacuumComplete"))
    window: dict[str, Any] = {"available": bool(start and end and end > start),
                              "start": iso(start), "end": iso(end)}
    if not start or not end or end <= start:
        pgstat["vacuumWindow"] = window
        diskstats["vacuumWindow"] = window
        return

    selected_pg = []
    for row in pg_rows:
        sampled = parse_time(row.get("serverSampledAt")) or parse_time(row.get("capturedAtUtc"))
        if sampled and start <= sampled <= end:
            selected_pg.append(row)
    pgstat["vacuumWindow"] = {**window, **analyze_pgstat(selected_pg)}

    per_device: dict[str, Any] = {}
    for kind, device in (diskstats.get("devices") or {}).items():
        intervals = [point for point in device.get("timeSeries", [])
                     if (sampled := parse_time(point.get("time"))) and start <= sampled < end]
        metrics = {}
        for name, units in DISK_METRIC_NAMES.items():
            metrics[name] = {"units": units, **summarize(
                [float(point[name]) for point in intervals if point.get(name) is not None])}
        in_flight = [point for point in device.get("inFlight", {}).get("timeSeries", [])
                     if (sampled := parse_time(point.get("time"))) and start <= sampled <= end]
        peak = max(in_flight, key=lambda point: point["inFlight"], default=None)
        per_device[kind] = {
            "intervalCount": len(intervals), "metrics": metrics,
            "inFlightSampleCount": len(in_flight),
            "inFlightPeak": peak.get("inFlight") if peak else None,
            "inFlightPeakTime": peak.get("time") if peak else None,
        }
    diskstats["vacuumWindow"] = {**window, "devices": per_device,
                                 "selection": "diskstats rate intervals by midpoint within VACUUM; in-flight samples by sample time within VACUUM"}


def render_plots(output_dir: Path, diskstats: dict[str, Any], azure: dict[str, Any]) -> list[str]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    output_dir.mkdir(parents=True, exist_ok=True)
    created: list[str] = []
    devices = diskstats.get("devices", {})
    fig, ax = plt.subplots(figsize=(11, 5.5))
    for kind, label in (("data", "DATA"), ("wal", "WAL")):
        intervals = devices.get(kind, {}).get("timeSeries", [])
        x = [parse_time(point.get("time")) for point in intervals]
        y = [finite_number(point.get("averageQueueDepth")) for point in intervals]
        points = [(time, value) for time, value in zip(x, y) if time and value is not None]
        if points:
            ax.plot([p[0] for p in points], [p[1] for p in points], label=label, linewidth=1.2)
    ax.set_title("Replica disk average queue depth")
    ax.set_ylabel("Average outstanding I/O")
    ax.set_xlabel("UTC")
    ax.set_yscale("linear")
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=.25)
    if ax.lines:
        ax.legend()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S", tz=UTC))
    else:
        ax.text(.5, .5, "No valid diskstats intervals", transform=ax.transAxes, ha="center", va="center")
        ax.set_ylim(0, 1)
    fig.autofmt_xdate()
    fig.tight_layout()
    name = "replica-average-queue-depth.png"
    fig.savefig(output_dir / name, dpi=150)
    plt.close(fig)
    created.append(name)

    fig, ax = plt.subplots(figsize=(11, 5.5))
    for kind, label in (("data", "DATA"), ("wal", "WAL")):
        samples = devices.get(kind, {}).get("inFlight", {}).get("timeSeries", [])
        points = [(parse_time(point.get("time")), finite_number(point.get("inFlight"))) for point in samples]
        points = [(time, value) for time, value in points if time and value is not None]
        if points:
            ax.step([p[0] for p in points], [p[1] for p in points], where="post", label=label, linewidth=1.0)
    ax.set_title("Replica disk in-flight I/O")
    ax.set_ylabel("Outstanding I/O at sample time")
    ax.set_xlabel("UTC")
    ax.set_yscale("linear")
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=.25)
    if ax.lines:
        ax.legend()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S", tz=UTC))
    else:
        ax.text(.5, .5, "No diskstats samples", transform=ax.transAxes, ha="center", va="center")
        ax.set_ylim(0, 1)
    fig.autofmt_xdate()
    fig.tight_layout()
    name = "replica-inflight-io.png"
    fig.savefig(output_dir / name, dpi=150)
    plt.close(fig)
    created.append(name)

    metric = azure.get("vacuumWindow") or {}
    points = metric.get("points") or []
    parsed = [(parse_time(point.get("time")), finite_number(point.get("averagePercent")),
               finite_number(point.get("minimumPercent")), finite_number(point.get("maximumPercent")))
              for point in points]
    parsed = [point for point in parsed if point[0] and point[1] is not None]
    fig, ax = plt.subplots(figsize=(11, 5.5))
    if parsed:
        x = [point[0] for point in parsed]
        avg = [point[1] for point in parsed]
        ax.plot(x, avg, marker="o", label="native 1m average", linewidth=1.2)
        low_high = [(point[2], point[3]) for point in parsed]
        if all(low is not None and high is not None for low, high in low_high):
            ax.fill_between(x, [pair[0] for pair in low_high], [pair[1] for pair in low_high],
                            alpha=.18, label="native 1m minimum–maximum")
        ax.legend()
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=UTC))
    else:
        ax.text(.5, .5, "No valid exact-VM Azure metric bucket overlaps VACUUM",
                transform=ax.transAxes, ha="center", va="center")
    ax.set_title("Replica VM uncached IOPS consumed percentage during VACUUM")
    ax.set_ylabel("Uncached IOPS consumed (%)")
    ax.set_xlabel("Native Azure metric bucket (UTC)")
    ax.set_yscale("linear")
    ax.set_ylim(0, 100)
    ax.grid(True, alpha=.25)
    fig.autofmt_xdate()
    fig.tight_layout()
    name = "replica-azure-uncached-iops-percent.png"
    fig.savefig(output_dir / name, dpi=150)
    plt.close(fig)
    created.append(name)
    return created


def analyze(run_dir: Path, output_dir: Path, max_interval: float) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    measurement_dir = run_dir / "measurement"
    instrumentation_dir = run_dir / "replica-instrumentation"
    timing, err_timing = read_json(measurement_dir / "timing.json")
    summary, err_summary = read_json(measurement_dir / "summary.json")
    measurement_preflight, err_preflight = read_json(measurement_dir / "preflight.json")
    coverage, err_coverage = read_json(instrumentation_dir / "coverage.json")
    collector_status, err_status = read_json(instrumentation_dir / "collector-status.json")
    instrumentation_preflight, err_ip = read_json(instrumentation_dir / "preflight.json")
    measurement_rows, measurement_meta = read_jsonl(measurement_dir / "samples.jsonl")
    pg_rows, pg_meta = read_jsonl(instrumentation_dir / "pgstat-receiver.jsonl")
    disk_rows, disk_meta = read_jsonl(instrumentation_dir / "diskstats.jsonl")
    azure_rows, azure_meta = read_jsonl(instrumentation_dir / "azure-vm-uncached-iops.jsonl")

    input_errors = [x for x in (err_timing, err_summary, err_preflight, err_coverage, err_status, err_ip) if x]
    mapping = disk_device_mapping(coverage, instrumentation_preflight)
    timing_map = timing or {}
    diskstats = analyze_diskstats(disk_rows, mapping, max_interval)
    pgstat = analyze_pgstat(pg_rows)
    add_vacuum_window_summaries(pgstat, diskstats, pg_rows, timing)
    identity = metric_identity(coverage, instrumentation_preflight)
    azure = analyze_azure(azure_rows, identity, timing_map)
    context = cache_and_node_context(run_dir, coverage, measurement_preflight, instrumentation_preflight)
    source_counts = {
        "measurementSamples": measurement_meta, "pgstatReceiver": pg_meta,
        "diskstats": disk_meta, "azureUncachedIops": azure_meta,
    }
    report: dict[str, Any] = {
        "schemaVersion": 1,
        "runDirectory": str(run_dir),
        "generatedAtUtc": iso(dt.datetime.now(UTC)),
        "measurement": measurement_context(run_dir, timing, summary, measurement_preflight,
                                            measurement_rows, source_counts),
        "instrumentation": {
            "collectorPhase": (collector_status or {}).get("phase"),
            "collectorFatalErrors": (collector_status or {}).get("fatalErrors", []),
            "coverage": coverage,
            "sourceRows": source_counts,
        },
        "replicaWalReceiver": pgstat,
        "replicaDiskstats": diskstats,
        "replicaVmUncachedIopsConsumedPercent": azure,
        "runConditions": context,
        "inputIssues": input_errors,
        "notes": [
            "No VACUUM result is inferred when measurement/timing.json is absent.",
            "Azure values are exact-VM native one-minute averages. Revised duplicate buckets use the latest captured revision.",
            "Diskstats one-second rates exclude intervals longer than the configured 1.5 second limit and counter resets.",
            "All generated plot y-axes are linear.",
        ],
    }
    names = render_plots(output_dir, report["replicaDiskstats"], azure)
    report["artifacts"] = {"analysis": "analysis.json", "plots": names}
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / "analysis.json"
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    tmp.replace(target)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path, help="downloaded results/<run-id> directory")
    parser.add_argument("--output-dir", type=Path, help="defaults to run_dir")
    parser.add_argument("--max-disk-interval", type=float, default=DISKSTATS_MAX_INTERVAL_SECONDS,
                        help="maximum seconds between disk counter samples accepted for rates (default: 1.5)")
    args = parser.parse_args()
    if args.max_disk_interval <= 0:
        parser.error("--max-disk-interval must be positive")
    if not args.run_dir.is_dir():
        parser.error(f"run directory does not exist: {args.run_dir}")
    output_dir = (args.output_dir or args.run_dir).resolve()
    report = analyze(args.run_dir, output_dir, args.max_disk_interval)
    measurement = report["measurement"]
    print(json.dumps({"analysis": str(output_dir / "analysis.json"),
                      "measurementStatus": measurement.get("status"),
                      "walReceiverSamples": report["replicaWalReceiver"]["samples"]["sampleCount"],
                      "diskstatsRows": report["replicaDiskstats"]["sourceRows"],
                      "exactAzureMetricBuckets": report["replicaVmUncachedIopsConsumedPercent"]["latestValidNativeBuckets"]["bucketCount"],
                      "plots": report["artifacts"]["plots"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
