"""Conservative, offline normalizer for Linux diskstats rm/wm uint32 wraps.

The input rows are never mutated. The function returns a deep-copied row set
and an audit. A role/device stream is adjusted only if its entire observed
identity/counter sequence is continuous and every candidate wrap is proven.
If a stream has any reset, identity change, unusable gap, or ambiguous negative
delta, that stream is returned unchanged and listed as rejected.

This module intentionally does not read or write files and is not imported by
the measurement collectors.
"""
from __future__ import annotations

import copy
import datetime as _datetime
import math
from typing import Any, Iterable

UINT32 = 1 << 32
UPPER_QUARTER = 3 * UINT32 // 4
LOWER_QUARTER = UINT32 // 4
DEFAULT_MAX_GAP_SECONDS = 12.5
DEFAULT_QUEUE_MULTIPLIER = 2.0

_MONOTONIC_FIELDS = ("r", "rs", "w", "ws")
_TIME_FIELDS = {"rm": ("r", "rs"), "wm": ("w", "ws")}
_IDENTITY_FIELDS = (
    "majorMinor", "path", "claim", "source", "node", "pv", "lun",
    "mount", "mountMajorMinor", "sysfsDevicePath", "attachmentDevicePath",
)


def _row_samples(row: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Return role -> disk sample mappings for supported collector layouts."""
    roles = [(role, row.get(role)) for role in ("primary", "replica")]
    roles = [(role, sample) for role, sample in roles
             if isinstance(sample, dict) and isinstance(sample.get("devices"), dict)
             and isinstance(sample.get("disks"), dict)]
    if roles:
        return roles
    sample = row.get("sample")
    if isinstance(sample, dict) and isinstance(sample.get("devices"), dict) \
            and isinstance(sample.get("disks"), dict):
        return [(str(sample.get("role") or row.get("role") or "sample"), sample)]
    if isinstance(row.get("devices"), dict) and isinstance(row.get("disks"), dict):
        return [(str(row.get("role") or "sample"), row)]
    return []


def _timestamp(sample: dict[str, Any], row: dict[str, Any]) -> tuple[float | None, str | None]:
    for holder in (sample, row):
        epoch = holder.get("epoch")
        if isinstance(epoch, (int, float)) and math.isfinite(epoch):
            stamp = holder.get("ts") or holder.get("sampledAt") or holder.get("capturedAtUtc")
            return float(epoch), str(stamp) if stamp is not None else None
    for holder in (sample, row):
        for key in ("ts", "sampledAt", "capturedAtUtc"):
            value = holder.get(key)
            if isinstance(value, str):
                try:
                    parsed = _datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    continue
                return parsed.timestamp(), value
    return None, None


def _identity(device: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple((key, device.get(key)) for key in _IDENTITY_FIELDS
                 if device.get(key) is not None)


def _queue_depth(device: dict[str, Any]) -> int | None:
    queue = device.get("queue")
    if not isinstance(queue, dict):
        return None
    values = []
    for key in ("scsiDeviceQueueDepth", "nrRequests"):
        value = queue.get(key)
        if isinstance(value, int) and value > 0:
            values.append(value)
    return max(values) if values else None


def _deltas_ok(before: dict[str, Any], after: dict[str, Any]) -> tuple[bool, list[str]]:
    bad = []
    for key in _MONOTONIC_FIELDS:
        try:
            if int(after[key]) < int(before[key]):
                bad.append(key)
        except (KeyError, TypeError, ValueError):
            bad.append(f"missing:{key}")
    return not bad, bad


def normalize_rows(
    rows: Iterable[dict[str, Any]],
    *,
    max_gap_seconds: float = DEFAULT_MAX_GAP_SECONDS,
    queue_multiplier: float = DEFAULT_QUEUE_MULTIPLIER,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return ``(normalized_copy, audit)`` without changing ``rows``.

    Only ``rm`` and ``wm`` are eligible for uint32 unwrapping. A candidate is
    accepted when the device identity is stable, the adjacent sample gap is
    positive and within ``max_gap_seconds``, the before/after values fall in
    opposite uint32 boundary quarters, the corresponding completed-operation
    counter advanced, operation and sector counters are monotonic, and the
    modulo delta is within elapsed time times the observed queue bound.

    Every stream is analyzed first. If any discontinuity or unexplained
    negative delta is observed, that role/device stream remains entirely raw;
    other proven streams can still be normalized.
    """
    original = list(rows)
    normalized = copy.deepcopy(original)
    audit: dict[str, Any] = {
        "schemaVersion": 1,
        "method": "offline in-memory modulo-2^32 correction of Linux diskstats rm/wm only",
        "rawInputModified": False,
        "maxGapSeconds": max_gap_seconds,
        "queuePlausibilityMultiplier": queue_multiplier,
        "events": [],
        "rejections": [],
        "streams": [],
    }
    if max_gap_seconds <= 0 or queue_multiplier <= 0:
        raise ValueError("max_gap_seconds and queue_multiplier must be positive")
    if len(original) < 1:
        audit["status"] = "empty-input"
        return normalized, audit

    # Index all captured stream samples by (role, logical device alias). The
    # alias-to-major:minor mapping itself is part of the stability proof.
    indexed: dict[tuple[str, str], list[tuple[int, dict[str, Any], dict[str, Any], str, float | None, str | None]]] = {}
    known_streams: set[tuple[str, str]] = set()
    for index, row in enumerate(original):
        role_samples = _row_samples(row)
        for role, sample in role_samples:
            epoch, stamp = _timestamp(sample, row)
            for alias, device in sample["devices"].items():
                if not isinstance(device, dict) or not device.get("majorMinor"):
                    continue
                stream = (role, str(alias))
                known_streams.add(stream)
                mm = str(device["majorMinor"])
                disk = sample["disks"].get(mm)
                if isinstance(disk, dict):
                    indexed.setdefault(stream, []).append((index, sample, device, mm, epoch, stamp))
                else:
                    audit["rejections"].append({
                        "role": role, "device": alias, "rowIndex": index,
                        "reason": "diskstats_entry_missing_for_major_minor", "majorMinor": mm,
                    })

    for role, alias in sorted(known_streams):
        records = indexed.get((role, alias), [])
        stream_rejections: list[dict[str, Any]] = []
        stream_events: list[dict[str, Any]] = []
        if len(records) != len(original):
            stream_rejections.append({
                "role": role, "device": alias, "reason": "missing_sample_or_device_identity",
                "observedSamples": len(records), "inputRows": len(original),
            })
        offsets = {field: 0 for field in _TIME_FIELDS}
        baseline_mm = records[0][3] if records else None
        baseline_identity = _identity(records[0][2]) if records else ()

        for pos, rec in enumerate(records):
            row_index, sample, device, mm, epoch, stamp = rec
            disk = sample["disks"][mm]
            if mm != baseline_mm or _identity(device) != baseline_identity:
                stream_rejections.append({
                    "role": role, "device": alias, "rowIndex": row_index,
                    "reason": "device_identity_changed", "expectedMajorMinor": baseline_mm,
                    "observedMajorMinor": mm,
                })

            if pos:
                prior_index, prior_sample, prior_device, prior_mm, prior_epoch, prior_stamp = records[pos - 1]
                gap = None if epoch is None or prior_epoch is None else epoch - prior_epoch
                if gap is None or gap <= 0 or gap > max_gap_seconds:
                    stream_rejections.append({
                        "role": role, "device": alias, "rowIndex": row_index,
                        "previousRowIndex": prior_index, "reason": "invalid_or_long_sampling_gap",
                        "gapSeconds": gap, "maximumGapSeconds": max_gap_seconds,
                    })
                prev_disk = prior_sample["disks"][prior_mm]
                monotonic, bad_fields = _deltas_ok(prev_disk, disk)
                if not monotonic:
                    stream_rejections.append({
                        "role": role, "device": alias, "rowIndex": row_index,
                        "previousRowIndex": prior_index, "reason": "operation_or_sector_counter_reset",
                        "fields": bad_fields,
                    })

                candidate_fields = [field for field in _TIME_FIELDS
                                    if _counter_delta(disk, prev_disk, field) < 0]
                accepted_for_interval: list[dict[str, Any]] = []
                rejected_candidates: list[dict[str, Any]] = []
                # Apply a correction only when every candidate field in this
                # interval satisfies the shared continuity proof.
                for field in candidate_fields:
                    reasons = []
                    before = _safe_int(prev_disk.get(field))
                    after = _safe_int(disk.get(field))
                    raw_delta = None if before is None or after is None else after - before
                    if before is None or after is None:
                        reasons.append("missing_or_noninteger_time_counter")
                    elif not (0 <= after < UINT32 and 0 <= before < UINT32):
                        reasons.append("time_counter_outside_uint32_range")
                    elif not (before >= UPPER_QUARTER and after < LOWER_QUARTER):
                        reasons.append("not_upper_to_lower_uint32_transition")
                    if gap is None or gap <= 0 or gap > max_gap_seconds:
                        reasons.append("sampling_gap_not_short_and_valid")
                    if not monotonic:
                        reasons.append("operation_or_sector_counters_not_monotonic")
                    if _identity(device) != baseline_identity or mm != baseline_mm or \
                            _identity(prior_device) != baseline_identity or prior_mm != baseline_mm:
                        reasons.append("device_identity_not_stable")

                    op_field = _TIME_FIELDS[field][0]
                    try:
                        op_delta = int(disk[op_field]) - int(prev_disk[op_field])
                        secondary_delta = int(disk[_TIME_FIELDS[field][1]]) - int(prev_disk[_TIME_FIELDS[field][1]])
                    except (KeyError, TypeError, ValueError):
                        op_delta = secondary_delta = -1
                    if op_delta <= 0:
                        reasons.append("no_positive_completed_operations")
                    if secondary_delta < 0:
                        reasons.append("sector_counter_not_monotonic")

                    depth_values = [_queue_depth(device), _queue_depth(prior_device)]
                    depth = max((x for x in depth_values if x is not None), default=None)
                    if depth is None:
                        reasons.append("queue_depth_unavailable_for_plausibility_bound")
                    delta = None if raw_delta is None else raw_delta + UINT32
                    max_plausible = None
                    if delta is not None and gap is not None and depth is not None:
                        max_plausible = max(1000.0, gap * 1000.0 * depth * queue_multiplier)
                        if delta <= 0 or delta > max_plausible:
                            reasons.append("modulo_delta_not_plausible_for_gap_and_queue_depth")
                    candidate = {
                        "role": role, "device": alias, "rowIndex": row_index,
                        "previousRowIndex": prior_index, "counter": field,
                        "beforeTime": prior_stamp, "afterTime": stamp,
                        "seconds": gap, "majorMinor": mm,
                        "beforeValue": before, "afterValue": after,
                        "rawDelta": raw_delta, "correctedDelta": delta,
                        "deltaOperations": op_delta, "deltaSecondaryCounter": secondary_delta,
                        "queueDepthBound": depth, "maximumPlausibleDeltaMs": max_plausible,
                        "correctedAwaitMsPerOperation": (delta / op_delta) if delta and op_delta > 0 else None,
                    }
                    if reasons:
                        rejected_candidates.append({**candidate, "reason": reasons})
                    else:
                        accepted_for_interval.append(candidate)

                if rejected_candidates:
                    stream_rejections.extend(rejected_candidates)
                elif candidate_fields and accepted_for_interval:
                    stream_events.extend(accepted_for_interval)
                    for event in accepted_for_interval:
                        offsets[event["counter"]] += UINT32

        if stream_rejections:
            audit["rejections"].extend(stream_rejections)
            audit["streams"].append({
                "role": role, "device": alias, "majorMinor": baseline_mm,
                "normalizationApplied": False,
                "acceptedCandidateEventsNotApplied": len(stream_events),
                "reason": "stream_contains_unresolved_discontinuity",
            })
        else:
            audit["events"].extend(stream_events)
            # Apply cumulative offsets in a separate pass over the deep copy.
            offsets = {field: 0 for field in _TIME_FIELDS}
            event_by_row: dict[int, list[dict[str, Any]]] = {}
            for event in stream_events:
                event_by_row.setdefault(event["rowIndex"], []).append(event)
            for row_index, sample, device, mm, _epoch, _stamp in records:
                for event in event_by_row.get(row_index, []):
                    offsets[event["counter"]] += UINT32
                copied = _sample_for_role(normalized[row_index], role)
                if copied is None:
                    continue
                copied_disk = copied["disks"][mm]
                for field in _TIME_FIELDS:
                    if field in copied_disk:
                        copied_disk[field] = int(copied_disk[field]) + offsets[field]
            audit["streams"].append({
                "role": role, "device": alias, "majorMinor": baseline_mm,
                "normalizationApplied": bool(stream_events),
                "acceptedWrapEvents": len(stream_events),
            })

    audit["status"] = "rejected-ambiguous-streams" if audit["rejections"] else (
        "passed-with-corrections" if audit["events"] else "passed-no-corrections")
    audit["events"].sort(key=lambda e: (e["rowIndex"], e["role"], e["device"], e["counter"]))
    return normalized, audit


def _sample_for_role(row: dict[str, Any], role: str) -> dict[str, Any] | None:
    if role in ("primary", "replica") and isinstance(row.get(role), dict):
        return row[role]
    candidate = row.get("sample")
    if isinstance(candidate, dict) and str(candidate.get("role") or row.get("role") or "sample") == role:
        return candidate
    if isinstance(row.get("devices"), dict) and str(row.get("role") or "sample") == role:
        return row
    return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _counter_delta(after: dict[str, Any], before: dict[str, Any], field: str) -> int:
    a = _safe_int(after.get(field))
    b = _safe_int(before.get(field))
    return 0 if a is None or b is None else a - b
