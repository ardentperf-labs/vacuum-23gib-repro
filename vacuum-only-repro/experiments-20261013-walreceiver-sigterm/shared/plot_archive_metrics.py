#!/usr/bin/env python3
"""Plot optional Azure Blob WAL-archive metrics on VACUUM-relative axes.

The inputs are the native metric rows written by instrumentation/azure_metrics.py.
Azure Blob metrics have PT1M grain; duplicate points from overlapping polls are
deduplicated by complete native identity, while missing points remain gaps.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

UTC = dt.timezone.utc
BYTE_METRICS = {"Ingress", "Egress"}
LATENCY_METRICS = {"SuccessE2ELatency", "SuccessServerLatency"}
METRICS = {"Transactions", *BYTE_METRICS, *LATENCY_METRICS}
MiB = 1024 * 1024
MAX_CONTIGUOUS_GAP_SECONDS = 90.0
LANE_COLORS = {"control": "#1766ac", "sigterm": "#cf5c24"}


def parse_time(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        stamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp.astimezone(UTC) if stamp.tzinfo else stamp.replace(tzinfo=UTC)


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def read_jsonl(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    malformed: list[dict[str, Any]] = []
    try:
        stream = path.open(encoding="utf-8")
    except OSError as exc:
        return rows, [{"line": None, "error": str(exc)}]
    with stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                malformed.append({"line": line_number, "error": str(exc)})
                continue
            if isinstance(row, dict):
                row["_sourceLine"] = line_number
                rows.append(row)
            else:
                malformed.append({"line": line_number, "error": "JSONL row is not an object"})
    return rows, malformed


def parent_results(path: Path) -> Path:
    return path.parent.parent.parent


def input_sources(root: Path, explicit: list[str]) -> list[tuple[str, Path]]:
    if explicit:
        result = []
        for item in explicit:
            if "=" not in item:
                raise ValueError(f"--input must be LABEL=PATH, got {item!r}")
            label, raw_path = item.split("=", 1)
            if not label:
                raise ValueError(f"--input has an empty label: {item!r}")
            result.append((label, Path(raw_path).expanduser().resolve()))
        return result
    return [(lane, root / lane / "results/instrumentation/azure/azure-metrics.jsonl")
            for lane in ("control", "sigterm")]


def dimensions(row: dict[str, Any]) -> dict[str, str]:
    raw = row.get("dimensions") or {}
    return {str(name).casefold(): str(value) for name, value in raw.items()}


def native_dimensions(row: dict[str, Any]) -> list[dict[str, str]]:
    values = row.get("nativeDimensions")
    if isinstance(values, list):
        return [{"name": str(item.get("name", "")), "value": str(item.get("value", ""))}
                for item in values if isinstance(item, dict)]
    return [{"name": key, "value": value} for key, value in sorted(dimensions(row).items())]


def is_blob_row(row: dict[str, Any]) -> bool:
    return (row.get("targetKind") == "blob-service"
            or "/blobservices/default" in str(row.get("resourceId", "")).casefold())


def point_identity(label: str, row: dict[str, Any]) -> tuple[Any, ...] | None:
    point = row.get("point") or {}
    timestamp = parse_time(point.get("timeStamp"))
    if timestamp is None:
        return None
    return (label, row.get("resourceId"), row.get("metric"),
            json.dumps(dimensions(row), sort_keys=True, separators=(",", ":")),
            timestamp.isoformat())


def vacuum_start(results: Path) -> dt.datetime | None:
    timing = read_json(results / "measurement/timing.json")
    if timing:
        for key in ("vacuumStart", "vacuumStartUtc"):
            stamp = parse_time(timing.get(key))
            if stamp:
                return stamp
        marker = timing.get("vacuumStartMarkerObserved") or {}
        stamp = parse_time(marker.get("time")) if isinstance(marker, dict) else None
        if stamp:
            return stamp
    events, _ = read_jsonl(results / "measurement/events.jsonl")
    for row in events:
        if row.get("event") in ("vacuum_started", "vacuum_start_marker_observed"):
            marker = row.get("marker") or {}
            stamp = parse_time(marker.get("time")) or parse_time(row.get("time"))
            if stamp:
                return stamp
    return None


def signal_marker(results: Path) -> tuple[dt.datetime | None, str]:
    path = results / "measurement/walreceiver-intervention.json"
    receipt = read_json(path)
    if not receipt:
        return None, "missing intervention receipt"
    signal_result = receipt.get("signalResult") or {}
    if signal_result.get("signalSent") is not True:
        return None, "no verified SIGTERM (signalResult.signalSent is not true)"
    stamp = parse_time(receipt.get("executedAtUtc"))
    if stamp is None:
        return None, "verified SIGTERM receipt has no parseable executedAtUtc"
    return stamp, "walreceiver-intervention.json executedAtUtc; signalSent=true"


def metric_units(path: Path) -> dict[tuple[str, str], set[str]]:
    units: dict[tuple[str, str], set[str]] = defaultdict(set)
    polls, _ = read_jsonl(path.parent / "azure-metric-polls.jsonl")
    for poll in polls:
        resource_id = str(poll.get("resourceId", ""))
        response = poll.get("rawResponse") or {}
        if not isinstance(response, dict):
            continue
        for item in response.get("value", []) or []:
            name = (item.get("name") or {}).get("value")
            unit = item.get("unit")
            if name and unit:
                units[(resource_id, str(name))].add(str(unit))
    return units


def load_series(sources: list[tuple[str, Path]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicate_rows: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    metadata: dict[str, Any] = {}
    for label, path in sources:
        results = parent_results(path)
        rows, malformed = read_jsonl(path)
        raw_count = len(rows)
        rows = [row for row in rows if row.get("metric") in METRICS and is_blob_row(row)]
        metadata[label] = {
            "path": str(path), "resultsPath": str(results), "rowsRead": raw_count,
            "blobRowsRead": len(rows), "malformedRows": malformed,
            "units": metric_units(path), "vacuumStartUtc": vacuum_start(results),
            "signalTimeUtc": signal_marker(results)[0],
            "signalMarkerSource": signal_marker(results)[1],
            "definitionInventory": read_json(path.parent / "definitions-inventory.json"),
            "definitionsPath": str(path.parent / "definitions-inventory.json"),
        }
        for row in rows:
            key = point_identity(label, row)
            if key is None:
                continue
            duplicate_rows[key].append(row)
            captured = parse_time(row.get("capturedAtUtc")) or dt.datetime.min.replace(tzinfo=UTC)
            old = latest.get(key)
            old_capture = parse_time(old.get("capturedAtUtc")) if old else None
            if old is None or captured >= (old_capture or dt.datetime.min.replace(tzinfo=UTC)):
                latest[key] = {**row, "_label": label, "_nativeTime": parse_time(row["point"]["timeStamp"])}

    points = []
    conflicting_duplicates = 0
    for key, row in latest.items():
        copies = duplicate_rows[key]
        values = {json.dumps(item.get("point"), sort_keys=True) for item in copies}
        conflicting_duplicates += len(values) > 1
        row["_duplicateCount"] = len(copies) - 1
        row["_duplicatePollIds"] = sorted({str(item.get("pollId", "")) for item in copies})
        row["_duplicateValueConflict"] = len(values) > 1
        points.append(row)
    points.sort(key=lambda row: (row["_label"], str(row.get("metric")), row["_nativeTime"]))
    for label, data in metadata.items():
        data["deduplicatedPoints"] = sum(row["_label"] == label for row in points)
        data["duplicateNativePointsRemoved"] = sum(row["_label"] == label for row in points
                                                   for _ in range(row["_duplicateCount"]))
        data["duplicateValueConflicts"] = sum(row["_label"] == label and row["_duplicateValueConflict"]
                                              for row in points)
    return points, {"sources": metadata, "conflictingDuplicateIdentityCount": conflicting_duplicates}


def numeric_value(row: dict[str, Any], field: str) -> float | None:
    value = (row.get("point") or {}).get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return float(value)


def api_family(row: dict[str, Any]) -> str | None:
    api = dimensions(row).get("apiname", "")
    if api.casefold().startswith("get"):
        return "GET"
    if api.casefold().startswith("put"):
        return "PUT"
    return None


def interval_seconds(row: dict[str, Any]) -> float | None:
    value = row.get("nativeInterval")
    if value == "PT1M":
        return 60.0
    if isinstance(value, str) and value.startswith("PT") and value.endswith("S"):
        try:
            result = float(value[2:-1])
            return result if result > 0 else None
        except ValueError:
            return None
    return None


def native_unit(metadata: dict[str, Any], label: str, row: dict[str, Any]) -> str | None:
    units = metadata["sources"][label]["units"].get((str(row.get("resourceId", "")),
                                                       str(row.get("metric", ""))), set())
    return sorted(units)[0] if len(units) == 1 else None


def value_in_ms(row: dict[str, Any], metadata: dict[str, Any]) -> float | None:
    value = numeric_value(row, "average")
    if value is None:
        return None
    unit = native_unit(metadata, row["_label"], row)
    if unit is None:
        return None
    if unit.casefold() in ("millisecond", "milliseconds", "ms"):
        return value
    if unit.casefold() in ("second", "seconds", "s"):
        return value * 1000.0
    return None


def x_minutes(row: dict[str, Any], metadata: dict[str, Any]) -> float | None:
    start = metadata["sources"][row["_label"]]["vacuumStartUtc"]
    stamp = row.get("_nativeTime")
    return (stamp - start).total_seconds() / 60 if start and stamp else None


def grouped_transactions(points: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[tuple[str, str, str], dict[float, float]]:
    grouped: dict[tuple[str, str, str], dict[float, float]] = defaultdict(dict)
    for row in points:
        if row.get("metric") != "Transactions":
            continue
        family = api_family(row)
        response = dimensions(row).get("responsetype")
        value = numeric_value(row, "total")
        x = x_minutes(row, metadata)
        if family is None or not response or value is None or x is None:
            continue
        outcome = "success" if response.casefold() == "success" else "failure"
        key = (row["_label"], family, outcome)
        grouped[key][x] = grouped[key].get(x, 0.0) + value
    return grouped


def grouped_traffic(points: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[tuple[str, str], dict[float, float]]:
    grouped: dict[tuple[str, str], dict[float, float]] = defaultdict(dict)
    for row in points:
        metric = row.get("metric")
        if metric not in BYTE_METRICS:
            continue
        value = numeric_value(row, "total")
        x = x_minutes(row, metadata)
        if value is None or x is None:
            continue
        key = (row["_label"], metric)
        grouped[key][x] = grouped[key].get(x, 0.0) + value / 60.0 / MiB
    return grouped


def grouped_latency(points: list[dict[str, Any]], metadata: dict[str, Any]) -> dict[tuple[str, str, str, str, str], dict[float, float]]:
    grouped: dict[tuple[str, str, str, str, str], dict[float, float]] = defaultdict(dict)
    for row in points:
        metric = row.get("metric")
        if metric not in LATENCY_METRICS:
            continue
        value = value_in_ms(row, metadata)
        x = x_minutes(row, metadata)
        if value is None or x is None:
            continue
        dims = dimensions(row)
        api = dims.get("apiname", "unknown API")
        extra = {key: value for key, value in dims.items() if key != "apiname"}
        identity_suffix = json.dumps(extra, sort_keys=True, separators=(",", ":")) if extra else ""
        account = str(row.get("accountResourceId") or row.get("resourceId", "")).rstrip("/").split("/")[-1]
        key = (row["_label"], metric, api, identity_suffix, account)
        grouped[key][x] = value
    return grouped


def plot_series_with_gaps(ax: Any, values: dict[float, float], **kwargs: Any) -> int:
    ordered = sorted(values.items())
    if not ordered:
        return 0
    x: list[float] = []
    y: list[float] = []
    previous = None
    for minute, value in ordered:
        if previous is not None and (minute - previous) * 60.0 > MAX_CONTIGUOUS_GAP_SECONDS:
            x.append(float("nan"))
            y.append(float("nan"))
        x.append(minute)
        y.append(value)
        previous = minute
    ax.plot(x, y, marker="o", markersize=2.8, linewidth=1.45, **kwargs)
    return len(ordered)


def source_coverage(points: list[dict[str, Any]], sources: list[tuple[str, Path]],
                    metadata: dict[str, Any]) -> dict[str, Any]:
    native: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    metric_counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in points:
        label = row["_label"]
        metric = str(row.get("metric", ""))
        metric_counts[(label, metric)] += 1
        dims = native_dimensions(row)
        dim_identity = json.dumps(dims, sort_keys=True, separators=(",", ":"))
        key = (label, metric, str(row.get("resourceId", "")), dim_identity)
        item = native.setdefault(key, {"run": label, "metric": metric,
            "resourceId": row.get("resourceId"), "nativeDimensions": dims,
            "nativeInterval": row.get("nativeInterval"), "aggregation": row.get("aggregation"),
            "nativeUnits": [], "pointCount": 0, "_timestamps": [],
            "firstPointUtc": None, "lastPointUtc": None, "duplicateRowsRemoved": 0,
            "duplicateValueConflicts": 0})
        unit = native_unit(metadata, label, row)
        if unit and unit not in item["nativeUnits"]:
            item["nativeUnits"].append(unit)
        item["pointCount"] += 1
        stamp = row.get("_nativeTime")
        stamp_text = stamp.isoformat().replace("+00:00", "Z") if stamp else None
        if stamp_text:
            item["_timestamps"].append(stamp)
            item["firstPointUtc"] = min(item["firstPointUtc"], stamp_text) if item["firstPointUtc"] else stamp_text
            item["lastPointUtc"] = max(item["lastPointUtc"], stamp_text) if item["lastPointUtc"] else stamp_text
        item["duplicateRowsRemoved"] += row.get("_duplicateCount", 0)
        item["duplicateValueConflicts"] += int(row.get("_duplicateValueConflict", False))

    for item in native.values():
        timestamps = sorted(set(item.pop("_timestamps")))
        interval = 60.0 if item["nativeInterval"] == "PT1M" else None
        gaps = []
        if interval:
            for previous, current in zip(timestamps, timestamps[1:]):
                elapsed = (current - previous).total_seconds()
                if elapsed > interval * 1.5:
                    gaps.append({"afterUtc": previous.isoformat().replace("+00:00", "Z"),
                                 "beforeUtc": current.isoformat().replace("+00:00", "Z"),
                                 "missingNativeIntervals": max(1, int(round(elapsed / interval)) - 1)})
        item["nativePointTimeGaps"] = gaps
        item["observedNativeIntervalSeconds"] = interval

    source_items = {}
    missing_metrics = []
    for label, path in sources:
        data = metadata["sources"][label]
        definitions = data.get("definitionInventory") or {}
        blob_definitions = [target for target in definitions.get("targets", [])
                            if target.get("kind") == "blob-service"
                            or target.get("scope") == "blob-service"]
        advertised = set()
        for target in blob_definitions:
            advertised.update(target.get("availableMetrics") or [])
            advertised.update(target.get("metrics") or [])
        gaps = []
        for metric in sorted(METRICS - set(metric_counts_metric for run, metric_counts_metric in metric_counts if run == label)):
            if metric in advertised:
                reason = "metric advertised, but no native points were retained"
            elif definitions:
                reason = "metric not advertised for this Blob service scope"
            else:
                reason = "Azure metric definition inventory is missing"
            gaps.append({"metric": metric, "reason": reason})
            missing_metrics.append({"run": label, **gaps[-1]})
        latency_unit_gaps = []
        for metric in sorted(LATENCY_METRICS):
            retained = [row for row in points if row["_label"] == label and row.get("metric") == metric]
            unusable = [row for row in retained if numeric_value(row, "average") is not None
                        and value_in_ms(row, metadata) is None]
            if unusable:
                latency_unit_gaps.append({"metric": metric,
                    "pointCountWithoutVerifiedTimeUnit": len(unusable),
                    "nativeUnits": sorted({unit for row in unusable
                        if (unit := native_unit(metadata, label, row))}),
                    "reason": "latency points were retained but omitted from the millisecond plot because their Azure unit was unavailable or unsupported"})
        start = data["vacuumStartUtc"]
        signal = data["signalTimeUtc"]
        source_items[label] = {
            "path": str(path), "resultsPath": data["resultsPath"],
            "rowsRead": data["rowsRead"], "blobRowsRead": data["blobRowsRead"],
            "deduplicatedPointCount": data["deduplicatedPoints"],
            "duplicateNativePointsRemoved": data["duplicateNativePointsRemoved"],
            "duplicateValueConflicts": data["duplicateValueConflicts"],
            "malformedRows": data["malformedRows"],
            "vacuumStartUtc": start.isoformat().replace("+00:00", "Z") if start else None,
            "vacuumStartSource": "measurement/timing.json or measurement/events.jsonl" if start else None,
            "signalTimeUtc": signal.isoformat().replace("+00:00", "Z") if signal else None,
            "signalMarkerSource": data["signalMarkerSource"],
            "definitionsPath": data["definitionsPath"],
            "advertisedMetrics": sorted(advertised), "metricGaps": gaps,
            "latencyUnitGaps": latency_unit_gaps,
            "nativeMetricPointCounts": {metric: metric_counts.get((label, metric), 0)
                                         for metric in sorted(METRICS)},
            "nativePointIdentities": sorted((item for item in native.values() if item["run"] == label),
                key=lambda item: (item["metric"], item["resourceId"] or "",
                                  json.dumps(item["nativeDimensions"], sort_keys=True))),
        }
    return {"nativeGrain": "PT1M (Azure Blob service metrics)",
            "sourceAggregation": "native Azure values; overlapping polls deduplicated by run/resource/metric/dimensions/timeStamp",
            "sourcePoints": source_items, "missingMetrics": missing_metrics}


def chart_template(ax: Any, title: str, ylabel: str, lanes: list[str], metadata: dict[str, Any],
                   has_data: bool, missing_reason: str) -> list[dict[str, str]]:
    ax.set_title(title)
    ax.set_xlabel("Minutes since observed VACUUM start")
    ax.set_ylabel(ylabel)
    ax.set_yscale("linear")
    ax.grid(True, alpha=0.25)
    markers = []
    if not has_data:
        ax.text(0.5, 0.5, missing_reason, ha="center", va="center", transform=ax.transAxes,
                color="#555555", wrap=True)
    for lane in lanes:
        source = metadata["sources"][lane]
        start = source["vacuumStartUtc"]
        if start:
            ax.axvline(0, color=LANE_COLORS.get(lane, "#666666"), linestyle=":", alpha=0.8,
                       label=f"{lane} VACUUM start")
        signal = source["signalTimeUtc"]
        if start and signal:
            minute = (signal - start).total_seconds() / 60.0
            exact_signal = signal.isoformat(timespec="seconds").replace("+00:00", "Z")
            ax.axvline(minute, color=LANE_COLORS.get(lane, "#666666"), linestyle="--", alpha=0.8,
                       label=f"{lane} SIGTERM · {exact_signal}")
            markers.append({"run": lane, "timeUtc": signal.isoformat().replace("+00:00", "Z"),
                            "minutesAfterVacuumStart": minute,
                            "source": source["signalMarkerSource"]})
    return markers


def draw_requests(points: list[dict[str, Any]], sources: list[tuple[str, Path]],
                  metadata: dict[str, Any], out: Path) -> dict[str, Any]:
    lanes = [label for label, _ in sources]
    grouped = grouped_transactions(points, metadata)
    fig, ax = plt.subplots(figsize=(12, 5.5))
    line_count = 0
    for (lane, family, outcome), values in sorted(grouped.items()):
        linestyle = "-" if outcome == "success" else "--"
        count = plot_series_with_gaps(ax, values, color=LANE_COLORS.get(lane, "#444444"),
            linestyle=linestyle, label=f"{lane} · {family} {outcome}")
        line_count += bool(count)
    markers = chart_template(ax, "Azure Blob WAL archive requests", "Requests per native minute",
                             lanes, metadata, line_count > 0,
                             "No matching Transactions points. No request counts were inferred.")
    if line_count or markers:
        ax.legend(fontsize=8, loc="best", ncol=2)
    return save_chart(fig, ax, out / "azure-blob-requests.png", "requests", line_count, markers,
                      "Transactions Total, grouped by GET/PUT family and Success versus non-Success ResponseType",
                      "native points with numeric Total only; absent/null totals remain gaps")


def draw_traffic(points: list[dict[str, Any]], sources: list[tuple[str, Path]],
                 metadata: dict[str, Any], out: Path) -> dict[str, Any]:
    lanes = [label for label, _ in sources]
    grouped = grouped_traffic(points, metadata)
    fig, ax = plt.subplots(figsize=(12, 5.5))
    line_count = 0
    for (lane, metric), values in sorted(grouped.items()):
        linestyle = "-" if metric == "Ingress" else "--"
        count = plot_series_with_gaps(ax, values, color=LANE_COLORS.get(lane, "#444444"),
            linestyle=linestyle, label=f"{lane} · {metric}")
        line_count += bool(count)
    markers = chart_template(ax, "Azure Blob WAL archive traffic", "MiB/s (native Total bytes per minute / 60 / 2^20)",
                             lanes, metadata, line_count > 0,
                             "No matching Ingress/Egress points. No traffic values were inferred.")
    if line_count or markers:
        ax.legend(fontsize=8, loc="best", ncol=2)
    return save_chart(fig, ax, out / "azure-blob-traffic.png", "traffic", line_count, markers,
                      "Ingress/Egress Total summed across native dimension series; bytes/min converted to MiB/s",
                      "native points with numeric Total only; absent/null totals remain gaps")


def draw_latency(points: list[dict[str, Any]], sources: list[tuple[str, Path]],
                 metadata: dict[str, Any], out: Path) -> dict[str, Any]:
    lanes = [label for label, _ in sources]
    grouped = grouped_latency(points, metadata)
    fig, ax = plt.subplots(figsize=(12, 5.5))
    line_count = 0
    for (lane, metric, api, suffix, account), values in sorted(grouped.items()):
        linestyle = "-" if metric == "SuccessE2ELatency" else "--"
        metric_name = "E2E" if metric == "SuccessE2ELatency" else "server"
        identity = f" · {suffix}" if suffix else ""
        count = plot_series_with_gaps(ax, values, color=LANE_COLORS.get(lane, "#444444"),
            linestyle=linestyle, label=f"{lane} · {account} {api} {metric_name}{identity}")
        line_count += bool(count)
    markers = chart_template(ax, "Azure Blob successful-request latency", "Milliseconds (native metric unit converted only when known)",
                             lanes, metadata, line_count > 0,
                             "No latency points with a verified time unit. No latency values were inferred.")
    if line_count or markers:
        ax.legend(fontsize=7, loc="best", ncol=2)
    return save_chart(fig, ax, out / "azure-blob-success-latency.png", "successfulLatency",
                      line_count, markers,
                      "SuccessE2ELatency and SuccessServerLatency Average; separate native API/dimension identities",
                      "native Average plotted only when the Azure unit is identified as milliseconds or seconds")


def save_chart(fig: Any, ax: Any, path: Path, chart_id: str, line_count: int,
               markers: list[dict[str, str]], aggregation: str, missing_rule: str) -> dict[str, Any]:
    fig.tight_layout()
    fig.savefig(path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    return {"chart": chart_id, "image": path.name, "scale": "linear", "lineCount": line_count,
            "missingPanel": line_count == 0, "sigtermMarkers": markers,
            "aggregation": aggregation, "missingDataRule": missing_rule,
            "xAxis": "minutes since each run's observed VACUUM start"}


def build(root: Path, out: Path, explicit: list[str]) -> dict[str, Any]:
    sources = input_sources(root, explicit)
    points, loaded = load_series(sources)
    metadata = loaded
    out.mkdir(parents=True, exist_ok=True)
    coverage = {"generatedAtUtc": dt.datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "nativeGrain": "PT1M", "sources": {}}
    coverage.update(source_coverage(points, sources, metadata))
    coverage["conflictingDuplicateIdentityCount"] = loaded["conflictingDuplicateIdentityCount"]

    with (out / "azure-blob-native-points.jsonl").open("w", encoding="utf-8") as stream:
        for row in points:
            stream.write(json.dumps({key: row.get(key) for key in (
                "_label", "capturedAtUtc", "pollId", "resourceId", "accountResourceId", "metric",
                "aggregation", "nativeInterval", "dimensionNames", "dimensions", "nativeDimensions",
                "point", "_duplicateCount", "_duplicatePollIds", "_duplicateValueConflict")},
                sort_keys=True, separators=(",", ":"), default=str) + "\n")

    coverage["charts"] = {
        "requests": draw_requests(points, sources, metadata, out),
        "traffic": draw_traffic(points, sources, metadata, out),
        "successfulLatency": draw_latency(points, sources, metadata, out),
    }
    (out / "azure-blob-archive-coverage.json").write_text(
        json.dumps(coverage, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {"output": str(out), "charts": coverage["charts"],
            "nativePointCount": len(points), "missingMetrics": coverage["missingMetrics"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", type=Path, default=Path(__file__).resolve().parents[1],
                        help="experiment directory containing control/ and sigterm/")
    parser.add_argument("--output", type=Path,
                        help="output directory; defaults to ROOT/results/azure-archive-graphs")
    parser.add_argument("--input", action="append", default=[], metavar="LABEL=PATH",
                        help="explicit Azure metrics JSONL input; repeat for comparison runs")
    args = parser.parse_args(argv)
    root = args.experiment_root.expanduser().resolve()
    out = (args.output or root / "results/azure-archive-graphs").expanduser().resolve()
    summary = build(root, out, args.input)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
