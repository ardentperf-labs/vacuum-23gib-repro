#!/usr/bin/env python3
"""Build linear-axis Azure Monitor plots while preserving native series rows.

By default this reads the completed `results/instrumentation/azure` artifacts
from the control and sigterm stacks. Use repeated `--input LABEL=PATH` options
to plot a different experiment pair or a single run.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

UTC = dt.timezone.utc

CHARTS = {
    "azure-disk-iops": {
        "title": "Azure disk IOPS by role and disk",
        "ylabel": "Operations/sec",
        "kind": "disk",
        "metrics": {
            "Composite Disk Read Operations/sec": ("managed-disk", "read", 1.0),
            "Composite Disk Write Operations/sec": ("managed-disk", "write", 1.0),
            "Data Disk Read Operations/Sec": ("vm-lun", "read", 1.0),
            "Data Disk Write Operations/Sec": ("vm-lun", "write", 1.0),
        },
    },
    "azure-disk-throughput": {
        "title": "Azure disk throughput by role and disk",
        "ylabel": "MiB/sec (Azure BytesPerSecond / 2^20)",
        "kind": "disk",
        "metrics": {
            "Composite Disk Read Bytes/sec": ("managed-disk", "read", 1 / (1024 * 1024)),
            "Composite Disk Write Bytes/sec": ("managed-disk", "write", 1 / (1024 * 1024)),
            "Data Disk Read Bytes/sec": ("vm-lun", "read", 1 / (1024 * 1024)),
            "Data Disk Write Bytes/sec": ("vm-lun", "write", 1 / (1024 * 1024)),
        },
    },
    "azure-disk-latency": {
        "title": "Azure per-LUN disk latency",
        "ylabel": "Azure metric units (see native unit in coverage JSON)",
        "kind": "disk",
        "metrics": {"Data Disk Latency": ("vm-lun", "latency", 1.0)},
    },
    "azure-disk-queue-depth": {
        "title": "Azure per-LUN disk queue depth",
        "ylabel": "Azure metric units (see native unit in coverage JSON)",
        "kind": "disk",
        "metrics": {"Data Disk Queue Depth": ("vm-lun", "queue", 1.0)},
    },
    "azure-vm-uncached-consumed": {
        "title": "Azure VM uncached storage consumed",
        "ylabel": "Consumed (%)",
        "kind": "vm",
        "metrics": {
            "VM Uncached IOPS Consumed Percentage": ("vm", "IOPS", 1.0),
            "VM Uncached Bandwidth Consumed Percentage": ("vm", "bandwidth", 1.0),
        },
    },
}


def parse_time(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return result.astimezone(UTC) if result.tzinfo else result.replace(tzinfo=UTC)


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    if not path.is_file():
        return rows
    with path.open(encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                value["_sourceLine"] = line_no
                rows.append(value)
    return rows


def input_sources(root: Path, explicit: list[str]) -> list[tuple[str, Path]]:
    if explicit:
        result = []
        for item in explicit:
            if "=" not in item:
                raise ValueError(f"--input must be LABEL=PATH, got {item!r}")
            label, raw_path = item.split("=", 1)
            path = Path(raw_path).expanduser().resolve()
            if not label or not path.is_file():
                raise ValueError(f"Azure metrics input is missing or unlabeled: {item!r}")
            result.append((label, path))
        return result
    result = []
    for label in ("control", "sigterm"):
        path = root / label / "results/instrumentation/azure/azure-metrics.jsonl"
        if path.is_file():
            result.append((label, path))
    if not result:
        raise FileNotFoundError(f"no completed Azure metric streams found under {root}")
    return result


def parent_results(path: Path) -> Path:
    # .../results/instrumentation/azure/azure-metrics.jsonl -> .../results
    return path.parent.parent.parent


def metric_units(path: Path) -> dict[tuple[str, str, str, str], str]:
    units: dict[tuple[str, str, str, str], str] = {}
    polls = read_jsonl(path.parent / "azure-metric-polls.jsonl")
    for poll in polls:
        response = poll.get("rawResponse") or {}
        for item in response.get("value", []) if isinstance(response, dict) else []:
            name = ((item.get("name") or {}).get("value"))
            unit = item.get("unit")
            if name and unit:
                key = (str(poll.get("role", "")), str(poll.get("targetKind", "")),
                       str(poll.get("resourceId", "")), str(name))
                units[key] = str(unit)
    return units


def vacuum_start(results: Path) -> dt.datetime | None:
    timing = read_json(results / "measurement/timing.json")
    if timing:
        value = parse_time(timing.get("vacuumStart"))
        if value:
            return value
    events = read_jsonl(results / "measurement/events.jsonl")
    for row in events:
        if row.get("event") == "vacuum_started":
            marker = row.get("marker") or {}
            value = parse_time(marker.get("time")) or parse_time(row.get("time"))
            if value:
                return value
    return None


def row_dimensions(row: dict[str, Any]) -> dict[str, str]:
    dims = row.get("dimensions") or {}
    return {str(key).casefold(): str(value) for key, value in dims.items()}


def row_disk_kind(row: dict[str, Any]) -> str | None:
    kind = row.get("targetKind")
    if kind in ("data-disk", "wal-disk"):
        return str(kind).split("-", 1)[0]
    if kind == "vm":
        lun = row_dimensions(row).get("lun")
        # In this experiment DATA and WAL use LUNs 0 and 1 respectively.
        # The attachment preflight files below provide the authoritative map.
        return "lun:" + lun if lun is not None else None
    return None


def disk_lun_map(results: Path) -> dict[str, str]:
    for candidate in (results / "instrumentation/preflight.json",
                      results.parent / ".state/attachment-verification/verified.json"):
        value = read_json(candidate)
        identities = (value or {}).get("attachmentIdentities") or value or {}
        roles = identities.get("roles") or {}
        if not roles:
            continue
        mapping = {}
        for role_data in roles.values():
            for kind, disk in (role_data.get("disks") or {}).items():
                if disk.get("lun") is not None:
                    mapping[str(disk["lun"])] = kind
        if mapping:
            return mapping
    return {"0": "data", "1": "wal"}


def point_identity(label: str, row: dict[str, Any]) -> tuple[Any, ...] | None:
    point = row.get("point") or {}
    timestamp = parse_time(point.get("timeStamp"))
    if timestamp is None:
        return None
    return (label, row.get("role"), row.get("targetKind"), row.get("resourceId"),
            row.get("metric"), json.dumps(row_dimensions(row), sort_keys=True), timestamp.isoformat())


def load_series(sources: list[tuple[str, Path]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
    duplicates: dict[tuple[Any, ...], list[str]] = defaultdict(list)
    source_metadata = {}
    for label, path in sources:
        results = parent_results(path)
        rows = read_jsonl(path)
        source_metadata[label] = {"path": str(path), "resultsPath": str(results),
                                  "units": metric_units(path),
                                  "vacuumStartUtc": vacuum_start(results),
                                  "lunMap": disk_lun_map(results),
                                  "rowsRead": len(rows)}
        for row in rows:
            key = point_identity(label, row)
            if key is None:
                continue
            captured = parse_time(row.get("capturedAtUtc")) or dt.datetime.min.replace(tzinfo=UTC)
            duplicates[key].append(str(row.get("pollId", "")))
            old = latest.get(key)
            old_capture = parse_time((old or {}).get("capturedAtUtc")) if old else None
            if old is None or captured >= (old_capture or dt.datetime.min.replace(tzinfo=UTC)):
                latest[key] = {**row, "_label": label, "_nativeTime": parse_time(row["point"]["timeStamp"]),
                               "_duplicates": duplicates[key]}
    retained = []
    for key, row in latest.items():
        row["_duplicates"] = sorted(set(duplicates[key]))
        row["_duplicateCount"] = max(0, len(duplicates[key]) - 1)
        retained.append(row)
    retained.sort(key=lambda x: (x["_label"], x.get("role", ""), x.get("targetKind", ""),
                                 x.get("metric", ""), x["_nativeTime"]))
    return retained, source_metadata


def metric_unit(source_metadata: dict[str, Any], label: str, row: dict[str, Any]) -> str | None:
    units = source_metadata[label]["units"]
    key = (str(row.get("role", "")), str(row.get("targetKind", "")),
           str(row.get("resourceId", "")), str(row.get("metric", "")))
    return units.get(key)


def chart_matches(chart_id: str, chart: dict[str, Any], row: dict[str, Any],
                  source_metadata: dict[str, Any]) -> tuple[str, str, float] | None:
    spec = chart["metrics"].get(row.get("metric"))
    if not spec:
        return None
    source_kind, operation, scale = spec
    kind = row.get("targetKind")
    if chart["kind"] == "vm":
        return (source_kind, operation, scale) if kind == "vm" else None
    role = row.get("role")
    if role not in ("primary", "replica"):
        return None
    if kind in ("data-disk", "wal-disk"):
        disk = str(kind).split("-", 1)[0]
        expected_source = "managed-disk"
    elif kind == "vm":
        lun = row_dimensions(row).get("lun")
        if lun is None:
            return None
        disk = source_metadata[row["_label"]]["lunMap"].get(str(lun))
        if disk not in ("data", "wal"):
            return None
        expected_source = "vm-lun"
    else:
        return None
    if source_kind != expected_source:
        return None
    row["_disk"] = disk
    return (source_kind, operation, scale)


def line_label(label: str, row: dict[str, Any], source_kind: str, operation: str) -> str:
    dims = row_dimensions(row)
    source = "managed disk" if source_kind == "managed-disk" else "VM" if source_kind == "vm" else "VM/LUN"
    if source_kind == "managed-disk":
        identity = str(row.get("resourceId", "")).rstrip("/").split("/")[-1]
    elif source_kind == "vm":
        identity = dims.get("vmname", str(row.get("resourceId", "")).rstrip("/").split("/")[-1])
    else:
        identity = f"{dims.get('vmname', 'VM')} LUN {dims.get('lun', '?')}"
    suffix = f" {operation}" if operation in ("read", "write") else f" {row.get('metric')}"
    return f"{label} · {source} {identity}{suffix}"


def coverage_for(chart_id: str, chart: dict[str, Any], points: list[dict[str, Any]],
                 sources: list[tuple[str, Path]], source_metadata: dict[str, Any]) -> dict[str, Any]:
    series = defaultdict(lambda: {"points": 0, "nativeUnits": set(), "resourceIds": set(), "metrics": set()})
    all_metrics = set(chart["metrics"])
    for row in points:
        match = chart_matches(chart_id, chart, row, source_metadata)
        if not match:
            continue
        key = (row["_label"], row.get("role"), row.get("_disk", "vm"), match[0], match[1])
        item = series[key]
        item["points"] += 1
        unit = metric_unit(source_metadata, row["_label"], row)
        if unit:
            item["nativeUnits"].add(unit)
        item["resourceIds"].add(row.get("resourceId"))
        item["metrics"].add(row.get("metric"))
    per_series = []
    for key, item in sorted(series.items(), key=lambda pair: tuple(str(x) for x in pair[0])):
        per_series.append({"run": key[0], "role": key[1], "disk": key[2],
                           "source": key[3], "operation": key[4], "pointCount": item["points"],
                           "nativeUnits": sorted(item["nativeUnits"]),
                           "resourceIds": sorted(x for x in item["resourceIds"] if x),
                           "nativeMetrics": sorted(item["metrics"])})
    inventory = {}
    missing = []
    for label, _ in sources:
        azure_dir = Path(source_metadata[label]["path"]).parent
        definitions = read_json(azure_dir / "definitions-inventory.json") or {}
        advertised = set()
        for target in definitions.get("targets", []):
            if chart["kind"] == "vm" and target.get("kind") == "vm":
                advertised.update(target.get("availableMetrics") or [])
            elif chart["kind"] == "disk" and target.get("kind") in ("vm", "data-disk", "wal-disk"):
                advertised.update(target.get("availableMetrics") or [])
        inventory[label] = {"advertisedMetrics": sorted(advertised & all_metrics),
                            "definitionGaps": definitions.get("gaps", [])}
        for metric in sorted(all_metrics - advertised):
            missing.append({"run": label, "metric": metric,
                            "reason": "not advertised by the selected Azure metric scope"})
    return {"chart": chart_id, "title": chart["title"], "scale": "linear",
            "nativeMetrics": sorted(all_metrics), "series": per_series,
            "inventoryByRun": inventory, "definitionMissingMetrics": missing}


def draw_chart(chart_id: str, chart: dict[str, Any], points: list[dict[str, Any]],
               sources: list[tuple[str, Path]], source_metadata: dict[str, Any], out: Path) -> dict[str, Any]:
    roles = ("primary", "replica")
    disks = ("data", "wal") if chart["kind"] == "disk" else (None,)
    rows, cols = (2, 2) if chart["kind"] == "disk" else (1, 2)
    fig, axes = plt.subplots(rows, cols, figsize=(16, 9 if rows == 2 else 5.5), squeeze=False)
    fig.suptitle(chart["title"], fontsize=15)
    plotted = 0
    missing_panels = []
    run_colors = {label: plt.get_cmap("tab10")(index % 10) for index, (label, _) in enumerate(sources)}
    for index, role in enumerate(roles):
        for disk_index, disk in enumerate(disks):
            ax = axes[index, disk_index] if disk is not None else axes[0, index]
            ax.set_title(f"{role.title()} · {disk.upper()}" if disk else role.title())
            ax.set_ylabel(chart["ylabel"])
            ax.set_yscale("linear")
            ax.grid(True, alpha=0.25)
            grouped: dict[tuple[Any, ...], list[tuple[dt.datetime, float, dict[str, Any], str, str, float]]] = defaultdict(list)
            for row in points:
                if row.get("role") != role:
                    continue
                match = chart_matches(chart_id, chart, row, source_metadata)
                if not match:
                    continue
                source_kind, operation, scale = match
                if disk is not None and row.get("_disk") != disk:
                    continue
                timestamp = row.get("_nativeTime")
                native_value = (row.get("point") or {}).get("average")
                if timestamp is None or not isinstance(native_value, (int, float)) or not math.isfinite(native_value):
                    continue
                group_key = (row["_label"], source_kind, row.get("resourceId"), row.get("metric"),
                             json.dumps(row_dimensions(row), sort_keys=True), operation)
                grouped[group_key].append((timestamp, native_value * scale, row,
                                           source_kind, operation, scale))
            panel_lines = 0
            for values in grouped.values():
                values.sort(key=lambda item: item[0])
                first = values[0]
                _, _, row, source_kind, operation, _ = first
                style = {"read": "-", "write": "--", "IOPS": "-", "bandwidth": "--",
                         "latency": "-", "queue": "-"}.get(operation, "-")
                ax.plot([item[0] for item in values], [item[1] for item in values],
                        linestyle=style, color=run_colors[row["_label"]], linewidth=1.7,
                        label=line_label(row["_label"], row, source_kind, operation))
                panel_lines += 1
                plotted += len(values)
            for label, _ in sources:
                start = source_metadata[label]["vacuumStartUtc"]
                if start:
                    ax.axvline(start, color=run_colors[label], linestyle=":", alpha=0.65,
                               label=f"{label} VACUUM start")
            if panel_lines == 0:
                ax.text(0.5, 0.5, "No Azure series returned", ha="center", va="center",
                        transform=ax.transAxes, color="#555555")
                missing_panels.append({"role": role, "disk": disk, "reason": "no matching points"})
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M\n%m-%d", tz=UTC))
            if ax.has_data():
                ax.legend(fontsize=7, loc="best")
            if disk_index == len(disks) - 1:
                ax.set_xlabel("UTC time")
    fig.autofmt_xdate(rotation=0)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    image_path = out / f"{chart_id}.png"
    fig.savefig(image_path, dpi=160, bbox_inches="tight")
    plt.close(fig)
    report = coverage_for(chart_id, chart, points, sources, source_metadata)
    report.update({"image": image_path.name, "plottedLinePoints": plotted,
                   "missingPanels": missing_panels,
                   "xAxis": "UTC native Azure timestamp",
                   "vacuumStartMarkers": {label: value["vacuumStartUtc"].isoformat().replace("+00:00", "Z")
                                           for label, value in source_metadata.items()
                                           if value["vacuumStartUtc"]}})
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", type=Path, default=Path(__file__).resolve().parents[1],
                        help="experiment directory containing control/ and sigterm/")
    parser.add_argument("--output", type=Path, help="graph output directory; defaults to ROOT/results/azure-graphs")
    parser.add_argument("--input", action="append", default=[], metavar="LABEL=PATH",
                        help="explicit Azure JSONL input; repeat for comparison runs")
    args = parser.parse_args()
    root = args.experiment_root.expanduser().resolve()
    out = (args.output or (root / "results/azure-graphs")).expanduser().resolve()
    sources = input_sources(root, args.input)
    points, metadata = load_series(sources)
    out.mkdir(parents=True, exist_ok=True)

    with (out / "azure-plot-series.jsonl").open("w", encoding="utf-8") as stream:
        for row in points:
            record = {key: row.get(key) for key in
                      ("_label", "capturedAtUtc", "pollId", "resourceId", "role", "targetKind",
                       "metric", "nativeInterval", "dimensions", "point", "_duplicateCount", "_duplicates")}
            record["nativeUnit"] = metric_unit(metadata, row["_label"], row)
            for chart_id, chart in CHARTS.items():
                match = chart_matches(chart_id, chart, row, metadata)
                if match and isinstance((row.get("point") or {}).get("average"), (int, float)):
                    record.setdefault("plotValues", {})[chart_id] = {
                        "value": (row["point"]["average"] * match[2]),
                        "unit": chart["ylabel"], "scale": "linear"}
            stream.write(json.dumps(record, sort_keys=True, separators=(",", ":"), default=str) + "\n")

    coverage = {"generatedAtUtc": dt.datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                "sources": {label: {"path": data["path"], "resultsPath": data["resultsPath"],
                                    "rowsRead": data["rowsRead"],
                                    "vacuumStartUtc": data["vacuumStartUtc"].isoformat().replace("+00:00", "Z")
                                    if data["vacuumStartUtc"] else None,
                                    "lunMap": data["lunMap"]}
                            for label, data in metadata.items()},
                "deduplicatedNativePoints": len(points), "charts": {}}
    for chart_id, chart in CHARTS.items():
        coverage["charts"][chart_id] = draw_chart(chart_id, chart, points, sources, metadata, out)
    (out / "azure-plot-coverage.json").write_text(json.dumps(coverage, indent=2, sort_keys=True, default=str) + "\n",
                                                 encoding="utf-8")
    print(json.dumps({"output": str(out), "charts": sorted(coverage["charts"]),
                      "deduplicatedNativePoints": len(points)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
