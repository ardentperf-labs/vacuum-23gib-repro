#!/usr/bin/env python3
"""Recompute per-role DATA/WAL five-second guest rates from run/results/samples.jsonl."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

STACK = Path(__file__).resolve().parents[1]
SAMPLES = STACK / "run" / "results" / "samples.jsonl"
ROLES = ("primary", "replica")
DEVICES = ("data", "wal")


def parse_time(value: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"timestamp lacks a timezone: {value}")
    return parsed.astimezone(dt.timezone.utc)


def percentile(values: list[float], q: float) -> float:
    """Floor-index percentile, matching the run's collector convention."""
    ordered = sorted(values)
    return ordered[math.floor(q * (len(ordered) - 1))]


def read_rows(samples_path: Path) -> list[dict]:
    rows = []
    with samples_path.open(encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"invalid JSON at {samples_path}:{line_number}: {exc}") from exc
    if len(rows) < 2:
        raise SystemExit("not enough samples to derive rates")
    return rows


def derive(rows: list[dict], role: str) -> dict:
    samples = []
    for row in rows:
        item = row.get(role)
        if not isinstance(item, dict) or not item.get("ts"):
            continue
        samples.append((row.get("phase"), parse_time(item["ts"]), item))

    windows = {device: [] for device in DEVICES}
    for (phase_a, time_a, before), (phase_b, time_b, after) in zip(samples, samples[1:]):
        seconds = (time_b - time_a).total_seconds()
        if phase_a != "vacuum" or phase_b != "vacuum" or not 4.0 <= seconds <= 6.0:
            continue
        for device in DEVICES:
            before_map = before.get("devices", {}).get(device, {})
            after_map = after.get("devices", {}).get(device, {})
            major_minor = before_map.get("majorMinor")
            if not major_minor or after_map.get("majorMinor") != major_minor:
                raise SystemExit(f"{role} {device} device mapping changed within a VACUUM interval")
            first = before.get("disks", {}).get(major_minor)
            last = after.get("disks", {}).get(major_minor)
            if not isinstance(first, dict) or not isinstance(last, dict):
                raise SystemExit(f"{role} {device} diskstats entry missing for {major_minor}")
            delta = {key: int(last[key]) - int(first[key]) for key in ("r", "w", "rs", "ws")}
            if any(value < 0 for value in delta.values()):
                raise SystemExit(f"{role} {device} diskstats counter reset or decreased")
            windows[device].append({
                "observedAtUtc": time_b.isoformat().replace("+00:00", "Z"),
                "intervalSeconds": seconds,
                "iops": (delta["r"] + delta["w"]) / seconds,
                "mbps": (delta["rs"] + delta["ws"]) * 512 / seconds / 1_000_000,
            })

    if not windows["data"] or len(windows["data"]) != len(windows["wal"]):
        raise SystemExit(f"{role}: no matched DATA/WAL VACUUM windows")
    result = {"windowCount": len(windows["data"])}
    result["firstWindowEndUtc"] = windows["data"][0]["observedAtUtc"]
    result["lastWindowEndUtc"] = windows["data"][-1]["observedAtUtc"]
    for device in DEVICES:
        result[device] = {}
        for metric in ("iops", "mbps"):
            values = [window[metric] for window in windows[device]]
            result[device][metric] = {
                "p50": statistics.median(values),
                "p95": percentile(values, 0.95),
                "max": max(values),
            }
    result["dataPlusWal"] = {}
    for metric in ("iops", "mbps"):
        values = [a[metric] + b[metric] for a, b in zip(windows["data"], windows["wal"])]
        result["dataPlusWal"][metric] = {
            "p50": statistics.median(values),
            "p95": percentile(values, 0.95),
            "max": max(values),
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=Path, default=SAMPLES,
                        help="samples.jsonl input (defaults to the live run log)")
    args = parser.parse_args()
    samples_path = args.samples.resolve()
    rows = read_rows(samples_path)
    result = {
        "source": str(samples_path.relative_to(STACK.resolve())),
        "sourceEvidence": {
            "bytes": samples_path.stat().st_size,
            "sha256": hashlib.sha256(samples_path.read_bytes()).hexdigest(),
        },
        "unitMethod": {
            "iops": "delta(r+w)/elapsed seconds",
            "mbps": "delta(rs+ws)*512/elapsed seconds/1,000,000 (decimal MB/s)",
            "deviceMapping": "per-sample devices.data/wal.majorMinor",
            "windows": "consecutive VACUUM-to-VACUUM samples with 4.0 <= elapsed seconds <= 6.0",
            "p95": "floor-index sorted[floor(0.95*(n-1))]",
        },
        "sampleRecordCount": len(rows),
        "lastSampleUtc": max(parse_time(row[role]["ts"]) for row in rows for role in ROLES
                              if isinstance(row.get(role), dict) and row[role].get("ts"))
                         .isoformat().replace("+00:00", "Z"),
        "roles": {role: derive(rows, role) for role in ROLES},
    }
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
