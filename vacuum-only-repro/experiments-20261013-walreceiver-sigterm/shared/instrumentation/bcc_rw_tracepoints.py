#!/usr/bin/env python3
"""Read/write block latency histograms using stable Linux block tracepoints.

This is a future-run alternative to biolatency -F. It pairs
block_io_start/block_io_done on (device, sector), as BCC biolatency already
does for its device-only tracepoint fallback, and uses each tracepoint's
rwbs field to keep READ and WRITE histograms separate. `--check-only` performs
only read-only tracefs inspection and does not load or attach a BPF program.
"""
from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import json
import math
import os
import platform
import re
import stat
import sys
import time
from pathlib import Path
from typing import Any


TRACEPOINTS = ("block_io_start", "block_io_done")
REQUIRED_FIELDS = ("dev", "sector", "rwbs")
UTC = dt.timezone.utc


def tracefs_root() -> Path:
    for root in (Path("/sys/kernel/tracing"), Path("/sys/kernel/debug/tracing")):
        if (root / "available_events").is_file():
            return root
    raise RuntimeError("tracefs available_events is not readable")


def tracepoint_check(root: Path | None = None) -> dict[str, Any]:
    root = root or tracefs_root()
    available = set((root / "available_events").read_text(encoding="utf-8").splitlines())
    checks = []
    for event in TRACEPOINTS:
        name = f"block:{event}"
        fmt_path = root / "events/block" / event / "format"
        exists = name in available and fmt_path.is_file()
        text = fmt_path.read_text(encoding="utf-8") if exists else ""
        fields = {match.group(1) for match in re.finditer(r"field:[^;]*?\s+(\w+)\s*(?:\[\d+\])?;", text)}
        missing = sorted(set(REQUIRED_FIELDS) - fields)
        checks.append({"event": name, "formatPath": str(fmt_path), "available": exists,
                       "fields": sorted(fields), "missingRequiredFields": missing,
                       "passed": exists and not missing})
    return {"kernelRelease": platform.release(), "tracefsRoot": str(root),
            "checks": checks, "passed": all(x["passed"] for x in checks)}


def bpf_text(data_dev: int, wal_dev: int) -> str:
    return r'''
#define DATA_DEV __DATA_DEV__
#define WAL_DEV __WAL_DEV__

/* Keep the map key padding explicit so start/done hash identically. */
struct io_key_t { u64 sector; u32 dev; u32 pad; };
struct start_t { u64 timestamp_ns; u32 operation; u32 pad; };
struct hist_key_t { u32 dev; u32 operation; u32 slot; };

BPF_HASH(starts, struct io_key_t, struct start_t, 32768);
BPF_HISTOGRAM(dist, struct hist_key_t, 16384);
BPF_ARRAY(stats, u64, 6);

enum stat_index {
    STAT_STARTS = 0,
    STAT_COMPLETIONS = 1,
    STAT_MATCHED = 2,
    STAT_KEY_COLLISIONS = 3,
    STAT_UNMATCHED_COMPLETIONS = 4,
    STAT_UPDATE_FAILURES = 5,
};

static __always_inline void increment_stat(u32 index)
{
    u64 *value = stats.lookup(&index);
    if (value) __sync_fetch_and_add(value, 1);
}

static __always_inline u32 operation_from_rwbs(const char *rwbs)
{
    if (rwbs[0] == 'R') return 1;
    if (rwbs[0] == 'W') return 2;
    return 0;
}

TRACEPOINT_PROBE(block, block_io_start)
{
    u32 op = operation_from_rwbs(args->rwbs);
    if (!op || (args->dev != DATA_DEV && args->dev != WAL_DEV)) return 0;
    struct io_key_t key = {.sector = args->sector, .dev = args->dev, .pad = 0};
    struct start_t value = {.timestamp_ns = bpf_ktime_get_ns(), .operation = op};
    increment_stat(STAT_STARTS);
    if (starts.lookup(&key)) increment_stat(STAT_KEY_COLLISIONS);
    if (starts.update(&key, &value) < 0) increment_stat(STAT_UPDATE_FAILURES);
    return 0;
}

TRACEPOINT_PROBE(block, block_io_done)
{
    if (args->dev != DATA_DEV && args->dev != WAL_DEV) return 0;
    increment_stat(STAT_COMPLETIONS);
    struct io_key_t key = {.sector = args->sector, .dev = args->dev, .pad = 0};
    struct start_t *value = starts.lookup(&key);
    if (!value) {
        increment_stat(STAT_UNMATCHED_COMPLETIONS);
        return 0;
    }
    increment_stat(STAT_MATCHED);
    u64 elapsed_us = (bpf_ktime_get_ns() - value->timestamp_ns) / 1000;
    if (elapsed_us < 1) elapsed_us = 1;
    struct hist_key_t hist = {.dev = args->dev, .operation = value->operation,
                              .slot = bpf_log2l(elapsed_us)};
    dist.atomic_increment(hist);
    starts.delete(&key);
    return 0;
}
'''.replace("__DATA_DEV__", str(int(data_dev))).replace("__WAL_DEV__", str(int(wal_dev)))


def device_number(path: str) -> int:
    info = os.stat(path)
    if not stat.S_ISBLK(info.st_mode):
        raise ValueError(f"not a block device: {path}")
    return (os.major(info.st_rdev) << 20) | os.minor(info.st_rdev)


def numeric_value(value: Any) -> int:
    if isinstance(value, (bytes, bytearray)):
        return int.from_bytes(value, byteorder=sys.byteorder, signed=False)
    if hasattr(value, "value"):
        return int(value.value)
    return int(value)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-only", action="store_true",
                        help="verify tracepoint names and fields without loading BPF")
    parser.add_argument("--data-device", help="attached DATA device path, such as /dev/sdb")
    parser.add_argument("--wal-device", help="attached WAL device path, such as /dev/sdc")
    parser.add_argument("--interval", type=float, default=1.0, help="histogram interval seconds")
    parser.add_argument("--count", type=int, default=0, help="number of intervals; 0 runs until SIGTERM")
    parser.add_argument("--duration-seconds", type=float, default=0,
                        help="stop after this many seconds (useful for bounded diagnostics)")
    args = parser.parse_args()
    try:
        check = tracepoint_check()
    except Exception as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 2
    print(json.dumps(check, sort_keys=True), flush=True)
    if not check["passed"] or args.check_only:
        return 0 if check["passed"] else 2
    if not args.data_device or not args.wal_device:
        parser.error("--data-device and --wal-device are required unless --check-only is used")

    data_dev = device_number(args.data_device)
    wal_dev = device_number(args.wal_device)
    if data_dev == wal_dev:
        parser.error("DATA and WAL paths resolve to the same block device")
    from bcc import BPF

    bpf = BPF(text=bpf_text(data_dev, wal_dev))
    names = {data_dev: ("data", os.path.basename(args.data_device)),
             wal_dev: ("wal", os.path.basename(args.wal_device))}
    interval = max(0.1, args.interval)
    duration = max(0.0, args.duration_seconds)
    interval_count = args.count or (math.ceil(duration / interval) if duration else 0)
    stat_names = ("starts", "completions", "matchedCompletions", "keyCollisions",
                  "unmatchedCompletions", "mapUpdateFailures")
    stat_table = bpf["stats"]

    def stat_snapshot() -> list[int]:
        values = []
        for i in range(len(stat_names)):
            raw = stat_table[ctypes.c_int(i)]
            values.append(numeric_value(raw))
        return values

    prior_stats = stat_snapshot()
    attached_at = time.monotonic()
    print(json.dumps({"event": "ready", "capturedAtUtc": dt.datetime.now(UTC).isoformat(
        timespec="milliseconds").replace("+00:00", "Z"), "kernelRelease": platform.release(),
        "tracepoints": list(TRACEPOINTS), "dataDevice": os.path.basename(args.data_device),
        "walDevice": os.path.basename(args.wal_device), "deviceNumbers": {
            "data": data_dev, "wal": wal_dev},
        "pairingKey": ["dev", "sector"],
        "pairingLimitations": ["same-device/same-sector overlapping starts can replace a pending start",
                               "unmatched completions are counted; tracepoint pairing is diagnostic"]},
        sort_keys=True), flush=True)
    intervals = 0
    while interval_count <= 0 or intervals < interval_count:
        remaining = interval - (time.monotonic() - attached_at - intervals * interval)
        if remaining > 0:
            time.sleep(remaining)
        rows = []
        table = bpf["dist"]
        for key, bucket_count in table.items():
            info = names.get(int(key.dev))
            if not info:
                continue
            rows.append({"diskKind": info[0], "device": info[1],
                         "operation": {1: "read", 2: "write"}.get(int(key.operation), "unknown"),
                         "log2Microseconds": numeric_value(key.slot),
                         "count": numeric_value(bucket_count)})
        table.clear()
        current_stats = stat_snapshot()
        deltas = {name: current_stats[i] - prior_stats[i] for i, name in enumerate(stat_names)}
        prior_stats = current_stats
        print(json.dumps({"event": "interval",
                          "capturedAtUtc": dt.datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                          "intervalSeconds": interval, "unit": "microseconds", "histograms": rows,
                          "diagnosticPairCounters": deltas},
                         sort_keys=True), flush=True)
        intervals += 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
