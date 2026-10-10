#!/usr/bin/env python3
"""Summarize fio, guest iostat, and Azure Monitor data for fio-hdd-repro.

Expected input layout::

    artifacts/
      vm-metrics.json
      guest.txt
      run-1/{meta.json,fio.json,iostat.json}
      run-2/{meta.json,fio.json,iostat.json}

The analyzer keeps unavailable values empty. Azure points are used only when
their complete metric bucket is inside the recorded fio measurement window and
the time series explicitly identifies LUN 0.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import statistics
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


UTC = dt.timezone.utc
MIB = 1024.0 * 1024.0
MB = 1_000_000.0
DEFAULT_AZURE_INTERVAL = 60.0
DEFAULT_AZURE_GUARD_SECONDS = 60.0


CSV_FIELDS = [
    "run",
    "threads",
    "measurement_start_utc",
    "measurement_end_utc",
    "fio_iops",
    "fio_MB_s",
    "fio_lat_mean_ms",
    "fio_clat_mean_ms",
    "fio_clat_p50_ms",
    "fio_clat_p95_ms",
    "fio_clat_p99_ms",
    "azure_guard_seconds",
    "guest_device",
    "guest_r_await_ms",
    "guest_r_s",
    "guest_MB_s",
    "guest_aqu_sz",
    "guest_full_samples",
    "azure_read_iops",
    "azure_read_iops_minutes",
    "azure_read_MB_s",
    "azure_read_MB_s_minutes",
    "azure_data_disk_latency_ms",
    "azure_data_disk_latency_minutes",
    "azure_queue_depth",
    "azure_queue_depth_minutes",
    "azure_full_window_read_iops",
    "azure_full_window_read_iops_minutes",
    "azure_full_window_read_MB_s",
    "azure_full_window_read_MB_s_minutes",
    "azure_full_window_data_disk_latency_ms",
    "azure_full_window_data_disk_latency_minutes",
    "azure_full_window_queue_depth",
    "azure_full_window_queue_depth_minutes",
    "azure_minus_fio_iops",
    "azure_minus_fio_MB_s",
    "azure_minus_fio_clat_mean_ms",
]


AZURE_METRICS = {
    "azure_read_iops": {
        "data disk read operations/sec",
        "data disk read operations/sec.",
        "data disk read operations/s",
    },
    "azure_read_bytes_per_sec": {
        "data disk read bytes/sec",
        "data disk read bytes/sec.",
    },
    "azure_latency_ms": {
        "data disk latency",
    },
    "azure_queue_depth": {
        "data disk queue depth",
    },
}


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_time(value: Any) -> Optional[dt.datetime]:
    """Parse ISO timestamps, Unix timestamps, and iostat date/time objects."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return dt.datetime.fromtimestamp(float(value), UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, dict):
        date_part = value.get("date") or value.get("Date")
        time_part = value.get("time") or value.get("Time")
        if date_part and time_part:
            value = f"{date_part} {time_part}"
        elif date_part:
            value = date_part
        else:
            return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        # sysstat timestamps are normally ISO-like, but some versions use a
        # locale-formatted date/time. The guest collector sets LC_ALL=C and
        # TZ=UTC, so these forms are unambiguous UTC timestamps.
        parsed = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%y %H:%M:%S"):
            try:
                parsed = dt.datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_utc(value: Optional[dt.datetime]) -> Optional[str]:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None


def as_float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def first_number(mapping: Dict[str, Any], names: Sequence[str]) -> Optional[float]:
    for name in names:
        number = as_float(mapping.get(name))
        if number is not None:
            return number
    return None


def fio_stats_in_ns(read_stats: Dict[str, Any], family: str) -> Optional[Dict[str, Any]]:
    """Find a fio latency family and convert its values to nanoseconds."""
    for suffix, multiplier in (("ns", 1.0), ("us", 1_000.0), ("ms", 1_000_000.0)):
        candidate = read_stats.get(f"{family}_{suffix}")
        if isinstance(candidate, dict):
            result = dict(candidate)
            result["_multiplier_to_ns"] = multiplier
            return result
    return None


def fio_latency_mean_ms(read_stats: Dict[str, Any], family: str) -> Optional[float]:
    stats = fio_stats_in_ns(read_stats, family)
    if not stats:
        return None
    mean = as_float(stats.get("mean"))
    if mean is None:
        return None
    return mean * float(stats["_multiplier_to_ns"]) / 1_000_000.0


def fio_clat_percentile_ms(read_stats: Dict[str, Any], wanted: float) -> Optional[float]:
    stats = fio_stats_in_ns(read_stats, "clat")
    if not stats:
        return None
    percentiles = stats.get("percentile")
    if not isinstance(percentiles, dict):
        return None
    for key, value in percentiles.items():
        try:
            percentile = float(key)
        except (TypeError, ValueError):
            continue
        if math.isclose(percentile, wanted, rel_tol=0.0, abs_tol=0.0001):
            number = as_float(value)
            if number is None:
                return None
            return number * float(stats["_multiplier_to_ns"]) / 1_000_000.0
    return None


def extract_fio(path: Path) -> Dict[str, Optional[float]]:
    data = load_json(path)
    jobs = data.get("jobs", []) if isinstance(data, dict) else []
    read_jobs = [job.get("read") for job in jobs if isinstance(job, dict) and isinstance(job.get("read"), dict)]
    if not read_jobs:
        raise ValueError(f"{path}: expected fio JSON with at least one read job")

    # --group_reporting normally emits one aggregate job. If a fio build emits
    # multiple job records, sum rates and weight means by completed read count.
    # Percentiles are reported only for a single aggregate job because taking
    # the arithmetic mean of per-job percentiles would be statistically wrong.
    iops_values = [as_float(read.get("iops")) for read in read_jobs]
    iops = sum(value for value in iops_values if value is not None) if any(value is not None for value in iops_values) else None

    bw_bytes_values = [as_float(read.get("bw_bytes")) for read in read_jobs]
    if all(value is not None for value in bw_bytes_values):
        bw_bytes_sec: Optional[float] = sum(value for value in bw_bytes_values if value is not None)
    else:
        # fio's legacy JSON `bw` field is KiB/s by default.
        bw_values = [as_float(read.get("bw")) for read in read_jobs]
        bw_bytes_sec = sum(value * 1024.0 for value in bw_values if value is not None) if any(value is not None for value in bw_values) else None

    def weighted_latency_mean(family: str) -> Optional[float]:
        vals: List[Tuple[float, float]] = []
        for read in read_jobs:
            value = fio_latency_mean_ms(read, family)
            stats = fio_stats_in_ns(read, family)
            count = as_float(stats.get("N")) if stats else None
            if value is not None:
                vals.append((value, count if count and count > 0 else as_float(read.get("total_ios")) or 1.0))
        if not vals:
            return None
        denominator = sum(weight for _, weight in vals)
        return sum(value * weight for value, weight in vals) / denominator

    single = read_jobs[0] if len(read_jobs) == 1 else None
    return {
        "iops": iops,
        "MB_s": bw_bytes_sec / MB if bw_bytes_sec is not None else None,
        "lat_mean_ms": weighted_latency_mean("lat"),
        "clat_mean_ms": weighted_latency_mean("clat"),
        "clat_p50_ms": fio_clat_percentile_ms(single, 50.0) if single else None,
        "clat_p95_ms": fio_clat_percentile_ms(single, 95.0) if single else None,
        "clat_p99_ms": fio_clat_percentile_ms(single, 99.0) if single else None,
    }


def iostat_timestamp(record: Dict[str, Any]) -> Optional[dt.datetime]:
    timestamp = record.get("timestamp") or record.get("timeStamp") or record.get("time")
    return parse_time(timestamp)


def iostat_disks(path: Path) -> Dict[str, List[Tuple[dt.datetime, Dict[str, Any]]]]:
    data = load_json(path)
    hosts: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        sysstat = data.get("sysstat")
        if isinstance(sysstat, dict) and isinstance(sysstat.get("hosts"), list):
            hosts = [host for host in sysstat["hosts"] if isinstance(host, dict)]
        elif isinstance(data.get("hosts"), list):
            hosts = [host for host in data["hosts"] if isinstance(host, dict)]
        elif isinstance(data.get("statistics"), list):
            hosts = [data]
    elif isinstance(data, list):
        hosts = [host for host in data if isinstance(host, dict)]

    by_device: Dict[str, List[Tuple[dt.datetime, Dict[str, Any]]]] = {}
    for host in hosts:
        records = host.get("statistics", [])
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            timestamp = iostat_timestamp(record)
            if timestamp is None:
                continue
            disks = record.get("disk") or record.get("disks") or []
            if isinstance(disks, dict):
                disks = [disks]
            if not isinstance(disks, list):
                continue
            for disk in disks:
                if not isinstance(disk, dict):
                    continue
                name = disk.get("disk-device") or disk.get("disk_device") or disk.get("device") or disk.get("diskDevice")
                if not name:
                    continue
                normalized = normalize_device(str(name))
                by_device.setdefault(normalized, []).append((timestamp, disk))
    for samples in by_device.values():
        samples.sort(key=lambda item: item[0])
    return by_device


def normalize_device(device: str) -> str:
    value = device.strip()
    if value.startswith("/dev/"):
        value = value[5:]
    return Path(value).name


def parse_guest_device(guest_path: Path) -> Optional[str]:
    """Infer the disk behind LUN 0 from guest inventory/findmnt output."""
    if not guest_path.exists():
        return None
    text = guest_path.read_text(encoding="utf-8", errors="replace")

    # Typical Azure guest symlink output: lun0 -> ../../../sdc.
    for line in text.splitlines():
        if "lun0" not in line.lower():
            continue
        match = re.search(r"\blun0\b\s*->\s*(\S+)", line, re.IGNORECASE)
        if match:
            return disk_from_mount_source(match.group(1))
        # findmnt output may put the mount path and source in columns.
        if "/mnt/fio-data" in line:
            match = re.search(r"(/dev/\S+)", line)
            if match:
                return disk_from_mount_source(match.group(1))

    # guest-test.py records the device used for the mounted read-only fixture.
    for line in text.splitlines():
        if "/mnt/fio-data" in line:
            match = re.search(r"(/dev/\S+)", line)
            if match:
                return disk_from_mount_source(match.group(1))
        if re.search(r"\b(?:device|source)\s*[:=]", line, re.IGNORECASE):
            match = re.search(r"(/dev/\S+)", line)
            if match:
                return disk_from_mount_source(match.group(1))
    return None


def parse_config_device(config_path: Path) -> Optional[str]:
    """Use the guest collector's explicit device record if guest.txt lacks it."""
    if not config_path.exists():
        return None
    try:
        config = load_json(config_path)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(config, dict) or not config.get("device"):
        return None
    return disk_from_mount_source(str(config["device"]))


def disk_from_mount_source(value: str) -> str:
    """Normalize a device or partition path to its whole-disk iostat name."""
    name = Path(value.strip().rstrip(",;)")).name
    if re.match(r"^(?:sd|hd|vd|xvd)[a-z]+\d+$", name):
        return re.sub(r"\d+$", "", name)
    if re.match(r"^nvme\d+n\d+p\d+$", name):
        return re.sub(r"p\d+$", "", name)
    return name


def normalize_iostat_MB_s(disk: Dict[str, Any]) -> Optional[float]:
    """Convert sysstat byte-rate keys to decimal MB/s."""
    for key in ("rBytes/s", "read_bytes/s", "rbytes/s"):
        number = as_float(disk.get(key))
        if number is not None:
            return number / MB
    for key in ("rMB/s", "rMBps", "rMB/s "):
        number = as_float(disk.get(key))
        if number is not None:
            # sysstat's -m output uses 1,048,576-byte MiB units.
            return number * MIB / MB
    for key in ("rkB/s", "rKB/s", "kB_read/s", "kB/s"):
        number = as_float(disk.get(key))
        if number is not None:
            # sysstat's kB output uses 1,024-byte KiB units.
            return number * 1024.0 / MB
    return None


def aggregate_iostat(
    samples: Sequence[Tuple[dt.datetime, Dict[str, Any]]],
    start: Optional[dt.datetime],
    end: Optional[dt.datetime],
    nominal_interval_seconds: float = 1.0,
) -> Dict[str, Optional[float]]:
    if not start or not end or end <= start or not samples:
        return {"r_await_ms": None, "r_s": None, "MB_s": None, "aqu_sz": None, "full_samples": 0}

    full: List[Tuple[float, Dict[str, Any]]] = []
    previous: Optional[dt.datetime] = None
    for timestamp, disk in samples:
        duration = (timestamp - previous).total_seconds() if previous else nominal_interval_seconds
        interval_start = timestamp - dt.timedelta(seconds=duration)
        previous = timestamp
        if duration <= 0:
            continue
        # Each iostat row summarizes the interval ending at its timestamp.
        if interval_start < start or timestamp > end:
            continue
        full.append((duration, disk))

    if not full:
        return {"r_await_ms": None, "r_s": None, "MB_s": None, "aqu_sz": None, "full_samples": 0}

    duration_total = sum(duration for duration, _ in full)
    reads_total = 0.0
    latency_weighted = 0.0
    latency_weight = 0.0
    r_s_weighted = 0.0
    bw_weighted = 0.0
    bw_weight = 0.0
    queue_weighted = 0.0
    queue_weight = 0.0
    for duration, disk in full:
        r_s = first_number(disk, ("r/s", "rps", "reads/s"))
        r_await = first_number(disk, ("r_await", "r-await", "read_await"))
        bw = normalize_iostat_MB_s(disk)
        queue = first_number(disk, ("aqu-sz", "avgqu-sz", "aqu_sz", "avg_queue_size"))

        if r_s is not None:
            read_count = max(0.0, r_s) * duration
            reads_total += read_count
            r_s_weighted += r_s * duration
            if r_await is not None and read_count > 0:
                latency_weighted += r_await * read_count
                latency_weight += read_count
        if bw is not None:
            bw_weighted += bw * duration
            bw_weight += duration
        if queue is not None:
            queue_weighted += queue * duration
            queue_weight += duration

    return {
        "r_await_ms": latency_weighted / latency_weight if latency_weight else None,
        "r_s": r_s_weighted / duration_total if duration_total else None,
        "MB_s": bw_weighted / bw_weight if bw_weight else None,
        "aqu_sz": queue_weighted / queue_weight if queue_weight else None,
        "full_samples": len(full),
        "read_ops": reads_total,
    }


def parse_interval_seconds(value: Any) -> float:
    if not isinstance(value, str):
        return DEFAULT_AZURE_INTERVAL
    text = value.strip().upper()
    match = re.fullmatch(r"PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?", text)
    if not match:
        return DEFAULT_AZURE_INTERVAL
    hours, minutes, seconds = (float(part or 0) for part in match.groups())
    total = hours * 3600 + minutes * 60 + seconds
    return total if total > 0 else DEFAULT_AZURE_INTERVAL


def metric_name(metric: Dict[str, Any]) -> str:
    name = metric.get("name")
    if isinstance(name, dict):
        return str(name.get("value") or name.get("localizedValue") or "").strip().lower()
    return str(name or "").strip().lower()


def metric_collection(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    if isinstance(data, dict):
        values = data.get("value")
        if isinstance(values, list):
            return [item for item in values if isinstance(item, dict)]
        # Also accept an export keyed by localized metric name.
        return [item for item in data.values() if isinstance(item, dict) and ("timeseries" in item or "timeSeries" in item)]
    return []


def series_lun(series: Dict[str, Any]) -> Optional[str]:
    metadata = series.get("metadatavalues") or series.get("metadataValues") or series.get("metadata_values") or []
    if isinstance(metadata, dict):
        metadata = [dict(name=key, value=value) for key, value in metadata.items()]
    if not isinstance(metadata, list):
        return None
    for item in metadata:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if isinstance(name, dict):
            name = name.get("value") or name.get("localizedValue")
        if str(name or "").strip().lower() == "lun":
            return str(item.get("value", "")).strip()
    return None


def azure_points_for_metric(
    metrics: Sequence[Dict[str, Any]],
    accepted_names: Iterable[str],
    start: Optional[dt.datetime],
    end: Optional[dt.datetime],
    interval_seconds: float,
    guard_seconds: float = 0.0,
) -> List[Tuple[dt.datetime, float]]:
    if not start or not end or end <= start:
        return []
    wanted = {name.lower() for name in accepted_names}
    guarded_start = start + dt.timedelta(seconds=guard_seconds)
    points_by_time: Dict[dt.datetime, List[float]] = {}
    for metric in metrics:
        if metric_name(metric) not in wanted:
            continue
        series_list = metric.get("timeseries") or metric.get("timeSeries") or []
        if not isinstance(series_list, list):
            continue
        for series in series_list:
            if not isinstance(series, dict) or series_lun(series) != "0":
                continue
            data = series.get("data", [])
            if not isinstance(data, list):
                continue
            for point in data:
                if not isinstance(point, dict):
                    continue
                timestamp = parse_time(point.get("timeStamp") or point.get("timestamp"))
                value = as_float(point.get("average"))
                if timestamp is None or value is None:
                    continue
                bucket_end = timestamp + dt.timedelta(seconds=interval_seconds)
                if timestamp >= guarded_start and bucket_end <= end:
                    points_by_time.setdefault(timestamp, []).append(value)

    result: List[Tuple[dt.datetime, float]] = []
    for timestamp, values in sorted(points_by_time.items()):
        if len(values) == 1 or all(math.isclose(values[0], other, rel_tol=1e-9, abs_tol=1e-9) for other in values[1:]):
            result.append((timestamp, values[0]))
        # Conflicting duplicate LUN-0 series are ambiguous; omit that bucket.
    return result


def aggregate_azure_metrics(
    metric_file: Path,
    start: Optional[dt.datetime],
    end: Optional[dt.datetime],
    guard_seconds: float = DEFAULT_AZURE_GUARD_SECONDS,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not metric_file.exists():
        for key in AZURE_METRICS:
            result[key] = {
                "average": None, "count": 0, "points": [],
                "full_window_average": None, "full_window_count": 0,
                "full_window_points": [],
            }
        result["azure_read_MB_s"] = {
            "average": None, "count": 0, "points": [],
            "full_window_average": None, "full_window_count": 0,
            "full_window_points": [],
        }
        return result
    data = load_json(metric_file)
    metrics = metric_collection(data)
    interval_seconds = parse_interval_seconds(data.get("interval") if isinstance(data, dict) else None)
    for key, names in AZURE_METRICS.items():
        full_window_points = azure_points_for_metric(metrics, names, start, end, interval_seconds)
        points = azure_points_for_metric(metrics, names, start, end, interval_seconds, guard_seconds)
        result[key] = {
            "average": statistics.fmean(value for _, value in points) if points else None,
            "count": len(points),
            "points": points,
            "full_window_average": statistics.fmean(value for _, value in full_window_points) if full_window_points else None,
            "full_window_count": len(full_window_points),
            "full_window_points": full_window_points,
        }
    if result["azure_read_bytes_per_sec"]["average"] is not None:
        result["azure_read_MB_s"] = {
            "average": result["azure_read_bytes_per_sec"]["average"] / MB,
            "count": result["azure_read_bytes_per_sec"]["count"],
            "points": [(timestamp, value / MB) for timestamp, value in result["azure_read_bytes_per_sec"]["points"]],
            "full_window_average": (
                result["azure_read_bytes_per_sec"]["full_window_average"] / MB
                if result["azure_read_bytes_per_sec"]["full_window_average"] is not None else None
            ),
            "full_window_count": result["azure_read_bytes_per_sec"]["full_window_count"],
            "full_window_points": [
                (timestamp, value / MB)
                for timestamp, value in result["azure_read_bytes_per_sec"]["full_window_points"]
            ],
        }
    else:
        result["azure_read_MB_s"] = {
            "average": None, "count": 0, "points": [],
            "full_window_average": None, "full_window_count": 0,
            "full_window_points": [],
        }
    return result


def format_float(value: Optional[float], digits: int = 6) -> Optional[float]:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def write_azure_minute_points(artifacts: Path, output: Path) -> int:
    """Export every returned Azure metric point without interpreting its bucket alignment."""
    fields = [
        "source_file", "metric_name", "localized_metric_name", "dimensions_json",
        "timestamp_utc", "interval", "average", "minimum", "maximum", "total",
        "count", "point_json",
    ]
    count = 0
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for filename in ("vm-metrics.json", "disk-metrics.json", "host-metrics.json"):
            source = artifacts / filename
            if not source.exists():
                continue
            try:
                data = load_json(source)
            except (OSError, json.JSONDecodeError):
                continue
            interval = data.get("interval") if isinstance(data, dict) else None
            for metric in metric_collection(data):
                name = metric.get("name")
                if isinstance(name, dict):
                    metric_name_value = name.get("value")
                    localized_name = name.get("localizedValue")
                else:
                    metric_name_value = name
                    localized_name = None
                series_list = metric.get("timeseries") or metric.get("timeSeries") or []
                if not isinstance(series_list, list):
                    continue
                for series in series_list:
                    if not isinstance(series, dict):
                        continue
                    metadata = series.get("metadatavalues") or series.get("metadataValues") or series.get("metadata_values") or []
                    if isinstance(metadata, list):
                        dimensions = {}
                        for item in metadata:
                            if not isinstance(item, dict):
                                continue
                            key = item.get("name")
                            if isinstance(key, dict):
                                key = key.get("value") or key.get("localizedValue")
                            if key is not None:
                                dimensions[str(key)] = item.get("value")
                    elif isinstance(metadata, dict):
                        dimensions = metadata
                    else:
                        dimensions = {}
                    points = series.get("data", [])
                    if not isinstance(points, list):
                        continue
                    for point in points:
                        if not isinstance(point, dict):
                            continue
                        timestamp = parse_time(point.get("timeStamp") or point.get("timestamp"))
                        writer.writerow({
                            "source_file": filename,
                            "metric_name": metric_name_value,
                            "localized_metric_name": localized_name,
                            "dimensions_json": json.dumps(dimensions, sort_keys=True, separators=(",", ":")),
                            "timestamp_utc": iso_utc(timestamp) if timestamp else point.get("timeStamp") or point.get("timestamp"),
                            "interval": interval,
                            "average": point.get("average"),
                            "minimum": point.get("minimum"),
                            "maximum": point.get("maximum"),
                            "total": point.get("total"),
                            "count": point.get("count"),
                            "point_json": json.dumps(point, sort_keys=True, separators=(",", ":")),
                        })
                        count += 1
    return count


def run_sort_key(path: Path) -> Tuple[int, str]:
    match = re.search(r"(\d+)$", path.name)
    return (int(match.group(1)) if match else 2**63 - 1, path.name)


def collect_run(
    run_dir: Path,
    azure_file: Path,
    guest_file: Path,
    config_file: Path,
    requested_device: Optional[str],
    azure_guard_seconds: float,
) -> Dict[str, Any]:
    meta_path = run_dir / "meta.json"
    fio_path = run_dir / "fio.json"
    iostat_path = run_dir / "iostat.json"
    missing = [path.name for path in (meta_path, fio_path) if not path.exists()]
    if missing:
        raise ValueError(f"{run_dir}: missing {', '.join(missing)}")

    meta = load_json(meta_path)
    if meta.get('returncode', 0) != 0:
        raise ValueError(f"{run_dir}: failed fio attempt (return code {meta['returncode']}); excluded")
    threads_number = as_float(meta.get("threads")) if isinstance(meta, dict) else None
    if threads_number is None or threads_number < 1 or not threads_number.is_integer():
        raise ValueError(f"{meta_path}: threads must be a positive integer")
    threads = int(threads_number)
    start = parse_time(meta.get("measurement_start")) if isinstance(meta, dict) else None
    end = parse_time(meta.get("measurement_end")) if isinstance(meta, dict) else None
    if start is None or end is None or end <= start:
        raise ValueError(f"{meta_path}: missing or invalid measurement_start/measurement_end")
    fio = extract_fio(fio_path)

    device = normalize_device(requested_device) if requested_device else parse_guest_device(guest_file)
    if not device:
        device = parse_config_device(config_file)
    guest = {"r_await_ms": None, "r_s": None, "MB_s": None, "aqu_sz": None, "full_samples": 0}
    if iostat_path.exists() and device:
        devices = iostat_disks(iostat_path)
        candidates = [device]
        # Device strings in guest mount output may name a partition, while
        # iostat can use either the whole disk or its partition name.
        if not candidates[0].endswith(tuple("0123456789")):
            candidates.append(candidates[0] + "1")
        selected = next((candidate for candidate in candidates if candidate in devices), None)
        if selected:
            guest = aggregate_iostat(devices[selected], start, end)
            device = selected
    azure = aggregate_azure_metrics(azure_file, start, end, azure_guard_seconds)

    azure_iops = azure["azure_read_iops"]["average"]
    azure_bw = azure["azure_read_MB_s"]["average"]
    azure_lat = azure["azure_latency_ms"]["average"]
    azure_queue = azure["azure_queue_depth"]["average"]
    return {
        "run": run_dir.name,
        "threads": threads,
        "measurement_start": start,
        "measurement_end": end,
        "azure_guard_seconds": azure_guard_seconds,
        "fio": fio,
        "guest_device": device,
        "guest": guest,
        "azure": azure,
        "row": {
            "run": run_dir.name,
            "threads": threads,
            "measurement_start_utc": iso_utc(start),
            "measurement_end_utc": iso_utc(end),
            "fio_iops": format_float(fio["iops"]),
            "fio_MB_s": format_float(fio["MB_s"]),
            "fio_lat_mean_ms": format_float(fio["lat_mean_ms"]),
            "fio_clat_mean_ms": format_float(fio["clat_mean_ms"]),
            "fio_clat_p50_ms": format_float(fio["clat_p50_ms"]),
            "fio_clat_p95_ms": format_float(fio["clat_p95_ms"]),
            "fio_clat_p99_ms": format_float(fio["clat_p99_ms"]),
            "azure_guard_seconds": format_float(azure_guard_seconds),
            "guest_device": device,
            "guest_r_await_ms": format_float(guest["r_await_ms"]),
            "guest_r_s": format_float(guest["r_s"]),
            "guest_MB_s": format_float(guest["MB_s"]),
            "guest_aqu_sz": format_float(guest["aqu_sz"]),
            "guest_full_samples": guest["full_samples"],
            "azure_read_iops": format_float(azure_iops),
            "azure_read_iops_minutes": azure["azure_read_iops"]["count"],
            "azure_read_MB_s": format_float(azure_bw),
            "azure_read_MB_s_minutes": azure["azure_read_MB_s"]["count"],
            "azure_data_disk_latency_ms": format_float(azure_lat),
            "azure_data_disk_latency_minutes": azure["azure_latency_ms"]["count"],
            "azure_queue_depth": format_float(azure_queue),
            "azure_queue_depth_minutes": azure["azure_queue_depth"]["count"],
            "azure_full_window_read_iops": format_float(azure["azure_read_iops"]["full_window_average"]),
            "azure_full_window_read_iops_minutes": azure["azure_read_iops"]["full_window_count"],
            "azure_full_window_read_MB_s": format_float(azure["azure_read_MB_s"]["full_window_average"]),
            "azure_full_window_read_MB_s_minutes": azure["azure_read_MB_s"]["full_window_count"],
            "azure_full_window_data_disk_latency_ms": format_float(azure["azure_latency_ms"]["full_window_average"]),
            "azure_full_window_data_disk_latency_minutes": azure["azure_latency_ms"]["full_window_count"],
            "azure_full_window_queue_depth": format_float(azure["azure_queue_depth"]["full_window_average"]),
            "azure_full_window_queue_depth_minutes": azure["azure_queue_depth"]["full_window_count"],
            "azure_minus_fio_iops": format_float(azure_iops - fio["iops"]) if azure_iops is not None and fio["iops"] is not None else None,
            "azure_minus_fio_MB_s": format_float(azure_bw - fio["MB_s"]) if azure_bw is not None and fio["MB_s"] is not None else None,
            "azure_minus_fio_clat_mean_ms": format_float(azure_lat - fio["clat_mean_ms"]) if azure_lat is not None and fio["clat_mean_ms"] is not None else None,
        },
    }


def annotation_delta(azure_value: Optional[float], fio_value: Optional[float], suffix: str, digits: int = 1) -> Optional[str]:
    if azure_value is None or fio_value is None:
        return None
    delta = azure_value - fio_value
    return f"Azure−fio {delta:+.{digits}f} {suffix}"


def make_plots(runs: Sequence[Dict[str, Any]], output_dir: Path) -> bool:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is unavailable; CSV and report were written, graphs were skipped. Install matplotlib to create them.", file=sys.stderr)
        return False

    output_dir.mkdir(parents=True, exist_ok=True)
    x = [run["threads"] for run in runs]

    def draw(
        filename: str,
        title: str,
        ylabel: str,
        series: Sequence[Tuple[str, Sequence[Optional[float]], str, str]],
        annotations: Optional[Sequence[Optional[str]]] = None,
        annotation_series_index: int = 1,
        x_limit: Optional[float] = None,
    ) -> None:
        fig, ax = plt.subplots(figsize=(9.2, 5.6), constrained_layout=True)
        for label, values, color, marker in series:
            ys = [float(value) if value is not None else float("nan") for value in values]
            if any(math.isfinite(value) for value in ys):
                ax.plot(x, ys, marker=marker, linewidth=1.8, markersize=5, label=label, color=color)
        if annotations:
            annotated_values = series[annotation_series_index][1] if annotation_series_index < len(series) else []
            for index, label in enumerate(annotations):
                if label and index < len(annotated_values) and annotated_values[index] is not None:
                    ax.annotate(label, (x[index], annotated_values[index]), xytext=(5, 7), textcoords="offset points", fontsize=7, rotation=18)
        ax.set_title(title)
        ax.set_xlabel("fio worker threads (numjobs)")
        ax.set_ylabel(ylabel)
        ax.grid(True, alpha=0.25)
        ax.set_xscale("linear")
        ax.set_yscale("linear")
        ax.set_ylim(bottom=0)
        if x_limit is not None:
            ax.set_xlim(0, x_limit)
        if filename == "latency.png" and max((v or 0 for v in fio_lat), default=0) >= 1500:
            ax.axhspan(1500, 5000, color="grey", alpha=.1, label="Requested 1.5–5 second range")
        if any(any(value is not None for value in item[1]) for item in series):
            ax.legend(loc="best", frameon=False)
        fig.savefig(output_dir / filename, dpi=160)
        plt.close(fig)

    fio_iops = [run["fio"]["iops"] for run in runs]
    guest_iops = [run["guest"]["r_s"] for run in runs]
    azure_iops = [run["azure"]["azure_read_iops"]["average"] for run in runs]
    fio_bw = [run["fio"]["MB_s"] for run in runs]
    guest_bw = [run["guest"]["MB_s"] for run in runs]
    azure_bw = [run["azure"]["azure_read_MB_s"]["average"] for run in runs]
    fio_lat = [run["fio"]["clat_mean_ms"] for run in runs]
    guest_await = [run["guest"]["r_await_ms"] for run in runs]
    azure_lat = [run["azure"]["azure_latency_ms"]["average"] for run in runs]

    draw(
        "iops.png",
        "4 KiB random read IOPS by fio thread count",
        "Read IOPS",
        [
            ("fio native group (staggered worker windows)", fio_iops, "#1f77b4", "o"),
            ("Guest (common steady measurement window)", guest_iops, "#2ca02c", "^"),
            ("Azure read IOPS (guarded LUN 0 PT1M average)", azure_iops, "#ff7f0e", "s"),
        ],
    )
    draw(
        "iops-low-threads.png", "4 KiB read IOPS: initial concurrency", "Read IOPS",
        [("fio", fio_iops, "#1f77b4", "o"),
         ("Azure (guarded LUN 0 minute)", azure_iops, "#ff7f0e", "s")],
        x_limit=64,
    )
    draw(
        "throughput.png",
        "4 KiB random read throughput by fio thread count",
        "Read throughput (MB/s, decimal)",
        [
            ("fio native group (staggered worker windows)", fio_bw, "#1f77b4", "o"),
            ("Guest (common steady measurement window)", guest_bw, "#2ca02c", "^"),
            ("Azure read throughput (guarded LUN 0 PT1M average)", azure_bw, "#ff7f0e", "s"),
        ],
    )
    draw(
        "latency.png",
        "Read latency by fio thread count",
        "Average read latency (ms)",
        [
            ("fio clat mean (request completion)", fio_lat, "#1f77b4", "o"),
            ("Guest iostat r_await (read-weighted)", guest_await, "#2ca02c", "^"),
            ("Azure Data Disk Latency (guarded LUN 0 PT1M average)", azure_lat, "#ff7f0e", "s"),
        ],
        annotation_series_index=2,
    )
    return True


def write_report(runs: Sequence[Dict[str, Any]], output: Path, plots_created: bool, device: Optional[str]) -> None:
    guard_seconds = runs[0]["azure_guard_seconds"] if runs else DEFAULT_AZURE_GUARD_SECONDS
    lines = [
        "# fio Standard HDD read scaling results",
        "",
        f"Analyzed **{len(runs)}** run(s). The analyzer uses only captured measurements; missing samples remain blank.",
        "",
        "## Results",
        "",
        "See [`summary.csv`](summary.csv) for fio IOPS, decimal MB/s, fio total and completion latency, guest `r_await` and queue depth, and Azure read IOPS/throughput/latency/queue depth. [`azure-minute-points.csv`](azure-minute-points.csv) preserves every returned Azure metric minute point from the VM, disk, and host metric queries.",
        "",
        f"The thread counts shown are fio `numjobs`; each worker uses a synchronous engine with one outstanding request. The displayed Azure values are averages of complete PT1M LUN 0 buckets whose returned timestamp is at least {guard_seconds:g} seconds after `measurement_start` and whose full PT1M interval fits before `measurement_end`. The extra leading guard is a conservative response to the first run's low Azure IOPS bucket despite steady guest I/O; it mitigates that observed mismatch but is not a proven timestamp correction. The CSV also retains the original full-window averages as `azure_full_window_*`; per-metric bucket counts show how many points contributed. Guest `r_await` is weighted by completed reads over complete iostat intervals. Azure averages are not fio percentiles and may differ because of sampling and aggregation windows.",
        "",
        "All graph axes are linear. Throughput uses decimal MB/s; the CSV also records Azure-minus-fio differences.",
        "",
    ]
    def shown(value: Optional[float], decimals: int = 1) -> str:
        return f"{value:.{decimals}f}" if value is not None else "—"
    lines.extend([
        "| IO threads | Guest IOPS | Guest MB/s | Guest await (ms) | Azure latency (ms) | Native fio IOPS | Native fio mean (ms) |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for run in runs:
        lines.append(f"| {run['threads']} | {shown(run['guest']['r_s'])} | {shown(run['guest']['MB_s'],3)} | "
                     f"{shown(run['guest']['r_await_ms'])} | {shown(run['azure']['azure_latency_ms']['average'])} | "
                     f"{shown(run['fio']['iops'])} | {shown(run['fio']['lat_mean_ms'])} |")
    lines.append("")
    if plots_created:
        lines.extend([
            "![Read IOPS by thread count](iops.png)",
            "",
            "![Initial read IOPS scaling](iops-low-threads.png)",
            "",
            "![Read throughput by thread count](throughput.png)",
            "",
            "![Read latency by thread count](latency.png)",
            "",
        ])
    else:
        lines.extend(["Graphs were not created because matplotlib was unavailable.", ""])

    lines.extend([
        "## Observed 1.5–5 second latency points",
        "",
        "Mean and tail latency are listed separately. These are measured points in the inclusive 1,500–5,000 ms band; no interpolation is applied.",
        "",
        "| Latency source | Last measured below 1.5 s | First measured at/above 1.5 s | Runs in band (threads: ms) |",
        "|---|---:|---:|---|",
    ])
    latency_sources = [
        ("fio clat mean", [run["fio"]["clat_mean_ms"] for run in runs]),
        ("fio clat p95", [run["fio"]["clat_p95_ms"] for run in runs]),
        ("fio clat p99", [run["fio"]["clat_p99_ms"] for run in runs]),
        ("Guest iostat r_await", [run["guest"]["r_await_ms"] for run in runs]),
        ("Azure Data Disk Latency", [run["azure"]["azure_latency_ms"]["average"] for run in runs]),
    ]
    for label, values in latency_sources:
        observations = sorted(
            [
                (run["threads"], value)
                for run, value in zip(runs, values)
                if run["threads"] is not None and value is not None
            ],
            key=lambda item: item[0],
        )
        crossing_index = next((index for index, item in enumerate(observations) if item[1] >= 1500.0), None)
        below = (observations[crossing_index - 1] if crossing_index is not None and crossing_index > 0
                 else observations[-1] if observations and crossing_index is None else None)
        at_or_above = observations[crossing_index] if crossing_index is not None else None
        below_text = f"{below[0]}: {below[1]:.3f} ms" if below else "—"
        above_text = f"{at_or_above[0]}: {at_or_above[1]:.3f} ms" if at_or_above else "—"
        observations = [
            (run["threads"], value)
            for run, value in zip(runs, values)
            if run["threads"] is not None and value is not None and 1500 <= value <= 5000
        ]
        rendered = ", ".join(f"{threads}: {value:.3f}" for threads, value in observations) if observations else "No complete observed point"
        lines.append(f"| {label} | {below_text} | {above_text} | {rendered} |")
    lines.extend([
        "",
        f"Guest iostat device: `{device}`." if device else "Guest iostat device: not inferred; rerun with `--device <whole-disk-name>` if guest values are blank.",
        "",
        "## Method and interpretation",
        "",
        "Native fio group statistics use staggered per-worker clocks. At high thread counts, summed worker IO counts divided by the maximum worker runtime overstate throughput over a shared wall-clock window. Use guest/Azure counters for steady disk throughput. The fio latency distribution summarizes its own collected worker samples, not exactly the guest's UTC window. See [fio 3.28 aggregation code](https://github.com/axboe/fio/blob/fio-3.28/stat.c). Failed attempts are excluded and their raw artifacts are retained.",
        "",
        "This report compares three layers: fio request completion, Linux block-device `r_await`, and Azure's one-minute VM metric. Azure's API timestamp anchoring is not used to shift points; the fixed leading guard is applied directly to the returned timestamp, and the unfiltered minute rows remain available for inspection. The guarded Azure-minus-fio differences are descriptive and should not be read as a calibration offset. The per-thread test is closed-loop: increasing threads increases concurrent synchronous reads, while each worker holds at most one outstanding request.",
        "",
    ])
    output.write_text("\n".join(lines), encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    script_dir = Path(__file__).resolve().parent
    parser.add_argument("--artifacts", type=Path, default=script_dir / "artifacts", help="input directory containing run-N folders and vm-metrics.json")
    parser.add_argument("--output", type=Path, help="output directory (default: <artifacts>/analysis)")
    parser.add_argument("--device", help="guest whole-disk device name for LUN 0, such as sdc; overrides inference from guest.txt")
    parser.add_argument(
        "--azure-guard-seconds", type=float, default=DEFAULT_AZURE_GUARD_SECONDS,
        help="minimum age of an Azure metric timestamp after measurement_start (default: 60 seconds)",
    )
    args = parser.parse_args(argv)

    artifacts = args.artifacts.resolve()
    output = args.output.resolve() if args.output else artifacts / "analysis"
    output.mkdir(parents=True, exist_ok=True)
    if not math.isfinite(args.azure_guard_seconds) or args.azure_guard_seconds < 0:
        parser.error("--azure-guard-seconds must be a finite non-negative number")
    azure_file = artifacts / "vm-metrics.json"
    guest_file = artifacts / "guest.txt"
    config_file = artifacts / "config.json"
    run_dirs = sorted((path for path in artifacts.glob("run-*") if path.is_dir() and (path / "meta.json").exists()), key=run_sort_key)
    if not run_dirs:
        parser.error(f"no run-N directories with meta.json found under {artifacts}")

    runs: List[Dict[str, Any]] = []
    for run_dir in run_dirs:
        try:
            runs.append(collect_run(run_dir, azure_file, guest_file, config_file, args.device, args.azure_guard_seconds))
        except (OSError, json.JSONDecodeError, ValueError, TypeError) as exc:
            print(f"warning: skipping {run_dir.name}: {exc}", file=sys.stderr)
    if not runs:
        print("error: no readable runs", file=sys.stderr)
        return 2
    runs.sort(key=lambda run: (run["threads"] if run["threads"] is not None else 2**63 - 1, run["run"]))

    csv_path = output / "summary.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for run in runs:
            writer.writerow(run["row"])

    minute_points_path = output / "azure-minute-points.csv"
    minute_points_count = write_azure_minute_points(artifacts, minute_points_path)

    device = normalize_device(args.device) if args.device else parse_guest_device(guest_file) or parse_config_device(config_file)
    plots_created = make_plots(runs, output)
    write_report(runs, output / "report.md", plots_created, device)
    print(f"Wrote {csv_path}")
    print(f"Wrote {output / 'report.md'}")
    print(f"Wrote {minute_points_path} ({minute_points_count} Azure minute points)")
    if plots_created:
        print(f"Wrote plots under {output}")
    if not device:
        print("warning: could not infer LUN 0's whole-disk device from guest.txt; guest iostat columns will be blank unless --device is supplied", file=sys.stderr)
    if not azure_file.exists():
        print(f"warning: {azure_file} is missing; Azure columns are blank", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
