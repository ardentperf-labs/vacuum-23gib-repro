#!/usr/bin/env python3
"""Build read-only guest and VMSS summaries from preserved experiment artifacts."""
from __future__ import annotations

import datetime as dt
import glob
import json
import math
import os
from collections import defaultdict

BASE = "/workspace/vacuum-only-repro/experiments-20261007-premium-v2"
OUT = os.path.join(BASE, "recovery/vm-burst-review")
PT5M = 300.0
MAX_GUEST_GAP = 12.5
RUNS = {
    "baseline-23gib": ("Baseline, 23 GiB", "baseline-23gib"),
    "50gib": ("50 GiB", "50gib"),
    "100gib": ("100 GiB", "100gib"),
}
METRICS = [
    "VM Remote Used Burst IO Credits Percentage",
    "VM Remote Used Burst BPS Credits Percentage",
    "VM Uncached IOPS Consumed Percentage",
    "VM Uncached Bandwidth Consumed Percentage",
]
METRIC_SHORT = {
    METRICS[0]: "Remote burst I/O credits used (%)",
    METRICS[1]: "Remote burst BPS credits used (%)",
    METRICS[2]: "VM uncached IOPS consumed (%)",
    METRICS[3]: "VM uncached bandwidth consumed (%)",
}


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def parse_time(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(dt.timezone.utc)


def iso(value):
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def azure_iso(value):
    return value.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def event(events, name):
    matches = [e for e in events if e.get("event") == name]
    if not matches:
        raise RuntimeError(f"missing required event: {name}")
    return matches[-1]


def load_run(key, label, dirname):
    results = os.path.join(BASE, dirname, "results")
    events = read_jsonl(os.path.join(results, "events.jsonl"))
    timing = read_json(os.path.join(results, "timing.json"))
    process = read_json(os.path.join(results, "vacuum-process.json"))
    started_event = event(events, "observation_started")
    finished_event = event(events, "observation_finished")
    vacuum_start = event(events, "vacuum_started")["marker"]["time"]
    vacuum_end = event(events, "vacuum_completed")["marker"]["time"]
    start = parse_time(started_event["time"])
    end = parse_time(finished_event["observationEnd"])
    vac_start = parse_time(vacuum_start)
    vac_end = parse_time(vacuum_end)
    # The SQL marker may precede the observation_started event by a few
    # milliseconds; the measured overlap is still clipped to the observation.
    if not (vac_start <= start + dt.timedelta(seconds=1) and vac_start < vac_end <= end):
        raise RuntimeError(f"invalid phase chronology for {key}")
    if (process.get("status") != "0" or process.get("remoteExitCode") != 0
            or not timing.get("testComplete") or not timing.get("replayCaughtUp")):
        raise RuntimeError(f"run not complete/successful: {key}")
    samples = read_jsonl(os.path.join(results, "samples.jsonl"))
    return {
        "key": key, "label": label, "dirname": dirname, "results": results,
        "events": events, "timing": timing, "process": process,
        "samples": samples, "start": start, "end": end,
        "vacuum_start": vac_start, "vacuum_end": vac_end,
        "start_raw": started_event["time"],
        "end_raw": finished_event["observationEnd"],
        "vacuum_start_raw": vacuum_start,
        "vacuum_end_raw": vacuum_end,
        "replay_end_raw": timing["replayEnd"],
        "finish_event_raw": finished_event["time"],
        "finish_event_time": parse_time(finished_event["time"]),
        "replay_end": parse_time(timing["replayEnd"]),
    }


def interpolate(x0, x1, v0, v1, x):
    if x1 == x0:
        return float(v1)
    return float(v0) + (float(v1) - float(v0)) * ((x - x0) / (x1 - x0))


def guest_series(run):
    series = {}
    for role in ("primary", "replica"):
        for dev in ("data", "wal"):
            name = f"{role}_{dev}"
            entries = []
            mm_values = set()
            for sample in run["samples"]:
                r = sample.get(role) or {}
                device = (r.get("devices") or {}).get(dev) or {}
                mm = device.get("majorMinor")
                ts = r.get("ts")
                if not mm or not ts:
                    continue
                counters = (r.get("disks") or {}).get(mm)
                if not counters:
                    continue
                mm_values.add(mm)
                entries.append({
                    "t": parse_time(ts).timestamp(),
                    "r": counters["r"], "w": counters["w"],
                    "rs": counters["rs"], "ws": counters["ws"],
                    "mm": mm,
                })
            entries.sort(key=lambda x: x["t"])
            if len(mm_values) != 1:
                raise RuntimeError(f"device mapping changed/ambiguous: {run['key']} {name} {mm_values}")
            series[name] = {"entries": entries, "majorMinor": next(iter(mm_values))}
    return series


def integrate(entries, start, end):
    """Integrate cumulative counters over [start,end], linearly clipping endpoints."""
    totals = {k: 0.0 for k in ("r", "w", "rs", "ws")}
    covered = 0.0
    intervals = []
    for left, right in zip(entries, entries[1:]):
        a = max(start, left["t"])
        b = min(end, right["t"])
        if b <= a:
            continue
        gap = right["t"] - left["t"]
        if gap <= 0 or gap > MAX_GUEST_GAP:
            continue
        for key in totals:
            va = interpolate(left["t"], right["t"], left[key], right[key], a)
            vb = interpolate(left["t"], right["t"], left[key], right[key], b)
            totals[key] += vb - va
        covered += b - a
        intervals.append((a, b))
    return totals, covered, intervals


def guest_rates(totals, covered):
    if covered <= 0:
        return None
    return {
        "readIOPS": totals["r"] / covered,
        "writeIOPS": totals["w"] / covered,
        "readMBpsDecimal": totals["rs"] * 512 / covered / 1_000_000,
        "writeMBpsDecimal": totals["ws"] * 512 / covered / 1_000_000,
    }


def guest_phase(run, series, start, end):
    output = {}
    for name, data in series.items():
        totals, covered, spans = integrate(data["entries"], start.timestamp(), end.timestamp())
        output[name] = {
            "majorMinor": data["majorMinor"], "coveredSeconds": covered,
            "requestedSeconds": (end - start).total_seconds(),
            "coverageFraction": covered / (end - start).total_seconds(),
            "rates": guest_rates(totals, covered),
        }
    return output


def guest_bins(run, series):
    out = []
    start_s, end_s = run["start"].timestamp(), run["end"].timestamp()
    bucket = math.floor(start_s / PT5M) * PT5M
    while bucket < end_s:
        bucket_end = bucket + PT5M
        lo, hi = max(start_s, bucket), min(end_s, bucket_end)
        # Do not claim coverage before the first or after the last actual sample.
        for data in series.values():
            if data["entries"]:
                lo = max(lo, data["entries"][0]["t"])
                hi = min(hi, data["entries"][-1]["t"])
        if hi > lo:
            if lo >= run["vacuum_start"].timestamp() and hi <= run["vacuum_end"].timestamp():
                phase = "vacuum"
            elif lo >= run["vacuum_end"].timestamp():
                phase = "replay_drain"
            elif hi <= run["vacuum_start"].timestamp():
                phase = "pre_vacuum"
            else:
                phase = "mixed_phase_boundary"
            roles = {}
            for name, data in series.items():
                totals, covered, _ = integrate(data["entries"], lo, hi)
                roles[name] = {
                    "majorMinor": data["majorMinor"],
                    "coveredSeconds": covered,
                    "coverageFractionOfClippedWindow": covered / (hi - lo),
                    "rates": guest_rates(totals, covered),
                }
            out.append({
                "utcBucketStart": iso(dt.datetime.fromtimestamp(bucket, dt.timezone.utc)),
                "utcBucketEnd": iso(dt.datetime.fromtimestamp(bucket_end, dt.timezone.utc)),
                "clippedSampleStart": iso(dt.datetime.fromtimestamp(lo, dt.timezone.utc)),
                "clippedSampleEnd": iso(dt.datetime.fromtimestamp(hi, dt.timezone.utc)),
                "clippedCoverageSeconds": hi - lo,
                "partialMeasurementBoundary": (lo > bucket + 1e-6 or hi < bucket_end - 1e-6),
                "phase": phase,
                "series": roles,
            })
        bucket = bucket_end
    return out


def vmss_points():
    files = sorted(glob.glob(os.path.join(OUT, "collected-retry-2", "vmss-metrics-*.json")))
    latest = {}
    captures = []
    for path in files:
        doc = read_json(path)
        captured = parse_time(doc["capturedAtUtc"])
        captures.append({"file": os.path.basename(path), "capturedAtUtc": doc["capturedAtUtc"],
                         "timespan": doc.get("timespan")})
        for stack in doc.get("stacks", []):
            key = stack.get("stack")
            raw = (stack.get("rawResponses") or {}).get("metrics") or {}
            values = raw.get("value") or []
            for metric in values:
                metric_name = ((metric.get("name") or {}).get("value"))
                if metric_name not in METRICS:
                    continue
                error = metric.get("errorCode")
                for ts in metric.get("timeseries") or []:
                    for point in ts.get("data") or []:
                        timestamp = point.get("timeStamp")
                        if not timestamp:
                            continue
                        point_key = (key, metric_name, timestamp)
                        candidate = {
                            "stack": key, "metric": metric_name, "timeStamp": timestamp,
                            "average": point.get("average"), "capturedAtUtc": doc["capturedAtUtc"],
                            "captureFile": os.path.basename(path), "errorCode": error,
                            "httpStatus": stack.get("httpStatus"),
                            "scopeCheck": stack.get("scopeCheck"),
                        }
                        old = latest.get(point_key)
                        if old is None or captured >= parse_time(old["capturedAtUtc"]):
                            latest[point_key] = candidate
    return files, captures, latest


def pt5_start_grid(start, end):
    s, e = start.timestamp(), end.timestamp()
    cur = math.ceil(s / PT5M) * PT5M
    out = []
    while cur + PT5M <= e + 1e-6:
        out.append(dt.datetime.fromtimestamp(cur, dt.timezone.utc))
        cur += PT5M
    return out


def vmss_window(latest, stack, start, end):
    eligible = {azure_iso(t): t for t in pt5_start_grid(start, end)}
    eligible_stamps = list(eligible)
    summaries = {}
    point_output = []
    for metric_name in METRICS:
        values = []
        for stamp in eligible:
            row = latest.get((stack, metric_name, stamp))
            if row is not None and row["average"] is not None:
                values.append((stamp, float(row["average"]), row["capturedAtUtc"], row["captureFile"]))
                point_output.append({
                    "metric": metric_name, "timeStamp": stamp, "average": float(row["average"]),
                    "capturedAtUtc": row["capturedAtUtc"], "captureFile": row["captureFile"],
                })
        nums = [v[1] for v in values]
        summaries[metric_name] = {
            "eligiblePointCount": len(eligible), "availablePointCount": len(nums),
            "missingPointCount": len(eligible) - len(nums),
            "firstEligibleTimestamp": eligible_stamps[0] if eligible_stamps else None,
            "lastEligibleTimestamp": eligible_stamps[-1] if eligible_stamps else None,
            "meanOfPT5MAverages": (sum(nums) / len(nums)) if nums else None,
            "minimumOfPT5MAverages": min(nums) if nums else None,
            "maximumOfPT5MAverages": max(nums) if nums else None,
            "firstAvailableTimestamp": values[0][0] if values else None,
            "lastAvailableTimestamp": values[-1][0] if values else None,
        }
    return summaries, point_output


def pct(value):
    return "n/a" if value is None else f"{value:.2f}%"


def rate_cell(rate):
    if rate is None:
        return "n/a"
    return (f"R {rate['readIOPS']:.0f} / W {rate['writeIOPS']:.0f} IOPS; "
            f"R {rate['readMBpsDecimal']:.1f} / W {rate['writeMBpsDecimal']:.1f} MB/s")


def main():
    runs = {key: load_run(key, *args) for key, args in RUNS.items()}
    files, captures, latest = vmss_points()
    if not files:
        raise RuntimeError("no VMSS collector captures found")
    long_window_captures = []
    for capture in captures:
        span = capture.get("timespan") or {}
        if span.get("startUtc") and span.get("endUtc"):
            span_seconds = (parse_time(span["endUtc"]) - parse_time(span["startUtc"])).total_seconds()
            if span_seconds >= 23 * 60 * 60:
                long_window_captures.append(capture)
    calculations = {
        "generatedAtUtc": iso(dt.datetime.now(dt.timezone.utc)),
        "sources": {
            "workspace": BASE,
            "vmssCaptureDirectory": os.path.join(OUT, "collected-retry-2"),
            "vmssCaptureCount": len(files),
            "vmssCaptures": captures,
            "latestCapture": captures[-1],
            "longWindowCaptures": long_window_captures,
            "deduplication": "For each stack, metric and Azure timeStamp, use the point from the greatest capturedAtUtc; retained point's capture filename is recorded.",
        },
        "metricDefinitions": METRIC_SHORT,
        "runs": {},
        "limitations": [
            "VMSS values are scale-set aggregate PT5M percentages for a capacity-two scale set; they do not identify either VM.",
            "VMSS percentages are not converted to IOPS or MB/s and are not treated as Premium SSD v2 service limits.",
            "Guest I/O comes from the experiment samples' Linux /proc/diskstats for separately mapped DATA and WAL devices; sectors are 512 bytes and throughput is decimal MB/s.",
            "PT5M metric points are included only when the complete timestamp-to-timestamp+5-minute bucket lies within the exact interval. Provider timestamps and experiment event times may have boundary-alignment uncertainty.",
            "Guest five-minute bins use UTC-aligned buckets clipped to the observation interval and actual sample coverage; clipped endpoints and their coverage are explicit.",
            "Zero reported burst-credit percentages do not establish Premium SSD v2 burst eligibility or ineligibility.",
        ],
    }
    md = [
        "# VACUUM guest I/O and VMSS metrics supplement",
        "",
        "This supplement describes the three completed VACUUM observations. It reports guest block I/O and Azure Monitor VMSS percentages as separate evidence streams; it does not convert VMSS percentages into disk throughput or IOPS, infer per-node activity from the scale-set aggregate, or conclude that a burst mechanism caused any change.",
        "",
        "## Measurement intervals",
        "",
        "The full observation runs from the collector's `observation_started` event through `timing.observationEnd`, the timestamp of the final observation sample. It includes replay drain and ends only after the replica reached the target LSN. VACUUM-only intervals use the exact SQL start/completion marker timestamps. Replay drain is reported from VACUUM completion to the final observation sample.",
        "",
        "| Stack | Full observation (UTC) | VACUUM (UTC) | Replay drain (UTC) | Replay drain |",
        "|---|---|---|---|---:|",
    ]
    vmss_metric_order = METRICS
    window_summaries = {}
    for key, run in runs.items():
        series = guest_series(run)
        phases = {
            "wholeObservation": (run["start"], run["end"]),
            "vacuum": (max(run["start"], run["vacuum_start"]), run["vacuum_end"]),
            "replayDrain": (run["vacuum_end"], run["end"]),
        }
        guest_phase_data = {}
        for pname, (a, b) in phases.items():
            guest_phase_data[pname] = guest_phase(run, series, a, b)
        guest_bin_data = guest_bins(run, series)
        vmss = {}
        vmss_points_used = {}
        for wname, (a, b) in phases.items():
            summaries, points = vmss_window(latest, key, a, b)
            vmss[wname] = summaries
            vmss_points_used[wname] = points
        window_summaries[key] = vmss
        finished = run["end"]
        duration = (run["end"] - run["vacuum_end"]).total_seconds()
        md.append(
            f"| {run['label']} | {run['start_raw']} – {run['end_raw']} | "
            f"{run['vacuum_start_raw']} – {run['vacuum_end_raw']} | "
            f"{run['vacuum_end_raw']} – {run['end_raw']} | {duration/60:.1f} min |"
        )
        calculations["runs"][key] = {
            "label": run["label"],
            "observation": {"startUtc": run["start_raw"], "endUtc": run["end_raw"],
                            "finishEventTimeUtc": run["finish_event_raw"],
                            "sampleCount": len(run["samples"])},
            "vacuum": {"startUtc": run["vacuum_start_raw"], "endUtc": run["vacuum_end_raw"],
                       "exitCode": run["process"].get("remoteExitCode")},
            "replayDrain": {"startUtc": run["vacuum_end_raw"], "endUtc": run["end_raw"],
                            "replayCaughtUpUtc": run["replay_end_raw"],
                            "durationSeconds": duration,
                            "targetLsn": run["timing"].get("replayTargetLsn"),
                            "replayLsnAtObservationEnd": run["timing"].get("replicaReplayLsnAtObservationEnd")},
            "guestPhaseSummaries": guest_phase_data,
            "guestUtcFiveMinuteBins": guest_bin_data,
            "vmss": vmss,
            "vmssSelectedPoints": vmss_points_used,
        }

    for phase_name, phase_title in (("wholeObservation", "Whole observation, including replay drain"),
                                    ("vacuum", "VACUUM-only interval")):
        md.extend(["", f"## VMSS metrics — {phase_title}", "",
                   "Each cell is arithmetic mean (minimum–maximum) of the included Azure PT5M `Average` point values, followed by available/eligible point count. The filter uses the Azure timestamp as the bucket start and includes a point only if that complete five-minute bucket lies inside this stack's exact phase interval. Values come from the latest collector response per metric/timestamp.", "",
                   "| Stack | Remote burst I/O credits (%) | Remote burst BPS credits (%) | Uncached IOPS consumed (%) | Uncached bandwidth consumed (%) |",
                   "|---|---:|---:|---:|---:|"])
        for key, run in runs.items():
            row = [run["label"]]
            for metric in vmss_metric_order:
                s = window_summaries[key][phase_name][metric]
                if s["availablePointCount"]:
                    cell = (f"{s['meanOfPT5MAverages']:.2f}% "
                            f"({s['minimumOfPT5MAverages']:.2f}–{s['maximumOfPT5MAverages']:.2f}%), "
                            f"{s['availablePointCount']}/{s['eligiblePointCount']}")
                else:
                    cell = f"n/a, 0/{s['eligiblePointCount']}"
                row.append(cell)
            md.append("| " + " | ".join(row) + " |")

    md.extend(["", "## Azure PT5M point coverage", "",
               "Timestamp ranges below are inclusive PT5M bucket start timestamps. Every included bucket ends no later than the corresponding phase end; the exact event boundaries are in the measurement-interval table. Coverage is reported for each of the four metrics.",
               "",
               "| Stack | Whole observation starts (available / eligible) | VACUUM starts (available / eligible) |",
               "|---|---|---|"])
    for key, run in runs.items():
        cells = [run["label"]]
        for phase_name in ("wholeObservation", "vacuum"):
            sample = window_summaries[key][phase_name][METRICS[0]]
            if sample["eligiblePointCount"]:
                span = f"{sample['firstEligibleTimestamp']}–{sample['lastEligibleTimestamp']}"
            else:
                span = "none"
            cells.append(f"{span} ({sample['availablePointCount']}/{sample['eligiblePointCount']})")
        md.append("| " + " | ".join(cells) + " |")

    md.extend(["", "## Guest disk I/O by phase", "",
               "The table reports time-weighted means from `/proc/diskstats` counter deltas, clipped to exact phase boundaries. Reads and writes are kept separate. Each DATA and WAL device is a separate series on its primary or replica node; they are never collapsed across roles. IOPS are completed operations per second. MB/s is decimal throughput from 512-byte sectors.", "",
               "| Stack | Phase | Primary DATA | Primary WAL | Replica DATA | Replica WAL |",
               "|---|---|---:|---:|---:|---:|"])
    for key, run in runs.items():
        for phase_name, phase_title in (("wholeObservation", "Whole observation"),
                                        ("vacuum", "VACUUM"),
                                        ("replayDrain", "Replay drain")):
            pdata = calculations["runs"][key]["guestPhaseSummaries"][phase_name]
            row = [run["label"], phase_title]
            for name in ("primary_data", "primary_wal", "replica_data", "replica_wal"):
                row.append(rate_cell(pdata[name]["rates"]))
            md.append("| " + " | ".join(row) + " |")

    md.extend(["", "## Scope and limitations", "",
               "The VMSS records are one host-level aggregate series for each exact two-instance `Standard_D2s_v4` scale set. They do not attribute I/O to an individual VM, so they cannot be divided into per-node peaks. The reported percentages are retained as percentages and are not normalized to a disk limit. The two reported burst-credit series were available in the collected responses; zero values do not prove eligibility or ineligibility for Premium SSD v2 burst behavior.",
               "",
               "Guest counters are Linux block-device totals for the PVC-mapped DATA and WAL device on each role. They are not Azure managed-disk telemetry. Guest bins are aligned to UTC five-minute boundaries and clipped to the observation window and available samples. The accompanying JSON records the actual clipped sample endpoints and coverage for every bin. Phase means use sample-counter deltas clipped at exact phase boundaries, with linear interpolation only across adjacent samples no more than 12.5 seconds apart.",
               "",
               "The Azure metric endpoint returns five-minute averages, and its UTC bucket boundaries need not coincide exactly with experiment events. A metric point is included only when its whole five-minute bucket falls inside the specified phase. The independent streams should be compared descriptively; they do not establish a causal performance ceiling or burst-expiry mechanism.",
               "",
               f"Machine-readable point selections and guest-bin calculations: [`measurement-calculations.json`](measurement-calculations.json). The latest-per-timestamp deduplication used {len(files)} preserved collector captures, including the final 24-hour query `{long_window_captures[-1]['file']}` at {long_window_captures[-1]['capturedAtUtc']}; later captures supersede overlapping timestamps when they contain that point. The newest capture in the folder is `{captures[-1]['file']}` at {captures[-1]['capturedAtUtc']}.",
               ""])

    json_path = os.path.join(OUT, "measurement-calculations.json")
    md_path = os.path.join(OUT, "MEASUREMENT-SUPPLEMENT.md")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(calculations, f, indent=2, sort_keys=True)
        f.write("\n")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md))
    print(json_path)
    print(md_path)


if __name__ == "__main__":
    main()
