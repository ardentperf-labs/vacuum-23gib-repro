"""Three linear-axis incident comparisons: IOPS, operation await, and decimal MB/s."""
import datetime as dt
import json
import math
from pathlib import Path

DEVICES = ("data", "wal")
ROLES = ("primary", "replica")
OPS = ("read", "write")
BUCKET_SECONDS = 60
MIN_BUCKET_COVERAGE = 0.90
# Incident values are references/bounds supplied by the user, not measurements from this run.
REF = {
    ("primary", "data", "iops", "read"): (75, "incident 75"),
    ("primary", "data", "await", "read"): (20, "incident <20 ms"),
    ("primary", "data", "throughput", "read"): (9, "incident 9 MB/s"),
    ("primary", "data", "await", "write"): (100, "incident >100 ms"),
    ("primary", "data", "throughput", "write"): (9, "incident 9 MB/s"),
    ("primary", "wal", "iops", "write"): (50, "incident 50"),
    ("primary", "wal", "await", "write"): (20, "incident <20 ms"),
    ("primary", "wal", "throughput", "write"): (20, "incident 20 MB/s"),
    ("replica", "data", "iops", "read"): (530, "incident 530"),
    ("replica", "data", "await", "read"): (2000, "incident >2,000 ms"),
    ("replica", "data", "throughput", "read"): (4.3, "incident 4.3 MB/s"),
    ("replica", "data", "iops", "write"): (15, "incident 15"),
    ("replica", "data", "await", "write"): (1000, "incident >1,000 ms"),
    ("replica", "data", "throughput", "write"): (4, "incident 4 MB/s"),
    ("replica", "wal", "iops", "write"): (6, "incident 6"),
    ("replica", "wal", "await", "write"): (400, "incident >400 ms"),
    ("replica", "wal", "throughput", "write"): (4.5, "incident 4.5 MB/s"),
}


def stamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def build(rows, interval):
    max_gap = max(interval * 2.5, interval + 3)
    series, maps = {}, {}
    for role in ROLES:
        for dev in DEVICES:
            key = f"{role}_{dev}"
            samples = [r[role] for r in rows]
            maps[key] = samples[0]["devices"][dev]
            initial = maps[key]
            points = []
            for before, after in zip(samples, samples[1:]):
                t0, t1 = before["epoch"], after["epoch"]
                dt_s = t1 - t0
                identity = lambda row: tuple(row["devices"][dev].get(k) for k in
                                             ("majorMinor", "path", "claim", "source"))
                initial_id = tuple(initial.get(k) for k in ("majorMinor", "path", "claim", "source"))
                identity_ok = identity(before) == identity(after) == initial_id
                a = before["disks"].get(before["devices"][dev]["majorMinor"])
                b = after["disks"].get(after["devices"][dev]["majorMinor"])
                p = {"t0": t0, "t1": t1, "seconds": dt_s, "valid": False}
                if a and b and identity_ok and 0 < dt_s <= max_gap:
                    delta = {k: b[k] - a[k] for k in ("r", "rs", "rm", "w", "ws", "wm")}
                    if min(delta.values()) >= 0:
                        p.update(valid=True, readOps=delta["r"], writeOps=delta["w"],
                                 readBytes=delta["rs"] * 512, writeBytes=delta["ws"] * 512,
                                 readMs=delta["rm"], writeMs=delta["wm"],
                                 readIops=delta["r"] / dt_s, writeIops=delta["w"] / dt_s,
                                 readMBps=delta["rs"] * 512 / 1e6 / dt_s,
                                 writeMBps=delta["ws"] * 512 / 1e6 / dt_s,
                                 readAwaitMs=delta["rm"] / delta["r"] if delta["r"] else None,
                                 writeAwaitMs=delta["wm"] / delta["w"] if delta["w"] else None)
                points.append(p)
            series[key] = points
    return series, maps, max_gap


def stats(points, start, end):
    selected, bad = [], []
    for p in points:
        overlap = max(0, min(p["t1"], end) - max(p["t0"], start))
        if not overlap:
            continue
        if not p["valid"]:
            bad.append(p)
        else:
            selected.append(dict(p, overlap=overlap, fraction=overlap / p["seconds"]))
    seconds = sum(p["overlap"] for p in selected)
    if not seconds:
        return {"coveredSeconds": 0, "coverage": 0, "rejectedIntervals": len(bad),
                "boundaryMethod": "valid counter deltas prorated by overlap duration"}
    total = lambda key: sum(p[key] * p["fraction"] for p in selected)
    ro, wo, rm, wm, rb, wb = (total(k) for k in
        ("readOps", "writeOps", "readMs", "writeMs", "readBytes", "writeBytes"))
    span = max(0, end - start)
    return {"coveredSeconds": seconds, "coverage": min(1, seconds / span) if span else 0,
            "rejectedIntervals": len(bad), "boundaryMethod": "valid counter deltas prorated by overlap duration",
            "readIops": ro / seconds, "writeIops": wo / seconds,
            "readMBps": rb / 1e6 / seconds, "writeMBps": wb / 1e6 / seconds,
            "readAwaitMs": rm / ro if ro else None, "writeAwaitMs": wm / wo if wo else None,
            "operationWeightedAwaitMs": (rm + wm) / (ro + wo) if ro + wo else None,
            "maxIntervalReadAwaitMs": max((p["readAwaitMs"] for p in selected if p["readAwaitMs"] is not None), default=None),
            "maxIntervalWriteAwaitMs": max((p["writeAwaitMs"] for p in selected if p["writeAwaitMs"] is not None), default=None)}


def minute_buckets(points, bucket_seconds=BUCKET_SECONDS, minimum_coverage=MIN_BUCKET_COVERAGE):
    """Aggregate raw counter intervals into nonoverlapping UTC-aligned buckets."""
    if not points:
        return []
    first = min(p["t0"] for p in points)
    last = max(p["t1"] for p in points)
    start = math.floor(first / bucket_seconds) * bucket_seconds
    stop = math.ceil(last / bucket_seconds) * bucket_seconds
    if stop <= start:
        stop = start + bucket_seconds

    buckets = []
    for t0 in range(start, stop, bucket_seconds):
        t1 = t0 + bucket_seconds
        covered = 0.0
        invalid_overlap = 0.0
        totals = {k: 0.0 for k in ("readOps", "writeOps", "readBytes", "writeBytes", "readMs", "writeMs")}
        for p in points:
            overlap = max(0, min(p["t1"], t1) - max(p["t0"], t0))
            if overlap <= 0:
                continue
            if not p["valid"]:
                invalid_overlap += overlap
                continue
            if p["seconds"] <= 0:
                invalid_overlap += overlap
                continue
            fraction = overlap / p["seconds"]
            covered += overlap
            for key in totals:
                totals[key] += p[key] * fraction
        coverage = min(1.0, covered / bucket_seconds)
        bucket = {"t0": t0, "t1": t1, "seconds": bucket_seconds,
                  "coveredSeconds": min(covered, bucket_seconds), "coverage": coverage,
                  "invalidOverlapSeconds": invalid_overlap,
                  "valid": coverage + 1e-12 >= minimum_coverage and invalid_overlap == 0}
        if bucket["valid"]:
            bucket.update(
                readIops=totals["readOps"] / covered,
                writeIops=totals["writeOps"] / covered,
                readMBps=totals["readBytes"] / 1e6 / covered,
                writeMBps=totals["writeBytes"] / 1e6 / covered,
                readAwaitMs=totals["readMs"] / totals["readOps"] if totals["readOps"] else None,
                writeAwaitMs=totals["writeMs"] / totals["writeOps"] if totals["writeOps"] else None,
                operationWeightedAwaitMs=((totals["readMs"] + totals["writeMs"]) /
                                          (totals["readOps"] + totals["writeOps"]))
                if totals["readOps"] + totals["writeOps"] else None)
        buckets.append(bucket)
    return buckets


def display_limit(values):
    """Return a chart-only p99 ceiling; numeric report values are not clipped."""
    values = sorted(float(v) for v in values if v is not None and math.isfinite(float(v)))
    if not values:
        return {"p99": None, "limit": 1.0, "maxValue": None, "basis": "empty fallback"}
    rank = (len(values) - 1) * 0.99
    lo = math.floor(rank)
    hi = math.ceil(rank)
    p99 = values[lo] + (values[hi] - values[lo]) * (rank - lo)
    peak = values[-1]
    # A zero p99 with sparse nonzero samples still needs a usable positive axis.
    if p99 > 0:
        limit, basis = p99 * 1.10, "p99 + 10%"
    elif peak > 0:
        limit, basis = peak * 0.01, "sparse-nonzero fallback"
    else:
        limit, basis = 1.0, "all-zero fallback"
    return {"p99": p99, "limit": limit, "maxValue": peak, "basis": basis}


def analyze(rows, timing, interval):
    series, maps, max_gap = build(rows, interval)
    windows = {}
    observation_end = timing.get("replayEnd") or timing.get("observationEnd")
    boundaries = [("vacuum", timing.get("vacuumStart"), timing.get("vacuumComplete")),
                       ("heapWork", timing.get("heapWorkStart"), timing.get("heapWorkComplete")),
                       ("vacuumFinalWait", timing.get("heapWorkComplete"), timing.get("vacuumComplete")),
                       ("replayDrain", timing.get("vacuumComplete"), observation_end),
                       ("vacuumAndReplay", timing.get("vacuumStart"), observation_end)]
    disconnects = [e["time"] for e in timing.get("replicationEvents", []) if e["event"] == "sender_timeout"]
    if disconnects and timing.get("vacuumStart") and timing.get("vacuumComplete"):
        first_drop = min(disconnects)
        if stamp(timing["vacuumStart"]) < stamp(first_drop) < stamp(timing["vacuumComplete"]):
            boundaries += [("beforeSenderTimeout", timing["vacuumStart"], first_drop),
                           ("afterSenderTimeout", first_drop, timing["vacuumComplete"])]
    for name, a, b in boundaries:
        if a and b:
            start, end = stamp(a).timestamp(), stamp(b).timestamp()
            if end <= start:
                continue
            windows[name] = {key: stats(points, start, end) for key, points in series.items()}
    coverage = {"maxIntervalSeconds": max_gap, "windows": {},
                "valid": {"vacuum", "replayDrain", "vacuumAndReplay"} <= set(windows)}
    for window, by_series in windows.items():
        valid = all(x.get("coverage", 0) >= .9 and x.get("rejectedIntervals", 0) == 0 for x in by_series.values())
        coverage["windows"][window] = {"valid": valid, "series": {
            k: {"coverage": v.get("coverage", 0), "coveredSeconds": v.get("coveredSeconds", 0),
                "rejectedIntervals": v.get("rejectedIntervals", 0)} for k, v in by_series.items()}}
        coverage["valid"] &= valid
    v = windows.get("vacuum", {})
    p_iops, r_iops = v.get("primary_data", {}).get("readIops"), v.get("replica_data", {}).get("readIops")
    amp = r_iops / p_iops if coverage["valid"] and p_iops else None
    return {"units": {"throughput": "decimal MB/s", "await": "ms per completed operation"},
            "topology": maps, "sampleCount": len(rows), "maximumAllowedSampleGapSeconds": max_gap,
            "maximumSampleGapSeconds": max((p["seconds"] for v in series.values() for p in v), default=0),
            "windows": windows, "coverage": coverage, "_series": series,
            "vacuumDataReadAmplification": {"basis": "full VACUUM command window, including any final replication wait",
                "ratio": amp, "replicaReadIops": r_iops,
                "primaryReadIops": p_iops, "incidentRatio": 530 / 75}}


def render(out, rows, timing, report):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    metrics = (("IOPS", "iops"), ("Await (ms / operation)", "await"),
               ("Throughput (decimal MB/s)", "throughput"))
    events = [(timing.get("vacuumStart"), "VACUUM start", "green"),
              (timing.get("vacuumComplete"), "VACUUM complete", "darkorange"),
              (timing.get("replayEnd"), "Replica replay caught up", "purple")]
    if timing.get("heapWorkComplete") and timing["heapWorkComplete"] != timing.get("vacuumComplete"):
        events.append((timing["heapWorkComplete"], "Heap work finished (observed)", "teal"))
    event_file = Path(out) / "replication-events.json"
    if event_file.exists():
        for event in json.loads(event_file.read_text()):
            if event["event"] == "sender_timeout":
                events.append((event["time"], "WAL sender timeout", "red"))
            elif event["event"] == "receiver_connected":
                events.append((event["time"], "WAL receiver reconnected", "brown"))
            elif event["event"] == "receiver_disconnect":
                events.append((event["time"], "WAL receiver disconnected", "magenta"))
            elif event["event"] == "receiver_timeout":
                events.append((event["time"], "WAL receiver timeout", "cyan"))
    for ylabel, key in metrics:
        fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharex=True)
        for ax, (role, dev) in zip(axes.flat, [(r, d) for r in ROLES for d in DEVICES]):
            series_key = f"{role}_{dev}"
            points = report["_series"][series_key]
            device = report["topology"][series_key]
            buckets = minute_buckets(points)
            names = {"iops": ("readIops", "writeIops"), "await": ("readAwaitMs", "writeAwaitMs"),
                     "throughput": ("readMBps", "writeMBps")}[key]
            x = [dt.datetime.fromtimestamp((p["t0"] + p["t1"]) / 2, dt.timezone.utc) for p in buckets]
            read = [p.get(names[0]) if p["valid"] else math.nan for p in buckets]
            write = [p.get(names[1]) if p["valid"] else math.nan for p in buckets]
            values = [v for p in buckets if p["valid"] for v in (p.get(names[0]), p.get(names[1]))
                      if v is not None and math.isfinite(v)]
            display = display_limit(values)
            style = {"marker": ".", "markersize": 3} if key == "await" else {}
            ax.plot(x, read, label="read", linewidth=1.25, **style)
            ax.plot(x, write, label="write", linewidth=1.25, **style)
            if key == "await" and role == "primary":
                ax.text(.98, .96, "Gaps: no completed I/O; await undefined",
                        transform=ax.transAxes, ha="right", va="top", fontsize=8,
                        bbox={"facecolor": "white", "alpha": .8, "edgecolor": "none"})
            ax.set_ylabel(ylabel)
            ax.set_yscale("linear")
            ax.grid(True, alpha=.25)
            for ev, label, color in events:
                if ev:
                    ax.axvline(stamp(ev), color=color, linestyle="--", linewidth=.9, label=label)
            refs_above = []
            for op in OPS:
                ref = REF.get((role, dev, key, op))
                if ref:
                    if ref[0] <= display["limit"]:
                        ax.axhline(ref[0], color="gray", linestyle=":", linewidth=1, label=ref[1])
                    else:
                        refs_above.append(f"{op[0].upper()} {ref[1].replace('incident ', '')}")
            above_buckets = []
            for p in buckets:
                pair = [p.get(name) for name in names] if p["valid"] else []
                pair = [v for v in pair if v is not None and math.isfinite(v)]
                if any(v > display["limit"] for v in pair):
                    above_buckets.append(max(pair))
            ax.set_ylim(0, display["limit"])
            max_bucket = display["maxValue"]
            summary = (f"{display['basis']} cap={display['limit']:.4g}; "
                       f"clipped buckets={len(above_buckets)}, "
                       f"max bucket={max_bucket:.4g}" if max_bucket is not None else
                       f"{display['basis']} cap={display['limit']:.4g}; no bucket values")
            if refs_above:
                summary += "; refs > axis: " + "; ".join(refs_above)
            title = f"{role.title()} / {dev.upper()} — PVC {device['claim']}, {device['source']} ({device['majorMinor']})"
            ax.set_title(title + "\n" + summary, fontsize=8, pad=4)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S", tz=dt.timezone.utc))
        if timing.get("replayEnd"):
            state = "completed VACUUM; replica caught up"
        elif timing.get("observationEnd"):
            state = "completed VACUUM; observation ended before replica catch-up"
        else:
            state = f"interim through {rows[-1]['primary']['ts'][:19]}Z"
        fig.suptitle(f"VACUUM only — separate DATA and WAL HDDs; linear axes\n"
                     f"{ylabel}; 60s UTC buckets, p99 display cap; {state}")
        legend = {}
        for ax in axes.flat:
            handles, labels = ax.get_legend_handles_labels()
            for handle, label in zip(handles, labels):
                legend.setdefault(label, handle)
        fig.legend(legend.values(), legend.keys(), loc="upper center",
                   bbox_to_anchor=(.5, .90), ncol=4, fontsize=8)
        fig.autofmt_xdate()
        fig.tight_layout(rect=(0, 0, 1, .89))
        fig.savefig(Path(out) / f"{key}.png", dpi=150)
        plt.close(fig)
