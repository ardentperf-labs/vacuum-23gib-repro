#!/usr/bin/env python3
"""Render recovered-evidence figures without exposing private chart data.

Existing Azure plotting helpers write coverage and point-series files beside
their PNGs. This wrapper stages all of that under .state, copies only figures
to recovered-analysis/graphs, and never edits the original results tree.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import sys
from pathlib import Path


def copy_metrics(root: Path, lane: str, target: Path) -> None:
    source_results = root / lane / "results"
    source_azure = source_results / "instrumentation/azure"
    target_azure = target / lane / "results/instrumentation/azure"
    target_azure.mkdir(parents=True, exist_ok=True)
    output = target_azure / "azure-metrics.jsonl"
    parts = [source_azure / "azure-metrics.jsonl"]
    supplemental = source_results / "diagnosis/private/supplemental"
    for pattern in ("azure-metrics-tail-deduplicated-points-*.jsonl",
                    "azure-metrics-final-tail-deduplicated-points-*.jsonl"):
        parts.extend(sorted(supplemental.glob(pattern)))
    graph_start = dt.datetime.fromisoformat("2026-10-13T05:13:00+00:00")
    graph_end = dt.datetime.fromisoformat("2026-10-13T08:36:00+00:00")

    def safe_id(row):
        role = str(row.get("role", "unknown"))
        kind = str(row.get("targetKind", "resource"))
        if kind == "blob-service":
            name = f"wal-archive-{lane}"
            provider = "Microsoft.Storage/storageAccounts"
            suffix = "/blobServices/default"
        elif kind == "vm":
            name = f"{lane}-{role}-vm"
            provider = "Microsoft.Compute/virtualMachineScaleSets"
            suffix = "/virtualMachines/0"
        else:
            name = f"{lane}-{role}-{kind}"
            provider = "Microsoft.Compute/disks"
            suffix = ""
        return f"/subscriptions/redacted/resourceGroups/experiment/providers/{provider}/{name}{suffix}"

    def sanitize(row):
        row = dict(row)
        identifier = safe_id(row)
        row["resourceId"] = identifier
        if "accountResourceId" in row:
            row["accountResourceId"] = identifier.split("/blobServices", 1)[0]
        dims = dict(row.get("dimensions") or {})
        if "vmname" in {str(key).casefold() for key in dims}:
            for key in list(dims):
                if str(key).casefold() == "vmname":
                    dims[key] = f"{row.get('role', 'node')}-vm"
        row["dimensions"] = dims
        if isinstance(row.get("nativeDimensions"), list):
            row["nativeDimensions"] = [
                {**item, "value": f"{row.get('role', 'node')}-vm"}
                if str(item.get("name", "")).casefold() == "vmname" else item
                for item in row["nativeDimensions"]]
        return row

    with output.open("w", encoding="utf-8") as dest:
        for part in parts:
            if not part.is_file():
                continue
            with part.open(encoding="utf-8") as source:
                for line in source:
                    try:
                        native = json.loads(line).get("point", {}).get("timeStamp")
                        stamp = dt.datetime.fromisoformat(native.replace("Z", "+00:00"))
                    except (ValueError, AttributeError, json.JSONDecodeError):
                        continue
                    if graph_start <= stamp <= graph_end:
                        dest.write(json.dumps(sanitize(json.loads(line)), sort_keys=True, separators=(",", ":")) + "\n")
    polls = source_azure / "azure-metric-polls.jsonl"
    if polls.exists():
        poll_out = target_azure / "azure-metric-polls.jsonl"
        with polls.open(encoding="utf-8") as source, poll_out.open("w", encoding="utf-8") as dest:
            for line in source:
                try:
                    poll = json.loads(line)
                except json.JSONDecodeError:
                    continue
                unit_items = []
                raw = poll.get("rawResponse") or {}
                for item in raw.get("value", []) if isinstance(raw, dict) else []:
                    name = (item.get("name") or {}).get("value")
                    unit = item.get("unit")
                    if name and unit:
                        unit_items.append({"name": {"value": name}, "unit": unit})
                if not unit_items:
                    continue
                safe_poll = {"role": poll.get("role"), "targetKind": poll.get("targetKind"),
                             "resourceId": safe_id(poll), "rawResponse": {"value": unit_items}}
                dest.write(json.dumps(safe_poll, separators=(",", ":")) + "\n")
    preflight = source_results / "instrumentation/preflight.json"
    if preflight.is_file():
        dest = target / lane / "results/instrumentation/preflight.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(preflight, dest)

    original_timing = json.loads((source_results / "measurement/timing.json").read_text())
    start_receipt = json.loads((root / f"results/preview/{lane}-vacuum-completion/receipt.json").read_text())
    start_text = start_receipt["startMarker"].split("|")[1]
    complete_text = start_receipt["completeMarker"].split("|")[1]
    original_timing["vacuumStart"] = start_text
    original_timing["vacuumComplete"] = complete_text
    original_timing["vacuumStartMarkerObserved"] = {"time": start_text}
    original_timing["vacuumCompleteMarkerObserved"] = {"time": complete_text}
    measurement = target / lane / "results/measurement"
    measurement.mkdir(parents=True, exist_ok=True)
    (measurement / "timing.json").write_text(json.dumps(original_timing), encoding="utf-8")
    if lane == "sigterm":
        intervention = json.loads((source_results / "measurement/walreceiver-intervention.json").read_text())
    else:
        intervention = {"executedAtUtc": json.loads((source_results / "measurement/walreceiver-intervention.json").read_text())["executedAtUtc"],
                        "status": "marked", "signalResult": {"signalSent": False}}
    (measurement / "walreceiver-intervention.json").write_text(json.dumps(intervention), encoding="utf-8")
    events = source_results / "measurement/events.jsonl"
    if events.is_file():
        shutil.copy2(events, measurement / "events.jsonl")


def render_stall(root: Path, dest: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from datetime import timezone
    def parse_time(value):
        if not isinstance(value, str) or not value:
            return None
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc)

    session_path = root / "sigterm/results/measurement/client/sessions.jsonl"
    progress_path = root / ".state/ongoing-monitor/native-progress-full/sigterm-pgbench.out"
    receipt = json.loads((root / "sigterm/results/diagnosis/recovered-client/recovery-receipt.json").read_text())
    timing = receipt["originalRunState"]
    start = parse_time(receipt["intervention"]["executedAtUtc"])
    cutoff = parse_time(timing["runnerObservationEndUtc"])
    times, active, waiting = [], [], []
    with session_path.open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            stamp = parse_time(row.get("observedAtUtc"))
            if stamp is None or not (start <= stamp <= cutoff):
                continue
            times.append(stamp)
            waiting.append(int(row.get("waitingSessions") or 0))
            active.append(int(row.get("activeSessions") or 0))
    progress_times, tps = [], []
    # pgbench reports an epoch-like elapsed-seconds field in this captured run.
    import re
    line_re = re.compile(r"^progress: ([0-9]+(?:\.[0-9]+)?) s, ([0-9]+(?:\.[0-9]+)?) tps,.* ([0-9]+) failed,")
    with progress_path.open(encoding="utf-8") as stream:
        for line in stream:
            match = line_re.search(line)
            if not match:
                continue
            stamp = dt.datetime.fromtimestamp(float(match.group(1)), timezone.utc)
            if start <= stamp <= cutoff:
                progress_times.append(stamp)
                tps.append(float(match.group(2)))
    if not times or not progress_times:
        raise ValueError("no native samples in recovered stall proof window")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7), sharex=True,
                                  gridspec_kw={"height_ratios": [1, 1.25]})
    ax1.plot(times, active, label="Active writer sessions", color="#4c78a8", linewidth=1)
    ax1.plot(times, waiting, label="Waiting writer sessions", color="#f58518", linewidth=1)
    ax1.set_ylabel("Sessions (of 10)")
    ax1.set_ylim(-0.3, 10.5)
    ax1.grid(True, alpha=.25)
    ax1.legend(loc="lower right")
    ax2.plot(progress_times, tps, label="Native pgbench TPS", color="#54a24b", linewidth=1)
    ax2.set_ylabel("Transactions per second")
    ax2.set_xlabel("UTC time")
    ax2.grid(True, alpha=.25)
    ax2.legend(loc="upper right")
    for ax in (ax1, ax2):
        ax.axvline(start, color="#e45756", linestyle="--", linewidth=1, label="SIGTERM")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=timezone.utc))
    fig.suptitle("Treatment writers and native pgbench progress through conservative pre-stop cutoff\n"
                 f"{start.isoformat()} to {cutoff.isoformat()} UTC; linear axes")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(dest, dpi=160, bbox_inches="tight")
    plt.close(fig)


def load_plot_module():
    import importlib.util
    path = Path(__file__).with_name("plot.py")
    spec = importlib.util.spec_from_file_location("incident_disk_plot", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def disk_rows(root: Path, lane: str):
    import json
    rows = [json.loads(line) for line in (root / lane / "results/measurement/samples.jsonl").read_text().splitlines()
            if line.strip()]
    return rows


def render_guest_diskstats(root: Path, dest: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import math
    plot = load_plot_module()
    metrics = (("IOPS", "iops", ("readIops", "writeIops")),
               ("Operation await (ms)", "await", ("readAwaitMs", "writeAwaitMs")),
               ("Throughput (decimal MB/s)", "throughput", ("readMBps", "writeMBps")))
    lane_colors = {"control": "#1766ac", "sigterm": "#cf5c24"}
    lane_line = {"read": "-", "write": "--"}
    runs, timings = {}, {}
    for lane in ("control", "sigterm"):
        rows = disk_rows(root, lane)
        timing = json.loads((root / lane / "results/measurement/timing.json").read_text())
        receipt = json.loads((root / f"results/preview/{lane}-vacuum-completion/receipt.json").read_text())
        start = receipt["startMarker"].split("|")[1]
        end = receipt["completeMarker"].split("|")[1]
        timing["vacuumStart"] = start
        timing["vacuumComplete"] = end
        timing["observationEnd"] = "2026-10-13T08:25:00Z"
        runs[lane] = plot.analyze(rows, timing, 5.0)
        timings[lane] = timing
    for ylabel, key, fields in metrics:
        fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharex=True)
        for ax, (role, disk) in zip(axes.flat, ((r, d) for r in ("primary", "replica") for d in ("data", "wal"))):
            for lane in ("control", "sigterm"):
                series = runs[lane]["_series"][f"{role}_{disk}"]
                buckets = plot.minute_buckets(series)
                x = [dt.datetime.fromtimestamp((p["t0"] + p["t1"]) / 2, dt.timezone.utc) for p in buckets]
                for op, field in zip(("read", "write"), fields):
                    y = [p.get(field) if p["valid"] else math.nan for p in buckets]
                    ax.plot(x, y, color=lane_colors[lane], linestyle=lane_line[op], linewidth=1.1,
                            label=f"{lane} {op}")
            ax.set_title(f"{role.title()} / {disk.upper()}")
            ax.set_ylabel(ylabel)
            ax.set_yscale("linear")
            ax.grid(True, alpha=.25)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=dt.timezone.utc))
            for lane in ("control", "sigterm"):
                for marker, label, color in (("vacuumStart", f"{lane} VACUUM start", "#59a14f"),
                                             ("vacuumComplete", f"{lane} VACUUM complete", "#edc949")):
                    stamp = dt.datetime.fromisoformat(timings[lane][marker].replace("Z", "+00:00"))
                    ax.axvline(stamp, color=color, linestyle=":" if lane == "control" else "-.", linewidth=.7)
        handles, labels = axes.flat[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.995), ncol=4)
        fig.suptitle(f"Guest diskstats: {ylabel}; UTC 60-second linear buckets\n"
                     "DATA and WAL shown separately; sampled counter intervals only", y=0.94)
        fig.tight_layout(rect=(0, 0, 1, .88))
        fig.savefig(dest / f"guest-{key}.png", dpi=155, bbox_inches="tight")
        plt.close(fig)


def render_bcc_latency(root: Path, dest: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import math
    from collections import defaultdict
    grouped = defaultdict(lambda: defaultdict(int))
    for lane in ("control", "sigterm"):
        path = root / lane / "results/instrumentation/bcc-read-write-histograms.jsonl"
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                hist = row.get("histogram") or {}
                try:
                    when = dt.datetime.fromisoformat(row.get("containerLogTimestamp", row["capturedAtUtc"]).replace("Z", "+00:00"))
                    index, count = int(hist["log2Microseconds"]), int(hist["count"])
                except (ValueError, TypeError, KeyError):
                    continue
                if count <= 0 or row.get("diskKind") not in ("data", "wal") or row.get("operation") not in ("read", "write"):
                    continue
                grouped[(lane, row.get("role"), row["diskKind"], row["operation"], when)][index] += count
    points = defaultdict(list)
    for (lane, role, disk, operation, when), buckets in grouped.items():
        total = sum(buckets.values())
        target = math.ceil(total * .95)
        cumulative = 0
        estimate = None
        for power, count in sorted(buckets.items()):
            cumulative += count
            if cumulative >= target:
                estimate = (2 ** (power + 1)) / 1000.0
                break
        if estimate is not None:
            points[(lane, role, disk, operation)].append((when, estimate))
    colors = {"control": "#1766ac", "sigterm": "#cf5c24"}
    styles = {"read": "-", "write": "--"}
    fig, axes = plt.subplots(2, 2, figsize=(14, 9), sharex=True)
    for ax, (role, disk) in zip(axes.flat, ((r, d) for r in ("primary", "replica") for d in ("data", "wal"))):
        for lane in ("control", "sigterm"):
            for operation in ("read", "write"):
                series = sorted(points.get((lane, role, disk, operation), []))
                ax.plot([p[0] for p in series], [p[1] for p in series], color=colors[lane],
                        linestyle=styles[operation], linewidth=.8, label=f"{lane} {operation}")
        ax.set_title(f"{role.title()} / {disk.upper()}")
        ax.set_ylabel("Approximate p95 bin upper bound (ms)")
        ax.set_yscale("linear")
        ax.grid(True, alpha=.25)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=dt.timezone.utc))
    handles, labels = axes.flat[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4)
    fig.suptitle("BCC block-I/O read/write tracepoint latency; log2-microsecond histogram bins\n"
                 "p95 is displayed at its bin upper edge; linear axes")
    fig.tight_layout(rect=(0, 0, 1, .93))
    fig.savefig(dest / "bcc-read-write-latency.png", dpi=155, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--experiment-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path)
    parser.add_argument("--work", type=Path)
    args = parser.parse_args()
    root = args.experiment_root.expanduser().resolve()
    output = (args.output or root / "recovered-analysis/graphs").expanduser().resolve()
    work = (args.work or root / ".state/recovered-analysis-work").expanduser().resolve()
    if work.exists():
        shutil.rmtree(work)
    if output.exists():
        output.mkdir(parents=True, exist_ok=True)
    work.mkdir(parents=True)
    output.mkdir(parents=True, exist_ok=True)
    for lane in ("control", "sigterm"):
        copy_metrics(root, lane, work)
    plot_azure = Path(__file__).with_name("plot_azure_metrics.py")
    plot_archive = Path(__file__).with_name("plot_archive_metrics.py")
    azure_out, archive_out = work / "azure-plots", work / "archive-plots"
    subprocess.run([sys.executable, str(plot_azure), "--experiment-root", str(work), "--output", str(azure_out)], check=True)
    subprocess.run([sys.executable, str(plot_archive), "--experiment-root", str(work), "--output", str(archive_out)], check=True)
    for file in azure_out.glob("*.png"):
        shutil.copy2(file, output / file.name)
    for file in archive_out.glob("*.png"):
        target = file.name.replace("azure-blob-success-latency", "azure-blob-successful-latency")
        shutil.copy2(file, output / target)
    render_stall(root, output / "recovered-writer-stall.png")
    render_guest_diskstats(root, output)
    render_bcc_latency(root, output)
    signal_out = work / "signal-plots"
    subprocess.run([sys.executable, str(Path(__file__).with_name("plot_signal_comparison.py")),
                    "--experiment-root", str(root), "--preview-incomplete",
                    "--output-directory", str(signal_out)], check=True)
    shutil.copy2(signal_out / "comparison-overview.png", output / "comparison-overview.png")
    azure_coverage = json.loads((azure_out / "azure-plot-coverage.json").read_text())
    archive_coverage = json.loads((archive_out / "azure-blob-archive-coverage.json").read_text())
    signal_coverage = json.loads((signal_out / "comparison-overview.json").read_text())
    guest_samples = {}
    for lane in ("control", "sigterm"):
        samples = [json.loads(line) for line in (root / lane / "results/measurement/samples.jsonl").read_text().splitlines()
                   if line.strip()]
        guest_samples[lane] = {"pairedSampleCount": len(samples),
                               "lastSampleUtc": max(row["primary"]["ts"] for row in samples)}
    bcc_counts = {}
    for lane in ("control", "sigterm"):
        count = sum(1 for line in (root / lane / "results/instrumentation/bcc-read-write-histograms.jsonl").open()
                    if line.strip())
        bcc_counts[lane] = {"readWriteHistogramRows": count}
    coverage = {
        "schemaVersion": 1, "linearAxes": True,
        "scope": {"azureNativeTimeStartUtc": "2026-10-13T05:13:00Z",
                  "azureNativeTimeEndUtc": "2026-10-13T08:36:00Z",
                  "treatmentClientOfferProofCutoffUtc": "2026-10-13T07:38:14.222643Z",
                  "postStreamContextSeconds": 300,
                  "rule": "Points are clipped by native timestamp for display. Source files remain unchanged; missing samples are not filled."},
        "azureMonitor": {"nativePointCount": azure_coverage.get("deduplicatedNativePoints"),
                         "charts": {key: {"linear": value.get("scale") == "linear",
                                          "plottedLinePoints": value.get("plottedLinePoints"),
                                          "missingPanels": value.get("missingPanels", [])}
                                    for key, value in azure_coverage.get("charts", {}).items()}},
        "azureBlob": {"nativeGrain": archive_coverage.get("nativeGrain"),
                      "nativePointCount": sum(value.get("deduplicatedPointCount", 0)
                                              for value in archive_coverage.get("sourcePoints", {}).values()
                                              if isinstance(value, dict)),
                      "charts": {key: {"linear": value.get("scale") == "linear",
                                       "lineCount": value.get("lineCount"),
                                       "missingPanel": value.get("missingPanel")}
                                 for key, value in archive_coverage.get("charts", {}).items()}},
        "guestDiskstats": {"lastSampleByLane": guest_samples, "intervalSeconds": 5,
                           "graphs": ["guest-iops.png", "guest-await.png", "guest-throughput.png"]},
        "bccReadWriteHistograms": {"rowsByLane": bcc_counts,
                                   "interpretation": "p95 displayed as log2-microsecond histogram bucket upper bound; figures do not imply exact individual-operation latency"},
        "clientReplicationOverview": {"nativeClientLogValidation": {
            run["lane"]: run["nativeLogValidation"].get("valid") for run in signal_coverage["runs"]},
            "caption": "Preview only. Treatment native per-transaction logs were unavailable; wait ages and replication telemetry come from sampled native measurements."},
    }
    (output / "graph-coverage.json").write_text(json.dumps(coverage, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"figures": sorted(path.name for path in output.glob("*.png")),
                      "privatePlotWork": str(work), "output": str(output)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
