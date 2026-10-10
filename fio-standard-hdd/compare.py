#!/usr/bin/env python3
"""Combine and plot cross-region Standard HDD fio results.

Primary comparisons use each summary.csv's common-window guest block-device
counters. Fio grouped statistics and guarded Azure PT1M metrics remain in the
combined CSV and are plotted as separate series where applicable. Optional
Single-validation observations show the measured single-thread range;
the separate headroom stack is supporting evidence and never enters the main
1.5-second await threshold calculation.

Run from any working directory with:

    python fio-standard-hdd/compare.py --allow-partial

The default requires all four primary East US and West US 2 summary files.
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
REGIONS = (("eastus", "East US"), ("westus2", "West US 2"))
WORKLOADS = (
    ("4KiB", "fio-hdd-repro"),
    ("1MiB", "fio-hdd-1m-repro"),
)
THRESHOLD_MS = 1500.0

METADATA_FIELDS = (
    "region",
    "region_label",
    "block_size",
    "source_role",
    "source_file",
)

# Kept as the schema for an empty partial archive. Actual source columns are
# unioned with this list, so newer summary fields remain preserved automatically.
BASE_SUMMARY_FIELDS = (
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
)

STATUS_FIELDS = (
    "region",
    "workload",
    "source_role",
    "source_file",
    "status",
    "rows",
    "scope",
)

SUMMARY_FIELDS = (
    "region",
    "region_label",
    "workload",
    "main_source_file",
    "main_rows",
    "max_tested_threads",
    "threshold_ms",
    "threshold_search_max_threads",
    "threshold_metric",
    "threshold_status",
    "first_threads_at_or_above_threshold",
    "first_guest_await_ms_at_or_above_threshold",
    "single_validation_rows",
    "single_baseline_rate_metric",
    "single_baseline_rate_low",
    "single_baseline_rate_high",
    "single_baseline_guest_await_low_ms",
    "single_baseline_guest_await_high_ms",
    "threshold_scope",
)


class InputError(RuntimeError):
    """Raised when an input CSV exists but cannot be safely compared."""


def numeric(row: dict[str, str], field: str) -> float | None:
    raw = row.get(field, "")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def has_observation(row: dict[str, str], field: str, count_field: str | None) -> bool:
    if numeric(row, field) is None:
        return False
    if count_field is None:
        return True
    count = numeric(row, count_field)
    return count is not None and count > 0


def read_summary(path: Path, required_fields: set[str]) -> tuple[list[str], list[dict[str, str]]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            fields = reader.fieldnames or []
            missing = sorted(required_fields - set(fields))
            if missing:
                raise InputError(
                    f"{path.relative_to(ROOT)} lacks required columns: {', '.join(missing)}"
                )
            rows = [dict(row) for row in reader if row and any((v or "").strip() for v in row.values())]
    except OSError as exc:
        raise InputError(f"Cannot read {path.relative_to(ROOT)}: {exc}") from exc
    return fields, rows


def input_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for region, label in REGIONS:
        for workload, folder in WORKLOADS:
            specs.append(
                {
                    "region": region,
                    "region_label": label,
                    "workload": workload,
                    "folder": folder,
                    "role": "main",
                    "required": True,
                    "path": ROOT / region / folder / "artifacts/analysis/summary.csv",
                    "scope": "primary regional run; threshold and regional series",
                }
            )

    # Single-thread repeats show measured baseline ranges for each region.
    # They do not contribute to the main curves or threshold calculation.
    for region, label in REGIONS:
        for workload, folder in WORKLOADS:
            specs.append(
                {
                    "region": region,
                    "region_label": label,
                    "workload": workload,
                    "folder": folder,
                    "role": "single_validation",
                    "required": False,
                    "path": ROOT / region / folder / "artifacts/single-validation/analysis/summary.csv",
                    "scope": "optional single-thread baseline range; excluded from main curves and threshold",
                }
            )

    # This isolated D8ds_v4 run supplied 4/144/160-thread headroom evidence for
    # the East US 1 MiB result. It is deliberately supplemental.
    specs.append(
        {
            "region": "eastus",
            "region_label": "East US",
            "workload": "1MiB",
            "folder": "fio-hdd-1m-headroom",
            "role": "supporting_headroom",
            "required": False,
            "path": ROOT / "eastus/fio-hdd-1m-headroom/artifacts/analysis/summary.csv",
            "scope": "supporting headroom validation only; excluded from main curves and threshold",
        }
    )
    return specs


def load_inputs(allow_partial: bool) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[str]]:
    required_columns = {
        "run",
        "threads",
        "fio_iops",
        "fio_MB_s",
        "guest_r_await_ms",
        "guest_r_s",
        "guest_MB_s",
        "guest_full_samples",
        "azure_read_iops",
        "azure_read_iops_minutes",
        "azure_read_MB_s",
        "azure_read_MB_s_minutes",
        "azure_data_disk_latency_ms",
        "azure_data_disk_latency_minutes",
    }
    records: list[dict[str, Any]] = []
    statuses: list[dict[str, str]] = []
    original_fields: list[str] = []
    missing_required: list[str] = []

    for spec in input_specs():
        path: Path = spec["path"]
        relpath = path.relative_to(ROOT).as_posix()
        status = {
            "region": spec["region"],
            "workload": spec["workload"],
            "source_role": spec["role"],
            "source_file": relpath,
            "status": "missing",
            "rows": "0",
            "scope": spec["scope"],
        }
        if not path.is_file():
            if spec["required"]:
                missing_required.append(relpath)
            statuses.append(status)
            continue

        fields, raw_rows = read_summary(path, required_columns)
        status["status"] = "present" if raw_rows else "empty"
        status["rows"] = str(len(raw_rows))
        statuses.append(status)
        if spec["required"] and not raw_rows:
            missing_required.append(relpath)

        for field in fields:
            if field not in original_fields:
                original_fields.append(field)
        for raw in raw_rows:
            row: dict[str, Any] = {
                "region": spec["region"],
                "region_label": spec["region_label"],
                "block_size": spec["workload"],
                "source_role": spec["role"],
                "source_file": relpath,
            }
            row.update(raw)
            records.append(row)

    if missing_required and not allow_partial:
        paths = "\n  ".join(missing_required)
        raise InputError(
            "All four primary regional summaries are required for a final comparison. "
            "Missing or empty input(s):\n  "
            f"{paths}\nUse --allow-partial only for an explicitly incomplete preview."
        )
    if not any(row["source_role"] == "main" for row in records):
        raise InputError("No primary summary CSV is available; no comparison can be generated.")

    return records, statuses, original_fields


def write_csv(path: Path, fields: tuple[str, ...] | list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") if row.get(field) is not None else "" for field in fields})


def rows_for(
    records: list[dict[str, Any]], region: str, workload: str, role: str = "main"
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in records
        if row.get("region") == region
        and row.get("block_size") == workload
        and row.get("source_role") == role
    ]
    return sorted(rows, key=lambda row: numeric(row, "threads") or math.inf)


def format_number(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6g}"


def first_threshold_point(rows: list[dict[str, Any]]) -> tuple[float, float] | None:
    eligible: list[tuple[float, float]] = []
    for row in rows:
        threads = numeric(row, "threads")
        await_ms = numeric(row, "guest_r_await_ms")
        samples = numeric(row, "guest_full_samples")
        if threads is None or threads < 1:
            continue
        if await_ms is None or samples is None or samples <= 0:
            continue
        eligible.append((threads, await_ms))
    for threads, await_ms in sorted(eligible):
        if await_ms >= THRESHOLD_MS:
            return threads, await_ms
    return None


def baseline_range(
    records: list[dict[str, Any]], region: str, workload: str, field: str, count_field: str | None
) -> tuple[float, float] | None:
    rows = rows_for(records, region, workload, "main") + rows_for(
        records, region, workload, "single_validation"
    )
    values: list[float] = []
    for row in rows:
        if numeric(row, "threads") != 1:
            continue
        if has_observation(row, field, count_field):
            value = numeric(row, field)
            if value is not None:
                values.append(value)
    if len(values) < 2:
        return None
    return min(values), max(values)


def region_summary(records: list[dict[str, Any]], statuses: list[dict[str, str]]) -> list[dict[str, str]]:
    status_by_key = {
        (row["region"], row["workload"], row["source_role"]): row
        for row in statuses
    }
    summary: list[dict[str, str]] = []
    for region, label in REGIONS:
        for workload, folder in WORKLOADS:
            main = rows_for(records, region, workload, "main")
            main_path = f"{region}/{folder}/artifacts/analysis/summary.csv"
            status = status_by_key.get((region, workload, "main"), {}).get("status", "missing")
            if status != "present":
                crossing_status = "missing_input"
            elif not any(
                numeric(row, "guest_r_await_ms") is not None
                and (numeric(row, "guest_full_samples") or 0) > 0
                and (numeric(row, "threads") or 0) >= 1
                for row in main
            ):
                crossing_status = "missing_guest_await"
            else:
                crossing_status = "not_reached"
            crossing = first_threshold_point(main)
            threshold_rows = [
                row
                for row in main
                if (numeric(row, "threads") or 0) >= 1
                and numeric(row, "guest_r_await_ms") is not None
                and (numeric(row, "guest_full_samples") or 0) > 0
            ]
            threshold_search_max_threads = max(
                (numeric(row, "threads") or 0 for row in threshold_rows), default=None
            )
            if crossing is not None:
                crossing_status = "crossed"

            single_rows = rows_for(records, region, workload, "single_validation")
            rate_field = "guest_r_s" if workload == "4KiB" else "guest_MB_s"
            rate_count = "guest_full_samples"
            rate_values: list[float] = []
            await_values: list[float] = []
            for row in main + single_rows:
                if numeric(row, "threads") != 1:
                    continue
                if has_observation(row, rate_field, rate_count):
                    value = numeric(row, rate_field)
                    if value is not None:
                        rate_values.append(value)
                if has_observation(row, "guest_r_await_ms", "guest_full_samples"):
                    value = numeric(row, "guest_r_await_ms")
                    if value is not None:
                        await_values.append(value)

            max_threads = max((numeric(row, "threads") or 0 for row in main), default=None)
            summary.append(
                {
                    "region": region,
                    "region_label": label,
                    "workload": workload,
                    "main_source_file": main_path,
                    "main_rows": str(len(main)),
                    "max_tested_threads": format_number(max_threads),
                    "threshold_ms": format_number(THRESHOLD_MS),
                    "threshold_search_max_threads": format_number(threshold_search_max_threads),
                    "threshold_metric": "guest_r_await_ms",
                    "threshold_status": crossing_status,
                    "first_threads_at_or_above_threshold": format_number(crossing[0] if crossing else None),
                    "first_guest_await_ms_at_or_above_threshold": format_number(crossing[1] if crossing else None),
                    "single_validation_rows": str(len(single_rows)),
                    "single_baseline_rate_metric": rate_field,
                    "single_baseline_rate_low": format_number(min(rate_values) if len(rate_values) >= 2 else None),
                    "single_baseline_rate_high": format_number(max(rate_values) if len(rate_values) >= 2 else None),
                    "single_baseline_guest_await_low_ms": format_number(min(await_values) if len(await_values) >= 2 else None),
                    "single_baseline_guest_await_high_ms": format_number(max(await_values) if len(await_values) >= 2 else None),
                    "threshold_scope": "all actual main regional sweep points with guest samples; guest common-window mean; no interpolation",
                }
            )
    return summary


def load_plotting() -> tuple[Any, Any]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise InputError("Plotting requires matplotlib in the selected Python environment.") from exc
    return matplotlib, plt


REGION_COLORS = {"eastus": "#1f77b4", "westus2": "#d62728"}
REGION_LABELS = dict(REGIONS)


def missing_region_note(records: list[dict[str, Any]], workload: str) -> str:
    missing = []
    for region, label in REGIONS:
        if not rows_for(records, region, workload, "main"):
            missing.append(label)
    return f" — partial; missing {', '.join(missing)}" if missing else ""


def draw_measurement_series(
    ax: Any,
    records: list[dict[str, Any]],
    workload: str,
    field: str,
    label_suffix: str,
    linestyle: str,
    marker: str,
    count_field: str | None,
    max_threads: int | None,
) -> None:
    for region, _label in REGIONS:
        color = REGION_COLORS[region]
        rows = rows_for(records, region, workload, "main")
        xs: list[float] = []
        ys: list[float] = []
        for row in rows:
            threads = numeric(row, "threads")
            value = numeric(row, field)
            if threads is None or value is None:
                continue
            if max_threads is not None and threads > max_threads:
                continue
            if not has_observation(row, field, count_field):
                continue
            xs.append(threads)
            ys.append(value)
        if xs:
            ax.plot(
                xs,
                ys,
                color=color,
                linestyle=linestyle,
                marker=marker,
                linewidth=1.7,
                markersize=4.2,
                label=f"{REGION_LABELS[region]} {label_suffix}",
            )

        # An optional regional repeat provides a real range around the main x=1
        # reading. It is an error bar on the same measured point, not a new
        # series connected into the main sweep.
        if max_threads in (None, 32, 64):
            main_one = next((r for r in rows if numeric(r, "threads") == 1), None)
            repeat = rows_for(records, region, workload, "single_validation")
            repeat_values = [
                numeric(r, field)
                for r in repeat
                if numeric(r, "threads") == 1 and has_observation(r, field, count_field)
            ]
            if main_one and has_observation(main_one, field, count_field) and repeat_values:
                main_value = numeric(main_one, field)
                all_values = [main_value, *[v for v in repeat_values if v is not None]]
                if main_value is not None and len(all_values) >= 2:
                    lo, hi = min(all_values), max(all_values)
                    ax.errorbar(
                        [1],
                        [main_value],
                        yerr=[[main_value - lo], [hi - main_value]],
                        fmt="none",
                        ecolor=color,
                        elinewidth=1.3,
                        capsize=4,
                        zorder=4,
                    )


def draw_headroom_markers(
    ax: Any,
    records: list[dict[str, Any]],
    field: str,
    label_suffix: str,
    marker: str,
    count_field: str | None,
    max_threads: int | None,
) -> None:
    rows = rows_for(records, "eastus", "1MiB", "supporting_headroom")
    xs: list[float] = []
    ys: list[float] = []
    for row in rows:
        threads, value = numeric(row, "threads"), numeric(row, field)
        if threads is None or value is None or not has_observation(row, field, count_field):
            continue
        if max_threads is not None and threads > max_threads:
            continue
        xs.append(threads)
        ys.append(value)
    if xs:
        ax.plot(
            xs,
            ys,
            linestyle="none",
            marker=marker,
            color="#666666",
            markerfacecolor="none",
            markersize=6,
            label=f"East US headroom validation, supporting only — {label_suffix}",
        )


def finish_plot(
    plt: Any,
    ax: Any,
    destination: Path,
    title: str,
    ylabel: str,
    max_threads: int | None,
    note: str,
) -> None:
    ax.set_title(title)
    ax.set_xlabel("fio worker threads")
    ax.set_ylabel(ylabel)
    ax.set_xscale("linear")
    ax.set_yscale("linear")
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)
    if max_threads is not None:
        ax.set_xlim(left=0, right=max_threads + max(1, max_threads * 0.04))
    ax.grid(True, alpha=0.25, linewidth=0.65)
    ax.legend(frameon=False, fontsize=8, ncol=2, loc="best")
    destination.parent.mkdir(parents=True, exist_ok=True)
    fig = ax.figure
    fig.text(0.5, 0.01, note, ha="center", va="bottom", fontsize=7, color="#444444")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(destination, dpi=190)
    plt.close(fig)


def make_charts(records: list[dict[str, Any]], output_dir: Path) -> list[Path]:
    _matplotlib, plt = load_plotting()
    output_dir.mkdir(parents=True, exist_ok=True)
    outputs: list[Path] = []
    window_note = (
        "Guest series use common-window block-device counters; Azure uses guarded PT1M metrics; "
        "fio series use native grouped output. See combined.csv for sample counts."
    )

    # 4 KiB small-read IOPS: show guest, fio, and Azure as distinct sources.
    for limit, suffix, title_suffix in (
        (None, "full", "full thread sweep"),
        (64, "up-to-64", "through 64 threads"),
    ):
        fig, ax = plt.subplots(figsize=(10.2, 5.8))
        draw_measurement_series(ax, records, "4KiB", "guest_r_s", "guest IOPS (common window)", "-", "o", "guest_full_samples", limit)
        draw_measurement_series(ax, records, "4KiB", "fio_iops", "fio grouped IOPS", ":", "x", None, limit)
        draw_measurement_series(ax, records, "4KiB", "azure_read_iops", "Azure IOPS (guarded)", "--", "s", "azure_read_iops_minutes", limit)
        path = output_dir / f"iops-4k-{suffix}.png"
        finish_plot(plt, ax, path, f"4 KiB random-read IOPS — {title_suffix}{missing_region_note(records, '4KiB')}", "Read IOPS", limit, window_note)
        outputs.append(path)

    # 1 MiB throughput: guest common-window rate is the primary regional curve.
    for limit, suffix, title_suffix in (
        (None, "full", "full thread sweep"),
        (32, "up-to-32", "through 32 threads"),
    ):
        fig, ax = plt.subplots(figsize=(10.2, 5.8))
        draw_measurement_series(ax, records, "1MiB", "guest_MB_s", "guest MB/s (common window)", "-", "o", "guest_full_samples", limit)
        draw_measurement_series(ax, records, "1MiB", "fio_MB_s", "fio grouped MB/s", ":", "x", None, limit)
        draw_measurement_series(ax, records, "1MiB", "azure_read_MB_s", "Azure MB/s (guarded)", "--", "s", "azure_read_MB_s_minutes", limit)
        draw_headroom_markers(ax, records, "guest_MB_s", "guest MB/s", "D", "guest_full_samples", limit)
        draw_headroom_markers(ax, records, "azure_read_MB_s", "Azure MB/s", "s", "azure_read_MB_s_minutes", limit)
        path = output_dir / f"throughput-1m-{suffix}.png"
        finish_plot(plt, ax, path, f"1 MiB random-read throughput — {title_suffix}{missing_region_note(records, '1MiB')}", "Throughput (decimal MB/s)", limit, window_note)
        outputs.append(path)

    # Compare the two latency sources directly; each stays on linear axes.
    for workload, label, filename in (("4KiB", "4 KiB", "latency-4k.png"), ("1MiB", "1 MiB", "latency-1m.png")):
        fig, ax = plt.subplots(figsize=(10.2, 5.8))
        draw_measurement_series(ax, records, workload, "guest_r_await_ms", "guest await (common window)", "-", "o", "guest_full_samples", None)
        draw_measurement_series(ax, records, workload, "azure_data_disk_latency_ms", "Azure latency (guarded)", "--", "s", "azure_data_disk_latency_minutes", None)
        if workload == "1MiB":
            draw_headroom_markers(ax, records, "guest_r_await_ms", "guest await", "D", "guest_full_samples", None)
            draw_headroom_markers(ax, records, "azure_data_disk_latency_ms", "Azure latency", "s", "azure_data_disk_latency_minutes", None)
        path = output_dir / filename
        finish_plot(plt, ax, path, f"{label} read latency by worker count{missing_region_note(records, workload)}", "Mean latency / await (ms)", None, window_note)
        outputs.append(path)
    return outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Generate an explicitly partial preview and source-status.csv when primary regional summaries are missing.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "comparison",
        help="Output directory (default: fio-standard-hdd/comparison).",
    )
    args = parser.parse_args(argv)
    output_dir = args.output_dir if args.output_dir.is_absolute() else ROOT / args.output_dir

    try:
        records, statuses, source_fields = load_inputs(args.allow_partial)
        output_dir.mkdir(parents=True, exist_ok=True)
        combined_fields = list(METADATA_FIELDS)
        for field in (*BASE_SUMMARY_FIELDS, *source_fields):
            if field not in combined_fields:
                combined_fields.append(field)
        write_csv(output_dir / "combined.csv", combined_fields, records)
        write_csv(output_dir / "source-status.csv", STATUS_FIELDS, statuses)
        summary = region_summary(records, statuses)
        write_csv(output_dir / "region-summary.csv", SUMMARY_FIELDS, summary)

        primary_count = sum(row["source_role"] == "main" for row in records)
        missing_required = [row for row in statuses if row["source_role"] == "main" and row["status"] != "present"]
        chart_paths: list[Path] = []
        if primary_count:
            chart_paths = make_charts(records, output_dir)
        print(f"Combined {primary_count} primary measurement rows from {len(records)} total included rows.")
        if missing_required:
            print("Partial input status (no values were synthesized):")
            for row in missing_required:
                print(f"  {row['region']}/{row['workload']}: {row['source_file']} [{row['status']}]")
        for chart in chart_paths:
            print(f"Wrote {chart.relative_to(ROOT) if chart.is_relative_to(ROOT) else chart}")
        print(f"Wrote {output_dir / 'combined.csv'}")
        print(f"Wrote {output_dir / 'source-status.csv'}")
        print(f"Wrote {output_dir / 'region-summary.csv'}")
    except (InputError, OSError, csv.Error) as exc:
        print(f"compare.py: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
