# Azure Standard HDD fio comparisons

This archive compares read scaling on Azure Standard HDD across East US and
West US 2. The four primary runs use the same prepared 512 GiB PostgreSQL
snapshot fixture on an isolated Standard HDD clone, with host caching disabled
and the guest data device mounted read-only. The archive keeps each run's
scripts, raw fio and guest samples, Azure metric responses, analysis, reports,
and graphs together by region.

## Primary run matrix

| Region | Block size | Run bundle | Status |
|---|---:|---|---|
| East US | 4 KiB | [run instructions](eastus/fio-hdd-repro/README.md), [report](eastus/fio-hdd-repro/REPORT.md) | Complete |
| East US | 1 MiB | [run instructions](eastus/fio-hdd-1m-repro/README.md), [report](eastus/fio-hdd-1m-repro/REPORT.md) | Complete |
| West US 2 | 4 KiB | [run instructions](westus2/fio-hdd-repro/README.md), [report](westus2/fio-hdd-repro/REPORT.md) | Complete |
| West US 2 | 1 MiB | [run instructions](westus2/fio-hdd-1m-repro/README.md), [report](westus2/fio-hdd-1m-repro/REPORT.md) | Complete |

The East US 1 MiB run has a separate [headroom validation bundle](eastus/fio-hdd-1m-headroom/README.md)
on a larger VM. Its measurements support interpretation of the throughput
plateau; they are not part of the primary regional sweep or the first-await
threshold calculation. East US single-thread repeats are retained under each
run's `artifacts/single-validation/` directory and show the measured baseline
range where available.

## Method

Both workloads use random reads, direct I/O, the synchronous `psync` engine,
and one request outstanding per worker. Each point has a 30-second ramp and a
180-second measurement. The 4 KiB sweep emphasizes IOPS. The 1 MiB sweep
emphasizes throughput and latency; the West US 2 main sweep includes the
144/160-thread refinements and is configured to run both 768 and 1,024 threads
to completion even if mean latency exceeds five seconds. Those points do not
use the 4 KiB sweep's five-second stop rule. East US's earlier 1 MiB main sweep and its
separate headroom validation retain their original point sets in their own
folders.

The comparison supports the separate single-thread repeat from both regions.
When available, repeat samples remain labeled separately in
`comparison/combined.csv` and contribute per-region baseline ranges and
error bars at the one-thread point; they do not extend the main curves or
affect the first-await threshold. All four repeats are included in this archive.

Regional throughput and guest await use block-device counters over the common
UTC measurement window. Azure comparisons use the guarded complete one-minute
LUN 0 metric buckets returned for each test point. Fio grouped statistics use
fio's worker timing and aggregation. These sources have different sampling and
aggregation windows; `comparison/combined.csv` retains their separate values,
Azure minute counts, and source-run labels. The plots use guest common-window
rates as the primary series, with fio and Azure values shown separately where
applicable.

The first-await summary searches main-run guest `r_await` measurements in
ascending thread count and reports the first tested point at or above 1,500 ms.
It uses the observed value at that point without interpolation. The separate
headroom validation never contributes to this threshold. All comparison plots
have linear axes and start their y-axis at zero.

## Comparison outputs

Run `compare.py` after all four primary `summary.csv` files have been copied:

```bash
python3 -m venv .venv
.venv/bin/pip install -r eastus/fio-hdd-repro/requirements.txt
.venv/bin/python compare.py
```

The default requires the East US and West US 2 primary summaries. For an incomplete archive, an explicit partial preview is available with
`.venv/bin/python compare.py --allow-partial`; `comparison/source-status.csv` records
missing inputs and no measurements are synthesized. The output directory
contains the combined rows, source status, per-region first-await summary, and
these figures:

- [4 KiB IOPS, full sweep](comparison/iops-4k-full.png) and
  [through 64 threads](comparison/iops-4k-up-to-64.png)
- [1 MiB throughput, full sweep](comparison/throughput-1m-full.png) and
  [through 32 threads](comparison/throughput-1m-up-to-32.png)
- [4 KiB latency](comparison/latency-4k.png) and
  [1 MiB latency](comparison/latency-1m.png)

The comparison files include all four primary runs and four separate single-thread repeats. See the [measured report](REPORT.md) for results and limitations.

## Reproducing a region run

Each run folder contains its provisioning, guest-test, polling, collection,
analysis, and teardown scripts plus its own requirements. Follow that region's
README from the region root. East US setup and the local keeper-manifest staging
step are documented in the [East US README](eastus/README.md).
