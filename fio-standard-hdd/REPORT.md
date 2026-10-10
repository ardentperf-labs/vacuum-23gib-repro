# Azure Standard HDD read scaling: East US and West US 2

Both regions sustained roughly **560 IOPS for 4 KiB reads** and **102 MB/s for 1 MiB reads**. West US 2 required more threads to reach those rates and had slower single-thread measurements. For 1 MiB reads there, **160 threads** first reached the 1.5–5.0-second analysis band at **1.645 seconds**.

All four read-only fio sweeps used clones of the same prepared 512 GiB snapshot and the same sixteen allocated 1 GiB files, with caching None, direct IO, psync, and one outstanding read per worker. Each point requested a 30-second ramp and 180-second measurement. Guest block-device counters over the common UTC window provide headline rates; fio native worker windows can stagger and Azure uses guarded complete PT1M buckets. Throughput uses decimal MB/s; a 1 MiB read is 1,048,576 bytes.

## Initial and repeat single-thread measurements

Each repeat remains a separate observation. The rate interval is the measured initial/repeat range; the two runs do not explain the difference.

| Region | Workload | Guest rate, initial / repeat | Guest await, initial / repeat (s) | Azure rate, initial / repeat |
|---|---|---:|---:|---:|
| East US | 4 KiB | 91.074 / 110.027 IOPS | 0.011 / 0.009 | 90.890 (1 min) / 110.020 (1 min) |
| East US | 1 MiB | 60.037 / 75.487 MB/s | 0.017 / 0.014 | 58.955 (1 min) / 77.318 (1 min) |
| West US 2 | 4 KiB | 20.837 / 60.583 IOPS | 0.048 / 0.016 | 20.480 (1 min) / — (0 min) |
| West US 2 | 1 MiB | 16.964 / 39.832 MB/s | 0.062 / 0.026 | 15.969 (1 min) / 39.289 (1 min) |

## Saturation knees and latency thresholds

| Region | Workload | Knee: first tested point at sustained plateau | Guest rate at knee | Measurement criterion |
|---|---|---:|---:|---|
| East US | 4 KiB | 8 threads | 560.818 IOPS | First tested count reaching about 561 guest IOPS; higher points sustain the same rate. |
| East US | 1 MiB | 4 threads | 102.002 MB/s | First tested count reaching about 102 guest MB/s; two threads reached 99.53 MB/s. |
| West US 2 | 4 KiB | 16 threads | 560.282 IOPS | First tested count reaching about 560 guest IOPS; eight threads reached 294.58 IOPS. |
| West US 2 | 1 MiB | 8 threads | 102.005 MB/s | First tested count reaching about 102 guest MB/s; four threads reached 79.25 MB/s. |

The plateau counts are measured points, with no interpolation.

The following table searches guest await, fio mean completion latency, and Azure Data Disk Latency independently for the first actual primary point within the inclusive 1.5–5.0 s band. Azure entries require at least one guarded PT1M bucket; missing samples are shown as not measured, with counts in the source point tables.

| Region | Workload | First guest `r_await` in band | First fio mean completion in band | First Azure latency in band |
|---|---|---|---|---|
| East US | 4 KiB | 1024 threads; 1.821 s; 179 one-second samples | 1024 threads; 1.821 s | not in band (15 sampled points; 15 guarded min total) |
| East US | 1 MiB | 256 threads; 2.632 s; 179 one-second samples | 256 threads; 2.632 s | 256 threads; 2.631 s; 1 guarded min |
| West US 2 | 4 KiB | not in band (15 sampled points; 2685 one-second samples total) | 1024 threads; 1.600 s | not in band (13 sampled points; 13 guarded min total) |
| West US 2 | 1 MiB | 160 threads; 1.645 s; 179 one-second samples | 160 threads; 1.645 s | 160 threads; 1.645 s; 1 guarded min |

## Main tested points

Every tested point has a complete table and a link to its raw `run-<threads>/` artifacts. West US 2’s 1 MiB report includes a separate detailed table for the 768- and 1,024-thread points; both are configured to complete their full measurements regardless of five-second latency. East US 4 KiB has 15 successful primary points and two failed 4,096-thread fio-initialization attempts; the failed attempts are retained but excluded from successful-point counts: [attempt 1](eastus/fio-hdd-repro/artifacts/run-4096/), [attempt 2](eastus/fio-hdd-repro/artifacts/run-4096-failed-1791524122/).

| Region | Workload | Main points | Complete point table | Primary CSV | Repeat CSV | Initial / repeat raw files |
|---|---|---:|---|---|---|---|
| East US | 4 KiB | 15 | [points and raw links](eastus/fio-hdd-repro/artifacts/analysis/report.md) | [CSV](eastus/fio-hdd-repro/artifacts/analysis/summary.csv) | [repeat CSV](eastus/fio-hdd-repro/artifacts/single-validation/analysis/summary.csv) | [initial](eastus/fio-hdd-repro/artifacts/run-1/) / [repeat](eastus/fio-hdd-repro/artifacts/single-validation/run-1/) |
| East US | 1 MiB | 10 | [points and raw links](eastus/fio-hdd-1m-repro/artifacts/analysis/report.md) | [CSV](eastus/fio-hdd-1m-repro/artifacts/analysis/summary.csv) | [repeat CSV](eastus/fio-hdd-1m-repro/artifacts/single-validation/analysis/summary.csv) | [initial](eastus/fio-hdd-1m-repro/artifacts/run-1/) / [repeat](eastus/fio-hdd-1m-repro/artifacts/single-validation/run-1/) |
| West US 2 | 4 KiB | 15 | [points and raw links](westus2/fio-hdd-repro/REPORT.md#all-tested-points) | [CSV](westus2/fio-hdd-repro/artifacts/analysis/summary.csv) | [repeat CSV](westus2/fio-hdd-repro/artifacts/single-validation/analysis/summary.csv) | [initial](westus2/fio-hdd-repro/artifacts/run-1/) / [repeat](westus2/fio-hdd-repro/artifacts/single-validation/run-1/) |
| West US 2 | 1 MiB | 14 | [points and raw links](westus2/fio-hdd-1m-repro/REPORT.md#all-tested-points) | [CSV](westus2/fio-hdd-1m-repro/artifacts/analysis/summary.csv) | [repeat CSV](westus2/fio-hdd-1m-repro/artifacts/single-validation/analysis/summary.csv) | [initial](westus2/fio-hdd-1m-repro/artifacts/run-1/) / [repeat](westus2/fio-hdd-1m-repro/artifacts/single-validation/run-1/) |

## Cross-run interpretation

| Region | Workload | Primary VM |
|---|---|---|
| East US | 4 KiB | `Standard_D4s_v4` |
| East US | 1 MiB | `Standard_D4s_v4` |
| West US 2 | 4 KiB | `Standard_E2bs_v5` |
| West US 2 | 1 MiB | `Standard_E2bs_v5` |

The East US main runs used Standard_D4s_v4; West US 2 used Standard_E2bs_v5. The West US 2 sweeps ran concurrently, while East US sweeps ran sequentially. The VM profiles and test overlap differ, so these observations do not isolate a causal regional effect.

## Captured West US 2 queue layout

Both captured West US 2 VM layouts show 2 Linux blk-mq queues, each with 316 tags (632 total) and active scheduler `none`: [4 KiB capture](westus2/fio-hdd-repro/artifacts/queue-topology.json), [1 MiB capture](westus2/fio-hdd-1m-repro/artifacts/queue-topology.json).

| Workload | Threads | Guest rate | Guest read IOPS | Guest await (s) | Estimated outstanding reads (rate × await) | fio mean completion (s) | Azure Data Disk Latency |
|---|---:|---:|---:|---:|---:|---:|---:|
| 4 KiB | 768 | 560.978 IOPS | 560.978 | 1.126 | ~631.5 | 1.306 | 456.270 ms (1 guarded min) |
| 4 KiB | 1024 | 561.140 IOPS | 561.140 | 1.126 | ~631.6 | 1.600 | 456.306 ms (1 guarded min) |
| 1 MiB | 768 | 101.993 MB/s | 97.268 | 6.496 | ~631.9 | 5.855 | 2631.628 ms (1 guarded min) |
| 1 MiB | 1024 | 100.212 MB/s | 95.570 | 6.607 | ~631.4 | 5.531 | 2631.529 ms (1 guarded min) |

For 4 KiB, guest rate × await estimates about 631.5 and 631.6 outstanding reads at 768 and 1,024 threads, close to the 632 captured queue tags; this offers a possible queue-depth interpretation of the plateau. The 1 MiB estimates are about 631.9 and 631.4 reads in flight at those points. These guest averages do not directly measure tag occupancy or establish causality. East US queue topology was not captured, so no East US queue-count comparison is available.

The nominal S20 disk specification is **500 IOPS / 60 MB/s**. The captured Azure runtime target was **550 IOPS / 100,000,000 bytes/s**. Both regions observed plateaus of approximately **560 IOPS** in 4 KiB reads and **102 MB/s** in 1 MiB reads. The nominal and observed limits differ; this experiment leaves that discrepancy unexplained. [Standard HDD specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types).

Fio-native rate aggregates are supporting measurements, not the headline. In particular, fio worker clocks can be staggered, while guest and Azure counters cover their own common or guarded windows. Empty Azure metrics and zero guarded-minute counts are preserved as missing; no bucket is synthesized, carried forward, or interpolated. Azure averages are not request percentiles. The Azure Data Disk Latency metric is preview and requires SCSI attachment. [Azure disk metrics](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-metrics).

## Figures

- [4 KiB IOPS, full sweep](comparison/iops-4k-full.png) and [through 64 threads](comparison/iops-4k-up-to-64.png)
- [1 MiB throughput, full sweep](comparison/throughput-1m-full.png) and [through 32 threads](comparison/throughput-1m-up-to-32.png)
- [4 KiB latency](comparison/latency-4k.png) and [1 MiB latency](comparison/latency-1m.png)

All comparison and per-run figures use linear axes starting at zero. [Combined source rows](comparison/combined.csv), [source status](comparison/source-status.csv), and [per-region summary](comparison/region-summary.csv) retain the underlying measurements and guarded sample counts.

## Snapshot preservation and supporting validation

The preserved snapshot inventory contains **45** IDs: all **44** original snapshots plus the West US 2 full keeper. The inventory is unchanged across cleanup, and both the original source and West regional keeper IDs are present. See [`transport-cleanup.json`](westus2/fixture-preparation/artifacts/transport-cleanup.json) for the recorded IDs. No snapshot was deleted.

The separate East US 1 MiB headroom test on Standard_D8ds_v4 sustained 102 MB/s at four threads with a documented 192 MB/s VM baseline. Its 144/160-thread refinements measured 1.480/1.645 seconds guest await, making 160 the first supporting point in the analysis band. They are separate from the original East US doubling sweep, whose first in-band point was 256 threads. [Headroom validation](eastus/fio-hdd-1m-headroom/artifacts/analysis/report.md).

West US 2 VM bandwidth consumption peaked at 58% in the 1 MiB main windows and CPU at 2.32%; returned used-burst-credit samples were zero where populated. [Validation and metric coverage](westus2/fio-hdd-1m-repro/artifacts/regional-validation.md). Both benchmark groups and the temporary transfer resources were deleted. The retained snapshot manifests support future reproductions.

## Run reports and fixture evidence

- [East US 4 KiB report](eastus/fio-hdd-repro/REPORT.md)
- [East US 1 MiB report](eastus/fio-hdd-1m-repro/REPORT.md)
- [West US 2 4 KiB report](westus2/fio-hdd-repro/REPORT.md)
- [West US 2 1 MiB report](westus2/fio-hdd-1m-repro/REPORT.md)
- [West US 2 fixture and preservation evidence](westus2/fixture-preparation/README.md)

