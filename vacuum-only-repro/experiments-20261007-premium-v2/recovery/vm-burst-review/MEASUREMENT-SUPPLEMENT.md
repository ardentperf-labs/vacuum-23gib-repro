# VACUUM guest I/O and VMSS metrics supplement

This supplement describes the three completed VACUUM observations. It reports guest block I/O and Azure Monitor VMSS percentages as separate evidence streams; it does not convert VMSS percentages into disk throughput or IOPS, infer per-node activity from the scale-set aggregate, or conclude that a burst mechanism caused any change.

## Measurement intervals

The full observation runs from the collector's `observation_started` event through `timing.observationEnd`, the timestamp of the final observation sample. It includes replay drain and ends only after the replica reached the target LSN. VACUUM-only intervals use the exact SQL start/completion marker timestamps. Replay drain is reported from VACUUM completion to the final observation sample.

| Stack | Full observation (UTC) | VACUUM (UTC) | Replay drain (UTC) | Replay drain |
|---|---|---|---|---:|
| Baseline, 23 GiB | 2026-10-07T08:36:00.429Z – 2026-10-07T09:09:00.293801378Z | 2026-10-07T08:36:00.425657Z – 2026-10-07T08:50:04.738699Z | 2026-10-07T08:50:04.738699Z – 2026-10-07T09:09:00.293801378Z | 18.9 min |
| 50 GiB | 2026-10-07T08:35:56.502Z – 2026-10-07T09:48:04.712695505Z | 2026-10-07T08:35:56.509036Z – 2026-10-07T09:06:27.031178Z | 2026-10-07T09:06:27.031178Z – 2026-10-07T09:48:04.712695505Z | 41.6 min |
| 100 GiB | 2026-10-07T08:36:00.982Z – 2026-10-07T12:12:15.673044477Z | 2026-10-07T08:36:00.983779Z – 2026-10-07T10:34:28.321130Z | 2026-10-07T10:34:28.321130Z – 2026-10-07T12:12:15.673044477Z | 97.8 min |

## VMSS metrics — Whole observation, including replay drain

Each cell is arithmetic mean (minimum–maximum) of the included Azure PT5M `Average` point values, followed by available/eligible point count. The filter uses the Azure timestamp as the bucket start and includes a point only if that complete five-minute bucket lies inside this stack's exact phase interval. Values come from the latest collector response per metric/timestamp.

| Stack | Remote burst I/O credits (%) | Remote burst BPS credits (%) | Uncached IOPS consumed (%) | Uncached bandwidth consumed (%) |
|---|---:|---:|---:|---:|
| Baseline, 23 GiB | 0.00% (0.00–0.00%), 5/5 | 0.00% (0.00–0.00%), 5/5 | 55.50% (48.00–63.20%), 5/5 | 76.00% (50.00–100.00%), 5/5 |
| 50 GiB | 0.00% (0.00–0.00%), 13/13 | 0.00% (0.00–0.00%), 13/13 | 55.28% (48.00–65.50%), 13/13 | 72.72% (50.00–100.00%), 13/13 |
| 100 GiB | 0.00% (0.00–0.00%), 42/42 | 0.00% (0.00–0.00%), 42/42 | 36.15% (14.50–65.50%), 42/42 | 77.56% (49.40–100.00%), 42/42 |

## VMSS metrics — VACUUM-only interval

Each cell is arithmetic mean (minimum–maximum) of the included Azure PT5M `Average` point values, followed by available/eligible point count. The filter uses the Azure timestamp as the bucket start and includes a point only if that complete five-minute bucket lies inside this stack's exact phase interval. Values come from the latest collector response per metric/timestamp.

| Stack | Remote burst I/O credits (%) | Remote burst BPS credits (%) | Uncached IOPS consumed (%) | Uncached bandwidth consumed (%) |
|---|---:|---:|---:|---:|
| Baseline, 23 GiB | 0.00% (0.00–0.00%), 2/2 | 0.00% (0.00–0.00%), 2/2 | 62.85% (62.50–63.20%), 2/2 | 100.00% (100.00–100.00%), 2/2 |
| 50 GiB | 0.00% (0.00–0.00%), 5/5 | 0.00% (0.00–0.00%), 5/5 | 63.74% (62.30–65.50%), 5/5 | 100.00% (100.00–100.00%), 5/5 |
| 100 GiB | 0.00% (0.00–0.00%), 22/22 | 0.00% (0.00–0.00%), 22/22 | 37.35% (14.50–65.50%), 22/22 | 99.67% (99.10–100.00%), 22/22 |

## Azure PT5M point coverage

Timestamp ranges below are inclusive PT5M bucket start timestamps. Every included bucket ends no later than the corresponding phase end; the exact event boundaries are in the measurement-interval table. Coverage is reported for each of the four metrics.

| Stack | Whole observation starts (available / eligible) | VACUUM starts (available / eligible) |
|---|---|---|
| Baseline, 23 GiB | 2026-10-07T08:40:00Z–2026-10-07T09:00:00Z (5/5) | 2026-10-07T08:40:00Z–2026-10-07T08:45:00Z (2/2) |
| 50 GiB | 2026-10-07T08:40:00Z–2026-10-07T09:40:00Z (13/13) | 2026-10-07T08:40:00Z–2026-10-07T09:00:00Z (5/5) |
| 100 GiB | 2026-10-07T08:40:00Z–2026-10-07T12:05:00Z (42/42) | 2026-10-07T08:40:00Z–2026-10-07T10:25:00Z (22/22) |

## Guest disk I/O by phase

The table reports time-weighted means from `/proc/diskstats` counter deltas, clipped to exact phase boundaries. Reads and writes are kept separate. Each DATA and WAL device is a separate series on its primary or replica node; they are never collapsed across roles. IOPS are completed operations per second. MB/s is decimal throughput from 512-byte sectors.

| Stack | Phase | Primary DATA | Primary WAL | Replica DATA | Replica WAL |
|---|---|---:|---:|---:|---:|
| Baseline, 23 GiB | Whole observation | R 191 / W 27 IOPS; R 24.8 / W 25.1 MB/s | R 35 / W 104 IOPS; R 4.6 / W 20.1 MB/s | R 3008 / W 38 IOPS; R 24.7 / W 24.9 MB/s | R 139 / W 80 IOPS; R 17.8 / W 37.6 MB/s |
| Baseline, 23 GiB | VACUUM | R 446 / W 63 IOPS; R 58.2 / W 58.4 MB/s | R 82 / W 243 IOPS; R 10.7 / W 47.1 MB/s | R 3032 / W 37 IOPS; R 24.9 / W 24.4 MB/s | R 177 / W 182 IOPS; R 22.7 / W 87.9 MB/s |
| Baseline, 23 GiB | Replay drain | R 1 / W 0 IOPS; R 0.1 / W 0.4 MB/s | R 0 / W 0 IOPS; R 0.0 / W 0.1 MB/s | R 2990 / W 38 IOPS; R 24.5 / W 25.3 MB/s | R 111 / W 3 IOPS; R 14.2 / W 0.2 MB/s |
| 50 GiB | Whole observation | R 192 / W 26 IOPS; R 25.0 / W 25.0 MB/s | R 61 / W 103 IOPS; R 7.6 / W 19.9 MB/s | R 3011 / W 38 IOPS; R 24.7 / W 24.9 MB/s | R 144 / W 78 IOPS; R 18.4 / W 37.4 MB/s |
| 50 GiB | VACUUM | R 454 / W 62 IOPS; R 59.2 / W 59.0 MB/s | R 144 / W 244 IOPS; R 18.1 / W 47.2 MB/s | R 3017 / W 37 IOPS; R 24.7 / W 25.1 MB/s | R 172 / W 180 IOPS; R 21.9 / W 88.2 MB/s |
| 50 GiB | Replay drain | R 0 / W 0 IOPS; R 0.0 / W 0.1 MB/s | R 0 / W 0 IOPS; R 0.0 / W 0.0 MB/s | R 3007 / W 38 IOPS; R 24.6 / W 24.8 MB/s | R 123 / W 3 IOPS; R 15.8 / W 0.1 MB/s |
| 100 GiB | Whole observation | R 129 / W 18 IOPS; R 16.9 / W 16.7 MB/s | R 13 / W 69 IOPS; R 1.7 / W 13.1 MB/s | R 2015 / W 27 IOPS; R 16.5 / W 16.7 MB/s | R 96 / W 47 IOPS; R 12.3 / W 25.0 MB/s |
| 100 GiB | VACUUM | R 236 / W 33 IOPS; R 30.8 / W 30.4 MB/s | R 24 / W 127 IOPS; R 3.1 / W 23.9 MB/s | R 1845 / W 25 IOPS; R 15.1 / W 15.2 MB/s | R 96 / W 83 IOPS; R 12.3 / W 45.5 MB/s |
| 100 GiB | Replay drain | R 0 / W 0 IOPS; R 0.0 / W 0.0 MB/s | R 0 / W 0 IOPS; R 0.0 / W 0.0 MB/s | R 2222 / W 29 IOPS; R 18.2 / W 18.4 MB/s | R 96 / W 3 IOPS; R 12.2 / W 0.1 MB/s |

## Scope and limitations

The VMSS records are one host-level aggregate series for each exact two-instance `Standard_D2s_v4` scale set. They do not attribute I/O to an individual VM, so they cannot be divided into per-node peaks. The reported percentages are retained as percentages and are not normalized to a disk limit. The two reported burst-credit series were available in the collected responses; zero values do not prove eligibility or ineligibility for Premium SSD v2 burst behavior.

Guest counters are Linux block-device totals for the PVC-mapped DATA and WAL device on each role. They are not Azure managed-disk telemetry. Guest bins are aligned to UTC five-minute boundaries and clipped to the observation window and available samples. The accompanying JSON records the actual clipped sample endpoints and coverage for every bin. Phase means use sample-counter deltas clipped at exact phase boundaries, with linear interpolation only across adjacent samples no more than 12.5 seconds apart.

The Azure metric endpoint returns five-minute averages, and its UTC bucket boundaries need not coincide exactly with experiment events. A metric point is included only when its whole five-minute bucket falls inside the specified phase. The independent streams should be compared descriptively; they do not establish a causal performance ceiling or burst-expiry mechanism.

Machine-readable point selections and guest-bin calculations: [`measurement-calculations.json`](measurement-calculations.json). The latest-per-timestamp deduplication used 46 preserved collector captures, including the final 24-hour query `vmss-metrics-20261007T121255Z.json` at 2026-10-07T12:12:55Z; later captures supersede overlapping timestamps when they contain that point. The newest capture in the folder is `vmss-metrics-20261007T121548Z.json` at 2026-10-07T12:15:48Z.
