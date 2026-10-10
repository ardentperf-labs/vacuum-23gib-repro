# Unchanged 23-GiB repeat — completed experiment summary

Estimated total DATA read-operation amplification (replica ÷ primary) was **15.789×**: primary **~376,881 reads** (194.067 IOPS × 1,942.02s VACUUM), replica **~5,950,398 reads** (537.554 IOPS × 11,069.39s from VACUUM start through full replica catch-up). The replica interval includes replay while VACUUM was running and the later replay tail.

Primary VACUUM took **32m 22s**. Replica catch-up required **2h 32m 07s more after VACUUM finished**, or **3h 04m 29s from VACUUM start**. Replay overlapped VACUUM; the additional time is the post-VACUUM replay tail.

**Run:** `20261005T062051Z` · **Fixture:** 23-GiB heap (24,696,061,952 bytes) · **Region:** East US (`eastus`), same region as original. Both use two `Standard_D2s_v4` workers.

This repeat matches the earlier East US run: standby DATA reads stayed near 538 IOPS and 4.4 MB/s during VACUUM, with roughly 0.40 s read await. Across the whole run, the replica/primary DATA read-byte ratio was 0.990×: primary reads averaged about 127.5 KiB each, versus about 8.0 KiB on the standby, and the long replay tail adds many more standby operations without substantially more DATA bytes. The earlier run measured 35.14 minutes VACUUM, 149.43 minutes replay tail, and 537.39 standby DATA read IOPS during VACUUM.

## Timing and measurement

| Interval | UTC start | UTC end | Duration |
|---|---|---|---:|
| VACUUM | 2026-10-05T06:21:02.062624Z | 2026-10-05T06:53:24.079983Z | 32m 22s |
| Replay tail to caught-up marker | 2026-10-05T06:53:24.079983Z | 2026-10-05T09:25:31.449889982Z | 2h 32m 07s |
| VACUUM through catch-up | 2026-10-05T06:21:02.062624Z | 2026-10-05T09:25:31.449889982Z | 3h 04m 29s |

VACUUM exited 0; replay was caught up through three consecutive healthy samples with zero final lag. Collector recorded 2,216 samples; maximum gap 6.20s (12.5s maximum allowed); all three windows had valid coverage and zero rejected intervals. Primary is idle during the later replay tail. The opening amplification estimates total completed reads using primary VACUUM IOPS × VACUUM duration and replica full-replay IOPS × the complete replay interval.

## DATA reads by window

Each device cell is **IOPS / estimated total DATA-read bytes / read await (ms/op)**. Byte totals are derived from the stored time-weighted read MB/s and covered seconds. The final column compares DATA read bytes (replica ÷ primary); the opening amplification compares estimated total read operations. Await is reported per device.

| Window | Primary DATA | Replica DATA | Replica/primary read bytes |
|---|---:|---:|---:|
| VACUUM command | 194.067 / 49.207 GB / 5.69 | 537.811 / 8.559 GB / 403.04 | 0.173942× |
| Replay tail | 0.020 / 23.866 MB / 3.27 | 537.500 / 40.198 GB / 329.78 | 1684.34× |
| VACUUM + replay | 34.064 / 49.231 GB / 5.69 | 537.554 / 48.757 GB / 342.64 | 0.990366× |

The full report contains the four-device read/write IOPS, throughput, and await tables for each window. 
This confirms repeatability against the original East US run: VACUUM 35.14 vs 32.37 min, replay tail 149.43 vs 152.12 min, and VACUUM-window standby DATA reads 537.39 vs 537.81 IOPS.

## Settings and environment

Both primary and replica used `recovery_prefetch=try` from PostgreSQL default; no experiment-only GUC change.

| GUC | Primary | Replica |
|---|---|---|
| `wal_level` | `logical` (configuration file) | `logical` (configuration file) |
| `full_page_writes` | `on` (configuration file) | `on` (configuration file) |
| `recovery_prefetch` | `try` (default) | `try` (default) |
| `vacuum_cost_delay` | `0` (default) | `0` (default) |

No application client workload was created. The two PostgreSQL roles ran on separate `Standard_D2s_v4` workers with distinct 512-GiB `Standard_LRS` DATA and WAL disks, caching disabled. The same pinned PostgreSQL 16.15 image and 23-GiB fixture layout were used in both completed stacks.

## Report, graphs, and evidence

- [Full report](REPORT.md)
- [Linear-axis await graph](results/20261005T062051Z/await.png) · [Linear-axis IOPS graph](results/20261005T062051Z/iops.png) · [Linear-axis throughput graph](results/20261005T062051Z/throughput.png)
- [Machine-readable summary](results/20261005T062051Z/summary.json) · [timing](results/20261005T062051Z/timing.json) · [preflight](results/20261005T062051Z/preflight.json)
- [Raw samples](results/20261005T062051Z/samples.jsonl.gz) · [VACUUM log](results/20261005T062051Z/vacuum.log) · [monitor summary](results/20261005T062051Z/monitoring/monitor-summary.json)
- [Independent measurement audit](root-audit.json) · [cleanup result](cleanup/result.json) · [retained snapshot inventory](cleanup/snapshots-after.json) · [shared cleanup safety audit](../root-cleanup-audit.json)


## Cleanup

All three stack snapshots remained Succeeded and unchanged by unique ID. Cleanup removed worker resources and temporary storage/networking but left the AKS control plane stopped because two incremental snapshots remain in nodeRG; deleting AKS risks deleting that node resource group. The shared East US watcher is also retained until shared cleanup.

[Estimated total DATA read operations and amplification](results/20261005T062051Z/estimated-data-read-operations.json).
