# 100-GiB size variation — completed experiment summary

Primary VACUUM took **2h 46m 54s**. Replica catch-up required **10h 43m 01s more after VACUUM finished**, or **13h 29m 55s from VACUUM start**. Replay overlapped VACUUM; the additional time is the post-VACUUM replay tail, not the entire replay-processing duration.

Estimated total DATA read-operation amplification (replica ÷ primary) was **15.621×**: primary **~1,674,122 reads** (167.173 IOPS × 10,014.33s VACUUM), replica **~26,150,759 reads** (538.138 IOPS × 48,594.90s from VACUUM start through full replica catch-up). The replica interval includes replay while VACUUM was running and the later replay tail.

**Run:** `100gib-20261005T130618Z` · **Fixture:** 100-GiB heap (107,374,182,400 bytes) · **Region:** East US (`eastus`) · **Workers:** two `Standard_D2s_v4` · **Status:** VACUUM exit 0, replica caught up, zero final lag.

During VACUUM, replica DATA reads averaged 536.99 IOPS, 4.40 MB/s, and 389.88 ms await. Replica DATA read IOPS stayed near 537 during the replay tail as well (538.43 IOPS). Primary DATA reads fell from 167.17 IOPS during VACUUM to just 0.007 IOPS during the tail, so combined-window IOPS ratios are dominated by the near-idle primary denominator and are not a useful amplification measure. Across VACUUM plus replay, replica/primary DATA read bytes were 0.980×. On the primary, average reads were 127.54 KiB; on the replica they were 8.00 KiB.

## Timing and measurement

| Interval | UTC start | UTC end | Duration |
|---|---|---|---:|
| VACUUM | 2026-10-05T13:06:29.794989Z | 2026-10-05T15:53:24.121515Z | 2h 46m 54s |
| Replay tail after VACUUM | 2026-10-05T15:53:24.121515Z | 2026-10-06T02:36:24.697250138Z | 10h 43m 01s |
| VACUUM start through replica catch-up | 2026-10-05T13:06:29.794989Z | 2026-10-06T02:36:24.697250138Z | 13h 29m 55s |

Replica replay ran during VACUUM as well as afterward. The replay-tail duration is the additional wait from VACUUM completion to confirmed catch-up, not the full replay-processing period. Catch-up was confirmed over three healthy samples through both the VACUUM completion LSN and current primary flush LSN. Final LSNs both reached `C0/15000000`; final backlog was zero. The final VACUUM exit code was 0.

Collected 9,721 samples; maximum sample gap was 6.91 seconds (12.5 seconds maximum allowed). All four devices and all three windows have valid coverage with zero rejected intervals after the disclosed offline counter correction. The full report includes every primary/replica DATA/WAL read/write IOPS, throughput, and await value.

## DATA reads by window

Each device cell is **IOPS / estimated total DATA-read bytes / read await (ms/op)**. Byte totals use the recorded time-weighted MB/s and covered seconds.

| Window | Primary DATA | Replica DATA |
|---|---:|---:|
| VACUUM command | 167.173 / 218.647 GB / 6.45 | 536.994 / 44.071 GB / 389.88 |
| Replay tail | 0.007 / 0.035 GB / 3.77 | 538.435 / 170.211 GB / 333.09 |
| VACUUM + replay | 34.456 / 218.683 GB / 6.45 | 538.138 / 214.282 GB / 344.77 |

The opening amplification estimates total DATA read operations using each side's relevant average IOPS and duration. The replica interval includes concurrent replay and the much longer tail; primary operation totals use only the active VACUUM window.

## Settings and environment

This is the 100-GiB size variation; fixture heap size was the intended changed workload dimension from the 23-GiB repeat. Both experiments use the same East US region, two `Standard_D2s_v4` workers, PostgreSQL image, disk layout, fixture construction and VACUUM/replay framework, and measured GUC settings. Separate 512-GiB `Standard_LRS` HDD DATA/WAL disks ran with host caching disabled. No application client workload was created.

| GUC | Primary | Replica |
|---|---|---|
| `wal_level` | `logical` | `logical` |
| `full_page_writes` | `on` | `on` |
| `recovery_prefetch` | `try` | `try` |
| `vacuum_cost_delay` | `0` | `0` |

Other measured settings: `maintenance_io_concurrency=10`, `maintenance_work_mem=64MB`, `shared_buffers=256MB`, `max_wal_size=1GB`, and `checkpoint_timeout=5min` on both roles. Explicit workload command: `VACUUM (VERBOSE, FREEZE) public.vacuum_target;` The `pg_stat_wal` counters increased by 160,575,263,332 bytes, 26,397,193 full-page images, and 65,719,441 records from the pre-VACUUM checkpoint baseline through VACUUM completion. These cluster-level counters include background WAL and are not physical disk-write totals.

## Prepared cold snapshot

Before VACUUM, CNPG completed the offline primary backup `vacuum-cold-e92bdc27`. Its retained paired DATA/WAL snapshots are saved in `rg-vacuum-retained-e92bdc2762775aa5ecbb33bf383e563d`, each 512-GiB `Standard_ZRS` with `Retain` policy. The pair was independently restored in a one-instance validation Cluster without Blob credentials, a Barman ObjectStore, or external-cluster archive source. The recovered system identifier, 100-GiB heap, GUC defaults, index, and indexed even-live/adjacent-odd-deleted sentinels matched preparation evidence. PostgreSQL retained `archive_mode=on` and CNPG's internal WAL `restore_command`; neither required an external archive source for this snapshot restore. The recovery Job ran with the pinned system image, then the PostgreSQL pod was checked using the measurement image. Validation PVCs and Cluster were deleted; both source snapshots remained ready.

The cold checkpoint restarted the PostgreSQL postmaster and reset `shared_buffers`. A few small indexed sentinel checks ran after the snapshot and before measurement, which is a cache-state confound. The restored `pg_stat_user_tables` live/dead estimates are advisory and can reset or become stale after restart; fixture operation counts and index sentinels are the stronger evidence.

## Analysis correction

The raw samples are unchanged. Two replica DATA read-time deltas crossed the unsigned 32-bit millisecond-counter boundary; the original analysis rejected those two intervals. Offline analysis unwrapped only those read-time values modulo 2^32. Timestamps and all other counters were unchanged, producing valid coverage in all windows with zero rejected intervals. Raw `samples.jsonl.gz` SHA-256 is `acf37b109194529e8c68b66555eaf60aed5404e8e0d71d1b39c7d72a4852875d`. The run report links the correction, independent rollover audit, and preserved original summary and graphs.

## Report, graphs, and evidence

- [Full report](results/100gib-20261005T130618Z/REPORT.md)
- [Linear-axis await graph](results/100gib-20261005T130618Z/await.png) · [linear-axis IOPS graph](results/100gib-20261005T130618Z/iops.png) · [linear-axis throughput graph](results/100gib-20261005T130618Z/throughput.png)
- [Machine-readable summary](results/100gib-20261005T130618Z/summary.json) · [timing](results/100gib-20261005T130618Z/timing.json) · [continuous log monitor](results/100gib-20261005T130618Z/monitoring/monitor-summary.json)
- [Raw samples](results/100gib-20261005T130618Z/samples.jsonl.gz) · [VACUUM log](results/100gib-20261005T130618Z/vacuum.log) · [cold snapshot restore evidence](evidence/cold-snapshot-validation.json)
- [Counter rollover correction](results/100gib-20261005T130618Z/counter-rollover-correction.json) · [independent audit](results/100gib-20261005T130618Z/counter-rollover-audit.json) · [original analysis](results/100gib-20261005T130618Z/original-analysis-before-counter-rollover-correction/summary.json)

Stack cleanup finished at 03:14:54 UTC on October 6. All five snapshots are preserved and locked against deletion in the separate retained resource group. A fresh Azure audit confirms the main and node resource groups are absent and the retention group contains snapshots only. [Cleanup result](cleanup/result.json) · [fresh Azure verification](cleanup/root-fresh-azure-audit.json).

[Estimated total DATA read operations and amplification](results/100gib-20261005T130618Z/estimated-data-read-operations.json).
