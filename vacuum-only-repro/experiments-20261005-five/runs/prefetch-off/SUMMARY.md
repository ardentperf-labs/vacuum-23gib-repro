# Standby recovery_prefetch=off — completed experiment summary

Estimated total DATA read-operation amplification (replica ÷ primary) was **0.000892×**: primary **~376,772 reads** (243.812 IOPS × 1,545.34s VACUUM), replica **~336 reads** (0.216 IOPS × 1,558.17s from VACUUM start through full replica catch-up). The replica interval includes replay while VACUUM was running and the later replay tail.

Primary VACUUM took **25m 45s**. Replica catch-up required **12.8s more after VACUUM finished**, or **25m 58s from VACUUM start**. Replay overlapped VACUUM; the additional time is the post-VACUUM replay tail.

**Run:** `20261005T072928Z-prefetch-off` · **Fixture:** 23-GiB heap (24,696,061,952 bytes) · **Region:** West US 3 (`westus3`) rather than East US: the required same-region D2s_v4 capacity was unavailable. This is a material region confound against the East US baselines.

The measured standby DATA read stream was dramatically smaller with `recovery_prefetch=off`: 0.217 IOPS and 14.434 MB during VACUUM; 14.434 MB across VACUUM plus catch-up, versus 49.216 GB on the primary. This is an observed association, not a causal attribution. Across the whole run, effective read sizes were about 130,580 B/op on primary and 42,961 B/op on standby, but total standby reads were only about 336 operations. Similar WAL/FPI totals and zero measured recovery-prefetch counter deltas do not establish which mechanism caused the difference.

The independent raw recount confirms the 0.000892× estimate: 335.986 prorated standby DATA reads over VACUUM start through catch-up versus 376,771.915 primary reads during VACUUM. The raw capture has 339 standby DATA read completions across its full sample span. Stable mount/PV/LUN checks distinguish standby DATA (`/var/lib/postgresql/data`, LUN 0, `/dev/sdc` 8:32) from WAL (`/var/lib/postgresql/wal`, LUN 1, `/dev/sdb` 8:16; 93,734 raw reads). This counts guest block I/O rather than logical PostgreSQL page accesses; guest cache residency was not measured. See the [independent raw-count audit](results/20261005T072928Z-prefetch-off/independent-prefetch-read-count-audit.json) and [root recount](results/20261005T072928Z-prefetch-off/root-prefetch-read-count-recheck.json).

## Timing and measurement

| Interval | UTC start | UTC end | Duration |
|---|---|---|---:|
| VACUUM | 2026-10-05T07:29:38.716537Z | 2026-10-05T07:55:24.056614Z | 25m 45s |
| Replay tail to caught-up marker | 2026-10-05T07:55:24.056614Z | 2026-10-05T07:55:36.885110617Z | 12.8s |
| VACUUM through catch-up | 2026-10-05T07:29:38.716537Z | 2026-10-05T07:55:36.885110617Z | 25m 58s |

VACUUM exited 0; replay was caught up through three consecutive healthy samples with zero final lag. Collector recorded 314 samples; maximum gap 5.68s (12.5s maximum allowed); all three windows had valid coverage and zero rejected intervals. The standby made only about 336 DATA reads during the VACUUM window. Its 92 ms read await and 1.26 s max interval describe sparse incidental I/O, not sustained replay-read latency. DATA write await remained about 483 ms while replay caught up. Region differs from both East US baselines.

## DATA reads by window

Each device cell is **IOPS / estimated total DATA-read bytes / read await (ms/op)**. Byte totals are derived from the stored time-weighted read MB/s and covered seconds. The final column compares DATA read bytes (replica ÷ primary); the opening amplification compares estimated total read operations. Await is reported per device.

| Window | Primary DATA | Replica DATA | Replica/primary read bytes |
|---|---:|---:|---:|
| VACUUM command | 243.812 / 49.199 GB / 4.96 | 0.217 / 14.434 MB / 92.00 | 0.000293382× |
| Replay tail | 10.355 / 17.348 MB / 3.45 | 0.000 / 0 B / — (no reads) | 0× |
| VACUUM + replay | 241.890 / 49.216 GB / 4.96 | 0.216 / 14.434 MB / 92.00 | 0.000293279× |

The full report contains the four-device read/write IOPS, throughput, and await tables for each window. 
WAL marker delta: 36,932,318,738 bytes, 15,115,437 records, 6,071,356 FPIs. The unchanged repeat produced 36,932,319,438 WAL bytes, 15,115,451 records, and 6,071,356 FPIs; the tiny differences do not isolate causality. `pg_stat_recovery_prefetch` cumulative counter deltas were zero on both roles during the measured interval.

## Settings and environment

Primary remained at `recovery_prefetch=try` (default); only standby was set to `off` in its local `postgresql.auto.conf`. Live postflight verified both settings, no pending restart, and no cluster-wide override.

| GUC | Primary | Replica |
|---|---|---|
| `wal_level` | `logical` (configuration file) | `logical` (configuration file) |
| `full_page_writes` | `on` (configuration file) | `on` (configuration file) |
| `recovery_prefetch` | `try` (default) | `off` (configuration file) |
| `vacuum_cost_delay` | `0` (default) | `0` (default) |

No application client workload was created. The two PostgreSQL roles ran on separate `Standard_D2s_v4` workers with distinct 512-GiB `Standard_LRS` DATA and WAL disks, caching disabled. The same pinned PostgreSQL 16.15 image and 23-GiB fixture layout were used in both completed stacks.

## Report, graphs, and evidence

- [Full report](results/20261005T072928Z-prefetch-off/REPORT.md)
- [Linear-axis await graph](results/20261005T072928Z-prefetch-off/await.png) · [Linear-axis IOPS graph](results/20261005T072928Z-prefetch-off/iops.png) · [Linear-axis throughput graph](results/20261005T072928Z-prefetch-off/throughput.png)
- [Machine-readable summary](results/20261005T072928Z-prefetch-off/summary.json) · [timing](results/20261005T072928Z-prefetch-off/timing.json) · [preflight](results/20261005T072928Z-prefetch-off/preflight.json)
- [Raw samples](results/20261005T072928Z-prefetch-off/samples.jsonl.gz) · [VACUUM log](results/20261005T072928Z-prefetch-off/vacuum.log) · [monitor summary](results/20261005T072928Z-prefetch-off/monitoring/monitor-summary.json)
- [Independent measurement audit](results/20261005T072928Z-prefetch-off/root-audit.json) · [cleanup result](cleanup/result.json) · [retained snapshot inventory](cleanup/snapshots-after.json)
- [Independent raw DATA read-count audit](results/20261005T072928Z-prefetch-off/independent-prefetch-read-count-audit.json) · [root read-count recheck](results/20261005T072928Z-prefetch-off/root-prefetch-read-count-recheck.json)
- [Artifact recovery audit](results/20261005T072928Z-prefetch-off/artifact-recovery.json) · [preparation recovery audit](evidence/preparation-recovery-audit.json)
- [PostgreSQL recovery-prefetch context](results/20261005T072928Z-prefetch-off/recovery-prefetch-context.md) · [shared cleanup safety audit](../root-cleanup-audit.json)


## Cleanup

All five stack snapshots were preserved. AKS, worker nodes, disks, temporary Blob storage, and the node resource group were removed; shared West US 3 Network Watcher remains until the read-pressure stack finishes.

[Estimated total DATA read operations and amplification](results/20261005T072928Z-prefetch-off/estimated-data-read-operations.json).
