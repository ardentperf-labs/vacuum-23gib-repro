# Five HDD replay experiments

Experiments 1, 3, 4, and 5 each completed primary `VACUUM (VERBOSE, FREEZE)` and verified that the physical standby replayed through the primary's final flush position. Experiment 2, the parallel-read-pressure run, completed VACUUM but was explicitly stopped by the user at three hours while replay remained incomplete; its progress and rate-conditioned projections are partial observations, never a full catch-up result.

For full experiments 1, 3, 4, and 5, timing separates the VACUUM duration, additional replay tail after VACUUM completion, and total VACUUM-start-to-full-catch-up time. Replay overlaps VACUUM, so the tail is only the time after VACUUM finishes. Estimated total DATA read-operation amplification is computed as primary average DATA read IOPS during VACUUM × primary VACUUM seconds, versus replica average DATA read IOPS from VACUUM start through verified full catch-up × that full interval. Experiment 2's device counters include all reads, including the five auxiliary reader processes; its observed partial ratio is not a completed replay-amplification result.

## Full-run timing and total DATA read operations (experiments 1, 3, 4, and 5)

| Experiment | Region | Primary VACUUM | Additional replay tail | Total to full catch-up | Estimated replica ÷ primary DATA reads |
|---|---|---:|---:|---:|---:|
| [1. Unchanged 23-GiB repeat](runs/baseline/REPORT.md) | eastus | 32m 22s | 2h 32m 07s | 3h 04m 29s | 15.79× (~5,950,398 ÷ ~376,881) |
| [3. 23-GiB standby recovery_prefetch=off](runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md) | westus3 | 25m 45s | 0m 13s | 25m 58s | 0.0008917× (~336 ÷ ~376,772) |
| [4. 100-GiB size variation](runs/100gib/results/100gib-20261005T130618Z/REPORT.md) | eastus | 2h 46m 54s | 10h 43m 01s | 13h 29m 55s | 15.62× (~26,150,759 ÷ ~1,674,122) |
| [5. 23-GiB primary full_page_writes=off + standby recovery_prefetch=off](runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/REPORT.md) | westus3 | 29m 55s | 10m 28s | 40m 23s | 1.003× (~374,969 ÷ ~373,886) |

**Experiment 2 (terminated early):** Primary VACUUM 34m 40s; observed post-VACUUM tail 2h 24m 59s; total observed 2h 59m 39s. Overall replica DATA read await was 2.652 seconds, including all external-reader I/O. At stop, 2.26 GiB of WAL address span had replayed (6.55%), with 32.29 GiB remaining. The observed device-read estimate is ~785,111 replica operations / ~374,600 primary VACUUM operations = 2.0959×, including auxiliary readers. A recent 5-minute rate projects about 154.2 additional hours only if that rate continues; replay behavior can change and catch-up was not observed.

Experiment 5 reused the prepared DATA/WAL snapshot pair from experiment 3 (source snapshot identities: DATA `d838e22b-a0e2-44d5-953c-819ed43dc87f`, WAL `74f4d862-be76-43f8-bf26-393d26def9ff`). The observed roles were primary `full_page_writes=off`, `recovery_prefetch=try`; standby `full_page_writes=on`, `recovery_prefetch=off`. Primary/standby `wal_log_hints` were `on` / `on`; observed `data_page_checksum_version` was `0` / `0`. Its primary `wal_fpi` counter moved from 2 before VACUUM to 2 after VACUUM (delta 0); this is an observed WAL statistic, not a claim that all generated FPIs were caused by the setting under test.

## Disk windows

Each full-run window reports the average counters over its named period: `vacuum` is primary VACUUM; `replayDrain` is from VACUUM completion through catch-up; `vacuumAndReplay` is the complete interval. DATA and WAL remain separate, as do primary and replica. Await is guest OS block-device counter latency; it includes device activity from any process on that device. The partial pressure run has no catch-up window, so its last window is explicitly observed only.

The table summarizes read IOPS / read throughput / read await for each DATA and WAL device. The machine-readable comparison also includes write IOPS, write throughput, write await, operation-weighted await, coverage and rejected-interval counts for each role in every available phase.

| Experiment / window | Primary DATA read IOPS / MB/s / await ms | Replica DATA read IOPS / MB/s / await ms | Primary WAL read IOPS / MB/s / await ms | Replica WAL read IOPS / MB/s / await ms |
|---|---:|---:|---:|---:|
| [1. Unchanged 23-GiB repeat](runs/baseline/REPORT.md) / `vacuum` | 194.07 / 25.34 / 5.69 | 537.81 / 4.41 / 403.04 | 0.01 / 0.00 / 4.20 | 30.67 / 3.89 / 7.63 |
| [1. Unchanged 23-GiB repeat](runs/baseline/REPORT.md) / `replayDrain` | 0.02 / 0.00 / 3.27 | 537.50 / 4.40 / 329.78 | 0.00 / 0.00 / — (no reads) | 24.03 / 3.08 / 4.29 |
| [1. Unchanged 23-GiB repeat](runs/baseline/REPORT.md) / `vacuumAndReplay` | 34.06 / 4.45 / 5.69 | 537.55 / 4.40 / 342.64 | 0.00 / 0.00 / 4.20 | 25.20 / 3.22 / 5.00 |
| [3. 23-GiB standby recovery_prefetch=off](runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md) / `vacuum` | 243.81 / 31.84 / 4.96 | 0.22 / 0.01 / 92.00 | 0.00 / 0.00 / 3.17 | 60.60 / 0.27 / 2.55 |
| [3. 23-GiB standby recovery_prefetch=off](runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md) / `replayDrain` | 10.35 / 1.35 / 3.45 | 0.00 / 0.00 / — (no reads) | 0.00 / 0.00 / — (no reads) | 3.25 / 0.01 / 2.45 |
| [3. 23-GiB standby recovery_prefetch=off](runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md) / `vacuumAndReplay` | 241.89 / 31.59 / 4.96 | 0.22 / 0.01 / 92.00 | 0.00 / 0.00 / 3.17 | 60.13 / 0.27 / 2.55 |
| [4. 100-GiB size variation](runs/100gib/results/100gib-20261005T130618Z/REPORT.md) / `vacuum` | 167.17 / 21.83 / 6.45 | 536.99 / 4.40 / 389.88 | 0.01 / 0.00 / 4.53 | 30.71 / 3.91 / 7.47 |
| [4. 100-GiB size variation](runs/100gib/results/100gib-20261005T130618Z/REPORT.md) / `replayDrain` | 0.01 / 0.00 / 3.77 | 538.43 / 4.41 / 333.09 | 0.00 / 0.00 / — (no reads) | 24.38 / 3.12 / 5.31 |
| [4. 100-GiB size variation](runs/100gib/results/100gib-20261005T130618Z/REPORT.md) / `vacuumAndReplay` | 34.46 / 4.50 / 6.45 | 538.14 / 4.41 / 344.77 | 0.00 / 0.00 / 4.53 | 25.68 / 3.28 / 5.84 |
| [5. 23-GiB primary full_page_writes=off + standby recovery_prefetch=off](runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/REPORT.md) / `vacuum` | 208.27 / 27.20 / 7.47 | 104.15 / 13.60 / 18.40 | 0.00 / 0.00 / 5.00 | 45.81 / 0.42 / 7.26 |
| [5. 23-GiB primary full_page_writes=off + standby recovery_prefetch=off](runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/REPORT.md) / `replayDrain` | 0.25 / 0.03 / 2.93 | 299.50 / 39.07 / 6.13 | 0.00 / 0.00 / — (no reads) | 4.81 / 0.62 / 5.60 |
| [5. 23-GiB primary full_page_writes=off + standby recovery_prefetch=off](runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/REPORT.md) / `vacuumAndReplay` | 154.38 / 20.16 / 7.47 | 154.76 / 20.20 / 12.25 | 0.00 / 0.00 / 5.00 | 35.19 / 0.47 / 7.20 |
| [Experiment 2 pressure partial](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/REPORT.md) / `vacuum` observed | 180.09 / 23.51 / 5.95 | 120.57 / 100.43 / 1687.66 | 18.62 / 2.44 / 4.39 | 2.51 / 0.24 / 5.84 |
| [Experiment 2 pressure partial](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/REPORT.md) / `replayDrain` observed | 0.02 / 0.00 / 3.86 | 61.43 / 53.19 / 3104.39 | 0.00 / 0.00 / — (no reads) | 0.55 / 0.07 / 2993.33 |
| [Experiment 2 pressure partial](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/REPORT.md) / `vacuumAndReplay` observed | 34.77 / 4.54 / 5.95 | 72.84 / 62.30 / 2651.85 | 3.59 / 0.47 / 4.39 | 0.93 / 0.10 / 1433.30 |

## Five-run outcomes

- **Experiment 1 — unchanged repeat:** completed full replay; detailed [report](runs/baseline/REPORT.md).
- **Experiment 2 — parallel DATA read pressure:** Primary VACUUM 34m 40s; observed post-VACUUM tail 2h 24m 59s; total observed 2h 59m 39s. Overall replica DATA read await was 2.652 seconds, including all external-reader I/O. At stop, 2.26 GiB of WAL address span had replayed (6.55%), with 32.29 GiB remaining. The observed device-read estimate is ~785,111 replica operations / ~374,600 primary VACUUM operations = 2.0959×, including auxiliary readers. A recent 5-minute rate projects about 154.2 additional hours only if that rate continues; replay behavior can change and catch-up was not observed. See the explicitly incomplete [partial report](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/REPORT.md) and [progress record](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/replay-progress.json). All pressure guest-device counts include external-reader I/O; the separate Azure Monitor diagnostic is supplemental and does not replace guest counters.
- **Experiment 3 — standby prefetch disabled:** completed full replay; detailed [report](runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md).
- **Experiment 4 — 100-GiB size variation:** completed full replay; fresh prepared paired DATA/WAL snapshot and standalone cold-restore validation are documented in the [report](runs/100gib/results/100gib-20261005T130618Z/REPORT.md).
- **Experiment 5 — primary full-page writes off + standby prefetch off:** completed full replay; it reused the exact prepared 23-GiB DATA/WAL source pair. The experiment 5 report and preflight record actual primary/standby GUCs and observed `wal_fpi`, `wal_log_hints`, and `data_page_checksum_version`; `full_page_writes=off` does not itself establish that zero full-page images were generated. PostgreSQL logs were fetched at completion without an independent continuous follower; the [capture audit](runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/root-completion-log-capture-audit.json) verifies the retained files and records that earlier log rotation or loss cannot be ruled out.

## Snapshot preservation and cleanup

The final shared cleanup was verified at 2026-10-06T07:19:35.342828+00:00. The subscription-wide inventory contained 32 snapshot identities before and 32 after cleanup; every prior `uniqueId` remains present. Root separately verified all 32 owned snapshots as `Succeeded` and protected with `CanNotDelete` locks, and confirmed that only the two documented stopped-AKS control planes remain. Owned shared watcher cleanup is recorded in [the shared cleanup receipt](evidence/shared-cleanup.json).

- **1. Unchanged 23-GiB repeat:** 3 stack snapshot(s) preserved. The retained-snapshot node resource group left a stopped AKS control plane; protected snapshots and their dependencies remain intact.
- **3. 23-GiB standby recovery_prefetch=off:** 5 stack snapshot(s) preserved. All non-snapshot stack resources removed.
- **4. 100-GiB size variation:** 5 stack snapshot(s) preserved. All non-snapshot stack resources removed.
- **5. 23-GiB primary full_page_writes=off + standby recovery_prefetch=off:** 2 stack snapshot(s) preserved. All non-snapshot stack resources removed.
- **Experiment 2 pressure partial:** 13 pressure-stack snapshots preserved; the pressure stack's cleanup audit records reader/queue restoration and stopped-cluster cleanup.
- **Earlier source experiment:** its cleanup receipt reports 3 snapshots preserved and a stopped AKS control plane because snapshots remain in its node resource group. Its worker and managed-disk resources were removed.

The baseline and earlier source run are the documented stopped-AKS residual exceptions caused by incremental snapshots retained in their node resource groups. Root's final inventory preserves those snapshots; shared cleanup removes only owned watchers after the last experiment. All snapshots are intentionally retained.

## Evidence and comparison

- [Machine-readable five-experiment comparison](comparison-five.json)
- [Experiment-owned retained snapshot inventory](evidence/retained-snapshot-inventory-five.json)
- [Pressure partial audit](runs/read-pressure/evidence/pressure-three-hour-partial-audit.json)
- [Pressure observed progress](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/replay-progress.json)
- [Pressure rate-conditioned projections](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/replay-extrapolation.json)
