# Snapshot baseline repeat with a 5 TPS pgbench client

This repeat restores the 23-GiB Premium SSD v2 fixture from the retained paired DATA/WAL source and measures VACUUM with a small logged client workload. The client targeted 5 TPS across ten persistent connections, with `synchronous_commit=on`; each connection updated its own row in a ten-row table. Replica WAL flush and replay are reported separately. No latency outcome was assumed before the run.

The client ran inside the primary pod over localhost TCP as the application role. The table has ten logged rows, no index, and one `UPDATE` per transaction. Native pgbench logs provide completion TPS and latency; PostgreSQL activity sampling records in-flight query/transaction ages and waits. Four hydrated 512-GiB PremiumV2_LRS disks (separate DATA/WAL on each instance) were used at 3000 IOPS and 125 MB/s per disk, with caching disabled. Both workers were Standard_D2s_v4 instances in East US zone 1.

## Measured outcome

The primary committed 1,484 transactions during the 301.817-second healthy warm-up (4.917 TPS). Across the full pre-VACUUM interval, including the checkpoint and replica replay wait, native logs show 4.931 completed TPS. VACUUM ran for 942.797 seconds at 4.992 completed TPS. VACUUM p95 end-to-end latency was 459.227 ms and p95 execution latency was 434.225 ms, compared with 6.498 ms and 6.237 ms during warm-up. No full 10-second zero-completion bucket occurred during VACUUM or across the active-client window.

The logs contain 13,176 completed transactions with zero failures, reconciled exactly against the client-row update counters. Eight startup completions happened before the healthy warm-up boundary; the six reported phases therefore contain 13,168 transactions.

The replica first reached the VACUUM completion LSN at **2026-10-09 09:14:15.923 UTC**, with the client still active. After five healthy recovery minutes and graceful client drain, final replay of the newer current-primary LSN was verified at **09:19:32.139 UTC** with zero lag. The [complete UTC timeline](run/results/timeline.json) records both boundaries and the preparation, hydration, measurement, and cleanup receipts.

The maximum sampled primary-flush-to-replica-replay backlog was 12.907 GiB. Across the VACUUM and replay storage window, replica/primary DATA read operations were 15.809× while DATA read throughput was 0.994×. These are guest block counters; the report documents the separate Azure Monitor/SKU bandwidth discrepancy and the limits of interpreting guest operations as physical or billed transactions.

Open the [measured report](run/results/REPORT.md) for the client, replay, and four-disk results, limitations, evidence, and cleanup disposition. The [combined client and replication graph](run/results/client-replication.png) shows the full client window, including the five-minute warm-up, VACUUM, replay catch-up, and recovery verification. The report also links the separate linear-axis await, IOPS, and throughput graphs.

## Run receipts and report commands

This directory is the completed, owner-bound experiment archive. Its receipts record the following phase history, including recovery detours:

| Phase | Recorded outcome |
|---|---|
| Provision | [Completed](run/.state/provision-process.json). |
| Prepare | [Stopped](run/.state/prepare-process.json) after CNPG selected the incorrect automatic base-backup join; no client or VACUUM had started. |
| Restore | [Initial attempt failed](run/.state/restore-process.json); [attempt 2 completed](run/.state/restore-process-attempt-2.json). |
| Seed | [Attempt 1 created the paired backup/snapshot set, then exited during finalization](run/.state/seed-process.json); its evidence was retained. The [standalone finalizer's first attempt failed](run/.state/finalize-process.json), and [attempt 2 completed](run/.state/finalize-process-attempt-2.json). |
| Cache conditioning | [Attempt 1 failed](run/.state/condition-process.json); [attempt 2 completed](run/.state/condition-process-attempt-2.json). |
| Measurement | [Attempts 1](run/.state/test-process-attempt-1.json) and [2](run/.state/test-process-attempt-2.json) failed before VACUUM. Attempt 2's pgbench client was gracefully stopped and reconciled before its tiny fixture table was reset. [Attempt 3 completed](run/.state/test-process.json) and produced the measured result. |

The [complete UTC timeline](run/results/timeline.json) links these phase receipts and the source, restore, hydration, measurement, and cleanup evidence. Do not rerun phase commands against this archive. The runner and helpers bind paths and state to this directory, its owner token, retained source manifest, and reviewed measurement gate. A future repeat requires a new isolated clone with its own `run-config.json`, source identity, private state and owner token, plus a newly reviewed gate and matching helper path/stack bindings. This archive does not contain a runnable recipe for creating that separately owned stack.

To regenerate report artifacts from a completed measurement, run from this experiment directory:

```sh
.venv/bin/python client-report.py run/results
.venv/bin/python render-storage-report.py
.venv/bin/python build-report.py
```

Guarded cleanup finished at 09:42 UTC on October 9. The owned AKS stack, node/test resource groups, all four measurement disks and temporary Blob storage were removed. All 44 snapshots and 48 protection locks remain. The regional Network Watcher is preserved because other regional stacks exist and its ETag changed; two exact source Reader grants remain under snapshot locks for the removed AKS principal. The [cleanup review](verification/final-cleanup-review.md) and [root Azure checks](verification/final-root-cloud-check.json) record the final disposition.

The [prior 23-GiB baseline](../experiments-20261007-premium-v2/baseline-23gib/results/REPORT.md) and this run's [experiment plan](experiment-plan.json) provide the comparison source and configuration.

Preparation restored the paired DATA/WAL source and used a fresh paired backup to seed the replica. An initial base-backup join attempt was stopped, its replica volumes removed, and the replica restored from the verified offline snapshot pair. After all four copies reached 100%, both PostgreSQL pods restarted on the pinned measurement image. Before measurement, privileged one-shot pods ran `sync` and wrote `3` to Linux `drop_caches` on the isolated workers, clearing page cache, dentries, and inodes but not PostgreSQL `shared_buffers`. The report retains this preparation history and the failed pre-VACUUM client-launch attempts.
