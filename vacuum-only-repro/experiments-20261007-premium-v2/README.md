# Premium SSD v2 VACUUM experiments

This archive contains three completed isolated experiments, originally run outside the GitHub checkout:

| Run | Fixture heap | Region / zone | Database disks |
|---|---:|---|---|
| [Baseline](baseline-23gib/) | 23 GiB | East US / 1 | 4 separate 512-GiB Premium SSD v2 disks |
| [Size variation](50gib/) | 50 GiB | West US 3 / 1 | same |
| [Size variation](100gib/) | 100 GiB | East US / 1 | same |

Each primary and replica has separate DATA and WAL disks. Every database disk
uses `PremiumV2_LRS`, 3,000 provisioned read/write IOPS, 125 MB/s throughput,
512-byte logical sectors, and no host caching. Disk capacity stays 512 GiB;
23/50/100 GiB describe the heap fixture, not the disk capacity. GiB follows the
historical experiment convention and was explicitly confirmed by the user.

The worker size, PostgreSQL 16.15 image, CNPG 1.30.1 version, fixture construction,
and `VACUUM (VERBOSE, FREEZE)` match the earlier baseline. There is no application
workload. Default recovery prefetch, full-page writes, WAL level and vacuum cost
delay remain unchanged. No disk scheduler treatment or global cache eviction
is introduced. Raw counters and continuous PostgreSQL logs accompany separate
linear-axis await, IOPS and throughput plots for primary/replica DATA/WAL reads
and writes. Success requires VACUUM exit 0 and three healthy samples through the
completion LSN and current primary flush LSN.

East US has DSv4 family quota 10, allowing two two-node stacks but not three.
Azure denied the requested increase to 12. The 50-GiB run uses West US 3 with
the same VM size, zone number and storage settings; its region difference is
a comparison limitation. Baseline and 100-GiB runs share East US. VM storage
limits remain part of the experiment: provisioned disk limits alone do not
establish the effective storage ceiling seen by PostgreSQL. The SKU API
advertises 3,200 uncached IOPS and 48 MB/s; the current Dsv4 table labels its
columns Premium SSD and provides no separate Premium-v2 row. We do not assume
the older Premium SSD burst figures apply to these disks. Supplementary
[full-run host telemetry and guest I/O](recovery/vm-burst-review/MEASUREMENT-SUPPLEMENT.md) is recorded at scale-set
scope, with its two-node aggregation limitation.

`framework/` preserves the adapted scripts. Each run has independent ownership
tokens, infrastructure state, kubeconfig, controllers and artifacts. Existing
prepared keeper-derived incremental snapshots are immutable input; each stack
creates its own compatible retained incremental copy and CNPG backup snapshots
in a separate owned retention resource group. Existing source locks remain in
place. All snapshots are preserved, including any incomplete experiment copies.
Shared owned Network Watchers remain until every new stack has been removed.

The live plan is [experiment-plan.json](experiment-plan.json); Azure availability,
quota, snapshot provenance and launch receipts are under `preflight/` and each
run's `.state/`. These directories are private runtime evidence and may contain
infrastructure identities; kubeconfigs and archive credentials must not be
published. No results are assumed before full measurement.

The three preparation processes and `coordinate-measurements.py` run independently
of the interactive shell. The coordinator waits for all three successful
preparations, releases one measurement barrier, starts the VACUUM collectors
together, captures continuous PostgreSQL logs, and waits for full replica
catch-up. Before releasing the barrier, a fresh ARM audit requires 100% copy
completion on every restored measured Premium SSD v2 disk. A separate guarded
retirement removes the two exact, unattached Standard staging disks after all
dependent preparations succeed, then verifies their absence and preservation
of every snapshot identity and lock. The user explicitly required that
hydration must not depress measured I/O. Known terminal failed snapshots stay
retained. The coordinator then produces [REPORT.md](REPORT.md) and
`comparison.json`. Current
phase and process identities are in [coordinator-state.json](coordinator-state.json)
and each run's `.state/prepare-process.json` and `.state/test-process.json`.

All three measured experiments are complete: VACUUM exited 0, the replicas fully
caught up, and the sample coverage checks passed. See the [comparison](REPORT.md)
and standalone [23-GiB](baseline-23gib/results/REPORT.md),
[50-GiB](50gib/results/REPORT.md), and [100-GiB](100gib/results/REPORT.md) reports.

Each final run report includes its complete end-to-end UTC timeline, from
source and backup snapshot creation through preparation, hydration, measured
VACUUM, replica catch-up, required WAL export, and cleanup. The evidence-linked
[timeline records](recovery/timelines/README.md) distinguish exact event times
from first-observed copy progress and bounded intervals. Missing preparation
transcripts are identified explicitly.

The first measurement startup on October 7 failed during its initial read-only
configuration query, before checkpointing or submitting VACUUM. PostgreSQL 16
exposes `pg_control_init().data_page_checksum_version`; the query had used
`data_checksum_version`. The one-field correction was verified on all six
instances, and the original scripts, failed receipts, and logs were archived.
The fresh hydration gate released all three corrected collectors at
08:35:43 UTC. The [source review](recovery/measurement-checksum-v2/source-review.json)
records the correction and runtime source hashes.

The first preparations exposed two restore issues. The baseline's paired backup
was complete, but its validation required Standard_LRS snapshots while CSI had
created valid Standard_ZRS snapshots. It resumes from that verified backup and
23-GiB fixture. The 100-GiB CopyStart failed with `IncrementalCopySourceInvalid`
because a newer snapshot already existed in the target family; the polling loop
then waited on that terminal failure, and the 50-GiB run timed out waiting for
its copy lock. Failed scripts, logs, and receipts remain under each run's
`.state/attempts/`.

The repaired 50/100-GiB path restores the immutable regional input to a new
owned Standard_LRS staging disk, then creates a new incremental Copy snapshot
from that disk in the protected retention group. This creates an independent
snapshot family and feeds the original VolumeSnapshot/CSI/CNPG restore path.
The helper records full source, staging-disk, snapshot, and lock metadata under
`.state/source-stage/`. All previous snapshots, including the failed copy, stay
protected. Only preparation changes; the measured fixture, four Premium v2
disks, PostgreSQL settings, VACUUM, and collection sequence stay the same.

The original final preparation wait also compared Azure's completion value to
the string `100`, so a valid `100.0` response kept it polling. The reviewed
[numeric finalizer](recovery/numeric-hydration-finalizer/README.md) resumes the
final audit of an existing prepared fixture and parses progress numerically.
It verifies source and backup lineage, seed completion, disk identity and
settings, PostgreSQL configuration, fixture statistics, and replica catch-up
before recording preparation as complete. Incomplete copy progress still
blocks preparation. The original script, stopped receipt, and finalization
evidence remain preserved; this repair does not rebuild or vacuum the fixture.

The 100-GiB fixture's original DML completion was captured before its temporary
logs were removed: `CREATE INDEX`, `DELETE 32768000`, `CHECKPOINT`, and exit
status 0. The private [capture](recovery/fixture-watch/100-dml-output/) binds
the raw output to the original SQL, script/helper, running process and pod.
The earlier INSERT output is unavailable. These are preparation records;
measured VACUUM results still require the shared hydration gate and full test
completion. The [Backup-wait continuation](recovery/backup-wait-resume/)
was used after the original 100-GiB script exceeded its one-hour Backup wait.
It preserves the existing fixture and paired Backup. A versioned guard repair
accepts CNPG-generated snapshots without the custom stack label while retaining
the exact Backup UID, owned class/namespace, source disk and snapshot identity
checks. The original timeout and blocked first continuation remain archived.

Azure's [disk restore documentation](https://learn.microsoft.com/en-us/azure/virtual-machines/scripts/create-managed-disk-from-snapshot)
states that `completionPercent` monitors hydration only for Premium SSD v2 and
Ultra Disks. Standard disks can report 100 while copying, and Standard-origin
snapshots can be [available while background transfer continues](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-instant-access-snapshots).
`Succeeded`/`Available` therefore establish usability for the temporary Standard
path, rather than verified copy completion. The measurement gate requires the
staging disks to be absent and verifies measured Premium disk hydration plus
observable backup/cross-region copy progress. It cannot establish that every
retained Standard-origin snapshot transfer or unrelated Azure backend operation
has finished; this limitation is recorded explicitly.

After all three measurements pass completion and coverage checks, the reviewed
cleanup scripts remove only these stacks' owned AKS, node and test groups.
The reviewed `post-measurement-cleanup-v2.py` checks every snapshot identity before
and after cleanup, then removes the two owned shared watchers after all three
stacks are gone. It preserves the original cleanup guards and records local
phase events and per-stack teardown observations for the end-to-end timelines;
the [source review](recovery/cleanup-timeline/root-review.json) records its exact
hash and execution conditions. Snapshot resource groups and locks remain.
Online paired snapshots also need
WAL archived after their snapshot cut to finish recovery. Before deleting the
temporary Blob accounts, preserve the exact required WAL range, history, and
snapshot metadata in the private [backup-retention exports](recovery/backup-retention-review/).
The exports contain database data and must stay private. Future reuse requires
republishing that archive to a configured recovery location. A blocked scoped
role-assignment deletion is recorded in the retained group's receipt. Any unresolved failure
stops automatic cleanup and leaves its evidence in the phase logs and receipts.

The first cleanup attempt stopped before deletion because the installed Azure
CLI rejected combining `role assignment list --scope` with `--all`. The
[reviewed correction and archived attempt evidence](recovery/cleanup-cli-correction/README.md)
preserve the original source and failure history. The retry keeps every snapshot
and all original ownership/protection guards. Final cleanup status is recorded
in [cleanup-state.json](cleanup-state.json) and the per-run timelines.

Moved into `vacuum-only-repro/experiments-20261007-premium-v2` on 2026-10-07.
The original `/workspace/vacuum-only-repro/experiments-20261007-premium-v2` path is a compatibility
symlink to this directory. As-run scripts, raw data, three standalone reports,
nine graphs, timelines, and snapshot/WAL retention evidence remain together.
Historical absolute paths in receipts are preserved. The
[relocation verification](relocation/verification.json) records file preservation
and the navigation-only edits; their originals are archived locally.
