# PostgreSQL VACUUM and Azure disk experiments

This repository contains PostgreSQL VACUUM and replica-replay experiments on
Azure Standard HDD and Premium SSD v2, a small pgbench client experiment,
regional fio read-scaling tests, and local PostgreSQL replay checks.

The original 23-GiB test's dedicated README is preserved unchanged in
[README-23GIB.md](README-23GIB.md), including its reproduction instructions,
prerequisites, results, and validation caveats.

Every experiment below links directly to its report or recorded validation.
The [complete graph catalog](GRAPHS.md) links all 92 graph files,
including repeats, archived copies, and superseded analyses: every graph is
at most two clicks from this README. The graph links in the tables open the
relevant catalog section. All experiment graphs use linear y-axes.

## Original and earlier VACUUM-only tests

These runs used separate primary/replica DATA and WAL Standard HDD disks,
with no application workload. SCSI and NVMe describe the VM disk interface.

| Test | Report and evidence | Graphs |
| --- | --- | --- |
| Original 23-GiB HDD test, October 4–5; VACUUM and full replica catch-up | [Report](REPORT.md) · [Dedicated README](README-23GIB.md) · [Raw results](results/20261004T205730Z/) | [Graphs](GRAPHS.md#original-23-gib-hdd) |
| Earlier 110-GiB HDD on D2s_v4 / SCSI; VACUUM completed, replay catch-up not observed | [Report](vacuum-only-repro/REPORT.md) · [Raw results](vacuum-only-repro/results/20261004T140103Z/) | [Graphs](GRAPHS.md#earlier-110-gib-scsi) |
| Earlier 110-GiB HDD on D2as_v6 / NVMe; VACUUM completed, replay catch-up not observed | [Comparison discussion](vacuum-only-repro/REPORT.md#same-phase-interval-comparison) · [Run summary](vacuum-only-repro/results/20261004T065423Z/summary.json) · [Raw results](vacuum-only-repro/results/20261004T065423Z/) | [Graphs](GRAPHS.md#earlier-110-gib-nvme) |

The NVMe run has a recorded summary and comparison discussion rather than a
separate standalone report. The [historical reproduction README](vacuum-only-repro/README.md)
documents the archived scripts; the [archived 23-GiB report](vacuum-only-repro/REPORT-23GIB.md)
and [graph copies](GRAPHS.md#archived-original-23-gib-hdd) preserve the original follow-up.

## 16-vCPU Standard HDD rerun with ReadOnly host caching

| Test | Report and evidence | Graphs |
| --- | --- | --- |
| October 12: original 23-GiB fixture on D16as_v6 / NVMe, separate DATA/WAL HDDs with ReadOnly host caching; VACUUM and replica catch-up completed | [Report](vacuum-only-repro/experiments-20261012-v6-readonly/REPORT.md) · [Offline analysis and execution archive](vacuum-only-repro/experiments-20261012-v6-readonly/README.md) · [Raw results](vacuum-only-repro/experiments-20261012-v6-readonly/results/20261012T040219Z/) | [Six graphs](GRAPHS.md#hdd-v6-readonly) |

Replica DATA guest reads averaged **7,942 IOPS versus 275 on the primary (28.9×)**,
with nearly equal bytes read. The low guest read latency and **0–2% VM uncached
IOPS consumption** strongly suggest host-cache service; this is not evidence
of equivalent physical HDD read amplification. A 2% bucket converts to approximately
**608 VM-wide uncached IOPS** using the recorded 30,400-IOPS VM limit.
[The BCC latency histograms are incomplete](vacuum-only-repro/experiments-20261012-v6-readonly/results/20261012T040219Z/biolatency-quality-audit.json)
and diagnostic only; diskstats, PostgreSQL and Azure metric coverage are retained.
All test infrastructure was deleted after data download, with all snapshots preserved.

## Five Standard HDD follow-up experiments

[Five-run comparison](vacuum-only-repro/experiments-20261005-five/REPORT.md) ·
[Archive README and offline tools](vacuum-only-repro/experiments-20261005-five/README.md).
Four tests completed full replica replay. The pressure test is explicitly partial.

| Test | Direct report | Graphs |
| --- | --- | --- |
| 1. Unchanged 23-GiB repeat | [Report](vacuum-only-repro/experiments-20261005-five/runs/baseline/REPORT.md) | [Graphs](GRAPHS.md#hdd-23-gib-repeat) |
| 2. 23-GiB test with five external DATA readers; stopped at three hours before replica catch-up | [Partial report](vacuum-only-repro/experiments-20261005-five/runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/REPORT.md) · [Azure Monitor diagnostic](vacuum-only-repro/experiments-20261005-five/runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/azure-monitor-diagnostic/README.md) | [Graphs](GRAPHS.md#hdd-read-pressure-partial) |
| 3. 23-GiB test with standby `recovery_prefetch=off` | [Report](vacuum-only-repro/experiments-20261005-five/runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md) | [Graphs](GRAPHS.md#hdd-prefetch-off) |
| 4. 100-GiB size variation | [Report](vacuum-only-repro/experiments-20261005-five/runs/100gib/results/100gib-20261005T130618Z/REPORT.md) | [Corrected graphs](GRAPHS.md#hdd-100-gib) · [Superseded analysis](GRAPHS.md#hdd-100-gib-superseded) |
| 5. 23-GiB test with primary `full_page_writes=off` and standby `recovery_prefetch=off` | [Report](vacuum-only-repro/experiments-20261005-five/runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/REPORT.md) | [Graphs](GRAPHS.md#hdd-full-page-writes-and-prefetch-off) |

## Premium SSD v2 VACUUM-only experiments

[Three-run comparison](vacuum-only-repro/experiments-20261007-premium-v2/REPORT.md) ·
[Experiment README](vacuum-only-repro/experiments-20261007-premium-v2/README.md).
Each run used four hydrated 512-GiB Premium SSD v2 DATA/WAL disks configured
at 3,000 IOPS and 125 MB/s per disk. All three completed replica catch-up.
The 50-GiB run used a different region, which limits comparisons.

| Test | Direct report with complete UTC timeline | Graphs |
| --- | --- | --- |
| 23-GiB baseline, East US | [Report](vacuum-only-repro/experiments-20261007-premium-v2/baseline-23gib/results/REPORT.md) | [Graphs](GRAPHS.md#premium-ssd-v2-23-gib) |
| 50-GiB variation, West US 3 | [Report](vacuum-only-repro/experiments-20261007-premium-v2/50gib/results/REPORT.md) | [Graphs](GRAPHS.md#premium-ssd-v2-50-gib) |
| 100-GiB variation, East US | [Report](vacuum-only-repro/experiments-20261007-premium-v2/100gib/results/REPORT.md) | [Graphs](GRAPHS.md#premium-ssd-v2-100-gib) |

## Premium SSD v2 with a small pgbench client

| Test | Direct report | Graphs |
| --- | --- | --- |
| Snapshot-based 23-GiB baseline, East US; VACUUM with a client targeting 5 TPS across ten persistent connections | [Client, replication, storage, and timeline report](vacuum-only-repro/experiments-20261009-pgbench-baseline/run/results/REPORT.md) · [Experiment README](vacuum-only-repro/experiments-20261009-pgbench-baseline/README.md) | [Client and storage graphs](GRAPHS.md#pgbench-23-gib-baseline) · [Raw collector plots](GRAPHS.md#pgbench-raw-collector-plots) |

## Standard HDD fio tests

[Regional comparison report](fio-standard-hdd/REPORT.md) ·
[Method and run matrix](fio-standard-hdd/README.md) ·
[Regional comparison graphs](GRAPHS.md#fio-regional-comparison).
The four main sweeps use random direct reads on clones of the same prepared
512-GiB snapshot. Single-thread repeats and the larger-VM headroom validation
are separate measurements.

| Main sweep | Direct report | Graphs |
| --- | --- | --- |
| East US, 4-KiB random reads | [Report](fio-standard-hdd/eastus/fio-hdd-repro/REPORT.md) · [Complete point table](fio-standard-hdd/eastus/fio-hdd-repro/artifacts/analysis/report.md) | [Graphs](GRAPHS.md#fio-east-us-4-kib) |
| East US, 1-MiB random reads | [Report](fio-standard-hdd/eastus/fio-hdd-1m-repro/REPORT.md) · [Complete point table](fio-standard-hdd/eastus/fio-hdd-1m-repro/artifacts/analysis/report.md) | [Graphs](GRAPHS.md#fio-east-us-1-mib) |
| West US 2, 4-KiB random reads | [Report](fio-standard-hdd/westus2/fio-hdd-repro/REPORT.md) · [Complete point table](fio-standard-hdd/westus2/fio-hdd-repro/artifacts/analysis/report.md) | [Graphs](GRAPHS.md#fio-west-us-2-4-kib) |
| West US 2, 1-MiB random reads | [Report](fio-standard-hdd/westus2/fio-hdd-1m-repro/REPORT.md) · [Complete point table](fio-standard-hdd/westus2/fio-hdd-1m-repro/artifacts/analysis/report.md) | [Graphs](GRAPHS.md#fio-west-us-2-1-mib) |

| Supporting test | Direct report | Graphs |
| --- | --- | --- |
| East US, 4-KiB single-thread repeat | [Repeat report](fio-standard-hdd/eastus/fio-hdd-repro/artifacts/single-validation/analysis/report.md) | [Graphs](GRAPHS.md#fio-east-us-4-kib-repeat) |
| East US, 1-MiB single-thread repeat | [Repeat report](fio-standard-hdd/eastus/fio-hdd-1m-repro/artifacts/single-validation/analysis/report.md) | [Graphs](GRAPHS.md#fio-east-us-1-mib-repeat) |
| West US 2, 4-KiB single-thread repeat | [Repeat report](fio-standard-hdd/westus2/fio-hdd-repro/artifacts/single-validation/analysis/report.md) | [Graphs](GRAPHS.md#fio-west-us-2-4-kib-repeat) |
| West US 2, 1-MiB single-thread repeat | [Repeat report](fio-standard-hdd/westus2/fio-hdd-1m-repro/artifacts/single-validation/analysis/report.md) | [Graphs](GRAPHS.md#fio-west-us-2-1-mib-repeat) |
| East US, 1-MiB larger-VM headroom validation on D8ds_v4; 4/144/160 workers | [Validation report](fio-standard-hdd/eastus/fio-hdd-1m-headroom/artifacts/analysis/report.md) · [README](fio-standard-hdd/eastus/fio-hdd-1m-headroom/README.md) | [Graphs](GRAPHS.md#fio-east-us-1-mib-headroom) |

The West US 2 1-MiB sweep includes the 144/160-thread refinements and
768/1,024-thread points. The East US 4-KiB report also records two failed
4,096-thread initialization attempts, excluded from successful measurements.

## Local replay and snapshot validation

These checks have recorded results but no graphs.

| Test or validation | Direct report or recorded result |
| --- | --- |
| Local PostgreSQL 16 replay of identical VACUUM WAL with prefetch `off` versus `try` | [Reproduction and findings](investigations/20261006-prefetch-reads/LOCAL-REPRO.md) · [Replay counters](investigations/20261006-prefetch-reads/local-replay-results.json) |
| Independent fresh local replay verification | [Verification result](investigations/20261006-prefetch-reads/root-validation/verification.json) · [Method](investigations/20261006-prefetch-reads/LOCAL-REPRO.md) |
| Prepared keeper restore, native CNPG paired DATA/WAL snapshot backup, and replica seeding | [Validation notes](vacuum-only-repro/results/keeper-validation/validation-notes.md) |
| 100-GiB paired snapshot cold-restore validation | [Run report and validation evidence](vacuum-only-repro/experiments-20261005-five/runs/100gib/results/100gib-20261005T130618Z/REPORT.md) |
| West US 2 regional fixture copy and snapshot preservation | [Preparation record](fio-standard-hdd/westus2/fixture-preparation/README.md) · [Completion receipt](fio-standard-hdd/westus2/fixture-preparation/artifacts/complete.json) |

## Investigation, scripts, and supporting contents

| Contents | Where to look |
| --- | --- |
| Cross-experiment analysis of read-operation amplification, prefetch, WAL flush/replay, and measured client impact | [Prefetch investigation](PREFETCH-READS-INVESTIGATION.md) |
| Archived experiment overview and publication history | [Archive index](vacuum-only-repro/INDEX.md) · [Historical experiment goal](vacuum-only-repro/GOAL.md) |
| Original test's provisioning, preparation, measurement, and teardown scripts | [Dedicated README](README-23GIB.md) · [Provision](01-provision.sh) · [Prepare](02-prepare.sh) · [Test](03-test.py) · [Teardown](04-teardown.sh) |
| Original test's graph generator and dependencies | [plot.py](plot.py) · [requirements.txt](requirements.txt) |
| Regional fio setup, fixture evidence, and comparison generator | [East US README](fio-standard-hdd/eastus/README.md) · [West US 2 README](fio-standard-hdd/westus2/README.md) · [compare.py](fio-standard-hdd/compare.py) |
| Local replay scripts, WAL decoding, and artifact audit | [Investigation bundle](investigations/20261006-prefetch-reads/) · [Source snapshot control metadata](investigations/20261006-prefetch-reads/source-snapshot-control-metadata.md) |
| Premium SSD v2 framework, preparation repairs, hydration, retention, and timelines | [Framework](vacuum-only-repro/experiments-20261007-premium-v2/framework/README.md) · [Recovery and audit records](vacuum-only-repro/experiments-20261007-premium-v2/recovery/) · [Timelines](vacuum-only-repro/experiments-20261007-premium-v2/recovery/timelines/README.md) |
| Pgbench instrumentation, preparation repairs, client reporting, and cleanup reviews | [Client code](vacuum-only-repro/experiments-20261009-pgbench-baseline/client/) · [Recovery records](vacuum-only-repro/experiments-20261009-pgbench-baseline/recovery/) · [Verification records](vacuum-only-repro/experiments-20261009-pgbench-baseline/verification/) |
| Publication redactions and file hashes | [Original-run provenance](RESULTS-PROVENANCE.json) · [Repository redaction manifest](PUBLICATION-REDACTIONS.json) · [Archived publication provenance](vacuum-only-repro/PUBLICATION-PROVENANCE.json) |

## Offline software tests

The root unit tests cover [plot statistics and coverage](test_plot.py),
[detached VACUUM execution](test_detached_vacuum.py),
[replica catch-up criteria](test_replay_completion.py),
[replication log parsing](test_replication_logs.py), and
[disk LUN metadata](test_lun_metadata.py). Installation and test commands are
in the [dedicated original-test README](README-23GIB.md#results-and-graphs).

Additional offline tests and documented checks are retained with each archive:

- [Five-run verification, replotting, counter rollover, and cleanup ownership tools](vacuum-only-repro/experiments-20261005-five/tools/), documented in the [archive README](vacuum-only-repro/experiments-20261005-five/README.md).
- [Historical VACUUM collector and plot tests](vacuum-only-repro/).
- [Premium SSD v2 verification tests](vacuum-only-repro/experiments-20261007-premium-v2/verification/) and [recovery-helper tests and reviews](vacuum-only-repro/experiments-20261007-premium-v2/recovery/).
- [Pgbench instrumentation tests](vacuum-only-repro/experiments-20261009-pgbench-baseline/client/test_client_instrumentation.py), [report and cleanup tests](vacuum-only-repro/experiments-20261009-pgbench-baseline/verification/), and [recovery-helper tests](vacuum-only-repro/experiments-20261009-pgbench-baseline/recovery/).

Published artifacts anonymize Azure subscription IDs and source-host workspace
paths; measurements are unchanged. Private state, credentials, snapshot
configuration, and WAL exports remain local and excluded from Git. Historical
archives contain owner-bound execution evidence; their READMEs distinguish
offline analysis from provisioning a new experiment. All snapshots must be
preserved until the user explicitly authorizes deletion.
