# VACUUM and prefetch investigation results

All experiment archives are physically inside the GitHub checkout for
`ardentperf-labs/vacuum-23gib-repro`, under this `vacuum-only-repro/` directory.
They were placed here on October 10, 2026. No files were staged, committed or
pushed as part of this relocation.

The [investigation write-up](../PREFETCH-READS-INVESTIGATION.md) explains the
read-operation ratio, prefetch behavior, environment-specific coalescing and
the distinction between replica WAL flush and replay delays.

| Experiments | Report | Contents |
|---|---|---|
| Original five experiments | [Comparison](experiments-20261005-five/REPORT.md) | Per-run reports, raw measurements and graphs |
| Premium SSD v2: 23, 50 and 100 GiB | [Comparison](experiments-20261007-premium-v2/REPORT.md) | Three completed runs, graphs, raw measurements and complete UTC timelines |
| Snapshot-based baseline with tiny pgbench workload | [Report](experiments-20261009-pgbench-baseline/run/results/REPORT.md) | Client and four-disk results, raw logs, graphs, timeline and cleanup verification |
| Earlier VACUUM-only runs | [Report](REPORT.md), [23-GiB report](REPORT-23GIB.md) | Archived scripts, reports and `results/` directories |

The latest [client and replication graph](experiments-20261009-pgbench-baseline/run/results/client-replication.png)
shows client TPS and latency alongside standby flush and replay backlogs.

The historical standalone repository's `.git` directory was excluded. Its
original experiment-directory paths remain compatibility symlinks so saved
absolute paths and local references still resolve. The copied top-level files
and the moved experiment archives were verified against a complete inventory
of file hashes, symlink targets and permissions before navigation/ignore rules
were added. The private relocation receipt is `github-relocation/verification.json`.

Existing ownership state, kubeconfigs, credentials, virtual environments and
private snapshot/WAL exports remain local and ignored by Git. Historical
scripts and receipts are archived evidence; do not rerun them against their
old owner-bound state. This relocation did not run any Azure commands or
change retained snapshots, protection locks or experiment results.

Links into ignored `.state/` directories and private retention exports refer
to local audit evidence. They are available in the original workspace but are
excluded from the published repository. Reports and public measurement
artifacts retain the recorded outcomes and timelines.

Published copies use an example Azure subscription UUID and `/workspace`
prefixes. [Publication redactions](../PUBLICATION-REDACTIONS.json) record
original and published hashes; historical as-run hashes refer to the private
originals when metadata or presentation was adapted. Numeric measurements are unchanged.
