# Azure Monitor diagnostic for the pressure run

This is an additional, read-only platform-metrics diagnostic for run `read-pressure-aux-latencycap5-full-20261006T0130Z`. It supplements the guest Linux per-device counters in the experiment report; it does not replace them or subtract the external readers' I/O. The run was user-stopped at 2026-10-06 04:30:00.114632 UTC and replay had not caught up.

## Resource identity and query scope

The saved preflight maps standby pod `hdd-pg-2` to Kubernetes node `aks-nodepool1-35454189-vmss000001`, DATA LUN 0, and WAL LUN 1. The corresponding Azure VMSS metric dimension is `aks-nodepool1-35454189-vmss_1` (instance ID 1, D2s_v4). The AKS resource in the run's node resource group has the exact experiment `reprotoken`; that owned node resource group contained one VMSS. The VMSS carries AKS-managed tags rather than a separate experiment token, so ownership was established through the tagged AKS resource's `nodeResourceGroup` relationship.

Metrics cover `[2026-10-06 01:20:00Z, 04:35:00Z)` at one-minute grain. Azure returned 195 buckets through 04:34 UTC. VM-wide metrics were queried with the `VMName` dimension; disk metrics used `VMName` and `LUN`. The queries were limited to this two-instance VMSS, then the raw results were filtered locally to the exact replica VMName and LUN 0/1. The existing `az` credential wrapper was used; there were no direct CLI calls, credentials, or infrastructure mutations.

Raw query responses, metric definitions, failed-attempt stderr, ownership/source details, phase summaries, and SHA-256 hashes are in [vmss-metrics-analysis.json](vmss-metrics-analysis.json). The raw response files are [VM metrics](vm-metrics-01-20-to-04-35.json), [per-disk metrics](data-disk-metrics-01-20-to-04-35.json), [VMSS-parent definitions](vmss-metric-definitions.json), and [instance definitions](metric-definitions.json).

## Measured findings

| Window | VM uncached bandwidth consumed | VM uncached IOPS consumed | DATA LUN 0 read rate | DATA LUN 0 bandwidth consumed |
|---|---:|---:|---:|---:|
| VACUUM, 01:30:20–02:05:00 UTC | 94.3% mean, 100% median | 3.9% mean | 96.9 MB/s mean, 100.9 MB/s median | 95.5% mean, 99% median |
| Replay tail, 02:05:00–04:29:59 UTC | 99.7% mean, 100% median | 1.3% mean | 53.6 MB/s mean, 48.8 MB/s median | 51.7% mean, 47% median |

DATA LUN 0 read throughput was near 100 MB/s at 02:15 UTC, then had settled at 48.7–48.8 MB/s by 02:20 UTC and stayed near that level for the observed replay tail. In the same tail, VM-level uncached bandwidth was saturated while VM-level uncached IOPS stayed low.

WAL LUN 1 averaged 69.8 kB/s of reads during the replay tail and its per-disk bandwidth/IOPS consumed percentages both averaged 0%. Its Azure Data Disk Latency metric nevertheless averaged about 3.09 seconds, with a 3.43-second median and a mean queue depth of 1.80. The guest PostgreSQL monitor independently showed WAL read-await near 3.4 seconds. The VM-level bandwidth metric is shared across the instance; per-LUN metrics distinguish the disks but do not identify which process issued the I/O.

The VM uncached burst-throughput credit metric was present only in 29 of 144 replay-tail minute buckets; those accumulated-use samples averaged 63.2% (range 56–70%). The VM uncached burst-IO credit metric had 27 replay-tail samples, all 0%. Per-disk burst-credit metrics returned no series. Azure documents VM burst-credit percentages as accumulated use emitted every five minutes, so these values are not a remaining-credit balance.

## Interpretation and limits

The pattern is consistent with a shared VM uncached-bandwidth cap: replay-tail DATA throughput centered at 48.8 MB/s while VM uncached bandwidth reported 100%; the DATA LUN itself was around half of its per-disk bandwidth limit, and VM uncached IOPS was far below its cap. Microsoft lists 48 MB/s uncached throughput and 200 MB/s burst throughput for Standard_D2s_v4. This supports the idea that the slow replay was constrained by the shared VM path, beyond a single replay process's ability to drive the disk, but it does not prove causation. The Dsv4 figures are specified for uncached Premium SSD, while this experiment used Standard_LRS HDD, and Azure metrics are one-minute platform aggregates without process attribution.

The high WAL LUN latency while its throughput-consumed metrics were near zero is compatible with shared-path contention or host-level effects; it is not proof of either. The five Azure buckets after the SIGINT are observational only. The run remains a partial experiment, and later local replay must not be represented as evidence that new WAL arrived or that replay completed.

Metric definitions and limits: [Azure VM monitoring reference](https://learn.microsoft.com/en-us/azure/virtual-machines/monitor-vm-reference), [Azure disk metrics](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-metrics), and [Dsv4 size series](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series).
