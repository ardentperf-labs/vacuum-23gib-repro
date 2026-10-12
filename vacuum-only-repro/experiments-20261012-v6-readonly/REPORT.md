# D16as_v6 ReadOnly-cache VACUUM rerun

The original 23-GiB fixture produced **28.90× guest DATA read IOPS amplification** on the replica, but host caching masked backing-HDD read load. VACUUM completed in **750.31 seconds** and the replica fully caught up before teardown. This run establishes guest request-count amplification and guest-visible latency; it does not isolate NVMe versus storvsc or establish incident backend read latency.

## Measured behavior

Full VACUUM window, 04:04:20.421–04:16:50.733 UTC on October 12:

| DATA reads | Primary | Replica |
| --- | ---: | ---: |
| Average completed IOPS | 274.77 | 7,941.67 |
| Throughput, decimal MB/s | 66.17 | 65.15 |
| Mean completed request size | 235.18 KiB | 8.01 KiB |
| Operation-weighted read await | 4.496 ms | 0.363 ms |
| Operation-weighted DATA write await | 105.56 ms | 81.90 ms |

The nearly equal read bytes with very different request sizes explain the operation-count ratio. Guest counts include host-cache hits, so this ratio cannot be called backing-HDD read amplification. One-second replica read IOPS peaked around 20,000. Sampled replica DATA in-flight I/O peaked at 199; mean time-weighted outstanding I/O was 26.94 and its highest one-second average 143.19. Replica WAL in-flight I/O peaked at 67.

Replica WAL averaged **428.31 write IOPS**, 98.43 decimal MB/s guest write throughput and **55.78 ms write await**. Individual 512-GiB Standard HDDs have a nominal up-to-500-IOPS base limit, so this is consistent with pressure on WAL even though the VM uncached budget was lightly used. VM-wide metrics do not establish per-disk saturation, and guest write bytes do not necessarily equal backend bytes.

See [counter-derived summary](results/20261012T040219Z/measurement/summary.json) and [one-second replica analysis](results/20261012T040219Z/analysis.json). The first uses overlap-prorated valid counter deltas at window boundaries; the second selects one-second intervals by midpoint, so their averages differ slightly.

## Uncached host IOPS and cache interpretation

The recorded SKU capability `UncachedDiskIOPS` is **30,400**. The exact replica VM series reports native one-minute **Average** uncached IOPS consumed percentage, with a maximum observed bucket of **2%**. During VACUUM the 13 overlapping buckets have a mean of **1.4615%**.

| Recorded VM percentage | Budget-equivalent uncached IOPS |
| --- | ---: |
| 1% | 304 |
| 2% | 608 |
| Mean of 13 VACUUM-overlapping buckets, 1.4615% | 444.3 |

Conversion: `IOPS ≈ percentage / 100 × 30,400`. These are estimates from the recorded percentage values, which appear quantized to whole percentages; 0% is not proof of zero activity. They include uncached reads and writes across DATA, WAL and OS disks, not replica DATA reads alone. A one-minute Average is not an instantaneous peak. The bucket mean includes the portions of boundary minutes outside VACUUM.

Compared with nearly 8,000 guest DATA read IOPS and 0.363 ms read await, this is strong evidence that the ReadOnly host cache served most reads. An exact hit rate cannot be derived from this aggregate alone. Approximately 600 VM-wide uncached IOPS also cannot be compared directly with a single disk's 500-IOPS limit; the VM had two separate test HDDs plus its OS disk. The WAL write rate and latency are consistent with disk-level pressure while cached reads continue quickly.

[Recorded SKU](results/20261012T040219Z/metadata/sku-at-provision.json) · [Azure VM specification](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dasv6-series) · [Standard HDD limits](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-scalability-targets) · [Native metric series](results/20261012T040219Z/replica-instrumentation/azure-vm-uncached-iops.jsonl)

## Conditions and comparison limits

Two D16as_v6 nodes: 16 vCPU / 64 GiB each, NVMe, West US 3. All four DATA/WAL volumes were 512-GiB Standard_LRS HDD with ReadOnly caching, verified at PV and Azure attachment levels. PostgreSQL 16.15 / CNPG 1.30.1 defaults included `wal_level=logical`, `full_page_writes=on`, `recovery_prefetch=try`, `vacuum_cost_delay=0`; no client workload ran. The command was `VACUUM (VERBOSE, FREEZE) public.vacuum_target;`.

The primary came from the existing, previously restore-verified pre-VACUUM paired snapshot with exactly 24,696,061,952 heap bytes. Replica seeding used CNPG physical streaming/basebackup. Guest Linux page caches were cleared afterward; Azure host cache was not purged. The controller and cache behavior differ from the original storvsc test, so this is not a controlled measurement of NVMe's causal effect. No Support ticket was opened.

## Completion and teardown

- VACUUM completed successfully at 04:16:50.733 UTC; target flush LSN `56/F63A3918`.
- At 04:17:08.968 UTC replica replay and current primary flush both reached `56/F63BFA30`; zero-byte lag and the VACUUM target passed.
- Downloads and original-file checksums passed before teardown began at 04:17:19.732 UTC.
- All three owned test resource groups were deleted by 04:24:34.198 UTC. The earlier BCC preflight-only attempt was also deleted and did not run VACUUM.
- All 47 snapshots were preserved, including the two background regional copies outside test teardown scope.

[Timing and replay evidence](results/20261012T040219Z/measurement/timing.json) · [Cleanup receipt](results/20261012T040219Z/metadata/cleanup.json) · [Controller timeline](preflight/controller.log)

## Instrumentation quality

Replica PostgreSQL wait/receiver monitoring has 783 approximately one-second samples; diskstats has 780 with no intervals longer than 1.5 seconds. All four diskstats series cover the full VACUUM window. Azure has 23 distinct native one-minute buckets filtered to the exact replica VM, rather than a VMSS aggregate. Structured PostgreSQL/controller logs and raw collector output are retained.

**BCC histograms are materially incomplete and must not be used as a representative latency distribution.** During VACUUM, persisted DATA histograms contain 633,959 events versus 6,169,070 diskstats completions (~10.3%); WAL contains 29,863 versus 321,167 (~9.3%). There is a seven-minute gap in positive histograms despite sustained diskstats I/O, and approximately the final 3m47s of BCC output was not persisted. Stdout buffering explains delivery delays and lost tail, but not the missing positive histograms during sustained I/O. The exact tracing failure is unproven. Initial automated completeness checks verified files, hashes and device presence, not event-count parity; the later audit supersedes any impression of complete BCC coverage.

[Quality audit](results/20261012T040219Z/biolatency-quality-audit.json) · [Raw BCC output](results/20261012T040219Z/replica-instrumentation/biolatency.raw.log) · [Receiver samples](results/20261012T040219Z/replica-instrumentation/pgstat-receiver.jsonl)

## Graphs

All axes are linear. Diskstats await is guest-visible mean completion latency, not a BCC percentile or a direct physical-media latency measurement.

- [Primary/replica DATA/WAL await](results/20261012T040219Z/measurement/await.png)
- [Primary/replica DATA/WAL IOPS](results/20261012T040219Z/measurement/iops.png)
- [Primary/replica DATA/WAL throughput](results/20261012T040219Z/measurement/throughput.png)
- [Replica average outstanding I/O](results/20261012T040219Z/replica-average-queue-depth.png)
- [Replica sampled in-flight I/O](results/20261012T040219Z/replica-inflight-io.png)
- [Replica VM uncached IOPS consumed percentage](results/20261012T040219Z/replica-azure-uncached-iops-percent.png)
