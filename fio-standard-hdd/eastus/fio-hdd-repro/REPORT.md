# Azure Standard HDD small-read thread scaling

Measured in East US on 2026-10-09. One synchronous fio thread drove approximately **100 IOPS**: 91 IOPS initially and 110 IOPS in a repeat after the sweep. The disk reached approximately **561 IOPS with eight threads**, then sustained approximately **2.30 MB/s** as further concurrency increased latency.

**1,024 was the first tested thread count with mean guest read await between 1.5 and 5 seconds** (1.821 seconds); 768 threads measured 1.369 seconds. The fio p95 first entered that band at 512 threads (1.602 seconds). Azure's reported average disk latency never reached the band: it plateaued near **456 ms**, even at 3,072 threads.

| Threads | Guest IOPS | Throughput, MB/s | Guest read await, seconds | Azure latency, seconds |
|---:|---:|---:|---:|---:|
| 1, initial | 91 | 0.373 | 0.0109 | 0.0107 |
| 1, repeat | 110 | 0.451 | 0.0090 | 0.0089 |
| 4 | 412 | 1.688 | 0.0096 | 0.0094 |
| 8 | 561 | 2.297 | 0.0142 | 0.0141 |
| 256 | 561 | 2.297 | 0.4565 | 0.4562 |
| 512 | 561 | 2.298 | 0.9126 | 0.4563 |
| 768 | 561 | 2.298 | 1.3690 | 0.4563 |
| 1,024 | 561 | 2.298 | 1.8210 | 0.4563 |
| 1,536 | 561 | 2.298 | 2.2501 | 0.4563 |
| 3,072 | 561 | 2.298 | 2.2522 | 0.4563 |

## Setup

The disk was a 512 GiB Standard_LRS HDD, S20, configured for nominal 500 IOPS and 60 MB/s. Azure's runtime target metric reported 550 IOPS; observed sustained reads were approximately 561 IOPS. These three values are preserved rather than assuming an explanation for the difference. The VM was Standard_D4s_v4, with uncached limits of 6,400 IOPS and 96 MB/s, providing headroom above the disk. Smaller candidate SKUs encountered quota/capacity constraints. See [disk specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types) and [VM specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series).

The fixture reused a clone of the retained prepared snapshot. Reads covered sixteen physically allocated 1 GiB PostgreSQL relation segments. Host caching was None; the guest block device was read-only and ext4 mounted `ro,noload`. fio 3.28 used `randread`, 4 KiB blocks, `direct=1`, `psync`, and one outstanding request per thread. Each phase requested a 30-second ramp and 180-second measurement. The main sweep had 15 successful points from 1 through 3,072 threads. A separate final single-thread repeat checked baseline variability and is retained under `artifacts/single-validation/`. All 15 sweep points and the separate repeat had zero fio write bytes, zero fio job errors, and zero observed guest disk writes/discards.

This is a small-read IOPS experiment. The 60 MB/s throughput ceiling was not exercised: 561 × 4,096 bytes is approximately 2.30 decimal MB/s. The tested workload and snapshot-backed fixture define the scope of these results.

## Measurement and interpretation

Guest throughput and read await use complete one-second iostat intervals in a common UTC measurement window; await is weighted by completed reads. Azure values use the VM's LUN 0 Data Disk metrics, with complete one-minute buckets inside the phase and a 60-second leading guard. This leaves one interior Azure bucket per phase. The guard avoids an observed transition-bucket mismatch; it is not a proven timestamp calibration. All returned points and unguarded averages are retained. Azure averages should not be interpreted as request percentiles. See [Azure metric definitions](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-compute-virtualmachines-metrics).

Native fio group throughput diverged from common-window disk throughput at extreme thread counts. fio sums worker IO counts and uses the maximum worker runtime, while worker measurement clocks can be staggered. At 3,072 threads, its native group rate was 762 IOPS while simultaneous guest and Azure counters remained approximately 561 IOPS. The table above therefore uses guest disk throughput. fio latency distributions describe fio's collected samples, rather than exactly the common guest UTC window. See [fio 3.28 aggregation source](https://github.com/axboe/fio/blob/fio-3.28/stat.c).

At 3,072 threads, fio mean completion latency was 3.797 seconds, guest await 2.252 seconds, and Azure latency 0.456 seconds. Azure queue depth plateaued near 256; the guest queue plateaued near 1,263, consistent with the observed four guest hardware queues and 316 requests per queue. These measurements are consistent with additional queueing outside Azure's latency reporting boundary; they do not prove the precise location or establish universal queue limits.

Two 4,096-thread attempts failed during fio initialization and are excluded. fio 3.28 has a compile-time job/segment limit; its `--max-jobs` option is ignored. These failures do not measure Azure disk behavior. Both failed attempts and their logs are retained. See [job allocation source](https://github.com/axboe/fio/blob/fio-3.28/init.c#L340-L369) and [ignored option source](https://github.com/axboe/fio/blob/fio-3.28/init.c#L2644-L2646).

## Artifacts

- [Complete sweep table and analysis](artifacts/analysis/report.md), [CSV](artifacts/analysis/summary.csv), and [single-thread repeat](artifacts/single-validation/analysis/report.md).
- Separate linear-axis graphs: [IOPS](artifacts/analysis/iops.png), [initial IOPS scaling](artifacts/analysis/iops-low-threads.png), [throughput](artifacts/analysis/throughput.png), and [latency](artifacts/analysis/latency.png).
- Raw fio, iostat, metadata, VM/disk definitions, and Azure minute metrics remain under `artifacts/`.
- [Reproduction instructions](README.md). The owned test resource group, VM, disks, and network resources were deleted; Azure confirmed the group no longer exists. [Cleanup verification](artifacts/cleanup.json) confirms every pre-teardown snapshot remains, including the prepared keeper. No snapshots were created or deleted by this experiment.
