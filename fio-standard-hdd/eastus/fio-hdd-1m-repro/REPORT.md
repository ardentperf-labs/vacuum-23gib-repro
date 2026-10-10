# Standard HDD 1 MiB random-read throughput scaling

Measured in East US on 2026-10-09, using synchronous, direct, read-only fio requests with one outstanding 1 MiB read per worker thread. All throughput below uses decimal MB/s.

One thread initially sustained **60.04 MB/s** and sustained **75.49 MB/s** in a repeat after the sweep. Two threads sustained **99.53 MB/s**, and four or more sustained approximately **102 MB/s**. Increasing concurrency thereafter increased latency rather than throughput. A separate ten-minute four-thread check on a VM with more uncached storage bandwidth also sustained **102 MB/s**. The two single-thread results document variability; they do not establish its cause.

**160 was the first measured thread count with mean read await in the 1.5–5-second analysis band**: 1.645 seconds, with Azure also reporting 1.645 seconds. The preceding measured point, 144 threads, was below the threshold at 1.480 seconds. These two refinement points used the separate validation VM. In the original doubling sweep, 128 threads measured 1.316 seconds and 256 threads measured 2.632 seconds. No exact threshold between measured points is inferred.

| Threads | VM | Guest MB/s | Guest await, seconds | Azure latency, seconds |
|---:|---|---:|---:|---:|
| 1, initial | D4s_v4 | 60.04 | 0.0174 | 0.0172 |
| 1, repeat | D4s_v4 | 75.49 | 0.0138 | 0.0133 |
| 2 | D4s_v4 | 99.53 | 0.0210 | 0.0209 |
| 4 | D4s_v4 | 102.00 | 0.0410 | 0.0408 |
| 8 | D4s_v4 | 102.00 | 0.0822 | 0.0819 |
| 16 | D4s_v4 | 102.00 | 0.1644 | 0.1642 |
| 32 | D4s_v4 | 102.00 | 0.3289 | 0.3286 |
| 64 | D4s_v4 | 102.02 | 0.6579 | 0.6576 |
| 128 | D4s_v4 | 102.02 | 1.3158 | 1.3155 |
| 144 | D8ds_v4 validation | 101.99 | 1.4803 | 1.4801 |
| 160 | D8ds_v4 validation | 101.99 | 1.6447 | 1.6445 |
| 256 | D4s_v4 | 101.99 | 2.6317 | 2.6314 |
| 512 | D4s_v4 | 101.99 | 5.2256 | 2.6316 |

![Throughput scaling at low thread counts](artifacts/analysis/throughput-low-threads.png)

![Throughput across the full sweep](artifacts/analysis/throughput.png)

![Latency across the full sweep](artifacts/analysis/latency.png)

The first measured fio p95 in the 1.5–5 second band was **144 threads, 1.502 seconds**, just above the lower bound. This is a percentile of the recorded fio samples, distinct from mean await. At 512 threads, fio mean completion latency was 5.251 seconds and p95 was 12.415 seconds. The sweep stopped after this point exceeded five seconds mean fio latency.

## Setup and bandwidth validation

The disk was an isolated 512 GiB Standard_LRS HDD clone of the same retained prepared snapshot used for the neighboring 4 KiB experiment. ARM reported `diskIOPSReadWrite=500` and `diskMBpsReadWrite=60`; the published S20 specifications are also 500 IOPS / 60 MB/s. Azure's runtime LUN target metrics instead reported **550 IOPS / 100,000,000 bytes/s**. The observed 102 MB/s plateau exceeds the nominal specification. The artifacts retain all three values; this experiment does not establish a reason for the difference or a guaranteed limit for other Standard HDD disks. [Standard HDD specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types), [Azure target metric definitions](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-compute-virtualmachines-metrics).

The main VM was Standard_D4s_v4, matching the earlier experiment, with host caching None. Its documented uncached baseline is 96 MB/s and its burst limit is 200 MB/s. Some bandwidth-consumption samples reached 100%, so the nominal baseline alone was insufficient to attribute the plateau to the disk. [Dsv4 specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series).

We therefore tested another owned snapshot clone on Standard_D8ds_v4, whose documented uncached baseline is **192 MB/s**. Four threads sustained **101.999 MB/s guest / 101.997 MB/s Azure** for a ten-minute measurement. Interior VM bandwidth-consumption samples were **52%**, and its returned used-bandwidth-burst-credit sample was **0% used**. This supports the conclusion that the approximately 102 MB/s result was not merely the main VM's 96 MB/s baseline. It also provided the 144- and 160-thread latency refinements. A D8s_v3 provisioning attempt encountered regional capacity restrictions; its failure log is retained, and D8ds_v4 was used instead. [Ddsv4 specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/ddsv4-series).

Both stacks read the same sixteen physically allocated 1 GiB PostgreSQL relation segments, for a 16 GiB working set. No PostgreSQL server or SQL client workload ran. Devices were marked read-only and ext4 mounted `ro,noload`. fio 3.28 used `randread`, `bs=1m` (1,048,576 bytes), `psync`, `direct=1`, `thread=1`, and `iodepth=1`. Each ordinary point requested 30 seconds ramp and 180 seconds measurement. The long validation used 600 seconds measurement. All fourteen successful phases had zero fio job errors, zero fio write/trim bytes, and zero observed test-disk write/discard counters.

The guest data devices were `sdb` on the main VM and `sdc` on the validation VM. Captured iostat read request sizes were 1,024 KiB, and `max_sectors_kb=1280` allowed a full 1 MiB request. Thus the captured guest request counts matched whole 1 MiB reads rather than an assumed split into smaller reads. At 102 MB/s, that corresponds to approximately **97.3 reads/second**, below the disk's nominal IOPS limit.

## Measurement boundaries and artifacts

Guest throughput uses complete one-second iostat intervals inside each recorded common UTC window; await is weighted by completed read requests. Azure measurements use complete LUN 0 PT1M buckets with the same conservative 60-second leading guard used in the earlier experiment. The ordinary points each retain one interior Azure minute, while the long validation retains several. This guard mitigates an observed transition mismatch; it is not a proven timestamp calibration. Raw returned points and unguarded averages remain available. Azure averages are distinct from fio percentiles.

Native fio grouping uses individual worker clocks, which can stagger. The charts include its native rate, but the headline sustained throughput uses common-window guest counters and the matching Azure counters. fio latency describes its collected worker samples, rather than precisely the common guest window. The Azure latency plateau near 2.632 seconds at 512 threads differs from the guest's 5.226 seconds; reported Azure queue depth also plateaus near 256. The measurements are consistent with queueing outside Azure's reporting boundary, without proving its precise location.

- [Main full table and generated analysis](artifacts/analysis/report.md), [main CSV](artifacts/analysis/summary.csv).
- [Validation table and analysis](../fio-hdd-1m-headroom/artifacts/analysis/report.md), [validation CSV](../fio-hdd-1m-headroom/artifacts/analysis/summary.csv). Its points are shown as a separate grey curve; they are not merged into the main CSV.
- [Single-thread repeat](artifacts/single-validation/analysis/report.md), [repeat CSV](artifacts/single-validation/analysis/summary.csv). The main chart's single-thread point is the initial measurement.
- [Reproduction instructions](README.md), raw fio/iostat/VM/disk/metric artifacts under each stack's `artifacts/` directory.

All chart axes are linear. Both owned test resource groups, their VMs, disks, and network resources were deleted. [Main cleanup verification](artifacts/cleanup.json) and [validation cleanup verification](../fio-hdd-1m-headroom/artifacts/cleanup.json) confirm all 44 pre-teardown snapshots remain, including the prepared keeper. Every snapshot present at the start of either stack also remains. This experiment created or deleted no snapshots. The previous 4 KiB experiment's local artifacts remain separate.
