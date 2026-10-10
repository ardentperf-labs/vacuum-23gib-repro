# fio Standard HDD read scaling results

Analyzed **15** run(s). The analyzer uses only captured measurements; missing samples remain blank.

## Results

See [`summary.csv`](summary.csv) for fio IOPS, decimal MB/s, fio total and completion latency, guest `r_await` and queue depth, and Azure read IOPS/throughput/latency/queue depth. [`azure-minute-points.csv`](azure-minute-points.csv) preserves every returned Azure metric minute point from the VM, disk, and host metric queries.

The thread counts shown are fio `numjobs`; each worker uses a synchronous engine with one outstanding request. The displayed Azure values are averages of complete PT1M LUN 0 buckets whose returned timestamp is at least 60 seconds after `measurement_start` and whose full PT1M interval fits before `measurement_end`. The extra leading guard is a conservative response to the first run's low Azure IOPS bucket despite steady guest I/O; it mitigates that observed mismatch but is not a proven timestamp correction. The CSV also retains the original full-window averages as `azure_full_window_*`; per-metric bucket counts show how many points contributed. Guest `r_await` is weighted by completed reads over complete iostat intervals. Azure averages are not fio percentiles and may differ because of sampling and aggregation windows.

All graph axes are linear. Throughput uses decimal MB/s; the CSV also records Azure-minus-fio differences.

| IO threads | Guest IOPS | Guest MB/s | Guest await (ms) | Azure latency (ms) | Native fio IOPS | Native fio mean (ms) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 91.1 | 0.373 | 10.9 | 10.7 | 91.1 | 11.0 |
| 2 | 196.3 | 0.804 | 10.1 | 10.1 | 196.3 | 10.2 |
| 4 | 412.1 | 1.688 | 9.6 | 9.4 | 412.1 | 9.7 |
| 8 | 560.8 | 2.297 | 14.2 | 14.1 | 560.6 | 14.3 |
| 16 | 560.9 | 2.298 | 28.5 | 28.4 | 561.1 | 28.5 |
| 32 | 560.9 | 2.298 | 57.0 | 56.9 | 560.9 | 57.0 |
| 64 | 560.9 | 2.298 | 114.0 | 114.0 | 560.8 | 114.1 |
| 128 | 560.9 | 2.298 | 228.1 | 228.0 | 560.6 | 228.1 |
| 256 | 560.8 | 2.297 | 456.5 | 456.2 | 560.1 | 456.4 |
| 512 | 561.1 | 2.298 | 912.6 | 456.3 | 559.3 | 911.6 |
| 768 | 561.0 | 2.298 | 1369.0 | 456.3 | 558.5 | 1366.8 |
| 1024 | 561.0 | 2.298 | 1821.0 | 456.3 | 556.9 | 1821.0 |
| 1536 | 560.9 | 2.298 | 2250.1 | 456.3 | 574.5 | 2628.0 |
| 2048 | 561.0 | 2.298 | 2250.9 | 456.3 | 626.6 | 3176.9 |
| 3072 | 561.0 | 2.298 | 2252.2 | 456.3 | 762.1 | 3797.3 |

![Read IOPS by thread count](iops.png)

![Initial read IOPS scaling](iops-low-threads.png)

![Read throughput by thread count](throughput.png)

![Read latency by thread count](latency.png)

## Observed 1.5–5 second latency points

Mean and tail latency are listed separately. These are measured points in the inclusive 1,500–5,000 ms band; no interpolation is applied.

| Latency source | Last measured below 1.5 s | First measured at/above 1.5 s | Runs in band (threads: ms) |
|---|---:|---:|---|
| fio clat mean | 768: 1366.836 ms | 1024: 1821.015 ms | 1024: 1821.015, 1536: 2628.047, 2048: 3176.939, 3072: 3797.342 |
| fio clat p95 | 256: 492.831 ms | 512: 1602.224 ms | 512: 1602.224, 768: 2197.815, 1024: 3372.220 |
| fio clat p99 | 256: 505.414 ms | 512: 1686.110 ms | 512: 1686.110, 768: 2432.696, 1024: 4244.636 |
| Guest iostat r_await | 768: 1369.032 ms | 1024: 1821.045 ms | 1024: 1821.045, 1536: 2250.116, 2048: 2250.860, 3072: 2252.206 |
| Azure Data Disk Latency | 3072: 456.293 ms | — | No complete observed point |

Guest iostat device: `sdb`.

## Method and interpretation

Native fio group statistics use staggered per-worker clocks. At high thread counts, summed worker IO counts divided by the maximum worker runtime overstate throughput over a shared wall-clock window. Use guest/Azure counters for steady disk throughput. The fio latency distribution summarizes its own collected worker samples, not exactly the guest's UTC window. See [fio 3.28 aggregation code](https://github.com/axboe/fio/blob/fio-3.28/stat.c). Failed attempts are excluded and their raw artifacts are retained.

This report compares three layers: fio request completion, Linux block-device `r_await`, and Azure's one-minute VM metric. Azure's API timestamp anchoring is not used to shift points; the fixed leading guard is applied directly to the returned timestamp, and the unfiltered minute rows remain available for inspection. The guarded Azure-minus-fio differences are descriptive and should not be read as a calibration offset. The per-thread test is closed-loop: increasing threads increases concurrent synchronous reads, while each worker holds at most one outstanding request.
