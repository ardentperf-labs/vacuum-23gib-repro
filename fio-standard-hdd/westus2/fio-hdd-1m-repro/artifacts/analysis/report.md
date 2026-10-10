# fio Standard HDD 1 MiB random read scaling results

Analyzed **14** run(s). The analyzer uses only captured measurements; missing samples remain blank.

## Results

The workload issues read-only 1 MiB random reads (`fio --bs=1m`, where fio's default `kb_base=1024`). See [`summary.csv`](summary.csv) for fio logical IOPS, decimal MB/s, fio total and completion latency, guest block-device `r/s`, throughput, `r_await` and queue depth, and Azure read operations/throughput/latency/queue depth. [`azure-minute-points.csv`](azure-minute-points.csv) preserves every returned Azure metric minute point from the VM, disk, and host metric queries.

The thread counts shown are fio `numjobs`; each worker uses a synchronous engine with one outstanding 1 MiB request. The displayed Azure values are averages of complete PT1M LUN 0 buckets whose returned timestamp is at least 60 seconds after `measurement_start` and whose full PT1M interval fits before `measurement_end`. The extra leading guard is a conservative response to the first run's low Azure operation rate despite steady guest I/O; it mitigates that observed mismatch but is not a proven timestamp correction. The CSV also retains the original full-window averages as `azure_full_window_*`; per-metric bucket counts show how many points contributed. Guest `r_await` is weighted by completed block-device reads over complete iostat intervals. Azure averages are not fio percentiles and may differ because of sampling and aggregation windows.

All graph axes are linear. Throughput uses decimal MB/s; the CSV also records Azure-minus-fio differences. The two throughput graphs show the full run and the initial concurrency range through 32 workers.

| IO threads | fio MB/s | Guest MB/s | Azure MB/s | Guest block requests/s | fio logical IOPS | Guest await (ms) | Azure latency (ms) |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 16.985 | 16.964 | 15.969 | 16.2 | 16.2 | 61.7 | 65.7 |
| 2 | 35.264 | 35.280 | 34.095 | 33.6 | 33.6 | 59.4 | 61.4 |
| 4 | 79.147 | 79.253 | 73.605 | 75.6 | 75.5 | 52.9 | 57.0 |
| 8 | 101.981 | 102.005 | 102.028 | 97.3 | 97.3 | 82.2 | 82.0 |
| 16 | 101.965 | 101.993 | 101.951 | 97.3 | 97.2 | 164.4 | 164.2 |
| 32 | 101.879 | 101.993 | 101.958 | 97.3 | 97.2 | 328.9 | 328.8 |
| 64 | 101.834 | 101.999 | 101.987 | 97.3 | 97.1 | 657.9 | 657.8 |
| 128 | 101.597 | 102.005 | 101.951 | 97.3 | 96.9 | 1315.8 | 1315.7 |
| 144 | 101.117 | 101.531 | 101.976 | 96.8 | 96.4 | 1487.5 | 1480.1 |
| 160 | 101.668 | 101.999 | 101.986 | 97.3 | 97.0 | 1644.8 | 1644.8 |
| 256 | 101.630 | 101.999 | 101.976 | 97.3 | 96.9 | 2631.7 | 2631.4 |
| 512 | 99.935 | 102.016 | 101.979 | 97.3 | 95.3 | 5254.1 | 2631.7 |
| 768 | 131.450 | 101.993 | 101.950 | 97.3 | 125.4 | 6496.3 | 2631.6 |
| 1024 | 180.125 | 100.212 | 101.977 | 95.6 | 171.8 | 6606.6 | 2631.5 |

![1 MiB read throughput by thread count](throughput.png)

![Initial 1 MiB read throughput scaling](throughput-low-threads.png)

![1 MiB read latency by thread count](latency.png)

## Observed 1.5–5 second latency points

Mean and tail latency are listed separately. These are measured points in the inclusive 1,500–5,000 ms band; no interpolation is applied.

| Latency source | Last measured below 1.5 s | First measured at/above 1.5 s | Runs in band (threads: ms) |
|---|---:|---:|---|
| fio clat mean | 144: 1486.854 ms | 160: 1644.530 ms | 160: 1644.530, 256: 2631.519 |
| fio clat p95 | 128: 1384.120 ms | 144: 1535.115 ms | 144: 1535.115, 160: 1686.110, 256: 2667.577 |
| fio clat p99 | 128: 1434.452 ms | 144: 1820.328 ms | 144: 1820.328, 160: 1753.219, 256: 2734.686 |
| Guest iostat r_await | 144: 1487.455 ms | 160: 1644.789 ms | 160: 1644.789, 256: 2631.693 |
| Azure Data Disk Latency | 144: 1480.132 ms | 160: 1644.761 ms | 160: 1644.761, 256: 2631.386, 512: 2631.722, 768: 2631.628, 1024: 2631.529 |

Guest iostat device: `sdb`.

## Method and interpretation

fio IOPS counts completed 1 MiB workload requests. Guest iostat `r/s` counts block-device read requests completed after merges; the Linux block layer may split a large request to respect the device queue's maximum transfer size. Thus fio IOPS and guest `r/s` count work at different layers and can differ substantially for 1 MiB reads. Guest throughput is converted from sysstat's `rkB/s` (1024-byte units) to decimal MB/s; fio and Azure byte rates are also shown in decimal MB/s. Azure Data Disk Read Operations/Sec is documented as per-disk read IOPS, while Data Disk Read Bytes/sec is a bytes-per-second metric averaged over PT1M. Azure's Standard HDD billing rule counts each I/O larger than 16 KiB as multiple 16 KiB billable transactions; that billing rule should not be confused with the definition or measured value of the Azure Monitor Read Operations/Sec metric. See the linked [fio size suffix documentation](https://github.com/axboe/fio/blob/master/HOWTO.rst), [sysstat iostat manual](https://github.com/sysstat/sysstat/blob/master/man/iostat.in), [Linux block queue documentation](https://kernel.org/doc/html/latest/block/queue-sysfs.html), and [Azure managed disk documentation](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types).

Native fio group bandwidth statistics use staggered per-worker clocks. At high thread counts, summed worker IO counts divided by the maximum worker runtime can overstate throughput over a shared wall-clock window. Use guest/Azure counters for steady disk throughput. The fio latency distribution summarizes its own collected worker samples, not exactly the guest's UTC window. Failed attempts are excluded and their raw artifacts are retained.

This report compares fio request completion, Linux block-device `r_await`, and Azure's one-minute VM metric. Azure's API timestamp anchoring is not used to shift points; the fixed leading guard is applied directly to the returned timestamp, and the unfiltered minute rows remain available for inspection. The guarded Azure-minus-fio differences are descriptive and should not be read as a calibration offset. The per-thread test is closed-loop: increasing threads increases concurrent synchronous reads, while each worker holds at most one outstanding request.
