# Azure Standard HDD read-scaling experiment

This experiment measures how synchronous 4 KiB random reads scale as fio adds worker threads to one Azure Standard HDD. It records fio request latency and throughput, Linux block-device `r_await`, and Azure's per-data-disk metrics. The measured workload is read-only.

See [the measured findings](REPORT.md) for the completed experiment and links to its tables and graphs.

## Test setup

The test VM is `Standard_D4s_v4` (4 vCPU, 16 GiB). Its documented uncached remote-storage limit is 6,400 IOPS and 96 MB/s, above the test disk's limit. We use it because the smaller Bsv2 quota was unavailable and DS2_v2 had no regional capacity. The VM's storage cap should therefore leave the data disk as the intended IOPS bottleneck. [Dsv4 size specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series)

The data disk is a clone of the retained 512 GiB keeper snapshot, presented as Standard HDD S20 with host caching set to `None`. S20 supports up to 500 base IOPS and 60 MB/s. All S4–S50 Standard HDD sizes have those same base limits. [Standard HDD specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-types)

The guest mounts the populated PostgreSQL data filesystem read-only and marks the backing block device read-only. The fixture uses up to sixteen existing files of at least 1 GiB each, and checks that their blocks are allocated before the run. This avoids testing empty or sparse zero-filled regions. No writes are issued during the measurement.

Each run uses:

- `rw=randread`, `bs=4k`, `direct=1`, and `--readonly`;
- `ioengine=psync`, `iodepth=1`, and `thread=1`;
- a different `numjobs` value for each thread-count point, so each worker has at most one synchronous request outstanding;
- a 30-second ramp followed by a 180-second measured period, started at a UTC minute boundary.

Fio documents `psync` as positional synchronous reads and states that `iodepth` above 1 does not increase queue depth for synchronous engines. With one outstanding request per worker, thread count controls the offered concurrency. [fio HOWTO](https://github.com/axboe/fio/blob/master/HOWTO.rst)

## Inputs and outputs

To reproduce, run from the `eastus/` region root with Azure CLI authentication.
Initialize its keeper manifest once as described in the [region README](../README.md):

```bash
python3 -m venv fio-hdd-repro/.venv
fio-hdd-repro/.venv/bin/pip install -r fio-hdd-repro/requirements.txt
python3 fio-hdd-repro/provision.py
FIO_VM_IP=$(python3 -c 'import json; print(json.load(open("fio-hdd-repro/state.json"))["ip"])')
ssh -i fio-hdd-repro/ssh-key -o StrictHostKeyChecking=accept-new "fioadmin@$FIO_VM_IP" 'sudo cloud-init status --wait'
scp -i fio-hdd-repro/ssh-key fio-hdd-repro/guest-test.py "fioadmin@$FIO_VM_IP:/home/fioadmin/guest-test.py"
ssh -i fio-hdd-repro/ssh-key "fioadmin@$FIO_VM_IP" 'nohup sudo python3 -u /home/fioadmin/guest-test.py > /home/fioadmin/guest-test.log 2>&1 < /dev/null &'
python3 fio-hdd-repro/poll.py
# Once poll reports complete=True, allow recent Azure samples to arrive:
python3 fio-hdd-repro/collect.py
fio-hdd-repro/.venv/bin/python fio-hdd-repro/analyze.py --artifacts fio-hdd-repro/artifacts
python3 fio-hdd-repro/teardown.py
```

Use a fresh experiment directory without `state.json` when provisioning another stack. The VM, clone, network, and OS disk are disposable. Teardown verifies subscription and ownership tags and refuses to delete a group containing any snapshot. It also verifies that the subscription snapshot inventory is preserved. The source snapshot is in a separate group and is never modified.

The analyzer reads `artifacts/run-N/meta.json`, `fio.json`, and `iostat.json`, plus the VM, disk, and host Azure metric JSON files and guest inventory. `meta.json` records `threads`, `start`, `end`, `measurement_start`, and `measurement_end` as ISO-8601 UTC timestamps. Azure's displayed values use complete PT1M LUN 0 buckets whose returned timestamp is at least 60 seconds after `measurement_start` and whose full PT1M interval ends by `measurement_end`. This is a leading timestamp guard; the analyzer does not shift timestamps or claim that it corrects Azure's bucket alignment.

Run from the `eastus/` region root:

```bash
python3 fio-hdd-repro/analyze.py --artifacts fio-hdd-repro/artifacts
```

The analyzer writes `artifacts/analysis/summary.csv`, `azure-minute-points.csv`, `report.md`, and four PNGs: `iops.png`, `iops-low-threads.png`, `throughput.png`, and `latency.png`. `azure-minute-points.csv` preserves all metric points returned by the VM, disk, and host queries, including rows outside the guarded summary selection. Install `matplotlib` if plots are needed. If the analyzer cannot map LUN 0 from guest inventory, specify the whole-disk device name, for example `--device sdc`; guest iostat columns remain blank when the device cannot be identified. The default Azure leading guard is 60 seconds and can be changed with `--azure-guard-seconds`.

The CSV includes:

- fio aggregate read IOPS and decimal MB/s, total `lat` mean, completion `clat` mean, and `clat` p50/p95/p99;
- guest `r_await` weighted by read operations in complete iostat samples, along with guest read IOPS, MB/s, average queue size, and sample count;
- guarded Azure read IOPS, read throughput, Data Disk Latency, and queue depth, each averaged across valid LUN 0 minute buckets, with the 60-second guard and per-metric minute count shown;
- the original full-window Azure averages in `azure_full_window_*` columns, with their full-window minute counts, so guarded and unguarded selections can be compared;
- Azure-minus-fio differences for IOPS, throughput, and mean completion latency when both values exist.

Unavailable measurements are empty in the CSV and omitted from graph series. The report lists observed fio, guest, and guarded Azure average latency points in the 1,500–5,000 ms band. It does not interpolate between thread counts.

## Reading the measurements

The guest `r_await` field is the average time for a block request to complete, including time queued and service time. Fio `clat` is the time from submitting a synchronous I/O to its completion; fio `lat` includes submission plus completion. [sysstat iostat definitions](https://github.com/sysstat/sysstat/blob/master/man/iostat.in), [fio latency definitions](https://github.com/axboe/fio/blob/master/HOWTO.rst)

Azure's VM-level `Data Disk Latency` is an average in milliseconds with a LUN dimension and one-minute grain. The latency metric is documented as preview and is available for SCSI-attached disks, not NVMe-attached disks. VM-level read IOPS and bytes/sec metrics provide the corresponding Azure throughput series. Azure's managed-disk resource metrics expose read operations and bytes, but do not list a latency metric, so this report uses the VM metrics. [Azure VM disk metrics](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-metrics), [supported VM metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-compute-virtualmachines-metrics), [supported managed-disk metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-compute-disks-metrics)

Azure values are one-minute averages. Fio values aggregate its workers’ individual timing windows; guest values aggregate one-second intervals within the fixed UTC measurement window. The first run showed an early Azure IOPS bucket well below the contemporaneous guest rate, so the displayed Azure average excludes timestamps in the first 60 seconds after the measurement begins. That guard mitigates the observed mismatch; it is not a proven correction for Azure timestamp semantics. `azure_full_window_*` columns and `azure-minute-points.csv` preserve the unguarded averages and every raw returned metric point for review. Runs with only one guarded minute show a single-bucket average; the per-metric count makes that limitation explicit. The graphs use linear axes. The extra IOPS graph shows the first 64 threads; the CSV records Azure-minus-fio differences.

With 4 KiB requests, 500 IOPS corresponds to about 2 MB/s, far below the disk's 60 MB/s cap. This experiment therefore focuses on IOPS and latency. Higher thread counts increase synchronous concurrency; CPU scheduling, guest queue limits, or a missing complete Azure minute can affect the observed crossing. The report uses measured points only.

At large thread counts, fio creates workers over several seconds and its per-worker ramp and measurement timers are staggered. The native group IOPS can therefore differ from the guest rate measured over the common UTC window. Keep both values: use guest/Azure counters to describe steady disk throughput and fio completion statistics to describe the latency seen by its workers.

Fio 3.28 sums worker I/O counts while retaining the maximum individual worker runtime for grouped IOPS. With staggered windows, that grouped rate is not a common wall-clock rate. The native fio latency distribution likewise describes its collected worker samples, rather than exactly the guest’s UTC window. [fio 3.28 aggregation and JSON output](https://github.com/axboe/fio/blob/fio-3.28/stat.c)
