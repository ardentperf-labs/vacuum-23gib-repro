# West US 2 Standard HDD 1 MiB fio run

This run measures synchronous 1 MiB random-read throughput and latency on a
read-only clone of the prepared PostgreSQL snapshot. The commands below are
archive-portable: they assume this folder is copied to
`westus2/fio-hdd-1m-repro/` and are run from the `westus2/` region root, where
the fixture manifest is `fixture.json`.

## VM and disk

The selected VM is `Standard_E2bs_v5`, with 2 vCPUs and 16 GiB of memory. Its
SCSI Ebsv5 profile has documented maximum uncached Premium SSD throughput of
6,000 IOPS and 170 MB/s. This size was selected after earlier candidates
encountered regional capacity limits and Dsv5-family quota was unavailable.
These VM limits describe the host, not a measured disk result. See Microsoft's
[Ebsv5 series specifications](https://learn.microsoft.com/en-us/azure/virtual-machines/ebdsv5-ebsv5-series).

The data device is a 512 GiB `Standard_LRS` clone on SCSI LUN 0 with host
caching `None`. The guest verifies the populated filesystem, marks the block
device read-only, and mounts it `ro,noload`. The fixture uses up to sixteen
allocated large relation files; the workload issues reads only.

The VM-level Azure Data Disk Latency metric is a one-minute average and is
available for SCSI-attached disks, not NVMe-attached disks. The metric is
documented as preview. The selected SCSI controller is required for that
series. See Microsoft's [Azure disk metrics documentation](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-metrics).

## Workload

The configured main-sweep points are 1, 2, 4, 8, 16, 32, 64, 128, 144, 160,
256, 512, 768, and 1,024 threads. Each requested point has a 30-second ramp
and 180-second measurement. The sweep has no five-second latency stop rule:
the requested 768- and 1,024-thread points run for their full measurement
period. A separate single-thread repeat is run and stored under
`artifacts/single-validation/` by the automated controller.

Each worker uses `rw=randread`, `bs=1m`, `direct=1`, `ioengine=psync`,
`iodepth=1`, and one outstanding synchronous request. Linux may split a 1 MiB
fio operation into smaller block requests, so fio completion latency, guest
block-device await, and Azure latency describe distinct request and sampling
boundaries. The run records those series separately, plus fio JSON, guest
`iostat`/`vmstat` samples, VM/disk definitions, and Azure metrics.

## Automated run

Authenticate with Azure CLI and confirm its active subscription matches the
subscription in `fixture.json`. From `westus2/`, create an external analysis
environment and prepare the selected VM size before starting the controller:

```bash
python3 -m venv .venv
.venv/bin/pip install -r fio-hdd-1m-repro/requirements.txt
python3 fio-hdd-1m-repro/provision.py --prepare-vm-only --vm-size Standard_E2bs_v5 --fixture-manifest fixture.json
python3 fio-hdd-1m-repro/run.py --analysis-python .venv/bin/python --fixture-manifest fixture.json
```

The virtual environment is local and is not part of the archive. The first
provision command creates the owned VM without attaching the snapshot disk;
the controller then waits for the fixture manifest, verifies and attaches the
disk, starts the main sweep and repeat, collects metrics, and runs analysis.
Both provisioning and the controller default to `Standard_E2bs_v5`; the
explicit prepare step can install guest tools while the fixture is prepared.

`run.py` leaves Azure resources in place for inspection. It does not run
teardown automatically. The teardown script verifies stack ownership, refuses
to delete a resource group that contains a snapshot, and checks that the
subscription's snapshot inventory is unchanged after cleanup. The prepared
keeper snapshot remains in its separate resource group.

## Manual main-stage option

For a manually controlled main sweep, run from `westus2/`:

```bash
python3 fio-hdd-1m-repro/provision.py --vm-size Standard_E2bs_v5 --fixture-manifest fixture.json
FIO_VM_IP=$(python3 -c 'import json; print(json.load(open("fio-hdd-1m-repro/state.json"))["ip"])')
ssh -i fio-hdd-1m-repro/ssh-key -o StrictHostKeyChecking=accept-new "fioadmin@$FIO_VM_IP" 'sudo cloud-init status --wait'
scp -i fio-hdd-1m-repro/ssh-key fio-hdd-1m-repro/guest-test.py "fioadmin@$FIO_VM_IP:/home/fioadmin/guest-test.py"
ssh -i fio-hdd-1m-repro/ssh-key "fioadmin@$FIO_VM_IP" 'nohup sudo python3 -u /home/fioadmin/guest-test.py > /home/fioadmin/guest-test.log 2>&1 < /dev/null &'
python3 fio-hdd-1m-repro/poll.py
python3 fio-hdd-1m-repro/collect.py
.venv/bin/python fio-hdd-1m-repro/analyze.py --artifacts fio-hdd-1m-repro/artifacts
```

Poll until the guest's `complete` marker is true before collecting. The
automated controller additionally runs and collects the isolated single-thread
repeat.

## Results

[Measured results and graphs](REPORT.md) retain the primary sweep and separate single-thread repeat.
