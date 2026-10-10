<!-- Latest retention policy: preserve every snapshot until explicit user deletion instruction. -->
# VACUUM-only HDD reproduction

This experiment runs PostgreSQL 16.15 with CloudNativePG 1.30.1 on two AKS
workers. Each primary/replica instance has its own 512-GiB Standard_LRS DATA
disk and WAL disk, with host caching disabled. The default worker size is
`Standard_D2s_v4` (2 vCPUs) in `eastus`. There is no application workload
during the measurement. SCSI/NVMe labels describe the VM’s virtual disk
interface; both recorded runs used Standard HDD managed disks. See
[Azure’s interface overview](https://learn.microsoft.com/en-us/azure/virtual-machines/nvme-overview).

## Run

Prerequisites: Azure CLI authenticated to the configured subscription, `kubectl`,
`jq`, `curl`, Bash, Python 3 with `venv`, and permission to create AKS, managed
disks and snapshots, and a private Blob storage account. Install Matplotlib in
a virtual environment:

```bash
python3 -m venv .venv
.venv/bin/pip install 'matplotlib>=3.9,<4'
```

Run these four actions from this directory, in order:

```bash
./01-provision.sh
./02-prepare.sh
.venv/bin/python 03-test.py
./04-teardown.sh
```

Provisioning defaults are `LOCATION=eastus`, `NODE_VM_SIZE=Standard_D2s_v4`,
and the `SUB` value in `01-provision.sh`; set those environment variables before
`01-provision.sh` to override them. Keep the pinned `snapshot.json` metadata
with the sole retained snapshot. `02-prepare.sh` reuses that source snapshot.
If it is in another region, the script makes a token-owned regional
incremental snapshot, then a full copy in the AKS node resource group for CSI
access. `SOURCE_SNAPSHOT_ID` can select a verified, token-owned regional
prepared snapshot in the stack resource group.

The preparation path restores the primary, separates WAL onto its own disk,
and temporarily enables CNPG's private Barman WAL archive. CNPG then creates a
native `volumeSnapshot` Backup containing paired PG_DATA and PG_WAL snapshots
and seeds the replica from that pair. The script waits for snapshot recovery,
streaming, and catch-up, then removes the object-store writer and credentials
before measurement. `archive_mode=on` remains a PostgreSQL/CNPG setting; the
temporary archive destination is removed. A fresh 110-GiB fixture and source
snapshot can be created if no usable source exists, but that fallback has not
been live-tested. If interrupted after snapshot creation but before metadata
promotion, teardown preserves the snapshot intent and ownership state for
recovery rather than discarding a potentially usable snapshot.

Set the desired test heap size when preparing, for example
`TEST_HEAP_GIB=23 ./02-prepare.sh`. The default is 23 GiB, projected to take
about 55 minutes for primary VACUUM on the default hosts from the measured
110-GiB / 4 h 20 m 35 s baseline. This is an estimate; changing hosts, cache
state or disk performance can change the duration. Concrete measured timings
are recorded beside the setting in `02-prepare.sh`. The retained large source
snapshot is preserved while preparation constructs the smaller test table.
Newly inserted tuples are too young for ordinary VACUUM to freeze: a local
PG16 check confirmed this. The rebuilt fixture therefore uses explicit
`FREEZE` to retain the survivor-freezing work seen in the historical run
(which froze every heap page). This is a deliberate workload option, while
the four requested settings remain at their defaults; runtime and WAL volume
for the smaller fixture still need Azure measurement.

`03-test.py` checkpoints before measurement and waits for the replica to replay
that checkpoint. It runs `VACUUM (VERBOSE, FREEZE) public.vacuum_target` for the
rebuilt fixture (plain `VERBOSE` for a legacy fixture) in a detached
process and records the SQL start/completion markers and remote exit status.
VACUUM is allowed to finish, with a 24-hour safety bound. No client workload is
started. The collector targets a 5-second cadence; actual counter intervals include
request overhead (the current SCSI run is near 5 seconds; the earlier NVMe run had a 6.6-second median). Window statistics
use the measured intervals.
The measurement checks `wal_level=logical`, `full_page_writes=on`,
`recovery_prefetch=try`, `vacuum_cost_delay=0`, and
`maintenance_io_concurrency=10`; these retain CNPG/PostgreSQL defaults.
`shared_buffers=256MB` is explicit, and `track_io_timing` and
`track_wal_io_timing` are enabled for observation. Each database pod requests
1 CPU / 2 GiB and is limited to 2 CPUs / 3 GiB.
Autovacuum stays on globally and is disabled only for the fixture table.
After VACUUM returns, collection continues until the replica has replayed the
exact VACUUM completion LSN and the sampled primary flush LSN for three
consecutive healthy polls. Successful test completion requires both primary
VACUUM exit 0 and verified replica catch-up. `--catchup-hours` sets a safety
bound (default 24); exceeding it makes the test fail and preserves evidence.
The historical 110-GiB run in the report used the earlier two-minute tail and
ended before replica catch-up.

Each run writes a timestamped directory under `results/`, including raw
`samples.jsonl`, timing and summary JSON, the verbose VACUUM log, and three
figures: `await.png`, `iops.png`, and `throughput.png`. Every figure has four
panels for primary/replica DATA/WAL and separate read/write traces. Y-axes are
linear. The charts use 60-second UTC buckets, with at least 90% valid coverage
and no overlap with invalid intervals. A p99-based ceiling, calculated from plotted 60-second bucket values, is only a display
limit for chart axes; it does not clip raw samples or numeric window statistics.
IOPS and throughput are time-weighted, while await is weighted by operation
count. Use `samples.jsonl` to inspect interval peaks; panels identify buckets
above the visible chart range. Await is Linux block-device time per completed
operation; throughput uses decimal MB/s.

Snapshot deletion is currently prohibited by user instruction. Automatic
teardown is on hold because deleting AKS or its node resource group can delete
snapshots indirectly. The run and metric collection continue.

The original `04-teardown.sh` cleanup implementation removes the token-owned AKS stack, node resource group,
temporary disks/snapshots, and archive resources. It verifies and preserves the
single retained prepared snapshot. Keep the local ownership metadata until
teardown succeeds; do not delete or edit `.state/` while resources remain.

See [REPORT.md](REPORT.md) for the completed measurements and `results/` for run
artifacts. The delivered raw counters are compressed as `samples.jsonl.gz`;
new runs write the uncompressed file.

The separate [23-GiB run report](REPORT-23GIB.md) covers that run exclusively, including full replica replay.

The completed [Premium SSD v2 experiments](experiments-20261007-premium-v2/README.md)
include the 23-, 50-, and 100-GiB runs, their
[comparison](experiments-20261007-premium-v2/REPORT.md), and complete per-run
snapshot, hydration, measurement, and cleanup timelines.
