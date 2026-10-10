# Premium SSD v2 VACUUM experiment framework

This directory contains the shared provision, prepare, measure, plot, and safe
teardown scripts for three isolated runs: a 23-GiB fixture in East US zone 1,
a 50-GiB fixture in West US 3 zone 1, and a 100-GiB fixture in East US zone 1.
Each stack uses two `Standard_D2s_v4` workers, the existing PostgreSQL 16.15
measurement image and baseline GUCs, and four separate 512-GiB `PremiumV2_LRS`
disks: primary DATA/WAL and replica DATA/WAL. Each disk is requested at
3,000 IOPS and 125 MB/s with 512-byte logical sectors and no host caching.

## Inputs and run order

The root orchestrator supplies each run's `LOCATION`, `AZURE_ZONE`, and
`TEST_HEAP_GIB`, copies the matching immutable `source-manifest.json` into the
run directory, and provides Azure credentials through the normal CLI login.
The manifest pins a same-region, 512-GiB incremental source snapshot and its
unique ID, owner tag, system identifier, and sector metadata. `02-prepare.sh`
accepts `SOURCE_MANIFEST=/path/to/source-manifest.json`; it fails closed if the
live snapshot differs from that manifest. It creates an owned incremental
CopyStart snapshot inside a separate, locked retained-snapshot resource group
before CNPG restores the fixture.

Install the Python plotting dependency once:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Run each isolated stack in order:

```bash
./01-provision.sh
TEST_HEAP_GIB=23 SOURCE_MANIFEST=./source-manifest.json ./02-prepare.sh
.venv/bin/python 03-test.py
./04-teardown.sh
```

Use `TEST_HEAP_GIB=50` or `100` for the other fixtures. Set `SUB` when the
subscription differs from the default in `01-provision.sh`. The provisioner
validates Premium v2 zonal availability and writes private ownership state only
under `.state/`.

## Measurement and retention

Preparation follows the existing fixture rebuild pipeline: it restores the
source through CNPG, rebuilds the half-dead indexed table to the requested
heap size, separates WAL onto its own disk, and seeds the replica from CNPG's
paired DATA/WAL snapshot backup. The preparation checks retain the baseline
PostgreSQL settings and synchronous replica configuration. No client workload
runs during measurement. The test waits for the replica to replay the VACUUM
completion LSN and samples primary/replica DATA/WAL counters, device mappings,
recovery-prefetch statistics, PostgreSQL settings, and Azure disk metadata.

The output directory contains raw five-second samples, preflight metadata,
VACUUM logs, timing, and three linear-axis charts: `await.png`, `iops.png`, and
`throughput.png`. Each chart separates primary/replica DATA/WAL reads and
writes. Disk preflight checks SKU, size, requested performance, zone, logical
sector size, provisioning completion, attachment, and independent DATA/WAL
mounts.

Every snapshot is retained. Source and experiment snapshots live in separate
owned resource groups protected by `CanNotDelete`; the framework does not issue
snapshot deletes. Teardown must refuse AKS or resource-group deletion whenever
it finds snapshots in the AKS node or test resource groups, because those
operations could remove snapshots indirectly. Keep `.state/` until cleanup has
verified ownership and retained-snapshot protection.

## Provenance

The scripts and plot helper are adapted from
`experiments/20261005-four/baseline`. The fixture construction, database image,
GUC checks, synchronous settings, no-workload policy, five-second sampling,
and linear plot axes are preserved. Azure disk provisioning, zone and
performance preflights, retained snapshot handling, and recovery-prefetch
instrumentation are the Premium SSD v2 adaptations. No environment files,
credentials, kubeconfigs, or runtime Azure state are part of this framework.
