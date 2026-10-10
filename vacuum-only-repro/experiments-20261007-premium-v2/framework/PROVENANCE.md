# Framework provenance and change ledger

Source: `/workspace/experiments/20261005-four/baseline`. The source
checkout was read only. The framework retains the baseline preparation and
measurement sequence and copies no kubeconfig, `.state/` directory, connection
secret, or source ownership token.

| File | Preserved behavior | Framework adaptation |
|---|---|---|
| `01-provision.sh` | AKS, CNPG 1.30.1, two workers, isolated archive account | Adds `AZURE_ZONE`, Premium v2 regional/zone preflight, 512-sector and 3000/125 StorageClass parameters, retained snapshot RG and locks, narrowly scoped CSI snapshot role, and longer CLI wait limits. |
| `02-prepare.sh` | Keeper-based fixture rebuild, WAL separation, CNPG paired snapshot backup, replica snapshot seed, baseline image/GUC/sync settings | Requires the orchestrator's immutable same-region source manifest; verifies source ID, unique ID, ownership, system identifier, size, sector, incremental state, readiness, and location; creates an owned Standard_LRS staging disk and a locked new-family incremental Copy restore source (the helper is under `recovery/source-restore/`); retains CNPG snapshots in the separate output RG; waits for full hydration and verifies all four Premium v2 disks. The baseline run resumes only final preparation audits from its already verified fixture and backup. |
| `03-test.py` | No client workload, detached VACUUM completion, five-second counter collection, primary/replica DATA/WAL, replica replay drain | Adds Premium v2 disk/zone/sector/performance checks, logical-sector StorageClass preflight, `wal_log_hints` and checksum capture, per-sample `pg_stat_recovery_prefetch`, and explicit baseline GUC/synchronous checks. |
| `04-teardown.sh` | Owner-scoped stack cleanup | Never deletes snapshots or the retained RG; verifies the RG and snapshot locks; refuses AKS/node/test RG deletion if any snapshot is present in the node or test RG; removes only verified owned compute/test resources; it deletes the recorded CSI role assignment when permitted and records it as a `ScopeLocked` residual when the retained-group lock blocks deletion. Shared Network Watchers are left to the root orchestrator. |
| `plot.py` | Linear axes, separate DATA/WAL read/write panels, await/IOPS/throughput | Updates the figure title to identify Premium SSD v2 and its requested performance. Existing counter deltas were inspected against 2,216 archived samples; no rollover or counter decrease was observed. |
| `snapshot.json` | Keeper fixture system identifier and heap/table sizing metadata | Reduced to fixture-only metadata; removed Azure IDs and owner tokens. |

The deployment-specific source manifests remain with the root-owned run
folders and are passed to `02-prepare.sh` using `SOURCE_MANIFEST`. They are
intentionally not copied into this shared framework.

Run-specific preparation recovery: baseline uses its preserved final-audit
wrapper; 50 GiB uses the numeric hydration finalizer to accept ARM's `100.0`
without weakening the Copy100 requirement. The 100-GiB original one-hour
Backup wait expired before CSI marked DATA ready. Its captured request and
terminal process chain feed a reviewed versioned continuation of the same
paired Backup. The v2 predicate reflects actual CNPG snapshot labels, and
keeps owned namespace/class, exact Backup UID, PVC/VSC/source disk, snapshot
UID and ARM ownership checks. All original scripts and failed attempts remain
archived. These wrappers finish preparation only. All three use the same common
`03-test.py` measurement sequence.

The first collectors stopped during the initial read-only configuration query
before submitting VACUUM because the added checksum field used
`data_checksum_version` instead of PostgreSQL16's `data_page_checksum_version`.
The corrected collector changes only that SQL column and preserves its JSON
key, workload, sampling, replay and analysis behavior. All six live instances
passed the full corrected query. Frozen v1 source and each failed startup are
preserved in `recovery/measurement-checksum-v2/` and run attempt folders; the
new common collector SHA is recorded in each actual test process receipt.
`READY.json` is the initial pre-integration handoff record, with historical
hashes; current runtime receipts and recovery reviews record later changes.
