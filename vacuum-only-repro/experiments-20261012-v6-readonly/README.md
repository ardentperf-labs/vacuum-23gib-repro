# 23-GiB VACUUM rerun: D16as_v6, Standard HDD, ReadOnly host cache

[Measured results and limitations](REPORT.md) · [Raw results](results/20261012T040219Z/) · [Publication provenance](PUBLICATION-PROVENANCE.json)

The October 12 rerun completed VACUUM and replica catch-up before downloading evidence and deleting its AKS infrastructure. It used two 16-vCPU D16as_v6 nodes in West US 3, PostgreSQL 16.15 / CNPG 1.30.1, and separate 512-GiB Standard HDD DATA/WAL disks with ReadOnly host caching on all four attachments. No client workload ran. Guest Linux caches were cleared after native replica seeding; Azure host cache was not purged.

The replica averaged 7,942 DATA read IOPS against 275 on the primary (28.9×), with nearly equal read throughput. This is guest request-count amplification. Low guest read await and 0–2% VM uncached consumption strongly suggest most reads were served by host cache; physical HDD read amplification and incident read latency were not established. BCC histograms are materially incomplete and diagnostic only.

## Offline analysis

These commands use local published data and do not create Azure resources:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python analyze-instrumentation.py results/20261012T040219Z --output-dir /tmp/v6-readonly-analysis
.venv/bin/python -m unittest discover -s instrumentation -p 'test_*.py' -v
.venv/bin/python -m unittest test_teardown_safety -v
```

The analyzer produces replica queue-depth, in-flight-I/O and exact-VM uncached-percent plots with linear y-axes. Original primary/replica DATA/WAL read/write await, IOPS and throughput graphs are retained under `measurement/`. Device bindings are explicit: primary DATA/WAL are nvme0n3/nvme0n2; replica DATA/WAL are nvme0n2/nvme0n3.

## Historical execution scripts

The four entry scripts and supporting helpers are included for review. Preparation restores the primary from the same previously verified pre-VACUUM DATA/WAL snapshot pair, then seeds the replica through CNPG physical streaming. Snapshot identity, original source-disk identity, resource-group ownership, prior restore proof, hashes and preservation locks are checked before use. The measurement controller waits for VACUUM and replay, downloads logs, checks hashes and dataset presence, then tears down only its owned infrastructure. Teardown refuses snapshot-containing scopes and verifies the subscription snapshot inventory after deletion.

Private snapshot inputs and live `.state/` are deliberately excluded. To execute a new stack, supply your own verified `westus3-source-pair.json`, `westus3-source-pair-proof.json` and `westus3-restore-proof.json` in the expected schema and review the ownership/restore guards. This archive is not a generic snapshot-preparation tool and is not ready for provisioning merely by cloning it. Validate the source before provisioning:

```bash
./02-prepare.sh --validate-source-only
SUB=<your-subscription-id> LOCATION=westus3 NODE_VM_SIZE=Standard_D16as_v6 ./01-provision.sh
./02-prepare.sh
.venv/bin/python run-measurement.py
```

The BCC tracing path is retained as executed, including its observed coverage failure. Before relying on a future latency distribution, replace or repair the tracing path and validate live event counts against diskstats; merely unbuffering stdout will not repair the missing histogram cycles. No Azure rerun was performed during publication. Publication adaptations require an explicit subscription, default to the measured region, use the running Python interpreter instead of an old workspace venv, and repair stale documentation paths.

## Publication and cleanup

Azure subscription IDs are replaced by the all-zero example UUID and original workspace paths by `/workspace`, including text inside controller-log archives. Historical ownership tokens are mapped to example tokens and retired RFC1918 addresses to documentation addresses. Measurements are unchanged. Original receipts/hashes describe the private as-run files; use `PUBLICATION-PROVENANCE.json` for hashes of published files. Private source configuration manifests/proofs, credentials, kubeconfig, full subscription snapshot inventories and database/WAL exports stay local. Anonymized historical source identifiers inside preparation receipts remain as execution evidence. Resource names in sanitized execution evidence are historical, not authorization to adopt/delete resources.

All test resource groups were deleted by 04:24:34 UTC. The complete subscription snapshot inventory remained 47 before and after cleanup. All snapshots remain preserved; no Support ticket was opened.
