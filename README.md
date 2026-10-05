# PostgreSQL VACUUM on Azure Standard HDD

A standalone reproduction and dataset for one **23-GiB VACUUM-only run** on
PostgreSQL 16.15 / CloudNativePG 1.30.1. Two AKS `Standard_D2s_v4` workers each
have separate 512-GiB Standard HDD DATA and WAL disks. No client workload runs.

The primary finished in **35 min 9 sec**. Its replica took another **2 h 29 min
26 sec** to replay the WAL and catch up. Across the full timeline, replica DATA
reads averaged **537 IOPS / 4.40 MB/s / 343 ms await**, and DATA write await
averaged **311 ms**. No matching replication timeout or reconnect messages
were detected in the collected logs.

Read [REPORT.md](REPORT.md) for the architecture, complete timeline, statistics,
methodology and graphs. The run is `20261004T205730Z`; actual measurement started
at 22:45 UTC on October 4, 2026, and ended at 01:50 UTC on October 5.

## Reproduce

Prerequisites: Bash, Azure CLI (`az login`), `kubectl`, `jq`, `curl`, GNU
`timeout`/`flock`, `ssh-keygen`, and Python 3.11+ with `venv`. The Azure account needs
permission to create AKS, disks, snapshots, a private Blob account and a network
watcher. Allow four Dsv4-family vCPUs in East US. Provisioning and fixture
preparation add time beyond VACUUM and replica replay.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
# Uses the Azure CLI's active subscription unless SUB is set explicitly.
./01-provision.sh
TEST_HEAP_GIB=23 ./02-prepare.sh
.venv/bin/python 03-test.py
./04-teardown.sh
```

`LOCATION=eastus` and `NODE_VM_SIZE=Standard_D2s_v4` can be overridden for
provisioning. Changing the host, fixture size or preparation path can change
runtime and performance. Preparation reuses compatible, locally supplied
`snapshot.json` metadata if available; this private metadata is gitignored.
Without it, preparation creates a fixture and snapshot. That fresh-source path
is implemented and locally checked, but was not Azure-tested for this published
run, which reused an existing prepared primary snapshot.

The replica is seeded from a native CNPG paired DATA/WAL snapshot backup. A
private Blob WAL archive supports preparation and is removed from the CNPG
configuration before measurement. The test checkpoints and verifies initial
replica replay, executes `VACUUM (VERBOSE, FREEZE)`, then continues five-second
sampling until three healthy observations confirm replica catch-up. Default
VACUUM and catch-up safety limits are 24 hours each; exceeding a limit fails
the test rather than reporting completion.

**All snapshots must be retained.** Teardown must not delete a snapshot
indirectly through AKS or resource-group deletion. Read its output carefully;
retention checks can defer infrastructure cleanup. In this measured run,
cleanup was deliberately held and infrastructure remained allocated. Keep
`.state/` until cleanup is resolved; it contains ownership records and private
credentials and must never be committed.

## Results and graphs

[results/20261004T205730Z](results/20261004T205730Z) contains compressed raw
samples, timing, preflight, summary, VACUUM output, replication logs and the
three separate graphs: [await](results/20261004T205730Z/await.png),
[IOPS](results/20261004T205730Z/iops.png), and
[throughput](results/20261004T205730Z/throughput.png).

Regenerate the figures locally without Azure access:

```bash
.venv/bin/python plot.py results/20261004T205730Z
.venv/bin/python -m unittest discover -q
```

Plots use linear axes, 60-second buckets and a p99 display ceiling. Await gaps
mean no completed I/O, not missing collection or zero latency. Numeric summary
statistics use the original counter intervals and are not clipped.

Subscription IDs and local workspace prefixes in published artifacts are
anonymized; numeric measurements are unchanged. [RESULTS-PROVENANCE.json](RESULTS-PROVENANCE.json)
records the redactions and file hashes. The scripts are portable adaptations
of the measured run; the fresh-source path and updated cleanup policy have not
been exercised end-to-end in Azure. No live infrastructure state, kubeconfig, local `snapshot.json` configuration
or Azure credentials are included. Anonymized snapshot resource references
remain in the recorded preparation evidence.
