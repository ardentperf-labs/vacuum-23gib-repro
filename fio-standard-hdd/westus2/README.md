# West US 2 Standard HDD experiments

This archive holds the completed West US 2 4 KiB and 1 MiB read sweeps, their
separate single-thread repeats, and regional fixture preparation. The 1 MiB
sweep includes the requested 768- and 1,024-thread points. See the
[comparison report](../REPORT.md), [4 KiB report](fio-hdd-repro/REPORT.md),
and [1 MiB report](fio-hdd-1m-repro/REPORT.md).

The `fixture.json` at this directory's root identifies a verified full 512 GiB
Standard HDD snapshot imported into a separate West US 2 keeper resource
group. The original East US keeper is also retained under `../eastus/fixture.json`.
All snapshots are to be preserved indefinitely unless the user explicitly
authorizes deletion. `fixture-preparation/cleanup.py` is a destructive cleanup
for temporary transfer resources; it checks that both keeper snapshots remain
and deletes only the transport resource group. Do not run it while the sweeps
are active. The archived version resolves the `fio-hdd-repro/` and
`fio-hdd-1m-repro/` aliases and reads their ownership records from
`artifacts/infrastructure-state.json`.

## Fixture preparation

The archived fixture is ready to use with the run scripts. To prepare another
regional copy from the retained East US source, work from this directory,
authenticate with Azure CLI, and verify the active subscription against the
East US fixture first. The preparation script reads the keeper manifest from
`.state/prepared-snapshot-keeper.json` and writes the resulting regional
manifest to `.state/fio-hdd-westus2-fixture.json`:

```bash
mkdir -p .state
cp ../eastus/fixture.json .state/prepared-snapshot-keeper.json
python3 fixture-preparation/prepare.py
cp .state/fio-hdd-westus2-fixture.json fixture.json
```

The full-copy workflow needs Azure CLI, the Azure CLI storage extension, and
AzCopy. `fixture-preparation/fast-copy.py` is the separately recorded AzCopy
transfer helper; `prepare.py --completed-fast-copy` is only for adopting a
successfully verified transfer marker. A newly prepared full snapshot must
also remain in its own keeper resource group and must not replace or delete
either existing keeper. The scripts, sanitized transfer logs, resource
metadata, and ownership state are kept under `fixture-preparation/` for audit.

## Running the sweeps

Run from this directory. The state and SSH keys created by the scripts stay
local and are excluded from the Git archive. The analysis virtual environment
is external to the archived run folders:

```bash
python3 -m venv .venv
.venv/bin/pip install -r fio-hdd-repro/requirements.txt
.venv/bin/pip install -r fio-hdd-1m-repro/requirements.txt
python3 fio-hdd-repro/provision.py --prepare-vm-only --vm-size Standard_E2bs_v5 --fixture-manifest fixture.json
python3 fio-hdd-repro/run.py --analysis-python .venv/bin/python --fixture-manifest fixture.json
python3 fio-hdd-1m-repro/provision.py --prepare-vm-only --vm-size Standard_E2bs_v5 --fixture-manifest fixture.json
python3 fio-hdd-1m-repro/run.py --analysis-python .venv/bin/python --fixture-manifest fixture.json
```

Both tests use `Standard_E2bs_v5` (2 vCPU, 16 GiB) with SCSI and a read-only
512 GiB Standard HDD data disk. The 4 KiB sweep tests 1 through 3,072 threads
and stops after a point reaches the configured five-second fio latency. The
1 MiB sweep tests 1, 2, 4, 8, 16, 32, 64, 128, 144, 160, 256, 512, 768, and
1,024 threads; it has no five-second stop, so the two added high-thread points
receive their full measurement. Each point uses a 30-second ramp and a
180-second measurement, followed by the separate single-thread validation.

See [the 4 KiB run notes](fio-hdd-repro/README.md) and
[the 1 MiB run notes](fio-hdd-1m-repro/README.md) for the workload and metric
details. Azure SCSI latency is sampled in PT1M windows; fio output and guest
block-device counters have different measurement windows and are reported
separately.
