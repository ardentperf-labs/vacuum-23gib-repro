# Throughput and latency validation

This isolated owned stack validates the neighboring [1 MiB read experiment](../fio-hdd-1m-repro/REPORT.md). It uses Standard_D8ds_v4, with a documented 192 MB/s uncached baseline, and a separate Standard_LRS clone of the same prepared 512 GiB snapshot. Host caching is None; the guest fixture is read-only.

Four fio workers ran for a 30-second ramp plus 600 seconds measurement. Subsequent 144- and 160-worker phases used 30-second ramp plus 180 seconds measurement. Every worker issued synchronous direct 1 MiB random reads, with one request outstanding. `validation-plan.json` records the plan, and `artifacts/` retains the guest data, metrics, definitions, and cleanup verification. The attempted D8s_v3 deployment failed due to regional capacity restrictions; its log is preserved.

The scripts use the same keeper manifest, ownership checks, and snapshot-preservation guards as the main experiment. Before provisioning, initialize the keeper manifest from the `eastus/` region root as described in the [region README](../README.md). Provision into a fresh directory without old state; copy `guest-test.py` to the VM and invoke `--only-threads 4 --runtime 600`, followed after completion by `--only-threads 144 160 --runtime 180`. `collect.py` collects the stack's own raw artifacts and Azure metrics. Analyze from the `eastus/` region root with:

```bash
fio-hdd-1m-repro/.venv/bin/python fio-hdd-1m-repro/analyze.py --artifacts fio-hdd-1m-headroom/artifacts
```

See [the measured validation table and charts](artifacts/analysis/report.md). `teardown.py` verifies ownership, refuses resource-group deletion if it contains any snapshot, and checks all subscription snapshots are preserved.
