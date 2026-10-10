# East US Standard HDD fio runs

This region archive contains the 4 KiB sweep, the 1 MiB sweep, and the separate
1 MiB bandwidth and latency validation stack. Each run directory contains its
own provisioning and collection scripts, analysis code, reports, graphs, and
captured raw artifacts. The 1 MiB report links to the neighboring headroom
validation directory.

To reproduce a stack, run commands from this `eastus/` directory. Set up the
region-local keeper manifest once before provisioning:

```bash
mkdir -p .state
cp fixture.json .state/prepared-snapshot-keeper.json
```

Then follow the run-specific instructions in the corresponding README. The
provisioning scripts create fresh `state.json` and SSH keys. The archive keeps
the original Azure resource and ownership metadata at each run's
`artifacts/infrastructure-state.json`; it omits the original root `state.json`,
SSH keys, and Python virtual environments. The copied keeper fixture contains
no access SAS or storage keys. `COPY-SHA256.json` records source and archive
hashes for every byte-identical copied file; the intentionally edited documents
are listed there.
