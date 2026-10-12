# Replica instrumentation

`collect.py` runs alongside the 23 GiB test. Start it once the replica pod and
the attachment-verification manifest exist, and wait for
`.state/attachment-verification/verified.json` plus
`instrumentation/collector-status.json` to reach `collecting` before launching
`03-test.py`. `collecting` means it has proved that the replica SQL watcher,
diskstats stream, per-device BCC histogram stream for at least one of the
replica DATA/WAL devices, and Azure metric query all started successfully. If
the collector exits nonzero or its status becomes `failed`, do not start
VACUUM.

From this experiment directory:

```bash
python3 instrumentation/collect.py \
  --namespace <namespace> \
  --replica-pod <replica-pod> \
  --out results/<run-id>/instrumentation \
  --kubeconfig .state/kubeconfig
```

The node name comes from the selected replica pod. The collector reads the
replica Azure VM resource ID from
`.state/attachment-verification/verified.json` at
`roles.replica.vmResourceId`, and verifies it matches the node's Azure
`providerID`. It discovers the Azure metric namespace before sampling, tries
the VMSS instance resource first, then allows a VMSS parent query only when it
can filter to the exact replica `VMName` dimension. It never accepts an
unfiltered VMSS aggregate as replica data. If the manifest lives elsewhere,
pass `--verified-attachments <path>`.

The collector installs Ubuntu's `bpfcc-tools` and `python3-bpfcc` packages in a
temporary `ubuntu:24.04` pod pinned to the replica node. The pod uses host PID
and privileged access for eBPF and mounts the node's `/lib/modules` and
`/usr/src` read-only so BCC can use its kernel modules and headers. It checks
that the installed `biolatency` supports JSON output, then runs
`biolatency -mTD -j 1`: millisecond latency histograms, timestamps, and a
separate histogram for each block device. A per-device histogram for DATA or
WAL must arrive before the collector reports ready. BCC has printed Python dict
representations, including byte-string device labels, in some releases, so the
collector safely parses both JSON and Python literal output. Override the image
with `--bcc-image` or `BCC_IMAGE`; the resolved image ID is saved in
`preflight.json`.

The process stays in the foreground. Send SIGINT or SIGTERM after `03-test.py`
has finished. It stops the SQL and diskstats streams, deletes its uniquely
named BCC pod, polls Azure Monitor for late one-minute metrics for up to 120
seconds, writes `coverage.json`, and exits. The pod is temporary experiment
infrastructure; the collector removes it before returning. Stop the collector
and retain its local output before tearing down the cluster.

Files written to `--out`:

- `pgstat-receiver.jsonl` records `pg_stat_activity` WAL receiver backend wait
  events, `pg_stat_wal_receiver`, and replay LSN once per second. The SQL query
  runs through one persistent local `psql` session with `\watch 1`.
- `diskstats.jsonl` records every row from the replica's `/proc/diskstats` once
  per second, with raw counters and the DATA/WAL device mapping. Raw stream and
  parser errors are kept in adjacent `.raw.log` and `*-errors.jsonl` files.
- `biolatency-histograms.jsonl` has parsed, timestamped per-device histograms;
  `biolatency.raw.log` preserves the original BCC output. The DATA and WAL
  device names are in `preflight.json`.
- `azure-vm-uncached-iops.jsonl` contains one-minute average
  points for **VM Uncached IOPS Consumed Percentage**, including successive
  updates to an open one-minute bucket. Every full raw Azure
  response and failed poll is also retained in `azure-monitor-polls.jsonl` and
  `azure-monitor-errors.jsonl`. Microsoft documents this metric at PT1M grain,
  so it cannot provide one-second samples.
- `preflight.json`, `collector-status.json`, `coverage.json`, and
  `collector-errors.jsonl` preserve readiness, identities, sample intervals,
  gaps, and failures.

The block-latency tracer uses the [BCC biolatency tool](https://github.com/iovisor/bcc/blob/master/tools/biolatency.py)
and its [container setup guidance](https://github.com/iovisor/bcc/blob/master/QUICKSTART.md).
Azure's [VM monitoring reference](https://learn.microsoft.com/en-us/azure/virtual-machines/monitor-vm-reference)
documents the VM uncached IOPS metric and its one-minute time grain.

Run the local parser checks with `python3 -m unittest discover -s instrumentation
-p 'test_*.py' -v`. They cover the published BCC per-device output shape, two
device labels, and the SQL watcher row fields.

The recorded run has materially incomplete BCC coverage. Read [the quality audit](../results/20261012T040219Z/biolatency-quality-audit.json) before interpreting histograms. Presence/readiness checks do not establish event-count parity. These as-run tracing semantics are retained; no claim is made that this collector yields complete NVMe latency distributions.
