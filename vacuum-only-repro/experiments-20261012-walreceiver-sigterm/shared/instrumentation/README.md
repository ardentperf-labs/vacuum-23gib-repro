# Shared run instrumentation

`collect.py` samples both PostgreSQL roles in one run. Start it after
`verify-attachments.py` has written `attachment-verification/verified.json`
and before VACUUM or the client:

```sh
python3 shared/instrumentation/collect.py \
  --namespace repro \
  --verified-attachments .state/attachment-verification/verified.json \
  --out results/<run-id>/instrumentation \
  --kubeconfig .state/kubeconfig \
  --database repro \
  --client-app-name hdd-repro-client
```

Wait for `collector-status.json` to report `collecting`. Required readiness
means both primary and replica PostgreSQL SQL watchers and DATA/WAL diskstats
streams have each produced a first sample. Azure definitions have been queried
and BCC has been attempted on both nodes before this phase. BCC is best-effort:
unsupported tools or kernel hooks are listed as gaps and do not prevent the
diskstats and PostgreSQL collectors from becoming ready. If BCC is supported,
the per-device histogram and per-device read/write flag processes must be alive
before their pod reports `__BCC_STARTED__`.

Send SIGINT or SIGTERM after the measured workload has ended. The collector
closes its SQL and diskstats streams, deletes only its uniquely named BCC pods,
re-queries Azure Monitor during the configured late-ingestion grace period, and
writes `coverage.json`. Do not delete the run's output directory before that
file and a final `stopped` collector status exist.

Each `*-pgstat.jsonl` row has a PostgreSQL server timestamp and includes client
backend wait events, transaction/query ages and blocker PIDs, WAL sender and
receiver state, replication LSNs, vacuum progress, WAL/database counters,
replication slot state, the synchronous replication and WAL timeout settings,
background-writer/checkpointer counters, `pg_stat_io` when exposed by the
server, and `pg_stat_recovery_prefetch` when exposed. The receiver's raw
`conninfo` is deliberately omitted because it can contain credentials. The
sample retains an explicit redaction marker instead.

`*-diskstats.jsonl` retains raw `/proc/diskstats` counters for every guest
device; use the verified device-to-mount mapping in `preflight.json` to derive
per-role DATA/WAL read/write operations, bytes, in-flight I/O, and await.
`*-hoststats.jsonl` samples aggregate `/proc/stat` CPU counters, key
`/proc/meminfo` fields, and CPU/memory/I/O pressure-stall counters on the same
one-second stream. Missing PSI files are represented by absent resource keys.

`bcc-device-histograms.jsonl` contains the available `biolatency -D` device
histograms. `bcc-read-write-histograms.jsonl` contains separate filtered `-F`
histograms for each exact DATA/WAL device. Linux request operation values 0 and
1 are labeled read and write; all other flag values remain recorded. BCC output
is diagnostic and must be compared with diskstats counts and coverage before
being interpreted as complete latency evidence.

The `azure/` directory retains each raw metric definition and metric response.
It queries exact VM resources where available, falling back to VMSS parent
metrics only with an exact VMName filter; attached-disk VMSS metrics also use
the verified LUN filter. The requested VM metrics include CPU, memory, cached
and uncached IOPS/bandwidth consumed percentages. Per-LUN metrics include
read/write operations and bytes, IOPS/bandwidth consumed percentages, queue
depth, latency, targets, and burst-credit values. Each managed disk resource is
also queried for composite read/write operations and bytes. `definitions-inventory.json`
and `coverage.json` list unavailable definitions, failed queries, empty series,
and dimension mismatches. Azure Monitor data remains at native PT1M grain.
