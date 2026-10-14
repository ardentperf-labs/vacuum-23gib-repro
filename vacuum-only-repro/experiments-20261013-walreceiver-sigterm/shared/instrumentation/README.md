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

The same rows include `pg_stat_archiver` and `archiveConfiguration`.
`archive_mode` retains its value and source; `archive_command`,
`restore_command`, and `archive_library` retain only configured state,
executable basename, recognized CNPG subcommand, backend-keyword booleans, and
setting source. Their raw values and arguments are never written.
`archive-source-receipt.json` records a whitelisted summary of live Cluster
backup/recovery/plugin references and available CNPG Backup, ScheduledBackup,
and ObjectStore metadata. Secret values are never queried or stored; only
referenced Secret names and key names may appear. Destination URLs are reduced
to host, path-component count, and a SHA-256 fingerprint.
At collector startup it also checks the adjacent `.state/wal-archive.json` and
`.state/wal-archive-smoke.json` receipts and copies only their approved status,
image/backend, selector names, WAL name/size, matching hashes, archiver counters,
and boolean checks. Owner tokens, source snapshot IDs, and arbitrary receipt
fields are omitted.

`*-diskstats.jsonl` retains raw `/proc/diskstats` counters for every guest
device; use the verified device-to-mount mapping in `preflight.json` to derive
per-role DATA/WAL read/write operations, bytes, in-flight I/O, and await.
`*-hoststats.jsonl` samples aggregate `/proc/stat` CPU counters, key
`/proc/meminfo` fields, and CPU/memory/I/O pressure-stall counters on the same
one-second stream. Missing PSI files are represented by absent resource keys.

`bcc-device-histograms.jsonl` contains the available `biolatency -D` device
histograms. `bcc-read-write-histograms.jsonl` contains `biolatency -F` buckets
when supported. If the filtered kprobe exits or `-F` is unavailable, the BCC
pod tries `bcc_rw_tracepoints.py` on `block_io_start` and `block_io_done` and
adds read/write buckets with `source: block-tracepoint-fallback`. The helper
uses the verified DATA/WAL device numbers and the tracepoints' `rwbs` field.
Its readiness, interval, and failure records are retained in
`*-bcc-rw-tracepoint-status.jsonl`, `bcc-rw-tracepoint-intervals.jsonl`, and
the BCC error/unparsed logs. Coverage reports the helper readiness, interval
and bucket counts, and pair counters.

The BCC log reader reconnects if `kubectl logs --follow` ends. It resumes from
two seconds before the latest container log timestamp and deduplicates exact
timestamp-plus-line repeats before parsing either biolatency or tracepoint
histograms. Segment, reconnect, duplicate, and timestamp-gap metadata are
written to the per-role `*-bcc-stream-events.jsonl`,
`*-bcc-stream-gaps.jsonl`, and `*-bcc-stream-duplicates.jsonl` files. If a
timestamp cannot be parsed, the record is retained and a gap is recorded; the
collector does not claim that replay was deduplicated for that line.

The tracepoint helper pairs events on `(device, sector)`. If two starts for the
same device and sector overlap, a later start replaces the earlier timestamp;
`keyCollisions` and `unmatchedCompletions` quantify this limitation, while
`mapUpdateFailures` records map-capacity errors. `matchedCompletions` is the
number of paired requests and should be compared with histogram bucket totals
and guest diskstats. Tracepoint BPF remains optional: a compile, verifier, or
hook failure is recorded as a coverage gap and does not fail PostgreSQL,
diskstats, or Azure collection.

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

For a read-only point-in-time archive/recovery capture after a run, invoke
`shared/capture-diagnosis.py` with the same namespace, Cluster name, kubeconfig,
and a new output directory. It records safe archive settings/counters, selected
redacted PostgreSQL and operator log events, and per-pod WAL segment plus
`archive_status` file metadata. It never stores unfiltered logs, raw command
settings, Secret values, or kubectl stderr; output files are created with
private filesystem permissions for the publisher to review and include. When a
PostgreSQL container has restarted, it also reads that pod's previous-container
logs into a separate redacted event file.

When the adjacent ready `.state/wal-archive.json` contains a validated Azure
Storage account resource ID, the collector also queries that account's exact
`/blobServices/default` resource. It records `Transactions` split by `ApiName`
and `ResponseType`, plus `SuccessE2ELatency`, `SuccessServerLatency`,
`Availability`, `Ingress`, and `Egress`. Transaction counts and byte counters
use `Total`; latency and availability use `Average` when the metric definition
advertises it. Every returned series keeps the native dimension values in the
point identity. Missing definitions, unsupported aggregations, empty series,
or missing `ApiName`/`ResponseType` values are explicit coverage gaps. This is
an optional diagnostic target and does not gate collector readiness. The
`Transactions` metric includes successful and failed requests, while the two
`Success*Latency` metrics cover successful requests only; interpret them beside
the transaction response types. The
dimension and aggregation choices follow Microsoft's [Blob service supported
metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-storage-storageaccounts-blobservices-metrics)
and the [Azure CLI metrics command](https://learn.microsoft.com/cli/azure/monitor/metrics).

CNPG's wrapped PostgreSQL JSON records are decoded through an allowlist of
`record.message`, `record.log_time`, `record.process_id`,
`record.error_severity`, and `record.sql_state_code`; query, detail, context,
conninfo, and other PostgreSQL fields are never copied. To summarize successful
archive restores after the intervention and relate them to the primary sender
and replica receiver returning to streaming, run:

```sh
python3 shared/analysis-restore-path-report.py \
  --postgres-log results/<run-id>/diagnosis/postgres-log-events-redacted.jsonl \
  --postgres-log results/<run-id>/diagnosis/postgres-previous-log-events-redacted.jsonl \
  --intervention results/<run-id>/walreceiver-intervention.json \
  --primary-pgstat results/<run-id>/instrumentation/primary-pgstat.jsonl \
  --replica-pgstat results/<run-id>/instrumentation/replica-pgstat.jsonl \
  --out results/<run-id>/diagnosis/archive-restore-timeline.json
```

The report reads PostgreSQL log input without copying raw lines, deduplicates
the same timestamp/outcome/WAL when raw and redacted captures overlap, and
emits post-signal restore counts, first/last WAL names, receiver absence or
PID-change evidence, the sender's first `catchup`, and subsequent streaming and
synchronous-quorum samples. If no disconnect bracket was observed, it labels a
post-signal streaming row as a sample rather than claiming a return. Missing
signal, log, or sample coverage remains explicit; temporal ordering alone is
not treated as proof of causality.
