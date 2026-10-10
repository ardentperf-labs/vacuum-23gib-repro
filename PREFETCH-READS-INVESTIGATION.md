# Why the completed prefetch-off run has almost no standby DATA reads

Investigation date: 2026-10-06. All experiment inputs were read only. No cloud
commands, workload changes, global cache eviction, or edits under `experiments`
were performed. The linked supporting artifacts and reproduction script are
published under `investigations/20261006-prefetch-reads`. Isolated local database
directories and extracted server binaries remain under `/tmp` on the source host
and are private; they are not included in this repository.

## Finding

The low DATA-read measurement is credible. PostgreSQL can replay the fixture's
VACUUM using full-page images (FPIs) from WAL, without reading the previous heap
page contents. Recovery prefetch can nevertheless read those old pages: its
lookahead encounters a later non-FPI reference before redo has installed the
earlier FPI for the same page. PostgreSQL 16.15 skips the FPI reference but does
not use that skipped reference to suppress the later speculative read.

This mechanism explains the direction and approximate magnitude of the Azure
measurements. The controlled local reproduction below tests the mechanism.
The completed Azure runs themselves were cross-region and did not capture
baseline prefetch counters or retain a WAL decode, so an exact attribution of
every baseline device read remains unavailable.

The baseline's approximately 15.8x replica/primary DATA read-operation ratio
does not mean each heap page was read fifteen times. Nearly equal read-byte
totals, with approximately 128-KiB primary requests versus 8-KiB replica
requests, explain that ratio. The replica's total read volume is consistent
with approximately two heap passes; severe repeated cache thrashing is not
required to explain the aggregate measurements. The distinction and its
limits are developed below.

## What was actually the same, and what differed

The comparison uses baseline `20261005T062051Z` and prefetch-off
`attempt-westus3/results/20261005T072928Z-prefetch-off`.

Both used PostgreSQL 16.15 with the same container image, retained source system
identifier, fixture construction, separate primary/standby DATA and WAL disks,
512-GiB Standard_LRS HDD disks, and host caching disabled. Both executed
`VACUUM (VERBOSE, FREEZE) public.vacuum_target`, without an application workload.
Among the captured preflight GUCs, the only difference is standby
`recovery_prefetch=try` versus `off`; both primaries remained at `try`.

The infrastructure was not identical: baseline was East US, off was West US 3
and used a regional snapshot copy. Each run had its own nodes, disks, startup,
cache history, and timing. These limit claims about Azure latency causality.

| Measurement | Baseline (`try`) | Prefetch off |
|---|---:|---:|
| Heap bytes | 24,696,061,952 | 24,696,061,952 |
| Heap pages scanned | 3,014,656 | 3,014,656 |
| Live tuples remaining | 7,536,640 | 7,536,640 |
| Tuples removed by this VACUUM | 7,536,638 | 7,536,638 |
| Index pages | 41,332 | 41,332 |
| VACUUM backend WAL records | 15,115,207 | 15,115,207 |
| VACUUM backend FPIs | 6,071,356 | 6,071,356 |
| VACUUM backend WAL bytes | 36,932,302,644 | 36,932,302,644 |
| Primary DATA reads, entire observed timeline | 49.231 GB | 49.216 GB |
| Standby DATA reads, entire observed timeline | 48.757 GB | 14.434 MB |
| Standby completed DATA read operations, estimated | 5,950,398 | 336 |
| Standby DATA writes, entire observed timeline | 49.560 GB | 49.531 GB |
| Standby WAL-device reads, entire observed timeline | 35.644 GB | 0.423 GB |
| Standby mean DATA read size | 8,193.95 bytes | 42,960.52 bytes |
| VACUUM command duration | 1,942.02 s | 1,545.34 s |
| VACUUM start through verified catch-up | 11,069.39 s | 1,558.17 s |

Units here are decimal GB/MB. Operation counts use boundary-prorated counter
deltas; they are not counts of SQL reads or distinct PostgreSQL pages.
Both runs completed VACUUM and verified replay through the final LSN. The off
raw-counter audit found 339 DATA reads in the slightly wider captured span,
approximately 336 inside the exact measurement window, stable device identity,
monotonic counters, and a separate WAL device. This is not a DATA/WAL mapping
mistake or a premature end of replay.

The backend WAL totals above are more precise for comparing VACUUM than
`pg_stat_wal` before/after deltas, which also include background WAL. Those
deltas differ by only 700 bytes and 14 records; their FPI counts also match.
Aggregate equality does not mean the two WAL streams are byte-for-byte identical.

Evidence: [baseline VACUUM log](vacuum-only-repro/experiments-20261005-five/runs/baseline/results/20261005T062051Z/vacuum.log),
[off VACUUM log](vacuum-only-repro/experiments-20261005-five/runs/prefetch-off/results/20261005T072928Z-prefetch-off/vacuum.log),
[off raw-counter recount](vacuum-only-repro/experiments-20261005-five/runs/prefetch-off/results/20261005T072928Z-prefetch-off/root-prefetch-read-count-recheck.json),
and [independent comparison with input hashes](investigations/20261006-prefetch-reads/artifact-comparison.json).
Recompute the comparison with `python3 investigations/20261006-prefetch-reads/audit-artifacts.py`; the script writes
only to stdout.

## Data layout and primary VACUUM work

The fixture is `id bigint, payload bytea`, with `payload SET STORAGE PLAIN` and
1,536-byte payloads. Preparation copies source rows in ID order and emits
adjacent `(2*id,payload)` / `(2*id+1,payload)` pairs, builds a B-tree on `id`, and
deletes odd IDs. The intended initial population is 15,073,280 tuples, half
live and half deleted. The payload stays inline; the VACUUM log confirms that
the TOAST table has zero pages. The index stores keys and heap TIDs, not the
payload. [Fixture script](vacuum-only-repro/experiments-20261005-five/runs/baseline/source/as-run/02-prepare.sh).

On this 64-bit, 8-KiB-page build, each non-null tuple has a 24-byte aligned
header, 8-byte ID, and 1,540-byte bytea (4-byte varlena header plus payload):
1,572 bytes of tuple data, occupying 1,576 aligned bytes. Including its 4-byte
line pointer costs 1,580 bytes. Five tuples fit per heap page; six do not.
An initially full page has 24 bytes of page header, 20 bytes of line pointers,
7,880 bytes of aligned tuple storage, and a 268-byte free-space hole.
Alternating deletion leaves two or three live tuples on every page; the file
does not become half-sized. See [PostgreSQL page layout](https://www.postgresql.org/docs/16/storage-page-layout.html).

The two-tuple difference between the intended dead population and VACUUM's
removed count is consistent with the preparation script's indexed payload
sample on the first heap page opportunistically pruning its two deleted tuple
bodies before measurement. That still leaves their dead line pointers/index
entries for VACUUM. This is a source-based explanation of the small discrepancy,
not a captured pre-VACUUM page inspection of the Azure fixture.

VACUUM first scans the heap, decides tuple visibility, prunes deleted tuple
bodies, freezes survivors, and remembers dead TIDs. Because the B-tree can
still point at those TIDs, pruning leaves `LP_DEAD` line pointers. The index
cleanup removes those entries; a later heap pass changes the pointers to
`LP_UNUSED` and sets visibility-map all-visible/all-frozen bits. Every heap page
participates in both passes. No heap pages were truncated. These operations
are visible in [`pruneheap.c`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/access/heap/pruneheap.c)
and [`vacuumlazy.c`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/access/heap/vacuumlazy.c).

Primary reads and writes are therefore approximately two full heap passes plus
index and metadata work, about 49 GB each. Its approximately 6.071 million
shared-buffer misses were serviced by far fewer completed device reads,
averaging about 130.6 KB each, because reads at the filesystem/block layer can
be larger than one PostgreSQL page. The reported 15.8x standby/primary
operation ratio in baseline mostly reflects approximately 8-KiB versus 128-KiB
requests; the DATA read-byte ratio is approximately 0.990x.

## Why 15.8x more read operations does not imply fifteen rereads per page

The reported amplification counts completed guest block-device operations,
not PostgreSQL page accesses or distinct heap blocks. A single approximately
128-KiB device read can supply sixteen 8-KiB PostgreSQL pages; sixteen separate
8-KiB device reads transfer the same bytes but count as sixteen operations.
Filesystem readahead and block-layer merging can affect this distinction;
the recorded mean request sizes do not identify the exact contribution of
each mechanism. Linux exposes read-ahead, request-size, scheduler, and merge
controls, so the same PostgreSQL prefetch pattern need not produce the same
guest device-request pattern on every system.
[Linux block-queue controls](https://docs.kernel.org/5.15/block/queue-sysfs.html).

For the baseline's headline comparison, the primary window is the VACUUM
command and the replica window is VACUUM start through verified catch-up:

| DATA read measurement | Primary VACUUM | Replica through catch-up |
|---|---:|---:|
| Estimated completed device operations | 376,881 | 5,950,398 |
| Read bytes | approximately 49.21 GB | approximately 48.76 GB |
| Effective mean bytes per operation | approximately 127.5 KiB | approximately 8.0 KiB |

For those windows, the arithmetic is:

```text
Replica/primary operation ratio
  = (replica read bytes / primary read bytes)
    * (primary mean request size / replica mean request size)
  ~= 0.991 * (127.5 / 8.0)
  ~= 15.8
```

The whole-timeline primary total in the earlier table is slightly larger
because it includes incidental reads after VACUUM. That window difference
does not change the interpretation. Evidence: [baseline summary](vacuum-only-repro/experiments-20261005-five/runs/baseline/results/20261005T062051Z/summary.json)
and [headline operation-count calculation](vacuum-only-repro/experiments-20261005-five/runs/baseline/results/20261005T062051Z/estimated-data-read-operations.json).

There are two separate cache questions. A page already in PostgreSQL shared
buffers is normally recognized by prefetch, so nearby subsequent WAL records
can reuse it. But PostgreSQL 16.15 prefetch does not allocate or populate a
shared buffer on a miss: it asks Linux to populate the filesystem page cache
through `POSIX_FADV_WILLNEED`. Lookahead can therefore request an old page
before redo has installed an earlier WAL image of that page in shared buffers.
The later delta can then hit the image-restored buffer even though a redundant
device read was already requested. This is an ordering problem, not evidence
that adjacent records repeatedly evict the same page.
See [shared-buffer prefetch](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/storage/buffer/bufmgr.c#L485)
and [filesystem prefetch](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/storage/file/fd.c).

Eviction between the two widely separated heap passes is nevertheless
expected: the 23-GiB heap is about 92 times the 256-MiB shared-buffer pool,
which also serves index and other pages. The plausible WAL sequence described
below offers an image-then-delta prefetch opportunity on both passes. A page
evicted after its first-pass records can encounter the same unnecessary-read
opportunity on the later pass. Whether that second request reaches the device
also depends on filesystem-cache residency; shared-buffer eviction alone does
not prove physical I/O.

Two 8-KiB reads per heap page predict 6,029,312 reads. The measured approximately
5,950,398 reads are 98.7% of that prediction, or approximately 1.97 operations
per heap page across the full replay interval. This is consistent with roughly
one read per page per pass and does not require fifteen rereads of each page.
The counts include index and other DATA activity, and no per-block Azure read
trace was retained, so this is an aggregate explanation rather than proof of
a uniform two-read history. Uneven rereading or additional cache churn cannot
be excluded.

The important cost remains: with prefetch enabled, the replica issued millions
of small DATA reads and transferred nearly 49 GB of old page contents that
FPI-based replay could largely avoid. Request size explains the 15.8x ratio
against the primary; unnecessary lookahead reads explain why the replica had
that DATA-read stream at all. These are distinct effects.

## IOPS ceilings, replay delay, and application availability

Nearly equal byte totals do not imply nearly equal storage cost. On storage
limited by operations per second, splitting the same bytes into approximately
sixteen times as many requests can sharply reduce useful throughput. As an
illustrative model, a remaining read budget of 500 operations/s permits about
4.1 MB/s with 8-KiB requests versus 65.5 MB/s with 128-KiB requests, assuming
no tighter bandwidth limit. Writes, WAL traffic, queueing, and other activity
also consume capacity, so this is not a prediction of an exact sixteenfold
runtime difference. Azure documents both disk and VM IOPS/throughput ceilings
and increased latency when demand exceeds them.
[Azure disk and VM performance limits](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-performance#disk-io-capping).

The impact can vary substantially across environments. Linux filesystem
read-ahead, page-cache hits, and block-layer request merging can satisfy
several nearby page references with fewer or larger device reads. Depending
on the storage implementation, hypervisor/backend caching or coalescing,
storage-controller or adapter firmware, and drive read-ahead/cache behavior
may also reduce the physical work or latency associated with small guest
requests. These are possible mitigating mechanisms, not mechanisms measured
in these experiments. Their effectiveness depends on access locality, request
timing and queue depth, cache state, and the particular storage stack.

The layer at which IOPS are counted or capped also matters. Coalescing before
a limiting layer can reduce the operation demand seen there; coalescing or
cache hits after that layer may reduce backend work without removing the
requests already counted against its limit. PostgreSQL prefetch calls, guest
completed block operations, hypervisor/backend operations, and physical drive
accesses are therefore not interchangeable counts. The observed approximately
8-KiB replica mean shows that the guest completed-operation counters retained
a small-request pattern; it does not establish a one-to-one mapping to physical
reads beneath the virtual disk. These tests did not trace those lower layers.
The measured 15.8x guest operation ratio and replay times characterize this
setup, while another environment could mitigate the same PostgreSQL behavior
to a substantially different degree.

The measurements illustrate the operational severity. Baseline replica DATA
reads averaged approximately 538 IOPS but only 4.40 MB/s, and full catch-up
took 3h 04m 29s, including a 2h 32m 07s tail after VACUUM finished. The
prefetch-off run caught up in 25m 58s with only a 13-second tail: approximately
7.1 times less total elapsed time. The 100-GiB default-prefetch run retained
approximately the same replica DATA read rate and needed 13h 29m 55s through
catch-up. These are measured elapsed times, not proof that a particular Azure
IOPS cap caused all of the delay; the on/off pair's region and cache differences
remain confounds. The 15.8x operation ratio should not be presented as a
measured 15.8x replay slowdown.
See [baseline](vacuum-only-repro/experiments-20261005-five/runs/baseline/REPORT.md),
[prefetch off](vacuum-only-repro/experiments-20261005-five/runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md),
and [100-GiB run](vacuum-only-repro/experiments-20261005-five/runs/100gib/results/100gib-20261005T130618Z/REPORT.md).

Synchronous commit waits depend on the configured acknowledgement stage:

- With required synchronous standbys and `synchronous_commit=remote_apply`,
  a commit waits for its WAL to be replayed there. A transaction whose commit
  record follows the backlog can wait behind that backlog. These tests did not measure client commits under this setting.
- With required synchronous standbys and `synchronous_commit=on`, a commit
  waits for remote WAL flush, not replay. Replay lag alone therefore does not
  imply a flush-based commit wait. Storage contention can affect commits if it
  stalls WAL receipt, flush, or acknowledgement, or leaves insufficient
  responsive synchronous standbys to satisfy the required count. Separate
  DATA/WAL disks do not remove shared VM storage limits.

These distinctions follow PostgreSQL's [synchronous commit semantics](https://www.postgresql.org/docs/16/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT).
The baseline and prefetch-off primary preflight captures actually recorded
`synchronous_commit=on` and `synchronous_standby_names=ANY 1 ("hdd-pg-2")`:
these were flush-based synchronous configurations, not `remote_apply` tests.
There was no application workload, so these runs do not measure client
commit latency.
[Baseline preflight](vacuum-only-repro/experiments-20261005-five/runs/baseline/results/20261005T062051Z/preflight.json)
and [prefetch-off preflight](vacuum-only-repro/experiments-20261005-five/runs/prefetch-off/results/20261005T072928Z-prefetch-off/preflight.json).

## Premium SSD v2 follow-up measurements

Three additional VACUUM-only runs completed on October 7, 2026, using 23-,
50-, and 100-GiB heaps. Each primary and replica had separate 512-GiB
Premium SSD v2 DATA/WAL disks configured at the minimum 3,000 IOPS and
125 MB/s, with host caching disabled. All measured disk copies passed a
100% hydration gate, and the two owned temporary Standard staging disks
were deleted before measurement. All three VACUUM commands exited 0,
replicas caught up through both the completion LSN and current primary
flush LSN, and sample coverage passed.

| Heap | Primary / replica DATA read GiB | Replica / primary read operations | VACUUM duration | Start through replica catch-up |
|---|---:|---:|---:|---:|
| 23 GiB | 45.85 / 45.47 | 15.78x | 14m 04s | 33m 00s |
| 50 GiB | 100.94 / 99.50 | 15.68x | 30m 31s | 1h 12m 08s |
| 100 GiB | 203.65 / 199.60 | 15.60x | 1h 58m 27s | 3h 36m 15s |

These whole-observation totals reproduce the operation/byte distinction:
mean primary DATA reads were approximately 127 KiB, while replica reads
averaged approximately 8 KiB. Replica DATA bytes were 98.0–99.2% of primary
bytes and about two prepared-heap equivalents. They do not establish fifteen
rereads of each page or individual-page cache histories. The full operation
ratios include replay after VACUUM: approximately 57.0%, 57.6%, and 49.9% of
replica DATA read operations occurred in that drain for the three sizes.
During VACUUM alone, the operation ratios were 6.79x, 6.65x, and 7.83x.

All runs used `recovery_prefetch=try`; these new runs have no matched
prefetch-off control. They used Standard_D2s_v4 workers, and the 100-GiB
run's guest storage rates changed substantially during execution, so its
duration should not be attributed solely to fixture size or a particular
disk IOPS limit. The 50-GiB stack used West US 3, while the others used East
US; OS caches were not explicitly dropped. No application workload or
commit-latency measurement was present. The earlier synchronous-commit
availability distinction and environment-specific coalescing caveats still
apply.

The [comparison report](vacuum-only-repro/experiments-20261007-premium-v2/REPORT.md) provides
separate phase totals and linear-axis latency, IOPS, and throughput plots.
Each standalone report embeds the complete end-to-end timeline, including
snapshot creation, preparation/recovery attempts, hydration, VACUUM,
replica catch-up, required WAL preservation, and cleanup:
[23 GiB](vacuum-only-repro/experiments-20261007-premium-v2/baseline-23gib/results/REPORT.md),
[50 GiB](vacuum-only-repro/experiments-20261007-premium-v2/50gib/results/REPORT.md), and
[100 GiB](vacuum-only-repro/experiments-20261007-premium-v2/100gib/results/REPORT.md).
Exact resource events, first-observed copy completion, and inherited source
metadata are labeled separately.

## Snapshot baseline with a tiny application workload

The October 9, 2026 repeat restored the same 23-GiB fixture from retained
paired snapshots, then seeded the replica from a fresh offline paired snapshot
backup. All four Premium SSD v2 DATA/WAL disks reached 100% hydration before
measurement. Both isolated workers had Linux page cache, dentries, and inodes
cleared before client setup. Disk settings stayed at 3,000 IOPS and 125 MB/s;
the two workers remained Standard_D2s_v4 instances in East US zone 1.

The added workload was deliberately small: pgbench offered an expected Poisson
rate of 5 TPS across ten persistent connections, each updating its own row in a
ten-row logged table. Sessions retained the baseline's `synchronous_commit=on`.
The client ran for five healthy minutes before VACUUM, throughout VACUUM and
replica catch-up, and for five healthy minutes after the replica reached the
VACUUM completion LSN.

| Phase | Completed TPS | p95 end-to-end latency | p95 execution latency including commit wait |
|---|---:|---:|---:|
| Healthy warm-up | 4.917 | 6.498 ms | 6.237 ms |
| VACUUM | 4.992 | 459.227 ms | 434.225 ms |
| Replica catch-up | 5.094 | 53.398 ms | 46.299 ms |
| Healthy recovery window | 5.070 | 7.595 ms | 7.153 ms |

VACUUM raised p95 end-to-end latency about **71x**. Its p99 was 951.539 ms and
maximum was 3.562 seconds, including pgbench scheduling delay; execution
latency alone reached 1.548 seconds. The one-second session sampler recorded
`SyncRep` waits and pending transactions up to 1.397 seconds old. Nevertheless,
completed throughput remained near the offered 5 TPS. No full ten-second
zero-completion bucket occurred. All 13,176 native logged completions matched
the final database update counter, with zero client failures.

VACUUM finished in **15m 43s**. The replica reached its completion LSN in
**33m 25s from VACUUM start**, with a peak observed replay backlog of
**12.907 GiB**. Final replay was verified after the recovery window and client
drain. This run illustrates substantial client latency under synchronous WAL
flush acknowledgement while also showing that a large replay backlog need not
establish a flush-based commit delay with `on`.

The read-operation distinction repeated: primary/replica DATA reads were
**46.29 / 45.99 GiB**, with **15.809x** as many replica read operations and
**0.994x** the read bytes. Mean read request sizes were **127.32 / 8.00 KiB**.
These are guest block counters; they do not count individual block identities
or separately billed Azure transactions. A matched prefetch-off client control
was not run, and the client was colocated with the primary, so this repeat does
not isolate prefetch as the cause of its latency increase.

Azure Monitor additionally reported 100% VM uncached bandwidth utilization on
both workers and 100% VM uncached IOPS utilization on the replica during the
captured VACUUM minutes. The live VM SKU exposes aggregate storage limits as
well as the disks' own limits. Guest throughput counters exceed the SKU's
published bandwidth figure; that measurement-layer discrepancy remains
unresolved. The [storage review](vacuum-only-repro/experiments-20261009-pgbench-baseline/verification/azure-d2sv4-storage-review.md)
retains raw Azure evidence and documents this qualification.

The [complete report](vacuum-only-repro/experiments-20261009-pgbench-baseline/run/results/REPORT.md)
includes the end-to-end UTC timeline, snapshot creation and hydration,
preparation retries, cache conditioning, client startup/drain, VACUUM,
recovery, and cleanup. The [combined client/replication graph](vacuum-only-repro/experiments-20261009-pgbench-baseline/run/results/client-replication.png)
shows completed TPS, execution latency, standby flush backlog, and replica
replay backlog on linear axes.

Owned test compute, managed disks, and temporary Blob storage were removed
after measurement. The final audit preserved all **44 snapshots** and all
**48 protection locks**. The regional Network Watcher and two exact source
Reader grants remain: the watcher has possible shared use and a changed ETag,
and the grants are protected by snapshot locks for the removed AKS principal.

## WAL images, hint bits, and subsequent delta records

An important setting omitted from the run's selected GUC capture is
`wal_log_hints`. Saved earlier snapshot control metadata for the same source
system identifier explicitly records `wal_log_hints=on`, checksum version 0,
8-KiB pages, and 8-byte maximum alignment. This is direct evidence of the source
state, rather than a live query of a running experiment. CloudNativePG 1.27
also documents `wal_log_hints=on` as a default. [Saved control metadata excerpt](investigations/20261006-prefetch-reads/source-snapshot-control-metadata.md),
[CNPG configuration](https://cloudnative-pg.io/docs/1.27/postgresql_conf/).
The selected measurement-time GUC capture did not include `wal_log_hints`;
the source capture and operator default strongly support `on`, but are not a
direct measurement of that GUC during these runs.

When VACUUM tests whether deleted tuples are removable, it can set their commit
hint bits. With hint WAL logging enabled, the first such modification after
a checkpoint can emit `FPI_FOR_HINT`, before pruning removes tuple bodies.
That image therefore includes the original five tuple bodies. Later PRUNE and
FREEZE_PAGE records describe changes to that same page without another image.
[`MarkBufferDirtyHint`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/storage/buffer/bufmgr.c)
and [`XLogSaveBufferForHint`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/access/transam/xloginsert.c)
implement this path.

Both Azure primary logs contain 67 completed checkpoints, mostly triggered by
WAL volume with `max_wal_size=1GB`. By the second heap pass, earlier pages have
crossed checkpoint boundaries and left the small buffer working set. The
HEAP2/VACUUM modification can carry another FPI, now containing the compacted
two or three survivors; HEAP2/VISIBLE subsequently references the heap page
without an image and the visibility-map fork. The latter maintains bits used
by future scans and index-only scans.
[`log_heap_visible`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/access/heap/heapam.c)
registers those separate block references. The prefetcher currently considers
the main fork, not the VM/FSM forks.

This produces a plausible dominant five-record, two-image sequence per heap
page: FPI_FOR_HINT, PRUNE, FREEZE_PAGE, then VACUUM-with-FPI and VISIBLE.
Five times 3,014,656 is 15,073,280 records, leaving 41,927 of the measured
15,115,207 records for index/other work. Two times the heap page count is
6,029,312 images, leaving 42,044 images for index/other work. The original Azure
WAL was not retained, so this decomposition is an inference supported by
source behavior and the local WAL decode, not a per-record Azure measurement.

The relevant WAL content is physical page state and instructions, not a
`VACUUM` SQL command to execute on the replica:

| Record | Relevant contents when a delta is logged |
|---|---|
| FPI_FOR_HINT | Page image, including tuple headers and inline payload bytes; standard-page free-space hole can be omitted |
| HEAP2/PRUNE | Snapshot-conflict horizon, catalog flag, counts and offset arrays identifying redirects/dead/unused items |
| HEAP2/FREEZE_PAGE | Conflict horizon, catalog flag, deduplicated freeze plans (`xmax`, infomasks, freeze flags), and tuple-offset arrays |
| HEAP2/VACUUM | Number and offsets of dead line pointers to mark unused |
| HEAP2/VISIBLE | Conflict horizon, visibility flags, separate VM and heap block references |

When an operation's FPI already contains the required resulting page state,
its per-block delta payload can be omitted. These layouts are defined in
[`heapam_xlog.h`](https://github.com/postgres/postgres/blob/REL_16_15/src/include/access/heapam_xlog.h).
`wal_level=logical` does not make this standby re-execute SQL or scan the table;
the CNPG standby uses physical WAL replay.

## Why `try` can add reads that `off` avoids

Consider a page absent from shared buffers, with the following WAL order:

```text
LSN A: full-page image of P
LSN B: delta modifying P
```

With prefetch off, redo consumes A, acquires a zero-and-lock buffer, restores P
from WAL, and dirties it. No old-page read is needed. Redo of B then finds P in
shared buffers and applies the delta. The function's name
`XLogReadBufferForRedoExtended` does not mean it always reads from storage;
its applicable-image branch returns `BLK_RESTORED`.
[`xlogutils.c`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/access/transam/xlogutils.c).

With prefetch enabled, WAL lookahead reaches B before redo executes A. It skips
A's image-bearing reference and returns **before updating its recent-block
deduplication window**. For B it checks the current shared-buffer table; P is
not there yet. The prefetcher requests a read of the existing disk page.
It tracks recent prefetch candidates, but not the future buffer contents that
will be installed by skipped FPIs. `maintenance_io_concurrency=10` permits a
nominal ten prefetch I/Os and up to forty block-reference queue entries, enough
to see the adjacent records. This is directly visible in
[`xlogprefetcher.c`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/access/transam/xlogprefetcher.c#L676).

The request goes through `PrefetchSharedBuffer`, `smgrprefetch`, and
`mdprefetch` to `FilePrefetch`: Linux receives
`posix_fadvise(fd, offset, 8192, POSIX_FADV_WILLNEED)`. It requests kernel
page-cache population; it does not load a PostgreSQL shared buffer. Redo can
still restore P from WAL and never consume that old disk content. Thus skipping
FPI records does **not** imply avoiding all unnecessary reads associated with
FPI-covered pages. [Buffer lookup](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/storage/buffer/bufmgr.c#L511),
[8-KiB request](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/storage/smgr/md.c#L724),
[Linux advice call](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/storage/file/fd.c#L2023).

Whether the request becomes device I/O depends on kernel cache residency and
timing. Once an FPI has been replayed, later references can hit shared buffers;
other close references are deduplicated. Those effects prevent an exact
one-prefetch-per-page prediction.

Two speculative 8-KiB reads per heap page predict 6,029,312 reads and
49,392,123,904 bytes. Baseline measured about 5,950,398 reads and
48,757,269,264 bytes: **98.7% of that prediction**. Off still wrote about
49.53 GB of DATA. It did the physical replay work while largely obtaining page
contents from WAL rather than old DATA pages.

The nominal prefetch queue is LSN-based: it treats earlier requests as completed
once redo has passed their records. It is not a measurement of outstanding
Linux device requests. The unnecessary-read path can invalidate the assumption
that passing a record means waiting for its read. This supports a possible
queueing effect, but does not independently establish the Azure latency cause.

WAL-device reads differ as well: 35.64 GB in baseline versus 0.423 GB with
prefetch off. Both arms still decode approximately the same 36.93 GB of
generated WAL. Reads of WAL already in the kernel page cache do not increment
the device counters. Baseline's long replay backlog allows received WAL to
leave cache before redo consumes it; the fast off replay can consume it near
arrival. That is a plausible secondary cache/timing effect, rather than a
different WAL workload. Cache residency was not instrumented, so the exact
share of the WAL-device difference attributable to this effect is unmeasured.

## Local reproduction

An isolated PostgreSQL 16.15 fixture with 1,000 rows occupies exactly 200 heap
pages. After alternating deletion, VACUUM removes 500 tuples, freezes 500
survivors, and keeps all 200 pages. It emits 1,008 records, 207 FPIs, and
1,686,662 WAL bytes for the main table. The same inline layout was confirmed
with `pageinspect`: five 1,572-byte tuples per initial page.

The decoded WAL for main-fork heap block 0 is:

| LSN | Record | Image and contents |
|---|---|---|
| `0/01905470` | FPI_FOR_HINT | 7,973-byte record; 7,924-byte page image, omitting the 268-byte free-space hole |
| `0/019073B0` | PRUNE | 59-byte record, no image; dead line pointers `[2,4]`, conflict horizon 734 |
| `0/019073F0` | FREEZE_PAGE | 71-byte record, no image; survivor offsets `[1,3,5]`, conflict horizon 732 |
| `0/01A95108` | VACUUM | 52-byte record, no image; unused line pointers `[2,4]` |
| `0/01A95140` | VISIBLE | Heap block 0 has no image; **VM fork** block 0 has a separate image; visibility flags `0x03` |

This directly confirms the first-pass image-then-delta opportunity. The tiny
VACUUM finishes without an intervening checkpoint, so its second pass does
not emit another main-fork image. It does not duplicate the large run's
checkpoint history. Afterward the surviving line pointers are normal, the
deleted pointers are unused, and survivors have infomask `0xb02`; all 200
pages are all-visible. Freeze changes transaction-status header flags and
does not rewrite or compress the payload.

The local server uses `wal_level=replica`, `wal_log_hints=on`, checksums enabled,
16-MB shared buffers, and a long checkpoint timeout. The payload is a constant
byte pattern with the same length and PLAIN storage, and WAL compression is off.
These deliberate simplifications preserve the image/delta and page-packing
mechanism. They do not duplicate CNPG's logical WAL level, source checksum
version 0, 256-MB buffer pool, random payload contents, or repeated checkpoints.
Both `wal_log_hints` and checksums can enable the hint-image protection path;
the Azure source's captured `wal_log_hints=on` supplies that protection there.

Two copies of the same pre-VACUUM database replayed the same WAL to the same
named restore point. Root independently reran the finished script on a fresh
fixture and verified both copies at LSN `0/1AA5308`. Only the recovery-prefetch setting differed
between the replay arms (their socket paths/ports were necessarily isolated).

| Captured replay statistic | `off` | `try` |
|---|---:|---:|
| `pg_stat_recovery_prefetch.prefetch` | 0 | 200 |
| `hit` | 0 | 201 |
| `skip_fpw` | 0 | 206 |
| `skip_rep` | 0 | 402 |
| `pg_stat_io` startup-process relation reads after stats flush | 1 | 1 |
| Startup relation-buffer hits after stats flush | 1,602 | 1,401 |
| Final restore-point LSN in independent fresh verification | `0/1AA5308` | `0/1AA5308` |

Enabled prefetch issued exactly one request per heap page even though the
ordinary startup-process relation read count was the same in both arms.
The WAL ordering and
source path explain why: the PRUNE/FREEZE candidates were inspected before the
preceding HINT images had installed the pages in shared buffers. This
establishes that the setting alone can introduce the unnecessary prefetches.
The `prefetch` statistic counts initiated requests, not completed device reads.

Statistics timing matters here: while recovery was paused, the shared
`pg_stat_io` view initially showed zero reads because the startup process's
pending counters had not been flushed. In the independent fresh run, each copy
was promoted after capturing the common endpoint. After the startup processes
exited and published their statistics, both showed one 8-KiB relation read,
with nonzero hit counters confirming publication. The zero paused values must
not be used as proof that all ordinary reads were absent. A preliminary run
with an extra restart had two cumulative ordinary reads in each arm; that
history is superseded for this comparison by the fresh verification.

The local files are small and kernel cache warmth differs between arms. No
global cache drop was performed. Startup-process `/proc/io` counters were
captured, but they are not used to claim an on/off physical-device difference:
the first, off arm recorded 28,672 physical read bytes; the later, try arm
recorded zero. That cache-dependent counter does not negate the 200 speculative
requests and does not reproduce the large Azure disks' cold-read behavior.

Reproduction commands: [LOCAL-REPRO.md](investigations/20261006-prefetch-reads/LOCAL-REPRO.md) and
[repro-local.sh](investigations/20261006-prefetch-reads/repro-local.sh). The independently executed script's
[verification](investigations/20261006-prefetch-reads/root-validation/verification.json),
[off prefetch statistics](investigations/20261006-prefetch-reads/root-validation/replay-off-stats.json),
[try prefetch statistics](investigations/20261006-prefetch-reads/root-validation/replay-try-stats.json),
[off flushed I/O statistics](investigations/20261006-prefetch-reads/root-validation/replay-off-startup-io-after-promotion.json),
[try flushed I/O statistics](investigations/20261006-prefetch-reads/root-validation/replay-try-startup-io-after-promotion.json),
and [decoded WAL](investigations/20261006-prefetch-reads/root-validation/page0-main-wal.txt) are preserved under `investigations/20261006-prefetch-reads`.
Both sets of isolated local postmasters were stopped. Local database files
were retained in `/tmp` on the source host; those private database directories
are not included here. No experiment or snapshot cleanup was performed.

## Related PostgreSQL prefetch research

Read on 2026-10-06, including thread replies and the first two patch attachments.
These establish prior upstream reports of the same prefetch bookkeeping defect.

Michael Zhilin's [March 2025 report](https://www.postgresql.org/message-id/9bdb428a-fc8e-4801-8fff-1ad0bc6fa76b@postgrespro.ru)
describes hours of standby lag resolved by disabling recovery prefetch. Its
fixture produced 32,383 prefetches and nine ordinary relation reads. Remembering
image-bearing references in the recent-block window reduced prefetches to zero.
Our independent small-fixture result reproduces this distinction between
speculative requests and ordinary reads.

Satyanarayana Narlapuram's [March 2026 proposal](https://www.postgresql.org/message-id/CAHg%2BQDf%3DeFFnOvmaPJQrXNiXaTOhhJhvO%3DDzmVUi%3DVGFRh5B0A%40mail.gmail.com)
extends the same idea to image-bearing and `BKPBLOCK_WILL_INIT` references.
It reports approximately 90% fewer prefetch calls and recovery time falling
from 188.04 to 60.02 seconds on its slower-disk test. The four-entry recent
window remains a limitation; this is not complete tracking of all future
image-installed pages.

A [September 2026 follow-up](https://www.postgresql.org/message-id/d934aa0c-e982-4ef0-a73a-24068b2db7d3@planetscale.com)
reports an IOPS-throttled test taking approximately 18 seconds unpatched,
six with the FPI bookkeeping patch, and 0.6 with all fadvise disabled. Its
[linked analysis](https://www.postgresql.org/message-id/1c9fe4ba-1f49-4116-9c44-add3ed0310e5@planetscale.com)
traces dominant 8-KiB fadvise requests at the device layer, despite larger WAL
read syscalls. This corroborates an IOPS mechanism under controlled storage
constraints. Disabling all fadvise in that experiment is broader than changing
only `recovery_prefetch`.

The [Commitfest entry](https://commitfest.postgresql.org/patch/6634/) lists the
FPI/WILL_INIT patch as Ready for Committer. A fresh read of PostgreSQL
[`REL_16_15`](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/access/transam/xlogprefetcher.c)
and [`master`](https://github.com/postgres/postgres/blob/master/src/backend/access/transam/xlogprefetcher.c)
still shows image and initialization references returning before recent-block
bookkeeping. A proposal, passing review, or target-version field is not evidence
that the fix is present in a released binary.

These discussions strengthen identification of the PostgreSQL issue. They do
not remove the original Azure pair's missing baseline counters and region
confound, or measure the exact contribution to our observed latency.

## The 2018 WAL prefetch discussion

The full [June and July 2018 WAL prefetch thread](https://www.postgresql.org/message-id/flat/49df9cd2-7086-02d0-3f8d-535a32d44c82@postgrespro.ru)
contains an earlier report of the same image and initialization bookkeeping
problem, as well as evidence that prefetch can help other workloads.

### The same unnecessary read mechanism was identified in 2018

On June 27, Konstantin Knizhnik attributed the slowdown of his prototype to
not accounting for initialized pages and not remembering full-page writes.
His revision skipped initialized heap pages and recorded new and image-bearing
pages in its prefetch cache to suppress subsequent requests for those pages.
This is the same principle as the 2025 and 2026 proposals: a page that an earlier
WAL record will reconstruct must count as supplied for lookahead purposes,
even before replay has installed it in shared buffers.
[June 27 explanation](https://www.postgresql.org/message-id/19b3c454-ca4c-bda5-6521-2f893f4451a9@postgrespro.ru).
The 2018 prototype and the PostgreSQL 16.15 implementation have different
architectures; this is evidence of the same design problem, not proof that
PostgreSQL 16.15 inherited that prototype's code.

On July 9, Tomas Vondra explicitly argued that ordinary filesystems with small
pages should skip full-page writes and treat them as already prefetched.
That distinguishes skipping the image's own reference from remembering the
image so that following delta references also avoid fetching the old page.
[July 9 reply](https://www.postgresql.org/message-id/e7116515-1fbe-06ca-a40d-530f8e899126@2ndquadrant.com).
Our local 200-page fixture demonstrates the practical distinction: `try`
initiates 200 prefetch requests, whereas `off` initiates none, even though both
startup processes ultimately report one ordinary relation read.

### Why useful prefetch can improve other workloads

Sean Chittenden separates three states: a page in PostgreSQL shared buffers,
a page only in the filesystem cache, and a page absent from both. Reading ahead
into the filesystem cache primarily helps the last state when replay really
needs the old page for a partial modification. A serial replay process can
otherwise wait for one cold read at a time and underuse storage capable of
servicing many requests concurrently. His implementation deduplicates requests
and bounds lookahead to avoid reading too far ahead.
[Sean's explanation](https://www.postgresql.org/message-id/20180709002659.pwkjqf2xn3o3gqij@joyent.com).

Tomas's July 7 benchmark used a roughly 16-GB pgbench dataset, an 8-GB replica,
and six SSDs in RAID0. During a one-hour workload generating about 90 GB of WAL,
prefetch reduced the ending backlog from about 70 to 30 GB, corresponding to
replay increasing from about 20 to 60 GB/hour. His interpretation was that
longer I/O queues made better use of the array.
[Benchmark and attached measurements](https://www.postgresql.org/message-id/39e22910-4604-3338-8320-741114c6c30e@2ndquadrant.com).
This supports the value of prefetch for required cold reads. It does not
contradict the unnecessary reads in our image-heavy VACUUM fixture or establish
that prefetch should always be disabled on HDDs.

### PostgreSQL reads and filesystem reads are different

Sean reports using compressed ZFS records of 16 KiB, larger than PostgreSQL's
8-KiB page. Modifying half a filesystem record can require reading its old
contents even when PostgreSQL supplies an entire page from WAL. He therefore
prefetched image-bearing pages in that configuration.
[ZFS qualification](https://www.postgresql.org/message-id/20180709002659.pwkjqf2xn3o3gqij@joyent.com).
Tomas acknowledged this as a reason for potentially configurable behavior.
[Response](https://www.postgresql.org/message-id/e7116515-1fbe-06ca-a40d-530f8e899126@2ndquadrant.com).

An applicable FPI eliminates PostgreSQL's need to read the old relation page;
it does not universally eliminate reads caused by filesystem record size,
partial cached writes, or underlying storage. The ZFS example is a separate
storage-layer read-modify-write mechanism, not evidence that our replica had
that configuration. Similarly, a PostgreSQL read syscall, a prefetch request,
a kernel cache hit, and a completed device read are distinct events. Keeping
these layers separate is essential to interpreting the Azure counters and the
local reproduction.

## Limits of the conclusion

The source mechanism, local controlled result, near-two-heap-pass read volume,
8-KiB request size, matching primary work, and unchanged standby write volume
jointly make unnecessary lookahead reads the strongest explanation.
The original baseline lacks `pg_stat_recovery_prefetch` deltas and a retained
record-by-record WAL decode. Region/storage/cache differences prevent assigning
every device read or the entire replay-time difference to this feature.
The near-zero off reads are not evidence of skipped VACUUM work, missing WAL,
or a broken counter. A tiny local test establishes PostgreSQL behavior; it
does not reproduce Azure HDD latency or validate a universal recommendation to
disable prefetch for other workloads.
