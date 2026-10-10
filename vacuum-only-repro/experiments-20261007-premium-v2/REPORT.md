# Premium SSD v2 VACUUM results

Three separate stacks use the baseline fixture construction and PostgreSQL settings, with 23, 50, and 100 GiB heap targets. Each DATA/WAL disk is a 512-GiB Premium SSD v2 disk provisioned at the minimum 3,000 IOPS and 125 MB/s, with host caching disabled and 512-byte logical sectors.

The 23- and 100-GiB stacks are in East US zone 1. The 50-GiB stack is in West US 3 zone 1 because Azure denied the East US DSv4 quota increase. Region therefore limits comparisons involving the 50-GiB result. All workers are Standard_D2s_v4. Azure SKU metadata advertises 3,200 uncached IOPS and 48 MB/s for this size, below the combined configured disk limits. The [Dsv4 table](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series) labels these columns Premium SSD and has no separate Premium SSD v2 row. Treat the advertised host values as context; this experiment measures the effective path rather than independently proving a Premium-v2-specific VM ceiling. The older Premium SSD burst figures are not assumed to apply here. Supplementary [full-run VM telemetry and guest I/O](recovery/vm-burst-review/MEASUREMENT-SUPPLEMENT.md) is aggregated across each two-node scale set and cannot resolve node-specific peaks.

Measurements require all three preparations to finish, every restored measured Premium SSD v2 disk to report 100% copy completion, and the two temporary Standard staging disks to be retired with all snapshot identities and locks preserved. Azure does not expose reliable copy progress for Standard disks or Standard-origin snapshots; availability alone cannot prove those transfers finished. The gate therefore establishes measured-disk hydration and staging-disk absence, rather than global Azure backend quiescence. See [Azure disk restore monitoring](https://learn.microsoft.com/en-us/azure/virtual-machines/scripts/create-managed-disk-from-snapshot) and [snapshot access semantics](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-instant-access-snapshots).

These are one measured VACUUM run per fixture size. OS page caches are not explicitly dropped; preparation and the pre-observation checkpoint can leave cached pages. The checkpoint and initial replay catch-up finish before the first counter sample. Cache history and the regional difference therefore limit size-only causal conclusions.

| Run | Region | Actual heap GiB | VACUUM | Start through catch-up | Status |
|---|---|---:|---:|---:|---|
| [baseline-23gib](baseline-23gib/results/) | eastus | 23.00 | 0h 14m 04s | 0h 33m 00s | complete with valid coverage |
| [50gib](50gib/results/) | westus3 | 50.00 | 0h 30m 31s | 1h 12m 08s | complete with valid coverage |
| [100gib](100gib/results/) | eastus | 100.00 | 1h 58m 27s | 3h 36m 15s | complete with valid coverage |

Full observation totals include the baseline sample through the final catch-up sample. They come directly from guest block-device counters. These are guest operation counts; backend merging and the cloud provider’s billing or throttling operation counts may differ. Read await is operation-weighted guest block-device time, including queueing; it does not measure Azure service time or application latency.

| Run | Primary DATA reads | Replica DATA reads | Operation ratio | Byte ratio | Primary mean read KiB | Replica mean read KiB | Replica read await ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline-23gib | 377,660 | 5,957,860 | 15.78 | 0.99 | 127.31 | 8.00 | 45.83 |
| 50gib | 831,437 | 13,035,441 | 15.68 | 0.99 | 127.30 | 8.00 | 45.71 |
| 100gib | 1,675,975 | 26,152,406 | 15.60 | 0.98 | 127.41 | 8.00 | 27.61 |

| Run | Primary DATA read GiB | Replica DATA read GiB | Primary heap equivalents | Replica heap equivalents |
|---|---:|---:|---:|---:|
| baseline-23gib | 45.85 | 45.47 | 1.99 | 1.98 |
| 50gib | 100.94 | 99.50 | 2.02 | 1.99 |
| 100gib | 203.65 | 199.60 | 2.04 | 2.00 |

Heap equivalents divide whole DATA-device read bytes by the prepared heap size. They include index and other PostgreSQL reads and do not count repeated reads of individual pages. An operation ratio alone cannot establish cache thrashing: smaller requests can produce many more operations while reading similar total bytes. All three runs retain recovery_prefetch=try; they do not include a matched prefetch-off control and therefore do not isolate prefetch as a causal factor.

For context, the earlier HDD baseline in the [prefetch read investigation](../../PREFETCH-READS-INVESTIGATION.md) had approximately 15.8 times as many replica DATA read operations with nearly equal read bytes: 48.757 versus 49.231 decimal GB. Its approximately 8-KiB replica requests versus 128-KiB primary requests explain that operation ratio without requiring fifteen reads of each page. The replica read volume was about two heap equivalents; aggregate counters cannot establish individual-page read histories. These new runs report direct whole-observation deltas, so exact window boundaries and units must be considered when comparing them with historical boundary-prorated estimates.

Replica replay can continue long after VACUUM exits. The whole-observation operation ratio above therefore includes that replay drain. The table below separates the two periods using the stored phase analysis; counter intervals crossing a boundary are prorated by their overlap duration. Rates and await remain guest block-device measurements.

| Run | Phase | Primary DATA read IOPS | Replica DATA read IOPS | Primary / replica read MB/s | Operation ratio | Byte-rate ratio | Replica read await ms |
|---|---|---:|---:|---:|---:|---:|---:|
| baseline-23gib | VACUUM | 446.20 | 3,031.56 | 58.17 / 24.85 | 6.79 | 0.43 | 36.76 |
| baseline-23gib | Replay after VACUUM | 0.56 | 2,990.46 | 0.07 / 24.50 | 5,299.60 | 332.56 | 52.69 |
| 50gib | VACUUM | 454.02 | 3,017.16 | 59.19 / 24.73 | 6.65 | 0.42 | 37.33 |
| 50gib | Replay after VACUUM | 0.04 | 3,007.09 | 0.01 / 24.64 | 77,035.33 | 4,844.73 | 51.88 |
| 100gib | VACUUM | 235.74 | 1,845.11 | 30.76 / 15.12 | 7.83 | 0.49 | 37.77 |
| 100gib | Replay after VACUUM | 0.04 | 2,221.80 | 0.01 / 18.21 | 57,316.09 | 3,628.43 | 17.39 |

Each run provides raw samples in its own `results/samples.jsonl`, effective configuration and storage identity in `preflight.json`, VACUUM output, continuous PostgreSQL logs, replay boundaries, and separate linear-axis `await.png`, `iops.png`, and `throughput.png` figures. The plot captions identify any clipped peaks; the raw samples preserve those values.

Each standalone report embeds its complete evidence-linked end-to-end UTC timeline: [baseline](baseline-23gib/results/REPORT.md), [50 GiB](50gib/results/REPORT.md), and [100 GiB](100gib/results/REPORT.md). Snapshot creation, preparation retries, disk hydration, VACUUM, replica catch-up, required WAL exports, and cleanup are included. Exact events and first-observed copy states are labeled separately.

No application workload runs in these tests. VACUUM exit 0, healthy replica catch-up through both the completion LSN and current primary flush LSN, and valid sample coverage are required for a completed result. Client latency is not measured.

All runs keep `maintenance_work_mem=64MB`. The available final sample streams record maximum completed index-vacuum counts of baseline-23gib: 1, 50gib: 2, 100gib: 3. PostgreSQL can interrupt its heap scan for index and heap cleanup when its dead-tuple memory fills, then resume the scan. These cycles divide heap work into sections; they do not mean every cycle scans the entire heap. See [PostgreSQL 16 VACUUM progress phases](https://www.postgresql.org/docs/16/progress-reporting.html#VACUUM-PROGRESS-REPORTING). The observed cycle counts and the time-varying storage rates should be considered alongside fixture size when interpreting duration differences.

All snapshots must remain in protected resource groups. Owned experiment compute, ordinary disks, and shared watchers were removed. All 42 snapshots remain protected; identities were checked before and after cleanup. Required online-backup WAL and history files are preserved in the private backup-retention exports; future recovery requires republishing that archive.
