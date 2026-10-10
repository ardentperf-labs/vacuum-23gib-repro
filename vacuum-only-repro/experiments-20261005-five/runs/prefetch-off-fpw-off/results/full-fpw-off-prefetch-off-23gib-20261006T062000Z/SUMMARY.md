# Summary: primary FPW off, standby prefetch off

This 23-GiB West US 3 run completed `VACUUM (VERBOSE, FREEZE)` in **29.92 minutes** and verified full replay catch-up after **10.46 more minutes**, for **40.38 minutes** total. Primary `full_page_writes=off`; standby `recovery_prefetch=off`; the other role retained the default setting. No application sessions ran.

Estimated total DATA read operations (primary VACUUM vs. standby from VACUUM start through catch-up) were **373,886 vs. 374,969**, a **1.002896×** standby/primary ratio. Primary/standby DATA read IOPS during VACUUM were **208.270/104.153**. The raw-counter audit validated all 72 device/window metrics; 487 samples had a maximum gap of 7.209s, with three final catch-up samples at the current flush LSN.

VACUUM markers reported 879,363,200 WAL bytes, 15,115,315 records, and zero FPI delta. `wal_log_hints=on` and data checksum version 0 remained unchanged; do not infer a broader cause from one observational run. The prior prefetch-off-only run in the same region showed far fewer standby DATA reads, but separate-run/cache effects remain possible.

See the [full report](REPORT.md), [IOPS](iops.png), [await](await.png), [throughput](throughput.png), [raw-counter audit](root-independent-counter-audit.json), [measurement completion audit](measurement-completion-audit.json), and [log-capture limitation audit](root-completion-log-capture-audit.json). The PostgreSQL logs were retrieved at collector completion; no independent continuous follower ran.

Cleanup completed at 07:09:35 UTC. The main and node resource groups are absent; all stack-created and reused source snapshots remain preserved and locked. See the [cleanup result](../../cleanup/result.json) and [fresh Azure snapshot audit](../../cleanup/root-fresh-azure-audit.json).
