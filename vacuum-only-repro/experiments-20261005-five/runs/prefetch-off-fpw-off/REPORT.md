# Fifth experiment: primary full_page_writes off, standby recovery_prefetch off

The full measured result is [here](results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/REPORT.md). The top-level summary and all three linear-axis figures are in the same result directory.

This run reuses the preserved prepared West US 3 23-GiB DATA/WAL snapshot pair. It changes the primary `full_page_writes` setting to `off` and keeps standby `recovery_prefetch=off`; other role values remain at defaults. It completed the full VACUUM and replay catch-up.

The root-owned snapshot-preserving cleanup completed; both seed snapshots and the reused source snapshot pair remain protected. The detailed report records the cleanup audit, retained-log capture method, and explicitly notes that no independent continuous follower was launched.
