# Current experiment instructions

- User authorized one snapshot-based 23-GiB baseline repeat on 2026-10-09, adding a tiny logged pgbench workload: 5 TPS total, 10 persistent connections, five-minute before/after intervals. Keep synchronous_commit=on and every other baseline PostgreSQL/storage/worker setting. Measure client impact without assuming stalls.
- Use separate primary/replica DATA/WAL 512-GiB PremiumV2_LRS disks at 3000 IOPS and 125 MB/s, East US zone 1, two Standard_D2s_v4 workers. All measured disks must be fully hydrated before client warm-up/measurement.
- Preserve ALL existing and new snapshots and their protection locks. Avoid indirect deletion through AKS/resource-group cleanup. Ordinary owned test compute/disks may be removed after verified complete results and required snapshot/WAL retention; never delete unrelated resources.
- Use Luna xhigh agents for substantial independent work, max four threads including root. Root owns integration, source review, cloud provisioning authorization and final verification.
- Each final report must include the full end-to-end UTC timeline, including snapshot creation/restore, hydration, client warm-up, VACUUM, replay catch-up, client recovery and cleanup.
- All graphs use linear y axes. Retain raw client/PG/block counters and source hashes.
- Existing experiment archives remain immutable. No Git staging, commits, pushes or PR publication are requested. Private runtime state, backups and credentials must stay private.
