<!-- Latest retention policy: preserve every snapshot until explicit user deletion instruction. -->
# Goal and experiment record

## Experiment objective

Measure PostgreSQL VACUUM-driven disk IOPS, throughput, latency, and replica
replay on Azure Standard HDD. Keep wal_level, full_page_writes,
recovery_prefetch, and vacuum_cost_delay at CNPG defaults. Use separate WAL
and DATA disks, let VACUUM finish, and exclude application workloads.
Provide provisioning, preparation, test, and teardown scripts; reuse a
prepared snapshot where available. Publish separate await, IOPS, and
throughput figures with the README, report, and raw measurements.

The latency conclusion follows measurement; it is not a condition imposed on the data.

The follow-up target is a configurable 23-GiB test heap, aiming for a 45–60-minute primary VACUUM, and mandatory replica replay through the VACUUM completion LSN before the test succeeds. Timings for previous real runs must be kept beside the size setting; the smaller-fixture duration remains an estimate until measured.

## Persistent constraints

- Keep exactly one verified large prepared primary-disk snapshot in its separately owned resource group. Verify any replacement before deleting the previous copy.
- Delete every other owned test resource after testing; never delete unrelated resources. Defer shared-resource cleanup until all experiment stacks are gone.
- Up to four Azure stacks may run concurrently; keep ownership, kubeconfig, controllers and state isolated.
- Use Luna xhigh agents for substantial independent work, at most four threads including root. Root owns integration and final verification.
- All graph axes are linear. Current display averages counters into 60-second UTC buckets and uses a chart-only p99 ceiling, preserving numeric peaks and gaps.
- Azure Blob archiving and CNPG paired DATA/WAL snapshot backups are authorized. Validate the replica-seeding path separately, preserving the original keeper.
- Do not ask questions; proceed using reasonable assumptions.

## Progress

- Reused the prepared 118,128,640,000-byte heap (about 110 GiB), with 36.05 million dead and 36.05 million live tuples.
- The earlier separate-disk NVMe run completed in 1 h 54 m 45 s. Replica DATA reads averaged 527 IOPS / 4.33 MB/s / 120 ms await over covered intervals; brief multi-second stalls occurred. That run had a sampling gap and did not observe replica catch-up.
- The current SCSI run used two 2-vCPU D2s_v4 workers, four separate HDDs and the required CNPG defaults. No application workload ran. VACUUM completed with remote exit 0 at 18:21:48 UTC on 2026-10-04, after 4 h 20 m 35 s. It removed 36.05 million tuples, completed four index passes and generated about 176.7 GB of logical WAL.
- Replica DATA reads averaged about 538 IOPS and 4.41 MB/s, with 383 ms mean read await and brief multi-second interval peaks. Primary DATA and WAL rates vary by phase; the report records those measurements.
- A same-region replacement keeper passed independent primary restore, native CNPG paired DATA/WAL snapshot backup and replica seeding. Both PostgreSQL instances reached Ready, streaming and replay catch-up before testing. The validation stack has been deleted.
- The main collector observed a two-minute tail after primary VACUUM completion; the replica had 115.6 GiB left to replay at teardown. Complete replica replay was not measured. Final logs contained zero WAL sender timeouts.
- Main teardown completed with exit 0. Independent Azure inventory verifies that all experiment resources are gone and exactly one verified prepared East US snapshot remains. The older keeper was deleted only after replacement verification and complete stack cleanup; metadata is pinned in snapshot.json.

See [REPORT.md](REPORT.md) for measurements and [README.md](README.md) for the four reproduction actions. The fresh-fixture fallback is implemented but was not live-tested, as the existing prepared snapshot was reusable.

- Follow-up scripts expose `TEST_HEAP_GIB` (default 23 GiB; estimated 54.5 minutes from the measured SCSI baseline), preserve the large keeper and require three healthy replica catch-up polls before test completion. The smaller fixture uses explicit FREEZE to preserve survivor-freezing work; this new duration is not yet measured on Azure.
