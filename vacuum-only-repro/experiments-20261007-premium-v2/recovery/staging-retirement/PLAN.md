# Premeasurement staging-disk retirement

`retire_staging_disks.py` is read-only by default. After all three preparations
finish, a plan-only run checks the preparation receipts and script hashes, live
resource-group owner tags, both source-stage receipts, the four bound
Premium SSD v2 disks per stack, completed paired CNPG backup/replica snapshots,
and the global snapshot/lock inventory. It writes a concrete local `plan.json`
and does not issue a disk delete. `--apply` repeats the checks, journals each
exact disk identity before issuing its delete, and verifies the resource ID is
absent. It deletes only the 50-GiB and 100-GiB unattached Standard_LRS restore
staging disks. It never deletes or unlocks a snapshot.

The completed global receipt is `preflight/staging-retirement.json`. It is
written only after both stage IDs are absent and the before/after retention
audits show the same snapshot ID/unique-ID set and the same direct/group
`CanNotDelete` protection. Interrupted operations have one durable journal per
stage under this directory. An absent disk without a matching journal proving
that Azure accepted this script's delete request fails closed.

Azure does not expose meaningful background-copy progress for Standard disk
and snapshot copies. A Standard snapshot can be `Available` while its backend
copy continues. Removing the unattached source stages after the four measured
Premium SSD v2 disks have completed their observable copy and the paired backup
and replica are ready removes concurrent stage-disk activity from the measured
test window. It does not establish that every Standard-origin snapshot backend
copy has completed. The live hydration barrier still checks all observable
Premium SSD v2 disk and snapshot copy states immediately before measurement.

Official references:

- [Create a managed disk from a snapshot](https://learn.microsoft.com/en-us/azure/virtual-machines/scripts/create-managed-disk-from-snapshot)
- [Incremental snapshots](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-incremental-snapshots)
- [Instant access snapshots](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-instant-access-snapshots)
- [Managed disks overview](https://learn.microsoft.com/en-us/azure/virtual-machines/managed-disks-overview)
