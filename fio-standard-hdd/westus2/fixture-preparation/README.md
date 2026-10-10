# West US 2 prepared HDD fixture

This folder records how the East US prepared PostgreSQL primary-disk snapshot
was copied into West US 2 for the 4 KiB and 1 MiB read-only fio runs. The
regional fixture is a full 512 GiB snapshot in its own keeper resource group;
it is independent of the source snapshot and remains available for later
reproductions.

The verified regional snapshot is `prepared-full`:

```text
/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-fio-hdd-keeper-westus2-168d63b3cf97eb87fa9f88d9/providers/Microsoft.Compute/snapshots/prepared-full
```

It is in `westus2`, is 512 GiB, has `incremental=false`, and reached
`provisioningState=Succeeded` at 2026-10-09 09:54 UTC. The original full source
snapshot remains in its separate East US keeper group:

```text
/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg-az-hdd-repro-snapshot-keeper/providers/Microsoft.Compute/snapshots/azhr-keep-scsi-prepared-7a297601434285b1
```

In the source workspace, experiment controllers consume
`../.state/fio-hdd-westus2-fixture.json` by default. In this archive, the
portable alias is `../fixture.json`; it records the subscription, region,
retained snapshot ID, keeper group, and preparation method.
`artifacts/fixture-manifest.json` is the captured copy from fixture creation;
`artifacts/retained-regional-snapshot.json` contains the verified snapshot
properties.

## Snapshot preservation

The subscription inventory had 44 snapshots before transfer and 45 after.
All 44 original IDs were still present, and the sole new ID was the West US 2
`prepared-full` keeper above. The evidence is in
`artifacts/snapshots-before.json`, `artifacts/snapshots-after-copy.json`, and
`artifacts/complete.json`.

Preserve every snapshot. Do not delete either keeper or snapshots in other
resource groups. This includes any snapshot created by a later retry. The
transport resource group is separate from both keepers; its disk and storage
account can be removed only after both West US 2 fio experiment groups have
been verified gone. `cleanup.py` defaults to a no-change preflight. Its
`--execute` path rechecks ownership, experiment-group absence, snapshot
inventory, and that no snapshot is in the transport group before deleting only
that group. Do not replace those checks with a direct resource-group delete.

## How this copy completed

The source was exported through an unattached, owned East US disk clone. A
private HTTPS-only Standard LRS StorageV2 account and private `vhds` container
in West US 2 received the page-blob VHD. The initial Azure asynchronous blob
copy failed while reading the source after the export access grant changed;
regranting access while that copy was still active invalidated its source
authorization. The original copy then reported `failed` at about 159 GB (148 GiB).

After the failed copy was confirmed, the final attempt used AzCopy from a fresh
read-only managed-disk export SAS to a distinct page blob. The script waited a
minute for the grant to propagate and sanitized its console stream; SAS values
were not saved to the artifacts. AzCopy reported one completed transfer, no
failed or skipped transfers, and 549,755,814,400 logical bytes transferred
(512 GiB plus the VHD footer). The run took about 8.5 minutes. The blob's Azure
copy-status field was empty, as expected for this AzCopy page-write path; the
completion marker and exact page-blob length, rather than that field, record
success. See `artifacts/fast-copy-complete.json`,
`artifacts/adopted-fast-copy.json`, and the sanitized `artifacts/fast-copy.log`.

`prepare.py` is the fixture-preparation entry point. For a new transfer, it
validates the source manifest and full East US snapshot, creates the tagged
transport and keeper groups, clones the source disk, and starts one resumable
Azure blob copy. It polls that copy, revokes export access after completion,
then imports a full snapshot from the completed VHD URI using the owned
storage-account ID and `incremental=false`. It checks that every snapshot ID
captured before transfer is still present and writes the regional manifest
only after the imported snapshot is verified. From the archived `westus2/`
region root, initialize the source keeper manifest and run the standard,
resumable path:

```bash
mkdir -p .state
cp ../eastus/fixture.json .state/prepared-snapshot-keeper.json
python3 fixture-preparation/prepare.py
cp .state/fio-hdd-westus2-fixture.json fixture.json
```

It expects Azure CLI authentication for the subscription in
`.state/prepared-snapshot-keeper.json`. Do not rotate or revoke the disk export
grant while an Azure blob copy is pending.

`fast-copy.py` is retained as a recovery helper for this specific failed-copy
state. It refuses unless the original `prepared.vhd` copy reports `failed`,
then obtains a fresh read-only export SAS and writes a different blob. It is
not a parallel accelerator for a pending blob copy. If a completed fast-copy
marker is present and verified, `prepare.py --completed-fast-copy` imports
that blob as the regional full snapshot. The script validates that the marker
matches the owned source snapshot, disk, storage account, and fixture token.

Neither script deletes snapshots. The temporary transport resources and VHD
blobs are intentionally retained until both read-test stacks have been torn
down and `cleanup.py` has passed its gates.

## Archive layout

This file is stored at `westus2/fixture-preparation/README.md`; the shared
manifest is `westus2/fixture.json`, with the 4 KiB and 1 MiB runbooks beside
it in `fio-hdd-repro/` and `fio-hdd-1m-repro/`. Cleanup can read the current
regional manifest, the source keeper manifest, and run ownership records from
those archive paths. It accepts run state in either the live `state.json`
location or `artifacts/infrastructure-state.json`.

This is a reproduction package, not a snapshot transferable to another
subscription: the fixture manifest contains Azure resource IDs. Run-time SSH
keys, state files, and analysis environments are excluded from Git. State
records with Azure IDs and ownership tokens remain under each run's
`artifacts/infrastructure-state.json`. Never put account keys, managed-disk
SAS URLs, or destination SAS URLs into the archive.

## Completed cleanup

Both benchmark resource groups and the temporary transport group were deleted after collection and validation. All 45 snapshots remain: the original 44 plus the West US 2 keeper. [Cleanup evidence](artifacts/transport-cleanup.json) records the unchanged inventory and deletion scope.
