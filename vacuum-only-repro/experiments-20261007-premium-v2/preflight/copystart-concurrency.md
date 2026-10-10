# Azure CopyStart concurrency note

Checked against Microsoft Learn on 2026-10-06; web documentation only, no Azure CLI or resource calls.

- The managed **cross-region** incremental snapshot-copy article documents up to 100 parallel copies per subscription per region, but only one incremental snapshot of a particular disk at a time, and requires copies in creation order. Its example uses `--incremental --copy-start`. Because that article scopes these restrictions to cross-region copies, it does not establish a universal one-copy-at-a-time limit for same-region CopyStart operations. [Microsoft Learn: Copy an incremental snapshot to a new region](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-copy-incremental-snapshot-across-regions)
- For **Premium SSD v2 and Ultra** incremental snapshots, the snapshot documentation says that creation starts a background copy; up to three total snapshots per disk may be pending while it runs, then the process must complete before more snapshots of that disk can be created. It separately allows up to five disks to be created concurrently from a snapshot. [Microsoft Learn: Create an incremental snapshot](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-incremental-snapshots)
- The Compute REST API defines `CopyStart` as a deep copy that completes after all source data is copied; it does not add a general same-source concurrency limit. [Microsoft Learn: Snapshots Get REST API](https://learn.microsoft.com/en-us/rest/api/compute/snapshots/get?view=rest-compute-2026-04-01)

The live failure reported by root was `AzureOperationNotAllowed: Another copy in progress` for source disk `keeper-pv2-f1...` while its baseline CopyStart was still in progress. This is consistent with a source-disk copy operation still holding an Azure service gate, but the public documentation above does not prove the precise same-region threshold. Conservative action: serialize CopyStart operations derived from that source and wait for the active baseline copy to complete before retrying; preserve both source and target snapshots.


Later live evidence (20:30 UTC review) showed that serialization alone did not
resolve the source-family restriction: the later copy failed immediately with
`IncrementalCopySourceInvalid` because a newer snapshot existed on the target.
See `recovery/source-restore/live-copy-failure.md` for operation times and IDs.
The repaired 50/100-GiB preparations now use independent owned staging disks
and incremental Copy snapshots from those disks. Both new snapshot families
were observed Succeeded/Available and directly locked before CNPG restoration.
The original failed copy remains protected.
