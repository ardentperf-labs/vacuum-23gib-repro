# PremiumV2 source-copy failure evidence

Read-only ARM and Activity Log observations recorded on 2026-10-06 UTC. The
failed snapshot is retained untouched; this note does not authorize deleting,
overwriting, or unlocking any snapshot.

The East US allowlisted source was `keeper-base-16244c2c`, with source unique ID
`5ba7640c-4e34-4227-be0d-d428baeaf2cf`. An earlier target,
`keeper-pv2-4b116309`, was created from that same source at 15:51:46Z. Its live
ARM properties showed `provisioningState=Succeeded`,
`snapshotAccessState=Available`, `completionPercent=100`, size 512 GiB,
`incremental=true`, and destination family ID
`318ac892-9fd9-4c38-8210-5d6b713269d8`.

The later target `keeper-pv2-f1b90aec` was created from the same source with
`creationData.createOption=CopyStart`. Live ARM properties showed
`provisioningState=Failed`, `completionPercent=0`,
`snapshotAccessState=Unknown`, size 512 GiB, and `incremental=true`; it remains
in the original target family `318ac892-9fd9-4c38-8210-5d6b713269d8`.

Activity Log evidence for that later copy:

- At 15:56:46.9730Z (correlation ID
  `b2061d17-901b-4325-85f7-482f3aa3b131`), Azure returned
  `OperationNotAllowed`: “Another copy is in progress for source disk
  keeper-pv2-f1b90aec. Overlapping copy operations are not supported.”
- At 16:29:15.7419Z the later `CopyStart` operation was accepted; at
  16:29:17.8767Z (correlation ID
  `df0fb0e5-1357-4ea2-af60-e9fe99f135ea`) Azure returned
  `IncrementalCopySourceInvalid`: “This operation is not permitted since a
  newer snapshot already exists on the target.”

This evidence supports avoiding another direct `CopyStart` to the same target
incremental family. The prepared recovery path stages a new owned Standard_LRS
disk from the pinned allowlisted incremental source, then creates a fresh
incremental `Copy` snapshot from that stage disk in the stack's owned,
CanNotDelete-locked snapshot resource group. The failed and successful legacy
snapshots remain unchanged.
