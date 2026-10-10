# Persistent experiment instructions

- Latest user instruction (2026-10-07): move the three completed Premium SSD v2
  runs into a new subdirectory of the GitHub repository. The archive now lives
  at `vacuum-only-repro/experiments-20261007-premium-v2`. This supersedes the
  earlier outside-checkout location requirement; retain all as-run evidence
  and the existing snapshot-preservation instructions.

- Current experiment request (2026-10-06): three parallel isolated stacks
  outside the GitHub checkout. Repeat the 23-GiB baseline, plus 50-GiB and
  100-GiB fixtures, using four 512-GiB PremiumV2_LRS DATA/WAL disks per stack
  at the minimum 3,000 read/write IOPS and 125 MB/s. Keep the baseline worker,
  PostgreSQL image, CNPG version, preparation, VACUUM, and GUC settings.
  Placement: baseline and 100 GiB in East US zone 1; 50 GiB in West US 3
  zone 1 due denied East US DSv4 quota increase. Record this confound.
  Preserve all existing and new snapshots; no snapshot deletion, direct or
  indirect. Old experiment instructions below provide safety/context but do
  not authorize edits to the original checkout or changes to old experiments.

- Latest user instruction (2026-10-04): preserve ALL snapshots until the user
  explicitly authorizes deletion. This supersedes the exactly-one snapshot
  policy below. Prevent indirect deletion through AKS/resource-group teardown.

- After testing, keep exactly one large prepared PostgreSQL primary-disk snapshot
  for future reproduction tests. The user explicitly requested this exception
  to the earlier delete-everything cleanup rule on 2026-10-03.
- Retain that snapshot in a separate owned resource group, record its Azure ID
  and preparation metadata locally, and delete every other test resource:
  AKS, worker nodes, managed disks, temporary snapshots, and test resource groups.
- Do not discard the last usable retained snapshot while replacing it. Verify
  its replacement before deleting the previous retained copy. Do not delete
  unrelated resources or adopt resource groups without verified ownership.
- Use Luna xhigh subagents for substantial independent work, with at most four
  concurrent threads including the root agent. Root owns architecture,
  integration, and final verification.
- The user authorized up to four simultaneous Azure experiment stacks on
  2026-10-04 when parallel experiments accelerate progress. This is independent
  of the four-agent concurrency limit. Keep each stack's infrastructure state,
  ownership tokens, kubeconfig, controllers, and artifacts isolated. Verify
  subscription/region quotas and cleanup scope before parallel provisioning.
  All stacks must preserve the same single prepared keeper snapshot; defer
  shared-resource cleanup until every experiment stack is gone.

- Use linear y-axes in all experiment graphs. The user explicitly requested
  linear scales on 2026-10-03; do not use log, symlog, or exponential scales.

- Experiment goal (2026-10-04): measure VACUUM-driven disk IOPS, throughput,
  and latency. Use separate data/WAL disks, no client workload, and
  let VACUUM finish. Restore CNPG defaults for wal_level, full_page_writes,
  recovery_prefetch and vacuum_cost_delay. Recovery prefetch is no longer the focus.
- Deliver a standalone clean Git repository in vacuum-only-repro with four minimal
  provisioning/preparation/test/teardown scripts, README, report, and separate
  linear-axis await, IOPS, and throughput graphs for primary/replica DATA/WAL
  reads/writes. Reuse the retained large snapshot whenever possible. Do not
  assume the latency outcome before measuring it.
- While the current VACUUM continues, test CNPG snapshot-backup creation and
  replica seeding in a separate owned stack (2026-10-04). Azure Blob storage and
  WAL archiving are authorized to support that path. Use a small validation
  fixture first; preserve the sole retained large snapshot until a verified
  large replacement exists, and remove all temporary validation resources.
