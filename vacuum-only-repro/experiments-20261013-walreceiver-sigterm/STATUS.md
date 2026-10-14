# Final artifact status

Cleanup is complete as of 2026-10-14 05:11 UTC. Both owned AKS stacks, worker nodes, test DATA/WAL disks, temporary Blob accounts, and the owned Network Watcher were removed. All five original retained snapshot IDs were verified unchanged, including both matched PostgreSQL DATA/WAL pairs.

WAL downloads were stopped at the user’s request. Required recovery metadata and the verified subset already copied remain private.

The report, raw metrics, and 14 linear-axis graphs are in the local GitHub checkout. The recovered evidence verifies more than two hours with all ten treatment writers blocked; the control continued writing. Both original harness outcomes remain incomplete, and the missing final treatment client logs and unknown exact resumption timestamp are documented.

No files have been staged, committed, or pushed.
