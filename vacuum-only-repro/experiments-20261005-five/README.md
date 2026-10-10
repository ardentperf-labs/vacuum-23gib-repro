# Five vacuum-only experiments

**Archive status:** ready for review. This directory is a review archive for the four completed experiments and the explicitly partial read-pressure observation. The pressure run was stopped by the user at three hours and did not reach replay catch-up.

The headline disk measurements use guest block-device counters. On the pressure run, replica DATA read IOPS and await include PostgreSQL/replay reads plus deliberate external O_DIRECT reader processes on that same DATA device. Reader caller completion latency is reported separately and is never substituted for device counters. In the partial pressure observation, primary DATA recorded about 374,600 reads during the 34m40s VACUUM; replica DATA recorded about 785,111 reads over 2h59m39s from VACUUM start to stop, a partial 2.0959× total-operation ratio that includes the five external readers. Replica catch-up was not observed. The operation-weighted guest replica DATA read-await averaged 2,651.85 ms over the same observed interval. The pressure run reports DATA read-await across the observed VACUUM-plus-partial-replay interval; no full replay result is claimed.

## Runs

- [Experiment 1: unchanged 23-GiB repeat](runs/baseline/REPORT.md)
- [Experiment 2: read-pressure partial observation](runs/read-pressure/results/read-pressure-aux-latencycap5-full-20261006T0130Z/REPORT.md)
- [Experiment 3: standby recovery_prefetch=off](runs/prefetch-off/results/20261005T072928Z-prefetch-off/REPORT.md)
- [Experiment 4: 100-GiB size variation](runs/100gib/results/100gib-20261005T130618Z/REPORT.md)
- [Experiment 5: full_page_writes/recovery_prefetch variation](runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/REPORT.md)

## Interpretation and reproducibility

The four full runs include primary VACUUM duration, post-VACUUM replay tail, total elapsed time through catch-up, full four-device read/write measurements, and estimated DATA read-operation amplification. Pressure reports only observed partial replay progress and rate-conditioned projections. Replay overlaps VACUUM, so post-VACUUM tail is additional time after VACUUM completion, not total replay processing duration.

`runs/*/source/as-run/` preserves the experiment scripts and their hash provenance. Some provisioning dependencies are external helpers, Azure CLI authentication, Kubernetes tooling, retained snapshot metadata, and a compatible Azure subscription; those are documented as dependencies rather than embedded credentials. Snapshot IDs and Azure resource identities are sanitized in packaged metadata. Replace the snapshot template values with an authorized retained snapshot before any new provisioning.

Published graph generators are presentation adaptations of the privately
preserved as-run sources. Recorded execution hashes identify those originals;
the repository publication manifest records the current published file hashes.
Raw captures and measured numerical results are unchanged.

The archive can be checked offline with `python3 tools/offline-verify.py`. Regenerate plots with `python3 tools/replot.py <run-slug>` after installing the pinned dependency in `requirements-offline.txt`; run rollover unit cases with `python3 tools/test_diskstats_rollover.py`. Neither offline tool uses Azure. All three graphs per experiment use linear y-axes.

Sensitive runtime state, kubeconfigs, `.state` directories, Azure secrets, Python environments, controller process files, and transient provisioning/preparation logs are excluded. The snapshot and cleanup ledger preserves the identity/provisioning state needed to reconcile retained snapshots while redacting subscription and tenant identifiers.


Experiment 5 PostgreSQL logs were fetched at completion; no independent continuous follower ran. The [capture audit](runs/prefetch-off-fpw-off/results/full-fpw-off-prefetch-off-23gib-20261006T062000Z/root-completion-log-capture-audit.json) verifies the retained log files and notes that earlier rotation or loss cannot be ruled out.


[Retained snapshot inventory](evidence/retained-snapshot-inventory-five.json) · [Snapshot lock evidence](evidence/snapshot-lock-evidence.json).

[Master comparison report](REPORT.md) · [Machine-readable five-run comparison](comparison-five.json).
