# Cleanup orchestrator review

`cleanup-run.py` is a local-only preflight by default. `--apply` runs the existing
retention audit, `run/04-teardown.sh`, exact source Reader cleanup, optional
owned-watcher cleanup, and a final retention audit. This review and its tests
made no Azure calls, and `--apply` was not run.

The apply gate requires a completed test-process receipt, complete test summary,
valid native sampling coverage, zero native client failures, reconciled client
counts, pinned archive inputs whose hashes and sizes match the source manifest,
and captured completed offline paired DATA/WAL seed Backup evidence. The
orchestrator records the 42 pre-run snapshot identities and current lock
inventory before teardown, stores a restricted log for a new `04` invocation,
and supports resume from a matching `run/.state/teardown.json` without invoking
`04` twice. It then calls the exact-scope Reader helper, preserving and
reporting `ScopeLocked` residual assignments, and compares full snapshot
inventories and lock identities in the final audit.

`run/04-teardown.sh` now rejects direct invocation before its first Azure
command. Immediately before launch, `cleanup-run.py` writes a mode-0600,
10-minute, one-use authorization under `run/.state/`. It binds the active
cleanup attempt and parent PID, exact stack token/subscription/resource groups,
the current teardown-script hash, and hashes/sizes for the successful test
receipts and all result files, pinned source/archive evidence and private
archive inputs, completed offline Backup evidence, Blob ownership receipts,
the 42-snapshot/lock baselines, and the fresh before-cleanup snapshot/lock
audits. The helper verifies those files and the cleanup-run receipt, checks that
the caller is the active cleanup-run process, then consumes the nonce before
`04` can issue any Azure request. A direct-call test uses a fake `az` executable
and confirms that it is never reached without authorization.

Prepared DATA/WAL unique IDs are checked against values in the preparation
audit when present. The current `02-prepare.sh` output does not store ARM unique
IDs in its paired snapshot records, so the guard derives them from the two
captured `seed-arm-snapshot-observed` ARM responses and pins those responses in
the authorization. The before-cleanup and final live audits, plus the teardown
receipt, must match those same unique IDs.

An owned Network Watcher never blocks compute cleanup. A missing, stale, invalid,
or unsafe root review preserves it. A safe deletion gate is bound to the exact
watcher ID, region, owner token, and stack name; it records a root review time,
shared-use assessment, watcher resource-group inventory, child flow-log IDs,
and the saved watcher ETag. Any live child flow logs make the gate non-deletable.
After the matching `04` receipt proves this experiment's AKS and node resource
groups were deleted, the orchestrator freshly reads the watcher's
connection-monitor, flow-log, and packet-capture child collections. It invokes
the existing exact-ownership watcher helper only if all three inventories are
complete and empty. That helper then rechecks watcher-group contents, lock and
role-assignment state, regional watcher inventory, and ETag before its
conditional delete. The gate does not claim unrelated subscription AKS clusters
belong to this experiment. A failed or incomplete optional watcher phase is
recorded for review while final snapshot retention verification continues.

The review gate at `run/.state/regional-watcher-safety-gate.json` has this shape
(use the exact watcher ID and region recorded by the owned-watcher state):

```json
{
  "version": 1,
  "authorized": true,
  "authorizedBy": "root",
  "authorizedAtUtc": "<recent UTC timestamp>",
  "stackName": "experiments-20261009-pgbench-baseline",
  "ownerToken": "<saved owner token>",
  "networkWatcherId": "<exact owned watcher ARM ID>",
  "region": "<exact saved region>",
  "sharedRegionalUseReviewedSafeToDelete": true,
  "sharedRegionalUseAssessment": "<root-reviewed shared regional use finding>",
  "watcherResourceReview": {
    "observedAtUtc": "<recent UTC timestamp>",
    "networkWatcherEtag": "<ETag matching saved watcher state>",
    "resourceGroupResourceIds": ["<exact owned watcher ARM ID>"],
    "childFlowLogIds": []
  }
}
```

Set `sharedRegionalUseReviewedSafeToDelete` to `false` when shared use is
present or unresolved. The orchestrator will finish compute cleanup and preserve
the watcher. A later safe root review can resume only the watcher phase from the
durable teardown receipt and cleanup record.

Validation run: 13 `test_cleanup_run.py` tests, the authorization-helper
self-test, the Reader helper self-test, and the owned-watcher helper self-test
passed. Python and Bash syntax checks passed. No Azure
credentials, tokens, or cloud resources were read or changed by this review.
