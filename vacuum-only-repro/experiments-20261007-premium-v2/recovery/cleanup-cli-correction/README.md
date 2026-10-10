# Cleanup retry

The first cleanup attempt launched at 2026-10-07 12:17:45 UTC and stopped at 12:18:39 UTC. All three stack teardown scripts returned exit 1 while inspecting role assignments, before the first role-assignment or compute deletion. The installed Azure CLI rejected combining `role assignment list --scope` with `--all`.

The correction removes `--all` from the four scoped list calls. Exact assignment ID, scope, principal and role checks remain unchanged, along with snapshot locks and all snapshot/compute ownership guards. The [Azure CLI scope option](https://learn.microsoft.com/en-us/cli/azure/role/assignment?view=azure-cli-latest#az-role-assignment-list) and installed CLI help describe scoped lookup. Live read-only checks found the expected exact assignment in all three stacks.

The original source is preserved as [04-teardown-before.sh](04-teardown-before.sh), the failed attempt evidence and hashes as [manifest.json](failed-attempt-evidence/manifest.json), and the live lookup verification as [live-scoped-read-verification.json](live-scoped-read-verification.json). Append-only cleanup journal and run-specific process receipts record both attempts; final timelines distinguish launches, observed exits, canonical completion receipts, and final cloud retention audits.

The second attempt completed all three compute teardowns, then stopped at
2026-10-07 12:37:54 UTC because the shared-watcher resource inventory was not empty
after the expected watcher deletion. A fresh read-only inspection found the East
US watcher group absent and the West US 3 group empty. The same reviewed
cleanup code was retried idempotently, with no broadened deletion scope or
relaxed guard. The [second attempt archive](second-attempt-evidence/manifest.json)
preserves its completion receipts and before/after-compute snapshot audits.
