# Treatment client evidence recovery (read-only)

Captured 2026-10-13T10:27:42.109511+00:00. This supplements rather than rewrites the original failed run artifacts. No SQL writes, PostgreSQL signals, or cloud/resource mutations were performed.

The original state remains `testComplete=false` with error `VACUUM not confirmed complete (exit=0)`.

The saved aggregate pgbench progress contains 1623 consecutive 0.0 TPS / 0-failure buckets from **2026-10-13T05:28:22.186000Z** through **2026-10-13T07:43:32.185000Z** (8109.999 seconds). It is not the native per-transaction log.

Primary activity telemetry provides stronger in-flight evidence: **10590 successful samples** showed the same ten client PIDs and query/xact starts active on `IPC/SyncRep` from **2026-10-13T05:28:26.709299Z** through **2026-10-13T08:24:55.707409Z**, a span of **10588.998 seconds** with a maximum observed interval of **1.05 seconds**. The client session sampler independently ended at **2026-10-13T07:53:16.689624Z** with all ten sessions still waiting and 4,610 fixture updates.

A later read-only primary capture at **2026-10-13T10:22:52.729741+00:00** found no client sessions and 4,620 fixture updates, ten above the last blocked client sample. Exact first resumed completion time is **unresolved**: it occurred after the last all-ten blocked sample at **2026-10-13T08:24:55.707409Z** and no later than the current fixture capture. PostgreSQL logs show archive restore followed by the replica beginning streaming and becoming a quorum candidate at **2026-10-13T08:31:05.336065Z**; that is consistent with a release boundary, but it is not a direct transaction-completion timestamp.

The treatment native raw log directory was not recovered: local `client/raw/` is absent, remote pgbench PID 6106 and workdir were absent on the read-only probe at **2026-10-13T10:25:40.974047+00:00**, and the local process receipt still says `RUNNING`. The source at `03-test.py:1511` removes the remote work directory unconditionally after a failed drain. That is the likely cause of the missing raw files, though the deletion time was not separately recorded.

The first read-only query attempt as database user `app` failed peer authentication before SQL ran; the successful SELECT-only retry used the container-local `postgres` role. The failed probe output is preserved separately.

Evidence is in `recovery-receipt.json`, `stable-writer-evidence-through-last-sample.json`, `native-progress-snapshot-evidence.json`, `current-readonly-capture.json`, and `remote-client-process-probe.json`.
