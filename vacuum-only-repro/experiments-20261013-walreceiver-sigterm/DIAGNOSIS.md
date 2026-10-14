# Diagnosis: receiver disconnect during a large VACUUM

The corrected archive-enabled experiment reproduced more than two hours with all ten client connections blocked on synchronous commit. The control continued writing. The treatment actually restored WAL from Azure Blob, then returned to streaming after finishing archive recovery. There is no captured evidence that a failed object-store restore caused the stall or its release.

The original harness outcomes remain failed/incomplete. A marker parsing bug and premature client-directory cleanup prevented complete treatment client reconciliation. The recovered evidence establishes the long stall, but does not recover its exact ending transaction timestamp or final per-transaction latency distribution. See [REPORT.md](REPORT.md) for the comparison, metrics, and these limits.

## Observed sequence

Both lanes used the same retained, matched DATA/WAL snapshot baseline, a 23-GiB prepared PostgreSQL heap, two Standard_D2s_v4 workers, separate 512-GiB Standard_LRS DATA/WAL disks, and host caching None. Both used PostgreSQL 16.15, CNPG 1.30.1, ten client connections offering five writes per second, synchronous_commit=on, and ANY 1 synchronous standby. The system PostgreSQL image includes Barman Cloud tools. There was no induced disk fault, object-store error, or second PostgreSQL signal.

| Event (UTC, 2026-10-13) | Treatment | Control |
|---|---|---|
| VACUUM starts | 05:18:12.699771 | 05:18:14.223251 |
| Receiver intervention | SIGTERM accepted 05:28:14.077112 | No signal; marker 05:28:16.431125 |
| VACUUM exits successfully | 06:14:43.543859 | 06:08:03.497441 |
| Last sample with the same ten writes on SyncRep | 08:24:55.707409 | No corresponding sustained stall |
| Last successful archived WAL restore | 08:31:02.120 | None after the marker |
| Streaming receiver returns | 08:31:05.335968 | Stayed streaming |

The treatment scheduler dispatched at +600 seconds. Signal delivery was +601.377 seconds because it verified the live SQL and Linux receiver identity before sending SIGTERM. This simulates a receiver/stream disconnect; it does not directly inject a wal_sender_timeout. The five-second sender and receiver timeout settings were identical in both lanes, and the captured logs do not establish a native sender-timeout event.

The native pgbench progress snapshot contains 1,623 consecutive five-second buckets reporting zero TPS and zero failed transactions, from 05:28:22.186 through 07:43:32.185. The client was still offering work for more than two hours before the harness requested its stop around 07:38. Database telemetry independently records 10,590 successful samples of the exact same ten active transaction identities waiting on IPC/SyncRep, from 05:28:26.709299 through 08:24:55.707409: **2 hours, 56 minutes, 29 seconds**, with no sample interval over 1.05 seconds. This is a sampled lower bound on the in-flight wait, not an exact completed transaction latency. The later part includes the harness drain attempt and must not be described as continued offered load.

The treatment committed-update count stayed at 4,610 while those ten transactions waited. A later SELECT-only capture found 4,620 and no client sessions. Exact first completion is bounded after 08:24:55.707409 and by the 10:22:52.729741 capture. The return to streaming/quorum at 08:31:05 is consistent with the release mechanism, but cannot substitute for the missing native completion timestamp. The control's native logs and fixture counter reconcile 43,670 committed client transactions.

## Actual archive recovery, not a no-op restore

Both lanes passed a premeasurement smoke: a closed 16-MiB primary WAL segment, an independent Azure Blob download, and a CNPG manager restore on the replica had identical sizes and SHA-256 hashes. Each primary had one earlier setup failure during Secret authorization/cache readiness; failed_count was already 1 at the clean smoke and did not increase during measurement. That baseline failure is not an outage-time object-store error.

After SIGTERM, treatment logs record **2,054 distinct, consecutive** archived WAL segments, from 000000020000004E000000FB through 000000020000005700000000, with no gap or duplicate. Azure's deduplicated native Transactions series independently records **2,054 successful GetBlob requests** in the corresponding recovery interval. Control has zero successful GetBlob requests and zero archive restore events after its marker. Azure totals are aggregates rather than a per-object request trace; the matching totals corroborate the native per-segment restore log without mapping each GET to a filename.

At the end, recovery requested 000000020000005700000001, which was the next/current WAL segment and had not yet been archived. The resulting missing-object boundary preceded a successful streaming connection. A standby can try the archive before streaming, so a missing current segment at that boundary is expected evidence of the transition, not proof of a broken backup configuration. The restored sequence and successful Blob GETs contradict the theory that this run merely waited on failing cloud restores.

With synchronous_commit=on, primary commits wait for a synchronous standby's flush acknowledgement, not its replay. After the receiver disconnected, the treatment had no streaming synchronous sender/receiver while the standby worked through archived VACUUM WAL. The measured SyncRep waits align with that absence. Returning to a streaming quorum candidate provides the acknowledgement path again. This supports the proposed mechanism in this controlled pair; the experiment does not prove that every real sender timeout produces the same behavior or that SIGTERM and a native timeout are identical.

## Harness failures and preserved evidence

VACUUM successfully exited in both lanes. The runner fetched separate start/completion marker files but parsed only the original log variable, leaving timing markers null and reporting VACUUM not confirmed complete (exit=0). It consequently skipped the normal catch-up/healthy-client completion path. The treatment stop attempt timed out after 900 seconds while its ten writes were still blocked; unconditional cleanup removed its remote client directory before those writes finished. The final treatment native transaction logs and summary were therefore not recovered. Neither lane's original testComplete=false status or timing has been changed.

Saved native progress, per-second backend identities, fixture counts, VACUUM completion markers, PostgreSQL logs, Linux/BCC metrics, Azure metrics, and independent recovery receipts support the separate recovered analysis. Linux/PG collectors ended around 08:25, before the 08:31 streaming return; native PostgreSQL logs and supplemental Azure metrics cover that later transition. Graphs show available measurements and gaps, with linear axes. Future runner code fixes marker parsing, preserves the client directory on drain/capture failure, and verifies bounded transfers; executed lane scripts remain preserved as the actual run sources.

The earlier [archive-invalid pilot](../experiments-20261012-walreceiver-sigterm/DIAGNOSIS.md) resumed after about 36 minutes because it had no configured archive backend and returned to streaming while replay was still behind. It cannot answer this archive-enabled experiment's question. The corrected run's long wait and actual Blob recovery are separate results.

PostgreSQL recovery metadata and the verified WAL subset already downloaded are retained privately, with byte/hash records. At the user’s request, the full post-test WAL export was stopped before completion; the experiment diagnosis does not depend on that optional copy. The retained offline DATA/WAL snapshot pair is self-contained, with matching begin/end recovery LSNs, and does not require this new experiment archive. Credential-bearing captures and WAL payloads are excluded from Git. Final cleanup receipts verify deletion of the two owned stacks and the owned Network Watcher, with all five original snapshot IDs preserved.

## Implementation references

- PostgreSQL 16 [synchronous commit acknowledgement levels](https://www.postgresql.org/docs/16/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT), [standby archive/streaming recovery](https://www.postgresql.org/docs/16/warm-standby.html), and [replication configuration](https://www.postgresql.org/docs/16/runtime-config-replication.html).
- PostgreSQL 16 [synchronous replication implementation](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/syncrep.c).
- CNPG 1.30.1 [WAL restore helper](https://github.com/cloudnative-pg/cloudnative-pg/blob/v1.30.1/internal/cmd/manager/walrestore/cmd.go) and [object-store backup path](https://cloudnative-pg.io/docs/1.30/appendixes/backup_barmanobjectstore/).
