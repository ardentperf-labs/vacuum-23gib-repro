# Final client and replication result review

Read-only review of the completed run artifacts in `run/results/`, including `client-summary.json`, `client-warmup.json`, `client-impact.json`, `client-replication.json`, `summary.json`, `samples.jsonl`, `events.jsonl`, the rendered report, and the client/replication plots. No collector, plotter, live system, or cloud state was changed.

## Reconciliation and client outcome

- pgbench exited 0 with 13,176 completed and logged transactions, zero failed transactions, no malformed rows, and no validation errors. The summed update counter from the ten client rows is also 13,176; the reported reconciliation difference is zero.
- The phase counts sum to 13,168 measured completions. Eight log rows completed before the measured warm-up boundary and zero completed after the drain boundary, so `totalCompletedTransactions` (13,176) is eight higher than the phase sum (13,168). pgbench launched at 08:35:40.518 UTC, before the monitor established the healthy warm-up start at 08:35:41.436894 UTC; the client log retains those startup completions, while completion-time phase assignment correctly excludes them from the measured window.
- The exact healthy warm-up counter delta is 1,484 commits over 301.817 seconds, or 4.9169 TPS. Native completion timestamps give 1,484 completions and 4.9173 TPS over the mapped window.
- VACUUM ran for 942.797 seconds. During it, the client completed 4.992 TPS; p95 end-to-end latency was 459.227 ms and p95 execution latency was 434.225 ms. The healthy warm-up p95 values were 6.498 ms and 6.237 ms respectively. No full 10-second zero-completion bucket occurred during VACUUM or across the active client window.
- Recovery-phase completion rates and durations reconcile with the rendered phase table. Latency includes schedule lag; execution latency subtracts schedule lag but still includes commit wait. The report accurately avoids attributing replica replay delay directly to synchronous commit wait.

## Replication and storage plots

The replay-backlog plot shows a maximum valid sampled primary-flush-to-replica-replay backlog of 12.907 GiB; the sampled primary-flush-to-standby-flush backlog peaked at 40.694 MiB. Replay data has 463 valid samples, 88.2% coverage of the full client-active x-range, no invalid samples, and an 8.126-second maximum valid-sample gap. Its first valid sample is at 08:40:50.302 UTC, about 308.9 seconds after the warm-up time zero. The replication traces start at that first sample; they do not insert zeroes into the unobserved warm-up. Native client completion buckets span the client-active period and contain no full zero-completion buckets.

The DATA read-amplification calculation is consistent with the sampled storage aggregates: replica/primary DATA reads are 15.809× by guest IOPS while throughput is 0.994× by guest MB/s. This is a guest block-counter comparison, not evidence that each operation is a separate physical or billed Azure transaction. The report also records the unresolved mismatch between guest throughput counters and the worker SKU bandwidth telemetry.

All reviewed graph axes are linear. The generated graph reviewed here covers the complete client-active window, but its then-current title says “during VACUUM” and x-axis says “since pre-vacuum start.” I updated `client-replication.py` to label the title as spanning warm-up, VACUUM, and recovery, and to label time zero as the healthy warm-up start. The root agent will regenerate the final plot and evidence hash.

## Recovery boundary semantics

The VACUUM target LSN was first reached with the client still running at 09:14:15.922748679 UTC. The five-minute healthy post-recovery interval completed at 09:19:15.898647501 UTC. pgbench was then gracefully stopped and drained by 09:19:19.446970 UTC. After the client stopped, the replica reached the newer current-primary flush target at 09:19:32.138817165 UTC. These are distinct milestones: the first is catch-up to the VACUUM completion LSN under client traffic; the last is catch-up to WAL generated through the later healthy interval and client drain.

The top-level report should give the first VACUUM-target timestamp separately from final current-LSN replay completion. The detailed event timeline contains both milestones.
