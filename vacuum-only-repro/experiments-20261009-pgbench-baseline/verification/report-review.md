# Client and final-report review

Reviewed `client-report.py` and `build-report.py`, plus the static client sampling, phase-event, and storage-window producers they consume. No run outputs or Azure data were read. A synthetic fixture was built only under ignored `run/.state/report-review/synthetic`; it exercised the report path without changing the reviewed scripts or using Azure.

## Findings

### P1 — The final report can present an invalid run as measured evidence

`build-report.py` reads `summary.json`, `client-impact.json`, and `client-summary.json`, but never gates report generation on `summary.status.testComplete`, `summary.coverage.valid`, the pgbench exit code, failed-transaction count, native/logged completion-count agreement, or client validation errors. Its replay-backlog calculation uses `default=0`, so missing or unusable `samples.jsonl` is reported as a measured zero backlog. The storage ratios also use coverage-weighted counters without checking that coverage passed.

I changed only the synthetic fixture to set `testComplete=false`, invalid coverage, seven client failures, pgbench exit code 5, a validation error, and no sample file. The builder still emitted “Measured run,” printed the ordinary performance narrative, and reported “Maximum sampled replay backlog: 0.000 GiB.” Refuse a success report unless the test receipt, all required window coverage, client validation, native/logged counts, and usable replay samples pass. For failed or incomplete data, emit an explicitly marked incomplete report with unavailable fields rather than zeros or success language.

References: `build-report.py` lines 27–29, 77–87, and 93–101.

### P1 — The claimed complete timeline omits source creation and cleanup

The timeline builder adds only provision, prepare, and test process receipts; it never reads `teardown.json` or a watcher/retention cleanup receipt. Its resource scan recognizes Kubernetes Backup, VolumeSnapshot, and PVC creation timestamps, but ignores Azure snapshot and disk ARM responses with `timeCreated`. It also reads the source manifest only to print IDs/UIDs, so the original DATA/WAL snapshot and backup creation/start/stop times are absent from the timeline.

The synthetic fixture included Azure source-snapshot/disk `timeCreated`, a completed teardown receipt, and a hydration event. The generated timeline contained the hydration observation but no source-snapshot creation or teardown event. Add historical source snapshot and backup timestamps from the pinned manifest, Azure snapshot/disk creation times from their preserved ARM responses, observed hydration milestones with their actual observation precision, and teardown plus final retention/WAL/watcher cleanup receipts. Until final retention evidence exists, remove or qualify the unconditional “All snapshots and protection locks are preserved” sentence.

References: `build-report.py` lines 30–75 and 110–115.

### P1 — Phase labels need explicit completion-time and recovery boundaries

`client-report.py` assigns each transaction to a phase by its **completion** timestamp (lines 74–82), which is a sound choice for completion counts, but it does not say that phase latency samples can include transactions that started before the phase boundary. State this in the report. The `post_recovery` interval starts at the event emitted after three consecutive healthy replay samples, then runs to the end of all client samples/logs. It therefore combines the five-minute post-recovery health check, the client stop/drain, and the final client-off replay drain. Split those intervals using `client_post_recovery_healthy_complete`, the client stop/drain event, and `replica_caught_up_after_client_stop`, or label the combined range clearly. Check that warm-up, VACUUM, recovery, and end timestamps are monotonic before calculating rates.

References: `client-report.py` lines 62–82; `03-test.py` emits the distinct recovery, healthy-duration, stop/drain, and final catch-up events.

### P2 — Client clock sources and UTC normalization are not checked

The warm-up start is `sampleEpoch` from the controller, while pgbench completion times and VACUUM/replay boundaries are generated in the PostgreSQL pod. The monitor also records the database-side `observedAtUtc`, but the report uses controller `sampleEpoch` for phase age and chart boundaries. A clock offset can move samples across phase boundaries. Use one clock source for interval boundaries or record and apply a measured controller/database offset. Both `epoch()` and `timestamp()` accept naive datetimes, and the final timeline prints the original timestamp text without normalizing offsets to UTC. Require timezone-aware inputs and normalize displayed values to UTC.

References: `client-report.py` lines 14–17, 62–68, and 124; `build-report.py` lines 17–18 and 70–75.

### P2 — Percentile calculation differs from the native client summary and clamps impossible execution times

`client-report.py` selects quantiles with `round((n-1)*p/100)` (line 25); `03-test.py` uses floor-index selection for the same logged transactions. The displayed p95/p99 can therefore disagree between client-impact and native summaries. Share one estimator and identify it in the JSON/report. Also, `max(0, latency-lag)` silently changes any negative execution-time result to zero (line 53). Preserve the raw difference and fail/flag rows where schedule lag exceeds logged latency instead of masking the inconsistency.

### P2 — Sampling gaps can be hidden in the pending-work graph

The client JSON records sample errors and maximum successful-sample gap, but `build-report.py` does not surface either. `client-report.py` drops errored samples and connects the remaining points into continuous lines, which can visually bridge a long collection gap. The synthetic sparse sample series produced a 299-second gap without any warning in the final report. Report errors/gaps and break plotted lines across missing intervals; require an explicit sampling-quality threshold before describing pending ages as continuously observed.

References: `client-report.py` lines 59–61, 98–100, and 123–130; `build-report.py` lines 91–95.

### Follow-up — Retract the preflight-link and fixed-rate findings

The preflight link is valid: `run/03-test.py` writes `run/results/preflight.json`, next to `REPORT.md`. The current builder title says “target-rate client” and its body correctly describes Poisson arrivals. The earlier broken-link and “fixed-rate” claims were incorrect and are withdrawn. The expected-offer legend was hardcoded in the initial client graph; the follow-up implementation now derives it from `client-config.json`.

## Correct behavior to retain

The initial client report correctly separated completion-only latency distributions from sampled unfinished-work ages, included schedule lag in end-to-end latency, subtracted it for execution latency, counted zero-completion buckets, and used the final bucket’s actual duration. The follow-up implementation preserves those semantics and makes stop/drain and sampling boundaries explicit.

## Validation performed

- Both scripts ran against a synthetic isolated fixture; no actual result artifacts were used.
- Confirmed the builder omitted synthetic teardown and Azure creation events while retaining the synthetic hydration event.
- Confirmed the builder still emitted success-style report text and a zero replay backlog for a synthetic invalid run with no samples.
- No Azure calls were made and no root-owned report scripts were edited.

The initial review findings about missing report validation and incomplete source/cleanup timeline remain for the root-owned `build-report.py`. `client-report.py` was updated separately and has its own offline tests and receipt in `client-report-review.md`.
