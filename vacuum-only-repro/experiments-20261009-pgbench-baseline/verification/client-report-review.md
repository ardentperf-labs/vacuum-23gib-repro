# Client-report follow-up verification

`client-report.py` now uses primary-pod `observedAtUtc` values for pending-work samples and maps the controller warm-up and stop-request times using paired sample offsets. It records observed offset bounds, requires timezone-aware ISO timestamps, checks timeline order, and assigns transactions by their completion time.

The report splits replay catch-up, the post-recovery healthy interval, active post-recovery work, and stop/drain completions. Client TPS buckets end at the mapped stop request, include zero-completion active buckets, and use the actual duration of the final partial bucket. Stop/drain completions remain visible in their phase without presenting an inactive-client TPS. Pending samples end at the observed drain boundary; error and over-2.5-second gaps break graph lines and are excluded from the stated coverage intervals. Percentiles use the native floor-index estimator. Malformed rows and negative execution differences are listed as validation errors and excluded rather than clamped. The expected Poisson offer legend comes from `client-config.json`.

The old review's preflight-link claim is withdrawn: `run/03-test.py` writes `run/results/preflight.json`, next to the generated report. The client graph's previous hardcoded offer label has also been removed.

Validation: `.venv/bin/python -m py_compile client-report.py verification/test_client_report.py` passed; `.venv/bin/python -m unittest verification.test_client_report -v` passed all 7 offline tests. The end-to-end synthetic fixture ran only in a temporary directory, produced the three client graphs, verified UTC offset mapping and phase assignment, kept the stop/drain transaction outside client-active TPS, and retained a zero-completion partial active bucket. No Azure calls or real run artifacts were used.

SHA-256:

- `client-report.py`: `cf3b682742b6085fce515a1c44b2c868a537b5f74dcce43bb63a7f97e888874a`
- `verification/test_client_report.py`: `a42741dedcd9004df1ec5d46cefc582b2efec084bda6a62bbb4f93868a8da644`
