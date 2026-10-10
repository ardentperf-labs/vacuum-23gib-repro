# Read-pressure experiment 2 — partial result

**User-terminated early; replica replay was incomplete.** Primary VACUUM completed in **34 min 40.0 s**. Replay was observed another **2 h 24 min**, without target catch-up. Whole observed replica DATA read-await averaged **2,651.9 ms**.

Primary DATA recorded an estimated **374,600 reads** during VACUUM; replica DATA recorded **785,111 reads** through the last sample, a partial ratio of **2.10×**. Replica totals include the five external readers.

Raw capture: [`samples.jsonl.gz`](samples.jsonl.gz), 2,158 samples, SHA-256 `72bbca6f1825deab40c6e482b244d20137c7d0de7e2af93bbf104c664caa462a`. Full report: [`REPORT.md`](REPORT.md). Linear graphs: [await](await.png), [IOPS](iops.png), [throughput](throughput.png).

Supplementary Azure Monitor data showed replay-tail VM uncached bandwidth at 99.7% mean / 100% median, while VM uncached IOPS averaged 1.3%; DATA LUN 0 averaged 53.6 MB/s (48.8 median), and WAL LUN 1 Azure latency averaged 3.09 s (3.43 s median). This is consistent with a shared VM bandwidth constraint contributing to slow replay, not proof of causation. These platform aggregates do not replace or subtract guest DATA-device counters, which include PostgreSQL and all five external readers. Dsv4 uncached-throughput documentation is for Premium SSD and does not directly characterize this Standard_LRS HDD run. [Diagnostic and source links](azure-monitor-diagnostic/README.md).

The selected five-reader 90-second replay-tail calibration measured 1,675.9 ms operation-weighted guest DATA read-await over 15 intervals (all ≥1,500 ms), at 100.67 MB/s with 34.55 MB replay progress. This calibration is separate from the full-run partial observation; device totals include the external reader operations. [Calibration evidence](../../evidence/calibration/root-calibration-audit.json).

The user-requested stop left replay incomplete. Stack cleanup later verified all 13 snapshots preserved and locked, with no experiment resource groups remaining: [cleanup result](../../cleanup/result.json) · [fresh Azure audit](../../cleanup/root-fresh-azure-audit.json).
