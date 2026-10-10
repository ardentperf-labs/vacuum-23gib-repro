# Experiment timeline evidence

Evidence-linked per-run timelines for the baseline and two scaled fixtures. Exact timestamps come from receipts or Kubernetes/ARM metadata; copy progress is labeled as first-observed evidence. These files are regenerated as measurement, WAL export, and cleanup receipts arrive.

- [Baseline 23 GiB](baseline-23gib/timeline.md) and [structured JSON](baseline-23gib/timeline.json)
- [50 GiB](50gib/timeline.md) and [structured JSON](50gib/timeline.json)
- [100 GiB](100gib/timeline.md) and [structured JSON](100gib/timeline.json)

The standalone per-run report publisher is [publish_run_reports.py](publish_run_reports.py). Its default is read-only (`--dry-run`); after review, `--write` updates only `RUN/results/REPORT.md` and embeds each full timeline. `--require-complete` refuses publication while any run lacks exit 0, complete timing, complete summary status, or valid coverage.
