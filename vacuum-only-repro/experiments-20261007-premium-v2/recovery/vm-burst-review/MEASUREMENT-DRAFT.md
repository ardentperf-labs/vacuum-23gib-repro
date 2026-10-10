# VACUUM guest I/O and supplementary VMSS metrics — interim draft

This is a descriptive supplement to the three VACUUM experiments, not a latency conclusion. It uses only the local collector output and already-collected Azure Monitor responses. The runs are not all complete yet, so refresh the final coverage and results after every VACUUM has exited successfully and each replica has replayed through its target LSN.

The current guest-data window is 2026-10-07 08:50–09:20 UTC. VACUUM started around 08:36 UTC in all three stacks. The baseline observation finished at 09:09:02.923Z after VACUUM and replay both completed, so its last three full five-minute windows are unavailable. The 50-GiB VACUUM exited 0 at 09:06:27.031Z but its replica was still replaying at 09:20Z. The 100-GiB VACUUM was still running at 09:20Z.

## Guest block I/O by five-minute window

The following rates come from the collector's node `/proc/diskstats` counters, summed across the mapped DATA and WAL devices for the named PostgreSQL role. IOPS are completed guest reads or writes per second; MB/s uses 512-byte sectors and decimal megabytes. Each rate is the counter delta divided by the actual sample-boundary duration (approximately 300 seconds). These are guest observations, not Azure managed-disk metrics, provisioned limits, or VMSS metrics. I/O remains separated by node role; each cell is `IOPS / MB/s`.

| UTC window | Stack | Primary DATA+WAL reads | Primary DATA+WAL writes | Replica DATA+WAL reads | Replica DATA+WAL writes |
|---|---|---:|---:|---:|---:|
| 08:50–08:55 | Baseline, 23 GiB | 6 / 0.7 | 5 / 1.8 | 3,175 / 44.4 | 46 / 27.4 |
| 08:55–09:00 | Baseline, 23 GiB | 0 / 0.0 | 0 / 0.0 | 3,120 / 37.3 | 41 / 25.5 |
| 09:00–09:05 | Baseline, 23 GiB | 0 / 0.0 | 0 / 0.0 | 3,111 / 37.1 | 42 / 25.7 |
| 09:05–09:10 | Baseline, 23 GiB | — | — | — | — |
| 09:10–09:15 | Baseline, 23 GiB | — | — | — | — |
| 09:15–09:20 | Baseline, 23 GiB | — | — | — | — |
| 08:50–08:55 | 50 GiB | 492 / 64.2 | 324 / 97.7 | 3,214 / 49.1 | 278 / 90.1 |
| 08:55–09:00 | 50 GiB | 562 / 69.6 | 314 / 113.6 | 3,214 / 49.3 | 202 / 115.3 |
| 09:00–09:05 | 50 GiB | 617 / 80.3 | 304 / 100.2 | 3,079 / 39.4 | 230 / 113.3 |
| 09:05–09:10 | 50 GiB | 140 / 18.2 | 93 / 28.1 | 3,122 / 37.3 | 105 / 42.9 |
| 09:10–09:15 | 50 GiB | 0 / 0.0 | 0 / 0.1 | 3,118 / 37.1 | 41 / 25.4 |
| 09:15–09:20 | 50 GiB | 0 / 0.0 | 0 / 0.0 | 3,121 / 37.3 | 40 / 24.5 |
| 08:50–08:55 | 100 GiB | 480 / 62.6 | 319 / 96.9 | 3,215 / 49.1 | 279 / 88.3 |
| 08:55–09:00 | 100 GiB | 473 / 61.6 | 305 / 106.0 | 3,193 / 48.9 | 211 / 112.6 |
| 09:00–09:05 | 100 GiB | 512 / 66.7 | 299 / 115.3 | 3,065 / 39.3 | 169 / 139.6 |
| 09:05–09:10 | 100 GiB | 474 / 61.8 | 301 / 105.0 | 3,122 / 37.3 | 206 / 120.4 |
| 09:10–09:15 | 100 GiB | 347 / 45.2 | 230 / 70.0 | 3,120 / 37.2 | 214 / 71.0 |
| 09:15–09:20 | 100 GiB | 149 / 19.4 | 101 / 29.6 | 3,121 / 37.3 | 119 / 44.3 |

The 100-GiB primary's combined DATA+WAL read-plus-write guest rate was about 167 MB/s in 09:05–09:10, 115 MB/s in 09:10–09:15, then 49 MB/s in 09:15–09:20. The 100-GiB VACUUM remained active across those bins. This describes a time change in the recorded guest counters; it does not identify its cause. For comparison, the 50-GiB VACUUM ended during 09:05–09:10, after which its primary DATA+WAL counters were nearly idle while its replica continued replay I/O. The baseline had already entered replay drain before the first window here.

The replica DATA+WAL read rates were close to 3,000 guest IOPS in these windows for all three stacks, while its write and throughput rates varied substantially by stack and time. These counts include only the two mapped data and WAL devices for that role; they are not per-disk rates and should not be interpreted as a single-device cap. Where a collector had stopped, `—` means there is no full five-minute observation, not zero activity.

## Azure Monitor VMSS observations

The local files in `collected-retry-2/` contain ten read-only captures from 08:35:48Z through 09:20:48Z. The newest response (09:20:48Z) returned six non-null PT5M points per metric for each of the three VMSSs, with HTTP 200, `errorCode: Success`, and passing owner-token, VMSS-name, `Standard_D2s_v4`, and capacity-two checks. For the six metric timestamps 08:50Z through 09:15Z, the table reports the arithmetic mean and min–max range from the newest available response for each timestamp. Overlapping rolling queries sometimes revised recent points; the newest captured value was used. This shared window is context only: the baseline observation ended at09:09Z, but the50-GiB observation remained in replay drain through09:48Z, so its points after VACUUM completion are still part of that measurement. Final per-run metrics must use only PT5M points fully inside each observation_started-to-observation_finished interval, with timestamp-boundary uncertainty stated.

| Stack | Remote burst I/O credits used | Remote burst BPS credits used | Uncached IOPS consumed | Uncached bandwidth consumed | Available points |
|---|---:|---:|---:|---:|---:|
| Baseline, 23 GiB | 0% (0–0%) | 0% (0–0%) | 34.87% (0–55.4%) | 40.24% (0.44–80%) | 6 / 6 per metric |
| 50 GiB | 0% (0–0%) | 0% (0–0%) | 57.49% (48–64.1%) | 82.55% (50–100%) | 6 / 6 per metric |
| 100 GiB | 0% (0–0%) | 0% (0–0%) | 60.40% (54–62.1%) | 99.97% (99.8–100%) | 6 / 6 per metric |

The corresponding uncached IOPS percentages at Azure timestamps 08:50 / 08:55 / 09:00 / 09:05 / 09:10 / 09:15 were 55.4 / 48.4 / 48 / 48 / 9.4 / 0 for baseline, 62.4 / 62.3 / 64.1 / 59.9 / 48 / 48.25 for 50 GiB, and 62.1 / 62 / 60.8 / 61.5 / 62 / 54 for 100 GiB. Uncached bandwidth percentages at those timestamps were 80 / 50 / 50 / 50 / 11 / 0.44, 100 / 100 / 100 / 95.3 / 50 / 50, and 100 / 100 / 100 / 100 / 100 / 99.8, respectively. Both reported burst-credit metrics were 0% at all six timestamps in all three VMSS scopes.

These are five-minute averages on one aggregate series per two-instance VMSS. They do not attribute activity to one node, and must not be divided by two or treated as per-node peaks. They are percentages and are not converted to IOPS or MB/s: the available records do not establish a D2s_v4 Premium SSD v2 normalization limit. Zero burst-credit use does not prove Premium SSD v2 burst eligibility or ineligibility. The parent VMSS metrics endpoint was available; per-instance Metrics - List had previously returned HTTP 404 for this provider mapping. Azure point timestamps are shown as returned and are not claimed to match the guest-counter window boundaries exactly.

Use these VMSS aggregates beside, not in place of, the role-specific guest disk counters. `FINDINGS.md` and `COLLECTOR.md` describe the SKU-specific documentation and attribution limits. The Dsv4 Premium SSD burst figures listed there are not applied as Premium SSD v2 limits here.

## Experiment state at this draft

- Baseline, 23 GiB: VACUUM exited 0 at 08:50:04.738699Z; replay reached the target and the observation finished successfully at 09:09:02.923Z.
- 50 GiB: VACUUM exited 0 at 09:06:27.031178Z; replay drain was still active in the latest local sample. Latest observed replica replay LSN was 75/2B7A5B90 against the VACUUM target 7B/D406CFD8; the receiver reported `streaming`.
- 100 GiB: VACUUM remained `RUNNING` at the latest sample (09:20:52Z). It was in `vacuuming heap`, with `heapBlksScanned=8,947,616` of `13,107,200`, `heapBlksVacuumed=7,789,371`, `indexVacuumCount=2`, and wait event `IO/WALSync` at 09:20:52Z. These are reported VACUUM progress counters, not a completion percentage.

Refresh this draft when both remaining final conditions are recorded: all three VACUUM processes exit 0, and each replica is caught up through its target LSN. Add later VMSS captures if they exist locally, recomputing the timestamp-deduplicated series from the latest capture per point. Do not infer a burst-expiry event or a 48 MB/s Premium SSD v2 cap from these observations.
