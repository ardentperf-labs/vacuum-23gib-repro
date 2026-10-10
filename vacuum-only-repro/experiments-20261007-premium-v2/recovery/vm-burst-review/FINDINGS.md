# D2s_v4 and Premium SSD v2 bursting review

Checked read-only at **2026-10-06 22:49 UTC**. All three owned AKS node scale sets were `Standard_D2s_v4`, with two VM instances each. The VMSS metric-definition requests first verified each node resource group's `reprotoken` tag against its local stack owner token. The access token was held in process memory only; it was not written or printed.

Microsoft's Dsv4 table lists `Standard_D2s_v4` uncached **Premium SSD** capacity at 3,200 IOPS / 48 MB/s sustained and 4,000 IOPS / 200 MB/s burst, with a 30-minute maximum burst duration. That table does not provide a Premium SSD v2 burst column for Dsv4. Microsoft's Premium SSD v2 guidance for HANA says P2V has no disk or VM bursting, while the newer Lsv4 table exposes distinct P2V uncached and burst columns. Those sources show that burst support depends on the VM SKU and documented configuration; they do not establish a global P2V rule. For this experiment, D2s_v4/P2V bursting is unproven, and the published Dsv4 4,000/200 figures should not be applied to the four P2V DATA/WAL disks. This conclusion is limited to the documented D2s_v4 table and the absence of a D2s_v4/P2V-specific statement.

If VM-level credit bursting were applicable, the credit bucket starts full, can provide up to 30 minutes at burst rate, refills while below target, and takes less than a day to replenish. That makes preparation history and the idle interval worth measuring. In the 22:49 UTC sample, the Azure Monitor scale-set aggregate reported **0% VM remote burst I/O credits used and 0% VM remote burst bandwidth credits used for every available 5-minute point** since the node pools launched around 15:36 UTC. Baseline preparation completed at 21:24 UTC and had been idle about 85 minutes; the 50-GiB and 100-GiB preparations were still running. The available evidence therefore shows no VM burst-credit consumption during these preparations; it does not prove D2s_v4/P2V burst eligibility or ineligibility. The 50/100 runs still need a fresh metric sample after preparation completes and before the VACUUM window.

The same scale-set samples reported maximum VM uncached IOPS consumed percentages of 13.1% / 6.1% / 4.0% and maximum uncached bandwidth consumed percentages of 50.0% / 50.0% / 49.8% for baseline / 50 GiB / 100 GiB. These are scale-set-level time-series, not per-node values; they do not establish which VM generated the activity. I have not converted the bandwidth percentages into MB/s because Dsv4's published 48 MB/s figure is in its Premium SSD column and is not a stated Premium SSD v2 cap.

The VMSS instances returned 64 metric definitions each, including the remote burst-credit and uncached-utilization metrics. ARM `Metrics - List` calls against the individual VMSS VM child IDs returned 404 `NotFound` (both API versions `2018-01-01` and `2023-10-01`); the same read-only call against each parent VM scale set returned 200 and usable 5-minute series. Thus collection is safe and available at scale-set scope, but the current ARM provider mapping does not let this collector split the samples by VM. Keep that attribution limit in the report. The sanitized query outcomes and raw summaries are in `metric-query-outcomes.json`, `live-metric-definitions.json`, `vm-instance-metric-query-errors.json`, and `vmss-metric-samples.json`.

## Reusable collector validation

The read-only `collect_vmss_metrics.py --once` run at **2026-10-06 22:54 UTC** verified all three live resource-group owner tags and exact VMSS names, D2s_v4 sizes, and capacity two. Each VMSS metrics request returned 200 with six 5-minute points over 22:24–22:54 UTC. Both remote burst-credit metrics remained 0% in all three stacks. Uncached-bandwidth percentages ranged 42.4–49.9% for 50 GiB and 44.1–49.9% for 100 GiB; baseline was 0% in that window. These are VMSS aggregate averages, not per-node peaks. The sanitized CLI summary and raw public ARM response bodies are in `collected/vmss-metrics-20261006T225421Z.json`; the script is documented in `COLLECTOR.md`.

I recommend capturing the four VMSS aggregate series below immediately before, during, and after each measured run. They are host metrics, returned by Azure Monitor over a read-only ARM GET; no guest agent or workload is needed. Do not treat the old Dsv4 Premium SSD burst-column numbers as Premium SSD v2 limits. A separate unresolved point is the D2s_v4 **sustained** VM storage ceiling for Premium SSD v2: the Dsv4 page provides Premium SSD limits but no P2V-specific D2s_v4 row. The 50% uncached-bandwidth samples during preparation are below a cap but do not test the VACUUM workload.

Recommended metrics (5-minute averages):

- `VM Remote Used Burst IO Credits Percentage`
- `VM Remote Used Burst BPS Credits Percentage`
- `VM Uncached IOPS Consumed Percentage`
- `VM Uncached Bandwidth Consumed Percentage`

Microsoft references:

- [Dsv4 size series](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series) — D2s_v4's Premium SSD sustained/burst columns and 30-minute note.
- [Managed disk bursting](https://learn.microsoft.com/en-us/azure/virtual-machines/disk-bursting) — disk versus VM bursting, credit refill and duration.
- [Premium SSD v2 VM guidance](https://learn.microsoft.com/en-us/azure/sap/workloads/hana-vm-premium-ssd-v2) — explicit no disk or VM bursting for Premium SSD v2.
- [Lsv4 size series](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/storage-optimized/lsv4-series) — separate Premium SSD and Ultra/Premium SSD v2 VM performance columns.
- [Disk metrics](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-metrics) and [supported VM metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-compute-virtualmachines-metrics) — burst-credit and uncached-utilization metric definitions and cadence.
- [Azure Monitor Metrics - List REST API](https://learn.microsoft.com/en-us/rest/api/monitor/metrics/list) — read-only metric query shape, time range, interval, names, and aggregation.
