# Standard_D2s_v4 storage ceiling and live I/O review

Checked 2026-10-09. This is an interim read-only review of the active run, not a final client-latency conclusion.

## Supported limits

Microsoft's Dsv4 size table lists `Standard_D2s_v4` as 2 vCPUs, with **3,200 uncached Premium SSD IOPS and 48 MB/s uncached Premium SSD throughput**. The same row lists Premium SSD burst ceilings of 4,000 IOPS / 200 MB/s, and the table notes burst may last up to 30 minutes. Microsoft defines MB/s here as decimal megabytes per second (`10^6` bytes/s). The Dsv4 family overview lists Premium SSD v2 as a supported disk type, but the per-size table is explicitly headed for Premium SSD; it does not publish a separate v2 per-size row. A new bounded read-only `az vm list-skus` response for `Standard_D2s_v4` in East US reports `UncachedDiskIOPS=3200` and `UncachedDiskBytesPerSecond=48000000` (48,000,000 bytes/s), among all 28 returned capabilities. The filtered actual SKU object is preserved in the [sanitized response file](azure-d2sv4-sku-raw-sanitized.json), captured at 2026-10-09 09:06:01.374406 UTC; the private raw stdout is `../.state/azure-evidence-requery-20261009/standard-d2sv4-eastus-correct-subscription.raw.json` (SHA-256 `31fe11d48a5ee386d55c6458389d2d03726cceb82b152cd7b17ff79465e38b69`). The SKU response contains no burst capability field. [Microsoft Dsv4 size series](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series)

This run's mounted DATA and WAL disks are Premium SSD v2, each configured to **3,000 IOPS and 125 MB/s**, with host caching disabled. Per instance, their combined configured disk-level values are 6,000 IOPS / 250 MB/s, before the VM aggregate cap. Microsoft documents Premium SSD v2's per-disk provisioned IOPS and throughput as separate limits and says cumulative disk performance must remain within the selected VM's limits. Premium SSD v2 does not support disk or VM bursting; therefore the Premium SSD burst figures above must not be applied to these v2 disks. [Premium SSD v2 limits](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-scalability-targets), [Premium SSD v2 deployment limitations](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-deploy-premium-v2), [Microsoft Premium SSD v2 VM guidance](https://learn.microsoft.com/en-us/azure/sap/workloads/hana-vm-premium-ssd-v2)

## Guest-observed 5-second samples

I derived interval rates from `run/results/samples.jsonl`, using each record's mapped DATA/WAL `majorMinor` devices and `/proc/diskstats` deltas (`r+w` for IOPS; `(rs+ws) * 512` bytes for throughput), divided by the elapsed sample timestamps. The reproducible derivation includes 189 consecutive VACUUM-to-VACUUM intervals of 4–6 seconds per role, from **2026-10-09 08:40:57.416 UTC** through **2026-10-09 08:56:37.341 UTC**. At the linked output's capture time, the derivation read 255 sample records through 2026-10-09 09:01:55.951829 UTC; its exact method and output are linked below. Throughput is decimal MB/s. These are guest block-device counters, not Azure's service-side completed-I/O counters.

| Instance / role | Device | IOPS p50 / p95 / max | MB/s p50 / p95 / max |
|---|---|---:|---:|
| VMSS `_0` / primary | DATA | 459 / 520 / 564 | 106 / 135 / 150 |
| VMSS `_0` / primary | WAL | 217 / 270 / 303 | 39 / 64 / 106 |
| VMSS `_0` / primary | DATA + WAL | 675 / 779 / 858 | 148 / 174 / 185 |
| VMSS `_1` / replica | DATA | 3,060 / 3,214 / 3,941 | 47 / 65 / 74 |
| VMSS `_1` / replica | WAL | 417 / 498 / 564 | 96 / 145 / 158 |
| VMSS `_1` / replica | DATA + WAL | 3,481 / 3,627 / 4,031 | 151 / 192 / 207 |

Compared with the nominal per-disk configuration, replica DATA guest counters are at/above 3,000 IOPS for most intervals, and replica WAL throughput exceeds 125 MB/s in some intervals; primary DATA throughput also exceeds 125 MB/s in some intervals. These guest rates cannot by themselves establish what Azure completed at the managed-disk boundary. In aggregate, the observed DATA+WAL request rates exceed the SKU's nominal 3,200 IOPS / 48 MB/s values, so they must not be presented as a direct measurement of remote storage delivery.

## Azure Monitor evidence

I repeated the VMSS-parent metric query with `VMName eq '*'`, PT1M grain, Average aggregation, over `2026-10-09 08:40–08:53 UTC`, using the broker-verified subscription from the run state. The raw response was captured at 2026-10-09 09:06:18.742327 UTC; its [sanitized response file](azure-d2sv4-monitor-raw-sanitized.json) preserves all returned datapoints and VMName dimensions. The private raw stdout is `../.state/azure-evidence-requery-20261009/vmss-parent-vmname-08-40-08-53-correct-subscription-correct-metrics.raw.json` (SHA-256 `5acb69ecd3e1b431ccdddfde48c0f8ab25a7c5f4db81f5ae16e1d41d113be655`). The returned per-VM metric series use VMName values `_0` and `_1`, matching the sample node/pod mappings. Microsoft's VMSS metrics reference defines both uncached-consumption metrics as per-minute percentages and the VM remote burst-credit metrics as percentages; it also documents per-data-disk consumption and burst-credit metrics. [Microsoft supported VMSS metrics](https://learn.microsoft.com/en-us/azure/azure-monitor/reference/supported-metrics/microsoft-compute-virtualmachinescalesets-metrics)

- **VM Uncached Bandwidth Consumed Percentage:** 100% on both `_0` and `_1` at every returned minute from 08:43 through 08:52 UTC.
- **VM Uncached IOPS Consumed Percentage:** `_1` was 100% from 08:43 through 08:52; `_0` ranged from 16% to 21% over those minutes.
- **VM Uncached Used Burst IO/BPS Credits Percentage:** returned values were zero or null, with no nonzero credit usage. Data Disk Used Burst IO/BPS Credits returned no series. This aligns with Premium SSD v2's no-bursting documentation.
- **Data Disk IOPS/throughput Consumed Percentage:** the response reported lower-than-100% data-disk consumption (roughly 58–59% IOPS and 73–78% bandwidth on `_1`; roughly 11–13% IOPS and 64–77% bandwidth on `_0` for most minutes). Azure returned only VMName metadata for these series, not separate LUN metadata, so these values do not distinguish DATA from WAL.

This is affirmative platform evidence that VM-level uncached bandwidth was saturated on both nodes and VM-level uncached IOPS was saturated on the replica during the observed minutes. The replica's guest IOPS readings also approach or exceed the 3,200 nominal VM figure. The Azure Monitor bandwidth percentage reaches 100% even though guest DATA+WAL counters report more than 48 MB/s. These measurements are at different layers and resolutions; retain both as separate evidence and do not reinterpret the guest counters as Azure-delivered throughput. The current evidence supports aggregate VM storage pressure more strongly than a per-disk bandwidth bottleneck, but does not by itself prove that the VM cap caused the pgbench latency or explain the guest/host throughput discrepancy. Correlate the final client-latency timeline with these minute metrics and keep any causal statement appropriately limited.

## Source and time notes

- Run plan: `experiment-plan.json` sets `nodeVmSize` to `Standard_D2s_v4`; sample device mappings bind primary to VMSS instance `_0` and replica to `_1`.
- The derivation input is [a captured 255-record prefix](azure-d2sv4-samples-input-capture.jsonl) of `run/results/samples.jsonl`, with latest sample at 2026-10-09T09:01:55.951829Z. Its SHA-256 is `f1db159b7ce81e4b0e5fe80ad4166a922aab9d6ae3065e60a89b8cdcb1c78334` and it matches the captured source hash in the output. The derivation uses 189 eligible intervals per role, ending at 08:56:37 UTC.
- Azure Monitor query covered 08:40:00–08:53:00 UTC; its latest datapoint was timestamped 08:52:00 UTC. Metrics use PT1M averages, unlike the guest's approximately five-second deltas.
- The first attempt to query metric values at nested VMSS-instance resource IDs returned an Azure Monitor `NotFound` resource-mapping response. The successful query used the VMSS parent plus the documented `VMName` dimension; no cloud resources were changed.


## Reproducibility and evidence files

- [Guest-rate derivation script](derive-azure-d2sv4-guest-rates.py) accepts a pinned `--samples` input (or defaults to the live run log); [its output](azure-d2sv4-guest-rate-derivation.json) records input bytes/SHA-256, interval counts, UTC bounds, percentile method, and per-device / combined rates. The linked input capture is the exact prefix matching that recorded input hash.
- [Azure evidence projection](azure-d2sv4-evidence-projection.json) remains a transcription of the earlier JMESPath-projected tool output; it is not a raw response capture. The new [sanitized Monitor response](azure-d2sv4-monitor-raw-sanitized.json) and [sanitized SKU object](azure-d2sv4-sku-raw-sanitized.json) are derived directly from new privately preserved raw stdout. They omit subscription/resource identifiers while retaining the Monitor datapoints/dimensions and all SKU capability fields. The original initial-query raw stdout was not saved.
- [SHA-256 manifest](azure-d2sv4-evidence.sha256) records hashes for this review and the linked evidence/derivation artifacts, including each private raw response hash embedded in its sanitized file.
