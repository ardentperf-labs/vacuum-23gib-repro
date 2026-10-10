# VMSS storage-metric collector

Run one read-only 30-minute sample with:

```sh
python3 recovery/vm-burst-review/collect_vmss_metrics.py --once
```

The optional `--until-test-receipts` mode refreshes its management token in
memory with a 120-second bound for each batch, samples every five minutes, and
stops after the three `.state/test-process.json` receipts reach a terminal state. It
requires the existing authorized `measurement-gate.json`, reads it only, and
does not start or control tests. Each sample queries the latest 30 minutes, so
the five-minute samples overlap to accommodate Azure Monitor publication delay.
If token refresh fails, it writes a sanitized supplementary-error record and
continues waiting for the receipts; it does not use a stale token or affect the
test processes or gate.

Before metric queries, the collector checks the local stack owner token against
the live node resource group's `reprotoken`, then requires the exact reviewed
VMSS name, `Standard_D2s_v4` size, and capacity of two. It acquires one Azure
management token with `az account get-access-token`, keeps it in memory, and
queries all three VMSSs concurrently. Successful Azure JSON response bodies are
saved locally under `collected/`; sanitized errors omit headers and raw error
bodies. Files are created with mode `0600` in a private `0700` directory.

The four collected metrics are `VM Remote Used Burst IO Credits Percentage`,
`VM Remote Used Burst BPS Credits Percentage`, `VM Uncached IOPS Consumed
Percentage`, and `VM Uncached Bandwidth Consumed Percentage`. They are five
minute Azure Monitor averages at the parent VMSS scope. They do not attribute
activity to a particular node, establish D2s_v4/Premium SSD v2 burst eligibility,
or provide a Premium SSD v2 IOPS/throughput limit. The collector never writes
experiment gates and its exit status is not consumed by the measurement
coordinator.

The Dsv4 page lists burst values under Premium SSD, while Lsv4 publishes separate
Ultra/Premium SSD v2 VM-performance columns; treat those tables as SKU-specific.
The [Dsv4 series](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/general-purpose/dsv4-series),
[Lsv4 series](https://learn.microsoft.com/en-us/azure/virtual-machines/sizes/storage-optimized/lsv4-series),
[managed disk bursting guidance](https://learn.microsoft.com/en-us/azure/virtual-machines/disk-bursting),
[Premium SSD v2 HANA guidance](https://learn.microsoft.com/en-us/azure/sap/workloads/hana-vm-premium-ssd-v2),
and [Azure Monitor disk metrics](https://learn.microsoft.com/en-us/azure/virtual-machines/disks-metrics)
are summarized in `FINDINGS.md`.
