# Standard HDD 1 MiB random-read scaling

This repeats the neighboring `fio-hdd-repro` experiment with fio `bs=1m` (1,048,576 bytes), synchronous `psync` reads, and one outstanding read per worker thread. It emphasizes throughput rather than IOPS. All chart axes are linear.

See [the measured findings and charts](REPORT.md). The main sweep, the final single-thread repeat, and the separate bandwidth-validation stack each retain their own raw artifacts.

The VM is Standard_D4s_v4 in East US, with uncached storage limits above the Standard HDD S20 disk's nominal 500 IOPS / 60 MB/s limits. The disk is an isolated Standard_LRS clone of the same populated 512 GiB retained snapshot used for the 4 KiB experiment. Host caching is None; the guest device and ext4 mount are read-only. Sixteen allocated 1 GiB PostgreSQL relation segments form the fixture.

Each point requests a 30-second ramp and 180-second measurement. The default thread sweep is 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 768, 1,024, 1,536, 2,048, and 3,072; it stops after a point's fio mean request latency reaches five seconds. Additional isolated points can be run with `--only-threads`. A repeat single-thread measurement uses `--single-validation`.

Linux can split a 1 MiB fio request into smaller block requests. fio completion latency measures the entire application request; Linux `r_await` and Azure latency may describe different request boundaries. Preserve all three rather than equating their latency or IOPS. Guest queue and maximum request-size settings are captured in `artifacts/guest.txt`.

For steady throughput, use guest and Azure bytes/second over the common measurement window. Native grouped fio rates use per-worker timing windows that can stagger at high concurrency. Linux await is weighted by completed block reads over complete one-second intervals. Azure summaries use complete LUN 0 one-minute buckets, with the same conservative 60-second leading guard used in the previous experiment. This generally selects one interior minute per point. Raw points and unguarded averages are retained.

Run from the `eastus/` region root with Azure CLI authentication. Initialize its
keeper manifest once as described in the [region README](../README.md):

```bash
python3 fio-hdd-1m-repro/provision.py
FIO_VM_IP=$(python3 -c 'import json; print(json.load(open("fio-hdd-1m-repro/state.json"))["ip"])')
ssh -i fio-hdd-1m-repro/ssh-key -o StrictHostKeyChecking=accept-new "fioadmin@$FIO_VM_IP" 'sudo cloud-init status --wait'
scp -i fio-hdd-1m-repro/ssh-key fio-hdd-1m-repro/guest-test.py "fioadmin@$FIO_VM_IP:/home/fioadmin/guest-test.py"
ssh -i fio-hdd-1m-repro/ssh-key "fioadmin@$FIO_VM_IP" 'nohup sudo python3 -u /home/fioadmin/guest-test.py > /home/fioadmin/guest-test.log 2>&1 < /dev/null &'
python3 fio-hdd-1m-repro/poll.py
# When complete, allow recent Azure metrics to arrive, then collect:
python3 fio-hdd-1m-repro/collect.py
python3 -m venv fio-hdd-1m-repro/.venv
fio-hdd-1m-repro/.venv/bin/pip install -r fio-hdd-1m-repro/requirements.txt
fio-hdd-1m-repro/.venv/bin/python fio-hdd-1m-repro/analyze.py
python3 fio-hdd-1m-repro/teardown.py
```

For the completed experiment's comparison overlay, run:

```bash
fio-hdd-1m-repro/.venv/bin/python fio-hdd-1m-repro/analyze.py --comparison-artifacts fio-hdd-1m-headroom/artifacts
```

The overlay uses only measured guest throughput and await from the separate Standard_D8ds_v4 validation VM. Its 192 MB/s uncached baseline provides headroom above the observed plateau; its CSV remains separate from the main sweep. The validation used four threads for ten minutes, then 144 and 160 threads for three minutes each. The main charts show the initial single-thread point; its repeat has a separate CSV under `artifacts/single-validation/analysis/`.

For another experiment, use a fresh directory without the old state or artifacts. Teardown verifies subscription and ownership, refuses resource-group deletion if any snapshot is inside it, and verifies that all subscription snapshots remain. The retained snapshot is never modified. The previous 4 KiB experiment and its artifacts remain separate.
