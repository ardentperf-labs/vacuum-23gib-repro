# Local PostgreSQL reproduction

Run repro-local.sh from this directory. It creates a unique work directory
under /tmp, uses PostgreSQL 16, and leaves databases and logs there. If server
binaries are absent, the script downloads and extracts the Ubuntu server
package into that temporary directory; it does not install a package or start
a system service. PG16_BINDIR and PG16_LIBDIR can point at existing
PostgreSQL 16 binaries and libraries.

The fixture inserts 1,000 ordered rows with a 1,536-byte PLAIN bytea, adds a
B-tree, deletes every even ID, checkpoints, and runs VACUUM (FREEZE, VERBOSE).
It makes identical pre-vacuum cluster copies and replays the same WAL to the
same named restore point with recovery_prefetch=off and try. The script uses
unique Unix socket directories, no TCP listener, 16-MiB shared buffers, no
application workload, and no cache eviction. It stops only the PostgreSQL
servers it starts and leaves generated files under its work directory. It
resets the local I/O statistics before cloning and captures startup I/O again
after promotion, so pending recovery-process reports are flushed first.

The observed run recorded 200 prefetch requests with try and zero with off.
Both copies reached the same restore-point LSN. The first page's WAL begins
with FPI_FOR_HINT and then has PRUNE and FREEZE_PAGE records without images.
This establishes different prefetch requests for pages whose earlier WAL
images can satisfy redo. The local result does not compare physical disk
traffic because local cache residency affects whether Linux turns these
requests into device reads.

Useful outputs are the decoded WAL for heap block 0
([local-page0-main-wal.txt](local-page0-main-wal.txt)), the VACUUM log
([local-vacuum-verbose.txt](local-vacuum-verbose.txt)), the replay counters
([local-replay-results.json](local-replay-results.json)), and the WAL summary
([local-wal-summary.txt](local-wal-summary.txt)).

An independent fresh run of this script is saved under
[root-validation](root-validation/verification.json), with raw startup I/O and
prefetch counters beside it.
