# Historical source snapshot control metadata excerpt

This public excerpt preserves only the PostgreSQL control values cited by the
investigation. It was extracted from a CloudNativePG `VolumeSnapshot` metadata
capture made during preparation on 2026-10-04. The original local artifact's
SHA-256 is `d2f61fd32e5bcb5a410425d8562e2399aa00df703ecbdb84ddd83684729694db`; its full Kubernetes object is not included because its
embedded cluster manifest contains private Azure credential references, secret
version references, and resource identifiers. No credential value is included
here.

The saved `cnpg.io/pgControldata` annotation recorded:

| Control field | Value |
|---|---:|
| Database block size | 8192 bytes |
| Maximum data alignment | 8 bytes |
| Data page checksum version | 0 |

The saved cluster manifest recorded `wal_log_hints=on` under
`spec.postgresql.parameters`. These are historical source metadata, not a live
GUC query from either measured run. The system identifier is omitted from this
excerpt.
