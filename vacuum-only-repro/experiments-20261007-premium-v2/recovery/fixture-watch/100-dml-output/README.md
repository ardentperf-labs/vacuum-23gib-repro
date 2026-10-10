# Original 100 GiB DML transcript verifier

`verify_delete_capture.py` is a local-only verifier for the two preserved
`psql_long` captures in this directory. It checks the pinned manifest hashes,
all recorded initial file sizes and SHA-256 values, the original running
process receipt/script/helper binding, seed path and query, pod UID, final raw
output, and exit-status bytes. It performs no Azure, Kubernetes, database, or
stack-state calls.

Root-owned audit publication code can verify and supplement a completed
100 GiB audit before publishing it:

```python
from verify_delete_capture import supplement_audit

verified_audit = supplement_audit(audit)
```

The function returns a deep copy. It attaches `checks.original100GiBFixtureDmlCapture`
with the verified DELETE proof, marks the original DELETE/CHECKPOINT output
captured and the original INSERT output unavailable, and refines either the
frozen finalizer's exact generic raw INSERT/DELETE limitation or the
continuation adapter's exact corroborating-control-flow limitation. Other
factual limitations and external receipt references remain intact. The legacy
`rawInsertDeleteOutputCaptured` field is left unchanged; separate fields
identify the captured DELETE and unavailable INSERT explicitly. A focused test
passes actual `corrected_attribution()` output through the supplementer to
guard that schema and restore the frozen finalizer's exact source-derived
`pg_stat_all_tables` estimate condition while preserving continuation caveats.

Run the offline focused checks with:

```sh
python3 -B -m unittest discover -s recovery/fixture-watch/100-dml-output -p 'test_*.py' -v
```

The expected capture identities and hashes are pinned in the verifier source;
changing a raw file or either manifest requires a new separately reviewed
capture and verifier pin.
