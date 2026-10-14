#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$ROOT/.state/diagnosis-required.json" ]]; then
  python3 - "$ROOT" <<'DIAGNOSIS_GATE'
import json,sys
from pathlib import Path
root=Path(sys.argv[1]);receipt=root/'results/diagnosis/verified-before-teardown.json'
if not receipt.exists() or json.loads(receipt.read_text()).get('readyForTeardown') is not True:
    raise SystemExit('Diagnostic evidence not yet verified; retaining test resources before teardown')
DIAGNOSIS_GATE
fi
exec python3 "$ROOT/teardown.py" "$@"
