#!/usr/bin/env python3
"""Copy reviewed measurement artifacts into the local Git checkout without Git writes."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent
WORKSPACE = ROOT.parents[1]
DEST = WORKSPACE / 'vacuum-23gib-repro/vacuum-only-repro/experiments-20261012-walreceiver-sigterm'
SUB = '00000000-0000-0000-0000-000000000000'
records = []

def redact(text):
    text = text.replace(SUB, '00000000-0000-0000-0000-000000000000')
    text = text.replace(str(WORKSPACE), '<workspace>')
    # Refuse artifacts carrying known credential payloads; keep originals private.
    if re.search(r'AccountKey\s*=|SharedAccessSignature\s*=|-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----|"password"\s*:\s*"[^"*]+"', text, re.I):
        raise RuntimeError('Credential-like content requires review before copying')
    return text

def copy(source, relative):
    if not source.is_file(): return
    if source.suffix in ('.png', '.pdf'):
        raw = source.read_bytes(); result = raw
    elif source.name.endswith('.tar.gz'):
        # Full private controller archives remain in the experiment workspace;
        # timestamped text logs and all raw metrics are included instead.
        return
    elif source.suffix == '.gz':
        raw = source.read_bytes()
        result = gzip.compress(redact(gzip.decompress(raw).decode()).encode(), mtime=0)
    else:
        raw = source.read_bytes()
        result = redact(raw.decode()).encode()
        if source.suffix in ('.jsonl', '.log', '.json') and len(result) > 5 * 1024 * 1024:
            result = gzip.compress(result, mtime=0)
            relative = Path(str(relative) + '.gz')
    target = DEST / relative; target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(result)
    os.chmod(target, 0o755 if source.stat().st_mode & 0o111 else 0o644)
    records.append({'path': str(relative), 'sourceSha256': hashlib.sha256(raw).hexdigest(),
                    'publishedSha256': hashlib.sha256(result).hexdigest(), 'bytes': len(result)})

def main():
    DEST.mkdir(parents=True, exist_ok=True)
    for name in ('README.md', 'REPORT.md', 'DIAGNOSIS.md', 'comparison.json', 'experiment-plan.json', 'controller-events.jsonl',
                 'controller-status.json', 'process-exit-codes.json'):
        copy(ROOT / name, Path(name))
    for name in ('run-controller.py', 'build-comparison.py', 'publish-local.py', 'finish-invalid-pilot.py'):
        copy(ROOT / name, Path(name))
    for path in (ROOT / 'client').rglob('*'):
        if '__pycache__' not in path.parts: copy(path, Path('client') / path.relative_to(ROOT / 'client'))
    for path in (ROOT / 'shared').rglob('*'):
        if '__pycache__' in path.parts or path.name.startswith('source-'): continue
        copy(path, Path('shared') / path.relative_to(ROOT / 'shared'))
    for path in (ROOT / 'results').rglob('*'):
        if '__pycache__' not in path.parts and 'private' not in path.parts: copy(path, Path('results') / path.relative_to(ROOT / 'results'))
    for name in ('sigterm', 'control'):
        stack = ROOT / name
        for path in (stack / 'results').rglob('*'):
            if '__pycache__' in path.parts or 'private' in path.parts or path.name.startswith(('.pgpass', '.connection')): continue
            copy(path, Path(name) / 'results' / path.relative_to(stack / 'results'))
        for fname in ('run-config.json', 'provision-launch.json', 'prepare-launch.json', 'provision.log', 'prepare.log', 'test.log', 'instrumentation.log', 'teardown.log'):
            copy(stack / fname, Path(name) / fname)
        for fname in ('01-provision.sh', '02-prepare.sh', '03-test.py', '04-teardown.sh', 'teardown.py', 'plot.py'):
            copy(stack / fname, Path(name) / fname)
        for fname in ('prepare.json', 'cleanup.json'):
            copy(stack / '.state' / fname, Path(name) / 'evidence' / fname)
    (DEST / 'PUBLICATION-PROVENANCE.json').write_text(json.dumps({'redactions': ['Azure subscription ID', 'workspace prefix'],
        'privateOmissions': ['kubeconfig', 'credentials', 'source snapshot access metadata', 'controller tar archives', 'unredacted private diagnostic captures'],
        'files': records}, indent=2) + '\n')
    print(f'Copied {len(records)} files into {DEST}; no staging, commit, or push performed.')

if __name__ == '__main__': main()
