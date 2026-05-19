"""Find collectors that may silently fail because they don't override .collect().

For each collector in the registry, check whether its class file contains a
'def collect(' override OR a 'def fetch_data' that itself calls save_to_bronze.
If neither, the collector relies on inherited BaseCollector.collect() which
only fetches — and the dispatcher path will never persist data.
"""
import os, sys, re
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()

from src.dispatcher.collector_registry import COLLECTOR_MAP as REGISTRY

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def audit(name, spec):
    mod = spec['module']
    cls = spec['class']
    file_path = PROJECT_ROOT / (mod.replace('.', os.sep) + '.py')
    if not file_path.exists():
        return name, cls, 'MISSING_FILE', str(file_path)

    src = file_path.read_text(encoding='utf-8', errors='ignore')

    # Find the class block
    m = re.search(rf'^class {re.escape(cls)}\b', src, re.M)
    if not m:
        return name, cls, 'CLASS_NOT_FOUND', None

    # Class body = from m.start() to next top-level "class " or EOF
    rest = src[m.start():]
    next_class = re.search(r'\n(class |if __name__)', rest)
    body = rest[: next_class.start()] if next_class else rest

    has_collect = bool(re.search(r'^\s{4}def collect\(', body, re.M))
    has_fetch_data = bool(re.search(r'^\s{4}def fetch_data\(', body, re.M))
    # Cheap check: does the class file persist anywhere (save_to_bronze, save_to_db, INSERT INTO bronze)
    persists = ('save_to_bronze' in body or
                'INSERT INTO bronze' in body or
                'INSERT INTO silver' in body or
                'self.persist' in body)

    if has_collect:
        return name, cls, 'OK (has collect override)', None
    if has_fetch_data and persists:
        return name, cls, 'OK (fetch_data persists inline)', None
    if persists:
        return name, cls, 'SUSPECT (persists but no collect override)', None
    return name, cls, 'NO PERSIST CODE', None


results = []
for name, spec in sorted(REGISTRY.items()):
    if not isinstance(spec, dict) or 'module' not in spec:
        continue
    results.append(audit(name, spec))

print(f"=== {len(results)} collectors audited ===\n")
buckets = {}
for r in results:
    buckets.setdefault(r[2], []).append(r)
for status in ['SUSPECT (persists but no collect override)', 'NO PERSIST CODE',
               'CLASS_NOT_FOUND', 'MISSING_FILE',
               'OK (has collect override)', 'OK (fetch_data persists inline)']:
    rows = buckets.get(status, [])
    if not rows: continue
    print(f"--- {status}: {len(rows)} ---")
    for r in rows:
        print(f"  {r[0]:<32s}  {r[1]}")
    print()
