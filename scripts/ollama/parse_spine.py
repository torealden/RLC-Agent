"""
Permit parse spine — drains bronze.source_documents (parse_status='pending')
through the local-LLM extractor, best-of-N union, loads to bronze, marks status.

Generalized downstream half of the acquisition architecture. v1 targets Title V
permits (doc_type='title_v_permit') but the queue/QC/load shape is source-agnostic.

Two GPUs, asymmetric by design (our convention):
  desktop 5080  -> qwen3-coder:30b   (heavy, accurate)   localhost:11434
  laptop  4060  -> qwen2.5:7b        (lighter grinder)   100.73.98.127:11434
Each permit is assigned to one endpoint and extracted N times there (best-of-N,
same model) -> union emission units by unit_id -> load -> mark. Two permits run
concurrently (one per endpoint) so both GPUs stay busy.

Dedup: a pending doc whose sha256 is already parse_status='parsed' (any key) is
skipped (the acquirer re-registered the legacy-parsed permits under new slugs).

Confidence gate: units appearing in >= ceil(N/2) runs = high; else medium; a
permit that yields 0 units across all runs = failed (don't poison bronze).
"""

import json
import math
import subprocess
import sys
import tempfile
import threading
import queue
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.documents import registry
from src.services.database.db_config import get_connection

EXTRACTOR = ROOT / "scripts" / "ollama" / "extract_titlev_permits.py"
LOADER = ROOT / "scripts" / "load_titlev_extractions_to_bronze.py"
CANONICAL = ROOT / "collectors" / "epa_echo" / "output" / "llm_titlev"   # loader reads here

ENDPOINTS = [
    # Desktop 5080 has 16GB VRAM; qwen3-coder:30b (18GB) only partially fits and spills to
    # CPU. qwen2.5:7b fits fully on GPU and is the model the working legacy batch used.
    # best-of-N=3 union compensates for 7b's weaker single-run accuracy. (2026-06-19)
    {"url": "http://localhost:11434",      "model": "qwen2.5:7b", "name": "desktop"},
    # Laptop 4060 (8GB) can't hold 7b + 32k KV on-GPU -> spills to CPU -> 12 min/run vs the
    # desktop's 27s (2026-06-19). Pull-based queue means it'd handle only ~10 permits while
    # adding timeout risk, so it's disabled. Re-enable with a smaller model/ctx if needed.
    # {"url": "http://100.73.98.127:11434",  "model": "qwen2.5:7b", "name": "laptop"},
]
N_RUNS = 3                       # best-of-N (variance memory: N>=3)
RUN_TIMEOUT = 1500               # per extraction subprocess (s)
NUM_CTX = 32768                  # qwen2.5:7b native context (n_ctx_train); the extractor's
                                 # 64k default forces RoPE extension + a KV cache that spills
                                 # to CPU on 8-16GB cards -> 25-min timeouts. 32k holds the
                                 # ~22k-token permit input and runs ~27s/run on GPU. (2026-06-19)


def _already_parsed_shas() -> set:
    with get_connection() as c:
        cur = c.cursor()
        cur.execute("SELECT DISTINCT sha256 FROM bronze.source_documents "
                    "WHERE parse_status='parsed' AND sha256 IS NOT NULL")
        return {r['sha256'] if isinstance(r, dict) else r[0] for r in cur.fetchall()}


def _extract_once(pdf: Path, endpoint: Dict, run_dir: Path) -> Optional[Dict]:
    run_dir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(EXTRACTOR), str(pdf),
           "--ollama-url", endpoint["url"], "--model", endpoint["model"],
           "--num-ctx", str(NUM_CTX), "--out-dir", str(run_dir), "--force"]
    try:
        subprocess.run(cmd, timeout=RUN_TIMEOUT, capture_output=True, text=True)
    except subprocess.TimeoutExpired:
        return None
    # extractor writes <run_dir>/<STATE>/<key>.json
    hits = list(run_dir.glob("*/*.json"))
    hits = [h for h in hits if not h.name.endswith(".raw.txt")]
    if not hits:
        return None
    try:
        return json.loads(hits[0].read_text(encoding="utf-8"))
    except Exception:
        return None


def _union(runs: List[Dict]) -> Dict:
    """Merge N extraction dicts: union emission units by unit_id, count agreement."""
    base = next((r for r in runs if r), {}) or {}
    units: Dict[str, Dict] = {}
    seen_count: Dict[str, int] = {}
    for r in runs:
        for u in (r or {}).get("emission_units", []) or []:
            uid = str(u.get("unit_id") or u.get("description", ""))[:120]
            if not uid:
                continue
            seen_count[uid] = seen_count.get(uid, 0) + 1
            # keep the richest version (most non-empty fields)
            if uid not in units or sum(1 for v in u.values() if v) > sum(1 for v in units[uid].values() if v):
                units[uid] = u
    n = len([r for r in runs if r])
    thresh = math.ceil(n / 2)
    merged_units = list(units.values())
    high = sum(1 for uid in units if seen_count[uid] >= thresh)
    confidence = "high" if merged_units and high / max(len(merged_units), 1) >= 0.6 else \
                 ("medium" if merged_units else "low")
    out = dict(base)
    out["emission_units"] = merged_units
    out["_bestofn"] = {"runs": n, "units": len(merged_units), "agreed_units": high}
    return out, confidence


def _process(doc: Dict, endpoint: Dict) -> str:
    pdf = Path(doc["local_path"])
    if not pdf.exists():
        registry.mark(doc["id"], "failed", error="pdf missing")
        return f"MISS {doc['source_key']}"
    runs = []
    with tempfile.TemporaryDirectory() as td:
        for i in range(N_RUNS):
            runs.append(_extract_once(pdf, endpoint, Path(td) / f"run{i}"))
        merged, conf = _union(runs)
        if not merged.get("emission_units"):
            registry.mark(doc["id"], "failed", method="ollama_bestofn",
                          model=endpoint["model"], error="0 units across runs")
            return f"FAIL {doc['source_key']} (0 units, {endpoint['name']})"
        # write canonical + load
        state = (merged.get("facility", {}) or {}).get("state") or "IA"
        key = pdf.stem
        canon = CANONICAL / state
        canon.mkdir(parents=True, exist_ok=True)
        cpath = canon / f"{key}.json"
        cpath.write_text(json.dumps(merged, indent=2), encoding="utf-8")
        # Load to bronze and FAIL LOUD if the loader errors. (A swallowed loader
        # crash once marked 256 permits 'parsed' while loading 0 rows. 2026-06-19)
        lr = subprocess.run([sys.executable, str(LOADER), "--state", state],
                            capture_output=True, text=True, timeout=300)
        if lr.returncode != 0:
            registry.mark(doc["id"], "failed", method="ollama_bestofn",
                          model=endpoint["model"],
                          error=f"loader rc={lr.returncode}: {(lr.stderr or '')[:160]}")
            return f"FAIL {doc['source_key']} (loader rc={lr.returncode}, {endpoint['name']})"
        registry.mark(doc["id"], "parsed", method="ollama_bestofn",
                      model=endpoint["model"], confidence=conf, output_ref=str(cpath))
        return f"OK   {doc['source_key']} ({len(merged['emission_units'])}u {conf}, {endpoint['name']})"


def run(limit: int = 30, doc_type: str = "title_v_permit"):
    parsed_shas = _already_parsed_shas()
    pending = registry.list_pending(doc_type=doc_type, limit=limit * 2)
    work = [d for d in pending if d.get("sha256") not in parsed_shas][:limit]
    # mark dupes skipped
    for d in pending:
        if d.get("sha256") in parsed_shas:
            registry.mark(d["id"], "skipped", error="sha256 already parsed")
    print(f"queue: {len(pending)} pending, {len(work)} to parse (rest dup/skipped)", flush=True)

    q = queue.Queue()
    for d in work:
        q.put(d)
    results = []
    lock = threading.Lock()

    def worker(ep):
        while True:
            try:
                d = q.get_nowait()
            except queue.Empty:
                return
            try:
                msg = _process(d, ep)
            except Exception as e:
                registry.mark(d["id"], "failed", error=str(e)[:200])
                msg = f"ERR  {d.get('source_key')}: {e}"
            with lock:
                results.append(msg)
                print(f"  [{len(results)}/{len(work)}] {msg}", flush=True)
            q.task_done()

    threads = [threading.Thread(target=worker, args=(ep,), daemon=True) for ep in ENDPOINTS]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return {"attempted": len(work), "results": results}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--doc-type", default="title_v_permit")
    args = ap.parse_args()
    out = run(limit=args.limit, doc_type=args.doc_type)
    print(out["attempted"], "attempted")
