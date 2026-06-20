"""
Retry title_v permits stuck in parse_status='failed' using CHUNKED extraction.

The single-shot extractor returns 0 units on very large multi-unit permits: the
JSON for 100+ units overflows the model's output budget and the input fills the
context window, leaving no room. Chunked mode splits the filtered text into small
page-chunks, extracts units per chunk (each well within budget), and unions them.
ADM Clinton: 0 -> 461 units this way (2026-06-20).

Runs on the desktop GPU with qwen2.5:7b, N=1 (the chunk fan-out already provides
broad coverage; best-of-N is less critical here). Writes canonical JSON, loads to
bronze via the fixed loader, marks parsed (units>0) or leaves failed (0 units).

  python scripts/ollama/retry_failed_chunked.py            # all failed
  python scripts/ollama/retry_failed_chunked.py --limit 2  # batch
"""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import scripts.ollama.extract_titlev_permits as ex
from scripts.ollama.extract_titlev_permits import process_pdf, warmup_model
from src.services.documents import registry
from src.services.database.db_config import get_connection

CANONICAL = ROOT / "collectors" / "epa_echo" / "output" / "llm_titlev"
LOADER = ROOT / "scripts" / "load_titlev_extractions_to_bronze.py"

OLLAMA_URL = "http://localhost:11434"
MODEL = "qwen2.5:7b"
CHAR_BUDGET = 250_000            # capture all unit pages of large permits
CHUNK_CHARS = 18_000             # per-chunk input; leaves ample output room at 32k ctx
NUM_CTX = 32768                  # qwen2.5:7b native


def _failed_docs(limit: int):
    with get_connection() as c:
        cur = c.cursor()
        cur.execute("SELECT id, source_key, local_path FROM bronze.source_documents "
                    "WHERE doc_type='title_v_permit' AND parse_status='failed' "
                    "ORDER BY source_key" + (f" LIMIT {int(limit)}" if limit else ""))
        return [dict(r) if isinstance(r, dict) else
                {"id": r[0], "source_key": r[1], "local_path": r[2]} for r in cur.fetchall()]


def run(limit: int = 0):
    ex.OLLAMA_BASE_URL = OLLAMA_URL
    docs = _failed_docs(limit)
    print(f"retrying {len(docs)} failed permit(s) with chunked extraction", flush=True)
    warmup_model(MODEL)

    results = []
    for i, d in enumerate(docs):
        pdf = Path(d["local_path"])
        if not pdf.exists():
            registry.mark(d["id"], "failed", error="pdf missing")
            results.append((d["source_key"], "MISS", 0))
            continue
        r = process_pdf(pdf, MODEL, CANONICAL, force=True, char_budget=CHAR_BUDGET,
                        num_ctx=NUM_CTX, chunk_chars=CHUNK_CHARS)
        n = r.get("n_units", 0)
        if r["status"] == "ok" and n > 0:
            state = Path(r["out"]).parent.name
            subprocess.run([sys.executable, str(LOADER), "--state", state],
                           capture_output=True, text=True, timeout=600)
            registry.mark(d["id"], "parsed", method="ollama_chunked",
                          model=MODEL, confidence="medium", output_ref=r["out"])
            results.append((d["source_key"], f"OK ({state})", n))
        else:
            registry.mark(d["id"], "failed", method="ollama_chunked", model=MODEL,
                          error=f"{r['status']} ({n} units)")
            results.append((d["source_key"], "FAIL", n))
        print(f"  [{i+1}/{len(docs)}] {results[-1][1]:12s} {d['source_key']} -> {n} units", flush=True)

    ok = sum(1 for _, s, _ in results if s.startswith("OK"))
    print(f"\ndone: {ok}/{len(docs)} recovered", flush=True)
    return results


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    run(limit=args.limit)
