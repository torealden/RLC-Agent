"""
Embed the dscan-cataloged D-drive archive into silver.dscan_embeddings.

Reads docs/reference/dscan_Document_renamed.csv (output of Pass 2),
calls Ollama's /api/embed endpoint with nomic-embed-text on each row that
has a content excerpt, and stores the 768-dim vectors in PostgreSQL.

Designed to run on the 4060 laptop (Tailscale IP) so the desktop stays free
for interactive work. Resumable — re-running skips already-embedded files.

Usage:
    # Run on the laptop (4060)
    python scripts/embed_dscan_archive.py --ollama-url http://100.73.98.127:11434

    # Limit for smoke-test
    python scripts/embed_dscan_archive.py --limit 10
"""
from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
load_dotenv()

import psycopg2.extras
from src.services.database.db_config import get_connection


DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_MODEL = "nomic-embed-text"
DEFAULT_INPUT = Path("docs/reference/dscan_Document_renamed.csv")
MAX_INPUT_CHARS = 1500   # truncate excerpts to keep embedding sizes consistent


def make_input_text(row: dict) -> str:
    """Build the text passed to the embedding model. We anchor with the
    LLM-suggested topic and name, then add the content excerpt — gives the
    embedding model both the curated label and the raw content signal."""
    topic = (row.get("topic") or "").strip()
    suggested = (row.get("suggested_name") or "").strip()
    excerpt = (row.get("content_excerpt") or "").strip()[:MAX_INPUT_CHARS]
    parts = [p for p in [topic, suggested, excerpt] if p]
    return " · ".join(parts)


def embed_one(ollama_url: str, model: str, text: str, timeout: int = 60) -> list[float] | None:
    """Call Ollama's /api/embed endpoint. Returns the vector, or None on error."""
    try:
        r = requests.post(
            f"{ollama_url}/api/embed",
            json={"model": model, "input": text},
            timeout=timeout,
        )
        r.raise_for_status()
        data = r.json()
        # New API: data['embeddings'] is a list of vectors
        embs = data.get("embeddings")
        if embs and len(embs) > 0:
            return embs[0]
        # Legacy: data['embedding'] singular
        return data.get("embedding")
    except Exception as e:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL)
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--batch-commit", type=int, default=50,
                    help="Commit every N rows so progress is preserved on interrupt")
    args = ap.parse_args()

    if not args.input.exists():
        sys.exit(f"Input not found: {args.input}")

    # Health check
    try:
        requests.get(f"{args.ollama_url}/api/tags", timeout=5).raise_for_status()
    except Exception as e:
        sys.exit(f"Ollama not reachable at {args.ollama_url}: {e}")

    # Verify model is available
    tags = requests.get(f"{args.ollama_url}/api/tags", timeout=5).json()
    model_names = [m["name"] for m in tags.get("models", [])]
    if not any(args.model in n for n in model_names):
        sys.exit(f"Model '{args.model}' not on {args.ollama_url}. Available: {model_names}")

    # Build skip-set from already-embedded rows
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT rel_path FROM silver.dscan_embeddings")
        seen = {r[0] if not isinstance(r, dict) else r["rel_path"] for r in cur.fetchall()}
    print(f"Resume: {len(seen):,} rows already embedded.", flush=True)

    # Load rows
    with open(args.input, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))
    print(f"Input: {args.input.name} — {len(rows):,} rows total", flush=True)

    # Filter: only rows with content + LLM-ok status, not yet embedded
    todo = [
        r for r in rows
        if r.get("rel_path") not in seen
        and r.get("llm_status") == "ok"
        and (r.get("content_excerpt") or "").strip()
    ]
    print(f"To embed this run: {len(todo):,}", flush=True)
    if args.limit:
        todo = todo[:args.limit]
        print(f"Limited to first {args.limit}", flush=True)
    if not todo:
        print("Nothing to do.")
        return

    print(f"Embedding via {args.ollama_url} model={args.model}", flush=True)

    n_ok = n_err = 0
    t0 = time.time()
    last_progress = t0
    batch_buffer: list[tuple] = []

    with get_connection() as conn:
        cur = conn.cursor()

        for i, row in enumerate(todo):
            text = make_input_text(row)
            if not text:
                continue
            vec = embed_one(args.ollama_url, args.model, text)
            if vec is None:
                n_err += 1
                continue

            try:
                size_bytes = int(row.get("size_bytes") or 0)
            except ValueError:
                size_bytes = 0

            batch_buffer.append((
                row["rel_path"],
                row.get("name") or "",
                row.get("ext") or "",
                size_bytes,
                row.get("topic") or "",
                row.get("suggested_name") or "",
                len(text),
                args.model,
                vec,
            ))
            n_ok += 1

            # Commit every batch_commit
            if len(batch_buffer) >= args.batch_commit:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO silver.dscan_embeddings
                      (rel_path, file_name, ext, size_bytes, topic, suggested_name,
                       embedded_text_len, embedding_model, embedding)
                    VALUES %s
                    ON CONFLICT (rel_path) DO UPDATE SET
                       embedding = EXCLUDED.embedding,
                       embedding_model = EXCLUDED.embedding_model,
                       embedded_at = NOW()
                    """,
                    batch_buffer,
                )
                conn.commit()
                batch_buffer = []

            now = time.time()
            if now - last_progress > 10:
                rate = n_ok / (now - t0) if now > t0 else 0
                eta_min = (len(todo) - i) / rate / 60 if rate > 0 else 0
                print(f"  {i+1:>6,}/{len(todo):,}  ok={n_ok:,} err={n_err:,}  "
                      f"{rate:.1f}/sec  ETA {eta_min:.0f}min", flush=True)
                last_progress = now

        # Flush remaining
        if batch_buffer:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO silver.dscan_embeddings
                  (rel_path, file_name, ext, size_bytes, topic, suggested_name,
                   embedded_text_len, embedding_model, embedding)
                VALUES %s
                ON CONFLICT (rel_path) DO UPDATE SET
                   embedding = EXCLUDED.embedding,
                   embedding_model = EXCLUDED.embedding_model,
                   embedded_at = NOW()
                """,
                batch_buffer,
            )
            conn.commit()

    elapsed = time.time() - t0
    print(f"\nDone. {n_ok:,} embedded, {n_err:,} errors in {elapsed/60:.1f} min "
          f"({n_ok/elapsed:.1f}/sec)", flush=True)


if __name__ == "__main__":
    main()
