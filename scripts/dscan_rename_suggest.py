"""
Pass 2: Read an inventory CSV from dscan_inventory.py, send each file's
content excerpt to a local Ollama LLM, and append two columns to the CSV:
    topic           — short label of what the file actually is
    suggested_name  — proposed filename (no extension)

NO files are renamed. Output is review-only.

Usage:
    python scripts/dscan_rename_suggest.py docs/reference/dscan_Document.csv
    python scripts/dscan_rename_suggest.py <inv> --model qwen2.5:7b
    python scripts/dscan_rename_suggest.py <inv> --limit 20    # quick sample
    python scripts/dscan_rename_suggest.py <inv> --out custom_path.csv

Resumable: re-running picks up from where it stopped.

Recommended: run --limit 20 first, eyeball the suggestions, then run full.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
DEFAULT_MODEL = "qwen2.5:7b"

# Prompt is short and explicit. Low temperature for consistent structured output.
PROMPT_TEMPLATE = """You are helping triage files recovered from an old hard drive.
Given a filename and a short content excerpt, produce a JSON object describing what the file is.

Filename: {name}
Size: {size_kb} KB
Content excerpt:
---
{excerpt}
---

Respond ONLY with valid JSON in this exact format, nothing else:
{{"topic": "...", "suggested_name": "..."}}

Rules:
- topic: 4-10 words describing what the file IS (a category + scope). Be concrete.
  Good examples: "NASS soybean acreage data 2010-2020", "weekly corn market commentary",
  "trading strategy backtest results", "personal correspondence with broker",
  "weather data export from DTN".
  Bad examples: "data file", "spreadsheet", "document".
- suggested_name: a clear filename WITHOUT extension. snake_case, no spaces, max 60 chars.
  If the existing filename is already meaningful, return it unchanged (without extension).
- If the content is unintelligible or the excerpt is empty, use:
  {{"topic": "unknown", "suggested_name": null}}
"""


def ollama_query(model: str, prompt: str, timeout: int = 60) -> str:
    """POST to Ollama's generate endpoint. Returns the raw response string."""
    r = requests.post(
        OLLAMA_URL,
        json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.2, "num_predict": 200},
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()["response"]


def parse_json_response(text: str) -> tuple[str | None, str | None]:
    """Extract topic + suggested_name from the LLM response."""
    text = text.strip()
    # Strip markdown code fences if the model added them
    if text.startswith("```"):
        text = text.split("```", 2)
        text = text[1] if len(text) > 1 else ""
        if text.startswith("json"):
            text = text[4:]
    # Find the first JSON object in the response
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0:
        return None, None
    try:
        obj = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None, None
    topic = obj.get("topic")
    name = obj.get("suggested_name")
    if isinstance(topic, str):
        topic = topic.strip()
    if isinstance(name, str):
        name = name.strip()
        if not name or name.lower() in ("null", "none"):
            name = None
    return topic, name


def warmup_ollama(model: str) -> None:
    """First request to a cold model takes ~30 sec to load weights into VRAM.
    Doing it once up front gives accurate throughput estimates."""
    print(f"Warming up {model} (loading weights into VRAM)...", flush=True)
    t0 = time.time()
    try:
        resp = ollama_query(model, "Reply with the JSON object {\"ok\": true}.", timeout=120)
        print(f"  ready in {time.time() - t0:.1f}s. Response: {resp[:80]}", flush=True)
    except Exception as e:
        print(f"  WARNING: warmup failed: {e}", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inventory", type=Path, help="Inventory CSV from dscan_inventory.py")
    ap.add_argument("--model", default=DEFAULT_MODEL,
                    help=f"Ollama model (default: {DEFAULT_MODEL})")
    ap.add_argument("--out", type=Path, default=None,
                    help="Output CSV path (default: <inventory>_renamed.csv)")
    ap.add_argument("--limit", type=int, default=0,
                    help="Stop after N LLM calls (0 = no limit; use --limit 20 for sampling)")
    ap.add_argument("--max-excerpt", type=int, default=1500,
                    help="Truncate content_excerpt to this many chars before sending")
    args = ap.parse_args()

    if not args.inventory.exists():
        sys.exit(f"Inventory not found: {args.inventory}")

    if args.out is None:
        args.out = args.inventory.with_name(args.inventory.stem + "_renamed.csv")

    # Load all rows
    with open(args.inventory, "r", encoding="utf-8", newline="") as f:
        all_rows = list(csv.DictReader(f))
    print(f"Inventory: {args.inventory.name} — {len(all_rows):,} rows", flush=True)

    # Resume: rows already in output get skipped
    seen_paths: set[str] = set()
    if args.out.exists():
        with open(args.out, "r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                seen_paths.add(row["rel_path"])
        print(f"Resume: {len(seen_paths):,} rows already in {args.out.name}", flush=True)

    # Verify Ollama is reachable
    try:
        requests.get("http://localhost:11434/api/tags", timeout=5).raise_for_status()
    except Exception as e:
        sys.exit(f"Ollama not reachable at localhost:11434 — is it running? ({e})")

    warmup_ollama(args.model)

    fieldnames = list(all_rows[0].keys()) + ["topic", "suggested_name", "llm_status"]
    write_header = not args.out.exists()

    f_out = open(args.out, "a", encoding="utf-8", newline="")
    writer = csv.DictWriter(f_out, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    if write_header:
        writer.writeheader()

    n_total = n_llm = n_skipped = n_err = 0
    n_remaining = sum(1 for r in all_rows if r["rel_path"] not in seen_paths)
    print(f"Files to process this run: {n_remaining:,}", flush=True)

    last_progress = time.time()
    t_start = time.time()

    try:
        for row in all_rows:
            if row["rel_path"] in seen_paths:
                continue
            n_total += 1

            new_row = dict(row)
            excerpt = (row.get("content_excerpt") or "").strip()
            status = row.get("extract_status", "")

            if status != "ok" or not excerpt:
                # No content to feed the LLM — write through with empty topic
                new_row["topic"] = ""
                new_row["suggested_name"] = ""
                new_row["llm_status"] = "skip:no_content"
                writer.writerow(new_row)
                n_skipped += 1
            else:
                prompt = PROMPT_TEMPLATE.format(
                    name=row["name"],
                    size_kb=int(row.get("size_bytes", 0) or 0) // 1024,
                    excerpt=excerpt[: args.max_excerpt],
                )
                try:
                    resp = ollama_query(args.model, prompt)
                    topic, name = parse_json_response(resp)
                    new_row["topic"] = topic or ""
                    new_row["suggested_name"] = name or ""
                    new_row["llm_status"] = "ok" if topic else "err:parse"
                    if not topic:
                        n_err += 1
                except requests.Timeout:
                    new_row["topic"] = new_row["suggested_name"] = ""
                    new_row["llm_status"] = "err:timeout"
                    n_err += 1
                except Exception as e:
                    new_row["topic"] = new_row["suggested_name"] = ""
                    new_row["llm_status"] = f"err:{type(e).__name__}"
                    n_err += 1
                writer.writerow(new_row)
                n_llm += 1

            now = time.time()
            if now - last_progress > 5:
                elapsed = now - t_start
                rate = n_llm / elapsed if elapsed > 0 else 0
                eta_min = (n_remaining - n_total) / rate / 60 if rate > 0 else 0
                print(f"  {n_total:>6,}/{n_remaining:,}  "
                      f"llm={n_llm:,} skip={n_skipped:,} err={n_err:,}  "
                      f"{rate:.1f} files/sec  ETA {eta_min:.0f} min", flush=True)
                last_progress = now
                f_out.flush()

            if args.limit and n_llm >= args.limit:
                print(f"\nHit --limit {args.limit}, stopping.")
                break
    except KeyboardInterrupt:
        print("\nInterrupted — partial progress saved. Re-run to resume.")
    finally:
        f_out.close()

    elapsed = time.time() - t_start
    print(f"\nDone. {n_total} rows processed in {elapsed/60:.1f} min")
    print(f"  LLM calls: {n_llm}  skipped (no content): {n_skipped}  errors: {n_err}")
    print(f"Output: {args.out}")


if __name__ == "__main__":
    main()
