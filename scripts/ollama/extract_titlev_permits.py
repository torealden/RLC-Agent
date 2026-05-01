"""
LLM extraction layer for state Title V air permits.

Pairs with the regex-based collectors in collectors/epa_echo/. The regex layer
is fast and reliable for top-level summary fields (single capacity number,
facility name) but has limited recall on the per-emission-unit detail. This
script runs a local Ollama model over the full extracted permit text and asks
for structured JSON for every emission unit, throughput limit, and stated
capacity. Output is review-friendly and append-only.

Designed to work for any state — input is just a PDF path.

Usage:
  # Single facility
  python scripts/ollama/extract_titlev_permits.py \
      collectors/epa_echo/raw/agp_eagle_grove_titlev.pdf

  # All Iowa soy facility PDFs in raw/
  python scripts/ollama/extract_titlev_permits.py --all-iowa

  # Use a smaller model for speed
  python scripts/ollama/extract_titlev_permits.py <pdf> --model qwen2.5:7b

Output:
  collectors/epa_echo/output/llm_titlev/<state>/<facility_key>.json

Resumable — re-running skips facilities that already have output unless
--force is passed.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import requests

try:
    import pdfplumber
except ImportError:
    sys.exit("ERROR: pdfplumber required. pip install pdfplumber")


DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_MODEL = "qwen3-coder:30b"
OUT_DIR = Path("collectors/epa_echo/output/llm_titlev")

# Set at runtime in main()
OLLAMA_BASE_URL = DEFAULT_OLLAMA_URL

PROMPT = """You are an environmental engineer reviewing a state air-quality
Title V operating permit. Extract structured information about the facility
and every distinct emission unit (process equipment) named in the permit.

Output a SINGLE valid JSON object with this exact shape and nothing else:

{
  "facility": {
    "name": "<facility name as written>",
    "operator": "<parent company / operator if different>",
    "city": "<city>",
    "state": "<two-letter>",
    "permit_number": "<as written>",
    "expiration_date": "<YYYY-MM-DD or null>",
    "industry": "<one of: oilseed_crush, biodiesel, renewable_diesel, ethanol, wheat_milling, livestock_processing, oil_refining, other>",
    "facility_description": "<2-3 sentence description of what the facility does — written from the permit's narrative, not made up>"
  },
  "emission_units": [
    {
      "unit_id": "<EU/EP number as written, e.g. EU-001 or 1.01>",
      "description": "<plain-English description of the equipment or process>",
      "category": "<one of: receiving, storage, drying, conditioning, dehulling, expansion, extraction, desolventization, refining, bleaching, deodorizing, packaging, boilers, dryers, baghouses, scrubbers, cooling, transesterification, distillation, hydrotreating, fermentation, milling, slaughter, rendering, other>",
      "rated_capacity": <number or null>,
      "rated_capacity_unit": "<e.g. 'tons/hour', 'bushels/day', 'MMBtu/hr', 'mil gal/yr', null>",
      "throughput_limit": <number or null>,
      "throughput_limit_unit": "<unit, or null>",
      "control_devices": ["<list of associated control device IDs/names>"]
    }
  ],
  "facility_totals": {
    "primary_throughput": <number or null>,
    "primary_throughput_unit": "<e.g. 'tons soybean/day', 'mil gal biodiesel/yr', null>",
    "primary_throughput_basis": "<verbatim sentence from permit that supports this, or null>"
  },
  "extraction_notes": "<any caveats, ambiguities, or sections you couldn't parse — keep brief>"
}

Rules:
- Do NOT invent numbers. If a value is not explicitly in the permit text, use null.
- Quote-style fidelity: when a permit gives "250 tons/hour rated capacity," set rated_capacity=250 and rated_capacity_unit="tons/hour".
- Skip purely administrative units (insurance, monitoring, recordkeeping). Only include process equipment.
- For "category", use exactly one of the allowed values; if unsure, use "other".
- The output MUST be valid JSON parseable by json.loads(). No markdown fences, no commentary outside the JSON.
"""


def extract_pdf_text(pdf_path: Path, max_pages: int | None = None) -> str:
    """Extract text from PDF. Truncate to first max_pages if given."""
    parts = []
    with pdfplumber.open(pdf_path) as pdf:
        pages = pdf.pages[:max_pages] if max_pages else pdf.pages
        for i, page in enumerate(pages):
            t = page.extract_text() or ""
            parts.append(f"=== PAGE {i+1} ===\n{t}")
    return "\n\n".join(parts)


# Patterns that anchor the must-keep "Facility Description and Equipment List"
# section — universal across Iowa DNR Title V permits and similar in many
# other states. This is where the full enumeration of emission units lives.
ANCHOR_HEADING_PATTERNS = [
    r"\bFacility\s+Description\s+and\s+Equipment\s+List\b",
    r"\bEquipment\s+List\b",
    r"\bList\s+of\s+Emission\s+(?:Units?|Points?)\b",
    r"\bInsignificant\s+(?:Equipment|Activities)\s+List\b",
    r"\bPlant[-\s]Wide\s+Conditions?\b",
    r"\bFacility[-\s]Wide\s+Conditions?\b",
]

# Per-unit detail patterns. Catch headings introducing one emission unit's
# section, including "EU 1" (space, no colon), "EU-001:", "Emission Unit 1.01",
# "Emission Point ID Number: EP 5", etc.
UNIT_HEADING_PATTERNS = [
    r"^\s*Emission\s+(?:Unit|Point)(?:\s+ID)?\s+(?:Number\s*)?[:\-]?\s*(?:EU|EP)?[-\s\.]?\d",
    r"^\s*Emission\s+Unit\s+[\d\.]+",
    r"^\s*EU[-\s\.]?\d{1,4}[A-Z]?\b",
    r"^\s*EP[-\s\.]?\d{1,4}[A-Z]?\b",
    r"^\s*Section\s+\d+\.[A-Z\d]",
    r"^\s*\d+\.\d{1,3}\s+(?:[A-Z][a-z]+\s+){1,5}",
    r"^\s*(?:Operating\s+conditions|Equipment\s+description)\s+for\s+E[UP]",
    r"^\s*(?:Process|Equipment|Unit)\s+description\s*[:\-]",
    r"^\s*Pollutant\s*:\s*\w+",         # AGP per-unit pollutant heading
    r"^\s*Operational\s+Limits?\s+(?:&|and)\s+Requirements?",
]

# Capacity / throughput phrases
CAPACITY_HINT_PATTERNS = [
    r"\brated\s+(?:design\s+)?capacity\b",
    r"\bthroughput\s+limit\b",
    r"\bmaximum\s+(?:design|hourly|annual|operating)\s+(?:rate|throughput|capacity)\b",
    r"\bdesign\s+rate\b",
    r"\bprocess\s+throughput\b",
    r"\b\d{1,5}(?:\.\d+)?\s*(?:tons?|bushels?|bu|gal|gallons?|lb|pounds?)\s*(?:/|per)\s*(?:hr|hour|day|year|yr)",
    r"\b\d{1,4}(?:\.\d+)?\s*MMBtu/hr",
    r"\bmil(?:lion)?\s*gal(?:lons?)?\s*(?:/|per)\s*(?:yr|year)",
]

_ANCHOR_RE = re.compile("|".join(ANCHOR_HEADING_PATTERNS), re.IGNORECASE)
_UNIT_RE = re.compile("|".join(UNIT_HEADING_PATTERNS), re.IGNORECASE | re.MULTILINE)
_CAP_RE = re.compile("|".join(CAPACITY_HINT_PATTERNS), re.IGNORECASE)


def _score_page(text: str) -> tuple[int, int, int]:
    """Return (anchor_hits, unit_heading_hits, capacity_hits)."""
    return (
        len(_ANCHOR_RE.findall(text)),
        len(_UNIT_RE.findall(text)),
        len(_CAP_RE.findall(text)),
    )


def filter_relevant_pages(pdf_path: Path, context_pad: int = 1,
                          char_budget: int = 80_000) -> tuple[str, int, int]:
    """Extract emission-unit and equipment-list pages from a Title V PDF.

    Strategy:
      1. ALWAYS keep first 3 pages (facility identity / cover sheet).
      2. ALWAYS keep any page that anchors the "Equipment List" section,
         plus the next `context_pad` pages (the table itself often spans
         multiple pages).
      3. Then fill remaining char budget with highest-scoring per-unit pages
         (unit heading hits weighted 2x, capacity hints 1x).

    Returns (filtered_text, n_pages_kept, n_pages_total).
    """
    page_texts = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_texts.append(page.extract_text() or "")

    n_total = len(page_texts)
    keep = [False] * n_total

    # 1. First 3 pages always
    for i in range(min(3, n_total)):
        keep[i] = True

    # 2. Anchor pages + their successors (equipment list often spans 2-3 pages)
    for i, t in enumerate(page_texts):
        if _ANCHOR_RE.search(t):
            for j in range(i, min(n_total, i + 4)):  # anchor + 3 following
                keep[j] = True

    chars_used = sum(len(page_texts[i]) for i in range(n_total) if keep[i])

    # 3. Score remaining pages and add until budget runs out
    scores = []
    for i, t in enumerate(page_texts):
        if keep[i]:
            scores.append((-1, i))  # already in
            continue
        anchor, unit, cap = _score_page(t)
        s = unit * 2 + cap
        scores.append((s, i))

    # Add by score descending
    for s, i in sorted(scores, key=lambda x: -x[0]):
        if s <= 0 or keep[i]:
            continue
        if chars_used + len(page_texts[i]) > char_budget:
            continue
        keep[i] = True
        chars_used += len(page_texts[i])
        # Optional: pad neighbors
        for j in range(max(0, i - context_pad), min(n_total, i + context_pad + 1)):
            if not keep[j] and chars_used + len(page_texts[j]) <= char_budget:
                keep[j] = True
                chars_used += len(page_texts[j])

    parts = []
    for i, k in enumerate(keep):
        if not k:
            continue
        parts.append(f"=== PAGE {i+1} ===\n{page_texts[i]}")

    return "\n\n".join(parts), sum(keep), n_total


def call_ollama(model: str, prompt: str, content: str,
                timeout: int = 1800, num_ctx: int = 65536) -> str:
    """POST to Ollama with prompt + content. Forces JSON-only output via the
    `format=json` parameter which constrains the model to emit valid JSON
    (prevents reverting to markdown summaries on long inputs)."""
    full_prompt = f"{prompt}\n\nPERMIT TEXT FOLLOWS:\n\n{content}"
    r = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": model,
            "prompt": full_prompt,
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.1,
                "num_predict": 16384,
                "num_ctx": num_ctx,
            },
        },
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()["response"]


def parse_json_response(text: str) -> dict | None:
    """Pull JSON object out of the LLM response."""
    text = text.strip()
    # Strip markdown fences if any
    if text.startswith("```"):
        # split on ``` and find the json block
        parts = text.split("```")
        for p in parts:
            p = p.strip()
            if p.startswith("json"):
                p = p[4:].strip()
            if p.startswith("{"):
                text = p
                break
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < 0:
        return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError as e:
        # Try to fix common issues — trailing commas
        cleaned = re.sub(r",(\s*[}\]])", r"\1", text[start : end + 1])
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            return None


def detect_state(pdf_path: Path) -> str:
    """Guess state from filename. Prefix-based (e.g. 'in_*') wins over
    operator-based hints because operators (ADM, Cargill, Bunge) run plants
    in many states."""
    name = pdf_path.stem.lower()
    # Strict 2-letter prefix
    prefix_map = {
        "in_": "IN", "ia_": "IA", "il_": "IL", "ne_": "NE",
        "mn_": "MN", "oh_": "OH", "mo_": "MO", "wi_": "WI",
        "ks_": "KS", "nd_": "ND", "sd_": "SD", "ar_": "AR",
        "tn_": "TN", "ky_": "KY", "tx_": "TX", "mt_": "MT",
    }
    for prefix, st in prefix_map.items():
        if name.startswith(prefix):
            return st
    # Substring fallback (legacy IA naming: agp_*, adm_*, cargill_*, bunge_*)
    state_hints = {
        "iowa": "IA",
        "indiana": "IN", "illinois": "IL", "nebraska": "NE",
        "minnesota": "MN", "ohio": "OH", "missouri": "MO",
    }
    for hint, st in state_hints.items():
        if hint in name:
            return st
    # Final fallback: legacy IA operators
    if any(name.startswith(op) for op in ("agp_", "adm_", "cargill_", "bunge_")):
        return "IA"
    return "UNKNOWN"


def process_pdf(pdf_path: Path, model: str, out_dir: Path, force: bool = False,
                char_budget: int = 80_000, num_ctx: int = 65536) -> dict:
    state = detect_state(pdf_path)
    facility_key = pdf_path.stem.replace("_titlev", "")
    out_path = out_dir / state / f"{facility_key}.json"

    if out_path.exists() and not force:
        return {"status": "skip:exists", "out": str(out_path)}

    out_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    print(f"[{facility_key}] reading PDF ({pdf_path.name})...", flush=True)
    text, n_kept, n_total = filter_relevant_pages(pdf_path, char_budget=char_budget)
    print(f"[{facility_key}]   filtered {n_kept}/{n_total} pages -> {len(text):,} chars; sending to {model}...", flush=True)

    try:
        raw = call_ollama(model, PROMPT, text, num_ctx=num_ctx)
    except requests.exceptions.Timeout:
        return {"status": "err:timeout", "facility": facility_key}
    except Exception as e:
        return {"status": f"err:{type(e).__name__}", "msg": str(e)[:200], "facility": facility_key}

    parsed = parse_json_response(raw)
    if parsed is None:
        # Save raw response for debugging
        debug_path = out_dir / state / f"{facility_key}.raw.txt"
        debug_path.write_text(raw, encoding="utf-8")
        return {
            "status": "err:json_parse",
            "facility": facility_key,
            "debug": str(debug_path),
        }

    # Wrap with provenance
    record = {
        "_source_pdf": str(pdf_path),
        "_extracted_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "_model": model,
        "_elapsed_sec": round(time.time() - t0, 1),
        **parsed,
    }
    out_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    n_units = len(parsed.get("emission_units", []))
    print(f"[{facility_key}]   ok in {record['_elapsed_sec']}s — {n_units} emission units", flush=True)
    return {"status": "ok", "facility": facility_key, "out": str(out_path), "n_units": n_units}


def warmup_model(model: str) -> None:
    print(f"Warming up {model} on {OLLAMA_BASE_URL}...", flush=True)
    t0 = time.time()
    try:
        r = requests.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model": model,
                "prompt": 'Reply with the JSON object {"ok": true}.',
                "stream": False,
                "options": {"temperature": 0, "num_predict": 30},
            },
            timeout=300,
        )
        r.raise_for_status()
        print(f"  ready in {time.time()-t0:.1f}s", flush=True)
    except Exception as e:
        print(f"  WARNING: warmup failed: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pdf", nargs="?", type=Path, help="Path to a Title V PDF")
    ap.add_argument("--all-iowa", action="store_true",
                    help="Process every IA-prefixed Title V PDF in collectors/epa_echo/raw/")
    ap.add_argument("--all-indiana", action="store_true",
                    help="Process every IN-prefixed Title V PDF in collectors/epa_echo/raw/")
    ap.add_argument("--state", default=None,
                    help="Filter PDFs by 2-letter state code (e.g. --state IA / --state IN)")
    ap.add_argument("--model", default=DEFAULT_MODEL,
                    help=f"Ollama model (default: {DEFAULT_MODEL})")
    ap.add_argument("--force", action="store_true",
                    help="Re-extract even if output already exists")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N PDFs (debug)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    ap.add_argument("--filter-only", action="store_true",
                    help="Run page filter only; print stats and skip LLM (debug)")
    ap.add_argument("--ollama-url", default=DEFAULT_OLLAMA_URL,
                    help=f"Ollama base URL (default: {DEFAULT_OLLAMA_URL}). "
                         f"For laptop via Tailscale: http://100.73.98.127:11434")
    ap.add_argument("--char-budget", type=int, default=80_000,
                    help="Max chars to send to LLM after page filter (default: 80,000)")
    ap.add_argument("--num-ctx", type=int, default=65536,
                    help="Ollama num_ctx context window (default: 65536)")
    args = ap.parse_args()

    global OLLAMA_BASE_URL
    OLLAMA_BASE_URL = args.ollama_url.rstrip("/")

    if not args.pdf and not args.all_iowa and not args.all_indiana and not args.state:
        ap.error("provide a pdf or --all-iowa / --all-indiana / --state XX")

    # Build PDF list
    if args.pdf:
        pdfs = [args.pdf]
    else:
        raw_dir = Path("collectors/epa_echo/raw")
        all_pdfs = sorted(raw_dir.glob("*_titlev.pdf"))
        if args.all_iowa:
            target_state = "IA"
        elif args.all_indiana:
            target_state = "IN"
        else:
            target_state = args.state.upper()
        pdfs = [p for p in all_pdfs if detect_state(p) == target_state]

    if args.limit:
        pdfs = pdfs[:args.limit]

    if not pdfs:
        sys.exit("No PDFs to process.")

    if args.filter_only:
        print(f"--filter-only: page filter stats over {len(pdfs)} PDF(s)")
        for pdf in pdfs:
            text, n_kept, n_total = filter_relevant_pages(pdf)
            pct = (n_kept / n_total * 100) if n_total else 0
            print(f"  {pdf.stem:40s} {n_kept:>3d}/{n_total:>3d} pages ({pct:>4.0f}%) -> {len(text):>7,} chars")
        return

    # Sanity check Ollama
    try:
        requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5).raise_for_status()
    except Exception as e:
        sys.exit(f"Ollama not reachable at {OLLAMA_BASE_URL}: {e}")

    warmup_model(args.model)

    print(f"Processing {len(pdfs)} PDF(s) with {args.model}", flush=True)
    results = []
    t_total = time.time()
    for pdf in pdfs:
        r = process_pdf(pdf, args.model, args.out_dir, force=args.force,
                        char_budget=args.char_budget, num_ctx=args.num_ctx)
        results.append(r)

    elapsed = time.time() - t_total
    n_ok = sum(1 for r in results if r["status"] == "ok")
    n_skip = sum(1 for r in results if r["status"].startswith("skip"))
    n_err = sum(1 for r in results if r["status"].startswith("err"))
    print(f"\nDone in {elapsed/60:.1f} min — ok={n_ok} skip={n_skip} err={n_err}", flush=True)
    if n_err:
        print("\nErrors:")
        for r in results:
            if r["status"].startswith("err"):
                print(f"  {r}")


if __name__ == "__main__":
    main()
