"""
SEC filing extractor — runs Ollama (qwen3-coder:30b by default) over each
filing in domain_knowledge/company_reports/{TICKER}/ and emits an
extraction.json next to the filing per the schema in
domain_knowledge/extraction_schemas/.

Designed for unattended overnight batch:
- Idempotent (skips filings with extraction.json unless --force)
- Reads form type from each filing dir name; picks correct schema
- 8-K: single prompt covering cover sheet + narrative exhibit
- 10-K/10-Q: chunked by section regex, merged into one extraction
- Logs to logs/sec_extract/{ticker}_{run_ts}.log
- Per-ticker manifest at extractions_manifest.csv

Usage:
  # Extract for one ticker (defaults to all forms / all filings without extraction.json)
  python scripts/extract_sec_filings.py --ticker ADM

  # Single filing dir
  python scripts/extract_sec_filings.py --filing-dir domain_knowledge/company_reports/ADM/000000708426000021_8-K_2026-05-05

  # Multiple tickers, only 8-K
  python scripts/extract_sec_filings.py --tickers ADM,BG,TSN --form 8-K

  # Force re-extract
  python scripts/extract_sec_filings.py --ticker ADM --force

  # Dry-run — list what would be extracted
  python scripts/extract_sec_filings.py --ticker ADM --dry-run

Environment:
  OLLAMA_HOST                  default http://localhost:11434
  SEC_EXTRACT_MODEL            default qwen3-coder:30b
  SEC_EXTRACT_MAX_CTX          default 32768 (model context window in tokens)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import requests

REPORTS_DIR = ROOT / "domain_knowledge" / "company_reports"
SCHEMAS_DIR = ROOT / "domain_knowledge" / "extraction_schemas"
LOGS_DIR = ROOT / "logs" / "sec_extract"

_raw_host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
# Normalize: env may have just 'host:port' without scheme
if _raw_host and not _raw_host.startswith(("http://", "https://")):
    _raw_host = f"http://{_raw_host}"
# 0.0.0.0 is a server-bind address, not a client connection address
OLLAMA_HOST = _raw_host.replace("0.0.0.0", "127.0.0.1")
MODEL = os.environ.get("SEC_EXTRACT_MODEL", "qwen3-coder:30b")
MAX_CTX_CHARS = int(os.environ.get("SEC_EXTRACT_MAX_CTX_CHARS", "15000"))  # ~3-4K tokens of input
NUM_CTX = int(os.environ.get("SEC_EXTRACT_NUM_CTX", "12288"))  # 12K — fits qwen3-coder:30b on 16GB VRAM
OLLAMA_TIMEOUT_SEC = int(os.environ.get("SEC_EXTRACT_OLLAMA_TIMEOUT", "240"))  # 4 min hard cap per call

# 10-K/Q sections we extract from. Other items (3-6, 8, 9, signatures, exhibits)
# are skipped to keep runtime bounded.
RELEVANT_ITEM_PREFIXES = ("Item 1.", "Item 1A.", "Item 2.", "Item 7.", "Item 7A.")

SCHEMA_8K_PATH = SCHEMAS_DIR / "sec_8k_v1.json"
SCHEMA_10KQ_PATH = SCHEMAS_DIR / "sec_10kq_v1.json"


# --- HTML -> text -------------------------------------------------------------

def html_to_text(html: str) -> str:
    """Cheap HTML → text. Preserves structure breaks via newlines."""
    # Drop scripts/styles
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html,
                  flags=re.DOTALL | re.IGNORECASE)
    # Convert block tags to newlines
    html = re.sub(r"</(p|div|tr|li|h[1-6]|table)>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    # Strip all remaining tags
    html = re.sub(r"<[^>]+>", " ", html)
    # Decode common entities
    html = (html.replace("&nbsp;", " ").replace("&#160;", " ")
                .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", '"').replace("&#8217;", "'").replace("&#8220;", '"')
                .replace("&#8221;", '"').replace("&#8211;", "-").replace("&#8212;", "-"))
    html = re.sub(r"&[a-zA-Z#0-9]+;", " ", html)
    # Collapse whitespace within lines, keep paragraph breaks
    html = re.sub(r"[ \t]+", " ", html)
    html = re.sub(r"\n[ \t]+", "\n", html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    return html.strip()


# --- Section chunking for long docs -------------------------------------------

ITEM_HEADER_RE = re.compile(
    r"\n(Item\s+\d+[A-Z]?\.?\s+[A-Z][A-Za-z—\s,\-/&]{4,80})", re.MULTILINE
)


def normalize_item_title(title: str) -> str:
    """Reduce 'Item 1A. RISK FACTORS' / 'Item 1A.' / 'Item 1a Risk Factors' to canonical form."""
    m = re.match(r"Item\s+(\d+[A-Z]?)\.?\s*(.*)", title.strip(), re.IGNORECASE)
    if not m:
        return title.strip()
    num = m.group(1).upper()
    rest = m.group(2).strip()
    return f"Item {num}." + (f" {rest}" if rest else "")


def is_relevant_item(title: str) -> bool:
    """Whether this Item section is one we extract from."""
    t = title.upper()
    return any(t.startswith(p.upper()) for p in RELEVANT_ITEM_PREFIXES)


def chunk_by_item_sections(text: str, max_chunk_chars: int = MAX_CTX_CHARS) -> list[dict]:
    """
    Split a 10-K/Q into Item-titled sections, then merge consecutive
    matches of the same Item (the regex over-matches because filings
    repeat 'Item 1. BUSINESS' as a running header). Returns
    [{title, content}] only for sections in RELEVANT_ITEM_PREFIXES.
    Each yielded chunk is at most max_chunk_chars long; long sections
    get split.
    """
    matches = list(ITEM_HEADER_RE.finditer(text))
    if not matches:
        # No Item headers — fall back to fixed chunks across whole doc
        chunks = []
        for i in range(0, min(len(text), max_chunk_chars * 6), max_chunk_chars):
            chunks.append({"title": f"chunk_{i // max_chunk_chars + 1}",
                           "content": text[i:i + max_chunk_chars]})
        return chunks

    # Build (start, end, normalized_title) ranges
    ranges = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        ranges.append((start, end, normalize_item_title(m.group(1))))

    # Merge consecutive same-title ranges (collapses repeated running-header matches)
    merged = []
    for s, e, t in ranges:
        if merged and merged[-1][2] == t and merged[-1][1] == s:
            merged[-1] = (merged[-1][0], e, t)
        else:
            merged.append([s, e, t])

    chunks = []
    for s, e, t in merged:
        if not is_relevant_item(t):
            continue
        content = text[s:e]
        if len(content) <= max_chunk_chars:
            chunks.append({"title": t, "content": content})
            continue
        # Long section: split, but cap to a reasonable max parts so we
        # don't burn an hour on a single risk-factors block
        max_parts = 6
        part_size = max(max_chunk_chars, len(content) // max_parts + 1)
        for j in range(0, len(content), part_size):
            chunks.append({
                "title": f"{t} (part {j // part_size + 1})",
                "content": content[j:j + part_size],
            })
    return chunks


# --- Ollama call --------------------------------------------------------------

def precheck_ollama() -> None:
    """
    Verify Ollama is reachable and the configured model is installed.
    Raises a clear error before we try to process any filings — without
    this, an unreachable Ollama or a typo'd model name would silently
    fail every filing in the batch (saw exactly that on a laptop run
    where the model name didn't match anything in `ollama list`).
    """
    # Reachability
    try:
        r = requests.get(f"{OLLAMA_HOST}/api/tags", timeout=5)
        r.raise_for_status()
    except requests.exceptions.ConnectionError as e:
        raise SystemExit(
            f"FATAL: cannot reach Ollama at {OLLAMA_HOST}. "
            f"Is `ollama serve` running? Original error: {e}"
        )
    except Exception as e:
        raise SystemExit(f"FATAL: Ollama health check at {OLLAMA_HOST}/api/tags "
                         f"failed: {e}")

    # Model availability
    available = [m["name"] for m in r.json().get("models", [])]
    if MODEL not in available:
        # Try matching base name (Ollama stores 'qwen3-coder:30b' but list has tags)
        base = MODEL.split(":")[0]
        partial = [m for m in available if m.startswith(base + ":") or m == base]
        msg = (
            f"FATAL: model '{MODEL}' is not installed on this Ollama instance.\n"
            f"  Host: {OLLAMA_HOST}\n"
            f"  Available models: {', '.join(available) if available else '(none)'}\n"
        )
        if partial:
            msg += f"  Did you mean: {', '.join(partial)} ?\n"
        msg += (
            f"\n"
            f"  Fix: either set SEC_EXTRACT_MODEL to one of the available names,\n"
            f"  or run `ollama pull {MODEL}` to install it.\n"
        )
        raise SystemExit(msg)


def ollama_extract(prompt: str, system: str, model: str = MODEL,
                   timeout: int = OLLAMA_TIMEOUT_SEC) -> str:
    """Call Ollama with format=json. Returns raw response text (the JSON).

    The timeout is BOTH connect and read — if Ollama doesn't return any
    bytes for `timeout` seconds, we abort. Setting this high is dangerous
    because long prompts can leave the GPU silent for minutes.
    """
    url = f"{OLLAMA_HOST}/api/generate"
    body = {
        "model": model,
        "prompt": prompt,
        "system": system,
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_ctx": NUM_CTX,
        },
    }
    # Use (connect_timeout, read_timeout) tuple for explicit control
    r = requests.post(url, json=body, timeout=(15, timeout))
    r.raise_for_status()
    return r.json()["response"]


# --- Schema loading -----------------------------------------------------------

def load_schema(form: str) -> dict:
    if form.startswith("8-K"):
        return json.loads(SCHEMA_8K_PATH.read_text(encoding="utf-8"))
    if form.startswith(("10-K", "10-Q")):
        return json.loads(SCHEMA_10KQ_PATH.read_text(encoding="utf-8"))
    return None


# --- Prompts ------------------------------------------------------------------

SYSTEM_INSTRUCTIONS = """You are a precise data extraction assistant.

Your task: read SEC filings and emit structured JSON conforming exactly
to the provided schema. Rules:

1. Output ONLY valid JSON. No prose, no markdown fences, no commentary.
2. If a field is not stated in the filing, use null (or empty array for
   list fields). Do NOT invent values.
3. Numbers should be JSON numbers, not strings. Use millions of USD
   unless the schema field name indicates otherwise (e.g. _usd_bn for
   billions). Convert if the filing uses different units.
4. ISO dates only: YYYY-MM-DD.
5. For lists like facilities or risk factors, include every distinct
   item you can find — but do not duplicate.
6. Keep summary text concise. No filler.
7. Confidence: 'high' if the source text is unambiguous; 'medium' if
   you had to interpret; 'low' if the text was largely silent.
"""


def build_8k_prompt(schema: dict, filing_meta: dict, text: str) -> str:
    return f"""Extract structured data from this SEC 8-K filing.

Filing metadata (already known):
- ticker: {filing_meta['ticker']}
- form: 8-K
- filing_date: {filing_meta['filing_date']}
- accession: {filing_meta['accession']}
- items_disclosed (from EDGAR): {filing_meta.get('items', '')}

Schema to populate (emit JSON matching this exact structure):
{json.dumps(schema['sections'], indent=2)}

Filing text (cover sheet + any exhibits):
{text[:MAX_CTX_CHARS]}

Emit only the JSON object."""


def build_10kq_section_prompt(schema: dict, filing_meta: dict,
                              section_title: str, section_text: str,
                              fields_for_section: list[str]) -> str:
    # Subset the schema sections relevant to this chunk
    return f"""Extract structured data from this section of an SEC {filing_meta['form']} filing.

Section: {section_title}
Filing metadata:
- ticker: {filing_meta['ticker']}
- form: {filing_meta['form']}
- filing_date: {filing_meta['filing_date']}

Focus on populating these schema sections (emit JSON with these keys plus
'extraction_confidence' and 'extraction_notes'):
{json.dumps({k: schema['sections'][k] for k in fields_for_section if k in schema['sections']}, indent=2)}

If a section's content does not pertain to your assigned fields, emit empty
arrays / null values rather than guessing.

Section text:
{section_text[:MAX_CTX_CHARS]}

Emit only the JSON object."""


# --- Filing extraction --------------------------------------------------------

FILING_DIR_RE = re.compile(r"^(\d+)_(8-K|10-K|10-Q)([-_/A]+)?_(\d{4}-\d{2}-\d{2})$")


def parse_filing_dir(name: str) -> dict | None:
    m = FILING_DIR_RE.match(name)
    if not m:
        return None
    return {
        "accession_compact": m.group(1),
        "form": m.group(2),
        "filing_date": m.group(4),
    }


def collect_filing_text(fdir: Path) -> str:
    """Concatenate all .htm content in a filing dir, primary first."""
    htms = sorted([p for p in fdir.glob("*.htm")])
    parts = []
    for p in htms:
        try:
            html = p.read_text(encoding="utf-8", errors="ignore")
            parts.append(f"=== Document: {p.name} ===\n" + html_to_text(html))
        except Exception as e:
            parts.append(f"=== Document: {p.name} (read error: {e}) ===")
    return "\n\n".join(parts)


SECTION_FIELD_MAP = {
    "Item 1.": ["business_overview", "filing_metadata"],
    "Item 1A.": ["risk_factors_top"],
    "Item 2.": ["facilities_named"],
    "Item 7.": ["segments", "capital_expenditures", "forward_guidance"],
    "Item 7A.": ["commodity_exposures"],
}


def fields_for_section(title: str) -> list[str]:
    for prefix, fields in SECTION_FIELD_MAP.items():
        if title.startswith(prefix):
            return fields
    return ["business_overview"]  # default


# For these list fields, dedupe by a name-like key rather than full JSON identity.
# An entry with the same name but more-filled scalar values wins.
LIST_DEDUPE_KEYS = {
    "segments": "name",
    "facilities_named": "name",
    "facilities_mentioned": "name",
    "risk_factors_top": "title",
    "forward_guidance": "metric",
    "commodity_exposures": "commodity",
    "ma_or_capital_allocation": "type",
    "leadership_changes": "name",
    "guidance_changes": "metric",
    "segment_commentary": "segment_name",
    "esg_or_sustainability_targets": "target",
}


def _entry_fill_score(d: dict) -> int:
    """How many non-null/non-empty scalar fields the entry has. Higher = more useful."""
    if not isinstance(d, dict):
        return 0
    s = 0
    for v in d.values():
        if v is None or v == "" or v == [] or v == {}:
            continue
        s += 1
    return s


def _merge_list_by_key(existing: list, incoming: list, key: str) -> list:
    """Dedupe by case-folded value at `key`; on collision keep the entry with more filled fields,
    plus shallow-merge scalar values from the loser into the winner where the winner had nothing."""
    by_k = {}
    order = []
    for x in existing + incoming:
        if not isinstance(x, dict):
            continue
        k = (x.get(key) or "").strip().lower()
        if not k:
            # No name to match — keep as anonymous, don't dedupe
            order.append(("__anon__", x))
            continue
        if k not in by_k:
            by_k[k] = x
            order.append((k, None))
        else:
            cur = by_k[k]
            if _entry_fill_score(x) > _entry_fill_score(cur):
                # Promote the more-filled entry; backfill from the loser
                for kk, vv in cur.items():
                    if x.get(kk) in (None, "", [], {}) and vv not in (None, "", [], {}):
                        x[kk] = vv
                by_k[k] = x
            else:
                # Keep current; backfill any empty fields from the new one
                for kk, vv in x.items():
                    if cur.get(kk) in (None, "", [], {}) and vv not in (None, "", [], {}):
                        cur[kk] = vv
    out = []
    for k, anon in order:
        if k == "__anon__":
            out.append(anon)
        elif k in by_k:
            out.append(by_k[k])
            del by_k[k]  # prevent re-adding on later same-k order entries
    return out


def merge_extractions(parts: list[dict]) -> dict:
    """Merge per-section extractions. Lists with known name fields dedupe by name;
    other lists union by JSON identity. Scalars: first-non-null wins."""
    out: dict = {}
    for p in parts:
        if not isinstance(p, dict):
            continue
        for k, v in p.items():
            if v is None or v == "" or v == []:
                continue
            if k not in out or out[k] is None or out[k] == "" or out[k] == []:
                out[k] = v
            elif isinstance(v, list) and isinstance(out[k], list):
                if k in LIST_DEDUPE_KEYS:
                    out[k] = _merge_list_by_key(out[k], v, LIST_DEDUPE_KEYS[k])
                else:
                    seen = {json.dumps(x, sort_keys=True) for x in out[k]}
                    for x in v:
                        s = json.dumps(x, sort_keys=True)
                        if s not in seen:
                            out[k].append(x)
                            seen.add(s)
            elif isinstance(v, dict) and isinstance(out[k], dict):
                # Shallow merge: prefer first non-null
                for kk, vv in v.items():
                    if kk not in out[k] or out[k][kk] in (None, "", [], {}):
                        out[k][kk] = vv
    return out


def extract_filing(fdir: Path, ticker: str, force: bool = False, log_lines: list = None) -> dict:
    """Extract one filing. Returns the result dict (also written to extraction.json)."""
    log = log_lines if log_lines is not None else []

    out_path = fdir / "extraction.json"
    if out_path.exists() and not force:
        log.append(f"  [skip] {fdir.name} (extraction.json exists)")
        return {"ticker": ticker, "filing_dir": fdir.name, "status": "skipped"}

    info = parse_filing_dir(fdir.name)
    if not info:
        log.append(f"  [skip] {fdir.name} (unrecognized dir name)")
        return {"ticker": ticker, "filing_dir": fdir.name, "status": "unrecognized"}

    schema = load_schema(info["form"])
    if not schema:
        log.append(f"  [skip] {fdir.name} (no schema for {info['form']})")
        return {"ticker": ticker, "filing_dir": fdir.name, "status": "no_schema"}

    text = collect_filing_text(fdir)
    if not text or len(text) < 500:
        log.append(f"  [skip] {fdir.name} (no text content)")
        return {"ticker": ticker, "filing_dir": fdir.name, "status": "empty"}

    filing_meta = {
        "ticker": ticker,
        "form": info["form"],
        "filing_date": info["filing_date"],
        "accession": info["accession_compact"],
    }

    started = time.time()
    log.append(f"  [run]  {fdir.name} (form={info['form']}, text={len(text)}c)")
    try:
        if info["form"] == "8-K":
            prompt = build_8k_prompt(schema, filing_meta, text)
            raw = ollama_extract(prompt, SYSTEM_INSTRUCTIONS)
            result = json.loads(raw)
        else:
            chunks = chunk_by_item_sections(text)
            log.append(f"         {len(chunks)} sections detected")
            parts = []
            for i, ch in enumerate(chunks):
                fields = fields_for_section(ch["title"])
                if not fields:
                    continue
                prompt = build_10kq_section_prompt(
                    schema, filing_meta, ch["title"], ch["content"], fields
                )
                t0 = time.time()
                raw = ollama_extract(prompt, SYSTEM_INSTRUCTIONS)
                try:
                    parts.append(json.loads(raw))
                    log.append(f"         [{i+1}/{len(chunks)}] {ch['title'][:60]:<60} {time.time()-t0:.0f}s")
                except json.JSONDecodeError as e:
                    log.append(f"         [{i+1}/{len(chunks)}] BAD JSON: {e}")
            result = merge_extractions(parts)

        # Add provenance
        result["_provenance"] = {
            "model": MODEL,
            "schema": schema["schema_name"],
            "schema_version": schema["schema_version"],
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            "elapsed_sec": round(time.time() - started, 1),
            "filing_meta": filing_meta,
        }

        out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
        log.append(f"  [ok]   {fdir.name}  ({time.time()-started:.0f}s)")
        return {"ticker": ticker, "filing_dir": fdir.name, "status": "ok",
                "elapsed_sec": round(time.time() - started, 1)}

    except Exception as e:
        log.append(f"  [fail] {fdir.name}: {e}")
        return {"ticker": ticker, "filing_dir": fdir.name, "status": "failed",
                "error": str(e)}


# --- Manifest -----------------------------------------------------------------

EXTRACT_MANIFEST_COLS = ["ticker", "filing_dir", "form", "filing_date", "status",
                         "elapsed_sec", "error"]


def update_extract_manifest(ticker_dir: Path, rows: list[dict]):
    p = ticker_dir / "extractions_manifest.csv"
    existing = {}
    if p.exists():
        with p.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                existing[r["filing_dir"]] = r
    for r in rows:
        info = parse_filing_dir(r["filing_dir"]) or {}
        existing[r["filing_dir"]] = {
            "ticker": r["ticker"],
            "filing_dir": r["filing_dir"],
            "form": info.get("form", ""),
            "filing_date": info.get("filing_date", ""),
            "status": r.get("status", ""),
            "elapsed_sec": r.get("elapsed_sec", ""),
            "error": r.get("error", ""),
        }
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXTRACT_MANIFEST_COLS)
        w.writeheader()
        w.writerows(existing[k] for k in sorted(existing.keys()))


# --- CLI ----------------------------------------------------------------------

def discover_filings_for_ticker(ticker: str, form_filter: list[str] | None) -> list[Path]:
    tdir = REPORTS_DIR / ticker
    if not tdir.exists():
        return []
    out = []
    for sub in sorted(tdir.iterdir()):
        if not sub.is_dir():
            continue
        info = parse_filing_dir(sub.name)
        if not info:
            continue
        if form_filter and info["form"] not in form_filter:
            continue
        out.append(sub)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--ticker", help="single ticker, e.g. ADM")
    src.add_argument("--tickers", help="comma-separated tickers")
    src.add_argument("--filing-dir", type=Path,
                     help="path to a single filing dir (override)")

    parser.add_argument("--form", default=None,
                        help="comma-separated form filter (e.g. 8-K,10-K). default: all")
    parser.add_argument("--force", action="store_true",
                        help="re-extract even if extraction.json exists")
    parser.add_argument("--dry-run", action="store_true",
                        help="list filings that would be extracted, don't run")

    args = parser.parse_args()

    # Fail fast if Ollama is unreachable or the model isn't installed,
    # rather than silently failing every filing in the batch.
    if not args.dry_run:
        precheck_ollama()
        print(f"Ollama precheck OK: model={MODEL} host={OLLAMA_HOST} num_ctx={NUM_CTX}")

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    form_filter = None
    if args.form:
        form_filter = [f.strip() for f in args.form.split(",") if f.strip()]

    if args.filing_dir:
        # Single dir mode
        fdir = args.filing_dir.resolve()
        ticker = fdir.parent.name
        log_lines = []
        log_path = LOGS_DIR / f"{ticker}_{run_ts}.log"
        result = extract_filing(fdir, ticker, force=args.force, log_lines=log_lines)
        log_path.write_text("\n".join(log_lines), encoding="utf-8")
        update_extract_manifest(fdir.parent, [result])
        print(f"Result: {result.get('status')}  log: {log_path}")
        return

    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]

    grand_total = {"ok": 0, "skipped": 0, "failed": 0, "empty": 0, "no_schema": 0, "unrecognized": 0}

    for ticker in tickers:
        filings = discover_filings_for_ticker(ticker, form_filter)
        print(f"\n[{ticker}] {len(filings)} filings to consider"
              f" (form_filter={form_filter}, force={args.force})")

        if args.dry_run:
            for fdir in filings[:30]:
                info = parse_filing_dir(fdir.name) or {}
                done = (fdir / "extraction.json").exists()
                marker = "[done]" if done and not args.force else "[run]"
                print(f"  {marker} {fdir.name}")
            if len(filings) > 30:
                print(f"  ... and {len(filings) - 30} more")
            continue

        log_lines = [f"=== SEC extract run {run_ts} ticker={ticker} ==="]
        log_path = LOGS_DIR / f"{ticker}_{run_ts}.log"
        results = []
        for i, fdir in enumerate(filings):
            print(f"  [{i+1}/{len(filings)}] {fdir.name}")
            r = extract_filing(fdir, ticker, force=args.force, log_lines=log_lines)
            results.append(r)
            grand_total[r.get("status", "failed")] = grand_total.get(r.get("status", "failed"), 0) + 1
            # Flush log periodically
            log_path.write_text("\n".join(log_lines), encoding="utf-8")

        update_extract_manifest(REPORTS_DIR / ticker, results)
        print(f"  done. log: {log_path}")

    print()
    print("Summary:", grand_total)


if __name__ == "__main__":
    main()
