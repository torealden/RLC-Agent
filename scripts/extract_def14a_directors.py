"""
Extract director rosters from SEC DEF 14A (proxy statement) filings.

For each DEF 14A filing on disk:
  1. Read the primary HTML, strip to text.
  2. Send to Claude with a structured director-roster schema.
  3. Save extraction.json next to the filing.
  4. Optionally load into silver.director_appointment.

The valuable signal is each director's "other public boards" list — that
lets one DEF 14A filing yield multiple cross-company link rows. Combined
with the filings of the OTHER side, the gold.cross_company_director_links
view paints the inter-company tie graph.

Cloud (Claude) rather than local because (a) bios have nuance —
quantitative facts about other boards are less obvious than tabular
data, (b) volume is small (~50 filings across all public ag companies
ever), (c) output is client-facing.

Usage:
    # Single filing, print + save extraction.json
    python scripts/extract_def14a_directors.py --filing-dir <path>

    # All DEF 14A filings for a ticker
    python scripts/extract_def14a_directors.py --ticker ADM

    # All DEF 14A filings on disk
    python scripts/extract_def14a_directors.py --all-tickers

    # Load saved extractions into silver.director_appointment
    python scripts/extract_def14a_directors.py --load-to-db
"""

from __future__ import annotations

import argparse
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

import anthropic

from src.services.database.db_config import get_connection

REPORTS_DIR = ROOT / "domain_knowledge" / "company_reports"
SCHEMA_PATH = ROOT / "domain_knowledge" / "extraction_schemas" / "sec_def14a_v1.json"

DEFAULT_MODEL = "claude-sonnet-4-6"
PROMPT_VERSION = "def14a-v1-2026-05-09"
COST_IN_M = 3.00
COST_OUT_M = 15.00


# ---------------------------------------------------------------------------
# HTML -> text
# ---------------------------------------------------------------------------

def html_to_text(html: str) -> str:
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html,
                  flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r"</(p|div|tr|li|h[1-6]|table)>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"<[^>]+>", " ", html)
    html = (html.replace("&nbsp;", " ").replace("&#160;", " ")
                .replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", '"').replace("&#8217;", "'").replace("&#8220;", '"')
                .replace("&#8221;", '"').replace("&#8211;", "-").replace("&#8212;", "-"))
    html = re.sub(r"&[a-zA-Z#0-9]+;", " ", html)
    html = re.sub(r"[ \t]+", " ", html)
    html = re.sub(r"\n[ \t]+", "\n", html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    return html.strip()


def find_director_section(text: str) -> str:
    """Best-effort: locate the 'Election of Directors' / 'Nominees' section.

    Proxies are 100-300 pages. The table of contents references the
    directors section near the top (~chars 0-8K). The real bio section
    has a high density of 'Director since' / 'Age N' / 'Independent
    Director' markers — those are the anchor.

    Strategy:
      1. Find the FIRST 'Director since' marker past the TOC. That's the
         start of the bio section.
      2. Grab the next 80K chars (or stop at very obvious headers).
    """
    skip = 8000
    # All anchor positions across the doc
    anchor_re = re.compile(
        r"Director\s+(since|nominee\s+since)|Independent\s+Director|"
        r"\bAge\s+\d{2}\b",
        re.IGNORECASE,
    )
    positions = [m_.start() for m_ in anchor_re.finditer(text)]
    positions = [p for p in positions if p >= skip]

    if positions:
        # Find the densest cluster of anchors. Slide a 60K window over
        # the positions and pick the start that maximizes count inside
        # the window.
        best_count, best_start = 0, None
        for i, p in enumerate(positions):
            window_end = p + 60000
            count = sum(1 for q in positions[i:] if q <= window_end)
            if count > best_count:
                best_count = count
                best_start = p
            # tiny optimization: stop scanning once we're past the
            # last position (count won't grow)
            if window_end > positions[-1]:
                break
        if best_start is not None and best_count >= 5:
            start = max(skip, best_start - 500)
            section = text[start : start + 80000]
            # Only apply hard stops if we're well past the TOC region
            if start > 20000:
                for sp in [r"\bAUDIT\s+COMMITTEE\s+REPORT\b",
                           r"\bCOMPENSATION\s+DISCUSSION\s+AND\s+ANALYSIS\b",
                           r"\bDELINQUENT\s+SECTION\s+16\b"]:
                    m2 = re.search(sp, section, flags=re.IGNORECASE)
                    if m2 and m2.start() > 8000:
                        section = section[: m2.start()]
                        break
            return section
    # Old heading-based fallback (kept in case bio markers fail)
    patterns = [
        r"ELECTION\s+OF\s+DIRECTORS",
        r"NOMINEES\s+FOR\s+ELECTION",
        r"DIRECTOR\s+NOMINEES",
        r"BOARD\s+OF\s+DIRECTORS",
        r"INFORMATION\s+(REGARDING|ABOUT)\s+(DIRECTORS|NOMINEES)",
        r"DIRECTOR\s+BIOGRAPHIES",
    ]
    # Find every heading occurrence past char 8000 (skip TOC)
    skip = 8000
    candidates = []  # list of (start_offset, end_offset)
    for p in patterns:
        for m in re.finditer(p, text[skip:], flags=re.IGNORECASE):
            candidates.append(skip + m.start())
    candidates.sort()
    if not candidates:
        # Fallback: take the chunk that has the densest "Director Since"
        # / "Independent Director" markers
        positions = [m.start() for m in re.finditer(
            r"Director\s+(since|independent)|Independent\s+Director|"
            r"Age\s+\d{2}\b", text, flags=re.IGNORECASE
        )]
        if positions:
            return text[positions[0] : positions[0] + 80000]
        return text[skip : skip + 60000]

    # For each candidate start, take up to 80K chars and stop only at
    # very obvious "leave the directors section" headers. Pick the
    # candidate that yields the LARGEST useful chunk.
    stop_hard = [
        r"\bAUDIT\s+COMMITTEE\s+REPORT\b",
        r"\bCOMPENSATION\s+DISCUSSION\s+AND\s+ANALYSIS\b",
        r"\bDELINQUENT\s+SECTION\s+16\b",
        r"\bPROPOSAL\s+\d+\s*[\-:]",
        r"\bSTOCKHOLDER\s+PROPOSALS\b",
        r"\bRATIFICATION\s+OF\s+APPOINTMENT\b",
    ]
    best = ""
    for start in candidates:
        section = text[start : start + 80000]
        for sp in stop_hard:
            m2 = re.search(sp, section, flags=re.IGNORECASE)
            if m2 and m2.start() > 2000:  # don't truncate to <2K
                section = section[: m2.start()]
                break
        if len(section) > len(best):
            best = section
    return best


# ---------------------------------------------------------------------------
# Claude prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You extract structured board-of-directors information from
SEC DEF 14A proxy statements. Output a single JSON object matching the
provided schema. No prose. No markdown fences.

Critical rules:
1. Use null when the proxy doesn't supply a value. Never invent.
2. For the 'other_public_boards' field — list ONLY companies named
   explicitly in the bio as current public-company directorships. Skip
   private companies, advisory boards, and past directorships unless
   the bio says 'currently serves'.
3. Normalize role to one of: director, chair, lead_independent,
   audit_committee_chair, compensation_committee_chair,
   nominating_committee_chair, executive_chair.
4. Person names: full name as written in bio (e.g. 'Donald A. Smith').
5. Ages and tenure are integers when stated; null when not.
"""


def build_prompt(schema: dict, ticker: str, filing_date: str, text: str) -> str:
    return (
        f"Extract the board-of-directors roster from this DEF 14A proxy "
        f"statement for {ticker}, filed {filing_date}. "
        f"Output JSON matching this schema exactly:\n\n"
        f"{json.dumps(schema['sections'], indent=2)}\n\n"
        f"Director-section text:\n\n"
        f"{text[:60000]}\n\n"
        f"Emit only the JSON object."
    )


def call_claude(prompt: str, model: str = DEFAULT_MODEL,
                timeout: int = 600) -> tuple[dict, dict]:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY not set in .env")
    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=model,
        max_tokens=8192,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(b.text for b in msg.content if hasattr(b, "text")).strip()
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0].strip()
    return json.loads(text), {
        "input_tokens": msg.usage.input_tokens,
        "output_tokens": msg.usage.output_tokens,
        "model": msg.model,
    }


# ---------------------------------------------------------------------------
# Filing discovery
# ---------------------------------------------------------------------------

DIR_RE = re.compile(r"^(\d+)_(DEF 14A|DEF14A)([-_/A]+)?_(\d{4}-\d{2}-\d{2})$")


def discover_def14a_filings(tickers: list[str] | None = None,
                            all_tickers: bool = False) -> list[Path]:
    out = []
    if all_tickers:
        ticker_roots = [d for d in REPORTS_DIR.iterdir() if d.is_dir()]
    else:
        ticker_roots = [REPORTS_DIR / t for t in (tickers or []) if (REPORTS_DIR / t).exists()]
    for tdir in ticker_roots:
        # New layout: <TICKER>/public_reports/sec_filings/.
        # Legacy: <TICKER>/ directly. Scan both so unmigrated tickers
        # still work.
        scan_dirs = [
            tdir / "public_reports" / "sec_filings",
            tdir,  # legacy fallback
        ]
        for scan in scan_dirs:
            if not scan.exists():
                continue
            for sub in sorted(scan.iterdir()):
                if sub.is_dir() and DIR_RE.match(sub.name):
                    out.append(sub)
    return out


def parse_filing_dir(name: str) -> dict | None:
    m = DIR_RE.match(name)
    if not m:
        return None
    return {"accession_compact": m.group(1), "form": "DEF 14A",
            "filing_date": m.group(4)}


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

DEFAULT_SCHEMA = {
    "schema_name": "sec_def14a_v1",
    "schema_version": "1.0",
    "description": "Director roster + executive officers extracted from DEF 14A",
    "sections": {
        "filing_metadata": {
            "company_ticker": "string",
            "company_name": "string",
            "fiscal_year_disclosed": "integer (year of the annual meeting)",
            "filing_date": "ISO date"
        },
        "directors": [
            {
                "name": "Full name as in bio",
                "role": "director | chair | lead_independent | audit_committee_chair | compensation_committee_chair | nominating_committee_chair | executive_chair",
                "is_independent": "boolean or null",
                "age": "integer or null",
                "tenure_years_on_board": "integer or null",
                "bio_summary": "1-3 sentence paraphrase of bio. Mention prior employer if cited.",
                "other_public_boards": ["list of company names where this person currently serves on the public-company board (excluding the filer). Empty list if none stated."],
                "prior_employers_named": ["list of prior employer company names mentioned in the bio (e.g. 'Former CEO of X', 'Previously at Y')"]
            }
        ],
        "executive_officers": [
            {
                "name": "Full name",
                "title": "title as stated",
                "tenure_years": "integer or null",
                "prior_employers_named": ["list of prior employer companies if stated"]
            }
        ],
        "extraction_confidence": "high | medium | low",
        "extraction_notes": "any caveats"
    }
}


def load_schema() -> dict:
    if SCHEMA_PATH.exists():
        return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(json.dumps(DEFAULT_SCHEMA, indent=2), encoding="utf-8")
    return DEFAULT_SCHEMA


# ---------------------------------------------------------------------------
# Per-filing extraction
# ---------------------------------------------------------------------------

def extract_one(fdir: Path, model: str = DEFAULT_MODEL,
                force: bool = False) -> dict:
    out_path = fdir / "extraction.json"
    if out_path.exists() and not force:
        return {"status": "skipped", "filing_dir": fdir.name}

    info = parse_filing_dir(fdir.name)
    if not info:
        return {"status": "unrecognized", "filing_dir": fdir.name}

    htms = sorted(fdir.glob("*.htm"))
    if not htms:
        return {"status": "no_html", "filing_dir": fdir.name}

    # The primary HTML is usually the largest .htm
    htms.sort(key=lambda p: p.stat().st_size, reverse=True)
    primary = htms[0]
    text = html_to_text(primary.read_text(encoding="utf-8", errors="ignore"))
    section = find_director_section(text)

    schema = load_schema()
    ticker = fdir.parent.name
    prompt = build_prompt(schema, ticker, info["filing_date"], section)

    started = time.time()
    try:
        parsed, usage = call_claude(prompt, model=model)
    except json.JSONDecodeError as e:
        return {"status": "bad_json", "filing_dir": fdir.name,
                "error": f"Claude response not parseable as JSON: {e}"}
    except Exception as e:
        return {"status": "api_error", "filing_dir": fdir.name,
                "error": f"{type(e).__name__}: {e}"}
    elapsed = time.time() - started

    cost = (usage["input_tokens"] * COST_IN_M
            + usage["output_tokens"] * COST_OUT_M) / 1_000_000

    parsed["_provenance"] = {
        "schema": schema["schema_name"],
        "schema_version": schema["schema_version"],
        "prompt_version": PROMPT_VERSION,
        "model": usage["model"],
        "extracted_at": datetime.utcnow().isoformat() + "Z",
        "filing_meta": {
            "ticker": ticker,
            "form": "DEF 14A",
            "filing_date": info["filing_date"],
            "accession": info["accession_compact"],
            "primary_doc": primary.name,
            "section_chars_used": len(section),
        },
        "input_tokens": usage["input_tokens"],
        "output_tokens": usage["output_tokens"],
        "cost_usd": round(cost, 4),
        "elapsed_sec": round(elapsed, 1),
    }
    out_path.write_text(json.dumps(parsed, indent=2), encoding="utf-8")
    return {"status": "ok", "filing_dir": fdir.name, "cost_usd": cost,
            "elapsed_sec": elapsed,
            "n_directors": len(parsed.get("directors", []))}


# ---------------------------------------------------------------------------
# Loader: extraction.json → silver.director_appointment
# ---------------------------------------------------------------------------

def normalize_person(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace, drop middle initials."""
    n = re.sub(r"[^A-Za-z\s]+", " ", name or "").strip().lower()
    parts = [p for p in n.split() if len(p) > 1]  # drop initials
    return "_".join(parts)


def load_to_db(extractions_dir: Path = REPORTS_DIR) -> dict:
    """Walk all extraction.json files in DEF 14A subdirs and upsert
    silver.director_appointment rows."""
    inserted = 0
    seen_filings = 0
    files = []
    for tdir in extractions_dir.iterdir():
        if not tdir.is_dir():
            continue
        # Walk both <TICKER>/public_reports/sec_filings/ (new layout)
        # and <TICKER>/ directly (legacy).
        scan_dirs = [
            tdir / "public_reports" / "sec_filings",
            tdir,
        ]
        for scan in scan_dirs:
            if not scan.exists():
                continue
            for sub in scan.iterdir():
                if not sub.is_dir():
                    continue
                if not DIR_RE.match(sub.name):
                    continue
                ext = sub / "extraction.json"
                if ext.exists():
                    files.append((tdir.name, sub.name, ext))

    with get_connection() as conn:
        cur = conn.cursor()
        for ticker, fdir_name, ext_path in files:
            try:
                data = json.loads(ext_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            seen_filings += 1
            prov = data.get("_provenance", {})
            meta = prov.get("filing_meta", {})
            year = None
            try:
                year = (data.get("filing_metadata", {})
                            .get("fiscal_year_disclosed")
                            or int((meta.get("filing_date") or "")[:4]))
            except Exception:
                year = None

            for d in data.get("directors", []):
                name = (d.get("name") or "").strip()
                if not name:
                    continue
                pn = normalize_person(name)
                cur.execute(
                    """
                    INSERT INTO silver.director_appointment (
                        person_name, person_normalized, bio_summary,
                        operator, operator_ticker, role, is_independent,
                        last_disclosed_year,
                        source_form, source_filing_accession,
                        extracted_at, extracted_by
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                            'DEF 14A', %s, NOW(), 'def14a_extractor')
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        name, pn, d.get("bio_summary"),
                        data.get("filing_metadata", {}).get("company_name") or ticker,
                        ticker,
                        d.get("role") or "director",
                        d.get("is_independent"),
                        year,
                        meta.get("accession"),
                    ),
                )
                if cur.rowcount > 0:
                    inserted += 1

            # Also insert each "other_public_boards" mention as a separate
            # appointment row, so the cross-link view picks it up even if
            # the OTHER company's DEF 14A isn't on disk yet.
            for d in data.get("directors", []):
                name = (d.get("name") or "").strip()
                if not name:
                    continue
                pn = normalize_person(name)
                for other in (d.get("other_public_boards") or []):
                    if not other or not isinstance(other, str):
                        continue
                    cur.execute(
                        """
                        INSERT INTO silver.director_appointment (
                            person_name, person_normalized, bio_summary,
                            operator, role, last_disclosed_year,
                            source_form, source_filing_accession,
                            extracted_at, extracted_by, notes
                        )
                        VALUES (%s, %s, %s, %s, 'director', %s,
                                'DEF 14A (cross-ref)', %s, NOW(),
                                'def14a_extractor',
                                'Cross-reference from another company DEF 14A')
                        ON CONFLICT DO NOTHING
                        """,
                        (name, pn,
                         f"Cross-referenced from {ticker} DEF 14A {meta.get('filing_date', '')}",
                         other, year, meta.get("accession")),
                    )
                    if cur.rowcount > 0:
                        inserted += 1

        conn.commit()

    return {"filings_seen": seen_filings, "rows_inserted": inserted}


# ---------------------------------------------------------------------------
# Note: silver.director_appointment doesn't have a unique constraint, so
# ON CONFLICT DO NOTHING is a no-op (always inserts). To make the loader
# idempotent we'd need a unique index. For now we treat the loader as
# write-once and use --truncate before re-running.
# ---------------------------------------------------------------------------

def truncate_director_table():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE silver.director_appointment RESTART IDENTITY")
        conn.commit()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    src = parser.add_mutually_exclusive_group()
    src.add_argument("--filing-dir", type=Path)
    src.add_argument("--ticker")
    src.add_argument("--all-tickers", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--load-to-db", action="store_true",
                        help="After extraction (or alone), load saved "
                             "extraction.json files into silver.director_appointment")
    parser.add_argument("--truncate", action="store_true",
                        help="Wipe silver.director_appointment before loading "
                             "(use to re-run cleanly).")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    args = parser.parse_args()

    if args.truncate:
        print("Truncating silver.director_appointment...")
        truncate_director_table()
        print("Done.")

    # Extraction
    filings = []
    if args.filing_dir:
        filings = [args.filing_dir.resolve()]
    elif args.ticker:
        filings = discover_def14a_filings(tickers=[args.ticker])
    elif args.all_tickers:
        filings = discover_def14a_filings(all_tickers=True)

    grand_cost = 0.0
    counts = {"ok": 0, "skipped": 0, "bad_json": 0, "api_error": 0,
              "no_html": 0, "unrecognized": 0}
    for f in filings:
        print(f"--- {f.parent.name} / {f.name} ---")
        try:
            r = extract_one(f, model=args.model, force=args.force)
        except Exception as e:
            r = {"status": "fatal", "error": f"{type(e).__name__}: {e}"}
        counts[r.get("status", "fatal")] = counts.get(r.get("status", "fatal"), 0) + 1
        if r["status"] == "ok":
            grand_cost += r.get("cost_usd", 0)
            print(f"  ok: {r['n_directors']} directors  "
                  f"${r['cost_usd']:.4f}  {r['elapsed_sec']:.1f}s")
        else:
            err = r.get("error", "")
            print(f"  {r['status']}{(': ' + err[:120]) if err else ''}")
    if filings:
        print(f"\nSummary: {counts}")
        print(f"Total extraction cost: ${grand_cost:.4f}")

    # Load
    if args.load_to_db:
        print("\nLoading extractions to silver.director_appointment ...")
        r = load_to_db()
        print(f"  filings seen: {r['filings_seen']}")
        print(f"  rows inserted: {r['rows_inserted']}")


if __name__ == "__main__":
    main()
