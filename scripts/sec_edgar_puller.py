"""
SEC EDGAR filing puller.

Downloads SEC filings (8-K, 10-K, 10-Q, etc.) for one or more public
companies into `domain_knowledge/company_reports/{TICKER}/`. Idempotent
— skips filings already on disk by accession number.

Usage:
  # One ticker, all 8-Ks since 2024-01-01
  python scripts/sec_edgar_puller.py --ticker ADM --form 8-K --since 2024-01-01

  # Multiple forms
  python scripts/sec_edgar_puller.py --ticker ADM --form 8-K,10-K,10-Q --since 2023-01-01

  # Use the starter ticker list
  python scripts/sec_edgar_puller.py --tickers-xlsx domain_knowledge/company_reports/public_company_tickers.xlsx \
      --form 8-K --since 2024-01-01 --skip "FRO,APPH"

  # Just list what would be downloaded (no actual fetch)
  python scripts/sec_edgar_puller.py --ticker ADM --form 8-K --since 2024-01-01 --dry-run

What it pulls:
  - Filing metadata (form, filing date, accession, primary document)
  - The primary document (usually .htm) for each filing — NOT the full
    submission ZIP. PDFs are not on EDGAR; what's there is the original
    HTML filed by the company. We save the HTML; conversion to PDF or
    structured text is a separate step.
  - Writes a manifest CSV per ticker so downstream steps know what to parse.

SEC compliance:
  - SEC's API docs require User-Agent identifying the requester.
    See https://www.sec.gov/os/accessing-edgar-data
  - Rate limit: 10 requests/sec max. We sleep 0.15s between requests
    to stay safely under.

Folder layout:
  domain_knowledge/company_reports/
    company_tickers_cache.json        # SEC's company_tickers.json, refreshed daily
    {TICKER}/
      manifest.csv                     # one row per filing
      {accession}_{form}_{date}/
        index.json                     # metadata
        primary_document.htm           # the filed document
        (other documents if present)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

import requests

# SEC rate-limit-friendly settings
USER_AGENT = os.environ.get(
    "SEC_EDGAR_USER_AGENT",
    "Round Lakes Commodities Research toremalden@gmail.com",
)
RATE_SLEEP_SEC = 0.15  # well under SEC's 10 req/sec limit
TIMEOUT_SEC = 30

REPORTS_DIR = ROOT / "domain_knowledge" / "company_reports"
TICKERS_CACHE = REPORTS_DIR / "company_tickers_cache.json"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})


# --- HTTP helpers -------------------------------------------------------------

def _get(url: str, **kwargs) -> requests.Response:
    """GET with rate-limit sleep + retry on 503."""
    for attempt in range(3):
        try:
            r = SESSION.get(url, timeout=TIMEOUT_SEC, **kwargs)
            time.sleep(RATE_SLEEP_SEC)
            if r.status_code == 503 and attempt < 2:
                # SEC throttle — back off
                time.sleep(2.0 * (attempt + 1))
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt == 2:
                raise
            time.sleep(1.0)
    raise RuntimeError(f"unreachable: {url}")


# --- ticker -> CIK ------------------------------------------------------------

def load_ticker_to_cik() -> dict[str, dict]:
    """
    Load SEC's company_tickers.json (ticker -> CIK lookup), refreshing
    the cache if older than 24h.

    Returns dict mapping uppercase ticker -> {cik_str, ticker, title}.
    """
    refresh = True
    if TICKERS_CACHE.exists():
        age = time.time() - TICKERS_CACHE.stat().st_mtime
        if age < 24 * 3600:
            refresh = False

    if refresh:
        url = "https://www.sec.gov/files/company_tickers.json"
        print(f"[ticker-cache] fetching {url}")
        r = _get(url)
        TICKERS_CACHE.parent.mkdir(parents=True, exist_ok=True)
        TICKERS_CACHE.write_text(r.text, encoding="utf-8")

    raw = json.loads(TICKERS_CACHE.read_text(encoding="utf-8"))
    # raw shape: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    out = {}
    for v in raw.values():
        out[v["ticker"].upper()] = {
            "cik_str": int(v["cik_str"]),
            "cik_padded": f"{int(v['cik_str']):010d}",
            "ticker": v["ticker"].upper(),
            "title": v["title"],
        }
    return out


# --- per-company submissions --------------------------------------------------

def fetch_submissions(cik_padded: str) -> dict:
    """Fetch the SEC submissions index for a CIK."""
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    r = _get(url)
    return r.json()


def list_filings(submissions: dict, forms: list[str], since: date | None = None) -> list[dict]:
    """
    Extract filings of the requested form types since `since` from a
    submissions doc. Returns list of dicts: {form, filing_date, accession,
    primary_document, primary_doc_description, items}.
    """
    recent = submissions.get("filings", {}).get("recent", {})
    n = len(recent.get("accessionNumber", []))
    filings = []
    for i in range(n):
        form = recent["form"][i]
        if form not in forms:
            continue
        filing_date = date.fromisoformat(recent["filingDate"][i])
        if since and filing_date < since:
            continue
        filings.append({
            "form": form,
            "filing_date": filing_date.isoformat(),
            "accession": recent["accessionNumber"][i],
            "primary_document": recent["primaryDocument"][i],
            "primary_doc_description": recent.get("primaryDocDescription", [""] * n)[i],
            "items": recent.get("items", [""] * n)[i],
            "report_date": recent.get("reportDate", [""] * n)[i],
        })

    # Older filings live in separate pagination files under filings.files[].name
    # We don't fetch those by default to stay in the 'recent ~1000' window;
    # if user passes since older than what's in 'recent', we'd need to walk
    # filings.files[] too. Most use cases care about last 1-3 years which is
    # well within recent.
    return filings


# --- per-filing download ------------------------------------------------------

ACCESSION_NORMAL = re.compile(r"^[\d-]+$")


def filing_dir_name(f: dict) -> str:
    acc = f["accession"].replace("-", "")
    return f"{acc}_{f['form'].replace('/', '_')}_{f['filing_date']}"


def filing_index_url(cik_int: int, accession: str) -> str:
    """https://www.sec.gov/Archives/edgar/data/{cik}/{accession-no-dashes}/index.json"""
    accn = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn}/index.json"


def filing_doc_url(cik_int: int, accession: str, doc_name: str) -> str:
    accn = accession.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accn}/{doc_name}"


def is_useful_doc(name: str, primary: str) -> bool:
    """
    Decide whether a document in a filing is worth downloading.

    Default policy: primary doc + any HTML exhibit (press releases, narrative).
    Skip XBRL technical files, schemas, images, zips, embedded JS/CSS.

    The valuable content in a typical 8-K is in `*-ex991*.htm` (Exhibit 99.1,
    the press release). The primary `.htm` is often just a cover sheet
    pointing to the exhibit. 10-K/Q have the substance in the primary doc
    plus longer narrative exhibits.
    """
    if name == primary:
        return True
    n = name.lower()
    # Skip technical / non-narrative files
    if n.endswith((".xsd", ".css", ".js", ".jpg", ".jpeg", ".png", ".gif",
                   ".zip", ".json", ".xml")):
        return False
    # Skip XBRL helpers (lab/pre/def/cal/htm.xml side files)
    if any(s in n for s in ("_lab.", "_pre.", "_def.", "_cal.", "_htm.xml",
                            "filingsummary", "metalinks", "show.js", "report.css")):
        return False
    # Skip the auto-generated R1.htm / R2.htm tabular reports (XBRL views)
    if re.match(r"^r\d+\.htm$", n):
        return False
    # Skip header-only HTML index pages
    if "index-headers" in n or n.endswith("-index.html"):
        return False
    # Keep .htm/.html (these are typically the exhibits we want)
    if n.endswith((".htm", ".html")):
        return True
    return False


def download_filing(cik_int: int, ticker: str, f: dict, out_root: Path,
                    download_all_docs: bool = False) -> dict:
    """
    Download useful documents (primary + exhibits) for one filing. With
    `download_all_docs=True`, grab everything.

    Returns a dict suitable for the manifest row.
    """
    fdir = out_root / filing_dir_name(f)
    fdir.mkdir(parents=True, exist_ok=True)

    # Get the index for this filing (lists all documents)
    idx_url = filing_index_url(cik_int, f["accession"])
    try:
        idx = _get(idx_url).json()
    except Exception as e:
        return {**f, "status": "index_failed", "error": str(e), "primary_path": ""}

    items = idx.get("directory", {}).get("item", [])
    primary_path = ""

    # Save the index
    (fdir / "index.json").write_text(json.dumps(idx, indent=2), encoding="utf-8")

    if download_all_docs:
        targets = items
    else:
        targets = [i for i in items if is_useful_doc(i["name"], f["primary_document"])]
    for it in targets:
        name = it["name"]
        out_file = fdir / name
        if out_file.exists():
            if name == f["primary_document"]:
                primary_path = str(out_file.relative_to(ROOT))
            continue
        try:
            r = _get(filing_doc_url(cik_int, f["accession"], name))
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_bytes(r.content)
            if name == f["primary_document"]:
                primary_path = str(out_file.relative_to(ROOT))
        except Exception as e:
            print(f"  [warn] failed to download {name}: {e}")

    return {**f, "status": "ok", "primary_path": primary_path,
            "filing_dir": str(fdir.relative_to(ROOT))}


# --- manifest writing ---------------------------------------------------------

MANIFEST_COLS = [
    "ticker", "cik", "form", "filing_date", "report_date", "accession",
    "items", "primary_doc_description", "primary_document",
    "primary_path", "filing_dir", "status", "error",
]


def update_manifest(manifest_path: Path, rows: list[dict]):
    """Append/update rows in the per-ticker manifest CSV. Keyed on accession."""
    existing: dict[str, dict] = {}
    if manifest_path.exists():
        with manifest_path.open(newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                existing[r["accession"]] = r

    for r in rows:
        existing[r["accession"]] = {k: r.get(k, "") for k in MANIFEST_COLS}

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MANIFEST_COLS)
        w.writeheader()
        w.writerows(existing[k] for k in sorted(existing.keys()))


# --- CLI orchestration --------------------------------------------------------

def pull_one_ticker(ticker: str, ticker_map: dict, forms: list[str],
                    since: date | None, dry_run: bool, all_docs: bool) -> int:
    ticker = ticker.upper()
    if ticker not in ticker_map:
        print(f"[{ticker}] NOT FOUND in SEC ticker list")
        return 0

    info = ticker_map[ticker]
    cik_padded = info["cik_padded"]
    cik_int = info["cik_str"]

    # Per the per-company folder convention (see
    # domain_knowledge/company_reports/README.md), SEC filings live at
    # <TICKER>/public_reports/sec_filings/<accession>_<form>_<date>/
    out_root = REPORTS_DIR / ticker / "public_reports" / "sec_filings"
    out_root.mkdir(parents=True, exist_ok=True)

    print(f"[{ticker}] CIK={cik_int}  ({info['title']})")
    try:
        sub = fetch_submissions(cik_padded)
    except Exception as e:
        print(f"  ERROR fetching submissions: {e}")
        return 0

    filings = list_filings(sub, forms, since=since)
    if not filings:
        print(f"  no {forms} filings since {since}")
        return 0

    print(f"  {len(filings)} filings to consider")

    if dry_run:
        for f in filings[:20]:
            print(f"    {f['filing_date']} {f['form']:<6} {f['accession']} "
                  f"items={f['items'][:50]} {f['primary_doc_description'][:40]}")
        if len(filings) > 20:
            print(f"    ... and {len(filings) - 20} more")
        return len(filings)

    rows = []
    for i, f in enumerate(filings):
        if i % 10 == 0:
            print(f"  [{i+1}/{len(filings)}] {f['filing_date']} {f['form']} {f['accession']}")
        rows.append({**download_filing(cik_int, ticker, f, out_root,
                                       download_all_docs=all_docs),
                     "ticker": ticker, "cik": cik_int})

    update_manifest(out_root / "manifest.csv", rows)
    return len(rows)


def parse_skip_list(s: str | None) -> set[str]:
    if not s:
        return set()
    return {t.strip().upper() for t in s.split(",") if t.strip()}


def load_tickers_xlsx(path: Path) -> list[str]:
    import openpyxl
    wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
    out = []
    for ws in wb.worksheets:
        for row in ws.iter_rows(values_only=True):
            for cell in row:
                if cell and isinstance(cell, str):
                    s = cell.strip().upper()
                    if 1 <= len(s) <= 6 and s.isalpha():
                        out.append(s)
    return list(dict.fromkeys(out))  # dedup, keep order


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--ticker", help="single ticker, e.g. ADM")
    src.add_argument("--tickers", help="comma-separated tickers")
    src.add_argument("--tickers-xlsx", type=Path, help="xlsx with tickers (any column)")

    parser.add_argument("--form", default="8-K",
                        help="comma-separated forms (8-K, 10-K, 10-Q, DEF 14A). default: 8-K")
    parser.add_argument("--since", default=None,
                        help="ISO date (YYYY-MM-DD); default: 1 year ago")
    parser.add_argument("--all-docs", action="store_true",
                        help="download every document in each filing (default: primary only)")
    parser.add_argument("--skip", default=None,
                        help="comma-separated tickers to skip when using --tickers-xlsx")
    parser.add_argument("--dry-run", action="store_true",
                        help="list filings without downloading")

    args = parser.parse_args()

    if args.since:
        since = date.fromisoformat(args.since)
    else:
        since = date.today() - timedelta(days=365)

    forms = [f.strip() for f in args.form.split(",") if f.strip()]

    if args.ticker:
        tickers = [args.ticker]
    elif args.tickers:
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = load_tickers_xlsx(args.tickers_xlsx)

    skip = parse_skip_list(args.skip)
    tickers = [t for t in tickers if t.upper() not in skip]

    print(f"Plan: {len(tickers)} tickers, forms={forms}, since={since}, "
          f"dry_run={args.dry_run}")
    print(f"Output root: {REPORTS_DIR}")
    print()

    ticker_map = load_ticker_to_cik()
    print(f"Loaded SEC ticker map ({len(ticker_map)} tickers)")
    print()

    total = 0
    for t in tickers:
        total += pull_one_ticker(t, ticker_map, forms, since,
                                 args.dry_run, args.all_docs)

    print()
    print(f"Done. {total} filings touched across {len(tickers)} tickers.")


if __name__ == "__main__":
    main()
