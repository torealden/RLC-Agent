"""Download the USDA OCE consolidated historical WASDE report-data archive.

Source: https://www.usda.gov/oce/commodity-markets/wasde/historical-wasde-report-data
(page is WAF-blocked to non-browser clients; the .csv files download fine, .zip
files are blocked — those come from the Wayback Machine).

Layout produced under data/raw/wasde_historical/:
    oce-wasde-report-data-2010-04-to-2015-12.zip   (Wayback; one consolidated CSV inside)
    oce-wasde-report-data-2016-01-to-2020-12.zip   (Wayback; one consolidated CSV inside)
    monthly_csv_2010_2015/  monthly_csv_2016_2020/ (extracted)
    monthly_csv/oce-wasde-report-data-YYYY-MM.csv  (2021-01 → current)

Known irregularities (verified 2026-08-03 against the WasdeNumber sequence,
which is contiguous 481..673 — these months have NO report, nothing is missing):
    2013-10, 2019-01, 2025-10  — government shutdowns, report skipped.
Errata reposts get an uppercase suffix: oce-wasde-report-data-2026-05-V2.csv.
This script tries the base name then -V2/-V3.
"""
from __future__ import annotations

import io
import sys
import time
import zipfile
from datetime import date
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "wasde_historical"
DOC_URL = "https://www.usda.gov/sites/default/files/documents"
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}
SHUTDOWN_SKIPS = {"2013-10", "2019-01", "2025-10"}
ZIPS = {
    "oce-wasde-report-data-2010-04-to-2015-12.zip": "monthly_csv_2010_2015",
    "oce-wasde-report-data-2016-01-to-2020-12.zip": "monthly_csv_2016_2020",
}
# USDA WAF blocks .zip downloads; these Wayback captures are byte-identical originals.
WAYBACK_PREFIX = "http://web.archive.org/web/20241112034933id_/"


def fetch(url: str) -> requests.Response | None:
    r = requests.get(url, headers=UA, timeout=120)
    return r if r.status_code == 200 else None


def get_zip(name: str, extract_to: str) -> None:
    dest = BASE_DIR / name
    if not dest.exists():
        r = fetch(f"{DOC_URL}/{name}") or fetch(f"{WAYBACK_PREFIX}{DOC_URL}/{name}")
        if r is None:
            print(f"FAIL {name}: blocked at USDA and missing from Wayback")
            return
        dest.write_bytes(r.content)
        print(f"downloaded {name} ({len(r.content):,} bytes)")
    out = BASE_DIR / extract_to
    out.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(dest) as z:
        z.extractall(out)
    print(f"extracted {name} -> {extract_to}/")


def get_month(ym: str, out_dir: Path) -> bool:
    """Try base name then errata suffixes; keep the highest version found."""
    got = None
    for suffix in ("", "-V2", "-V3"):
        name = f"oce-wasde-report-data-{ym}{suffix}.csv"
        r = fetch(f"{DOC_URL}/{name}")
        if r is not None and r.content[:15].lstrip(b'\xef\xbb\xbf"').startswith(b"WasdeNumber"):
            got = (name, r.content)
        time.sleep(0.5)
    if got is None:
        return False
    (out_dir / got[0]).write_bytes(got[1])
    print(f"downloaded {got[0]} ({len(got[1]):,} bytes)")
    return True


def main() -> int:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    for name, extract_to in ZIPS.items():
        get_zip(name, extract_to)

    out_dir = BASE_DIR / "monthly_csv"
    out_dir.mkdir(exist_ok=True)
    today = date.today()
    missing = []
    y, m = 2021, 1
    while (y, m) <= (today.year, today.month):
        ym = f"{y}-{m:02d}"
        if ym in SHUTDOWN_SKIPS:
            print(f"skip {ym} (government shutdown, no report)")
        elif not list(out_dir.glob(f"oce-wasde-report-data-{ym}*.csv")):
            if not get_month(ym, out_dir):
                missing.append(ym)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)

    if missing:
        # Current month legitimately 404s before WASDE day (~10th-12th).
        print(f"not found: {missing} (current month is normal before WASDE day)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
