"""
NASS Grain Crushings & Co-Products (GCCP) PDF collector.

The GCCP co-product OUTPUT data (distillers grains, corn gluten feed/meal,
corn germ meal, condensed distillers solubles, distillers corn oil, CO2) and
the dry-mill/wet-mill corn-consumed SPLIT are published ONLY in the monthly
GCCP release PDF — they are NOT in the NASS QuickStats API (verified: 467
QuickStats commodities, none of these). QuickStats exposes only the corn-usage
series (commodity_desc='CORN', statisticcat_desc='USAGE') and corn OIL via the
Fats & Oils report. This collector fills that gap.

Source: https://release.nass.usda.gov/reports/cagcMMYY.pdf  (MM/YY = release
month/year; each release covers data ~2 months prior — period is parsed from
the PDF header, not inferred from the filename).

Pipeline: PDF -> pdfplumber text -> regex parse (page 2 tables) -> cross-check
against page-1 narrative headline figures -> bronze.nass_processing
(source='NASS_GCCP').

Design note (LLM-cooperation testbed): the GCCP format is fixed and tabular, so
regex is the correct primary extractor — deterministic, free, verifiable. The
page-1 narrative restates a few headline numbers, giving us an independent
ground truth IN THE SAME DOCUMENT to auto-verify the table parse. A local-LLM
second-reader / reconciliation harness (transferable to the messier permit
PDFs) plugs in at verify(); see verify_against_narrative() for the v1 check.
"""

import os
import re
import sys
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import requests
import pdfplumber
from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")
logger = logging.getLogger(__name__)

GCCP_URL = "https://release.nass.usda.gov/reports/cagc{mm:02d}{yy:02d}.pdf"

# Map a GCCP PDF line label -> (commodity_desc for bronze, statisticcat_desc,
# raw unit). The label match is a case-insensitive substring of the line's
# leading text (before the dotted leader). Order matters: more specific labels
# first so "Dry mill"/"Wet mill" (fuel split) are caught before generic checks.
CORN_CONSUMED = [
    ("Beverage alcohol",     "CORN FOR BEVERAGE ALCOHOL",   "USAGE", "1000 BU"),
    ("Dry mill",             "CORN FUEL ALCOHOL DRY MILL",  "USAGE", "1000 BU"),
    ("Wet mill",             "CORN FUEL ALCOHOL WET MILL",  "USAGE", "1000 BU"),
    ("Fuel alcohol",         "CORN FOR FUEL ALCOHOL",       "USAGE", "1000 BU"),
    ("Industrial alcohol",   "CORN FOR INDUSTRIAL ALCOHOL", "USAGE", "1000 BU"),
    ("Total wet mill products other than fuel", "CORN WET MILL OTHER THAN FUEL", "USAGE", "1000 BU"),
]
CO_PRODUCTS = [
    ("Condensed distillers solubles",        "CONDENSED DISTILLERS SOLUBLES",      "PRODUCTION", "TONS"),
    ("Corn oil (Corn Distillers Oil",        "DISTILLERS CORN OIL",                "PRODUCTION", "TONS"),
    ("Distillers dried grains with solubles","DISTILLERS DRIED GRAINS W SOLUBLES", "PRODUCTION", "TONS"),
    ("Distillers dried grains (DDG)",        "DISTILLERS DRIED GRAINS",            "PRODUCTION", "TONS"),
    ("Distillers wet grains",                "DISTILLERS WET GRAINS",              "PRODUCTION", "TONS"),
    ("Modified distillers wet grains",       "MODIFIED DISTILLERS WET GRAINS",     "PRODUCTION", "TONS"),
    ("Corn germ meal",                       "CORN GERM MEAL",                     "PRODUCTION", "TONS"),
    ("Corn gluten feed",                     "CORN GLUTEN FEED",                   "PRODUCTION", "TONS"),
    ("Corn gluten meal",                     "CORN GLUTEN MEAL",                   "PRODUCTION", "TONS"),
    ("Wet corn gluten feed",                 "WET CORN GLUTEN FEED",               "PRODUCTION", "TONS"),
    ("Carbon dioxide captured",              "CARBON DIOXIDE CAPTURED",            "PRODUCTION", "TONS"),
]

_MONTHS = {m: i for i, m in enumerate(
    ["january","february","march","april","may","june","july","august",
     "september","october","november","december"], start=1)}
# 3 trailing numbers on a line: year-ago, prior-month, current-month
_THREE_NUMS = re.compile(r"([\d,]+)\s+([\d,]+)\s+([\d,]+)\s*$")


@dataclass
class GCCPResult:
    success: bool = False
    source: str = "nass_gccp"
    year: Optional[int] = None
    month: Optional[int] = None
    records: List[Dict] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


class NASSGrainCrushPDFCollector:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    # ---- dispatcher entry point -------------------------------------------
    def collect(self, **kwargs):
        """Dispatcher entry point. Fetch the current + prior release month
        (GCCP lands ~1st business day; over-fetching is safe — upserts are
        idempotent), parse, save to bronze."""
        from dataclasses import dataclass, field as dc_field
        from datetime import datetime as dt

        @dataclass
        class _Result:
            success: bool = False
            source: str = "nass_grain_crush_pdf"
            records_fetched: int = 0
            error_message: Optional[str] = None
            warnings: list = dc_field(default_factory=list)
            collected_at: dt = dc_field(default_factory=dt.now)
            data_as_of: Optional[str] = None

        res = _Result()
        now = dt.now()
        targets, y, m = [], now.year, now.month
        for _ in range(2):  # current + prior release month
            targets.append((y, m))
            m -= 1
            if m == 0:
                m, y = 12, y - 1
        total, periods = 0, []
        for ry, rm in targets:
            raw = self.download(rm, ry)
            if not raw:
                continue
            try:
                pr = self.parse(raw)
            except Exception as e:
                res.warnings.append(f"{ry}-{rm:02d}: parse error {e}")
                continue
            if not pr.success:
                res.warnings.append(f"{ry}-{rm:02d}: {pr.error_message}")
                continue
            total += self.save_to_bronze(pr.records)
            res.warnings += pr.warnings
            periods.append(f"{pr.year}-{pr.month:02d}")
        res.records_fetched = total
        res.data_as_of = ",".join(periods) if periods else None
        res.success = total > 0
        if not res.success:
            res.error_message = "no GCCP release available/parsed for current or prior month"
        return res

    # ---- fetch -------------------------------------------------------------
    def download(self, release_month: int, release_year: int) -> Optional[bytes]:
        url = GCCP_URL.format(mm=release_month, yy=release_year % 100)
        try:
            r = requests.get(url, timeout=60)
            if r.status_code == 404:
                self.logger.info(f"GCCP not released yet: {url}")
                return None
            if r.status_code != 200:
                self.logger.warning(f"GCCP HTTP {r.status_code}: {url}")
                return None
            return r.content
        except requests.RequestException as e:
            self.logger.warning(f"GCCP fetch error {url}: {e}")
            return None

    # ---- parse -------------------------------------------------------------
    def parse(self, pdf_bytes: bytes) -> GCCPResult:
        res = GCCPResult()
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            page1 = pdf.pages[0].extract_text() or ""
            if len(pdf.pages) < 2:
                res.error_message = "no page 2"
                return res
            rows = self._extract_rows(pdf.pages[1])

        # period from the corn-consumed header row
        hdr = next((r for r in rows if "corn consumed" in r["text"].lower()
                    and "united states" in r["text"].lower()), None)
        yr, mo = self._parse_period(hdr["text"]) if hdr else (None, None)
        if not yr:
            res.error_message = "could not parse data period from PDF header"
            return res
        res.year, res.month = yr, mo

        # section boundaries by row y. The Corn-Consumed table is bounded below
        # by the Sorghum-Consumed header (older PDFs carry a sorghum block whose
        # "Fuel alcohol"/"Dry mill" rows otherwise collide with corn labels).
        def y_of(substr):
            r = next((r for r in rows if substr in r["text"].lower()), None)
            return r["y"] if r else None
        corn_top = hdr["y"]
        corn_bot = y_of("sorghum consumed") or y_of("co-products and products produced") or 1e9
        coprod_top = y_of("co-products and products produced") or 1e9

        recs = []
        for r in rows:
            if not r["nums"]:
                continue  # sub-headers / label-only rows
            current = r["nums"][-1]   # rightmost column = current month
            if corn_top < r["y"] < corn_bot:
                recs += self._match(r["label"], current, CORN_CONSUMED, yr, mo)
            elif r["y"] > coprod_top:
                recs += self._match(r["label"], current, CO_PRODUCTS, yr, mo)
        res.records = recs

        res.warnings = self.verify_against_narrative(page1, recs)
        res.success = len(recs) >= 12  # expect ~17 line items
        if not res.success:
            res.error_message = f"only parsed {len(recs)} line items (expected ~17)"
        return res

    def _extract_rows(self, page) -> List[Dict]:
        """Group page words into rows by y. Each row: label (text before the
        dotted leader) + numeric values left-to-right. Positional, so it
        survives the long-dotted-leader layouts that break extract_text()."""
        words = page.extract_words(use_text_flow=False, keep_blank_chars=False)
        buckets: Dict[int, list] = {}
        for w in words:
            buckets.setdefault(round(w["top"] / 3), []).append(w)
        rows = []
        for key in sorted(buckets):
            ws = sorted(buckets[key], key=lambda x: x["x0"])
            text = " ".join(w["text"] for w in ws)
            nums = []
            for w in ws:
                t = w["text"].strip().lstrip(".").replace(",", "")
                if re.fullmatch(r"\d+(\.\d+)?", t):
                    nums.append((w["x0"], float(t)))
            nums.sort()
            rows.append({"y": key, "text": text,
                         "label": text.split(".")[0].strip(),
                         "nums": [v for _, v in nums]})
        return rows

    def _match(self, label: str, value: float, spec, yr: int, mo: int) -> List[Dict]:
        for needle, commodity_desc, stat, unit in spec:
            # startswith (not substring) so "Total wet mill products other than
            # fuel" != "Wet mill", "Modified distillers wet grains" != "Distillers
            # wet grains".
            if label.lower().startswith(needle.lower()):
                return [{
                    "commodity_desc": commodity_desc, "class_desc": "CORN",
                    "statisticcat": stat,
                    "short_desc": f"{commodity_desc} - {stat}, MEASURED IN {unit}",
                    "unit": unit, "year": yr, "month": mo,
                    "value": value, "source": "NASS_GCCP",
                }]
        return []

    def _parse_period(self, text: str) -> Tuple[Optional[int], Optional[int]]:
        m = re.search(r"United States:\s+([A-Za-z]+)\s+(\d{4})", text)
        if not m:
            return None, None
        return int(m.group(2)), _MONTHS.get(m.group(1).lower())

    # ---- persist -----------------------------------------------------------
    def save_to_bronze(self, records: List[Dict]) -> int:
        """Upsert into bronze.nass_processing (source='NASS_GCCP'), same shape
        and conflict key as the NASS processing collector."""
        import psycopg2
        conn = psycopg2.connect(
            host=os.environ.get("RLC_PG_HOST"), port=os.environ.get("RLC_PG_PORT", 5432),
            database=os.environ.get("RLC_PG_DATABASE", "rlc_commodities"),
            user=os.environ.get("RLC_PG_USER", "postgres"),
            password=os.environ.get("RLC_PG_PASSWORD"),
            sslmode=os.environ.get("RLC_PG_SSLMODE", "require"))
        n = 0
        try:
            cur = conn.cursor()
            for r in records:
                cur.execute("""
                    INSERT INTO bronze.nass_processing (
                        commodity_desc, class_desc, statisticcat_desc, short_desc,
                        unit_desc, domaincat_desc, year, reference_period_desc,
                        month, value, report_type, source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (commodity_desc, COALESCE(class_desc,''),
                        statisticcat_desc, short_desc, year, COALESCE(month,0),
                        COALESCE(domaincat_desc,'')) DO UPDATE SET
                        value=EXCLUDED.value, unit_desc=EXCLUDED.unit_desc,
                        collected_at=NOW()
                """, (r["commodity_desc"], r["class_desc"], r["statisticcat"],
                      r["short_desc"], r["unit"], "", r["year"], "", r["month"],
                      r["value"], "grain_crushings_pdf", r["source"]))
                n += 1
            conn.commit()
        finally:
            conn.close()
        return n

    def backfill(self, start_year: int = 2015, save: bool = True) -> Dict:
        """Walk every release month from start_year to now. Each release covers
        data ~2 months prior; period is read from the PDF itself."""
        now = datetime.now()
        total, months, fails = 0, 0, []
        for y in range(start_year, now.year + 1):
            for m in range(1, 13):
                if (y, m) > (now.year, now.month):
                    break
                raw = self.download(m, y)
                if not raw:
                    continue
                try:
                    res = self.parse(raw)
                except Exception as e:
                    fails.append((y, m, str(e))); continue
                if not res.success:
                    fails.append((y, m, res.error_message)); continue
                if save:
                    total += self.save_to_bronze(res.records)
                months += 1
                if res.warnings:
                    self.logger.warning(f"{y}-{m:02d} QC: {res.warnings}")
        return {"releases_parsed": months, "rows": total, "failures": fails}

    def verify_against_narrative(self, page1: str, recs: List[Dict]) -> List[str]:
        """v1 cross-check: a few headline numbers stated in prose on page 1
        must agree with the parsed table values. This is the ground-truth hook
        the permit pipeline lacks; the LLM second-reader will extend it."""
        warns = []
        by = {r["commodity_desc"]: r["value"] for r in recs}

        def near(label, parsed, narrative, tol=0.03):
            if parsed is None or narrative is None:
                return
            if narrative == 0:
                return
            if abs(parsed - narrative) / narrative > tol:
                warns.append(f"narrative mismatch [{label}]: table={parsed:,.0f} vs prose~{narrative:,.0f}")

        # "Corn for fuel alcohol, at 425 million bushels" -> 425,000 (1000 bu)
        m = re.search(r"fuel alcohol,?\s+at\s+([\d.]+)\s+million bushels", page1, re.I)
        if m:
            near("fuel_alcohol", by.get("CORN FOR FUEL ALCOHOL"), float(m.group(1)) * 1000)
        # "DDGS) was 1.63 million tons"
        m = re.search(r"DDGS\)?\s+was\s+([\d.]+)\s+million tons", page1, re.I)
        if m:
            near("ddgs", by.get("DISTILLERS DRIED GRAINS W SOLUBLES"), float(m.group(1)) * 1e6)
        # "corn gluten feed production was 242,146 tons"
        m = re.search(r"corn gluten feed production was\s+([\d,]+)\s+tons", page1, re.I)
        if m:
            near("corn_gluten_feed", by.get("CORN GLUTEN FEED"), float(m.group(1).replace(",", "")))
        return warns


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--release-month", type=int)
    ap.add_argument("--release-year", type=int)
    ap.add_argument("--save-db", action="store_true")
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--start-year", type=int, default=2015)
    args = ap.parse_args()
    c = NASSGrainCrushPDFCollector()
    if args.backfill:
        print("backfill:", c.backfill(start_year=args.start_year, save=True))
        return
    raw = c.download(args.release_month, args.release_year)
    if not raw:
        print("no PDF"); return
    res = c.parse(raw)
    print(f"period={res.year}-{res.month:02d} success={res.success} records={len(res.records)}")
    for r in res.records:
        print(f"  {r['commodity_desc']:38} {r['statisticcat']:10} {r['value']:>14,.0f} {r['unit']}")
    if res.warnings:
        print("WARNINGS:", res.warnings)
    else:
        print("QC: all narrative cross-checks passed")
    if args.save_db and res.success:
        print("saved to bronze:", c.save_to_bronze(res.records))


if __name__ == "__main__":
    main()
