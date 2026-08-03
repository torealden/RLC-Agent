"""
USDA AMS Grain Report — Futures Settlement Block collector.

Guidance Report price-feed layer, Tier A #1-4 (brief:
clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md). Lands official CBOT/KCBT/MGEX grain
settlement strips into the new price layer (silver.price_mark + silver.curve_snapshot).

ROUTE (logged per the brief's "log the chosen route" instruction):
    The MARS "My Market News" API (marsapi.ams.usda.gov) exposes slug 3192 with only two
    reportSections -- 'Report Header' and 'Report Detail' (cash basis). The "Futures Settlements"
    block is NOT in the API. Verified 2026-07-28. So this collector uses the brief's stated
    fallback: parse the "Closing Settlement Prices" block out of the daily grain PDF.

    ams_3192 (Springfield IL), ams_2850 (Iowa) and ams_2771 (Montana) carry a BYTE-IDENTICAL
    settlement block (verified 2026-07-28 across all three). We pull 3192 as primary and fall
    back to 2850 then 2771 only if 3192 is unreachable or the block is missing -- one strip, three
    redundant sources.

WHAT LANDS (all public-domain USDA -> quality_rank SETTLE_OFFICIAL, can_republish=TRUE):
    CBOT Corn -> ZC        CBOT Soybeans -> ZS      CBOT Wheat (SRW) -> ZW
    CBOT White Oats -> ZO  KCBT Wheat (HRW) -> KE   MGEX Wheat (HRS) -> MWE
    Register #1-4 are ZC/ZW/KE/MWE; ZS and ZO ride along free. ZL (soybean oil, #5) is deliberately
    absent -- the AMS grain PDFs do not carry ZL deferreds (that is the CME route).

    Each contract lands BOTH as a silver.price_mark CONTRACT row (what a report cell consumes for a
    named tenor) AND as a silver.curve_snapshot row (the strip form the curve module's EXCHANGE_STRIP
    method reads). AMS gives no volume/OI, so those snapshot columns are NULL -- the thin-OI guard
    simply cannot fire on this source, which is fine for the liquid front of these boards. Both writes
    come from one parse and are idempotent UPSERTs, so they cannot drift.

Prices are quoted in cents per bushel (the PDF header reads "Closing Settlement Prices (cents/bu)").
"""

from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Month abbreviation -> futures month code (F..Z), so "Sep 26" -> contract 'U26'.
_MONTH_CODE = {
    "Jan": "F", "Feb": "G", "Mar": "H", "Apr": "J", "May": "K", "Jun": "M",
    "Jul": "N", "Aug": "Q", "Sep": "U", "Oct": "V", "Nov": "X", "Dec": "Z",
}

# (exchange token as printed, commodity as printed) -> series_key
_SERIES_MAP = {
    ("CBOT", "Corn"): "ZC",
    ("CBOT", "Soybeans"): "ZS",
    ("CBOT", "Wheat"): "ZW",
    ("CBOT", "White Oats"): "ZO",
    ("KCBT", "Wheat"): "KE",
    ("MGE", "Wheat"): "MWE",
    ("MGEX", "Wheat"): "MWE",
}

# Slugs carrying the identical settlement block, primary first.
_SLUGS = ("3192", "2850", "2771")
_PDF_URL = "https://www.ams.usda.gov/mnreports/ams_{slug}.pdf"

# A settlement line: "<EXCHANGE> <Commodity> <price> (<Mon> <YY>) <price> (<Mon> <YY>) ..."
_LINE_RE = re.compile(
    r"^(CBOT|KCBT|MGEX|MGE)\s+(Corn|Soybeans|White Oats|Oats|Wheat)\s+(.+)$"
)
# A single "price (Mon YY)" pair.
_PAIR_RE = re.compile(r"([\d,]+\.\d+|\d+)\s*\(([A-Z][a-z]{2})\s+(\d{2})\)")
_ASOF_RE = re.compile(r"as of\s+(\d{1,2}/\d{1,2}/\d{4})")
_GRAINDATE_RE = re.compile(r"Grain Report for\s+(\d{1,2}/\d{1,2}/\d{4})")


@dataclass
class SettlementMark:
    series_key: str
    contract: str           # e.g. 'ZC_U26'
    contract_month: str     # e.g. 'U26'
    value: float            # cents/bu
    exchange: str
    commodity: str


def _parse_report_date(text: str) -> Optional[date]:
    m = _ASOF_RE.search(text) or _GRAINDATE_RE.search(text)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%m/%d/%Y").date()


def parse_settlement_block(text: str) -> tuple[Optional[date], list[SettlementMark]]:
    """Pure parser: PDF text -> (report_date, marks). No I/O, unit-tested against a fixture."""
    report_date = _parse_report_date(text)
    marks: list[SettlementMark] = []
    for raw in text.split("\n"):
        line = raw.strip()
        m = _LINE_RE.match(line)
        if not m:
            continue
        exchange, commodity, rest = m.group(1), m.group(2), m.group(3)
        series_key = _SERIES_MAP.get((exchange, commodity))
        if series_key is None:
            logger.debug("unmapped settlement line: %s %s", exchange, commodity)
            continue
        for price_s, mon, yy in _PAIR_RE.findall(rest):
            code = _MONTH_CODE.get(mon)
            if code is None:
                logger.warning("unknown month abbrev %r in line: %s", mon, line)
                continue
            contract_month = f"{code}{yy}"
            marks.append(SettlementMark(
                series_key=series_key,
                contract=f"{series_key}_{contract_month}",
                contract_month=contract_month,
                value=float(price_s.replace(",", "")),
                exchange="MGEX" if exchange == "MGE" else exchange,
                commodity=commodity,
            ))
    return report_date, marks


def _fetch_pdf_text(slug: str, timeout: int = 60) -> Optional[str]:
    url = _PDF_URL.format(slug=slug)
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0 (RLC-Agent)"})
    except requests.RequestException as e:
        logger.warning("AMS %s fetch failed: %s", slug, e)
        return None
    if r.status_code != 200:
        logger.warning("AMS %s HTTP %s", slug, r.status_code)
        return None
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            return "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception as e:  # pragma: no cover - depends on pdf lib
        logger.error("AMS %s PDF extract failed: %s", slug, e)
        return None


class AMSSettlementCollector:
    """Fetch the AMS grain settlement block and upsert it into the price layer."""

    SOURCE = "usda_ams_settle"
    COLLECTOR_NAME = "ams_grain_settlement"

    def collect(self, triggered_by: str | None = None):
        # Dispatcher runs pass NO triggered_by: collector_runner owns the
        # collection_status row there (self-logging too produced paired rows —
        # 2026-08-03). Only the __main__ path (triggered_by='cli'), which
        # bypasses the runner, self-logs. Runner requires a .success attribute,
        # so return CollectorResult, not a dict.
        from src.agents.base.base_collector import CollectorResult
        started = datetime.now(timezone.utc)
        report_date, marks, slug_used = None, [], None
        for slug in _SLUGS:
            text = _fetch_pdf_text(slug)
            if not text:
                continue
            report_date, marks = parse_settlement_block(text)
            if marks and report_date:
                slug_used = slug
                break
            logger.warning("AMS %s parsed no marks / no date; trying next slug", slug)

        if not marks or not report_date:
            if triggered_by:
                self._log_run(started, "FAILED", 0, 0, report_date,
                              error="no settlement marks parsed from any slug",
                              triggered_by=triggered_by)
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message="no settlement marks parsed from any slug")

        inserted = self._write(report_date, marks, slug_used)
        if triggered_by:
            self._log_run(started, "SUCCESS", len(marks) * 2, inserted, report_date,
                          triggered_by=triggered_by,
                          notes=f"slug {slug_used}; {len({m.series_key for m in marks})} series")
        return CollectorResult(success=True, source=self.COLLECTOR_NAME,
                               records_fetched=len(marks) * 2,
                               data={"slug": slug_used, "rows_written": inserted},
                               period_end=str(report_date))

    def _write(self, report_date: date, marks: list[SettlementMark], slug: str) -> int:
        from src.services.database.db_config import get_connection
        src = f"{self.SOURCE}_{slug}"
        written = 0
        with get_connection() as conn:
            cur = conn.cursor()
            for m in marks:
                cur.execute(
                    """INSERT INTO silver.price_mark
                       (series_key, obs_date, tenor_type, tenor, value, unit, currency,
                        source, quality_rank, can_republish)
                       VALUES (%s,%s,'CONTRACT',%s,%s,'cents/bu','USD',%s,'SETTLE_OFFICIAL',TRUE)
                       ON CONFLICT (series_key, obs_date, tenor_type, tenor, source)
                       DO UPDATE SET value=EXCLUDED.value, quality_rank=EXCLUDED.quality_rank,
                                     can_republish=EXCLUDED.can_republish, collected_at=now()""",
                    (m.series_key, report_date, m.contract, m.value, src),
                )
                written += cur.rowcount
                cur.execute(
                    """INSERT INTO silver.curve_snapshot
                       (series_key, obs_date, contract, settle, volume, open_interest,
                        unit, currency, source, quality_rank)
                       VALUES (%s,%s,%s,%s,NULL,NULL,'cents/bu','USD',%s,'SETTLE_OFFICIAL')
                       ON CONFLICT (series_key, obs_date, contract, source)
                       DO UPDATE SET settle=EXCLUDED.settle, collected_at=now()""",
                    (m.series_key, report_date, m.contract, m.value, src),
                )
                written += cur.rowcount
            conn.commit()
        return written

    def _log_run(self, started, status, rows_collected, rows_inserted, report_date,
                 error=None, notes=None, triggered_by="manual"):
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO core.collection_status
                       (collector_name, run_started_at, run_finished_at, status,
                        rows_collected, rows_inserted, error_message, data_period,
                        commodities, is_new_data, triggered_by, notes)
                       VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (self.COLLECTOR_NAME, started, status, rows_collected, rows_inserted, error,
                     str(report_date) if report_date else None,
                     ["corn", "soybeans", "wheat", "oats"], rows_inserted > 0, triggered_by, notes),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = AMSSettlementCollector().collect(triggered_by="cli")
    print(result)


if __name__ == "__main__":
    main()
