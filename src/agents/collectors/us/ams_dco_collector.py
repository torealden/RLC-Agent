"""
USDA AMS Distillers Corn Oil (DCO) regional FOB-plant prices collector.

Guidance Report price-feed layer, Tier A #13 (brief:
clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md; register #13 "DCO, US regional FOB plant").
Lands the 8 regional DCO cash prices from USDA AMS report ams_3618 (National Weekly Grain
Co-Products Report) into silver.price_mark.

ROUTE (logged per the brief's "log the chosen route" instruction):
    The brief anticipated a PDF parse of ams_3618. Investigated 2026-07-28: the MARS "My Market
    News" API (marsapi.ams.usda.gov) DOES expose slug 3618's "Report Detail" section as structured
    JSON -- unlike slug 3192's futures-settlement block, which is API-invisible and forced the PDF
    fallback in the sister ams_settlement_collector. So DCO takes the clean route: MARS API JSON,
    no PDF/regex. One call filtered to commodity='Distillers Corn Oil' returns the full history
    (all regions, ~2022-07-25 -> current) in a single response (~1,600 rows), so every run re-pulls
    the whole series and upserts -- idempotent and self-healing on gaps, cheap at this size.

RELATIONSHIP TO THE LEGACY COLLECTOR:
    src/agents/collectors/us/usda_ams_collector.py::GrainCoProductsCollector already touches ams_3618
    but via a fragile text-report (sf_gr112.txt) regex that yields a single coarse DCO average into
    bronze.usda_ams_ddgs. It does NOT carry the 8-region granularity the price layer needs. This is a
    separate, dedicated price-layer collector, not a bridge off that bronze table.

WHAT LANDS (public-domain USDA -> quality_rank OFFICIAL_GOV, can_republish=TRUE):
    8 regional series_keys, one per AMS trade_loc:
        Iowa->DCO_IA  Kansas->DCO_KS  Wisconsin->DCO_WI  Missouri->DCO_MO  Nebraska->DCO_NE
        South Dakota->DCO_SD  Minnesota->DCO_MN  Eastern Cornbelt->DCO_ECB
    Each is a SPOT cash price (tenor_type='SPOT', tenor='SPOT') -- a weekly regional FOB-plant
    assessment, NOT an exchange settlement, hence OFFICIAL_GOV (a government survey price), not the
    SETTLE_OFFICIAL used for exchange strips. No forward curve, so NO curve_snapshot rows.

    Quality note: OFFICIAL_GOV sits below SETTLE_OFFICIAL/ASSESSED_LICENSED in the ladder (migration
    156), which is correct for a weekly surveyed cash price versus a daily exchange settle.

UNITS: the DCO mark is `price`, quoted 'Cents Per Lb' (stored unit 'cents/lb'). The report's
    separate `value` field ('$/Bu') is the corn-processing co-product CREDIT per bushel, a different
    quantity -- deliberately NOT stored as the price. A unit-drift guard skips any row whose
    price_unit is not 'Cents Per Lb' and logs it, so a silent AMS format change fails visibly.

OBS_DATE: report_end_date (the Friday close of the survey week) is the observation date -- the
    "price as of" for a weekly assessment. report_date (the Monday begin) is recoverable from the
    weekly cadence if ever needed.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_MARS_BASE = "https://marsapi.ams.usda.gov/services/v1.2"
_SLUG = "3618"
_COMMODITY = "Distillers Corn Oil"
_EXPECTED_PRICE_UNIT = "Cents Per Lb"

# AMS trade_loc (as printed) -> price-layer series_key.
_REGION_MAP = {
    "Iowa": "DCO_IA",
    "Kansas": "DCO_KS",
    "Wisconsin": "DCO_WI",
    "Missouri": "DCO_MO",
    "Nebraska": "DCO_NE",
    "South Dakota": "DCO_SD",
    "Minnesota": "DCO_MN",
    "Eastern Cornbelt": "DCO_ECB",
}


@dataclass
class DCOMark:
    series_key: str
    obs_date: date
    value: float            # cents/lb
    trade_loc: str


def _parse_ams_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except ValueError:
        return None


def parse_dco_rows(payload: dict) -> list[DCOMark]:
    """Pure parser: a MARS 'Report Detail' JSON payload -> DCO marks.

    No I/O; unit-tested against a stored fixture. Silently drops non-DCO rows, null prices, and
    unmapped regions (logged), and skips any row whose price_unit is not the expected 'Cents Per Lb'
    (unit-drift guard -- fail visibly rather than store a wrongly-scaled number)."""
    marks: list[DCOMark] = []
    for r in payload.get("results", []):
        if (r.get("commodity") or "") != _COMMODITY:
            continue
        loc = (r.get("trade_loc") or "").strip()
        series_key = _REGION_MAP.get(loc)
        if series_key is None:
            logger.debug("unmapped DCO trade_loc: %r", loc)
            continue
        price = r.get("price")
        if price is None:
            continue
        unit = (r.get("price_unit") or "").strip()
        if unit != _EXPECTED_PRICE_UNIT:
            logger.warning("DCO %s: unexpected price_unit %r (expected %r) -- skipped",
                           loc, unit, _EXPECTED_PRICE_UNIT)
            continue
        obs = _parse_ams_date(r.get("report_end_date")) or _parse_ams_date(r.get("report_date"))
        if obs is None:
            logger.warning("DCO %s: unparseable report dates %r/%r -- skipped",
                           loc, r.get("report_end_date"), r.get("report_date"))
            continue
        marks.append(DCOMark(series_key=series_key, obs_date=obs,
                             value=float(price), trade_loc=loc))
    return marks


class AMSDCOCollector:
    """Fetch the AMS DCO regional cash prices (MARS API) and upsert them into the price layer."""

    SOURCE = "usda_ams_3618"
    COLLECTOR_NAME = "ams_dco_prices"

    def __init__(self, api_key: Optional[str] = None):
        key = api_key or os.getenv("USDA_AMS_API_KEY")
        if not key:
            # Standalone runs (python -m) don't inherit a dotenv-loaded env the way the dispatcher
            # does; load the project .env explicitly so the key resolves either way.
            try:
                from dotenv import load_dotenv
                load_dotenv(os.path.join(os.getcwd(), ".env"))
                key = os.getenv("USDA_AMS_API_KEY")
            except Exception:  # pragma: no cover
                pass
        self.api_key = key

    def _fetch(self, timeout: int = 60) -> Optional[dict]:
        if not self.api_key:
            logger.error("USDA_AMS_API_KEY not set; cannot query MARS API")
            return None
        # One call, full history, filtered to DCO. MARS filter grammar: q=field=value.
        url = f"{_MARS_BASE}/reports/{_SLUG}/Report Detail"
        try:
            r = requests.get(
                url,
                params={"q": f"commodity={_COMMODITY}"},
                auth=(self.api_key, ""),
                headers={"User-Agent": "Mozilla/5.0 (RLC-Agent)"},
                timeout=timeout,
            )
        except requests.RequestException as e:
            logger.warning("MARS %s fetch failed: %s", _SLUG, e)
            return None
        if r.status_code != 200:
            logger.warning("MARS %s HTTP %s", _SLUG, r.status_code)
            return None
        try:
            return r.json()
        except ValueError as e:
            logger.error("MARS %s JSON decode failed: %s", _SLUG, e)
            return None

    def collect(self, triggered_by: str | None = None):
        # Dispatcher runs pass NO triggered_by: collector_runner owns the
        # collection_status row there (self-logging too produced paired rows —
        # 2026-08-03). Only the __main__ path (triggered_by='cli'), which
        # bypasses the runner, self-logs. Runner requires a .success attribute,
        # so return CollectorResult, not a dict.
        from src.agents.base.base_collector import CollectorResult
        started = datetime.now(timezone.utc)
        payload = self._fetch()
        if payload is None:
            if triggered_by:
                self._log_run(started, "FAILED", 0, 0, None,
                              error="MARS fetch/decode failed", triggered_by=triggered_by)
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message="MARS fetch/decode failed")

        marks = parse_dco_rows(payload)
        if not marks:
            if triggered_by:
                self._log_run(started, "FAILED", 0, 0, None,
                              error="no DCO marks parsed", triggered_by=triggered_by)
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message="no DCO marks parsed")

        inserted = self._write(marks)
        max_date = max(m.obs_date for m in marks)
        if triggered_by:
            self._log_run(started, "SUCCESS", len(marks), inserted, max_date,
                          triggered_by=triggered_by,
                          notes=f"{len({m.series_key for m in marks})} regions; latest {max_date}")
        return CollectorResult(success=True, source=self.COLLECTOR_NAME,
                               records_fetched=len(marks), period_end=str(max_date))

    def _write(self, marks: list[DCOMark]) -> int:
        from psycopg2.extras import execute_values
        from src.services.database.db_config import get_connection
        rows = [
            (m.series_key, m.obs_date, "SPOT", "SPOT", m.value, "cents/lb", "USD",
             self.SOURCE, "OFFICIAL_GOV", True)
            for m in marks
        ]
        with get_connection() as conn:
            cur = conn.cursor()
            before = self._count(cur)
            execute_values(
                cur,
                """INSERT INTO silver.price_mark
                   (series_key, obs_date, tenor_type, tenor, value, unit, currency,
                    source, quality_rank, can_republish)
                   VALUES %s
                   ON CONFLICT (series_key, obs_date, tenor_type, tenor, source)
                   DO UPDATE SET value=EXCLUDED.value, quality_rank=EXCLUDED.quality_rank,
                                 can_republish=EXCLUDED.can_republish, collected_at=now()""",
                rows,
            )
            after = self._count(cur)
            conn.commit()
        return after - before

    def _count(self, cur) -> int:
        cur.execute(
            "SELECT count(*) AS n FROM silver.price_mark WHERE source=%s", (self.SOURCE,))
        return cur.fetchone()["n"]

    def _log_run(self, started, status, rows_collected, rows_inserted, data_period,
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
                     str(data_period) if data_period else None,
                     ["distillers_corn_oil"], rows_inserted > 0, triggered_by, notes),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = AMSDCOCollector().collect(triggered_by="cli")
    print(result)


if __name__ == "__main__":
    main()
