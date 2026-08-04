"""
FRED (H.10) daily FX collector — the canonical upgrade for the interim ECB FX collector.

Guidance Report price-feed layer, Tier A #11 (brief:
clients/Contracts/Helios/CLAUDE_CODE_BRIEF_price_feeds.md; register #11 "FX set").
Lands the six USD FX pairs the report's conversions consume into silver.price_mark from the
Federal Reserve H.10 release via the FRED API.

ROUTE / RELATIONSHIP TO THE ECB COLLECTOR:
    The register lists "FRED (H.10) or ECB reference rates". FRED is the register's primary and is
    what this collector implements once FRED_API_KEY exists; ecb_fx_collector.py was the keyless
    interim. Both write the SAME six series_keys with the SAME unit/currency convention, at the SAME
    OFFICIAL_GOV rank, differing only in `source` ('fred_h10' vs 'ecb_ref*'). Because the price_mark
    PK includes source, they COEXIST rather than collide -- so a consumer that picks one per
    (series_key, obs_date, tenor) must break the OFFICIAL_GOV tie by source preference (FRED primary).
    That precedence belongs in the planned gold.price_mark_best view, not here.

    Why FRED over the ECB interim: direct H.10 pairs (no EUR triangulation), the canonical US-gov
    source, and materially longer history -- DEXCAUS/DEXMAUS to 1971, DEXMXUS 1993, vs ECB's 1999.

    ARS IS STILL A GAP. Argentina is not in the Fed H.10 release -- there is no daily DEXARUS. FRED
    does NOT close the ARS hole; that needs BCRA / datos.gob.ar and is deferred (same as under ECB).

DIRECTION (no inversion needed -- every FRED series is already in the stored direction):
    DEXUSEU -> FX_EURUSD  (USD per EUR)      DEXMAUS -> FX_USDMYR  (MYR per USD)
    DEXCHUS -> FX_USDCNY  (CNY per USD)      DEXMXUS -> FX_USDMXN  (MXN per USD)
    DEXCAUS -> FX_USDCAD  (CAD per USD)      DEXBZUS -> FX_USDBRL  (BRL per USD)

All land SPOT / OFFICIAL_GOV / can_republish=TRUE (H.10 is US public-domain government data).
FRED marks missing observations as '.'; those are skipped (price_mark.value is NOT NULL).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

_FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

# series_key -> (fred_series_id, unit, currency). unit/currency match ecb_fx_collector exactly so the
# FRED and ECB rows are directly comparable (currency = the unit's numerator).
_SERIES = {
    "FX_EURUSD": ("DEXUSEU", "USD/EUR", "USD"),
    "FX_USDMYR": ("DEXMAUS", "MYR/USD", "MYR"),
    "FX_USDCNY": ("DEXCHUS", "CNY/USD", "CNY"),
    "FX_USDMXN": ("DEXMXUS", "MXN/USD", "MXN"),
    "FX_USDCAD": ("DEXCAUS", "CAD/USD", "CAD"),
    "FX_USDBRL": ("DEXBZUS", "BRL/USD", "BRL"),
}


@dataclass
class FXMark:
    series_key: str
    obs_date: date
    value: float
    unit: str
    currency: str


def parse_fred_observations(series_key: str, unit: str, currency: str, payload: dict) -> list[FXMark]:
    """Pure parser: a FRED /series/observations JSON payload -> FX marks. No I/O; unit-tested.

    Skips FRED's '.' missing-value sentinel and any unparseable date/value."""
    marks: list[FXMark] = []
    for o in payload.get("observations", []):
        raw = o.get("value")
        if raw is None or raw == ".":
            continue
        try:
            value = float(raw)
            obs = datetime.strptime(o["date"], "%Y-%m-%d").date()
        except (ValueError, KeyError):
            logger.warning("%s: unparseable observation %r", series_key, o)
            continue
        marks.append(FXMark(series_key=series_key, obs_date=obs,
                            value=value, unit=unit, currency=currency))
    return marks


class FREDFXCollector:
    """Fetch FRED H.10 daily FX pairs and upsert them into the price layer (SPOT, OFFICIAL_GOV)."""

    SOURCE = "fred_h10"
    COLLECTOR_NAME = "fred_fx"

    def __init__(self, api_key: Optional[str] = None):
        key = api_key or os.getenv("FRED_API_KEY")
        if not key:
            try:
                from dotenv import load_dotenv
                load_dotenv(os.path.join(os.getcwd(), ".env"))
                key = os.getenv("FRED_API_KEY")
            except Exception:  # pragma: no cover
                pass
        self.api_key = key

    def _fetch(self, fred_id: str, timeout: int = 60) -> Optional[dict]:
        try:
            r = requests.get(
                _FRED_URL,
                params={"series_id": fred_id, "api_key": self.api_key, "file_type": "json"},
                headers={"User-Agent": "RLC-Agent"},
                timeout=timeout,
            )
        except requests.RequestException as e:
            logger.warning("FRED %s fetch failed: %s", fred_id, e)
            return None
        if r.status_code != 200:
            logger.warning("FRED %s HTTP %s: %s", fred_id, r.status_code, r.text[:200])
            return None
        try:
            return r.json()
        except ValueError as e:
            logger.error("FRED %s JSON decode failed: %s", fred_id, e)
            return None

    def collect(self, triggered_by: str | None = None):
        # Dispatcher runs pass NO triggered_by: collector_runner owns the
        # collection_status row there (self-logging too produced paired rows —
        # 2026-08-03). Only the __main__ path (triggered_by='cli'), which
        # bypasses the runner, self-logs. Runner requires a .success attribute,
        # so return CollectorResult, not a dict.
        from src.agents.base.base_collector import CollectorResult
        started = datetime.now(timezone.utc)
        if not self.api_key:
            if triggered_by:
                self._log_run(started, "FAILED", 0, 0, None,
                              error="FRED_API_KEY not set", triggered_by=triggered_by)
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message="FRED_API_KEY not set")

        all_marks: list[FXMark] = []
        failed: list[str] = []
        for series_key, (fred_id, unit, currency) in _SERIES.items():
            payload = self._fetch(fred_id)
            if payload is None:
                failed.append(series_key)
                continue
            all_marks.extend(parse_fred_observations(series_key, unit, currency, payload))

        if not all_marks:
            if triggered_by:
                self._log_run(started, "FAILED", 0, 0, None,
                              error=f"no marks parsed (failed: {failed})", triggered_by=triggered_by)
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message=f"no marks parsed (failed: {failed})")

        inserted = self._write(all_marks)
        max_date = max(m.obs_date for m in all_marks)
        if triggered_by:
            status = "SUCCESS" if not failed else "PARTIAL"
            self._log_run(started, status, len(all_marks), inserted, max_date,
                          triggered_by=triggered_by,
                          notes=f"{len({m.series_key for m in all_marks})}/6 pairs; latest {max_date}"
                                + (f"; FAILED {failed}" if failed else ""))
        return CollectorResult(success=True, source=self.COLLECTOR_NAME,
                               records_fetched=len(all_marks),
                               period_end=str(max_date),
                               warnings=[f"failed pairs: {failed}"] if failed else [])

    def _write(self, marks: list[FXMark]) -> int:
        from psycopg2.extras import execute_values
        from src.services.database.db_config import get_connection
        rows = [
            (m.series_key, m.obs_date, "SPOT", "SPOT", m.value, m.unit, m.currency,
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
                rows, page_size=1000,
            )
            after = self._count(cur)
            conn.commit()
        return after - before

    def _count(self, cur) -> int:
        cur.execute("SELECT count(*) AS n FROM silver.price_mark WHERE source=%s", (self.SOURCE,))
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
                     ["fx"], rows_inserted > 0, triggered_by, notes),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = FREDFXCollector().collect(triggered_by="cli")
    print(result)


if __name__ == "__main__":
    main()
