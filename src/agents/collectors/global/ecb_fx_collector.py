"""
ECB reference-rate FX collector — INTERIM for Guidance Report Tier A #11.

The brief's primary FX source is FRED (direct USD pairs + ARS + long history). No FRED key is
available yet (checked .env + email 2026-07-28), so this lands the FX set from the European Central
Bank's daily euro reference rates (data-api.ecb.europa.eu, keyless, official, back to 1999-01-04).

ECB quotes every currency against the euro from the same daily concertation (14:15 CET), so a USD
cross is an EXACT triangulation, not an estimate:

    USD/xxx  =  (xxx per EUR)  /  (USD per EUR)

Landed into silver.price_mark as SPOT marks, quality_rank OFFICIAL_GOV (official central-bank fixing;
triangulation of two official legs does not degrade quality), can_republish TRUE (ECB reference rates
are freely reusable). EURUSD is published directly (USD per EUR) so its source is 'ecb_ref'; the crosses
are 'ecb_ref_xrate' to keep the triangulation visible in provenance.

COVERAGE vs the brief's set {USDMYR, USDCNY, USDMXN, EURUSD, USDCAD, USDBRL, ARS}: this delivers 6 of 7.
ARS is NOT publishable by ECB (last observation 2020-10-30) — it stays a gap until FRED (direct) or an
Argentina-official source lands it. Documented, not silently dropped.

Convention stored: USDxxx value = units of xxx per 1 USD (e.g. USDMYR = MYR per USD), so a MYR/t price
divides by USDMYR to reach USD/t. EURUSD value = USD per 1 EUR (the market convention).
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

_ECB_URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.{cur}.EUR.SP00.A"

# EUR-leg currencies we pull (each = units of that currency per 1 EUR).
_LEGS = ["USD", "MYR", "CNY", "MXN", "CAD", "BRL"]

# Output pairs. (series_key, numerator_leg, unit, currency, source)
#   numerator_leg=None => the EURUSD direct rate (USD per EUR).
#   otherwise USDxxx = leg[numerator] / leg[USD]  (xxx per USD).
_PAIRS = [
    ("FX_EURUSD", None,  "USD/EUR", "USD", "ecb_ref"),
    ("FX_USDMYR", "MYR", "MYR/USD", "MYR", "ecb_ref_xrate"),
    ("FX_USDCNY", "CNY", "CNY/USD", "CNY", "ecb_ref_xrate"),
    ("FX_USDMXN", "MXN", "MXN/USD", "MXN", "ecb_ref_xrate"),
    ("FX_USDCAD", "CAD", "CAD/USD", "CAD", "ecb_ref_xrate"),
    ("FX_USDBRL", "BRL", "BRL/USD", "BRL", "ecb_ref_xrate"),
]


class ECBFXCollector:
    COLLECTOR_NAME = "ecb_fx"

    def collect(self, start: str = "1999-01-01", triggered_by: str = "manual") -> dict:
        started = datetime.now()
        try:
            legs = {cur: self._fetch_leg(cur, start) for cur in _LEGS}
        except Exception as e:
            logger.error("ECB FX fetch failed: %s", e)
            self._log_run(started, "FAILED", 0, error=str(e), triggered_by=triggered_by)
            return {"success": False, "error": str(e)}

        rows = self._build_rows(legs)
        if not rows:
            self._log_run(started, "FAILED", 0, error="no rows built", triggered_by=triggered_by)
            return {"success": False, "error": "no rows built"}

        written = self._write(rows)
        latest = max(r[1] for r in rows)
        self._log_run(started, "SUCCESS", written, data_period=latest, triggered_by=triggered_by,
                      notes=f"{len({r[0] for r in rows})} pairs; ARS gap (ECB stale since 2020)")
        return {"success": True, "rows_written": written, "pairs": len({r[0] for r in rows}),
                "latest": latest}

    def _fetch_leg(self, cur: str, start: str) -> dict[str, float]:
        """Return {date_str: rate} of `cur` units per 1 EUR."""
        r = requests.get(_ECB_URL.format(cur=cur),
                         params={"format": "csvdata", "startPeriod": start},
                         headers={"Accept": "text/csv"}, timeout=90)
        r.raise_for_status()
        out: dict[str, float] = {}
        reader = csv.DictReader(io.StringIO(r.text))
        for row in reader:
            period, val = row.get("TIME_PERIOD"), row.get("OBS_VALUE")
            if period and val not in (None, "", "NaN"):
                try:
                    out[period] = float(val)
                except ValueError:
                    continue
        logger.info("ECB %s: %d obs", cur, len(out))
        return out

    def _build_rows(self, legs: dict[str, dict[str, float]]) -> list[tuple]:
        """-> [(series_key, obs_date, value, unit, currency, source), ...] via exact triangulation."""
        usd = legs["USD"]
        rows: list[tuple] = []
        for series_key, num_leg, unit, currency, source in _PAIRS:
            if num_leg is None:
                for d, u in usd.items():
                    rows.append((series_key, d, round(u, 6), unit, currency, source))
            else:
                leg = legs[num_leg]
                for d, u in usd.items():
                    if d in leg and u:
                        rows.append((series_key, d, round(leg[d] / u, 6), unit, currency, source))
        return rows

    def _write(self, rows: list[tuple]) -> int:
        from psycopg2.extras import execute_values
        from src.services.database.db_config import get_connection
        # (series_key, obs_date, value, unit, currency, source) -> full price_mark tuple
        values = [(sk, d, "SPOT", "SPOT", v, u, c, src, "OFFICIAL_GOV", True)
                  for sk, d, v, u, c, src in rows]
        with get_connection() as conn:
            cur = conn.cursor()
            execute_values(
                cur,
                """INSERT INTO silver.price_mark
                   (series_key, obs_date, tenor_type, tenor, value, unit, currency,
                    source, quality_rank, can_republish)
                   VALUES %s
                   ON CONFLICT (series_key, obs_date, tenor_type, tenor, source)
                   DO UPDATE SET value=EXCLUDED.value, collected_at=now()""",
                values, page_size=1000,
            )
            conn.commit()
        return len(values)

    def _log_run(self, started, status, rows, data_period=None, error=None, notes=None,
                 triggered_by="manual"):
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO core.collection_status
                       (collector_name, run_started_at, run_finished_at, status, rows_collected,
                        rows_inserted, error_message, data_period, commodities, is_new_data,
                        triggered_by, notes)
                       VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (self.COLLECTOR_NAME, started, status, rows, rows, error, data_period,
                     ["fx"], rows > 0, triggered_by, notes),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(ECBFXCollector().collect(triggered_by="cli"))


if __name__ == "__main__":
    main()
