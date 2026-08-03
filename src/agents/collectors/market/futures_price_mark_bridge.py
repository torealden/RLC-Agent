"""
Daily bronze.futures_daily_settlement -> silver.price_mark / silver.curve_snapshot bridge.

Migration 159 backfilled the delayed yfinance/ibkr futures history into the canonical price layer
ONCE (Tore ruling 2026-07-28: keep the 2000-> depth at NEWS_INDICATIVE / can_republish=FALSE).
It was a one-shot migration, so price_mark's delayed-source rows froze at the migration date while
bronze kept collecting daily (yfinance_futures, 17:15 ET). This bridge is the standing daily version
of the same mapping, so downstream consumers (the curve engine in src/curves/ above all) always see
current dated-contract settles in the canonical layer.

Mapping is IDENTICAL to migration 159 (same symbol->series_key/unit table, same tenor rules):
    FRONT           -> price_mark  tenor_type NEARBY,   tenor 'M1'          (continuous, roll gaps)
    dated contracts -> price_mark  tenor_type CONTRACT, tenor '<key>_<Mon>' (e.g. ZL_U26)
    dated contracts -> curve_snapshot legs (volume kept; OI is NULL in the delayed source)
All at NEWS_INDICATIVE / can_republish=FALSE. Official AMS settles coexist on top (PK includes
source; consumers take MAX rank).

Only a trailing window (default 14 days) is synced each run, with ON CONFLICT DO UPDATE so late
revisions in bronze propagate; the deep history is already in place from migration 159.
"""

from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# symbol -> (series_key, unit, currency); MUST stay in lockstep with migration 159.
_SYMBOLS = [
    ("ZC", "ZC", "cents/bu", "USD"),
    ("ZW", "ZW", "cents/bu", "USD"),
    ("KE", "KE", "cents/bu", "USD"),
    ("ZS", "ZS", "cents/bu", "USD"),
    ("ZL", "ZL", "cents/lb", "USD"),
    ("ZM", "ZM", "USD/short ton", "USD"),
    ("ZR", "ZR", "USD/cwt", "USD"),
    ("CL", "CL", "USD/bbl", "USD"),
    ("HO", "HO", "USD/gal", "USD"),
    ("RB", "RB", "USD/gal", "USD"),
    ("NG", "NG", "USD/MMBtu", "USD"),
    ("DC", "DC", "USD/cwt", "USD"),
    ("FCPO", "FCPO", "MYR/t", "MYR"),
]

_SYM_VALUES = ", ".join("(%s,%s,%s,%s)" for _ in _SYMBOLS)
_SYM_PARAMS = [v for row in _SYMBOLS for v in row]


class FuturesPriceMarkBridge:
    COLLECTOR_NAME = "futures_price_mark_bridge"
    WINDOW_DAYS = 14

    def collect(self, triggered_by: str = "manual") -> dict:
        started = datetime.now()
        try:
            counts, latest = self._sync()
        except Exception as e:
            logger.error("futures price_mark bridge failed: %s", e)
            self._log_run(started, "FAILED", 0, error=str(e), triggered_by=triggered_by)
            return {"success": False, "error": str(e)}
        total = sum(counts.values())
        self._log_run(started, "SUCCESS", total, data_period=str(latest) if latest else None,
                      triggered_by=triggered_by)
        return {"success": True, "rows_written": total, **counts,
                "latest": str(latest) if latest else None}

    def _sync(self):
        from src.services.database.db_config import get_connection
        counts = {}
        with get_connection() as conn:
            cur = conn.cursor()
            sym_cte = f"WITH sym(symbol, series_key, unit, currency) AS (VALUES {_SYM_VALUES})"

            cur.execute(
                sym_cte + """
                INSERT INTO silver.price_mark
                    (series_key, obs_date, tenor_type, tenor, value, unit, currency,
                     source, quality_rank, can_republish)
                SELECT s.series_key, f.trade_date, 'NEARBY', 'M1', f.settlement, s.unit, s.currency,
                       f.source, 'NEWS_INDICATIVE', FALSE
                FROM bronze.futures_daily_settlement f
                JOIN sym s ON s.symbol = f.symbol
                WHERE f.contract_month = 'FRONT' AND f.settlement IS NOT NULL
                  AND f.trade_date >= CURRENT_DATE - %s
                ON CONFLICT (series_key, obs_date, tenor_type, tenor, source)
                DO UPDATE SET value = EXCLUDED.value, collected_at = now()""",
                _SYM_PARAMS + [self.WINDOW_DAYS],
            )
            counts["nearby_rows"] = cur.rowcount

            cur.execute(
                sym_cte + """
                INSERT INTO silver.price_mark
                    (series_key, obs_date, tenor_type, tenor, value, unit, currency,
                     source, quality_rank, can_republish)
                SELECT s.series_key, f.trade_date, 'CONTRACT', s.series_key || '_' || f.contract_month,
                       f.settlement, s.unit, s.currency, f.source, 'NEWS_INDICATIVE', FALSE
                FROM bronze.futures_daily_settlement f
                JOIN sym s ON s.symbol = f.symbol
                WHERE f.contract_month <> 'FRONT' AND f.settlement IS NOT NULL
                  AND f.trade_date >= CURRENT_DATE - %s
                ON CONFLICT (series_key, obs_date, tenor_type, tenor, source)
                DO UPDATE SET value = EXCLUDED.value, collected_at = now()""",
                _SYM_PARAMS + [self.WINDOW_DAYS],
            )
            counts["contract_rows"] = cur.rowcount

            cur.execute(
                sym_cte + """
                INSERT INTO silver.curve_snapshot
                    (series_key, obs_date, contract, settle, volume, open_interest,
                     unit, currency, source, quality_rank)
                SELECT s.series_key, f.trade_date, s.series_key || '_' || f.contract_month,
                       f.settlement, f.total_volume, f.open_interest, s.unit, s.currency,
                       f.source, 'NEWS_INDICATIVE'
                FROM bronze.futures_daily_settlement f
                JOIN sym s ON s.symbol = f.symbol
                WHERE f.contract_month <> 'FRONT' AND f.settlement IS NOT NULL
                  AND f.trade_date >= CURRENT_DATE - %s
                ON CONFLICT (series_key, obs_date, contract, source)
                DO UPDATE SET settle = EXCLUDED.settle, volume = EXCLUDED.volume,
                              open_interest = EXCLUDED.open_interest""",
                _SYM_PARAMS + [self.WINDOW_DAYS],
            )
            counts["snapshot_rows"] = cur.rowcount

            cur.execute(
                """SELECT MAX(obs_date) AS latest FROM silver.price_mark
                   WHERE source IN ('yfinance', 'ibkr_tws')""")
            row = cur.fetchone()
            latest = row["latest"] if isinstance(row, dict) else row[0]
            conn.commit()
        return counts, latest

    def _log_run(self, started, status, rows, data_period=None, error=None, triggered_by="manual"):
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO core.collection_status
                       (collector_name, run_started_at, run_finished_at, status, rows_collected,
                        rows_inserted, error_message, data_period, commodities, is_new_data, triggered_by)
                       VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (self.COLLECTOR_NAME, started, status, rows, rows, error, data_period,
                     ["futures"], rows > 0, triggered_by),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(FuturesPriceMarkBridge().collect(triggered_by="cli"))


if __name__ == "__main__":
    main()
