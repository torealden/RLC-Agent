"""
EIA crude spot -> price layer bridge (Guidance Report Tier A #12).

The EIA v2 collector already lands WTI Cushing (RWTC) and Europe Brent (RBRTE) daily spot into
bronze.eia_observations back to 1990. This bridge promotes those two series into the canonical
price layer as SPOT marks with provenance, so the Guidance Report consumes crude the same way it
consumes every other series.

    series_key 'WTI' / 'BRENT', tenor_type SPOT, unit 'USD/bbl', currency USD,
    source 'eia_spot', quality_rank OFFICIAL_GOV (EIA is an official government series,
    below an exchange SETTLE but above any derived mark), can_republish TRUE (EIA is public domain).

There is no text parser here -- the data is already structured in bronze -- so verification is
functional (row counts + latest date) rather than a fixture unit test. First run backfills the full
1990-> history (idempotent UPSERT); subsequent daily runs add only new observations.

Register AFTER the EIA v2 crude pull so the bridge sees the freshest bronze.
"""

from __future__ import annotations

import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# EIA series_id -> price-layer series_key
_SERIES = {
    "RWTC": "WTI",
    "RBRTE": "BRENT",
}


class EIACrudePriceBridge:
    COLLECTOR_NAME = "eia_crude_price_bridge"
    SOURCE = "eia_spot"

    def collect(self, triggered_by: str = "manual") -> dict:
        started = datetime.now()
        try:
            written, latest = self._sync()
        except Exception as e:
            logger.error("EIA crude bridge failed: %s", e)
            self._log_run(started, "FAILED", 0, error=str(e), triggered_by=triggered_by)
            return {"success": False, "error": str(e)}
        self._log_run(started, "SUCCESS", written, data_period=str(latest) if latest else None,
                      triggered_by=triggered_by)
        return {"success": True, "rows_written": written, "latest": str(latest) if latest else None}

    def _sync(self):
        from src.services.database.db_config import get_connection
        written, latest = 0, None
        with get_connection() as conn:
            cur = conn.cursor()
            for series_id, series_key in _SERIES.items():
                cur.execute(
                    """INSERT INTO silver.price_mark
                       (series_key, obs_date, tenor_type, tenor, value, unit, currency,
                        source, quality_rank, can_republish)
                       SELECT %s, period, 'SPOT', 'SPOT', value, 'USD/bbl', 'USD',
                              %s, 'OFFICIAL_GOV', TRUE
                       FROM bronze.eia_observations
                       WHERE series_id = %s AND value IS NOT NULL
                       ON CONFLICT (series_key, obs_date, tenor_type, tenor, source)
                       DO UPDATE SET value = EXCLUDED.value, collected_at = now()""",
                    (series_key, self.SOURCE, series_id),
                )
                written += cur.rowcount
                cur.execute(
                    "SELECT MAX(obs_date) AS latest FROM silver.price_mark WHERE series_key=%s AND source=%s",
                    (series_key, self.SOURCE),
                )
                row = cur.fetchone()
                d = row["latest"] if isinstance(row, dict) else row[0]
                latest = max(latest, d) if latest and d else (d or latest)
            conn.commit()
        return written, latest

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
                     ["crude_oil"], rows > 0, triggered_by),
                )
                conn.commit()
        except Exception as e:  # pragma: no cover
            logger.error("collection_status log failed: %s", e)


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(EIACrudePriceBridge().collect(triggered_by="cli"))


if __name__ == "__main__":
    main()
