"""Helios WAPR daily collector -> bronze.helios_climate_risk (+ forecast vintage archive).

Daily wrapper over the fetch path proven in scripts/collect_helios_climate.py (the one-time
2026-07-21 load). Each pull returns the FULL index per commodity x country pair (~5yr history
+ ~2yr forward), so one run is also a complete self-healing backfill of any missed days:
  - bronze.helios_climate_risk      current state, full upsert (existing consumers unchanged)
  - bronze.helios_climate_risk_vintage   is_forecasted rows archived by pull date (mig 174)

API notes carried over from the script:
  - base https://api.helios.sc, Bearer HELIOS_API_KEY from .env
  - country must be the two-letter code from /countries; display names 404
  - an explicit User-Agent is REQUIRED — default Python-urllib gets a 403 at the edge

Scheduler-facing contract (registered 'helios_wapr', daily 07:00 ET): returns CollectorResult,
no self-logging (EIAV2CrudeDaily pattern).
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.services.database.db_config import get_connection

logger = logging.getLogger(__name__)

BASE = "https://api.helios.sc"
USER_AGENT = "RLC-Agent/1.0 (Round Lakes Commodities; tore.alden@roundlakescommodities.com)"

# Pepsi/Helios scope, same as the 7/21 load. Coverage is re-checked against the API each
# run, so a slug Helios drops just logs and skips rather than failing the run.
DEFAULT_COMMODITIES = ["soya_beans", "canola", "oil_palm_fruit", "wheat", "durum_wheat",
                       "corn_commodity_tracked", "seed_cotton_unginned"]

FIELDS = ['wapr', 'wapr_hist_avg', 'wapr_shade', 'too_hot_wapr', 'too_cold_wapr',
          'too_wet_wapr', 'too_dry_wapr', 'season_status', 'severity', 'hist_severity',
          'phase', 'is_in_season', 'is_forecasted', 'harvest_year']

VINTAGE_FIELDS = ['wapr', 'wapr_hist_avg', 'too_hot_wapr', 'too_cold_wapr',
                  'too_wet_wapr', 'too_dry_wapr', 'severity', 'phase', 'harvest_year']

# 88 pairs delivered data on the 7/21 load; a large coverage drop is worth a partial flag.
MIN_EXPECTED_PAIRS = 80


def _get(path: str, key: str, tries: int = 3) -> Optional[dict]:
    req = urllib.request.Request(BASE + path, headers={
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    })
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None          # no coverage for this pair; not an error
            if e.code in (429, 500, 502, 503) and i < tries - 1:
                time.sleep(2 ** i)
                continue
            raise
        except (urllib.error.URLError, TimeoutError):
            if i < tries - 1:
                time.sleep(2 ** i)
                continue
            raise
    return None


def fetch_all(key: str, commodities: Optional[List[str]] = None):
    """Pull the full index. Returns (rows, pairs, skipped_slugs)."""
    commodities = commodities or DEFAULT_COMMODITIES
    covered = {c["slug"] for c in (_get("/v1/climate/commodities", key) or {})
               .get("commodities", [])}
    skipped = [c for c in commodities if c not in covered]
    if skipped:
        logger.warning(f"helios_wapr: not in Helios coverage, skipping: {skipped}")

    rows, pairs = [], 0
    for slug in [c for c in commodities if c in covered]:
        cl = _get(f"/v1/climate/commodities/{slug}/countries", key) or {}
        for c in cl.get("countries", []):
            code, name = c["code"], c.get("name")
            d = _get(f"/v1/climate/country-risk-index/{slug}/{code}", key)
            pts = (d or {}).get("points") or []
            if not pts:
                continue
            pairs += 1
            for p in pts:
                rows.append([slug, code, name, p.get("date_on")] + [p.get(f) for f in FIELDS])
    return rows, pairs, skipped


def persist(rows: List[list], vintage_date) -> int:
    """Upsert current state + archive forecast rows under vintage_date. Returns row count."""
    from psycopg2.extras import execute_values
    cols = ['commodity_slug', 'country_code', 'country_name', 'date_on'] + FIELDS
    fc_idx = 4 + FIELDS.index('is_forecasted')
    vin_cols = ['vintage_date', 'commodity_slug', 'country_code', 'date_on'] + VINTAGE_FIELDS
    vin_rows = [
        [vintage_date, r[0], r[1], r[3]] + [r[4 + FIELDS.index(f)] for f in VINTAGE_FIELDS]
        for r in rows if r[fc_idx]
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, f"""
            INSERT INTO bronze.helios_climate_risk ({','.join(cols)}) VALUES %s
            ON CONFLICT (commodity_slug, country_code, date_on) DO UPDATE SET
            {','.join(f'{c}=EXCLUDED.{c}' for c in cols[2:])}, collected_at=now()""",
            rows, page_size=1000)
        if vin_rows:
            execute_values(cur, f"""
                INSERT INTO bronze.helios_climate_risk_vintage ({','.join(vin_cols)}) VALUES %s
                ON CONFLICT (vintage_date, commodity_slug, country_code, date_on) DO UPDATE SET
                {','.join(f'{c}=EXCLUDED.{c}' for c in vin_cols[4:])}, collected_at=now()""",
                vin_rows, page_size=1000)
        conn.commit()
    return len(rows)


class HeliosWAPRCollector:
    """Scheduler-facing wrapper (EIAV2CrudeDaily pattern)."""

    COLLECTOR_NAME = "helios_wapr"

    def collect(self, commodities: Optional[List[str]] = None, **kwargs):
        from src.agents.base.base_collector import CollectorResult
        key = os.environ.get("HELIOS_API_KEY", "")
        if not key:
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message="HELIOS_API_KEY not set in env")
        try:
            rows, pairs, skipped = fetch_all(key, commodities)
            if not rows:
                return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                       error_message="Helios returned no data for any pair")
            n = persist(rows, datetime.now(timezone.utc).date())
        except Exception as e:
            logger.error(f"helios_wapr failed: {e}")
            return CollectorResult(success=False, source=self.COLLECTOR_NAME,
                                   error_message=str(e))

        warnings = []
        if pairs < MIN_EXPECTED_PAIRS:
            warnings.append(f"coverage drop: only {pairs} pairs (expected >={MIN_EXPECTED_PAIRS})")
        if skipped:
            warnings.append(f"slugs no longer in Helios coverage: {skipped}")
        return CollectorResult(
            success=True,
            source=self.COLLECTOR_NAME,
            records_fetched=n,
            warnings=warnings,
        )


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    res = HeliosWAPRCollector().collect()
    print(res.success, res.records_fetched, res.warnings, res.error_message)
