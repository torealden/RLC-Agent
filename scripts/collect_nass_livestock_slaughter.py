"""NASS Livestock & Poultry Slaughter collector -> bronze.nass_livestock_slaughter.

Feeds the tallow chain: bronze.nass_livestock_slaughter -> silver.animal_slaughter ->
silver.tallow_production (SLAUGHTER_DERIVED) -> the tallow_biofuel_use guardrail the feedstock
allocator reads. Before this script the bronze table was a one-off load frozen at 2026-02 with
NO collector, which silently capped tallow allocation at Feb (SLAUGHTER_DERIVED is the oleo-trend
estimator base; the guardrail loop halts on any month missing it).

Pulls the NASS QuickStats "Livestock Slaughter" (cattle/hogs) and "Poultry Slaughter"
(chickens/turkeys) monthly national series, filtered to the SAME curated short_desc set already
in the table (commercial/FI aggregates), so the grain and downstream short_desc patterns are
preserved exactly. Idempotent upsert on (year, month, attribute).

Usage:
    python scripts/collect_nass_livestock_slaughter.py                # current + prior year
    python scripts/collect_nass_livestock_slaughter.py --years 2024 2025 2026
NASS releases each month's slaughter ~end of the following month.
"""
import os
import sys
import argparse
import logging
from pathlib import Path

import requests

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("nass_slaughter")

BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
COMMODITIES = ["CATTLE", "HOGS", "CHICKENS", "TURKEYS"]
MONTHS = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
          "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}

# Canonical curated mapping already established in bronze.nass_livestock_slaughter (20 short_descs,
# verified 1:1). Only these short_descs are kept; every finer NASS sub-cut is discarded so the grain
# matches the historical load and build_silver_animal_slaughter's short_desc patterns still resolve.
# short_desc -> (species, attribute, unit)
MAP = {
    "CATTLE, CALVES, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN HEAD": ("calves", "calves_slaughter_head", "HEAD"),
    "CATTLE, CALVES, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN LB, LIVE BASIS": ("calves", "calves_production_live_weight_lbs", "LB_LIVE"),
    "CATTLE, GE 500 LBS, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN HEAD": ("cattle", "cattle_slaughter_head", "HEAD"),
    "CATTLE, GE 500 LBS, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN LB / HEAD, LIVE BASIS": ("cattle", "cattle_avg_live_weight_per_head", "LB_PER_HEAD_LIVE"),
    "CATTLE, GE 500 LBS, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN LB, LIVE BASIS": ("cattle", "cattle_production_live_weight_lbs", "LB_LIVE"),
    "CHICKENS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": ("chickens", "chickens_slaughter_head", "HEAD"),
    "CHICKENS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN LB / HEAD, LIVE BASIS": ("chickens", "chickens_avg_live_weight_per_head", "LB_PER_HEAD_LIVE"),
    "CHICKENS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN LB, LIVE BASIS": ("chickens", "chickens_production_live_weight_lbs", "LB_LIVE"),
    "CHICKENS, YOUNG, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": ("broilers", "broilers_slaughter_head", "HEAD"),
    "CHICKENS, YOUNG, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN LB / HEAD, LIVE BASIS": ("broilers", "broilers_avg_live_weight_per_head", "LB_PER_HEAD_LIVE"),
    "CHICKENS, YOUNG, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN LB, LIVE BASIS": ("broilers", "broilers_production_live_weight_lbs", "LB_LIVE"),
    "HOGS, BARROWS & GILTS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD": ("hogs", "slaughter_head_fi_barrows_gilts", "HEAD"),
    "HOGS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD": ("hogs", "slaughter_head_fi", "HEAD"),
    "HOGS, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN HEAD": ("hogs", "slaughter_head", "HEAD"),
    "HOGS, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN LB / HEAD, LIVE BASIS": ("hogs", "avg_live_weight_per_head", "LB_PER_HEAD_LIVE"),
    "HOGS, SLAUGHTER, COMMERCIAL - SLAUGHTERED, MEASURED IN LB, LIVE BASIS": ("hogs", "production_live_weight_lbs", "LB_LIVE"),
    "HOGS, SOWS, SLAUGHTER, COMMERCIAL, FI - SLAUGHTERED, MEASURED IN HEAD": ("hogs", "slaughter_head_fi_sows", "HEAD"),
    "TURKEYS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN HEAD": ("turkeys", "turkeys_slaughter_head", "HEAD"),
    "TURKEYS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN LB / HEAD, LIVE BASIS": ("turkeys", "turkeys_avg_live_weight_per_head", "LB_PER_HEAD_LIVE"),
    "TURKEYS, SLAUGHTER, FI - SLAUGHTERED, MEASURED IN LB, LIVE BASIS": ("turkeys", "turkeys_production_live_weight_lbs", "LB_LIVE"),
}


def _fetch(commodity: str, year: int, api_key: str):
    """One NASS QuickStats call: national monthly slaughter for a commodity/year."""
    params = {
        "key": api_key, "format": "JSON",
        "commodity_desc": commodity, "statisticcat_desc": "SLAUGHTERED",
        "agg_level_desc": "NATIONAL", "freq_desc": "MONTHLY", "year": str(year),
    }
    r = requests.get(BASE_URL, params=params, timeout=90)
    if r.status_code == 401:
        raise RuntimeError("NASS API key rejected (401)")
    if r.status_code == 400:
        # NASS returns 400 when a query matches zero rows — treat as empty, not fatal.
        log.info("  %s %s: no records (400)", commodity, year)
        return []
    r.raise_for_status()
    return r.json().get("data", [])


def _parse_value(raw):
    """NASS Value string -> float, or None for withheld/suppressed ((D),(NA),(Z), blanks)."""
    if raw is None:
        return None
    s = str(raw).strip().replace(",", "")
    if not s or s.startswith("("):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def collect_slaughter(years=None, **kwargs) -> int:
    """Fetch + upsert NASS slaughter. Returns rows upserted."""
    api_key = os.environ.get("NASS_API_KEY")
    if not api_key:
        raise RuntimeError("NASS_API_KEY not set (register at quickstats.nass.usda.gov/api)")
    if not years:
        # default: current calendar year + prior (covers late-arriving revisions)
        cy = _current_year()
        years = [cy - 1, cy]

    batch = []
    for year in years:
        for commodity in COMMODITIES:
            data = _fetch(commodity, year, api_key)
            kept = 0
            for rec in data:
                sd = rec.get("short_desc")
                if sd not in MAP:
                    continue
                month = MONTHS.get((rec.get("reference_period_desc") or "").strip().upper())
                if month is None:
                    continue  # skip non-monthly reference periods (annual, marketing year)
                val = _parse_value(rec.get("Value"))
                if val is None:
                    continue
                species, attribute, unit = MAP[sd]
                batch.append((int(rec["year"]), month, attribute, sd, val, unit, "USDA_NASS", species))
                kept += 1
            log.info("  %s %s: %d records, %d kept", commodity, year, len(data), kept)

    if not batch:
        log.warning("No slaughter rows fetched")
        return 0

    with get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO bronze.nass_livestock_slaughter
                (year, month, attribute, short_desc, value, unit, source, species)
            VALUES %s
            ON CONFLICT (year, month, attribute) DO UPDATE
              SET value = EXCLUDED.value, short_desc = EXCLUDED.short_desc,
                  unit = EXCLUDED.unit, species = EXCLUDED.species,
                  source = EXCLUDED.source, collected_at = now()
        """, batch, page_size=500)
        conn.commit()
        cur.execute("SELECT max(year*100+month) mx FROM bronze.nass_livestock_slaughter")
        log.info("upserted %d rows; frontier now %s", len(batch), cur.fetchone()["mx"])
    return len(batch)


def _current_year() -> int:
    # Date.now() is fine in a normal script (only workflow scripts forbid it).
    import datetime
    return datetime.date.today().year


class _Result:
    """Minimal CollectorResult shape the dispatcher's runner reads (records_fetched)."""
    def __init__(self, n: int):
        self.records_fetched = n
        self.success = True


class NASSLivestockSlaughterCollector:
    """Dispatcher-registered collector — monthly NASS Livestock & Poultry Slaughter.

    Registered as 'nass_livestock_slaughter' in collector_registry + master_scheduler.
    Idempotent (upsert on year/month/attribute), so the daily-window retry pattern is safe.
    """
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def collect(self, **kwargs) -> _Result:
        years = kwargs.get("years") or self.kwargs.get("years")
        return _Result(collect_slaughter(years=years))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, nargs="+", help="years to pull (default: current + prior)")
    args = ap.parse_args()
    n = collect_slaughter(years=args.years)
    print(f"Done: {n} rows upserted to bronze.nass_livestock_slaughter")
