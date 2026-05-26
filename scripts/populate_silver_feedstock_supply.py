"""
Populate silver.feedstock_supply from bronze.eia_feedstock_monthly
==================================================================

The allocator's `_estimate_supply` fallback uses hard-coded national
supply totals that haven't been updated since the RD market exploded
in 2023. Result: allocator under-allocates by ~40% vs EIA actuals
for recent years.

This script replaces the hard-coded estimates with EIA-derived actuals:

  1. Group bronze.eia_feedstock_monthly by (year, month, feedstock_name)
     where plant_type='total'.
  2. Map feedstock_name -> feedstock_code via FEEDSTOCK_NAME_TO_CODE.
  3. Distribute national total across PADDs using PADD_WEIGHTS
     (same weights the allocator's _estimate_supply uses).
  4. Pull avg_price_per_lb from bronze.feedstock_prices.
  5. Upsert into silver.feedstock_supply.

When silver.feedstock_supply has data for a period, the allocator
prefers it over the estimate fallback (see allocator.py
`load_feedstock_supply`).

Caveats:
  - Pre-2022: data is BIODIESEL-ONLY (the old EIA report didn't cover
    RD). Allocator output for 2010-2021 will reflect BD-era totals.
    Acceptable since RD was small in that era.
  - DCO and UCO are not separately tracked in EIA Form 819. DCO falls
    out entirely; UCO is reported as Yellow Grease (which is the EIA
    convention — they don't distinguish UCO from YG). Both fall back
    to estimates unless we add custom logic.
  - 2021-2022 vegetable oil BD/RD splits not available (Form 819
    didn't introduce table_2c plant_type splits until 2023).

Usage:
  python scripts/populate_silver_feedstock_supply.py
  python scripts/populate_silver_feedstock_supply.py --year 2024
  python scripts/populate_silver_feedstock_supply.py --range 2010 2025
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')

from src.services.database.db_config import get_connection

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('populate_supply')

# EIA feedstock name -> allocator feedstock code
# Note: EIA Form 819 "Corn Oil" refers to distillers corn oil (DCO) from
# ethanol plants — the biofuel-relevant grade. Food-grade corn oil isn't
# meaningfully used for biofuel.
# UCO isn't separated from Yellow Grease in EIA reporting; allocator's
# UCO code falls back to its hard-coded estimate.
FEEDSTOCK_NAME_TO_CODE = {
    'Soybean Oil':     'SBO',
    'Canola Oil':      'CO',     # Canola Oil
    'Corn Oil':        'DCO',    # Distillers corn oil (biofuel grade)
    'Tallow':          'BFT',    # Total tallow — allocator splits EBFT/IBFT
    'White Grease':    'CWG',
    'Yellow Grease':   'YG',
    'Poultry':         'PF',     # Poultry fat
    'Cottonseed Oil':  'CSO',
}

PADD_WEIGHTS = {
    'PADD1': 0.08,
    'PADD2': 0.45,
    'PADD3': 0.30,
    'PADD4': 0.05,
    'PADD5': 0.12,
}

# How much of US production goes to biofuel vs other uses (food/industrial/exports)
# Used to estimate net_available_biofuel from total US production.
# EIA Form 819 reports CONSUMPTION at biofuel plants, which is already
# net_available_biofuel — so for these we set the share to 1.0.
BIOFUEL_SHARE = {
    'SBO':     1.0,  # EIA reports SBO going to biofuel plants
    'CO':      1.0,
    'DCO':     1.0,
    'BFT':     1.0,
    'CWG':     1.0,
    'YG':      1.0,
    'PF':      1.0,
    'CSO':     1.0,
}

# Region priorities for looking up avg prices (same as allocator._load_real_prices)
PRICE_REGIONS = {
    'SBO':     ['central_il', 'central_il_rbd', 'us_gulf', 'cbot_futures'],
    'CO':      ['central_us', 'canada_cnf', 'los_angeles'],
    'DCO':     ['il_wi', 'west_coast'],
    'BFT':     ['chicago', 'west_coast'],
    'CWG':     ['missouri_river', 'west_coast'],
    'YG':      ['il_wi', 'los_angeles'],
    'PF':      ['southeast', 'west_coast'],
    'CSO':     ['us_gulf'],
}


def get_price(cur, feedstock_code: str, period: date) -> float | None:
    """Get avg price per lb for a feedstock at a period."""
    regions = PRICE_REGIONS.get(feedstock_code, [])
    for region in regions:
        cur.execute("""
            SELECT price_per_lb FROM bronze.feedstock_prices
            WHERE feedstock_code = %s AND region = %s
              AND price_date <= %s AND price_per_lb > 0
            ORDER BY price_date DESC LIMIT 1
        """, (feedstock_code, region, period))
        row = cur.fetchone()
        if row and row['price_per_lb']:
            val = float(row['price_per_lb'])
            if val > 1.0:  # cents/lb -> $/lb
                val = val / 100.0
            return val
    return None


def fetch_eia_monthly(cur, year: int, month: int) -> dict:
    """Return {feedstock_name: total_mil_lbs} for the period.

    Prefers plant_type='total' (Form 819 era, 2022+) where available.
    Falls back to plant_type='biodiesel' for pre-2022 (old report era +
    USDA F&O), which is biodiesel-only — under-counts in that era is
    expected and acceptable since RD was small.
    """
    # First pass: try 'total' (Form 819)
    cur.execute("""
        SELECT feedstock_name, SUM(quantity_mil_lbs) AS total
        FROM bronze.eia_feedstock_monthly
        WHERE year = %s AND month = %s
          AND plant_type = 'total'
          AND is_withheld = FALSE
          AND quantity_mil_lbs IS NOT NULL
          AND feedstock_name = ANY(%s)
        GROUP BY 1
    """, (year, month, list(FEEDSTOCK_NAME_TO_CODE.keys())))
    out = {r['feedstock_name']: float(r['total']) for r in cur.fetchall()}

    # Second pass: for feedstocks NOT found in 'total', try 'biodiesel'
    missing = [n for n in FEEDSTOCK_NAME_TO_CODE if n not in out]
    if missing:
        cur.execute("""
            SELECT feedstock_name, SUM(quantity_mil_lbs) AS total
            FROM bronze.eia_feedstock_monthly
            WHERE year = %s AND month = %s
              AND plant_type = 'biodiesel'
              AND is_withheld = FALSE
              AND quantity_mil_lbs IS NOT NULL
              AND feedstock_name = ANY(%s)
            GROUP BY 1
        """, (year, month, missing))
        for r in cur.fetchall():
            out[r['feedstock_name']] = float(r['total'])

    return out


def populate_period(cur, year: int, month: int) -> int:
    """Build silver.feedstock_supply rows for one period. Returns row count."""
    period = date(year, month, 1)
    eia_data = fetch_eia_monthly(cur, year, month)
    if not eia_data:
        return 0

    inserted = 0
    for name, total_mil_lbs in eia_data.items():
        fs_code = FEEDSTOCK_NAME_TO_CODE.get(name)
        if not fs_code:
            continue
        share = BIOFUEL_SHARE.get(fs_code, 1.0)
        net_biofuel = total_mil_lbs * share
        price = get_price(cur, fs_code, period) or 0.40  # fallback price

        for padd, weight in PADD_WEIGHTS.items():
            regional_avail = net_biofuel * weight
            cur.execute("""
                INSERT INTO silver.feedstock_supply
                    (period, feedstock_code, region, domestic_production,
                     net_available_biofuel, avg_price_per_lb, source, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (period, feedstock_code, region) DO UPDATE SET
                    domestic_production = EXCLUDED.domestic_production,
                    net_available_biofuel = EXCLUDED.net_available_biofuel,
                    avg_price_per_lb = EXCLUDED.avg_price_per_lb,
                    source = EXCLUDED.source,
                    created_at = NOW()
            """, (
                period, fs_code, padd, total_mil_lbs, regional_avail,
                price, 'eia_form819_actual',
            ))
            inserted += 1

    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int, help='Single year (1-12 months)')
    parser.add_argument('--range', nargs=2, type=int, metavar=('START', 'END'),
                        help='Year range (inclusive)')
    args = parser.parse_args()

    if args.year:
        years = [args.year]
    elif args.range:
        years = list(range(args.range[0], args.range[1] + 1))
    else:
        # All available history
        years = list(range(2006, 2026))

    # Ensure silver.feedstock_supply has the right unique constraint
    with get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    ALTER TABLE silver.feedstock_supply
                    ADD CONSTRAINT feedstock_supply_period_code_region_key
                    UNIQUE (period, feedstock_code, region)
                """)
                conn.commit()
                logger.info("Added unique constraint to silver.feedstock_supply")
            except Exception as e:
                conn.rollback()
                # Constraint likely already exists — that's fine
                if 'already exists' not in str(e):
                    logger.warning(f"Constraint setup: {e}")

            total = 0
            for year in years:
                year_total = 0
                for month in range(1, 13):
                    try:
                        cur.execute("SAVEPOINT sp_supply")
                        n = populate_period(cur, year, month)
                        year_total += n
                        cur.execute("RELEASE SAVEPOINT sp_supply")
                    except Exception as e:
                        cur.execute("ROLLBACK TO SAVEPOINT sp_supply")
                        logger.error(f"  {year}-{month:02d}: {e}")
                logger.info(f"  {year}: {year_total} rows populated")
                total += year_total
            conn.commit()
            logger.info(f"Done: {total} rows total")


if __name__ == '__main__':
    main()
