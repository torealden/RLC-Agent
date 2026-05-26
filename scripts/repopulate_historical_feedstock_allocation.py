"""
Repopulate bronze.historical_feedstock_allocation from gold.feedstock_allocation
================================================================================

Adds RLC-allocator-derived RD + SAF + coprocessing rows alongside the
existing eia_form819 (biodiesel canon) and fastmarkets (legacy reference)
data. NO ROWS ARE DELETED.

IP RULE (memory: feedback_fastmarkets_keep_dont_show):
The fastmarkets-sourced rows are kept in the DB for internal triangulation
and cross-check, but must NEVER appear in client-facing material. The new
source='rlc_allocator_v1' rows are what downstream consumers (eia_data.xlsm,
dashboards, reports, client deliverables) should read. The long-term goal
is facility-agent real-time allocation as the source of truth; the current
allocator is the interim.

What this does:
  1. DELETE any pre-existing source='rlc_allocator_v1' rows (so this script
     is idempotent — safe to re-run after another allocator pass).
  2. For each (period, scenario, fuel_type, feedstock_code, facility_id) in
     the LATEST run of gold.feedstock_allocation where fuel_type IN
     (renewable_diesel, saf, coprocessing): INSERT facility-level rows with
     source='rlc_allocator_v1'.
  3. eia_form819 (BD canon) and fastmarkets (legacy reference) rows are
     left untouched.

Downstream filtering rule: any client-facing consumer should filter source
NOT IN ('fastmarkets'). Prefer source IN ('eia_form819', 'rlc_allocator_v1').

Usage:
  python scripts/repopulate_historical_feedstock_allocation.py
  python scripts/repopulate_historical_feedstock_allocation.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')

from src.services.database.db_config import get_connection

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('repop_hfa')

# fuel_types our allocator covers that should be exposed downstream.
# Biodiesel is NOT included here — the eia_form819-sourced BD rows are
# EIA-canonical and should not be shadowed by a model output. If Tore
# wants to add BD from the allocator too, append 'biodiesel' here.
ALLOCATOR_FUEL_TYPES = ['renewable_diesel', 'saf', 'coprocessing']

NEW_SOURCE = 'rlc_allocator_v1'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would change without modifying anything')
    args = parser.parse_args()

    with get_connection() as conn:
        with conn.cursor() as cur:
            # --- 1. Show current state ---
            cur.execute("""
                SELECT source, COUNT(*) AS n, COUNT(DISTINCT period) AS months,
                       MIN(period) AS first, MAX(period) AS last
                FROM bronze.historical_feedstock_allocation
                GROUP BY 1 ORDER BY 1
            """)
            logger.info("Current state of bronze.historical_feedstock_allocation:")
            for r in cur.fetchall():
                logger.info(f"  {r['source']:20}  rows={r['n']:>5}  months={r['months']:>3}  "
                            f"{r['first']} -> {r['last']}")

            # --- 2. Show what we'd insert ---
            cur.execute("""
                WITH latest_per_period AS (
                    SELECT DISTINCT ON (scenario, period)
                           scenario, period, run_id
                    FROM gold.feedstock_allocation
                    WHERE fuel_type = ANY(%s) AND scenario = 'base'
                    ORDER BY scenario, period, created_at DESC
                )
                SELECT COUNT(*) AS n,
                       MIN(fa.period) AS first,
                       MAX(fa.period) AS last,
                       COUNT(DISTINCT fa.fuel_type) AS fuels,
                       COUNT(DISTINCT fa.facility_id) AS facilities
                FROM gold.feedstock_allocation fa
                JOIN latest_per_period lpp
                  ON lpp.scenario = fa.scenario
                 AND lpp.period   = fa.period
                 AND lpp.run_id   = fa.run_id
                WHERE fa.fuel_type = ANY(%s) AND fa.scenario = 'base'
            """, (ALLOCATOR_FUEL_TYPES, ALLOCATOR_FUEL_TYPES))
            r = cur.fetchone()
            logger.info(f"\nWould INSERT (facility-level): {r['n']} rows "
                        f"({r['first']} -> {r['last']}, {r['fuels']} fuels, "
                        f"{r['facilities']} facilities)")

            if args.dry_run:
                logger.info("\n--dry-run: no changes made")
                return

            # --- 3. Idempotent: remove any prior rlc_allocator_v1 rows ---
            cur.execute("""
                DELETE FROM bronze.historical_feedstock_allocation
                WHERE source = %s
            """, (NEW_SOURCE,))
            logger.info(f"DELETED {cur.rowcount} prior rows with source='{NEW_SOURCE}'")

            # --- 4. Insert from allocator output ---
            cur.execute("""
                WITH latest_per_period AS (
                    SELECT DISTINCT ON (scenario, period)
                           scenario, period, run_id
                    FROM gold.feedstock_allocation
                    WHERE fuel_type = ANY(%s) AND scenario = 'base'
                    ORDER BY scenario, period, created_at DESC
                )
                INSERT INTO bronze.historical_feedstock_allocation
                    (period, facility_id, facility_name, fuel_type, feedstock_code,
                     quantity_mil_lbs, quantity_mil_gal, pct_of_total, source, created_at)
                SELECT
                    fa.period,
                    fa.facility_id,
                    bf.facility_name,
                    fa.fuel_type,
                    fa.feedstock_code,
                    fa.allocated_mil_lbs,
                    fa.allocated_mil_gal,
                    fa.pct_of_facility,
                    %s,
                    NOW()
                FROM gold.feedstock_allocation fa
                JOIN latest_per_period lpp
                  ON lpp.scenario = fa.scenario
                 AND lpp.period   = fa.period
                 AND lpp.run_id   = fa.run_id
                LEFT JOIN reference.biofuel_facilities bf
                  ON bf.facility_id = fa.facility_id
                WHERE fa.fuel_type = ANY(%s) AND fa.scenario = 'base'
            """, (ALLOCATOR_FUEL_TYPES, NEW_SOURCE, ALLOCATOR_FUEL_TYPES))
            logger.info(f"INSERTED {cur.rowcount} rows (source='{NEW_SOURCE}')")

            conn.commit()

            # --- 5. Verify final state ---
            cur.execute("""
                SELECT source, COUNT(*) AS n, COUNT(DISTINCT period) AS months,
                       MIN(period) AS first, MAX(period) AS last
                FROM bronze.historical_feedstock_allocation
                GROUP BY 1 ORDER BY 1
            """)
            logger.info("\nFinal state:")
            for r in cur.fetchall():
                logger.info(f"  {r['source']:20}  rows={r['n']:>5}  months={r['months']:>3}  "
                            f"{r['first']} -> {r['last']}")


if __name__ == '__main__':
    main()
