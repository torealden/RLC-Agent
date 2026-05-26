"""
Interpolate 2021-2022 vegetable oil supply for silver.feedstock_supply
=======================================================================

Background: EIA Form 819 launched the table_2c vegetable oil BD/RD
splits only in 2023. For 2021-2022, EIA's xlsx had table_2c empty —
so bronze.eia_feedstock_monthly has zero rows for SBO/Canola/Corn-Oil
in those two years. The populator (which reads bronze) consequently
left silver.feedstock_supply with no veg oil records for 2021-2022,
and the allocator allocated only tallow + grease for those months.

This script fills the gap with a linear interpolation between:
  - 2020 monthly averages (BD-only era, old EIA report)
  - 2023 monthly averages (full BD+RD Form 819 era)

For each (feedstock × PADD × month) in 2021-2022:
    interpolated = 2020_avg + ((month_idx - 12) / 25) * (2023_avg - 2020_avg)
  where month_idx counts from 2020-12 (idx=0) through 2023-01 (idx=25).

Feedstocks filled: SBO, CO (Canola), DCO (the EIA "Corn Oil" = DCO).

Inserts into silver.feedstock_supply with source='interpolated_2020_2023'
so the placeholder origin is explicit in any downstream join.

Caveats:
  - Linear interpolation under-represents the explosive RD growth that
    actually happened mid-2022 → late-2023. Conservative for 2022.
  - This is a placeholder; the right long-term fix is bottom-up from
    facility production + the biotracker (rail-car flow data).

Usage:
  python scripts/interpolate_2021_2022_veg_oil_supply.py
  python scripts/interpolate_2021_2022_veg_oil_supply.py --dry-run
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
logger = logging.getLogger('interp_veg')

FEEDSTOCKS = ['SBO', 'CO', 'DCO']
PADDS = ['PADD1', 'PADD2', 'PADD3', 'PADD4', 'PADD5']
TAG = 'interpolated_2020_2023'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get 2020 monthly average per (feedstock, PADD)
            cur.execute("""
                SELECT feedstock_code, region, AVG(net_available_biofuel) AS avg_monthly
                FROM silver.feedstock_supply
                WHERE EXTRACT(YEAR FROM period) = 2020
                  AND feedstock_code = ANY(%s)
                GROUP BY 1, 2
            """, (FEEDSTOCKS,))
            anchors_2020 = {(r['feedstock_code'], r['region']): float(r['avg_monthly']) for r in cur.fetchall()}

            # Get 2023 monthly average per (feedstock, PADD)
            cur.execute("""
                SELECT feedstock_code, region, AVG(net_available_biofuel) AS avg_monthly
                FROM silver.feedstock_supply
                WHERE EXTRACT(YEAR FROM period) = 2023
                  AND feedstock_code = ANY(%s)
                GROUP BY 1, 2
            """, (FEEDSTOCKS,))
            anchors_2023 = {(r['feedstock_code'], r['region']): float(r['avg_monthly']) for r in cur.fetchall()}

            # Get 2023 price per feedstock (use any PADD as proxy; price doesn't vary by PADD here)
            cur.execute("""
                SELECT feedstock_code, AVG(avg_price_per_lb) AS price
                FROM silver.feedstock_supply
                WHERE EXTRACT(YEAR FROM period) = 2023
                  AND feedstock_code = ANY(%s)
                GROUP BY 1
            """, (FEEDSTOCKS,))
            prices = {r['feedstock_code']: float(r['price']) for r in cur.fetchall()}

            logger.info(f"Anchor counts: 2020={len(anchors_2020)}, 2023={len(anchors_2023)}, prices={len(prices)}")

            # Build interpolated rows for 2021-01 through 2022-12 (24 months)
            # Index: 2020-12 = 0, ..., 2023-01 = 25 (total 26 steps).
            # So 2021-01 = idx 1, 2022-12 = idx 24.
            inserts = []
            for fs in FEEDSTOCKS:
                for padd in PADDS:
                    key = (fs, padd)
                    a20 = anchors_2020.get(key)
                    a23 = anchors_2023.get(key)
                    if a20 is None and a23 is None:
                        continue
                    # Use 0 as fallback for missing anchor (2020 BD-only didn't have DCO etc.)
                    a20 = a20 or 0.0
                    a23 = a23 or 0.0
                    price = prices.get(fs, 0.40)

                    for year in (2021, 2022):
                        for month in range(1, 13):
                            idx = (year - 2020) * 12 + (month - 12)  # 2021-01 -> idx 1, 2022-12 -> idx 24
                            ratio = idx / 25.0
                            val = a20 + ratio * (a23 - a20)
                            inserts.append({
                                'period': date(year, month, 1),
                                'fs': fs,
                                'region': padd,
                                'avail': val,
                                'price': price,
                            })

            logger.info(f"Built {len(inserts)} interpolated rows for 2021-2022")
            if inserts[:3]:
                logger.info("Sample (first 3):")
                for r in inserts[:3]:
                    logger.info(f"  {r['period']} {r['fs']}/{r['region']}  avail={r['avail']:.1f}  price={r['price']:.3f}")

            if args.dry_run:
                logger.info("--dry-run: no changes made")
                return

            # Delete existing 2021-2022 rows for these feedstocks (clean slate)
            cur.execute("""
                DELETE FROM silver.feedstock_supply
                WHERE EXTRACT(YEAR FROM period) IN (2021, 2022)
                  AND feedstock_code = ANY(%s)
            """, (FEEDSTOCKS,))
            logger.info(f"DELETED {cur.rowcount} pre-existing 2021-2022 veg oil rows")

            # Insert interpolated rows
            for r in inserts:
                cur.execute("""
                    INSERT INTO silver.feedstock_supply
                        (period, feedstock_code, region, net_available_biofuel,
                         avg_price_per_lb, source, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (period, feedstock_code, region) DO UPDATE SET
                        net_available_biofuel = EXCLUDED.net_available_biofuel,
                        avg_price_per_lb = EXCLUDED.avg_price_per_lb,
                        source = EXCLUDED.source,
                        created_at = NOW()
                """, (r['period'], r['fs'], r['region'], r['avail'], r['price'], TAG))
            conn.commit()
            logger.info(f"INSERTED {len(inserts)} interpolated rows")

            # Verify
            cur.execute("""
                SELECT EXTRACT(YEAR FROM period)::int AS yr,
                       COUNT(DISTINCT feedstock_code) AS n_feed,
                       SUM(net_available_biofuel) / 12.0 AS avg_monthly_total
                FROM silver.feedstock_supply
                WHERE EXTRACT(YEAR FROM period) IN (2020, 2021, 2022, 2023)
                GROUP BY 1 ORDER BY 1
            """)
            logger.info("Post-interpolation summary (2020-2023):")
            for r in cur.fetchall():
                logger.info(f"  {r['yr']}  {r['n_feed']} feedstocks  avg_total={float(r['avg_monthly_total']):>6,.0f} mil lbs/mo")


if __name__ == '__main__':
    main()
