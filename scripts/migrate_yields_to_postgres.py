"""
Migrate historical state-level crop yield data into PostgreSQL.

Sources:
  1. NASS QuickStats API — state-level area, yield, production (2000–present)
  2. Existing SQLite data (if available)

Outputs:
  - bronze.nass_state_yields  — raw state/year yield data
  - silver.yield_trend        — linear trend coefficients per state/crop

Usage:
  python scripts/migrate_yields_to_postgres.py --fetch-api        # Fetch from NASS QuickStats
  python scripts/migrate_yields_to_postgres.py --compute-trends   # Only recompute trends
  python scripts/migrate_yields_to_postgres.py --verify           # Print verification summary
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# State abbreviation ↔ full name mapping
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming',
}
STATE_TO_ABBREV = {v.upper(): k for k, v in US_STATES.items()}

# NASS QuickStats commodity mappings
COMMODITY_MAP = {
    'CORN': {'commodity_desc': 'CORN', 'util_practice_desc': 'GRAIN', 'yield_unit': 'bu/acre', 'prod_unit': '1000_bu'},
    'SOYBEANS': {'commodity_desc': 'SOYBEANS', 'yield_unit': 'bu/acre', 'prod_unit': '1000_bu'},
    'WHEAT_WINTER': {'commodity_desc': 'WHEAT', 'class_desc': 'WINTER', 'yield_unit': 'bu/acre', 'prod_unit': '1000_bu'},
    'WHEAT_SPRING': {'commodity_desc': 'WHEAT', 'class_desc': 'SPRING, (EXCL DURUM)', 'yield_unit': 'bu/acre', 'prod_unit': '1000_bu'},
    'WHEAT_ALL': {'commodity_desc': 'WHEAT', 'yield_unit': 'bu/acre', 'prod_unit': '1000_bu'},
    'COTTON': {'commodity_desc': 'COTTON', 'util_practice_desc': 'ALL UTILIZATION PRACTICES', 'yield_unit': 'lb/acre', 'prod_unit': '1000_480lb_bales'},
}


def get_db_connection():
    """Get PostgreSQL connection."""
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")

    password = (
        os.environ.get("RLC_PG_PASSWORD")
        or os.environ.get("DATABASE_PASSWORD")
        or os.environ.get("DB_PASSWORD")
    )
    return psycopg2.connect(
        host=os.environ.get("DATABASE_HOST", "localhost"),
        port=os.environ.get("DATABASE_PORT", "5432"),
        database=os.environ.get("DATABASE_NAME", "rlc_commodities"),
        user=os.environ.get("DATABASE_USER", "postgres"),
        password=password,
    )


def fetch_nass_quickstats(commodity_key: str, min_year: int = 2000) -> list:
    """
    Fetch state-level yield data from NASS QuickStats API.

    Returns list of dicts with: state, year, area_planted, area_harvested,
    yield_per_acre, production.
    """
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.environ.get("NASS_API_KEY") or os.environ.get("USDA_NASS_API_KEY")
    if not api_key:
        logger.error("No NASS_API_KEY found in .env")
        return []

    base_url = "https://quickstats.nass.usda.gov/api/api_GET/"
    config = COMMODITY_MAP[commodity_key]

    results = []

    def _parse_value(raw):
        """Parse a NASS value string, returning float or None."""
        if raw is None:
            return None
        val = str(raw).strip().replace(',', '')
        if not val or val in ('(D)', '(NA)', '(Z)', '(S)'):
            return None
        try:
            return float(val)
        except ValueError:
            return None

    def _fetch_stat(stat_desc, extra_params=None):
        """Fetch one statistic from QuickStats, returning {(state,year): value}."""
        p = {
            'key': api_key,
            'commodity_desc': config['commodity_desc'],
            'statisticcat_desc': stat_desc,
            'agg_level_desc': 'STATE',
            'prodn_practice_desc': 'ALL PRODUCTION PRACTICES',
            'year__GE': str(min_year),
            'format': 'JSON',
        }
        if 'class_desc' in config:
            p['class_desc'] = config['class_desc']
        if 'util_practice_desc' in config:
            p['util_practice_desc'] = config['util_practice_desc']
        if extra_params:
            p.update(extra_params)

        logger.info(f"Fetching {commodity_key} {stat_desc}...")
        result = {}
        try:
            resp = requests.get(base_url, params=p, timeout=60)
            resp.raise_for_status()
            data = resp.json().get('data', [])
            logger.info(f"  Got {len(data)} records")
            for row in data:
                state = row.get('state_name', '').upper()
                year_str = row.get('year', '0')
                try:
                    year = int(year_str)
                except ValueError:
                    continue
                val = _parse_value(row.get('Value'))
                if val is not None:
                    result[(state, year)] = val
        except Exception as e:
            logger.error(f"NASS API error fetching {stat_desc}: {e}")
        time.sleep(1)
        return result

    yield_by_state_year = _fetch_stat('YIELD')
    planted_by_state_year = _fetch_stat('AREA PLANTED')
    harvested_by_state_year = _fetch_stat('AREA HARVESTED')
    production_by_state_year = _fetch_stat('PRODUCTION')

    # Merge all data by (state, year)
    all_keys = set(yield_by_state_year.keys()) | set(planted_by_state_year.keys()) | set(harvested_by_state_year.keys())

    for state, year in all_keys:
        abbrev = STATE_TO_ABBREV.get(state)
        results.append({
            'commodity': commodity_key,
            'state': state.title(),
            'state_abbrev': abbrev,
            'year': year,
            'area_planted': planted_by_state_year.get((state, year)),
            'area_harvested': harvested_by_state_year.get((state, year)),
            'yield_per_acre': yield_by_state_year.get((state, year)),
            'production': production_by_state_year.get((state, year)),
            'yield_unit': config['yield_unit'],
            'production_unit': config['prod_unit'],
        })

    return results


def load_to_postgres(rows: list) -> int:
    """Upsert yield data into bronze.nass_state_yields."""
    if not rows:
        return 0

    conn = get_db_connection()
    cur = conn.cursor()
    count = 0

    try:
        for r in rows:
            cur.execute("""
                INSERT INTO bronze.nass_state_yields
                    (commodity, state, state_abbrev, year,
                     area_planted, area_harvested, yield_per_acre, production,
                     yield_unit, production_unit, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'NASS_QUICKSTATS')
                ON CONFLICT (commodity, state, year)
                DO UPDATE SET
                    state_abbrev = EXCLUDED.state_abbrev,
                    area_planted = COALESCE(EXCLUDED.area_planted, bronze.nass_state_yields.area_planted),
                    area_harvested = COALESCE(EXCLUDED.area_harvested, bronze.nass_state_yields.area_harvested),
                    yield_per_acre = COALESCE(EXCLUDED.yield_per_acre, bronze.nass_state_yields.yield_per_acre),
                    production = COALESCE(EXCLUDED.production, bronze.nass_state_yields.production),
                    yield_unit = EXCLUDED.yield_unit,
                    production_unit = EXCLUDED.production_unit,
                    collected_at = NOW()
            """, (
                r['commodity'], r['state'], r.get('state_abbrev'),
                r['year'], r.get('area_planted'), r.get('area_harvested'),
                r.get('yield_per_acre'), r.get('production'),
                r['yield_unit'], r['production_unit'],
            ))
            count += 1

        conn.commit()
        logger.info(f"Loaded {count} rows to bronze.nass_state_yields")

    except Exception as e:
        conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

    return count


def compute_trends():
    """Compute linear and quadratic yield trends per state/crop."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Get all commodity/state combinations with enough data
        cur.execute("""
            SELECT commodity, state, state_abbrev,
                   ARRAY_AGG(year ORDER BY year) AS years,
                   ARRAY_AGG(yield_per_acre ORDER BY year) AS yields
            FROM bronze.nass_state_yields
            WHERE yield_per_acre IS NOT NULL
            GROUP BY commodity, state, state_abbrev
            HAVING COUNT(*) >= 10
            ORDER BY commodity, state
        """)
        rows = cur.fetchall()
        logger.info(f"Computing trends for {len(rows)} state/commodity combinations")

        current_year = datetime.now().year
        count = 0

        for commodity, state, abbrev, years, yields in rows:
            years_arr = np.array(years, dtype=float)
            yields_arr = np.array(yields, dtype=float)

            # Linear trend
            coeffs = np.polyfit(years_arr, yields_arr, 1)
            slope, intercept = coeffs[0], coeffs[1]
            predicted = np.polyval(coeffs, years_arr)
            ss_res = np.sum((yields_arr - predicted) ** 2)
            ss_tot = np.sum((yields_arr - np.mean(yields_arr)) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            trend_current = intercept + slope * current_year
            years_range = f"{int(years_arr.min())}-{int(years_arr.max())}"

            # Warn on unusual slopes
            if commodity in ('CORN',) and abs(slope) > 3.0:
                logger.warning(f"  {commodity} {state}: slope={slope:.2f} bu/acre/year (unusual)")
            if commodity in ('SOYBEANS',) and abs(slope) > 1.5:
                logger.warning(f"  {commodity} {state}: slope={slope:.2f} bu/acre/year (unusual)")

            cur.execute("""
                INSERT INTO silver.yield_trend
                    (commodity, state, trend_type, intercept, slope, r_squared,
                     years_used, trend_yield_current)
                VALUES (%s, %s, 'linear', %s, %s, %s, %s, %s)
                ON CONFLICT (commodity, state, trend_type)
                DO UPDATE SET
                    intercept = EXCLUDED.intercept,
                    slope = EXCLUDED.slope,
                    r_squared = EXCLUDED.r_squared,
                    years_used = EXCLUDED.years_used,
                    trend_yield_current = EXCLUDED.trend_yield_current,
                    updated_at = NOW()
            """, (commodity, state, float(intercept), float(slope),
                  float(r_squared), years_range, float(trend_current)))
            count += 1

            # Quadratic trend (if enough data points)
            if len(years_arr) >= 15:
                coeffs_q = np.polyfit(years_arr, yields_arr, 2)
                predicted_q = np.polyval(coeffs_q, years_arr)
                ss_res_q = np.sum((yields_arr - predicted_q) ** 2)
                r_sq_q = 1 - (ss_res_q / ss_tot) if ss_tot > 0 else 0
                trend_current_q = coeffs_q[0] * current_year**2 + coeffs_q[1] * current_year + coeffs_q[2]

                cur.execute("""
                    INSERT INTO silver.yield_trend
                        (commodity, state, trend_type, intercept, slope,
                         slope_quadratic, r_squared, years_used, trend_yield_current)
                    VALUES (%s, %s, 'quadratic', %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (commodity, state, trend_type)
                    DO UPDATE SET
                        intercept = EXCLUDED.intercept,
                        slope = EXCLUDED.slope,
                        slope_quadratic = EXCLUDED.slope_quadratic,
                        r_squared = EXCLUDED.r_squared,
                        years_used = EXCLUDED.years_used,
                        trend_yield_current = EXCLUDED.trend_yield_current,
                        updated_at = NOW()
                """, (commodity, state, float(coeffs_q[2]), float(coeffs_q[1]),
                      float(coeffs_q[0]), float(r_sq_q), years_range,
                      float(trend_current_q)))
                count += 1

        conn.commit()
        logger.info(f"Saved {count} trend rows to silver.yield_trend")

        # Print R-squared summary
        cur.execute("""
            SELECT commodity, trend_type,
                   COUNT(*) as n,
                   ROUND(AVG(r_squared)::numeric, 3) as avg_r2,
                   ROUND(MIN(r_squared)::numeric, 3) as min_r2,
                   ROUND(MAX(r_squared)::numeric, 3) as max_r2
            FROM silver.yield_trend
            GROUP BY commodity, trend_type
            ORDER BY commodity, trend_type
        """)
        print(f"\n{'Commodity':<16} {'Type':<12} {'N':>4} {'Avg R2':>8} {'Min R2':>8} {'Max R2':>8}")
        print(f"{'-'*16} {'-'*12} {'-'*4} {'-'*8} {'-'*8} {'-'*8}")
        for row in cur.fetchall():
            print(f"{row[0]:<16} {row[1]:<12} {row[2]:>4} {row[3]:>8} {row[4]:>8} {row[5]:>8}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Trend computation error: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def verify():
    """Print summary of loaded yield data."""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        print(f"\n{'='*70}")
        print(f"  State-Level Yield Data — Verification")
        print(f"{'='*70}")

        cur.execute("""
            SELECT commodity, COUNT(*) as rows,
                   COUNT(DISTINCT state) as states,
                   MIN(year) as min_yr, MAX(year) as max_yr,
                   COUNT(yield_per_acre) as has_yield,
                   ROUND(AVG(yield_per_acre)::numeric, 1) as avg_yield
            FROM bronze.nass_state_yields
            GROUP BY commodity
            ORDER BY commodity
        """)
        rows = cur.fetchall()

        if not rows:
            print("  No data in bronze.nass_state_yields")
        else:
            print(f"\n  {'Commodity':<16} {'Rows':>6} {'States':>7} {'Years':>12} {'w/Yield':>8} {'Avg Yield':>10}")
            print(f"  {'-'*16} {'-'*6} {'-'*7} {'-'*12} {'-'*8} {'-'*10}")
            total = 0
            for commodity, count, states, min_yr, max_yr, has_yield, avg_yield in rows:
                avg_str = f"{avg_yield}" if avg_yield is not None else "N/A"
                print(f"  {commodity:<16} {count:>6} {states:>7} {min_yr}-{max_yr:>6} {has_yield:>8} {avg_str:>10}")
                total += count
            print(f"  {'TOTAL':<16} {total:>6}")

        # Trend summary
        cur.execute("SELECT COUNT(*) FROM silver.yield_trend")
        trend_count = cur.fetchone()[0]
        print(f"\n  Trend coefficients: {trend_count} rows")

        if trend_count > 0:
            cur.execute("""
                SELECT commodity, state, slope, r_squared, trend_yield_current
                FROM silver.yield_trend
                WHERE trend_type = 'linear'
                ORDER BY r_squared DESC LIMIT 5
            """)
            print(f"\n  Top 5 best-fit trends:")
            for commodity, state, slope, r2, trend in cur.fetchall():
                print(f"    {commodity} {state}: slope={slope:.2f}/yr, R²={r2:.3f}, trend={trend:.1f}")

        print(f"{'='*70}\n")

    finally:
        cur.close()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Migrate state-level yield data to PostgreSQL")
    parser.add_argument("--fetch-api", action="store_true",
                        help="Fetch from NASS QuickStats API")
    parser.add_argument("--compute-trends", action="store_true",
                        help="Only recompute trend coefficients")
    parser.add_argument("--verify", action="store_true",
                        help="Print verification summary")
    parser.add_argument("--commodities", type=str, default="CORN,SOYBEANS,WHEAT_ALL,COTTON",
                        help="Comma-separated commodity list")
    parser.add_argument("--min-year", type=int, default=2000,
                        help="Minimum year to fetch")
    parser.add_argument("--verbose", "-v", action="store_true")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.verify:
        verify()
        return

    if args.compute_trends:
        compute_trends()
        return

    if args.fetch_api:
        commodities = [c.strip() for c in args.commodities.split(",")]
        total = 0
        for commodity in commodities:
            if commodity not in COMMODITY_MAP:
                logger.warning(f"Unknown commodity: {commodity}, skipping")
                continue
            rows = fetch_nass_quickstats(commodity, min_year=args.min_year)
            if rows:
                count = load_to_postgres(rows)
                total += count
                print(f"  {commodity}: {count} rows loaded")

        print(f"\nTotal rows loaded: {total}")

        # Compute trends after loading
        print("\nComputing trend yields...")
        compute_trends()

        # Verify
        verify()
        return

    # Default: show help
    parser.print_help()


if __name__ == "__main__":
    main()
