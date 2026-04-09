"""
Collect UCO Model Inputs: Census CBP Restaurant Counts + USDA ERS FAFH Spending

Fetches two data sources needed for the UCO collection estimation model:
1. Census County Business Patterns — NAICS 722 restaurant establishment counts
2. USDA ERS Food Expenditure Series — Monthly food-away-from-home spending

Usage:
    python scripts/collect_uco_model_inputs.py
    python scripts/collect_uco_model_inputs.py --cbp-only
    python scripts/collect_uco_model_inputs.py --fafh-only
"""

import argparse
import csv
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / '.env')

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("uco_inputs")

CENSUS_API_KEY = os.getenv('CENSUS_API_KEY')

# NAICS 722 sub-codes for food service
NAICS_CODES = {
    '722': 'Food Services and Drinking Places',
    '7223': 'Special Food Services',
    '7224': 'Drinking Places (Alcoholic Beverages)',
    '7225': 'Restaurants and Other Eating Places',
    '72251': 'Full-Service Restaurants',
    '72252': 'Limited-Service Restaurants',
    '72253': 'Cafeterias, Grill Buffets, and Buffets',
}


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('RLC_PG_HOST'),
        dbname='rlc_commodities',
        user=os.getenv('RLC_PG_USER'),
        password=os.getenv('RLC_PG_PASSWORD'),
        sslmode='require',
    )


# ── Census CBP: Restaurant Establishment Counts ───────────────────────

def fetch_cbp_year(year):
    """Fetch Census CBP data for NAICS 722 sub-codes for one year."""
    # CBP API endpoint varies by year
    if year >= 2017:
        dataset = f'{year}/cbp'
        naics_param = 'NAICS2017'
    elif year >= 2012:
        dataset = f'{year}/cbp'
        naics_param = 'NAICS2012'
    else:
        dataset = f'{year}/cbp'
        naics_param = 'NAICS2007'

    base_url = f'https://api.census.gov/data/{dataset}'
    results = []

    for naics, desc in NAICS_CODES.items():
        params = {
            'get': 'ESTAB,EMP,PAYANN',
            'for': 'us:*',
            naics_param: naics,
            'key': CENSUS_API_KEY,
        }

        try:
            resp = requests.get(base_url, params=params, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:
                    headers = data[0]
                    row = data[1]
                    estab_idx = headers.index('ESTAB')
                    emp_idx = headers.index('EMP')
                    pay_idx = headers.index('PAYANN')
                    results.append({
                        'year': year,
                        'naics_code': naics,
                        'naics_description': desc,
                        'establishment_count': int(row[estab_idx]) if row[estab_idx] else None,
                        'employee_count': int(row[emp_idx]) if row[emp_idx] else None,
                        'annual_payroll_thousands': int(row[pay_idx]) if row[pay_idx] else None,
                    })
                    logger.info(f"  {year} NAICS {naics}: {row[estab_idx]} establishments")
            elif resp.status_code == 204:
                logger.debug(f"  {year} NAICS {naics}: no data")
            else:
                logger.warning(f"  {year} NAICS {naics}: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"  {year} NAICS {naics}: {e}")

        time.sleep(0.3)  # Rate limit

    return results


def collect_cbp():
    """Fetch Census CBP restaurant data for 2010-2023 and save to DB."""
    logger.info("=== Collecting Census CBP Restaurant Counts ===")

    conn = get_db_connection()
    cur = conn.cursor()
    total = 0

    for year in range(2010, 2024):
        logger.info(f"Fetching CBP {year}...")
        rows = fetch_cbp_year(year)

        for r in rows:
            cur.execute("""
                INSERT INTO bronze.census_cbp_restaurants
                    (year, naics_code, naics_description, geography, establishment_count,
                     employee_count, annual_payroll_thousands, updated_at)
                VALUES (%s, %s, %s, 'US', %s, %s, %s, NOW())
                ON CONFLICT (year, naics_code, COALESCE(state_fips, '00')) DO UPDATE SET
                    establishment_count = EXCLUDED.establishment_count,
                    employee_count = EXCLUDED.employee_count,
                    annual_payroll_thousands = EXCLUDED.annual_payroll_thousands,
                    updated_at = NOW()
            """, (r['year'], r['naics_code'], r['naics_description'],
                  r['establishment_count'], r['employee_count'],
                  r['annual_payroll_thousands']))
            total += 1

        conn.commit()
        time.sleep(0.5)

    conn.close()
    logger.info(f"CBP: Saved {total} records")
    return total


# ── USDA ERS: Monthly Food-Away-From-Home Spending ────────────────────

ERS_URLS = [
    "https://www.ers.usda.gov/media/5200/monthly-sales-of-food-with-taxes-and-tips-for-all-purchasers-by-outlet-type.csv",
    "https://www.ers.usda.gov/webdocs/DataFiles/50606/monthly_sales_food_all_purchasers.csv",
]


def collect_fafh():
    """Download ERS monthly food sales CSV and extract FAFH data."""
    logger.info("=== Collecting USDA ERS Food Expenditure Data ===")

    # Try to download the CSV
    csv_text = None
    for url in ERS_URLS:
        try:
            logger.info(f"  Trying: {url}")
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                csv_text = resp.text
                logger.info(f"  Downloaded {len(csv_text)} bytes")
                break
            else:
                logger.warning(f"  HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"  Failed: {e}")

    # Fall back to local sample if download fails
    if not csv_text:
        local_path = PROJECT_ROOT / "domain_knowledge" / "sample_reports" / "data" / "cross_commodity" / "Food Sales - 0925.csv"
        if local_path.exists():
            csv_text = local_path.read_text(encoding='utf-8-sig')
            logger.info(f"  Using local file: {local_path}")
        else:
            logger.error("  No ERS data available")
            return 0

    # Parse the CSV
    reader = csv.reader(io.StringIO(csv_text))
    rows_list = list(reader)

    if not rows_list:
        logger.error("  Empty CSV")
        return 0

    # Find header row (contains 'Year' or 'year')
    header_idx = None
    for i, row in enumerate(rows_list):
        if any('year' in str(c).lower() for c in row):
            header_idx = i
            break

    if header_idx is None:
        logger.error("  Could not find header row")
        return 0

    headers = [str(h).strip() for h in rows_list[header_idx]]
    logger.info(f"  Headers: {headers[:8]}...")

    conn = get_db_connection()
    cur = conn.cursor()
    total = 0

    MONTH_MAP = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
    }

    for row in rows_list[header_idx + 1:]:
        if len(row) < 3:
            continue

        try:
            year = int(str(row[0]).strip())
            month_raw = str(row[1]).strip().lower()
            month = MONTH_MAP.get(month_raw)
            if month is None:
                month = int(month_raw)
        except (ValueError, IndexError):
            continue

        if year < 1997 or year > 2030:
            continue

        # Parse each column as an outlet type
        for col_idx in range(2, min(len(headers), len(row))):
            outlet_type = headers[col_idx].strip()
            if not outlet_type:
                continue

            raw_val = str(row[col_idx]).strip()
            # Clean the value
            clean_val = raw_val.replace(',', '').replace('$', '').replace('"', '').strip()
            if not clean_val or clean_val in ('', 'NA', 'N/A', '.', '-'):
                continue

            try:
                sales_value = float(clean_val)
            except ValueError:
                continue

            # Determine food category from outlet name
            outlet_lower = outlet_type.lower()
            if 'away' in outlet_lower or 'restaurant' in outlet_lower or 'hotel' in outlet_lower or 'bar' in outlet_lower:
                food_category = 'Food away from home'
            elif 'home' in outlet_lower or 'grocery' in outlet_lower or 'store' in outlet_lower:
                food_category = 'Food at home'
            elif 'total' in outlet_lower:
                food_category = 'Total'
            else:
                food_category = 'Other'

            cur.execute("""
                INSERT INTO bronze.ers_food_sales_monthly
                    (year, month, outlet_type, purchaser_type, sales_value,
                     food_category, raw_value_text, updated_at)
                VALUES (%s, %s, %s, 'All purchasers', %s, %s, %s, NOW())
                ON CONFLICT (year, month, outlet_type, COALESCE(purchaser_type, 'ALL'), COALESCE(food_category, 'TOTAL'))
                DO UPDATE SET
                    sales_value = EXCLUDED.sales_value,
                    updated_at = NOW()
            """, (year, month, outlet_type, sales_value, food_category, raw_val))
            total += 1

    conn.commit()
    conn.close()
    logger.info(f"ERS FAFH: Saved {total} records")
    return total


def main():
    parser = argparse.ArgumentParser(description="Collect UCO model input data")
    parser.add_argument("--cbp-only", action="store_true")
    parser.add_argument("--fafh-only", action="store_true")
    args = parser.parse_args()

    if args.cbp_only:
        collect_cbp()
    elif args.fafh_only:
        collect_fafh()
    else:
        collect_fafh()
        collect_cbp()


if __name__ == '__main__':
    main()
