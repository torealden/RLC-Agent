"""
Backfill NASS Crop Condition and Progress Data (2000-2025)

Pulls state-level weekly crop condition and progress data from
NASS Quick Stats API for corn, soybeans, wheat, sorghum, cotton.

Usage:
    python scripts/backfill_nass_conditions.py
    python scripts/backfill_nass_conditions.py --start-year 2010 --end-year 2020
    python scripts/backfill_nass_conditions.py --commodity corn --data-type condition
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("nass_backfill")

NASS_API_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
NASS_API_KEY = os.getenv("NASS_API_KEY")

COMMODITIES = {
    'corn': {'commodity_desc': 'CORN'},
    'soybeans': {'commodity_desc': 'SOYBEANS'},
    'wheat': {'commodity_desc': 'WHEAT'},
    'sorghum': {'commodity_desc': 'SORGHUM'},
    'cotton': {'commodity_desc': 'COTTON'},
    'barley': {'commodity_desc': 'BARLEY'},
}

CONDITION_CATEGORIES = ['EXCELLENT', 'GOOD', 'FAIR', 'POOR', 'VERY POOR']


def get_connection():
    return psycopg2.connect(
        host=os.getenv('RLC_PG_HOST', 'localhost'),
        port=5432,
        dbname='rlc_commodities',
        user='postgres',
        password=os.getenv('RLC_PG_PASSWORD', os.getenv('DB_PASSWORD', '')),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def fetch_nass(commodity_desc, statisticcat_desc, year, agg_level='STATE'):
    """Fetch one year of data from NASS API with retry logic."""
    params = {
        'key': NASS_API_KEY,
        'format': 'JSON',
        'commodity_desc': commodity_desc,
        'statisticcat_desc': statisticcat_desc,
        'year': str(year),
        'freq_desc': 'WEEKLY',
        'agg_level_desc': agg_level,
    }

    for attempt in range(3):
        try:
            resp = requests.get(NASS_API_URL, params=params, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('data', [])
            elif resp.status_code == 413:
                logger.warning(f"  Too many records for {commodity_desc} {year} at {agg_level} level")
                return []
            else:
                logger.warning(f"  HTTP {resp.status_code} for {commodity_desc} {year}")
                return []
        except Exception as e:
            logger.warning(f"  Attempt {attempt + 1}/3 failed: {e}")
            if attempt < 2:
                time.sleep(3)
    logger.error(f"  All 3 attempts failed for {commodity_desc} {year}")
    return []


def parse_week_ending(item):
    """Parse the week_ending date from NASS reference_period_desc or end_code."""
    # NASS uses reference_period_desc like "WEEK #18" and end_code like "20250504"
    end_code = item.get('end_code', '')
    if end_code and len(end_code) == 8:
        try:
            return date(int(end_code[:4]), int(end_code[4:6]), int(end_code[6:8]))
        except ValueError:
            pass

    # Fallback: try to construct from year + week
    year = int(item.get('year', 0))
    ref = item.get('reference_period_desc', '')
    if 'WEEK' in ref:
        try:
            week_num = int(ref.replace('WEEK #', '').replace('WEEK', '').strip())
            # Approximate: week number × 7 days from Jan 1
            d = date(year, 1, 1) + __import__('datetime').timedelta(weeks=week_num - 1, days=6)
            return d
        except Exception:
            pass
    return None


def parse_value(val_str):
    """Parse NASS value string to float."""
    if val_str is None or val_str == '' or val_str == '(D)' or val_str == '(NA)':
        return None
    try:
        return float(val_str.replace(',', ''))
    except (ValueError, TypeError):
        return None


def save_conditions(conn, records):
    """Save condition records to bronze.nass_crop_condition.

    Schema: commodity, year, week_ending, reference_period, state, agg_level,
            condition_category, short_desc, unit, value, source, collected_at
    """
    if not records:
        return 0

    sql = """
        INSERT INTO bronze.nass_crop_condition
            (commodity, year, week_ending, reference_period, state, agg_level,
             condition_category, short_desc, unit, value, source, collected_at)
        VALUES (%(commodity)s, %(year)s, %(week_ending)s, %(reference_period)s,
                %(state)s, %(agg_level)s, %(condition_category)s, %(short_desc)s,
                %(unit)s, %(value)s, %(source)s, NOW())
        ON CONFLICT DO NOTHING
    """
    cur = conn.cursor()
    count = 0
    for rec in records:
        try:
            cur.execute(sql, rec)
            count += cur.rowcount
        except Exception as e:
            logger.debug(f"  Insert error: {e}")
            conn.rollback()
            continue
    conn.commit()
    return count


def save_progress(conn, records):
    """Save progress records to bronze.nass_crop_progress.

    Schema: commodity, year, week_ending, reference_period, state, agg_level,
            statisticcat, short_desc, unit, value, source, collected_at
    """
    if not records:
        return 0

    sql = """
        INSERT INTO bronze.nass_crop_progress
            (commodity, year, week_ending, reference_period, state, agg_level,
             statisticcat, short_desc, unit, value, source, collected_at)
        VALUES (%(commodity)s, %(year)s, %(week_ending)s, %(reference_period)s,
                %(state)s, %(agg_level)s, %(statisticcat)s, %(short_desc)s,
                %(unit)s, %(value)s, %(source)s, NOW())
        ON CONFLICT DO NOTHING
    """
    cur = conn.cursor()
    count = 0
    for rec in records:
        try:
            cur.execute(sql, rec)
            count += cur.rowcount
        except Exception as e:
            logger.debug(f"  Insert error: {e}")
            conn.rollback()
            continue
    conn.commit()
    return count


def process_condition_data(raw_items, commodity_key):
    """Parse condition items into flat records matching bronze.nass_crop_condition schema."""
    records = []
    for item in raw_items:
        week_ending = parse_week_ending(item)
        if not week_ending:
            continue

        value = parse_value(item.get('Value'))
        if value is None:
            continue

        # Extract condition category from short_desc
        short_desc = item.get('short_desc', '')
        category = None
        sd_upper = short_desc.upper()
        if 'EXCELLENT' in sd_upper:
            category = 'EXCELLENT'
        elif 'VERY POOR' in sd_upper:
            category = 'VERY POOR'
        elif 'POOR' in sd_upper:
            category = 'POOR'
        elif 'GOOD' in sd_upper:
            category = 'GOOD'
        elif 'FAIR' in sd_upper:
            category = 'FAIR'

        if not category:
            continue

        records.append({
            'commodity': commodity_key,
            'year': int(item.get('year', 0)),
            'week_ending': week_ending,
            'reference_period': item.get('reference_period_desc', ''),
            'state': item.get('state_alpha', 'US'),
            'agg_level': item.get('agg_level_desc', 'STATE'),
            'condition_category': category,
            'short_desc': short_desc,
            'unit': item.get('unit_desc', 'PCT'),
            'value': value,
            'source': 'NASS_BACKFILL',
        })

    return records


def process_progress_data(raw_items, commodity_key):
    """Parse progress items into flat records matching bronze.nass_crop_progress schema."""
    records = []
    for item in raw_items:
        week_ending = parse_week_ending(item)
        if not week_ending:
            continue

        value = parse_value(item.get('Value'))
        if value is None:
            continue

        records.append({
            'commodity': commodity_key,
            'year': int(item.get('year', 0)),
            'week_ending': week_ending,
            'reference_period': item.get('reference_period_desc', ''),
            'state': item.get('state_alpha', 'US'),
            'agg_level': item.get('agg_level_desc', 'STATE'),
            'statisticcat': item.get('statisticcat_desc', 'PROGRESS'),
            'short_desc': item.get('short_desc', ''),
            'unit': item.get('unit_desc', 'PCT'),
            'value': value,
            'source': 'NASS_BACKFILL',
        })

    return records


def main():
    parser = argparse.ArgumentParser(description="Backfill NASS crop conditions/progress")
    parser.add_argument("--start-year", type=int, default=2000)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--commodity", help="Single commodity (default: all)")
    parser.add_argument("--data-type", choices=['condition', 'progress', 'both'], default='both')
    parser.add_argument("--level", choices=['STATE', 'NATIONAL'], default='STATE')
    args = parser.parse_args()

    if not NASS_API_KEY:
        logger.error("NASS_API_KEY not set in environment")
        sys.exit(1)

    conn = get_connection()
    commodities = {args.commodity: COMMODITIES[args.commodity]} if args.commodity else COMMODITIES

    total_condition = 0
    total_progress = 0

    for year in range(args.start_year, args.end_year + 1):
        for comm_key, comm_info in commodities.items():
            commodity_desc = comm_info['commodity_desc']

            # Condition data
            if args.data_type in ('condition', 'both'):
                logger.info(f"Fetching {comm_key} condition {year} ({args.level})...")
                raw = fetch_nass(commodity_desc, 'CONDITION', year, args.level)
                if raw:
                    records = process_condition_data(raw, comm_key)
                    saved = save_conditions(conn, records)
                    total_condition += saved
                    logger.info(f"  {comm_key} {year}: {len(raw)} raw -> {len(records)} grouped -> {saved} saved")
                else:
                    logger.info(f"  {comm_key} {year}: no data")
                time.sleep(1)  # Rate limit

            # Progress data
            if args.data_type in ('progress', 'both'):
                logger.info(f"Fetching {comm_key} progress {year} ({args.level})...")
                raw = fetch_nass(commodity_desc, 'PROGRESS', year, args.level)
                if raw:
                    records = process_progress_data(raw, comm_key)
                    saved = save_progress(conn, records)
                    total_progress += saved
                    logger.info(f"  {comm_key} {year}: {len(raw)} raw -> {len(records)} parsed -> {saved} saved")
                else:
                    logger.info(f"  {comm_key} {year}: no data")
                time.sleep(1)

    conn.close()
    logger.info(f"\nBackfill complete:")
    logger.info(f"  Condition records saved: {total_condition}")
    logger.info(f"  Progress records saved: {total_progress}")
    logger.info(f"  Total: {total_condition + total_progress}")


if __name__ == "__main__":
    main()
