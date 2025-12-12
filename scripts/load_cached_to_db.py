#!/usr/bin/env python3
"""
Load Cached Data into Database

Loads cached collector data from JSON files into the SQLite/PostgreSQL database.
This is useful for getting data into the database quickly when collectors are
having API issues.

Usage:
    python scripts/load_cached_to_db.py
    python scripts/load_cached_to_db.py --verbose
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    logger.warning("pandas not available, some features may be limited")


# Cache directory
CACHE_DIR = Path(__file__).parent.parent / 'data' / 'cache'

# Mapping of cache files to database tables
CACHE_TO_TABLE = {
    'eia_ethanol': 'ethanol_data',
    'eia_petroleum': 'energy_prices',
    'usda_nass': 'crop_progress',
    'cftc_cot': 'cot_positions',
    'usda_fas': 'export_sales',
    'drought': 'drought_data',
    'census_trade': 'trade_flows',
}


def detect_table_from_records(records: list) -> str:
    """Auto-detect table name based on record fields"""
    if not records:
        return 'unknown_data'

    sample = records[0]
    fields = set(sample.keys()) if isinstance(sample, dict) else set()

    # Detect based on field patterns
    if 'production_kbd' in fields or 'stocks_kb' in fields:
        return 'ethanol_data'
    elif 'series_id' in fields and any('PET' in str(v) for v in sample.values()):
        return 'energy_prices'
    elif 'statisticcat_desc' in fields or 'commodity_desc' in fields:
        return 'crop_progress'
    elif 'mm_long' in fields or 'mm_short' in fields or 'open_interest' in fields:
        return 'cot_positions'
    elif 'week_ending' in fields and 'weekly_exports' in fields:
        return 'export_sales'
    elif 'd0_pct' in fields or 'd1_pct' in fields:
        return 'drought_data'
    elif 'hs_code' in fields or 'flow' in fields:
        return 'trade_flows'
    elif 'value' in fields and 'series_id' in fields:
        return 'energy_prices'
    else:
        return 'cached_data'


def get_database_connection():
    """Get database connection"""
    database_url = os.getenv('DATABASE_URL', 'sqlite:///./data/commodity.db')

    if database_url.startswith('postgresql'):
        import psycopg2
        from urllib.parse import urlparse
        parsed = urlparse(database_url)
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:],
            user=parsed.username,
            password=parsed.password
        )
        return conn, 'postgresql'
    else:
        import sqlite3
        db_path = database_url.replace('sqlite:///', '')
        os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
        conn = sqlite3.connect(db_path)
        return conn, 'sqlite'


def load_cache_file(cache_path: Path) -> tuple:
    """Load and parse a cache file"""
    try:
        with open(cache_path, 'r') as f:
            data = json.load(f)

        # Handle different cache formats
        if isinstance(data, list):
            # Direct list of records
            records = data
            source = cache_path.stem
            timestamp = ''
        elif isinstance(data, dict):
            records = data.get('data', [])
            source = data.get('source', cache_path.stem)
            timestamp = data.get('timestamp', '')

            # Handle DataFrame-style JSON
            if isinstance(records, dict):
                if 'columns' in records and 'data' in records:
                    records = [dict(zip(records['columns'], row)) for row in records['data']]
                else:
                    records = [records]
        else:
            records = []
            source = cache_path.stem
            timestamp = ''

        return records, source, timestamp
    except Exception as e:
        logger.error(f"Error loading {cache_path}: {e}")
        return [], None, None


def create_table_if_not_exists(conn, db_type: str, table_name: str, sample_record: dict):
    """Create table based on sample record structure"""
    cursor = conn.cursor()

    # Build column definitions
    columns = []
    for key, value in sample_record.items():
        if key.startswith('_'):
            continue

        if isinstance(value, bool):
            col_type = 'BOOLEAN' if db_type == 'postgresql' else 'INTEGER'
        elif isinstance(value, int):
            col_type = 'INTEGER'
        elif isinstance(value, float):
            col_type = 'REAL' if db_type == 'sqlite' else 'DOUBLE PRECISION'
        else:
            col_type = 'TEXT'

        columns.append(f"{key} {col_type}")

    if not columns:
        return False

    # Add id column
    if db_type == 'sqlite':
        id_col = 'id INTEGER PRIMARY KEY AUTOINCREMENT'
    else:
        id_col = 'id SERIAL PRIMARY KEY'

    columns_sql = ', '.join([id_col] + columns)

    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql})")
        conn.commit()
        logger.info(f"  Created/verified table: {table_name}")
        return True
    except Exception as e:
        logger.error(f"  Error creating table {table_name}: {e}")
        return False
    finally:
        cursor.close()


def insert_records(conn, db_type: str, table_name: str, records: list):
    """Insert records into database"""
    if not records:
        return 0

    cursor = conn.cursor()
    inserted = 0

    # Get columns from first record
    sample = records[0]
    columns = [k for k in sample.keys() if not k.startswith('_')]

    if db_type == 'postgresql':
        placeholders = ', '.join(['%s'] * len(columns))
        col_list = ', '.join(columns)

        for record in records:
            values = [record.get(c) for c in columns]
            try:
                cursor.execute(
                    f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
                    values
                )
                inserted += 1
            except Exception as e:
                logger.debug(f"Insert error: {e}")
                conn.rollback()
    else:
        # SQLite
        placeholders = ', '.join(['?'] * len(columns))
        col_list = ', '.join(columns)

        for record in records:
            values = []
            for c in columns:
                v = record.get(c)
                # Convert complex types to strings
                if isinstance(v, (dict, list)):
                    v = json.dumps(v)
                values.append(v)

            try:
                cursor.execute(
                    f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
                    values
                )
                inserted += 1
            except Exception as e:
                logger.debug(f"Insert error: {e}")

    conn.commit()
    cursor.close()
    return inserted


def main():
    parser = argparse.ArgumentParser(description='Load cached data into database')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--list', '-l', action='store_true', help='List cache files only')
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Find cache files
    if not CACHE_DIR.exists():
        logger.error(f"Cache directory not found: {CACHE_DIR}")
        sys.exit(1)

    cache_files = list(CACHE_DIR.glob('*.json'))

    if not cache_files:
        logger.error("No cache files found")
        sys.exit(1)

    if args.list:
        print("\n" + "="*60)
        print("CACHED DATA FILES")
        print("="*60 + "\n")

        total_records = 0
        for cache_file in cache_files:
            records, source, timestamp = load_cache_file(cache_file)
            count = len(records)
            total_records += count
            table = CACHE_TO_TABLE.get(cache_file.stem, cache_file.stem)
            print(f"  {cache_file.stem:25} → {table:20} ({count:,} records)")

        print(f"\nTotal: {total_records:,} records")
        return

    # Connect to database
    try:
        conn, db_type = get_database_connection()
        logger.info(f"Connected to {db_type} database")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

    print("\n" + "="*60)
    print("LOADING CACHED DATA INTO DATABASE")
    print("="*60 + "\n")

    total_inserted = 0
    results = {}

    for cache_file in cache_files:
        cache_name = cache_file.stem
        logger.info(f"Processing: {cache_name}")

        # Load cache
        records, source, timestamp = load_cache_file(cache_file)

        if not records:
            logger.warning(f"  No records in {cache_name}")
            results[cache_name] = {'status': 'empty', 'records': 0}
            continue

        logger.info(f"  Loaded {len(records):,} records from cache")

        # Determine table name - use mapping if known, otherwise auto-detect
        if cache_name in CACHE_TO_TABLE:
            table_name = CACHE_TO_TABLE[cache_name]
        else:
            table_name = detect_table_from_records(records)
            logger.info(f"  Auto-detected table: {table_name}")

        # Create table if needed
        if not create_table_if_not_exists(conn, db_type, table_name, records[0]):
            results[cache_name] = {'status': 'error', 'records': 0}
            continue

        # Insert records
        inserted = insert_records(conn, db_type, table_name, records)
        logger.info(f"  ✓ Inserted {inserted:,} records into {table_name}")

        total_inserted += inserted
        results[cache_name] = {'status': 'success', 'records': inserted, 'table': table_name}

    conn.close()

    # Summary
    print("\n" + "="*60)
    print("LOAD SUMMARY")
    print("="*60)

    success_count = sum(1 for r in results.values() if r.get('status') == 'success')
    print(f"Sources loaded: {success_count}/{len(results)}")
    print(f"Total records inserted: {total_inserted:,}")

    print("\nDetails:")
    for name, result in results.items():
        status = result.get('status')
        records = result.get('records', 0)
        table = result.get('table', 'N/A')
        icon = '✓' if status == 'success' else '○' if status == 'empty' else '✗'
        print(f"  {icon} {name}: {records:,} records → {table}")

    print("\n" + "-"*60)
    print("DATABASE READY FOR POWER BI CONNECTION")
    print("-"*60)
    db_path = os.getenv('DATABASE_URL', 'sqlite:///./data/commodity.db').replace('sqlite:///', '')
    print(f"Database: {db_path}")


if __name__ == '__main__':
    main()
