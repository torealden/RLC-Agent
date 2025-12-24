#!/usr/bin/env python3
"""
Manual Data Collection Script

Run individual or multiple collectors on demand.
Used for manual updates, cron jobs, and scheduled tasks.

Usage:
    python scripts/collect.py --collector cftc_cot
    python scripts/collect.py --collectors eia_ethanol eia_petroleum
    python scripts/collect.py --today  # Run all scheduled for today
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Load credentials from centralized config
credentials_path = project_root / "config" / "credentials.env"
if credentials_path.exists():
    load_dotenv(credentials_path)
else:
    load_dotenv()  # Fall back to default .env

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Collector name to class mapping
COLLECTOR_MAP = {
    'cftc_cot': 'CFTCCOTCollector',
    'usda_fas': 'USDATFASCollector',
    'drought': 'DroughtCollector',
    'usda_ers_feed_grains': 'FeedGrainsCollector',
    'usda_ers_oil_crops': 'OilCropsCollector',
    'usda_ers_wheat': 'WheatDataCollector',
    'usda_ams_tallow': 'TallowProteinCollector',
    'usda_ams_ddgs': 'GrainCoProductsCollector',
    'epa_rfs': 'EPARFSCollector',
    'census_trade': 'CensusTradeCollector',
    'canada_cgc': 'CGCCollector',
    'canada_statscan': 'StatsCanCollector',
    'cme_settlements': 'CMESettlementsCollector',
    'mpob': 'MPOBCollector',
    'eia_ethanol': 'EIAEthanolCollector',
    'eia_petroleum': 'EIAPetroleumCollector',
    'usda_nass': 'NASSCollector',
}


def get_collector(name: str):
    """Get collector class by name"""
    class_name = COLLECTOR_MAP.get(name)
    if not class_name:
        return None

    from commodity_pipeline.data_collectors import (
        CFTCCOTCollector,
        USDATFASCollector,
        DroughtCollector,
        FeedGrainsCollector,
        OilCropsCollector,
        WheatDataCollector,
        TallowProteinCollector,
        GrainCoProductsCollector,
        EPARFSCollector,
        CensusTradeCollector,
        CGCCollector,
        StatsCanCollector,
        CMESettlementsCollector,
        MPOBCollector,
        EIAEthanolCollector,
        EIAPetroleumCollector,
        NASSCollector,
    )

    collectors = {
        'CFTCCOTCollector': CFTCCOTCollector,
        'USDATFASCollector': USDATFASCollector,
        'DroughtCollector': DroughtCollector,
        'FeedGrainsCollector': FeedGrainsCollector,
        'OilCropsCollector': OilCropsCollector,
        'WheatDataCollector': WheatDataCollector,
        'TallowProteinCollector': TallowProteinCollector,
        'GrainCoProductsCollector': GrainCoProductsCollector,
        'EPARFSCollector': EPARFSCollector,
        'CensusTradeCollector': CensusTradeCollector,
        'CGCCollector': CGCCollector,
        'StatsCanCollector': StatsCanCollector,
        'CMESettlementsCollector': CMESettlementsCollector,
        'MPOBCollector': MPOBCollector,
        'EIAEthanolCollector': EIAEthanolCollector,
        'EIAPetroleumCollector': EIAPetroleumCollector,
        'NASSCollector': NASSCollector,
    }

    return collectors.get(class_name)


def get_todays_collectors():
    """Get collectors scheduled for today"""
    from commodity_pipeline.scheduler import ReportScheduler

    scheduler = ReportScheduler()
    todays_schedules = scheduler.get_todays_collections()

    return [s.collector_name for s in todays_schedules]


def run_collector(name: str, save_to_db: bool = True, verbose: bool = False) -> dict:
    """Run a single collector"""
    result = {
        'name': name,
        'status': 'unknown',
        'records': 0,
        'error': None,
    }

    CollectorClass = get_collector(name)
    if not CollectorClass:
        result['status'] = 'error'
        result['error'] = f'Unknown collector: {name}'
        return result

    try:
        collector = CollectorClass()
        logger.info(f"Running {name}...")

        collection_result = collector.collect()

        if collection_result.success:
            result['status'] = 'success'
            result['records'] = collection_result.records_fetched or 0
            logger.info(f"  ✓ {result['records']} records fetched")

            # Save to database if enabled
            if save_to_db and result['records'] > 0:
                inserted = save_to_database(name, collection_result)
                result['inserted'] = inserted
                logger.info(f"  ✓ {inserted} records inserted to database")

        else:
            result['status'] = 'failed'
            result['error'] = collection_result.error_message
            logger.error(f"  ✗ {collection_result.error_message}")

        if collection_result.warnings:
            result['warnings'] = collection_result.warnings
            if verbose:
                for w in collection_result.warnings:
                    logger.warning(f"  Warning: {w}")

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)
        logger.error(f"  ✗ Error: {e}")

        if verbose:
            import traceback
            traceback.print_exc()

    return result


def save_to_database(collector_name: str, result) -> int:
    """Save collection result to database"""
    # Get database connection using DB_* environment variables
    db_type = os.getenv('DB_TYPE', 'postgresql')

    try:
        if db_type == 'postgresql':
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME', 'rlc_commodities'),
                user=os.getenv('DB_USER', 'postgres'),
                password=os.getenv('DB_PASSWORD', '')
            )
        else:
            import sqlite3
            db_path = os.getenv('SQLITE_PATH', './data/rlc_commodities.db')
            conn = sqlite3.connect(db_path)
    except Exception as e:
        logger.warning(f"Database connection failed: {e}")
        return 0

    # Determine target table from collector name
    table_map = {
        'cftc_cot': 'cot_positions',
        'usda_fas': 'export_sales',
        'drought': 'drought_data',
        'eia_ethanol': 'ethanol_data',
        'eia_petroleum': 'energy_prices',
        'usda_nass': 'crop_progress',
        'cme_settlements': 'futures_settlements',
        'census_trade': 'trade_flows',
        'usda_ams_tallow': 'feedstock_prices',
        'usda_ams_ddgs': 'feedstock_prices',
    }

    table = table_map.get(collector_name, collector_name)

    # Convert result data to records
    if hasattr(result.data, 'to_dict'):
        records = result.data.to_dict('records')
    elif isinstance(result.data, list):
        records = result.data
    else:
        return 0

    if not records:
        return 0

    # Insert records
    cursor = conn.cursor()
    inserted = 0

    sample = records[0]
    columns = [c for c in sample.keys() if c and not c.startswith('_')]

    if db_type == 'postgresql':
        placeholders = ', '.join(['%s'] * len(columns))
    else:
        placeholders = ', '.join(['?'] * len(columns))

    col_list = ', '.join(columns)

    for record in records:
        values = [record.get(c) for c in columns]
        try:
            if db_type == 'postgresql':
                cursor.execute(
                    f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    values
                )
            else:
                cursor.execute(
                    f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})",
                    values
                )
            inserted += cursor.rowcount
        except Exception as e:
            logger.debug(f"Insert error: {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

    return inserted


def main():
    parser = argparse.ArgumentParser(
        description='Run commodity data collectors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/collect.py --list
  python scripts/collect.py --collector cftc_cot
  python scripts/collect.py --collectors eia_ethanol eia_petroleum
  python scripts/collect.py --today
        """
    )

    parser.add_argument('--list', '-l', action='store_true',
                       help='List available collectors')
    parser.add_argument('--collector', '-c',
                       help='Single collector to run')
    parser.add_argument('--collectors', nargs='+',
                       help='Multiple collectors to run')
    parser.add_argument('--today', action='store_true',
                       help='Run all collectors scheduled for today')
    parser.add_argument('--no-db', action='store_true',
                       help='Skip saving to database')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output')

    args = parser.parse_args()

    if args.list:
        print("\nAvailable collectors:")
        for name in sorted(COLLECTOR_MAP.keys()):
            print(f"  {name}")
        return

    # Determine which collectors to run
    collectors = []

    if args.today:
        collectors = get_todays_collectors()
        if not collectors:
            print("No collectors scheduled for today")
            return
        print(f"Running {len(collectors)} collectors scheduled for today:")
        for c in collectors:
            print(f"  - {c}")

    elif args.collectors:
        collectors = args.collectors

    elif args.collector:
        collectors = [args.collector]

    else:
        parser.print_help()
        return

    # Run collectors
    print("\n" + "="*50)
    results = []

    for name in collectors:
        result = run_collector(
            name,
            save_to_db=not args.no_db,
            verbose=args.verbose
        )
        results.append(result)

    # Summary
    print("\n" + "="*50)
    print("COLLECTION SUMMARY")
    print("="*50)

    success = sum(1 for r in results if r['status'] == 'success')
    total_records = sum(r.get('records', 0) for r in results)
    total_inserted = sum(r.get('inserted', 0) for r in results)

    print(f"Collectors: {success}/{len(results)} successful")
    print(f"Records fetched: {total_records:,}")
    print(f"Records inserted: {total_inserted:,}")


if __name__ == '__main__':
    main()
