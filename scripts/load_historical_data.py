#!/usr/bin/env python3
"""
Historical Data Loader

Loads historical commodity data from all available sources into the database.

Usage:
    python scripts/load_historical_data.py --all --start-year 2018
    python scripts/load_historical_data.py --collectors cftc_cot usda_fas --start-year 2020
    python scripts/load_historical_data.py --list  # Show available collectors
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Collector configurations with historical data info
HISTORICAL_COLLECTORS = {
    # =========================================================================
    # NO API KEY REQUIRED
    # =========================================================================
    'cftc_cot': {
        'class': 'CFTCCOTCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'CFTC Commitments of Traders positioning data',
        'auth_required': False,
        'history_available': True,
        'history_start': 1986,
        'commodities': ['corn', 'soybeans', 'wheat', 'soybean_oil', 'soybean_meal'],
        'table': 'cot_positions',
    },
    'usda_fas_export_sales': {
        'class': 'USDATFASCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA FAS Weekly Export Sales',
        'auth_required': False,
        'history_available': True,
        'history_start': 1990,
        'data_type': 'export_sales',
        'commodities': ['corn', 'wheat', 'soybeans', 'soybean_oil', 'soybean_meal', 'cotton'],
        'table': 'export_sales',
    },
    'usda_fas_psd': {
        'class': 'USDATFASCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA FAS Production Supply Demand',
        'auth_required': False,
        'history_available': True,
        'history_start': 1960,
        'data_type': 'psd',
        'commodities': ['corn', 'wheat', 'soybeans', 'soybean_oil', 'soybean_meal'],
        'table': 'supply_demand',
    },
    'drought_monitor': {
        'class': 'DroughtCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'US Drought Monitor',
        'auth_required': False,
        'history_available': True,
        'history_start': 2000,
        'table': 'drought_data',
    },
    'usda_ers_feed_grains': {
        'class': 'FeedGrainsCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA ERS Feed Grains Database',
        'auth_required': False,
        'history_available': True,
        'history_start': 1980,
        'commodities': ['corn', 'sorghum', 'barley', 'oats'],
        'table': 'supply_demand',
    },
    'usda_ers_oil_crops': {
        'class': 'OilCropsCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA ERS Oil Crops Yearbook',
        'auth_required': False,
        'history_available': True,
        'history_start': 1980,
        'commodities': ['soybeans', 'canola', 'sunflower'],
        'table': 'supply_demand',
    },
    'usda_ers_wheat': {
        'class': 'WheatDataCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA ERS Wheat Data',
        'auth_required': False,
        'history_available': True,
        'history_start': 1980,
        'commodities': ['wheat'],
        'table': 'supply_demand',
    },
    'usda_ams_feedstocks': {
        'class': 'TallowProteinCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA AMS Tallow & Grease Prices',
        'auth_required': False,
        'history_available': False,  # Current prices only
        'commodities': ['yellow_grease', 'tallow', 'cwg', 'lard'],
        'table': 'feedstock_prices',
    },
    'usda_ams_ddgs': {
        'class': 'GrainCoProductsCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA AMS DDGS & Co-Products',
        'auth_required': False,
        'history_available': False,
        'commodities': ['ddgs', 'dco'],
        'table': 'feedstock_prices',
    },
    'epa_rfs': {
        'class': 'EPARFSCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'EPA RFS RIN Generation',
        'auth_required': False,
        'history_available': True,
        'history_start': 2010,
        'table': 'rin_data',
    },
    'canada_cgc': {
        'class': 'CGCCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'Canadian Grain Commission',
        'auth_required': False,
        'history_available': False,  # Current data only
        'commodities': ['wheat', 'canola', 'barley', 'oats'],
        'table': 'trade_flows',
    },
    'canada_statscan': {
        'class': 'StatsCanCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'Statistics Canada Agriculture',
        'auth_required': False,
        'history_available': True,
        'history_start': 2000,
        'commodities': ['wheat', 'canola', 'barley', 'oats'],
        'table': 'supply_demand',
    },
    'mpob': {
        'class': 'MPOBCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'Malaysian Palm Oil Board',
        'auth_required': False,
        'history_available': True,
        'history_start': 2010,
        'commodities': ['palm_oil'],
        'table': 'supply_demand',
    },

    # =========================================================================
    # API KEY REQUIRED
    # =========================================================================
    'eia_ethanol': {
        'class': 'EIAEthanolCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'EIA Ethanol Production & Stocks',
        'auth_required': True,
        'env_var': 'EIA_API_KEY',
        'history_available': True,
        'history_start': 2010,
        'commodities': ['ethanol'],
        'table': 'ethanol_data',
    },
    'eia_petroleum': {
        'class': 'EIAPetroleumCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'EIA Petroleum & Natural Gas',
        'auth_required': True,
        'env_var': 'EIA_API_KEY',
        'history_available': True,
        'history_start': 1990,
        'commodities': ['crude_oil', 'gasoline', 'diesel', 'jet_fuel', 'natural_gas'],
        'table': 'energy_prices',
    },
    'usda_nass': {
        'class': 'NASSCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'USDA NASS Crop Progress & Production',
        'auth_required': True,
        'env_var': 'NASS_API_KEY',
        'history_available': True,
        'history_start': 2000,
        'commodities': ['corn', 'soybeans', 'wheat', 'cotton', 'sorghum'],
        'table': 'crop_progress',
    },
    'census_trade': {
        'class': 'CensusTradeCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'US Census International Trade',
        'auth_required': False,  # Works without, but rate limited
        'env_var': 'CENSUS_API_KEY',
        'history_available': True,
        'history_start': 2013,
        'commodities': ['all'],
        'table': 'trade_flows',
    },
    'cme_settlements': {
        'class': 'CMESettlementsCollector',
        'module': 'commodity_pipeline.data_collectors',
        'description': 'CME Futures Settlements',
        'auth_required': False,
        'history_available': False,  # Current day only without subscription
        'commodities': ['corn', 'wheat', 'soybeans', 'crude_oil', 'ethanol'],
        'table': 'futures_settlements',
    },
}


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


def load_collector_class(collector_info: dict):
    """Dynamically load collector class"""
    import importlib
    module = importlib.import_module(collector_info['module'])
    return getattr(module, collector_info['class'])


def check_api_key(collector_name: str, collector_info: dict) -> bool:
    """Check if required API key is available"""
    if not collector_info.get('auth_required'):
        return True

    env_var = collector_info.get('env_var')
    if env_var and os.getenv(env_var):
        return True

    logger.warning(f"Skipping {collector_name}: requires {env_var}")
    return False


def insert_records(conn, db_type: str, table: str, records: List[Dict], collector_name: str):
    """Insert records into database table"""
    if not records:
        return 0

    cursor = conn.cursor()
    inserted = 0

    # Get sample record to determine columns
    sample = records[0]
    columns = list(sample.keys())

    # Filter to valid columns (basic validation)
    valid_columns = [c for c in columns if c and not c.startswith('_')]

    if db_type == 'postgresql':
        # Use INSERT ... ON CONFLICT DO NOTHING for idempotency
        placeholders = ', '.join(['%s'] * len(valid_columns))
        col_list = ', '.join(valid_columns)

        for record in records:
            values = [record.get(c) for c in valid_columns]
            try:
                cursor.execute(
                    f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                    values
                )
                inserted += cursor.rowcount
            except Exception as e:
                logger.debug(f"Insert error: {e}")
                conn.rollback()
                continue
    else:
        # SQLite
        placeholders = ', '.join(['?'] * len(valid_columns))
        col_list = ', '.join(valid_columns)

        for record in records:
            values = [record.get(c) for c in valid_columns]
            try:
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
    return inserted


def record_collection_run(conn, db_type: str, collector_name: str, success: bool,
                          records_fetched: int, records_inserted: int, error: str = None):
    """Record collection run in tracking table"""
    cursor = conn.cursor()

    if db_type == 'postgresql':
        cursor.execute("""
            INSERT INTO collection_runs
            (collector_name, started_at, completed_at, status, records_fetched, records_inserted, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            collector_name,
            datetime.now(),
            datetime.now(),
            'completed' if success else 'failed',
            records_fetched,
            records_inserted,
            error
        ))
    else:
        cursor.execute("""
            INSERT INTO collection_runs
            (collector_name, started_at, completed_at, status, records_fetched, records_inserted, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            collector_name,
            datetime.now().isoformat(),
            datetime.now().isoformat(),
            'completed' if success else 'failed',
            records_fetched,
            records_inserted,
            error
        ))

    conn.commit()
    cursor.close()


def load_historical_data(
    collectors: List[str],
    start_year: int,
    end_year: int = None,
    verbose: bool = False
):
    """Load historical data from specified collectors"""

    end_year = end_year or date.today().year
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    logger.info(f"Loading historical data from {start_date} to {end_date}")
    logger.info(f"Collectors to run: {collectors}")

    # Connect to database
    try:
        conn, db_type = get_database_connection()
        logger.info(f"Connected to {db_type} database")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

    results = {}

    for collector_name in collectors:
        if collector_name not in HISTORICAL_COLLECTORS:
            logger.warning(f"Unknown collector: {collector_name}")
            continue

        collector_info = HISTORICAL_COLLECTORS[collector_name]

        # Check API key if required
        if not check_api_key(collector_name, collector_info):
            results[collector_name] = {'status': 'skipped', 'reason': 'missing API key'}
            continue

        # Check if historical data available
        if not collector_info.get('history_available'):
            logger.info(f"{collector_name}: No historical data available, fetching current only")

        logger.info(f"\n{'='*60}")
        logger.info(f"Loading: {collector_name}")
        logger.info(f"  {collector_info['description']}")
        logger.info(f"{'='*60}")

        try:
            # Load collector class
            CollectorClass = load_collector_class(collector_info)
            collector = CollectorClass()

            # Prepare collection parameters
            collect_params = {
                'start_date': start_date,
                'end_date': end_date,
            }

            # Add data_type if specified
            if 'data_type' in collector_info:
                collect_params['data_type'] = collector_info['data_type']

            # Add commodities if specified
            if 'commodities' in collector_info and collector_info['commodities'] != ['all']:
                collect_params['commodities'] = collector_info['commodities']

            # Run collection
            logger.info(f"  Fetching data...")
            result = collector.collect(**collect_params)

            if result.success:
                logger.info(f"  ✓ Fetched {result.records_fetched} records")

                # Convert to list of dicts for insertion
                if hasattr(result.data, 'to_dict'):
                    # pandas DataFrame
                    records = result.data.to_dict('records')
                elif isinstance(result.data, list):
                    records = result.data
                else:
                    records = []

                # Insert into database
                if records:
                    table = collector_info.get('table', collector_name)
                    logger.info(f"  Inserting into {table}...")

                    inserted = insert_records(conn, db_type, table, records, collector_name)
                    logger.info(f"  ✓ Inserted {inserted} new records")

                    record_collection_run(conn, db_type, collector_name, True,
                                        result.records_fetched, inserted)

                    results[collector_name] = {
                        'status': 'success',
                        'fetched': result.records_fetched,
                        'inserted': inserted,
                    }
                else:
                    logger.warning(f"  No records to insert")
                    results[collector_name] = {
                        'status': 'success',
                        'fetched': result.records_fetched,
                        'inserted': 0,
                    }
            else:
                error_msg = result.error_message or 'Unknown error'
                logger.error(f"  ✗ Collection failed: {error_msg}")

                record_collection_run(conn, db_type, collector_name, False,
                                    0, 0, error_msg)

                results[collector_name] = {
                    'status': 'failed',
                    'error': error_msg,
                }

            if result.warnings:
                for warning in result.warnings:
                    logger.warning(f"  Warning: {warning}")

        except Exception as e:
            logger.error(f"  ✗ Error: {e}")
            if verbose:
                import traceback
                traceback.print_exc()

            record_collection_run(conn, db_type, collector_name, False,
                                0, 0, str(e))

            results[collector_name] = {
                'status': 'error',
                'error': str(e),
            }

    conn.close()

    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("COLLECTION SUMMARY")
    logger.info(f"{'='*60}")

    success_count = sum(1 for r in results.values() if r.get('status') == 'success')
    total_fetched = sum(r.get('fetched', 0) for r in results.values())
    total_inserted = sum(r.get('inserted', 0) for r in results.values())

    logger.info(f"Collectors: {success_count}/{len(collectors)} successful")
    logger.info(f"Total records fetched: {total_fetched:,}")
    logger.info(f"Total records inserted: {total_inserted:,}")

    for name, result in results.items():
        status_icon = '✓' if result.get('status') == 'success' else '✗'
        if result.get('status') == 'skipped':
            status_icon = '○'
        logger.info(f"  {status_icon} {name}: {result.get('status')}")

    return results


def list_collectors():
    """List all available collectors"""
    print("\n" + "="*80)
    print("AVAILABLE HISTORICAL DATA COLLECTORS")
    print("="*80)

    # Group by auth requirement
    no_auth = {k: v for k, v in HISTORICAL_COLLECTORS.items() if not v.get('auth_required')}
    auth_req = {k: v for k, v in HISTORICAL_COLLECTORS.items() if v.get('auth_required')}

    print("\n--- NO API KEY REQUIRED ---\n")
    for name, info in sorted(no_auth.items()):
        hist = f"from {info.get('history_start', 'N/A')}" if info.get('history_available') else "current only"
        print(f"  {name:25} {info['description'][:40]:42} [{hist}]")

    print("\n--- API KEY REQUIRED ---\n")
    for name, info in sorted(auth_req.items()):
        env_var = info.get('env_var', '?')
        hist = f"from {info.get('history_start', 'N/A')}" if info.get('history_available') else "current only"
        key_status = '✓' if os.getenv(env_var) else '✗'
        print(f"  {name:25} {info['description'][:40]:42} [{hist}] {key_status} {env_var}")

    print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Load historical commodity data into database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/load_historical_data.py --list
  python scripts/load_historical_data.py --all --start-year 2020
  python scripts/load_historical_data.py --collectors cftc_cot usda_fas --start-year 2018
  python scripts/load_historical_data.py --no-auth --start-year 2020  # Only free sources
        """
    )

    parser.add_argument('--list', action='store_true',
                       help='List available collectors')
    parser.add_argument('--all', action='store_true',
                       help='Run all collectors')
    parser.add_argument('--no-auth', action='store_true',
                       help='Run only collectors that require no API key')
    parser.add_argument('--collectors', nargs='+',
                       help='Specific collectors to run')
    parser.add_argument('--start-year', type=int, default=2020,
                       help='Start year for historical data (default: 2020)')
    parser.add_argument('--end-year', type=int,
                       help='End year (default: current year)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Verbose output with stack traces')

    args = parser.parse_args()

    if args.list:
        list_collectors()
        return

    # Determine which collectors to run
    if args.all:
        collectors = list(HISTORICAL_COLLECTORS.keys())
    elif args.no_auth:
        collectors = [k for k, v in HISTORICAL_COLLECTORS.items()
                     if not v.get('auth_required')]
    elif args.collectors:
        collectors = args.collectors
    else:
        parser.print_help()
        print("\nError: Specify --all, --no-auth, or --collectors")
        sys.exit(1)

    load_historical_data(
        collectors=collectors,
        start_year=args.start_year,
        end_year=args.end_year,
        verbose=args.verbose
    )


if __name__ == '__main__':
    main()
