#!/usr/bin/env python3
"""
EPA RFS Historical Data Collection - Overnight Run

Downloads and loads EPA RFS data chronologically from 2010 (EMTS launch) to present.
Processes data chronologically to capture updates and restatements properly.

The EPA publishes monthly CSV files with cumulative data. Each new file contains
updates to historical data, so we want to process chronologically to capture
any restatements.

File types collected:
- generationbreakout - Annual RIN generation by D-code
- rindata - Monthly RIN generation
- fuelproduction - Fuel production by type
- availablerins - Available RIN inventory
- retiretransaction - Retirement data
- separatetransaction - Separation data

Usage:
    python scripts/epa_rfs_history_overnight.py
    python scripts/epa_rfs_history_overnight.py --start-year 2020  # Partial run
    python scripts/epa_rfs_history_overnight.py --download-only    # Just download
"""

import sys
import logging
import argparse
import time
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

import requests

# Setup logging
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f'epa_rfs_history_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# =============================================================================
# FILE TYPE DEFINITIONS
# =============================================================================

FILE_TYPES = [
    'generationbreakout',
    'rindata',
    'fuelproduction',
    'availablerins',
    'retiretransaction',
    'separatetransaction',
]

MONTHS_ABBREV = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']


def build_file_url(file_type: str, data_year: int, data_month: int) -> str:
    """
    Build URL for an EPA RFS file.

    URL pattern: https://www.epa.gov/system/files/other-files/YYYY-MM/[type]_monYYYY.csv

    Publication is typically 1-2 months after data month.
    """
    mon = MONTHS_ABBREV[data_month - 1]

    # Publication date (data released ~1 month after)
    if data_month == 12:
        pub_year = data_year + 1
        pub_month = 1
    else:
        pub_year = data_year
        pub_month = data_month + 1

    url = f"https://www.epa.gov/system/files/other-files/{pub_year}-{pub_month:02d}/{file_type}_{mon}{data_year}.csv"
    return url


def download_file(url: str, save_path: Path, retries: int = 3) -> bool:
    """Download a file with retries."""
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=60)
            if response.status_code == 200:
                with open(save_path, 'wb') as f:
                    f.write(response.content)
                return True
            elif response.status_code == 404:
                logger.debug(f"File not found (404): {url}")
                return False
            else:
                logger.warning(f"HTTP {response.status_code} for {url}")

        except requests.exceptions.RequestException as e:
            logger.warning(f"Download attempt {attempt + 1} failed: {e}")
            time.sleep(2)

    return False


def get_available_months(start_year: int = 2010) -> List[Tuple[int, int]]:
    """
    Get list of (year, month) tuples for available EPA data.

    EPA EMTS data starts July 2010.
    """
    months = []

    # EMTS launched July 2010
    if start_year <= 2010:
        start_year = 2010
        start_month = 7
    else:
        start_month = 1

    today = date.today()
    # Data is typically 2 months behind
    end_year = today.year
    end_month = today.month - 2
    if end_month <= 0:
        end_year -= 1
        end_month += 12

    for year in range(start_year, end_year + 1):
        m_start = start_month if year == start_year else 1
        m_end = end_month if year == end_year else 12

        for month in range(m_start, m_end + 1):
            months.append((year, month))

    return months


def run_historical_collection(
    start_year: int = 2010,
    download_only: bool = False,
    save_db: bool = True,
    data_dir: Path = None
) -> Dict:
    """
    Run historical EPA RFS data collection.

    Processes chronologically from earliest to latest to capture restatements.

    Args:
        start_year: First year to collect (2010 minimum for EMTS)
        download_only: Only download files, don't save to database
        save_db: Save to database after loading
        data_dir: Directory for downloaded files

    Returns:
        Collection statistics
    """
    from agents.collectors.us.epa_rfs_collector_v2 import EPARFSCollectorV2

    data_dir = data_dir or PROJECT_ROOT / 'data' / 'raw' / 'rfs_data' / 'historical'
    data_dir.mkdir(parents=True, exist_ok=True)

    collector = EPARFSCollectorV2()

    logger.info("=" * 60)
    logger.info("EPA RFS HISTORICAL DATA COLLECTION")
    logger.info("=" * 60)
    logger.info(f"Start year: {start_year}")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Save to DB: {save_db}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("=" * 60)

    stats = {
        'files_downloaded': 0,
        'files_failed': 0,
        'files_not_found': 0,
        'records_saved': {},
        'start_time': datetime.now().isoformat(),
    }

    # Get available months chronologically
    months = get_available_months(start_year)
    logger.info(f"Processing {len(months)} months from {months[0]} to {months[-1]}")

    # Process chronologically
    for i, (year, month) in enumerate(months):
        mon_name = MONTHS_ABBREV[month - 1].capitalize()
        logger.info(f"\n[{i+1}/{len(months)}] Processing {mon_name} {year}...")

        month_files = {}

        # Download all file types for this month
        for file_type in FILE_TYPES:
            url = build_file_url(file_type, year, month)
            filename = f"{file_type}_{MONTHS_ABBREV[month - 1]}{year}.csv"
            save_path = data_dir / filename

            # Skip if already downloaded
            if save_path.exists():
                logger.debug(f"  {file_type}: Already exists")
                month_files[file_type] = save_path
                continue

            if download_file(url, save_path):
                logger.info(f"  {file_type}: Downloaded")
                month_files[file_type] = save_path
                stats['files_downloaded'] += 1
                time.sleep(0.5)  # Be nice to EPA servers
            else:
                stats['files_not_found'] += 1

        # Load and save to database
        if not download_only and save_db and month_files:
            logger.info(f"  Loading {len(month_files)} files to database...")

            for file_type, file_path in month_files.items():
                try:
                    # Load based on file type
                    if file_type == 'generationbreakout':
                        records = collector.load_generation_breakout(file_path)
                        if records:
                            result = collector.save_generation_to_bronze(records)
                            stats['records_saved'].setdefault('generation', 0)
                            stats['records_saved']['generation'] += result['inserted'] + result['updated']

                    elif file_type == 'rindata':
                        records = collector.load_rin_data_monthly(file_path)
                        if records:
                            result = collector.save_monthly_to_bronze(records)
                            stats['records_saved'].setdefault('monthly', 0)
                            stats['records_saved']['monthly'] += result['inserted'] + result['updated']

                    elif file_type == 'fuelproduction':
                        records = collector.load_fuel_production(file_path)
                        if records:
                            result = collector.save_fuel_production_to_bronze(records)
                            stats['records_saved'].setdefault('fuel_production', 0)
                            stats['records_saved']['fuel_production'] += result['inserted'] + result['updated']

                    elif file_type == 'availablerins':
                        records = collector.load_available_rins(file_path)
                        if records:
                            result = collector.save_available_to_bronze(records)
                            stats['records_saved'].setdefault('available', 0)
                            stats['records_saved']['available'] += result['inserted'] + result['updated']

                    elif file_type == 'retiretransaction':
                        records = collector.load_retirement_transactions(file_path)
                        if records:
                            result = collector.save_retirement_to_bronze(records)
                            stats['records_saved'].setdefault('retirement', 0)
                            stats['records_saved']['retirement'] += result['inserted'] + result['updated']

                    elif file_type == 'separatetransaction':
                        records = collector.load_separation_transactions(file_path)
                        if records:
                            result = collector.save_separation_to_bronze(records)
                            stats['records_saved'].setdefault('separation', 0)
                            stats['records_saved']['separation'] += result['inserted'] + result['updated']

                except Exception as e:
                    logger.error(f"  Error processing {file_type}: {e}")
                    stats['files_failed'] += 1

    # Transform to silver layer
    if save_db and not download_only:
        logger.info("\nTransforming to silver layer...")
        try:
            count = collector.transform_to_silver()
            stats['silver_transformed'] = count
        except Exception as e:
            logger.error(f"Silver transformation failed: {e}")

    stats['end_time'] = datetime.now().isoformat()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("COLLECTION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Files downloaded: {stats['files_downloaded']}")
    logger.info(f"Files not found: {stats['files_not_found']}")
    logger.info(f"Files failed: {stats['files_failed']}")
    logger.info("\nRecords saved by type:")
    for rec_type, count in stats['records_saved'].items():
        logger.info(f"  {rec_type}: {count:,}")
    if 'silver_transformed' in stats:
        logger.info(f"\nSilver layer: {stats['silver_transformed']} records")
    logger.info(f"\nLog file: {LOG_FILE}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='EPA RFS Historical Data Collection'
    )

    parser.add_argument(
        '--start-year',
        type=int,
        default=2010,
        help='First year to collect (2010 minimum, EMTS launch)'
    )

    parser.add_argument(
        '--download-only',
        action='store_true',
        help='Only download files, do not save to database'
    )

    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Load files but do not save to database'
    )

    parser.add_argument(
        '--data-dir',
        help='Directory for downloaded files'
    )

    args = parser.parse_args()

    try:
        data_dir = Path(args.data_dir) if args.data_dir else None

        stats = run_historical_collection(
            start_year=args.start_year,
            download_only=args.download_only,
            save_db=not args.no_save,
            data_dir=data_dir
        )

        print(f"\nCollection complete! Files downloaded: {stats['files_downloaded']}")
        print(f"Log file: {LOG_FILE}")

    except KeyboardInterrupt:
        logger.warning("\nCollection interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
