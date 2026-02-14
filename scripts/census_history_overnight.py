#!/usr/bin/env python3
"""
Census Trade History Collection - Overnight Run

Collects all available Census Bureau trade history back to 2013
(API limitation - data starts 2013, not 1993).

Designed to run overnight due to rate limiting (30 req/min).
Estimated runtime: 2-4 hours depending on data availability.

Usage:
    python scripts/census_history_overnight.py
    python scripts/census_history_overnight.py --save-db
    python scripts/census_history_overnight.py --year-start 2020  # Partial backfill
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import date, datetime
from typing import Dict, List, Any

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

# Setup logging with file output for overnight monitoring
LOG_FILE = PROJECT_ROOT / 'logs' / f'census_history_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
LOG_FILE.parent.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def run_history_collection(
    year_start: int = 2013,
    year_end: int = None,
    save_db: bool = False,
    exports_only: bool = False
) -> Dict[str, Any]:
    """
    Run complete history collection for all commodities.

    Args:
        year_start: First year to collect (2013 is API minimum)
        year_end: Last year to collect (default: current year)
        save_db: Whether to save to PostgreSQL
        exports_only: Only collect exports (faster for first run)

    Returns:
        Collection summary
    """
    from agents.collectors.us.census_trade_collector_v2 import (
        CensusTradeCollectorV2,
        CENSUS_SCHEDULE_B_CODES
    )

    year_end = year_end or date.today().year
    year_start = max(year_start, 2013)  # API limitation

    logger.info("=" * 60)
    logger.info("CENSUS TRADE HISTORY COLLECTION")
    logger.info("=" * 60)
    logger.info(f"Period: {year_start}-01 to {year_end}-12")
    logger.info(f"Commodities: {len(CENSUS_SCHEDULE_B_CODES)} codes")
    logger.info(f"Save to DB: {save_db}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info("=" * 60)

    collector = CensusTradeCollectorV2()

    start_date = date(year_start, 1, 1)
    end_date = date(year_end, 12, 31)

    # Cap at previous month (current month data not yet available)
    today = date.today()
    if end_date >= today:
        if today.month == 1:
            end_date = date(today.year - 1, 12, 31)
        else:
            end_date = date(today.year, today.month - 1, 28)

    flows = ['exports'] if exports_only else ['imports', 'exports']

    all_records = []
    stats = {
        'commodities_processed': 0,
        'commodities_failed': 0,
        'total_records': 0,
        'records_by_year': {},
        'start_time': datetime.now().isoformat(),
    }

    # Filter out 4-digit codes (use 10-digit only for detailed collection)
    detailed_codes = {
        k: v for k, v in CENSUS_SCHEDULE_B_CODES.items()
        if len(v) >= 6
    }

    logger.info(f"Processing {len(detailed_codes)} commodity codes...")

    for i, (commodity_name, hs_code) in enumerate(detailed_codes.items(), 1):
        logger.info(f"\n[{i}/{len(detailed_codes)}] {commodity_name} ({hs_code})")

        commodity_records = []

        for flow in flows:
            try:
                records = collector.fetch_trade_data(
                    flow, hs_code, start_date, end_date
                )
                commodity_records.extend(records)
                logger.info(f"  {flow}: {len(records)} records")

            except Exception as e:
                logger.error(f"  {flow} ERROR: {e}")
                stats['commodities_failed'] += 1

        all_records.extend(commodity_records)
        stats['commodities_processed'] += 1

        # Track by year
        for rec in commodity_records:
            year = rec.get('year')
            if year:
                stats['records_by_year'][year] = stats['records_by_year'].get(year, 0) + 1

        # Save periodically to avoid losing data
        if save_db and len(all_records) >= 5000:
            logger.info(f"  Saving batch of {len(all_records)} records to database...")
            try:
                db_result = collector.save_to_bronze(all_records)
                logger.info(f"  Saved: {db_result}")
                all_records = []  # Clear after save
            except Exception as e:
                logger.error(f"  Database save failed: {e}")

    # Final save
    if save_db and all_records:
        logger.info(f"\nSaving final batch of {len(all_records)} records...")
        try:
            db_result = collector.save_to_bronze(all_records)
            logger.info(f"Final save result: {db_result}")
        except Exception as e:
            logger.error(f"Final database save failed: {e}")

    stats['total_records'] = sum(stats['records_by_year'].values())
    stats['end_time'] = datetime.now().isoformat()

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("COLLECTION COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Commodities processed: {stats['commodities_processed']}")
    logger.info(f"Commodities failed: {stats['commodities_failed']}")
    logger.info(f"Total records: {stats['total_records']:,}")
    logger.info("\nRecords by year:")
    for year in sorted(stats['records_by_year'].keys()):
        logger.info(f"  {year}: {stats['records_by_year'][year]:,}")
    logger.info(f"\nLog file: {LOG_FILE}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Census Trade History Collection - Overnight Run'
    )

    parser.add_argument(
        '--year-start',
        type=int,
        default=2013,
        help='First year to collect (2013 is API minimum)'
    )

    parser.add_argument(
        '--year-end',
        type=int,
        default=None,
        help='Last year to collect (default: current year)'
    )

    parser.add_argument(
        '--save-db',
        action='store_true',
        help='Save data to PostgreSQL bronze layer'
    )

    parser.add_argument(
        '--exports-only',
        action='store_true',
        help='Only collect exports (faster initial run)'
    )

    args = parser.parse_args()

    try:
        stats = run_history_collection(
            year_start=args.year_start,
            year_end=args.year_end,
            save_db=args.save_db,
            exports_only=args.exports_only
        )

        print(f"\nCollection complete! Total records: {stats['total_records']:,}")
        print(f"Log file: {LOG_FILE}")

    except KeyboardInterrupt:
        logger.warning("\nCollection interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
