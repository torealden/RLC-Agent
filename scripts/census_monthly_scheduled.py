#!/usr/bin/env python3
"""
Census Trade Monthly Scheduled Collection

Collects the latest available month's trade data on Census release dates.
Designed to be run by scheduler (cron, Task Scheduler, etc.) on release days.

Census FT900 Release Schedule (8:30 AM ET):
- Data is released ~35 days after the reference month
- Schedule at: https://www.census.gov/foreign-trade/schedule.html

Recommended scheduling:
- Run at 9:00 AM ET on Census release dates (30 min after publication)
- Or run daily and script will check if it's a release day

Usage:
    python scripts/census_monthly_scheduled.py
    python scripts/census_monthly_scheduled.py --force  # Run even if not release day
    python scripts/census_monthly_scheduled.py --reference-month 2025-10
"""

import sys
import logging
import argparse
from pathlib import Path
from datetime import date, datetime
from typing import Dict, Optional

# Setup paths
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

# Setup logging
LOG_DIR = PROJECT_ROOT / 'logs'
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'census_monthly.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Census release schedule 2025-2026
CENSUS_RELEASE_DATES = {
    '2024-11': '2025-01-07',
    '2024-12': '2025-02-05',
    '2025-01': '2025-03-06',
    '2025-02': '2025-04-03',
    '2025-03': '2025-05-06',
    '2025-04': '2025-06-05',
    '2025-05': '2025-07-03',
    '2025-06': '2025-08-05',
    '2025-07': '2025-09-04',
    '2025-08': '2025-11-19',
    '2025-09': '2025-12-11',
    '2025-10': '2026-01-08',
    '2025-11': '2026-01-29',
    '2025-12': '2026-02-19',
}


def get_reference_month_for_today() -> Optional[str]:
    """Check if today is a Census release day, return reference month if so."""
    today_str = date.today().isoformat()
    for ref_month, release_date in CENSUS_RELEASE_DATES.items():
        if release_date == today_str:
            return ref_month
    return None


def get_next_release() -> Optional[Dict]:
    """Get information about the next Census release."""
    today = date.today()
    for ref_month, release_str in sorted(CENSUS_RELEASE_DATES.items()):
        release_date = datetime.strptime(release_str, '%Y-%m-%d').date()
        if release_date >= today:
            return {
                'reference_month': ref_month,
                'release_date': release_str,
                'days_until': (release_date - today).days
            }
    return None


def run_monthly_collection(
    reference_month: str = None,
    save_db: bool = True,
    force: bool = False
) -> Dict:
    """
    Run monthly Census trade collection.

    Args:
        reference_month: Month to collect (YYYY-MM format)
        save_db: Save to database
        force: Run even if not a release day

    Returns:
        Collection result summary
    """
    from agents.collectors.us.census_trade_collector_v2 import CensusTradeCollectorV2

    # Determine reference month
    if reference_month:
        ref_month = reference_month
    else:
        ref_month = get_reference_month_for_today()

    if not ref_month and not force:
        next_release = get_next_release()
        if next_release:
            logger.info(f"Not a Census release day. Next release: "
                       f"{next_release['release_date']} for {next_release['reference_month']} "
                       f"({next_release['days_until']} days)")
        return {'skipped': True, 'reason': 'Not a release day'}

    # If forcing without reference month, use previous month
    if not ref_month:
        today = date.today()
        if today.month == 1:
            ref_month = f"{today.year - 1}-12"
        else:
            ref_month = f"{today.year}-{today.month - 1:02d}"

    logger.info(f"Collecting Census trade data for {ref_month}")

    # Parse reference month
    year, month = map(int, ref_month.split('-'))
    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1)
    else:
        end_date = date(year, month + 1, 1)

    collector = CensusTradeCollectorV2()

    result = collector.collect_all_commodities(
        start_date=start_date,
        end_date=end_date,
        flow='both'
    )

    if result['success'] and save_db:
        db_stats = collector.save_to_bronze(result['records'])
        result['db_stats'] = db_stats
        logger.info(f"Saved to database: {db_stats}")

    logger.info(f"Collection complete: {result.get('stats', {}).get('total_records', 0)} records")

    return result


def main():
    parser = argparse.ArgumentParser(
        description='Census Trade Monthly Scheduled Collection'
    )

    parser.add_argument(
        '--reference-month',
        help='Reference month to collect (YYYY-MM format)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Run even if not a Census release day'
    )

    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save to database'
    )

    parser.add_argument(
        '--show-schedule',
        action='store_true',
        help='Show Census release schedule and exit'
    )

    args = parser.parse_args()

    if args.show_schedule:
        print("\nCensus FT900 Release Schedule:")
        print("-" * 50)
        for ref_month, release_date in sorted(CENSUS_RELEASE_DATES.items()):
            print(f"  {ref_month} data -> Released {release_date}")

        next_release = get_next_release()
        if next_release:
            print(f"\nNext release: {next_release['release_date']} "
                  f"for {next_release['reference_month']} "
                  f"({next_release['days_until']} days)")
        return

    try:
        result = run_monthly_collection(
            reference_month=args.reference_month,
            save_db=not args.no_save,
            force=args.force
        )

        if result.get('skipped'):
            print(f"Skipped: {result.get('reason')}")
        else:
            print(f"Complete: {result.get('stats', {}).get('total_records', 0)} records")

    except Exception as e:
        logger.error(f"Collection failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
