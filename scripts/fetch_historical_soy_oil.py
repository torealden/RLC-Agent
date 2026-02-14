#!/usr/bin/env python3
"""
Fetch historical data for soybean oil codes that were recently added.
Codes: 1507904020 (once refined), 1507904040 (fully refined imports)
"""
import os
import sys
from datetime import date
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')

from src.agents.collectors.us.census_trade_collector import CensusTradeCollector

def main():
    """Fetch historical soybean oil data for new codes"""

    collector = CensusTradeCollector()

    # New soybean oil codes to fetch historical data for
    new_codes = [
        '1507904020',  # Soybean oil once refined (exports primarily)
        '1507904040',  # Soybean oil fully refined (imports primarily)
    ]

    # Also include palm oil and canola oil codes
    additional_codes = [
        '1511100000',  # Palm oil crude
        '1511900000',  # Palm oil refined
        '1514110000',  # Canola oil crude
        '1514190000',  # Canola oil NESOI
        '1512190020',  # Sunflower oil refined
        '1515290040',  # Corn oil fully refined
        '1513290000',  # Palm kernel oil
    ]

    all_codes = new_codes + additional_codes

    print("=" * 60)
    print("Fetching historical vegetable oil data (2020-2025)")
    print("=" * 60)
    print(f"HS Codes: {all_codes}")
    print()

    # Fetch historical data from 2020 to present
    counts = collector.save_to_bronze(
        flow='both',
        hs_codes=all_codes,
        start_date=date(2020, 1, 1),
        end_date=date(2025, 12, 31)
    )

    print("\nResults:")
    print(f"  Inserted: {counts['inserted']}")
    print(f"  Errors: {counts['errors']}")

    return counts

if __name__ == '__main__':
    main()
