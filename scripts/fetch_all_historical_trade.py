#!/usr/bin/env python3
"""
Fetch ALL historical trade data for all configured HS codes.
This pulls data from 2020 to present for imports and exports.
"""
import os
import sys
import time
from datetime import date
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / '.env')

from src.agents.collectors.us.census_trade_collector import CensusTradeCollector, AG_HS_CODES

def main():
    """Fetch all historical trade data"""

    collector = CensusTradeCollector()

    # Get all 10-digit HS codes (exclude 4-digit value-only codes)
    all_codes = []
    for name, code in AG_HS_CODES.items():
        if len(code) == 10:  # Only 10-digit codes have quantity data
            all_codes.append(code)

    # Remove duplicates and sort
    all_codes = sorted(set(all_codes))

    print("=" * 70)
    print("FETCHING ALL HISTORICAL TRADE DATA (2020-2025)")
    print("=" * 70)
    print(f"Total HS codes to fetch: {len(all_codes)}")
    print(f"Codes: {all_codes}")
    print()
    print("This will fetch both imports and exports for each code...")
    print("Estimated API calls: ~{} (codes × years × flows)".format(
        len(all_codes) * 6 * 2  # 6 years, 2 flows
    ))
    print()

    start_time = time.time()

    # Fetch all historical data
    counts = collector.save_to_bronze(
        flow='both',
        hs_codes=all_codes,
        start_date=date(2020, 1, 1),
        end_date=date(2025, 12, 31)
    )

    elapsed = time.time() - start_time

    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)
    print(f"Records inserted: {counts['inserted']:,}")
    print(f"Errors: {counts['errors']}")
    print(f"Time elapsed: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"Records per second: {counts['inserted']/elapsed:.1f}")

    return counts

if __name__ == '__main__':
    main()
