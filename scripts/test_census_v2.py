#!/usr/bin/env python3
"""Test the Census Trade Collector V2"""

import sys
import logging
from pathlib import Path
from datetime import date

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

from agents.collectors.us.census_trade_collector_v2 import (
    CensusTradeCollectorV2,
    CENSUS_SCHEDULE_B_CODES,
    get_primary_hs_code
)

def main():
    collector = CensusTradeCollectorV2()

    # Test fetching with working Census code
    hs_code = CENSUS_SCHEDULE_B_CODES['soybeans']
    print(f'Testing soybean export fetch with code {hs_code}...')

    records = collector.fetch_trade_data(
        'exports',
        hs_code,
        date(2024, 10, 1),
        date(2024, 10, 31)
    )

    print(f'Records fetched: {len(records)}')
    if records:
        print('\nSample record:')
        for k, v in list(records[0].items())[:6]:
            print(f'  {k}: {v}')

        # Show top 5 destinations by value
        print('\nTop 5 destinations by value:')
        sorted_recs = sorted(records, key=lambda x: x.get('value_usd') or 0, reverse=True)[:5]
        for r in sorted_recs:
            val = r.get('value_usd', 0) or 0
            qty = r.get('quantity', 0) or 0
            print(f"  {r['country_name']}: ${val:,.0f} ({qty:,.0f} MT)")

    # Test priority commodity lookup
    print('\n\nTesting priority commodity code lookup:')
    for commodity in ['soybeans', 'corn', 'wheat', 'ddgs', 'soybean_meal']:
        code = CENSUS_SCHEDULE_B_CODES.get(commodity)
        print(f'  {commodity}: {code}')

if __name__ == '__main__':
    main()
