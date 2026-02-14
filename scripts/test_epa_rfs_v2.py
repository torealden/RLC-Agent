#!/usr/bin/env python3
"""Test the EPA RFS Collector V2"""

import sys
import logging
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

from agents.collectors.us.epa_rfs_collector_v2 import EPARFSCollectorV2, D_CODE_INFO

def main():
    collector = EPARFSCollectorV2()

    # Load the generation breakout file
    data_dir = PROJECT_ROOT / 'data' / 'raw' / 'rfs_data'
    print(f"\nLoading EPA RFS data from: {data_dir}")

    all_data = collector.load_all_files(data_dir)
    records = all_data.get('generation_breakout', [])

    print(f"\nLoaded {len(records)} generation records")

    if records:
        # Show summary by D-code
        print("\n=== RIN Generation Summary ===")
        print(f"{'Year':<6} {'D3 Cellulosic':>15} {'D4 BBD':>15} {'D5 Advanced':>15} {'D6 Conv':>15} {'D7 Cell Diesel':>15}")
        print("-" * 90)

        # Group by year
        by_year = {}
        for r in records:
            year = r['rin_year']
            d_code = r['d_code']
            total = r['total_rins'] or 0
            if year not in by_year:
                by_year[year] = {}
            by_year[year][d_code] = total

        for year in sorted(by_year.keys())[-10:]:  # Last 10 years
            d = by_year[year]
            print(f"{year:<6} {d.get('3', 0):>15,} {d.get('4', 0):>15,} {d.get('5', 0):>15,} {d.get('6', 0):>15,} {d.get('7', 0):>15,}")

        # Show 2025 detail
        print("\n=== 2025 Detail ===")
        for r in records:
            if r['rin_year'] == 2025:
                d_code = r['d_code']
                info = D_CODE_INFO.get(d_code, {})
                total = r['total_rins'] or 0
                domestic = r['domestic_rins'] or 0
                imported = r['importer_rins'] or 0
                foreign = r['foreign_generation_rins'] or 0
                domestic_pct = 100 * domestic / total if total else 0

                print(f"\nD{d_code} - {info.get('name', 'Unknown')}")
                print(f"  Total:    {total:>15,} RINs")
                print(f"  Domestic: {domestic:>15,} ({domestic_pct:.1f}%)")
                print(f"  Import:   {imported:>15,}")
                print(f"  Foreign:  {foreign:>15,}")
                ev = info.get('equivalence_value', 1.0)
                gallons = total / ev
                print(f"  ~Gallons: {gallons:>15,.0f} (EV={ev})")


if __name__ == '__main__':
    main()
