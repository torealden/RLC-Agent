#!/usr/bin/env python3
"""
Export Cached Collector Data for Power BI

Exports data directly from collector cache files to CSV/Excel format
for immediate use in Power BI without needing database setup.

Usage:
    python scripts/export_cached_data.py --output ./exports/
    python scripts/export_cached_data.py --format xlsx --output ./exports/commodity_data.xlsx
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

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
    logger.error("pandas is required. Install with: pip install pandas openpyxl")
    sys.exit(1)


# Cache directory
CACHE_DIR = Path(__file__).parent.parent / 'data' / 'cache'


def find_cache_files():
    """Find all cache files and return their info"""
    cache_files = {}

    if not CACHE_DIR.exists():
        logger.warning(f"Cache directory not found: {CACHE_DIR}")
        return cache_files

    for cache_file in CACHE_DIR.glob('*.json'):
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)

            # Extract metadata
            records = data.get('data', [])
            if isinstance(records, dict):
                # Handle DataFrame-style JSON
                if 'columns' in records and 'data' in records:
                    records = records['data']
                else:
                    records = [records]

            cache_files[cache_file.stem] = {
                'path': cache_file,
                'records': len(records) if isinstance(records, list) else 0,
                'source': data.get('source', cache_file.stem),
                'timestamp': data.get('timestamp', ''),
                'data': records
            }
        except Exception as e:
            logger.warning(f"Error reading {cache_file}: {e}")

    return cache_files


def load_cache_as_dataframe(cache_data: dict) -> pd.DataFrame:
    """Convert cache data to DataFrame"""
    records = cache_data.get('data', [])

    if not records:
        return pd.DataFrame()

    # Handle different data structures
    if isinstance(records, list):
        df = pd.DataFrame(records)
    elif isinstance(records, dict):
        if 'columns' in records and 'data' in records:
            df = pd.DataFrame(records['data'], columns=records['columns'])
        else:
            df = pd.DataFrame([records])
    else:
        df = pd.DataFrame()

    return df


def export_to_csv(cache_files: dict, output_dir: str):
    """Export all cache files to CSV"""
    os.makedirs(output_dir, exist_ok=True)
    exported = []

    for name, info in cache_files.items():
        df = load_cache_as_dataframe(info)

        if df.empty:
            logger.warning(f"  ○ No data in {name}")
            continue

        filepath = os.path.join(output_dir, f"{name}.csv")
        df.to_csv(filepath, index=False)
        logger.info(f"  ✓ {len(df)} rows → {filepath}")
        exported.append({'name': name, 'rows': len(df), 'file': filepath, 'columns': list(df.columns)})

    return exported


def export_to_excel(cache_files: dict, output_file: str):
    """Export all cache files to single Excel workbook"""
    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
    exported = []

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for name, info in cache_files.items():
            df = load_cache_as_dataframe(info)

            if df.empty:
                logger.warning(f"  ○ No data in {name}")
                continue

            # Excel sheet names max 31 chars
            sheet_name = name[:31]
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            logger.info(f"  ✓ {len(df)} rows → sheet '{sheet_name}'")
            exported.append({'name': name, 'rows': len(df), 'sheet': sheet_name, 'columns': list(df.columns)})

    return exported


def create_data_dictionary(cache_files: dict, output_dir: str):
    """Create a data dictionary CSV describing all fields"""
    rows = []

    for name, info in cache_files.items():
        df = load_cache_as_dataframe(info)

        if df.empty:
            continue

        for col in df.columns:
            dtype = str(df[col].dtype)
            sample = str(df[col].dropna().iloc[0]) if not df[col].dropna().empty else ''
            sample = sample[:100] if len(sample) > 100 else sample

            rows.append({
                'table': name,
                'column': col,
                'data_type': dtype,
                'sample_value': sample,
                'null_count': df[col].isnull().sum(),
                'unique_count': df[col].nunique()
            })

    if rows:
        dict_df = pd.DataFrame(rows)
        filepath = os.path.join(output_dir, '_data_dictionary.csv')
        dict_df.to_csv(filepath, index=False)
        logger.info(f"  ✓ Data dictionary → {filepath}")
        return filepath
    return None


def main():
    parser = argparse.ArgumentParser(description='Export cached collector data for Power BI')

    parser.add_argument('--format', '-f', choices=['csv', 'xlsx'], default='csv',
                       help='Export format (default: csv)')
    parser.add_argument('--output', '-o', required=False,
                       help='Output directory (csv) or file (xlsx)')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List available cache files')

    args = parser.parse_args()

    # Find cache files
    logger.info("Scanning for cached data...")
    cache_files = find_cache_files()

    if not cache_files:
        logger.error("No cached data found. Run collectors first.")
        sys.exit(1)

    if args.list:
        print("\n" + "="*70)
        print("AVAILABLE CACHED DATA")
        print("="*70 + "\n")

        total_records = 0
        for name, info in cache_files.items():
            print(f"  {name}")
            print(f"    Source: {info['source']}")
            print(f"    Records: {info['records']:,}")
            if info['timestamp']:
                print(f"    Cached: {info['timestamp']}")
            print()
            total_records += info['records']

        print(f"Total records available: {total_records:,}")
        return

    # Set default output
    if not args.output:
        args.output = './exports/commodity_data.xlsx' if args.format == 'xlsx' else './exports/'

    print("\n" + "="*70)
    print("EXPORTING CACHED DATA FOR POWER BI")
    print("="*70 + "\n")

    # Export
    if args.format == 'csv':
        os.makedirs(args.output, exist_ok=True)
        exported = export_to_csv(cache_files, args.output)
        create_data_dictionary(cache_files, args.output)
    else:
        exported = export_to_excel(cache_files, args.output)

    # Summary
    print("\n" + "="*70)
    print("EXPORT SUMMARY")
    print("="*70)
    print(f"Format: {args.format.upper()}")
    print(f"Output: {args.output}")
    print(f"Datasets exported: {len(exported)}")
    print(f"Total rows: {sum(e['rows'] for e in exported):,}")

    print("\n" + "-"*70)
    print("POWER BI IMPORT INSTRUCTIONS")
    print("-"*70)

    if args.format == 'csv':
        print("""
1. Open Power BI Desktop
2. Click 'Get Data' → 'Text/CSV'
3. Navigate to: """ + args.output + """
4. Select each CSV file to import
5. For each file, review columns and click 'Load'

TIP: Import all files, then create relationships between tables
     using common columns like 'commodity', 'date', etc.
""")
    else:
        print("""
1. Open Power BI Desktop
2. Click 'Get Data' → 'Excel'
3. Navigate to: """ + args.output + """
4. Select the sheets you want to import
5. Click 'Load' to import all selected sheets

TIP: Each sheet becomes a separate table in Power BI.
     Create relationships using the Relationship view.
""")

    # Print column info for relationships
    print("\n" + "-"*70)
    print("KEY COLUMNS FOR RELATIONSHIPS")
    print("-"*70 + "\n")

    for exp in exported:
        print(f"{exp['name']}:")
        cols = exp.get('columns', [])
        # Highlight potential relationship columns
        key_cols = [c for c in cols if any(k in c.lower() for k in
                   ['date', 'commodity', 'code', 'week', 'year', 'month', 'symbol', 'id'])]
        if key_cols:
            print(f"  Key columns: {', '.join(key_cols)}")
        print()


if __name__ == '__main__':
    main()
