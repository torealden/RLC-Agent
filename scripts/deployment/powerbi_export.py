#!/usr/bin/env python3
"""
PowerBI Data Export and Connection Utilities

Exports commodity database data in formats optimized for PowerBI:
1. Excel files for direct import
2. CSV files for scheduled refresh
3. ODBC connection setup instructions

For live connections, use SQLite ODBC driver or the Python data connector.

Usage:
    python deployment/powerbi_export.py --export-excel
    python deployment/powerbi_export.py --export-csv
    python deployment/powerbi_export.py --query "SELECT * FROM commodity_balance_sheets WHERE commodity='soybeans'"
"""

import argparse
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "rlc_commodities.db"
EXPORT_DIR = DATA_DIR / "powerbi_exports"


def get_connection():
    """Get database connection."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    return sqlite3.connect(str(DB_PATH))


def export_to_excel(output_dir: Path = EXPORT_DIR):
    """Export all data to Excel files organized by commodity."""
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection()

    # Get list of commodities
    commodities = pd.read_sql("SELECT DISTINCT commodity FROM commodity_balance_sheets", conn)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Export each commodity to a separate sheet in one workbook
    output_file = output_dir / f"rlc_commodity_data_{timestamp}.xlsx"

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Export full dataset
        df_all = pd.read_sql("""
            SELECT
                source_file,
                sheet_name,
                commodity,
                country,
                section,
                metric,
                marketing_year,
                value,
                unit,
                extracted_at
            FROM commodity_balance_sheets
            ORDER BY commodity, country, marketing_year
        """, conn)
        df_all.to_excel(writer, sheet_name='All_Data', index=False)

        # Export pivot table style summary
        df_pivot = pd.read_sql("""
            SELECT
                commodity,
                country,
                section,
                metric,
                marketing_year,
                value
            FROM commodity_balance_sheets
            WHERE value IS NOT NULL
        """, conn)
        df_pivot.to_excel(writer, sheet_name='Pivot_Ready', index=False)

        # Export summary stats
        df_summary = pd.read_sql("""
            SELECT
                commodity,
                country,
                COUNT(*) as data_points,
                MIN(marketing_year) as earliest_year,
                MAX(marketing_year) as latest_year,
                COUNT(DISTINCT section) as sections,
                COUNT(DISTINCT metric) as metrics
            FROM commodity_balance_sheets
            GROUP BY commodity, country
            ORDER BY commodity, data_points DESC
        """, conn)
        df_summary.to_excel(writer, sheet_name='Summary', index=False)

    conn.close()

    print(f"\nExported to: {output_file}")
    print(f"  - All_Data: {len(df_all):,} rows")
    print(f"  - Pivot_Ready: {len(df_pivot):,} rows")
    print(f"  - Summary: {len(df_summary):,} rows")

    return output_file


def export_to_csv(output_dir: Path = EXPORT_DIR):
    """Export data to CSV files for scheduled PowerBI refresh."""
    output_dir.mkdir(parents=True, exist_ok=True)

    conn = get_connection()

    # Export main dataset
    df = pd.read_sql("SELECT * FROM commodity_balance_sheets", conn)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Full dataset
    full_csv = output_dir / f"commodity_data_full_{timestamp}.csv"
    df.to_csv(full_csv, index=False)

    # Latest data only (current folder for PowerBI auto-refresh)
    latest_csv = output_dir / "commodity_data_latest.csv"
    df.to_csv(latest_csv, index=False)

    # Export by commodity for smaller file sizes
    for commodity in df['commodity'].unique():
        commodity_csv = output_dir / f"commodity_{commodity}_{timestamp}.csv"
        df[df['commodity'] == commodity].to_csv(commodity_csv, index=False)

    conn.close()

    print(f"\nExported CSV files to: {output_dir}")
    print(f"  - Full dataset: {full_csv.name} ({len(df):,} rows)")
    print(f"  - Latest (for auto-refresh): {latest_csv.name}")
    print(f"  - Per-commodity files for each of {df['commodity'].nunique()} commodities")

    return latest_csv


def run_query(query: str, output_format: str = 'table'):
    """Run a custom SQL query and display/export results."""
    conn = get_connection()

    df = pd.read_sql(query, conn)
    conn.close()

    if output_format == 'table':
        print(f"\nQuery returned {len(df):,} rows:\n")
        print(df.to_string(max_rows=50))
    elif output_format == 'csv':
        output_file = EXPORT_DIR / f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
        print(f"\nExported {len(df):,} rows to: {output_file}")

    return df


def print_powerbi_instructions():
    """Print PowerBI connection instructions."""
    print("""
================================================================================
  POWERBI CONNECTION OPTIONS
================================================================================

Option 1: Excel Import (Easiest)
--------------------------------
1. Run: python deployment/powerbi_export.py --export-excel
2. In PowerBI Desktop: Get Data -> Excel
3. Select the exported .xlsx file from data/powerbi_exports/
4. Load All_Data or Pivot_Ready sheets
5. Set up scheduled refresh by re-running export periodically

Option 2: CSV with Auto-Refresh (Recommended)
---------------------------------------------
1. Run: python deployment/powerbi_export.py --export-csv
2. In PowerBI Desktop: Get Data -> Text/CSV
3. Select: data/powerbi_exports/commodity_data_latest.csv
4. Set up a scheduled task to re-run the export script
5. PowerBI will pick up new data on refresh

Option 3: SQLite ODBC (Live Connection)
---------------------------------------
1. Install SQLite ODBC driver:
   - Download from: http://www.ch-werner.de/sqliteodbc/
   - Install the 64-bit version for PowerBI Desktop

2. Create ODBC Data Source:
   - Open "ODBC Data Sources (64-bit)" in Windows
   - Add -> SQLite3 ODBC Driver
   - Configure:
     * Data Source Name: RLC_Commodities
     * Database: [full path to data/rlc_commodities.db]

3. In PowerBI Desktop:
   - Get Data -> ODBC
   - Select "RLC_Commodities" data source
   - Load tables

Option 4: Python Script Data Source
-----------------------------------
In PowerBI Desktop: Get Data -> Python script

Paste this script:
```python
import sqlite3
import pandas as pd

db_path = r"[FULL_PATH_TO]/data/rlc_commodities.db"
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM commodity_balance_sheets", conn)
conn.close()
```

Replace [FULL_PATH_TO] with your actual path.

================================================================================
  RECOMMENDED POWERBI DATA MODEL
================================================================================

Tables to create:
- commodity_balance_sheets (fact table)
- dim_commodity (commodity dimension)
- dim_country (country dimension)
- dim_time (marketing year dimension)

Relationships:
- commodity_balance_sheets.commodity -> dim_commodity.commodity
- commodity_balance_sheets.country -> dim_country.country
- commodity_balance_sheets.marketing_year -> dim_time.marketing_year

Key Measures:
- Total Value = SUM(value)
- YoY Change = ([This Year Value] - [Last Year Value]) / [Last Year Value]
- Country Share = [Country Value] / [Total Value]

Suggested Visuals:
1. Line chart: Value by marketing_year, segmented by country
2. Bar chart: Top countries by total value per commodity
3. Matrix: Metrics x Years with conditional formatting
4. Map: Country data with bubble size = value
5. Slicer: Filter by commodity, country, section

================================================================================
""")


def main():
    parser = argparse.ArgumentParser(description='PowerBI Data Export')
    parser.add_argument('--export-excel', action='store_true', help='Export to Excel')
    parser.add_argument('--export-csv', action='store_true', help='Export to CSV')
    parser.add_argument('--query', type=str, help='Run custom SQL query')
    parser.add_argument('--instructions', action='store_true', help='Show PowerBI setup instructions')

    args = parser.parse_args()

    if args.export_excel:
        export_to_excel()
    elif args.export_csv:
        export_to_csv()
    elif args.query:
        run_query(args.query)
    elif args.instructions:
        print_powerbi_instructions()
    else:
        print_powerbi_instructions()


if __name__ == '__main__':
    main()
