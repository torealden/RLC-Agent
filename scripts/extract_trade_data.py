"""
Trade Data Extractor
Extracts historical trade data from RLC Excel trade sheets and loads to PostgreSQL.

Usage:
    python scripts/extract_trade_data.py --scan        # Scan and list files
    python scripts/extract_trade_data.py --preview    # Preview data from one file
    python scripts/extract_trade_data.py --migrate    # Full migration to database
"""

import os
import re
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ============================================================================
# CONFIGURATION
# ============================================================================

# Source directory for trade sheets (relative to project root)
# On Windows: C:\RLC\projects\rlc-agent\Models
# On Linux: /home/user/RLC-Agent/Models
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
TRADE_SHEETS_DIR = PROJECT_ROOT / "Models"

# PostgreSQL configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "rlc_commodities"
PG_USER = "postgres"
PG_PASSWORD = "SoupBoss1"

# ============================================================================
# TRAINING DATA CUTOFF
# ============================================================================
# This will be set at runtime based on user input or default calculation
MAX_MARKETING_YEAR = None  # Will be set by get_cutoff_year()


def get_current_marketing_year() -> str:
    """
    Calculate the current marketing year based on today's date.
    Marketing year runs Sep-Aug, so:
    - Sep 2025 - Aug 2026 = 2025/26 marketing year
    - Jan 2026 is still in 2025/26 marketing year
    """
    today = datetime.now()
    year = today.year
    month = today.month

    # If we're in Sep-Dec, we're in the current year's marketing year
    # If we're in Jan-Aug, we're still in the previous year's marketing year
    if month >= 9:  # Sep-Dec
        start_year = year
    else:  # Jan-Aug
        start_year = year - 1

    end_year_short = (start_year + 1) % 100
    return f"{start_year}/{end_year_short:02d}"


def get_default_cutoff_year() -> str:
    """
    Calculate the default cutoff year (5 years before current marketing year).
    Example: Current = 2025/26, Default cutoff = 2020/21
    """
    current_my = get_current_marketing_year()
    current_start = int(current_my.split('/')[0])
    cutoff_start = current_start - 5
    cutoff_end_short = (cutoff_start + 1) % 100
    return f"{cutoff_start}/{cutoff_end_short:02d}"


def prompt_for_cutoff_year() -> str:
    """
    Prompt user for cutoff year with smart default.
    Returns the selected cutoff marketing year.
    """
    current_my = get_current_marketing_year()
    default_cutoff = get_default_cutoff_year()

    print("\n" + "=" * 70)
    print("DATA CUTOFF YEAR SELECTION")
    print("=" * 70)
    print(f"Current marketing year: {current_my}")
    print(f"Default cutoff (5 years back): {default_cutoff}")
    print()
    print("Enter the last marketing year to INCLUDE in the extraction.")
    print("Data after this year will be excluded.")
    print("Examples: 2020/21, 2019/20, 2023/24")
    print("Press Enter to use the default, or type 'all' to include all data.")
    print()

    while True:
        user_input = input(f"Cutoff year [{default_cutoff}]: ").strip()

        if user_input == "":
            return default_cutoff
        elif user_input.lower() == "all":
            return None  # No cutoff
        elif re.match(r"^\d{4}/\d{2}$", user_input):
            return user_input
        elif re.match(r"^\d{2}/\d{2}$", user_input):
            # Convert short format like "20/21" to "2020/21"
            first = int(user_input.split('/')[0])
            if first >= 60:
                full_year = 1900 + first
            else:
                full_year = 2000 + first
            return f"{full_year}/{user_input.split('/')[1]}"
        else:
            print("Invalid format. Please use format like '2020/21' or '20/21'")


def set_cutoff_year(cutoff: str):
    """Set the global cutoff year and update related constants."""
    global MAX_MARKETING_YEAR
    MAX_MARKETING_YEAR = cutoff

    if cutoff:
        print(f"\n✅ Cutoff year set to: {cutoff}")
        print(f"   Data through {cutoff} will be included.")
    else:
        print(f"\n✅ No cutoff year - ALL data will be included.")

# Patterns to identify trade-related sheets
TRADE_SHEET_PATTERNS = [
    r'trade',
    r'export',
    r'import',
    r'inspection',
    r'sales',
    r'shipment',
]

# Month abbreviations to full month mapping
MONTH_MAP = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_marketing_year(col_name: str) -> Optional[str]:
    """
    Parse marketing year from column header like "'19/20" or "19/20".
    Returns normalized format like "2019/20".

    Year interpretation:
    - 60-99 -> 1960-1999 (historical data)
    - 00-59 -> 2000-2059 (recent/forecast data)
    """
    if not col_name:
        return None

    col_str = str(col_name).strip().replace("'", "")

    # Match patterns like "19/20" or "2019/20"
    match = re.match(r"(\d{2,4})/(\d{2})", col_str)
    if match:
        year1 = match.group(1)
        year2 = match.group(2)

        # Convert 2-digit year to 4-digit
        if len(year1) == 2:
            year1_int = int(year1)
            # 60-99 -> 1960-1999, 00-59 -> 2000-2059
            if year1_int >= 60:
                year1 = f"19{year1}"
            else:
                year1 = f"20{year1}"

        return f"{year1}/{year2}"

    return None


def marketing_year_to_int(my: str) -> int:
    """Convert marketing year string like '2019/20' to integer 2019 for comparison."""
    if not my:
        return 0
    try:
        return int(my.split('/')[0])
    except (ValueError, IndexError):
        return 0


def is_within_training_period(marketing_year: str) -> bool:
    """Check if marketing year is within the training data cutoff."""
    if MAX_MARKETING_YEAR is None:
        return True  # No cutoff, include all data
    if not marketing_year:
        return True  # Include records without marketing year

    cutoff = marketing_year_to_int(MAX_MARKETING_YEAR)
    current = marketing_year_to_int(marketing_year)

    return current <= cutoff


def parse_month_year(col_name: str) -> Optional[Tuple[int, int]]:
    """
    Parse month and year from column header like "Sep 93" or "Oct-23".
    Returns (month_num, full_year) tuple.

    Year interpretation:
    - 60-99 -> 1960-1999 (historical data)
    - 00-59 -> 2000-2059 (recent/forecast data)
    """
    if not col_name:
        return None

    col_str = str(col_name).strip().lower()

    # Match patterns like "sep 93", "sep-93", "sep93"
    match = re.match(r"([a-z]{3})[\s\-]?(\d{2,4})", col_str)
    if match:
        month_str = match.group(1)
        year_str = match.group(2)

        month_num = MONTH_MAP.get(month_str)
        if not month_num:
            return None

        # Convert 2-digit year to 4-digit
        year_int = int(year_str)
        if len(year_str) == 2:
            # 60-99 -> 1960-1999, 00-59 -> 2000-2059
            if year_int >= 60:
                year_int = 1900 + year_int
            else:
                year_int = 2000 + year_int

        return (month_num, year_int)

    return None


def is_month_within_training_period(month: int, year: int) -> bool:
    """Check if a month/year is within the training data cutoff."""
    if MAX_MARKETING_YEAR is None:
        return True  # No cutoff, include all data

    # Calculate cutoff based on MAX_MARKETING_YEAR
    # Marketing year runs Sep-Aug, so 2020/21 ends in Aug 2021
    # Using Sep of the end year as the inclusive cutoff to cover both soybeans (Sep-Aug) and meal/oil (Oct-Sep)
    cutoff_start = marketing_year_to_int(MAX_MARKETING_YEAR)
    cutoff_year = cutoff_start + 1  # End year of the marketing year
    cutoff_month = 9  # September (covers both Sep-Aug and Oct-Sep marketing years)

    if year < cutoff_year:
        return True
    elif year == cutoff_year:
        return month <= cutoff_month
    else:
        return False


def classify_columns(df: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Classify columns into accumulator (marketing year) vs monthly columns.
    """
    result = {
        'accumulator': [],  # Marketing year totals like '19/20
        'monthly': [],      # Monthly data like Sep 93
        'other': []         # Country names, labels, etc.
    }

    for col in df.columns:
        col_str = str(col).strip()

        if parse_marketing_year(col_str):
            result['accumulator'].append(col)
        elif parse_month_year(col_str):
            result['monthly'].append(col)
        else:
            result['other'].append(col)

    return result


def extract_trade_data_from_sheet(
    file_path: Path,
    sheet_name: str,
    commodity: str = None
) -> List[Dict]:
    """
    Extract trade data from a single sheet.

    Returns list of records with:
    - commodity
    - country (destination or origin)
    - flow_type (export/import)
    - period_type (monthly/marketing_year)
    - period_date or marketing_year
    - value
    - source_file
    - sheet_name
    """
    records = []

    try:
        # Read the sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

        if df.empty:
            return records

        # Try to detect unit from title row (row 0)
        unit = 'MT'  # default
        title_row = str(df.iloc[0, 0]) if len(df) > 0 else ''
        if 'million' in title_row.lower():
            unit = 'Million MT'
        elif 'thousand' in title_row.lower():
            unit = 'Thousand MT'

        # Find the header row (usually row 0-2)
        # Look for rows with month patterns OR marketing year patterns
        header_row = 0
        for i in range(min(5, len(df))):
            row_values = df.iloc[i].astype(str).tolist()
            # Look for month patterns (Sep 93) or marketing year patterns (93/94)
            month_count = sum(1 for v in row_values if parse_month_year(v))
            my_count = sum(1 for v in row_values if parse_marketing_year(v))
            if month_count >= 3 or my_count >= 3:
                header_row = i
                break

        # Set headers
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:].reset_index(drop=True)

        # Skip any empty rows at the start
        while len(df) > 0 and df.iloc[0].isna().all():
            df = df.iloc[1:].reset_index(drop=True)

        # Classify columns
        col_types = classify_columns(df)

        # First column is usually country/region (may have NaN header)
        # Try to find first 'other' column, otherwise use first column
        if col_types['other']:
            country_col = col_types['other'][0]
        else:
            country_col = df.columns[0]

        # If country_col is NaN or unnamed, still use first column position
        country_col_idx = 0  # Fallback to first column by position

        # Infer commodity from filename if not provided
        if not commodity:
            commodity = infer_commodity_from_filename(file_path.stem)

        # Infer flow type from title row (preferred) or sheet name (fallback)
        flow_type = infer_flow_type(title_row, sheet_name)

        # Process each row (country)
        for idx, row in df.iterrows():
            # Get country from first column (by position, more reliable)
            country_value = row.iloc[country_col_idx] if len(row) > 0 else None
            country = str(country_value).strip() if pd.notna(country_value) else None

            # Skip empty rows, totals, headers
            if not country or country.lower() in ['total', 'totals', 'nan', '', 'none']:
                continue
            if any(skip in country.lower() for skip in ['region', 'subtotal', '---']):
                continue

            # Extract monthly data
            for col in col_types['monthly']:
                value = row[col]
                if pd.notna(value) and value != 0:
                    try:
                        value_float = float(value)
                        if value_float != 0:
                            month_year = parse_month_year(str(col))
                            if month_year:
                                month, year = month_year
                                # Filter by training period cutoff
                                if not is_month_within_training_period(month, year):
                                    continue
                                records.append({
                                    'commodity': commodity,
                                    'country': country,
                                    'flow_type': flow_type,
                                    'period_type': 'monthly',
                                    'year': year,
                                    'month': month,
                                    'marketing_year': None,
                                    'value': value_float,
                                    'unit': unit,
                                    'source_file': file_path.name,
                                    'sheet_name': sheet_name
                                })
                    except (ValueError, TypeError):
                        continue

            # Extract accumulator (marketing year) data
            for col in col_types['accumulator']:
                value = row[col]
                if pd.notna(value) and value != 0:
                    try:
                        value_float = float(value)
                        if value_float != 0:
                            my = parse_marketing_year(str(col))
                            if my:
                                # Filter by training period cutoff
                                if not is_within_training_period(my):
                                    continue
                                records.append({
                                    'commodity': commodity,
                                    'country': country,
                                    'flow_type': flow_type,
                                    'period_type': 'marketing_year',
                                    'year': None,
                                    'month': None,
                                    'marketing_year': my,
                                    'value': value_float,
                                    'unit': unit,
                                    'source_file': file_path.name,
                                    'sheet_name': sheet_name
                                })
                    except (ValueError, TypeError):
                        continue

        return records

    except Exception as e:
        print(f"  Error processing {sheet_name}: {e}")
        return records


def infer_commodity_from_filename(filename: str) -> str:
    """Infer commodity from filename."""
    filename_lower = filename.lower()

    commodities = {
        'soybean': 'Soybeans',
        'soy': 'Soybeans',
        'corn': 'Corn',
        'wheat': 'Wheat',
        'rapeseed': 'Rapeseed',
        'canola': 'Canola',
        'sunflower': 'Sunflower',
        'palm': 'Palm Oil',
        'cotton': 'Cotton',
        'barley': 'Barley',
        'sorghum': 'Sorghum',
        'meal': 'Soybean Meal',
        'oil': 'Soybean Oil',
    }

    for key, value in commodities.items():
        if key in filename_lower:
            return value

    return filename  # Use filename as commodity if no match


def infer_flow_type_from_text(text: str) -> Optional[str]:
    """
    Infer flow type from text (title row or sheet name).
    Returns None if flow type cannot be determined.
    """
    text_lower = text.lower()

    if 'export' in text_lower or 'shipment' in text_lower:
        return 'export'
    elif 'import' in text_lower:
        return 'import'
    elif 'inspection' in text_lower:
        return 'inspection'
    elif 'sales' in text_lower:
        return 'sales'
    return None


def infer_flow_type(title_row: str, sheet_name: str) -> str:
    """
    Infer flow type, preferring title row over sheet name.

    IMPORTANT: The title row is more reliable than sheet name because
    some spreadsheets have mismatched sheet names vs. actual data.
    For example, a sheet named "Indonesia Palm Kernel Exports" might
    have title "Indonesia Palm Kernel IMPORTS".
    """
    # First try title row - it's more reliable
    flow = infer_flow_type_from_text(title_row)
    if flow:
        return flow

    # Fall back to sheet name
    flow = infer_flow_type_from_text(sheet_name)
    if flow:
        return flow

    return 'trade'


def is_trade_sheet(sheet_name: str) -> bool:
    """Check if sheet name suggests trade data."""
    sheet_lower = sheet_name.lower()
    return any(re.search(pattern, sheet_lower) for pattern in TRADE_SHEET_PATTERNS)


def is_trade_file(filename: str) -> bool:
    """Check if filename suggests trade data."""
    filename_lower = filename.lower()
    return any(re.search(pattern, filename_lower) for pattern in TRADE_SHEET_PATTERNS)


# ============================================================================
# MAIN FUNCTIONS
# ============================================================================

def scan_trade_files(directory: Path) -> List[Dict]:
    """Scan directory for Excel files with trade data."""
    results = []

    if not directory.exists():
        print(f"ERROR: Directory not found: {directory}")
        return results

    print(f"\nScanning: {directory}")
    print("=" * 70)

    # Recursively scan all subdirectories
    excel_files = list(directory.glob("**/*.xlsx")) + list(directory.glob("**/*.xls"))
    # Exclude temp files (start with ~$)
    excel_files = [f for f in excel_files if not f.name.startswith('~$')]
    print(f"Found {len(excel_files)} Excel files\n")

    for file_path in sorted(excel_files):
        try:
            xl = pd.ExcelFile(file_path)
            trade_sheets = [s for s in xl.sheet_names if is_trade_sheet(s)]

            # Also include file if filename suggests trade data
            file_is_trade = is_trade_file(file_path.name)

            if trade_sheets or file_is_trade:
                # If file is trade but no matching sheets, include all sheets
                sheets_to_use = trade_sheets if trade_sheets else xl.sheet_names

                results.append({
                    'file': file_path.name,
                    'path': str(file_path),
                    'trade_sheets': sheets_to_use,
                    'total_sheets': len(xl.sheet_names),
                    'matched_by': 'filename' if file_is_trade and not trade_sheets else 'sheetname'
                })

                match_type = "[filename]" if file_is_trade else "[sheets]"
                print(f"📁 {file_path.name} {match_type}")
                for sheet in sheets_to_use[:5]:  # Show first 5 sheets
                    print(f"   └─ {sheet}")
                if len(sheets_to_use) > 5:
                    print(f"   └─ ... and {len(sheets_to_use) - 5} more sheets")

        except Exception as e:
            print(f"❌ {file_path.name}: {e}")

    print(f"\n{'=' * 70}")
    print(f"Files with trade data: {len(results)}")
    print(f"Total trade sheets: {sum(len(r['trade_sheets']) for r in results)}")

    return results


def preview_file(file_path: Path, max_sheets: int = 2):
    """Preview data from a trade file."""
    print(f"\nPreviewing: {file_path.name}")
    print("=" * 70)

    try:
        xl = pd.ExcelFile(file_path)
        trade_sheets = [s for s in xl.sheet_names if is_trade_sheet(s)]

        if not trade_sheets:
            print("No trade sheets found in this file.")
            return

        for sheet_name in trade_sheets[:max_sheets]:
            print(f"\n📋 Sheet: {sheet_name}")
            print("-" * 50)

            records = extract_trade_data_from_sheet(file_path, sheet_name)

            if records:
                # Show summary
                countries = set(r['country'] for r in records)
                monthly_records = [r for r in records if r['period_type'] == 'monthly']
                my_records = [r for r in records if r['period_type'] == 'marketing_year']

                print(f"  Total records: {len(records)}")
                print(f"  Countries: {len(countries)}")
                print(f"  Monthly records: {len(monthly_records)}")
                print(f"  Marketing year records: {len(my_records)}")

                if monthly_records:
                    years = sorted(set(r['year'] for r in monthly_records if r['year']))
                    print(f"  Year range: {min(years)} - {max(years)}")

                # Show sample records
                print(f"\n  Sample records:")
                for r in records[:5]:
                    if r['period_type'] == 'monthly':
                        print(f"    {r['country']}: {r['month']}/{r['year']} = {r['value']:,.0f}")
                    else:
                        print(f"    {r['country']}: {r['marketing_year']} = {r['value']:,.0f}")
            else:
                print("  No data extracted from this sheet.")

    except Exception as e:
        print(f"Error: {e}")


def migrate_to_database(directory: Path, dry_run: bool = False):
    """Migrate all trade data to PostgreSQL."""
    print("\n" + "=" * 70)
    print("TRADE DATA MIGRATION TO POSTGRESQL")
    print("=" * 70)

    if MAX_MARKETING_YEAR:
        cutoff_start = marketing_year_to_int(MAX_MARKETING_YEAR)
        cutoff_end_year = cutoff_start + 1
        print(f"\n*** DATA CUTOFF ACTIVE ***")
        print(f"*** Only extracting data through {MAX_MARKETING_YEAR} marketing year ***")
        print(f"*** (Monthly data through Sep {cutoff_end_year}) ***\n")
    else:
        print(f"\n*** NO CUTOFF - Including ALL data ***\n")

    # Scan for files
    files = scan_trade_files(directory)

    if not files:
        print("No trade files found. Exiting.")
        return

    # Connect to PostgreSQL
    if not dry_run:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                user=PG_USER,
                password=PG_PASSWORD
            )
            cursor = conn.cursor()
            print(f"\n✅ Connected to PostgreSQL: {PG_DATABASE}")

            # Create table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS bronze.trade_data_raw (
                    id SERIAL PRIMARY KEY,
                    commodity VARCHAR(100),
                    country VARCHAR(200),
                    flow_type VARCHAR(50),
                    period_type VARCHAR(50),
                    year INT,
                    month INT,
                    marketing_year VARCHAR(20),
                    value NUMERIC(20, 4),
                    unit VARCHAR(50),
                    source_file VARCHAR(500),
                    sheet_name VARCHAR(200),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(commodity, country, flow_type, period_type, year, month, marketing_year, source_file)
                );

                CREATE INDEX IF NOT EXISTS idx_trade_commodity ON bronze.trade_data_raw(commodity);
                CREATE INDEX IF NOT EXISTS idx_trade_country ON bronze.trade_data_raw(country);
                CREATE INDEX IF NOT EXISTS idx_trade_year ON bronze.trade_data_raw(year);
                CREATE INDEX IF NOT EXISTS idx_trade_my ON bronze.trade_data_raw(marketing_year);
            """)
            conn.commit()
            print("✅ Table bronze.trade_data_raw ready")

        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return

    # Process each file
    total_records = 0
    total_inserted = 0

    for file_info in files:
        file_path = Path(file_info['path'])
        print(f"\n📁 Processing: {file_info['file']}")

        for sheet_name in file_info['trade_sheets']:
            print(f"   📋 {sheet_name}...", end=" ")

            records = extract_trade_data_from_sheet(file_path, sheet_name)
            total_records += len(records)

            if records and not dry_run:
                # Insert records
                try:
                    insert_sql = """
                        INSERT INTO bronze.trade_data_raw
                        (commodity, country, flow_type, period_type, year, month,
                         marketing_year, value, unit, source_file, sheet_name)
                        VALUES %s
                        ON CONFLICT (commodity, country, flow_type, period_type, year, month, marketing_year, source_file)
                        DO UPDATE SET value = EXCLUDED.value, created_at = NOW()
                    """

                    values = [
                        (r['commodity'], r['country'], r['flow_type'], r['period_type'],
                         r['year'], r['month'], r['marketing_year'], r['value'],
                         r['unit'], r['source_file'], r['sheet_name'])
                        for r in records
                    ]

                    execute_values(cursor, insert_sql, values)
                    conn.commit()
                    total_inserted += len(records)
                    print(f"{len(records):,} records")

                except Exception as e:
                    print(f"ERROR: {e}")
                    conn.rollback()
            else:
                print(f"{len(records):,} records (dry run)")

    # Summary
    print("\n" + "=" * 70)
    print("MIGRATION SUMMARY")
    print("=" * 70)
    print(f"Files processed: {len(files)}")
    print(f"Total records extracted: {total_records:,}")
    if not dry_run:
        print(f"Records inserted/updated: {total_inserted:,}")

        # Show table stats
        cursor.execute("SELECT COUNT(*) FROM bronze.trade_data_raw")
        total_in_db = cursor.fetchone()[0]
        print(f"Total records in database: {total_in_db:,}")

        cursor.execute("""
            SELECT commodity, COUNT(*) as cnt
            FROM bronze.trade_data_raw
            GROUP BY commodity
            ORDER BY cnt DESC
        """)
        print("\nRecords by commodity:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]:,}")

        conn.close()

    print("\n✅ Migration complete!")


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Extract trade data from Excel sheets")
    parser.add_argument("--scan", action="store_true", help="Scan directory and list trade files")
    parser.add_argument("--preview", type=str, help="Preview data from a specific file")
    parser.add_argument("--migrate", action="store_true", help="Migrate all trade data to database")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be migrated without writing")
    parser.add_argument("--dir", type=str, help="Override source directory")
    parser.add_argument("--cutoff", type=str, help="Cutoff marketing year (e.g., '2020/21'). Use 'all' for no cutoff.")
    parser.add_argument("--no-prompt", action="store_true", help="Skip interactive prompts, use defaults")

    args = parser.parse_args()

    directory = Path(args.dir) if args.dir else TRADE_SHEETS_DIR

    if args.scan:
        scan_trade_files(directory)
    elif args.preview:
        preview_file(Path(args.preview))
    elif args.migrate:
        # Determine cutoff year
        if args.cutoff:
            # Use command-line argument
            if args.cutoff.lower() == 'all':
                set_cutoff_year(None)
            else:
                set_cutoff_year(args.cutoff)
        elif args.no_prompt:
            # Use default without prompting
            set_cutoff_year(get_default_cutoff_year())
        else:
            # Interactive prompt
            cutoff = prompt_for_cutoff_year()
            set_cutoff_year(cutoff)

        migrate_to_database(directory, dry_run=args.dry_run)
    else:
        # Default: scan
        scan_trade_files(directory)
        print("\n💡 Use --migrate to load data to database")
        print("💡 Use --preview <file> to see sample data from a file")
        print("💡 Use --cutoff <year> to set data cutoff (e.g., --cutoff 2020/21)")


if __name__ == "__main__":
    main()
