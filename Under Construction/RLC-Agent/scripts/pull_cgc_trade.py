#!/usr/bin/env python3
"""
Pull Canadian Grain Commission (CGC) Trade Data

Downloads monthly export data from CGC website and:
1. Saves to PostgreSQL database
2. Updates Excel model files (using win32com to preserve external links)

Data sources:
- Historical CSV: https://grainscanada.gc.ca/.../exports-grain-licensed-facilities/csv/exports.csv
- Monthly Excel: https://grainscanada.gc.ca/.../exports-grain-licensed-facilities/2025/october.xlsx

Commodities tracked:
- Canola (Seed)
- Canola Meal
- Canola Oil

Usage:
    python scripts/pull_cgc_trade.py --download-csv
    python scripts/pull_cgc_trade.py --csv-file path/to/exports.csv --update-excel
    python scripts/pull_cgc_trade.py --excel-file path/to/october.xlsx --update-excel
"""

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dotenv import load_dotenv

# Add project root to path and load environment variables
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env files
load_dotenv(PROJECT_ROOT / '.env')
api_manager_env = PROJECT_ROOT / 'api Manager' / '.env'
if api_manager_env.exists():
    load_dotenv(api_manager_env)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("WARNING: pandas not installed. Some features may be limited.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

# CGC Data URLs
CGC_BASE_URL = "https://grainscanada.gc.ca/en/grain-research/statistics/exports-grain-licensed-facilities"
CGC_HISTORICAL_CSV = f"{CGC_BASE_URL}/csv/exports.csv"

# CGC commodity codes mapping to Excel sheets
# The CGC CSV uses specific commodity names
CGC_COMMODITIES = {
    'CANOLA': {
        'cgc_names': ['Canola', 'CANOLA', 'Canola Seed'],
        'excel_sheets': {
            'exports': 'Canada Seed Exports',
            'imports': 'Canada Seed Imports',
        },
    },
    'CANOLA_MEAL': {
        'cgc_names': ['Canola Meal', 'CANOLA MEAL', 'Canola meal'],
        'excel_sheets': {
            'exports': 'Canada Meal Exports',
            'imports': 'Canada Meal Imports',
        },
    },
    'CANOLA_OIL': {
        'cgc_names': ['Canola Oil', 'CANOLA OIL', 'Canola oil'],
        'excel_sheets': {
            'exports': 'Canada Oil Exports',
            'imports': 'Canada Oil Imports',
        },
    },
}

# Excel file path
EXCEL_FILE = 'Models/Oilseeds/World Rapeseed Trade.xlsx'

# Marketing year for canola starts August 1
MARKETING_YEAR_START_MONTH = 8  # August

# Column mappings - CGC CSV typically has these columns
# Will be adjusted based on actual CSV structure
CGC_CSV_COLUMNS = {
    'date': ['Date', 'date', 'DATE', 'Month', 'Period'],
    'commodity': ['Commodity', 'commodity', 'COMMODITY', 'Grain', 'Product'],
    'destination': ['Destination', 'destination', 'DESTINATION', 'Country'],
    'quantity': ['Quantity', 'quantity', 'QUANTITY', 'Volume', 'Tonnes', 'MT'],
    'port': ['Port', 'port', 'PORT', 'Point of Exit'],
}

# Country name mapping from CGC to Excel format
CGC_COUNTRY_MAPPING = {
    # Common variations
    'UNITED STATES': 'United States',
    'USA': 'United States',
    'U.S.A.': 'United States',
    'CHINA': 'China',
    "CHINA, PEOPLE'S REPUBLIC OF": 'China',
    'PEOPLES REPUBLIC OF CHINA': 'China',
    'JAPAN': 'Japan',
    'MEXICO': 'Mexico',
    'PAKISTAN': 'Pakistan',
    'BANGLADESH': 'Bangladesh',
    'UNITED ARAB EMIRATES': 'United Arab Emirates',
    'UAE': 'United Arab Emirates',
    'SOUTH KOREA': 'Korea, Republic of',
    'REPUBLIC OF KOREA': 'Korea, Republic of',
    'KOREA': 'Korea, Republic of',
    'EUROPEAN UNION': 'European Union-28',
    'EU': 'European Union-28',
    'GERMANY': 'Germany, Federal Republic of',
    'UNITED KINGDOM': 'United Kingdom',
    'UK': 'United Kingdom',
    'FRANCE': 'France',
    'NETHERLANDS': 'Netherlands',
    'BELGIUM': 'Belgium',
    'ITALY': 'Italy',
    'SPAIN': 'Spain',
    'VIETNAM': 'Vietnam',
    'THAILAND': 'Thailand',
    'INDONESIA': 'Indonesia',
    'PHILIPPINES': 'Philippines',
    'INDIA': 'India',
    'TAIWAN': 'Taiwan',
    'HONG KONG': 'Hong Kong',
    'SINGAPORE': 'Singapore',
    'MALAYSIA': 'Malaysia',
    'AUSTRALIA': 'Australia',
    'NEW ZEALAND': 'New Zealand',
}


# =============================================================================
# DATA PARSING FUNCTIONS
# =============================================================================

def get_marketing_year(dt: date) -> str:
    """
    Get marketing year string for a date.
    Canola marketing year runs August 1 to July 31.
    """
    if dt.month >= MARKETING_YEAR_START_MONTH:
        my_start = dt.year
    else:
        my_start = dt.year - 1

    # Format as "23/24" for 2023/24 marketing year
    return f"{str(my_start)[2:]}/{str(my_start + 1)[2:]}"


def parse_cgc_date(date_str: str) -> Optional[date]:
    """
    Parse date from CGC CSV.
    CGC uses various date formats - try common ones.
    """
    if not date_str:
        return None

    date_str = str(date_str).strip()

    # Try various formats
    formats = [
        '%Y-%m-%d',      # 2024-10-01
        '%d/%m/%Y',      # 01/10/2024
        '%m/%d/%Y',      # 10/01/2024
        '%Y/%m/%d',      # 2024/10/01
        '%B %Y',         # October 2024
        '%b %Y',         # Oct 2024
        '%Y-%m',         # 2024-10
        '%m-%Y',         # 10-2024
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.date() if hasattr(dt, 'date') else dt
        except ValueError:
            continue

    # Try to extract year and month from numeric values
    try:
        # Could be Excel serial date
        if isinstance(date_str, (int, float)) or date_str.replace('.', '').isdigit():
            serial = float(date_str)
            if serial > 1000:  # Excel serial date
                excel_epoch = datetime(1899, 12, 30)
                return (excel_epoch + timedelta(days=int(serial))).date()
    except:
        pass

    logger.warning(f"Could not parse date: {date_str}")
    return None


def clean_country_name(name: str) -> str:
    """Standardize country name to match Excel format"""
    if not name:
        return ''

    name = name.strip()

    # Check mapping
    name_upper = name.upper()
    if name_upper in CGC_COUNTRY_MAPPING:
        return CGC_COUNTRY_MAPPING[name_upper]

    # Return as-is with title case
    return name.title()


def parse_quantity(value: Any) -> Optional[float]:
    """Parse quantity value - CGC reports in tonnes"""
    if value is None or value == '' or value == '-':
        return None

    try:
        # Remove commas and whitespace
        cleaned = str(value).replace(',', '').replace(' ', '').strip()

        # Handle parentheses for negative numbers
        if cleaned.startswith('(') and cleaned.endswith(')'):
            cleaned = '-' + cleaned[1:-1]

        return float(cleaned)
    except (ValueError, TypeError):
        return None


# =============================================================================
# CSV PARSING
# =============================================================================

def detect_csv_columns(headers: List[str]) -> Dict[str, int]:
    """
    Detect column positions in CGC CSV based on header names.

    Returns dict mapping field name to column index.
    """
    column_map = {}
    headers_lower = [h.lower().strip() for h in headers]

    for field, possible_names in CGC_CSV_COLUMNS.items():
        for name in possible_names:
            if name.lower() in headers_lower:
                column_map[field] = headers_lower.index(name.lower())
                break

    return column_map


def parse_cgc_csv(csv_path: Path) -> List[Dict]:
    """
    Parse CGC historical exports CSV file.

    Args:
        csv_path: Path to the CSV file

    Returns:
        List of trade records
    """
    records = []

    logger.info(f"Parsing CGC CSV: {csv_path}")

    # Try different encodings
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                # Detect delimiter
                sample = f.read(4096)
                f.seek(0)

                if '\t' in sample:
                    delimiter = '\t'
                elif ';' in sample:
                    delimiter = ';'
                else:
                    delimiter = ','

                reader = csv.reader(f, delimiter=delimiter)

                # Get headers
                headers = next(reader)
                logger.info(f"CSV headers: {headers}")

                # Detect column positions
                col_map = detect_csv_columns(headers)
                logger.info(f"Detected columns: {col_map}")

                if not col_map:
                    logger.warning("Could not detect required columns. Using positional parsing.")
                    # Fallback: assume standard format
                    # Date, Commodity, Destination, Quantity
                    col_map = {'date': 0, 'commodity': 1, 'destination': 2, 'quantity': 3}

                # Parse data rows
                row_count = 0
                for row in reader:
                    if len(row) < 3:
                        continue

                    try:
                        # Extract values
                        date_val = row[col_map.get('date', 0)] if 'date' in col_map else None
                        commodity = row[col_map.get('commodity', 1)] if 'commodity' in col_map else ''
                        destination = row[col_map.get('destination', 2)] if 'destination' in col_map else ''
                        quantity = row[col_map.get('quantity', 3)] if 'quantity' in col_map else None

                        # Parse date
                        parsed_date = parse_cgc_date(date_val)
                        if not parsed_date:
                            continue

                        # Parse quantity
                        qty = parse_quantity(quantity)
                        if qty is None:
                            continue

                        # Clean country name
                        dest_clean = clean_country_name(destination)

                        record = {
                            'date': parsed_date,
                            'year': parsed_date.year,
                            'month': parsed_date.month,
                            'commodity': commodity.strip(),
                            'destination': dest_clean,
                            'quantity_tonnes': qty,
                            'marketing_year': get_marketing_year(parsed_date),
                            'source': 'CGC',
                        }

                        records.append(record)
                        row_count += 1

                    except (IndexError, ValueError) as e:
                        logger.debug(f"Skipping row: {e}")
                        continue

                logger.info(f"Parsed {row_count} records from CSV (encoding: {encoding})")
                break  # Success, exit encoding loop

        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.error(f"Error parsing CSV with {encoding}: {e}")
            continue

    return records


def parse_cgc_excel(excel_path: Path) -> List[Dict]:
    """
    Parse CGC monthly Excel file.

    Args:
        excel_path: Path to the Excel file

    Returns:
        List of trade records
    """
    if not PANDAS_AVAILABLE:
        logger.error("pandas is required for Excel parsing")
        return []

    records = []

    logger.info(f"Parsing CGC Excel: {excel_path}")

    try:
        # Read all sheets
        xl = pd.ExcelFile(excel_path)

        for sheet_name in xl.sheet_names:
            logger.info(f"Processing sheet: {sheet_name}")

            df = pd.read_excel(xl, sheet_name=sheet_name)

            # Try to identify the data structure
            # CGC monthly files typically have countries in rows, dates in columns

            if df.empty:
                continue

            # Convert to records based on sheet structure
            for idx, row in df.iterrows():
                for col in df.columns:
                    if col == df.columns[0]:
                        continue  # Skip country/label column

                    # Try to parse column as date
                    col_date = parse_cgc_date(str(col))
                    if not col_date:
                        continue

                    value = row[col]
                    qty = parse_quantity(value)

                    if qty is None or qty == 0:
                        continue

                    destination = str(row[df.columns[0]]) if len(df.columns) > 0 else ''
                    dest_clean = clean_country_name(destination)

                    record = {
                        'date': col_date,
                        'year': col_date.year,
                        'month': col_date.month,
                        'commodity': 'CANOLA',  # Infer from sheet name
                        'destination': dest_clean,
                        'quantity_tonnes': qty,
                        'marketing_year': get_marketing_year(col_date),
                        'source': 'CGC',
                    }

                    records.append(record)

        logger.info(f"Parsed {len(records)} records from Excel")

    except Exception as e:
        logger.error(f"Error parsing Excel: {e}")
        import traceback
        traceback.print_exc()

    return records


# =============================================================================
# DATA AGGREGATION
# =============================================================================

def aggregate_monthly_by_destination(
    records: List[Dict],
    commodity_filter: List[str] = None
) -> Dict[Tuple, Dict]:
    """
    Aggregate records to monthly totals by destination.

    Returns:
        Dict mapping (year, month, destination) to aggregated data
    """
    monthly = defaultdict(lambda: {
        'quantity_tonnes': 0,
        'count': 0,
        'marketing_year': None
    })

    for r in records:
        # Filter by commodity if specified
        if commodity_filter:
            if not any(cf.lower() in r.get('commodity', '').lower() for cf in commodity_filter):
                continue

        key = (r['year'], r['month'], r['destination'])
        monthly[key]['quantity_tonnes'] += r.get('quantity_tonnes') or 0
        monthly[key]['count'] += 1
        monthly[key]['marketing_year'] = r.get('marketing_year')

    return dict(monthly)


def aggregate_by_marketing_year(
    records: List[Dict],
    commodity_filter: List[str] = None
) -> Dict[Tuple, Dict]:
    """
    Aggregate records to marketing year totals by destination.
    """
    yearly = defaultdict(lambda: {
        'quantity_tonnes': 0,
        'count': 0,
    })

    for r in records:
        if commodity_filter:
            if not any(cf.lower() in r.get('commodity', '').lower() for cf in commodity_filter):
                continue

        key = (r.get('marketing_year', ''), r['destination'])
        yearly[key]['quantity_tonnes'] += r.get('quantity_tonnes') or 0
        yearly[key]['count'] += 1

    return dict(yearly)


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

def get_database_url() -> str:
    """Get database connection URL from environment variables."""
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return database_url

    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'rlc_commodities')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', '')

    if db_password:
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    else:
        return f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}"


def save_to_database(records: List[Dict], connection_string: str = None) -> int:
    """
    Save CGC trade records to PostgreSQL database.
    """
    connection_string = connection_string or get_database_url()

    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        return 0

    from urllib.parse import urlparse
    parsed = urlparse(connection_string)

    db_name = parsed.path[1:]
    print(f"Saving {len(records)} records to PostgreSQL: {parsed.hostname}/{db_name}")

    try:
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=db_name,
            user=parsed.username,
            password=parsed.password
        )
    except psycopg2.OperationalError as e:
        print(f"ERROR connecting to PostgreSQL: {e}")
        return 0

    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cgc_trade_records (
            id BIGSERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            commodity VARCHAR(50) NOT NULL,
            destination VARCHAR(100) NOT NULL,
            quantity_tonnes NUMERIC(18,4),
            marketing_year VARCHAR(10),
            source VARCHAR(20) DEFAULT 'CGC',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, commodity, destination)
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cgc_date ON cgc_trade_records(trade_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cgc_commodity ON cgc_trade_records(commodity)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cgc_destination ON cgc_trade_records(destination)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cgc_my ON cgc_trade_records(marketing_year)")

    conn.commit()

    # Insert records
    inserted = 0
    for r in records:
        try:
            cursor.execute("""
                INSERT INTO cgc_trade_records
                (trade_date, year, month, commodity, destination, quantity_tonnes, marketing_year)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, commodity, destination)
                DO UPDATE SET
                    quantity_tonnes = EXCLUDED.quantity_tonnes,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                r['date'], r['year'], r['month'], r['commodity'],
                r['destination'], r.get('quantity_tonnes'), r.get('marketing_year')
            ))
            inserted += 1
        except psycopg2.Error as e:
            logger.warning(f"Failed to insert record: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"SUCCESS: Saved {inserted} records to PostgreSQL")
    return inserted


# =============================================================================
# EXCEL UPDATE FUNCTIONS
# =============================================================================

def parse_excel_date_header(cell_value) -> Optional[Tuple[int, int]]:
    """
    Parse a date from an Excel header cell.
    Handles datetime objects and Excel serial dates.
    """
    if cell_value is None:
        return None

    # Handle datetime objects
    if isinstance(cell_value, datetime):
        return (cell_value.year, cell_value.month)

    if isinstance(cell_value, date):
        return (cell_value.year, cell_value.month)

    # Handle Excel serial date numbers
    if isinstance(cell_value, (int, float)) and cell_value > 1000:
        try:
            excel_epoch = datetime(1899, 12, 30)
            dt = excel_epoch + timedelta(days=int(cell_value))
            return (dt.year, dt.month)
        except:
            pass

    # Handle string formats
    if isinstance(cell_value, str):
        cell_str = cell_value.strip()

        # Skip marketing year formats like "93/94"
        if '/' in cell_str and len(cell_str) <= 7:
            parts = cell_str.split('/')
            if len(parts) == 2 and all(p.isdigit() and len(p) <= 2 for p in parts):
                return None  # Marketing year column

        # Parse monthly formats
        for fmt in ['%b-%y', '%b %y', '%B-%y', '%B %y', '%Y-%m-%d', '%d/%m/%Y']:
            try:
                dt = datetime.strptime(cell_str, fmt)
                return (dt.year, dt.month)
            except ValueError:
                continue

    return None


def find_country_rows(ws, max_row: int = 400) -> Dict[str, int]:
    """
    Dynamically find country/destination rows by scanning column A.
    """
    country_rows = {}

    for row in range(1, max_row + 1):
        cell_value = ws.Cells(row, 1).Value
        if cell_value and isinstance(cell_value, str):
            country = cell_value.strip()

            # Skip header rows
            skip_patterns = [
                'CANADA', 'EXPORTS', 'IMPORTS', 'MILLION', 'TONNES', '1,000',
                'TOTAL', 'SOURCE', 'NOTES'
            ]

            if any(pattern in country.upper() for pattern in skip_patterns):
                continue

            if len(country) < 3:
                continue

            # Store with original case for matching
            country_rows[country] = row
            country_rows[country.upper()] = row
            country_rows[country.lower()] = row

    return country_rows


def update_excel_file(
    excel_path: Path,
    monthly_data: Dict[Tuple, Dict],
    sheet_name: str
) -> bool:
    """
    Update the Excel file with CGC trade data using win32com.
    """
    try:
        import win32com.client
        import pythoncom
    except ImportError:
        print("ERROR: pywin32 not installed. Run: pip install pywin32")
        return False

    import shutil
    import tempfile
    import time

    excel_path = Path(excel_path).resolve()

    if not excel_path.exists():
        print(f"ERROR: Excel file not found: {excel_path}")
        return False

    print(f"  Updating sheet: {sheet_name}")
    print(f"  Excel file: {excel_path}")

    # Handle cloud sync files
    temp_path = None
    working_path = excel_path
    is_cloud_sync = any(x in str(excel_path).lower() for x in ['dropbox', 'onedrive', 'google drive'])

    if is_cloud_sync:
        try:
            temp_dir = Path(tempfile.gettempdir())
            temp_path = temp_dir / f"temp_{excel_path.name}"
            print(f"  Cloud sync detected - copying to temp: {temp_path}")
            shutil.copy2(excel_path, temp_path)
            working_path = temp_path
        except Exception as e:
            print(f"  Warning: Could not create temp copy: {e}")
            working_path = excel_path

    excel = None
    wb = None

    try:
        pythoncom.CoInitialize()

        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.ScreenUpdating = False

        wb = excel.Workbooks.Open(str(working_path), UpdateLinks=0, ReadOnly=False)

    except Exception as e:
        print(f"ERROR: Failed to open Excel file: {e}")
        if excel:
            try:
                excel.Quit()
            except:
                pass
        pythoncom.CoUninitialize()
        if temp_path and temp_path.exists():
            temp_path.unlink()
        return False

    try:
        # Find sheet
        ws = None
        sheet_names = []
        for i in range(1, wb.Sheets.Count + 1):
            sheet_names.append(wb.Sheets(i).Name)
            if wb.Sheets(i).Name == sheet_name:
                ws = wb.Sheets(i)
                break

        if ws is None:
            print(f"ERROR: Sheet '{sheet_name}' not found")
            print(f"Available sheets: {sheet_names}")
            wb.Close(SaveChanges=False)
            excel.Quit()
            pythoncom.CoUninitialize()
            return False

        # Get dimensions
        used_range = ws.UsedRange
        max_col = used_range.Columns.Count
        max_row = used_range.Rows.Count

        # Find date columns (row 2)
        date_columns = {}
        header_row = 2

        print(f"  Scanning row {header_row} for date columns...")

        for col in range(1, max_col + 1):
            cell_value = ws.Cells(header_row, col).Value
            parsed = parse_excel_date_header(cell_value)
            if parsed:
                year, month = parsed
                if year >= 1990:
                    date_columns[(year, month)] = col

        print(f"  Found {len(date_columns)} monthly date columns")

        if date_columns:
            years = sorted(set(y for y, m in date_columns.keys()))
            print(f"  Date range: {min(years)} to {max(years)}")

        # Find country rows
        print(f"  Scanning column A for country names...")
        country_rows = find_country_rows(ws, max_row)
        print(f"  Found {len(country_rows)//3} country rows")  # Divide by 3 due to case variants

        # Update cells
        updated = 0
        not_found_destinations = set()
        not_found_dates = set()

        for (year, month, destination), data in monthly_data.items():
            # Get row
            row = country_rows.get(destination)
            if not row:
                row = country_rows.get(destination.upper())
            if not row:
                row = country_rows.get(destination.title())

            if not row:
                not_found_destinations.add(destination)
                continue

            # Get column
            col = date_columns.get((year, month))
            if not col:
                not_found_dates.add((year, month))
                continue

            # Get value - CGC data is in tonnes, Excel is in 1,000 tonnes
            value = data.get('quantity_tonnes')

            if value and value > 0:
                value_in_tmt = value / 1000.0  # Convert tonnes to 1,000 tonnes
                ws.Cells(row, col).Value = round(value_in_tmt, 3)
                updated += 1

        # Save
        wb.Save()
        print(f"  Updated {updated} cells in {sheet_name}")

        if not_found_destinations:
            unmapped = list(not_found_destinations)[:10]
            print(f"  Unmapped destinations ({len(not_found_destinations)} total): {unmapped}")

        if not_found_dates:
            missing = sorted(list(not_found_dates))[:10]
            print(f"  Dates not in sheet ({len(not_found_dates)} total): {missing}")

        wb.Close(SaveChanges=True)
        excel.Quit()
        pythoncom.CoUninitialize()

        time.sleep(0.5)

        # Copy back if temp file used
        if temp_path and temp_path.exists():
            try:
                shutil.copy2(temp_path, excel_path)
                temp_path.unlink()
                print(f"  Successfully updated: {excel_path}")
            except Exception as e:
                print(f"  WARNING: Could not copy back: {e}")

        return True

    except Exception as e:
        print(f"ERROR: Failed to update Excel: {e}")
        import traceback
        traceback.print_exc()
        try:
            if wb:
                wb.Close(SaveChanges=False)
            if excel:
                excel.Quit()
            pythoncom.CoUninitialize()
        except:
            pass
        return False


def update_all_excel_sheets(
    records: List[Dict],
    project_root: Path
) -> Dict[str, int]:
    """
    Update all relevant Excel sheets for CGC canola data.
    """
    results = {}

    excel_path = project_root / EXCEL_FILE

    if not excel_path.exists():
        print(f"ERROR: Excel file not found: {excel_path}")
        return results

    for commodity_key, commodity_info in CGC_COMMODITIES.items():
        cgc_names = commodity_info['cgc_names']

        for flow, sheet_name in commodity_info['excel_sheets'].items():
            # Filter records for this commodity
            commodity_records = [r for r in records if any(
                cn.lower() in r.get('commodity', '').lower() for cn in cgc_names
            )]

            if not commodity_records:
                print(f"\nNo {commodity_key} records for {flow}")
                continue

            # Aggregate monthly by destination
            monthly_data = aggregate_monthly_by_destination(commodity_records, cgc_names)

            print(f"\n{'=' * 60}")
            print(f"Updating {sheet_name}")
            print(f"Records: {len(commodity_records)}, Month/destination combos: {len(monthly_data)}")
            print(f"{'=' * 60}")

            if update_excel_file(excel_path, monthly_data, sheet_name):
                results[sheet_name] = len(monthly_data)
            else:
                results[sheet_name] = 0

    return results


# =============================================================================
# DOWNLOAD FUNCTIONS
# =============================================================================

def download_cgc_csv(output_path: Path = None) -> Optional[Path]:
    """
    Download the CGC historical exports CSV.
    """
    output_path = output_path or Path('data/cgc_exports.csv')
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Downloading CGC historical CSV from: {CGC_HISTORICAL_CSV}")

    try:
        response = requests.get(
            CGC_HISTORICAL_CSV,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            timeout=60
        )

        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded to: {output_path}")
            return output_path
        else:
            print(f"ERROR: HTTP {response.status_code}")
            return None

    except Exception as e:
        print(f"ERROR downloading: {e}")
        return None


def download_cgc_monthly(year: int, month: str, output_path: Path = None) -> Optional[Path]:
    """
    Download a specific CGC monthly Excel file.

    Args:
        year: Year (e.g., 2025)
        month: Month name (e.g., 'october')
        output_path: Optional output path
    """
    month_lower = month.lower()
    url = f"{CGC_BASE_URL}/{year}/{month_lower}.xlsx"

    output_path = output_path or Path(f'data/cgc_{year}_{month_lower}.xlsx')
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Downloading CGC monthly Excel from: {url}")

    try:
        response = requests.get(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'},
            timeout=60
        )

        if response.status_code == 200:
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Downloaded to: {output_path}")
            return output_path
        else:
            print(f"ERROR: HTTP {response.status_code}")
            return None

    except Exception as e:
        print(f"ERROR downloading: {e}")
        return None


# =============================================================================
# MAIN WORKFLOW
# =============================================================================

def run_cgc_update(
    csv_path: Path = None,
    excel_path: Path = None,
    download_csv: bool = False,
    download_month: str = None,
    download_year: int = None,
    save_to_db: bool = False,
    update_excel: bool = False
) -> Dict:
    """
    Run the CGC trade data update workflow.
    """
    results = {
        'success': False,
        'records_parsed': 0,
        'records_saved': 0,
    }

    project_root = Path(__file__).parent.parent
    all_records = []

    # Download if requested
    if download_csv:
        csv_path = download_cgc_csv(project_root / 'data' / 'cgc_exports.csv')
        if not csv_path:
            print("Failed to download CGC CSV")
            return results

    if download_month and download_year:
        excel_path = download_cgc_monthly(
            download_year, download_month,
            project_root / 'data' / f'cgc_{download_year}_{download_month.lower()}.xlsx'
        )

    # Parse CSV if provided
    if csv_path:
        csv_path = Path(csv_path)
        if csv_path.exists():
            records = parse_cgc_csv(csv_path)
            all_records.extend(records)
            print(f"\nParsed {len(records)} records from CSV")
        else:
            print(f"ERROR: CSV file not found: {csv_path}")

    # Parse Excel if provided
    if excel_path:
        excel_path = Path(excel_path)
        if excel_path.exists():
            records = parse_cgc_excel(excel_path)
            all_records.extend(records)
            print(f"\nParsed {len(records)} records from Excel")
        else:
            print(f"ERROR: Excel file not found: {excel_path}")

    results['records_parsed'] = len(all_records)

    if not all_records:
        print("\nNo records to process!")
        return results

    # Show summary
    print(f"\n{'=' * 60}")
    print("DATA SUMMARY")
    print(f"{'=' * 60}")

    # Unique commodities
    commodities = set(r.get('commodity', '') for r in all_records)
    print(f"Commodities found: {commodities}")

    # Date range
    dates = [r['date'] for r in all_records if 'date' in r]
    if dates:
        print(f"Date range: {min(dates)} to {max(dates)}")

    # Top destinations
    dest_counts = defaultdict(int)
    for r in all_records:
        dest_counts[r.get('destination', '')] += 1

    print(f"\nTop 10 destinations:")
    for dest, count in sorted(dest_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"  {dest}: {count} records")

    # Save to database
    if save_to_db:
        saved = save_to_database(all_records)
        results['records_saved'] = saved

    # Update Excel
    if update_excel:
        print(f"\n{'=' * 60}")
        print("UPDATING EXCEL FILES")
        print(f"{'=' * 60}")

        excel_results = update_all_excel_sheets(all_records, project_root)

        for sheet, count in excel_results.items():
            print(f"  {sheet}: {count} month/destination combinations")

    results['success'] = True
    return results


def main():
    """Command-line entry point"""
    parser = argparse.ArgumentParser(
        description='Download and process Canadian Grain Commission trade data'
    )

    parser.add_argument(
        '--csv-file',
        help='Path to CGC exports CSV file'
    )

    parser.add_argument(
        '--excel-file',
        help='Path to CGC monthly Excel file'
    )

    parser.add_argument(
        '--download-csv',
        action='store_true',
        help='Download the historical CSV from CGC website'
    )

    parser.add_argument(
        '--download-month',
        help='Download monthly Excel file (e.g., "october")'
    )

    parser.add_argument(
        '--download-year',
        type=int,
        default=datetime.now().year,
        help='Year for monthly download (default: current year)'
    )

    parser.add_argument(
        '--save-to-db',
        action='store_true',
        help='Save data to PostgreSQL database'
    )

    parser.add_argument(
        '--update-excel',
        action='store_true',
        help='Update Excel model files'
    )

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("CANADIAN GRAIN COMMISSION TRADE DATA COLLECTOR")
    print("=" * 60)

    results = run_cgc_update(
        csv_path=args.csv_file,
        excel_path=args.excel_file,
        download_csv=args.download_csv,
        download_month=args.download_month,
        download_year=args.download_year,
        save_to_db=args.save_to_db,
        update_excel=args.update_excel
    )

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Success: {results['success']}")
    print(f"Records parsed: {results['records_parsed']}")
    print(f"Records saved to DB: {results['records_saved']}")


if __name__ == '__main__':
    main()
