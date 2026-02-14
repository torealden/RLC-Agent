#!/usr/bin/env python3
"""
Fast Balance Sheet Extractor using Pandas

Uses pandas for bulk reading which is 10-50x faster than cell-by-cell openpyxl.
Designed for RLC commodity balance sheet Excel files.

Usage:
    python deployment/fast_extractor.py --extract "path/to/file.xlsx"
    python deployment/fast_extractor.py --extract-all
    python deployment/fast_extractor.py --status
"""

import argparse
import sqlite3
import logging
import re
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
MODELS_DIR = PROJECT_ROOT / "Models"
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "rlc_commodities.db"

# Marketing year patterns
MARKETING_YEAR_PATTERN_4DIGIT = re.compile(r'^(\d{4})/(\d{2})\s*$')
MARKETING_YEAR_PATTERN_2DIGIT = re.compile(r'^(\d{2})/(\d{2})\s*$')

# Historical data range
MIN_YEAR = 1965
MAX_YEAR = 2023

# Target balance sheet files
TARGET_FILES = [
    "World Sunflower Balance Sheets",
    "World Rapeseed Balance Sheets",
    "World Lauric Oils Balance Sheets",
    "World Peanut Balance Sheets",
    "World Vegetable Oil Balance Sheets",
    "US Oilseed Balance Sheets",
]


def is_marketing_year(value: Any) -> Optional[Tuple[int, int]]:
    """Check if value is a marketing year. Returns (start_year, end_year) or None."""
    if pd.isna(value):
        return None

    val_str = str(value).strip()

    # Try 4-digit format: "2020/21"
    match = MARKETING_YEAR_PATTERN_4DIGIT.match(val_str)
    if match:
        start_year = int(match.group(1))
        end_year_short = int(match.group(2))
        end_year = (start_year // 100) * 100 + end_year_short
        if end_year_short < (start_year % 100):
            end_year += 100
        return (start_year, end_year)

    # Try 2-digit format: "64/65"
    match = MARKETING_YEAR_PATTERN_2DIGIT.match(val_str)
    if match:
        start_short = int(match.group(1))
        end_short = int(match.group(2))

        if start_short >= 50:
            start_year = 1900 + start_short
        else:
            start_year = 2000 + start_short

        if end_short >= 50:
            end_year = 1900 + end_short
        else:
            end_year = 2000 + end_short

        if end_year < start_year:
            end_year += 100

        return (start_year, end_year)

    return None


def normalize_marketing_year(value: Any) -> Optional[str]:
    """Normalize a marketing year to standard format 'YYYY/YY'."""
    years = is_marketing_year(value)
    if years:
        start_year, end_year = years
        return f"{start_year}/{end_year % 100:02d}"
    return None


def is_historical_year(marketing_year: str) -> bool:
    """Check if marketing year is within historical range."""
    years = is_marketing_year(marketing_year)
    if years:
        return MIN_YEAR <= years[0] <= MAX_YEAR
    return False


def detect_commodity_from_filename(filename: str) -> str:
    """Detect commodity from filename."""
    filename_lower = filename.lower()

    commodities = {
        'sunflower': ['sunflower', 'sunseed'],
        'rapeseed': ['rapeseed', 'canola'],
        'soybeans': ['soybean', 'soy'],
        'peanuts': ['peanut', 'groundnut'],
        'lauric_oils': ['lauric', 'palm', 'coconut'],
        'vegetable_oils': ['vegetable oil'],
        'corn': ['corn'],
        'wheat': ['wheat'],
    }

    for commodity, patterns in commodities.items():
        if any(p in filename_lower for p in patterns):
            return commodity

    return 'unknown'


def detect_country_from_sheet(sheet_name: str) -> str:
    """Detect country/region from sheet name."""
    name_lower = sheet_name.lower()

    countries = {
        'US': ['us ', 'u.s.', 'united states', 'usa'],
        'Brazil': ['brazil', 'brasil'],
        'Argentina': ['argentina', 'arg'],
        'China': ['china'],
        'EU': ['eu ', 'europe', 'european'],
        'India': ['india'],
        'Canada': ['canada'],
        'Australia': ['australia'],
        'World': ['world', 'global'],
        'Indonesia': ['indonesia'],
        'Malaysia': ['malaysia'],
        'Ukraine': ['ukraine'],
        'Russia': ['russia'],
        'Paraguay': ['paraguay'],
        'Turkey': ['turkey'],
        'Kazakhstan': ['kazakh'],
        'Moldova': ['moldova'],
        'Romania': ['romania'],
        'Bulgaria': ['bulgaria'],
        'France': ['france'],
        'Germany': ['germany'],
        'Poland': ['poland'],
        'Hungary': ['hungary'],
    }

    for country, patterns in countries.items():
        if any(p in name_lower for p in patterns):
            return country

    return 'Unknown'


class FastExtractor:
    """Fast pandas-based balance sheet extractor."""

    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def init_database(self):
        """Initialize database with balance sheet tables."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS commodity_balance_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file TEXT NOT NULL,
                sheet_name TEXT NOT NULL,
                commodity TEXT NOT NULL,
                country TEXT,
                section TEXT,
                metric TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                value REAL,
                unit TEXT,
                extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_file, sheet_name, section, metric, marketing_year)
            );

            CREATE INDEX IF NOT EXISTS idx_cbs_commodity ON commodity_balance_sheets(commodity);
            CREATE INDEX IF NOT EXISTS idx_cbs_country ON commodity_balance_sheets(country);
            CREATE INDEX IF NOT EXISTS idx_cbs_year ON commodity_balance_sheets(marketing_year);
            CREATE INDEX IF NOT EXISTS idx_cbs_section ON commodity_balance_sheets(section);
        """)

        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")

    def find_year_columns(self, df: pd.DataFrame, max_rows: int = 10) -> Dict[str, int]:
        """Find columns that contain marketing years in the header rows."""
        year_columns = {}

        # Check first max_rows rows for year headers
        for row_idx in range(min(max_rows, len(df))):
            for col_idx, value in enumerate(df.iloc[row_idx]):
                normalized = normalize_marketing_year(value)
                if normalized and is_historical_year(normalized):
                    if normalized not in year_columns:
                        year_columns[normalized] = col_idx

        return year_columns

    def extract_sheet_pandas(self, df: pd.DataFrame, sheet_name: str,
                             source_file: str, commodity: str) -> List[Dict]:
        """Extract data from a sheet using pandas."""
        extracted = []

        # Find year columns
        year_columns = self.find_year_columns(df)

        if not year_columns:
            logger.warning(f"  No year columns found in {sheet_name}")
            return extracted

        logger.info(f"  Found {len(year_columns)} year columns ({min(year_columns.keys())} to {max(year_columns.keys())})")

        country = detect_country_from_sheet(sheet_name)
        current_section = "General"

        # Process each row
        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            col_a = row.iloc[0] if len(row) > 0 else None

            if pd.isna(col_a):
                continue

            col_a_str = str(col_a).strip()

            # Skip if it's a year value in column A
            if is_marketing_year(col_a_str):
                continue

            # Check for section header (ALL CAPS, longer than 3 chars)
            if col_a_str.isupper() and len(col_a_str) > 3:
                # Check next row to confirm it's a section header
                if row_idx + 1 < len(df):
                    next_val = df.iloc[row_idx + 1, 0] if len(df.iloc[row_idx + 1]) > 0 else None
                    if pd.isna(next_val) or str(next_val).strip().lower() in [
                        'jan', 'feb', 'mar', 'apr', 'may', 'jun',
                        'jul', 'aug', 'sep', 'oct', 'nov', 'dec'
                    ]:
                        current_section = col_a_str
                        continue

            # Extract values for each year
            for year, col_idx in year_columns.items():
                if col_idx < len(row):
                    cell_value = row.iloc[col_idx]

                    if pd.notna(cell_value) and isinstance(cell_value, (int, float)):
                        extracted.append({
                            'source_file': source_file,
                            'sheet_name': sheet_name,
                            'commodity': commodity,
                            'country': country,
                            'section': current_section,
                            'metric': col_a_str,
                            'marketing_year': year,
                            'value': float(cell_value),
                            'unit': ''
                        })

        return extracted

    def extract_file(self, filepath: Path, complex_only: bool = True) -> int:
        """Extract all data from an Excel file using pandas."""
        logger.info(f"\n{'='*60}")
        logger.info(f"Extracting: {filepath.name}")
        logger.info(f"{'='*60}")

        start_time = time.time()

        # Get sheet names first
        xl = pd.ExcelFile(filepath, engine='openpyxl')
        sheet_names = xl.sheet_names

        commodity = detect_commodity_from_filename(filepath.name)
        all_extracted = []
        sheets_processed = 0

        for sheet_name in sheet_names:
            # Filter to Complex sheets if requested
            if complex_only and 'complex' not in sheet_name.lower():
                logger.info(f"  Skipping: {sheet_name} (no 'Complex' in name)")
                continue

            logger.info(f"\n  Processing: {sheet_name}")

            try:
                # Read entire sheet at once (this is the speed improvement)
                df = pd.read_excel(filepath, sheet_name=sheet_name,
                                   header=None, engine='openpyxl')

                extracted = self.extract_sheet_pandas(df, sheet_name,
                                                       filepath.name, commodity)
                all_extracted.extend(extracted)
                sheets_processed += 1

                logger.info(f"    -> Extracted {len(extracted)} data points")

            except Exception as e:
                logger.warning(f"    Error reading {sheet_name}: {e}")
                continue

        xl.close()

        # Save to database
        if all_extracted:
            self.save_to_database(all_extracted)

        elapsed = time.time() - start_time
        logger.info(f"\n  Completed in {elapsed:.1f}s")
        logger.info(f"  Sheets processed: {sheets_processed}")
        logger.info(f"  Total data points: {len(all_extracted)}")

        return len(all_extracted)

    def save_to_database(self, data: List[Dict]):
        """Save extracted data to database using batch insert."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        # Use executemany for batch insert
        cursor.executemany("""
            INSERT OR REPLACE INTO commodity_balance_sheets
            (source_file, sheet_name, commodity, country, section,
             metric, marketing_year, value, unit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (d['source_file'], d['sheet_name'], d['commodity'], d['country'],
             d['section'], d['metric'], d['marketing_year'], d['value'], d['unit'])
            for d in data
        ])

        conn.commit()
        conn.close()

        logger.info(f"  Saved {len(data)} rows to database")

    def extract_target_files(self, files: List[str] = None) -> Dict[str, int]:
        """Extract from specified or all target files."""
        results = {}
        targets = files if files else TARGET_FILES

        oilseeds_dir = MODELS_DIR / "Oilseeds"

        for target in targets:
            matching = list(oilseeds_dir.glob(f"*{target}*.xlsx"))

            if not matching:
                logger.warning(f"No file found matching: {target}")
                continue

            filepath = matching[0]
            try:
                rows = self.extract_file(filepath, complex_only=True)
                results[filepath.name] = rows
            except Exception as e:
                logger.error(f"Failed to extract {filepath.name}: {e}")
                results[filepath.name] = -1

        return results

    def get_status(self) -> Dict[str, Any]:
        """Get database status."""
        if not self.db_path.exists():
            return {'database_exists': False}

        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()

        status = {
            'database_exists': True,
            'database_size_mb': self.db_path.stat().st_size / (1024*1024),
        }

        try:
            cursor.execute("SELECT COUNT(*) FROM commodity_balance_sheets")
            status['total_data_points'] = cursor.fetchone()[0]

            cursor.execute("""
                SELECT commodity, COUNT(*) FROM commodity_balance_sheets
                GROUP BY commodity ORDER BY COUNT(*) DESC
            """)
            status['by_commodity'] = {r[0]: r[1] for r in cursor.fetchall()}

            cursor.execute("""
                SELECT country, COUNT(*) FROM commodity_balance_sheets
                GROUP BY country ORDER BY COUNT(*) DESC LIMIT 15
            """)
            status['by_country'] = {r[0]: r[1] for r in cursor.fetchall()}

            cursor.execute("""
                SELECT MIN(marketing_year), MAX(marketing_year)
                FROM commodity_balance_sheets
            """)
            row = cursor.fetchone()
            status['year_range'] = f"{row[0]} to {row[1]}" if row[0] else "No data"

            cursor.execute("SELECT DISTINCT source_file FROM commodity_balance_sheets")
            status['source_files'] = [r[0] for r in cursor.fetchall()]

        except Exception as e:
            status['error'] = str(e)

        conn.close()
        return status


def main():
    parser = argparse.ArgumentParser(description='Fast Balance Sheet Extractor')
    parser.add_argument('--extract', type=str, help='Extract from specific file')
    parser.add_argument('--extract-all', action='store_true', help='Extract from all target files')
    parser.add_argument('--status', action='store_true', help='Show database status')
    parser.add_argument('--all-sheets', action='store_true', help='Process all sheets, not just Complex')

    args = parser.parse_args()

    extractor = FastExtractor()

    if args.status:
        status = extractor.get_status()
        print("\n" + "="*60)
        print("  DATABASE STATUS")
        print("="*60)

        if not status['database_exists']:
            print("\n  Database not found. Run extraction first.")
        else:
            print(f"\n  Database: {DB_PATH}")
            print(f"  Size: {status['database_size_mb']:.2f} MB")
            print(f"  Total data points: {status.get('total_data_points', 0):,}")
            print(f"  Year range: {status.get('year_range', 'N/A')}")

            print("\n  By Commodity:")
            for comm, count in status.get('by_commodity', {}).items():
                print(f"    {comm}: {count:,}")

            print("\n  By Country (top 15):")
            for country, count in status.get('by_country', {}).items():
                print(f"    {country}: {count:,}")

            print("\n  Source Files:")
            for f in status.get('source_files', []):
                print(f"    - {f}")
        print()
        return

    if args.extract:
        filepath = Path(args.extract)
        if not filepath.exists():
            filepath = PROJECT_ROOT / args.extract
        if not filepath.exists():
            filepath = MODELS_DIR / "Oilseeds" / args.extract

        if not filepath.exists():
            print(f"File not found: {args.extract}")
            return

        extractor.init_database()
        rows = extractor.extract_file(filepath, complex_only=not args.all_sheets)
        print(f"\nExtracted {rows:,} data points to {DB_PATH}")
        return

    if args.extract_all:
        extractor.init_database()
        results = extractor.extract_target_files()

        print("\n" + "="*60)
        print("  EXTRACTION COMPLETE")
        print("="*60)

        total = sum(r for r in results.values() if r > 0)
        for filename, rows in results.items():
            status = f"{rows:,} rows" if rows >= 0 else "FAILED"
            print(f"  {filename}: {status}")

        print(f"\n  Total: {total:,} data points")
        print(f"  Database: {DB_PATH}")
        print()
        return

    parser.print_help()


if __name__ == '__main__':
    main()
