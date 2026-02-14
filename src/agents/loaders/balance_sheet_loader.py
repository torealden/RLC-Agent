"""
Balance Sheet CSV Loader

Load user S&D estimates from CSV files in domain_knowledge/balance_sheets/
into the silver.user_sd_estimate table for variance tracking against
realized monthly data.

Usage:
    python balance_sheet_loader.py                    # Load all CSVs
    python balance_sheet_loader.py --file path.csv   # Load specific file
    python balance_sheet_loader.py --list            # List available CSVs
"""

import os
import sys
import logging
from pathlib import Path
from datetime import date
from typing import Dict, List, Optional
import argparse

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import psycopg2
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Path to balance sheet CSVs
BALANCE_SHEET_DIR = PROJECT_ROOT / 'domain_knowledge' / 'balance_sheets'

# Expected CSV column mappings (CSV column -> DB column)
COLUMN_MAPPING = {
    # Required columns
    'commodity': 'commodity',
    'country': 'country',
    'marketing_year': 'marketing_year',

    # Supply side
    'area_planted': 'area_planted',
    'area_harvested': 'area_harvested',
    'yield': 'yield',
    'beginning_stocks': 'beginning_stocks',
    'production': 'production',
    'imports': 'imports',
    'total_supply': 'total_supply',

    # Demand side
    'crush': 'crush',
    'feed_residual': 'feed_residual',
    'fsi': 'fsi',
    'ethanol': 'ethanol',
    'domestic_use': 'domestic_use',
    'exports': 'exports',
    'total_use': 'total_use',

    # Ending
    'ending_stocks': 'ending_stocks',
    'stocks_use_ratio': 'stocks_use_ratio',

    # Metadata
    'unit': 'unit',
    'notes': 'notes',
}


def get_db_connection():
    """Get database connection."""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', '5432'),
        dbname=os.environ.get('DB_NAME', 'rlc_commodities'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', '')
    )


def load_csv_file(csv_path: Path, conn=None) -> Dict[str, int]:
    """
    Load a single CSV file into silver.user_sd_estimate.

    Args:
        csv_path: Path to CSV file
        conn: Optional database connection

    Returns:
        Dict with inserted/updated/skipped counts
    """
    if not PANDAS_AVAILABLE:
        logger.error("pandas is required for CSV loading")
        return {'inserted': 0, 'updated': 0, 'skipped': 0, 'error': 'pandas not available'}

    if not csv_path.exists():
        logger.error(f"File not found: {csv_path}")
        return {'inserted': 0, 'updated': 0, 'skipped': 0, 'error': 'File not found'}

    logger.info(f"Loading {csv_path}...")

    should_close = False
    if conn is None:
        conn = get_db_connection()
        should_close = True

    counts = {'inserted': 0, 'updated': 0, 'skipped': 0}

    try:
        # Read CSV
        df = pd.read_csv(csv_path)

        # Normalize column names (lowercase, strip whitespace)
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')

        # Validate required columns
        required = ['commodity', 'marketing_year']
        missing = [col for col in required if col not in df.columns]
        if missing:
            logger.error(f"Missing required columns: {missing}")
            return {'inserted': 0, 'updated': 0, 'skipped': 0,
                    'error': f'Missing columns: {missing}'}

        # Default country if not specified
        if 'country' not in df.columns:
            df['country'] = 'United States'

        # Default unit if not specified
        if 'unit' not in df.columns:
            df['unit'] = 'mil bu'

        cur = conn.cursor()

        # Mark previous estimates for these commodities as not current
        commodities = tuple(df['commodity'].unique().tolist())
        if len(commodities) == 1:
            commodities = (commodities[0],)  # Handle single item tuple

        cur.execute("""
            UPDATE silver.user_sd_estimate
            SET is_current = FALSE
            WHERE commodity IN %s AND is_current = TRUE
        """, (commodities,))

        # Insert each row
        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO silver.user_sd_estimate (
                        commodity, country, marketing_year, estimate_date,
                        area_planted, area_harvested, yield,
                        beginning_stocks, production, imports, total_supply,
                        crush, feed_residual, fsi, ethanol, domestic_use,
                        exports, total_use, ending_stocks, stocks_use_ratio,
                        unit, source_file, notes, is_current
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE
                    )
                    ON CONFLICT (commodity, country, marketing_year, estimate_date)
                    DO UPDATE SET
                        area_planted = EXCLUDED.area_planted,
                        area_harvested = EXCLUDED.area_harvested,
                        yield = EXCLUDED.yield,
                        beginning_stocks = EXCLUDED.beginning_stocks,
                        production = EXCLUDED.production,
                        imports = EXCLUDED.imports,
                        total_supply = EXCLUDED.total_supply,
                        crush = EXCLUDED.crush,
                        feed_residual = EXCLUDED.feed_residual,
                        fsi = EXCLUDED.fsi,
                        ethanol = EXCLUDED.ethanol,
                        domestic_use = EXCLUDED.domestic_use,
                        exports = EXCLUDED.exports,
                        total_use = EXCLUDED.total_use,
                        ending_stocks = EXCLUDED.ending_stocks,
                        stocks_use_ratio = EXCLUDED.stocks_use_ratio,
                        is_current = TRUE,
                        updated_at = NOW()
                """, (
                    row.get('commodity'),
                    row.get('country', 'United States'),
                    int(row.get('marketing_year')),
                    date.today(),
                    row.get('area_planted'),
                    row.get('area_harvested'),
                    row.get('yield'),
                    row.get('beginning_stocks'),
                    row.get('production'),
                    row.get('imports'),
                    row.get('total_supply'),
                    row.get('crush'),
                    row.get('feed_residual'),
                    row.get('fsi'),
                    row.get('ethanol'),
                    row.get('domestic_use'),
                    row.get('exports'),
                    row.get('total_use'),
                    row.get('ending_stocks'),
                    row.get('stocks_use_ratio'),
                    row.get('unit', 'mil bu'),
                    str(csv_path),
                    row.get('notes'),
                ))
                counts['inserted'] += 1

            except Exception as e:
                logger.warning(f"Failed to insert row: {e}")
                counts['skipped'] += 1

        conn.commit()
        logger.info(f"Loaded {counts['inserted']} estimates from {csv_path.name}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading {csv_path}: {e}")
        counts['error'] = str(e)

    finally:
        if should_close:
            conn.close()

    return counts


def load_all_balance_sheets() -> Dict[str, Dict]:
    """
    Scan balance_sheets directory and load all CSV files.

    Returns:
        Dict of filename -> load results
    """
    results = {}

    if not BALANCE_SHEET_DIR.exists():
        logger.error(f"Balance sheet directory not found: {BALANCE_SHEET_DIR}")
        return results

    conn = get_db_connection()

    # Find all CSV files
    csv_files = list(BALANCE_SHEET_DIR.rglob('*.csv'))

    if not csv_files:
        logger.warning("No CSV files found in balance_sheets directory")
        return results

    logger.info(f"Found {len(csv_files)} CSV files")

    for csv_path in csv_files:
        relative_path = csv_path.relative_to(BALANCE_SHEET_DIR)
        results[str(relative_path)] = load_csv_file(csv_path, conn)

    conn.close()
    return results


def list_csv_files() -> List[str]:
    """List all CSV files in balance_sheets directory."""
    if not BALANCE_SHEET_DIR.exists():
        return []

    csv_files = list(BALANCE_SHEET_DIR.rglob('*.csv'))
    return [str(f.relative_to(BALANCE_SHEET_DIR)) for f in csv_files]


def main():
    parser = argparse.ArgumentParser(description='Load user S&D estimates from CSV files')

    parser.add_argument(
        '--file', '-f',
        help='Specific CSV file to load'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List available CSV files'
    )

    parser.add_argument(
        '--all', '-a',
        action='store_true',
        help='Load all CSV files'
    )

    args = parser.parse_args()

    if args.list:
        files = list_csv_files()
        if files:
            print("Available CSV files:")
            for f in files:
                print(f"  {f}")
        else:
            print("No CSV files found in domain_knowledge/balance_sheets/")
        return

    if args.file:
        path = Path(args.file)
        if not path.is_absolute():
            path = BALANCE_SHEET_DIR / path
        result = load_csv_file(path)
        print(f"Result: {result}")
        return

    if args.all:
        results = load_all_balance_sheets()
        total = sum(r.get('inserted', 0) for r in results.values())
        print(f"\nLoaded {total} estimates from {len(results)} files")
        return

    # Default: show help
    parser.print_help()


if __name__ == '__main__':
    main()
