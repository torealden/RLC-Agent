"""
Load FAS Export Sales XLSX files into bronze.fas_export_sales

Reads ESRQS-format export sales Excel files and inserts into the database.
Handles the ESRQS header format (7 metadata rows, then data).

Usage:
    python scripts/load_export_sales_xlsx.py data/raw/food_grains/hrw_export_sales_03072026.xlsx
    python scripts/load_export_sales_xlsx.py data/raw/food_grains/*.xlsx
    python scripts/load_export_sales_xlsx.py data/raw/food_grains/ --all
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime, date
from pathlib import Path

import pandas as pd

# Project setup
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

from src.services.database.db_config import get_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger('load_export_sales')

# Map ESRQS commodity names to our standard commodity codes
COMMODITY_MAP = {
    # Grains
    'Corn': ('corn', 4400),
    'Sorghum': ('sorghum', 4500),
    'Barley': ('barley', 410),
    'Oats': ('oats', 4030),

    # Wheat varieties
    'Wheat': ('wheat', 1000),
    'All Wheat': ('wheat_all', 1000),
    'Wheat - HRW': ('wheat_hrw', 1001),
    'Wheat - SRW': ('wheat_srw', 1002),
    'Wheat - HRS': ('wheat_hrs', 1003),
    'Wheat - White': ('wheat_white', 1004),
    'Wheat - Durum': ('wheat_durum', 1005),
    'Wheat Products': ('wheat_products', 1010),

    # Rice
    'All Rice': ('rice', 422),
    'Rice': ('rice', 422),
    'Rice - LG Rough': ('rice_lg_rough', 4221),
    'Rice- Med, Short,Other Rough': ('rice_ms_rough', 4222),
    'Rice- LG Brown': ('rice_lg_brown', 4223),

    # Oilseeds & products
    'Soybeans': ('soybeans', 2222),
    'Soybean Cake and Meal': ('soybean_meal', 813),
    'Soybean cake & meal': ('soybean_meal', 813),
    'Soybean Oil': ('soybean_oil', 4232),
    'Cottonseed': ('cottonseed', 2226),
    'Cottonseed Oil': ('cottonseed_oil', 4234),
    'Flaxseed': ('flaxseed', 2224),
    'Linseed Oil': ('linseed_oil', 4236),
    'Sunflowerseed Oil': ('sunflower_oil', 4238),

    # Cotton (bytype)
    'Cotton': ('cotton', 2631),
    'Cotton- Am Pima': ('cotton_pima', 2632),
    'Cotton- Upland 1 1/16" & over': ('cotton_upland', 2633),

    # Livestock / Meats
    'Fresh, Chilled, or Frozen Muscle Cuts of Beef': ('beef', 120),
    'Fresh, Chilled, or Frozen Muscle Cuts of Pork': ('pork', 121),
    'Cattle Hides - Whole - Excluding Wet Blues': ('cattle_hides', 211),
}


def compute_marketing_year(commodity: str, week_date: date) -> int:
    """
    Compute marketing year from the week_ending date.
    Wheat: Jun 1; Rice/Cotton: Aug 1; Livestock: calendar year;
    Everything else (corn, soy, sorghum, barley, oats, oilseeds): Sep 1.
    """
    if commodity.startswith('wheat'):
        # Wheat MY: Jun 1 - May 31
        return week_date.year if week_date.month >= 6 else week_date.year - 1
    elif commodity.startswith('rice'):
        # Rice MY: Aug 1 - Jul 31
        return week_date.year if week_date.month >= 8 else week_date.year - 1
    elif commodity.startswith('cotton'):
        # Cotton MY: Aug 1 - Jul 31
        return week_date.year if week_date.month >= 8 else week_date.year - 1
    elif commodity in ('beef', 'pork', 'cattle_hides'):
        # Livestock: calendar year
        return week_date.year
    else:
        # Corn, soy, sorghum, barley, oats, oilseeds: Sep 1 - Aug 31
        return week_date.year if week_date.month >= 9 else week_date.year - 1


def parse_esrqs_xlsx(filepath: Path) -> pd.DataFrame:
    """Parse an ESRQS export sales Excel file."""
    logger.info(f"Reading {filepath.name}...")

    df = pd.read_excel(filepath, header=None, skiprows=7)

    # The ESRQS format has 14 columns (with blanks at 0 and 3)
    if len(df.columns) == 14:
        df.columns = [
            '_blank', 'commodity_raw', 'date', '_blank2', 'country',
            'weekly_exports', 'accum_exports', 'outstanding_sales_cmy',
            'gross_sales_cmy', 'net_sales_cmy', 'total_commitments_cmy',
            'outstanding_sales_nmy', 'net_sales_nmy', 'unit',
        ]
    else:
        raise ValueError(
            f"Unexpected column count: {len(df.columns)} (expected 14). "
            f"File may not be in ESRQS format."
        )

    # Drop blank columns and rows with no commodity
    df = df.drop(columns=['_blank', '_blank2'])
    df = df.dropna(subset=['commodity_raw'])

    # Parse dates
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])

    # Map commodity
    df['commodity'] = df['commodity_raw'].map(
        lambda x: COMMODITY_MAP.get(x, (x.lower().replace(' ', '_'), None))[0]
    )
    df['commodity_code'] = df['commodity_raw'].map(
        lambda x: COMMODITY_MAP.get(x, (None, None))[1]
    )

    # Compute marketing year
    df['marketing_year'] = df.apply(
        lambda r: compute_marketing_year(r['commodity'], r['date'].date()), axis=1
    )

    # Numeric columns — coerce errors to 0
    numeric_cols = [
        'weekly_exports', 'accum_exports', 'outstanding_sales_cmy',
        'gross_sales_cmy', 'net_sales_cmy', 'total_commitments_cmy',
        'outstanding_sales_nmy', 'net_sales_nmy',
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    logger.info(
        f"  Parsed {len(df):,} rows | "
        f"Commodity: {df['commodity_raw'].iloc[0]} | "
        f"Date range: {df['date'].min().date()} to {df['date'].max().date()} | "
        f"Countries: {df['country'].nunique()}"
    )

    return df


def load_to_database(df: pd.DataFrame, run_id: uuid.UUID) -> int:
    """Insert parsed export sales into bronze.fas_export_sales."""
    now = datetime.now()
    inserted = 0

    with get_connection() as conn:
        cursor = conn.cursor()

        # Batch insert using executemany pattern
        batch_size = 1000
        rows_to_insert = []

        for _, row in df.iterrows():
            rows_to_insert.append((
                row['commodity'],
                row['commodity_code'],
                row['country'],
                None,  # country_code — ESRQS doesn't provide ISO codes
                None,  # region
                row['marketing_year'],
                row['date'].date(),
                row['weekly_exports'],
                row['accum_exports'],
                row['outstanding_sales_cmy'],
                row['gross_sales_cmy'],
                row['net_sales_cmy'],
                None,  # prev_my_accumulated (not in ESRQS CMY/NMY split)
                row.get('unit', 'Metric Tons'),
                now,
                str(run_id),
            ))

            if len(rows_to_insert) >= batch_size:
                _insert_batch(cursor, rows_to_insert)
                inserted += len(rows_to_insert)
                rows_to_insert = []

        # Insert remaining
        if rows_to_insert:
            _insert_batch(cursor, rows_to_insert)
            inserted += len(rows_to_insert)

        conn.commit()

    return inserted


def _insert_batch(cursor, rows):
    """Insert a batch of rows using execute_values for performance."""
    from psycopg2.extras import execute_values

    execute_values(
        cursor,
        """
        INSERT INTO bronze.fas_export_sales (
            commodity, commodity_code, country, country_code, region,
            marketing_year, week_ending, weekly_exports, accumulated_exports,
            outstanding_sales, gross_new_sales, net_sales,
            prev_my_accumulated, unit, collected_at, ingest_run_id
        ) VALUES %s
        ON CONFLICT DO NOTHING
        """,
        rows,
        page_size=1000,
    )


def process_file(filepath: Path) -> dict:
    """Process a single ESRQS export sales file."""
    run_id = uuid.uuid4()

    df = parse_esrqs_xlsx(filepath)

    # Disambiguate cottonseed vs cottonseed_meal — both files have "Cottonseed" as
    # the commodity name in the data, so use filename to distinguish
    if 'meal' in filepath.stem and 'cottonseed' in filepath.stem.lower():
        df.loc[df['commodity'] == 'cottonseed', 'commodity'] = 'cottonseed_meal'
        df.loc[df['commodity'] == 'cottonseed_meal', 'commodity_code'] = 817

    inserted = load_to_database(df, run_id)

    logger.info(f"  Loaded {inserted:,} rows into bronze.fas_export_sales (run_id={run_id})")

    return {
        'file': filepath.name,
        'rows_parsed': len(df),
        'rows_inserted': inserted,
        'commodity': df['commodity_raw'].iloc[0] if len(df) > 0 else None,
        'date_range': f"{df['date'].min().date()} to {df['date'].max().date()}" if len(df) > 0 else None,
        'run_id': str(run_id),
    }


def main():
    parser = argparse.ArgumentParser(description='Load ESRQS export sales XLSX into database')
    parser.add_argument('paths', nargs='+', help='XLSX file(s) or directory')
    parser.add_argument('--all', action='store_true', help='Load all .xlsx files in directory')
    parser.add_argument('--dry-run', action='store_true', help='Parse but do not insert')
    args = parser.parse_args()

    files = []
    for p in args.paths:
        path = Path(p)
        if path.is_dir():
            files.extend(sorted(path.glob('*export_sales*.xlsx')))
        elif path.is_file() and path.suffix == '.xlsx':
            files.append(path)
        else:
            logger.warning(f"Skipping: {p}")

    if not files:
        logger.error("No XLSX files found")
        sys.exit(1)

    logger.info(f"Processing {len(files)} file(s)...")
    results = []

    for f in files:
        try:
            if args.dry_run:
                df = parse_esrqs_xlsx(f)
                results.append({'file': f.name, 'rows': len(df), 'status': 'dry-run'})
            else:
                result = process_file(f)
                results.append(result)
        except Exception as e:
            logger.error(f"Failed to process {f.name}: {e}", exc_info=True)
            results.append({'file': f.name, 'error': str(e)})

    # Summary
    print(f"\n{'='*60}")
    print(f"Export Sales Load Summary")
    print(f"{'='*60}")
    total_rows = 0
    for r in results:
        rows = r.get('rows_inserted', r.get('rows', 0))
        total_rows += rows
        status = r.get('error', 'OK')
        commodity = r.get('commodity', '')
        print(f"  {r['file']:<45} {rows:>8,} rows  {status}")

    print(f"\n  Total: {total_rows:,} rows loaded")


if __name__ == '__main__':
    main()
