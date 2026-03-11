"""
EPA EMTS Monthly RIN Generation CSV Loader
===========================================
Reads downloaded EPA CSV and loads into bronze.epa_emts_monthly.

The user manually downloads the CSV from EPA's interactive table:
  https://www.epa.gov/fuels-registration-reporting-and-compliance-help/
  rins-generated-transactions

Usage:
    python emts_csv_loader.py path/to/monthly_rin_generation.csv

Once loaded, use Ctrl+E in the EMTS Data workbook to pull into Excel.
"""

import csv
import sys
import os
import logging
import argparse

import psycopg2

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'rlc_commodities',
    'user': 'postgres',
    'password': 'SoupBoss1',
}

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'emts_loader.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CSV Parsing
# ---------------------------------------------------------------------------
def parse_number(s):
    """Parse quoted number with embedded commas (e.g. '"56,301,349"')."""
    if not s:
        return None
    try:
        return int(s.replace('"', '').replace(',', '').strip())
    except (ValueError, AttributeError):
        return None


def read_epa_csv(csv_path):
    """Read EPA monthly RIN generation CSV into a list of dicts."""
    records = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                record = {
                    'rin_year': int(row['RIN Year'].replace('"', '').strip()),
                    'month': int(row['Month'].replace('"', '').strip()),
                    'producer_type': row['Producer Type'].replace('"', '').strip(),
                    'd_code': row['Fuel (D Code)'].replace('"', '').strip(),
                    'fuel_category': row['Fuel Category'].replace('"', '').strip(),
                    'rins': parse_number(row['RINs']),
                    'volume': parse_number(row['Volume (Gal.)']),
                }
                records.append(record)
            except (ValueError, KeyError) as e:
                logger.warning(f"Skipping malformed row: {e} — {row}")
    return records


# ---------------------------------------------------------------------------
# Database Loading
# ---------------------------------------------------------------------------
UPSERT_SQL = """
    INSERT INTO bronze.epa_emts_monthly
        (rin_year, month, producer_type, d_code, fuel_category,
         rins, volume, source_file, collected_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
    ON CONFLICT (rin_year, month, producer_type, d_code, fuel_category)
    DO UPDATE SET
        rins = EXCLUDED.rins,
        volume = EXCLUDED.volume,
        source_file = EXCLUDED.source_file,
        collected_at = NOW()
    RETURNING (xmax = 0) AS is_insert
"""


def save_to_bronze(records, source_file):
    """Upsert records into bronze.epa_emts_monthly."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    inserted = 0
    updated = 0
    errors = 0

    for rec in records:
        try:
            cursor.execute(UPSERT_SQL, (
                rec['rin_year'],
                rec['month'],
                rec['producer_type'],
                rec['d_code'],
                rec['fuel_category'],
                rec['rins'],
                rec['volume'],
                source_file,
            ))
            result = cursor.fetchone()
            if result and result[0]:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            logger.error(f"Error saving record {rec}: {e}")
            errors += 1
            conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()
    return inserted, updated, errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description='Load EPA EMTS monthly RIN generation CSV into database')
    parser.add_argument('csv_path', help='Path to EPA CSV file')
    args = parser.parse_args()

    csv_path = args.csv_path
    if not os.path.isfile(csv_path):
        logger.error(f"File not found: {csv_path}")
        sys.exit(1)

    source_file = os.path.basename(csv_path)

    logger.info(f"Reading EPA CSV: {csv_path}")
    records = read_epa_csv(csv_path)
    logger.info(f"Loaded {len(records)} records from CSV")

    if not records:
        logger.error("No records found in CSV")
        sys.exit(1)

    # Show data range
    years = sorted(set(r['rin_year'] for r in records))
    months = sorted(set((r['rin_year'], r['month']) for r in records))
    d_codes = sorted(set(r['d_code'] for r in records))
    logger.info(f"Data range: {months[0][0]}-{months[0][1]:02d} to "
                f"{months[-1][0]}-{months[-1][1]:02d}")
    logger.info(f"D-codes: {', '.join(d_codes)}")
    logger.info(f"Unique combinations: {len(set((r['d_code'], r['fuel_category'], r['producer_type']) for r in records))}")

    logger.info("Saving to bronze.epa_emts_monthly...")
    inserted, updated, errors = save_to_bronze(records, source_file)

    logger.info(f"Done: {inserted} inserted, {updated} updated, {errors} errors")
    print(f"\nLoaded {len(records)} records into bronze.epa_emts_monthly")
    print(f"  Inserted: {inserted}")
    print(f"  Updated:  {updated}")
    print(f"  Errors:   {errors}")

    if errors > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
