"""
FGIS Grain Export Inspections — Historical CSV Loader

Loads CY{year}.csv files (1990-present) from USDA FGIS into
bronze.fgis_inspections_history. Each row is an individual inspection
certificate.

Data source:
  C:/dev/RLC-Agent/data/raw/cross_commodity/CY{year}.csv
  ~280MB total, ~900k rows across 36 years

Usage:
    # Load all years
    python scripts/load_fgis_inspections.py

    # Load specific years
    python scripts/load_fgis_inspections.py --years 2024 2025 2026

    # Dry run (parse only, no DB writes)
    python scripts/load_fgis_inspections.py --dry-run --years 2026

    # Test parse one file
    python scripts/load_fgis_inspections.py --test --years 2026
"""

import csv
import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.services.database.db_config import get_connection as get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATA_DIR = PROJECT_ROOT / "data" / "raw" / "cross_commodity"

# Column name mapping — CSV headers have trailing spaces in some files
# Map stripped header name -> our internal name
COLUMN_MAP = {
    'Cert Date': 'cert_date',
    'Serial No.': 'serial_no',
    'Type Serv': 'type_service',
    'Type Shipm': 'type_shipment',
    'Type Carrier': 'type_carrier',
    'Grain': 'grain',
    'Class': 'grain_class',
    'SubClass': 'grain_subclass',
    'Grade': 'grade',
    'Destination': 'destination',
    'Port': 'port_region',
    'Field Office': 'port_name',
    'AMS Reg': 'ams_region',
    'FGIS Reg': 'fgis_region',
    'City': 'city',
    'State': 'state',
    'MKT YR': 'marketing_year',
    'Metric Ton': 'metric_tons',
    '1000 Bushels': 'bushels_1000',
    'Pounds': 'pounds',
    'TW': 'test_weight',
    'M AVG': 'moisture_avg',
    'DKG AVG': 'damaged_kernels',
    'FM': 'foreign_material',
    'Carrier Name': 'carrier_name',
}

# Bushels-per-metric-ton conversion factors (for calculating 1000 Bu from MT)
BUSHELS_PER_MT = {
    'CORN': 39.368,        # 1 MT = 39.368 bu (56 lb/bu)
    'SOYBEANS': 36.744,    # 1 MT = 36.744 bu (60 lb/bu)
    'WHEAT': 36.744,       # 60 lb/bu
    'SORGHUM': 39.368,     # 56 lb/bu
    'BARLEY': 45.930,      # 48 lb/bu
    'OATS': 68.894,        # 32 lb/bu
    'RYE': 39.368,         # 56 lb/bu
    'FLAXSEED': 39.368,    # 56 lb/bu
    'CANOLA': 44.092,      # 50 lb/bu (approx)
    'SUNFLOWER SEED': 36.744,
    'RICE': 22.046,        # 100 lb/cwt -> ~22 cwt/MT, but rice often by cwt
}

# Batch size for DB inserts
BATCH_SIZE = 5000


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def parse_date(raw: str) -> Optional[datetime]:
    """Parse YYYYMMDD date string."""
    raw = raw.strip().strip('"')
    if not raw or len(raw) != 8:
        return None
    try:
        return datetime.strptime(raw, '%Y%m%d').date()
    except ValueError:
        return None


def parse_numeric(raw: str) -> Optional[float]:
    """Parse a numeric field, returning None for empty/invalid."""
    raw = raw.strip().strip('"')
    if not raw or raw == '0' or raw == '0.0':
        # Keep actual zero values for metric_tons/bushels but return None
        # for truly empty fields. We return 0.0 for "0" strings.
        try:
            v = float(raw)
            return v if v != 0 else 0.0
        except ValueError:
            return None
    try:
        return float(raw.replace(',', ''))
    except ValueError:
        return None


def parse_numeric_nullable(raw: str) -> Optional[float]:
    """Parse numeric, returning None for empty or zero."""
    raw = raw.strip().strip('"')
    if not raw:
        return None
    try:
        v = float(raw.replace(',', ''))
        return v if v != 0 else None
    except ValueError:
        return None


def build_header_map(headers: List[str]) -> Dict[str, int]:
    """Build mapping from our internal names to column indices.

    CSV headers may have trailing spaces or be quoted. We strip both.
    """
    header_map = {}
    for idx, raw_header in enumerate(headers):
        clean = raw_header.strip().strip('"').strip()
        if clean in COLUMN_MAP:
            header_map[COLUMN_MAP[clean]] = idx
    return header_map


def parse_row(row: List[str], hmap: Dict[str, int], calendar_year: int) -> Optional[Dict]:
    """Parse a single CSV row into a record dict."""

    def get(field: str) -> str:
        idx = hmap.get(field)
        if idx is None or idx >= len(row):
            return ''
        return row[idx].strip().strip('"').strip()

    # Required fields
    cert_date = parse_date(get('cert_date'))
    serial_no = get('serial_no')
    grain = get('grain').upper()
    destination = get('destination')

    if not cert_date or not serial_no or not grain or not destination:
        return None

    # Volumes
    mt_raw = get('metric_tons')
    bu_raw = get('bushels_1000')
    lbs_raw = get('pounds')

    metric_tons = parse_numeric(mt_raw)
    bushels_1000 = parse_numeric_nullable(bu_raw)
    pounds = parse_numeric(lbs_raw)

    # Calculate bushels from MT if bushels is missing but MT is present
    if (bushels_1000 is None or bushels_1000 == 0) and metric_tons and metric_tons > 0:
        factor = BUSHELS_PER_MT.get(grain)
        if factor:
            bushels_1000 = round(metric_tons * factor / 1000.0, 3)

    # Calculate MT from pounds if MT is missing
    if (metric_tons is None or metric_tons == 0) and pounds and pounds > 0:
        metric_tons = round(pounds / 2204.623, 2)

    return {
        'cert_date': cert_date,
        'serial_no': serial_no,
        'calendar_year': calendar_year,
        'type_service': get('type_service') or None,
        'type_shipment': get('type_shipment') or None,
        'type_carrier': get('type_carrier') or None,
        'grain': grain,
        'grain_class': get('grain_class') or None,
        'grain_subclass': get('grain_subclass') or None,
        'grade': get('grade') or None,
        'destination': destination,
        'port_region': get('port_region') or None,
        'port_name': get('port_name') or None,
        'ams_region': get('ams_region') or None,
        'fgis_region': get('fgis_region') or None,
        'city': get('city') or None,
        'state': get('state') or None,
        'marketing_year': get('marketing_year') or None,
        'metric_tons': metric_tons,
        'bushels_1000': bushels_1000,
        'pounds': pounds,
        'test_weight': parse_numeric_nullable(get('test_weight')),
        'moisture_avg': parse_numeric_nullable(get('moisture_avg')),
        'damaged_kernels': parse_numeric_nullable(get('damaged_kernels')),
        'foreign_material': parse_numeric_nullable(get('foreign_material')),
        'carrier_name': get('carrier_name') or None,
    }


def read_csv_file(filepath: Path, calendar_year: int) -> Tuple[List[Dict], int, int]:
    """Read and parse a CY{year}.csv file.

    Returns: (records, total_rows, skipped_rows)
    """
    records = []
    total = 0
    skipped = 0

    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        # Detect quoting style: old files quote everything, new files don't
        first_line = f.readline()
        f.seek(0)

        # Use csv.reader which handles both quoted and unquoted
        reader = csv.reader(f)
        headers = next(reader)
        hmap = build_header_map(headers)

        if not hmap:
            logger.error(f"  Could not map any headers in {filepath.name}")
            return [], 0, 0

        mapped_fields = list(hmap.keys())
        logger.debug(f"  Mapped {len(mapped_fields)} fields: {mapped_fields[:8]}...")

        for row in reader:
            total += 1
            rec = parse_row(row, hmap, calendar_year)
            if rec:
                records.append(rec)
            else:
                skipped += 1

    return records, total, skipped


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

UPSERT_SQL = """
    INSERT INTO bronze.fgis_inspections_history (
        cert_date, serial_no, calendar_year,
        type_service, type_shipment, type_carrier,
        grain, grain_class, grain_subclass, grade,
        destination, port_region, port_name, ams_region, fgis_region,
        city, state, marketing_year,
        metric_tons, bushels_1000, pounds,
        test_weight, moisture_avg, damaged_kernels, foreign_material,
        carrier_name, collected_at
    ) VALUES (
        %(cert_date)s, %(serial_no)s, %(calendar_year)s,
        %(type_service)s, %(type_shipment)s, %(type_carrier)s,
        %(grain)s, %(grain_class)s, %(grain_subclass)s, %(grade)s,
        %(destination)s, %(port_region)s, %(port_name)s, %(ams_region)s, %(fgis_region)s,
        %(city)s, %(state)s, %(marketing_year)s,
        %(metric_tons)s, %(bushels_1000)s, %(pounds)s,
        %(test_weight)s, %(moisture_avg)s, %(damaged_kernels)s, %(foreign_material)s,
        %(carrier_name)s, NOW()
    )
    ON CONFLICT (calendar_year, serial_no) DO UPDATE SET
        cert_date = EXCLUDED.cert_date,
        type_service = EXCLUDED.type_service,
        type_shipment = EXCLUDED.type_shipment,
        type_carrier = EXCLUDED.type_carrier,
        grain = EXCLUDED.grain,
        grain_class = EXCLUDED.grain_class,
        grain_subclass = EXCLUDED.grain_subclass,
        grade = EXCLUDED.grade,
        destination = EXCLUDED.destination,
        port_region = EXCLUDED.port_region,
        port_name = EXCLUDED.port_name,
        ams_region = EXCLUDED.ams_region,
        fgis_region = EXCLUDED.fgis_region,
        city = EXCLUDED.city,
        state = EXCLUDED.state,
        marketing_year = EXCLUDED.marketing_year,
        metric_tons = EXCLUDED.metric_tons,
        bushels_1000 = EXCLUDED.bushels_1000,
        pounds = EXCLUDED.pounds,
        test_weight = EXCLUDED.test_weight,
        moisture_avg = EXCLUDED.moisture_avg,
        damaged_kernels = EXCLUDED.damaged_kernels,
        foreign_material = EXCLUDED.foreign_material,
        carrier_name = EXCLUDED.carrier_name,
        collected_at = NOW()
"""


def insert_batch(conn, records: List[Dict]) -> int:
    """Insert a batch of records using executemany-style loop."""
    cur = conn.cursor()
    count = 0
    for rec in records:
        try:
            cur.execute(UPSERT_SQL, rec)
            count += 1
        except Exception as e:
            logger.warning(f"  Row error (serial={rec.get('serial_no')}): {e}")
            conn.rollback()
            # Re-establish state for next row
            continue
    conn.commit()
    return count


def load_year(filepath: Path, calendar_year: int, dry_run: bool = False) -> Tuple[int, int, int]:
    """Load one CY{year}.csv file into bronze.

    Returns: (loaded, total_rows, skipped)
    """
    logger.info(f"Reading {filepath.name}...")
    records, total, skipped = read_csv_file(filepath, calendar_year)
    logger.info(f"  Parsed {len(records):,} records ({skipped:,} skipped of {total:,} rows)")

    if dry_run or not records:
        return len(records), total, skipped

    loaded = 0
    with get_db_connection() as conn:
        # Process in batches
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            n = insert_batch(conn, batch)
            loaded += n
            if (i + BATCH_SIZE) % 10000 == 0 or i + BATCH_SIZE >= len(records):
                logger.info(f"  Loaded {loaded:,} / {len(records):,}")

    logger.info(f"  Done: {loaded:,} records loaded for CY{calendar_year}")
    return loaded, total, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def get_available_years() -> List[int]:
    """Find all CY{year}.csv files and return sorted list of years."""
    years = []
    for f in DATA_DIR.glob("CY*.csv"):
        try:
            yr = int(f.stem.replace("CY", ""))
            years.append(yr)
        except ValueError:
            continue
    return sorted(years)


def main():
    parser = argparse.ArgumentParser(
        description='Load FGIS inspection certificates from CY*.csv into bronze'
    )
    parser.add_argument(
        '--years', nargs='+', type=int,
        help='Specific years to load (default: all available)'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Parse files but do not write to database'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Parse first file only, print 5 sample records, no DB writes'
    )
    args = parser.parse_args()

    available = get_available_years()
    if not available:
        logger.error(f"No CY*.csv files found in {DATA_DIR}")
        sys.exit(1)

    logger.info(f"Found {len(available)} CSV files: CY{available[0]} through CY{available[-1]}")

    years = args.years or available

    if args.test:
        # Just parse and show samples from first year
        yr = years[0]
        filepath = DATA_DIR / f"CY{yr}.csv"
        if not filepath.exists():
            logger.error(f"File not found: {filepath}")
            sys.exit(1)

        records, total, skipped = read_csv_file(filepath, yr)
        logger.info(f"CY{yr}: {len(records):,} records parsed ({skipped} skipped)")
        print("\n--- Sample records ---")
        for rec in records[:5]:
            print(f"  {rec['cert_date']}  Serial={rec['serial_no']}  "
                  f"{rec['grain']:12s} Class={rec.get('grain_class',''):6s} "
                  f"Dest={rec['destination']:20s} "
                  f"MT={rec['metric_tons']:>12,.1f}  "
                  f"1000Bu={rec['bushels_1000'] or 0:>10,.3f}  "
                  f"Port={rec.get('port_region',''):20s} "
                  f"TypeServ={rec.get('type_service','')}")

        # Show grain breakdown
        grain_counts = {}
        grain_mt = {}
        for rec in records:
            g = rec['grain']
            grain_counts[g] = grain_counts.get(g, 0) + 1
            grain_mt[g] = grain_mt.get(g, 0) + (rec['metric_tons'] or 0)
        print("\n--- Grain summary ---")
        for g in sorted(grain_counts.keys(), key=lambda x: grain_mt.get(x, 0), reverse=True):
            print(f"  {g:20s}  {grain_counts[g]:>6,} certs  {grain_mt[g]:>14,.0f} MT")

        # Show type_service breakdown
        ts_counts = {}
        for rec in records:
            ts = rec.get('type_service', 'NONE')
            ts_counts[ts] = ts_counts.get(ts, 0) + 1
        print("\n--- Type of Service ---")
        for ts, cnt in sorted(ts_counts.items(), key=lambda x: -x[1]):
            print(f"  {ts or 'EMPTY':6s}  {cnt:>8,}")

        return

    # Full load
    grand_loaded = 0
    grand_total = 0
    grand_skipped = 0
    start = datetime.now()

    for yr in years:
        filepath = DATA_DIR / f"CY{yr}.csv"
        if not filepath.exists():
            logger.warning(f"File not found: CY{yr}.csv — skipping")
            continue

        loaded, total, skipped = load_year(filepath, yr, dry_run=args.dry_run)
        grand_loaded += loaded
        grand_total += total
        grand_skipped += skipped

    elapsed = (datetime.now() - start).total_seconds()
    mode = "DRY RUN" if args.dry_run else "LOADED"
    logger.info(f"\n{'='*60}")
    logger.info(f"{mode}: {grand_loaded:,} records across {len(years)} years")
    logger.info(f"  Total CSV rows: {grand_total:,}  Skipped: {grand_skipped:,}")
    logger.info(f"  Elapsed: {elapsed:.1f}s")
    logger.info(f"{'='*60}")


if __name__ == '__main__':
    main()
