#!/usr/bin/env python3
"""
NASS Crop Production Annual Summary Parser

Parses the annual Crop Production Summary text file from NASS.
Contains state-level acreage, yield, and production data for all major crops.

Crops covered:
- Corn, Sorghum, Oats, Barley
- All wheat types (winter, spring, durum)
- Rice, Soybeans, Peanuts
- Cotton, Sunflower, Canola
- Hay and Forage

Usage:
    python scripts/ingest_crop_production_summary.py
    python scripts/ingest_crop_production_summary.py --file "Models/Data/Crop Production Annual Summary.txt"
"""

import argparse
import logging
import re
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('crop_production')

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
DEFAULT_FILE = PROJECT_ROOT / "Models" / "Data" / "Crop Production Annual Summary.txt"


def create_tables(conn):
    """Create bronze table for crop production data."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nass_crop_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_source TEXT DEFAULT 'NASS_CROP_PRODUCTION_SUMMARY',
            commodity TEXT NOT NULL,
            state TEXT NOT NULL,
            year INTEGER NOT NULL,
            area_planted REAL,
            area_harvested REAL,
            yield_value REAL,
            production REAL,
            yield_unit TEXT,
            production_unit TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(data_source, commodity, state, year)
        )
    """)
    conn.commit()
    logger.info("NASS crop production table ready")


def parse_state_data_row(line: str) -> dict:
    """Parse a state data row from the fixed-width format."""
    # Format: 'Alabama .........:    2,120       2,030       1,960       2,034       1,958       1,891  '
    if ':' not in line:
        return None

    parts = line.split(':')
    state = parts[0].replace('.', '').strip()

    # Skip non-state lines
    if not state or len(state) < 2:
        return None
    if state.startswith('1/') or state.startswith('2/'):
        return None
    if 'United States' in state:
        # This is the US total - still include it
        state = 'US'

    # Extract numbers - can be negative or have commas
    values_str = parts[1]
    # Handle (NA) values and negative numbers
    values_str = values_str.replace('(NA)', ' -999999 ')
    numbers = re.findall(r'-?[\d,]+', values_str)

    if len(numbers) < 3:
        return None

    # Convert to integers, treating -999999 as None (NA)
    values = []
    for n in numbers:
        val = int(n.replace(',', ''))
        if val == -999999:
            values.append(None)
        else:
            values.append(val)

    return {
        'state': state,
        'values': values
    }


def parse_crop_table(lines: list, start_idx: int, commodity: str, conn) -> int:
    """Parse a crop table and insert data."""
    count = 0
    i = start_idx
    table_line_count = 0

    while i < len(lines) and table_line_count < 200:  # Limit table size
        line = lines[i]
        table_line_count += 1

        # Skip empty and separator lines
        if not line.strip() or line.strip().startswith('---'):
            i += 1
            continue

        if 'continued' in line.lower() or 'See footnote' in line:
            i += 1
            continue

        # Skip header lines
        if 'State' in line and ':' not in line:
            i += 1
            continue
        if '1,000 acres' in line or '1,000 bushels' in line:
            i += 1
            continue

        # Check for next section/table start (but allow US total line)
        stripped = line.strip()
        if stripped and not stripped[0].isalpha() and ':' not in line:
            i += 1
            continue

        # Parse state data row
        result = parse_state_data_row(line)
        if result:
            state = result['state']
            values = result['values']

            # Format: 2023 planted, 2024 planted, 2025 planted, 2023 harvested, 2024 harvested, 2025 harvested
            if len(values) >= 6:
                for j, year in enumerate([2023, 2024, 2025]):
                    planted = values[j] if j < len(values) else None
                    harvested = values[j + 3] if j + 3 < len(values) else None

                    if planted is not None or harvested is not None:
                        try:
                            conn.execute("""
                                INSERT OR REPLACE INTO nass_crop_production
                                (commodity, state, year, area_planted, area_harvested)
                                VALUES (?, ?, ?, ?, ?)
                            """, (commodity, state, year, planted, harvested))
                            count += 1
                        except sqlite3.Error as e:
                            logger.warning(f"Error: {e}")

        i += 1

    conn.commit()
    return count


def parse_file(filepath: Path):
    """Parse the Crop Production Annual Summary file."""
    logger.info(f"Parsing {filepath}...")

    # Try latin-1 encoding for NASS files
    with open(filepath, 'r', encoding='latin-1', errors='ignore') as f:
        content = f.read()

    lines = content.split('\n')
    logger.info(f"Read {len(lines)} lines")

    conn = sqlite3.connect(str(DB_PATH))
    create_tables(conn)

    # Clear existing data
    conn.execute("DELETE FROM nass_crop_production WHERE data_source = 'NASS_CROP_PRODUCTION_SUMMARY'")
    conn.commit()

    total_count = 0

    # Find and parse each crop table
    crop_patterns = [
        ('Principal Crops Area Planted', 'PRINCIPAL_CROPS'),
        ('Corn Area Planted for All Purposes', 'CORN'),
        ('Sorghum Area Planted', 'SORGHUM'),
        ('Oat Area Planted', 'OATS'),
        ('Barley Area Planted', 'BARLEY'),
        ('All Wheat Area Planted', 'WHEAT_ALL'),
        ('Winter Wheat Area Planted', 'WHEAT_WINTER'),
        ('Other Spring Wheat Area Planted', 'WHEAT_SPRING'),
        ('Durum Wheat Area Planted', 'WHEAT_DURUM'),
        ('Rice Area Planted', 'RICE'),
        ('Soybeans for Beans Area Planted', 'SOYBEANS'),
        ('Peanut Area Planted', 'PEANUTS'),
        ('Sunflower Area Planted', 'SUNFLOWER'),
        ('Canola Area Planted', 'CANOLA'),
        ('Cotton', 'COTTON'),
        ('Flaxseed Area Planted', 'FLAXSEED'),
    ]

    for pattern, commodity in crop_patterns:
        for i, line in enumerate(lines):
            if pattern in line:
                count = parse_crop_table(lines, i, commodity, conn)
                if count > 0:
                    logger.info(f"  {commodity}: {count} records")
                    total_count += count
                break

    conn.close()

    logger.info("=" * 60)
    logger.info(f"Crop Production Summary parsing complete: {total_count} records")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Parse NASS Crop Production Summary')
    parser.add_argument('--file', type=Path, default=DEFAULT_FILE)
    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"File not found: {args.file}")
        return

    parse_file(args.file)


if __name__ == '__main__':
    main()
