#!/usr/bin/env python3
"""
Oil Crops Yearbook CSV Ingestion Script

Parses the ERS Oil Crops Yearbook comprehensive CSV export (OilCropsAllTables.csv).
Contains ~30K rows covering all oil crops data from 1980-present.

Tables included:
- Soybean stocks, supply, disappearance, prices (Tables 1-9)
- Peanuts (Tables 10-16)
- Cottonseed (Tables 17-20)
- Sunflowerseed (Tables 21-25)
- Canola (Tables 26-27)
- Flaxseed (Tables 28-30)
- And many more

Usage:
    python scripts/ingest_oil_crops_csv.py
"""

import argparse
import logging
import sqlite3
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('oil_crops_csv')

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
DEFAULT_FILE = PROJECT_ROOT / "Models" / "Data" / "OilCropsAllTables.csv"


def create_tables(conn):
    """Create bronze table for oil crops yearbook data."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oil_crops_yearbook (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_source TEXT DEFAULT 'ERS_OIL_CROPS_YEARBOOK',
            timeperiod_desc TEXT,
            marketing_year TEXT,
            my_definition TEXT,
            commodity_group TEXT,
            commodity TEXT,
            commodity_desc2 TEXT,
            attribute_desc TEXT,
            attribute_desc2 TEXT,
            geography_desc TEXT,
            geography_desc2 TEXT,
            amount REAL,
            unit_desc TEXT,
            table_number INTEGER,
            table_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Create index for efficient queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_oil_crops_commodity_year
        ON oil_crops_yearbook(commodity, marketing_year, attribute_desc)
    """)

    conn.commit()
    logger.info("Bronze table ready")


def ingest_csv(filepath: Path):
    """Ingest the Oil Crops CSV file."""
    logger.info(f"Reading {filepath}...")

    # Read CSV
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} rows")

    # Connect to database
    conn = sqlite3.connect(str(DB_PATH))
    create_tables(conn)

    # Clear existing data to avoid duplicates on re-run
    conn.execute("DELETE FROM oil_crops_yearbook")
    conn.commit()

    # Rename columns to match table schema
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Insert data in batches
    batch_size = 1000
    total = 0

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]

        for _, row in batch.iterrows():
            conn.execute("""
                INSERT INTO oil_crops_yearbook
                (timeperiod_desc, marketing_year, my_definition, commodity_group,
                 commodity, commodity_desc2, attribute_desc, attribute_desc2,
                 geography_desc, geography_desc2, amount, unit_desc, table_number, table_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['timeperiod_desc'], row['marketing_year'], row['my_definition'],
                row['commodity_group'], row['commodity'], row['commodity_desc2'],
                row['attribute_desc'], row['attribute_desc2'], row['geography_desc'],
                row['geography_desc2'], row['amount'], row['unit_desc'],
                row['table_number'], row['table_name']
            ))

        conn.commit()
        total += len(batch)
        logger.info(f"  Inserted {total}/{len(df)} rows...")

    conn.close()

    logger.info("=" * 60)
    logger.info(f"Ingestion complete: {total} rows inserted")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description='Ingest Oil Crops Yearbook CSV')
    parser.add_argument('--file', type=Path, default=DEFAULT_FILE,
                        help='Path to OilCropsAllTables.csv')
    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"File not found: {args.file}")
        return

    ingest_csv(args.file)


if __name__ == '__main__':
    main()
