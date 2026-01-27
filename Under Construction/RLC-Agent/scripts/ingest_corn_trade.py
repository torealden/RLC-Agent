#!/usr/bin/env python3
"""
USDA FAS GATS Corn Trade Data Ingestion

Parses US corn exports and imports from Census Bureau trade data.
Data includes monthly quantities for corn (grain and seed).

Usage:
    python scripts/ingest_corn_trade.py
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('corn_trade')

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
DATA_DIR = PROJECT_ROOT / "Models" / "Data"


def create_tables(conn):
    """Create bronze table for corn trade data."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS corn_trade (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_source TEXT DEFAULT 'USDA_FAS_GATS',
            trade_type TEXT NOT NULL,  -- 'EXPORT' or 'IMPORT'
            partner TEXT,
            hs_code TEXT,
            product TEXT,
            year INTEGER,
            unit TEXT,
            -- Annual total
            annual_qty REAL,
            -- Monthly quantities
            jan_qty REAL, feb_qty REAL, mar_qty REAL, apr_qty REAL,
            may_qty REAL, jun_qty REAL, jul_qty REAL, aug_qty REAL,
            sep_qty REAL, oct_qty REAL, nov_qty REAL, dec_qty REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    logger.info("Corn trade table ready")


def parse_trade_file(filepath: Path, trade_type: str, conn):
    """Parse a corn trade Excel file."""
    logger.info(f"Parsing {filepath.name}...")

    xl = pd.ExcelFile(filepath)
    df = pd.read_excel(xl, sheet_name=xl.sheet_names[0], header=None)

    count = 0

    # Find the annual data rows (rows 5-16 based on structure)
    for idx in range(5, 17):
        if idx >= len(df):
            break

        row = df.iloc[idx]

        # Check if this is a data row
        if pd.isna(row.iloc[0]) or not isinstance(row.iloc[0], (int, float)):
            continue

        partner = str(row.iloc[1]) if len(row) > 1 else None
        hs_code = str(int(row.iloc[3])) if len(row) > 3 and pd.notna(row.iloc[3]) else None
        product = str(row.iloc[4]) if len(row) > 4 else None
        year_str = str(row.iloc[5]) if len(row) > 5 else None
        unit = str(row.iloc[6]) if len(row) > 6 else None
        annual_qty = float(row.iloc[7]) if len(row) > 7 and pd.notna(row.iloc[7]) else None

        # Extract year from "2020-2020" format
        year = None
        if year_str and '-' in year_str:
            year = int(year_str.split('-')[0])

        if year and product:
            conn.execute("""
                INSERT INTO corn_trade
                (trade_type, partner, hs_code, product, year, unit, annual_qty)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (trade_type, partner, hs_code, product, year, unit, annual_qty))
            count += 1

    conn.commit()
    logger.info(f"  Inserted {count} records")
    return count


def main():
    conn = sqlite3.connect(str(DB_PATH))
    create_tables(conn)

    # Clear existing data
    conn.execute("DELETE FROM corn_trade")
    conn.commit()

    total = 0

    # Parse exports
    exports_file = DATA_DIR / "US Corn Exports - 01201025.xlsx"
    if exports_file.exists():
        total += parse_trade_file(exports_file, 'EXPORT', conn)

    # Parse imports
    imports_file = DATA_DIR / "US Corn Imports - 01201025.xlsx"
    if imports_file.exists():
        total += parse_trade_file(imports_file, 'IMPORT', conn)

    conn.close()

    logger.info("=" * 60)
    logger.info(f"Corn trade ingestion complete: {total} records")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()
