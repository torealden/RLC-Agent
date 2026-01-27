#!/usr/bin/env python3
"""
Feed Grains Yearbook Ingestion Script

Parses the USDA ERS Feed Grains Database yearbook Excel file.
Contains 35 tables covering:
- Acreage, production, yield for corn, sorghum, barley, oats (Table 1)
- World coarse grains (Table 2)
- Feed grains aggregate S&D (Table 3)
- Individual crop S&D with quarterly breakdowns (Tables 4-7)
- Hay production and stocks (Table 8)
- Farm prices (Tables 9-11)
- Many more detailed tables

Data source: ERS Feed Grains Database
https://www.ers.usda.gov/data-products/feed-grains-database/

Usage:
    python scripts/ingest_feed_grains_yearbook.py
    python scripts/ingest_feed_grains_yearbook.py --file "Models/Data/US Feed Grains Outlook - Dec 25.xlsx"
"""

import argparse
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('feed_grains_yearbook')

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
DEFAULT_FILE = PROJECT_ROOT / "Models" / "Data" / "US Feed Grains Outlook - Dec 25.xlsx"

# Marketing year pattern
YEAR_PATTERN = re.compile(r'^(\d{4})/(\d{2})$')


class FeedGrainsYearbookIngestor:
    """Ingests Feed Grains Yearbook data."""

    DATA_SOURCE = 'ERS_FEED_GRAINS_YEARBOOK'

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.xl = pd.ExcelFile(filepath)
        self.conn = sqlite3.connect(str(DB_PATH))
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

        self.stats = {
            'production': 0,
            'balance_sheet': 0,
            'quarterly_balance': 0,
            'price': 0,
            'monthly_price': 0
        }

    def _create_tables(self):
        """Create bronze tables for feed grains data."""
        # Production/acreage table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS feed_grain_production (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source TEXT NOT NULL,
                commodity_code TEXT NOT NULL,
                location_code TEXT DEFAULT 'US',
                marketing_year TEXT NOT NULL,
                planted_area REAL,
                harvested_area REAL,
                production REAL,
                yield_value REAL,
                farm_price REAL,
                loan_rate REAL,
                area_unit TEXT,
                production_unit TEXT,
                yield_unit TEXT,
                price_unit TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_source, commodity_code, location_code, marketing_year)
            )
        """)

        # Balance sheet with quarterly breakdowns
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS feed_grain_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source TEXT NOT NULL,
                commodity_code TEXT NOT NULL,
                location_code TEXT DEFAULT 'US',
                marketing_year TEXT NOT NULL,
                period_type TEXT NOT NULL,  -- 'ANNUAL', 'Q1', 'Q2', 'Q3', 'Q4'
                -- Supply
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                -- Demand
                food_alcohol_industrial REAL,
                seed_use REAL,
                feed_residual REAL,
                total_domestic REAL,
                exports REAL,
                total_use REAL,
                ending_stocks REAL,
                -- Units
                unit_desc TEXT DEFAULT 'Million bushels',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_source, commodity_code, location_code, marketing_year, period_type)
            )
        """)

        # Monthly/annual prices
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS feed_grain_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source TEXT NOT NULL,
                commodity_code TEXT NOT NULL,
                location_code TEXT DEFAULT 'US',
                marketing_year TEXT NOT NULL,
                period_type TEXT NOT NULL,  -- 'ANNUAL', month name
                price REAL,
                unit_desc TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_source, commodity_code, location_code, marketing_year, period_type)
            )
        """)

        self.conn.commit()
        logger.info("Bronze tables ready")

    def _clean_year(self, value) -> Optional[str]:
        """Extract marketing year from value."""
        if pd.isna(value):
            return None
        s = str(value).strip()
        match = YEAR_PATTERN.match(s)
        if match:
            return s
        return None

    def _clean_numeric(self, value) -> Optional[float]:
        """Clean numeric value."""
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return float(value) if value != 0 else None
        s = str(value).strip()
        if s in ['', 'NA', 'N/A', '--', '-']:
            return None
        try:
            return float(s.replace(',', ''))
        except ValueError:
            return None

    def parse_table1_production(self):
        """Parse Table 1: Acreage, production, yield, and prices."""
        logger.info("Parsing Table 1 (Production/Acreage)...")

        df = pd.read_excel(self.xl, sheet_name='FGYearbookTable01', header=None)

        current_commodity = None
        count = 0

        # Map commodity names to codes
        commodity_map = {
            'Corn': 'CORN',
            'Sorghum': 'SORGHUM',
            'Barley': 'BARLEY',
            'Oats': 'OATS'
        }

        for idx, row in df.iterrows():
            if idx < 2:  # Skip header rows
                continue

            first_cell = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''

            # Check for commodity header
            if first_cell in commodity_map:
                current_commodity = commodity_map[first_cell]
                # This row may also have data
                year = self._clean_year(row.iloc[1]) if len(row) > 1 else None
                if year:
                    self._insert_production(
                        commodity_code=current_commodity,
                        marketing_year=year,
                        planted_area=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                        harvested_area=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                        production=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                        yield_value=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                        farm_price=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                        loan_rate=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None
                    )
                    count += 1
                continue

            # Check for data row (starts with year)
            year = self._clean_year(first_cell) or self._clean_year(row.iloc[1]) if len(row) > 1 else None
            if year and current_commodity:
                # Determine which column is year
                if self._clean_year(first_cell):
                    col_offset = 0
                else:
                    col_offset = 1

                self._insert_production(
                    commodity_code=current_commodity,
                    marketing_year=year,
                    planted_area=self._clean_numeric(row.iloc[col_offset + 1]) if len(row) > col_offset + 1 else None,
                    harvested_area=self._clean_numeric(row.iloc[col_offset + 2]) if len(row) > col_offset + 2 else None,
                    production=self._clean_numeric(row.iloc[col_offset + 3]) if len(row) > col_offset + 3 else None,
                    yield_value=self._clean_numeric(row.iloc[col_offset + 4]) if len(row) > col_offset + 4 else None,
                    farm_price=self._clean_numeric(row.iloc[col_offset + 5]) if len(row) > col_offset + 5 else None,
                    loan_rate=self._clean_numeric(row.iloc[col_offset + 6]) if len(row) > col_offset + 6 else None
                )
                count += 1

        self.conn.commit()
        logger.info(f"  Parsed {count} production records")
        self.stats['production'] = count

    def parse_table4_corn_sd(self):
        """Parse Table 4: Corn Supply and Disappearance with quarterly data."""
        logger.info("Parsing Table 4 (Corn S&D)...")

        df = pd.read_excel(self.xl, sheet_name='FGYearbookTable04', header=None)

        # Columns: Marketing year | Quarter | Beg stocks | Production | Imports | Total supply |
        # Food/alcohol/industrial | Seed | Feed/residual | Total domestic | Exports | Total use | Ending stocks
        current_my = None
        count = 0
        quarterly_count = 0

        for idx, row in df.iterrows():
            if idx < 2:  # Skip header
                continue

            first_cell = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''
            second_cell = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else ''

            # Check for marketing year in first column
            year = self._clean_year(first_cell)
            if year:
                current_my = year

            # Determine quarter type
            period_type = None
            if 'Q1' in second_cell or 'September-November' in second_cell:
                period_type = 'Q1'
            elif 'Q2' in second_cell or 'December-February' in second_cell:
                period_type = 'Q2'
            elif 'Q3' in second_cell or 'March-May' in second_cell:
                period_type = 'Q3'
            elif 'Q4' in second_cell or 'June-August' in second_cell:
                period_type = 'Q4'
            elif 'MY' in second_cell or 'September-August' in second_cell:
                period_type = 'ANNUAL'

            if current_my and period_type:
                # Get data values (starting from column 2)
                self._insert_balance(
                    commodity_code='CORN',
                    marketing_year=current_my,
                    period_type=period_type,
                    beginning_stocks=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                    production=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                    imports=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                    total_supply=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                    food_alcohol_industrial=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                    seed_use=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                    feed_residual=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                    total_domestic=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                    exports=self._clean_numeric(row.iloc[10]) if len(row) > 10 else None,
                    total_use=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                    ending_stocks=self._clean_numeric(row.iloc[12]) if len(row) > 12 else None
                )
                if period_type == 'ANNUAL':
                    count += 1
                else:
                    quarterly_count += 1

        self.conn.commit()
        logger.info(f"  Parsed {count} annual, {quarterly_count} quarterly corn records")
        self.stats['balance_sheet'] += count
        self.stats['quarterly_balance'] += quarterly_count

    def parse_table5_sorghum_sd(self):
        """Parse Table 5: Sorghum Supply and Disappearance."""
        logger.info("Parsing Table 5 (Sorghum S&D)...")
        self._parse_crop_sd_table('FGYearbookTable05', 'SORGHUM')

    def parse_table6_barley_sd(self):
        """Parse Table 6: Barley Supply and Disappearance."""
        logger.info("Parsing Table 6 (Barley S&D)...")
        self._parse_crop_sd_table('FGYearbookTable06', 'BARLEY')

    def parse_table7_oats_sd(self):
        """Parse Table 7: Oats Supply and Disappearance."""
        logger.info("Parsing Table 7 (Oats S&D)...")
        self._parse_crop_sd_table('FGYearbookTable07', 'OATS')

    def _parse_crop_sd_table(self, sheet_name: str, commodity_code: str):
        """Generic parser for crop S&D tables (similar structure to corn)."""
        df = pd.read_excel(self.xl, sheet_name=sheet_name, header=None)

        current_my = None
        count = 0
        quarterly_count = 0

        for idx, row in df.iterrows():
            if idx < 2:
                continue

            first_cell = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''
            second_cell = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else ''

            year = self._clean_year(first_cell)
            if year:
                current_my = year

            period_type = None
            if 'Q1' in second_cell or 'September-November' in second_cell:
                period_type = 'Q1'
            elif 'Q2' in second_cell or 'December-February' in second_cell:
                period_type = 'Q2'
            elif 'Q3' in second_cell or 'March-May' in second_cell:
                period_type = 'Q3'
            elif 'Q4' in second_cell or 'June-August' in second_cell:
                period_type = 'Q4'
            elif 'MY' in second_cell or 'September-August' in second_cell:
                period_type = 'ANNUAL'

            if current_my and period_type:
                self._insert_balance(
                    commodity_code=commodity_code,
                    marketing_year=current_my,
                    period_type=period_type,
                    beginning_stocks=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                    production=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                    imports=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                    total_supply=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                    food_alcohol_industrial=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                    seed_use=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                    feed_residual=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                    total_domestic=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                    exports=self._clean_numeric(row.iloc[10]) if len(row) > 10 else None,
                    total_use=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                    ending_stocks=self._clean_numeric(row.iloc[12]) if len(row) > 12 else None
                )
                if period_type == 'ANNUAL':
                    count += 1
                else:
                    quarterly_count += 1

        self.conn.commit()
        logger.info(f"  Parsed {count} annual, {quarterly_count} quarterly {commodity_code} records")
        self.stats['balance_sheet'] += count
        self.stats['quarterly_balance'] += quarterly_count

    def parse_table9_corn_prices(self):
        """Parse Table 9: Corn and Sorghum prices with monthly detail."""
        logger.info("Parsing Table 9 (Corn/Sorghum Prices)...")

        df = pd.read_excel(self.xl, sheet_name='FGYearbookTable09', header=None)

        # Columns: Commodity | Year | Sep | Oct | Nov | Dec | Jan | Feb | Mar | Apr | May | Jun | Jul | Aug | Annual
        months = ['September', 'October', 'November', 'December', 'January', 'February',
                  'March', 'April', 'May', 'June', 'July', 'August']

        current_commodity = None
        corn_count = 0
        sorghum_count = 0

        for idx, row in df.iterrows():
            if idx < 2:
                continue

            first_cell = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''

            # Check commodity header
            if 'Corn' in first_cell and 'dollar' in first_cell.lower():
                current_commodity = 'CORN'
                continue
            elif 'Sorghum' in first_cell or 'sorghum' in first_cell:
                current_commodity = 'SORGHUM'
                continue

            if not current_commodity:
                continue

            # Get marketing year
            year = self._clean_year(row.iloc[1]) if len(row) > 1 else None
            if not year:
                continue

            # Get annual average (last column with data)
            annual_price = None
            for col_idx in range(13, 1, -1):
                if len(row) > col_idx and pd.notna(row.iloc[col_idx]):
                    annual_price = self._clean_numeric(row.iloc[col_idx])
                    break

            # Insert annual price
            if annual_price:
                self._insert_price(
                    commodity_code=current_commodity,
                    marketing_year=year,
                    period_type='ANNUAL',
                    price=annual_price,
                    unit_desc='Dollars per bushel'
                )
                if current_commodity == 'CORN':
                    corn_count += 1
                else:
                    sorghum_count += 1

            # Insert monthly prices (columns 2-13)
            for i, month in enumerate(months):
                col_idx = i + 2
                if len(row) > col_idx:
                    price = self._clean_numeric(row.iloc[col_idx])
                    if price:
                        self._insert_price(
                            commodity_code=current_commodity,
                            marketing_year=year,
                            period_type=month,
                            price=price,
                            unit_desc='Dollars per bushel'
                        )
                        self.stats['monthly_price'] += 1

        self.conn.commit()
        logger.info(f"  Parsed {corn_count} corn, {sorghum_count} sorghum price records")
        self.stats['price'] += corn_count + sorghum_count

    def parse_table10_barley_oats_prices(self):
        """Parse Table 10: Barley and Oats prices (June-May marketing year)."""
        logger.info("Parsing Table 10 (Barley/Oats Prices)...")

        df = pd.read_excel(self.xl, sheet_name='FGYearbookTable10', header=None)

        # Barley/oats have June-May marketing year
        months = ['June', 'July', 'August', 'September', 'October', 'November',
                  'December', 'January', 'February', 'March', 'April', 'May']

        current_commodity = None
        count = 0

        for idx, row in df.iterrows():
            if idx < 2:
                continue

            first_cell = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''

            # Check for commodity header (just "Barley" or "Oats")
            if first_cell == 'Barley':
                current_commodity = 'BARLEY'
                continue
            elif first_cell == 'Oats':
                current_commodity = 'OATS'
                continue

            if not current_commodity:
                continue

            year = self._clean_year(row.iloc[1]) if len(row) > 1 else None
            if not year:
                continue

            # Get annual average (column 14, index 14)
            annual_price = self._clean_numeric(row.iloc[14]) if len(row) > 14 else None

            if annual_price:
                self._insert_price(
                    commodity_code=current_commodity,
                    marketing_year=year,
                    period_type='ANNUAL',
                    price=annual_price,
                    unit_desc='Dollars per bushel'
                )
                count += 1

            # Monthly prices (columns 2-13)
            for i, month in enumerate(months):
                col_idx = i + 2
                if len(row) > col_idx:
                    price = self._clean_numeric(row.iloc[col_idx])
                    if price:
                        self._insert_price(
                            commodity_code=current_commodity,
                            marketing_year=year,
                            period_type=month,
                            price=price,
                            unit_desc='Dollars per bushel'
                        )
                        self.stats['monthly_price'] += 1

        self.conn.commit()
        logger.info(f"  Parsed {count} barley/oats price records")
        self.stats['price'] += count

    def _insert_production(self, commodity_code: str, marketing_year: str, **kwargs):
        """Insert production record."""
        fields = {k: v for k, v in kwargs.items() if v is not None}

        columns = ['data_source', 'commodity_code', 'marketing_year']
        values = [self.DATA_SOURCE, commodity_code, marketing_year]

        for field, value in fields.items():
            columns.append(field)
            values.append(value)

        placeholders = ','.join(['?' for _ in values])
        column_str = ','.join(columns)

        try:
            self.conn.execute(f"""
                INSERT OR REPLACE INTO feed_grain_production ({column_str})
                VALUES ({placeholders})
            """, values)
        except sqlite3.Error as e:
            logger.warning(f"Error inserting production: {e}")

    def _insert_balance(self, commodity_code: str, marketing_year: str, period_type: str, **kwargs):
        """Insert balance sheet record."""
        fields = {k: v for k, v in kwargs.items() if v is not None}

        columns = ['data_source', 'commodity_code', 'marketing_year', 'period_type']
        values = [self.DATA_SOURCE, commodity_code, marketing_year, period_type]

        for field, value in fields.items():
            columns.append(field)
            values.append(value)

        placeholders = ','.join(['?' for _ in values])
        column_str = ','.join(columns)

        try:
            self.conn.execute(f"""
                INSERT OR REPLACE INTO feed_grain_balance ({column_str})
                VALUES ({placeholders})
            """, values)
        except sqlite3.Error as e:
            logger.warning(f"Error inserting balance: {e}")

    def _insert_price(self, commodity_code: str, marketing_year: str, period_type: str,
                      price: float, unit_desc: str):
        """Insert price record."""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO feed_grain_price
                (data_source, commodity_code, marketing_year, period_type, price, unit_desc)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (self.DATA_SOURCE, commodity_code, marketing_year, period_type, price, unit_desc))
        except sqlite3.Error as e:
            logger.warning(f"Error inserting price: {e}")

    def run(self):
        """Execute full ingestion."""
        logger.info(f"Starting Feed Grains Yearbook ingestion from {self.filepath}")

        # Parse key tables
        self.parse_table1_production()
        self.parse_table4_corn_sd()
        self.parse_table5_sorghum_sd()
        self.parse_table6_barley_sd()
        self.parse_table7_oats_sd()
        self.parse_table9_corn_prices()
        self.parse_table10_barley_oats_prices()

        self.conn.close()

        logger.info("=" * 60)
        logger.info("Ingestion Summary:")
        logger.info(f"  Production records: {self.stats['production']}")
        logger.info(f"  Annual balance records: {self.stats['balance_sheet']}")
        logger.info(f"  Quarterly balance records: {self.stats['quarterly_balance']}")
        logger.info(f"  Annual price records: {self.stats['price']}")
        logger.info(f"  Monthly price records: {self.stats['monthly_price']}")
        logger.info("=" * 60)

        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Ingest Feed Grains Yearbook data')
    parser.add_argument('--file', type=Path, default=DEFAULT_FILE,
                        help='Path to Feed Grains Excel file')
    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"File not found: {args.file}")
        return

    ingestor = FeedGrainsYearbookIngestor(args.file)
    ingestor.run()


if __name__ == '__main__':
    main()
