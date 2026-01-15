#!/usr/bin/env python3
"""
Oil Crops Monthly Outlook Ingestion Script

Parses the USDA ERS Oil Crops Outlook monthly Excel file (oiltables.xlsx).
This file contains supply & demand tables and prices for:
- Soybeans, soybean meal, soybean oil (Tables 1-3)
- Cottonseed, cottonseed meal, cottonseed oil (Tables 4-6)
- Peanuts (Table 7)
- Oilseed farm prices (Table 8)
- Vegetable oil prices (Table 9)
- Oilseed meal prices (Table 10)

The script extracts both annual and monthly data, storing them in the bronze layer
with a distinct data_source to avoid conflicts with the yearbook data.

Usage:
    python scripts/ingest_oil_crops_monthly.py
    python scripts/ingest_oil_crops_monthly.py --file Models/Data/oiltables_202601.xlsx
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
logger = logging.getLogger('oil_crops_monthly')

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
DEFAULT_FILE = PROJECT_ROOT / "Models" / "Data" / "oiltables.xlsx"

# Marketing year patterns - match year like "2023/24" or "2024/251" (with superscript)
ANNUAL_PATTERN = re.compile(r'^(\d{4})/(\d{2})(\d*)$')  # Captures start year, 2-digit end year, and optional indicator
MONTH_NAMES = ['September', 'October', 'November', 'December', 'January',
               'February', 'March', 'April', 'May', 'June', 'July', 'August']
QUARTER_PATTERNS = ['September–November', 'December–February', 'March-May', 'June–August']


class OilCropsMonthlyIngestor:
    """Ingests monthly Oil Crops Outlook data."""

    DATA_SOURCE = 'ERS_OIL_CROPS_MONTHLY'

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.xl = pd.ExcelFile(filepath)
        self.conn = sqlite3.connect(str(DB_PATH))
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

        # Track ingestion stats
        self.stats = {
            'balance_sheet': 0,
            'price': 0,
            'monthly_balance': 0,
            'monthly_price': 0
        }

    def _create_tables(self):
        """Create bronze tables for monthly outlook data."""
        # Monthly balance sheet (annual and monthly granularity)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oilseed_monthly_balance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source TEXT NOT NULL,
                commodity_code TEXT NOT NULL,
                location_code TEXT DEFAULT 'US',
                marketing_year TEXT NOT NULL,
                period_type TEXT NOT NULL,  -- 'ANNUAL', 'MONTHLY', 'QUARTERLY'
                period_detail TEXT,          -- Month name or quarter label
                -- Area (for crops with acreage)
                planted_area REAL,
                harvested_area REAL,
                yield_value REAL,
                -- Supply
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                -- Demand
                crush REAL,
                domestic_use REAL,
                exports REAL,
                seed_residual REAL,
                biofuel_use REAL,
                food_use REAL,
                other_use REAL,
                total_use REAL,
                ending_stocks REAL,
                -- Units and metadata
                unit_desc TEXT,
                forecast_flag TEXT,  -- '1' = Estimated, '2' = Forecast
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_source, commodity_code, location_code, marketing_year, period_type, period_detail)
            )
        """)

        # Monthly prices
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oilseed_monthly_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source TEXT NOT NULL,
                commodity_code TEXT NOT NULL,
                location_code TEXT DEFAULT 'US',
                marketing_year TEXT NOT NULL,
                period_type TEXT NOT NULL,  -- 'ANNUAL', 'MONTHLY'
                period_detail TEXT,
                price_type TEXT NOT NULL,   -- 'FARM', 'WHOLESALE'
                price REAL,
                unit_desc TEXT,
                forecast_flag TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_source, commodity_code, location_code, marketing_year, period_type, period_detail, price_type)
            )
        """)

        self.conn.commit()
        logger.info("Bronze tables ready")

    def _clean_year(self, value) -> Optional[Tuple[str, str]]:
        """Extract marketing year and forecast flag from value like '2024/251'."""
        if pd.isna(value):
            return None

        s = str(value).strip()
        match = ANNUAL_PATTERN.match(s)
        if match:
            start_year = match.group(1)
            end_year = match.group(2)  # Always 2 digits now
            indicator = match.group(3)  # Optional forecast indicator (1=estimated, 2=forecast)

            full_year = f"{start_year}/{end_year}"
            forecast_flag = indicator if indicator else None

            return (full_year, forecast_flag)
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

    def _is_month_row(self, value) -> Optional[str]:
        """Check if value is a month name and return standardized form."""
        if pd.isna(value):
            return None
        s = str(value).strip()
        for month in MONTH_NAMES:
            if s.lower().startswith(month.lower()):
                return month
        return None

    def _is_quarter_row(self, value) -> Optional[str]:
        """Check if value is a quarterly aggregate row."""
        if pd.isna(value):
            return None
        s = str(value).strip()
        for pattern in QUARTER_PATTERNS:
            if pattern.lower() in s.lower():
                return pattern
        return None

    def parse_table1_soybeans(self):
        """Parse Table 1: Soybeans U.S. supply and disappearance."""
        logger.info("Parsing Table 1 (Soybeans S&D)...")

        df = pd.read_excel(self.xl, sheet_name='Table 1', header=None)

        # Columns based on header structure (row 3):
        # Year | Planted | Harvested | Yield | Beg Stocks | Production | Imports | Total Supply |
        # Crush | Seed & residual | Exports | Total Use | Ending stocks
        col_map = {
            0: 'year_col',
            1: 'planted_area',
            2: 'harvested_area',
            3: 'yield_value',
            4: 'beginning_stocks',
            5: 'production',
            6: 'imports',
            7: 'total_supply',
            # 8 is spacer
            9: 'crush',
            10: 'seed_residual',
            11: 'exports',
            12: 'total_use',
            13: 'ending_stocks'
        }

        current_my = None  # Current marketing year for monthly rows

        for idx, row in df.iterrows():
            if idx < 5:  # Skip header rows
                continue

            first_cell = row.iloc[0] if len(row) > 0 else None

            # Check if this is an annual row
            year_info = self._clean_year(first_cell)
            if year_info:
                my, flag = year_info

                # Check if we have actual data (not just a monthly section header)
                has_data = any(pd.notna(row.iloc[i]) for i in [1, 2, 5, 9, 11] if i < len(row))

                if has_data:
                    self._insert_balance(
                        commodity_code='SOYBEANS',
                        marketing_year=my,
                        period_type='ANNUAL',
                        planted_area=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                        harvested_area=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                        yield_value=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                        beginning_stocks=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                        production=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                        imports=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                        total_supply=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                        crush=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                        seed_residual=self._clean_numeric(row.iloc[10]) if len(row) > 10 else None,
                        exports=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                        total_use=self._clean_numeric(row.iloc[12]) if len(row) > 12 else None,
                        ending_stocks=self._clean_numeric(row.iloc[13]) if len(row) > 13 else None,
                        unit_desc='Million bushels',
                        forecast_flag=flag
                    )
                    self.stats['balance_sheet'] += 1
                else:
                    # This is a section header for monthly data
                    current_my = my
                continue

            # Check for monthly data
            month = self._is_month_row(first_cell)
            if month and current_my:
                self._insert_balance(
                    commodity_code='SOYBEANS',
                    marketing_year=current_my,
                    period_type='MONTHLY',
                    period_detail=month,
                    beginning_stocks=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                    imports=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                    crush=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                    exports=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                    ending_stocks=self._clean_numeric(row.iloc[13]) if len(row) > 13 else None,
                    unit_desc='Million bushels'
                )
                self.stats['monthly_balance'] += 1
                continue

            # Check for quarterly aggregates
            quarter = self._is_quarter_row(first_cell)
            if quarter and current_my:
                self._insert_balance(
                    commodity_code='SOYBEANS',
                    marketing_year=current_my,
                    period_type='QUARTERLY',
                    period_detail=quarter,
                    beginning_stocks=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                    production=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                    imports=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                    total_supply=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                    crush=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                    exports=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                    ending_stocks=self._clean_numeric(row.iloc[13]) if len(row) > 13 else None,
                    unit_desc='Million bushels'
                )
                self.stats['monthly_balance'] += 1

        self.conn.commit()
        logger.info(f"  Parsed Table 1: {self.stats['balance_sheet']} annual, {self.stats['monthly_balance']} periodic")

    def parse_table2_meal(self):
        """Parse Table 2: Soybean meal S&D."""
        logger.info("Parsing Table 2 (Soybean Meal S&D)...")

        df = pd.read_excel(self.xl, sheet_name='Table 2', header=None)

        # Columns: Year | Beg stocks | Production | Imports | Total | (spacer) | Domestic | Exports | Total Use | Ending
        current_my = None
        count = 0

        for idx, row in df.iterrows():
            if idx < 5:
                continue

            first_cell = row.iloc[0] if len(row) > 0 else None
            year_info = self._clean_year(first_cell)

            if year_info:
                my, flag = year_info
                has_data = any(pd.notna(row.iloc[i]) for i in [1, 2, 6, 7] if i < len(row))

                if has_data:
                    self._insert_balance(
                        commodity_code='SOYBEAN_MEAL',
                        marketing_year=my,
                        period_type='ANNUAL',
                        beginning_stocks=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                        production=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                        imports=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                        total_supply=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                        domestic_use=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                        exports=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                        total_use=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                        ending_stocks=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                        unit_desc='1,000 short tons',
                        forecast_flag=flag
                    )
                    count += 1
                else:
                    current_my = my
                continue

            # Monthly rows
            month = self._is_month_row(first_cell)
            if month and current_my:
                self._insert_balance(
                    commodity_code='SOYBEAN_MEAL',
                    marketing_year=current_my,
                    period_type='MONTHLY',
                    period_detail=month,
                    beginning_stocks=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                    production=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                    imports=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                    domestic_use=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                    exports=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                    ending_stocks=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                    unit_desc='1,000 short tons'
                )

        self.conn.commit()
        logger.info(f"  Parsed Table 2: {count} records")
        self.stats['balance_sheet'] += count

    def parse_table3_oil(self):
        """Parse Table 3: Soybean oil S&D."""
        logger.info("Parsing Table 3 (Soybean Oil S&D)...")

        df = pd.read_excel(self.xl, sheet_name='Table 3', header=None)

        # Columns: Year | Beg stocks | Production | Imports | Total | (spacer) | Dom Total | Biofuel | Food/Other | Exports | Total Use | Ending
        current_my = None
        count = 0

        for idx, row in df.iterrows():
            if idx < 5:
                continue

            first_cell = row.iloc[0] if len(row) > 0 else None
            year_info = self._clean_year(first_cell)

            if year_info:
                my, flag = year_info
                has_data = any(pd.notna(row.iloc[i]) for i in [1, 2, 6, 9] if i < len(row))

                if has_data:
                    self._insert_balance(
                        commodity_code='SOYBEAN_OIL',
                        marketing_year=my,
                        period_type='ANNUAL',
                        beginning_stocks=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                        production=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                        imports=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                        total_supply=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                        domestic_use=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                        biofuel_use=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                        food_use=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                        exports=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                        total_use=self._clean_numeric(row.iloc[10]) if len(row) > 10 else None,
                        ending_stocks=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                        unit_desc='Million pounds',
                        forecast_flag=flag
                    )
                    count += 1
                else:
                    current_my = my
                continue

            month = self._is_month_row(first_cell)
            if month and current_my:
                self._insert_balance(
                    commodity_code='SOYBEAN_OIL',
                    marketing_year=current_my,
                    period_type='MONTHLY',
                    period_detail=month,
                    beginning_stocks=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                    production=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                    imports=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                    domestic_use=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                    biofuel_use=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                    exports=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                    ending_stocks=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                    unit_desc='Million pounds'
                )

        self.conn.commit()
        logger.info(f"  Parsed Table 3: {count} records")
        self.stats['balance_sheet'] += count

    def parse_tables47_cottonseed_peanuts(self):
        """Parse Tables 4-7: Cottonseed complex and Peanuts."""
        logger.info("Parsing Tables 4-7 (Cottonseed & Peanuts)...")

        df = pd.read_excel(self.xl, sheet_name='Tables 4-7', header=None)

        current_table = None
        count = 0

        for idx, row in df.iterrows():
            first_cell = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ''

            # Detect table headers
            if 'Table 4' in first_cell:
                current_table = 'COTTONSEED'
                continue
            elif 'Table 5' in first_cell:
                current_table = 'COTTONSEED_MEAL'
                continue
            elif 'Table 6' in first_cell:
                current_table = 'COTTONSEED_OIL'
                continue
            elif 'Table 7' in first_cell:
                current_table = 'PEANUTS'
                continue

            if not current_table:
                continue

            year_info = self._clean_year(row.iloc[0])
            if year_info:
                my, flag = year_info
                has_data = any(pd.notna(row.iloc[i]) for i in range(1, 8) if i < len(row))

                if has_data:
                    if current_table == 'COTTONSEED':
                        self._insert_balance(
                            commodity_code='COTTONSEED',
                            marketing_year=my,
                            period_type='ANNUAL',
                            beginning_stocks=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                            production=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                            imports=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                            total_supply=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                            crush=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                            exports=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                            other_use=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                            ending_stocks=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                            unit_desc='1,000 short tons',
                            forecast_flag=flag
                        )
                        count += 1
                    elif current_table == 'COTTONSEED_MEAL':
                        self._insert_balance(
                            commodity_code='COTTONSEED_MEAL',
                            marketing_year=my,
                            period_type='ANNUAL',
                            beginning_stocks=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                            production=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                            imports=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                            total_supply=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                            domestic_use=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                            exports=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                            total_use=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                            ending_stocks=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                            unit_desc='1,000 short tons',
                            forecast_flag=flag
                        )
                        count += 1
                    elif current_table == 'COTTONSEED_OIL':
                        self._insert_balance(
                            commodity_code='COTTONSEED_OIL',
                            marketing_year=my,
                            period_type='ANNUAL',
                            beginning_stocks=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                            production=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                            imports=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                            total_supply=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                            domestic_use=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                            exports=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                            total_use=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                            ending_stocks=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                            unit_desc='Million pounds',
                            forecast_flag=flag
                        )
                        count += 1
                    elif current_table == 'PEANUTS':
                        self._insert_balance(
                            commodity_code='PEANUTS',
                            marketing_year=my,
                            period_type='ANNUAL',
                            planted_area=self._clean_numeric(row.iloc[1]) if len(row) > 1 else None,
                            harvested_area=self._clean_numeric(row.iloc[2]) if len(row) > 2 else None,
                            yield_value=self._clean_numeric(row.iloc[3]) if len(row) > 3 else None,
                            beginning_stocks=self._clean_numeric(row.iloc[4]) if len(row) > 4 else None,
                            production=self._clean_numeric(row.iloc[5]) if len(row) > 5 else None,
                            imports=self._clean_numeric(row.iloc[6]) if len(row) > 6 else None,
                            total_supply=self._clean_numeric(row.iloc[7]) if len(row) > 7 else None,
                            food_use=self._clean_numeric(row.iloc[8]) if len(row) > 8 else None,
                            crush=self._clean_numeric(row.iloc[9]) if len(row) > 9 else None,
                            seed_residual=self._clean_numeric(row.iloc[10]) if len(row) > 10 else None,
                            exports=self._clean_numeric(row.iloc[11]) if len(row) > 11 else None,
                            total_use=self._clean_numeric(row.iloc[12]) if len(row) > 12 else None,
                            ending_stocks=self._clean_numeric(row.iloc[13]) if len(row) > 13 else None,
                            unit_desc='Million pounds',
                            forecast_flag=flag
                        )
                        count += 1

        self.conn.commit()
        logger.info(f"  Parsed Tables 4-7: {count} records")
        self.stats['balance_sheet'] += count

    def parse_table8_farm_prices(self):
        """Parse Table 8: Oilseed prices received by farmers."""
        logger.info("Parsing Table 8 (Farm Prices)...")

        df = pd.read_excel(self.xl, sheet_name='Table 8', header=None)

        # Commodities and their columns
        commodities = [
            (1, 'SOYBEANS', 'Dollars per bushel'),
            (2, 'COTTONSEED', 'Dollars per short ton'),
            (3, 'SUNFLOWERSEED', 'Dollars per hundredweight'),
            (4, 'CANOLA', 'Dollars per hundredweight'),
            (5, 'PEANUTS', 'Cents per pound'),
            (6, 'FLAXSEED', 'Dollars per bushel'),
        ]

        current_my = None
        count = 0

        for idx, row in df.iterrows():
            if idx < 5:
                continue

            first_cell = row.iloc[0] if len(row) > 0 else None
            year_info = self._clean_year(first_cell)

            if year_info:
                my, flag = year_info
                has_data = any(pd.notna(row.iloc[i]) for i in range(1, 7) if i < len(row))

                if has_data:
                    for col_idx, commodity, unit in commodities:
                        price = self._clean_numeric(row.iloc[col_idx]) if len(row) > col_idx else None
                        if price:
                            self._insert_price(
                                commodity_code=commodity,
                                marketing_year=my,
                                period_type='ANNUAL',
                                price_type='FARM',
                                price=price,
                                unit_desc=unit,
                                forecast_flag=flag
                            )
                            count += 1
                else:
                    current_my = my
                continue

            # Monthly prices
            month = self._is_month_row(first_cell)
            if month and current_my:
                for col_idx, commodity, unit in commodities:
                    price = self._clean_numeric(row.iloc[col_idx]) if len(row) > col_idx else None
                    if price:
                        self._insert_price(
                            commodity_code=commodity,
                            marketing_year=current_my,
                            period_type='MONTHLY',
                            period_detail=month,
                            price_type='FARM',
                            price=price,
                            unit_desc=unit
                        )
                        self.stats['monthly_price'] += 1

        self.conn.commit()
        logger.info(f"  Parsed Table 8: {count} annual prices")
        self.stats['price'] += count

    def parse_table9_oil_prices(self):
        """Parse Table 9: Vegetable oil prices."""
        logger.info("Parsing Table 9 (Oil Prices)...")

        df = pd.read_excel(self.xl, sheet_name='Table 9', header=None)

        commodities = [
            (1, 'SOYBEAN_OIL', 'Cents per pound'),
            (2, 'COTTONSEED_OIL', 'Cents per pound'),
            (3, 'SUNFLOWER_OIL', 'Cents per pound'),
            (4, 'CANOLA_OIL', 'Cents per pound'),
            (5, 'PEANUT_OIL', 'Cents per pound'),
            (6, 'CORN_OIL', 'Cents per pound'),
            (7, 'LARD', 'Cents per pound'),
            (8, 'EDIBLE_TALLOW', 'Cents per pound'),
        ]

        current_my = None
        count = 0

        for idx, row in df.iterrows():
            if idx < 5:
                continue

            first_cell = row.iloc[0] if len(row) > 0 else None
            year_info = self._clean_year(first_cell)

            if year_info:
                my, flag = year_info
                has_data = any(pd.notna(row.iloc[i]) for i in range(1, 9) if i < len(row))

                if has_data:
                    for col_idx, commodity, unit in commodities:
                        price = self._clean_numeric(row.iloc[col_idx]) if len(row) > col_idx else None
                        if price:
                            self._insert_price(
                                commodity_code=commodity,
                                marketing_year=my,
                                period_type='ANNUAL',
                                price_type='WHOLESALE',
                                price=price,
                                unit_desc=unit,
                                forecast_flag=flag
                            )
                            count += 1
                else:
                    current_my = my
                continue

            month = self._is_month_row(first_cell)
            if month and current_my:
                for col_idx, commodity, unit in commodities:
                    price = self._clean_numeric(row.iloc[col_idx]) if len(row) > col_idx else None
                    if price:
                        self._insert_price(
                            commodity_code=commodity,
                            marketing_year=current_my,
                            period_type='MONTHLY',
                            period_detail=month,
                            price_type='WHOLESALE',
                            price=price,
                            unit_desc=unit
                        )
                        self.stats['monthly_price'] += 1

        self.conn.commit()
        logger.info(f"  Parsed Table 9: {count} annual prices")
        self.stats['price'] += count

    def parse_table10_meal_prices(self):
        """Parse Table 10: Oilseed meal prices."""
        logger.info("Parsing Table 10 (Meal Prices)...")

        df = pd.read_excel(self.xl, sheet_name='Table 10', header=None)

        commodities = [
            (1, 'SOYBEAN_MEAL', 'Dollars per short ton'),
            (2, 'COTTONSEED_MEAL', 'Dollars per short ton'),
            (3, 'SUNFLOWER_MEAL', 'Dollars per short ton'),
            (4, 'PEANUT_MEAL', 'Dollars per short ton'),
            (5, 'CANOLA_MEAL', 'Dollars per short ton'),
            (6, 'LINSEED_MEAL', 'Dollars per short ton'),
        ]

        current_my = None
        count = 0

        for idx, row in df.iterrows():
            if idx < 5:
                continue

            first_cell = row.iloc[0] if len(row) > 0 else None
            year_info = self._clean_year(first_cell)

            if year_info:
                my, flag = year_info
                has_data = any(pd.notna(row.iloc[i]) for i in range(1, 7) if i < len(row))

                if has_data:
                    for col_idx, commodity, unit in commodities:
                        price = self._clean_numeric(row.iloc[col_idx]) if len(row) > col_idx else None
                        if price:
                            self._insert_price(
                                commodity_code=commodity,
                                marketing_year=my,
                                period_type='ANNUAL',
                                price_type='WHOLESALE',
                                price=price,
                                unit_desc=unit,
                                forecast_flag=flag
                            )
                            count += 1
                else:
                    current_my = my
                continue

            month = self._is_month_row(first_cell)
            if month and current_my:
                for col_idx, commodity, unit in commodities:
                    price = self._clean_numeric(row.iloc[col_idx]) if len(row) > col_idx else None
                    if price:
                        self._insert_price(
                            commodity_code=commodity,
                            marketing_year=current_my,
                            period_type='MONTHLY',
                            period_detail=month,
                            price_type='WHOLESALE',
                            price=price,
                            unit_desc=unit
                        )
                        self.stats['monthly_price'] += 1

        self.conn.commit()
        logger.info(f"  Parsed Table 10: {count} annual prices")
        self.stats['price'] += count

    def _insert_balance(self, commodity_code: str, marketing_year: str, period_type: str,
                        period_detail: str = None, **kwargs):
        """Insert a balance sheet record."""
        # Filter out None values
        fields = {k: v for k, v in kwargs.items() if v is not None}

        columns = ['data_source', 'commodity_code', 'marketing_year', 'period_type', 'period_detail']
        values = [self.DATA_SOURCE, commodity_code, marketing_year, period_type, period_detail]

        for field, value in fields.items():
            columns.append(field)
            values.append(value)

        placeholders = ','.join(['?' for _ in values])
        column_str = ','.join(columns)

        try:
            self.conn.execute(f"""
                INSERT OR REPLACE INTO oilseed_monthly_balance ({column_str})
                VALUES ({placeholders})
            """, values)
        except sqlite3.Error as e:
            logger.warning(f"Error inserting balance: {e}")

    def _insert_price(self, commodity_code: str, marketing_year: str, period_type: str,
                      price_type: str, price: float, unit_desc: str,
                      period_detail: str = None, forecast_flag: str = None):
        """Insert a price record."""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO oilseed_monthly_price
                (data_source, commodity_code, marketing_year, period_type, period_detail,
                 price_type, price, unit_desc, forecast_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.DATA_SOURCE, commodity_code, marketing_year, period_type, period_detail,
                  price_type, price, unit_desc, forecast_flag))
        except sqlite3.Error as e:
            logger.warning(f"Error inserting price: {e}")

    def run(self):
        """Execute full ingestion."""
        logger.info(f"Starting Oil Crops Monthly ingestion from {self.filepath}")

        # Parse all tables
        self.parse_table1_soybeans()
        self.parse_table2_meal()
        self.parse_table3_oil()
        self.parse_tables47_cottonseed_peanuts()
        self.parse_table8_farm_prices()
        self.parse_table9_oil_prices()
        self.parse_table10_meal_prices()

        self.conn.close()

        logger.info("=" * 60)
        logger.info("Ingestion Summary:")
        logger.info(f"  Annual balance sheet records: {self.stats['balance_sheet']}")
        logger.info(f"  Monthly/quarterly balance records: {self.stats['monthly_balance']}")
        logger.info(f"  Annual price records: {self.stats['price']}")
        logger.info(f"  Monthly price records: {self.stats['monthly_price']}")
        logger.info("=" * 60)

        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Ingest Oil Crops Monthly Outlook data')
    parser.add_argument('--file', type=Path, default=DEFAULT_FILE,
                        help='Path to oiltables.xlsx file')
    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"File not found: {args.file}")
        return

    ingestor = OilCropsMonthlyIngestor(args.file)
    ingestor.run()


if __name__ == '__main__':
    main()
