#!/usr/bin/env python3
"""
Feed Grains Data Ingestion Script

Parses and ingests data from:
1. ERS Feed Grains Outlook Excel (36 sheets with prices, balance sheets, trade, industrial uses)
2. Census Trade Excel files (monthly corn imports/exports)
3. NASS Crop Production Annual Summary (text file with state-level data)

Usage:
    python scripts/ingest_feed_grains_data.py [--dry-run]
"""

import argparse
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# Project root
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "Models" / "Data"
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"


def get_db_connection():
    """Get SQLite database connection."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def parse_marketing_year(my_str: str) -> tuple:
    """Parse marketing year string like '2024/25' to (start_year, end_year)."""
    if not my_str or pd.isna(my_str):
        return None, None
    my_str = str(my_str).strip()
    match = re.match(r'(\d{4})[/-](\d{2,4})', my_str)
    if match:
        start = int(match.group(1))
        end_str = match.group(2)
        if len(end_str) == 2:
            end = int(str(start)[:2] + end_str)
        else:
            end = int(end_str)
        return start, end
    # Try single year
    match = re.match(r'^(\d{4})$', my_str)
    if match:
        return int(match.group(1)), int(match.group(1))
    return None, None


def clean_numeric(value) -> Optional[float]:
    """Clean a value to numeric, handling various formats."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    val_str = str(value).strip()
    if val_str in ['', 'NA', '(NA)', 'NaN', '-', '--']:
        return None
    # Remove commas and try to parse
    val_str = val_str.replace(',', '')
    try:
        return float(val_str)
    except ValueError:
        return None


def get_state_code(state_name: str) -> Optional[str]:
    """Map state name to code."""
    state_map = {
        'Alabama': 'US_AL', 'Alaska': 'US_AK', 'Arizona': 'US_AZ', 'Arkansas': 'US_AR',
        'California': 'US_CA', 'Colorado': 'US_CO', 'Connecticut': 'US_CT', 'Delaware': 'US_DE',
        'Florida': 'US_FL', 'Georgia': 'US_GA', 'Hawaii': 'US_HI', 'Idaho': 'US_ID',
        'Illinois': 'US_IL', 'Indiana': 'US_IN', 'Iowa': 'US_IA', 'Kansas': 'US_KS',
        'Kentucky': 'US_KY', 'Louisiana': 'US_LA', 'Maine': 'US_ME', 'Maryland': 'US_MD',
        'Massachusetts': 'US_MA', 'Michigan': 'US_MI', 'Minnesota': 'US_MN', 'Mississippi': 'US_MS',
        'Missouri': 'US_MO', 'Montana': 'US_MT', 'Nebraska': 'US_NE', 'Nevada': 'US_NV',
        'New Hampshire': 'US_NH', 'New Jersey': 'US_NJ', 'New Mexico': 'US_NM', 'New York': 'US_NY',
        'North Carolina': 'US_NC', 'North Dakota': 'US_ND', 'Ohio': 'US_OH', 'Oklahoma': 'US_OK',
        'Oregon': 'US_OR', 'Pennsylvania': 'US_PA', 'Rhode Island': 'US_RI', 'South Carolina': 'US_SC',
        'South Dakota': 'US_SD', 'Tennessee': 'US_TN', 'Texas': 'US_TX', 'Utah': 'US_UT',
        'Vermont': 'US_VT', 'Virginia': 'US_VA', 'Washington': 'US_WA', 'West Virginia': 'US_WV',
        'Wisconsin': 'US_WI', 'Wyoming': 'US_WY', 'United States': 'US'
    }
    # Clean state name
    clean_name = state_name.strip().rstrip('.').replace(' 1/', '').replace(' 2/', '')
    return state_map.get(clean_name)


def get_commodity_code(commodity_str: str) -> Optional[str]:
    """Map commodity string to code."""
    if pd.isna(commodity_str):
        return None
    comm = str(commodity_str).lower().strip()
    if 'corn' in comm:
        if 'silage' in comm:
            return 'CORN_SILAGE'
        return 'CORN'
    if 'sorghum' in comm:
        return 'SORGHUM'
    if 'barley' in comm:
        return 'BARLEY'
    if 'oat' in comm:
        return 'OATS'
    if 'soybean' in comm:
        return 'SOYBEANS'
    if 'hay' in comm:
        if 'alfalfa' in comm:
            return 'ALFALFA'
        return 'HAY'
    return None


class FeedGrainsIngestor:
    """Ingests ERS Feed Grains Outlook data."""

    def __init__(self, conn: sqlite3.Connection, dry_run: bool = False):
        self.conn = conn
        self.dry_run = dry_run
        self.stats = {'inserted': 0, 'updated': 0, 'skipped': 0}

    def ingest_all(self):
        """Ingest all data from the ERS Feed Grains file."""
        excel_path = DATA_DIR / "US Feed Grains Outlook - Dec 25.xlsx"
        if not excel_path.exists():
            print(f"ERROR: File not found: {excel_path}")
            return

        print(f"\n{'='*60}")
        print("INGESTING ERS FEED GRAINS DATA")
        print(f"{'='*60}")

        xl = pd.ExcelFile(excel_path)
        print(f"Found {len(xl.sheet_names)} sheets")

        # Ingest specific table types
        self.ingest_table01_acreage(xl)
        self.ingest_price_tables(xl)
        self.ingest_balance_sheet_tables(xl)
        self.ingest_industrial_use(xl)
        self.ingest_trade_tables(xl)

        print(f"\nStats: {self.stats}")

    def ingest_table01_acreage(self, xl: pd.ExcelFile):
        """Ingest Table 1: Acreage, production, yield, price."""
        print("\n--- Table 1: Acreage/Production/Yield ---")
        df = pd.read_excel(xl, 'FGYearbookTable01', header=None)

        current_commodity = None
        for idx, row in df.iterrows():
            # Skip header rows
            if idx < 2:
                continue

            # Check for commodity label
            if pd.notna(row[0]) and any(c in str(row[0]).lower() for c in ['corn', 'sorghum', 'barley', 'oat']):
                current_commodity = get_commodity_code(row[0])
                continue

            # Parse data row
            if current_commodity and pd.notna(row[1]):
                my_str = str(row[1]).strip()
                start_year, _ = parse_marketing_year(my_str)
                if not start_year:
                    continue

                area_planted = clean_numeric(row[2])
                area_harvested = clean_numeric(row[3])
                production = clean_numeric(row[4])
                yield_val = clean_numeric(row[5])
                price = clean_numeric(row[6])

                # Insert crop production
                if area_harvested or production:
                    self._insert_crop_production(
                        commodity_code=current_commodity,
                        location_code='US',
                        crop_year=start_year,
                        area_planted=area_planted,
                        area_harvested=area_harvested,
                        yield_per_acre=yield_val,
                        production=production
                    )

                # Insert farm price
                if price:
                    self._insert_farm_price(
                        commodity_code=current_commodity,
                        location_code='US',
                        marketing_year=my_str,
                        price=price,
                        is_annual=True
                    )

    def ingest_price_tables(self, xl: pd.ExcelFile):
        """Ingest Tables 9-14: Monthly prices."""
        print("\n--- Tables 9-14: Monthly Prices ---")

        # Table 9: Farm prices - Corn and Sorghum
        self._ingest_monthly_price_table(xl, 'FGYearbookTable09', 'farm')

        # Table 12: Cash prices - Corn at Central Illinois
        self._ingest_cash_price_table(xl, 'FGYearbookTable12')

    def _ingest_monthly_price_table(self, xl: pd.ExcelFile, sheet_name: str, price_source: str):
        """Generic monthly price ingestion."""
        df = pd.read_excel(xl, sheet_name, header=None)

        months = ['September', 'October', 'November', 'December', 'January',
                  'February', 'March', 'April', 'May', 'June', 'July', 'August']
        month_to_num = {m: i+1 for i, m in enumerate(months)}

        current_commodity = None
        for idx, row in df.iterrows():
            if idx < 2:
                continue

            # Check for commodity
            if pd.notna(row[0]) and 'dollars' in str(row[0]).lower():
                if 'corn' in str(row[0]).lower():
                    current_commodity = 'CORN'
                elif 'sorghum' in str(row[0]).lower():
                    current_commodity = 'SORGHUM'
                elif 'barley' in str(row[0]).lower():
                    current_commodity = 'BARLEY'
                elif 'oat' in str(row[0]).lower():
                    current_commodity = 'OATS'
                continue

            # Data row
            if current_commodity and pd.notna(row[1]):
                my_str = str(row[1]).strip()
                if '/' not in my_str:
                    continue

                # Monthly prices (columns 2-13)
                for i, month_name in enumerate(months):
                    col_idx = i + 2
                    if col_idx < len(row):
                        price = clean_numeric(row[col_idx])
                        if price:
                            # Calculate actual month number in marketing year
                            month_num = month_to_num[month_name]
                            self._insert_farm_price(
                                commodity_code=current_commodity,
                                location_code='US',
                                marketing_year=my_str,
                                price=price,
                                month=month_num,
                                is_annual=False
                            )

                # Annual average (last column usually)
                annual_price = clean_numeric(row[14]) if len(row) > 14 else None
                if annual_price:
                    self._insert_farm_price(
                        commodity_code=current_commodity,
                        location_code='US',
                        marketing_year=my_str,
                        price=annual_price,
                        is_annual=True
                    )

    def _ingest_cash_price_table(self, xl: pd.ExcelFile, sheet_name: str):
        """Ingest cash prices at markets."""
        df = pd.read_excel(xl, sheet_name, header=None)

        months = ['September', 'October', 'November', 'December', 'January',
                  'February', 'March', 'April', 'May', 'June', 'July', 'August']

        current_commodity = None
        current_market = None
        for idx, row in df.iterrows():
            if idx < 2:
                continue

            # Check for commodity/market
            row0 = str(row[0]) if pd.notna(row[0]) else ''
            if 'no. 2' in row0.lower() or 'central illinois' in row0.lower():
                current_commodity = 'CORN'
                current_market = 'Central Illinois'
                continue

            # Data row
            if current_commodity and current_market and pd.notna(row[1]):
                my_str = str(row[1]).strip()
                if '/' not in my_str:
                    continue

                # Monthly prices
                for i, month_name in enumerate(months):
                    col_idx = i + 2
                    if col_idx < len(row):
                        price = clean_numeric(row[col_idx])
                        if price:
                            # Determine year for this month
                            start_year, _ = parse_marketing_year(my_str)
                            if start_year:
                                # Sept-Dec is start year, Jan-Aug is next year
                                if i < 4:  # Sept-Dec
                                    price_year = start_year
                                else:  # Jan-Aug
                                    price_year = start_year + 1
                                month_num = [9, 10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8][i]
                                price_date = f"{price_year}-{month_num:02d}-01"

                                self._insert_cash_price(
                                    commodity_code=current_commodity,
                                    market_location=current_market,
                                    price_date=price_date,
                                    marketing_year=my_str,
                                    price=price
                                )

    def ingest_balance_sheet_tables(self, xl: pd.ExcelFile):
        """Ingest Tables 3-7: Supply and disappearance."""
        print("\n--- Tables 3-7: Balance Sheets ---")

        table_commodity_map = {
            'FGYearbookTable03': None,  # Feed grains aggregate
            'FGYearbookTable04': 'CORN',
            'FGYearbookTable05': 'SORGHUM',
            'FGYearbookTable06': 'BARLEY',
            'FGYearbookTable07': 'OATS',
        }

        for sheet_name, commodity in table_commodity_map.items():
            if commodity:  # Skip aggregate for now
                self._ingest_balance_sheet(xl, sheet_name, commodity)

    def _ingest_balance_sheet(self, xl: pd.ExcelFile, sheet_name: str, commodity_code: str):
        """Ingest a single balance sheet table."""
        df = pd.read_excel(xl, sheet_name, header=None)

        # Column mapping for balance sheet (based on Table 4 structure)
        col_map = {
            2: 'BEGINNING_STOCKS',
            3: 'PRODUCTION',
            4: 'IMPORTS',
            5: 'TOTAL_SUPPLY',
            6: 'FOOD_ALCOHOL_INDUSTRIAL',
            7: 'SEED',
            8: 'FEED_RESIDUAL',
            9: 'TOTAL_DOMESTIC',
            10: 'EXPORTS',
            11: 'TOTAL_USE',
            12: 'ENDING_STOCKS'
        }

        for idx, row in df.iterrows():
            if idx < 2:
                continue

            my_cell = row[0] if pd.notna(row[0]) else None
            quarter_cell = row[1] if pd.notna(row[1]) else None

            if not my_cell and not quarter_cell:
                continue

            # Get marketing year
            if pd.notna(my_cell) and '/' in str(my_cell):
                current_my = str(my_cell).strip()

            # Get quarter
            quarter = None
            if pd.notna(quarter_cell):
                q_str = str(quarter_cell)
                if 'MY' in q_str or 'Sep-Aug' in q_str:
                    quarter = 'MY'
                elif 'Q1' in q_str or 'Sep' in q_str:
                    quarter = 'Q1'
                elif 'Q2' in q_str or 'Dec' in q_str:
                    quarter = 'Q2'
                elif 'Q3' in q_str or 'Mar' in q_str:
                    quarter = 'Q3'
                elif 'Q4' in q_str or 'Jun' in q_str:
                    quarter = 'Q4'

            if not quarter:
                continue

            # Insert each balance sheet item
            for col_idx, item_type in col_map.items():
                if col_idx < len(row):
                    value = clean_numeric(row[col_idx])
                    if value is not None:
                        self._insert_balance_sheet_item(
                            commodity_code=commodity_code,
                            location_code='US',
                            item_type=item_type,
                            marketing_year=current_my if 'current_my' in dir() else None,
                            quarter=quarter,
                            value=value
                        )

    def ingest_industrial_use(self, xl: pd.ExcelFile):
        """Ingest Table 31: Corn industrial uses."""
        print("\n--- Table 31: Industrial Uses ---")
        df = pd.read_excel(xl, 'FGYearbookTable31', header=None)

        # Column mapping for industrial use categories
        col_map = {
            2: 'HFCS',
            3: 'GLUCOSE_DEXTROSE',
            4: 'STARCH',
            5: 'FUEL_ALCOHOL',
            6: 'BEVERAGE_ALCOHOL',
            7: 'CEREALS_OTHER',
            8: 'SEED',
            9: 'TOTAL_FSI'
        }

        current_my = None
        for idx, row in df.iterrows():
            if idx < 2:
                continue

            my_cell = row[0] if pd.notna(row[0]) else None
            quarter_cell = row[1] if pd.notna(row[1]) else None

            # Update marketing year
            if pd.notna(my_cell) and '/' in str(my_cell):
                current_my = str(my_cell).strip()
                continue

            if not current_my:
                continue

            # Get quarter
            quarter = None
            if pd.notna(quarter_cell):
                q_str = str(quarter_cell)
                if 'MY' in q_str:
                    quarter = 'MY'
                elif 'Q1' in q_str:
                    quarter = 'Q1'
                elif 'Q2' in q_str:
                    quarter = 'Q2'
                elif 'Q3' in q_str:
                    quarter = 'Q3'
                elif 'Q4' in q_str:
                    quarter = 'Q4'

            if not quarter:
                continue

            # Only process annual (MY) data for now
            if quarter != 'MY':
                continue

            # Insert each use category
            for col_idx, use_category in col_map.items():
                if col_idx < len(row):
                    value = clean_numeric(row[col_idx])
                    if value is not None:
                        self._insert_industrial_use(
                            commodity_code='CORN',
                            use_category=use_category,
                            marketing_year=current_my,
                            quarter=quarter,
                            quantity=value
                        )

    def ingest_trade_tables(self, xl: pd.ExcelFile):
        """Ingest Tables 18, 22: Trade data."""
        print("\n--- Tables 18, 22: Trade Data ---")

        # Table 18: Monthly exports (we'll extract annual totals)
        df = pd.read_excel(xl, 'FGYearbookTable18', header=None)

        current_commodity = None
        for idx, row in df.iterrows():
            if idx < 2:
                continue

            # Check for commodity
            row0 = str(row[0]) if pd.notna(row[0]) else ''
            if 'corn' in row0.lower():
                current_commodity = 'CORN'
                continue
            if 'sorghum' in row0.lower():
                current_commodity = 'SORGHUM'
                continue

            # Data row
            if current_commodity and pd.notna(row[1]):
                my_str = str(row[1]).strip()
                if '/' not in my_str:
                    continue

                # Annual total (column 14)
                annual_qty = clean_numeric(row[14]) if len(row) > 14 else None
                if annual_qty:
                    self._insert_trade_flow(
                        commodity_code=current_commodity,
                        location_code='WORLD',
                        flow_direction='EXPORT',
                        marketing_year=my_str,
                        quantity=annual_qty
                    )

    # Database insertion methods
    def _insert_crop_production(self, commodity_code, location_code, crop_year,
                                 area_planted, area_harvested, yield_per_acre, production):
        """Insert crop production record."""
        if self.dry_run:
            print(f"  [DRY-RUN] crop_production: {commodity_code} {location_code} {crop_year}")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO crop_production
                (commodity_code, location_code, crop_year, area_planted_acres,
                 area_harvested_acres, yield_per_acre, production, utilization_type, data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'GRAIN', 'USDA_ERS')
            """, (commodity_code, location_code, crop_year, area_planted,
                  area_harvested, yield_per_acre, production))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting crop_production: {e}")
            self.stats['skipped'] += 1

    def _insert_farm_price(self, commodity_code, location_code, marketing_year,
                           price, month=None, is_annual=False):
        """Insert farm price record."""
        if self.dry_run:
            print(f"  [DRY-RUN] farm_price: {commodity_code} {marketing_year} month={month} ${price}")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO farm_price
                (commodity_code, location_code, marketing_year, price_month, price,
                 is_annual_average, data_source)
                VALUES (?, ?, ?, ?, ?, ?, 'USDA_ERS')
            """, (commodity_code, location_code, marketing_year, month, price, is_annual))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting farm_price: {e}")
            self.stats['skipped'] += 1

    def _insert_cash_price(self, commodity_code, market_location, price_date,
                           marketing_year, price):
        """Insert cash price record."""
        if self.dry_run:
            print(f"  [DRY-RUN] cash_price: {commodity_code} {market_location} {price_date} ${price}")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO cash_price
                (commodity_code, market_location, price_date, marketing_year, price,
                 is_monthly_average, data_source)
                VALUES (?, ?, ?, ?, ?, 1, 'USDA_ERS')
            """, (commodity_code, market_location, price_date, marketing_year, price))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting cash_price: {e}")
            self.stats['skipped'] += 1

    def _insert_balance_sheet_item(self, commodity_code, location_code, item_type,
                                    marketing_year, quarter, value):
        """Insert balance sheet item."""
        if self.dry_run:
            print(f"  [DRY-RUN] balance_sheet: {commodity_code} {item_type} {marketing_year} {quarter} = {value}")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO balance_sheet_item
                (commodity_code, location_code, item_type, marketing_year, quarter, value, data_source)
                VALUES (?, ?, ?, ?, ?, ?, 'USDA_ERS')
            """, (commodity_code, location_code, item_type, marketing_year, quarter, value))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting balance_sheet_item: {e}")
            self.stats['skipped'] += 1

    def _insert_industrial_use(self, commodity_code, use_category, marketing_year,
                                quarter, quantity):
        """Insert industrial use record."""
        if self.dry_run:
            print(f"  [DRY-RUN] industrial_use: {commodity_code} {use_category} {marketing_year} = {quantity}")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO industrial_use
                (commodity_code, use_category, marketing_year, quarter, quantity, data_source)
                VALUES (?, ?, ?, ?, ?, 'USDA_ERS')
            """, (commodity_code, use_category, marketing_year, quarter, quantity))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting industrial_use: {e}")
            self.stats['skipped'] += 1

    def _insert_trade_flow(self, commodity_code, location_code, flow_direction,
                           marketing_year, quantity):
        """Insert trade flow record."""
        if self.dry_run:
            print(f"  [DRY-RUN] trade_flow: {commodity_code} {flow_direction} {marketing_year} = {quantity}")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO trade_flow
                (commodity_code, location_code, flow_direction, marketing_year, quantity_1000bu, data_source)
                VALUES (?, ?, ?, ?, ?, 'USDA_ERS')
            """, (commodity_code, location_code, flow_direction, marketing_year, quantity))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting trade_flow: {e}")
            self.stats['skipped'] += 1


class CensusTradeIngestor:
    """Ingests Census Bureau trade data from Excel files."""

    def __init__(self, conn: sqlite3.Connection, dry_run: bool = False):
        self.conn = conn
        self.dry_run = dry_run
        self.stats = {'inserted': 0, 'skipped': 0}

    def ingest_all(self):
        """Ingest all Census trade files."""
        print(f"\n{'='*60}")
        print("INGESTING CENSUS TRADE DATA")
        print(f"{'='*60}")

        # Process exports
        exports_file = DATA_DIR / "US Corn Exports - 01201025.xlsx"
        if exports_file.exists():
            self._ingest_trade_file(exports_file, 'EXPORT')

        # Process imports
        imports_file = DATA_DIR / "US Corn Imports - 01201025.xlsx"
        if imports_file.exists():
            self._ingest_trade_file(imports_file, 'IMPORT')

        print(f"\nStats: {self.stats}")

    def _ingest_trade_file(self, filepath: Path, flow: str):
        """Ingest a single trade Excel file."""
        print(f"\n--- Processing {filepath.name} ({flow}) ---")

        xl = pd.ExcelFile(filepath)
        df = pd.read_excel(xl, xl.sheet_names[0], header=None)

        months = ['January', 'February', 'March', 'April', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December']

        for idx, row in df.iterrows():
            if idx < 5:  # Skip headers
                continue

            # Check for data row (World Total or specific country)
            partner = str(row[1]) if pd.notna(row[1]) else ''
            if not partner or partner == 'Partner':
                continue

            hs_code = str(row[3]) if pd.notna(row[3]) else ''
            product = str(row[4]) if pd.notna(row[4]) else ''
            year_str = str(row[5]) if pd.notna(row[5]) else ''

            # Parse year from format like '2020-2020'
            if '-' in year_str:
                year = int(year_str.split('-')[0])
            else:
                continue

            # Monthly quantities (columns 7-18)
            for month_idx, month_name in enumerate(months):
                col_idx = 7 + month_idx
                if col_idx < len(row):
                    qty = clean_numeric(row[col_idx])
                    if qty is not None and qty > 0:
                        self._insert_monthly_trade(
                            year=year,
                            month=month_idx + 1,
                            flow=flow,
                            hs_code=hs_code,
                            partner_name=partner,
                            quantity_mt=qty
                        )

    def _insert_monthly_trade(self, year, month, flow, hs_code, partner_name, quantity_mt):
        """Insert monthly trade record."""
        if self.dry_run:
            print(f"  [DRY-RUN] census_trade: {flow} {year}-{month:02d} {partner_name} {quantity_mt} MT")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO census_trade_monthly
                (year, month, flow, hs_code, partner_name, quantity_mt, commodity_code)
                VALUES (?, ?, ?, ?, ?, ?, 'CORN')
            """, (year, month, flow, hs_code, partner_name, quantity_mt))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting census_trade: {e}")
            self.stats['skipped'] += 1


class NASSCropIngestor:
    """Ingests NASS Crop Production data from text file."""

    def __init__(self, conn: sqlite3.Connection, dry_run: bool = False):
        self.conn = conn
        self.dry_run = dry_run
        self.stats = {'inserted': 0, 'skipped': 0}

    def ingest_all(self):
        """Ingest NASS Crop Production Annual Summary."""
        print(f"\n{'='*60}")
        print("INGESTING NASS CROP PRODUCTION DATA")
        print(f"{'='*60}")

        txt_file = DATA_DIR / "Crop Production Annual Summary.txt"
        if not txt_file.exists():
            print(f"ERROR: File not found: {txt_file}")
            return

        with open(txt_file, 'r', encoding='latin-1') as f:
            content = f.read()

        # Parse corn data section
        self._parse_corn_section(content)

        print(f"\nStats: {self.stats}")

    def _parse_corn_section(self, content: str):
        """Parse corn area/yield/production table."""
        print("\n--- Parsing Corn Production Data ---")

        lines = content.split('\n')
        in_corn_table = False
        years = []

        for i, line in enumerate(lines):
            # Find corn table start
            if 'Corn Area Planted for All Purposes' in line and 'Harvested for Grain' in line:
                in_corn_table = True
                # Years should be in header (2023, 2024, 2025)
                years = [2023, 2024, 2025]
                continue

            # Check for end of table
            if in_corn_table and ('See footnote' in line or line.strip().startswith('(')):
                in_corn_table = False
                continue

            # Parse state rows
            if in_corn_table and ':' in line:
                parts = line.split(':')
                if len(parts) >= 2:
                    state_name = parts[0].strip().rstrip('.')
                    state_code = get_state_code(state_name)

                    if state_code and state_code != 'US':
                        # Parse values from the rest of the line
                        values_part = ':'.join(parts[1:])
                        values = re.findall(r'[\d,]+(?:\.\d+)?', values_part)

                        # Extract planted acres for each year (first 3 values in grain section)
                        if len(values) >= 6:
                            for j, year in enumerate(years):
                                try:
                                    # Area harvested for grain
                                    harvested = float(values[j+3].replace(',', '')) if values[j+3] else None

                                    if harvested:
                                        self._insert_crop_production(
                                            commodity_code='CORN',
                                            location_code=state_code,
                                            crop_year=year,
                                            area_harvested=harvested * 1000,  # Convert 1000 acres to acres
                                            utilization_type='GRAIN'
                                        )
                                except (ValueError, IndexError):
                                    pass

    def _insert_crop_production(self, commodity_code, location_code, crop_year,
                                 area_harvested, utilization_type):
        """Insert crop production record."""
        if self.dry_run:
            print(f"  [DRY-RUN] nass_crop: {commodity_code} {location_code} {crop_year} {area_harvested} acres")
            return

        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO nass_crop_production
                (commodity_code, location_code, crop_year, area_harvested_acres,
                 utilization_type, data_source)
                VALUES (?, ?, ?, ?, ?, 'USDA_NASS')
            """, (commodity_code, location_code, crop_year, area_harvested, utilization_type))
            self.stats['inserted'] += 1
        except Exception as e:
            print(f"  ERROR inserting nass_crop: {e}")
            self.stats['skipped'] += 1


def create_sqlite_tables(conn: sqlite3.Connection):
    """Create simplified SQLite tables for the data."""
    print("\n--- Creating SQLite tables ---")

    # Crop production table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crop_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_code TEXT NOT NULL,
            location_code TEXT NOT NULL,
            crop_year INTEGER NOT NULL,
            area_planted_acres REAL,
            area_harvested_acres REAL,
            yield_per_acre REAL,
            production REAL,
            utilization_type TEXT DEFAULT 'GRAIN',
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, location_code, crop_year, utilization_type)
        )
    """)

    # Farm price table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS farm_price (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_code TEXT NOT NULL,
            location_code TEXT NOT NULL,
            marketing_year TEXT NOT NULL,
            price_month INTEGER DEFAULT 0,
            price REAL NOT NULL,
            is_annual_average INTEGER DEFAULT 0,
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, location_code, marketing_year, price_month, is_annual_average)
        )
    """)

    # Cash price table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cash_price (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_code TEXT NOT NULL,
            market_location TEXT NOT NULL,
            price_date TEXT NOT NULL,
            marketing_year TEXT,
            price REAL NOT NULL,
            is_monthly_average INTEGER DEFAULT 0,
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, market_location, price_date)
        )
    """)

    # Balance sheet item table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS balance_sheet_item (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_code TEXT NOT NULL,
            location_code TEXT NOT NULL,
            item_type TEXT NOT NULL,
            marketing_year TEXT NOT NULL,
            quarter TEXT DEFAULT 'MY',
            value REAL,
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, location_code, item_type, marketing_year, quarter)
        )
    """)

    # Industrial use table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS industrial_use (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_code TEXT NOT NULL,
            use_category TEXT NOT NULL,
            marketing_year TEXT NOT NULL,
            quarter TEXT DEFAULT 'MY',
            quantity REAL,
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, use_category, marketing_year, quarter)
        )
    """)

    # Trade flow table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trade_flow (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_code TEXT NOT NULL,
            location_code TEXT NOT NULL,
            flow_direction TEXT NOT NULL,
            marketing_year TEXT NOT NULL,
            quantity_1000bu REAL,
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, location_code, flow_direction, marketing_year)
        )
    """)

    # Census monthly trade table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS census_trade_monthly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            flow TEXT NOT NULL,
            hs_code TEXT,
            partner_name TEXT,
            quantity_mt REAL,
            commodity_code TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(year, month, flow, partner_name)
        )
    """)

    # NASS crop production table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nass_crop_production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commodity_code TEXT NOT NULL,
            location_code TEXT NOT NULL,
            crop_year INTEGER NOT NULL,
            area_harvested_acres REAL,
            utilization_type TEXT,
            data_source TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(commodity_code, location_code, crop_year, utilization_type)
        )
    """)

    conn.commit()
    print("Tables created successfully")


def main():
    parser = argparse.ArgumentParser(description='Ingest Feed Grains data')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without executing')
    args = parser.parse_args()

    print(f"Feed Grains Data Ingestion")
    print(f"Data directory: {DATA_DIR}")
    print(f"Database: {DB_PATH}")
    print(f"Dry run: {args.dry_run}")

    # List available files
    print(f"\nAvailable data files:")
    for f in DATA_DIR.glob('*'):
        print(f"  - {f.name}")

    # Connect to database
    conn = get_db_connection()

    # Create tables
    create_sqlite_tables(conn)

    # Run ingestors
    fg_ingestor = FeedGrainsIngestor(conn, dry_run=args.dry_run)
    fg_ingestor.ingest_all()

    census_ingestor = CensusTradeIngestor(conn, dry_run=args.dry_run)
    census_ingestor.ingest_all()

    nass_ingestor = NASSCropIngestor(conn, dry_run=args.dry_run)
    nass_ingestor.ingest_all()

    # Commit and close
    if not args.dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print("INGESTION COMPLETE")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
