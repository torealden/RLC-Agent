#!/usr/bin/env python3
"""
Oil Crops Data Ingestion Script

Parses and ingests data from the ERS Oil Crops Yearbook CSV file.
This file contains 43 tables covering:
- Oilseeds (soybeans, peanuts, sunflower, cottonseed, canola, flaxseed)
- Fats & Oils (soybean oil, corn oil, canola oil, palm oil, tallow, lard)
- Oilmeals (soybean meal, cottonseed meal, canola meal)
- Price indexes

Data Source: https://www.ers.usda.gov/data-products/oil-crops-yearbook/

Usage:
    python scripts/ingest_oil_crops_data.py [--dry-run]
"""

import argparse
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


def clean_numeric(value) -> Optional[float]:
    """Clean a value to numeric."""
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    val_str = str(value).strip()
    if val_str in ['', 'NA', '(NA)', 'NaN', '-', '--', '(D)', '(S)']:
        return None
    val_str = val_str.replace(',', '')
    try:
        return float(val_str)
    except ValueError:
        return None


def get_commodity_code(commodity: str) -> Optional[str]:
    """Map commodity name to standardized code."""
    if pd.isna(commodity):
        return None

    comm = str(commodity).lower().strip()

    # Oilseeds
    if 'soybean' in comm and 'oil' not in comm and 'meal' not in comm and 'hull' not in comm:
        return 'SOYBEANS'
    if 'peanut' in comm and 'oil' not in comm and 'meal' not in comm and 'butter' not in comm:
        return 'PEANUTS'
    if 'sunflower' in comm and 'oil' not in comm and 'meal' not in comm:
        if 'nonoil' in comm:
            return 'SUNFLOWER_NONOIL'
        elif 'oil type' in comm:
            return 'SUNFLOWER_OIL_TYPE'
        return 'SUNFLOWER'
    if 'cottonseed' in comm and 'oil' not in comm and 'meal' not in comm:
        return 'COTTONSEED'
    if 'canola' in comm and 'oil' not in comm and 'meal' not in comm:
        return 'CANOLA'
    if 'flaxseed' in comm or 'linseed' in comm:
        if 'oil' in comm:
            return 'LINSEED_OIL'
        if 'meal' in comm:
            return 'LINSEED_MEAL'
        return 'FLAXSEED'
    if 'rapeseed' in comm and 'oil' not in comm and 'meal' not in comm:
        return 'RAPESEED'
    if 'copra' in comm and 'meal' not in comm:
        return 'COPRA'
    if 'palm kernel' in comm and 'oil' not in comm and 'meal' not in comm:
        return 'PALM_KERNEL'

    # Oils
    if 'soybean oil' in comm:
        return 'SOYBEAN_OIL'
    if 'corn oil' in comm:
        return 'CORN_OIL'
    if 'canola oil' in comm:
        return 'CANOLA_OIL'
    if 'sunflower' in comm and 'oil' in comm:
        return 'SUNFLOWER_OIL'
    if 'cottonseed oil' in comm:
        return 'COTTONSEED_OIL'
    if 'palm oil' in comm and 'kernel' not in comm:
        return 'PALM_OIL'
    if 'palm kernel oil' in comm:
        return 'PALM_KERNEL_OIL'
    if 'coconut oil' in comm:
        return 'COCONUT_OIL'
    if 'peanut oil' in comm:
        return 'PEANUT_OIL'
    if 'olive oil' in comm:
        return 'OLIVE_OIL'
    if 'safflower oil' in comm:
        return 'SAFFLOWER_OIL'
    if 'sesame oil' in comm:
        return 'SESAME_OIL'
    if 'linseed oil' in comm:
        return 'LINSEED_OIL'
    if 'tallow' in comm:
        return 'TALLOW'
    if 'lard' in comm:
        return 'LARD'
    if 'yellow grease' in comm:
        return 'YELLOW_GREASE'
    if 'distillers corn oil' in comm:
        return 'DCO'

    # Meals
    if 'soybean meal' in comm:
        return 'SOYBEAN_MEAL'
    if 'soybean hull' in comm:
        return 'SOYBEAN_HULLS'
    if 'cottonseed meal' in comm:
        return 'COTTONSEED_MEAL'
    if 'canola meal' in comm:
        return 'CANOLA_MEAL'
    if 'sunflower' in comm and 'meal' in comm:
        return 'SUNFLOWER_MEAL'
    if 'peanut meal' in comm:
        return 'PEANUT_MEAL'
    if 'copra meal' in comm:
        return 'COPRA_MEAL'
    if 'palm kernel meal' in comm:
        return 'PALM_KERNEL_MEAL'
    if 'fish meal' in comm:
        return 'FISH_MEAL'
    if 'rapeseed meal' in comm:
        return 'RAPESEED_MEAL'

    # Processed products
    if 'peanut butter' in comm:
        return 'PEANUT_BUTTER'
    if 'margarine' in comm:
        return 'MARGARINE'
    if 'shortening' in comm:
        return 'SHORTENING'
    if 'biodiesel' in comm:
        return 'BIODIESEL'

    # Aggregates
    if 'total oilseed' in comm:
        return 'TOTAL_OILSEEDS'
    if 'total vegetable oil' in comm:
        return 'TOTAL_VEG_OILS'
    if 'total protein meal' in comm:
        return 'TOTAL_MEALS'
    if 'edible fats' in comm:
        return 'EDIBLE_FATS_OILS'
    if 'inedible fats' in comm:
        return 'INEDIBLE_FATS_OILS'
    if 'animal fats' in comm:
        return 'ANIMAL_FATS'

    return None


def get_attribute_type(attr: str) -> Optional[str]:
    """Map attribute description to standardized type."""
    if pd.isna(attr):
        return None

    attr = str(attr).lower().strip()

    # Supply/Demand
    if 'production' in attr:
        return 'PRODUCTION'
    if 'beginning stock' in attr:
        return 'BEGINNING_STOCKS'
    if 'ending stock' in attr:
        return 'ENDING_STOCKS'
    if 'import' in attr:
        return 'IMPORTS'
    if 'export' in attr:
        return 'EXPORTS'
    if 'total supply' in attr:
        return 'TOTAL_SUPPLY'
    if 'total disappearance' in attr or 'total domestic' in attr:
        return 'TOTAL_USE'
    if 'domestic disappearance' in attr:
        return 'DOMESTIC_USE'
    if 'crush' in attr:
        return 'CRUSH'
    if 'seed' in attr and 'feed' in attr:
        return 'SEED_FEED_RESIDUAL'
    if 'biofuel' in attr:
        return 'BIOFUEL_USE'
    if 'consumption' in attr:
        return 'CONSUMPTION'

    # Acreage/Yield
    if 'planted acre' in attr:
        return 'AREA_PLANTED'
    if 'harvested acre' in attr:
        return 'AREA_HARVESTED'
    if 'yield' in attr:
        return 'YIELD'

    # Prices
    if 'wholesale price' in attr:
        return 'PRICE_WHOLESALE'
    if 'received' in attr and 'farmer' in attr:
        return 'PRICE_FARM'
    if 'season-average price' in attr:
        return 'PRICE_SEASON_AVG'
    if 'cash price' in attr:
        return 'PRICE_CASH'
    if 'loan rate' in attr:
        return 'LOAN_RATE'
    if 'price spread' in attr:
        return 'PRICE_SPREAD'
    if 'price' in attr:
        return 'PRICE'

    # Storage
    if 'on-farm storage' in attr:
        return 'STOCKS_ONFARM'
    if 'off-farm storage' in attr:
        return 'STOCKS_OFFFARM'
    if 'total storage' in attr:
        return 'STOCKS_TOTAL'

    # Value
    if 'value' in attr:
        return 'VALUE'

    # Price indexes
    if 'price index' in attr or 'bureau of labor' in attr:
        return 'PRICE_INDEX'

    return attr.upper().replace(' ', '_').replace(',', '').replace('-', '_')[:50]


def get_location_code(geography: str) -> str:
    """Map geography description to location code."""
    if pd.isna(geography):
        return 'US'

    geo = str(geography).strip()

    # Countries/Regions
    geo_map = {
        'United States': 'US',
        'World': 'WORLD',
        'Argentina': 'AR',
        'Brazil': 'BR',
        'China': 'CN',
        'European Union': 'EU',
        'India': 'IN',
        'Canada': 'CA',
        'Mexico': 'MX',
        'Japan': 'JP',
        'South Korea': 'KR',
        'Taiwan': 'TW',
        'Indonesia': 'ID',
        'Malaysia': 'MY',
        'Thailand': 'TH',
        'Vietnam': 'VN',
        'Philippines': 'PH',
        'Egypt': 'EG',
        'Turkey': 'TR',
        'Pakistan': 'PK',
        'Bangladesh': 'BD',
        'Nigeria': 'NG',
        'South Africa': 'ZA',
        'Russia': 'RU',
        'Ukraine': 'UA',
        'Paraguay': 'PY',
        'Bolivia': 'BO',
        'Australia': 'AU',
    }

    if geo in geo_map:
        return geo_map[geo]

    # US Regions
    if 'Southeast' in geo:
        return 'US_SOUTHEAST'
    if 'Southwest' in geo:
        return 'US_SOUTHWEST'
    if 'Virginia' in geo and 'Carolina' in geo:
        return 'US_VA_NC'

    # Return cleaned version for unknown
    return geo.upper().replace(' ', '_').replace('/', '_')[:20]


class OilCropsIngestor:
    """Ingests ERS Oil Crops Yearbook data."""

    def __init__(self, conn: sqlite3.Connection, dry_run: bool = False):
        self.conn = conn
        self.dry_run = dry_run
        self.stats = {
            'bronze_inserted': 0,
            'prices_inserted': 0,
            'balance_sheet_inserted': 0,
            'production_inserted': 0,
            'trade_inserted': 0,
            'skipped': 0
        }

    def create_tables(self):
        """Create oil crops specific tables."""

        # Bronze table for raw oil crops data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oil_crops_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                -- Derived fields
                commodity_code TEXT,
                attribute_type TEXT,
                location_code TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(marketing_year, commodity, attribute_desc, geography_desc, timeperiod_desc)
            )
        """)

        # Oilseed production table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oilseed_production (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                crop_year INTEGER,
                marketing_year TEXT,
                area_planted REAL,
                area_planted_unit TEXT,
                area_harvested REAL,
                area_harvested_unit TEXT,
                yield_per_acre REAL,
                yield_unit TEXT,
                production REAL,
                production_unit TEXT,
                value REAL,
                value_unit TEXT,
                loan_rate REAL,
                loan_rate_unit TEXT,
                data_source TEXT DEFAULT 'USDA_ERS_OILCROPS',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year)
            )
        """)

        # Oilseed/oil/meal balance sheet
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oilseed_balance_sheet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                period TEXT DEFAULT 'MY',  -- 'MY', 'Q1', 'Q2', etc., or month
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                crush REAL,
                exports REAL,
                domestic_use REAL,
                seed_feed_residual REAL,
                biofuel_use REAL,
                total_use REAL,
                ending_stocks REAL,
                unit_desc TEXT,
                data_source TEXT DEFAULT 'USDA_ERS_OILCROPS',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year, period)
            )
        """)

        # Oilseed prices
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oilseed_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT,
                calendar_year INTEGER,
                price_month TEXT,
                price_type TEXT NOT NULL,  -- 'WHOLESALE', 'FARM', 'CASH', 'SEASON_AVG'
                price REAL NOT NULL,
                unit_desc TEXT,
                data_source TEXT DEFAULT 'USDA_ERS_OILCROPS',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year, price_type, price_month)
            )
        """)

        self.conn.commit()
        print("Oil crops tables created/verified")

    def ingest_csv(self, filepath: Path):
        """Ingest the Oil Crops CSV file."""
        print(f"\n{'='*60}")
        print("INGESTING OIL CROPS DATA")
        print(f"{'='*60}")

        df = pd.read_csv(filepath)
        print(f"Loaded {len(df):,} rows from CSV")

        # First pass: Load into bronze table
        print("\n--- Loading into bronze table ---")
        self._load_bronze(df)

        # Second pass: Transform into silver tables
        print("\n--- Transforming to silver tables ---")
        self._transform_production(df)
        self._transform_balance_sheets(df)
        self._transform_prices(df)

        print(f"\nStats: {self.stats}")

    def _load_bronze(self, df: pd.DataFrame):
        """Load raw data into bronze table."""
        for idx, row in df.iterrows():
            commodity_code = get_commodity_code(row['Commodity'])
            attribute_type = get_attribute_type(row['Attribute_Desc'])
            location_code = get_location_code(row['Geography_Desc'])
            amount = clean_numeric(row['Amount'])

            if self.dry_run:
                if idx < 5:
                    print(f"  [DRY-RUN] {row['Commodity']} -> {commodity_code}, {row['Attribute_Desc']} -> {attribute_type}")
                continue

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO oil_crops_raw
                    (timeperiod_desc, marketing_year, my_definition, commodity_group,
                     commodity, commodity_desc2, attribute_desc, attribute_desc2,
                     geography_desc, geography_desc2, amount, unit_desc,
                     table_number, table_name, commodity_code, attribute_type, location_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row['Timeperiod_Desc'], row['Marketing_Year'], row['MY_Definition'],
                    row['Commodity_Group'], row['Commodity'], row['Commodity_Desc2'],
                    row['Attribute_Desc'], row['Attribute_Desc2'],
                    row['Geography_Desc'], row['Geography_Desc2'],
                    amount, row['Unit_Desc'],
                    row['Table_number'], row['Table_name'],
                    commodity_code, attribute_type, location_code
                ))
                self.stats['bronze_inserted'] += 1
            except Exception as e:
                self.stats['skipped'] += 1

        self.conn.commit()
        print(f"  Bronze: {self.stats['bronze_inserted']:,} rows")

    def _transform_production(self, df: pd.DataFrame):
        """Transform production/acreage data."""
        # Filter to production-related attributes
        prod_attrs = ['Production', 'Planted acres', 'Harvested acres', 'Yield', 'Value', 'Loan rate']
        prod_df = df[df['Attribute_Desc'].isin(prod_attrs)]

        # Group by commodity, geography, marketing year
        for (commodity, geography, my), group in prod_df.groupby(['Commodity', 'Geography_Desc', 'Marketing_Year']):
            commodity_code = get_commodity_code(commodity)
            location_code = get_location_code(geography)

            if not commodity_code:
                continue

            # Build production record
            record = {
                'commodity_code': commodity_code,
                'location_code': location_code,
                'marketing_year': my
            }

            for _, row in group.iterrows():
                attr = row['Attribute_Desc']
                amount = clean_numeric(row['Amount'])
                unit = row['Unit_Desc']

                if attr == 'Production':
                    record['production'] = amount
                    record['production_unit'] = unit
                elif attr == 'Planted acres':
                    record['area_planted'] = amount
                    record['area_planted_unit'] = unit
                elif attr == 'Harvested acres':
                    record['area_harvested'] = amount
                    record['area_harvested_unit'] = unit
                elif attr == 'Yield':
                    record['yield_per_acre'] = amount
                    record['yield_unit'] = unit
                elif attr == 'Value':
                    record['value'] = amount
                    record['value_unit'] = unit
                elif attr == 'Loan rate':
                    record['loan_rate'] = amount
                    record['loan_rate_unit'] = unit

            if self.dry_run:
                continue

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO oilseed_production
                    (commodity_code, location_code, marketing_year,
                     area_planted, area_planted_unit, area_harvested, area_harvested_unit,
                     yield_per_acre, yield_unit, production, production_unit,
                     value, value_unit, loan_rate, loan_rate_unit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get('commodity_code'), record.get('location_code'),
                    record.get('marketing_year'),
                    record.get('area_planted'), record.get('area_planted_unit'),
                    record.get('area_harvested'), record.get('area_harvested_unit'),
                    record.get('yield_per_acre'), record.get('yield_unit'),
                    record.get('production'), record.get('production_unit'),
                    record.get('value'), record.get('value_unit'),
                    record.get('loan_rate'), record.get('loan_rate_unit')
                ))
                self.stats['production_inserted'] += 1
            except Exception as e:
                self.stats['skipped'] += 1

        self.conn.commit()
        print(f"  Production: {self.stats['production_inserted']:,} rows")

    def _transform_balance_sheets(self, df: pd.DataFrame):
        """Transform supply/disappearance data into balance sheets."""
        # Balance sheet attributes
        bs_attrs = ['Beginning stocks', 'Production', 'Imports', 'Total supply',
                    'Crush', 'Exports', 'Total disappearance', 'Total domestic use',
                    'Domestic disappearance', 'Seed, feed, and residual use',
                    'Domestic use, Biofuel', 'Ending stocks']

        bs_df = df[df['Attribute_Desc'].isin(bs_attrs)]

        # Group by commodity, geography, marketing year
        for (commodity, geography, my), group in bs_df.groupby(['Commodity', 'Geography_Desc', 'Marketing_Year']):
            commodity_code = get_commodity_code(commodity)
            location_code = get_location_code(geography)

            if not commodity_code:
                continue

            record = {
                'commodity_code': commodity_code,
                'location_code': location_code,
                'marketing_year': my,
                'period': 'MY'
            }

            unit_desc = None
            for _, row in group.iterrows():
                attr = row['Attribute_Desc']
                amount = clean_numeric(row['Amount'])
                if unit_desc is None:
                    unit_desc = row['Unit_Desc']

                if 'Beginning stock' in attr:
                    record['beginning_stocks'] = amount
                elif attr == 'Production':
                    record['production'] = amount
                elif attr == 'Imports':
                    record['imports'] = amount
                elif 'Total supply' in attr:
                    record['total_supply'] = amount
                elif attr == 'Crush':
                    record['crush'] = amount
                elif attr == 'Exports':
                    record['exports'] = amount
                elif 'Seed, feed' in attr:
                    record['seed_feed_residual'] = amount
                elif 'Biofuel' in attr:
                    record['biofuel_use'] = amount
                elif 'Total disappearance' in attr or 'Total domestic' in attr:
                    record['total_use'] = amount
                elif 'Domestic disappearance' in attr:
                    record['domestic_use'] = amount
                elif 'Ending stock' in attr:
                    record['ending_stocks'] = amount

            record['unit_desc'] = unit_desc

            if self.dry_run:
                continue

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO oilseed_balance_sheet
                    (commodity_code, location_code, marketing_year, period,
                     beginning_stocks, production, imports, total_supply,
                     crush, exports, domestic_use, seed_feed_residual,
                     biofuel_use, total_use, ending_stocks, unit_desc)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get('commodity_code'), record.get('location_code'),
                    record.get('marketing_year'), record.get('period'),
                    record.get('beginning_stocks'), record.get('production'),
                    record.get('imports'), record.get('total_supply'),
                    record.get('crush'), record.get('exports'),
                    record.get('domestic_use'), record.get('seed_feed_residual'),
                    record.get('biofuel_use'), record.get('total_use'),
                    record.get('ending_stocks'), record.get('unit_desc')
                ))
                self.stats['balance_sheet_inserted'] += 1
            except Exception as e:
                self.stats['skipped'] += 1

        self.conn.commit()
        print(f"  Balance Sheets: {self.stats['balance_sheet_inserted']:,} rows")

    def _transform_prices(self, df: pd.DataFrame):
        """Transform price data."""
        price_attrs = ['Wholesale prices', 'Received by U.S farmers',
                       'Season-average price received by farmers',
                       'Cash prices at terminal markets', 'Price', 'Loan rate']

        price_df = df[df['Attribute_Desc'].isin(price_attrs)]

        for idx, row in price_df.iterrows():
            commodity_code = get_commodity_code(row['Commodity'])
            location_code = get_location_code(row['Geography_Desc'])

            if not commodity_code:
                continue

            # Determine price type
            attr = row['Attribute_Desc']
            if 'Wholesale' in attr:
                price_type = 'WHOLESALE'
            elif 'farmer' in attr.lower():
                price_type = 'FARM'
            elif 'Season-average' in attr:
                price_type = 'SEASON_AVG'
            elif 'Cash' in attr:
                price_type = 'CASH'
            elif 'Loan rate' in attr:
                price_type = 'LOAN_RATE'
            else:
                price_type = 'OTHER'

            amount = clean_numeric(row['Amount'])
            if amount is None:
                continue

            if self.dry_run:
                continue

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO oilseed_price
                    (commodity_code, location_code, marketing_year, price_type,
                     price_month, price, unit_desc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    commodity_code, location_code, row['Marketing_Year'],
                    price_type, row['Timeperiod_Desc'], amount, row['Unit_Desc']
                ))
                self.stats['prices_inserted'] += 1
            except Exception as e:
                self.stats['skipped'] += 1

        self.conn.commit()
        print(f"  Prices: {self.stats['prices_inserted']:,} rows")


def create_oilseed_commodities(conn: sqlite3.Connection):
    """Create commodity entries for oilseeds."""
    commodities = [
        # Oilseeds
        ('SOYBEANS', 'Soybeans', 'OILSEED', 'BU', 60.0, 9),
        ('PEANUTS', 'Peanuts', 'OILSEED', 'LB', None, 8),
        ('SUNFLOWER', 'Sunflowerseed', 'OILSEED', 'LB', None, 9),
        ('COTTONSEED', 'Cottonseed', 'OILSEED', 'ST', None, 8),
        ('CANOLA', 'Canola', 'OILSEED', 'LB', None, 9),
        ('FLAXSEED', 'Flaxseed', 'OILSEED', 'BU', 56.0, 9),
        ('RAPESEED', 'Rapeseed', 'OILSEED', 'MT', None, 7),
        # Oils
        ('SOYBEAN_OIL', 'Soybean Oil', 'VEG_OIL', 'LB', None, 10),
        ('CORN_OIL', 'Corn Oil', 'VEG_OIL', 'LB', None, 10),
        ('CANOLA_OIL', 'Canola Oil', 'VEG_OIL', 'LB', None, 10),
        ('SUNFLOWER_OIL', 'Sunflower Oil', 'VEG_OIL', 'LB', None, 10),
        ('COTTONSEED_OIL', 'Cottonseed Oil', 'VEG_OIL', 'LB', None, 10),
        ('PALM_OIL', 'Palm Oil', 'VEG_OIL', 'MT', None, 10),
        ('COCONUT_OIL', 'Coconut Oil', 'VEG_OIL', 'MT', None, 10),
        ('PEANUT_OIL', 'Peanut Oil', 'VEG_OIL', 'LB', None, 10),
        ('OLIVE_OIL', 'Olive Oil', 'VEG_OIL', 'MT', None, 10),
        # Meals
        ('SOYBEAN_MEAL', 'Soybean Meal', 'OILMEAL', 'ST', None, 10),
        ('SOYBEAN_HULLS', 'Soybean Hulls', 'OILMEAL', 'ST', None, 10),
        ('CANOLA_MEAL', 'Canola Meal', 'OILMEAL', 'ST', None, 10),
        ('COTTONSEED_MEAL', 'Cottonseed Meal', 'OILMEAL', 'ST', None, 10),
        ('SUNFLOWER_MEAL', 'Sunflower Meal', 'OILMEAL', 'ST', None, 10),
        # Animal Fats
        ('TALLOW', 'Tallow (Edible)', 'ANIMAL_FAT', 'LB', None, 10),
        ('LARD', 'Lard', 'ANIMAL_FAT', 'LB', None, 10),
        ('YELLOW_GREASE', 'Yellow Grease', 'ANIMAL_FAT', 'LB', None, 10),
    ]

    conn.execute("""
        CREATE TABLE IF NOT EXISTS commodity (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            default_unit TEXT,
            bushel_weight_lbs REAL,
            marketing_year_start_month INTEGER
        )
    """)

    for comm in commodities:
        try:
            conn.execute("""
                INSERT OR REPLACE INTO commodity
                (code, name, category, default_unit, bushel_weight_lbs, marketing_year_start_month)
                VALUES (?, ?, ?, ?, ?, ?)
            """, comm)
        except:
            pass

    conn.commit()
    print(f"Created/updated {len(commodities)} commodity entries")


def main():
    parser = argparse.ArgumentParser(description='Ingest Oil Crops data')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without executing')
    args = parser.parse_args()

    csv_path = DATA_DIR / "OilCropsAllTables.csv"

    if not csv_path.exists():
        print(f"ERROR: File not found: {csv_path}")
        return

    print(f"Oil Crops Data Ingestion")
    print(f"CSV file: {csv_path}")
    print(f"Database: {DB_PATH}")
    print(f"Dry run: {args.dry_run}")

    conn = get_db_connection()

    # Create commodity entries
    create_oilseed_commodities(conn)

    # Create tables and ingest
    ingestor = OilCropsIngestor(conn, dry_run=args.dry_run)
    ingestor.create_tables()
    ingestor.ingest_csv(csv_path)

    if not args.dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print("OIL CROPS INGESTION COMPLETE")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
