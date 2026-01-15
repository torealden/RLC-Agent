#!/usr/bin/env python3
"""
Wheat Data Ingestion Script

Parses and ingests data from the ERS Wheat Yearbook CSV files.
Processes all CSV files in the Wheat folder:
- US wheat acreage, production, yield, prices (by class)
- Rye production and S&D
- World wheat supply/disappearance
- US supply/disappearance balance sheets
- Food use, flour production
- Export/import trade data by destination and class
- Price relationships

Data Source: https://www.ers.usda.gov/data-products/wheat-data/

Usage:
    python scripts/ingest_wheat_data.py [--dry-run]
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
DATA_DIR = PROJECT_ROOT / "Models" / "Data" / "Wheat"
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
    if val_str in ['', 'NA', '(NA)', 'NaN', '-', '--', '(D)', '(S)', '#VALUE!']:
        return None
    val_str = val_str.replace(',', '')
    try:
        return float(val_str)
    except ValueError:
        return None


def get_wheat_class_code(commodity: str) -> Optional[str]:
    """Map wheat class name to standardized code."""
    if pd.isna(commodity):
        return None

    comm = str(commodity).lower().strip()

    # Wheat classes
    if 'hard red winter' in comm or comm == 'hrw':
        return 'WHEAT_HRW'
    if 'hard red spring' in comm or comm == 'hrs':
        return 'WHEAT_HRS'
    if 'soft red winter' in comm or comm == 'srw':
        return 'WHEAT_SRW'
    if 'white' in comm and 'wheat' in comm:
        return 'WHEAT_WHITE'
    if 'durum' in comm:
        return 'WHEAT_DURUM'
    if 'all wheat' in comm or comm == 'wheat':
        return 'WHEAT'
    if 'total wheat' in comm:
        return 'WHEAT'

    # Rye
    if 'rye' in comm:
        return 'RYE'

    # Flour
    if 'flour' in comm:
        return 'WHEAT_FLOUR'
    if 'semolina' in comm:
        return 'SEMOLINA'

    return None


def get_attribute_type(attr: str) -> Optional[str]:
    """Map attribute description to standardized type."""
    if pd.isna(attr):
        return None

    attr = str(attr).lower().strip()

    # Production/Acreage
    if 'harvested' in attr and ('acre' in attr or 'area' in attr):
        return 'AREA_HARVESTED'
    if 'planted' in attr and 'acre' in attr:
        return 'AREA_PLANTED'
    if 'production' in attr:
        return 'PRODUCTION'
    if 'yield' in attr:
        return 'YIELD'

    # Supply/Demand
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
    if 'total disappearance' in attr or 'total use' in attr:
        return 'TOTAL_USE'
    if 'food' in attr and 'use' in attr:
        return 'FOOD_USE'
    if 'feed' in attr and 'residual' in attr:
        return 'FEED_RESIDUAL'
    if 'seed' in attr:
        return 'SEED_USE'

    # Prices
    if 'price' in attr and ('farm' in attr or 'received' in attr):
        return 'PRICE_FARM'
    if 'wholesale' in attr and 'price' in attr:
        return 'PRICE_WHOLESALE'
    if 'price' in attr:
        return 'PRICE'

    # Trade
    if 'inspection' in attr:
        return 'INSPECTIONS'
    if 'export' in attr and 'qty' in attr:
        return 'EXPORT_QTY'
    if 'import' in attr and 'qty' in attr:
        return 'IMPORT_QTY'

    return attr.upper().replace(' ', '_').replace(',', '').replace('-', '_')[:50]


def get_location_code(geography: str) -> str:
    """Map geography to location code."""
    if pd.isna(geography):
        return 'US'

    geo = str(geography).strip()

    geo_map = {
        'United States': 'US',
        'World': 'WORLD',
        'Argentina': 'AR',
        'Australia': 'AU',
        'Brazil': 'BR',
        'Canada': 'CA',
        'China': 'CN',
        'European Union': 'EU',
        'India': 'IN',
        'Kazakhstan': 'KZ',
        'Mexico': 'MX',
        'Russia': 'RU',
        'Ukraine': 'UA',
        'Japan': 'JP',
        'South Korea': 'KR',
        'Egypt': 'EG',
        'Algeria': 'DZ',
        'Morocco': 'MA',
        'Nigeria': 'NG',
        'Philippines': 'PH',
        'Indonesia': 'ID',
        'Taiwan': 'TW',
        'Iraq': 'IQ',
        'Saudi Arabia': 'SA',
        'Turkey': 'TR',
        'Bangladesh': 'BD',
        'Pakistan': 'PK',
        'Kansas City': 'US_KC',
        'Minneapolis': 'US_MPLS',
        'Portland': 'US_PDX',
        'Gulf': 'US_GULF',
    }

    if geo in geo_map:
        return geo_map[geo]

    return geo.upper().replace(' ', '_').replace('/', '_')[:20]


class WheatIngestor:
    """Ingests ERS Wheat Yearbook data from multiple CSV files."""

    def __init__(self, conn: sqlite3.Connection, dry_run: bool = False):
        self.conn = conn
        self.dry_run = dry_run
        self.stats = {
            'files_processed': 0,
            'bronze_inserted': 0,
            'production_inserted': 0,
            'balance_sheet_inserted': 0,
            'prices_inserted': 0,
            'trade_inserted': 0,
            'skipped': 0
        }

    def create_tables(self):
        """Create wheat-specific tables."""

        # Bronze table for all raw wheat data
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wheat_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file TEXT,
                commodity_desc TEXT,
                commodity_desc2 TEXT,
                attribute_desc TEXT,
                attribute_desc2 TEXT,
                geography_desc TEXT,
                unit_desc TEXT,
                marketing_year TEXT,
                calendar_year INTEGER,
                fiscal_year TEXT,
                timeperiod_desc TEXT,
                amount REAL,
                -- Derived fields
                commodity_code TEXT,
                attribute_type TEXT,
                location_code TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Wheat production table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wheat_production (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT,
                crop_year INTEGER,
                area_planted REAL,
                area_harvested REAL,
                yield_per_acre REAL,
                production REAL,
                farm_price REAL,
                area_unit TEXT,
                production_unit TEXT,
                price_unit TEXT,
                data_source TEXT DEFAULT 'USDA_ERS_WHEAT',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year)
            )
        """)

        # Wheat balance sheet
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wheat_balance_sheet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                period TEXT DEFAULT 'MY',
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                food_use REAL,
                seed_use REAL,
                feed_residual REAL,
                exports REAL,
                total_use REAL,
                ending_stocks REAL,
                unit_desc TEXT,
                data_source TEXT DEFAULT 'USDA_ERS_WHEAT',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year, period)
            )
        """)

        # Wheat prices
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wheat_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT,
                price_month TEXT,
                price_type TEXT NOT NULL,
                price REAL NOT NULL,
                unit_desc TEXT,
                data_source TEXT DEFAULT 'USDA_ERS_WHEAT',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year, price_type, price_month)
            )
        """)

        # Wheat trade by destination
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wheat_trade (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                partner_code TEXT NOT NULL,
                flow_direction TEXT NOT NULL,
                marketing_year TEXT,
                timeperiod TEXT,
                quantity REAL,
                value REAL,
                unit_desc TEXT,
                data_source TEXT DEFAULT 'USDA_ERS_WHEAT',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, partner_code, flow_direction, marketing_year, timeperiod)
            )
        """)

        self.conn.commit()
        print("Wheat tables created/verified")

    def ingest_all_files(self):
        """Ingest all CSV files in the Wheat folder."""
        print(f"\n{'='*60}")
        print("INGESTING WHEAT DATA")
        print(f"{'='*60}")

        if not DATA_DIR.exists():
            print(f"ERROR: Directory not found: {DATA_DIR}")
            return

        csv_files = sorted(DATA_DIR.glob('*.csv'))
        print(f"Found {len(csv_files)} CSV files")

        for csv_file in csv_files:
            self._ingest_file(csv_file)

        print(f"\nStats: {self.stats}")

    def _ingest_file(self, filepath: Path):
        """Ingest a single CSV file."""
        print(f"\n--- Processing: {filepath.name} ---")

        try:
            df = pd.read_csv(filepath)
            print(f"  Rows: {len(df)}")
        except Exception as e:
            print(f"  ERROR reading file: {e}")
            return

        self.stats['files_processed'] += 1

        # Load into bronze
        self._load_bronze(df, filepath.name)

        # Route to appropriate transformer based on file content
        filename = filepath.name.lower()

        if 'acreage_production' in filename or 'yield' in filename:
            self._transform_production(df)
        elif 'supply' in filename and 'disappearance' in filename:
            self._transform_balance_sheet(df)
        elif 'price' in filename:
            self._transform_prices(df)
        elif 'trade' in filename or 'export' in filename or 'destination' in filename:
            self._transform_trade(df)

    def _load_bronze(self, df: pd.DataFrame, source_file: str):
        """Load raw data into bronze table."""
        # Standardize column names
        df.columns = [c.strip() for c in df.columns]

        for idx, row in df.iterrows():
            # Get commodity from various possible columns
            commodity = None
            for col in ['Commodity_Desc', 'Commodity_Desc2', '26']:  # '26' is wheat class column in inspections file
                if col in row and pd.notna(row[col]):
                    commodity = str(row[col])
                    break

            commodity_code = get_wheat_class_code(commodity)

            # Get attribute
            attr = row.get('Attribute_Desc', row.get('Attribute_Desc2'))
            attribute_type = get_attribute_type(attr)

            # Get geography
            geography = row.get('Geography_Desc', 'United States')
            location_code = get_location_code(geography)

            # Get amount
            amount = clean_numeric(row.get('Amount'))

            if self.dry_run:
                continue

            try:
                self.conn.execute("""
                    INSERT INTO wheat_raw
                    (source_file, commodity_desc, commodity_desc2, attribute_desc,
                     attribute_desc2, geography_desc, unit_desc, marketing_year,
                     calendar_year, fiscal_year, timeperiod_desc, amount,
                     commodity_code, attribute_type, location_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    source_file,
                    row.get('Commodity_Desc'),
                    row.get('Commodity_Desc2', row.get('26')),
                    row.get('Attribute_Desc'),
                    row.get('Attribute_Desc2'),
                    geography,
                    row.get('Unit_Desc'),
                    row.get('Marketing_Year'),
                    clean_numeric(row.get('Calendar_Year')),
                    row.get('Fiscal_Year'),
                    row.get('Timeperiod_Desc'),
                    amount,
                    commodity_code,
                    attribute_type,
                    location_code
                ))
                self.stats['bronze_inserted'] += 1
            except Exception as e:
                self.stats['skipped'] += 1

        self.conn.commit()

    def _transform_production(self, df: pd.DataFrame):
        """Transform production/acreage data."""
        # Group by commodity, geography, marketing year
        groupby_cols = []
        for col in ['Commodity_Desc', 'Commodity_Desc2', 'Geography_Desc', 'Marketing_Year']:
            if col in df.columns:
                groupby_cols.append(col)

        if len(groupby_cols) < 2:
            return

        for group_key, group in df.groupby(groupby_cols):
            commodity = group_key[0] if len(group_key) > 0 else None
            if len(group_key) > 1 and 'Commodity_Desc2' in groupby_cols:
                commodity = group_key[1]

            commodity_code = get_wheat_class_code(commodity)
            if not commodity_code:
                continue

            geography_idx = groupby_cols.index('Geography_Desc') if 'Geography_Desc' in groupby_cols else -1
            location_code = get_location_code(group_key[geography_idx]) if geography_idx >= 0 else 'US'

            my_idx = groupby_cols.index('Marketing_Year') if 'Marketing_Year' in groupby_cols else -1
            marketing_year = group_key[my_idx] if my_idx >= 0 else None

            record = {
                'commodity_code': commodity_code,
                'location_code': location_code,
                'marketing_year': marketing_year
            }

            for _, row in group.iterrows():
                attr = str(row.get('Attribute_Desc', '')).lower()
                attr2 = str(row.get('Attribute_Desc2', '')).lower()
                amount = clean_numeric(row.get('Amount'))
                unit = row.get('Unit_Desc')

                if 'harvested' in attr or 'harvested' in attr2:
                    record['area_harvested'] = amount
                    record['area_unit'] = unit
                elif 'planted' in attr:
                    record['area_planted'] = amount
                    record['area_unit'] = unit
                elif 'production' in attr or 'production' in attr2:
                    record['production'] = amount
                    record['production_unit'] = unit
                elif 'yield' in attr or 'yield' in attr2:
                    record['yield_per_acre'] = amount
                elif 'price' in attr or 'price' in attr2:
                    record['farm_price'] = amount
                    record['price_unit'] = unit

            if self.dry_run:
                continue

            if record.get('production') or record.get('area_harvested'):
                try:
                    self.conn.execute("""
                        INSERT OR REPLACE INTO wheat_production
                        (commodity_code, location_code, marketing_year,
                         area_planted, area_harvested, yield_per_acre, production,
                         farm_price, area_unit, production_unit, price_unit)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('commodity_code'),
                        record.get('location_code'),
                        record.get('marketing_year'),
                        record.get('area_planted'),
                        record.get('area_harvested'),
                        record.get('yield_per_acre'),
                        record.get('production'),
                        record.get('farm_price'),
                        record.get('area_unit'),
                        record.get('production_unit'),
                        record.get('price_unit')
                    ))
                    self.stats['production_inserted'] += 1
                except Exception as e:
                    self.stats['skipped'] += 1

        self.conn.commit()

    def _transform_balance_sheet(self, df: pd.DataFrame):
        """Transform supply/disappearance data."""
        groupby_cols = []
        for col in ['Commodity_Desc', 'Commodity_Desc2', 'Geography_Desc', 'Marketing_Year']:
            if col in df.columns:
                groupby_cols.append(col)

        if len(groupby_cols) < 2:
            return

        for group_key, group in df.groupby(groupby_cols):
            commodity = group_key[1] if len(group_key) > 1 and 'Commodity_Desc2' in groupby_cols else group_key[0]
            commodity_code = get_wheat_class_code(commodity)
            if not commodity_code:
                continue

            geography_idx = groupby_cols.index('Geography_Desc') if 'Geography_Desc' in groupby_cols else -1
            location_code = get_location_code(group_key[geography_idx]) if geography_idx >= 0 else 'US'

            my_idx = groupby_cols.index('Marketing_Year') if 'Marketing_Year' in groupby_cols else -1
            marketing_year = group_key[my_idx] if my_idx >= 0 else None

            record = {
                'commodity_code': commodity_code,
                'location_code': location_code,
                'marketing_year': marketing_year,
                'period': 'MY'
            }

            unit_desc = None
            for _, row in group.iterrows():
                attr = str(row.get('Attribute_Desc', '')).lower()
                amount = clean_numeric(row.get('Amount'))
                if unit_desc is None:
                    unit_desc = row.get('Unit_Desc')

                if 'beginning stock' in attr:
                    record['beginning_stocks'] = amount
                elif attr == 'production':
                    record['production'] = amount
                elif attr == 'imports' or 'import' in attr:
                    record['imports'] = amount
                elif 'total supply' in attr:
                    record['total_supply'] = amount
                elif 'food' in attr:
                    record['food_use'] = amount
                elif 'seed' in attr:
                    record['seed_use'] = amount
                elif 'feed' in attr and 'residual' in attr:
                    record['feed_residual'] = amount
                elif attr == 'exports' or 'export' in attr:
                    record['exports'] = amount
                elif 'total' in attr and ('disappearance' in attr or 'use' in attr):
                    record['total_use'] = amount
                elif 'ending stock' in attr:
                    record['ending_stocks'] = amount

            record['unit_desc'] = unit_desc

            if self.dry_run:
                continue

            if record.get('production') or record.get('total_supply'):
                try:
                    self.conn.execute("""
                        INSERT OR REPLACE INTO wheat_balance_sheet
                        (commodity_code, location_code, marketing_year, period,
                         beginning_stocks, production, imports, total_supply,
                         food_use, seed_use, feed_residual, exports, total_use,
                         ending_stocks, unit_desc)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get('commodity_code'),
                        record.get('location_code'),
                        record.get('marketing_year'),
                        record.get('period'),
                        record.get('beginning_stocks'),
                        record.get('production'),
                        record.get('imports'),
                        record.get('total_supply'),
                        record.get('food_use'),
                        record.get('seed_use'),
                        record.get('feed_residual'),
                        record.get('exports'),
                        record.get('total_use'),
                        record.get('ending_stocks'),
                        record.get('unit_desc')
                    ))
                    self.stats['balance_sheet_inserted'] += 1
                except Exception as e:
                    self.stats['skipped'] += 1

        self.conn.commit()

    def _transform_prices(self, df: pd.DataFrame):
        """Transform price data."""
        for idx, row in df.iterrows():
            commodity = row.get('Commodity_Desc', row.get('Commodity_Desc2'))
            commodity_code = get_wheat_class_code(commodity)
            if not commodity_code:
                continue

            geography = row.get('Geography_Desc', 'United States')
            location_code = get_location_code(geography)

            attr = str(row.get('Attribute_Desc', '')).lower()
            if 'farm' in attr or 'received' in attr:
                price_type = 'FARM'
            elif 'wholesale' in attr:
                price_type = 'WHOLESALE'
            else:
                price_type = 'OTHER'

            amount = clean_numeric(row.get('Amount'))
            if amount is None:
                continue

            if self.dry_run:
                continue

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO wheat_price
                    (commodity_code, location_code, marketing_year, price_month,
                     price_type, price, unit_desc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    commodity_code,
                    location_code,
                    row.get('Marketing_Year'),
                    row.get('Timeperiod_Desc'),
                    price_type,
                    amount,
                    row.get('Unit_Desc')
                ))
                self.stats['prices_inserted'] += 1
            except Exception as e:
                self.stats['skipped'] += 1

        self.conn.commit()

    def _transform_trade(self, df: pd.DataFrame):
        """Transform trade data."""
        for idx, row in df.iterrows():
            commodity = row.get('Commodity_Desc', row.get('Commodity_Desc2', row.get('26')))
            commodity_code = get_wheat_class_code(commodity)
            if not commodity_code:
                continue

            geography = row.get('Geography_Desc', 'World')
            partner_code = get_location_code(geography)

            attr = str(row.get('Attribute_Desc', '')).lower()
            if 'export' in attr:
                flow = 'EXPORT'
            elif 'import' in attr:
                flow = 'IMPORT'
            else:
                flow = 'EXPORT'  # Default for inspection data

            amount = clean_numeric(row.get('Amount'))
            if amount is None:
                continue

            if self.dry_run:
                continue

            try:
                self.conn.execute("""
                    INSERT OR REPLACE INTO wheat_trade
                    (commodity_code, partner_code, flow_direction, marketing_year,
                     timeperiod, quantity, unit_desc)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    commodity_code,
                    partner_code,
                    flow,
                    row.get('Marketing_Year'),
                    row.get('Timeperiod_Desc'),
                    amount,
                    row.get('Unit_Desc')
                ))
                self.stats['trade_inserted'] += 1
            except Exception as e:
                self.stats['skipped'] += 1

        self.conn.commit()


def create_wheat_commodities(conn: sqlite3.Connection):
    """Create commodity entries for wheat classes."""
    commodities = [
        ('WHEAT', 'All Wheat', 'GRAIN', 'BU', 60.0, 6),
        ('WHEAT_HRW', 'Hard Red Winter Wheat', 'GRAIN', 'BU', 60.0, 6),
        ('WHEAT_HRS', 'Hard Red Spring Wheat', 'GRAIN', 'BU', 60.0, 6),
        ('WHEAT_SRW', 'Soft Red Winter Wheat', 'GRAIN', 'BU', 60.0, 6),
        ('WHEAT_WHITE', 'White Wheat', 'GRAIN', 'BU', 60.0, 6),
        ('WHEAT_DURUM', 'Durum Wheat', 'GRAIN', 'BU', 60.0, 6),
        ('RYE', 'Rye', 'GRAIN', 'BU', 56.0, 6),
        ('WHEAT_FLOUR', 'Wheat Flour', 'PROCESSED', 'CWT', None, 6),
        ('SEMOLINA', 'Semolina', 'PROCESSED', 'CWT', None, 6),
    ]

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
    print(f"Created/updated {len(commodities)} wheat commodity entries")


def main():
    parser = argparse.ArgumentParser(description='Ingest Wheat data')
    parser.add_argument('--dry-run', action='store_true', help='Print what would be done without executing')
    args = parser.parse_args()

    print(f"Wheat Data Ingestion")
    print(f"Data directory: {DATA_DIR}")
    print(f"Database: {DB_PATH}")
    print(f"Dry run: {args.dry_run}")

    if not DATA_DIR.exists():
        print(f"ERROR: Directory not found: {DATA_DIR}")
        return

    conn = get_db_connection()

    # Create commodity entries
    create_wheat_commodities(conn)

    # Create tables and ingest
    ingestor = WheatIngestor(conn, dry_run=args.dry_run)
    ingestor.create_tables()
    ingestor.ingest_all_files()

    if not args.dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'='*60}")
    print("WHEAT INGESTION COMPLETE")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
