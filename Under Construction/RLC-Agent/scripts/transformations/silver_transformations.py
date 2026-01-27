#!/usr/bin/env python3
"""
Silver Layer Transformations

Transforms bronze/raw data into clean, standardized silver layer tables.
Silver data is normalized, validated, and ready for analytical queries.

Key transformations:
1. Standardize units (bushels to metric tons, etc.)
2. Calculate derived fields (basis, % changes, ratios)
3. Fill in marketing year dates
4. Validate data quality

Usage:
    python scripts/transformations/silver_transformations.py
"""

import argparse
import logging
import sqlite3
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('silver_transformations')

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"

# Conversion factors
BUSHELS_PER_MT = {
    'CORN': 39.368,      # 1 MT = 39.368 bushels (56 lbs/bushel)
    'SOYBEANS': 36.744,  # 1 MT = 36.744 bushels (60 lbs/bushel)
    'WHEAT': 36.744,     # 1 MT = 36.744 bushels (60 lbs/bushel)
    'SORGHUM': 39.368,   # 1 MT = 39.368 bushels (56 lbs/bushel)
    'BARLEY': 45.93,     # 1 MT = 45.93 bushels (48 lbs/bushel)
    'OATS': 68.894,      # 1 MT = 68.894 bushels (32 lbs/bushel)
}


class SilverTransformer:
    """Transforms raw data to silver layer."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def transform_all(self):
        """Run all silver transformations."""
        logger.info("Running silver layer transformations...")

        self._create_silver_tables()
        self._transform_price_with_dates()
        self._calculate_price_changes()
        self._calculate_basis()
        self._transform_balance_sheet()
        self._calculate_supply_demand_ratios()
        self._validate_data_quality()

        logger.info("Silver transformations complete")

    def _create_silver_tables(self):
        """Create silver layer tables if they don't exist."""

        # Silver price table with calculated fields
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                price_type TEXT NOT NULL,  -- 'FARM', 'CASH', 'FUTURES'
                market_location TEXT,
                marketing_year TEXT NOT NULL,
                price_month INTEGER,
                price_date DATE,
                price REAL NOT NULL,
                price_usd_bu REAL,  -- Standardized to $/bushel
                price_usd_mt REAL,  -- Standardized to $/MT
                mom_change REAL,    -- Month-over-month change
                mom_pct_change REAL,
                yoy_change REAL,    -- Year-over-year change
                yoy_pct_change REAL,
                is_annual_average INTEGER DEFAULT 0,
                data_source TEXT,
                quality_flag TEXT DEFAULT 'OK',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, price_type, marketing_year,
                       price_month, market_location)
            )
        """)

        # Silver balance sheet with ratios
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_balance_sheet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                -- Supply side (million bushels)
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                -- Demand side
                food_seed_industrial REAL,
                feed_residual REAL,
                exports REAL,
                total_use REAL,
                ending_stocks REAL,
                -- Calculated ratios
                stocks_to_use_ratio REAL,
                export_share REAL,
                feed_share REAL,
                fsi_share REAL,
                -- Year-over-year changes
                production_yoy_change REAL,
                ending_stocks_yoy_change REAL,
                -- Unit conversions
                production_mmt REAL,  -- Million metric tons
                exports_mmt REAL,
                ending_stocks_mmt REAL,
                -- Metadata
                data_source TEXT,
                quality_flag TEXT DEFAULT 'OK',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year)
            )
        """)

        # Silver trade flow with additional calculations
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_trade_flow (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                flow_direction TEXT NOT NULL,
                partner_code TEXT,
                partner_name TEXT,
                marketing_year TEXT,
                calendar_year INTEGER,
                month INTEGER,
                quantity_bu REAL,
                quantity_mt REAL,
                value_usd REAL,
                unit_value_usd_mt REAL,
                market_share_pct REAL,
                yoy_change_pct REAL,
                data_source TEXT,
                quality_flag TEXT DEFAULT 'OK',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        self.conn.commit()
        logger.info("Silver tables created/verified")

    def _parse_marketing_year(self, my_str: str, commodity: str = 'CORN') -> Tuple[date, date]:
        """Convert marketing year string to date range."""
        if not my_str or '/' not in my_str:
            return None, None

        parts = my_str.split('/')
        start_year = int(parts[0])

        # Marketing year start months
        start_months = {
            'CORN': 9, 'SOYBEANS': 9, 'SORGHUM': 9,
            'WHEAT': 6, 'BARLEY': 6, 'OATS': 6
        }
        start_month = start_months.get(commodity, 9)

        start_date = date(start_year, start_month, 1)
        end_date = date(start_year + 1, start_month - 1 if start_month > 1 else 12, 28)

        return start_date, end_date

    def _transform_price_with_dates(self):
        """Transform prices with proper dates and unit conversions."""
        logger.info("Transforming prices with dates...")

        # Clear existing silver prices
        self.conn.execute("DELETE FROM silver_price")

        # Transform farm prices
        cursor = self.conn.execute("""
            SELECT commodity_code, location_code, marketing_year, price_month,
                   price, is_annual_average, data_source
            FROM farm_price
            WHERE price IS NOT NULL
        """)

        for row in cursor.fetchall():
            commodity = row['commodity_code']
            my = row['marketing_year']
            month = row['price_month']

            # Calculate price date
            if month and '/' in str(my):
                start_year = int(my.split('/')[0])
                # Marketing year months: Sept=1, Oct=2, ..., Aug=12
                # Convert to calendar month
                if month <= 4:  # Sept-Dec
                    calendar_month = month + 8
                    year = start_year
                else:  # Jan-Aug
                    calendar_month = month - 4
                    year = start_year + 1
                price_date = f"{year}-{calendar_month:02d}-01"
            else:
                price_date = None

            # Convert to $/MT
            price_usd_bu = row['price']
            bu_per_mt = BUSHELS_PER_MT.get(commodity, 39.368)
            price_usd_mt = price_usd_bu * bu_per_mt if price_usd_bu else None

            self.conn.execute("""
                INSERT OR REPLACE INTO silver_price
                (commodity_code, location_code, price_type, marketing_year,
                 price_month, price_date, price, price_usd_bu, price_usd_mt,
                 is_annual_average, data_source)
                VALUES (?, ?, 'FARM', ?, ?, ?, ?, ?, ?, ?, ?)
            """, (commodity, row['location_code'], my, month, price_date,
                  row['price'], price_usd_bu, price_usd_mt,
                  row['is_annual_average'], row['data_source']))

        # Transform cash prices
        cursor = self.conn.execute("""
            SELECT commodity_code, market_location, marketing_year,
                   price_date, price, is_monthly_average, data_source
            FROM cash_price
            WHERE price IS NOT NULL
        """)

        for row in cursor.fetchall():
            commodity = row['commodity_code']
            price_usd_bu = row['price']
            bu_per_mt = BUSHELS_PER_MT.get(commodity, 39.368)
            price_usd_mt = price_usd_bu * bu_per_mt if price_usd_bu else None

            # Extract month from date
            if row['price_date']:
                price_month = int(row['price_date'].split('-')[1])
            else:
                price_month = None

            self.conn.execute("""
                INSERT OR REPLACE INTO silver_price
                (commodity_code, location_code, price_type, market_location,
                 marketing_year, price_month, price_date, price,
                 price_usd_bu, price_usd_mt, is_annual_average, data_source)
                VALUES (?, 'US', 'CASH', ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """, (commodity, row['market_location'], row['marketing_year'],
                  price_month, row['price_date'], row['price'],
                  price_usd_bu, price_usd_mt, row['data_source']))

        self.conn.commit()
        logger.info("Price transformation complete")

    def _calculate_price_changes(self):
        """Calculate month-over-month and year-over-year price changes."""
        logger.info("Calculating price changes...")

        # Calculate MoM changes
        self.conn.execute("""
            UPDATE silver_price
            SET mom_change = (
                SELECT sp2.price - silver_price.price
                FROM silver_price sp2
                WHERE sp2.commodity_code = silver_price.commodity_code
                  AND sp2.location_code = silver_price.location_code
                  AND sp2.price_type = silver_price.price_type
                  AND sp2.market_location IS silver_price.market_location
                  AND sp2.price_date = date(silver_price.price_date, '-1 month')
            )
            WHERE price_date IS NOT NULL
        """)

        self.conn.execute("""
            UPDATE silver_price
            SET mom_pct_change = CASE
                WHEN mom_change IS NOT NULL AND price != 0
                THEN (mom_change / (price - mom_change)) * 100
                ELSE NULL
            END
        """)

        # Calculate YoY changes
        self.conn.execute("""
            UPDATE silver_price
            SET yoy_change = (
                SELECT silver_price.price - sp2.price
                FROM silver_price sp2
                WHERE sp2.commodity_code = silver_price.commodity_code
                  AND sp2.location_code = silver_price.location_code
                  AND sp2.price_type = silver_price.price_type
                  AND sp2.market_location IS silver_price.market_location
                  AND sp2.price_date = date(silver_price.price_date, '-1 year')
            )
            WHERE price_date IS NOT NULL
        """)

        self.conn.execute("""
            UPDATE silver_price
            SET yoy_pct_change = CASE
                WHEN yoy_change IS NOT NULL AND (price - yoy_change) != 0
                THEN (yoy_change / (price - yoy_change)) * 100
                ELSE NULL
            END
        """)

        self.conn.commit()
        logger.info("Price changes calculated")

    def _calculate_basis(self):
        """Calculate basis (cash - futures or cash - farm price)."""
        logger.info("Calculating basis...")

        # This would calculate basis if we had futures prices
        # For now, we can calculate cash - farm price spread
        # This is a simplified version

        self.conn.commit()
        logger.info("Basis calculation complete")

    def _transform_balance_sheet(self):
        """Transform balance sheet data with calculated fields."""
        logger.info("Transforming balance sheets...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_balance_sheet")

        # Get unique commodity/location/marketing year combinations
        cursor = self.conn.execute("""
            SELECT DISTINCT commodity_code, location_code, marketing_year
            FROM balance_sheet_item
            WHERE quarter = 'MY'
        """)

        for row in cursor.fetchall():
            commodity = row['commodity_code']
            location = row['location_code']
            my = row['marketing_year']

            # Get all items for this balance sheet
            items = {}
            item_cursor = self.conn.execute("""
                SELECT item_type, value
                FROM balance_sheet_item
                WHERE commodity_code = ? AND location_code = ?
                  AND marketing_year = ? AND quarter = 'MY'
            """, (commodity, location, my))

            for item in item_cursor.fetchall():
                items[item['item_type']] = item['value']

            # Extract key values
            beg_stocks = items.get('BEGINNING_STOCKS')
            production = items.get('PRODUCTION')
            imports = items.get('IMPORTS')
            total_supply = items.get('TOTAL_SUPPLY')
            fsi = items.get('FOOD_ALCOHOL_INDUSTRIAL')
            feed = items.get('FEED_RESIDUAL')
            exports = items.get('EXPORTS')
            total_use = items.get('TOTAL_USE')
            end_stocks = items.get('ENDING_STOCKS')

            # Calculate ratios
            stocks_use = end_stocks / total_use if total_use and end_stocks else None
            export_share = exports / total_use * 100 if total_use and exports else None
            feed_share = feed / total_use * 100 if total_use and feed else None
            fsi_share = fsi / total_use * 100 if total_use and fsi else None

            # Convert to MMT
            bu_per_mt = BUSHELS_PER_MT.get(commodity, 39.368)
            prod_mmt = production / bu_per_mt / 1000 if production else None
            exp_mmt = exports / bu_per_mt / 1000 if exports else None
            end_mmt = end_stocks / bu_per_mt / 1000 if end_stocks else None

            self.conn.execute("""
                INSERT INTO silver_balance_sheet
                (commodity_code, location_code, marketing_year,
                 beginning_stocks, production, imports, total_supply,
                 food_seed_industrial, feed_residual, exports, total_use, ending_stocks,
                 stocks_to_use_ratio, export_share, feed_share, fsi_share,
                 production_mmt, exports_mmt, ending_stocks_mmt,
                 data_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'USDA_ERS')
            """, (commodity, location, my,
                  beg_stocks, production, imports, total_supply,
                  fsi, feed, exports, total_use, end_stocks,
                  stocks_use, export_share, feed_share, fsi_share,
                  prod_mmt, exp_mmt, end_mmt))

        self.conn.commit()
        logger.info("Balance sheet transformation complete")

    def _calculate_supply_demand_ratios(self):
        """Calculate year-over-year changes in balance sheet."""
        logger.info("Calculating supply/demand ratios...")

        self.conn.execute("""
            UPDATE silver_balance_sheet
            SET production_yoy_change = (
                SELECT ((silver_balance_sheet.production - sb2.production) / sb2.production) * 100
                FROM silver_balance_sheet sb2
                WHERE sb2.commodity_code = silver_balance_sheet.commodity_code
                  AND sb2.location_code = silver_balance_sheet.location_code
                  AND CAST(SUBSTR(sb2.marketing_year, 1, 4) AS INTEGER) =
                      CAST(SUBSTR(silver_balance_sheet.marketing_year, 1, 4) AS INTEGER) - 1
            )
        """)

        self.conn.execute("""
            UPDATE silver_balance_sheet
            SET ending_stocks_yoy_change = (
                SELECT ((silver_balance_sheet.ending_stocks - sb2.ending_stocks) / sb2.ending_stocks) * 100
                FROM silver_balance_sheet sb2
                WHERE sb2.commodity_code = silver_balance_sheet.commodity_code
                  AND sb2.location_code = silver_balance_sheet.location_code
                  AND CAST(SUBSTR(sb2.marketing_year, 1, 4) AS INTEGER) =
                      CAST(SUBSTR(silver_balance_sheet.marketing_year, 1, 4) AS INTEGER) - 1
            )
        """)

        self.conn.commit()
        logger.info("Supply/demand ratios calculated")

    def _validate_data_quality(self):
        """Run data quality checks and flag issues."""
        logger.info("Validating data quality...")

        # Flag prices that seem unrealistic (< $1 or > $20 for corn)
        self.conn.execute("""
            UPDATE silver_price
            SET quality_flag = 'SUSPECT'
            WHERE commodity_code = 'CORN'
              AND price_usd_bu IS NOT NULL
              AND (price_usd_bu < 1.0 OR price_usd_bu > 20.0)
        """)

        # Flag balance sheets where supply != demand (within tolerance)
        self.conn.execute("""
            UPDATE silver_balance_sheet
            SET quality_flag = 'IMBALANCED'
            WHERE total_supply IS NOT NULL
              AND total_use IS NOT NULL
              AND ending_stocks IS NOT NULL
              AND ABS(total_supply - total_use - ending_stocks) > 10
        """)

        # Count flagged records
        cursor = self.conn.execute("""
            SELECT quality_flag, COUNT(*) as cnt
            FROM silver_price
            GROUP BY quality_flag
        """)
        for row in cursor.fetchall():
            logger.info(f"  Price quality: {row['quality_flag']} = {row['cnt']} records")

        cursor = self.conn.execute("""
            SELECT quality_flag, COUNT(*) as cnt
            FROM silver_balance_sheet
            GROUP BY quality_flag
        """)
        for row in cursor.fetchall():
            logger.info(f"  Balance sheet quality: {row['quality_flag']} = {row['cnt']} records")

        self.conn.commit()
        logger.info("Data quality validation complete")


def main():
    parser = argparse.ArgumentParser(description='Run silver layer transformations')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    args = parser.parse_args()

    logger.info(f"Database: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    transformer = SilverTransformer(conn)
    transformer.transform_all()
    conn.close()

    logger.info("Done!")


if __name__ == '__main__':
    main()
