#!/usr/bin/env python3
"""
Wheat Silver Layer Transformations

Transforms wheat bronze data into clean, standardized silver layer tables.
Calculates derived fields like stocks-to-use ratios, export share, etc.

Wheat marketing year: June 1 - May 31

Usage:
    python scripts/transformations/wheat_silver_transformations.py
"""

import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('wheat_silver')

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"

# Conversion factors
BUSHELS_PER_MT = 36.744  # 1 MT = 36.744 bushels (60 lbs/bushel)
LBS_PER_BUSHEL = 60


class WheatSilverTransformer:
    """Transforms wheat data to silver layer."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def transform_all(self):
        """Run all wheat silver transformations."""
        logger.info("Running wheat silver transformations...")

        self._create_silver_tables()
        self._transform_balance_sheets()
        self._transform_prices()
        self._transform_production()
        self._transform_trade()
        self._calculate_ratios()

        logger.info("Wheat silver transformations complete")

    def _create_silver_tables(self):
        """Create wheat silver layer tables."""

        # Silver wheat balance sheet with ratios
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_wheat_balance_sheet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                -- Supply (million bushels)
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                -- Demand
                food_use REAL,
                seed_use REAL,
                feed_residual REAL,
                exports REAL,
                total_use REAL,
                ending_stocks REAL,
                -- Unit conversions (million metric tons)
                production_mmt REAL,
                exports_mmt REAL,
                ending_stocks_mmt REAL,
                -- Calculated ratios
                stocks_to_use_ratio REAL,
                food_use_pct REAL,
                export_pct REAL,
                feed_pct REAL,
                -- YoY changes
                production_yoy_pct REAL,
                ending_stocks_yoy_pct REAL,
                exports_yoy_pct REAL,
                -- Quality
                quality_flag TEXT DEFAULT 'OK',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year)
            )
        """)

        # Silver wheat prices
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_wheat_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT,
                price_month TEXT,
                price_type TEXT NOT NULL,
                price REAL NOT NULL,
                price_usd_bu REAL,
                price_usd_mt REAL,
                original_unit TEXT,
                mom_change REAL,
                yoy_change REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year, price_type, price_month)
            )
        """)

        # Silver wheat production
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_wheat_production (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                -- Area (million acres)
                area_planted REAL,
                area_harvested REAL,
                -- Yield
                yield_bu_acre REAL,
                -- Production
                production_mbu REAL,    -- Million bushels
                production_mmt REAL,    -- Million metric tons
                -- Price
                farm_price_usd_bu REAL,
                farm_price_usd_mt REAL,
                -- Ratios
                harvest_ratio REAL,     -- Harvested / Planted
                -- YoY changes
                production_yoy_pct REAL,
                yield_yoy_pct REAL,
                price_yoy_pct REAL,
                -- Quality
                quality_flag TEXT DEFAULT 'OK',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year)
            )
        """)

        # Silver wheat trade with market share
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_wheat_trade (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                partner_code TEXT NOT NULL,
                partner_name TEXT,
                flow_direction TEXT NOT NULL,
                marketing_year TEXT,
                quantity_mbu REAL,      -- Million bushels
                quantity_mmt REAL,      -- Million metric tons
                value_usd REAL,
                unit_value_usd_mt REAL,
                market_share_pct REAL,
                yoy_change_pct REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, partner_code, flow_direction, marketing_year)
            )
        """)

        self.conn.commit()
        logger.info("Silver wheat tables created")

    def _transform_balance_sheets(self):
        """Transform wheat balance sheets with calculated fields."""
        logger.info("Transforming wheat balance sheets...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_wheat_balance_sheet")

        # Get all balance sheet records
        cursor = self.conn.execute("""
            SELECT commodity_code, location_code, marketing_year,
                   beginning_stocks, production, imports, total_supply,
                   food_use, seed_use, feed_residual, exports,
                   total_use, ending_stocks, unit_desc
            FROM wheat_balance_sheet
            WHERE commodity_code IS NOT NULL
        """)

        count = 0
        for row in cursor.fetchall():
            # Calculate ratios
            total_use = row['total_use']
            stocks_use = None
            food_pct = None
            export_pct = None
            feed_pct = None

            if total_use and total_use > 0:
                if row['ending_stocks']:
                    stocks_use = row['ending_stocks'] / total_use
                if row['food_use']:
                    food_pct = (row['food_use'] / total_use) * 100
                if row['exports']:
                    export_pct = (row['exports'] / total_use) * 100
                if row['feed_residual']:
                    feed_pct = (row['feed_residual'] / total_use) * 100

            # Convert to MMT (million metric tons)
            prod_mmt = None
            exp_mmt = None
            end_mmt = None

            if row['production']:
                prod_mmt = row['production'] / BUSHELS_PER_MT / 1000
            if row['exports']:
                exp_mmt = row['exports'] / BUSHELS_PER_MT / 1000
            if row['ending_stocks']:
                end_mmt = row['ending_stocks'] / BUSHELS_PER_MT / 1000

            self.conn.execute("""
                INSERT INTO silver_wheat_balance_sheet
                (commodity_code, location_code, marketing_year,
                 beginning_stocks, production, imports, total_supply,
                 food_use, seed_use, feed_residual, exports, total_use, ending_stocks,
                 production_mmt, exports_mmt, ending_stocks_mmt,
                 stocks_to_use_ratio, food_use_pct, export_pct, feed_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['commodity_code'], row['location_code'], row['marketing_year'],
                row['beginning_stocks'], row['production'], row['imports'], row['total_supply'],
                row['food_use'], row['seed_use'], row['feed_residual'], row['exports'],
                row['total_use'], row['ending_stocks'],
                prod_mmt, exp_mmt, end_mmt,
                stocks_use, food_pct, export_pct, feed_pct
            ))
            count += 1

        self.conn.commit()
        logger.info(f"  Transformed {count} balance sheet records")

    def _transform_prices(self):
        """Transform wheat prices."""
        logger.info("Transforming wheat prices...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_wheat_price")

        cursor = self.conn.execute("""
            SELECT commodity_code, location_code, marketing_year,
                   price_month, price_type, price, unit_desc
            FROM wheat_price
            WHERE price IS NOT NULL AND commodity_code IS NOT NULL
        """)

        count = 0
        for row in cursor.fetchall():
            price = row['price']
            unit = row['unit_desc'] or ''

            # Convert to standard units
            price_usd_bu = None
            price_usd_mt = None

            # Assume prices in $/bushel unless stated otherwise
            unit_lower = unit.lower()
            if 'bushel' in unit_lower or 'bu' in unit_lower or not unit_lower:
                price_usd_bu = price
                price_usd_mt = price * BUSHELS_PER_MT
            elif 'cwt' in unit_lower or 'hundredweight' in unit_lower:
                # $/cwt to $/bu (60 lbs/bu / 100 lbs/cwt = 0.6)
                price_usd_bu = price * 0.6
                price_usd_mt = price_usd_bu * BUSHELS_PER_MT
            elif 'metric' in unit_lower or 'mt' in unit_lower:
                price_usd_mt = price
                price_usd_bu = price / BUSHELS_PER_MT

            self.conn.execute("""
                INSERT OR REPLACE INTO silver_wheat_price
                (commodity_code, location_code, marketing_year, price_month,
                 price_type, price, price_usd_bu, price_usd_mt, original_unit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['commodity_code'], row['location_code'], row['marketing_year'],
                row['price_month'], row['price_type'], price,
                price_usd_bu, price_usd_mt, unit
            ))
            count += 1

        self.conn.commit()
        logger.info(f"  Transformed {count} price records")

    def _transform_production(self):
        """Transform wheat production data."""
        logger.info("Transforming wheat production...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_wheat_production")

        cursor = self.conn.execute("""
            SELECT commodity_code, location_code, marketing_year,
                   area_planted, area_harvested, yield_per_acre,
                   production, farm_price, production_unit
            FROM wheat_production
            WHERE commodity_code IS NOT NULL
        """)

        count = 0
        for row in cursor.fetchall():
            production = row['production']
            farm_price = row['farm_price']

            # Convert production to MMT
            prod_mmt = None
            if production:
                prod_mmt = production / BUSHELS_PER_MT / 1000

            # Convert price to $/MT
            price_usd_mt = None
            if farm_price:
                price_usd_mt = farm_price * BUSHELS_PER_MT

            # Calculate harvest ratio
            harvest_ratio = None
            if row['area_planted'] and row['area_planted'] > 0 and row['area_harvested']:
                harvest_ratio = row['area_harvested'] / row['area_planted']

            self.conn.execute("""
                INSERT OR REPLACE INTO silver_wheat_production
                (commodity_code, location_code, marketing_year,
                 area_planted, area_harvested, yield_bu_acre,
                 production_mbu, production_mmt, farm_price_usd_bu, farm_price_usd_mt,
                 harvest_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['commodity_code'], row['location_code'], row['marketing_year'],
                row['area_planted'], row['area_harvested'], row['yield_per_acre'],
                production, prod_mmt, farm_price, price_usd_mt,
                harvest_ratio
            ))
            count += 1

        self.conn.commit()
        logger.info(f"  Transformed {count} production records")

    def _transform_trade(self):
        """Transform wheat trade data with market share."""
        logger.info("Transforming wheat trade...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_wheat_trade")

        # First, calculate total exports by year and commodity
        totals_cursor = self.conn.execute("""
            SELECT commodity_code, marketing_year, flow_direction,
                   SUM(quantity) as total_qty
            FROM wheat_trade
            WHERE commodity_code IS NOT NULL AND quantity IS NOT NULL
            GROUP BY commodity_code, marketing_year, flow_direction
        """)

        totals = {}
        for row in totals_cursor.fetchall():
            key = (row['commodity_code'], row['marketing_year'], row['flow_direction'])
            totals[key] = row['total_qty']

        # Partner name mapping
        partner_names = {
            'JP': 'Japan', 'MX': 'Mexico', 'PH': 'Philippines',
            'KR': 'South Korea', 'TW': 'Taiwan', 'NG': 'Nigeria',
            'EG': 'Egypt', 'ID': 'Indonesia', 'CN': 'China',
            'EU': 'European Union', 'BR': 'Brazil', 'CA': 'Canada',
            'CO': 'Colombia', 'VE': 'Venezuela', 'PE': 'Peru',
            'CL': 'Chile', 'DZ': 'Algeria', 'MA': 'Morocco',
            'TH': 'Thailand', 'MY': 'Malaysia', 'BD': 'Bangladesh',
        }

        cursor = self.conn.execute("""
            SELECT commodity_code, partner_code, flow_direction,
                   marketing_year, quantity, value, unit_desc
            FROM wheat_trade
            WHERE commodity_code IS NOT NULL AND quantity IS NOT NULL
        """)

        count = 0
        for row in cursor.fetchall():
            quantity = row['quantity']

            # Convert to MMT
            qty_mmt = quantity / BUSHELS_PER_MT / 1000 if quantity else None

            # Calculate unit value
            unit_value = None
            if quantity and row['value'] and quantity > 0:
                # Value in $/bu / bushels per MT = $/MT
                unit_value = row['value'] / quantity * BUSHELS_PER_MT

            # Calculate market share
            market_share = None
            total_key = (row['commodity_code'], row['marketing_year'], row['flow_direction'])
            if total_key in totals and totals[total_key] > 0:
                market_share = (quantity / totals[total_key]) * 100

            partner_name = partner_names.get(row['partner_code'], row['partner_code'])

            self.conn.execute("""
                INSERT OR REPLACE INTO silver_wheat_trade
                (commodity_code, partner_code, partner_name, flow_direction,
                 marketing_year, quantity_mbu, quantity_mmt, value_usd,
                 unit_value_usd_mt, market_share_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['commodity_code'], row['partner_code'], partner_name,
                row['flow_direction'], row['marketing_year'],
                quantity, qty_mmt, row['value'], unit_value, market_share
            ))
            count += 1

        self.conn.commit()
        logger.info(f"  Transformed {count} trade records")

    def _calculate_ratios(self):
        """Calculate year-over-year changes."""
        logger.info("Calculating YoY changes...")

        # Balance sheet YoY changes
        for field in ['production', 'ending_stocks', 'exports']:
            yoy_field = f"{field}_yoy_pct"
            self.conn.execute(f"""
                UPDATE silver_wheat_balance_sheet
                SET {yoy_field} = (
                    SELECT ((silver_wheat_balance_sheet.{field} - prev.{field}) / prev.{field}) * 100
                    FROM silver_wheat_balance_sheet prev
                    WHERE prev.commodity_code = silver_wheat_balance_sheet.commodity_code
                      AND prev.location_code = silver_wheat_balance_sheet.location_code
                      AND CAST(SUBSTR(prev.marketing_year, 1, 4) AS INTEGER) =
                          CAST(SUBSTR(silver_wheat_balance_sheet.marketing_year, 1, 4) AS INTEGER) - 1
                      AND prev.{field} > 0
                )
                WHERE {field} IS NOT NULL
            """)

        # Production YoY changes
        yoy_mappings = [
            ('production_mbu', 'production_yoy_pct'),
            ('yield_bu_acre', 'yield_yoy_pct'),
            ('farm_price_usd_bu', 'price_yoy_pct'),
        ]
        for field, yoy_field in yoy_mappings:
            self.conn.execute(f"""
                UPDATE silver_wheat_production
                SET {yoy_field} = (
                    SELECT ((silver_wheat_production.{field} - prev.{field}) / prev.{field}) * 100
                    FROM silver_wheat_production prev
                    WHERE prev.commodity_code = silver_wheat_production.commodity_code
                      AND prev.location_code = silver_wheat_production.location_code
                      AND CAST(SUBSTR(prev.marketing_year, 1, 4) AS INTEGER) =
                          CAST(SUBSTR(silver_wheat_production.marketing_year, 1, 4) AS INTEGER) - 1
                      AND prev.{field} > 0
                )
                WHERE {field} IS NOT NULL
            """)

        # Trade YoY changes
        self.conn.execute("""
            UPDATE silver_wheat_trade
            SET yoy_change_pct = (
                SELECT ((silver_wheat_trade.quantity_mbu - prev.quantity_mbu) / prev.quantity_mbu) * 100
                FROM silver_wheat_trade prev
                WHERE prev.commodity_code = silver_wheat_trade.commodity_code
                  AND prev.partner_code = silver_wheat_trade.partner_code
                  AND prev.flow_direction = silver_wheat_trade.flow_direction
                  AND CAST(SUBSTR(prev.marketing_year, 1, 4) AS INTEGER) =
                      CAST(SUBSTR(silver_wheat_trade.marketing_year, 1, 4) AS INTEGER) - 1
                  AND prev.quantity_mbu > 0
            )
            WHERE quantity_mbu IS NOT NULL
        """)

        # Price YoY changes
        self.conn.execute("""
            UPDATE silver_wheat_price
            SET yoy_change = (
                SELECT silver_wheat_price.price_usd_bu - prev.price_usd_bu
                FROM silver_wheat_price prev
                WHERE prev.commodity_code = silver_wheat_price.commodity_code
                  AND prev.location_code = silver_wheat_price.location_code
                  AND prev.price_type = silver_wheat_price.price_type
                  AND COALESCE(prev.price_month, '') = COALESCE(silver_wheat_price.price_month, '')
                  AND CAST(SUBSTR(prev.marketing_year, 1, 4) AS INTEGER) =
                      CAST(SUBSTR(silver_wheat_price.marketing_year, 1, 4) AS INTEGER) - 1
            )
            WHERE price_usd_bu IS NOT NULL
        """)

        self.conn.commit()
        logger.info("  YoY changes calculated")


def main():
    logger.info(f"Database: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    transformer = WheatSilverTransformer(conn)
    transformer.transform_all()
    conn.close()

    logger.info("Done!")


if __name__ == '__main__':
    main()
