#!/usr/bin/env python3
"""
Oilseed Silver Layer Transformations

Transforms oilseed bronze data into clean, standardized silver layer tables.
Calculates derived fields like crush margins, stocks-to-use ratios, etc.

Usage:
    python scripts/transformations/oilseed_silver_transformations.py
"""

import logging
import sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('oilseed_silver')

PROJECT_ROOT = Path(__file__).parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"

# Conversion factors
BUSHELS_PER_MT = {
    'SOYBEANS': 36.744,      # 1 MT = 36.744 bushels (60 lbs/bushel)
    'CORN': 39.368,
}

LBS_PER_MT = 2204.62


class OilseedSilverTransformer:
    """Transforms oilseed data to silver layer."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    def transform_all(self):
        """Run all oilseed silver transformations."""
        logger.info("Running oilseed silver transformations...")

        self._create_silver_tables()
        self._transform_balance_sheets()
        self._transform_prices()
        self._calculate_crush_margins()
        self._calculate_ratios()

        logger.info("Oilseed silver transformations complete")

    def _create_silver_tables(self):
        """Create oilseed silver layer tables."""

        # Silver oilseed balance sheet with ratios
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_oilseed_balance_sheet (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                -- Supply (various units standardized)
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                -- Demand
                crush REAL,
                exports REAL,
                domestic_use REAL,
                seed_feed_residual REAL,
                biofuel_use REAL,
                total_use REAL,
                ending_stocks REAL,
                -- Unit info
                original_unit TEXT,
                -- Calculated ratios
                stocks_to_use_ratio REAL,
                crush_pct REAL,
                export_pct REAL,
                -- YoY changes
                production_yoy_pct REAL,
                ending_stocks_yoy_pct REAL,
                crush_yoy_pct REAL,
                -- Quality
                quality_flag TEXT DEFAULT 'OK',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year)
            )
        """)

        # Silver oilseed prices
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_oilseed_price (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT NOT NULL,
                location_code TEXT NOT NULL,
                marketing_year TEXT,
                price_type TEXT NOT NULL,
                price REAL NOT NULL,
                price_usd_bu REAL,      -- For soybeans
                price_cents_lb REAL,    -- For oils/meals
                price_usd_st REAL,      -- For meals
                original_unit TEXT,
                mom_change REAL,
                yoy_change REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, location_code, marketing_year, price_type)
            )
        """)

        # Crush margins
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_crush_margin (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                price_month TEXT,
                -- Prices
                soybean_price_bu REAL,
                meal_price_st REAL,
                oil_price_lb REAL,
                -- Calculated values
                meal_value_bu REAL,     -- Value of meal per bushel crushed
                oil_value_bu REAL,      -- Value of oil per bushel crushed
                gross_margin_bu REAL,   -- Total product value - bean cost
                -- Typical yields: 47.5 lbs meal, 11 lbs oil per bushel
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(location_code, marketing_year, price_month)
            )
        """)

        self.conn.commit()
        logger.info("Silver oilseed tables created")

    def _transform_balance_sheets(self):
        """Transform oilseed balance sheets with calculated fields."""
        logger.info("Transforming oilseed balance sheets...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_oilseed_balance_sheet")

        # Get all balance sheet records
        cursor = self.conn.execute("""
            SELECT commodity_code, location_code, marketing_year,
                   beginning_stocks, production, imports, total_supply,
                   crush, exports, domestic_use, seed_feed_residual,
                   biofuel_use, total_use, ending_stocks, unit_desc
            FROM oilseed_balance_sheet
            WHERE commodity_code IS NOT NULL
        """)

        count = 0
        for row in cursor.fetchall():
            # Calculate ratios
            total_use = row['total_use'] or row['ending_stocks']  # Fallback
            stocks_use = None
            crush_pct = None
            export_pct = None

            if total_use and total_use > 0:
                if row['ending_stocks']:
                    stocks_use = row['ending_stocks'] / total_use
                if row['crush']:
                    crush_pct = (row['crush'] / total_use) * 100
                if row['exports']:
                    export_pct = (row['exports'] / total_use) * 100

            self.conn.execute("""
                INSERT INTO silver_oilseed_balance_sheet
                (commodity_code, location_code, marketing_year,
                 beginning_stocks, production, imports, total_supply,
                 crush, exports, domestic_use, seed_feed_residual,
                 biofuel_use, total_use, ending_stocks, original_unit,
                 stocks_to_use_ratio, crush_pct, export_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['commodity_code'], row['location_code'], row['marketing_year'],
                row['beginning_stocks'], row['production'], row['imports'], row['total_supply'],
                row['crush'], row['exports'], row['domestic_use'], row['seed_feed_residual'],
                row['biofuel_use'], row['total_use'], row['ending_stocks'], row['unit_desc'],
                stocks_use, crush_pct, export_pct
            ))
            count += 1

        self.conn.commit()
        logger.info(f"  Transformed {count} balance sheet records")

    def _transform_prices(self):
        """Transform oilseed prices."""
        logger.info("Transforming oilseed prices...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_oilseed_price")

        cursor = self.conn.execute("""
            SELECT commodity_code, location_code, marketing_year,
                   price_type, price, unit_desc
            FROM oilseed_price
            WHERE price IS NOT NULL AND commodity_code IS NOT NULL
        """)

        count = 0
        for row in cursor.fetchall():
            price = row['price']
            unit = row['unit_desc'] or ''
            commodity = row['commodity_code']

            # Convert to standard units
            price_usd_bu = None
            price_cents_lb = None
            price_usd_st = None

            if 'bushel' in unit.lower():
                price_usd_bu = price
            elif 'cent' in unit.lower() and 'pound' in unit.lower():
                price_cents_lb = price
            elif 'dollar' in unit.lower() and 'short ton' in unit.lower():
                price_usd_st = price
            elif 'dollar' in unit.lower() and 'hundred' in unit.lower():
                # $/cwt -> $/short ton (x20)
                price_usd_st = price * 20

            self.conn.execute("""
                INSERT OR REPLACE INTO silver_oilseed_price
                (commodity_code, location_code, marketing_year, price_type,
                 price, price_usd_bu, price_cents_lb, price_usd_st, original_unit)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['commodity_code'], row['location_code'], row['marketing_year'],
                row['price_type'], price, price_usd_bu, price_cents_lb, price_usd_st, unit
            ))
            count += 1

        self.conn.commit()
        logger.info(f"  Transformed {count} price records")

    def _calculate_crush_margins(self):
        """Calculate soybean crush margins."""
        logger.info("Calculating crush margins...")

        # Clear existing
        self.conn.execute("DELETE FROM silver_crush_margin")

        # Get soybean, meal, and oil prices by marketing year
        cursor = self.conn.execute("""
            SELECT marketing_year,
                   MAX(CASE WHEN commodity_code = 'SOYBEANS' AND price_type IN ('FARM', 'SEASON_AVG')
                       THEN price_usd_bu END) AS bean_price,
                   MAX(CASE WHEN commodity_code = 'SOYBEAN_MEAL' AND price_type = 'WHOLESALE'
                       THEN price_usd_st END) AS meal_price,
                   MAX(CASE WHEN commodity_code = 'SOYBEAN_OIL' AND price_type = 'WHOLESALE'
                       THEN price_cents_lb END) AS oil_price
            FROM silver_oilseed_price
            WHERE location_code = 'US'
            GROUP BY marketing_year
            HAVING bean_price IS NOT NULL
        """)

        count = 0
        for row in cursor.fetchall():
            bean_price = row['bean_price']
            meal_price = row['meal_price']
            oil_price = row['oil_price']

            # Calculate values per bushel crushed
            # Typical yields: 47.5 lbs meal (0.02375 short tons), 11 lbs oil
            meal_value = None
            oil_value = None
            gross_margin = None

            if meal_price:
                meal_value = meal_price * 0.02375  # $/ST * ST/bu

            if oil_price:
                oil_value = (oil_price / 100) * 11  # cents/lb -> $/lb * lbs/bu

            if meal_value and oil_value and bean_price:
                gross_margin = meal_value + oil_value - bean_price

            self.conn.execute("""
                INSERT INTO silver_crush_margin
                (location_code, marketing_year, soybean_price_bu, meal_price_st,
                 oil_price_lb, meal_value_bu, oil_value_bu, gross_margin_bu)
                VALUES ('US', ?, ?, ?, ?, ?, ?, ?)
            """, (
                row['marketing_year'], bean_price, meal_price, oil_price,
                meal_value, oil_value, gross_margin
            ))
            count += 1

        self.conn.commit()
        logger.info(f"  Calculated {count} crush margin records")

    def _calculate_ratios(self):
        """Calculate year-over-year changes."""
        logger.info("Calculating YoY changes...")

        # Production YoY
        self.conn.execute("""
            UPDATE silver_oilseed_balance_sheet
            SET production_yoy_pct = (
                SELECT ((silver_oilseed_balance_sheet.production - prev.production) / prev.production) * 100
                FROM silver_oilseed_balance_sheet prev
                WHERE prev.commodity_code = silver_oilseed_balance_sheet.commodity_code
                  AND prev.location_code = silver_oilseed_balance_sheet.location_code
                  AND CAST(SUBSTR(prev.marketing_year, 1, 4) AS INTEGER) =
                      CAST(SUBSTR(silver_oilseed_balance_sheet.marketing_year, 1, 4) AS INTEGER) - 1
                  AND prev.production > 0
            )
            WHERE production IS NOT NULL
        """)

        # Ending stocks YoY
        self.conn.execute("""
            UPDATE silver_oilseed_balance_sheet
            SET ending_stocks_yoy_pct = (
                SELECT ((silver_oilseed_balance_sheet.ending_stocks - prev.ending_stocks) / prev.ending_stocks) * 100
                FROM silver_oilseed_balance_sheet prev
                WHERE prev.commodity_code = silver_oilseed_balance_sheet.commodity_code
                  AND prev.location_code = silver_oilseed_balance_sheet.location_code
                  AND CAST(SUBSTR(prev.marketing_year, 1, 4) AS INTEGER) =
                      CAST(SUBSTR(silver_oilseed_balance_sheet.marketing_year, 1, 4) AS INTEGER) - 1
                  AND prev.ending_stocks > 0
            )
            WHERE ending_stocks IS NOT NULL
        """)

        # Crush YoY
        self.conn.execute("""
            UPDATE silver_oilseed_balance_sheet
            SET crush_yoy_pct = (
                SELECT ((silver_oilseed_balance_sheet.crush - prev.crush) / prev.crush) * 100
                FROM silver_oilseed_balance_sheet prev
                WHERE prev.commodity_code = silver_oilseed_balance_sheet.commodity_code
                  AND prev.location_code = silver_oilseed_balance_sheet.location_code
                  AND CAST(SUBSTR(prev.marketing_year, 1, 4) AS INTEGER) =
                      CAST(SUBSTR(silver_oilseed_balance_sheet.marketing_year, 1, 4) AS INTEGER) - 1
                  AND prev.crush > 0
            )
            WHERE crush IS NOT NULL
        """)

        self.conn.commit()
        logger.info("  YoY changes calculated")


def main():
    logger.info(f"Database: {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    transformer = OilseedSilverTransformer(conn)
    transformer.transform_all()
    conn.close()

    logger.info("Done!")


if __name__ == '__main__':
    main()
