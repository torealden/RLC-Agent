#!/usr/bin/env python3
"""
WASDE (World Agricultural Supply and Demand Estimates) PDF Parser

Parses the monthly WASDE report from USDA to extract key supply and demand projections.
WASDE is released around the 12th of each month.

Key tables extracted:
- U.S. Wheat Supply and Use (page 11)
- U.S. Feed Grain and Corn Supply and Use (page 12)
- U.S. Sorghum, Barley, Oats Supply and Use (page 13)
- U.S. Soybeans and Products Supply and Use (page 15)
- U.S. Cotton Supply and Use (page 17)

Data source: USDA Office of the Chief Economist
https://www.usda.gov/oce/commodity/wasde

Usage:
    python scripts/ingest_wasde.py
    python scripts/ingest_wasde.py --file Models/Data/wasde0226.pdf
"""

import argparse
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('wasde_ingest')

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
DEFAULT_FILE = PROJECT_ROOT / "Models" / "Data" / "wasde0126.pdf"

# Try to import fitz (PyMuPDF)
try:
    import fitz
    HAS_FITZ = True
except ImportError:
    HAS_FITZ = False
    logger.warning("PyMuPDF (fitz) not available. Install with: pip install pymupdf")


class WASPEParser:
    """Parses WASDE PDF reports."""

    DATA_SOURCE = 'USDA_WASDE'

    def __init__(self, filepath: Path):
        if not HAS_FITZ:
            raise ImportError("PyMuPDF required. Install with: pip install pymupdf")

        self.filepath = filepath
        self.doc = fitz.open(str(filepath))
        self.conn = sqlite3.connect(str(DB_PATH))
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

        # Extract report metadata from filename
        self.report_id = self._extract_report_id()
        self.report_date = self._extract_report_date()

        self.stats = {'projections': 0, 'prices': 0}

    def _extract_report_id(self) -> str:
        """Extract WASDE report ID from filename (e.g., wasde0126 -> 667)."""
        # Try to find it in first page
        text = self.doc[0].get_text()
        match = re.search(r'WASDE\s*-?\s*(\d+)', text)
        if match:
            return match.group(1)
        return self.filepath.stem

    def _extract_report_date(self) -> Optional[str]:
        """Extract report date from PDF."""
        text = self.doc[0].get_text()
        # Look for date patterns like "January 12, 2026"
        match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,?\s+\d{4}', text)
        if match:
            return match.group(0)
        return None

    def _create_tables(self):
        """Create WASDE-specific tables."""
        # WASDE projections table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wasde_projection (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source TEXT NOT NULL,
                report_id TEXT NOT NULL,
                report_date TEXT,
                commodity_code TEXT NOT NULL,
                marketing_year TEXT NOT NULL,
                estimate_type TEXT,  -- 'Est.' or 'Proj.'
                projection_month TEXT,  -- 'Dec', 'Jan', etc.
                -- Area (million acres)
                area_planted REAL,
                area_harvested REAL,
                yield_value REAL,
                yield_unit TEXT,
                -- Supply (million bushels or other unit)
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                -- Demand
                food_use REAL,
                seed_use REAL,
                feed_residual REAL,
                domestic_total REAL,
                exports REAL,
                total_use REAL,
                ending_stocks REAL,
                -- Price
                avg_farm_price REAL,
                price_unit TEXT,
                -- Unit info
                quantity_unit TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_source, report_id, commodity_code, marketing_year, projection_month)
            )
        """)

        # WASDE soybean products
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS wasde_soybean_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                data_source TEXT NOT NULL,
                report_id TEXT NOT NULL,
                report_date TEXT,
                product_code TEXT NOT NULL,  -- SOYBEAN_OIL, SOYBEAN_MEAL
                marketing_year TEXT NOT NULL,
                estimate_type TEXT,
                projection_month TEXT,
                beginning_stocks REAL,
                production REAL,
                imports REAL,
                total_supply REAL,
                domestic_use REAL,
                biofuel_use REAL,
                food_feed_other REAL,
                exports REAL,
                total_use REAL,
                ending_stocks REAL,
                avg_price REAL,
                price_unit TEXT,
                quantity_unit TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(data_source, report_id, product_code, marketing_year, projection_month)
            )
        """)

        self.conn.commit()
        logger.info("WASDE tables ready")

    def _clean_numeric(self, value: str) -> Optional[float]:
        """Clean numeric value from PDF text."""
        if not value or value.strip() in ['', '-', '--', 'NA', 'Filler']:
            return None
        # Remove commas and whitespace
        cleaned = re.sub(r'[,\s]', '', value.strip())
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _parse_us_wheat(self):
        """Parse U.S. Wheat Supply and Use table (page 11)."""
        logger.info("Parsing U.S. Wheat Supply and Use...")

        # Page 11 (0-indexed: 10)
        page = self.doc[10]
        text = page.get_text()
        lines = text.split('\n')

        # Find the main wheat table
        data = {
            'commodity_code': 'WHEAT',
            'quantity_unit': 'Million bushels',
            'yield_unit': 'Bushels per acre',
            'price_unit': 'Dollars per bushel'
        }

        # Parse the table - look for key row labels
        for i, line in enumerate(lines):
            label = line.strip()

            # Match row labels (may have trailing spaces in PDF)
            if label.startswith('Area Planted'):
                values = self._extract_values_from_context(lines, i, 'area_planted')
                if values:
                    self._store_wheat_values(values, 'area_planted')
            elif label.startswith('Area Harvested') and 'Yield' not in label:
                values = self._extract_values_from_context(lines, i, 'area_harvested')
                if values:
                    self._store_wheat_values(values, 'area_harvested')
            elif label.startswith('Yield per Harvested Acre'):
                values = self._extract_values_from_context(lines, i, 'yield_value')
                if values:
                    self._store_wheat_values(values, 'yield_value')
            elif label.startswith('Beginning Stocks'):
                values = self._extract_values_from_context(lines, i, 'beginning_stocks')
                if values:
                    self._store_wheat_values(values, 'beginning_stocks')
            elif label.startswith('Production'):
                values = self._extract_values_from_context(lines, i, 'production')
                if values:
                    self._store_wheat_values(values, 'production')
            elif label.startswith('Exports'):
                values = self._extract_values_from_context(lines, i, 'exports')
                if values:
                    self._store_wheat_values(values, 'exports')
            elif label.startswith('Ending Stocks') and 'Total' not in label:
                values = self._extract_values_from_context(lines, i, 'ending_stocks')
                if values:
                    self._store_wheat_values(values, 'ending_stocks')
            elif label.startswith('Avg. Farm Price'):
                values = self._extract_values_from_context(lines, i, 'avg_farm_price')
                if values:
                    self._store_wheat_values(values, 'avg_farm_price')

    def _parse_us_corn(self):
        """Parse U.S. Corn Supply and Use table (page 12)."""
        logger.info("Parsing U.S. Corn Supply and Use...")

        page = self.doc[11]
        text = page.get_text()
        lines = text.split('\n')

        for i, line in enumerate(lines):
            label = line.strip()

            if label.startswith('Area Planted'):
                values = self._extract_values_from_context(lines, i, 'area_planted')
                if values:
                    self._store_corn_values(values, 'area_planted')
            elif label.startswith('Area Harvested') and 'Yield' not in label:
                values = self._extract_values_from_context(lines, i, 'area_harvested')
                if values:
                    self._store_corn_values(values, 'area_harvested')
            elif label.startswith('Yield per Harvested Acre'):
                values = self._extract_values_from_context(lines, i, 'yield_value')
                if values:
                    self._store_corn_values(values, 'yield_value')
            elif label.startswith('Beginning Stocks'):
                values = self._extract_values_from_context(lines, i, 'beginning_stocks')
                if values:
                    self._store_corn_values(values, 'beginning_stocks')
            elif label.startswith('Production'):
                values = self._extract_values_from_context(lines, i, 'production')
                if values:
                    self._store_corn_values(values, 'production')
            elif label.startswith('Ending Stocks'):
                values = self._extract_values_from_context(lines, i, 'ending_stocks')
                if values:
                    self._store_corn_values(values, 'ending_stocks')
            elif label.startswith('Avg. Farm Price'):
                values = self._extract_values_from_context(lines, i, 'avg_farm_price')
                if values:
                    self._store_corn_values(values, 'avg_farm_price')

    def _parse_us_soybeans(self):
        """Parse U.S. Soybeans and Products Supply and Use (page 15)."""
        logger.info("Parsing U.S. Soybeans and Products...")

        page = self.doc[14]
        text = page.get_text()
        lines = text.split('\n')

        in_soybeans = True
        in_oil = False
        in_meal = False

        for i, line in enumerate(lines):
            label = line.strip()

            # Section detection
            if 'SOYBEAN OIL' in label:
                in_soybeans = False
                in_oil = True
                in_meal = False
                continue
            elif 'SOYBEAN MEAL' in label:
                in_soybeans = False
                in_oil = False
                in_meal = True
                continue

            if in_soybeans:
                if label.startswith('Area Planted'):
                    values = self._extract_values_from_context(lines, i, 'area_planted')
                    if values:
                        self._store_soybean_values(values, 'area_planted')
                elif label.startswith('Area Harvested') and 'Yield' not in label:
                    values = self._extract_values_from_context(lines, i, 'area_harvested')
                    if values:
                        self._store_soybean_values(values, 'area_harvested')
                elif label.startswith('Yield per Harvested Acre'):
                    values = self._extract_values_from_context(lines, i, 'yield_value')
                    if values:
                        self._store_soybean_values(values, 'yield_value')
                elif label.startswith('Beginning Stocks'):
                    values = self._extract_values_from_context(lines, i, 'beginning_stocks')
                    if values:
                        self._store_soybean_values(values, 'beginning_stocks')
                elif label.startswith('Production'):
                    values = self._extract_values_from_context(lines, i, 'production')
                    if values:
                        self._store_soybean_values(values, 'production')
                elif label.startswith('Crushings'):
                    values = self._extract_values_from_context(lines, i, 'crush')
                    if values:
                        self._store_soybean_values(values, 'domestic_total')
                elif label.startswith('Ending Stocks'):
                    values = self._extract_values_from_context(lines, i, 'ending_stocks')
                    if values:
                        self._store_soybean_values(values, 'ending_stocks')
                elif label.startswith('Avg. Farm Price'):
                    values = self._extract_values_from_context(lines, i, 'avg_farm_price')
                    if values:
                        self._store_soybean_values(values, 'avg_farm_price')

    def _extract_values_from_context(self, lines: List[str], row_idx: int, field_name: str) -> Optional[Dict]:
        """Extract numeric values from the 4 lines following the label."""
        # The PDF text extraction puts each value on its own line after the label
        # So we look at lines row_idx+1 through row_idx+4 for the 4 column values
        values = []

        for offset in range(1, 5):
            if row_idx + offset >= len(lines):
                break
            line = lines[row_idx + offset].strip()

            # Check if this line is a number (may have commas)
            if re.match(r'^[\d,]+\.?\d*$', line):
                values.append(self._clean_numeric(line))
            else:
                # Hit a non-numeric line, stop
                break

        if len(values) >= 3:
            return {
                '2023/24': values[0] if len(values) > 0 else None,
                '2024/25': values[1] if len(values) > 1 else None,
                '2025/26_dec': values[2] if len(values) > 2 else None,
                '2025/26_jan': values[3] if len(values) > 3 else None,
            }

        return None

    def _store_wheat_values(self, values: Dict, field_name: str):
        """Store wheat projection values."""
        self._store_commodity_values('WHEAT', values, field_name)

    def _store_corn_values(self, values: Dict, field_name: str):
        """Store corn projection values."""
        self._store_commodity_values('CORN', values, field_name)

    def _store_soybean_values(self, values: Dict, field_name: str):
        """Store soybean projection values."""
        self._store_commodity_values('SOYBEANS', values, field_name)

    def _store_commodity_values(self, commodity: str, values: Dict, field_name: str):
        """Store commodity projection values in database."""
        # Store each marketing year
        year_configs = [
            ('2023/24', None, values.get('2023/24')),
            ('2024/25', 'Est.', values.get('2024/25')),
            ('2025/26', 'Dec', values.get('2025/26_dec')),
            ('2025/26', 'Jan', values.get('2025/26_jan')),
        ]

        for my, proj_month, value in year_configs:
            if value is None:
                continue

            # Check if record exists
            existing = self.conn.execute("""
                SELECT id FROM wasde_projection
                WHERE data_source = ? AND report_id = ? AND commodity_code = ?
                AND marketing_year = ? AND (projection_month = ? OR projection_month IS NULL)
            """, (self.DATA_SOURCE, self.report_id, commodity, my,
                  proj_month)).fetchone()

            if existing:
                # Update existing record
                self.conn.execute(f"""
                    UPDATE wasde_projection SET {field_name} = ?
                    WHERE id = ?
                """, (value, existing[0]))
            else:
                # Insert new record
                self.conn.execute(f"""
                    INSERT INTO wasde_projection
                    (data_source, report_id, report_date, commodity_code, marketing_year,
                     estimate_type, projection_month, {field_name})
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (self.DATA_SOURCE, self.report_id, self.report_date, commodity, my,
                      'Proj.' if proj_month else None, proj_month, value))
                self.stats['projections'] += 1

        self.conn.commit()

    def run(self):
        """Execute full WASDE parsing."""
        logger.info(f"Parsing WASDE report: {self.filepath}")
        logger.info(f"Report ID: {self.report_id}, Date: {self.report_date}")

        self._parse_us_wheat()
        self._parse_us_corn()
        self._parse_us_soybeans()

        self.conn.close()
        self.doc.close()

        logger.info("=" * 60)
        logger.info(f"WASDE parsing complete")
        logger.info(f"  Projections stored: {self.stats['projections']}")
        logger.info("=" * 60)

        return self.stats


def main():
    parser = argparse.ArgumentParser(description='Parse WASDE PDF report')
    parser.add_argument('--file', type=Path, default=DEFAULT_FILE,
                        help='Path to WASDE PDF file')
    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"File not found: {args.file}")
        return

    wasde_parser = WASPEParser(args.file)
    wasde_parser.run()


if __name__ == '__main__':
    main()
