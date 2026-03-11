"""
Statistics Canada Agricultural Data Collector

Collects agricultural statistics from Statistics Canada:
- Field Crop Reporting Series (area, yield, production by province)
- Quarterly grain stocks
- Farm product prices

Data source: https://www150.statcan.gc.ca/n1/en/type/data
Method: CSV bulk download (ZIP) from StatsCan open data portal.

The WDS REST API (getDataFromCubePidCoordAndLatestNPeriods) requires
valid per-table coordinate strings that vary by dimension count. The
CSV bulk download is more reliable — each table is a single ZIP file
containing a UTF-8 CSV with standardised columns.

Free - No authentication required.
"""

import csv
import io
import logging
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Any

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

from src.services.database.db_config import get_connection as get_db_connection

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# Table definitions
# ============================================================================

# Statistics Canada Table IDs for agricultural data.
# The numeric product ID is the table code with dashes removed.
STATSCAN_TABLES = {
    'field_crop_production': {
        'product_id': '32100359',
        'table_id': '32-10-0359-01',
        'name': 'Estimated areas, yield, production, average farm price and total farm value',
        'frequency': 'annual',
        # Column that holds the commodity/crop name
        'commodity_col': 'Type of crop',
        # Other dimension columns that become the "attribute"
        'attribute_cols': ['Harvest disposition'],
    },
    'grain_stocks': {
        'product_id': '32100007',
        'table_id': '32-10-0007-01',
        'name': 'Stocks of principal field crops at December 31',
        'frequency': 'quarterly',
        'commodity_col': 'Type of crop',
        'attribute_cols': ['Type of stock'],
        'release_months': [3, 6, 9, 12],
    },
    'farm_prices': {
        'product_id': '32100077',
        'table_id': '32-10-0077-01',
        'name': 'Farm product prices, crops and livestock',
        'frequency': 'monthly',
        # Farm prices has a combined dimension; commodity extracted by keyword match
        'commodity_col': None,
        'attribute_cols': ['Farm products'],
    },
}


# Normalise StatsCan crop names to our standard commodity names.
# Keys are LOWERCASE versions of the "Type of crop" / "Farm products" column.
CROP_NAME_MAP = {
    # -- Type of crop (production + stocks tables) --
    'wheat, all': 'wheat',
    'wheat': 'wheat',
    'all wheat': 'wheat',
    'wheat, spring': 'wheat_spring',
    'spring wheat': 'wheat_spring',
    'wheat, durum': 'wheat_durum',
    'durum wheat': 'wheat_durum',
    'wheat, winter': 'wheat_winter',
    'winter wheat': 'wheat_winter',
    'canola (rapeseed)': 'canola',
    'canola': 'canola',
    'barley': 'barley',
    'oats': 'oats',
    'corn for grain': 'corn',
    'corn': 'corn',
    'soybeans': 'soybeans',
    'flaxseed': 'flaxseed',
    'lentils': 'lentils',
    'dry peas': 'peas',
    'peas, dry': 'peas',
    'rye, all': 'rye',
    'rye': 'rye',
    'sunflower seed': 'sunflower',
    'mustard seed': 'mustard',
}

# Keywords used to extract commodity from the "Farm products" column
# in the farm_prices table where the column is a descriptive phrase.
FARM_PRICE_COMMODITY_KEYWORDS = [
    ('wheat', 'wheat'),
    ('canola', 'canola'),
    ('barley', 'barley'),
    ('oats', 'oats'),
    ('corn', 'corn'),
    ('soybeans', 'soybeans'),
    ('soybean', 'soybeans'),
    ('flaxseed', 'flaxseed'),
    ('rye', 'rye'),
    ('lentils', 'lentils'),
    ('peas', 'peas'),
]


# Provinces we care about (skip territories and aggregates except Canada)
PROVINCE_MAP = {
    'Canada': 'CA',
    'Prince Edward Island': 'PE',
    'Nova Scotia': 'NS',
    'New Brunswick': 'NB',
    'Quebec': 'QC',
    'Ontario': 'ON',
    'Manitoba': 'MB',
    'Saskatchewan': 'SK',
    'Alberta': 'AB',
    'British Columbia': 'BC',
}


# ============================================================================
# Config
# ============================================================================

@dataclass
class StatsCanConfig(CollectorConfig):
    """Statistics Canada configuration"""
    source_name: str = "Statistics Canada"
    source_url: str = "https://www150.statcan.gc.ca"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # CSV bulk download base URL
    csv_base_url: str = "https://www150.statcan.gc.ca/n1/tbl/csv"

    # Tables to fetch (keys into STATSCAN_TABLES)
    tables: List[str] = field(default_factory=lambda: [
        'field_crop_production', 'grain_stocks', 'farm_prices'
    ])

    # Only keep records from this year onward (avoids loading 100+ years)
    min_year: int = 2015

    # Request settings
    timeout: int = 120  # ZIPs can be several MB
    retry_attempts: int = 3


# ============================================================================
# Collector
# ============================================================================

class StatsCanCollector(BaseCollector):
    """
    Collector for Statistics Canada agricultural data.

    Downloads CSV bulk files from the StatsCan open data portal, parses
    them, filters to recent years and relevant commodities, and saves
    to bronze.canada_statscan.

    Provides:
    - Field crop production estimates (area, yield, production by province)
    - Quarterly grain stocks
    - Farm product prices
    """

    def __init__(self, config: StatsCanConfig = None):
        config = config or StatsCanConfig()
        super().__init__(config)
        self.config: StatsCanConfig = config

    def get_table_name(self) -> str:
        return "canada_statscan"

    # ------------------------------------------------------------------
    # collect() override — saves to bronze after fetch
    # ------------------------------------------------------------------
    def collect(self, start_date=None, end_date=None, use_cache=True, **kwargs):
        """Override collect to save results to bronze after fetching."""
        result = super().collect(start_date, end_date, use_cache, **kwargs)
        if result.success and result.data is not None and not getattr(result, 'from_cache', False):
            try:
                records = (
                    result.data.to_dict('records')
                    if hasattr(result.data, 'to_dict')
                    else result.data
                )
                if records:
                    saved = self.save_to_bronze(records)
                    result.records_fetched = saved
            except Exception as e:
                self.logger.error(f"Bronze save failed (data still returned): {e}")
        return result

    # ------------------------------------------------------------------
    # fetch_data — main entry point
    # ------------------------------------------------------------------
    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        tables: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from Statistics Canada via CSV bulk download.

        Args:
            start_date: Earliest ref_date to keep (default: min_year-01-01)
            end_date: Latest ref_date to keep (default: today)
            tables: List of table keys (default: config.tables)

        Returns:
            CollectorResult with parsed records
        """
        tables = tables or self.config.tables
        end_date = end_date or date.today()
        start_date = start_date or date(self.config.min_year, 1, 1)

        all_records = []
        warnings = []

        for table_key in tables:
            if table_key not in STATSCAN_TABLES:
                warnings.append(f"Unknown table key: {table_key}")
                continue

            table_info = STATSCAN_TABLES[table_key]
            self.logger.info(
                f"Downloading StatsCan {table_key} ({table_info['product_id']}): "
                f"{table_info['name']}"
            )

            try:
                records = self._download_and_parse_csv(
                    table_key, table_info, start_date, end_date
                )
                all_records.extend(records)
                self.logger.info(
                    f"Parsed {len(records)} records from {table_key}"
                )
            except Exception as e:
                warnings.append(f"{table_key}: {e}")
                self.logger.error(
                    f"Error fetching {table_key}: {e}", exc_info=True
                )

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved from any table",
                warnings=warnings
            )

        # Build DataFrame
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            df = df.sort_values(['table_key', 'ref_date']).reset_index(drop=True)
        else:
            df = all_records

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=df,
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
            warnings=warnings
        )

    # ------------------------------------------------------------------
    # CSV download and parse
    # ------------------------------------------------------------------
    def _download_and_parse_csv(
        self,
        table_key: str,
        table_info: Dict,
        start_date: date,
        end_date: date
    ) -> List[Dict]:
        """
        Download a StatsCan bulk CSV ZIP, extract, and parse to records.

        URL pattern:
            https://www150.statcan.gc.ca/n1/tbl/csv/{product_id}-eng.zip

        Each ZIP contains:
            {product_id}.csv          — data rows
            {product_id}_MetaData.csv — dimension metadata (ignored here)

        CSV standard columns:
            REF_DATE, GEO, DGUID, [dimension cols...], UOM, UOM_ID,
            SCALAR_FACTOR, SCALAR_ID, VECTOR, COORDINATE, VALUE,
            STATUS, SYMBOL, TERMINATED, DECIMALS
        """
        product_id = table_info['product_id']
        download_url = f"{self.config.csv_base_url}/{product_id}-eng.zip"

        response, error = self._make_request(
            download_url,
            timeout=self.config.timeout
        )

        if error:
            raise RuntimeError(f"Download failed for {product_id}: {error}")

        if response.status_code != 200:
            raise RuntimeError(
                f"HTTP {response.status_code} downloading {product_id}"
            )

        content_type = response.headers.get('content-type', '')
        if 'zip' not in content_type and 'octet' not in content_type:
            raise RuntimeError(
                f"Unexpected content-type for {product_id}: {content_type}"
            )

        # Extract and parse ZIP
        try:
            zf = zipfile.ZipFile(io.BytesIO(response.content))
        except zipfile.BadZipFile:
            raise RuntimeError(f"Invalid ZIP file for {product_id}")

        # Find the data CSV (not the MetaData file)
        data_files = [
            n for n in zf.namelist()
            if n.endswith('.csv') and 'MetaData' not in n
        ]
        if not data_files:
            raise RuntimeError(f"No data CSV found in ZIP for {product_id}")

        records = []
        with zf.open(data_files[0]) as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f, encoding='utf-8-sig')
            )
            for row in reader:
                parsed = self._parse_csv_row(
                    row, table_key, table_info, start_date, end_date
                )
                if parsed is not None:
                    records.append(parsed)

        return records

    def _parse_csv_row(
        self,
        row: Dict[str, str],
        table_key: str,
        table_info: Dict,
        start_date: date,
        end_date: date
    ) -> Optional[Dict]:
        """
        Parse a single CSV row into a normalised record.

        Returns None if the row should be skipped (wrong date range,
        no value, terminated series, etc.).
        """
        # Skip terminated series
        if row.get('TERMINATED', '').strip():
            return None

        # Parse value
        raw_value = row.get('VALUE', '').strip()
        if not raw_value:
            return None
        try:
            value = float(raw_value)
        except (ValueError, TypeError):
            return None

        # Parse ref_date and filter
        ref_date_str = row.get('REF_DATE', '').strip()
        if not ref_date_str:
            return None

        ref_date = self._parse_ref_date(ref_date_str)
        if ref_date is None:
            return None
        if ref_date < start_date or ref_date > end_date:
            return None

        # Geography filter — only keep known provinces/Canada
        geo = row.get('GEO', '').strip()
        province_code = PROVINCE_MAP.get(geo)
        if province_code is None:
            # Check if it's a sub-aggregate we don't want
            # (e.g. "Maritime provinces", "Prairie provinces")
            return None

        # Extract commodity name
        commodity_col = table_info.get('commodity_col')
        commodity = None
        if commodity_col and commodity_col in row:
            # Dedicated commodity column (e.g. "Type of crop")
            commodity_raw = row[commodity_col].strip().lower()
            commodity = CROP_NAME_MAP.get(commodity_raw, commodity_raw)
        else:
            # No dedicated column — extract commodity by keyword matching
            # from the first attribute column (e.g. farm_prices "Farm products")
            for attr_col in table_info.get('attribute_cols', []):
                text = row.get(attr_col, '').strip().lower()
                if text:
                    for keyword, norm_name in FARM_PRICE_COMMODITY_KEYWORDS:
                        if keyword in text:
                            commodity = norm_name
                            break
                if commodity:
                    break

        # Build the attribute string from attribute columns
        # (e.g. "Harvested area (hectares)" or "Farm and commercial, total")
        attribute_parts = []
        for attr_col in table_info.get('attribute_cols', []):
            val = row.get(attr_col, '').strip()
            if val:
                attribute_parts.append(val)
        attribute = ' | '.join(attribute_parts) if attribute_parts else None

        # Scalar factor (e.g. "thousands" means value is in 000s)
        scalar_factor = row.get('SCALAR_FACTOR', '').strip().lower()

        # Unit of measure
        uom = row.get('UOM', '').strip()

        return {
            'table_key': table_key,
            'product_id': table_info['product_id'],
            'ref_date': ref_date.isoformat(),
            'ref_date_raw': ref_date_str,
            'geo': geo,
            'province_code': province_code,
            'commodity': commodity,
            'attribute': attribute,
            'uom': uom,
            'scalar_factor': scalar_factor,
            'value': value,
            'vector': row.get('VECTOR', '').strip(),
            'coordinate': row.get('COORDINATE', '').strip(),
            'status': row.get('STATUS', '').strip(),
            'decimals': int(row.get('DECIMALS', '0') or '0'),
            'source': 'STATSCAN',
        }

    @staticmethod
    def _parse_ref_date(ref_date_str: str) -> Optional[date]:
        """
        Parse StatsCan REF_DATE values.

        Formats observed:
            '2024'       -> 2024-01-01  (annual)
            '2024-03'    -> 2024-03-01  (monthly/quarterly)
            '2024-03-15' -> 2024-03-15  (daily, rare)
        """
        ref_date_str = ref_date_str.strip()
        for fmt in ('%Y-%m-%d', '%Y-%m', '%Y'):
            try:
                return datetime.strptime(ref_date_str, fmt).date()
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    # save_to_bronze
    # ------------------------------------------------------------------
    def save_to_bronze(self, records: list) -> int:
        """Upsert records to bronze.canada_statscan."""
        if not records:
            return 0

        with get_db_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                cur.execute("""
                    INSERT INTO bronze.canada_statscan
                        (table_key, product_id, ref_date, ref_date_raw,
                         geo, province_code, commodity, attribute,
                         uom, scalar_factor, value, vector, coordinate,
                         status, decimals, source, collected_at)
                    VALUES
                        (%(table_key)s, %(product_id)s, %(ref_date)s,
                         %(ref_date_raw)s, %(geo)s, %(province_code)s,
                         %(commodity)s, %(attribute)s, %(uom)s,
                         %(scalar_factor)s, %(value)s, %(vector)s,
                         %(coordinate)s, %(status)s, %(decimals)s,
                         %(source)s, NOW())
                    ON CONFLICT (product_id, vector, ref_date)
                    DO UPDATE SET
                        value = EXCLUDED.value,
                        status = EXCLUDED.status,
                        collected_at = NOW()
                """, rec)
                count += 1
            conn.commit()
            self.logger.info(
                f"Saved {count} records to bronze.canada_statscan"
            )
            return count

    # ------------------------------------------------------------------
    # parse_response (required by BaseCollector ABC)
    # ------------------------------------------------------------------
    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_grain_stocks(self, commodity: str = None) -> Optional[Any]:
        """
        Get Canadian grain stocks data.

        Args:
            commodity: Optional filter: 'wheat', 'canola', 'barley', 'oats'

        Returns:
            DataFrame with stocks data
        """
        result = self.collect(tables=['grain_stocks'], use_cache=False)

        if not result.success or result.data is None:
            return None

        if commodity and PANDAS_AVAILABLE and hasattr(result.data, 'query'):
            return result.data.query(f"commodity == '{commodity}'")

        return result.data

    def get_crop_production(self, year: int = None) -> Optional[Any]:
        """
        Get crop production estimates.

        Args:
            year: Year for estimates (default: last 3 years)

        Returns:
            DataFrame with production data
        """
        year = year or date.today().year
        result = self.collect(
            tables=['field_crop_production'],
            start_date=date(year - 2, 1, 1),
            end_date=date(year, 12, 31),
            use_cache=False
        )
        return result.data if result.success else None

    def get_farm_prices(self, commodity: str = None) -> Optional[Any]:
        """
        Get farm product prices.

        Args:
            commodity: Optional filter commodity name

        Returns:
            DataFrame with price data
        """
        result = self.collect(tables=['farm_prices'], use_cache=False)
        if not result.success or result.data is None:
            return None

        if commodity and PANDAS_AVAILABLE and hasattr(result.data, 'query'):
            return result.data.query(f"commodity == '{commodity}'")

        return result.data


# =============================================================================
# CANOLA COUNCIL COLLECTOR (unchanged — kept for reference)
# =============================================================================

@dataclass
class CanolaConcilConfig(CollectorConfig):
    """Canola Council of Canada configuration"""
    source_name: str = "Canola Council of Canada"
    source_url: str = "https://www.canolacouncil.org"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY


class CanolaCouncilCollector(BaseCollector):
    """
    Collector for Canola Council of Canada data.

    Provides:
    - Canola production estimates
    - Crushing statistics
    - Export data
    - Industry statistics

    Note: Most data available via PDF reports and requires manual extraction
    or web scraping. Statistics Canada is primary source for production/trade.
    """

    def __init__(self, config: CanolaConcilConfig = None):
        config = config or CanolaConcilConfig()
        super().__init__(config)
        self.config: CanolaConcilConfig = config

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        })

    def get_table_name(self) -> str:
        return "canola_council"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from Canola Council.

        Note: Most Canola Council data is in PDF format or on pages
        requiring JavaScript rendering. This collector focuses on
        available static data.
        """
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year - 1, 1, 1)

        records = []
        warnings = []

        # Try to fetch statistics page
        stats_url = f"{self.config.source_url}/statistics"

        response, error = self._make_request(stats_url)

        if error:
            warnings.append(f"Failed to fetch statistics: {error}")
        elif response.status_code == 200:
            # Parse available data
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for data tables
            tables = soup.find_all('table')
            for table in tables:
                table_records = self._parse_table(table)
                records.extend(table_records)

            # Look for downloadable files
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '.pdf' in href or '.xlsx' in href or '.csv' in href:
                    self.logger.info(f"Found data file: {href}")
        else:
            warnings.append(f"HTTP {response.status_code}")

        # Return with note about primary source
        if not records:
            warnings.append(
                "Canola Council provides limited API data. "
                "Use Statistics Canada (StatsCanCollector) for comprehensive canola statistics."
            )

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(records),
            data=records if records else None,
            period_start=start_date.isoformat(),
            period_end=end_date.isoformat(),
            warnings=warnings
        )

    def _parse_table(self, table) -> List[Dict]:
        """Parse HTML table"""
        records = []

        try:
            rows = table.find_all('tr')
            if len(rows) < 2:
                return records

            headers = [th.get_text(strip=True) for th in rows[0].find_all(['th', 'td'])]

            for row in rows[1:]:
                cells = row.find_all(['td', 'th'])
                values = [cell.get_text(strip=True) for cell in cells]

                if len(values) >= len(headers):
                    record = dict(zip(headers, values))
                    record['source'] = 'CANOLA_COUNCIL'
                    records.append(record)

        except Exception as e:
            self.logger.warning(f"Error parsing table: {e}")

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for Statistics Canada collector"""
    import argparse
    import json

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )

    parser = argparse.ArgumentParser(
        description='Statistics Canada Agricultural Data Collector'
    )

    parser.add_argument(
        'command',
        choices=['fetch', 'stocks', 'production', 'prices', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--tables',
        nargs='+',
        default=None,
        help='Tables to fetch (default: all configured)'
    )

    parser.add_argument(
        '--year',
        type=int,
        help='Year for data'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file'
    )

    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Skip saving to bronze (fetch only)'
    )

    args = parser.parse_args()

    collector = StatsCanCollector()

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'stocks':
        data = collector.get_grain_stocks()
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        else:
            print("No data returned")
        return

    if args.command == 'production':
        data = collector.get_crop_production(args.year)
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        else:
            print("No data returned")
        return

    if args.command == 'prices':
        data = collector.get_farm_prices()
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        else:
            print("No data returned")
        return

    if args.command == 'fetch':
        result = collector.collect(
            tables=args.tables,
            use_cache=False
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if result.warnings:
            print(f"Warnings: {result.warnings}")

        if args.output and result.data is not None:
            if args.output.endswith('.csv') and PANDAS_AVAILABLE:
                result.data.to_csv(args.output, index=False)
            else:
                with open(args.output, 'w') as f:
                    if PANDAS_AVAILABLE and hasattr(result.data, 'to_dict'):
                        json.dump(
                            result.data.to_dict('records'), f,
                            indent=2, default=str
                        )
                    else:
                        json.dump(result.data, f, indent=2, default=str)
            print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
