"""
FAOSTAT Collector

Collects global food and agriculture statistics from FAO:
- Production data (crops, livestock)
- Trade flows
- Food balances
- Prices
- Land use

Data source:
- https://www.fao.org/faostat/en/

No API key required - public data access.
Uses bulk download and API endpoints.
"""

import logging
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from io import BytesIO, StringIO
from pathlib import Path

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# FAOSTAT domain codes
FAOSTAT_DOMAINS = {
    'QCL': {
        'name': 'Crops and livestock products',
        'description': 'Production quantity and harvested area',
        'url': 'https://fenixservices.fao.org/faostat/api/v1/en/data/QCL'
    },
    'TCL': {
        'name': 'Crops and livestock products (Trade)',
        'description': 'Import/export quantity and value',
        'url': 'https://fenixservices.fao.org/faostat/api/v1/en/data/TCL'
    },
    'FBS': {
        'name': 'Food Balances',
        'description': 'Supply utilization accounts',
        'url': 'https://fenixservices.fao.org/faostat/api/v1/en/data/FBS'
    },
    'PP': {
        'name': 'Producer Prices',
        'description': 'Annual producer prices',
        'url': 'https://fenixservices.fao.org/faostat/api/v1/en/data/PP'
    },
}

# FAO commodity codes (Item Codes)
FAO_COMMODITY_CODES = {
    'corn': {'code': 56, 'name': 'Maize (corn)'},
    'wheat': {'code': 15, 'name': 'Wheat'},
    'soybeans': {'code': 236, 'name': 'Soya beans'},
    'rice': {'code': 27, 'name': 'Rice'},
    'sorghum': {'code': 83, 'name': 'Sorghum'},
    'barley': {'code': 44, 'name': 'Barley'},
    'oats': {'code': 75, 'name': 'Oats'},
    'cotton': {'code': 767, 'name': 'Cotton lint'},
    'sunflower': {'code': 267, 'name': 'Sunflower seed'},
    'sugar_cane': {'code': 156, 'name': 'Sugar cane'},
    'soybean_oil': {'code': 237, 'name': 'Soya bean oil'},
    'soybean_meal': {'code': 236, 'name': 'Soya bean cake'},  # Different code for cake
}

# FAO country codes for South America
FAO_SA_COUNTRIES = {
    'AR': {'code': 9, 'name': 'Argentina'},
    'BR': {'code': 21, 'name': 'Brazil'},
    'CL': {'code': 40, 'name': 'Chile'},
    'CO': {'code': 44, 'name': 'Colombia'},
    'EC': {'code': 58, 'name': 'Ecuador'},
    'PY': {'code': 169, 'name': 'Paraguay'},
    'PE': {'code': 170, 'name': 'Peru'},
    'UY': {'code': 234, 'name': 'Uruguay'},
    'VE': {'code': 236, 'name': 'Venezuela'},
    'BO': {'code': 19, 'name': 'Bolivia'},
}

# FAO element codes
FAO_ELEMENTS = {
    'area_harvested': 5312,
    'production': 5510,
    'yield': 5419,
    'import_quantity': 5610,
    'import_value': 5622,
    'export_quantity': 5910,
    'export_value': 5922,
}


@dataclass
class FAOSTATConfig(CollectorConfig):
    """FAOSTAT specific configuration"""
    source_name: str = "FAOSTAT"
    source_url: str = "https://www.fao.org/faostat/en/"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.ANNUAL

    # API endpoints
    api_base: str = "https://fenixservices.fao.org/faostat/api/v1"
    bulk_download_base: str = "https://fenixservices.fao.org/faostat/static/bulkdownloads"

    # Target countries (South America focus)
    countries: List[str] = field(default_factory=lambda: [
        'AR', 'BR', 'CO', 'PY', 'UY', 'BO', 'CL', 'PE', 'EC'
    ])

    # Commodities
    commodities: List[str] = field(default_factory=lambda: [
        'corn', 'wheat', 'soybeans', 'rice', 'sorghum', 'barley',
        'cotton', 'sunflower', 'soybean_oil'
    ])

    # Rate limiting
    rate_limit_per_minute: int = 30
    timeout: int = 120


class FAOSTATCollector(BaseCollector):
    """
    Collector for FAOSTAT global agricultural data.

    FAOSTAT provides free access to food and agriculture data for
    over 245 countries from 1961 to present.

    Key data:
    - Production quantities and area harvested
    - Trade flows (import/export quantities and values)
    - Food balance sheets
    - Producer prices

    No API key required.
    """

    def __init__(self, config: FAOSTATConfig = None):
        config = config or FAOSTATConfig()
        super().__init__(config)
        self.config: FAOSTATConfig = config

    def get_table_name(self) -> str:
        return "faostat_data"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        data_type: str = "production",
        commodities: List[str] = None,
        countries: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from FAOSTAT.

        Args:
            start_date: Start year
            end_date: End year (default: latest available)
            data_type: 'production', 'trade', 'food_balance', or 'prices'
            commodities: List of commodities to fetch
            countries: List of country codes (ISO2)

        Returns:
            CollectorResult with fetched data
        """
        commodities = commodities or self.config.commodities
        countries = countries or self.config.countries

        # Convert dates to years
        start_year = start_date.year if start_date else 2015
        end_year = end_date.year if end_date else datetime.now().year - 1

        if data_type == "production":
            return self._fetch_production(commodities, countries, start_year, end_year)
        elif data_type == "trade":
            return self._fetch_trade(commodities, countries, start_year, end_year)
        elif data_type == "food_balance":
            return self._fetch_food_balance(commodities, countries, start_year, end_year)
        elif data_type == "prices":
            return self._fetch_prices(commodities, countries, start_year, end_year)
        else:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Unknown data_type: {data_type}"
            )

    def _fetch_production(
        self,
        commodities: List[str],
        countries: List[str],
        start_year: int,
        end_year: int
    ) -> CollectorResult:
        """Fetch crop production data from FAOSTAT QCL domain"""
        all_records = []
        warnings = []

        # Build item codes list
        item_codes = []
        for commodity in commodities:
            if commodity in FAO_COMMODITY_CODES:
                item_codes.append(FAO_COMMODITY_CODES[commodity]['code'])
            else:
                warnings.append(f"Unknown commodity: {commodity}")

        # Build country codes list
        country_codes = []
        for country in countries:
            if country in FAO_SA_COUNTRIES:
                country_codes.append(FAO_SA_COUNTRIES[country]['code'])

        # Years
        years = list(range(start_year, end_year + 1))

        # Elements: area harvested, production, yield
        elements = [5312, 5510, 5419]

        # Build API URL
        url = f"{self.config.api_base}/en/data/QCL"

        params = {
            'area': ','.join(str(c) for c in country_codes),
            'item': ','.join(str(i) for i in item_codes),
            'element': ','.join(str(e) for e in elements),
            'year': ','.join(str(y) for y in years),
            'output_type': 'json'
        }

        response, error = self._make_request(url, params=params)

        if error:
            # Try bulk download as fallback
            self.logger.warning(f"API request failed: {error}, trying bulk download")
            return self._fetch_bulk_download('QCL', commodities, countries, start_year, end_year)

        if response.status_code != 200:
            warnings.append(f"API returned {response.status_code}")
            return self._fetch_bulk_download('QCL', commodities, countries, start_year, end_year)

        try:
            data = response.json()

            if 'data' in data:
                for record in data['data']:
                    parsed = self._parse_production_record(record)
                    if parsed:
                        all_records.append(parsed)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {str(e)}",
                warnings=warnings
            )

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            period_start=str(start_year),
            period_end=str(end_year),
            warnings=warnings
        )

    def _fetch_trade(
        self,
        commodities: List[str],
        countries: List[str],
        start_year: int,
        end_year: int
    ) -> CollectorResult:
        """Fetch trade data from FAOSTAT TCL domain"""
        all_records = []
        warnings = []

        item_codes = [FAO_COMMODITY_CODES[c]['code'] for c in commodities if c in FAO_COMMODITY_CODES]
        country_codes = [FAO_SA_COUNTRIES[c]['code'] for c in countries if c in FAO_SA_COUNTRIES]
        years = list(range(start_year, end_year + 1))

        # Elements: import/export quantity and value
        elements = [5610, 5622, 5910, 5922]

        url = f"{self.config.api_base}/en/data/TCL"

        params = {
            'area': ','.join(str(c) for c in country_codes),
            'item': ','.join(str(i) for i in item_codes),
            'element': ','.join(str(e) for e in elements),
            'year': ','.join(str(y) for y in years),
            'output_type': 'json'
        }

        response, error = self._make_request(url, params=params)

        if error:
            self.logger.warning(f"Trade API failed: {error}")
            return self._fetch_bulk_download('TCL', commodities, countries, start_year, end_year)

        if response.status_code != 200:
            return self._fetch_bulk_download('TCL', commodities, countries, start_year, end_year)

        try:
            data = response.json()

            if 'data' in data:
                for record in data['data']:
                    parsed = self._parse_trade_record(record)
                    if parsed:
                        all_records.append(parsed)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {str(e)}",
                warnings=warnings
            )

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            period_start=str(start_year),
            period_end=str(end_year),
            warnings=warnings
        )

    def _fetch_food_balance(
        self,
        commodities: List[str],
        countries: List[str],
        start_year: int,
        end_year: int
    ) -> CollectorResult:
        """Fetch food balance sheet data"""
        all_records = []
        warnings = []

        item_codes = [FAO_COMMODITY_CODES[c]['code'] for c in commodities if c in FAO_COMMODITY_CODES]
        country_codes = [FAO_SA_COUNTRIES[c]['code'] for c in countries if c in FAO_SA_COUNTRIES]
        years = list(range(start_year, end_year + 1))

        url = f"{self.config.api_base}/en/data/FBS"

        params = {
            'area': ','.join(str(c) for c in country_codes),
            'item': ','.join(str(i) for i in item_codes),
            'year': ','.join(str(y) for y in years),
            'output_type': 'json'
        }

        response, error = self._make_request(url, params=params)

        if error or response.status_code != 200:
            warnings.append("Food balance API unavailable")
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="Food balance API unavailable",
                warnings=warnings
            )

        try:
            data = response.json()

            if 'data' in data:
                for record in data['data']:
                    parsed = self._parse_food_balance_record(record)
                    if parsed:
                        all_records.append(parsed)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {str(e)}",
                warnings=warnings
            )

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            warnings=warnings
        )

    def _fetch_prices(
        self,
        commodities: List[str],
        countries: List[str],
        start_year: int,
        end_year: int
    ) -> CollectorResult:
        """Fetch producer price data"""
        all_records = []
        warnings = []

        item_codes = [FAO_COMMODITY_CODES[c]['code'] for c in commodities if c in FAO_COMMODITY_CODES]
        country_codes = [FAO_SA_COUNTRIES[c]['code'] for c in countries if c in FAO_SA_COUNTRIES]
        years = list(range(start_year, end_year + 1))

        url = f"{self.config.api_base}/en/data/PP"

        params = {
            'area': ','.join(str(c) for c in country_codes),
            'item': ','.join(str(i) for i in item_codes),
            'year': ','.join(str(y) for y in years),
            'output_type': 'json'
        }

        response, error = self._make_request(url, params=params)

        if error or response.status_code != 200:
            warnings.append("Price API unavailable")
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="Price API unavailable",
                warnings=warnings
            )

        try:
            data = response.json()

            if 'data' in data:
                for record in data['data']:
                    parsed = self._parse_price_record(record)
                    if parsed:
                        all_records.append(parsed)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {str(e)}",
                warnings=warnings
            )

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            warnings=warnings
        )

    def _fetch_bulk_download(
        self,
        domain: str,
        commodities: List[str],
        countries: List[str],
        start_year: int,
        end_year: int
    ) -> CollectorResult:
        """
        Fallback: Download bulk CSV file from FAOSTAT.

        Bulk downloads are large but don't have the same API limitations.
        """
        all_records = []
        warnings = []

        # Bulk download URL pattern
        bulk_url = f"{self.config.bulk_download_base}/{domain}_E_All_Data_(Normalized).zip"

        self.logger.info(f"Attempting bulk download from {bulk_url}")

        response, error = self._make_request(bulk_url, timeout=300)

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Bulk download failed: {error}",
                warnings=warnings
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Bulk download HTTP {response.status_code}",
                warnings=warnings
            )

        try:
            # Extract and parse ZIP file
            with zipfile.ZipFile(BytesIO(response.content)) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]

                if not csv_files:
                    return CollectorResult(
                        success=False,
                        source=self.config.source_name,
                        error_message="No CSV files in bulk download"
                    )

                # Read the main data file
                with zf.open(csv_files[0]) as f:
                    if PANDAS_AVAILABLE:
                        df = pd.read_csv(f, encoding='utf-8')

                        # Filter by countries and commodities
                        country_names = [FAO_SA_COUNTRIES[c]['name'] for c in countries if c in FAO_SA_COUNTRIES]
                        item_names = [FAO_COMMODITY_CODES[c]['name'] for c in commodities if c in FAO_COMMODITY_CODES]

                        if 'Area' in df.columns:
                            df = df[df['Area'].isin(country_names)]
                        if 'Item' in df.columns:
                            df = df[df['Item'].isin(item_names)]
                        if 'Year' in df.columns:
                            df = df[(df['Year'] >= start_year) & (df['Year'] <= end_year)]

                        for _, row in df.iterrows():
                            record = {
                                'country': row.get('Area', ''),
                                'commodity': row.get('Item', ''),
                                'element': row.get('Element', ''),
                                'year': row.get('Year', ''),
                                'value': row.get('Value', ''),
                                'unit': row.get('Unit', ''),
                                'source': 'FAOSTAT',
                                'collected_at': datetime.now().isoformat()
                            }
                            all_records.append(record)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Bulk parse error: {str(e)}",
                warnings=warnings
            )

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            warnings=warnings
        )

    def _parse_production_record(self, record: Dict) -> Optional[Dict]:
        """Parse production record from API response"""
        try:
            # Reverse lookup commodity name
            item_code = record.get('Item Code')
            commodity = None
            for name, info in FAO_COMMODITY_CODES.items():
                if info['code'] == item_code:
                    commodity = name
                    break

            # Reverse lookup country
            area_code = record.get('Area Code')
            country = None
            for code, info in FAO_SA_COUNTRIES.items():
                if info['code'] == area_code:
                    country = code
                    break

            element = record.get('Element', '')

            return {
                'country': country or record.get('Area', ''),
                'country_name': record.get('Area', ''),
                'commodity': commodity or record.get('Item', ''),
                'commodity_name': record.get('Item', ''),
                'element': element,
                'year': record.get('Year'),
                'value': self._safe_float(record.get('Value')),
                'unit': record.get('Unit', ''),
                'flag': record.get('Flag', ''),
                'source': 'FAOSTAT',
                'domain': 'QCL',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing production record: {e}")
            return None

    def _parse_trade_record(self, record: Dict) -> Optional[Dict]:
        """Parse trade record from API response"""
        try:
            item_code = record.get('Item Code')
            commodity = None
            for name, info in FAO_COMMODITY_CODES.items():
                if info['code'] == item_code:
                    commodity = name
                    break

            area_code = record.get('Area Code')
            country = None
            for code, info in FAO_SA_COUNTRIES.items():
                if info['code'] == area_code:
                    country = code
                    break

            element = record.get('Element', '')

            # Determine if import or export
            flow = 'export' if 'Export' in element else 'import' if 'Import' in element else 'other'
            measure = 'quantity' if 'Quantity' in element else 'value' if 'Value' in element else 'other'

            return {
                'country': country or record.get('Area', ''),
                'country_name': record.get('Area', ''),
                'commodity': commodity or record.get('Item', ''),
                'commodity_name': record.get('Item', ''),
                'flow': flow,
                'measure': measure,
                'element': element,
                'year': record.get('Year'),
                'value': self._safe_float(record.get('Value')),
                'unit': record.get('Unit', ''),
                'partner': record.get('Partner Countries', 'World'),
                'source': 'FAOSTAT',
                'domain': 'TCL',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing trade record: {e}")
            return None

    def _parse_food_balance_record(self, record: Dict) -> Optional[Dict]:
        """Parse food balance record"""
        try:
            return {
                'country': record.get('Area', ''),
                'commodity': record.get('Item', ''),
                'element': record.get('Element', ''),
                'year': record.get('Year'),
                'value': self._safe_float(record.get('Value')),
                'unit': record.get('Unit', ''),
                'source': 'FAOSTAT',
                'domain': 'FBS',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing food balance record: {e}")
            return None

    def _parse_price_record(self, record: Dict) -> Optional[Dict]:
        """Parse price record"""
        try:
            return {
                'country': record.get('Area', ''),
                'commodity': record.get('Item', ''),
                'year': record.get('Year'),
                'price': self._safe_float(record.get('Value')),
                'unit': record.get('Unit', ''),
                'currency': 'USD',
                'source': 'FAOSTAT',
                'domain': 'PP',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing price record: {e}")
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == '' or str(value).strip() == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse API response"""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_south_america_production(
        self,
        commodity: str,
        years: int = 5
    ) -> Optional[Any]:
        """
        Get production data for South American countries.

        Args:
            commodity: Commodity name
            years: Number of years of history

        Returns:
            DataFrame or list of records
        """
        end_year = datetime.now().year - 1
        start_year = end_year - years + 1

        result = self.collect(
            start_date=date(start_year, 1, 1),
            end_date=date(end_year, 12, 31),
            data_type="production",
            commodities=[commodity],
            countries=self.config.countries
        )

        return result.data if result.success else None

    def get_brazil_argentina_comparison(
        self,
        commodity: str
    ) -> Optional[Dict]:
        """
        Get production comparison between Brazil and Argentina.

        Args:
            commodity: Commodity name

        Returns:
            Dict with comparison data
        """
        result = self.collect(
            data_type="production",
            commodities=[commodity],
            countries=['BR', 'AR']
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'pivot_table'):
            df = result.data
            # Filter for production element
            df_prod = df[df['element'].str.contains('Production', case=False, na=False)]

            if not df_prod.empty:
                pivot = df_prod.pivot_table(
                    index='year',
                    columns='country',
                    values='value',
                    aggfunc='sum'
                )
                return pivot.to_dict()

        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for FAOSTAT collector"""
    import argparse
    import json

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='FAOSTAT Data Collector')

    parser.add_argument(
        'command',
        choices=['production', 'trade', 'food_balance', 'prices', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--commodities',
        nargs='+',
        default=['soybeans', 'corn', 'wheat'],
        help='Commodities to fetch'
    )

    parser.add_argument(
        '--countries',
        nargs='+',
        default=['BR', 'AR', 'PY', 'UY'],
        help='Countries to fetch (ISO2 codes)'
    )

    parser.add_argument(
        '--years',
        type=int,
        default=5,
        help='Number of years of history'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    config = FAOSTATConfig(
        commodities=args.commodities,
        countries=args.countries
    )
    collector = FAOSTATCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    end_year = datetime.now().year - 1
    start_year = end_year - args.years + 1

    result = collector.collect(
        start_date=date(start_year, 1, 1),
        end_date=date(end_year, 12, 31),
        data_type=args.command,
        commodities=args.commodities,
        countries=args.countries
    )

    print(f"Success: {result.success}")
    print(f"Records: {result.records_fetched}")

    if result.warnings:
        print(f"Warnings: {result.warnings}")

    if result.error_message:
        print(f"Error: {result.error_message}")

    if args.output and result.data is not None:
        if args.output.endswith('.csv') and PANDAS_AVAILABLE:
            result.data.to_csv(args.output, index=False)
        else:
            if PANDAS_AVAILABLE and hasattr(result.data, 'to_json'):
                result.data.to_json(args.output, orient='records', date_format='iso')
            else:
                with open(args.output, 'w') as f:
                    json.dump(result.data, f, default=str)
        print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
