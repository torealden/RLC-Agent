"""
Statistics Canada Agricultural Data Collector

Collects agricultural statistics from Statistics Canada:
- Field Crop Reporting Series
- Production estimates
- Farm product prices
- Trade statistics
- Crushing and crush data

Data source: https://www150.statcan.gc.ca/n1/en/type/data
API: https://www.statcan.gc.ca/en/developers/wds

Free - No authentication required for most data
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple

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


# Statistics Canada Table IDs for agricultural data
STATSCAN_TABLES = {
    # Field Crops
    'field_crop_areas': {
        'table_id': '32-10-0359-01',
        'name': 'Estimated areas, yield, production, average farm price and total farm value',
        'frequency': 'annual',
        'commodities': ['wheat', 'canola', 'barley', 'oats', 'corn', 'soybeans', 'flaxseed'],
    },
    'crop_production': {
        'table_id': '32-10-0002-01',
        'name': 'Stocks of Canadian grain',
        'frequency': 'quarterly',
        'commodities': ['wheat', 'canola', 'barley', 'oats', 'corn'],
    },
    'grain_stocks': {
        'table_id': '32-10-0002-01',
        'name': 'Stocks of Canadian grain',
        'frequency': 'quarterly',
        'release_months': [3, 6, 9, 12],
        'commodities': ['wheat', 'canola', 'barley', 'oats'],
    },
    # Oilseed crushing
    'canola_crush': {
        'table_id': '32-10-0054-01',
        'name': 'Supply and disposition of crude vegetable oils',
        'frequency': 'monthly',
        'commodities': ['canola oil', 'soybean oil'],
    },
    'oilseed_processing': {
        'table_id': '32-10-0055-01',
        'name': 'Production and stocks of margarine',
        'frequency': 'monthly',
    },
    # Trade
    'exports_monthly': {
        'table_id': '12-10-0119-01',
        'name': 'International merchandise trade by commodity',
        'frequency': 'monthly',
    },
    'imports_monthly': {
        'table_id': '12-10-0121-01',
        'name': 'International merchandise trade by commodity',
        'frequency': 'monthly',
    },
    # Farm prices
    'farm_prices': {
        'table_id': '32-10-0077-01',
        'name': 'Farm product prices, crops and livestock',
        'frequency': 'monthly',
        'commodities': ['wheat', 'canola', 'barley', 'oats'],
    },
    # Livestock
    'cattle_inventory': {
        'table_id': '32-10-0130-01',
        'name': 'Cattle and calves',
        'frequency': 'semi-annual',
    },
    'hog_inventory': {
        'table_id': '32-10-0131-01',
        'name': 'Hogs',
        'frequency': 'quarterly',
    },
}


# HS codes for Canadian agricultural exports
CANADA_HS_CODES = {
    'wheat_durum': '100110',
    'wheat_other': '100190',
    'barley': '1003',
    'oats': '1004',
    'corn': '1005',
    'canola_seed': '120510',
    'soybeans': '1201',
    'flaxseed': '1204',
    'canola_oil': '151411',
    'soybean_oil': '1507',
    'canola_meal': '230649',
}


@dataclass
class StatsCanConfig(CollectorConfig):
    """Statistics Canada configuration"""
    source_name: str = "Statistics Canada"
    source_url: str = "https://www150.statcan.gc.ca"
    api_url: str = "https://www150.statcan.gc.ca/t1/wds/rest"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Tables to fetch
    tables: List[str] = field(default_factory=lambda: [
        'field_crop_areas', 'grain_stocks', 'canola_crush', 'farm_prices'
    ])

    # Request settings
    timeout: int = 60
    retry_attempts: int = 3


class StatsCanCollector(BaseCollector):
    """
    Collector for Statistics Canada agricultural data.

    Uses Statistics Canada Web Data Service (WDS) API.

    Provides:
    - Field crop production estimates
    - Quarterly grain stocks
    - Oilseed crushing statistics
    - Farm prices
    - Trade data
    """

    def __init__(self, config: StatsCanConfig = None):
        config = config or StatsCanConfig()
        super().__init__(config)
        self.config: StatsCanConfig = config

    def get_table_name(self) -> str:
        return "canada_statscan"

    def _get_api_url(self, endpoint: str) -> str:
        """Build API URL"""
        return f"{self.config.api_url}/{endpoint}"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        tables: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from Statistics Canada.

        Args:
            tables: List of table names to fetch
            start_date: Start date for data
            end_date: End date for data

        Returns:
            CollectorResult with agricultural data
        """
        tables = tables or self.config.tables
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year - 2, 1, 1)

        all_records = []
        warnings = []

        for table_name in tables:
            if table_name not in STATSCAN_TABLES:
                warnings.append(f"Unknown table: {table_name}")
                continue

            table_info = STATSCAN_TABLES[table_name]
            self.logger.info(f"Fetching StatsCan {table_info['name']}")

            try:
                records = self._fetch_table_data(
                    table_name, table_info, start_date, end_date
                )
                all_records.extend(records)
                self.logger.info(f"Retrieved {len(records)} records from {table_name}")

            except Exception as e:
                warnings.append(f"{table_name}: {e}")
                self.logger.error(f"Error fetching {table_name}: {e}", exc_info=True)

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved",
                warnings=warnings
            )

        # Convert to DataFrame
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            df = df.sort_values(['table_name', 'ref_date'])
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

    def _fetch_table_data(
        self,
        table_name: str,
        table_info: Dict,
        start_date: date,
        end_date: date
    ) -> List[Dict]:
        """Fetch data for a specific Statistics Canada table"""
        records = []
        table_id = table_info['table_id']

        # Statistics Canada WDS API endpoints
        # Get cube metadata first
        metadata_url = self._get_api_url(f"getCubeMetadata")

        metadata_response, error = self._make_request(
            metadata_url,
            method='POST',
            json_data=[{"productId": table_id}]
        )

        if error:
            self.logger.warning(f"Metadata request failed: {error}")
            # Try direct data fetch anyway

        # Fetch data vectors
        # Use getDataFromCubePidCoordAndLatestNPeriods for recent data
        data_url = self._get_api_url("getDataFromCubePidCoordAndLatestNPeriods")

        # Calculate number of periods based on frequency
        frequency = table_info.get('frequency', 'monthly')
        if frequency == 'monthly':
            n_periods = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)
        elif frequency == 'quarterly':
            n_periods = (end_date.year - start_date.year) * 4 + 4
        elif frequency == 'annual':
            n_periods = end_date.year - start_date.year + 1
        else:
            n_periods = 24  # Default

        n_periods = min(n_periods, 120)  # Limit to 120 periods

        # StatsCan requires specific coordinate format
        request_data = [{
            "productId": table_id,
            "coordinate": "1.1.0.0.0.0.0.0.0.0",  # Default coordinate
            "latestN": n_periods
        }]

        data_response, error = self._make_request(
            data_url,
            method='POST',
            json_data=request_data
        )

        if error:
            # Try alternative endpoint
            return self._fetch_table_csv(table_id, table_name, start_date, end_date)

        if data_response.status_code == 200:
            try:
                data = data_response.json()
                records = self._parse_wds_response(data, table_name, table_info)
            except Exception as e:
                self.logger.warning(f"Error parsing WDS response: {e}")
                # Fallback to CSV download
                return self._fetch_table_csv(table_id, table_name, start_date, end_date)
        else:
            return self._fetch_table_csv(table_id, table_name, start_date, end_date)

        return records

    def _fetch_table_csv(
        self,
        table_id: str,
        table_name: str,
        start_date: date,
        end_date: date
    ) -> List[Dict]:
        """Fetch table data via CSV download (fallback)"""
        records = []

        # Statistics Canada CSV download URL
        csv_url = f"https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid={table_id.replace('-', '')}"

        self.logger.info(f"Attempting CSV fetch for {table_id}")

        # Note: Direct CSV downloads require specific formatting
        # For production, you may need to use their download form

        # Alternative: Use the bulk download endpoint
        download_url = f"https://www150.statcan.gc.ca/n1/en/tbl/csv/{table_id.replace('-', '')}-eng.zip"

        response, error = self._make_request(download_url)

        if error or response.status_code != 200:
            self.logger.info(f"CSV download not available for {table_id}")
            return records

        # If we got a zip file, we'd need to extract and parse
        # For now, return empty and note limitation
        self.logger.info(f"CSV data available at: {download_url}")

        return records

    def _parse_wds_response(
        self,
        data: Any,
        table_name: str,
        table_info: Dict
    ) -> List[Dict]:
        """Parse Statistics Canada WDS API response"""
        records = []

        if not isinstance(data, list):
            return records

        for item in data:
            if item.get('status') != 'SUCCESS':
                continue

            vector_data = item.get('object', {})
            vector_data_points = vector_data.get('vectorDataPoint', [])

            for point in vector_data_points:
                ref_date = point.get('refPer', '')
                value = point.get('value')

                if value is not None:
                    records.append({
                        'table_name': table_name,
                        'table_id': table_info['table_id'],
                        'ref_date': ref_date,
                        'value': float(value) if value else None,
                        'scalar_factor': point.get('scalarFactorCode'),
                        'decimals': point.get('decimals'),
                        'source': 'STATSCAN',
                    })

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_grain_stocks(self, commodity: str = 'wheat') -> Optional[Any]:
        """
        Get Canadian grain stocks data.

        Args:
            commodity: 'wheat', 'canola', 'barley', 'oats'

        Returns:
            DataFrame with stocks data
        """
        result = self.collect(tables=['grain_stocks'])

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'query'):
            # Filter for commodity if possible
            return result.data

        return result.data

    def get_canola_crush(self) -> Optional[Any]:
        """
        Get canola crushing statistics.

        Returns:
            DataFrame with crush data
        """
        result = self.collect(tables=['canola_crush'])
        return result.data if result.success else None

    def get_crop_production(self, year: int = None) -> Optional[Any]:
        """
        Get crop production estimates.

        Args:
            year: Year for estimates (default: current)

        Returns:
            DataFrame with production data
        """
        year = year or date.today().year

        result = self.collect(
            tables=['field_crop_areas'],
            start_date=date(year, 1, 1),
            end_date=date(year, 12, 31)
        )

        return result.data if result.success else None

    def get_farm_prices(self, commodity: str = 'canola') -> Optional[Any]:
        """
        Get farm product prices.

        Args:
            commodity: Commodity name

        Returns:
            DataFrame with price data
        """
        result = self.collect(tables=['farm_prices'])
        return result.data if result.success else None


# =============================================================================
# CANOLA COUNCIL COLLECTOR
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

    parser = argparse.ArgumentParser(description='Statistics Canada Agricultural Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'stocks', 'crush', 'production', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--tables',
        nargs='+',
        default=['grain_stocks', 'canola_crush'],
        help='Tables to fetch'
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
        return

    if args.command == 'crush':
        data = collector.get_canola_crush()
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        return

    if args.command == 'production':
        data = collector.get_crop_production(args.year)
        if data is not None:
            if PANDAS_AVAILABLE and hasattr(data, 'to_string'):
                print(data.to_string())
            else:
                print(json.dumps(data, indent=2, default=str))
        return

    if args.command == 'fetch':
        result = collector.collect(tables=args.tables)

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
                        json.dump(result.data.to_dict('records'), f, indent=2, default=str)
                    else:
                        json.dump(result.data, f, indent=2, default=str)
            print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
