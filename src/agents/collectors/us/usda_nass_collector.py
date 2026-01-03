"""
USDA NASS Quick Stats Collector

Collects data from USDA National Agricultural Statistics Service:
- Crop Progress & Condition (weekly during growing season)
- Acreage (planted, harvested)
- Production estimates
- Stocks (quarterly)
- Prices received

Requires free API key from: https://quickstats.nass.usda.gov/api
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

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


# NASS Commodity mappings
NASS_COMMODITIES = {
    'corn': {'commodity_desc': 'CORN', 'class_desc': 'ALL CLASSES'},
    'soybeans': {'commodity_desc': 'SOYBEANS', 'class_desc': 'ALL CLASSES'},
    'wheat': {'commodity_desc': 'WHEAT', 'class_desc': 'ALL CLASSES'},
    'wheat_winter': {'commodity_desc': 'WHEAT', 'class_desc': 'WINTER'},
    'wheat_spring': {'commodity_desc': 'WHEAT', 'class_desc': 'SPRING, (EXCL DURUM)'},
    'wheat_durum': {'commodity_desc': 'WHEAT', 'class_desc': 'DURUM'},
    'sorghum': {'commodity_desc': 'SORGHUM', 'class_desc': 'ALL CLASSES'},
    'barley': {'commodity_desc': 'BARLEY', 'class_desc': 'ALL CLASSES'},
    'oats': {'commodity_desc': 'OATS', 'class_desc': 'ALL CLASSES'},
    'cotton': {'commodity_desc': 'COTTON', 'class_desc': 'ALL CLASSES'},
    'sunflower': {'commodity_desc': 'SUNFLOWER', 'class_desc': 'ALL CLASSES'},
    'canola': {'commodity_desc': 'CANOLA', 'class_desc': 'ALL CLASSES'},
}

# Crop condition categories
CONDITION_CATEGORIES = ['EXCELLENT', 'GOOD', 'FAIR', 'POOR', 'VERY POOR']


@dataclass
class NASSConfig(CollectorConfig):
    """USDA NASS Quick Stats configuration"""
    source_name: str = "USDA NASS"
    source_url: str = "https://quickstats.nass.usda.gov/api"
    auth_type: AuthType = AuthType.API_KEY
    frequency: DataFrequency = DataFrequency.WEEKLY

    # API key from environment or direct setting
    api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get('NASS_API_KEY')
    )

    # Default commodities
    commodities: List[str] = field(default_factory=lambda: [
        'corn', 'soybeans', 'wheat', 'sorghum'
    ])

    # Rate limit (NASS allows ~50k queries/day)
    rate_limit_per_minute: int = 30


class NASSCollector(BaseCollector):
    """
    Collector for USDA NASS Quick Stats data.

    Provides:
    - Weekly crop progress (planting, emergence, silking, harvest)
    - Weekly crop condition ratings
    - Acreage estimates (prospective plantings, actual)
    - Production estimates
    - Grain stocks (quarterly)

    Release Schedule:
    - Crop Progress: Monday 4:00 PM ET (Apr-Nov)
    - Prospective Plantings: Late March
    - Acreage: Late June
    - Production: Monthly (Aug-Jan)
    - Grain Stocks: Quarterly
    """

    def __init__(self, config: NASSConfig = None):
        config = config or NASSConfig()
        super().__init__(config)
        self.config: NASSConfig = config

        if not self.config.api_key:
            self.logger.warning(
                "No NASS API key. Register at: https://quickstats.nass.usda.gov/api"
            )

    def get_table_name(self) -> str:
        return "usda_nass"

    def _build_query_params(
        self,
        commodity: str,
        statisticcat_desc: str,
        year: int = None,
        state_alpha: str = None,
        agg_level_desc: str = "NATIONAL",
        freq_desc: str = None,
        **kwargs
    ) -> Dict[str, str]:
        """Build NASS API query parameters"""
        params = {
            'key': self.config.api_key,
            'format': 'JSON',
            'agg_level_desc': agg_level_desc,
        }

        # Add commodity
        if commodity in NASS_COMMODITIES:
            comm_info = NASS_COMMODITIES[commodity]
            params['commodity_desc'] = comm_info['commodity_desc']
            if comm_info['class_desc'] != 'ALL CLASSES':
                params['class_desc'] = comm_info['class_desc']
        else:
            params['commodity_desc'] = commodity.upper()

        # Add statistic category
        params['statisticcat_desc'] = statisticcat_desc

        if year:
            params['year'] = str(year)

        if state_alpha:
            params['state_alpha'] = state_alpha

        if freq_desc:
            params['freq_desc'] = freq_desc

        return params

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        data_type: str = "crop_progress",
        commodities: List[str] = None,
        year: int = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from NASS API.

        Args:
            data_type: 'crop_progress', 'condition', 'acreage', 'production', 'stocks'
            commodities: List of commodities to fetch
            year: Specific year (default: current)
        """
        if not self.config.api_key:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No API key. Set NASS_API_KEY environment variable."
            )

        commodities = commodities or self.config.commodities
        year = year or datetime.now().year

        if data_type == "crop_progress":
            return self._fetch_crop_progress(commodities, year)
        elif data_type == "condition":
            return self._fetch_crop_condition(commodities, year)
        elif data_type == "acreage":
            return self._fetch_acreage(commodities, year)
        elif data_type == "production":
            return self._fetch_production(commodities, year)
        elif data_type == "stocks":
            return self._fetch_stocks(commodities, year)
        else:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Unknown data_type: {data_type}"
            )

    def _fetch_crop_progress(
        self,
        commodities: List[str],
        year: int
    ) -> CollectorResult:
        """Fetch crop progress data (planting, harvest, etc.)"""
        all_records = []
        warnings = []

        for commodity in commodities:
            params = self._build_query_params(
                commodity=commodity,
                statisticcat_desc='PROGRESS',
                year=year,
                freq_desc='WEEKLY'
            )

            url = f"{self.config.source_url}/api_GET/"
            response, error = self._make_request(url, params=params)

            if error:
                warnings.append(f"{commodity}: {error}")
                continue

            if response.status_code == 401:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="Invalid API key"
                )

            if response.status_code != 200:
                warnings.append(f"{commodity}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()
                if 'data' in data:
                    for item in data['data']:
                        records = self._parse_nass_record(item, commodity, 'progress')
                        all_records.extend(records)
            except Exception as e:
                warnings.append(f"{commodity}: Parse error - {e}")

        return self._build_result(all_records, warnings)

    def _fetch_crop_condition(
        self,
        commodities: List[str],
        year: int
    ) -> CollectorResult:
        """Fetch crop condition ratings"""
        all_records = []
        warnings = []

        for commodity in commodities:
            params = self._build_query_params(
                commodity=commodity,
                statisticcat_desc='CONDITION',
                year=year,
                freq_desc='WEEKLY'
            )

            url = f"{self.config.source_url}/api_GET/"
            response, error = self._make_request(url, params=params)

            if error:
                warnings.append(f"{commodity}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{commodity}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()
                if 'data' in data:
                    for item in data['data']:
                        records = self._parse_nass_record(item, commodity, 'condition')
                        all_records.extend(records)
            except Exception as e:
                warnings.append(f"{commodity}: Parse error - {e}")

        return self._build_result(all_records, warnings)

    def _fetch_acreage(
        self,
        commodities: List[str],
        year: int
    ) -> CollectorResult:
        """Fetch planted/harvested acreage"""
        all_records = []
        warnings = []

        for commodity in commodities:
            for stat_cat in ['AREA PLANTED', 'AREA HARVESTED']:
                params = self._build_query_params(
                    commodity=commodity,
                    statisticcat_desc=stat_cat,
                    year=year,
                    freq_desc='ANNUAL'
                )

                url = f"{self.config.source_url}/api_GET/"
                response, error = self._make_request(url, params=params)

                if error:
                    warnings.append(f"{commodity}/{stat_cat}: {error}")
                    continue

                if response.status_code != 200:
                    warnings.append(f"{commodity}/{stat_cat}: HTTP {response.status_code}")
                    continue

                try:
                    data = response.json()
                    if 'data' in data:
                        for item in data['data']:
                            records = self._parse_nass_record(item, commodity, 'acreage')
                            all_records.extend(records)
                except Exception as e:
                    warnings.append(f"{commodity}/{stat_cat}: Parse error - {e}")

        return self._build_result(all_records, warnings)

    def _fetch_production(
        self,
        commodities: List[str],
        year: int
    ) -> CollectorResult:
        """Fetch production estimates"""
        all_records = []
        warnings = []

        for commodity in commodities:
            params = self._build_query_params(
                commodity=commodity,
                statisticcat_desc='PRODUCTION',
                year=year,
                freq_desc='ANNUAL'
            )

            url = f"{self.config.source_url}/api_GET/"
            response, error = self._make_request(url, params=params)

            if error:
                warnings.append(f"{commodity}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{commodity}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()
                if 'data' in data:
                    for item in data['data']:
                        records = self._parse_nass_record(item, commodity, 'production')
                        all_records.extend(records)
            except Exception as e:
                warnings.append(f"{commodity}: Parse error - {e}")

        return self._build_result(all_records, warnings)

    def _fetch_stocks(
        self,
        commodities: List[str],
        year: int
    ) -> CollectorResult:
        """Fetch grain stocks (quarterly)"""
        all_records = []
        warnings = []

        for commodity in commodities:
            params = self._build_query_params(
                commodity=commodity,
                statisticcat_desc='STOCKS',
                year=year
            )

            url = f"{self.config.source_url}/api_GET/"
            response, error = self._make_request(url, params=params)

            if error:
                warnings.append(f"{commodity}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{commodity}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()
                if 'data' in data:
                    for item in data['data']:
                        records = self._parse_nass_record(item, commodity, 'stocks')
                        all_records.extend(records)
            except Exception as e:
                warnings.append(f"{commodity}: Parse error - {e}")

        return self._build_result(all_records, warnings)

    def _parse_nass_record(
        self,
        item: Dict,
        commodity: str,
        data_type: str
    ) -> List[Dict]:
        """Parse a NASS API response record"""
        records = []

        try:
            value_str = item.get('Value', '')
            # Remove commas and convert
            value_str = str(value_str).replace(',', '')

            try:
                value = float(value_str)
            except ValueError:
                return records

            record = {
                'commodity': commodity,
                'data_type': data_type,
                'year': item.get('year'),
                'week_ending': item.get('week_ending'),
                'reference_period_desc': item.get('reference_period_desc'),
                'state': item.get('state_alpha', 'US'),
                'agg_level': item.get('agg_level_desc'),
                'statisticcat': item.get('statisticcat_desc'),
                'unit': item.get('unit_desc'),
                'short_desc': item.get('short_desc'),
                'value': value,
                'source': 'USDA_NASS',
            }

            # Add domain info for condition ratings
            if item.get('domaincat_desc'):
                record['domain'] = item['domaincat_desc']

            records.append(record)

        except Exception as e:
            self.logger.warning(f"Error parsing record: {e}")

        return records

    def _build_result(
        self,
        records: List[Dict],
        warnings: List[str]
    ) -> CollectorResult:
        """Build collector result from records"""
        if not records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved",
                warnings=warnings
            )

        if PANDAS_AVAILABLE:
            df = pd.DataFrame(records)
        else:
            df = records

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(records),
            data=df,
            warnings=warnings
        )

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_current_crop_progress(
        self,
        commodity: str = 'corn'
    ) -> Optional[Dict]:
        """Get latest crop progress for a commodity"""
        result = self.collect(
            data_type="crop_progress",
            commodities=[commodity],
            year=datetime.now().year
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'iloc'):
            df = result.data
            if df.empty:
                return None

            # Get latest week
            latest = df.sort_values('week_ending').iloc[-1]
            return latest.to_dict()

        return None

    def get_good_excellent_rating(
        self,
        commodity: str = 'corn',
        year: int = None
    ) -> Optional[Any]:
        """
        Get weekly good/excellent crop condition ratings.

        Returns DataFrame with weekly G/E percentages.
        """
        year = year or datetime.now().year

        result = self.collect(
            data_type="condition",
            commodities=[commodity],
            year=year
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            df = result.data

            # Filter for Good and Excellent
            ge_df = df[df['short_desc'].str.contains('GOOD|EXCELLENT', case=False, na=False)]

            # Pivot to get weekly summary
            if not ge_df.empty:
                pivot = ge_df.groupby('week_ending')['value'].sum().reset_index()
                pivot.columns = ['week_ending', 'good_excellent_pct']
                return pivot

        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for NASS collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='USDA NASS Data Collector')

    parser.add_argument(
        'data_type',
        choices=['crop_progress', 'condition', 'acreage', 'production', 'stocks'],
        help='Type of data to fetch'
    )

    parser.add_argument(
        '--commodities',
        nargs='+',
        default=['corn', 'soybeans'],
        help='Commodities to fetch'
    )

    parser.add_argument(
        '--year',
        type=int,
        default=datetime.now().year,
        help='Year to fetch'
    )

    parser.add_argument(
        '--api-key',
        help='NASS API key (or set NASS_API_KEY env var)'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    if args.api_key:
        os.environ['NASS_API_KEY'] = args.api_key

    collector = NASSCollector()

    result = collector.collect(
        data_type=args.data_type,
        commodities=args.commodities,
        year=args.year
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
                result.data.to_json(args.output, orient='records')
        print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
