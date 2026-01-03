"""
EIA Ethanol Data Collector

Collects ethanol production, stocks, and blending data from the
US Energy Information Administration (EIA).

Requires free API key from: https://www.eia.gov/opendata/register.php

Key series:
- Weekly ethanol production (thousand barrels/day)
- Weekly ethanol stocks (million barrels)
- Weekly ethanol imports
- Weekly ethanol blending (fuel ethanol blended into motor gasoline)
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


# EIA Series IDs for ethanol data
EIA_ETHANOL_SERIES = {
    'production': {
        'series_id': 'PET.W_EPOOXE_YOP_NUS_MBBLD.W',
        'description': 'Weekly U.S. Fuel Ethanol Production',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },
    'stocks': {
        'series_id': 'STEO.ETHSTUS.M',
        'description': 'U.S. Fuel Ethanol Ending Stocks',
        'unit': 'million barrels',
        'frequency': 'monthly',
    },
    'stocks_weekly': {
        'series_id': 'PET.WCESTUS1.W',
        'description': 'Weekly U.S. Ending Stocks of Fuel Ethanol',
        'unit': 'thousand barrels',
        'frequency': 'weekly',
    },
    'imports': {
        'series_id': 'PET.W_EPOOXE_IM0_NUS-Z00_MBBLD.W',
        'description': 'Weekly U.S. Imports of Fuel Ethanol',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },
    'blending': {
        'series_id': 'PET.W_EPOOXE_YPB_NUS_MBBLD.W',
        'description': 'Weekly U.S. Refiner and Blender Net Input of Fuel Ethanol',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },
    'exports': {
        'series_id': 'PET.W_EPOOXE_EX0_NUS-Z00_MBBLD.W',
        'description': 'Weekly U.S. Exports of Fuel Ethanol',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },
}


@dataclass
class EIAEthanolConfig(CollectorConfig):
    """EIA Ethanol specific configuration"""
    source_name: str = "EIA Ethanol"
    source_url: str = "https://api.eia.gov/v2"
    auth_type: AuthType = AuthType.API_KEY
    frequency: DataFrequency = DataFrequency.WEEKLY

    # API key - can be set via environment variable
    api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get('EIA_API_KEY')
    )

    # Series to fetch
    series: List[str] = field(default_factory=lambda: [
        'production', 'stocks_weekly', 'imports', 'blending'
    ])

    # API version
    api_version: str = "v2"


class EIAEthanolCollector(BaseCollector):
    """
    Collector for EIA ethanol data.

    Uses EIA Open Data API v2.
    Requires API key (free registration).

    Features:
    - Weekly ethanol production data
    - Weekly stocks data
    - Import/export volumes
    - Corn grind estimation
    """

    # Corn grind calculation constants
    GALLONS_PER_BARREL = 42
    GALLONS_ETHANOL_PER_BUSHEL_CORN = 2.8
    DAYS_PER_WEEK = 7

    def __init__(self, config: EIAEthanolConfig = None):
        config = config or EIAEthanolConfig()
        super().__init__(config)
        self.config: EIAEthanolConfig = config

        if not self.config.api_key:
            self.logger.warning(
                "No EIA API key configured. Set EIA_API_KEY environment variable "
                "or register at https://www.eia.gov/opendata/register.php"
            )

    def get_table_name(self) -> str:
        return "eia_ethanol"

    def _build_api_url(self, series_id: str) -> str:
        """Build API URL for a series"""
        # EIA API v2 format
        return f"{self.config.source_url}/seriesid/{series_id}"

    def _build_params(
        self,
        start_date: date = None,
        end_date: date = None,
        limit: int = 100
    ) -> Dict[str, str]:
        """Build API query parameters"""
        params = {
            'api_key': self.config.api_key,
            'out': 'json',
        }

        if start_date:
            params['start'] = start_date.strftime('%Y%m%d')

        if end_date:
            params['end'] = end_date.strftime('%Y%m%d')

        return params

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        series: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch ethanol data from EIA API.

        Args:
            start_date: Start date for data range
            end_date: End date (default: today)
            series: List of series to fetch

        Returns:
            CollectorResult with ethanol data
        """
        if not self.config.api_key:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No API key configured. Set EIA_API_KEY environment variable."
            )

        series = series or self.config.series
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=365))

        all_records = []
        warnings = []

        for series_name in series:
            if series_name not in EIA_ETHANOL_SERIES:
                warnings.append(f"Unknown series: {series_name}")
                continue

            series_info = EIA_ETHANOL_SERIES[series_name]

            url = self._build_api_url(series_info['series_id'])
            params = self._build_params(start_date, end_date)

            response, error = self._make_request(url, params=params)

            if error:
                warnings.append(f"{series_name}: {error}")
                continue

            if response.status_code == 401:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="Invalid API key"
                )

            if response.status_code != 200:
                warnings.append(f"{series_name}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()

                # Parse EIA response format
                records = self._parse_eia_response(
                    data, series_name, series_info
                )
                all_records.extend(records)

            except Exception as e:
                warnings.append(f"{series_name}: Parse error - {e}")

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved",
                warnings=warnings
            )

        # Convert to DataFrame and pivot
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            df['date'] = pd.to_datetime(df['date'])

            # Pivot to have series as columns
            df_pivot = df.pivot_table(
                index='date',
                columns='series',
                values='value',
                aggfunc='first'
            ).reset_index()

            # Add calculated fields
            if 'production' in df_pivot.columns:
                df_pivot['implied_corn_grind'] = self._calculate_corn_grind(
                    df_pivot['production']
                )

            df = df_pivot
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

    def _parse_eia_response(
        self,
        data: Dict,
        series_name: str,
        series_info: Dict
    ) -> List[Dict]:
        """Parse EIA API response"""
        records = []

        try:
            # EIA API v2 response format
            if 'response' in data and 'data' in data['response']:
                series_data = data['response']['data']
            elif 'series' in data and data['series']:
                series_data = data['series'][0].get('data', [])
            else:
                self.logger.warning(f"Unexpected response format for {series_name}")
                return records

            for item in series_data:
                if isinstance(item, list) and len(item) >= 2:
                    # Format: [date, value]
                    date_str, value = item[0], item[1]
                elif isinstance(item, dict):
                    date_str = item.get('period') or item.get('date')
                    value = item.get('value')
                else:
                    continue

                if value is not None:
                    records.append({
                        'date': date_str,
                        'series': series_name,
                        'value': float(value),
                        'unit': series_info['unit'],
                        'description': series_info['description'],
                        'source': 'EIA',
                    })

        except Exception as e:
            self.logger.warning(f"Error parsing {series_name}: {e}")

        return records

    def _calculate_corn_grind(self, production_mbpd: Any) -> Any:
        """
        Calculate implied corn grind from ethanol production.

        Formula: corn_grind (million bushels) =
            production (mbpd) * 1000 * gallons_per_barrel * days_per_week /
            gallons_ethanol_per_bushel / 1_000_000

        Args:
            production_mbpd: Ethanol production in thousand barrels per day

        Returns:
            Implied corn grind in million bushels per week
        """
        if production_mbpd is None:
            return None

        if PANDAS_AVAILABLE and hasattr(production_mbpd, 'apply'):
            return production_mbpd.apply(
                lambda x: (
                    x * 1000 * self.GALLONS_PER_BARREL * self.DAYS_PER_WEEK /
                    self.GALLONS_ETHANOL_PER_BUSHEL_CORN / 1_000_000
                ) if pd.notna(x) else None
            )

        if isinstance(production_mbpd, (int, float)):
            return (
                production_mbpd * 1000 * self.GALLONS_PER_BARREL * self.DAYS_PER_WEEK /
                self.GALLONS_ETHANOL_PER_BUSHEL_CORN / 1_000_000
            )

        return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse API response"""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_weekly_ethanol_data(
        self,
        weeks: int = 12
    ) -> Optional[Any]:
        """
        Get recent weekly ethanol data.

        Args:
            weeks: Number of weeks to fetch

        Returns:
            DataFrame with weekly ethanol metrics
        """
        end_date = date.today()
        start_date = end_date - timedelta(weeks=weeks)

        result = self.collect(
            start_date=start_date,
            end_date=end_date
        )

        return result.data if result.success else None

    def get_current_production(self) -> Optional[Dict]:
        """
        Get most recent ethanol production figures.

        Returns:
            Dict with production, stocks, and implied corn grind
        """
        result = self.collect(
            start_date=date.today() - timedelta(days=14),
            end_date=date.today(),
            series=['production', 'stocks_weekly']
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'iloc'):
            latest = result.data.iloc[-1]

            production = latest.get('production')
            stocks = latest.get('stocks_weekly')

            corn_grind = None
            if production:
                corn_grind = self._calculate_corn_grind(production)

            return {
                'date': str(latest.get('date')),
                'production_mbpd': production,
                'stocks_thousand_barrels': stocks,
                'implied_corn_grind_mbu': corn_grind,
            }

        return None

    def get_production_trend(self, weeks: int = 52) -> Optional[Dict]:
        """
        Get production trend statistics.

        Args:
            weeks: Number of weeks for trend analysis

        Returns:
            Dict with trend statistics
        """
        data = self.get_weekly_ethanol_data(weeks=weeks)

        if data is None or not PANDAS_AVAILABLE:
            return None

        if 'production' not in data.columns:
            return None

        production = data['production'].dropna()

        if len(production) < 2:
            return None

        return {
            'latest': production.iloc[-1],
            'previous': production.iloc[-2],
            'week_over_week_change': production.iloc[-1] - production.iloc[-2],
            'year_to_date_avg': production.mean(),
            'year_to_date_max': production.max(),
            'year_to_date_min': production.min(),
            '4_week_avg': production.tail(4).mean(),
        }


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for EIA Ethanol collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='EIA Ethanol Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'current', 'trend', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--api-key',
        help='EIA API key (or set EIA_API_KEY env var)'
    )

    parser.add_argument(
        '--weeks',
        type=int,
        default=12,
        help='Number of weeks to fetch'
    )

    parser.add_argument(
        '--series',
        nargs='+',
        default=['production', 'stocks_weekly', 'imports', 'blending'],
        help='Series to fetch'
    )

    parser.add_argument(
        '--output',
        '-o',
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    # Set API key if provided
    if args.api_key:
        os.environ['EIA_API_KEY'] = args.api_key

    # Create collector
    config = EIAEthanolConfig(series=args.series)
    collector = EIAEthanolCollector(config)

    if args.command == 'test':
        if not collector.config.api_key:
            print("No API key configured. Set EIA_API_KEY or use --api-key")
            return

        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'current':
        current = collector.get_current_production()
        if current:
            print(json.dumps(current, indent=2, default=str))
        else:
            print("Failed to get current production data")
        return

    if args.command == 'trend':
        trend = collector.get_production_trend(weeks=args.weeks)
        if trend:
            print(json.dumps(trend, indent=2, default=str))
        else:
            print("Failed to get production trend")
        return

    if args.command == 'fetch':
        end_date = date.today()
        start_date = end_date - timedelta(weeks=args.weeks)

        result = collector.collect(
            start_date=start_date,
            end_date=end_date,
            series=args.series
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
