"""
EIA Petroleum Data Collector

Comprehensive collector for EIA petroleum and energy data:
- Crude oil (WTI, Brent)
- Refined products (gasoline, diesel, jet fuel)
- Natural gas
- Refinery operations
- Imports/exports

Requires free API key from: https://www.eia.gov/opendata/register.php
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


# EIA Series IDs for petroleum products
EIA_PETROLEUM_SERIES = {
    # Crude Oil Prices
    'wti_spot': {
        'series_id': 'PET.RWTC.W',
        'description': 'Cushing, OK WTI Spot Price FOB',
        'unit': '$/barrel',
        'frequency': 'weekly',
    },
    'brent_spot': {
        'series_id': 'PET.RBRTE.W',
        'description': 'Europe Brent Spot Price FOB',
        'unit': '$/barrel',
        'frequency': 'weekly',
    },

    # Gasoline
    'rbob_spot': {
        'series_id': 'PET.EER_EPMRR_PF4_RGC_DPG.W',
        'description': 'RBOB Regular Gasoline Spot Price',
        'unit': '$/gallon',
        'frequency': 'weekly',
    },
    'gasoline_stocks': {
        'series_id': 'PET.WGTSTUS1.W',
        'description': 'U.S. Total Gasoline Stocks',
        'unit': 'thousand barrels',
        'frequency': 'weekly',
    },
    'gasoline_production': {
        'series_id': 'PET.WGFRPUS2.W',
        'description': 'U.S. Gasoline Production',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },

    # Diesel/Distillate
    'ulsd_spot': {
        'series_id': 'PET.EER_EPD2DXL0_PF4_RGC_DPG.W',
        'description': 'NY Harbor ULSD Spot Price',
        'unit': '$/gallon',
        'frequency': 'weekly',
    },
    'distillate_stocks': {
        'series_id': 'PET.WDISTUS1.W',
        'description': 'U.S. Distillate Fuel Oil Stocks',
        'unit': 'thousand barrels',
        'frequency': 'weekly',
    },
    'distillate_production': {
        'series_id': 'PET.WDIRUPUS2.W',
        'description': 'U.S. Distillate Fuel Oil Production',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },

    # Jet Fuel
    'jet_spot': {
        'series_id': 'PET.EER_EPJK_PF4_RGC_DPG.W',
        'description': 'U.S. Gulf Coast Kerosene-Type Jet Fuel Spot Price',
        'unit': '$/gallon',
        'frequency': 'weekly',
    },
    'jet_stocks': {
        'series_id': 'PET.WKJSTUS1.W',
        'description': 'U.S. Kerosene-Type Jet Fuel Stocks',
        'unit': 'thousand barrels',
        'frequency': 'weekly',
    },

    # Crude Oil
    'crude_stocks': {
        'series_id': 'PET.WCESTUS1.W',
        'description': 'U.S. Crude Oil Stocks (Excl. SPR)',
        'unit': 'thousand barrels',
        'frequency': 'weekly',
    },
    'crude_production': {
        'series_id': 'PET.WCRFPUS2.W',
        'description': 'U.S. Crude Oil Production',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },
    'crude_imports': {
        'series_id': 'PET.WCRIMUS2.W',
        'description': 'U.S. Crude Oil Imports',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },

    # Natural Gas
    'natgas_spot': {
        'series_id': 'NG.RNGWHHD.W',
        'description': 'Henry Hub Natural Gas Spot Price',
        'unit': '$/MMBtu',
        'frequency': 'weekly',
    },
    'natgas_storage': {
        'series_id': 'NG.NW2_EPG0_SWO_R48_BCF.W',
        'description': 'Lower 48 States Natural Gas Working Storage',
        'unit': 'billion cubic feet',
        'frequency': 'weekly',
    },

    # Refinery
    'refinery_utilization': {
        'series_id': 'PET.WPULEUS3.W',
        'description': 'U.S. Refinery Utilization Rate',
        'unit': 'percent',
        'frequency': 'weekly',
    },
    'refinery_inputs': {
        'series_id': 'PET.WGIRIUS2.W',
        'description': 'U.S. Refinery and Blender Net Input',
        'unit': 'thousand barrels per day',
        'frequency': 'weekly',
    },

    # Propane (important for ag - crop drying)
    'propane_stocks': {
        'series_id': 'PET.WPRSTUS1.W',
        'description': 'U.S. Propane/Propylene Stocks',
        'unit': 'thousand barrels',
        'frequency': 'weekly',
    },
    'propane_spot': {
        'series_id': 'PET.EER_EPLLPA_PF4_RGC_DPG.W',
        'description': 'Mont Belvieu Propane Spot Price',
        'unit': '$/gallon',
        'frequency': 'weekly',
    },
}


@dataclass
class EIAPetroleumConfig(CollectorConfig):
    """EIA Petroleum configuration"""
    source_name: str = "EIA Petroleum"
    source_url: str = "https://api.eia.gov/v2"
    auth_type: AuthType = AuthType.API_KEY
    frequency: DataFrequency = DataFrequency.WEEKLY

    # API key
    api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get('EIA_API_KEY')
    )

    # Default series to fetch
    series: List[str] = field(default_factory=lambda: [
        'wti_spot', 'brent_spot', 'rbob_spot', 'ulsd_spot',
        'crude_stocks', 'gasoline_stocks', 'distillate_stocks',
        'natgas_spot', 'natgas_storage'
    ])


class EIAPetroleumCollector(BaseCollector):
    """
    Collector for EIA Petroleum and Energy data.

    Provides comprehensive energy market data:
    - Crude oil prices and stocks
    - Refined product prices and stocks
    - Natural gas prices and storage
    - Refinery operations

    Release Schedule:
    - Weekly Petroleum Status Report: Wednesday 10:30 AM ET
    - Natural Gas Storage: Thursday 10:30 AM ET
    """

    def __init__(self, config: EIAPetroleumConfig = None):
        config = config or EIAPetroleumConfig()
        super().__init__(config)
        self.config: EIAPetroleumConfig = config

        if not self.config.api_key:
            self.logger.warning(
                "No EIA API key. Register at: https://www.eia.gov/opendata/register.php"
            )

    def get_table_name(self) -> str:
        return "eia_petroleum"

    def _build_api_url(self, series_id: str) -> str:
        """Build EIA API v2 URL"""
        return f"{self.config.source_url}/seriesid/{series_id}"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        series: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch petroleum data from EIA API.

        Args:
            start_date: Start date for data range
            end_date: End date
            series: List of series names to fetch
        """
        if not self.config.api_key:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No API key. Set EIA_API_KEY environment variable."
            )

        series = series or self.config.series
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=365))

        all_records = []
        warnings = []

        for series_name in series:
            if series_name not in EIA_PETROLEUM_SERIES:
                warnings.append(f"Unknown series: {series_name}")
                continue

            series_info = EIA_PETROLEUM_SERIES[series_name]
            url = self._build_api_url(series_info['series_id'])

            params = {
                'api_key': self.config.api_key,
                'out': 'json',
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d'),
            }

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
                records = self._parse_eia_response(data, series_name, series_info)
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
            df = df.sort_values(['series', 'date'])
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
            # Handle different API response formats
            if 'response' in data and 'data' in data['response']:
                series_data = data['response']['data']
            elif 'series' in data and data['series']:
                series_data = data['series'][0].get('data', [])
            else:
                return records

            for item in series_data:
                if isinstance(item, list) and len(item) >= 2:
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
                        'series_id': series_info['series_id'],
                        'value': float(value) if value else None,
                        'unit': series_info['unit'],
                        'description': series_info['description'],
                        'source': 'EIA',
                    })

        except Exception as e:
            self.logger.warning(f"Error parsing {series_name}: {e}")

        return records

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_weekly_prices(self) -> Optional[Any]:
        """Get latest weekly energy prices"""
        price_series = [
            'wti_spot', 'brent_spot', 'rbob_spot', 'ulsd_spot',
            'jet_spot', 'natgas_spot', 'propane_spot'
        ]

        result = self.collect(
            start_date=date.today() - timedelta(days=14),
            series=price_series
        )

        return result.data if result.success else None

    def get_weekly_stocks(self) -> Optional[Any]:
        """Get latest weekly inventory levels"""
        stock_series = [
            'crude_stocks', 'gasoline_stocks', 'distillate_stocks',
            'jet_stocks', 'propane_stocks', 'natgas_storage'
        ]

        result = self.collect(
            start_date=date.today() - timedelta(days=14),
            series=stock_series
        )

        return result.data if result.success else None

    def get_crack_spread_components(self) -> Optional[Dict]:
        """
        Get components needed to calculate crack spreads.

        3:2:1 Crack Spread = (2 * RBOB + 1 * ULSD) / 3 - WTI
        """
        result = self.collect(
            start_date=date.today() - timedelta(days=14),
            series=['wti_spot', 'rbob_spot', 'ulsd_spot']
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'pivot'):
            df = result.data

            # Get latest values
            latest = df.groupby('series').last().reset_index()

            return {
                'date': str(latest['date'].iloc[0]),
                'wti': latest[latest['series'] == 'wti_spot']['value'].iloc[0]
                    if 'wti_spot' in latest['series'].values else None,
                'rbob': latest[latest['series'] == 'rbob_spot']['value'].iloc[0]
                    if 'rbob_spot' in latest['series'].values else None,
                'ulsd': latest[latest['series'] == 'ulsd_spot']['value'].iloc[0]
                    if 'ulsd_spot' in latest['series'].values else None,
            }

        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for EIA Petroleum collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='EIA Petroleum Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'prices', 'stocks', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--series',
        nargs='+',
        help='Specific series to fetch'
    )

    parser.add_argument(
        '--weeks',
        type=int,
        default=52,
        help='Number of weeks of history'
    )

    parser.add_argument(
        '--api-key',
        help='EIA API key'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file'
    )

    args = parser.parse_args()

    if args.api_key:
        os.environ['EIA_API_KEY'] = args.api_key

    collector = EIAPetroleumCollector()

    if args.command == 'test':
        if not collector.config.api_key:
            print("No API key configured")
            return
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'prices':
        data = collector.get_weekly_prices()
        if data is not None:
            print(data.to_string() if PANDAS_AVAILABLE else data)
        return

    if args.command == 'stocks':
        data = collector.get_weekly_stocks()
        if data is not None:
            print(data.to_string() if PANDAS_AVAILABLE else data)
        return

    if args.command == 'fetch':
        result = collector.collect(
            start_date=date.today() - timedelta(weeks=args.weeks),
            series=args.series
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if result.warnings:
            print(f"Warnings: {result.warnings}")

        if args.output and result.data is not None:
            if args.output.endswith('.csv') and PANDAS_AVAILABLE:
                result.data.to_csv(args.output, index=False)
            print(f"Saved to: {args.output}")


if __name__ == '__main__':
    main()
