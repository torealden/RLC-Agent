"""
USDA FAS OpenDataWeb Collector

Collects data from USDA Foreign Agricultural Service:
- Export Sales Report (ESR) - Weekly export sales by country
- Production, Supply, Distribution (PSD) - Supply/demand balances

No API key required for basic access.

Data sources:
- https://apps.fas.usda.gov/OpenData/api/
"""

import logging
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


# FAS Commodity Codes
FAS_COMMODITY_CODES = {
    'corn': {
        'code': '0440000',
        'description': 'Corn',
        'unit': 'MT',
    },
    'soybeans': {
        'code': '2222000',
        'description': 'Soybeans',
        'unit': 'MT',
    },
    'wheat': {
        'code': '0410000',
        'description': 'Wheat',
        'unit': 'MT',
    },
    'soybean_meal': {
        'code': '0813100',
        'description': 'Soybean Meal',
        'unit': 'MT',
    },
    'soybean_oil': {
        'code': '4232000',
        'description': 'Soybean Oil',
        'unit': 'MT',
    },
    'sorghum': {
        'code': '0459000',
        'description': 'Sorghum',
        'unit': 'MT',
    },
    'cotton': {
        'code': '2631000',
        'description': 'Cotton',
        'unit': '480LB BALES',
    },
    'rice': {
        'code': '0422000',
        'description': 'Rice, Milled',
        'unit': 'MT',
    },
}


@dataclass
class USDATFASConfig(CollectorConfig):
    """USDA FAS specific configuration"""
    source_name: str = "USDA FAS"
    source_url: str = "https://apps.fas.usda.gov/OpenData/api"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # FAS-specific settings
    commodities: List[str] = field(default_factory=lambda: [
        'corn', 'soybeans', 'wheat', 'soybean_meal', 'soybean_oil'
    ])

    # API endpoints
    esr_endpoint: str = "/esr/exports"  # Export Sales Report
    psd_endpoint: str = "/psd"  # Production, Supply, Distribution
    gats_endpoint: str = "/gats"  # Global Agricultural Trade System


class USDATFASCollector(BaseCollector):
    """
    Collector for USDA FAS Export Sales and PSD data.

    Available endpoints:
    - ESR: Weekly export sales by commodity and country
    - PSD: Production, supply, and distribution data
    - GATS: Global trade statistics

    No API key required for basic access.
    """

    def __init__(self, config: USDATFASConfig = None):
        config = config or USDATFASConfig()
        super().__init__(config)
        self.config: USDATFASConfig = config

    def get_table_name(self) -> str:
        return "export_sales"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        data_type: str = "export_sales",
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from USDA FAS.

        Args:
            start_date: Start date for data range
            end_date: End date (default: today)
            data_type: 'export_sales', 'psd', or 'gats'
            commodities: List of commodities to fetch

        Returns:
            CollectorResult with fetched data
        """
        commodities = commodities or self.config.commodities
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=365))

        if data_type == "export_sales":
            return self._fetch_export_sales(
                start_date, end_date, commodities
            )
        elif data_type == "psd":
            return self._fetch_psd(commodities, **kwargs)
        else:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Unknown data_type: {data_type}"
            )

    def _fetch_export_sales(
        self,
        start_date: date,
        end_date: date,
        commodities: List[str]
    ) -> CollectorResult:
        """Fetch weekly export sales data"""
        all_records = []
        warnings = []

        for commodity in commodities:
            if commodity not in FAS_COMMODITY_CODES:
                warnings.append(f"Unknown commodity: {commodity}")
                continue

            commodity_info = FAS_COMMODITY_CODES[commodity]
            url = f"{self.config.source_url}{self.config.esr_endpoint}/commodityCode/{commodity_info['code']}"

            response, error = self._make_request(url)

            if error:
                warnings.append(f"{commodity}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{commodity}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()

                for record in data:
                    parsed = self._parse_export_sales_record(
                        record, commodity, commodity_info
                    )
                    if parsed:
                        # Filter by date
                        week_ending = self.parse_date(parsed.get('week_ending'))
                        if week_ending and start_date <= week_ending <= end_date:
                            all_records.append(parsed)

            except Exception as e:
                warnings.append(f"{commodity}: Parse error - {e}")

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
            df['week_ending'] = pd.to_datetime(df['week_ending'])
            df = df.sort_values(['commodity', 'week_ending'])
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

    def _parse_export_sales_record(
        self,
        record: Dict,
        commodity: str,
        commodity_info: Dict
    ) -> Optional[Dict]:
        """Parse export sales record"""
        try:
            return {
                'commodity': commodity,
                'commodity_code': commodity_info['code'],
                'week_ending': record.get('weekEndingDate'),
                'marketing_year': record.get('marketYear'),
                'country': record.get('countryDescription'),
                'country_code': record.get('countryCode'),
                'region': record.get('regionDescription'),

                # Sales data (in MT)
                'weekly_exports': self._safe_float(record.get('weeklyExports')),
                'accumulated_exports': self._safe_float(record.get('accumulatedExports')),
                'outstanding_sales': self._safe_float(record.get('outstandingSales')),
                'gross_new_sales': self._safe_float(record.get('grossNewSales')),
                'buying_new_sales': self._safe_float(record.get('buyingNewSales')),
                'net_sales': self._safe_float(record.get('netSales')),

                # Comparisons
                'previous_my_accumulated': self._safe_float(
                    record.get('prevMarketYearAccumulatedExports')
                ),

                'unit': commodity_info['unit'],
                'source': 'USDA_FAS_ESR',
            }
        except Exception as e:
            self.logger.warning(f"Error parsing ESR record: {e}")
            return None

    def _fetch_psd(
        self,
        commodities: List[str],
        countries: List[str] = None,
        years: List[int] = None
    ) -> CollectorResult:
        """Fetch PSD (Production, Supply, Distribution) data"""
        all_records = []
        warnings = []

        countries = countries or ['US']  # Default to US
        years = years or [datetime.now().year - 1, datetime.now().year]

        for commodity in commodities:
            if commodity not in FAS_COMMODITY_CODES:
                warnings.append(f"Unknown commodity: {commodity}")
                continue

            commodity_info = FAS_COMMODITY_CODES[commodity]

            # PSD API has different structure
            url = f"{self.config.source_url}{self.config.psd_endpoint}/commodity/{commodity_info['code']}"

            response, error = self._make_request(url)

            if error:
                warnings.append(f"{commodity}: {error}")
                continue

            if response.status_code != 200:
                warnings.append(f"{commodity}: HTTP {response.status_code}")
                continue

            try:
                data = response.json()

                for record in data:
                    # Filter by country and year
                    country_code = record.get('countryCode', '')
                    year = record.get('marketYear')

                    if countries and country_code not in countries:
                        continue
                    if years and year not in years:
                        continue

                    parsed = self._parse_psd_record(record, commodity, commodity_info)
                    if parsed:
                        all_records.append(parsed)

            except Exception as e:
                warnings.append(f"{commodity}: Parse error - {e}")

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No PSD data retrieved",
                warnings=warnings
            )

        # Convert to DataFrame
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
        else:
            df = all_records

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=df,
            warnings=warnings
        )

    def _parse_psd_record(
        self,
        record: Dict,
        commodity: str,
        commodity_info: Dict
    ) -> Optional[Dict]:
        """Parse PSD record into standardized format"""
        try:
            return {
                'commodity': commodity,
                'commodity_code': commodity_info['code'],
                'country': record.get('countryDescription'),
                'country_code': record.get('countryCode'),
                'marketing_year': record.get('marketYear'),
                'month': record.get('month'),

                # Supply
                'beginning_stocks': self._safe_float(record.get('beginningStocks')),
                'production': self._safe_float(record.get('production')),
                'imports': self._safe_float(record.get('imports')),
                'total_supply': self._safe_float(record.get('totalSupply')),

                # Use
                'domestic_consumption': self._safe_float(record.get('domesticConsumption')),
                'exports': self._safe_float(record.get('exports')),
                'total_use': self._safe_float(record.get('totalUse')),

                # Stocks
                'ending_stocks': self._safe_float(record.get('endingStocks')),

                # Area/Yield
                'area_harvested': self._safe_float(record.get('areaHarvested')),
                'yield_per_hectare': self._safe_float(record.get('yieldPerHectare')),

                'unit': record.get('unit', '1000 MT'),
                'source': 'USDA_FAS_PSD',
            }
        except Exception as e:
            self.logger.warning(f"Error parsing PSD record: {e}")
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == '':
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

    def get_weekly_export_sales(
        self,
        commodity: str,
        weeks: int = 4
    ) -> Optional[Any]:
        """
        Get recent weekly export sales for a commodity.

        Args:
            commodity: Commodity name
            weeks: Number of weeks to fetch

        Returns:
            DataFrame or list of records
        """
        end_date = date.today()
        start_date = end_date - timedelta(weeks=weeks)

        result = self.collect(
            start_date=start_date,
            end_date=end_date,
            data_type="export_sales",
            commodities=[commodity]
        )

        return result.data if result.success else None

    def get_top_destinations(
        self,
        commodity: str,
        marketing_year: int = None,
        top_n: int = 10
    ) -> Dict[str, float]:
        """
        Get top export destinations for a commodity.

        Args:
            commodity: Commodity name
            marketing_year: Marketing year (default: current)
            top_n: Number of top destinations

        Returns:
            Dict of country -> accumulated exports
        """
        result = self.collect(
            data_type="export_sales",
            commodities=[commodity]
        )

        if not result.success or result.data is None:
            return {}

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            df = result.data

            if marketing_year:
                df = df[df['marketing_year'] == marketing_year]

            # Get latest week's accumulated exports by country
            latest_week = df['week_ending'].max()
            latest = df[df['week_ending'] == latest_week]

            top = (latest
                   .groupby('country')['accumulated_exports']
                   .sum()
                   .sort_values(ascending=False)
                   .head(top_n))

            return top.to_dict()

        return {}

    def get_us_supply_demand(
        self,
        commodity: str,
        years: List[int] = None
    ) -> Optional[Any]:
        """
        Get US supply/demand balance for a commodity.

        Args:
            commodity: Commodity name
            years: Marketing years to fetch

        Returns:
            DataFrame or list of records
        """
        result = self.collect(
            data_type="psd",
            commodities=[commodity],
            countries=['US'],
            years=years
        )

        return result.data if result.success else None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for USDA FAS collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='USDA FAS Data Collector')

    parser.add_argument(
        'command',
        choices=['export_sales', 'psd', 'top_destinations', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--commodities',
        nargs='+',
        default=['corn', 'soybeans', 'wheat'],
        help='Commodities to fetch'
    )

    parser.add_argument(
        '--weeks',
        type=int,
        default=4,
        help='Number of weeks (for export_sales)'
    )

    parser.add_argument(
        '--output',
        '-o',
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    # Create collector
    config = USDATFASConfig(commodities=args.commodities)
    collector = USDATFASCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == 'export_sales':
        end_date = date.today()
        start_date = end_date - timedelta(weeks=args.weeks)

        result = collector.collect(
            start_date=start_date,
            end_date=end_date,
            data_type="export_sales",
            commodities=args.commodities
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if result.warnings:
            print(f"Warnings: {result.warnings}")

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

    elif args.command == 'psd':
        result = collector.collect(
            data_type="psd",
            commodities=args.commodities,
            countries=['US']
        )

        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")

        if args.output and result.data is not None:
            if PANDAS_AVAILABLE and hasattr(result.data, 'to_json'):
                result.data.to_json(args.output, orient='records')
            print(f"Saved to: {args.output}")

    elif args.command == 'top_destinations':
        for commodity in args.commodities:
            destinations = collector.get_top_destinations(commodity)
            print(f"\n{commodity.upper()} Top Destinations:")
            for country, volume in destinations.items():
                print(f"  {country}: {volume:,.0f} MT")


if __name__ == '__main__':
    main()
