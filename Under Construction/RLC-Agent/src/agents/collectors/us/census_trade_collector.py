"""
US Census Bureau International Trade API Collector

Collects trade data from the US Census Bureau:
- Monthly imports/exports by HS code
- Trade by country/partner
- Agricultural trade via USDA codes

Requires free API key from: https://api.census.gov/data/key_signup.html
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


# Common agricultural HS codes
AG_HS_CODES = {
    # Chapter 10 - Cereals
    'wheat': '1001',
    'corn': '1005',
    'barley': '1003',
    'oats': '1004',
    'sorghum': '1007',

    # Chapter 12 - Oilseeds
    'soybeans': '1201',
    'canola': '1205',
    'sunflower': '1206',

    # Chapter 15 - Fats and Oils
    'soybean_oil': '1507',
    'palm_oil': '1511',
    'sunflower_oil': '1512',
    'canola_oil': '1514',
    'coconut_oil': '1513',

    # Chapter 23 - Residues
    'soybean_meal': '2304',
    'canola_meal': '2306',

    # Chapter 38 - Miscellaneous (Biodiesel)
    'biodiesel': '382600',
}


@dataclass
class CensusTradeConfig(CollectorConfig):
    """Census Trade API configuration"""
    source_name: str = "US Census Trade"
    source_url: str = "https://api.census.gov/data/timeseries/intltrade"
    auth_type: AuthType = AuthType.API_KEY
    frequency: DataFrequency = DataFrequency.MONTHLY

    # API key
    api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get('CENSUS_API_KEY')
    )

    # Default HS codes to fetch
    hs_codes: List[str] = field(default_factory=lambda: [
        '1001', '1005', '1201', '1507', '2304'  # wheat, corn, soybeans, soy oil, soy meal
    ])


class CensusTradeCollector(BaseCollector):
    """
    Collector for US Census Bureau International Trade data.

    Provides:
    - Monthly import/export values by HS code
    - Trade by partner country
    - Port-level data

    Data is released monthly with ~6 week lag.
    """

    def __init__(self, config: CensusTradeConfig = None):
        config = config or CensusTradeConfig()
        super().__init__(config)
        self.config: CensusTradeConfig = config

        if not self.config.api_key:
            self.logger.warning(
                "No Census API key. Register at: https://api.census.gov/data/key_signup.html"
                "\n(API will work without key but limited to 500 calls/day)"
            )

    def get_table_name(self) -> str:
        return "census_trade"

    def _build_api_url(self, flow: str) -> str:
        """Build API URL for imports or exports"""
        return f"{self.config.source_url}/{flow}/hs"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        flow: str = "both",
        hs_codes: List[str] = None,
        partner_country: str = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch trade data from Census API.

        Args:
            flow: 'imports', 'exports', or 'both'
            hs_codes: List of HS codes (2-10 digits)
            partner_country: ISO country code filter

        Note: Monthly data available from 2013-present.
              For earlier data, use annual endpoint (2005-present).
        """
        hs_codes = hs_codes or self.config.hs_codes
        end_date = end_date or date.today()
        # Monthly API data starts from 2013
        min_date = date(2013, 1, 1)
        start_date = start_date or date(end_date.year - 1, 1, 1)
        if start_date < min_date:
            self.logger.warning(f"Monthly data only available from 2013. Adjusting start date.")
            start_date = min_date

        all_records = []
        warnings = []

        flows = ['imports', 'exports'] if flow == 'both' else [flow]

        for trade_flow in flows:
            for hs_code in hs_codes:
                result = self._fetch_hs_data(
                    trade_flow, hs_code, start_date, end_date, partner_country
                )

                if result['success']:
                    all_records.extend(result['records'])
                else:
                    warnings.append(f"{trade_flow}/{hs_code}: {result.get('error')}")

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No data retrieved",
                warnings=warnings
            )

        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
            df = df.sort_values(['hs_code', 'flow', 'year', 'month'])
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

    def _fetch_hs_data(
        self,
        flow: str,
        hs_code: str,
        start_date: date,
        end_date: date,
        partner_country: str = None
    ) -> Dict:
        """Fetch data for a specific HS code and flow"""
        url = self._build_api_url(flow)

        # Census API uses different field prefixes for imports vs exports
        # Imports: I_COMMODITY, GEN_VAL_MO (general value)
        # Exports: E_COMMODITY, ALL_VAL_MO (all value)
        if flow == 'imports':
            commodity_field = 'I_COMMODITY'
            value_field = 'GEN_VAL_MO'
            qty_field = 'GEN_QY1_MO'
        else:
            commodity_field = 'E_COMMODITY'
            value_field = 'ALL_VAL_MO'
            qty_field = 'QY1_MO'

        all_records = []

        # Fetch month by month for more reliable results
        current = date(start_date.year, start_date.month, 1)
        while current <= end_date:
            time_str = f"{current.year}-{current.month:02d}"

            params = {
                'get': f'{value_field},{qty_field},CTY_CODE,CTY_NAME',
                commodity_field: hs_code,
                'time': time_str,
            }

            if self.config.api_key:
                params['key'] = self.config.api_key

            if partner_country:
                params['CTY_CODE'] = partner_country

            response, error = self._make_request(url, params=params)

            if error:
                # Move to next month
                if current.month == 12:
                    current = date(current.year + 1, 1, 1)
                else:
                    current = date(current.year, current.month + 1, 1)
                continue

            if response.status_code == 200:
                try:
                    data = response.json()
                    if data and len(data) > 1:
                        headers = data[0]
                        for row in data[1:]:
                            record = dict(zip(headers, row))
                            all_records.append({
                                'year': current.year,
                                'month': current.month,
                                'flow': flow,
                                'hs_code': hs_code,
                                'country_code': record.get('CTY_CODE'),
                                'country_name': record.get('CTY_NAME'),
                                'value_usd': self._safe_float(record.get(value_field)),
                                'quantity': self._safe_float(record.get(qty_field)),
                                'source': 'CENSUS_TRADE',
                            })
                except Exception:
                    pass

            # Move to next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        if all_records:
            return {'success': True, 'records': all_records}
        return {'success': False, 'error': 'No data retrieved', 'records': []}

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert to float"""
        if value is None or value == '':
            return None
        try:
            return float(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_commodity_trade(
        self,
        commodity: str,
        flow: str = 'both',
        year: int = None
    ) -> Optional[Any]:
        """
        Get trade data for a named commodity.

        Args:
            commodity: Commodity name (e.g., 'corn', 'soybeans')
            flow: 'imports', 'exports', or 'both'
            year: Year to fetch (default: current)
        """
        if commodity not in AG_HS_CODES:
            self.logger.warning(f"Unknown commodity: {commodity}")
            return None

        hs_code = AG_HS_CODES[commodity]
        year = year or datetime.now().year

        result = self.collect(
            start_date=date(year, 1, 1),
            end_date=date(year, 12, 31),
            flow=flow,
            hs_codes=[hs_code]
        )

        return result.data if result.success else None

    def get_top_trade_partners(
        self,
        commodity: str,
        flow: str = 'exports',
        top_n: int = 10,
        year: int = None
    ) -> Optional[Dict]:
        """Get top trade partners for a commodity"""
        data = self.get_commodity_trade(commodity, flow, year)

        if data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(data, 'groupby'):
            top = (data
                   .groupby('country_name')['value_usd']
                   .sum()
                   .sort_values(ascending=False)
                   .head(top_n))
            return top.to_dict()

        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for Census Trade collector"""
    import argparse
    import json

    parser = argparse.ArgumentParser(description='Census Trade Data Collector')

    parser.add_argument(
        'command',
        choices=['fetch', 'commodity', 'partners'],
        help='Command to execute'
    )

    parser.add_argument(
        '--commodity',
        default='soybeans',
        help='Commodity name'
    )

    parser.add_argument(
        '--flow',
        choices=['imports', 'exports', 'both'],
        default='both',
        help='Trade flow'
    )

    parser.add_argument(
        '--year',
        type=int,
        help='Year to fetch'
    )

    parser.add_argument(
        '--hs-codes',
        nargs='+',
        help='HS codes to fetch'
    )

    parser.add_argument(
        '--api-key',
        help='Census API key'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file'
    )

    args = parser.parse_args()

    if args.api_key:
        os.environ['CENSUS_API_KEY'] = args.api_key

    collector = CensusTradeCollector()

    if args.command == 'commodity':
        data = collector.get_commodity_trade(
            args.commodity, args.flow, args.year
        )
        if data is not None and PANDAS_AVAILABLE:
            print(data.to_string())
        return

    if args.command == 'partners':
        partners = collector.get_top_trade_partners(
            args.commodity, args.flow, year=args.year
        )
        if partners:
            print(json.dumps(partners, indent=2))
        return

    if args.command == 'fetch':
        result = collector.collect(
            flow=args.flow,
            hs_codes=args.hs_codes
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
