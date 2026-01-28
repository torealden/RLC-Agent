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


# Agricultural HS codes - 10-digit for quantity data, 4-digit for value-only
# 10-digit codes provide both value AND quantity; 4-digit only provides value
AG_HS_CODES = {
    # ==========================================================================
    # GRAINS (Chapter 10) - 10-digit codes with quantity in metric tons (T)
    # ==========================================================================
    'corn_yellow_dent_2': '1005902030',      # Yellow Dent Corn US No. 2 (main bulk)
    'corn_yellow_dent_3': '1005902035',      # Yellow Dent Corn US No. 3
    'corn_seed': '1005100010',               # Yellow Corn Seed
    'wheat': '1001992055',                   # Wheat NESOI (main bulk wheat)
    'wheat_white': '1001992015',             # White Wheat
    'wheat_seed': '1001910000',              # Wheat Seed
    'sorghum_seed': '1007100000',            # Grain Sorghum Seed
    'barley': '1003900000',                  # Barley except seed

    # ==========================================================================
    # OILSEEDS (Chapter 12) - 10-digit codes with quantity
    # ==========================================================================
    'soybeans': '1201900095',                # Soybeans bulk (NOT seed) - qty in T
    'soybeans_oilstock': '1201900005',       # Soybeans for oil stock - qty in KG
    'soybeans_seed': '1201100000',           # Soybean seeds for sowing - qty in KG
    'canola': '1205100000',                  # Low erucic acid rapeseed/canola - qty in KG
    'sunflower_oilstock': '1206000020',      # Sunflower seeds for oil - qty in KG
    'sunflower_other': '1206000090',         # Sunflower seeds NESOI - qty in KG
    'cotton_seed': '1207290000',             # Cotton seeds except sowing - qty in KG

    # ==========================================================================
    # VEGETABLE OILS (Chapter 15) - 10-digit codes with quantity in KG
    # ==========================================================================
    'soybean_oil_refined': '1507904050',     # Soybean oil fully refined
    'soybean_oil_crude': '1507100000',       # Soybean oil crude
    'palm_oil_refined': '1511900000',        # Palm oil refined
    'palm_oil_crude': '1511100000',          # Palm oil crude
    'sunflower_oil': '1512190020',           # Sunflower oil refined
    'canola_oil': '1514190000',              # Rapeseed/canola oil NESOI
    'canola_oil_crude': '1514110000',        # Rapeseed/canola oil crude
    'corn_oil_refined': '1515290040',        # Corn oil fully refined
    'palm_kernel_oil': '1513290000',         # Palm kernel oil refined

    # ==========================================================================
    # MEALS & RESIDUES (Chapter 23) - 10-digit codes
    # ==========================================================================
    'soybean_meal': '2304000000',            # Soybean oilcake/meal - qty in KG
    'sunflower_meal': '2306300000',          # Sunflower seed meal - qty in KG
    'cotton_meal': '2306100000',             # Cotton seed meal - qty in KG
    'canola_meal': '2306490000',             # Rapeseed/canola meal - qty in KG
    'corn_gluten_feed': '2303100010',        # Corn gluten feed - qty in T
    'corn_gluten_meal': '2303100020',        # Corn gluten meal - qty in T
    'ddgs': '2303300000',                    # Distillers grains (DDGS) - qty in T

    # ==========================================================================
    # COTTON (Chapter 52) - 10-digit codes with quantity in KG
    # ==========================================================================
    'cotton_raw': '5201009000',              # Cotton not carded, staple >28.575mm
    'cotton_medium': '5201001090',           # Cotton staple 25.4-28.575mm
    'cotton_pima': '5201002030',             # American Pima cotton

    # ==========================================================================
    # LEGACY 4-DIGIT CODES (value only, no quantity)
    # Use these if you only need value data
    # ==========================================================================
    'wheat_4digit': '1001',
    'corn_4digit': '1005',
    'soybeans_4digit': '1201',
    'soybean_oil_4digit': '1507',
    'soybean_meal_4digit': '2304',
}

# Unit conversions for standardization
HS_UNITS = {
    # Grains in metric tons
    '1005902030': 'T',   # corn
    '1005902035': 'T',   # corn
    '1001992055': 'T',   # wheat
    '1001992015': 'T',   # wheat white
    '1003900000': 'T',   # barley
    # Oilseeds - soybeans bulk in T, others in KG
    '1201900095': 'T',   # soybeans bulk
    '1201900005': 'KG',
    '1205100000': 'KG',  # canola
    # Oils in KG
    '1507904050': 'KG',
    '1511900000': 'KG',
    # Meals - mixed units
    '2304000000': 'KG',  # soy meal
    '2303100010': 'T',   # corn gluten feed
    '2303100020': 'T',   # corn gluten meal
    '2303300000': 'T',   # DDGS
    # Cotton in KG
    '5201009000': 'KG',
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

    # Default HS codes to fetch (10-digit for quantity data)
    hs_codes: List[str] = field(default_factory=lambda: [
        '1005902030',  # Corn Yellow Dent #2 (bulk)
        '1001992055',  # Wheat NESOI (bulk)
        '1201900095',  # Soybeans bulk
        '1507904050',  # Soybean oil refined
        '2304000000',  # Soybean meal
        '1205100000',  # Canola/Rapeseed
        '2303300000',  # DDGS
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
        # Imports: I_COMMODITY, GEN_VAL_MO (general value), GEN_QY1_MO (quantity)
        # Exports: E_COMMODITY, ALL_VAL_MO (all value), QTY_1_MO (quantity)
        if flow == 'imports':
            commodity_field = 'I_COMMODITY'
            value_field = 'GEN_VAL_MO'
            qty_field = 'GEN_QY1_MO'
        else:
            commodity_field = 'E_COMMODITY'
            value_field = 'ALL_VAL_MO'
            qty_field = 'QTY_1_MO'

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

    # =========================================================================
    # DATABASE METHODS
    # =========================================================================

    def save_to_bronze(
        self,
        flow: str = 'both',
        hs_codes: List[str] = None,
        start_date: date = None,
        end_date: date = None,
        conn=None
    ) -> Dict[str, int]:
        """
        Collect and save Census trade data to bronze layer.

        Args:
            flow: 'imports', 'exports', or 'both'
            hs_codes: List of HS codes to fetch
            start_date: Start date for data
            end_date: End date for data
            conn: Optional database connection

        Returns:
            Dict with counts of records saved
        """
        import psycopg2
        from pathlib import Path
        from dotenv import load_dotenv

        # Load .env from project root
        project_root = Path(__file__).parent.parent.parent.parent.parent
        load_dotenv(project_root / '.env')

        close_conn = False
        if conn is None:
            password = (os.environ.get('RLC_PG_PASSWORD') or
                       os.environ.get('DATABASE_PASSWORD') or
                       os.environ.get('DB_PASSWORD'))
            conn = psycopg2.connect(
                host=os.environ.get('DATABASE_HOST', 'localhost'),
                port=os.environ.get('DATABASE_PORT', '5432'),
                database=os.environ.get('DATABASE_NAME', 'rlc_commodities'),
                user=os.environ.get('DATABASE_USER', 'postgres'),
                password=password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        # Fetch data
        result = self.collect(
            flow=flow,
            hs_codes=hs_codes,
            start_date=start_date,
            end_date=end_date
        )

        if not result.success or result.data is None:
            self.logger.error(f"Failed to fetch trade data: {result.error_message}")
            return counts

        # Convert to list of dicts if DataFrame
        if hasattr(result.data, 'to_dict'):
            records = result.data.to_dict('records')
        else:
            records = result.data

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.census_trade
                    (year, month, flow, hs_code, country_code, country_name,
                     value_usd, quantity, source, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (year, month, flow, hs_code, country_code)
                    DO UPDATE SET
                        country_name = EXCLUDED.country_name,
                        value_usd = EXCLUDED.value_usd,
                        quantity = EXCLUDED.quantity,
                        collected_at = NOW()
                """, (
                    record.get('year'),
                    record.get('month'),
                    record.get('flow'),
                    record.get('hs_code'),
                    record.get('country_code'),
                    record.get('country_name'),
                    record.get('value_usd'),
                    record.get('quantity'),
                    record.get('source', 'CENSUS_TRADE')
                ))

                if cursor.rowcount > 0:
                    counts['inserted'] += 1

            except Exception as e:
                self.logger.error(f"Error saving record: {e}")
                counts['errors'] += 1

        conn.commit()

        if close_conn:
            cursor.close()
            conn.close()

        self.logger.info(f"Saved {counts['inserted']} records to bronze.census_trade")
        return counts


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

    parser.add_argument(
        '--save-db',
        action='store_true',
        help='Save data to PostgreSQL bronze layer'
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
        if args.save_db:
            from datetime import date
            start = date(args.year, 1, 1) if args.year else None
            end = date(args.year, 12, 31) if args.year else None
            counts = collector.save_to_bronze(
                flow=args.flow,
                hs_codes=args.hs_codes,
                start_date=start,
                end_date=end
            )
            print(f"Saved to database: {counts}")
            return

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
