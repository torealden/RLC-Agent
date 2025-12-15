"""
IBGE SIDRA Collector

Collects Brazilian agricultural statistics from IBGE SIDRA API:
- PAM (Municipal Agricultural Production) - Annual
- LSPA (Systematic Survey of Agricultural Production) - Monthly
- Geographic data

Data source:
- https://sidra.ibge.gov.br/
- API: https://servicodados.ibge.gov.br/api/docs/agregados

No API key required - public data access.
"""

import logging
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

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# SIDRA table codes for agricultural data
SIDRA_TABLES = {
    # PAM - Produção Agrícola Municipal (Annual)
    'pam_temp_crops': {
        'table': 5457,
        'description': 'Temporary crops - area, production, yield',
        'frequency': 'annual',
        'variables': {
            '109': 'planted_area_ha',
            '216': 'harvested_area_ha',
            '214': 'production_tonnes',
            '112': 'yield_kg_ha',
            '215': 'value_1000_brl',
        }
    },
    'pam_perm_crops': {
        'table': 1613,
        'description': 'Permanent crops - area, production, yield',
        'frequency': 'annual',
    },
    # LSPA - Levantamento Sistemático (Monthly estimates)
    'lspa_area': {
        'table': 6588,
        'description': 'Monthly crop area estimates',
        'frequency': 'monthly',
    },
    'lspa_production': {
        'table': 6588,
        'description': 'Monthly production estimates',
        'frequency': 'monthly',
    },
}

# IBGE product codes for commodities
IBGE_PRODUCT_CODES = {
    'soybeans': '39',
    'corn': '633',  # Milho (em grão)
    'wheat': '695',
    'rice': '117',
    'cotton': '52',  # Algodão herbáceo (em caroço)
    'sorghum': '40',
    'barley': '59',
    'beans': '83',  # Feijão (em grão)
    'sugarcane': '151',
    'coffee': '124',
}

# Brazilian state codes
BR_STATE_CODES = {
    'AC': 12, 'AL': 27, 'AP': 16, 'AM': 13, 'BA': 29, 'CE': 23,
    'DF': 53, 'ES': 32, 'GO': 52, 'MA': 21, 'MT': 51, 'MS': 50,
    'MG': 31, 'PA': 15, 'PB': 25, 'PR': 41, 'PE': 26, 'PI': 22,
    'RJ': 33, 'RN': 24, 'RS': 43, 'RO': 11, 'RR': 14, 'SC': 42,
    'SP': 35, 'SE': 28, 'TO': 17
}


@dataclass
class IBGESIDRAConfig(CollectorConfig):
    """IBGE SIDRA specific configuration"""
    source_name: str = "IBGE SIDRA"
    source_url: str = "https://sidra.ibge.gov.br"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.ANNUAL

    # API endpoint
    api_base: str = "https://apisidra.ibge.gov.br/values"

    # Default commodities
    commodities: List[str] = field(default_factory=lambda: [
        'soybeans', 'corn', 'wheat', 'rice', 'cotton', 'sorghum'
    ])

    # Default states (major producers)
    states: List[str] = field(default_factory=lambda: [
        'MT', 'PR', 'RS', 'GO', 'MS', 'BA', 'SP', 'MG', 'SC', 'TO', 'PI', 'MA'
    ])

    # Rate limiting
    rate_limit_per_minute: int = 30
    timeout: int = 60


class IBGESIDRACollector(BaseCollector):
    """
    Collector for IBGE SIDRA agricultural statistics.

    IBGE (Instituto Brasileiro de Geografia e Estatística) provides
    comprehensive agricultural data through the SIDRA system:
    - PAM: Annual municipal production data (1974-present)
    - LSPA: Monthly production estimates

    No API key required - fully public API.
    """

    def __init__(self, config: IBGESIDRAConfig = None):
        config = config or IBGESIDRAConfig()
        super().__init__(config)
        self.config: IBGESIDRAConfig = config

    def get_table_name(self) -> str:
        return "ibge_sidra"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        data_type: str = "production",
        commodities: List[str] = None,
        states: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from IBGE SIDRA.

        Args:
            start_date: Start year
            end_date: End year (default: latest)
            data_type: 'production', 'area', 'yield', or 'value'
            commodities: List of commodities
            states: List of state codes (e.g., ['MT', 'PR'])

        Returns:
            CollectorResult with fetched data
        """
        commodities = commodities or self.config.commodities
        states = states or self.config.states

        # Convert dates to years
        start_year = start_date.year if start_date else 2020
        end_year = end_date.year if end_date else datetime.now().year - 1

        if data_type == "production":
            return self._fetch_pam_data(commodities, states, start_year, end_year, 'production')
        elif data_type == "area":
            return self._fetch_pam_data(commodities, states, start_year, end_year, 'area')
        elif data_type == "yield":
            return self._fetch_pam_data(commodities, states, start_year, end_year, 'yield')
        elif data_type == "value":
            return self._fetch_pam_data(commodities, states, start_year, end_year, 'value')
        elif data_type == "monthly":
            return self._fetch_lspa_data(commodities, states)
        else:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Unknown data_type: {data_type}"
            )

    def _fetch_pam_data(
        self,
        commodities: List[str],
        states: List[str],
        start_year: int,
        end_year: int,
        variable: str = 'production'
    ) -> CollectorResult:
        """
        Fetch PAM (Municipal Agricultural Production) data.

        API format: /t/{table}/n{geo_level}/{geo_codes}/v/{variables}/p/{periods}/c{class}/{items}

        Example:
        /t/5457/n3/all/v/214/p/2020,2021,2022/c782/39
        """
        all_records = []
        warnings = []

        # Table 5457 - Temporary crops
        table = 5457

        # Variable codes
        var_codes = {
            'production': '214',  # Quantidade produzida (Toneladas)
            'area': '216',        # Área colhida (Hectares)
            'yield': '112',       # Rendimento médio (kg/ha)
            'value': '215',       # Valor da produção (Mil Reais)
        }

        var_code = var_codes.get(variable, '214')

        # Build geographic codes
        geo_codes = []
        for state in states:
            if state in BR_STATE_CODES:
                geo_codes.append(str(BR_STATE_CODES[state]))

        # Build product codes
        product_codes = []
        for commodity in commodities:
            if commodity in IBGE_PRODUCT_CODES:
                product_codes.append(IBGE_PRODUCT_CODES[commodity])
            else:
                warnings.append(f"Unknown commodity: {commodity}")

        if not product_codes:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No valid commodity codes",
                warnings=warnings
            )

        # Build years list
        years = ','.join(str(y) for y in range(start_year, end_year + 1))

        # Build API URL
        # /t/{table}/n{level}/{codes}/v/{var}/p/{periods}/c{class}/{items}
        # n3 = state level, c782 = products
        url = (
            f"{self.config.api_base}"
            f"/t/{table}"
            f"/n3/{','.join(geo_codes)}"
            f"/v/{var_code}"
            f"/p/{years}"
            f"/c782/{','.join(product_codes)}"
        )

        response, error = self._make_request(url)

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"API request failed: {error}",
                warnings=warnings
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"HTTP {response.status_code}",
                warnings=warnings
            )

        try:
            data = response.json()

            # SIDRA returns array of records
            # First record is header with metadata
            if isinstance(data, list) and len(data) > 1:
                # Skip header row
                for record in data[1:]:
                    parsed = self._parse_sidra_record(record, variable)
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

    def _fetch_lspa_data(
        self,
        commodities: List[str],
        states: List[str]
    ) -> CollectorResult:
        """
        Fetch LSPA (Systematic Survey) monthly data.

        LSPA provides monthly production estimates during the growing season.
        """
        all_records = []
        warnings = []

        # Table 6588 - LSPA
        table = 6588

        # Get current year monthly data
        current_year = datetime.now().year

        # Build geographic codes
        geo_codes = [str(BR_STATE_CODES[s]) for s in states if s in BR_STATE_CODES]

        # Build product codes
        product_codes = [IBGE_PRODUCT_CODES[c] for c in commodities if c in IBGE_PRODUCT_CODES]

        if not product_codes:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No valid commodity codes"
            )

        # LSPA uses different period format
        # Try current year months
        periods = ','.join(f"{current_year}{m:02d}" for m in range(1, 13))

        url = (
            f"{self.config.api_base}"
            f"/t/{table}"
            f"/n3/{','.join(geo_codes)}"
            f"/v/all"
            f"/p/{periods}"
            f"/c782/{','.join(product_codes)}"
        )

        response, error = self._make_request(url)

        if error or response.status_code != 200:
            warnings.append(f"LSPA fetch failed: {error or response.status_code}")
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="LSPA data not available",
                warnings=warnings
            )

        try:
            data = response.json()

            if isinstance(data, list) and len(data) > 1:
                for record in data[1:]:
                    parsed = self._parse_sidra_record(record, 'monthly')
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

    def _parse_sidra_record(self, record: Dict, variable: str) -> Optional[Dict]:
        """Parse SIDRA API response record"""
        try:
            # SIDRA returns dict with various fields
            # Common fields: NC (level), NN (name), D1C/D1N (dimension 1), V (value)

            # Extract state
            state_code = record.get('D1C', '')
            state_name = record.get('D1N', '')

            # Extract product
            product_code = record.get('D3C', record.get('D2C', ''))
            product_name = record.get('D3N', record.get('D2N', ''))

            # Extract year/period
            period = record.get('D2C', record.get('D4C', ''))
            period_name = record.get('D2N', record.get('D4N', ''))

            # Extract value
            value = record.get('V', '')

            # Reverse lookup commodity name
            commodity = None
            for name, code in IBGE_PRODUCT_CODES.items():
                if code == product_code:
                    commodity = name
                    break

            # Reverse lookup state code
            state = None
            for abbr, code in BR_STATE_CODES.items():
                if str(code) == state_code:
                    state = abbr
                    break

            return {
                'commodity': commodity or product_name,
                'product_code': product_code,
                'product_name': product_name,
                'state': state or state_code,
                'state_name': state_name,
                'year': period[:4] if len(period) >= 4 else period,
                'period': period,
                'period_name': period_name,
                'variable': variable,
                'value': self._safe_float(value),
                'source': 'IBGE_SIDRA',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            logger.warning(f"Error parsing SIDRA record: {e}")
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float"""
        if value is None or value == '' or value == '-' or value == '...':
            return None
        try:
            return float(str(value).replace(',', '.'))
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse API response"""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_state_production(
        self,
        commodity: str,
        state: str,
        years: int = 5
    ) -> Optional[Any]:
        """
        Get production history for a commodity in a specific state.

        Args:
            commodity: Commodity name
            state: State abbreviation (e.g., 'MT')
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
            states=[state]
        )

        return result.data if result.success else None

    def get_brazil_production_by_state(
        self,
        commodity: str,
        year: int = None
    ) -> Optional[Dict[str, float]]:
        """
        Get production breakdown by state for a commodity.

        Args:
            commodity: Commodity name
            year: Year (default: latest available)

        Returns:
            Dict of state -> production (tonnes)
        """
        year = year or (datetime.now().year - 1)

        result = self.collect(
            start_date=date(year, 1, 1),
            end_date=date(year, 12, 31),
            data_type="production",
            commodities=[commodity],
            states=self.config.states
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            df = result.data
            by_state = df.groupby('state')['value'].sum()
            return by_state.to_dict()

        return None

    def get_production_trend(
        self,
        commodity: str,
        state: str = None,
        years: int = 10
    ) -> Optional[Any]:
        """
        Get production trend over time.

        Args:
            commodity: Commodity name
            state: Optional state (default: all states)
            years: Number of years

        Returns:
            DataFrame with yearly production
        """
        end_year = datetime.now().year - 1
        start_year = end_year - years + 1

        states = [state] if state else self.config.states

        result = self.collect(
            start_date=date(start_year, 1, 1),
            end_date=date(end_year, 12, 31),
            data_type="production",
            commodities=[commodity],
            states=states
        )

        if not result.success or result.data is None:
            return None

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            df = result.data
            trend = df.groupby('year')['value'].sum().reset_index()
            trend.columns = ['year', 'production_tonnes']
            return trend

        return result.data


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for IBGE SIDRA collector"""
    import argparse
    import json

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='IBGE SIDRA Data Collector')

    parser.add_argument(
        'command',
        choices=['production', 'area', 'yield', 'value', 'monthly', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--commodities',
        nargs='+',
        default=['soybeans', 'corn', 'wheat'],
        help='Commodities to fetch'
    )

    parser.add_argument(
        '--states',
        nargs='+',
        default=['MT', 'PR', 'RS', 'GO'],
        help='States to fetch'
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

    config = IBGESIDRAConfig(
        commodities=args.commodities,
        states=args.states
    )
    collector = IBGESIDRACollector(config)

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
        states=args.states
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
    elif result.data is not None:
        print("\nData sample:")
        if PANDAS_AVAILABLE and hasattr(result.data, 'head'):
            print(result.data.head(10))
        else:
            print(json.dumps(result.data[:5] if isinstance(result.data, list) else result.data, indent=2, default=str))


if __name__ == '__main__':
    main()
