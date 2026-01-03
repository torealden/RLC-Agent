"""
Brazil Comex Stat API Trade Data Agent
Collects trade data from MDIC/SECEX Comex Stat API
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Generator

import pandas as pd

from .base_trade_agent import BaseTradeAgent, FetchResult, LoadResult


class BrazilComexStatAgent(BaseTradeAgent):
    """
    Agent for collecting Brazil trade data from Comex Stat API

    Data characteristics:
    - Source: Comex Stat API (MDIC/SECEX)
    - Format: JSON API responses
    - HS Code: NCM (8 digits)
    - Update frequency: Early in the following month (around 5th-10th)
    - Authentication: No auth required for public queries
    - Rate limiting: Yes, requires backoff on 429
    """

    # API endpoints
    BASE_URL = "https://api-comex.stat.gov.br"

    # Known API paths (may vary by version)
    API_PATHS = {
        'export': '/comexstat/export',
        'import': '/comexstat/import',
        'v1_export': '/api/v1/export',
        'v1_import': '/api/v1/import',
    }

    def __init__(self, config, db_session_factory=None):
        super().__init__(config, db_session_factory)
        self.logger = logging.getLogger("BrazilComexStatAgent")

        # Configure session for JSON
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        })

    def _build_api_url(self, flow: str, version: str = 'default') -> str:
        """Build API URL for the given flow type"""
        if version == 'v1':
            path = self.API_PATHS.get(f'v1_{flow}', self.API_PATHS[flow])
        else:
            path = self.API_PATHS.get(flow, f'/comexstat/{flow}')

        return f"{self.BASE_URL}{path}"

    def _build_query_params(
        self,
        year: int,
        month: int,
        flow: str,
        hs_level: int = 8,
        offset: int = 0,
        limit: int = 5000
    ) -> Dict:
        """
        Build query parameters for Comex Stat API

        Sample query:
        GET /comexstat?freq=M&type=export&year=2024&month=08&hs_level=8&partner=all&offset=0&limit=5000
        """
        params = {
            'freq': 'M',  # Monthly
            'type': flow,
            'year': year,
            'month': month,
            'hs_level': hs_level,
            'partner': 'all',
            'offset': offset,
            'limit': limit,
        }

        return params

    def fetch_data(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Fetch trade data from Comex Stat API

        Args:
            year: Year to fetch
            month: Month to fetch (1-12)
            flow: 'export' or 'import'

        Returns:
            FetchResult with fetched data
        """
        period = self.normalize_period(year, month)
        self.logger.info(f"Fetching Brazil {flow}s for {period}")

        all_records = []
        offset = 0
        page_size = self.config.page_size if hasattr(self.config, 'page_size') else 5000

        # Try different API versions
        api_versions = ['default', 'v1']
        successful_url = None

        for version in api_versions:
            url = self._build_api_url(flow, version)

            # Paginated fetch
            while True:
                params = self._build_query_params(
                    year=year,
                    month=month,
                    flow=flow,
                    offset=offset,
                    limit=page_size
                )

                response, error = self._make_request(url, params=params)

                if error:
                    self.logger.warning(f"API error with {version}: {error}")
                    break

                if response is None:
                    break

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                if response.status_code != 200:
                    self.logger.warning(f"HTTP {response.status_code} from {url}")
                    break

                try:
                    data = response.json()
                except json.JSONDecodeError:
                    self.logger.error("Failed to parse JSON response")
                    break

                # Extract records from response
                records = self._extract_records_from_response(data)

                if not records:
                    self.logger.debug(f"No more records at offset {offset}")
                    break

                all_records.extend(records)
                successful_url = url

                self.logger.debug(
                    f"Fetched {len(records)} records at offset {offset}, "
                    f"total: {len(all_records)}"
                )

                # Check if we got a full page
                if len(records) < page_size:
                    break  # Last page

                offset += page_size

                # Safety limit
                if offset > 1000000:
                    self.logger.warning("Reached safety limit of 1M records")
                    break

            if all_records:
                break  # Found data with this API version

        if not all_records:
            return FetchResult(
                success=False,
                source="COMEX_STAT",
                period=period,
                error_message="No data fetched from any API endpoint"
            )

        # Convert to DataFrame
        df = pd.DataFrame(all_records)

        return FetchResult(
            success=True,
            source="COMEX_STAT",
            period=period,
            records_fetched=len(all_records),
            data=df
        )

    def _extract_records_from_response(self, data: Any) -> List[Dict]:
        """Extract records from Comex Stat API response"""

        # Handle different response formats
        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            # Check common keys
            for key in ['data', 'records', 'result', 'results', 'items', 'content']:
                if key in data and isinstance(data[key], list):
                    return data[key]

            # Check for nested structure
            if 'response' in data and isinstance(data['response'], dict):
                return self._extract_records_from_response(data['response'])

        return []

    def parse_response(self, response_data: Any) -> pd.DataFrame:
        """Parse API response into DataFrame"""
        if isinstance(response_data, pd.DataFrame):
            return response_data

        records = self._extract_records_from_response(response_data)
        return pd.DataFrame(records)

    def transform_to_records(self, df: pd.DataFrame, flow: str) -> List[Dict]:
        """
        Transform Comex Stat DataFrame to standardized trade records

        Args:
            df: DataFrame from API
            flow: 'export' or 'import'

        Returns:
            List of normalized record dictionaries
        """
        records = []

        # Map column names (Comex Stat uses various naming conventions)
        column_map = {
            'year': ['year', 'ano', 'Year', 'ANO'],
            'month': ['month', 'mes', 'Month', 'MES'],
            'ncm': ['ncm', 'NCM', 'co_ncm', 'CO_NCM', 'product', 'hs_code'],
            'ncm_description': ['ncm_description', 'no_ncm', 'NO_NCM', 'description'],
            'country': ['country', 'pais', 'partner', 'co_pais', 'CO_PAIS'],
            'country_code': ['country_code', 'co_pais', 'CO_PAIS', 'partner_code'],
            'state': ['state', 'uf', 'sg_uf', 'SG_UF'],
            'port': ['port', 'urf', 'no_urf', 'NO_URF'],
            'kg_net': ['kg_net', 'kg_liquido', 'KG_LIQUIDO', 'weight_net_kg', 'quantity'],
            'value_fob': ['value_fob_usd', 'vl_fob', 'VL_FOB', 'fob_value', 'trade_value_fob'],
            'value_cif': ['value_cif_usd', 'vl_cif', 'VL_CIF', 'cif_value', 'trade_value_cif'],
        }

        def get_column_value(row, key_names):
            """Get value from row using multiple possible column names"""
            for name in key_names:
                if name in row.index and pd.notna(row[name]):
                    return row[name]
            return None

        for _, row in df.iterrows():
            try:
                year = get_column_value(row, column_map['year'])
                month = get_column_value(row, column_map['month'])
                ncm = get_column_value(row, column_map['ncm'])

                if not all([year, month, ncm]):
                    continue

                year = int(year)
                month = int(month)
                ncm = str(ncm).strip()

                # Get values
                kg_net = get_column_value(row, column_map['kg_net'])
                value_fob = get_column_value(row, column_map['value_fob'])
                value_cif = get_column_value(row, column_map['value_cif'])

                # Use appropriate value based on flow
                if flow == 'export':
                    value_usd = float(value_fob) if value_fob else 0.0
                else:
                    value_usd = float(value_cif) if value_cif else (float(value_fob) if value_fob else 0.0)

                # Get partner country
                partner = get_column_value(row, column_map['country'])
                partner_code = get_column_value(row, column_map['country_code'])

                record = {
                    'data_source': 'COMEX_STAT',
                    'reporter_country': 'BRA',
                    'flow': flow,
                    'year': year,
                    'month': month,
                    'period': self.normalize_period(year, month),
                    'hs_code': ncm.replace('.', ''),
                    'hs_level': len(ncm.replace('.', '')),
                    'hs_code_6': self.normalize_hs_code(ncm, 6),
                    'partner_country': self.normalize_country_name(str(partner)) if partner else 'UNKNOWN',
                    'partner_country_code': str(partner_code) if partner_code else None,
                    'quantity_kg': float(kg_net) if kg_net else None,
                    'quantity_tons': self.convert_to_metric_tons(float(kg_net), 'kg') if kg_net else None,
                    'value_usd': value_usd,
                    'value_fob_usd': float(value_fob) if value_fob else None,
                    'value_cif_usd': float(value_cif) if value_cif else None,
                    'ingested_at': datetime.utcnow(),
                }

                # Optional fields
                ncm_desc = get_column_value(row, column_map['ncm_description'])
                if ncm_desc:
                    record['hs_description'] = str(ncm_desc)

                state = get_column_value(row, column_map['state'])
                if state:
                    record['state_region'] = str(state)

                port = get_column_value(row, column_map['port'])
                if port:
                    record['customs_office'] = str(port)

                records.append(record)

            except Exception as e:
                self.logger.warning(f"Error transforming row: {str(e)}")
                continue

        return records

    def fetch_by_ncm(
        self,
        year: int,
        month: int,
        ncm_codes: List[str],
        flow: str = "export"
    ) -> FetchResult:
        """
        Fetch data for specific NCM codes

        Args:
            year: Year to fetch
            month: Month to fetch
            ncm_codes: List of NCM codes to filter
            flow: 'export' or 'import'

        Returns:
            FetchResult with filtered data
        """
        # First fetch all data
        result = self.fetch_data(year, month, flow)

        if not result.success or result.data is None:
            return result

        # Filter to specified NCM codes
        df = result.data

        # Find the NCM column
        ncm_col = None
        for col in ['ncm', 'NCM', 'co_ncm', 'CO_NCM']:
            if col in df.columns:
                ncm_col = col
                break

        if ncm_col is None:
            return result  # Can't filter, return all

        # Normalize codes for comparison
        ncm_set = set(str(code).replace('.', '')[:6] for code in ncm_codes)

        # Filter DataFrame
        df['ncm_6'] = df[ncm_col].astype(str).str.replace('.', '', regex=False).str[:6]
        filtered_df = df[df['ncm_6'].isin(ncm_set)].copy()
        filtered_df = filtered_df.drop(columns=['ncm_6'])

        return FetchResult(
            success=True,
            source="COMEX_STAT",
            period=result.period,
            records_fetched=len(filtered_df),
            data=filtered_df
        )

    def validate_against_dashboard(
        self,
        records: List[Dict],
        year: int,
        month: int,
        flow: str
    ) -> Tuple[bool, Dict]:
        """
        Validate fetched totals against Comex Stat dashboard

        Note: This requires scraping the dashboard or having known reference values.
        For now, performs internal consistency checks.
        """
        total_value = sum(r.get('value_usd', 0) or 0 for r in records)
        total_quantity = sum(r.get('quantity_kg', 0) or 0 for r in records)
        record_count = len(records)

        validation = {
            'period': f"{year}-{month:02d}",
            'flow': flow,
            'record_count': record_count,
            'total_value_usd': total_value,
            'total_quantity_kg': total_quantity,
            'is_valid': True,
            'issues': []
        }

        # Basic sanity checks
        if record_count == 0:
            validation['is_valid'] = False
            validation['issues'].append("No records found")

        # Check for reasonable totals
        # Brazil's monthly trade is typically in the billions
        if flow == 'export' and total_value < 1e9:  # Less than $1B
            validation['issues'].append(
                f"Unusually low export total: ${total_value:,.0f}"
            )

        # Check unique dimensions
        unique_partners = len(set(r.get('partner_country') for r in records))
        unique_products = len(set(r.get('hs_code') for r in records))
        unique_states = len(set(r.get('state_region') for r in records if r.get('state_region')))

        validation['unique_partners'] = unique_partners
        validation['unique_products'] = unique_products
        validation['unique_states'] = unique_states

        # Brazil typically has diverse trade
        if unique_partners < 50:
            validation['issues'].append(
                f"Few trading partners: {unique_partners}"
            )

        return validation['is_valid'], validation


# =============================================================================
# Example API Request Templates
# =============================================================================

EXAMPLE_REQUESTS = {
    "monthly_exports_8digit": {
        "url": "https://api-comex.stat.gov.br/comexstat",
        "params": {
            "freq": "M",
            "type": "export",
            "year": 2024,
            "month": 8,
            "hs_level": 8,
            "partner": "all",
            "offset": 0,
            "limit": 5000,
        },
        "description": "Monthly exports at NCM 8-digit level"
    },
    "monthly_imports_by_state": {
        "url": "https://api-comex.stat.gov.br/comexstat",
        "params": {
            "freq": "M",
            "type": "import",
            "year": 2024,
            "month": 8,
            "hs_level": 8,
            "partner": "all",
            "detail": "state",
            "offset": 0,
            "limit": 5000,
        },
        "description": "Monthly imports with state breakdown"
    },
}


# =============================================================================
# CLI
# =============================================================================

def main():
    """Command-line interface for testing Brazil agent"""
    import argparse
    from ..config.settings import BrazilConfig

    parser = argparse.ArgumentParser(
        description='Brazil Comex Stat Trade Data Agent'
    )

    parser.add_argument(
        'command',
        choices=['fetch', 'test', 'status'],
        help='Command to execute'
    )

    parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    parser.add_argument('--month', '-m', type=int, default=datetime.now().month - 1 or 12)
    parser.add_argument('--flow', '-f', choices=['export', 'import'], default='export')

    args = parser.parse_args()

    config = BrazilConfig()
    agent = BrazilComexStatAgent(config)

    if args.command == 'fetch':
        result = agent.fetch_data(args.year, args.month, args.flow)
        print(f"Fetch result: success={result.success}, records={result.records_fetched}")

        if result.success and result.data is not None:
            print(f"Columns: {list(result.data.columns)}")
            print(f"Sample:\n{result.data.head()}")

    elif args.command == 'test':
        results = agent.run_monthly_pull(args.year, args.month)
        for flow, result in results.items():
            print(f"\n{flow}: inserted={result.records_inserted}, errors={result.records_errored}")

    elif args.command == 'status':
        status = agent.get_status()
        print(json.dumps(status, indent=2, default=str))


if __name__ == '__main__':
    main()
