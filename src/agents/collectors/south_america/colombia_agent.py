"""
Colombia DANE Trade Data Agent
Collects trade data from DANE Open Data (Datos Abiertos) using Socrata-style API
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd

from .base_trade_agent import BaseTradeAgent, FetchResult, LoadResult


class ColombiaDANEAgent(BaseTradeAgent):
    """
    Agent for collecting Colombia trade data from DANE Open Data

    Data characteristics:
    - Source: DANE Datos Abiertos (datos.gov.co)
    - API: Socrata-style with SoQL queries
    - Format: JSON responses
    - HS Code: Up to 10 digits
    - Update frequency: Mid-month for previous month
    - Optional: Socrata app token for improved rate limits
    """

    # Base URLs
    DATOS_GOV_BASE = "https://www.datos.gov.co"
    DANE_PORTAL = "https://www.dane.gov.co"

    def __init__(self, config, db_session_factory=None):
        super().__init__(config, db_session_factory)
        self.logger = logging.getLogger("ColombiaDANEAgent")

        # Set up Socrata app token if available
        self.app_token = getattr(config, 'socrata_app_token', None)
        if self.app_token and self.app_token != 'XXX_SOCRATA_APP_TOKEN':
            self.session.headers.update({
                'X-App-Token': self.app_token
            })
            # Higher rate limits with token
            self.config.rate_limit_per_minute = getattr(
                config, 'rate_limit_with_token', 240
            )

    def _get_dataset_id(self, flow: str) -> str:
        """
        Get dataset ID for exports or imports

        These IDs need to be discovered from datos.gov.co
        They may change periodically when DANE publishes new datasets
        """
        if flow == 'export':
            dataset_id = getattr(self.config, 'export_dataset_id', 'XXX_EXPORT_DATASET_ID')
        else:
            dataset_id = getattr(self.config, 'import_dataset_id', 'XXX_IMPORT_DATASET_ID')

        return dataset_id

    def _build_socrata_url(self, dataset_id: str) -> str:
        """Build Socrata API endpoint URL"""
        return f"{self.DATOS_GOV_BASE}/resource/{dataset_id}.json"

    def _build_soql_query(
        self,
        year: int,
        month: int,
        offset: int = 0,
        limit: int = 50000
    ) -> Dict:
        """
        Build SoQL query parameters

        Socrata Query Language (SoQL) is similar to SQL
        """
        # Select relevant columns
        select_fields = [
            "ncm",
            "descripcion",
            "pais",
            "pais_destino",
            "pais_origen",
            "year",
            "anio",
            "month",
            "mes",
            "kg_neto",
            "peso_neto",
            "fob_usd",
            "valor_fob",
            "cif_usd",
            "valor_cif",
            "departamento",
            "aduana",
        ]

        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "ncm",
        }

        # Build WHERE clause - try various column name formats
        where_clauses = []

        # Year filter
        where_clauses.append(f"(year = {year} OR anio = {year})")

        # Month filter
        where_clauses.append(f"(month = {month} OR mes = {month})")

        if where_clauses:
            params["$where"] = " AND ".join(where_clauses)

        return params

    def fetch_data(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Fetch trade data from DANE via Socrata API

        Args:
            year: Year to fetch
            month: Month to fetch (1-12)
            flow: 'export' or 'import'

        Returns:
            FetchResult with fetched data
        """
        period = self.normalize_period(year, month)
        self.logger.info(f"Fetching Colombia {flow}s for {period}")

        dataset_id = self._get_dataset_id(flow)

        if 'XXX' in dataset_id:
            self.logger.warning(
                f"Dataset ID not configured for {flow}. "
                "Please update config with actual datos.gov.co dataset ID."
            )
            # Try to discover dataset ID
            discovered_id = self._discover_dataset_id(flow)
            if discovered_id:
                dataset_id = discovered_id
                self.logger.info(f"Discovered dataset ID: {dataset_id}")
            else:
                return FetchResult(
                    success=False,
                    source="DANE",
                    period=period,
                    error_message="Dataset ID not configured and discovery failed"
                )

        url = self._build_socrata_url(dataset_id)
        all_records = []
        offset = 0
        page_size = getattr(self.config, 'page_size', 50000)

        while True:
            params = self._build_soql_query(
                year=year,
                month=month,
                offset=offset,
                limit=page_size
            )

            response, error = self._make_request(url, params=params)

            if error:
                if all_records:
                    # Partial success
                    self.logger.warning(f"Error after {len(all_records)} records: {error}")
                    break
                return FetchResult(
                    success=False,
                    source="DANE",
                    period=period,
                    error_message=error
                )

            if response is None or response.status_code != 200:
                status = response.status_code if response else "No response"
                if all_records:
                    break
                return FetchResult(
                    success=False,
                    source="DANE",
                    period=period,
                    error_message=f"HTTP {status}"
                )

            try:
                data = response.json()
            except json.JSONDecodeError:
                self.logger.error("Failed to parse JSON")
                break

            if not isinstance(data, list):
                self.logger.warning(f"Unexpected response format: {type(data)}")
                break

            if not data:
                break  # No more records

            all_records.extend(data)
            self.logger.debug(f"Fetched {len(data)} records at offset {offset}")

            if len(data) < page_size:
                break  # Last page

            offset += page_size

        if not all_records:
            return FetchResult(
                success=False,
                source="DANE",
                period=period,
                error_message="No data returned from API"
            )

        df = pd.DataFrame(all_records)

        return FetchResult(
            success=True,
            source="DANE",
            period=period,
            records_fetched=len(all_records),
            data=df
        )

    def _discover_dataset_id(self, flow: str) -> Optional[str]:
        """
        Try to discover the dataset ID by searching datos.gov.co

        This is a fallback when dataset IDs are not configured
        """
        search_terms = {
            'export': ['exportaciones', 'comercio exterior exportaciones', 'expo dane'],
            'import': ['importaciones', 'comercio exterior importaciones', 'impo dane'],
        }

        search_url = f"{self.DATOS_GOV_BASE}/api/catalog/v1"

        for term in search_terms.get(flow, []):
            params = {
                'q': term,
                'domains': 'www.datos.gov.co',
                'search_context': 'www.datos.gov.co',
                'limit': 10,
            }

            response, error = self._make_request(search_url, params=params)

            if error or not response:
                continue

            try:
                results = response.json()
                if 'results' in results:
                    for item in results['results']:
                        resource = item.get('resource', {})
                        name = resource.get('name', '').lower()
                        desc = resource.get('description', '').lower()

                        # Look for monthly trade data
                        if 'mensual' in name or 'mensual' in desc:
                            dataset_id = resource.get('id')
                            if dataset_id:
                                self.logger.info(f"Found potential dataset: {name}")
                                return dataset_id
            except Exception as e:
                self.logger.debug(f"Discovery search failed: {e}")
                continue

        return None

    def parse_response(self, response_data: Any) -> pd.DataFrame:
        """Parse Socrata API response"""
        if isinstance(response_data, pd.DataFrame):
            return response_data

        if isinstance(response_data, list):
            return pd.DataFrame(response_data)

        if isinstance(response_data, dict):
            if 'data' in response_data:
                return pd.DataFrame(response_data['data'])

        raise ValueError(f"Unexpected response type: {type(response_data)}")

    def transform_to_records(self, df: pd.DataFrame, flow: str) -> List[Dict]:
        """
        Transform DANE DataFrame to standardized trade records

        Args:
            df: DataFrame from Socrata API
            flow: 'export' or 'import'

        Returns:
            List of normalized record dictionaries
        """
        records = []

        # Column name variations in DANE data
        column_map = {
            'ncm': ['ncm', 'NCM', 'posicion_arancelaria', 'subpartida'],
            'description': ['descripcion', 'desc_ncm', 'descripcion_ncm'],
            'partner': ['pais', 'pais_destino', 'pais_origen', 'nombre_pais'],
            'year': ['year', 'anio', 'ano', 'periodo_anio'],
            'month': ['month', 'mes', 'periodo_mes'],
            'quantity': ['kg_neto', 'peso_neto', 'peso_neto_kg', 'kilogramos'],
            'fob': ['fob_usd', 'valor_fob', 'fob_dolares', 'valor_fob_dolar'],
            'cif': ['cif_usd', 'valor_cif', 'cif_dolares', 'valor_cif_dolar'],
            'state': ['departamento', 'departamento_origen', 'departamento_destino'],
            'customs': ['aduana', 'nombre_aduana'],
        }

        def get_value(row, keys):
            for key in keys:
                if key in row.index and pd.notna(row[key]):
                    return row[key]
            return None

        for _, row in df.iterrows():
            try:
                # Required fields
                ncm = get_value(row, column_map['ncm'])
                if not ncm:
                    continue

                year = get_value(row, column_map['year'])
                month = get_value(row, column_map['month'])

                if not year or not month:
                    continue

                year = int(year)
                month = int(month)
                ncm = str(ncm).strip()

                # Values
                fob = get_value(row, column_map['fob'])
                cif = get_value(row, column_map['cif'])

                if flow == 'export':
                    value_usd = float(fob) if fob else 0.0
                else:
                    value_usd = float(cif) if cif else (float(fob) if fob else 0.0)

                quantity = get_value(row, column_map['quantity'])
                partner = get_value(row, column_map['partner'])

                record = {
                    'data_source': 'DANE',
                    'reporter_country': 'COL',
                    'flow': flow,
                    'year': year,
                    'month': month,
                    'period': self.normalize_period(year, month),
                    'hs_code': ncm.replace('.', ''),
                    'hs_level': len(ncm.replace('.', '')),
                    'hs_code_6': self.normalize_hs_code(ncm, 6),
                    'partner_country': self.normalize_country_name(str(partner)) if partner else 'UNKNOWN',
                    'quantity_kg': float(quantity) if quantity else None,
                    'quantity_tons': self.convert_to_metric_tons(float(quantity), 'kg') if quantity else None,
                    'value_usd': value_usd,
                    'value_fob_usd': float(fob) if fob else None,
                    'value_cif_usd': float(cif) if cif else None,
                    'ingested_at': datetime.utcnow(),
                }

                # Optional fields
                desc = get_value(row, column_map['description'])
                if desc:
                    record['hs_description'] = str(desc)

                state = get_value(row, column_map['state'])
                if state:
                    record['state_region'] = str(state)

                customs = get_value(row, column_map['customs'])
                if customs:
                    record['customs_office'] = str(customs)

                records.append(record)

            except Exception as e:
                self.logger.warning(f"Error transforming row: {str(e)}")
                continue

        return records

    def fetch_from_dane_portal(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Alternative: Fetch directly from DANE portal CSV files

        This is a fallback if the Socrata API is unavailable
        """
        period = self.normalize_period(year, month)

        # DANE portal URL patterns
        flow_name = "exportaciones" if flow == "export" else "importaciones"

        url_patterns = [
            f"https://www.dane.gov.co/files/investigaciones/comercio_exterior/{flow_name}/{year}/anex-{flow_name[:4]}{year}-{month:02d}.csv",
            f"https://www.dane.gov.co/files/investigaciones/comercio_exterior/{flow_name}/{year}/{flow_name}_{year}_{month:02d}.csv",
        ]

        for url in url_patterns:
            response, error = self._make_request(url, timeout=120)

            if error or not response:
                continue

            if response.status_code == 200:
                try:
                    # Try to parse as CSV
                    import io
                    df = pd.read_csv(
                        io.BytesIO(response.content),
                        encoding='utf-8',
                        dtype=str
                    )

                    if not df.empty:
                        return FetchResult(
                            success=True,
                            source="DANE_PORTAL",
                            period=period,
                            records_fetched=len(df),
                            data=df
                        )
                except Exception as e:
                    self.logger.debug(f"Failed to parse {url}: {e}")
                    continue

        return FetchResult(
            success=False,
            source="DANE_PORTAL",
            period=period,
            error_message="Could not fetch from DANE portal"
        )


# =============================================================================
# Socrata SoQL Reference
# =============================================================================

SOQL_EXAMPLES = {
    "basic_query": {
        "url": "https://www.datos.gov.co/resource/{dataset_id}.json",
        "params": {
            "$select": "ncm, pais, year, month, kg_neto, fob_usd",
            "$where": "year = 2024 AND month = 8",
            "$limit": 50000,
            "$offset": 0,
        },
        "description": "Basic monthly query"
    },
    "with_aggregation": {
        "url": "https://www.datos.gov.co/resource/{dataset_id}.json",
        "params": {
            "$select": "ncm, pais, SUM(kg_neto) as total_kg, SUM(fob_usd) as total_fob",
            "$where": "year = 2024",
            "$group": "ncm, pais",
            "$having": "SUM(fob_usd) > 1000000",
            "$order": "total_fob DESC",
            "$limit": 1000,
        },
        "description": "Aggregated query with grouping"
    },
}


# =============================================================================
# CLI
# =============================================================================

def main():
    """Command-line interface for testing Colombia agent"""
    import argparse
    from ..config.settings import ColombiaConfig

    parser = argparse.ArgumentParser(
        description='Colombia DANE Trade Data Agent'
    )

    parser.add_argument(
        'command',
        choices=['fetch', 'discover', 'test', 'status'],
        help='Command to execute'
    )

    parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    parser.add_argument('--month', '-m', type=int, default=datetime.now().month - 1 or 12)
    parser.add_argument('--flow', '-f', choices=['export', 'import'], default='export')
    parser.add_argument('--token', type=str, help='Socrata app token')

    args = parser.parse_args()

    config = ColombiaConfig()
    if args.token:
        config.socrata_app_token = args.token

    agent = ColombiaDANEAgent(config)

    if args.command == 'fetch':
        result = agent.fetch_data(args.year, args.month, args.flow)
        print(f"Fetch result: success={result.success}, records={result.records_fetched}")

        if result.success and result.data is not None:
            print(f"Columns: {list(result.data.columns)}")
            print(f"Sample:\n{result.data.head()}")

    elif args.command == 'discover':
        export_id = agent._discover_dataset_id('export')
        import_id = agent._discover_dataset_id('import')
        print(f"Discovered dataset IDs:")
        print(f"  Exports: {export_id}")
        print(f"  Imports: {import_id}")

    elif args.command == 'test':
        results = agent.run_monthly_pull(args.year, args.month)
        for flow, result in results.items():
            print(f"\n{flow}: inserted={result.records_inserted}, errors={result.records_errored}")

    elif args.command == 'status':
        status = agent.get_status()
        print(json.dumps(status, indent=2, default=str))


if __name__ == '__main__':
    main()
