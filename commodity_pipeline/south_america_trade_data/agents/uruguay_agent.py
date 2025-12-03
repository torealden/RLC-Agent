"""
Uruguay DNA Trade Data Agent
Collects trade data from Uruguay Open Data (datos.gub.uy) via CKAN API
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd

from .base_trade_agent import BaseTradeAgent, FetchResult, LoadResult


class UruguayDNAAgent(BaseTradeAgent):
    """
    Agent for collecting Uruguay trade data from DNA (Direccion Nacional de Aduanas)

    Data characteristics:
    - Source: catalogodatos.gub.uy (Uruguay Open Data)
    - API: CKAN API (datastore_search)
    - Format: JSON responses
    - HS Code: NCM (MERCOSUR Nomenclature)
    - Update frequency: Mid-month for previous month
    """

    # CKAN API endpoints
    CKAN_BASE = "https://catalogodatos.gub.uy"
    CKAN_API = "https://catalogodatos.gub.uy/api/3/action"

    def __init__(self, config, db_session_factory=None):
        super().__init__(config, db_session_factory)
        self.logger = logging.getLogger("UruguayDNAAgent")

    def _get_resource_id(self, flow: str) -> str:
        """
        Get CKAN resource ID for exports or imports

        These IDs need to be discovered from catalogodatos.gub.uy
        """
        if flow == 'export':
            return getattr(self.config, 'export_resource_id', 'XXX_EXPORT_RESOURCE_ID')
        else:
            return getattr(self.config, 'import_resource_id', 'XXX_IMPORT_RESOURCE_ID')

    def _build_datastore_url(self) -> str:
        """Build CKAN datastore_search endpoint URL"""
        return f"{self.CKAN_API}/datastore_search"

    def _build_ckan_params(
        self,
        resource_id: str,
        year: int,
        month: int,
        offset: int = 0,
        limit: int = 1000
    ) -> Dict:
        """
        Build CKAN datastore_search parameters

        CKAN API reference:
        https://docs.ckan.org/en/latest/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search
        """
        # Build filters for year and month
        filters = json.dumps({
            "anio": year,
            "mes": month
        })

        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset,
            "filters": filters,
        }

        return params

    def fetch_data(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Fetch trade data from Uruguay via CKAN API

        Args:
            year: Year to fetch
            month: Month to fetch (1-12)
            flow: 'export' or 'import'

        Returns:
            FetchResult with fetched data
        """
        period = self.normalize_period(year, month)
        self.logger.info(f"Fetching Uruguay {flow}s for {period}")

        resource_id = self._get_resource_id(flow)

        if 'XXX' in resource_id:
            self.logger.warning(
                f"Resource ID not configured for {flow}. "
                "Attempting to discover from catalogodatos.gub.uy..."
            )
            discovered_id = self._discover_resource_id(flow)
            if discovered_id:
                resource_id = discovered_id
            else:
                return FetchResult(
                    success=False,
                    source="DNA_UY",
                    period=period,
                    error_message="Resource ID not configured and discovery failed"
                )

        url = self._build_datastore_url()
        all_records = []
        offset = 0
        page_size = getattr(self.config, 'page_size', 1000)

        while True:
            params = self._build_ckan_params(
                resource_id=resource_id,
                year=year,
                month=month,
                offset=offset,
                limit=page_size
            )

            response, error = self._make_request(url, params=params)

            if error:
                if all_records:
                    self.logger.warning(f"Error after {len(all_records)} records: {error}")
                    break
                return FetchResult(
                    success=False,
                    source="DNA_UY",
                    period=period,
                    error_message=error
                )

            if response is None or response.status_code != 200:
                status = response.status_code if response else "No response"
                if all_records:
                    break
                return FetchResult(
                    success=False,
                    source="DNA_UY",
                    period=period,
                    error_message=f"HTTP {status}"
                )

            try:
                data = response.json()
            except json.JSONDecodeError:
                self.logger.error("Failed to parse JSON")
                break

            # CKAN response structure
            if not data.get('success', False):
                error_msg = data.get('error', {}).get('message', 'Unknown error')
                if all_records:
                    break
                return FetchResult(
                    success=False,
                    source="DNA_UY",
                    period=period,
                    error_message=f"CKAN error: {error_msg}"
                )

            result = data.get('result', {})
            records = result.get('records', [])

            if not records:
                break

            all_records.extend(records)
            self.logger.debug(f"Fetched {len(records)} records at offset {offset}")

            # Check if there are more records
            total = result.get('total', 0)
            if offset + len(records) >= total:
                break

            offset += page_size

        if not all_records:
            return FetchResult(
                success=False,
                source="DNA_UY",
                period=period,
                error_message="No data returned from CKAN API"
            )

        df = pd.DataFrame(all_records)

        return FetchResult(
            success=True,
            source="DNA_UY",
            period=period,
            records_fetched=len(all_records),
            data=df
        )

    def _discover_resource_id(self, flow: str) -> Optional[str]:
        """
        Discover resource ID by searching catalogodatos.gub.uy

        Search for DNA trade datasets
        """
        search_url = f"{self.CKAN_API}/package_search"

        # Search terms for trade data
        search_queries = [
            'comercio exterior',
            'exportaciones importaciones',
            'DNA comercio',
            'aduana comercio exterior',
        ]

        for query in search_queries:
            params = {
                'q': query,
                'rows': 20,
            }

            response, error = self._make_request(search_url, params=params)

            if error or not response:
                continue

            try:
                data = response.json()
                if not data.get('success'):
                    continue

                results = data.get('result', {}).get('results', [])

                for package in results:
                    name = package.get('name', '').lower()
                    title = package.get('title', '').lower()

                    # Look for monthly trade data from DNA
                    if 'dna' in name or 'aduana' in name or 'comercio' in name:
                        resources = package.get('resources', [])

                        for resource in resources:
                            res_name = resource.get('name', '').lower()
                            res_desc = resource.get('description', '').lower()

                            # Match flow type
                            if flow == 'export' and ('export' in res_name or 'export' in res_desc):
                                return resource.get('id')
                            elif flow == 'import' and ('import' in res_name or 'import' in res_desc):
                                return resource.get('id')

            except Exception as e:
                self.logger.debug(f"Discovery error: {e}")
                continue

        return None

    def parse_response(self, response_data: Any) -> pd.DataFrame:
        """Parse CKAN API response"""
        if isinstance(response_data, pd.DataFrame):
            return response_data

        if isinstance(response_data, dict):
            if 'result' in response_data:
                records = response_data['result'].get('records', [])
                return pd.DataFrame(records)
            if 'records' in response_data:
                return pd.DataFrame(response_data['records'])

        if isinstance(response_data, list):
            return pd.DataFrame(response_data)

        raise ValueError(f"Unexpected response type: {type(response_data)}")

    def transform_to_records(self, df: pd.DataFrame, flow: str) -> List[Dict]:
        """
        Transform Uruguay DataFrame to standardized trade records

        Args:
            df: DataFrame from CKAN API
            flow: 'export' or 'import'

        Returns:
            List of normalized record dictionaries
        """
        records = []

        # Column name variations in Uruguay data
        column_map = {
            'ncm': ['ncm', 'NCM', 'codigo_ncm', 'posicion_arancelaria'],
            'description': ['descripcion', 'desc_ncm', 'descripcion_ncm'],
            'partner': ['pais', 'pais_destino', 'pais_origen', 'nombre_pais'],
            'year': ['anio', 'ano', 'year'],
            'month': ['mes', 'month'],
            'quantity': ['peso_kg', 'peso_neto', 'kg_neto', 'kilogramos'],
            'fob': ['fob_usd', 'valor_fob', 'fob_dolares'],
            'cif': ['cif_usd', 'valor_cif', 'cif_dolares'],
            'regime': ['regimen', 'tipo_regimen'],
        }

        def get_value(row, keys):
            for key in keys:
                if key in row.index and pd.notna(row[key]):
                    return row[key]
            return None

        for _, row in df.iterrows():
            try:
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

                fob = get_value(row, column_map['fob'])
                cif = get_value(row, column_map['cif'])

                if flow == 'export':
                    value_usd = float(fob) if fob else 0.0
                else:
                    value_usd = float(cif) if cif else (float(fob) if fob else 0.0)

                quantity = get_value(row, column_map['quantity'])
                partner = get_value(row, column_map['partner'])

                record = {
                    'data_source': 'DNA_UY',
                    'reporter_country': 'URY',
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

                regime = get_value(row, column_map['regime'])
                if regime:
                    record['customs_office'] = str(regime)

                records.append(record)

            except Exception as e:
                self.logger.warning(f"Error transforming row: {str(e)}")
                continue

        return records

    def fetch_csv_download(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Alternative: Fetch via direct CSV download

        Fallback if CKAN datastore API is unavailable
        """
        import io

        period = self.normalize_period(year, month)

        # Try common CSV URL patterns
        resource_id = self._get_resource_id(flow)

        if 'XXX' not in resource_id:
            # Try direct resource download
            download_url = f"{self.CKAN_BASE}/dataset/{resource_id}/resource/{resource_id}/download"

            response, error = self._make_request(download_url, timeout=120)

            if response and response.status_code == 200:
                try:
                    df = pd.read_csv(
                        io.BytesIO(response.content),
                        encoding='utf-8',
                        dtype=str
                    )

                    # Filter to requested period
                    year_col = None
                    month_col = None

                    for col in df.columns:
                        if col.lower() in ['anio', 'ano', 'year']:
                            year_col = col
                        if col.lower() in ['mes', 'month']:
                            month_col = col

                    if year_col and month_col:
                        df = df[
                            (df[year_col].astype(int) == year) &
                            (df[month_col].astype(int) == month)
                        ]

                    if not df.empty:
                        return FetchResult(
                            success=True,
                            source="DNA_UY_CSV",
                            period=period,
                            records_fetched=len(df),
                            data=df
                        )
                except Exception as e:
                    self.logger.debug(f"CSV parse failed: {e}")

        return FetchResult(
            success=False,
            source="DNA_UY_CSV",
            period=period,
            error_message="CSV download not available"
        )


# =============================================================================
# CKAN API Reference
# =============================================================================

CKAN_API_EXAMPLES = {
    "datastore_search": {
        "url": "https://catalogodatos.gub.uy/api/3/action/datastore_search",
        "params": {
            "resource_id": "{resource_id}",
            "limit": 1000,
            "offset": 0,
            "filters": '{"anio": 2024, "mes": 8}',
        },
        "description": "Query datastore with filters"
    },
    "package_search": {
        "url": "https://catalogodatos.gub.uy/api/3/action/package_search",
        "params": {
            "q": "comercio exterior DNA",
            "rows": 20,
        },
        "description": "Search for datasets"
    },
}


# =============================================================================
# CLI
# =============================================================================

def main():
    """Command-line interface for testing Uruguay agent"""
    import argparse
    from ..config.settings import UruguayConfig

    parser = argparse.ArgumentParser(
        description='Uruguay DNA Trade Data Agent'
    )

    parser.add_argument(
        'command',
        choices=['fetch', 'discover', 'test', 'status'],
        help='Command to execute'
    )

    parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    parser.add_argument('--month', '-m', type=int, default=datetime.now().month - 1 or 12)
    parser.add_argument('--flow', '-f', choices=['export', 'import'], default='export')

    args = parser.parse_args()

    config = UruguayConfig()
    agent = UruguayDNAAgent(config)

    if args.command == 'fetch':
        result = agent.fetch_data(args.year, args.month, args.flow)
        print(f"Fetch result: success={result.success}, records={result.records_fetched}")

        if result.success and result.data is not None:
            print(f"Columns: {list(result.data.columns)}")
            print(f"Sample:\n{result.data.head()}")

    elif args.command == 'discover':
        export_id = agent._discover_resource_id('export')
        import_id = agent._discover_resource_id('import')
        print(f"Discovered resource IDs:")
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
