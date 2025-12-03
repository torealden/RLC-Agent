"""
Paraguay Trade Data Agent
Primary: Attempts to fetch from DNA Paraguay (limited availability)
Fallback: WITS (World Bank) and UN Comtrade APIs
"""

import json
import logging
import io
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd

from .base_trade_agent import BaseTradeAgent, FetchResult, LoadResult


class ParaguayAgent(BaseTradeAgent):
    """
    Agent for collecting Paraguay trade data

    Due to limited structured data availability from Paraguay's customs service,
    this agent implements a multi-source strategy:

    1. Primary: DNA Paraguay (if available)
    2. Fallback 1: WITS (World Bank) API
    3. Fallback 2: UN Comtrade API

    Data characteristics:
    - WITS provides annual data with HS6 level detail
    - Comtrade provides more granular data but requires API key
    - Both have 1-2 month data lag
    """

    # Paraguay WITS code
    WITS_REPORTER_CODE = "600"

    # API endpoints
    WITS_BASE = "https://wits.worldbank.org/API/V1"
    COMTRADE_BASE = "https://comtradeapi.un.org/data/v1/get"

    def __init__(self, config, db_session_factory=None):
        super().__init__(config, db_session_factory)
        self.logger = logging.getLogger("ParaguayAgent")

        # Get API keys
        self.comtrade_api_key = getattr(config, 'comtrade_api_key', 'XXX_COMTRADE_API_KEY')

        # Track which source was used
        self.last_source = None

    def fetch_data(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Fetch Paraguay trade data with multi-source fallback

        Args:
            year: Year to fetch
            month: Month to fetch (1-12)
            flow: 'export' or 'import'

        Returns:
            FetchResult with fetched data
        """
        period = self.normalize_period(year, month)
        self.logger.info(f"Fetching Paraguay {flow}s for {period}")

        # Strategy: Try sources in order of preference
        sources = []

        # 1. Try DNA Paraguay primary source (if configured)
        if getattr(self.config, 'use_dna_primary', True):
            sources.append(('DNA_PY', self._fetch_from_dna))

        # 2. WITS fallback (annual data - aggregate to month)
        if getattr(self.config, 'wits_enabled', True):
            sources.append(('WITS', self._fetch_from_wits))

        # 3. UN Comtrade fallback
        if getattr(self.config, 'comtrade_enabled', True) and 'XXX' not in self.comtrade_api_key:
            sources.append(('COMTRADE', self._fetch_from_comtrade))

        for source_name, fetch_func in sources:
            self.logger.info(f"Trying source: {source_name}")

            try:
                result = fetch_func(year, month, flow)

                if result.success:
                    self.last_source = source_name
                    self.logger.info(
                        f"Successfully fetched {result.records_fetched} records from {source_name}"
                    )
                    return result

                self.logger.warning(f"{source_name} failed: {result.error_message}")

            except Exception as e:
                self.logger.warning(f"{source_name} error: {str(e)}")
                continue

        # All sources failed
        return FetchResult(
            success=False,
            source="NONE",
            period=period,
            error_message="All data sources failed for Paraguay"
        )

    def _fetch_from_dna(
        self,
        year: int,
        month: int,
        flow: str
    ) -> FetchResult:
        """
        Attempt to fetch from DNA Paraguay

        Paraguay's customs doesn't have a well-documented API.
        This method tries common URL patterns for downloadable files.
        """
        period = self.normalize_period(year, month)

        # Try common bulletin URL patterns
        url_patterns = [
            f"https://www.aduana.gov.py/uploads/estadisticas/boletin_{year}_{month:02d}.xlsx",
            f"https://www.aduana.gov.py/uploads/estadisticas/comercio_exterior_{year}{month:02d}.csv",
            f"https://www.aduana.gov.py/uploads/estadisticas/{year}/boletin_mensual_{month:02d}.xlsx",
        ]

        for url in url_patterns:
            response, error = self._make_request(url, timeout=120)

            if error or not response:
                continue

            if response.status_code != 200:
                continue

            # Try to parse the response
            content_type = response.headers.get('content-type', '').lower()

            try:
                if 'excel' in content_type or url.endswith('.xlsx'):
                    df = pd.read_excel(io.BytesIO(response.content), dtype=str)
                elif 'csv' in content_type or url.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(response.content), dtype=str)
                else:
                    continue

                if not df.empty:
                    return FetchResult(
                        success=True,
                        source="DNA_PY",
                        period=period,
                        records_fetched=len(df),
                        data=df,
                        file_hash=self._compute_file_hash(response.content)
                    )
            except Exception as e:
                self.logger.debug(f"Parse error for {url}: {e}")
                continue

        return FetchResult(
            success=False,
            source="DNA_PY",
            period=period,
            error_message="No structured data available from DNA Paraguay"
        )

    def _fetch_from_wits(
        self,
        year: int,
        month: int,
        flow: str
    ) -> FetchResult:
        """
        Fetch from WITS (World Bank) API

        WITS API endpoint template:
        https://wits.worldbank.org/API/V1/commodity/{flow}/{reporter}/{partner}/{product}?year=YYYY

        Note: WITS provides annual data, not monthly.
        """
        period = self.normalize_period(year, month)

        flow_param = "EXPORT" if flow == "export" else "IMPORT"

        # Build WITS API URL
        # Using TOTAL product to get all products, then we'll disaggregate
        url = f"{self.WITS_BASE}/commodity/{flow_param}/{self.WITS_REPORTER_CODE}/ALL/ALL"

        params = {
            "year": year,
            "page": 1,
            "max": 50000,
        }

        all_records = []
        page = 1

        while True:
            params['page'] = page

            response, error = self._make_request(url, params=params)

            if error:
                if all_records:
                    break
                return FetchResult(
                    success=False,
                    source="WITS",
                    period=period,
                    error_message=error
                )

            if not response or response.status_code != 200:
                if all_records:
                    break
                return FetchResult(
                    success=False,
                    source="WITS",
                    period=period,
                    error_message=f"HTTP {response.status_code if response else 'No response'}"
                )

            # WITS returns XML
            try:
                records = self._parse_wits_xml(response.content)
            except Exception as e:
                self.logger.error(f"WITS parse error: {e}")
                if all_records:
                    break
                return FetchResult(
                    success=False,
                    source="WITS",
                    period=period,
                    error_message=f"Parse error: {e}"
                )

            if not records:
                break

            all_records.extend(records)

            if len(records) < params['max']:
                break

            page += 1

            if page > 100:  # Safety limit
                break

        if not all_records:
            return FetchResult(
                success=False,
                source="WITS",
                period=period,
                error_message="No data returned from WITS"
            )

        df = pd.DataFrame(all_records)

        # Note: WITS is annual - we'll attribute to the requested month
        # but flag that this is annual data distributed
        df['data_note'] = 'Annual data from WITS (not monthly)'

        return FetchResult(
            success=True,
            source="WITS",
            period=period,
            records_fetched=len(df),
            data=df
        )

    def _parse_wits_xml(self, content: bytes) -> List[Dict]:
        """Parse WITS XML response"""
        records = []

        try:
            root = ET.fromstring(content)

            # Navigate WITS XML structure
            for record in root.findall('.//record') or root.findall('.//Row'):
                rec = {}

                # Map WITS fields
                field_map = {
                    'Reporter': 'reporter_country',
                    'Partner': 'partner_country',
                    'Product': 'hs_code',
                    'ProductDescription': 'description',
                    'Year': 'year',
                    'TradeValue': 'value_usd',
                    'Quantity': 'quantity',
                    'QuantityUnit': 'unit',
                }

                for wits_field, our_field in field_map.items():
                    elem = record.find(f'.//{wits_field}')
                    if elem is not None and elem.text:
                        rec[our_field] = elem.text

                if rec:
                    records.append(rec)

        except ET.ParseError:
            # Try JSON fallback (some WITS endpoints return JSON)
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    return data
            except json.JSONDecodeError:
                pass

        return records

    def _fetch_from_comtrade(
        self,
        year: int,
        month: int,
        flow: str
    ) -> FetchResult:
        """
        Fetch from UN Comtrade API

        UN Comtrade API (v1):
        https://comtradeapi.un.org/data/v1/get/{typeCode}/{freqCode}/{clCode}

        Requires API key (free tier available)
        """
        period = self.normalize_period(year, month)

        if 'XXX' in self.comtrade_api_key:
            return FetchResult(
                success=False,
                source="COMTRADE",
                period=period,
                error_message="Comtrade API key not configured"
            )

        # Build Comtrade API request
        # typeCode: C (commodities)
        # freqCode: M (monthly)
        # clCode: HS (harmonized system)
        base_url = "https://comtradeapi.un.org/data/v1/get/C/M/HS"

        flow_code = "X" if flow == "export" else "M"  # X=export, M=import

        params = {
            "reporterCode": "600",  # Paraguay
            "period": f"{year}{month:02d}",  # YYYYMM format
            "flowCode": flow_code,
            "partnerCode": "0",  # All partners (World)
            "cmdCode": "TOTAL",  # All commodities
            "subscription-key": self.comtrade_api_key,
        }

        # Add headers
        headers = {
            "Ocp-Apim-Subscription-Key": self.comtrade_api_key,
        }

        response, error = self._make_request(base_url, params=params, headers=headers)

        if error:
            return FetchResult(
                success=False,
                source="COMTRADE",
                period=period,
                error_message=error
            )

        if not response or response.status_code != 200:
            return FetchResult(
                success=False,
                source="COMTRADE",
                period=period,
                error_message=f"HTTP {response.status_code if response else 'No response'}"
            )

        try:
            data = response.json()
        except json.JSONDecodeError:
            return FetchResult(
                success=False,
                source="COMTRADE",
                period=period,
                error_message="Invalid JSON response from Comtrade"
            )

        # Extract records
        records = data.get('data', [])

        if not records:
            return FetchResult(
                success=False,
                source="COMTRADE",
                period=period,
                error_message="No data in Comtrade response"
            )

        df = pd.DataFrame(records)

        return FetchResult(
            success=True,
            source="COMTRADE",
            period=period,
            records_fetched=len(df),
            data=df
        )

    def parse_response(self, response_data: Any) -> pd.DataFrame:
        """Parse response based on source type"""
        if isinstance(response_data, pd.DataFrame):
            return response_data

        if isinstance(response_data, list):
            return pd.DataFrame(response_data)

        if isinstance(response_data, dict):
            if 'data' in response_data:
                return pd.DataFrame(response_data['data'])
            return pd.DataFrame([response_data])

        raise ValueError(f"Unexpected response type: {type(response_data)}")

    def transform_to_records(self, df: pd.DataFrame, flow: str) -> List[Dict]:
        """
        Transform DataFrame to standardized trade records

        Handles multiple source formats (DNA, WITS, Comtrade)
        """
        records = []

        # Detect source format by columns
        columns_lower = {col.lower(): col for col in df.columns}

        # WITS columns
        is_wits = 'tradvalue' in columns_lower or 'tradevalue' in columns_lower

        # Comtrade columns
        is_comtrade = 'primaryvalue' in columns_lower or 'cifvalue' in columns_lower

        for _, row in df.iterrows():
            try:
                if is_wits:
                    record = self._transform_wits_row(row, flow)
                elif is_comtrade:
                    record = self._transform_comtrade_row(row, flow)
                else:
                    record = self._transform_generic_row(row, flow)

                if record:
                    records.append(record)

            except Exception as e:
                self.logger.warning(f"Error transforming row: {str(e)}")
                continue

        return records

    def _transform_wits_row(self, row: pd.Series, flow: str) -> Optional[Dict]:
        """Transform WITS format row"""
        columns = {col.lower(): col for col in row.index}

        def get_val(key):
            actual_col = columns.get(key.lower())
            if actual_col and pd.notna(row[actual_col]):
                return row[actual_col]
            return None

        year = get_val('year')
        hs_code = get_val('product')
        partner = get_val('partner')
        value = get_val('tradevalue') or get_val('tradvalue')

        if not all([year, hs_code]):
            return None

        record = {
            'data_source': 'WITS',
            'reporter_country': 'PRY',
            'flow': flow,
            'year': int(year),
            'month': None,  # WITS is annual
            'period': f"{int(year)}-00",  # Indicate annual
            'hs_code': str(hs_code).replace('.', ''),
            'hs_level': len(str(hs_code).replace('.', '')),
            'hs_code_6': self.normalize_hs_code(str(hs_code), 6),
            'partner_country': self.normalize_country_name(str(partner)) if partner else 'WORLD',
            'quantity_kg': float(get_val('quantity')) if get_val('quantity') else None,
            'value_usd': float(value) if value else 0.0,
            'ingested_at': datetime.utcnow(),
        }

        desc = get_val('productdescription')
        if desc:
            record['hs_description'] = str(desc)

        return record

    def _transform_comtrade_row(self, row: pd.Series, flow: str) -> Optional[Dict]:
        """Transform UN Comtrade format row"""
        columns = {col.lower(): col for col in row.index}

        def get_val(key):
            actual_col = columns.get(key.lower())
            if actual_col and pd.notna(row[actual_col]):
                return row[actual_col]
            return None

        period = get_val('period')  # YYYYMM format
        cmd_code = get_val('cmdcode')  # HS code
        partner = get_val('partnerdesc') or get_val('partnercode')

        # Values
        primary_value = get_val('primaryvalue')
        fob_value = get_val('fobvalue')
        cif_value = get_val('cifvalue')

        if not all([period, cmd_code]):
            return None

        year = int(str(period)[:4])
        month = int(str(period)[4:6]) if len(str(period)) >= 6 else None

        if flow == 'export':
            value_usd = float(fob_value or primary_value or 0)
        else:
            value_usd = float(cif_value or primary_value or 0)

        record = {
            'data_source': 'COMTRADE',
            'reporter_country': 'PRY',
            'flow': flow,
            'year': year,
            'month': month,
            'period': self.normalize_period(year, month) if month else f"{year}-00",
            'hs_code': str(cmd_code).replace('.', ''),
            'hs_level': len(str(cmd_code).replace('.', '')),
            'hs_code_6': self.normalize_hs_code(str(cmd_code), 6),
            'partner_country': self.normalize_country_name(str(partner)) if partner else 'WORLD',
            'quantity_kg': float(get_val('netweight') or get_val('qty') or 0) if get_val('netweight') or get_val('qty') else None,
            'value_usd': value_usd,
            'value_fob_usd': float(fob_value) if fob_value else None,
            'value_cif_usd': float(cif_value) if cif_value else None,
            'ingested_at': datetime.utcnow(),
        }

        cmd_desc = get_val('cmddesc')
        if cmd_desc:
            record['hs_description'] = str(cmd_desc)

        return record

    def _transform_generic_row(self, row: pd.Series, flow: str) -> Optional[Dict]:
        """Transform generic/DNA format row"""
        columns = {col.lower(): col for col in row.index}

        def get_val(*keys):
            for key in keys:
                actual_col = columns.get(key.lower())
                if actual_col and pd.notna(row[actual_col]):
                    return row[actual_col]
            return None

        hs_code = get_val('ncm', 'hs_code', 'posicion', 'codigo')
        year = get_val('year', 'anio', 'ano')
        month = get_val('month', 'mes')
        partner = get_val('pais', 'partner', 'destino', 'origen')
        value = get_val('valor', 'value', 'fob', 'cif', 'dolares')
        quantity = get_val('peso', 'kg', 'cantidad', 'quantity')

        if not hs_code:
            return None

        record = {
            'data_source': 'DNA_PY',
            'reporter_country': 'PRY',
            'flow': flow,
            'year': int(year) if year else None,
            'month': int(month) if month else None,
            'period': self.normalize_period(int(year), int(month)) if year and month else None,
            'hs_code': str(hs_code).replace('.', ''),
            'hs_level': len(str(hs_code).replace('.', '')),
            'hs_code_6': self.normalize_hs_code(str(hs_code), 6),
            'partner_country': self.normalize_country_name(str(partner)) if partner else 'UNKNOWN',
            'quantity_kg': float(quantity) if quantity else None,
            'value_usd': float(value) if value else 0.0,
            'ingested_at': datetime.utcnow(),
        }

        return record


# =============================================================================
# API Reference
# =============================================================================

API_EXAMPLES = {
    "wits_export": {
        "url": "https://wits.worldbank.org/API/V1/commodity/export/600/all/TOTAL",
        "params": {
            "year": 2023,
            "page": 1,
            "max": 50000,
        },
        "description": "Paraguay exports via WITS (annual)"
    },
    "comtrade_monthly": {
        "url": "https://comtradeapi.un.org/data/v1/get/C/M/HS",
        "params": {
            "reporterCode": "600",
            "period": "202408",
            "flowCode": "X",
            "partnerCode": "0",
            "cmdCode": "TOTAL",
            "subscription-key": "YOUR_API_KEY",
        },
        "description": "Paraguay monthly exports via Comtrade"
    },
}


# =============================================================================
# CLI
# =============================================================================

def main():
    """Command-line interface for testing Paraguay agent"""
    import argparse
    from ..config.settings import ParaguayConfig

    parser = argparse.ArgumentParser(
        description='Paraguay Trade Data Agent (with WITS/Comtrade fallback)'
    )

    parser.add_argument(
        'command',
        choices=['fetch', 'test', 'status', 'wits', 'comtrade'],
        help='Command to execute'
    )

    parser.add_argument('--year', '-y', type=int, default=datetime.now().year)
    parser.add_argument('--month', '-m', type=int, default=datetime.now().month - 2 or 10)
    parser.add_argument('--flow', '-f', choices=['export', 'import'], default='export')
    parser.add_argument('--comtrade-key', type=str, help='UN Comtrade API key')

    args = parser.parse_args()

    config = ParaguayConfig()
    if args.comtrade_key:
        config.comtrade_api_key = args.comtrade_key

    agent = ParaguayAgent(config)

    if args.command == 'fetch':
        result = agent.fetch_data(args.year, args.month, args.flow)
        print(f"Fetch result: success={result.success}, source={result.source}, records={result.records_fetched}")

        if result.success and result.data is not None:
            print(f"Columns: {list(result.data.columns)}")
            print(f"Sample:\n{result.data.head()}")

    elif args.command == 'wits':
        result = agent._fetch_from_wits(args.year, args.month, args.flow)
        print(f"WITS result: success={result.success}, records={result.records_fetched}")

    elif args.command == 'comtrade':
        result = agent._fetch_from_comtrade(args.year, args.month, args.flow)
        print(f"Comtrade result: success={result.success}, records={result.records_fetched}")

    elif args.command == 'test':
        results = agent.run_monthly_pull(args.year, args.month)
        for flow, result in results.items():
            print(f"\n{flow}: inserted={result.records_inserted}, errors={result.records_errored}")

    elif args.command == 'status':
        status = agent.get_status()
        print(json.dumps(status, indent=2, default=str))


if __name__ == '__main__':
    main()
