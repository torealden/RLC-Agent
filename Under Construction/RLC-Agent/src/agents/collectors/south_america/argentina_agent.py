"""
Argentina INDEC Trade Data Agent
Collects trade data from INDEC's Foreign Trade portal (Comercio Exterior)
"""

import io
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd
import requests

from .base_trade_agent import BaseTradeAgent, FetchResult, LoadResult


class ArgentinaINDECAgent(BaseTradeAgent):
    """
    Agent for collecting Argentina trade data from INDEC

    Data characteristics:
    - Source: INDEC Comercio Exterior portal
    - Format: Monthly CSV/XLS files
    - HS Code: NCM (Nomenclatura Comun del MERCOSUR) - up to 10 digits
    - Update frequency: Mid-month for previous month's data
    - Encoding: Latin-1 (ISO-8859-1)
    - Delimiter: Semicolon (;)
    """

    def __init__(self, config, db_session_factory=None):
        super().__init__(config, db_session_factory)
        self.logger = logging.getLogger(f"ArgentinaINDECAgent")

        # INDEC-specific settings
        self.encoding = config.encoding if hasattr(config, 'encoding') else 'latin-1'
        self.delimiter = config.delimiter if hasattr(config, 'delimiter') else ';'

    def _build_download_url(self, year: int, month: int, flow: str) -> List[str]:
        """
        Build possible download URLs for INDEC data

        INDEC URL patterns vary; we try multiple patterns
        """
        urls = []

        # Primary pattern from config
        if hasattr(self.config, 'download_url_pattern'):
            urls.append(
                self.config.download_url_pattern.format(year=year, month=month)
            )

        # Alternative patterns
        flow_prefix = "expo" if flow == "export" else "impo"

        url_patterns = [
            # Pattern 1: Basic monthly file
            f"https://www.indec.gob.ar/ftp/cuadros/economia/{flow_prefix}_{month:02d}_{year}.csv",
            f"https://www.indec.gob.ar/ftp/cuadros/economia/{flow_prefix}_{year}_{month:02d}.csv",

            # Pattern 2: With full names
            f"https://www.indec.gob.ar/ftp/cuadros/economia/exportaciones_{year}_{month:02d}.csv",
            f"https://www.indec.gob.ar/ftp/cuadros/economia/importaciones_{year}_{month:02d}.csv",

            # Pattern 3: Intercambio format
            f"https://www.indec.gob.ar/ftp/cuadros/economia/intercambio_{year}_{month:02d}.csv",

            # Pattern 4: With underscores and different date format
            f"https://www.indec.gob.ar/ftp/cuadros/economia/com_ext_{flow_prefix}_{year}{month:02d}.csv",

            # Excel formats
            f"https://www.indec.gob.ar/ftp/cuadros/economia/{flow_prefix}_{month:02d}_{year}.xlsx",
            f"https://www.indec.gob.ar/ftp/cuadros/economia/{flow_prefix}_{year}_{month:02d}.xlsx",
        ]

        urls.extend(url_patterns)
        return urls

    def _try_download_file(self, urls: List[str]) -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        """
        Try downloading from multiple URLs

        Returns:
            Tuple of (content, successful_url, error_message)
        """
        last_error = None

        for url in urls:
            self.logger.debug(f"Trying URL: {url}")

            response, error = self._make_request(url, timeout=120)

            if error:
                last_error = error
                continue

            if response and response.status_code == 200:
                # Verify we got actual data, not an error page
                content_type = response.headers.get('content-type', '').lower()

                if 'text/html' in content_type and len(response.content) < 10000:
                    # Likely an error page
                    last_error = "Received HTML instead of data file"
                    continue

                self.logger.info(f"Successfully downloaded from: {url}")
                return response.content, url, None

            elif response:
                last_error = f"HTTP {response.status_code}"

        return None, None, last_error or "All download URLs failed"

    def fetch_data(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Fetch trade data from INDEC for a specific period

        Args:
            year: Year to fetch
            month: Month to fetch (1-12)
            flow: 'export' or 'import'

        Returns:
            FetchResult with fetched data
        """
        period = self.normalize_period(year, month)
        self.logger.info(f"Fetching Argentina {flow}s for {period}")

        # Build possible URLs
        urls = self._build_download_url(year, month, flow)

        # Try to download
        content, successful_url, error = self._try_download_file(urls)

        if error or not content:
            return FetchResult(
                success=False,
                source="INDEC",
                period=period,
                error_message=error or "Failed to download file"
            )

        # Compute file hash
        file_hash = self._compute_file_hash(content)

        # Parse the content
        try:
            df = self._parse_file_content(content, successful_url)

            if df is None or df.empty:
                return FetchResult(
                    success=False,
                    source="INDEC",
                    period=period,
                    error_message="Parsed file contains no data"
                )

            return FetchResult(
                success=True,
                source="INDEC",
                period=period,
                records_fetched=len(df),
                data=df,
                file_hash=file_hash
            )

        except Exception as e:
            self.logger.error(f"Error parsing INDEC file: {str(e)}", exc_info=True)
            return FetchResult(
                success=False,
                source="INDEC",
                period=period,
                error_message=f"Parse error: {str(e)}"
            )

    def _parse_file_content(self, content: bytes, url: str) -> Optional[pd.DataFrame]:
        """
        Parse file content based on file type

        Args:
            content: Raw file bytes
            url: Source URL (to determine file type)

        Returns:
            Parsed DataFrame
        """
        if url.endswith('.xlsx') or url.endswith('.xls'):
            return self._parse_excel(content)
        else:
            return self._parse_csv(content)

    def _parse_csv(self, content: bytes) -> pd.DataFrame:
        """Parse CSV content with INDEC-specific handling"""

        # Try multiple encodings
        encodings = ['latin-1', 'utf-8', 'iso-8859-1', 'cp1252']
        delimiters = [';', ',', '\t']

        for encoding in encodings:
            for delimiter in delimiters:
                try:
                    df = pd.read_csv(
                        io.BytesIO(content),
                        encoding=encoding,
                        delimiter=delimiter,
                        dtype=str,  # Read all as strings initially
                        on_bad_lines='skip'
                    )

                    # Check if we got meaningful columns
                    if len(df.columns) > 3 and len(df) > 0:
                        self.logger.debug(
                            f"Successfully parsed CSV with encoding={encoding}, delimiter={delimiter}"
                        )
                        return df

                except Exception as e:
                    continue

        raise ValueError("Failed to parse CSV with any encoding/delimiter combination")

    def _parse_excel(self, content: bytes) -> pd.DataFrame:
        """Parse Excel content"""
        try:
            # Try reading as xlsx first
            df = pd.read_excel(
                io.BytesIO(content),
                dtype=str,
                engine='openpyxl'
            )
        except Exception:
            # Fall back to xls format
            df = pd.read_excel(
                io.BytesIO(content),
                dtype=str,
                engine='xlrd'
            )

        return df

    def parse_response(self, response_data: Any) -> pd.DataFrame:
        """Parse raw response - delegates to internal methods"""
        if isinstance(response_data, pd.DataFrame):
            return response_data
        elif isinstance(response_data, bytes):
            return self._parse_csv(response_data)
        else:
            raise ValueError(f"Unexpected response type: {type(response_data)}")

    def _map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Map INDEC column names to standardized names

        INDEC columns are typically in Spanish
        """
        # Get field mappings from config or use defaults
        if hasattr(self.config, 'field_mappings'):
            field_map = self.config.field_mappings
        else:
            field_map = {
                "PERIODO": "period",
                "FLUJO": "flow",
                "NCM": "hs_code",
                "POSICION": "hs_code",
                "DESCRIPCION": "description",
                "PAIS_DESTINO": "partner_country",
                "PAIS_ORIGEN": "partner_country",
                "PAIS": "partner_country",
                "PESO_KG": "quantity_kg",
                "PESO_NETO_KG": "quantity_kg",
                "KILOS": "quantity_kg",
                "FOB_USD": "value_usd",
                "FOB_DOLARES": "value_usd",
                "CIF_USD": "value_usd",
                "CIF_DOLARES": "value_usd",
                "DOLARES": "value_usd",
                "CANTIDAD": "quantity_units",
                "UNIDAD": "unit",
                "VIA_TRANSPORTE": "transport_mode",
                "TRANSPORTE": "transport_mode",
                "ADUANA": "customs_office",
            }

        # Normalize column names for matching
        df_columns_upper = {col.upper().strip(): col for col in df.columns}

        # Build rename mapping
        rename_map = {}
        for spanish_name, english_name in field_map.items():
            if spanish_name.upper() in df_columns_upper:
                original_col = df_columns_upper[spanish_name.upper()]
                rename_map[original_col] = english_name

        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    def transform_to_records(self, df: pd.DataFrame, flow: str) -> List[Dict]:
        """
        Transform INDEC DataFrame to standardized trade records

        Args:
            df: Parsed DataFrame from INDEC
            flow: 'export' or 'import'

        Returns:
            List of normalized record dictionaries
        """
        # Map columns
        df = self._map_columns(df)

        records = []

        for _, row in df.iterrows():
            try:
                record = self._transform_row(row, flow)
                if record:
                    records.append(record)
            except Exception as e:
                self.logger.warning(f"Error transforming row: {str(e)}")
                continue

        return records

    def _transform_row(self, row: pd.Series, flow: str) -> Optional[Dict]:
        """Transform a single row to a normalized record"""

        # Extract HS code
        hs_code = None
        for col in ['hs_code', 'ncm', 'posicion', 'NCM', 'POSICION']:
            if col in row.index and pd.notna(row.get(col)):
                hs_code = str(row[col]).strip()
                break

        if not hs_code:
            return None

        # Extract value
        value_usd = None
        for col in ['value_usd', 'fob_usd', 'cif_usd', 'dolares', 'FOB_USD', 'CIF_USD']:
            if col in row.index and pd.notna(row.get(col)):
                try:
                    # Handle Argentine number format (1.234,56 -> 1234.56)
                    val_str = str(row[col]).replace('.', '').replace(',', '.')
                    value_usd = float(val_str)
                    break
                except (ValueError, TypeError):
                    continue

        if value_usd is None:
            return None

        # Extract quantity
        quantity_kg = None
        for col in ['quantity_kg', 'peso_kg', 'peso_neto_kg', 'kilos', 'PESO_KG']:
            if col in row.index and pd.notna(row.get(col)):
                try:
                    val_str = str(row[col]).replace('.', '').replace(',', '.')
                    quantity_kg = float(val_str)
                    break
                except (ValueError, TypeError):
                    continue

        # Extract partner country
        partner_country = None
        for col in ['partner_country', 'pais_destino', 'pais_origen', 'pais', 'PAIS']:
            if col in row.index and pd.notna(row.get(col)):
                partner_country = str(row[col]).strip()
                break

        if not partner_country:
            partner_country = "UNKNOWN"

        # Extract period
        period = None
        year = None
        month = None
        for col in ['period', 'periodo', 'PERIODO']:
            if col in row.index and pd.notna(row.get(col)):
                period_str = str(row[col]).strip()
                # Try to parse various formats: YYYY-MM, YYYYMM, MM/YYYY
                if '-' in period_str:
                    parts = period_str.split('-')
                    year = int(parts[0])
                    month = int(parts[1])
                elif '/' in period_str:
                    parts = period_str.split('/')
                    month = int(parts[0])
                    year = int(parts[1])
                elif len(period_str) == 6:
                    year = int(period_str[:4])
                    month = int(period_str[4:])
                break

        if year and month:
            period = self.normalize_period(year, month)

        # Build record
        record = {
            'data_source': 'INDEC',
            'reporter_country': 'ARG',
            'flow': flow,
            'year': year,
            'month': month,
            'period': period,
            'hs_code': hs_code.replace('.', '').replace(' ', ''),
            'hs_level': len(hs_code.replace('.', '').replace(' ', '')),
            'hs_code_6': self.normalize_hs_code(hs_code, 6),
            'partner_country': self.normalize_country_name(partner_country),
            'quantity_kg': quantity_kg,
            'quantity_tons': self.convert_to_metric_tons(quantity_kg, 'kg') if quantity_kg else None,
            'value_usd': value_usd,
            'value_fob_usd': value_usd if flow == 'export' else None,
            'value_cif_usd': value_usd if flow == 'import' else None,
            'ingested_at': datetime.utcnow(),
        }

        # Optional fields
        if 'description' in row.index and pd.notna(row.get('description')):
            record['hs_description'] = str(row['description'])

        if 'transport_mode' in row.index and pd.notna(row.get('transport_mode')):
            record['transport_mode'] = str(row['transport_mode'])

        if 'customs_office' in row.index and pd.notna(row.get('customs_office')):
            record['customs_office'] = str(row['customs_office'])

        return record

    def validate_monthly_totals(
        self,
        records: List[Dict],
        year: int,
        month: int,
        expected_total: float = None
    ) -> Tuple[bool, Dict]:
        """
        Validate that record totals match expected values

        Args:
            records: List of parsed records
            year: Year being validated
            month: Month being validated
            expected_total: Expected total value (if known)

        Returns:
            Tuple of (is_valid, validation_details)
        """
        # Calculate totals
        total_value = sum(r.get('value_usd', 0) or 0 for r in records)
        total_quantity = sum(r.get('quantity_kg', 0) or 0 for r in records)
        record_count = len(records)

        validation = {
            'period': f"{year}-{month:02d}",
            'record_count': record_count,
            'total_value_usd': total_value,
            'total_quantity_kg': total_quantity,
            'is_valid': True,
            'issues': []
        }

        # Check record count
        if record_count == 0:
            validation['is_valid'] = False
            validation['issues'].append("No records found")

        # Check against expected total if provided
        if expected_total is not None:
            deviation = abs(total_value - expected_total) / expected_total * 100
            validation['expected_total'] = expected_total
            validation['deviation_pct'] = deviation

            if deviation > 5:  # More than 5% deviation
                validation['is_valid'] = False
                validation['issues'].append(
                    f"Total deviates by {deviation:.2f}% from expected"
                )

        # Check for suspicious patterns
        unique_partners = len(set(r.get('partner_country') for r in records))
        unique_hs_codes = len(set(r.get('hs_code') for r in records))

        validation['unique_partners'] = unique_partners
        validation['unique_hs_codes'] = unique_hs_codes

        if unique_partners < 5:
            validation['issues'].append(
                f"Suspiciously few partners: {unique_partners}"
            )

        return validation['is_valid'], validation


# =============================================================================
# CLI for standalone testing
# =============================================================================

def main():
    """Command-line interface for testing Argentina agent"""
    import argparse
    from ..config.settings import ArgentinaConfig

    parser = argparse.ArgumentParser(
        description='Argentina INDEC Trade Data Agent'
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

    # Create agent with default config
    config = ArgentinaConfig()
    agent = ArgentinaINDECAgent(config)

    if args.command == 'fetch':
        result = agent.fetch_data(args.year, args.month, args.flow)
        print(f"Fetch result: {result}")

        if result.success and result.data is not None:
            print(f"Records fetched: {len(result.data)}")
            print(f"Columns: {list(result.data.columns)}")
            print(f"Sample:\n{result.data.head()}")

    elif args.command == 'test':
        results = agent.run_monthly_pull(args.year, args.month)
        for flow, result in results.items():
            print(f"\n{flow}: {result}")

    elif args.command == 'status':
        status = agent.get_status()
        import json
        print(json.dumps(status, indent=2, default=str))


if __name__ == '__main__':
    main()
