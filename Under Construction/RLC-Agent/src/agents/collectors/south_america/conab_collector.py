"""
CONAB Collector (Companhia Nacional de Abastecimento)

Collects Brazilian crop estimates and supply/demand data from CONAB:
- Grain & fiber harvest estimates (safras)
- Supply & demand balances
- Historical production data

Data sources:
- Portal: https://portaldeinformacoes.conab.gov.br/download-arquivos.html
- Safras: https://www.conab.gov.br/info-agro/safras
- S&D: https://www.conab.gov.br/info-agro/oferta-e-demanda-agropecuaria

No API key required - data available via file downloads.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from io import StringIO, BytesIO

# Handle imports for both module and direct script execution
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from .base_collector import (
        BaseCollector,
        CollectorConfig,
        CollectorResult,
        DataFrequency,
        AuthType
    )
except ImportError:
    # Running directly - import from base_collector file directly
    import importlib.util
    _bc_path = Path(__file__).parent / 'base_collector.py'
    _spec = importlib.util.spec_from_file_location("base_collector", _bc_path)
    _base_collector = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_base_collector)
    BaseCollector = _base_collector.BaseCollector
    CollectorConfig = _base_collector.CollectorConfig
    CollectorResult = _base_collector.CollectorResult
    DataFrequency = _base_collector.DataFrequency
    AuthType = _base_collector.AuthType

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# CONAB commodity mappings
CONAB_COMMODITIES = {
    'soja': {
        'en_name': 'soybeans',
        'unit': '1000 t',
        'category': 'oleaginosas'
    },
    'milho': {
        'en_name': 'corn',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'milho_1_safra': {
        'en_name': 'corn_first_crop',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'milho_2_safra': {
        'en_name': 'corn_safrinha',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'trigo': {
        'en_name': 'wheat',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'arroz': {
        'en_name': 'rice',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'algodao': {
        'en_name': 'cotton',
        'unit': '1000 t',
        'category': 'fibras'
    },
    'feijao': {
        'en_name': 'beans',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'sorgo': {
        'en_name': 'sorghum',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'cevada': {
        'en_name': 'barley',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'aveia': {
        'en_name': 'oats',
        'unit': '1000 t',
        'category': 'cereais'
    },
    'girassol': {
        'en_name': 'sunflower',
        'unit': '1000 t',
        'category': 'oleaginosas'
    },
    'amendoim': {
        'en_name': 'peanuts',
        'unit': '1000 t',
        'category': 'oleaginosas'
    },
    'mamona': {
        'en_name': 'castor_bean',
        'unit': '1000 t',
        'category': 'oleaginosas'
    },
    'canola': {
        'en_name': 'canola',
        'unit': '1000 t',
        'category': 'oleaginosas'
    },
}

# Brazilian state codes
BR_STATES = {
    'AC': 'Acre', 'AL': 'Alagoas', 'AP': 'Amapá', 'AM': 'Amazonas',
    'BA': 'Bahia', 'CE': 'Ceará', 'DF': 'Distrito Federal',
    'ES': 'Espírito Santo', 'GO': 'Goiás', 'MA': 'Maranhão',
    'MT': 'Mato Grosso', 'MS': 'Mato Grosso do Sul', 'MG': 'Minas Gerais',
    'PA': 'Pará', 'PB': 'Paraíba', 'PR': 'Paraná', 'PE': 'Pernambuco',
    'PI': 'Piauí', 'RJ': 'Rio de Janeiro', 'RN': 'Rio Grande do Norte',
    'RS': 'Rio Grande do Sul', 'RO': 'Rondônia', 'RR': 'Roraima',
    'SC': 'Santa Catarina', 'SP': 'São Paulo', 'SE': 'Sergipe',
    'TO': 'Tocantins'
}


@dataclass
class CONABConfig(CollectorConfig):
    """CONAB specific configuration"""
    source_name: str = "CONAB"
    source_url: str = "https://portaldeinformacoes.conab.gov.br"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # CONAB endpoints
    download_base: str = "https://portaldeinformacoes.conab.gov.br/downloads/"
    safras_url: str = "https://www.conab.gov.br/info-agro/safras"
    sd_url: str = "https://www.conab.gov.br/info-agro/oferta-e-demanda-agropecuaria"

    # Data types to fetch
    commodities: List[str] = field(default_factory=lambda: [
        'soybeans', 'corn', 'wheat', 'rice', 'cotton', 'sorghum', 'barley'
    ])

    # Rate limiting - be respectful to government servers
    rate_limit_per_minute: int = 10
    timeout: int = 60


class CONABCollector(BaseCollector):
    """
    Collector for CONAB Brazilian crop estimates and supply/demand data.

    CONAB (Companhia Nacional de Abastecimento) is Brazil's national supply
    company responsible for official crop production estimates.

    Key data:
    - Monthly grain & fiber harvest estimates
    - Supply/demand balances
    - State-level production data
    - Historical series

    No API key required.
    """

    def __init__(self, config: CONABConfig = None):
        config = config or CONABConfig()
        super().__init__(config)
        self.config: CONABConfig = config

        # Map English names back to Portuguese for queries
        self._en_to_pt = {v['en_name']: k for k, v in CONAB_COMMODITIES.items()}

    def get_table_name(self) -> str:
        return "conab_crop_estimates"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        data_type: str = "production",
        commodities: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from CONAB.

        Args:
            start_date: Start date for data range (crop year start)
            end_date: End date (default: current crop year)
            data_type: 'production', 'supply_demand', or 'prices'
            commodities: List of commodities to fetch

        Returns:
            CollectorResult with fetched data
        """
        commodities = commodities or self.config.commodities

        if data_type == "production":
            return self._fetch_production_estimates(commodities, **kwargs)
        elif data_type == "supply_demand":
            return self._fetch_supply_demand(commodities, **kwargs)
        elif data_type == "prices":
            return self._fetch_prices(commodities, **kwargs)
        elif data_type == "historical":
            return self._fetch_historical_series(commodities, start_date, end_date)
        else:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Unknown data_type: {data_type}"
            )

    def _fetch_production_estimates(
        self,
        commodities: List[str],
        crop_year: str = None,
        survey_number: int = None
    ) -> CollectorResult:
        """
        Fetch production estimates from CONAB safras.

        CONAB releases up to 12 surveys per crop year, typically:
        - Survey 1: October (planting intentions)
        - Surveys 2-12: Monthly updates through September
        """
        all_records = []
        warnings = []

        # Determine crop year (Brazilian crop year: Aug-Jul)
        if not crop_year:
            now = datetime.now()
            if now.month >= 8:
                crop_year = f"{now.year}/{str(now.year + 1)[2:]}"
            else:
                crop_year = f"{now.year - 1}/{str(now.year)[2:]}"

        # Try to fetch the downloadable data file
        # CONAB portal provides CSV files with crop estimates
        download_url = f"{self.config.download_base}safras_series_historica.csv"

        response, error = self._make_request(download_url)

        if error:
            # Try alternative endpoint
            self.logger.warning(f"Primary download failed: {error}")
            warnings.append(f"Primary download failed, trying alternative")

            # Try the pentaho endpoint
            alt_url = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/api/repos/:home:SIMASA2:SerieHistoricaGraos.csv/generatedContent"
            response, error = self._make_request(alt_url)

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Failed to fetch production data: {error}",
                warnings=warnings
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"HTTP {response.status_code}",
                warnings=warnings
            )

        # Parse the CSV response
        try:
            if PANDAS_AVAILABLE:
                # Try different encodings common in Brazilian data
                for encoding in ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']:
                    try:
                        df = pd.read_csv(
                            StringIO(response.text),
                            encoding=encoding,
                            sep=';',  # Brazilian CSVs often use semicolon
                            decimal=','  # Brazilian decimal separator
                        )
                        break
                    except UnicodeDecodeError:
                        continue

                # Normalize column names
                df.columns = [self._normalize_column_name(c) for c in df.columns]

                # Filter commodities
                for _, row in df.iterrows():
                    commodity_pt = str(row.get('produto', '')).lower()
                    commodity_info = CONAB_COMMODITIES.get(commodity_pt)

                    if commodity_info and commodity_info['en_name'] in commodities:
                        record = self._parse_production_record(row, commodity_info)
                        if record:
                            all_records.append(record)
            else:
                # Parse without pandas
                lines = response.text.split('\n')
                if lines:
                    headers = [self._normalize_column_name(h) for h in lines[0].split(';')]
                    for line in lines[1:]:
                        if line.strip():
                            values = line.split(';')
                            row = dict(zip(headers, values))
                            commodity_pt = str(row.get('produto', '')).lower()
                            commodity_info = CONAB_COMMODITIES.get(commodity_pt)

                            if commodity_info and commodity_info['en_name'] in commodities:
                                record = self._parse_production_record(row, commodity_info)
                                if record:
                                    all_records.append(record)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {str(e)}",
                warnings=warnings
            )

        if not all_records:
            warnings.append("No matching records found for specified commodities")

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            data_as_of=datetime.now().isoformat(),
            warnings=warnings
        )

    def _fetch_supply_demand(
        self,
        commodities: List[str],
        **kwargs
    ) -> CollectorResult:
        """Fetch supply and demand balance data"""
        all_records = []
        warnings = []

        # Try the supply/demand download
        sd_url = f"{self.config.download_base}oferta_demanda.csv"

        response, error = self._make_request(sd_url)

        if error:
            warnings.append(f"S&D download failed: {error}")
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Failed to fetch S&D data: {error}",
                warnings=warnings
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"HTTP {response.status_code}",
                warnings=warnings
            )

        # Parse the response
        try:
            if PANDAS_AVAILABLE:
                for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(
                            StringIO(response.text),
                            encoding=encoding,
                            sep=';',
                            decimal=','
                        )
                        break
                    except (UnicodeDecodeError, pd.errors.ParserError):
                        continue

                df.columns = [self._normalize_column_name(c) for c in df.columns]

                for _, row in df.iterrows():
                    commodity_pt = str(row.get('produto', '')).lower()
                    commodity_info = CONAB_COMMODITIES.get(commodity_pt)

                    if commodity_info and commodity_info['en_name'] in commodities:
                        record = self._parse_sd_record(row, commodity_info)
                        if record:
                            all_records.append(record)

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

    def _fetch_prices(
        self,
        commodities: List[str],
        frequency: str = "weekly",
        **kwargs
    ) -> CollectorResult:
        """Fetch agricultural prices from CONAB"""
        all_records = []
        warnings = []

        # CONAB price data endpoint
        if frequency == "weekly":
            price_url = f"{self.config.download_base}precos_semanal.csv"
        else:
            price_url = f"{self.config.download_base}precos_mensal.csv"

        response, error = self._make_request(price_url)

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Failed to fetch price data: {error}",
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
            if PANDAS_AVAILABLE:
                for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(
                            StringIO(response.text),
                            encoding=encoding,
                            sep=';',
                            decimal=','
                        )
                        break
                    except UnicodeDecodeError:
                        continue

                df.columns = [self._normalize_column_name(c) for c in df.columns]

                for _, row in df.iterrows():
                    commodity_pt = str(row.get('produto', '')).lower()
                    commodity_info = CONAB_COMMODITIES.get(commodity_pt)

                    if commodity_info and commodity_info['en_name'] in commodities:
                        record = self._parse_price_record(row, commodity_info)
                        if record:
                            all_records.append(record)

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

    def _fetch_historical_series(
        self,
        commodities: List[str],
        start_date: date = None,
        end_date: date = None
    ) -> CollectorResult:
        """Fetch historical production series"""
        # Historical data endpoint
        hist_url = f"{self.config.download_base}serie_historica_graos.csv"

        response, error = self._make_request(hist_url)

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Failed to fetch historical data: {error}"
            )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"HTTP {response.status_code}"
            )

        all_records = []

        try:
            if PANDAS_AVAILABLE:
                for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(
                            StringIO(response.text),
                            encoding=encoding,
                            sep=';',
                            decimal=','
                        )
                        break
                    except UnicodeDecodeError:
                        continue

                df.columns = [self._normalize_column_name(c) for c in df.columns]

                for _, row in df.iterrows():
                    commodity_pt = str(row.get('produto', '')).lower()
                    commodity_info = CONAB_COMMODITIES.get(commodity_pt)

                    if commodity_info and commodity_info['en_name'] in commodities:
                        record = self._parse_historical_record(row, commodity_info)
                        if record:
                            # Filter by date if specified
                            if start_date or end_date:
                                crop_year = record.get('crop_year', '')
                                year_start = int(crop_year.split('/')[0]) if '/' in crop_year else int(crop_year[:4])

                                if start_date and year_start < start_date.year:
                                    continue
                                if end_date and year_start > end_date.year:
                                    continue

                            all_records.append(record)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {str(e)}"
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
            period_start=start_date.isoformat() if start_date else None,
            period_end=end_date.isoformat() if end_date else None
        )

    def _normalize_column_name(self, name: str) -> str:
        """Normalize column names"""
        # Remove accents and special characters
        normalized = str(name).lower().strip()

        # Common mappings
        mappings = {
            'safra': 'crop_year',
            'produto': 'produto',
            'uf': 'state',
            'região': 'region',
            'regiao': 'region',
            'área plantada': 'planted_area',
            'area_plantada': 'planted_area',
            'área colhida': 'harvested_area',
            'area_colhida': 'harvested_area',
            'produção': 'production',
            'producao': 'production',
            'produtividade': 'yield',
            'preço': 'price',
            'preco': 'price',
            'estoque inicial': 'beginning_stocks',
            'estoque_inicial': 'beginning_stocks',
            'estoque final': 'ending_stocks',
            'estoque_final': 'ending_stocks',
            'importação': 'imports',
            'importacao': 'imports',
            'exportação': 'exports',
            'exportacao': 'exports',
            'consumo': 'consumption',
        }

        for pt, en in mappings.items():
            if pt in normalized:
                return en

        # Replace spaces and special chars with underscore
        return re.sub(r'[^a-z0-9]', '_', normalized)

    def _parse_production_record(
        self,
        row: Dict,
        commodity_info: Dict
    ) -> Optional[Dict]:
        """Parse production estimate record"""
        try:
            return {
                'commodity': commodity_info['en_name'],
                'commodity_pt': row.get('produto', ''),
                'crop_year': row.get('crop_year', row.get('safra', '')),
                'state': row.get('state', row.get('uf', 'BRASIL')),
                'region': row.get('region', ''),

                # Area (1000 ha)
                'planted_area_1000ha': self._safe_float(row.get('planted_area', row.get('area_plantada'))),
                'harvested_area_1000ha': self._safe_float(row.get('harvested_area', row.get('area_colhida'))),

                # Production (1000 t)
                'production_1000t': self._safe_float(row.get('production', row.get('producao'))),

                # Yield (kg/ha)
                'yield_kg_ha': self._safe_float(row.get('yield', row.get('produtividade'))),

                'unit': commodity_info['unit'],
                'source': 'CONAB',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing production record: {e}")
            return None

    def _parse_sd_record(
        self,
        row: Dict,
        commodity_info: Dict
    ) -> Optional[Dict]:
        """Parse supply/demand balance record"""
        try:
            return {
                'commodity': commodity_info['en_name'],
                'commodity_pt': row.get('produto', ''),
                'crop_year': row.get('crop_year', row.get('safra', '')),

                # Supply (1000 t)
                'beginning_stocks_1000t': self._safe_float(row.get('beginning_stocks', row.get('estoque_inicial'))),
                'production_1000t': self._safe_float(row.get('production', row.get('producao'))),
                'imports_1000t': self._safe_float(row.get('imports', row.get('importacao'))),
                'total_supply_1000t': self._safe_float(row.get('total_supply', row.get('oferta_total'))),

                # Demand (1000 t)
                'consumption_1000t': self._safe_float(row.get('consumption', row.get('consumo'))),
                'exports_1000t': self._safe_float(row.get('exports', row.get('exportacao'))),
                'seed_loss_1000t': self._safe_float(row.get('seed_loss', row.get('semente_perda'))),
                'total_demand_1000t': self._safe_float(row.get('total_demand', row.get('demanda_total'))),

                # Balance
                'ending_stocks_1000t': self._safe_float(row.get('ending_stocks', row.get('estoque_final'))),
                'stocks_to_use_ratio': self._safe_float(row.get('stocks_use', row.get('relacao_estoque_uso'))),

                'unit': commodity_info['unit'],
                'source': 'CONAB',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing S&D record: {e}")
            return None

    def _parse_price_record(
        self,
        row: Dict,
        commodity_info: Dict
    ) -> Optional[Dict]:
        """Parse price record"""
        try:
            return {
                'commodity': commodity_info['en_name'],
                'commodity_pt': row.get('produto', ''),
                'date': row.get('date', row.get('data', '')),
                'state': row.get('state', row.get('uf', '')),
                'municipality': row.get('municipality', row.get('municipio', '')),

                'price_brl_per_60kg': self._safe_float(row.get('price', row.get('preco'))),

                'source': 'CONAB',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing price record: {e}")
            return None

    def _parse_historical_record(
        self,
        row: Dict,
        commodity_info: Dict
    ) -> Optional[Dict]:
        """Parse historical series record"""
        try:
            return {
                'commodity': commodity_info['en_name'],
                'commodity_pt': row.get('produto', ''),
                'crop_year': row.get('crop_year', row.get('safra', '')),

                'planted_area_1000ha': self._safe_float(row.get('planted_area')),
                'harvested_area_1000ha': self._safe_float(row.get('harvested_area')),
                'production_1000t': self._safe_float(row.get('production')),
                'yield_kg_ha': self._safe_float(row.get('yield')),

                'source': 'CONAB',
                'collected_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Error parsing historical record: {e}")
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert value to float, handling Brazilian number format"""
        if value is None or value == '' or str(value).strip() == '':
            return None
        try:
            # Handle Brazilian format (1.234,56 -> 1234.56)
            str_val = str(value).strip()
            str_val = str_val.replace('.', '').replace(',', '.')
            return float(str_val)
        except (ValueError, TypeError):
            return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse API response"""
        return response_data

    # =========================================================================
    # CONVENIENCE METHODS
    # =========================================================================

    def get_current_crop_estimates(
        self,
        commodities: List[str] = None
    ) -> Optional[Any]:
        """
        Get current crop year production estimates.

        Args:
            commodities: List of commodities (default: all configured)

        Returns:
            DataFrame or list of records
        """
        result = self.collect(
            data_type="production",
            commodities=commodities or self.config.commodities
        )
        return result.data if result.success else None

    def get_brazil_supply_demand(
        self,
        commodity: str
    ) -> Optional[Any]:
        """
        Get Brazilian supply/demand balance for a commodity.

        Args:
            commodity: Commodity name (e.g., 'soybeans', 'corn')

        Returns:
            DataFrame or list of records
        """
        result = self.collect(
            data_type="supply_demand",
            commodities=[commodity]
        )
        return result.data if result.success else None

    def get_production_by_state(
        self,
        commodity: str,
        crop_year: str = None
    ) -> Dict[str, float]:
        """
        Get production breakdown by Brazilian state.

        Args:
            commodity: Commodity name
            crop_year: Crop year (e.g., '2024/25')

        Returns:
            Dict of state -> production (1000 t)
        """
        result = self.collect(
            data_type="production",
            commodities=[commodity]
        )

        if not result.success or result.data is None:
            return {}

        if PANDAS_AVAILABLE and hasattr(result.data, 'groupby'):
            df = result.data
            if crop_year:
                df = df[df['crop_year'] == crop_year]

            by_state = df.groupby('state')['production_1000t'].sum()
            return by_state.to_dict()

        return {}


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for CONAB collector"""
    import argparse
    import json

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='CONAB Data Collector')

    parser.add_argument(
        'command',
        choices=['production', 'supply_demand', 'prices', 'historical', 'test'],
        help='Command to execute'
    )

    parser.add_argument(
        '--commodities',
        nargs='+',
        default=['soybeans', 'corn', 'wheat'],
        help='Commodities to fetch'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file (JSON or CSV)'
    )

    args = parser.parse_args()

    config = CONABConfig(commodities=args.commodities)
    collector = CONABCollector(config)

    if args.command == 'test':
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    result = collector.collect(
        data_type=args.command,
        commodities=args.commodities
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


if __name__ == '__main__':
    main()
