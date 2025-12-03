"""
South America Trade Data Configuration Settings
Comprehensive configuration for trade data collection from:
- Argentina (INDEC)
- Brazil (Comex Stat / MDIC/SECEX)
- Colombia (DANE)
- Uruguay (DNA - datos.gub.uy)
- Paraguay (DNA / WITS fallback)
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime


class DatabaseType(Enum):
    """Supported database types"""
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"


class FlowType(Enum):
    """Trade flow types"""
    EXPORT = "export"
    IMPORT = "import"


class HSLevel(Enum):
    """HS Code classification levels"""
    HS2 = 2
    HS4 = 4
    HS6 = 6
    HS8 = 8
    HS10 = 10


# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    db_type: DatabaseType = DatabaseType.SQLITE
    host: str = "localhost"
    port: int = 3306
    database: str = "south_america_trade"
    username: str = ""
    password: str = ""

    # SQLite specific
    sqlite_path: Optional[Path] = None

    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30

    def get_connection_string(self) -> str:
        """Generate database connection string"""
        if self.db_type == DatabaseType.SQLITE:
            path = self.sqlite_path or Path("./data/south_america_trade.db")
            return f"sqlite:///{path}"
        elif self.db_type == DatabaseType.MYSQL:
            return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == DatabaseType.POSTGRESQL:
            return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")


# =============================================================================
# BASE COUNTRY CONFIGURATION
# =============================================================================

@dataclass
class CountryConfig:
    """Base configuration for a country data source"""
    country_code: str  # ISO3
    country_name: str
    enabled: bool = True

    # API/Data source settings
    base_url: str = ""
    api_key: str = ""  # Use XXX as placeholder

    # Rate limiting
    rate_limit_per_minute: int = 60
    retry_attempts: int = 3
    retry_delay_base: float = 2.0  # Base for exponential backoff
    timeout: int = 60

    # Pagination
    page_size: int = 1000
    max_pages: int = 1000

    # Data parameters
    hs_levels: List[int] = field(default_factory=lambda: [6])  # Default to HS6 for cross-country comparison
    flow_types: List[str] = field(default_factory=lambda: ["export", "import"])

    # Release schedule (approximate day of month)
    release_day_of_month: int = 15
    release_lag_months: int = 1  # How many months behind is the data

    # Data directories
    data_directory: Path = field(default_factory=lambda: Path("./data/raw"))
    processed_directory: Path = field(default_factory=lambda: Path("./data/processed"))


# =============================================================================
# ARGENTINA - INDEC CONFIGURATION
# =============================================================================

@dataclass
class ArgentinaConfig(CountryConfig):
    """
    Argentina INDEC Configuration
    Source: INDEC's Foreign Trade portal (Consulta Interactiva / Comercio Exterior)
    Data format: Monthly CSV/XLS releases
    """
    country_code: str = "ARG"
    country_name: str = "Argentina"

    # INDEC URLs
    base_url: str = "https://www.indec.gob.ar"
    comercio_exterior_url: str = "https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-2-40"

    # Download patterns - URLs typically embed year/month in filename
    # Pattern: intercambio_MM_YYYY.csv or similar
    download_url_pattern: str = "https://www.indec.gob.ar/ftp/cuadros/economia/intercambio_{month:02d}_{year}.csv"
    alt_download_patterns: List[str] = field(default_factory=lambda: [
        "https://www.indec.gob.ar/ftp/cuadros/economia/expo_{month:02d}_{year}.csv",
        "https://www.indec.gob.ar/ftp/cuadros/economia/impo_{month:02d}_{year}.csv",
    ])

    # File formats
    file_formats: List[str] = field(default_factory=lambda: ["csv", "xls", "xlsx"])
    encoding: str = "latin-1"  # Argentine CSV files often use Latin-1
    delimiter: str = ";"  # Semi-colon delimiter common

    # HS code specifics
    hs_column_name: str = "NCM"  # Nomenclatura Comun del MERCOSUR
    hs_levels: List[int] = field(default_factory=lambda: [6, 8, 10])

    # Field mappings (Spanish to internal)
    field_mappings: Dict[str, str] = field(default_factory=lambda: {
        "PERIODO": "period",
        "FLUJO": "flow",
        "NCM": "hs_code",
        "DESCRIPCION": "description",
        "PAIS_DESTINO": "partner_country",
        "PAIS_ORIGEN": "partner_country",
        "PESO_KG": "quantity_kg",
        "PESO_NETO_KG": "quantity_kg",
        "FOB_USD": "value_usd",
        "CIF_USD": "value_usd",
        "CANTIDAD": "quantity_units",
        "UNIDAD": "unit",
        "VIA_TRANSPORTE": "transport_mode",
        "ADUANA": "customs_office",
    })

    # Release schedule
    release_day_of_month: int = 15  # Mid-month
    release_lag_months: int = 1

    # Quality thresholds
    deviation_threshold_pct: float = 20.0  # Flag >20% deviation from 12-month mean


# =============================================================================
# BRAZIL - COMEX STAT CONFIGURATION
# =============================================================================

@dataclass
class BrazilConfig(CountryConfig):
    """
    Brazil Comex Stat API Configuration
    Source: Comex Stat API (Dados Abertos) - MDIC/SECEX
    API: JSON, no auth for public queries, rate-limited
    """
    country_code: str = "BRA"
    country_name: str = "Brazil"

    # API URLs
    base_url: str = "https://api-comex.stat.gov.br"
    api_path: str = "/comexstat"

    # Full endpoint patterns
    export_endpoint: str = "https://api-comex.stat.gov.br/comexstat/export"
    import_endpoint: str = "https://api-comex.stat.gov.br/comexstat/import"

    # Alternative endpoints (older API versions)
    alt_endpoints: Dict[str, str] = field(default_factory=lambda: {
        "v1_export": "https://api-comex.stat.gov.br/api/v1/export",
        "v1_import": "https://api-comex.stat.gov.br/api/v1/import",
    })

    # API parameters
    api_params: Dict[str, Any] = field(default_factory=lambda: {
        "freq": "M",  # Monthly
        "type": "export",  # or "import"
        "hs_level": 8,  # NCM 8-digit
        "measure": ["kg_net", "value_fob_usd", "value_cif_usd"],
    })

    # HS code specifics - Brazil uses NCM (8 digits)
    hs_column_name: str = "NCM"
    hs_levels: List[int] = field(default_factory=lambda: [2, 4, 6, 8])
    default_hs_level: int = 8  # 8-digit NCM for granular commodities

    # Pagination
    page_size: int = 5000
    uses_cursor: bool = False  # Uses offset/limit
    offset_param: str = "offset"
    limit_param: str = "limit"

    # Rate limiting - Brazil is rate-limited
    rate_limit_per_minute: int = 30
    retry_on_429: bool = True
    backoff_multiplier: float = 2.0

    # Field mappings
    field_mappings: Dict[str, str] = field(default_factory=lambda: {
        "year": "year",
        "month": "month",
        "ncm": "hs_code",
        "ncm_description": "description",
        "country": "partner_country",
        "country_code": "partner_country_code",
        "state": "state",
        "port": "port",
        "kg_net": "quantity_kg",
        "value_fob_usd": "value_fob_usd",
        "value_cif_usd": "value_cif_usd",
    })

    # Release schedule - Brazil posts early in following month
    release_day_of_month: int = 8  # Around 5th-10th
    release_lag_months: int = 1

    # Dashboard URL for validation
    dashboard_url: str = "http://comexstat.mdic.gov.br/pt/home"


# =============================================================================
# COLOMBIA - DANE CONFIGURATION
# =============================================================================

@dataclass
class ColombiaConfig(CountryConfig):
    """
    Colombia DANE Configuration
    Source: DANE Open Data (Datos Abiertos) + DIAN customs microdata
    API: Socrata-style API with $limit/$offset
    """
    country_code: str = "COL"
    country_name: str = "Colombia"

    # API URLs
    base_url: str = "https://www.datos.gov.co"

    # Dataset IDs (these may need to be updated periodically)
    export_dataset_id: str = "XXX_EXPORT_DATASET_ID"  # Replace with actual ID
    import_dataset_id: str = "XXX_IMPORT_DATASET_ID"  # Replace with actual ID

    # Socrata API endpoints
    export_endpoint: str = "https://www.datos.gov.co/resource/{dataset_id}.json"
    import_endpoint: str = "https://www.datos.gov.co/resource/{dataset_id}.json"

    # Alternative: DANE direct portal
    dane_portal_url: str = "https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional"

    # Socrata API configuration
    socrata_app_token: str = "XXX_SOCRATA_APP_TOKEN"  # Optional, improves rate limits

    # Query parameters (Socrata SoQL)
    soql_select: str = "$select=ncm,pais,year,month,kg_neto,fob_usd,cif_usd"
    soql_where_template: str = "$where=year={year} AND month={month}"
    soql_limit: str = "$limit"
    soql_offset: str = "$offset"

    # Pagination
    page_size: int = 50000
    uses_cursor: bool = False

    # Rate limiting
    rate_limit_per_minute: int = 60
    rate_limit_with_token: int = 240  # Higher with app token

    # Field mappings
    field_mappings: Dict[str, str] = field(default_factory=lambda: {
        "ncm": "hs_code",
        "descripcion": "description",
        "pais": "partner_country",
        "year": "year",
        "month": "month",
        "kg_neto": "quantity_kg",
        "fob_usd": "value_fob_usd",
        "cif_usd": "value_cif_usd",
        "departamento": "state",
        "aduana": "customs_office",
    })

    # Release schedule
    release_day_of_month: int = 15  # Mid-month
    release_lag_months: int = 1

    # Bulletin URL for validation
    bulletin_url: str = "https://www.dane.gov.co/index.php/estadisticas-por-tema/comercio-internacional/exportaciones"


# =============================================================================
# URUGUAY - DNA CONFIGURATION
# =============================================================================

@dataclass
class UruguayConfig(CountryConfig):
    """
    Uruguay DNA (Direccion Nacional de Aduanas) Configuration
    Source: Uruguay Open Data (datos.gub.uy) - CKAN API
    """
    country_code: str = "URY"
    country_name: str = "Uruguay"

    # CKAN API URLs
    base_url: str = "https://catalogodatos.gub.uy"
    ckan_api_base: str = "https://catalogodatos.gub.uy/api/3/action"

    # Resource IDs for trade data (need to be discovered/updated)
    export_resource_id: str = "XXX_EXPORT_RESOURCE_ID"  # Replace with actual ID
    import_resource_id: str = "XXX_IMPORT_RESOURCE_ID"  # Replace with actual ID

    # CKAN API endpoints
    datastore_search: str = "/datastore_search"
    package_show: str = "/package_show"
    resource_show: str = "/resource_show"

    # Full endpoint template
    datastore_endpoint: str = "https://catalogodatos.gub.uy/api/3/action/datastore_search"

    # CKAN query parameters
    ckan_params: Dict[str, Any] = field(default_factory=lambda: {
        "resource_id": "",  # Set per request
        "limit": 1000,
        "offset": 0,
        "filters": {},  # year, month filters
    })

    # Pagination
    page_size: int = 1000
    uses_offset: bool = True

    # Field mappings
    field_mappings: Dict[str, str] = field(default_factory=lambda: {
        "NCM": "hs_code",
        "descripcion": "description",
        "pais": "partner_country",
        "anio": "year",
        "mes": "month",
        "peso_kg": "quantity_kg",
        "fob_usd": "value_fob_usd",
        "cif_usd": "value_cif_usd",
        "regimen": "regime",
    })

    # Alternative: Direct CSV downloads if API unavailable
    csv_download_pattern: str = "https://catalogodatos.gub.uy/dataset/{dataset_name}/resource/{resource_id}/download"

    # Release schedule
    release_day_of_month: int = 15
    release_lag_months: int = 1


# =============================================================================
# PARAGUAY - DNA/WITS FALLBACK CONFIGURATION
# =============================================================================

@dataclass
class ParaguayConfig(CountryConfig):
    """
    Paraguay Configuration
    Primary: DNA Paraguay (limited structured data)
    Fallback: WITS/UN Comtrade API
    """
    country_code: str = "PRY"
    country_name: str = "Paraguay"

    # Paraguay DNA URLs (limited availability)
    dna_base_url: str = "https://www.aduana.gov.py"
    dna_stats_url: str = "https://www.aduana.gov.py/estadisticas.html"

    # BCP (Central Bank) - aggregated data
    bcp_base_url: str = "https://www.bcp.gov.py"
    bcp_trade_url: str = "https://www.bcp.gov.py/webapps/web/comercio-exterior"

    # Primary source: Try DNA Paraguay first
    use_dna_primary: bool = True

    # PDF/Excel scraping patterns (if structured files become available)
    bulletin_patterns: List[str] = field(default_factory=lambda: [
        "https://www.aduana.gov.py/uploads/estadisticas/boletin_{year}_{month:02d}.xlsx",
        "https://www.aduana.gov.py/uploads/estadisticas/comercio_exterior_{year}_{month:02d}.pdf",
    ])

    # WITS Fallback Configuration
    wits_enabled: bool = True
    wits_base_url: str = "https://wits.worldbank.org/API/V1"
    wits_reporter_code: str = "600"  # Paraguay's WITS code

    # WITS API endpoint templates
    wits_endpoint_template: str = "https://wits.worldbank.org/API/V1/commodity/{flow}/{reporter}/{partner}/{product}"

    # WITS parameters
    wits_params: Dict[str, Any] = field(default_factory=lambda: {
        "reporter": "600",  # Paraguay
        "partner": "all",
        "product": "TOTAL",  # or HS chapter/4/6 digits
        "flow": "export",  # or "import"
    })

    # WITS pagination
    wits_page_param: str = "page"
    wits_max_param: str = "max"
    wits_page_size: int = 50000

    # Field mappings for WITS
    wits_field_mappings: Dict[str, str] = field(default_factory=lambda: {
        "Reporter": "reporter_country",
        "Partner": "partner_country",
        "Product": "hs_code",
        "ProductDescription": "description",
        "Year": "year",
        "TradeValue": "value_usd",
        "Quantity": "quantity",
        "QuantityUnit": "unit",
    })

    # UN Comtrade as secondary fallback
    comtrade_enabled: bool = True
    comtrade_api_url: str = "https://comtradeapi.un.org/data/v1/get"
    comtrade_api_key: str = "XXX_COMTRADE_API_KEY"  # Free tier available

    # Release schedule - WITS has 1-2 month lag
    release_lag_months: int = 2


# =============================================================================
# ISO COUNTRY CODES AND MAPPINGS
# =============================================================================

ISO_COUNTRY_CODES: Dict[str, Dict[str, str]] = {
    "ARG": {"iso2": "AR", "name": "Argentina", "numeric": "032"},
    "BOL": {"iso2": "BO", "name": "Bolivia", "numeric": "068"},
    "BRA": {"iso2": "BR", "name": "Brazil", "numeric": "076"},
    "CHL": {"iso2": "CL", "name": "Chile", "numeric": "152"},
    "COL": {"iso2": "CO", "name": "Colombia", "numeric": "170"},
    "ECU": {"iso2": "EC", "name": "Ecuador", "numeric": "218"},
    "GUY": {"iso2": "GY", "name": "Guyana", "numeric": "328"},
    "PRY": {"iso2": "PY", "name": "Paraguay", "numeric": "600"},
    "PER": {"iso2": "PE", "name": "Peru", "numeric": "604"},
    "SUR": {"iso2": "SR", "name": "Suriname", "numeric": "740"},
    "URY": {"iso2": "UY", "name": "Uruguay", "numeric": "858"},
    "VEN": {"iso2": "VE", "name": "Venezuela", "numeric": "862"},
}

# MERCOSUR members for regional analysis
MERCOSUR_MEMBERS = ["ARG", "BRA", "PRY", "URY"]
MERCOSUR_ASSOCIATES = ["BOL", "CHL", "COL", "ECU", "GUY", "PER", "SUR"]


# =============================================================================
# HS CODE CONFIGURATION
# =============================================================================

@dataclass
class HSCodeConfig:
    """HS Code classification configuration"""
    # Standard HS levels for harmonization
    harmonized_level: int = 6  # HS6 for cross-country comparison

    # Keep native levels as detail
    native_levels: Dict[str, int] = field(default_factory=lambda: {
        "ARG": 10,  # Argentina NCM
        "BRA": 8,   # Brazil NCM
        "COL": 10,  # Colombia
        "URY": 10,  # Uruguay NCM
        "PRY": 6,   # Paraguay (from WITS)
    })

    # Key commodity chapters for agriculture/commodities
    key_chapters: Dict[str, str] = field(default_factory=lambda: {
        "10": "Cereals",
        "12": "Oil seeds and oleaginous fruits",
        "15": "Animal or vegetable fats and oils",
        "17": "Sugars and sugar confectionery",
        "23": "Residues from food industries",
        "02": "Meat and edible meat offal",
        "04": "Dairy produce; eggs; honey",
        "03": "Fish and crustaceans",
        "08": "Edible fruit and nuts",
        "07": "Edible vegetables",
    })

    # Specific commodities of interest (HS6 level)
    target_commodities: Dict[str, str] = field(default_factory=lambda: {
        "100190": "Wheat",
        "100590": "Maize (corn)",
        "120190": "Soybeans",
        "120810": "Soybean flour and meal",
        "150710": "Soybean oil, crude",
        "230400": "Soybean oil-cake",
        "020230": "Beef, boneless frozen",
        "170111": "Cane sugar, raw",
        "270900": "Petroleum oils, crude",
    })


# =============================================================================
# QUALITY AND VALIDATION CONFIGURATION
# =============================================================================

@dataclass
class QualityConfig:
    """Data quality validation configuration"""
    # Deviation thresholds
    monthly_deviation_threshold_pct: float = 20.0  # Flag if >20% from 12-month mean
    yoy_deviation_threshold_pct: float = 50.0  # Year-over-year

    # Z-score threshold for outlier detection
    zscore_threshold: float = 3.0

    # Minimum sample size for statistics
    min_trailing_months: int = 12

    # Duplicate detection
    unique_keys: List[str] = field(default_factory=lambda: [
        "reporter", "flow", "period", "hs_code", "partner"
    ])

    # Completeness checks
    required_fields: List[str] = field(default_factory=lambda: [
        "reporter", "flow", "period", "hs_code", "partner_country",
        "value_usd", "quantity_kg"
    ])

    # Value ranges
    min_value_usd: float = 0.0
    max_value_usd: float = 1e12  # $1 trillion max per record
    min_quantity_kg: float = 0.0
    max_quantity_kg: float = 1e12


# =============================================================================
# MAIN CONFIGURATION
# =============================================================================

@dataclass
class SouthAmericaTradeConfig:
    """Main configuration for South America Trade Data Pipeline"""

    # Pipeline identification
    pipeline_name: str = "SouthAmericaTradeDataPipeline"
    pipeline_version: str = "1.0.0"

    # Logging
    log_level: str = "INFO"
    log_directory: Path = field(default_factory=lambda: Path("./logs"))

    # Database
    database: DatabaseConfig = field(default_factory=DatabaseConfig)

    # Country-specific configurations
    argentina: ArgentinaConfig = field(default_factory=ArgentinaConfig)
    brazil: BrazilConfig = field(default_factory=BrazilConfig)
    colombia: ColombiaConfig = field(default_factory=ColombiaConfig)
    uruguay: UruguayConfig = field(default_factory=UruguayConfig)
    paraguay: ParaguayConfig = field(default_factory=ParaguayConfig)

    # HS Code configuration
    hs_codes: HSCodeConfig = field(default_factory=HSCodeConfig)

    # Quality validation
    quality: QualityConfig = field(default_factory=QualityConfig)

    # Scheduling
    default_schedule_day: int = 15  # Day of month
    schedule_timezone: str = "America/Sao_Paulo"  # Use Brazil time as reference

    # Processing
    batch_size: int = 10000
    parallel_workers: int = 4

    # Data directories
    data_directory: Path = field(default_factory=lambda: Path("./data"))
    raw_data_directory: Path = field(default_factory=lambda: Path("./data/raw"))
    processed_data_directory: Path = field(default_factory=lambda: Path("./data/processed"))

    # Metadata logging
    log_requests: bool = True
    log_file_hashes: bool = True
    store_raw_responses: bool = True

    def get_country_config(self, country_code: str) -> CountryConfig:
        """Get configuration for a specific country"""
        configs = {
            "ARG": self.argentina,
            "BRA": self.brazil,
            "COL": self.colombia,
            "URY": self.uruguay,
            "PRY": self.paraguay,
        }
        return configs.get(country_code.upper())

    def get_enabled_countries(self) -> List[str]:
        """Get list of enabled country codes"""
        enabled = []
        for code, config in [
            ("ARG", self.argentina),
            ("BRA", self.brazil),
            ("COL", self.colombia),
            ("URY", self.uruguay),
            ("PRY", self.paraguay),
        ]:
            if config.enabled:
                enabled.append(code)
        return enabled

    @classmethod
    def from_environment(cls) -> "SouthAmericaTradeConfig":
        """Create configuration from environment variables"""
        config = cls()

        # Database settings from environment
        db_type = os.getenv("SA_TRADE_DB_TYPE", "sqlite")
        config.database.db_type = DatabaseType(db_type.lower())
        config.database.host = os.getenv("SA_TRADE_DB_HOST", "localhost")
        config.database.port = int(os.getenv("SA_TRADE_DB_PORT", "3306"))
        config.database.database = os.getenv("SA_TRADE_DB_NAME", "south_america_trade")
        config.database.username = os.getenv("SA_TRADE_DB_USER", "")
        config.database.password = os.getenv("SA_TRADE_DB_PASSWORD", "")

        # API keys from environment
        config.colombia.socrata_app_token = os.getenv("SOCRATA_APP_TOKEN", "XXX_SOCRATA_APP_TOKEN")
        config.paraguay.comtrade_api_key = os.getenv("COMTRADE_API_KEY", "XXX_COMTRADE_API_KEY")

        # Logging from environment
        config.log_level = os.getenv("SA_TRADE_LOG_LEVEL", "INFO")

        return config


# =============================================================================
# DEFAULT CONFIGURATION INSTANCE
# =============================================================================

default_config = SouthAmericaTradeConfig()


# =============================================================================
# SAMPLE API QUERIES (Reference)
# =============================================================================

SAMPLE_QUERIES = {
    "brazil_export": {
        "description": "Brazil monthly exports at NCM 8-digit level",
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
    },
    "wits_paraguay_export": {
        "description": "Paraguay exports via WITS API",
        "url": "https://wits.worldbank.org/API/V1/commodity/export/600/all/TOTAL",
        "params": {
            "year": 2023,
            "page": 1,
            "max": 50000,
        },
    },
    "colombia_socrata": {
        "description": "Colombia exports via Socrata API",
        "url": "https://www.datos.gov.co/resource/{dataset_id}.json",
        "params": {
            "$select": "ncm,pais,year,month,kg_neto,fob_usd",
            "$where": "year=2024 AND month=8",
            "$limit": 50000,
            "$offset": 0,
        },
    },
    "uruguay_ckan": {
        "description": "Uruguay exports via CKAN API",
        "url": "https://catalogodatos.gub.uy/api/3/action/datastore_search",
        "params": {
            "resource_id": "{resource_id}",
            "limit": 1000,
            "offset": 0,
            "filters": '{"year": 2024, "month": 8}',
        },
    },
}
