"""
HB Weekly Report Writer Configuration Settings

Comprehensive configuration for the HigbyBarrett Weekly Report generation agent.
Supports multiple data sources (Dropbox, Database, APIs) and flexible output options.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime, time


class DatabaseType(Enum):
    """Supported database types for internal data"""
    SQLITE = "sqlite"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"


class DataSourceType(Enum):
    """Data source types"""
    DROPBOX = "dropbox"
    DATABASE = "database"


class OutputFormat(Enum):
    """Supported output formats"""
    DOCX = "docx"
    PDF = "pdf"
    HTML = "html"


# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

@dataclass
class DatabaseConfig:
    """Database connection configuration for future database support"""
    db_type: DatabaseType = DatabaseType.SQLITE
    host: str = "localhost"
    port: int = 5432
    database: str = "hb_market_data"
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
            path = self.sqlite_path or Path("./data/hb_market_data.db")
            return f"sqlite:///{path}"
        elif self.db_type == DatabaseType.MYSQL:
            return f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.db_type == DatabaseType.POSTGRESQL:
            return f"postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")


# =============================================================================
# DROPBOX CONFIGURATION
# =============================================================================

@dataclass
class DropboxConfig:
    """Dropbox integration configuration for internal spreadsheet data"""
    enabled: bool = True
    access_token: str = ""  # OAuth2 token
    app_key: str = ""
    app_secret: str = ""
    refresh_token: str = ""

    # Paths within Dropbox
    root_folder: str = "/rlc documents"
    data_folder: str = "/rlc documents/llm model and documents/data"
    reports_folder: str = "/rlc documents/llm model and documents/reports"

    # File patterns
    hb_spreadsheet_pattern: str = "HB Weekly Data*.xlsx"
    hb_spreadsheet_name: str = "HB Weekly Data.xlsx"

    # Sheet names and ranges
    supply_demand_sheet: str = "Supply_Demand"
    price_data_sheet: str = "Price_Data"
    forecast_sheet: str = "Forecasts"

    # Download settings
    download_timeout: int = 60
    max_retry_attempts: int = 3
    retry_delay: float = 2.0


# =============================================================================
# API MANAGER CONFIGURATION
# =============================================================================

@dataclass
class APIManagerConfig:
    """Configuration for connecting to the API Manager agent"""
    enabled: bool = True
    base_url: str = "http://localhost:8000"  # API Manager service URL
    api_key: str = ""
    timeout: int = 30

    # Price series configuration file
    price_series_config: Path = field(default_factory=lambda: Path("./config/price_series.json"))

    # Default price series to fetch
    default_series: List[str] = field(default_factory=lambda: [
        "corn_front_month",
        "wheat_hrw_front_month",
        "wheat_srw_front_month",
        "soybeans_front_month",
        "soybean_meal_front_month",
        "soybean_oil_front_month",
        "corn_dec_mar_spread",
        "soybean_jan_mar_spread",
        "gulf_corn_fob",
        "gulf_soybean_fob",
        "brazil_soybean_fob",
    ])

    # Historical lookback
    week_ago_days: int = 7
    year_ago_days: int = 365

    # Rate limiting
    rate_limit_per_minute: int = 60

    # USDA AMS direct access fallback
    usda_ams_base_url: str = "https://marsapi.ams.usda.gov/services/v1.2/reports"
    usda_api_key: str = ""


# =============================================================================
# COMMODITY CONFIGURATION
# =============================================================================

@dataclass
class CommodityConfig:
    """Configuration for commodity analysis"""
    # Primary commodities for deep dive analysis
    primary_commodities: List[str] = field(default_factory=lambda: [
        "corn",
        "wheat",
        "soybeans",
        "soybean_meal",
        "soybean_oil",
    ])

    # Minimum factors required per commodity
    min_bullish_factors: int = 2
    min_bearish_factors: int = 2
    min_swing_factors: int = 2

    # Marketing year definitions (US)
    marketing_years: Dict[str, Dict[str, int]] = field(default_factory=lambda: {
        "corn": {"start_month": 9, "start_day": 1},  # Sep 1
        "soybeans": {"start_month": 9, "start_day": 1},  # Sep 1
        "wheat": {"start_month": 6, "start_day": 1},  # Jun 1
        "soybean_meal": {"start_month": 10, "start_day": 1},  # Oct 1
        "soybean_oil": {"start_month": 10, "start_day": 1},  # Oct 1
    })

    # Unit configurations
    units: Dict[str, str] = field(default_factory=lambda: {
        "corn": "bu",
        "wheat": "bu",
        "soybeans": "bu",
        "soybean_meal": "st",  # short tons
        "soybean_oil": "lb",
    })

    # Spread calculation assumptions
    storage_cost_per_month: float = 0.05  # $/bu/month
    interest_rate_annual: float = 0.07  # 7% for carry calculations

    # Key data fields from internal spreadsheet
    supply_demand_fields: Dict[str, List[str]] = field(default_factory=lambda: {
        "corn": [
            "production", "beginning_stocks", "total_supply",
            "feed_domestic", "ethanol", "exports", "total_use",
            "ending_stocks", "stocks_to_use"
        ],
        "wheat": [
            "production", "beginning_stocks", "imports", "total_supply",
            "food", "feed_seed", "exports", "total_use",
            "ending_stocks", "stocks_to_use"
        ],
        "soybeans": [
            "production", "beginning_stocks", "total_supply",
            "crush", "exports", "seed_residual", "total_use",
            "ending_stocks", "stocks_to_use"
        ],
        "soybean_meal": [
            "production", "beginning_stocks", "imports", "total_supply",
            "domestic_use", "exports", "total_use",
            "ending_stocks"
        ],
        "soybean_oil": [
            "production", "beginning_stocks", "imports", "total_supply",
            "domestic_use", "biodiesel", "exports", "total_use",
            "ending_stocks"
        ],
    })


# =============================================================================
# OUTPUT CONFIGURATION
# =============================================================================

@dataclass
class OutputConfig:
    """Configuration for report output"""
    # Output format
    primary_format: OutputFormat = OutputFormat.DOCX
    secondary_formats: List[OutputFormat] = field(default_factory=list)

    # Output directories
    output_directory: Path = field(default_factory=lambda: Path("./reports"))
    template_directory: Path = field(default_factory=lambda: Path("./templates"))

    # File naming
    filename_pattern: str = "HigbyBarrett Weekly Report {month} {day}, {year}"

    # Document settings
    template_file: Optional[str] = "hb_report_template.docx"

    # Styling
    heading1_font_size: int = 16
    heading2_font_size: int = 14
    body_font_size: int = 11
    font_family: str = "Calibri"

    # Table formatting
    table_header_color: str = "#4472C4"  # Blue header
    table_alt_row_color: str = "#D9E2F3"  # Light blue alternating
    price_decimal_places: int = 2
    percentage_decimal_places: int = 1

    # Metadata
    include_metadata: bool = True
    author: str = "HB Report Writer Agent"
    company: str = "HigbyBarrett"


# =============================================================================
# SCHEDULING CONFIGURATION
# =============================================================================

@dataclass
class SchedulingConfig:
    """Configuration for automated scheduling"""
    enabled: bool = True

    # Primary schedule - Tuesday morning
    day_of_week: int = 1  # Monday=0, Tuesday=1, ...
    execution_time: time = field(default_factory=lambda: time(6, 0))  # 6:00 AM
    timezone: str = "America/Chicago"  # Central Time

    # Fallback schedule if Tuesday data not ready
    retry_delay_hours: int = 2
    max_retries: int = 3

    # Data readiness checks
    wait_for_internal_data: bool = True
    internal_data_cutoff_hour: int = 7  # Wait until 7 AM for internal data

    # Use previous day prices if current not available
    allow_previous_day_prices: bool = True
    price_data_cutoff_hour: int = 8


# =============================================================================
# NOTIFICATION CONFIGURATION
# =============================================================================

@dataclass
class NotificationConfig:
    """Configuration for notifications and escalations"""
    enabled: bool = True

    # Email notifications
    smtp_server: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    from_email: str = ""

    # Recipients
    report_complete_recipients: List[str] = field(default_factory=list)
    error_recipients: List[str] = field(default_factory=list)

    # Question escalation
    shared_inbox_email: str = ""
    question_wait_hours: float = 2.0  # Wait 2 hours for answers

    # Slack integration (optional)
    slack_webhook_url: str = ""
    slack_channel: str = "#hb-reports"


# =============================================================================
# LLM CONFIGURATION
# =============================================================================

@dataclass
class LLMConfig:
    """Configuration for LLM-based analysis and writing"""
    enabled: bool = True
    provider: str = "openai"  # openai, anthropic, ollama
    model: str = "gpt-4"
    api_key: str = ""

    # Ollama settings
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama2:13b"

    # Generation parameters
    temperature: float = 0.3  # Lower for more consistent output
    max_tokens: int = 4000

    # Style prompts
    style_reference: str = """
    Write in a professional, analytical tone suitable for agricultural commodity market reports.
    Use industry-standard terminology and units (bushels, metric tons, etc.).
    Be concise but thorough. Avoid speculation without data support.
    Reference specific data points when making assertions.
    Balance bullish and bearish factors objectively.
    """


# =============================================================================
# MAIN CONFIGURATION
# =============================================================================

@dataclass
class HBWeeklyReportConfig:
    """Main configuration for HB Weekly Report Writer Agent"""

    # Agent identification
    agent_name: str = "HB Weekly Report Writer"
    agent_version: str = "1.0.0"

    # Logging
    log_level: str = "INFO"
    log_directory: Path = field(default_factory=lambda: Path("./logs"))

    # Data source preference
    internal_data_source: DataSourceType = DataSourceType.DROPBOX

    # Sub-configurations
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    dropbox: DropboxConfig = field(default_factory=DropboxConfig)
    api_manager: APIManagerConfig = field(default_factory=APIManagerConfig)
    commodities: CommodityConfig = field(default_factory=CommodityConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    scheduling: SchedulingConfig = field(default_factory=SchedulingConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)

    # Error handling
    question_wait_timeout_hours: float = 2.0
    max_placeholder_allowed: int = 5  # Max placeholders before failing
    fail_on_missing_critical_data: bool = False

    # Data directories
    data_directory: Path = field(default_factory=lambda: Path("./data"))
    cache_directory: Path = field(default_factory=lambda: Path("./data/cache"))

    def get_data_source(self) -> DataSourceType:
        """Get current data source configuration"""
        return self.internal_data_source

    def is_database_ready(self) -> bool:
        """Check if database is configured and ready"""
        if self.internal_data_source != DataSourceType.DATABASE:
            return False
        return bool(self.database.host and self.database.database)

    def is_dropbox_ready(self) -> bool:
        """Check if Dropbox is configured and ready"""
        if not self.dropbox.enabled:
            return False
        return bool(self.dropbox.access_token or self.dropbox.refresh_token)

    @classmethod
    def from_environment(cls) -> "HBWeeklyReportConfig":
        """Create configuration from environment variables"""
        config = cls()

        # Agent settings
        config.log_level = os.getenv("HB_LOG_LEVEL", "INFO")

        # Data source
        data_source = os.getenv("HB_DATA_SOURCE", "dropbox").lower()
        config.internal_data_source = DataSourceType(data_source)

        # Database settings
        db_type = os.getenv("HB_DB_TYPE", "postgresql")
        config.database.db_type = DatabaseType(db_type.lower())
        config.database.host = os.getenv("HB_DB_HOST", "localhost")
        config.database.port = int(os.getenv("HB_DB_PORT", "5432"))
        config.database.database = os.getenv("HB_DB_NAME", "hb_market_data")
        config.database.username = os.getenv("HB_DB_USER", "")
        config.database.password = os.getenv("HB_DB_PASSWORD", "")

        # Dropbox settings
        config.dropbox.access_token = os.getenv("DROPBOX_ACCESS_TOKEN", "")
        config.dropbox.app_key = os.getenv("DROPBOX_APP_KEY", "")
        config.dropbox.app_secret = os.getenv("DROPBOX_APP_SECRET", "")
        config.dropbox.refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN", "")

        # API Manager settings
        config.api_manager.base_url = os.getenv("API_MANAGER_URL", "http://localhost:8000")
        config.api_manager.api_key = os.getenv("API_MANAGER_KEY", "")
        config.api_manager.usda_api_key = os.getenv("USDA_API_KEY", "")

        # LLM settings
        config.llm.provider = os.getenv("LLM_PROVIDER", "openai")
        config.llm.model = os.getenv("LLM_MODEL", "gpt-4")
        config.llm.api_key = os.getenv("OPENAI_API_KEY", "")
        config.llm.ollama_base_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        config.llm.ollama_model = os.getenv("OLLAMA_MODEL", "llama2:13b")

        # Notification settings
        config.notifications.smtp_server = os.getenv("SMTP_SERVER", "")
        config.notifications.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        config.notifications.smtp_username = os.getenv("SMTP_USER", "")
        config.notifications.smtp_password = os.getenv("SMTP_PASSWORD", "")
        config.notifications.from_email = os.getenv("NOTIFICATION_FROM", "")
        config.notifications.shared_inbox_email = os.getenv("SHARED_INBOX_EMAIL", "")
        config.notifications.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")

        # Recipients from comma-separated env vars
        report_recipients = os.getenv("REPORT_RECIPIENTS", "")
        if report_recipients:
            config.notifications.report_complete_recipients = [
                r.strip() for r in report_recipients.split(",")
            ]

        error_recipients = os.getenv("ERROR_RECIPIENTS", "")
        if error_recipients:
            config.notifications.error_recipients = [
                r.strip() for r in error_recipients.split(",")
            ]

        return config


# =============================================================================
# DEFAULT CONFIGURATION INSTANCE
# =============================================================================

default_config = HBWeeklyReportConfig()


# =============================================================================
# PRICE SERIES DEFINITIONS
# =============================================================================

PRICE_SERIES_DEFINITIONS = {
    "corn_front_month": {
        "name": "Corn Front Month",
        "exchange": "CME",
        "contract": "ZC",
        "unit": "cents/bu",
        "decimals": 2,
    },
    "wheat_hrw_front_month": {
        "name": "Hard Red Winter Wheat Front Month",
        "exchange": "CME",
        "contract": "KE",
        "unit": "cents/bu",
        "decimals": 2,
    },
    "wheat_srw_front_month": {
        "name": "Soft Red Winter Wheat Front Month",
        "exchange": "CME",
        "contract": "ZW",
        "unit": "cents/bu",
        "decimals": 2,
    },
    "soybeans_front_month": {
        "name": "Soybeans Front Month",
        "exchange": "CME",
        "contract": "ZS",
        "unit": "cents/bu",
        "decimals": 2,
    },
    "soybean_meal_front_month": {
        "name": "Soybean Meal Front Month",
        "exchange": "CME",
        "contract": "ZM",
        "unit": "$/ton",
        "decimals": 1,
    },
    "soybean_oil_front_month": {
        "name": "Soybean Oil Front Month",
        "exchange": "CME",
        "contract": "ZL",
        "unit": "cents/lb",
        "decimals": 2,
    },
    "corn_dec_mar_spread": {
        "name": "Corn Dec-Mar Spread",
        "exchange": "CME",
        "type": "spread",
        "unit": "cents/bu",
        "decimals": 2,
    },
    "soybean_jan_mar_spread": {
        "name": "Soybean Jan-Mar Spread",
        "exchange": "CME",
        "type": "spread",
        "unit": "cents/bu",
        "decimals": 2,
    },
    "gulf_corn_fob": {
        "name": "Gulf Corn FOB",
        "source": "USDA AMS",
        "unit": "$/bu",
        "decimals": 2,
    },
    "gulf_soybean_fob": {
        "name": "Gulf Soybean FOB",
        "source": "USDA AMS",
        "unit": "$/bu",
        "decimals": 2,
    },
    "brazil_soybean_fob": {
        "name": "Brazil Soybean FOB Paranagua",
        "source": "USDA AMS",
        "unit": "$/mt",
        "decimals": 2,
    },
}
