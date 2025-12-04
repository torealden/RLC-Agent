"""
Commodity Database Configuration Settings

Comprehensive configuration for the RLC Commodity Database system.
Supports multiple database backends (SQLite, MySQL, PostgreSQL),
data source integrations, and agent pipeline settings.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime, time


# =============================================================================
# ENUMS
# =============================================================================

class DatabaseType(Enum):
    """Supported database types"""
    SQLITE = "sqlite"
    MYSQL = "mysql"
    POSTGRESQL = "postgresql"


class IngestionMode(Enum):
    """Data ingestion modes"""
    FULL = "full"           # Full reload
    INCREMENTAL = "incremental"  # Only new/changed data
    UPSERT = "upsert"       # Insert or update


class AlertChannel(Enum):
    """Notification channels for alerts"""
    EMAIL = "email"
    SLACK = "slack"
    LOG = "log"


class ApprovalLevel(Enum):
    """Autonomy levels for agent operations"""
    ALWAYS_ASK = 1      # Ask for approval on everything
    ASK_SCHEMA = 2      # Only ask for schema changes
    AUTONOMOUS = 3      # Proceed autonomously, just notify


# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

@dataclass
class DatabaseConfig:
    """
    Database connection configuration.

    Supports SQLite for development/testing and MySQL/PostgreSQL for production.
    Connection pooling and timeout settings included for production use.
    """
    db_type: DatabaseType = DatabaseType.SQLITE
    host: str = "localhost"
    port: int = 5432
    database: str = "commodity_data"
    username: str = ""
    password: str = ""

    # SQLite specific
    sqlite_path: Optional[Path] = None

    # Connection pool settings
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600  # Recycle connections after 1 hour

    # Connection options
    echo_sql: bool = False  # Log SQL statements
    connect_timeout: int = 10

    # SSL settings for cloud databases
    ssl_enabled: bool = False
    ssl_ca_cert: Optional[str] = None

    def get_connection_string(self) -> str:
        """Generate SQLAlchemy database connection string."""
        if self.db_type == DatabaseType.SQLITE:
            path = self.sqlite_path or Path("./data/commodity_data.db")
            # Ensure parent directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            return f"sqlite:///{path}"
        elif self.db_type == DatabaseType.MYSQL:
            return (
                f"mysql+pymysql://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
                f"?charset=utf8mb4"
            )
        elif self.db_type == DatabaseType.POSTGRESQL:
            ssl_mode = "?sslmode=require" if self.ssl_enabled else ""
            return (
                f"postgresql+psycopg2://{self.username}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
                f"{ssl_mode}"
            )
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def get_engine_kwargs(self) -> Dict[str, Any]:
        """Get SQLAlchemy engine keyword arguments."""
        kwargs = {
            "echo": self.echo_sql,
            "pool_pre_ping": True,  # Test connections before use
        }

        if self.db_type != DatabaseType.SQLITE:
            kwargs.update({
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
                "pool_timeout": self.pool_timeout,
                "pool_recycle": self.pool_recycle,
            })

        return kwargs

    def is_configured(self) -> bool:
        """Check if database is properly configured."""
        if self.db_type == DatabaseType.SQLITE:
            return True
        return bool(self.host and self.database and self.username)


# =============================================================================
# DATA SOURCE CONFIGURATION
# =============================================================================

@dataclass
class DropboxSourceConfig:
    """Configuration for Dropbox data sources (Excel spreadsheets)."""
    enabled: bool = True
    access_token: str = ""
    app_key: str = ""
    app_secret: str = ""
    refresh_token: str = ""

    # Paths within Dropbox
    root_folder: str = "/rlc documents"
    models_folder: str = "/rlc documents/Models"

    # File patterns for different data types
    supply_demand_patterns: Dict[str, str] = field(default_factory=lambda: {
        "corn": "Corn*.xlsx",
        "wheat": "Wheat*.xlsx",
        "soybeans": "Soybean*.xlsx",
        "fcoj": "FCOJ*.xlsx",
        "sweeteners": "Sweeteners*.xlsx",
        "sugar": "Sugar*.xlsx",
    })

    # Sheet name mappings
    supply_demand_sheet: str = "S&D"
    price_history_sheet: str = "Prices"

    # Download settings
    download_timeout: int = 120
    max_retry_attempts: int = 3
    retry_delay: float = 2.0


@dataclass
class APISourceConfig:
    """Configuration for API data sources."""
    enabled: bool = True

    # USDA API settings
    usda_base_url: str = "https://marsapi.ams.usda.gov/services/v1.2/reports"
    usda_api_key: str = ""
    usda_rate_limit: int = 30  # Requests per minute

    # USDA NASS QuickStats API
    nass_base_url: str = "https://quickstats.nass.usda.gov/api"
    nass_api_key: str = ""
    nass_rate_limit: int = 60

    # USDA FAS API (for trade data)
    fas_base_url: str = "https://apps.fas.usda.gov/psdonline/api"
    fas_rate_limit: int = 30

    # Internal API Manager
    api_manager_url: str = "http://localhost:8000"
    api_manager_key: str = ""

    # Request settings
    timeout: int = 60
    max_retries: int = 3
    retry_backoff_factor: float = 2.0


@dataclass
class DataSourcesConfig:
    """Combined configuration for all data sources."""
    dropbox: DropboxSourceConfig = field(default_factory=DropboxSourceConfig)
    api: APISourceConfig = field(default_factory=APISourceConfig)

    # Source priority (first available is used)
    price_source_priority: List[str] = field(default_factory=lambda: [
        "api_manager", "usda_ams", "internal_spreadsheet"
    ])
    fundamental_source_priority: List[str] = field(default_factory=lambda: [
        "usda_official", "internal_spreadsheet", "internal_estimate"
    ])


# =============================================================================
# COMMODITY CONFIGURATION
# =============================================================================

@dataclass
class CommodityDefinition:
    """Definition for a single commodity."""
    name: str
    symbol: Optional[str] = None
    category: str = "other"
    price_unit: str = "$/unit"
    quantity_unit: str = "units"
    marketing_year_start_month: Optional[int] = None
    marketing_year_start_day: int = 1
    bushel_weight_lbs: Optional[float] = None


# Default commodity definitions
DEFAULT_COMMODITIES: Dict[str, CommodityDefinition] = {
    "corn": CommodityDefinition(
        name="Corn",
        symbol="ZC",
        category="grain",
        price_unit="cents/bu",
        quantity_unit="million bushels",
        marketing_year_start_month=9,
        bushel_weight_lbs=56.0
    ),
    "wheat": CommodityDefinition(
        name="Wheat",
        symbol="ZW",
        category="grain",
        price_unit="cents/bu",
        quantity_unit="million bushels",
        marketing_year_start_month=6,
        bushel_weight_lbs=60.0
    ),
    "soybeans": CommodityDefinition(
        name="Soybeans",
        symbol="ZS",
        category="oilseed",
        price_unit="cents/bu",
        quantity_unit="million bushels",
        marketing_year_start_month=9,
        bushel_weight_lbs=60.0
    ),
    "soybean_meal": CommodityDefinition(
        name="Soybean Meal",
        symbol="ZM",
        category="oilseed",
        price_unit="$/ton",
        quantity_unit="thousand short tons",
        marketing_year_start_month=10
    ),
    "soybean_oil": CommodityDefinition(
        name="Soybean Oil",
        symbol="ZL",
        category="oilseed",
        price_unit="cents/lb",
        quantity_unit="million pounds",
        marketing_year_start_month=10
    ),
    "fcoj": CommodityDefinition(
        name="FCOJ",
        symbol="OJ",
        category="juice",
        price_unit="cents/lb",
        quantity_unit="thousand gallons",
        marketing_year_start_month=10
    ),
    "sugar_11": CommodityDefinition(
        name="Sugar #11 (World)",
        symbol="SB",
        category="sweetener",
        price_unit="cents/lb",
        quantity_unit="thousand metric tons",
        marketing_year_start_month=10
    ),
    "sugar_16": CommodityDefinition(
        name="Sugar #16 (US)",
        symbol="SF",
        category="sweetener",
        price_unit="cents/lb",
        quantity_unit="thousand short tons",
        marketing_year_start_month=10
    ),
    "hfcs_42": CommodityDefinition(
        name="HFCS-42",
        category="sweetener",
        price_unit="$/cwt",
        quantity_unit="thousand short tons dry weight",
        marketing_year_start_month=10
    ),
    "hfcs_55": CommodityDefinition(
        name="HFCS-55",
        category="sweetener",
        price_unit="$/cwt",
        quantity_unit="thousand short tons dry weight",
        marketing_year_start_month=10
    ),
}


@dataclass
class CommodityConfig:
    """Configuration for commodity metadata."""
    # Commodity definitions
    commodities: Dict[str, CommodityDefinition] = field(
        default_factory=lambda: DEFAULT_COMMODITIES.copy()
    )

    # Standard supply/demand fields by category
    supply_demand_fields: Dict[str, List[str]] = field(default_factory=lambda: {
        "supply": [
            "beginning_stocks",
            "production",
            "imports",
            "total_supply"
        ],
        "demand": [
            "domestic_use",
            "feed_use",
            "food_use",
            "ethanol_use",
            "crush",
            "exports",
            "total_use"
        ],
        "balance": [
            "ending_stocks",
            "stocks_to_use"
        ]
    })

    # Field aliases for normalization
    field_aliases: Dict[str, str] = field(default_factory=lambda: {
        "beg_stocks": "beginning_stocks",
        "end_stocks": "ending_stocks",
        "s/u": "stocks_to_use",
        "stks/use": "stocks_to_use",
        "dom_use": "domestic_use",
        "total_dom_use": "domestic_use",
        "total_exports": "exports",
        "prod": "production",
    })


# =============================================================================
# INGESTION CONFIGURATION
# =============================================================================

@dataclass
class IngestionConfig:
    """Configuration for data ingestion pipeline."""
    # Ingestion mode
    default_mode: IngestionMode = IngestionMode.UPSERT

    # Batch settings
    batch_size: int = 1000
    commit_frequency: int = 5000  # Commit every N records

    # Validation settings
    validate_before_insert: bool = True
    reject_on_validation_error: bool = False  # If False, log and skip

    # Duplicate handling
    update_on_duplicate: bool = True
    track_changes: bool = True  # Store previous values on update

    # Schema handling
    auto_add_commodities: bool = False  # Auto-add new commodities
    auto_add_fields: bool = False  # Auto-add new fundamental fields
    approval_level: ApprovalLevel = ApprovalLevel.ASK_SCHEMA

    # Error handling
    max_errors_before_abort: int = 100
    continue_on_error: bool = True

    # Scheduling
    price_fetch_schedule: str = "0 6 * * 1-5"  # 6 AM weekdays (cron format)
    fundamental_fetch_schedule: str = "0 7 10 * *"  # 7 AM on 10th of month


@dataclass
class ValidationConfig:
    """Configuration for data validation rules."""
    # Price validation
    max_price_change_pct: float = 50.0  # Flag if price changes > 50%
    min_valid_price: float = 0.01
    max_valid_price: float = 100000.0

    # Fundamental validation
    max_stocks_to_use: float = 200.0  # Flag if S/U > 200%
    min_stocks_to_use: float = 0.0

    # Crop progress validation
    min_progress_pct: float = 0.0
    max_progress_pct: float = 100.0

    # Trade flow validation
    max_trade_value: float = 1e12  # $1 trillion max

    # Statistical validation
    outlier_std_devs: float = 3.0  # Flag if > 3 std devs from mean
    min_data_points_for_stats: int = 10


# =============================================================================
# NOTIFICATION CONFIGURATION
# =============================================================================

@dataclass
class NotificationConfig:
    """Configuration for notifications and alerts."""
    enabled: bool = True
    default_channel: AlertChannel = AlertChannel.EMAIL

    # Email settings
    smtp_server: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    from_email: str = ""

    # Recipients by alert type
    schema_change_recipients: List[str] = field(default_factory=list)
    error_recipients: List[str] = field(default_factory=list)
    quality_alert_recipients: List[str] = field(default_factory=list)
    success_recipients: List[str] = field(default_factory=list)

    # Slack settings
    slack_webhook_url: str = ""
    slack_channel: str = "#commodity-data"

    # Alert thresholds
    min_severity_for_email: str = "warning"  # info, warning, error, critical
    min_severity_for_slack: str = "error"

    # Digest settings
    send_daily_digest: bool = True
    digest_time: time = field(default_factory=lambda: time(8, 0))


# =============================================================================
# VERIFICATION CONFIGURATION
# =============================================================================

@dataclass
class VerificationConfig:
    """Configuration for data verification agent."""
    enabled: bool = True

    # Verification types
    verify_record_counts: bool = True
    verify_checksums: bool = True
    verify_value_ranges: bool = True
    verify_statistical_distribution: bool = True

    # Sampling for large datasets
    sample_size: int = 1000  # Random sample size for verification
    sample_pct: float = 10.0  # Or use percentage

    # Comparison thresholds
    count_tolerance_pct: float = 1.0  # Allow 1% count difference
    value_tolerance_pct: float = 0.01  # Allow 0.01% value difference

    # Historical comparison
    compare_to_previous_load: bool = True
    flag_large_changes: bool = True
    large_change_threshold_pct: float = 25.0


# =============================================================================
# MAIN CONFIGURATION
# =============================================================================

@dataclass
class CommodityDatabaseConfig:
    """Main configuration for Commodity Database system."""

    # System identification
    system_name: str = "RLC Commodity Database"
    system_version: str = "1.0.0"

    # Logging
    log_level: str = "INFO"
    log_directory: Path = field(default_factory=lambda: Path("./logs"))
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Directories
    data_directory: Path = field(default_factory=lambda: Path("./data"))
    cache_directory: Path = field(default_factory=lambda: Path("./data/cache"))
    temp_directory: Path = field(default_factory=lambda: Path("./data/temp"))

    # Sub-configurations
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    data_sources: DataSourcesConfig = field(default_factory=DataSourcesConfig)
    commodities: CommodityConfig = field(default_factory=CommodityConfig)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)
    verification: VerificationConfig = field(default_factory=VerificationConfig)

    def is_database_ready(self) -> bool:
        """Check if database is configured and ready."""
        return self.database.is_configured()

    def is_dropbox_ready(self) -> bool:
        """Check if Dropbox is configured and ready."""
        if not self.data_sources.dropbox.enabled:
            return False
        dropbox = self.data_sources.dropbox
        return bool(dropbox.access_token or dropbox.refresh_token)

    def is_api_ready(self) -> bool:
        """Check if API sources are configured."""
        api = self.data_sources.api
        return bool(api.usda_api_key or api.api_manager_key)

    def ensure_directories(self):
        """Create required directories if they don't exist."""
        for directory in [
            self.data_directory,
            self.cache_directory,
            self.temp_directory,
            self.log_directory
        ]:
            directory.mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_environment(cls) -> "CommodityDatabaseConfig":
        """Create configuration from environment variables."""
        config = cls()

        # System settings
        config.log_level = os.getenv("COMMODITY_DB_LOG_LEVEL", "INFO")

        # Database settings
        db_type = os.getenv("COMMODITY_DB_TYPE", "sqlite")
        config.database.db_type = DatabaseType(db_type.lower())
        config.database.host = os.getenv("COMMODITY_DB_HOST", "localhost")
        config.database.port = int(os.getenv("COMMODITY_DB_PORT", "5432"))
        config.database.database = os.getenv("COMMODITY_DB_NAME", "commodity_data")
        config.database.username = os.getenv("COMMODITY_DB_USER", "")
        config.database.password = os.getenv("COMMODITY_DB_PASSWORD", "")
        config.database.echo_sql = os.getenv("COMMODITY_DB_ECHO", "false").lower() == "true"
        config.database.ssl_enabled = os.getenv("COMMODITY_DB_SSL", "false").lower() == "true"

        # SQLite path
        sqlite_path = os.getenv("COMMODITY_DB_SQLITE_PATH")
        if sqlite_path:
            config.database.sqlite_path = Path(sqlite_path)

        # Dropbox settings
        config.data_sources.dropbox.access_token = os.getenv("DROPBOX_ACCESS_TOKEN", "")
        config.data_sources.dropbox.app_key = os.getenv("DROPBOX_APP_KEY", "")
        config.data_sources.dropbox.app_secret = os.getenv("DROPBOX_APP_SECRET", "")
        config.data_sources.dropbox.refresh_token = os.getenv("DROPBOX_REFRESH_TOKEN", "")

        # API settings
        config.data_sources.api.usda_api_key = os.getenv("USDA_API_KEY", "")
        config.data_sources.api.nass_api_key = os.getenv("NASS_API_KEY", "")
        config.data_sources.api.api_manager_url = os.getenv("API_MANAGER_URL", "http://localhost:8000")
        config.data_sources.api.api_manager_key = os.getenv("API_MANAGER_KEY", "")

        # Ingestion settings
        approval = os.getenv("COMMODITY_DB_APPROVAL_LEVEL", "2")
        config.ingestion.approval_level = ApprovalLevel(int(approval))
        config.ingestion.auto_add_commodities = os.getenv(
            "COMMODITY_DB_AUTO_ADD_COMMODITIES", "false"
        ).lower() == "true"

        # Notification settings
        config.notifications.smtp_server = os.getenv("SMTP_SERVER", "")
        config.notifications.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        config.notifications.smtp_username = os.getenv("SMTP_USER", "")
        config.notifications.smtp_password = os.getenv("SMTP_PASSWORD", "")
        config.notifications.from_email = os.getenv("NOTIFICATION_FROM", "")
        config.notifications.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")

        # Parse comma-separated recipients
        for env_var, recipient_list in [
            ("SCHEMA_CHANGE_RECIPIENTS", config.notifications.schema_change_recipients),
            ("ERROR_RECIPIENTS", config.notifications.error_recipients),
            ("QUALITY_ALERT_RECIPIENTS", config.notifications.quality_alert_recipients),
        ]:
            recipients = os.getenv(env_var, "")
            if recipients:
                recipient_list.extend([r.strip() for r in recipients.split(",")])

        return config


# =============================================================================
# DEFAULT CONFIGURATION INSTANCE
# =============================================================================

default_config = CommodityDatabaseConfig()


# =============================================================================
# FIELD CATEGORY MAPPINGS
# =============================================================================

# Standard supply/demand field mappings for data normalization
SUPPLY_DEMAND_FIELD_CATEGORIES = {
    # Supply fields
    "beginning_stocks": "supply",
    "production": "supply",
    "imports": "supply",
    "total_supply": "supply",

    # Demand fields
    "domestic_use": "demand",
    "feed_use": "demand",
    "food_use": "demand",
    "seed_use": "demand",
    "residual_use": "demand",
    "ethanol_use": "demand",
    "crush": "demand",
    "exports": "demand",
    "total_use": "demand",

    # Balance fields
    "ending_stocks": "balance",
    "stocks_to_use": "balance",
    "stocks_to_use_pct": "balance",
}

# Crop progress field mappings
CROP_PROGRESS_FIELDS = {
    "progress": [
        "pct_planted",
        "pct_emerged",
        "pct_silking",
        "pct_blooming",
        "pct_setting_pods",
        "pct_dough",
        "pct_dented",
        "pct_mature",
        "pct_harvested"
    ],
    "condition": [
        "pct_very_poor",
        "pct_poor",
        "pct_fair",
        "pct_good",
        "pct_excellent",
        "pct_good_excellent",
        "condition_index"
    ]
}
