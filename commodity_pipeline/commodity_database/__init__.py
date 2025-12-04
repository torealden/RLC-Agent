"""
RLC Commodity Database

A comprehensive database system for storing and managing commodity price series,
fundamental supply/demand data, crop progress information, and trade flows.

Components:
- database/: SQLAlchemy models and database utilities
- config/: Configuration classes and settings
- agents/: DatabaseAgent and VerificationAgent for data operations
- services/: Pipeline orchestrator and schedulers
- utils/: Helper functions and utilities

Usage:
    from commodity_pipeline.commodity_database import (
        CommodityDatabaseConfig,
        DatabaseAgent,
        VerificationAgent,
        DatabasePipelineOrchestrator
    )

    # Initialize with default config
    config = CommodityDatabaseConfig.from_environment()

    # Create agents
    db_agent = DatabaseAgent(config)
    verification_agent = VerificationAgent(config)

    # Run pipeline
    orchestrator = DatabasePipelineOrchestrator(config)
    result = orchestrator.run_full_pipeline(sources=[...])
"""

__version__ = "1.0.0"
__author__ = "RLC"

# Configuration
from .config.settings import (
    CommodityDatabaseConfig,
    DatabaseConfig,
    DataSourcesConfig,
    IngestionConfig,
    ValidationConfig,
    VerificationConfig,
    NotificationConfig,
    CommodityConfig,
    CommodityDefinition,
    DatabaseType,
    IngestionMode,
    ApprovalLevel,
    AlertChannel,
    default_config,
    DEFAULT_COMMODITIES,
    SUPPLY_DEMAND_FIELD_CATEGORIES,
    CROP_PROGRESS_FIELDS,
)

# Database Models
from .database.models import (
    Base,
    Commodity,
    Location,
    DataSource,
    PriceData,
    FundamentalData,
    CropProgress,
    TradeFlow,
    DataLoadLog,
    QualityAlert,
    SchemaChange,
    DataFrequency,
    DataSourceType,
    CommodityCategory,
    FlowType,
    LoadStatus,
    AlertSeverity,
    init_database,
    create_tables,
    get_session_factory,
    get_or_create_commodity,
    get_or_create_location,
    get_or_create_data_source,
    create_load_log,
    create_quality_alert,
    create_schema_change_request,
    get_latest_price,
    get_fundamental_data_for_period,
    get_pending_schema_changes,
    get_unresolved_alerts,
)

# Agents
from .agents.database_agent import (
    DatabaseAgent,
    IngestionResult,
    ValidationResult,
    SchemaValidationResult,
)

from .agents.verification_agent import (
    VerificationAgent,
    VerificationResult,
    VerificationCheck,
    DataQualityReport,
)

# Services
from .services.orchestrator import (
    DatabasePipelineOrchestrator,
    PipelineResult,
    SourceFetchResult,
    ScheduledTask,
)

# Utilities
from .utils.helpers import (
    parse_date,
    parse_marketing_year,
    get_marketing_year_for_date,
    parse_numeric,
    safe_divide,
    calculate_percent_change,
    normalize_commodity_name,
    normalize_field_name,
    normalize_country_name,
    convert_units,
    bushels_to_metric_tons,
    metric_tons_to_bushels,
    calculate_checksum,
    calculate_row_hash,
    validate_percentage,
    validate_positive,
    validate_date_range,
)

__all__ = [
    # Version
    "__version__",

    # Configuration
    "CommodityDatabaseConfig",
    "DatabaseConfig",
    "DataSourcesConfig",
    "IngestionConfig",
    "ValidationConfig",
    "VerificationConfig",
    "NotificationConfig",
    "CommodityConfig",
    "CommodityDefinition",
    "DatabaseType",
    "IngestionMode",
    "ApprovalLevel",
    "AlertChannel",
    "default_config",
    "DEFAULT_COMMODITIES",
    "SUPPLY_DEMAND_FIELD_CATEGORIES",
    "CROP_PROGRESS_FIELDS",

    # Database Models
    "Base",
    "Commodity",
    "Location",
    "DataSource",
    "PriceData",
    "FundamentalData",
    "CropProgress",
    "TradeFlow",
    "DataLoadLog",
    "QualityAlert",
    "SchemaChange",
    "DataFrequency",
    "DataSourceType",
    "CommodityCategory",
    "FlowType",
    "LoadStatus",
    "AlertSeverity",
    "init_database",
    "create_tables",
    "get_session_factory",
    "get_or_create_commodity",
    "get_or_create_location",
    "get_or_create_data_source",
    "create_load_log",
    "create_quality_alert",
    "create_schema_change_request",
    "get_latest_price",
    "get_fundamental_data_for_period",
    "get_pending_schema_changes",
    "get_unresolved_alerts",

    # Agents
    "DatabaseAgent",
    "IngestionResult",
    "ValidationResult",
    "SchemaValidationResult",
    "VerificationAgent",
    "VerificationResult",
    "VerificationCheck",
    "DataQualityReport",

    # Services
    "DatabasePipelineOrchestrator",
    "PipelineResult",
    "SourceFetchResult",
    "ScheduledTask",

    # Utilities
    "parse_date",
    "parse_marketing_year",
    "get_marketing_year_for_date",
    "parse_numeric",
    "safe_divide",
    "calculate_percent_change",
    "normalize_commodity_name",
    "normalize_field_name",
    "normalize_country_name",
    "convert_units",
    "bushels_to_metric_tons",
    "metric_tons_to_bushels",
    "calculate_checksum",
    "calculate_row_hash",
    "validate_percentage",
    "validate_positive",
    "validate_date_range",
]
