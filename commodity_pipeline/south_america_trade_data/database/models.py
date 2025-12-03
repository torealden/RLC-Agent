"""
Database Models for South America Trade Data
SQLAlchemy ORM models for storing trade flow data from Argentina, Brazil, Colombia, Uruguay, and Paraguay
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Float, Numeric,
    Date, DateTime, Boolean, ForeignKey, Index, UniqueConstraint,
    create_engine, event, Enum as SQLEnum
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.pool import StaticPool
import enum

Base = declarative_base()


# =============================================================================
# ENUMS
# =============================================================================

class FlowType(enum.Enum):
    """Trade flow direction"""
    EXPORT = "export"
    IMPORT = "import"


class DataSource(enum.Enum):
    """Data source identifier"""
    INDEC = "INDEC"  # Argentina
    COMEX_STAT = "COMEX_STAT"  # Brazil
    DANE = "DANE"  # Colombia
    DNA_UY = "DNA_UY"  # Uruguay
    DNA_PY = "DNA_PY"  # Paraguay
    WITS = "WITS"  # World Bank fallback
    COMTRADE = "COMTRADE"  # UN Comtrade fallback


# =============================================================================
# REFERENCE TABLES
# =============================================================================

class CountryReference(Base):
    """Country reference with ISO codes and regional mappings"""
    __tablename__ = 'sa_country_reference'

    id = Column(Integer, primary_key=True, autoincrement=True)
    iso3_code = Column(String(3), unique=True, nullable=False)
    iso2_code = Column(String(2), unique=True)
    numeric_code = Column(String(3))
    name = Column(String(100), nullable=False)
    name_spanish = Column(String(100))
    name_portuguese = Column(String(100))

    # Regional groupings
    region = Column(String(50))  # South America, Central America, etc.
    sub_region = Column(String(50))  # MERCOSUR, Andean, etc.
    is_mercosur_member = Column(Boolean, default=False)
    is_mercosur_associate = Column(Boolean, default=False)

    # Activity flags
    is_reporter = Column(Boolean, default=False)  # Is this country a data reporter
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class HSCodeReference(Base):
    """HS Code reference with descriptions at multiple levels"""
    __tablename__ = 'sa_hs_code_reference'

    id = Column(Integer, primary_key=True, autoincrement=True)
    hs_code = Column(String(12), unique=True, nullable=False)
    hs_level = Column(Integer, nullable=False)  # 2, 4, 6, 8, 10

    # Hierarchy
    chapter = Column(String(2))  # HS2
    heading = Column(String(4))  # HS4
    subheading = Column(String(6))  # HS6

    # Descriptions
    description_en = Column(Text)
    description_es = Column(Text)  # Spanish
    description_pt = Column(Text)  # Portuguese

    # Categorization
    section = Column(String(10))  # Roman numeral section
    section_description = Column(String(200))

    # Commodity flags for key products
    is_agriculture = Column(Boolean, default=False)
    is_key_commodity = Column(Boolean, default=False)
    commodity_group = Column(String(50))  # e.g., "Cereals", "Oilseeds", "Meat"

    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('ix_hs_level_chapter', 'hs_level', 'chapter'),
    )


# =============================================================================
# MAIN TRADE DATA TABLES
# =============================================================================

class TradeFlowRecord(Base):
    """
    Raw trade flow record - stores original data from each source
    One record per unique (reporter, flow, period, hs_code, partner) combination
    """
    __tablename__ = 'sa_trade_flow_records'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Source identification
    data_source = Column(String(20), nullable=False, index=True)  # INDEC, COMEX_STAT, etc.
    source_file = Column(String(255))  # Original filename/URL
    source_record_id = Column(String(100))  # Original record ID if available

    # Reporter country (who reported this data)
    reporter_country = Column(String(3), nullable=False, index=True)  # ISO3

    # Trade flow
    flow = Column(String(10), nullable=False, index=True)  # export/import

    # Time period
    year = Column(Integer, nullable=False, index=True)
    month = Column(Integer, nullable=False, index=True)
    period = Column(String(7), nullable=False, index=True)  # YYYY-MM format

    # HS Code classification
    hs_code = Column(String(12), nullable=False, index=True)
    hs_level = Column(Integer, nullable=False)  # Native level (6, 8, 10)
    hs_code_6 = Column(String(6), index=True)  # Harmonized to 6-digit
    hs_description = Column(Text)

    # Partner country
    partner_country = Column(String(100), nullable=False, index=True)
    partner_country_code = Column(String(3), index=True)  # ISO3 if resolved

    # Quantities
    quantity_kg = Column(Numeric(20, 4))  # Net weight in kilograms
    quantity_tons = Column(Numeric(18, 4))  # Metric tons (calculated)
    quantity_units = Column(Numeric(18, 4))  # Statistical units if different
    quantity_unit_type = Column(String(20))  # kg, tons, units, etc.

    # Values
    value_usd = Column(Numeric(20, 2), nullable=False)  # Primary value in USD
    value_fob_usd = Column(Numeric(20, 2))  # FOB value (exports)
    value_cif_usd = Column(Numeric(20, 2))  # CIF value (imports)
    currency_original = Column(String(3))  # Original currency if not USD
    value_original = Column(Numeric(20, 2))  # Value in original currency

    # Additional dimensions
    transport_mode = Column(String(50))  # Sea, Air, Road, Rail
    customs_office = Column(String(100))  # Aduana/Port
    state_region = Column(String(100))  # State/Province/Department

    # Audit fields
    ingested_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    file_hash = Column(String(64))  # SHA256 of source file
    record_hash = Column(String(64))  # Hash for duplicate detection

    __table_args__ = (
        # Unique constraint for deduplication
        UniqueConstraint(
            'reporter_country', 'flow', 'period', 'hs_code', 'partner_country',
            name='uq_trade_flow_record'
        ),
        # Performance indexes
        Index('ix_trade_reporter_period', 'reporter_country', 'period'),
        Index('ix_trade_flow_period', 'flow', 'period'),
        Index('ix_trade_hs6_period', 'hs_code_6', 'period'),
        Index('ix_trade_partner_period', 'partner_country_code', 'period'),
    )


class TradeFlowHarmonized(Base):
    """
    Harmonized trade flow data at HS6 level
    Used for cross-country comparison and balancing
    All values normalized to USD and metric tons
    """
    __tablename__ = 'sa_trade_flow_harmonized'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Reporter
    reporter_country = Column(String(3), nullable=False, index=True)

    # Flow and period
    flow = Column(String(10), nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    month = Column(Integer, nullable=False)
    period = Column(String(7), nullable=False, index=True)

    # HS Code at 6-digit level
    hs_code_6 = Column(String(6), nullable=False, index=True)
    chapter = Column(String(2), index=True)

    # Partner
    partner_country = Column(String(3), nullable=False, index=True)

    # Normalized values
    quantity_tons = Column(Numeric(18, 4))
    value_usd = Column(Numeric(20, 2), nullable=False)

    # Pre-balance vs post-balance
    is_balanced = Column(Boolean, default=False)
    balance_version = Column(Integer, default=0)

    # Source tracking
    source_count = Column(Integer, default=1)  # Number of source records aggregated
    primary_source = Column(String(20))

    # Audit
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            'reporter_country', 'flow', 'period', 'hs_code_6', 'partner_country',
            name='uq_harmonized_flow'
        ),
        Index('ix_harmonized_hs6_flow', 'hs_code_6', 'flow', 'period'),
    )


class TradeBalanceMatrix(Base):
    """
    Reporter-Partner-HS balance matrix for reconciliation
    Stores both reported values and discrepancies
    """
    __tablename__ = 'sa_trade_balance_matrix'

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Dimensions
    year = Column(Integer, nullable=False, index=True)
    month = Column(Integer, nullable=False)
    period = Column(String(7), nullable=False, index=True)
    hs_code_6 = Column(String(6), nullable=False, index=True)

    # Reporter (exporter) perspective
    exporter_country = Column(String(3), nullable=False, index=True)
    importer_country = Column(String(3), nullable=False, index=True)

    # Reported values
    exports_reported_by_exporter_usd = Column(Numeric(20, 2))
    exports_reported_by_exporter_tons = Column(Numeric(18, 4))

    imports_reported_by_importer_usd = Column(Numeric(20, 2))
    imports_reported_by_importer_tons = Column(Numeric(18, 4))

    # Calculated discrepancies
    value_delta_usd = Column(Numeric(20, 2))  # export - import (should be ~0)
    quantity_delta_tons = Column(Numeric(18, 4))
    value_delta_pct = Column(Numeric(10, 4))  # Percentage difference

    # Balanced values (after reconciliation)
    balanced_value_usd = Column(Numeric(20, 2))
    balanced_quantity_tons = Column(Numeric(18, 4))
    balance_method = Column(String(50))  # Method used for balancing
    adjustment_factor = Column(Numeric(10, 6))

    # Flags
    has_discrepancy = Column(Boolean, default=False)
    discrepancy_flagged = Column(Boolean, default=False)

    # Audit
    calculated_at = Column(DateTime, default=datetime.utcnow)
    balance_version = Column(Integer, default=1)

    __table_args__ = (
        UniqueConstraint(
            'period', 'hs_code_6', 'exporter_country', 'importer_country',
            name='uq_balance_matrix'
        ),
        Index('ix_balance_discrepancy', 'has_discrepancy', 'period'),
    )


# =============================================================================
# AGGREGATION TABLES
# =============================================================================

class MonthlyAggregate(Base):
    """
    Pre-aggregated monthly totals by various dimensions
    For quick reporting and dashboard queries
    """
    __tablename__ = 'sa_monthly_aggregates'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Dimensions
    reporter_country = Column(String(3), nullable=False, index=True)
    flow = Column(String(10), nullable=False, index=True)
    year = Column(Integer, nullable=False, index=True)
    month = Column(Integer, nullable=False)
    period = Column(String(7), nullable=False, index=True)

    # Aggregation level
    agg_level = Column(String(20), nullable=False)  # 'total', 'chapter', 'hs6', 'partner'
    hs_chapter = Column(String(2), index=True)  # NULL for total
    hs_code_6 = Column(String(6), index=True)  # NULL for chapter/total
    partner_country = Column(String(3), index=True)  # NULL for product-only aggs

    # Metrics
    total_value_usd = Column(Numeric(20, 2), nullable=False)
    total_quantity_tons = Column(Numeric(18, 4))
    record_count = Column(Integer)
    partner_count = Column(Integer)  # Number of unique partners
    product_count = Column(Integer)  # Number of unique HS codes

    # Comparisons
    prior_month_value_usd = Column(Numeric(20, 2))
    prior_year_value_usd = Column(Numeric(20, 2))
    mom_change_pct = Column(Numeric(10, 4))  # Month-over-month
    yoy_change_pct = Column(Numeric(10, 4))  # Year-over-year

    # Year-to-date
    ytd_value_usd = Column(Numeric(20, 2))
    ytd_quantity_tons = Column(Numeric(18, 4))

    calculated_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            'reporter_country', 'flow', 'period', 'agg_level',
            'hs_chapter', 'hs_code_6', 'partner_country',
            name='uq_monthly_aggregate'
        ),
        Index('ix_agg_reporter_flow', 'reporter_country', 'flow', 'period'),
    )


# =============================================================================
# METADATA AND LOGGING TABLES
# =============================================================================

class DataSourceLog(Base):
    """
    Track data ingestion operations for audit and recovery
    """
    __tablename__ = 'sa_data_source_log'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Source identification
    data_source = Column(String(20), nullable=False)
    country_code = Column(String(3), nullable=False)

    # Operation details
    operation_type = Column(String(20), nullable=False)  # fetch, parse, load, validate
    operation_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Request metadata
    request_url = Column(Text)
    request_params = Column(Text)  # JSON string
    response_status = Column(Integer)
    response_time_ms = Column(Integer)

    # Data metrics
    period_start = Column(String(7))  # YYYY-MM
    period_end = Column(String(7))
    records_fetched = Column(Integer)
    records_inserted = Column(Integer)
    records_updated = Column(Integer)
    records_skipped = Column(Integer)
    records_errored = Column(Integer)

    # File info
    source_file = Column(String(255))
    file_hash = Column(String(64))  # SHA256
    file_size_bytes = Column(BigInteger)
    file_modified_date = Column(DateTime)

    # Status
    status = Column(String(20), nullable=False)  # SUCCESS, PARTIAL, FAILED
    error_message = Column(Text)
    error_details = Column(Text)  # Stack trace if applicable

    # Duration
    duration_seconds = Column(Float)

    # Version info
    source_version_date = Column(Date)  # Version date from source

    __table_args__ = (
        Index('ix_log_source_date', 'data_source', 'operation_timestamp'),
        Index('ix_log_status', 'status', 'operation_timestamp'),
    )


class QualityAlert(Base):
    """
    Track data quality issues and outliers
    """
    __tablename__ = 'sa_quality_alerts'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Alert identification
    alert_type = Column(String(50), nullable=False)  # deviation, missing, duplicate, outlier
    severity = Column(String(20), nullable=False)  # info, warning, error, critical

    # Context
    data_source = Column(String(20))
    reporter_country = Column(String(3))
    period = Column(String(7))
    hs_code = Column(String(12))
    partner_country = Column(String(100))

    # Alert details
    message = Column(Text, nullable=False)
    expected_value = Column(Numeric(20, 4))
    actual_value = Column(Numeric(20, 4))
    deviation_pct = Column(Numeric(10, 4))
    zscore = Column(Numeric(10, 4))

    # Status
    is_acknowledged = Column(Boolean, default=False)
    acknowledged_by = Column(String(100))
    acknowledged_at = Column(DateTime)
    resolution_notes = Column(Text)

    # Auto-generated
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    __table_args__ = (
        Index('ix_alert_type_severity', 'alert_type', 'severity'),
        Index('ix_alert_unacknowledged', 'is_acknowledged', 'created_at'),
    )


class LastSuccessfulPull(Base):
    """
    Track last successful data pull per source for incremental updates
    """
    __tablename__ = 'sa_last_successful_pull'

    id = Column(Integer, primary_key=True, autoincrement=True)

    data_source = Column(String(20), nullable=False)
    country_code = Column(String(3), nullable=False)

    # Last successful period
    last_period = Column(String(7), nullable=False)  # YYYY-MM
    last_success_timestamp = Column(DateTime, nullable=False)

    # Next expected
    next_expected_period = Column(String(7))
    next_expected_date = Column(Date)

    # Metadata
    total_records_loaded = Column(BigInteger, default=0)
    consecutive_failures = Column(Integer, default=0)

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('data_source', 'country_code', name='uq_last_pull'),
    )


# =============================================================================
# DATABASE ENGINE AND SESSION MANAGEMENT
# =============================================================================

def create_db_engine(connection_string: str, echo: bool = False):
    """Create database engine with appropriate settings"""
    if connection_string.startswith('sqlite'):
        engine = create_engine(
            connection_string,
            echo=echo,
            connect_args={'check_same_thread': False},
            poolclass=StaticPool
        )

        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    else:
        engine = create_engine(
            connection_string,
            echo=echo,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600
        )

    return engine


def create_all_tables(engine):
    """Create all tables in the database"""
    Base.metadata.create_all(engine)


def get_session_factory(engine):
    """Get sessionmaker bound to engine"""
    return sessionmaker(bind=engine)


def init_database(connection_string: str, echo: bool = False):
    """Initialize database and return session factory"""
    engine = create_db_engine(connection_string, echo)
    create_all_tables(engine)
    return get_session_factory(engine), engine
