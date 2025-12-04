"""
Commodity Database Models

This module defines SQLAlchemy models for the RLC commodity data database.
The schema supports:
- Price series data (daily, weekly, monthly)
- Fundamental supply/demand data (annual, marketing year)
- Crop progress/condition data (weekly during growing season)
- Trade flow data (imports/exports by origin/destination)
- Data audit and quality tracking

The design follows best practices:
- Normalized schema with reference tables
- Composite unique constraints to prevent duplicates
- Flexible tall-format for fundamental data
- Multi-database support (SQLite, MySQL, PostgreSQL)
- Full audit trail for data loads
"""

from datetime import datetime, date
from decimal import Decimal
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List

from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Float, Numeric, Boolean,
    Text, ForeignKey, Index, UniqueConstraint, Enum, create_engine,
    CheckConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.sql import func

Base = declarative_base()


# ============================================================================
# ENUMS
# ============================================================================

class DataFrequency(str, PyEnum):
    """Frequency of data observations."""
    TICK = "tick"           # Intraday tick data
    DAILY = "daily"         # Daily observations
    WEEKLY = "weekly"       # Weekly observations
    MONTHLY = "monthly"     # Monthly observations
    QUARTERLY = "quarterly" # Quarterly observations
    ANNUAL = "annual"       # Annual observations
    MARKETING_YEAR = "marketing_year"  # Marketing year (e.g., 2024/25)


class DataSourceType(str, PyEnum):
    """Source type for data provenance."""
    USDA_OFFICIAL = "usda_official"     # Official USDA data
    USDA_AMS = "usda_ams"               # USDA Agricultural Marketing Service
    USDA_NASS = "usda_nass"             # USDA National Agricultural Statistics
    USDA_FAS = "usda_fas"               # USDA Foreign Agricultural Service
    INTERNAL_SPREADSHEET = "internal_spreadsheet"  # Internal Excel files
    INTERNAL_ESTIMATE = "internal_estimate"        # Internal RLC estimates
    API = "api"                          # External API source
    MANUAL_ENTRY = "manual_entry"        # Manually entered data
    LLM_GENERATED = "llm_generated"      # LLM-generated estimate


class CommodityCategory(str, PyEnum):
    """Category classification for commodities."""
    GRAIN = "grain"
    OILSEED = "oilseed"
    LIVESTOCK = "livestock"
    DAIRY = "dairy"
    JUICE = "juice"
    SWEETENER = "sweetener"
    FIBER = "fiber"
    ENERGY = "energy"
    OTHER = "other"


class FlowType(str, PyEnum):
    """Type of trade flow."""
    EXPORT = "export"
    IMPORT = "import"


class LoadStatus(str, PyEnum):
    """Status of data load operations."""
    SUCCESS = "success"
    PARTIAL = "partial"       # Some records failed
    FAILED = "failed"
    VALIDATION_ERROR = "validation_error"


class AlertSeverity(str, PyEnum):
    """Severity level for data quality alerts."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# ============================================================================
# REFERENCE TABLES
# ============================================================================

class Commodity(Base):
    """
    Reference table for commodities.

    Each commodity has a unique identifier and associated metadata
    including category, default units, and marketing year configuration.
    """
    __tablename__ = 'commodities'

    commodity_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False, index=True)
    symbol = Column(String(20), unique=True, nullable=True)  # Trading symbol (e.g., ZC for Corn)
    category = Column(Enum(CommodityCategory), nullable=False, default=CommodityCategory.OTHER)

    # Default units for this commodity
    price_unit = Column(String(50), nullable=True)  # e.g., "cents/lb", "$/bu"
    quantity_unit = Column(String(50), nullable=True)  # e.g., "bushels", "metric tons"

    # Marketing year configuration
    marketing_year_start_month = Column(Integer, nullable=True)  # 1-12, e.g., 9 for September
    marketing_year_start_day = Column(Integer, nullable=True, default=1)

    # Conversion factors
    bushel_weight_lbs = Column(Numeric(10, 4), nullable=True)  # Pounds per bushel

    # Metadata
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    price_data = relationship("PriceData", back_populates="commodity", lazy="dynamic")
    fundamental_data = relationship("FundamentalData", back_populates="commodity", lazy="dynamic")
    crop_progress = relationship("CropProgress", back_populates="commodity", lazy="dynamic")
    trade_flows = relationship("TradeFlow", back_populates="commodity", lazy="dynamic")

    def __repr__(self):
        return f"<Commodity(id={self.commodity_id}, name='{self.name}', category='{self.category.value}')>"

    def get_marketing_year(self, for_date: date) -> str:
        """
        Get the marketing year string for a given date.

        Args:
            for_date: The date to determine marketing year for

        Returns:
            Marketing year string (e.g., "2024/25") or calendar year if not configured
        """
        if not self.marketing_year_start_month:
            return str(for_date.year)

        start_month = self.marketing_year_start_month
        start_day = self.marketing_year_start_day or 1

        # Determine which marketing year this date falls into
        year = for_date.year
        if for_date.month < start_month or (for_date.month == start_month and for_date.day < start_day):
            year -= 1

        return f"{year}/{str(year + 1)[-2:]}"


class Location(Base):
    """
    Reference table for geographic locations.

    Used for price data locations, trade flow origins/destinations,
    and crop progress regions.
    """
    __tablename__ = 'locations'

    location_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    location_type = Column(String(50), nullable=False)  # country, state, region, port, market
    country_code = Column(String(3), nullable=True)  # ISO 3166-1 alpha-3
    parent_location_id = Column(Integer, ForeignKey('locations.location_id'), nullable=True)

    # Geographic coordinates (optional)
    latitude = Column(Numeric(10, 6), nullable=True)
    longitude = Column(Numeric(10, 6), nullable=True)

    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=func.now(), nullable=False)

    # Self-referential relationship for hierarchical locations
    parent = relationship("Location", remote_side=[location_id], backref="children")

    __table_args__ = (
        UniqueConstraint('name', 'location_type', name='uq_location_name_type'),
        Index('idx_location_country', 'country_code'),
    )

    def __repr__(self):
        return f"<Location(id={self.location_id}, name='{self.name}', type='{self.location_type}')>"


class DataSource(Base):
    """
    Reference table for data sources.

    Tracks all sources of data with their configuration and metadata.
    """
    __tablename__ = 'data_sources'

    source_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)
    source_type = Column(Enum(DataSourceType), nullable=False)

    # Source configuration
    base_url = Column(String(500), nullable=True)  # For API sources
    file_path = Column(String(500), nullable=True)  # For file-based sources

    # Update frequency
    update_frequency = Column(Enum(DataFrequency), nullable=True)
    expected_update_day = Column(Integer, nullable=True)  # Day of week (0=Mon) or day of month
    expected_update_time = Column(String(10), nullable=True)  # HH:MM format

    # Metadata
    description = Column(Text, nullable=True)
    is_active = Column(Boolean, default=True, nullable=False)
    last_successful_fetch = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<DataSource(id={self.source_id}, name='{self.name}', type='{self.source_type.value}')>"


# ============================================================================
# PRICE DATA
# ============================================================================

class PriceData(Base):
    """
    Time-series price data for commodities.

    Stores individual price observations with full provenance tracking.
    Supports multiple frequencies (tick, daily, weekly, monthly).
    Uses composite unique constraint to prevent duplicate entries.
    """
    __tablename__ = 'price_data'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core dimensions
    commodity_id = Column(Integer, ForeignKey('commodities.commodity_id'), nullable=False, index=True)
    observation_date = Column(Date, nullable=False, index=True)
    location = Column(String(100), nullable=False, default="National Average")

    # Price values
    price = Column(Numeric(12, 4), nullable=False)
    price_unit = Column(String(50), nullable=True)  # Override commodity default if different

    # Optional additional price points
    open_price = Column(Numeric(12, 4), nullable=True)
    high_price = Column(Numeric(12, 4), nullable=True)
    low_price = Column(Numeric(12, 4), nullable=True)
    close_price = Column(Numeric(12, 4), nullable=True)
    volume = Column(Integer, nullable=True)

    # Basis and spreads
    basis = Column(Numeric(10, 4), nullable=True)
    basis_location = Column(String(100), nullable=True)

    # Data frequency and source
    frequency = Column(Enum(DataFrequency), nullable=False, default=DataFrequency.DAILY)
    source_type = Column(Enum(DataSourceType), nullable=False)
    source_report = Column(String(200), nullable=False, default="Unknown")
    source_id = Column(Integer, ForeignKey('data_sources.source_id'), nullable=True)

    # Quality flags
    is_estimate = Column(Boolean, default=False, nullable=False)
    is_preliminary = Column(Boolean, default=False, nullable=False)

    # Audit fields
    fetched_at = Column(DateTime, default=func.now(), nullable=False)
    load_id = Column(Integer, ForeignKey('data_load_logs.load_id'), nullable=True)

    # Relationships
    commodity = relationship("Commodity", back_populates="price_data")
    data_source = relationship("DataSource")
    load_log = relationship("DataLoadLog")

    __table_args__ = (
        # Prevent duplicate price entries for same commodity/date/location/source
        UniqueConstraint(
            'commodity_id', 'observation_date', 'location', 'source_report', 'frequency',
            name='uq_price_entry'
        ),
        Index('idx_price_commodity_date', 'commodity_id', 'observation_date'),
        Index('idx_price_date_range', 'observation_date', 'commodity_id'),
        Index('idx_price_source', 'source_report'),
        # Ensure price is positive
        CheckConstraint('price >= 0', name='ck_price_positive'),
    )

    def __repr__(self):
        return f"<PriceData(commodity_id={self.commodity_id}, date={self.observation_date}, price={self.price})>"


# ============================================================================
# FUNDAMENTAL DATA (SUPPLY/DEMAND)
# ============================================================================

class FundamentalData(Base):
    """
    Supply/demand and other fundamental data in tall format.

    Uses a flexible structure where each metric is stored as a row,
    allowing new fields to be added without schema changes.
    Supports marketing year periods (e.g., "2024/25").
    """
    __tablename__ = 'fundamental_data'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core dimensions
    commodity_id = Column(Integer, ForeignKey('commodities.commodity_id'), nullable=False, index=True)
    period = Column(String(20), nullable=False, index=True)  # e.g., "2024/25" or "2024"
    period_type = Column(Enum(DataFrequency), nullable=False, default=DataFrequency.MARKETING_YEAR)

    # Field definition (tall format)
    field_name = Column(String(100), nullable=False, index=True)  # e.g., "production", "exports"
    field_category = Column(String(50), nullable=True)  # e.g., "supply", "demand", "balance"

    # Value
    value = Column(Numeric(18, 4), nullable=False)
    unit = Column(String(50), nullable=True)  # e.g., "million bushels", "1000 MT"

    # Source tracking
    source_type = Column(Enum(DataSourceType), nullable=False)
    source_report = Column(String(200), nullable=True)  # e.g., "WASDE December 2024"
    source_id = Column(Integer, ForeignKey('data_sources.source_id'), nullable=True)

    # Comparison values (optional)
    previous_value = Column(Numeric(18, 4), nullable=True)  # Previous period value
    change_from_previous = Column(Numeric(18, 4), nullable=True)
    usda_value = Column(Numeric(18, 4), nullable=True)  # Official USDA for comparison
    change_from_usda = Column(Numeric(18, 4), nullable=True)

    # Quality flags
    is_estimate = Column(Boolean, default=False, nullable=False)
    is_forecast = Column(Boolean, default=False, nullable=False)
    confidence_level = Column(String(20), nullable=True)  # e.g., "high", "medium", "low"

    # Audit fields
    effective_date = Column(Date, nullable=True)  # When this estimate was made
    fetched_at = Column(DateTime, default=func.now(), nullable=False)
    load_id = Column(Integer, ForeignKey('data_load_logs.load_id'), nullable=True)

    # Relationships
    commodity = relationship("Commodity", back_populates="fundamental_data")
    data_source = relationship("DataSource")
    load_log = relationship("DataLoadLog")

    __table_args__ = (
        # Prevent duplicate fundamental entries
        UniqueConstraint(
            'commodity_id', 'period', 'field_name', 'source_type',
            name='uq_fundamental_entry'
        ),
        Index('idx_fundamental_commodity_period', 'commodity_id', 'period'),
        Index('idx_fundamental_field', 'field_name'),
        Index('idx_fundamental_source', 'source_type'),
    )

    def __repr__(self):
        return f"<FundamentalData(commodity_id={self.commodity_id}, period='{self.period}', field='{self.field_name}', value={self.value})>"


# ============================================================================
# CROP PROGRESS DATA
# ============================================================================

class CropProgress(Base):
    """
    Weekly crop progress and condition data.

    Tracks planting progress, crop development stages, and condition ratings
    during the growing season. Primarily sourced from USDA NASS.
    """
    __tablename__ = 'crop_progress'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core dimensions
    commodity_id = Column(Integer, ForeignKey('commodities.commodity_id'), nullable=False, index=True)
    week_ending = Column(Date, nullable=False, index=True)
    state = Column(String(50), nullable=True)  # NULL for national, otherwise state name

    # Marketing year
    crop_year = Column(Integer, nullable=False)  # e.g., 2024
    week_number = Column(Integer, nullable=True)  # Week of year

    # Progress percentages (0-100)
    pct_planted = Column(Numeric(5, 2), nullable=True)
    pct_emerged = Column(Numeric(5, 2), nullable=True)
    pct_silking = Column(Numeric(5, 2), nullable=True)  # Corn specific
    pct_blooming = Column(Numeric(5, 2), nullable=True)  # Soybeans specific
    pct_setting_pods = Column(Numeric(5, 2), nullable=True)
    pct_dough = Column(Numeric(5, 2), nullable=True)
    pct_dented = Column(Numeric(5, 2), nullable=True)
    pct_mature = Column(Numeric(5, 2), nullable=True)
    pct_harvested = Column(Numeric(5, 2), nullable=True)

    # Condition ratings (0-100, percent in category)
    pct_very_poor = Column(Numeric(5, 2), nullable=True)
    pct_poor = Column(Numeric(5, 2), nullable=True)
    pct_fair = Column(Numeric(5, 2), nullable=True)
    pct_good = Column(Numeric(5, 2), nullable=True)
    pct_excellent = Column(Numeric(5, 2), nullable=True)

    # Derived metrics
    pct_good_excellent = Column(Numeric(5, 2), nullable=True)  # Good + Excellent combined
    condition_index = Column(Numeric(5, 2), nullable=True)  # Weighted condition score

    # Comparison values
    year_ago_pct_planted = Column(Numeric(5, 2), nullable=True)
    avg_pct_planted = Column(Numeric(5, 2), nullable=True)  # 5-year average
    year_ago_pct_good_excellent = Column(Numeric(5, 2), nullable=True)
    avg_pct_good_excellent = Column(Numeric(5, 2), nullable=True)

    # Source tracking
    source_type = Column(Enum(DataSourceType), nullable=False, default=DataSourceType.USDA_NASS)
    source_report = Column(String(200), nullable=True)

    # Audit fields
    fetched_at = Column(DateTime, default=func.now(), nullable=False)
    load_id = Column(Integer, ForeignKey('data_load_logs.load_id'), nullable=True)

    # Relationships
    commodity = relationship("Commodity", back_populates="crop_progress")
    load_log = relationship("DataLoadLog")

    __table_args__ = (
        # Prevent duplicate progress entries
        UniqueConstraint(
            'commodity_id', 'week_ending', 'state',
            name='uq_crop_progress_entry'
        ),
        Index('idx_crop_progress_week', 'commodity_id', 'week_ending'),
        Index('idx_crop_progress_year', 'crop_year'),
        # Validate percentage ranges
        CheckConstraint('pct_planted >= 0 AND pct_planted <= 100', name='ck_pct_planted'),
        CheckConstraint('pct_harvested >= 0 AND pct_harvested <= 100', name='ck_pct_harvested'),
    )

    def __repr__(self):
        state_str = self.state or "National"
        return f"<CropProgress(commodity_id={self.commodity_id}, week={self.week_ending}, state='{state_str}')>"

    def calculate_condition_index(self) -> Optional[float]:
        """
        Calculate weighted condition index (1-5 scale).

        Formula: (1*VP + 2*P + 3*F + 4*G + 5*E) / 100
        """
        if all(v is not None for v in [self.pct_very_poor, self.pct_poor,
                                         self.pct_fair, self.pct_good, self.pct_excellent]):
            return (
                1 * float(self.pct_very_poor) +
                2 * float(self.pct_poor) +
                3 * float(self.pct_fair) +
                4 * float(self.pct_good) +
                5 * float(self.pct_excellent)
            ) / 100
        return None


# ============================================================================
# TRADE FLOW DATA
# ============================================================================

class TradeFlow(Base):
    """
    Import/export trade flow data.

    Tracks trade volumes and values by commodity, origin, destination,
    and time period. Supports multiple data sources and reconciliation.
    """
    __tablename__ = 'trade_flows'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core dimensions
    commodity_id = Column(Integer, ForeignKey('commodities.commodity_id'), nullable=False, index=True)
    flow_type = Column(Enum(FlowType), nullable=False, index=True)

    # Time period
    period_year = Column(Integer, nullable=False, index=True)
    period_month = Column(Integer, nullable=True)  # 1-12, NULL for annual
    marketing_year = Column(String(20), nullable=True)  # e.g., "2024/25"

    # Geography
    reporter_country = Column(String(100), nullable=False)  # Country reporting the data
    partner_country = Column(String(100), nullable=False, index=True)  # Trade partner

    # Trade classification
    hs_code = Column(String(12), nullable=True)  # HS code (up to 10 digits)
    hs_description = Column(String(500), nullable=True)

    # Quantities and values
    quantity = Column(Numeric(18, 4), nullable=True)
    quantity_unit = Column(String(50), nullable=True)  # e.g., "metric tons", "bushels"
    value = Column(Numeric(18, 4), nullable=True)
    value_unit = Column(String(10), default="USD", nullable=False)

    # Derived metrics
    unit_value = Column(Numeric(12, 4), nullable=True)  # Value per unit
    pct_of_total = Column(Numeric(8, 4), nullable=True)  # Percentage of total exports/imports

    # Comparison values
    year_ago_quantity = Column(Numeric(18, 4), nullable=True)
    quantity_change_pct = Column(Numeric(10, 4), nullable=True)

    # Source tracking
    source_type = Column(Enum(DataSourceType), nullable=False)
    source_report = Column(String(200), nullable=True)
    source_id = Column(Integer, ForeignKey('data_sources.source_id'), nullable=True)

    # Quality flags
    is_estimate = Column(Boolean, default=False, nullable=False)
    is_revised = Column(Boolean, default=False, nullable=False)

    # Audit fields
    fetched_at = Column(DateTime, default=func.now(), nullable=False)
    load_id = Column(Integer, ForeignKey('data_load_logs.load_id'), nullable=True)

    # Relationships
    commodity = relationship("Commodity", back_populates="trade_flows")
    data_source = relationship("DataSource")
    load_log = relationship("DataLoadLog")

    __table_args__ = (
        # Prevent duplicate trade flow entries
        UniqueConstraint(
            'commodity_id', 'flow_type', 'period_year', 'period_month',
            'reporter_country', 'partner_country', 'hs_code', 'source_type',
            name='uq_trade_flow_entry'
        ),
        Index('idx_trade_flow_period', 'period_year', 'period_month'),
        Index('idx_trade_flow_partner', 'partner_country'),
        Index('idx_trade_flow_commodity', 'commodity_id', 'flow_type'),
    )

    def __repr__(self):
        return f"<TradeFlow(commodity_id={self.commodity_id}, flow={self.flow_type.value}, year={self.period_year}, partner='{self.partner_country}')>"


# ============================================================================
# AUDIT AND QUALITY TABLES
# ============================================================================

class DataLoadLog(Base):
    """
    Audit log for data load operations.

    Tracks each data ingestion with statistics, status, and error details
    for debugging and data lineage tracking.
    """
    __tablename__ = 'data_load_logs'

    load_id = Column(Integer, primary_key=True, autoincrement=True)

    # Load identification
    load_type = Column(String(50), nullable=False)  # e.g., "price", "fundamental", "crop_progress"
    source_type = Column(Enum(DataSourceType), nullable=False)
    source_name = Column(String(200), nullable=False)

    # Timing
    started_at = Column(DateTime, nullable=False, default=func.now())
    completed_at = Column(DateTime, nullable=True)
    duration_seconds = Column(Numeric(10, 2), nullable=True)

    # Statistics
    records_read = Column(Integer, default=0, nullable=False)
    records_inserted = Column(Integer, default=0, nullable=False)
    records_updated = Column(Integer, default=0, nullable=False)
    records_skipped = Column(Integer, default=0, nullable=False)
    records_errored = Column(Integer, default=0, nullable=False)

    # Status
    status = Column(Enum(LoadStatus), nullable=False, default=LoadStatus.SUCCESS)
    error_message = Column(Text, nullable=True)
    error_details = Column(Text, nullable=True)  # JSON with detailed error info

    # Metadata
    parameters = Column(Text, nullable=True)  # JSON with load parameters
    checksum = Column(String(64), nullable=True)  # SHA256 of source data

    __table_args__ = (
        Index('idx_load_log_date', 'started_at'),
        Index('idx_load_log_source', 'source_type', 'source_name'),
        Index('idx_load_log_status', 'status'),
    )

    def __repr__(self):
        return f"<DataLoadLog(id={self.load_id}, type='{self.load_type}', status='{self.status.value}')>"

    def mark_complete(self, status: LoadStatus = LoadStatus.SUCCESS, error_message: str = None):
        """Mark load as complete with final status."""
        self.completed_at = datetime.utcnow()
        self.status = status
        if error_message:
            self.error_message = error_message
        if self.started_at and self.completed_at:
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()


class QualityAlert(Base):
    """
    Data quality alerts and anomalies.

    Tracks data quality issues detected during ingestion or validation,
    with severity levels and resolution tracking.
    """
    __tablename__ = 'quality_alerts'

    alert_id = Column(Integer, primary_key=True, autoincrement=True)

    # Alert identification
    alert_type = Column(String(50), nullable=False)  # e.g., "duplicate", "outlier", "missing"
    severity = Column(Enum(AlertSeverity), nullable=False, default=AlertSeverity.WARNING)

    # Context
    table_name = Column(String(100), nullable=False)
    record_id = Column(Integer, nullable=True)  # ID of affected record
    commodity_id = Column(Integer, ForeignKey('commodities.commodity_id'), nullable=True)

    # Alert details
    message = Column(Text, nullable=False)
    details = Column(Text, nullable=True)  # JSON with additional context

    # Resolution
    is_resolved = Column(Boolean, default=False, nullable=False)
    resolved_at = Column(DateTime, nullable=True)
    resolved_by = Column(String(100), nullable=True)
    resolution_notes = Column(Text, nullable=True)

    # Audit
    created_at = Column(DateTime, default=func.now(), nullable=False)
    load_id = Column(Integer, ForeignKey('data_load_logs.load_id'), nullable=True)

    # Relationships
    commodity = relationship("Commodity")
    load_log = relationship("DataLoadLog")

    __table_args__ = (
        Index('idx_alert_severity', 'severity', 'is_resolved'),
        Index('idx_alert_table', 'table_name'),
        Index('idx_alert_created', 'created_at'),
    )

    def __repr__(self):
        return f"<QualityAlert(id={self.alert_id}, type='{self.alert_type}', severity='{self.severity.value}')>"


class SchemaChange(Base):
    """
    Track schema change requests from agents.

    When the Database Agent encounters unexpected data structures,
    it logs the proposed change here for human review.
    """
    __tablename__ = 'schema_changes'

    change_id = Column(Integer, primary_key=True, autoincrement=True)

    # Change details
    change_type = Column(String(50), nullable=False)  # e.g., "new_field", "new_commodity", "type_change"
    table_name = Column(String(100), nullable=False)
    field_name = Column(String(100), nullable=True)

    # Proposed change
    proposed_change = Column(Text, nullable=False)  # Description of the change
    sample_data = Column(Text, nullable=True)  # JSON sample that triggered this

    # Status
    status = Column(String(20), default="pending", nullable=False)  # pending, approved, rejected
    reviewed_by = Column(String(100), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    review_notes = Column(Text, nullable=True)

    # Audit
    created_at = Column(DateTime, default=func.now(), nullable=False)
    source_file = Column(String(500), nullable=True)  # File that triggered this

    __table_args__ = (
        Index('idx_schema_change_status', 'status'),
        Index('idx_schema_change_table', 'table_name'),
    )

    def __repr__(self):
        return f"<SchemaChange(id={self.change_id}, type='{self.change_type}', status='{self.status}')>"


# ============================================================================
# DATABASE INITIALIZATION AND UTILITIES
# ============================================================================

def create_database_engine(connection_string: str, echo: bool = False):
    """
    Create SQLAlchemy engine from connection string.

    Args:
        connection_string: Database connection URL
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    return create_engine(connection_string, echo=echo)


def create_tables(engine):
    """
    Create all tables in the database.

    Args:
        engine: SQLAlchemy Engine instance
    """
    Base.metadata.create_all(engine)


def drop_tables(engine):
    """
    Drop all tables from the database. Use with caution!

    Args:
        engine: SQLAlchemy Engine instance
    """
    Base.metadata.drop_all(engine)


def get_session_factory(engine) -> sessionmaker:
    """
    Get a session factory bound to the engine.

    Args:
        engine: SQLAlchemy Engine instance

    Returns:
        Session factory (sessionmaker)
    """
    return sessionmaker(bind=engine)


def init_database(connection_string: str, echo: bool = False) -> tuple:
    """
    Initialize database connection and create tables.

    Args:
        connection_string: Database connection URL
        echo: If True, log all SQL statements

    Returns:
        Tuple of (engine, session_factory)
    """
    engine = create_database_engine(connection_string, echo=echo)
    create_tables(engine)
    session_factory = get_session_factory(engine)
    return engine, session_factory


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_or_create_commodity(session: Session, name: str, **kwargs) -> Commodity:
    """
    Get existing commodity or create new one.

    Args:
        session: SQLAlchemy session
        name: Commodity name
        **kwargs: Additional fields for new commodity

    Returns:
        Commodity instance
    """
    commodity = session.query(Commodity).filter_by(name=name).first()
    if commodity is None:
        commodity = Commodity(name=name, **kwargs)
        session.add(commodity)
        session.flush()  # Get ID without committing
    return commodity


def get_or_create_location(session: Session, name: str, location_type: str, **kwargs) -> Location:
    """
    Get existing location or create new one.

    Args:
        session: SQLAlchemy session
        name: Location name
        location_type: Type of location (country, state, port, etc.)
        **kwargs: Additional fields

    Returns:
        Location instance
    """
    location = session.query(Location).filter_by(name=name, location_type=location_type).first()
    if location is None:
        location = Location(name=name, location_type=location_type, **kwargs)
        session.add(location)
        session.flush()
    return location


def get_or_create_data_source(session: Session, name: str, source_type: DataSourceType, **kwargs) -> DataSource:
    """
    Get existing data source or create new one.

    Args:
        session: SQLAlchemy session
        name: Source name
        source_type: Type of data source
        **kwargs: Additional fields

    Returns:
        DataSource instance
    """
    source = session.query(DataSource).filter_by(name=name).first()
    if source is None:
        source = DataSource(name=name, source_type=source_type, **kwargs)
        session.add(source)
        session.flush()
    return source


def create_load_log(
    session: Session,
    load_type: str,
    source_type: DataSourceType,
    source_name: str,
    parameters: Dict[str, Any] = None
) -> DataLoadLog:
    """
    Create a new data load log entry.

    Args:
        session: SQLAlchemy session
        load_type: Type of data being loaded
        source_type: Type of data source
        source_name: Name of the source
        parameters: Optional load parameters dict

    Returns:
        DataLoadLog instance
    """
    import json

    log = DataLoadLog(
        load_type=load_type,
        source_type=source_type,
        source_name=source_name,
        parameters=json.dumps(parameters) if parameters else None
    )
    session.add(log)
    session.flush()
    return log


def create_quality_alert(
    session: Session,
    alert_type: str,
    table_name: str,
    message: str,
    severity: AlertSeverity = AlertSeverity.WARNING,
    **kwargs
) -> QualityAlert:
    """
    Create a quality alert.

    Args:
        session: SQLAlchemy session
        alert_type: Type of alert
        table_name: Table where issue was found
        message: Alert message
        severity: Alert severity level
        **kwargs: Additional fields

    Returns:
        QualityAlert instance
    """
    alert = QualityAlert(
        alert_type=alert_type,
        table_name=table_name,
        message=message,
        severity=severity,
        **kwargs
    )
    session.add(alert)
    session.flush()
    return alert


def create_schema_change_request(
    session: Session,
    change_type: str,
    table_name: str,
    proposed_change: str,
    **kwargs
) -> SchemaChange:
    """
    Create a schema change request for human review.

    Args:
        session: SQLAlchemy session
        change_type: Type of change
        table_name: Affected table
        proposed_change: Description of proposed change
        **kwargs: Additional fields

    Returns:
        SchemaChange instance
    """
    change = SchemaChange(
        change_type=change_type,
        table_name=table_name,
        proposed_change=proposed_change,
        **kwargs
    )
    session.add(change)
    session.flush()
    return change


def get_latest_price(
    session: Session,
    commodity_id: int,
    location: str = None,
    as_of_date: date = None
) -> Optional[PriceData]:
    """
    Get the most recent price for a commodity.

    Args:
        session: SQLAlchemy session
        commodity_id: Commodity ID
        location: Optional location filter
        as_of_date: Optional maximum date

    Returns:
        Most recent PriceData or None
    """
    query = session.query(PriceData).filter_by(commodity_id=commodity_id)

    if location:
        query = query.filter_by(location=location)

    if as_of_date:
        query = query.filter(PriceData.observation_date <= as_of_date)

    return query.order_by(PriceData.observation_date.desc()).first()


def get_fundamental_data_for_period(
    session: Session,
    commodity_id: int,
    period: str,
    source_type: DataSourceType = None
) -> Dict[str, Any]:
    """
    Get all fundamental data for a commodity and period as a dictionary.

    Args:
        session: SQLAlchemy session
        commodity_id: Commodity ID
        period: Period string (e.g., "2024/25")
        source_type: Optional source type filter

    Returns:
        Dictionary of field_name -> value
    """
    query = session.query(FundamentalData).filter_by(
        commodity_id=commodity_id,
        period=period
    )

    if source_type:
        query = query.filter_by(source_type=source_type)

    result = {}
    for row in query.all():
        result[row.field_name] = {
            'value': float(row.value) if row.value else None,
            'unit': row.unit,
            'source': row.source_type.value,
            'is_estimate': row.is_estimate,
            'is_forecast': row.is_forecast
        }

    return result


def get_pending_schema_changes(session: Session) -> List[SchemaChange]:
    """
    Get all pending schema change requests.

    Args:
        session: SQLAlchemy session

    Returns:
        List of pending SchemaChange instances
    """
    return session.query(SchemaChange).filter_by(status="pending").all()


def get_unresolved_alerts(
    session: Session,
    severity: AlertSeverity = None,
    limit: int = 100
) -> List[QualityAlert]:
    """
    Get unresolved quality alerts.

    Args:
        session: SQLAlchemy session
        severity: Optional minimum severity filter
        limit: Maximum number of alerts to return

    Returns:
        List of QualityAlert instances
    """
    query = session.query(QualityAlert).filter_by(is_resolved=False)

    if severity:
        severity_order = [AlertSeverity.INFO, AlertSeverity.WARNING,
                         AlertSeverity.ERROR, AlertSeverity.CRITICAL]
        min_index = severity_order.index(severity)
        query = query.filter(QualityAlert.severity.in_(severity_order[min_index:]))

    return query.order_by(QualityAlert.created_at.desc()).limit(limit).all()
