"""
Database Models for Export Inspections Data
SQLAlchemy ORM models compatible with MySQL, PostgreSQL, and SQLite
"""

from datetime import datetime, date
from decimal import Decimal
from typing import Optional
from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Float, Numeric,
    Date, DateTime, Boolean, ForeignKey, Index, UniqueConstraint,
    create_engine, event
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from sqlalchemy.pool import StaticPool

Base = declarative_base()


# =============================================================================
# LOOKUP TABLES
# =============================================================================

class Commodity(Base):
    """Commodity reference table"""
    __tablename__ = 'commodities'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(20), unique=True, nullable=False)  # e.g., "SOYBEANS"
    name = Column(String(100), nullable=False)
    bushel_weight_lbs = Column(Float, nullable=False)  # lbs per bushel
    marketing_year_start_month = Column(Integer, nullable=False)  # 1-12
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    classes = relationship("CommodityClass", back_populates="commodity")
    inspection_records = relationship("InspectionRecord", back_populates="commodity_ref")


class CommodityClass(Base):
    """Commodity class/subclass reference (especially for wheat)"""
    __tablename__ = 'commodity_classes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    commodity_id = Column(Integer, ForeignKey('commodities.id'), nullable=False)
    class_code = Column(String(20), nullable=False)  # e.g., "HRW", "HRS"
    class_name = Column(String(100), nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    commodity = relationship("Commodity", back_populates="classes")
    
    __table_args__ = (
        UniqueConstraint('commodity_id', 'class_code', name='uq_commodity_class'),
    )


class Country(Base):
    """Destination country reference with region mapping"""
    __tablename__ = 'countries'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(10), unique=True)  # ISO code if available
    name = Column(String(100), unique=True, nullable=False)
    region = Column(String(50), nullable=False)  # EU, ASIA_OCEANIA, FSU, etc.
    sub_region = Column(String(50))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    inspection_records = relationship("InspectionRecord", back_populates="country_ref")


class Port(Base):
    """US export port/region reference"""
    __tablename__ = 'ports'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    region = Column(String(50), nullable=False)  # GULF, PACIFIC, ATLANTIC, GREAT_LAKES, INTERIOR
    state = Column(String(50))
    city = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    inspection_records = relationship("InspectionRecord", back_populates="port_ref")


class FieldOffice(Base):
    """FGIS Field Office reference"""
    __tablename__ = 'field_offices'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    region = Column(String(50))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class Grade(Base):
    """Grain grade reference"""
    __tablename__ = 'grades'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(30), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    numeric_grade = Column(Integer)  # 1, 2, 3, etc. or NULL for sample grade
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# =============================================================================
# MAIN DATA TABLES
# =============================================================================

class InspectionRecord(Base):
    """
    Raw inspection record - one row per certificate
    Maps directly to the FGIS CSV file structure
    """
    __tablename__ = 'inspection_records'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    
    # Temporal identifiers
    week_ending_date = Column(Date, nullable=False, index=True)  # "Thursday" column
    cert_date = Column(Date)  # Certificate date
    calendar_year = Column(Integer, nullable=False, index=True)
    marketing_year = Column(Integer, index=True)
    
    # Record identifiers
    serial_number = Column(String(50), nullable=False)
    source_file = Column(String(100))  # Track which CSV file this came from
    
    # Shipment type info
    type_shipment = Column(String(20))  # Export, Domestic, etc.
    type_service = Column(String(20))   # Original, Reinspection, Appeal
    type_carrier = Column(String(30))   # Vessel, Rail, Truck, Barge
    carrier_name = Column(String(200))
    
    # Location info
    field_office = Column(String(100), index=True)
    port = Column(String(100), index=True)
    port_id = Column(Integer, ForeignKey('ports.id'))
    ams_region = Column(String(50))
    fgis_region = Column(String(50))
    city = Column(String(100))
    state = Column(String(50))
    
    # Commodity info
    grain = Column(String(50), nullable=False, index=True)
    commodity_id = Column(Integer, ForeignKey('commodities.id'))
    commodity_class = Column(String(30), index=True)
    subclass = Column(String(30))
    grade = Column(String(30))
    special_grade_1 = Column(String(50))
    special_grade_2 = Column(String(50))
    
    # Destination
    destination = Column(String(100), nullable=False, index=True)
    country_id = Column(Integer, ForeignKey('countries.id'))
    destination_region = Column(String(50), index=True)
    
    # Quantity - all units stored for flexibility
    pounds = Column(BigInteger, nullable=False)
    metric_tons = Column(Numeric(18, 4))
    bushels = Column(Numeric(18, 4))  # Calculated field
    thousand_bushels = Column(Numeric(18, 4))  # Legacy pre-2014 field
    
    # Sublot/carrier count
    sublot_carriers = Column(Integer)
    
    # Quality metrics - Dockage
    dockage_high = Column(Numeric(8, 4))
    dockage_low = Column(Numeric(8, 4))
    dockage_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Test Weight
    test_weight = Column(Numeric(8, 3))  # lb/bu
    
    # Quality metrics - Moisture
    moisture_high = Column(Numeric(8, 4))
    moisture_low = Column(Numeric(8, 4))
    moisture_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Broken/Damaged
    broken_corn_fm_high = Column(Numeric(8, 4))
    broken_corn_fm_low = Column(Numeric(8, 4))
    broken_corn_fm_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Damage
    total_damage_high = Column(Numeric(8, 4))
    total_damage_low = Column(Numeric(8, 4))
    total_damage_avg = Column(Numeric(8, 4))
    
    heat_damage_high = Column(Numeric(8, 4))
    heat_damage_low = Column(Numeric(8, 4))
    heat_damage_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Foreign Material
    foreign_material_high = Column(Numeric(8, 4))
    foreign_material_low = Column(Numeric(8, 4))
    foreign_material_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Shrunken & Broken (wheat)
    shrunken_broken_high = Column(Numeric(8, 4))
    shrunken_broken_low = Column(Numeric(8, 4))
    shrunken_broken_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Defects
    total_defects_high = Column(Numeric(8, 4))
    total_defects_low = Column(Numeric(8, 4))
    total_defects_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Splits (soybeans)
    splits_high = Column(Numeric(8, 4))
    splits_low = Column(Numeric(8, 4))
    splits_avg = Column(Numeric(8, 4))
    
    # Quality metrics - Protein
    protein_high = Column(Numeric(8, 4))
    protein_low = Column(Numeric(8, 4))
    protein_avg = Column(Numeric(8, 4))
    protein_basis = Column(String(20))  # "12%" moisture basis
    
    # Quality metrics - Oil
    oil_high = Column(Numeric(8, 4))
    oil_low = Column(Numeric(8, 4))
    oil_avg = Column(Numeric(8, 4))
    oil_basis = Column(String(20))
    
    # Quality metrics - Starch
    starch_high = Column(Numeric(8, 4))
    starch_low = Column(Numeric(8, 4))
    starch_avg = Column(Numeric(8, 4))
    
    # Mycotoxin testing - Aflatoxin
    aflatoxin_required = Column(Boolean)
    aflatoxin_basis = Column(String(20))
    aflatoxin_avg_ppb = Column(Numeric(10, 4))
    aflatoxin_rejected = Column(Boolean)
    
    # Mycotoxin testing - DON (Vomitoxin)
    don_required = Column(Boolean)
    don_basis = Column(String(20))
    don_avg_ppm = Column(Numeric(10, 4))
    don_rejected = Column(Boolean)
    
    # Pest management
    fumigant = Column(String(50))
    insecticide = Column(String(50))
    insect_count = Column(Integer)
    
    # Additional quality factors
    falling_number = Column(Numeric(10, 2))  # Wheat
    kernel_damage = Column(Numeric(8, 4))
    odor = Column(String(50))
    musty = Column(Boolean)
    sour = Column(Boolean)
    
    # Audit fields
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    commodity_ref = relationship("Commodity", back_populates="inspection_records")
    country_ref = relationship("Country", back_populates="inspection_records")
    port_ref = relationship("Port", back_populates="inspection_records")
    
    __table_args__ = (
        # Composite unique constraint to prevent duplicates
        UniqueConstraint('serial_number', 'week_ending_date', 'grain', name='uq_inspection_record'),
        # Performance indexes
        Index('ix_inspection_week_grain', 'week_ending_date', 'grain'),
        Index('ix_inspection_week_dest', 'week_ending_date', 'destination'),
        Index('ix_inspection_my_grain', 'marketing_year', 'grain'),
        Index('ix_inspection_port_region', 'port', 'destination_region'),
    )


class WeeklyCommoditySummary(Base):
    """
    Aggregated weekly summary by commodity
    Pre-calculated for reporting efficiency
    """
    __tablename__ = 'weekly_commodity_summary'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    week_ending_date = Column(Date, nullable=False, index=True)
    calendar_year = Column(Integer, nullable=False)
    marketing_year = Column(Integer, nullable=False, index=True)
    week_number = Column(Integer)  # Week of calendar year
    marketing_year_week = Column(Integer)  # Week of marketing year
    
    commodity = Column(String(50), nullable=False, index=True)
    
    # Volumes
    total_pounds = Column(BigInteger, nullable=False)
    total_metric_tons = Column(Numeric(18, 4))
    total_bushels = Column(Numeric(18, 4))
    
    # Certificate counts
    certificate_count = Column(Integer)
    sublot_count = Column(Integer)
    
    # Comparisons (populated by update job)
    prior_week_pounds = Column(BigInteger)
    year_ago_pounds = Column(BigInteger)
    
    # Marketing year to date
    my_to_date_pounds = Column(BigInteger)
    my_to_date_metric_tons = Column(Numeric(18, 4))
    
    # Audit
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('week_ending_date', 'commodity', name='uq_weekly_commodity'),
        Index('ix_weekly_summary_my', 'marketing_year', 'commodity'),
    )


class WeeklyCountryExports(Base):
    """
    Weekly exports by commodity and destination country
    """
    __tablename__ = 'weekly_country_exports'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    week_ending_date = Column(Date, nullable=False, index=True)
    marketing_year = Column(Integer, nullable=False, index=True)
    
    commodity = Column(String(50), nullable=False, index=True)
    destination_country = Column(String(100), nullable=False, index=True)
    destination_region = Column(String(50), nullable=False, index=True)
    
    # Volumes
    total_pounds = Column(BigInteger, nullable=False)
    total_metric_tons = Column(Numeric(18, 4))
    total_bushels = Column(Numeric(18, 4))
    
    certificate_count = Column(Integer)
    
    # Year comparisons
    prior_week_pounds = Column(BigInteger)
    year_ago_pounds = Column(BigInteger)
    my_to_date_pounds = Column(BigInteger)
    
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('week_ending_date', 'commodity', 'destination_country', 
                        name='uq_weekly_country'),
        Index('ix_weekly_country_region', 'week_ending_date', 'destination_region'),
    )


class WeeklyRegionExports(Base):
    """
    Weekly exports by commodity and destination region (aggregated)
    """
    __tablename__ = 'weekly_region_exports'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    week_ending_date = Column(Date, nullable=False, index=True)
    marketing_year = Column(Integer, nullable=False, index=True)
    
    commodity = Column(String(50), nullable=False, index=True)
    destination_region = Column(String(50), nullable=False, index=True)
    
    # Volumes
    total_pounds = Column(BigInteger, nullable=False)
    total_metric_tons = Column(Numeric(18, 4))
    total_bushels = Column(Numeric(18, 4))
    
    certificate_count = Column(Integer)
    country_count = Column(Integer)  # Number of destination countries
    
    my_to_date_pounds = Column(BigInteger)
    
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('week_ending_date', 'commodity', 'destination_region',
                        name='uq_weekly_region'),
    )


class WeeklyPortExports(Base):
    """
    Weekly exports by commodity and US port region
    """
    __tablename__ = 'weekly_port_exports'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    week_ending_date = Column(Date, nullable=False, index=True)
    marketing_year = Column(Integer, nullable=False, index=True)
    
    commodity = Column(String(50), nullable=False, index=True)
    port_region = Column(String(50), nullable=False, index=True)
    
    # Volumes
    total_pounds = Column(BigInteger, nullable=False)
    total_metric_tons = Column(Numeric(18, 4))
    total_bushels = Column(Numeric(18, 4))
    
    certificate_count = Column(Integer)
    
    my_to_date_pounds = Column(BigInteger)
    
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('week_ending_date', 'commodity', 'port_region',
                        name='uq_weekly_port'),
    )


class WheatClassExports(Base):
    """
    Wheat-specific tracking by class (HRW, HRS, SRW, etc.)
    """
    __tablename__ = 'wheat_class_exports'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    week_ending_date = Column(Date, nullable=False, index=True)
    marketing_year = Column(Integer, nullable=False, index=True)
    
    wheat_class = Column(String(30), nullable=False, index=True)  # HRW, HRS, SRW, etc.
    destination_country = Column(String(100), index=True)  # NULL for total
    destination_region = Column(String(50), index=True)
    port_region = Column(String(50), index=True)
    
    # Volumes
    total_pounds = Column(BigInteger, nullable=False)
    total_metric_tons = Column(Numeric(18, 4))
    total_bushels = Column(Numeric(18, 4))
    
    certificate_count = Column(Integer)
    
    # Quality averages for wheat
    avg_test_weight = Column(Numeric(8, 3))
    avg_protein = Column(Numeric(8, 4))
    avg_moisture = Column(Numeric(8, 4))
    avg_dockage = Column(Numeric(8, 4))
    avg_falling_number = Column(Numeric(10, 2))
    
    my_to_date_pounds = Column(BigInteger)
    
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_wheat_class_week', 'week_ending_date', 'wheat_class'),
    )


class WeeklyQualityStats(Base):
    """
    Weekly quality statistics by commodity
    """
    __tablename__ = 'weekly_quality_stats'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    week_ending_date = Column(Date, nullable=False, index=True)
    marketing_year = Column(Integer, nullable=False)
    
    commodity = Column(String(50), nullable=False, index=True)
    destination_region = Column(String(50))  # NULL for overall
    
    # Sample size
    certificate_count = Column(Integer)
    total_pounds = Column(BigInteger)
    
    # Test weight stats
    test_weight_avg = Column(Numeric(8, 3))
    test_weight_min = Column(Numeric(8, 3))
    test_weight_max = Column(Numeric(8, 3))
    
    # Moisture stats
    moisture_avg = Column(Numeric(8, 4))
    moisture_min = Column(Numeric(8, 4))
    moisture_max = Column(Numeric(8, 4))
    
    # Damage stats
    total_damage_avg = Column(Numeric(8, 4))
    heat_damage_avg = Column(Numeric(8, 4))
    
    # Foreign material
    foreign_material_avg = Column(Numeric(8, 4))
    dockage_avg = Column(Numeric(8, 4))
    
    # Protein (wheat/soybeans)
    protein_avg = Column(Numeric(8, 4))
    protein_min = Column(Numeric(8, 4))
    protein_max = Column(Numeric(8, 4))
    
    # Oil (soybeans)
    oil_avg = Column(Numeric(8, 4))
    
    # Mycotoxins
    aflatoxin_tested_count = Column(Integer)
    aflatoxin_avg_ppb = Column(Numeric(10, 4))
    aflatoxin_reject_count = Column(Integer)
    
    don_tested_count = Column(Integer)
    don_avg_ppm = Column(Numeric(10, 4))
    don_reject_count = Column(Integer)
    
    calculated_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint('week_ending_date', 'commodity', 'destination_region',
                        name='uq_weekly_quality'),
    )


class DataLoadLog(Base):
    """
    Track data load operations for auditing and recovery
    """
    __tablename__ = 'data_load_log'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    load_timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    source_file = Column(String(200), nullable=False)
    file_year = Column(Integer, nullable=False)
    
    records_read = Column(Integer)
    records_inserted = Column(Integer)
    records_updated = Column(Integer)
    records_skipped = Column(Integer)
    records_errored = Column(Integer)
    
    weeks_processed = Column(Integer)
    earliest_week = Column(Date)
    latest_week = Column(Date)
    
    status = Column(String(20), nullable=False)  # SUCCESS, PARTIAL, FAILED
    error_message = Column(Text)
    
    duration_seconds = Column(Float)
    
    __table_args__ = (
        Index('ix_load_log_file', 'source_file', 'load_timestamp'),
    )


# =============================================================================
# DATABASE ENGINE AND SESSION MANAGEMENT
# =============================================================================

def create_db_engine(connection_string: str, echo: bool = False):
    """Create database engine with appropriate settings"""
    if connection_string.startswith('sqlite'):
        # SQLite specific settings
        engine = create_engine(
            connection_string,
            echo=echo,
            connect_args={'check_same_thread': False},
            poolclass=StaticPool
        )
        
        # Enable foreign keys for SQLite
        @event.listens_for(engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()
    else:
        # MySQL/PostgreSQL settings
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
