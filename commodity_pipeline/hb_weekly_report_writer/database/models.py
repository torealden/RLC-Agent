"""
Database Models for HB Weekly Report Writer

SQLAlchemy ORM models for storing report data, price information,
internal data snapshots, and questions/escalations.
"""

import logging
from datetime import datetime, date
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum as PyEnum

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    Date,
    Text,
    Boolean,
    Enum,
    ForeignKey,
    JSON,
    Index,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

Base = declarative_base()


# =============================================================================
# ENUMS
# =============================================================================

class ReportStatus(PyEnum):
    """Status of a report"""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    PUBLISHED = "published"
    ARCHIVED = "archived"


class QuestionStatus(PyEnum):
    """Status of a question/escalation"""
    PENDING = "pending"
    ANSWERED = "answered"
    TIMEOUT = "timeout"
    IGNORED = "ignored"


class DataSourceType(PyEnum):
    """Source of data"""
    INTERNAL_SPREADSHEET = "internal_spreadsheet"
    DATABASE = "database"
    API_MANAGER = "api_manager"
    LLM_GENERATED = "llm_generated"
    MANUAL = "manual"


# =============================================================================
# WEEKLY REPORT MODEL
# =============================================================================

class WeeklyReport(Base):
    """Main weekly report record"""
    __tablename__ = "weekly_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_date = Column(Date, nullable=False, index=True)
    week_ending = Column(Date, nullable=False)

    # Status tracking
    status = Column(Enum(ReportStatus), default=ReportStatus.DRAFT)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    published_at = Column(DateTime, nullable=True)

    # File paths
    document_path = Column(String(500), nullable=True)
    dropbox_path = Column(String(500), nullable=True)

    # Generation metadata
    generation_time_seconds = Column(Float, nullable=True)
    data_sources_used = Column(JSON, nullable=True)
    placeholders_count = Column(Integer, default=0)
    llm_estimates_count = Column(Integer, default=0)

    # Quality metrics
    data_completeness_score = Column(Float, nullable=True)  # 0-100
    has_all_commodities = Column(Boolean, default=False)
    has_price_tables = Column(Boolean, default=False)

    # Error tracking
    has_errors = Column(Boolean, default=False)
    error_summary = Column(Text, nullable=True)

    # Relationships
    sections = relationship("ReportSection", back_populates="report", cascade="all, delete-orphan")
    price_data = relationship("PriceData", back_populates="report", cascade="all, delete-orphan")
    internal_data = relationship("InternalData", back_populates="report", cascade="all, delete-orphan")
    questions = relationship("Question", back_populates="report", cascade="all, delete-orphan")
    metadata_entries = relationship("ReportMetadata", back_populates="report", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('report_date', name='uq_report_date'),
        Index('ix_weekly_reports_status', 'status'),
    )

    def __repr__(self):
        return f"<WeeklyReport(id={self.id}, date={self.report_date}, status={self.status})>"


# =============================================================================
# REPORT SECTION MODEL
# =============================================================================

class ReportSection(Base):
    """Individual section of a report"""
    __tablename__ = "report_sections"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(Integer, ForeignKey("weekly_reports.id"), nullable=False)

    # Section identification
    section_name = Column(String(100), nullable=False)  # e.g., "executive_summary", "corn", "wheat"
    section_order = Column(Integer, default=0)

    # Content
    heading = Column(String(200), nullable=True)
    content = Column(Text, nullable=True)

    # Structured data for the section
    bullish_factors = Column(JSON, nullable=True)  # List of bullish points
    bearish_factors = Column(JSON, nullable=True)  # List of bearish points
    swing_factors = Column(JSON, nullable=True)  # List of catalysts

    # Metadata
    data_source = Column(Enum(DataSourceType), nullable=True)
    has_placeholder = Column(Boolean, default=False)
    is_llm_generated = Column(Boolean, default=False)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    report = relationship("WeeklyReport", back_populates="sections")

    __table_args__ = (
        Index('ix_report_sections_report_section', 'report_id', 'section_name'),
    )

    def __repr__(self):
        return f"<ReportSection(id={self.id}, section={self.section_name})>"


# =============================================================================
# PRICE DATA MODEL
# =============================================================================

class PriceData(Base):
    """Price data used in the report"""
    __tablename__ = "price_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(Integer, ForeignKey("weekly_reports.id"), nullable=False)

    # Series identification
    series_id = Column(String(100), nullable=False)
    series_name = Column(String(200), nullable=True)
    commodity = Column(String(50), nullable=True)

    # Price values
    current_price = Column(Float, nullable=True)
    current_date = Column(Date, nullable=True)
    week_ago_price = Column(Float, nullable=True)
    week_ago_date = Column(Date, nullable=True)
    year_ago_price = Column(Float, nullable=True)
    year_ago_date = Column(Date, nullable=True)

    # Calculated changes
    week_change = Column(Float, nullable=True)
    week_change_pct = Column(Float, nullable=True)
    year_change = Column(Float, nullable=True)
    year_change_pct = Column(Float, nullable=True)

    # Additional data
    unit = Column(String(20), nullable=True)
    source = Column(String(100), nullable=True)

    # Metadata
    fetched_at = Column(DateTime, default=datetime.utcnow)
    is_estimate = Column(Boolean, default=False)

    # Relationships
    report = relationship("WeeklyReport", back_populates="price_data")

    __table_args__ = (
        Index('ix_price_data_report_series', 'report_id', 'series_id'),
    )

    def __repr__(self):
        return f"<PriceData(id={self.id}, series={self.series_id}, price={self.current_price})>"


# =============================================================================
# INTERNAL DATA MODEL
# =============================================================================

class InternalData(Base):
    """Internal HB data snapshot used in the report"""
    __tablename__ = "internal_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(Integer, ForeignKey("weekly_reports.id"), nullable=False)

    # Data identification
    commodity = Column(String(50), nullable=False)
    data_type = Column(String(100), nullable=False)  # e.g., "supply_demand", "forecast"
    marketing_year = Column(String(20), nullable=True)  # e.g., "2024/25"

    # Values
    field_name = Column(String(100), nullable=False)
    value = Column(Float, nullable=True)
    previous_value = Column(Float, nullable=True)  # Last week's value
    usda_value = Column(Float, nullable=True)  # USDA comparison

    # Change tracking
    change_from_previous = Column(Float, nullable=True)
    change_from_usda = Column(Float, nullable=True)

    # Metadata
    unit = Column(String(20), nullable=True)
    source = Column(Enum(DataSourceType), default=DataSourceType.INTERNAL_SPREADSHEET)
    source_file = Column(String(200), nullable=True)
    fetched_at = Column(DateTime, default=datetime.utcnow)

    is_estimate = Column(Boolean, default=False)
    is_placeholder = Column(Boolean, default=False)

    # Relationships
    report = relationship("WeeklyReport", back_populates="internal_data")

    __table_args__ = (
        Index('ix_internal_data_report_commodity', 'report_id', 'commodity'),
    )

    def __repr__(self):
        return f"<InternalData(id={self.id}, commodity={self.commodity}, field={self.field_name})>"


# =============================================================================
# QUESTION MODEL
# =============================================================================

class Question(Base):
    """Questions raised during report generation for human input"""
    __tablename__ = "questions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(Integer, ForeignKey("weekly_reports.id"), nullable=False)

    # Question details
    question_text = Column(Text, nullable=False)
    context = Column(Text, nullable=True)  # What triggered this question
    category = Column(String(50), nullable=True)  # e.g., "missing_data", "validation", "forecast"
    commodity = Column(String(50), nullable=True)  # Related commodity if applicable

    # Status tracking
    status = Column(Enum(QuestionStatus), default=QuestionStatus.PENDING)
    asked_at = Column(DateTime, default=datetime.utcnow)
    answered_at = Column(DateTime, nullable=True)
    timeout_at = Column(DateTime, nullable=True)

    # Response
    answer = Column(Text, nullable=True)
    answered_by = Column(String(100), nullable=True)

    # Action taken
    action_taken = Column(String(200), nullable=True)  # What the agent did with the answer
    placeholder_used = Column(Boolean, default=False)  # Was a placeholder used due to timeout?

    # Relationships
    report = relationship("WeeklyReport", back_populates="questions")

    __table_args__ = (
        Index('ix_questions_status', 'status'),
        Index('ix_questions_report', 'report_id'),
    )

    def __repr__(self):
        return f"<Question(id={self.id}, status={self.status}, commodity={self.commodity})>"


# =============================================================================
# REPORT METADATA MODEL
# =============================================================================

class ReportMetadata(Base):
    """Additional metadata for reports"""
    __tablename__ = "report_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_id = Column(Integer, ForeignKey("weekly_reports.id"), nullable=False)

    # Metadata key-value
    key = Column(String(100), nullable=False)
    value = Column(Text, nullable=True)

    # Categorization
    category = Column(String(50), nullable=True)  # e.g., "data_source", "assumption", "llm_forecast"

    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    report = relationship("WeeklyReport", back_populates="metadata_entries")

    __table_args__ = (
        Index('ix_report_metadata_report_key', 'report_id', 'key'),
    )

    def __repr__(self):
        return f"<ReportMetadata(id={self.id}, key={self.key})>"


# =============================================================================
# DATABASE INITIALIZATION
# =============================================================================

def init_database(connection_string: str, echo: bool = False) -> Tuple[Any, sessionmaker]:
    """
    Initialize database connection and create tables

    Args:
        connection_string: SQLAlchemy connection string
        echo: Whether to echo SQL statements

    Returns:
        Tuple of (engine, session_factory)
    """
    logger.info(f"Initializing database: {connection_string.split('@')[-1] if '@' in connection_string else connection_string}")

    engine = create_engine(
        connection_string,
        echo=echo,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
    )

    # Create all tables
    Base.metadata.create_all(engine)

    # Create session factory
    session_factory = sessionmaker(bind=engine)

    logger.info("Database initialized successfully")

    return engine, session_factory


def get_session(session_factory: sessionmaker) -> Session:
    """Get a new database session"""
    return session_factory()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_latest_report(session: Session) -> Optional[WeeklyReport]:
    """Get the most recent weekly report"""
    return session.query(WeeklyReport).order_by(WeeklyReport.report_date.desc()).first()


def get_report_by_date(session: Session, report_date: date) -> Optional[WeeklyReport]:
    """Get report by specific date"""
    return session.query(WeeklyReport).filter(WeeklyReport.report_date == report_date).first()


def get_pending_questions(session: Session, report_id: int = None) -> List[Question]:
    """Get all pending questions, optionally filtered by report"""
    query = session.query(Question).filter(Question.status == QuestionStatus.PENDING)
    if report_id:
        query = query.filter(Question.report_id == report_id)
    return query.all()


def create_question(
    session: Session,
    report_id: int,
    question_text: str,
    category: str = None,
    commodity: str = None,
    context: str = None
) -> Question:
    """Create a new question for escalation"""
    question = Question(
        report_id=report_id,
        question_text=question_text,
        category=category,
        commodity=commodity,
        context=context,
    )
    session.add(question)
    session.commit()
    return question


def answer_question(
    session: Session,
    question_id: int,
    answer: str,
    answered_by: str = None
) -> Question:
    """Record an answer to a question"""
    question = session.query(Question).get(question_id)
    if question:
        question.answer = answer
        question.answered_by = answered_by
        question.answered_at = datetime.utcnow()
        question.status = QuestionStatus.ANSWERED
        session.commit()
    return question
