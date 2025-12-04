"""
Database Agent for Commodity Data Ingestion

This agent handles the ingestion of commodity data from various sources
(spreadsheets, APIs, JSON files) into the commodity database.
Includes validation, duplicate handling, and schema change detection.
"""

import json
import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any, Tuple, Union, Generator
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ..database.models import (
    Base, Commodity, PriceData, FundamentalData, CropProgress, TradeFlow,
    DataLoadLog, QualityAlert, SchemaChange, DataSource,
    DataSourceType, DataFrequency, FlowType, LoadStatus, AlertSeverity,
    CommodityCategory,
    create_tables, get_or_create_commodity, get_or_create_data_source,
    create_load_log, create_quality_alert, create_schema_change_request
)
from ..config.settings import (
    CommodityDatabaseConfig, IngestionMode, ApprovalLevel,
    SUPPLY_DEMAND_FIELD_CATEGORIES, default_config
)


logger = logging.getLogger(__name__)


# =============================================================================
# RESULT DATACLASSES
# =============================================================================

@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    cleaned_data: Optional[Dict[str, Any]] = None


@dataclass
class IngestionResult:
    """Result of data ingestion operation."""
    success: bool
    load_id: Optional[int] = None
    records_read: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_errored: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    quality_alerts: List[Dict[str, Any]] = field(default_factory=list)
    schema_changes_requested: List[Dict[str, Any]] = field(default_factory=list)
    duration_seconds: float = 0.0


@dataclass
class SchemaValidationResult:
    """Result of schema validation against incoming data."""
    is_compatible: bool
    new_fields: List[str] = field(default_factory=list)
    new_commodities: List[str] = field(default_factory=list)
    type_mismatches: List[Dict[str, Any]] = field(default_factory=list)
    suggested_changes: List[str] = field(default_factory=list)


# =============================================================================
# DATABASE AGENT
# =============================================================================

class DatabaseAgent:
    """
    Agent for ingesting commodity data into the database.

    Handles:
    - Price data from various sources (APIs, spreadsheets)
    - Fundamental supply/demand data (Excel files)
    - Crop progress data (USDA NASS)
    - Trade flow data (various sources)

    Features:
    - Validation before insertion
    - Duplicate detection and handling
    - Schema change detection with approval workflow
    - Quality alerting for anomalies
    - Full audit trail via DataLoadLog
    """

    def __init__(
        self,
        config: Optional[CommodityDatabaseConfig] = None,
        session_factory: Optional[sessionmaker] = None
    ):
        """
        Initialize the Database Agent.

        Args:
            config: Configuration object (uses default if not provided)
            session_factory: SQLAlchemy session factory (creates one if not provided)
        """
        self.config = config or default_config
        self._engine = None
        self._session_factory = session_factory
        self._commodity_cache: Dict[str, int] = {}  # name -> commodity_id
        self._source_cache: Dict[str, int] = {}  # name -> source_id

    @property
    def engine(self):
        """Lazy initialization of database engine."""
        if self._engine is None:
            connection_string = self.config.database.get_connection_string()
            engine_kwargs = self.config.database.get_engine_kwargs()
            self._engine = create_engine(connection_string, **engine_kwargs)
            # Ensure tables exist
            create_tables(self._engine)
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory

    def get_session(self) -> Session:
        """Create a new database session."""
        return self.session_factory()

    # =========================================================================
    # COMMODITY MANAGEMENT
    # =========================================================================

    def get_or_create_commodity(
        self,
        session: Session,
        name: str,
        **kwargs
    ) -> Tuple[Commodity, bool]:
        """
        Get existing commodity or create new one.

        Args:
            session: Database session
            name: Commodity name
            **kwargs: Additional commodity fields

        Returns:
            Tuple of (Commodity, was_created)
        """
        # Check cache first
        if name in self._commodity_cache:
            commodity = session.query(Commodity).get(self._commodity_cache[name])
            if commodity:
                return commodity, False

        # Query database
        commodity = session.query(Commodity).filter_by(name=name).first()
        if commodity:
            self._commodity_cache[name] = commodity.commodity_id
            return commodity, False

        # Check if we should auto-create
        if not self.config.ingestion.auto_add_commodities:
            if self.config.ingestion.approval_level == ApprovalLevel.ALWAYS_ASK:
                # Create schema change request
                self._request_schema_change(
                    session,
                    change_type="new_commodity",
                    table_name="commodities",
                    proposed_change=f"Add new commodity: {name}",
                    sample_data=json.dumps(kwargs)
                )
                raise ValueError(f"New commodity '{name}' requires approval. Schema change request created.")

        # Create new commodity
        category = kwargs.pop('category', CommodityCategory.OTHER)
        if isinstance(category, str):
            category = CommodityCategory(category)

        commodity = Commodity(name=name, category=category, **kwargs)
        session.add(commodity)
        session.flush()

        self._commodity_cache[name] = commodity.commodity_id
        logger.info(f"Created new commodity: {name} (id={commodity.commodity_id})")

        return commodity, True

    def get_commodity_id(self, session: Session, name: str) -> Optional[int]:
        """Get commodity ID by name, returns None if not found."""
        if name in self._commodity_cache:
            return self._commodity_cache[name]

        commodity = session.query(Commodity).filter_by(name=name).first()
        if commodity:
            self._commodity_cache[name] = commodity.commodity_id
            return commodity.commodity_id

        return None

    # =========================================================================
    # PRICE DATA INGESTION
    # =========================================================================

    def ingest_price_data(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]],
        source_type: DataSourceType,
        source_name: str,
        mode: IngestionMode = None
    ) -> IngestionResult:
        """
        Ingest price data into the database.

        Args:
            data: Price data as DataFrame, list of dicts, or single dict
            source_type: Type of data source
            source_name: Name of the source (for audit)
            mode: Ingestion mode (full, incremental, upsert)

        Returns:
            IngestionResult with statistics
        """
        mode = mode or self.config.ingestion.default_mode
        start_time = datetime.utcnow()
        result = IngestionResult(success=True)

        # Convert to list of dicts if needed
        records = self._normalize_input_data(data)
        result.records_read = len(records)

        session = self.get_session()
        try:
            # Create load log
            load_log = create_load_log(
                session, "price", source_type, source_name,
                parameters={"mode": mode.value, "record_count": len(records)}
            )
            result.load_id = load_log.load_id

            # Process each record
            for record in records:
                try:
                    outcome = self._process_price_record(
                        session, record, source_type, source_name, load_log.load_id, mode
                    )
                    if outcome == "inserted":
                        result.records_inserted += 1
                    elif outcome == "updated":
                        result.records_updated += 1
                    elif outcome == "skipped":
                        result.records_skipped += 1

                except ValueError as e:
                    result.records_errored += 1
                    result.errors.append(str(e))
                    if not self.config.ingestion.continue_on_error:
                        raise
                    if result.records_errored >= self.config.ingestion.max_errors_before_abort:
                        result.success = False
                        result.errors.append("Max errors reached, aborting")
                        break

            # Commit and finalize
            session.commit()
            load_log.records_read = result.records_read
            load_log.records_inserted = result.records_inserted
            load_log.records_updated = result.records_updated
            load_log.records_skipped = result.records_skipped
            load_log.records_errored = result.records_errored
            load_log.mark_complete(
                LoadStatus.SUCCESS if result.success else LoadStatus.PARTIAL
            )
            session.commit()

        except Exception as e:
            session.rollback()
            result.success = False
            result.errors.append(f"Database error: {str(e)}")
            logger.exception("Price data ingestion failed")

        finally:
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            session.close()

        return result

    def _process_price_record(
        self,
        session: Session,
        record: Dict[str, Any],
        source_type: DataSourceType,
        source_name: str,
        load_id: int,
        mode: IngestionMode
    ) -> str:
        """
        Process a single price record.

        Returns: "inserted", "updated", or "skipped"
        """
        # Validate and clean
        validation = self._validate_price_record(record)
        if not validation.is_valid:
            raise ValueError(f"Validation failed: {', '.join(validation.errors)}")

        cleaned = validation.cleaned_data

        # Get or create commodity
        commodity_name = cleaned.pop('commodity_name', cleaned.pop('commodity', None))
        if not commodity_name:
            raise ValueError("Missing commodity name")

        commodity, _ = self.get_or_create_commodity(session, commodity_name)

        # Check for existing record
        existing = session.query(PriceData).filter_by(
            commodity_id=commodity.commodity_id,
            observation_date=cleaned['observation_date'],
            location=cleaned.get('location', 'National Average'),
            source_report=source_name,
            frequency=cleaned.get('frequency', DataFrequency.DAILY)
        ).first()

        if existing:
            if mode == IngestionMode.FULL:
                # Delete and re-insert
                session.delete(existing)
                session.flush()
            elif mode == IngestionMode.UPSERT:
                # Update existing
                for key, value in cleaned.items():
                    if hasattr(existing, key):
                        setattr(existing, key, value)
                existing.fetched_at = datetime.utcnow()
                existing.load_id = load_id
                return "updated"
            else:  # INCREMENTAL
                return "skipped"

        # Create new record
        price_record = PriceData(
            commodity_id=commodity.commodity_id,
            observation_date=cleaned['observation_date'],
            location=cleaned.get('location', 'National Average'),
            price=cleaned['price'],
            price_unit=cleaned.get('price_unit'),
            open_price=cleaned.get('open_price'),
            high_price=cleaned.get('high_price'),
            low_price=cleaned.get('low_price'),
            close_price=cleaned.get('close_price'),
            volume=cleaned.get('volume'),
            basis=cleaned.get('basis'),
            basis_location=cleaned.get('basis_location'),
            frequency=cleaned.get('frequency', DataFrequency.DAILY),
            source_type=source_type,
            source_report=source_name,
            is_estimate=cleaned.get('is_estimate', False),
            is_preliminary=cleaned.get('is_preliminary', False),
            load_id=load_id
        )
        session.add(price_record)

        return "inserted"

    def _validate_price_record(self, record: Dict[str, Any]) -> ValidationResult:
        """Validate a price record."""
        result = ValidationResult(is_valid=True)
        cleaned = {}

        # Required fields
        for field in ['observation_date', 'price']:
            if field not in record or record[field] is None:
                result.errors.append(f"Missing required field: {field}")
                result.is_valid = False

        if not result.is_valid:
            return result

        # Parse date
        obs_date = record['observation_date']
        if isinstance(obs_date, str):
            try:
                obs_date = datetime.strptime(obs_date, '%Y-%m-%d').date()
            except ValueError:
                try:
                    obs_date = datetime.strptime(obs_date, '%m/%d/%Y').date()
                except ValueError:
                    result.errors.append(f"Invalid date format: {obs_date}")
                    result.is_valid = False
        cleaned['observation_date'] = obs_date

        # Parse price
        price = record['price']
        try:
            price = Decimal(str(price))
            if price < self.config.validation.min_valid_price:
                result.warnings.append(f"Price {price} below minimum threshold")
            if price > self.config.validation.max_valid_price:
                result.warnings.append(f"Price {price} above maximum threshold")
            cleaned['price'] = price
        except (InvalidOperation, ValueError) as e:
            result.errors.append(f"Invalid price value: {price}")
            result.is_valid = False

        # Copy other fields
        for field in ['commodity', 'commodity_name', 'location', 'price_unit',
                      'open_price', 'high_price', 'low_price', 'close_price',
                      'volume', 'basis', 'basis_location', 'frequency',
                      'is_estimate', 'is_preliminary']:
            if field in record and record[field] is not None:
                cleaned[field] = record[field]

        # Parse frequency if present
        if 'frequency' in cleaned and isinstance(cleaned['frequency'], str):
            try:
                cleaned['frequency'] = DataFrequency(cleaned['frequency'])
            except ValueError:
                cleaned['frequency'] = DataFrequency.DAILY

        result.cleaned_data = cleaned
        return result

    # =========================================================================
    # FUNDAMENTAL DATA INGESTION
    # =========================================================================

    def ingest_fundamental_data(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]],
        source_type: DataSourceType,
        source_name: str,
        mode: IngestionMode = None
    ) -> IngestionResult:
        """
        Ingest fundamental (supply/demand) data into the database.

        Supports both wide format (one row per commodity-period with columns for fields)
        and tall format (one row per commodity-period-field).

        Args:
            data: Fundamental data
            source_type: Type of data source
            source_name: Name of the source
            mode: Ingestion mode

        Returns:
            IngestionResult with statistics
        """
        mode = mode or self.config.ingestion.default_mode
        start_time = datetime.utcnow()
        result = IngestionResult(success=True)

        records = self._normalize_input_data(data)
        result.records_read = len(records)

        session = self.get_session()
        try:
            load_log = create_load_log(
                session, "fundamental", source_type, source_name,
                parameters={"mode": mode.value, "record_count": len(records)}
            )
            result.load_id = load_log.load_id

            for record in records:
                try:
                    # Detect format and process
                    if self._is_wide_format(record):
                        outcomes = self._process_wide_fundamental_record(
                            session, record, source_type, source_name, load_log.load_id, mode
                        )
                        for outcome in outcomes:
                            if outcome == "inserted":
                                result.records_inserted += 1
                            elif outcome == "updated":
                                result.records_updated += 1
                            elif outcome == "skipped":
                                result.records_skipped += 1
                    else:
                        outcome = self._process_tall_fundamental_record(
                            session, record, source_type, source_name, load_log.load_id, mode
                        )
                        if outcome == "inserted":
                            result.records_inserted += 1
                        elif outcome == "updated":
                            result.records_updated += 1
                        elif outcome == "skipped":
                            result.records_skipped += 1

                except ValueError as e:
                    result.records_errored += 1
                    result.errors.append(str(e))
                    if not self.config.ingestion.continue_on_error:
                        raise
                    if result.records_errored >= self.config.ingestion.max_errors_before_abort:
                        result.success = False
                        break

            session.commit()
            load_log.records_read = result.records_read
            load_log.records_inserted = result.records_inserted
            load_log.records_updated = result.records_updated
            load_log.records_skipped = result.records_skipped
            load_log.records_errored = result.records_errored
            load_log.mark_complete(
                LoadStatus.SUCCESS if result.success else LoadStatus.PARTIAL
            )
            session.commit()

        except Exception as e:
            session.rollback()
            result.success = False
            result.errors.append(f"Database error: {str(e)}")
            logger.exception("Fundamental data ingestion failed")

        finally:
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            session.close()

        return result

    def _is_wide_format(self, record: Dict[str, Any]) -> bool:
        """Check if record is in wide format (multiple fields as columns)."""
        supply_demand_fields = set(SUPPLY_DEMAND_FIELD_CATEGORIES.keys())
        record_fields = set(record.keys())
        # If record has multiple supply/demand fields, it's wide format
        overlap = record_fields.intersection(supply_demand_fields)
        return len(overlap) >= 2

    def _process_wide_fundamental_record(
        self,
        session: Session,
        record: Dict[str, Any],
        source_type: DataSourceType,
        source_name: str,
        load_id: int,
        mode: IngestionMode
    ) -> List[str]:
        """
        Process a wide-format fundamental record.

        Converts to tall format and inserts each field separately.
        """
        outcomes = []

        # Extract commodity and period
        commodity_name = record.get('commodity') or record.get('commodity_name')
        period = record.get('period') or record.get('marketing_year') or record.get('year')

        if not commodity_name or not period:
            raise ValueError("Wide format record missing commodity or period")

        commodity, _ = self.get_or_create_commodity(session, commodity_name)

        # Process each supply/demand field
        for field_name, category in SUPPLY_DEMAND_FIELD_CATEGORIES.items():
            if field_name in record and record[field_name] is not None:
                try:
                    value = Decimal(str(record[field_name]))
                except (InvalidOperation, ValueError):
                    continue  # Skip non-numeric fields

                tall_record = {
                    'commodity': commodity_name,
                    'period': str(period),
                    'field_name': field_name,
                    'field_category': category,
                    'value': value,
                    'unit': record.get('unit') or record.get(f'{field_name}_unit'),
                }

                outcome = self._process_tall_fundamental_record(
                    session, tall_record, source_type, source_name, load_id, mode
                )
                outcomes.append(outcome)

        return outcomes

    def _process_tall_fundamental_record(
        self,
        session: Session,
        record: Dict[str, Any],
        source_type: DataSourceType,
        source_name: str,
        load_id: int,
        mode: IngestionMode
    ) -> str:
        """Process a tall-format fundamental record."""
        # Validate
        validation = self._validate_fundamental_record(record)
        if not validation.is_valid:
            raise ValueError(f"Validation failed: {', '.join(validation.errors)}")

        cleaned = validation.cleaned_data

        # Get commodity
        commodity_name = cleaned.pop('commodity_name', cleaned.pop('commodity', None))
        commodity, _ = self.get_or_create_commodity(session, commodity_name)

        # Check for existing
        existing = session.query(FundamentalData).filter_by(
            commodity_id=commodity.commodity_id,
            period=cleaned['period'],
            field_name=cleaned['field_name'],
            source_type=source_type
        ).first()

        if existing:
            if mode == IngestionMode.FULL:
                session.delete(existing)
                session.flush()
            elif mode == IngestionMode.UPSERT:
                # Track previous value if configured
                if self.config.ingestion.track_changes:
                    existing.previous_value = existing.value
                    existing.change_from_previous = cleaned['value'] - existing.value

                existing.value = cleaned['value']
                existing.unit = cleaned.get('unit')
                existing.source_report = source_name
                existing.fetched_at = datetime.utcnow()
                existing.load_id = load_id
                return "updated"
            else:  # INCREMENTAL
                return "skipped"

        # Create new record
        fundamental_record = FundamentalData(
            commodity_id=commodity.commodity_id,
            period=cleaned['period'],
            period_type=cleaned.get('period_type', DataFrequency.MARKETING_YEAR),
            field_name=cleaned['field_name'],
            field_category=cleaned.get('field_category'),
            value=cleaned['value'],
            unit=cleaned.get('unit'),
            source_type=source_type,
            source_report=source_name,
            is_estimate=cleaned.get('is_estimate', False),
            is_forecast=cleaned.get('is_forecast', False),
            load_id=load_id
        )
        session.add(fundamental_record)

        return "inserted"

    def _validate_fundamental_record(self, record: Dict[str, Any]) -> ValidationResult:
        """Validate a fundamental data record."""
        result = ValidationResult(is_valid=True)
        cleaned = {}

        # Required fields
        required = ['period', 'field_name', 'value']
        for field in required:
            if field not in record or record[field] is None:
                result.errors.append(f"Missing required field: {field}")
                result.is_valid = False

        if not result.is_valid:
            return result

        cleaned['period'] = str(record['period'])
        cleaned['field_name'] = self._normalize_field_name(record['field_name'])

        # Parse value
        try:
            cleaned['value'] = Decimal(str(record['value']))
        except (InvalidOperation, ValueError):
            result.errors.append(f"Invalid value: {record['value']}")
            result.is_valid = False

        # Copy optional fields
        for field in ['commodity', 'commodity_name', 'field_category', 'unit',
                      'period_type', 'is_estimate', 'is_forecast']:
            if field in record and record[field] is not None:
                cleaned[field] = record[field]

        # Determine field category if not provided
        if 'field_category' not in cleaned:
            cleaned['field_category'] = SUPPLY_DEMAND_FIELD_CATEGORIES.get(
                cleaned['field_name'], None
            )

        result.cleaned_data = cleaned
        return result

    def _normalize_field_name(self, field_name: str) -> str:
        """Normalize field name using aliases."""
        normalized = field_name.lower().strip().replace(' ', '_').replace('-', '_')
        return self.config.commodities.field_aliases.get(normalized, normalized)

    # =========================================================================
    # CROP PROGRESS INGESTION
    # =========================================================================

    def ingest_crop_progress(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        source_type: DataSourceType = DataSourceType.USDA_NASS,
        source_name: str = "USDA NASS Crop Progress",
        mode: IngestionMode = None
    ) -> IngestionResult:
        """Ingest crop progress/condition data."""
        mode = mode or self.config.ingestion.default_mode
        start_time = datetime.utcnow()
        result = IngestionResult(success=True)

        records = self._normalize_input_data(data)
        result.records_read = len(records)

        session = self.get_session()
        try:
            load_log = create_load_log(
                session, "crop_progress", source_type, source_name,
                parameters={"mode": mode.value, "record_count": len(records)}
            )
            result.load_id = load_log.load_id

            for record in records:
                try:
                    outcome = self._process_crop_progress_record(
                        session, record, source_type, source_name, load_log.load_id, mode
                    )
                    if outcome == "inserted":
                        result.records_inserted += 1
                    elif outcome == "updated":
                        result.records_updated += 1
                    elif outcome == "skipped":
                        result.records_skipped += 1

                except ValueError as e:
                    result.records_errored += 1
                    result.errors.append(str(e))
                    if not self.config.ingestion.continue_on_error:
                        raise

            session.commit()
            load_log.records_read = result.records_read
            load_log.records_inserted = result.records_inserted
            load_log.records_updated = result.records_updated
            load_log.records_skipped = result.records_skipped
            load_log.records_errored = result.records_errored
            load_log.mark_complete(
                LoadStatus.SUCCESS if result.success else LoadStatus.PARTIAL
            )
            session.commit()

        except Exception as e:
            session.rollback()
            result.success = False
            result.errors.append(f"Database error: {str(e)}")
            logger.exception("Crop progress ingestion failed")

        finally:
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            session.close()

        return result

    def _process_crop_progress_record(
        self,
        session: Session,
        record: Dict[str, Any],
        source_type: DataSourceType,
        source_name: str,
        load_id: int,
        mode: IngestionMode
    ) -> str:
        """Process a single crop progress record."""
        commodity_name = record.get('commodity') or record.get('commodity_name')
        if not commodity_name:
            raise ValueError("Missing commodity name")

        commodity, _ = self.get_or_create_commodity(session, commodity_name)

        # Parse week_ending date
        week_ending = record.get('week_ending')
        if isinstance(week_ending, str):
            week_ending = datetime.strptime(week_ending, '%Y-%m-%d').date()

        state = record.get('state')  # None for national

        # Check existing
        existing = session.query(CropProgress).filter_by(
            commodity_id=commodity.commodity_id,
            week_ending=week_ending,
            state=state
        ).first()

        if existing:
            if mode == IngestionMode.UPSERT:
                # Update existing
                for field in ['pct_planted', 'pct_emerged', 'pct_silking', 'pct_blooming',
                              'pct_setting_pods', 'pct_dough', 'pct_dented', 'pct_mature',
                              'pct_harvested', 'pct_very_poor', 'pct_poor', 'pct_fair',
                              'pct_good', 'pct_excellent', 'pct_good_excellent']:
                    if field in record and record[field] is not None:
                        setattr(existing, field, Decimal(str(record[field])))
                existing.fetched_at = datetime.utcnow()
                existing.load_id = load_id
                return "updated"
            elif mode == IngestionMode.INCREMENTAL:
                return "skipped"

        # Create new record
        progress = CropProgress(
            commodity_id=commodity.commodity_id,
            week_ending=week_ending,
            state=state,
            crop_year=record.get('crop_year', week_ending.year),
            week_number=record.get('week_number'),
            source_type=source_type,
            source_report=source_name,
            load_id=load_id
        )

        # Set progress fields
        for field in ['pct_planted', 'pct_emerged', 'pct_silking', 'pct_blooming',
                      'pct_setting_pods', 'pct_dough', 'pct_dented', 'pct_mature',
                      'pct_harvested', 'pct_very_poor', 'pct_poor', 'pct_fair',
                      'pct_good', 'pct_excellent']:
            if field in record and record[field] is not None:
                try:
                    setattr(progress, field, Decimal(str(record[field])))
                except (InvalidOperation, ValueError):
                    pass

        # Calculate derived fields
        if progress.pct_good is not None and progress.pct_excellent is not None:
            progress.pct_good_excellent = progress.pct_good + progress.pct_excellent

        progress.condition_index = progress.calculate_condition_index()

        session.add(progress)
        return "inserted"

    # =========================================================================
    # TRADE FLOW INGESTION
    # =========================================================================

    def ingest_trade_flows(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        source_type: DataSourceType,
        source_name: str,
        mode: IngestionMode = None
    ) -> IngestionResult:
        """Ingest trade flow (import/export) data."""
        mode = mode or self.config.ingestion.default_mode
        start_time = datetime.utcnow()
        result = IngestionResult(success=True)

        records = self._normalize_input_data(data)
        result.records_read = len(records)

        session = self.get_session()
        try:
            load_log = create_load_log(
                session, "trade_flow", source_type, source_name,
                parameters={"mode": mode.value, "record_count": len(records)}
            )
            result.load_id = load_log.load_id

            for record in records:
                try:
                    outcome = self._process_trade_flow_record(
                        session, record, source_type, source_name, load_id, mode
                    )
                    if outcome == "inserted":
                        result.records_inserted += 1
                    elif outcome == "updated":
                        result.records_updated += 1
                    elif outcome == "skipped":
                        result.records_skipped += 1

                except ValueError as e:
                    result.records_errored += 1
                    result.errors.append(str(e))

            session.commit()
            load_log.records_read = result.records_read
            load_log.records_inserted = result.records_inserted
            load_log.records_updated = result.records_updated
            load_log.records_skipped = result.records_skipped
            load_log.records_errored = result.records_errored
            load_log.mark_complete(
                LoadStatus.SUCCESS if result.success else LoadStatus.PARTIAL
            )
            session.commit()

        except Exception as e:
            session.rollback()
            result.success = False
            result.errors.append(f"Database error: {str(e)}")
            logger.exception("Trade flow ingestion failed")

        finally:
            result.duration_seconds = (datetime.utcnow() - start_time).total_seconds()
            session.close()

        return result

    def _process_trade_flow_record(
        self,
        session: Session,
        record: Dict[str, Any],
        source_type: DataSourceType,
        source_name: str,
        load_id: int,
        mode: IngestionMode
    ) -> str:
        """Process a single trade flow record."""
        commodity_name = record.get('commodity') or record.get('commodity_name')
        if not commodity_name:
            raise ValueError("Missing commodity name")

        commodity, _ = self.get_or_create_commodity(session, commodity_name)

        flow_type = record.get('flow_type', 'export')
        if isinstance(flow_type, str):
            flow_type = FlowType(flow_type.lower())

        period_year = int(record['period_year'])
        period_month = record.get('period_month')
        if period_month is not None:
            period_month = int(period_month)

        reporter_country = record.get('reporter_country', 'USA')
        partner_country = record.get('partner_country')
        hs_code = record.get('hs_code')

        # Check existing
        existing = session.query(TradeFlow).filter_by(
            commodity_id=commodity.commodity_id,
            flow_type=flow_type,
            period_year=period_year,
            period_month=period_month,
            reporter_country=reporter_country,
            partner_country=partner_country,
            hs_code=hs_code,
            source_type=source_type
        ).first()

        if existing:
            if mode == IngestionMode.UPSERT:
                for field in ['quantity', 'value', 'quantity_unit', 'value_unit']:
                    if field in record and record[field] is not None:
                        setattr(existing, field, record[field])
                existing.fetched_at = datetime.utcnow()
                existing.load_id = load_id
                return "updated"
            elif mode == IngestionMode.INCREMENTAL:
                return "skipped"

        # Create new record
        trade_flow = TradeFlow(
            commodity_id=commodity.commodity_id,
            flow_type=flow_type,
            period_year=period_year,
            period_month=period_month,
            marketing_year=record.get('marketing_year'),
            reporter_country=reporter_country,
            partner_country=partner_country,
            hs_code=hs_code,
            hs_description=record.get('hs_description'),
            quantity=record.get('quantity'),
            quantity_unit=record.get('quantity_unit'),
            value=record.get('value'),
            value_unit=record.get('value_unit', 'USD'),
            source_type=source_type,
            source_report=source_name,
            is_estimate=record.get('is_estimate', False),
            is_revised=record.get('is_revised', False),
            load_id=load_id
        )
        session.add(trade_flow)

        return "inserted"

    # =========================================================================
    # SCHEMA CHANGE HANDLING
    # =========================================================================

    def validate_schema_compatibility(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]]],
        data_type: str  # "price", "fundamental", "crop_progress", "trade_flow"
    ) -> SchemaValidationResult:
        """
        Validate incoming data against current schema.

        Detects new fields, new commodities, and type mismatches.
        """
        result = SchemaValidationResult(is_compatible=True)
        records = self._normalize_input_data(data)

        session = self.get_session()
        try:
            # Check for new commodities
            commodity_names = set()
            for record in records:
                name = record.get('commodity') or record.get('commodity_name')
                if name:
                    commodity_names.add(name)

            existing_commodities = {
                c.name for c in session.query(Commodity.name).all()
            }
            new_commodities = commodity_names - existing_commodities
            result.new_commodities = list(new_commodities)

            # Check for new fields (for fundamental data)
            if data_type == "fundamental":
                known_fields = set(SUPPLY_DEMAND_FIELD_CATEGORIES.keys())
                for record in records:
                    if self._is_wide_format(record):
                        for key in record.keys():
                            if key not in ['commodity', 'commodity_name', 'period',
                                           'marketing_year', 'year', 'unit']:
                                normalized = self._normalize_field_name(key)
                                if normalized not in known_fields:
                                    result.new_fields.append(key)
                    else:
                        field_name = record.get('field_name')
                        if field_name:
                            normalized = self._normalize_field_name(field_name)
                            if normalized not in known_fields:
                                result.new_fields.append(field_name)

            # Flag as incompatible if new items found and not auto-adding
            if result.new_commodities and not self.config.ingestion.auto_add_commodities:
                result.is_compatible = False
                result.suggested_changes.append(
                    f"Add new commodities: {', '.join(result.new_commodities)}"
                )

            if result.new_fields and not self.config.ingestion.auto_add_fields:
                result.is_compatible = False
                result.suggested_changes.append(
                    f"Add new fields: {', '.join(set(result.new_fields))}"
                )

        finally:
            session.close()

        return result

    def _request_schema_change(
        self,
        session: Session,
        change_type: str,
        table_name: str,
        proposed_change: str,
        **kwargs
    ) -> SchemaChange:
        """Create a schema change request for human review."""
        return create_schema_change_request(
            session, change_type, table_name, proposed_change, **kwargs
        )

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def _normalize_input_data(
        self,
        data: Union[pd.DataFrame, List[Dict[str, Any]], Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Convert input data to list of dictionaries."""
        if isinstance(data, pd.DataFrame):
            return data.to_dict('records')
        elif isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    def calculate_checksum(self, data: Any) -> str:
        """Calculate SHA256 checksum of data for verification."""
        if isinstance(data, pd.DataFrame):
            data_str = data.to_json()
        elif isinstance(data, (list, dict)):
            data_str = json.dumps(data, sort_keys=True, default=str)
        else:
            data_str = str(data)

        return hashlib.sha256(data_str.encode()).hexdigest()

    def get_load_history(
        self,
        load_type: str = None,
        source_name: str = None,
        limit: int = 100
    ) -> List[DataLoadLog]:
        """Get recent data load history."""
        session = self.get_session()
        try:
            query = session.query(DataLoadLog)

            if load_type:
                query = query.filter_by(load_type=load_type)
            if source_name:
                query = query.filter_by(source_name=source_name)

            return query.order_by(DataLoadLog.started_at.desc()).limit(limit).all()
        finally:
            session.close()

    def get_quality_alerts(
        self,
        severity: AlertSeverity = None,
        resolved: bool = False,
        limit: int = 100
    ) -> List[QualityAlert]:
        """Get quality alerts."""
        session = self.get_session()
        try:
            query = session.query(QualityAlert).filter_by(is_resolved=resolved)

            if severity:
                query = query.filter(QualityAlert.severity >= severity)

            return query.order_by(QualityAlert.created_at.desc()).limit(limit).all()
        finally:
            session.close()

    def get_pending_schema_changes(self) -> List[SchemaChange]:
        """Get pending schema change requests."""
        session = self.get_session()
        try:
            return session.query(SchemaChange).filter_by(status="pending").all()
        finally:
            session.close()
