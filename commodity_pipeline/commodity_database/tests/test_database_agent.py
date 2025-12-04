"""
Tests for Database Agent

Unit tests for the DatabaseAgent data ingestion functionality.
"""

import pytest
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..database.models import (
    Base, Commodity, PriceData, FundamentalData, CropProgress,
    DataSourceType, DataFrequency, CommodityCategory,
    create_tables
)
from ..agents.database_agent import (
    DatabaseAgent, IngestionResult, ValidationResult
)
from ..config.settings import CommodityDatabaseConfig, IngestionMode


@pytest.fixture
def config():
    """Create a test configuration."""
    config = CommodityDatabaseConfig()
    config.database.sqlite_path = None  # Use in-memory
    config.ingestion.auto_add_commodities = True
    config.ingestion.continue_on_error = True
    return config


@pytest.fixture
def session_factory():
    """Create a session factory for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    create_tables(engine)
    return sessionmaker(bind=engine)


@pytest.fixture
def agent(config, session_factory):
    """Create a database agent for testing."""
    agent = DatabaseAgent(config, session_factory)
    agent._engine = session_factory.kw['bind']
    return agent


class TestDatabaseAgentInit:
    """Tests for DatabaseAgent initialization."""

    def test_agent_creation(self, config):
        """Test creating a database agent."""
        agent = DatabaseAgent(config)
        assert agent.config == config

    def test_lazy_engine_creation(self, config):
        """Test that engine is created lazily."""
        agent = DatabaseAgent(config)
        assert agent._engine is None
        # Access engine property
        _ = agent.engine
        assert agent._engine is not None


class TestPriceDataIngestion:
    """Tests for price data ingestion."""

    def test_ingest_single_price_record(self, agent):
        """Test ingesting a single price record."""
        data = {
            "commodity": "Corn",
            "observation_date": "2024-12-01",
            "price": 450.50,
            "location": "National Average"
        }

        result = agent.ingest_price_data(
            data,
            DataSourceType.USDA_AMS,
            "Test Report"
        )

        assert result.success
        assert result.records_inserted == 1
        assert result.records_errored == 0

    def test_ingest_price_dataframe(self, agent):
        """Test ingesting price data from DataFrame."""
        df = pd.DataFrame({
            "commodity": ["Corn", "Corn", "Corn"],
            "observation_date": ["2024-12-01", "2024-12-02", "2024-12-03"],
            "price": [450.50, 452.00, 448.75],
            "location": ["National Average"] * 3
        })

        result = agent.ingest_price_data(
            df,
            DataSourceType.USDA_AMS,
            "Test Report"
        )

        assert result.success
        assert result.records_inserted == 3

    def test_ingest_price_list_of_dicts(self, agent):
        """Test ingesting price data from list of dictionaries."""
        data = [
            {"commodity": "Corn", "observation_date": "2024-12-01", "price": 450.50},
            {"commodity": "Wheat", "observation_date": "2024-12-01", "price": 580.25},
        ]

        result = agent.ingest_price_data(
            data,
            DataSourceType.USDA_AMS,
            "Test Report"
        )

        assert result.success
        assert result.records_inserted == 2

    def test_ingest_price_with_validation_error(self, agent):
        """Test that invalid records are handled."""
        data = [
            {"commodity": "Corn", "observation_date": "2024-12-01", "price": 450.50},
            {"commodity": "Wheat", "observation_date": "invalid-date", "price": 580.25},
        ]

        result = agent.ingest_price_data(
            data,
            DataSourceType.USDA_AMS,
            "Test Report"
        )

        assert result.records_inserted == 1
        assert result.records_errored == 1
        assert len(result.errors) > 0

    def test_ingest_price_upsert_mode(self, agent):
        """Test upserting price data."""
        # First insert
        data1 = {"commodity": "Corn", "observation_date": "2024-12-01", "price": 450.50}
        result1 = agent.ingest_price_data(
            data1, DataSourceType.USDA_AMS, "Report1", mode=IngestionMode.UPSERT
        )
        assert result1.records_inserted == 1

        # Update with same key
        data2 = {"commodity": "Corn", "observation_date": "2024-12-01", "price": 455.00}
        result2 = agent.ingest_price_data(
            data2, DataSourceType.USDA_AMS, "Report1", mode=IngestionMode.UPSERT
        )
        assert result2.records_updated == 1

    def test_ingest_price_incremental_mode(self, agent):
        """Test incremental mode skips existing records."""
        # First insert
        data = {"commodity": "Corn", "observation_date": "2024-12-01", "price": 450.50}
        result1 = agent.ingest_price_data(
            data, DataSourceType.USDA_AMS, "Report1", mode=IngestionMode.INCREMENTAL
        )
        assert result1.records_inserted == 1

        # Try to insert same record (should skip)
        result2 = agent.ingest_price_data(
            data, DataSourceType.USDA_AMS, "Report1", mode=IngestionMode.INCREMENTAL
        )
        assert result2.records_skipped == 1


class TestFundamentalDataIngestion:
    """Tests for fundamental data ingestion."""

    def test_ingest_tall_format(self, agent):
        """Test ingesting tall-format fundamental data."""
        data = [
            {"commodity": "Corn", "period": "2024/25", "field_name": "production", "value": 15200},
            {"commodity": "Corn", "period": "2024/25", "field_name": "exports", "value": 2100},
            {"commodity": "Corn", "period": "2024/25", "field_name": "ending_stocks", "value": 1500},
        ]

        result = agent.ingest_fundamental_data(
            data,
            DataSourceType.USDA_OFFICIAL,
            "WASDE December 2024"
        )

        assert result.success
        assert result.records_inserted == 3

    def test_ingest_wide_format(self, agent):
        """Test ingesting wide-format fundamental data."""
        data = {
            "commodity": "Corn",
            "period": "2024/25",
            "production": 15200,
            "exports": 2100,
            "ending_stocks": 1500,
            "unit": "million bushels"
        }

        result = agent.ingest_fundamental_data(
            data,
            DataSourceType.INTERNAL_SPREADSHEET,
            "Corn Supply Demand.xlsx"
        )

        assert result.success
        assert result.records_inserted == 3  # 3 fields converted from wide format

    def test_ingest_fundamental_with_comparison(self, agent):
        """Test ingesting fundamental data with comparison values."""
        data = {
            "commodity": "Corn",
            "period": "2024/25",
            "field_name": "ending_stocks",
            "value": 1500,
            "previous_value": 1400,
            "usda_value": 1480,
            "is_estimate": True
        }

        result = agent.ingest_fundamental_data(
            data,
            DataSourceType.INTERNAL_ESTIMATE,
            "Internal Estimate"
        )

        assert result.success
        assert result.records_inserted == 1


class TestCropProgressIngestion:
    """Tests for crop progress data ingestion."""

    def test_ingest_crop_progress(self, agent):
        """Test ingesting crop progress data."""
        data = [
            {
                "commodity": "Corn",
                "week_ending": "2024-06-15",
                "crop_year": 2024,
                "pct_planted": 95.0,
                "pct_emerged": 85.0,
                "pct_good": 45.0,
                "pct_excellent": 25.0
            },
            {
                "commodity": "Corn",
                "week_ending": "2024-06-22",
                "crop_year": 2024,
                "pct_planted": 98.0,
                "pct_emerged": 92.0,
                "pct_good": 44.0,
                "pct_excellent": 26.0
            }
        ]

        result = agent.ingest_crop_progress(
            data,
            DataSourceType.USDA_NASS,
            "USDA NASS Crop Progress"
        )

        assert result.success
        assert result.records_inserted == 2

    def test_ingest_crop_progress_calculates_derived(self, agent):
        """Test that derived fields are calculated."""
        data = {
            "commodity": "Corn",
            "week_ending": "2024-06-15",
            "crop_year": 2024,
            "pct_very_poor": 5.0,
            "pct_poor": 10.0,
            "pct_fair": 25.0,
            "pct_good": 40.0,
            "pct_excellent": 20.0
        }

        result = agent.ingest_crop_progress(
            data,
            DataSourceType.USDA_NASS,
            "Test"
        )

        assert result.success

        # Verify derived fields were calculated
        session = agent.get_session()
        try:
            progress = session.query(CropProgress).first()
            assert progress.pct_good_excellent == Decimal("60.0")
            assert progress.condition_index is not None
        finally:
            session.close()


class TestValidation:
    """Tests for data validation."""

    def test_validate_price_record_valid(self, agent):
        """Test validation of a valid price record."""
        record = {
            "observation_date": "2024-12-01",
            "price": 450.50,
            "commodity": "Corn"
        }

        result = agent._validate_price_record(record)

        assert result.is_valid
        assert len(result.errors) == 0

    def test_validate_price_record_missing_fields(self, agent):
        """Test validation fails for missing required fields."""
        record = {
            "commodity": "Corn"
            # Missing observation_date and price
        }

        result = agent._validate_price_record(record)

        assert not result.is_valid
        assert len(result.errors) > 0

    def test_validate_price_record_invalid_date(self, agent):
        """Test validation fails for invalid date."""
        record = {
            "observation_date": "not-a-date",
            "price": 450.50,
            "commodity": "Corn"
        }

        result = agent._validate_price_record(record)

        assert not result.is_valid

    def test_validate_fundamental_record_valid(self, agent):
        """Test validation of a valid fundamental record."""
        record = {
            "period": "2024/25",
            "field_name": "production",
            "value": 15200,
            "commodity": "Corn"
        }

        result = agent._validate_fundamental_record(record)

        assert result.is_valid

    def test_normalize_field_name(self, agent):
        """Test field name normalization."""
        assert agent._normalize_field_name("beg_stocks") == "beginning_stocks"
        assert agent._normalize_field_name("End Stocks") == "ending_stocks"
        assert agent._normalize_field_name("S/U") == "stocks_to_use"
        assert agent._normalize_field_name("production") == "production"


class TestSchemaValidation:
    """Tests for schema validation."""

    def test_validate_schema_compatibility_new_commodity(self, agent):
        """Test detecting new commodities in data."""
        agent.config.ingestion.auto_add_commodities = False

        data = [
            {"commodity": "NewCommodity", "period": "2024/25", "field_name": "production", "value": 100}
        ]

        result = agent.validate_schema_compatibility(data, "fundamental")

        assert "NewCommodity" in result.new_commodities
        assert not result.is_compatible

    def test_validate_schema_compatibility_known_commodity(self, agent):
        """Test that known commodities pass validation."""
        # First add a commodity
        session = agent.get_session()
        try:
            agent.get_or_create_commodity(session, "Corn")
            session.commit()
        finally:
            session.close()

        data = [
            {"commodity": "Corn", "period": "2024/25", "field_name": "production", "value": 100}
        ]

        result = agent.validate_schema_compatibility(data, "fundamental")

        assert len(result.new_commodities) == 0


class TestCommodityManagement:
    """Tests for commodity management."""

    def test_get_or_create_new_commodity(self, agent):
        """Test creating a new commodity."""
        session = agent.get_session()
        try:
            commodity, was_created = agent.get_or_create_commodity(
                session, "Soybeans", category=CommodityCategory.OILSEED
            )
            session.commit()

            assert was_created
            assert commodity.name == "Soybeans"
            assert commodity.commodity_id is not None
        finally:
            session.close()

    def test_get_or_create_existing_commodity(self, agent):
        """Test getting an existing commodity."""
        session = agent.get_session()
        try:
            # Create first
            commodity1, created1 = agent.get_or_create_commodity(session, "Corn")
            session.commit()

            # Get existing
            commodity2, created2 = agent.get_or_create_commodity(session, "Corn")

            assert created1
            assert not created2
            assert commodity1.commodity_id == commodity2.commodity_id
        finally:
            session.close()

    def test_commodity_id_caching(self, agent):
        """Test that commodity IDs are cached."""
        session = agent.get_session()
        try:
            agent.get_or_create_commodity(session, "Wheat")
            session.commit()

            assert "Wheat" in agent._commodity_cache
        finally:
            session.close()


class TestLoadHistory:
    """Tests for load history tracking."""

    def test_get_load_history(self, agent):
        """Test retrieving load history."""
        # Create some loads
        data = {"commodity": "Corn", "observation_date": "2024-12-01", "price": 450.50}
        agent.ingest_price_data(data, DataSourceType.USDA_AMS, "Report1")
        agent.ingest_price_data(data, DataSourceType.USDA_AMS, "Report2")

        history = agent.get_load_history(limit=10)

        assert len(history) >= 2

    def test_get_load_history_filtered(self, agent):
        """Test filtering load history."""
        data = {"commodity": "Corn", "observation_date": "2024-12-01", "price": 450.50}
        agent.ingest_price_data(data, DataSourceType.USDA_AMS, "SpecificReport")

        history = agent.get_load_history(source_name="SpecificReport")

        assert len(history) == 1
        assert history[0].source_name == "SpecificReport"
