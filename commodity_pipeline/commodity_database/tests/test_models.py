"""
Tests for Commodity Database Models

Unit tests for SQLAlchemy models and database utilities.
"""

import pytest
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..database.models import (
    Base, Commodity, Location, DataSource, PriceData, FundamentalData,
    CropProgress, TradeFlow, DataLoadLog, QualityAlert, SchemaChange,
    DataFrequency, DataSourceType, CommodityCategory, FlowType, LoadStatus,
    AlertSeverity, init_database, create_tables, get_session_factory,
    get_or_create_commodity, get_or_create_data_source, create_load_log,
    create_quality_alert, get_latest_price, get_fundamental_data_for_period
)


@pytest.fixture
def engine():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    create_tables(engine)
    return engine


@pytest.fixture
def session(engine):
    """Create a database session for testing."""
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


class TestCommodityModel:
    """Tests for the Commodity model."""

    def test_create_commodity(self, session):
        """Test creating a commodity."""
        commodity = Commodity(
            name="Corn",
            symbol="ZC",
            category=CommodityCategory.GRAIN,
            price_unit="cents/bu",
            quantity_unit="million bushels",
            marketing_year_start_month=9,
            bushel_weight_lbs=Decimal("56.0")
        )
        session.add(commodity)
        session.commit()

        assert commodity.commodity_id is not None
        assert commodity.name == "Corn"
        assert commodity.category == CommodityCategory.GRAIN

    def test_get_marketing_year(self, session):
        """Test marketing year calculation."""
        commodity = Commodity(
            name="Corn",
            marketing_year_start_month=9,
            marketing_year_start_day=1
        )
        session.add(commodity)
        session.commit()

        # Before marketing year start
        assert commodity.get_marketing_year(date(2024, 8, 15)) == "2023/24"

        # After marketing year start
        assert commodity.get_marketing_year(date(2024, 9, 15)) == "2024/25"

    def test_commodity_unique_name(self, session):
        """Test that commodity names must be unique."""
        commodity1 = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity1)
        session.commit()

        commodity2 = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity2)

        with pytest.raises(Exception):  # IntegrityError
            session.commit()


class TestPriceDataModel:
    """Tests for the PriceData model."""

    def test_create_price_data(self, session):
        """Test creating price data."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        price = PriceData(
            commodity_id=commodity.commodity_id,
            observation_date=date(2024, 12, 1),
            location="National Average",
            price=Decimal("450.50"),
            price_unit="cents/bu",
            frequency=DataFrequency.DAILY,
            source_type=DataSourceType.USDA_AMS,
            source_report="USDA Grain Report"
        )
        session.add(price)
        session.commit()

        assert price.id is not None
        assert price.price == Decimal("450.50")

    def test_price_data_unique_constraint(self, session):
        """Test unique constraint on price data."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        # First price
        price1 = PriceData(
            commodity_id=commodity.commodity_id,
            observation_date=date(2024, 12, 1),
            location="National Average",
            price=Decimal("450.50"),
            source_type=DataSourceType.USDA_AMS,
            source_report="Report1",
            frequency=DataFrequency.DAILY
        )
        session.add(price1)
        session.commit()

        # Duplicate price (same date, location, source)
        price2 = PriceData(
            commodity_id=commodity.commodity_id,
            observation_date=date(2024, 12, 1),
            location="National Average",
            price=Decimal("451.00"),
            source_type=DataSourceType.USDA_AMS,
            source_report="Report1",
            frequency=DataFrequency.DAILY
        )
        session.add(price2)

        with pytest.raises(Exception):  # IntegrityError
            session.commit()


class TestFundamentalDataModel:
    """Tests for the FundamentalData model."""

    def test_create_fundamental_data(self, session):
        """Test creating fundamental data."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        fundamental = FundamentalData(
            commodity_id=commodity.commodity_id,
            period="2024/25",
            period_type=DataFrequency.MARKETING_YEAR,
            field_name="production",
            field_category="supply",
            value=Decimal("15200.5"),
            unit="million bushels",
            source_type=DataSourceType.USDA_OFFICIAL
        )
        session.add(fundamental)
        session.commit()

        assert fundamental.id is not None
        assert fundamental.value == Decimal("15200.5")

    def test_fundamental_data_with_comparison(self, session):
        """Test fundamental data with comparison values."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        fundamental = FundamentalData(
            commodity_id=commodity.commodity_id,
            period="2024/25",
            field_name="ending_stocks",
            value=Decimal("1500"),
            previous_value=Decimal("1400"),
            change_from_previous=Decimal("100"),
            usda_value=Decimal("1480"),
            change_from_usda=Decimal("20"),
            source_type=DataSourceType.INTERNAL_ESTIMATE,
            is_estimate=True
        )
        session.add(fundamental)
        session.commit()

        assert fundamental.change_from_previous == Decimal("100")
        assert fundamental.is_estimate is True


class TestCropProgressModel:
    """Tests for the CropProgress model."""

    def test_create_crop_progress(self, session):
        """Test creating crop progress data."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        progress = CropProgress(
            commodity_id=commodity.commodity_id,
            week_ending=date(2024, 6, 15),
            crop_year=2024,
            pct_planted=Decimal("95.0"),
            pct_emerged=Decimal("85.0"),
            pct_good=Decimal("45.0"),
            pct_excellent=Decimal("25.0"),
            source_type=DataSourceType.USDA_NASS
        )
        session.add(progress)
        session.commit()

        assert progress.id is not None

    def test_calculate_condition_index(self, session):
        """Test condition index calculation."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        progress = CropProgress(
            commodity_id=commodity.commodity_id,
            week_ending=date(2024, 6, 15),
            crop_year=2024,
            pct_very_poor=Decimal("5.0"),
            pct_poor=Decimal("10.0"),
            pct_fair=Decimal("25.0"),
            pct_good=Decimal("40.0"),
            pct_excellent=Decimal("20.0"),
            source_type=DataSourceType.USDA_NASS
        )

        index = progress.calculate_condition_index()
        # (1*5 + 2*10 + 3*25 + 4*40 + 5*20) / 100 = 3.6
        assert index == pytest.approx(3.6, abs=0.01)


class TestTradeFlowModel:
    """Tests for the TradeFlow model."""

    def test_create_trade_flow(self, session):
        """Test creating trade flow data."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        trade = TradeFlow(
            commodity_id=commodity.commodity_id,
            flow_type=FlowType.EXPORT,
            period_year=2024,
            period_month=11,
            reporter_country="United States",
            partner_country="Mexico",
            quantity=Decimal("1500000"),
            quantity_unit="metric tons",
            value=Decimal("450000000"),
            value_unit="USD",
            source_type=DataSourceType.USDA_FAS
        )
        session.add(trade)
        session.commit()

        assert trade.id is not None
        assert trade.flow_type == FlowType.EXPORT


class TestDataLoadLogModel:
    """Tests for the DataLoadLog model."""

    def test_create_load_log(self, session):
        """Test creating a load log."""
        log = DataLoadLog(
            load_type="price",
            source_type=DataSourceType.USDA_AMS,
            source_name="USDA Grain Prices",
            records_read=100,
            records_inserted=95,
            records_skipped=5,
            status=LoadStatus.SUCCESS
        )
        session.add(log)
        session.commit()

        assert log.load_id is not None
        assert log.status == LoadStatus.SUCCESS

    def test_load_log_mark_complete(self, session):
        """Test marking a load log as complete."""
        log = DataLoadLog(
            load_type="price",
            source_type=DataSourceType.USDA_AMS,
            source_name="Test",
            records_read=100
        )
        session.add(log)
        session.flush()

        log.mark_complete(LoadStatus.PARTIAL, "Some records failed")

        assert log.status == LoadStatus.PARTIAL
        assert log.error_message == "Some records failed"
        assert log.completed_at is not None


class TestQualityAlertModel:
    """Tests for the QualityAlert model."""

    def test_create_quality_alert(self, session):
        """Test creating a quality alert."""
        alert = QualityAlert(
            alert_type="outlier",
            severity=AlertSeverity.WARNING,
            table_name="price_data",
            message="Price outlier detected: 1000 (z-score: 4.5)"
        )
        session.add(alert)
        session.commit()

        assert alert.alert_id is not None
        assert alert.is_resolved is False


class TestUtilityFunctions:
    """Tests for database utility functions."""

    def test_get_or_create_commodity_new(self, session):
        """Test creating a new commodity."""
        commodity = get_or_create_commodity(
            session, "Wheat", category=CommodityCategory.GRAIN
        )
        session.commit()

        assert commodity.name == "Wheat"
        assert commodity.commodity_id is not None

    def test_get_or_create_commodity_existing(self, session):
        """Test getting an existing commodity."""
        # Create first
        commodity1 = Commodity(name="Wheat", category=CommodityCategory.GRAIN)
        session.add(commodity1)
        session.commit()

        # Get existing
        commodity2 = get_or_create_commodity(session, "Wheat")

        assert commodity2.commodity_id == commodity1.commodity_id

    def test_create_load_log_function(self, session):
        """Test the create_load_log utility function."""
        log = create_load_log(
            session, "fundamental",
            DataSourceType.INTERNAL_SPREADSHEET,
            "Corn Supply Demand.xlsx",
            parameters={"mode": "upsert"}
        )
        session.commit()

        assert log.load_id is not None
        assert "upsert" in log.parameters

    def test_create_quality_alert_function(self, session):
        """Test the create_quality_alert utility function."""
        alert = create_quality_alert(
            session,
            alert_type="missing_data",
            table_name="fundamental_data",
            message="Missing production data for 2024/25",
            severity=AlertSeverity.ERROR
        )
        session.commit()

        assert alert.alert_id is not None
        assert alert.severity == AlertSeverity.ERROR

    def test_get_latest_price(self, session):
        """Test getting the latest price."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        # Add multiple prices
        for i, d in enumerate([date(2024, 12, 1), date(2024, 12, 2), date(2024, 12, 3)]):
            price = PriceData(
                commodity_id=commodity.commodity_id,
                observation_date=d,
                price=Decimal(str(450 + i)),
                source_type=DataSourceType.USDA_AMS,
                source_report=f"Report{i}",
                frequency=DataFrequency.DAILY
            )
            session.add(price)
        session.commit()

        latest = get_latest_price(session, commodity.commodity_id)
        assert latest.observation_date == date(2024, 12, 3)
        assert latest.price == Decimal("452")

    def test_get_fundamental_data_for_period(self, session):
        """Test getting fundamental data for a period."""
        commodity = Commodity(name="Corn", category=CommodityCategory.GRAIN)
        session.add(commodity)
        session.flush()

        # Add fundamental data
        for field, value in [("production", 15200), ("exports", 2100), ("ending_stocks", 1500)]:
            fundamental = FundamentalData(
                commodity_id=commodity.commodity_id,
                period="2024/25",
                field_name=field,
                value=Decimal(str(value)),
                unit="million bushels",
                source_type=DataSourceType.USDA_OFFICIAL
            )
            session.add(fundamental)
        session.commit()

        data = get_fundamental_data_for_period(
            session, commodity.commodity_id, "2024/25"
        )

        assert "production" in data
        assert data["production"]["value"] == 15200.0
        assert data["exports"]["value"] == 2100.0
