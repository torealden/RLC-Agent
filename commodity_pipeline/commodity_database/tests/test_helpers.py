"""
Tests for Utility Helper Functions

Unit tests for data parsing, validation, and conversion utilities.
"""

import pytest
from datetime import date
from decimal import Decimal

from ..utils.helpers import (
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


class TestDateParsing:
    """Tests for date parsing functions."""

    def test_parse_date_iso_format(self):
        """Test parsing ISO format dates."""
        assert parse_date("2024-12-01") == date(2024, 12, 1)

    def test_parse_date_us_format(self):
        """Test parsing US format dates."""
        assert parse_date("12/01/2024") == date(2024, 12, 1)

    def test_parse_date_from_date_object(self):
        """Test passing a date object."""
        d = date(2024, 12, 1)
        assert parse_date(d) == d

    def test_parse_date_none(self):
        """Test parsing None returns None."""
        assert parse_date(None) is None

    def test_parse_date_empty_string(self):
        """Test parsing empty string returns None."""
        assert parse_date("") is None

    def test_parse_date_invalid(self):
        """Test parsing invalid date returns None."""
        assert parse_date("not-a-date") is None


class TestMarketingYearParsing:
    """Tests for marketing year parsing."""

    def test_parse_marketing_year_correct_format(self):
        """Test parsing already correct format."""
        assert parse_marketing_year("2024/25") == "2024/25"

    def test_parse_marketing_year_full_years(self):
        """Test parsing full year format."""
        assert parse_marketing_year("2024/2025") == "2024/25"

    def test_parse_marketing_year_single_year(self):
        """Test parsing single year."""
        assert parse_marketing_year("2024") == "2024/25"

    def test_parse_marketing_year_abbreviated(self):
        """Test parsing abbreviated format."""
        assert parse_marketing_year("24/25") == "2024/25"

    def test_get_marketing_year_for_date(self):
        """Test getting marketing year for a date."""
        # Before September (still in previous year)
        assert get_marketing_year_for_date(date(2024, 8, 15), 9) == "2023/24"

        # After September (current year)
        assert get_marketing_year_for_date(date(2024, 9, 15), 9) == "2024/25"


class TestNumericParsing:
    """Tests for numeric parsing functions."""

    def test_parse_numeric_decimal(self):
        """Test parsing Decimal."""
        assert parse_numeric(Decimal("123.45")) == Decimal("123.45")

    def test_parse_numeric_int(self):
        """Test parsing integer."""
        assert parse_numeric(123) == Decimal("123")

    def test_parse_numeric_float(self):
        """Test parsing float."""
        assert parse_numeric(123.45) == Decimal("123.45")

    def test_parse_numeric_string(self):
        """Test parsing string number."""
        assert parse_numeric("123.45") == Decimal("123.45")

    def test_parse_numeric_with_commas(self):
        """Test parsing string with commas."""
        assert parse_numeric("1,234,567.89") == Decimal("1234567.89")

    def test_parse_numeric_parenthetical_negative(self):
        """Test parsing parenthetical negative."""
        assert parse_numeric("(123.45)") == Decimal("-123.45")

    def test_parse_numeric_percentage(self):
        """Test parsing percentage."""
        assert parse_numeric("45.5%") == Decimal("45.5")

    def test_parse_numeric_currency(self):
        """Test parsing currency."""
        assert parse_numeric("$1,234.56") == Decimal("1234.56")

    def test_parse_numeric_na(self):
        """Test parsing NA values."""
        assert parse_numeric("N/A") is None
        assert parse_numeric("na") is None
        assert parse_numeric("-") is None

    def test_parse_numeric_with_default(self):
        """Test parsing with default value."""
        assert parse_numeric("invalid", default=Decimal("0")) == Decimal("0")


class TestMathOperations:
    """Tests for math operation functions."""

    def test_safe_divide_normal(self):
        """Test normal division."""
        assert safe_divide(10, 2) == 5.0

    def test_safe_divide_by_zero(self):
        """Test division by zero returns default."""
        assert safe_divide(10, 0) == 0.0
        assert safe_divide(10, 0, default=-1) == -1

    def test_calculate_percent_change(self):
        """Test percent change calculation."""
        assert calculate_percent_change(110, 100) == 10.0
        assert calculate_percent_change(90, 100) == -10.0

    def test_calculate_percent_change_from_zero(self):
        """Test percent change from zero returns None."""
        assert calculate_percent_change(100, 0) is None


class TestNameNormalization:
    """Tests for name normalization functions."""

    def test_normalize_commodity_name(self):
        """Test commodity name normalization."""
        assert normalize_commodity_name("corn") == "Corn"
        assert normalize_commodity_name("SOYBEANS") == "Soybeans"
        assert normalize_commodity_name("maize") == "Corn"
        assert normalize_commodity_name("sbm") == "Soybean Meal"

    def test_normalize_field_name(self):
        """Test field name normalization."""
        assert normalize_field_name("Beginning Stocks") == "beginning_stocks"
        assert normalize_field_name("beg_stocks") == "beginning_stocks"
        assert normalize_field_name("S/U") == "stocks_to_use"
        assert normalize_field_name("Total Exports") == "exports"

    def test_normalize_country_name(self):
        """Test country name normalization."""
        assert normalize_country_name("usa") == "United States"
        assert normalize_country_name("US") == "United States"
        assert normalize_country_name("brasil") == "Brazil"


class TestUnitConversion:
    """Tests for unit conversion functions."""

    def test_convert_same_unit(self):
        """Test conversion between same units."""
        assert convert_units(100, "mt", "mt") == 100.0

    def test_convert_weight_units(self):
        """Test weight unit conversions."""
        # Metric tons to pounds
        result = convert_units(1, "mt", "lb")
        assert result == pytest.approx(2204.62, abs=0.01)

        # Short tons to metric tons
        result = convert_units(1, "st", "mt")
        assert result == pytest.approx(0.907, abs=0.01)

    def test_bushels_to_metric_tons(self):
        """Test bushel to metric ton conversion."""
        # Corn: 56 lbs/bushel
        result = bushels_to_metric_tons(1000, "corn")
        assert result == pytest.approx(25.4, abs=0.1)

    def test_metric_tons_to_bushels(self):
        """Test metric ton to bushel conversion."""
        # Corn: 56 lbs/bushel
        result = metric_tons_to_bushels(25.4, "corn")
        assert result == pytest.approx(1000, abs=10)


class TestChecksums:
    """Tests for checksum functions."""

    def test_calculate_checksum_dict(self):
        """Test checksum calculation for dict."""
        data = {"a": 1, "b": 2}
        checksum = calculate_checksum(data)

        assert len(checksum) == 64  # SHA256 hex length
        assert checksum == calculate_checksum(data)  # Deterministic

    def test_calculate_checksum_list(self):
        """Test checksum calculation for list."""
        data = [1, 2, 3]
        checksum = calculate_checksum(data)

        assert len(checksum) == 64

    def test_calculate_row_hash(self):
        """Test row hash calculation."""
        row = {"a": 1, "b": 2, "c": 3}

        hash1 = calculate_row_hash(row)
        hash2 = calculate_row_hash(row, key_fields=["a", "b"])

        assert hash1 != hash2  # Different fields produce different hashes


class TestValidation:
    """Tests for validation functions."""

    def test_validate_percentage_valid(self):
        """Test valid percentage values."""
        is_valid, error = validate_percentage(50)
        assert is_valid
        assert error is None

    def test_validate_percentage_negative(self):
        """Test negative percentage fails."""
        is_valid, error = validate_percentage(-10)
        assert not is_valid
        assert "negative" in error.lower()

    def test_validate_percentage_over_100(self):
        """Test percentage over 100 fails by default."""
        is_valid, error = validate_percentage(110)
        assert not is_valid

    def test_validate_percentage_over_100_allowed(self):
        """Test percentage over 100 can be allowed."""
        is_valid, error = validate_percentage(110, allow_over_100=True)
        assert is_valid

    def test_validate_positive(self):
        """Test positive value validation."""
        is_valid, error = validate_positive(10)
        assert is_valid

        is_valid, error = validate_positive(-10)
        assert not is_valid

    def test_validate_date_range_valid(self):
        """Test valid date range."""
        is_valid, error = validate_date_range(date(2024, 1, 1), date(2024, 12, 31))
        assert is_valid

    def test_validate_date_range_invalid(self):
        """Test invalid date range (start after end)."""
        is_valid, error = validate_date_range(date(2024, 12, 31), date(2024, 1, 1))
        assert not is_valid
        assert "after" in error.lower()
