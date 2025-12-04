"""
Utility Functions for Commodity Database

Common helper functions for data parsing, validation, and conversion.
"""

import re
import hashlib
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Any, Union, Tuple
import pandas as pd


# =============================================================================
# DATE UTILITIES
# =============================================================================

def parse_date(value: Any, formats: List[str] = None) -> Optional[date]:
    """
    Parse a date from various formats.

    Args:
        value: Date value (string, datetime, date, or None)
        formats: List of date format strings to try

    Returns:
        Parsed date or None if parsing fails
    """
    if value is None:
        return None

    if isinstance(value, date):
        return value

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, (int, float)):
        # Excel serial date
        try:
            return pd.Timestamp.fromordinal(int(value) + 693594).date()
        except (ValueError, OverflowError):
            pass

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None

        # Default formats to try
        if formats is None:
            formats = [
                '%Y-%m-%d',
                '%m/%d/%Y',
                '%m-%d-%Y',
                '%d/%m/%Y',
                '%Y/%m/%d',
                '%b %d, %Y',
                '%B %d, %Y',
                '%d-%b-%Y',
                '%Y%m%d',
            ]

        for fmt in formats:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue

    return None


def parse_marketing_year(value: Any) -> Optional[str]:
    """
    Parse a marketing year string.

    Normalizes various formats to "YYYY/YY" format.

    Args:
        value: Marketing year value

    Returns:
        Normalized marketing year string (e.g., "2024/25")
    """
    if value is None:
        return None

    value = str(value).strip()

    # Already in correct format
    if re.match(r'^\d{4}/\d{2}$', value):
        return value

    # Full year format: "2024/2025"
    match = re.match(r'^(\d{4})/(\d{4})$', value)
    if match:
        year1 = match.group(1)
        year2 = match.group(2)[-2:]
        return f"{year1}/{year2}"

    # Single year: "2024"
    match = re.match(r'^(\d{4})$', value)
    if match:
        year = int(match.group(1))
        return f"{year}/{str(year + 1)[-2:]}"

    # Abbreviated format: "24/25"
    match = re.match(r'^(\d{2})/(\d{2})$', value)
    if match:
        year1 = int(match.group(1))
        # Assume 2000s
        year1 = 2000 + year1 if year1 < 50 else 1900 + year1
        return f"{year1}/{match.group(2)}"

    return value  # Return as-is if no pattern matches


def get_marketing_year_for_date(
    for_date: date,
    start_month: int,
    start_day: int = 1
) -> str:
    """
    Get marketing year string for a given date.

    Args:
        for_date: The date
        start_month: Month when marketing year starts (1-12)
        start_day: Day when marketing year starts

    Returns:
        Marketing year string (e.g., "2024/25")
    """
    year = for_date.year

    if for_date.month < start_month or (
        for_date.month == start_month and for_date.day < start_day
    ):
        year -= 1

    return f"{year}/{str(year + 1)[-2:]}"


# =============================================================================
# NUMBER UTILITIES
# =============================================================================

def parse_numeric(
    value: Any,
    default: Optional[Decimal] = None
) -> Optional[Decimal]:
    """
    Parse a numeric value to Decimal.

    Handles various formats including comma-separated numbers,
    percentages, and parenthetical negatives.

    Args:
        value: Value to parse
        default: Default value if parsing fails

    Returns:
        Decimal value or default
    """
    if value is None:
        return default

    if isinstance(value, Decimal):
        return value

    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return default

    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() in ['na', 'n/a', '-', '--', '...']:
            return default

        # Remove commas and spaces
        value = value.replace(',', '').replace(' ', '')

        # Handle parenthetical negatives: (123) -> -123
        if value.startswith('(') and value.endswith(')'):
            value = '-' + value[1:-1]

        # Handle percentages
        if value.endswith('%'):
            value = value[:-1]

        # Handle currency symbols
        for symbol in ['$', '€', '£', '¥']:
            value = value.replace(symbol, '')

        try:
            return Decimal(value)
        except InvalidOperation:
            return default

    return default


def safe_divide(
    numerator: Union[int, float, Decimal],
    denominator: Union[int, float, Decimal],
    default: float = 0.0
) -> float:
    """
    Safely divide two numbers.

    Args:
        numerator: The numerator
        denominator: The denominator
        default: Default value if division fails

    Returns:
        Result of division or default
    """
    try:
        if denominator == 0:
            return default
        return float(numerator) / float(denominator)
    except (TypeError, ValueError):
        return default


def calculate_percent_change(
    current: Union[int, float, Decimal],
    previous: Union[int, float, Decimal]
) -> Optional[float]:
    """
    Calculate percentage change.

    Args:
        current: Current value
        previous: Previous value

    Returns:
        Percentage change or None if calculation fails
    """
    try:
        if previous == 0:
            return None
        return ((float(current) - float(previous)) / float(previous)) * 100
    except (TypeError, ValueError, ZeroDivisionError):
        return None


# =============================================================================
# STRING UTILITIES
# =============================================================================

def normalize_commodity_name(name: str) -> str:
    """
    Normalize commodity name to standard format.

    Args:
        name: Raw commodity name

    Returns:
        Normalized name
    """
    if not name:
        return name

    name = name.strip()

    # Standard mappings
    mappings = {
        'corn': 'Corn',
        'maize': 'Corn',
        'wheat': 'Wheat',
        'hrw': 'Wheat HRW',
        'srw': 'Wheat SRW',
        'soybeans': 'Soybeans',
        'soybean': 'Soybeans',
        'beans': 'Soybeans',
        'soybean meal': 'Soybean Meal',
        'soymeal': 'Soybean Meal',
        'sbm': 'Soybean Meal',
        'soybean oil': 'Soybean Oil',
        'soy oil': 'Soybean Oil',
        'sbo': 'Soybean Oil',
        'fcoj': 'FCOJ',
        'orange juice': 'FCOJ',
        'sugar': 'Sugar',
        'sugar #11': 'Sugar #11',
        'sugar #16': 'Sugar #16',
        'hfcs': 'HFCS',
        'hfcs-42': 'HFCS-42',
        'hfcs-55': 'HFCS-55',
    }

    normalized = name.lower()
    return mappings.get(normalized, name.title())


def normalize_field_name(field_name: str) -> str:
    """
    Normalize a supply/demand field name.

    Args:
        field_name: Raw field name

    Returns:
        Normalized field name in snake_case
    """
    if not field_name:
        return field_name

    # Convert to lowercase and replace spaces/dashes with underscores
    normalized = field_name.lower().strip()
    normalized = re.sub(r'[\s\-]+', '_', normalized)

    # Common aliases
    aliases = {
        'beg_stocks': 'beginning_stocks',
        'beg_stks': 'beginning_stocks',
        'beginning_stks': 'beginning_stocks',
        'end_stocks': 'ending_stocks',
        'end_stks': 'ending_stocks',
        'ending_stks': 'ending_stocks',
        's/u': 'stocks_to_use',
        'stks/use': 'stocks_to_use',
        'stocks_use': 'stocks_to_use',
        'dom_use': 'domestic_use',
        'domestic': 'domestic_use',
        'total_dom_use': 'domestic_use',
        'total_exports': 'exports',
        'exp': 'exports',
        'prod': 'production',
        'imp': 'imports',
    }

    return aliases.get(normalized, normalized)


def normalize_country_name(country: str) -> str:
    """
    Normalize country name to standard format.

    Args:
        country: Raw country name

    Returns:
        Normalized country name
    """
    if not country:
        return country

    country = country.strip()

    # Standard mappings
    mappings = {
        'usa': 'United States',
        'us': 'United States',
        'united states of america': 'United States',
        'uk': 'United Kingdom',
        'britain': 'United Kingdom',
        'china': 'China',
        'prc': 'China',
        "people's republic of china": 'China',
        'brasil': 'Brazil',
        'argentina': 'Argentina',
        'argentine': 'Argentina',
    }

    normalized = country.lower()
    return mappings.get(normalized, country.title())


# =============================================================================
# DATA CONVERSION UTILITIES
# =============================================================================

def convert_units(
    value: Union[int, float, Decimal],
    from_unit: str,
    to_unit: str,
    commodity: str = None
) -> Optional[float]:
    """
    Convert between common commodity units.

    Args:
        value: Value to convert
        from_unit: Source unit
        to_unit: Target unit
        commodity: Commodity name (needed for bushel conversions)

    Returns:
        Converted value or None if conversion not possible
    """
    if from_unit == to_unit:
        return float(value)

    # Normalize unit names
    from_unit = from_unit.lower().strip()
    to_unit = to_unit.lower().strip()

    # Bushel weights by commodity (lbs per bushel)
    bushel_weights = {
        'corn': 56,
        'wheat': 60,
        'soybeans': 60,
        'oats': 32,
        'barley': 48,
        'sorghum': 56,
    }

    # Weight conversions (base: pounds)
    weight_to_lbs = {
        'lb': 1,
        'lbs': 1,
        'pound': 1,
        'pounds': 1,
        'kg': 2.20462,
        'kilogram': 2.20462,
        'kilograms': 2.20462,
        'mt': 2204.62,
        'metric ton': 2204.62,
        'metric tons': 2204.62,
        'tonne': 2204.62,
        'tonnes': 2204.62,
        'st': 2000,
        'short ton': 2000,
        'short tons': 2000,
        'ton': 2000,
        'tons': 2000,
        'cwt': 100,
    }

    # Try bushel conversions
    if 'bu' in from_unit or 'bushel' in from_unit:
        if commodity and commodity.lower() in bushel_weights:
            lbs = float(value) * bushel_weights[commodity.lower()]
            if to_unit in weight_to_lbs:
                return lbs / weight_to_lbs[to_unit]

    if 'bu' in to_unit or 'bushel' in to_unit:
        if commodity and commodity.lower() in bushel_weights:
            if from_unit in weight_to_lbs:
                lbs = float(value) * weight_to_lbs[from_unit]
                return lbs / bushel_weights[commodity.lower()]

    # Try weight-to-weight conversions
    if from_unit in weight_to_lbs and to_unit in weight_to_lbs:
        lbs = float(value) * weight_to_lbs[from_unit]
        return lbs / weight_to_lbs[to_unit]

    return None


def bushels_to_metric_tons(
    bushels: Union[int, float, Decimal],
    commodity: str
) -> Optional[float]:
    """
    Convert bushels to metric tons for a commodity.

    Args:
        bushels: Number of bushels
        commodity: Commodity name

    Returns:
        Metric tons or None if commodity not found
    """
    return convert_units(bushels, 'bu', 'mt', commodity)


def metric_tons_to_bushels(
    mt: Union[int, float, Decimal],
    commodity: str
) -> Optional[float]:
    """
    Convert metric tons to bushels for a commodity.

    Args:
        mt: Number of metric tons
        commodity: Commodity name

    Returns:
        Bushels or None if commodity not found
    """
    return convert_units(mt, 'mt', 'bu', commodity)


# =============================================================================
# CHECKSUM AND HASH UTILITIES
# =============================================================================

def calculate_checksum(data: Any) -> str:
    """
    Calculate SHA256 checksum of data.

    Args:
        data: Data to hash (supports DataFrame, dict, list, str)

    Returns:
        Hex-encoded SHA256 hash
    """
    import json

    if isinstance(data, pd.DataFrame):
        data_str = data.to_json(orient='records', date_format='iso')
    elif isinstance(data, (dict, list)):
        data_str = json.dumps(data, sort_keys=True, default=str)
    else:
        data_str = str(data)

    return hashlib.sha256(data_str.encode('utf-8')).hexdigest()


def calculate_row_hash(row: Dict[str, Any], key_fields: List[str] = None) -> str:
    """
    Calculate hash for a data row.

    Args:
        row: Row data as dictionary
        key_fields: Fields to include in hash (all if None)

    Returns:
        Hex-encoded MD5 hash
    """
    import json

    if key_fields:
        row = {k: v for k, v in row.items() if k in key_fields}

    data_str = json.dumps(row, sort_keys=True, default=str)
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()


# =============================================================================
# VALIDATION UTILITIES
# =============================================================================

def validate_percentage(
    value: Union[int, float, Decimal],
    allow_over_100: bool = False
) -> Tuple[bool, Optional[str]]:
    """
    Validate a percentage value.

    Args:
        value: Value to validate
        allow_over_100: Whether to allow values > 100

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        val = float(value)
    except (TypeError, ValueError):
        return False, f"Invalid numeric value: {value}"

    if val < 0:
        return False, f"Percentage cannot be negative: {val}"

    if not allow_over_100 and val > 100:
        return False, f"Percentage cannot exceed 100: {val}"

    return True, None


def validate_positive(value: Union[int, float, Decimal]) -> Tuple[bool, Optional[str]]:
    """
    Validate that a value is positive.

    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        val = float(value)
    except (TypeError, ValueError):
        return False, f"Invalid numeric value: {value}"

    if val < 0:
        return False, f"Value must be positive: {val}"

    return True, None


def validate_date_range(
    start_date: date,
    end_date: date
) -> Tuple[bool, Optional[str]]:
    """
    Validate a date range.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if start_date > end_date:
        return False, f"Start date {start_date} is after end date {end_date}"

    if end_date > date.today() + pd.Timedelta(days=365):
        return False, f"End date {end_date} is too far in the future"

    return True, None
