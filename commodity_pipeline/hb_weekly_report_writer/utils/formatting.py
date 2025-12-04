"""
Formatting Utilities

Helper functions for formatting data in the HB Weekly Report.
"""

from datetime import date, datetime
from typing import Optional, Union


def format_price(
    value: Optional[float],
    decimals: int = 2,
    unit: str = "",
    include_unit: bool = False
) -> str:
    """
    Format a price value

    Args:
        value: Price value
        decimals: Number of decimal places
        unit: Unit (e.g., "$/bu", "cents/lb")
        include_unit: Whether to append unit

    Returns:
        Formatted price string
    """
    if value is None:
        return "N/A"

    formatted = f"{value:,.{decimals}f}"

    if include_unit and unit:
        formatted = f"{formatted} {unit}"

    return formatted


def format_percentage(
    value: Optional[float],
    decimals: int = 1,
    include_sign: bool = True
) -> str:
    """
    Format a percentage value

    Args:
        value: Percentage value (e.g., 5.5 for 5.5%)
        decimals: Number of decimal places
        include_sign: Whether to include + for positive values

    Returns:
        Formatted percentage string
    """
    if value is None:
        return "N/A"

    if include_sign and value > 0:
        return f"+{value:.{decimals}f}%"
    else:
        return f"{value:.{decimals}f}%"


def format_change(
    value: Optional[float],
    decimals: int = 2,
    include_sign: bool = True
) -> str:
    """
    Format a change value (with +/- sign)

    Args:
        value: Change value
        decimals: Number of decimal places
        include_sign: Whether to include + for positive values

    Returns:
        Formatted change string
    """
    if value is None:
        return "N/A"

    if include_sign:
        return f"{value:+.{decimals}f}"
    else:
        return f"{value:.{decimals}f}"


def format_date(
    dt: Optional[Union[date, datetime]],
    format_str: str = "%B %d, %Y"
) -> str:
    """
    Format a date

    Args:
        dt: Date or datetime object
        format_str: strftime format string

    Returns:
        Formatted date string
    """
    if dt is None:
        return "N/A"

    if isinstance(dt, datetime):
        return dt.strftime(format_str)
    elif isinstance(dt, date):
        return dt.strftime(format_str)
    else:
        return str(dt)


def format_commodity_name(commodity: str) -> str:
    """
    Format commodity name for display

    Args:
        commodity: Internal commodity name (e.g., "soybean_meal")

    Returns:
        Display name (e.g., "Soybean Meal")
    """
    name_map = {
        "corn": "Corn",
        "wheat": "Wheat",
        "soybeans": "Soybeans",
        "soybean_meal": "Soybean Meal",
        "soybean_oil": "Soybean Oil",
        "hrw_wheat": "Hard Red Winter Wheat",
        "srw_wheat": "Soft Red Winter Wheat",
    }

    return name_map.get(commodity.lower(), commodity.replace("_", " ").title())


def format_marketing_year(year: int, commodity: str = "corn") -> str:
    """
    Format marketing year

    Args:
        year: Starting year of marketing year
        commodity: Commodity for determining marketing year convention

    Returns:
        Formatted marketing year (e.g., "2024/25")
    """
    return f"{year}/{str(year + 1)[-2:]}"


def format_volume(
    value: Optional[float],
    unit: str = "bu",
    scale: str = "millions"
) -> str:
    """
    Format volume/quantity value

    Args:
        value: Value in base units
        unit: Unit (bu, mt, etc.)
        scale: "millions", "billions", "thousands", or "none"

    Returns:
        Formatted volume string
    """
    if value is None:
        return "N/A"

    scale_factors = {
        "billions": (1e9, "B"),
        "millions": (1e6, "M"),
        "thousands": (1e3, "K"),
        "none": (1, ""),
    }

    factor, suffix = scale_factors.get(scale, (1, ""))
    scaled_value = value / factor

    return f"{scaled_value:,.1f}{suffix} {unit}"


def format_bullish_bearish(factor_type: str) -> str:
    """
    Format bullish/bearish text with emphasis

    Args:
        factor_type: "bullish" or "bearish"

    Returns:
        Formatted string
    """
    if factor_type.lower() == "bullish":
        return "**Bullish**"
    elif factor_type.lower() == "bearish":
        return "**Bearish**"
    else:
        return factor_type.title()


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate text to maximum length

    Args:
        text: Text to truncate
        max_length: Maximum length
        suffix: Suffix to append if truncated

    Returns:
        Truncated text
    """
    if len(text) <= max_length:
        return text

    return text[:max_length - len(suffix)] + suffix
