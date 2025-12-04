"""
Validation Utilities

Helper functions for validating data in the HB Weekly Report.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of a validation check"""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: Dict[str, Any] = field(default_factory=dict)


def validate_price_data(
    prices: Dict[str, Any],
    required_series: List[str] = None
) -> ValidationResult:
    """
    Validate price data completeness and consistency

    Args:
        prices: Dictionary of price comparisons
        required_series: List of required series IDs

    Returns:
        ValidationResult
    """
    result = ValidationResult(is_valid=True)

    if not prices:
        result.is_valid = False
        result.errors.append("No price data available")
        return result

    # Check required series
    if required_series:
        for series_id in required_series:
            if series_id not in prices:
                result.warnings.append(f"Missing required series: {series_id}")

    # Validate each price comparison
    for series_id, comparison in prices.items():
        # Check for current price
        if not hasattr(comparison, 'current') or comparison.current is None:
            result.warnings.append(f"{series_id}: Missing current price")
        elif comparison.current.price is None:
            result.warnings.append(f"{series_id}: Current price is null")

        # Check for historical prices
        if not hasattr(comparison, 'week_ago') or comparison.week_ago is None:
            result.warnings.append(f"{series_id}: Missing week-ago price")

        if not hasattr(comparison, 'year_ago') or comparison.year_ago is None:
            result.warnings.append(f"{series_id}: Missing year-ago price")

        # Validate price ranges (sanity check)
        if hasattr(comparison, 'current') and comparison.current and comparison.current.price:
            price = comparison.current.price

            # Check for suspicious values
            if price <= 0:
                result.errors.append(f"{series_id}: Negative or zero price ({price})")
                result.is_valid = False
            elif price > 10000:  # Unlikely for most ag commodities
                result.warnings.append(f"{series_id}: Unusually high price ({price})")

    result.info["series_count"] = len(prices)
    result.info["warnings_count"] = len(result.warnings)

    return result


def validate_internal_data(
    data: Dict[str, Any],
    required_commodities: List[str] = None,
    required_fields: Dict[str, List[str]] = None
) -> ValidationResult:
    """
    Validate internal spreadsheet data

    Args:
        data: Internal data dictionary
        required_commodities: List of required commodities
        required_fields: Dict mapping commodity to required fields

    Returns:
        ValidationResult
    """
    result = ValidationResult(is_valid=True)

    if not data:
        result.is_valid = False
        result.errors.append("No internal data available")
        return result

    supply_demand = data.get("supply_demand", {})
    forecasts = data.get("forecasts", {})

    # Check required commodities
    if required_commodities:
        for commodity in required_commodities:
            if commodity not in supply_demand:
                result.warnings.append(f"Missing supply/demand data for: {commodity}")

    # Check required fields
    if required_fields:
        for commodity, fields in required_fields.items():
            if commodity in supply_demand:
                commodity_data = supply_demand[commodity]
                for field_name in fields:
                    # Check if field exists (simplified check)
                    if not _field_exists(commodity_data, field_name):
                        result.warnings.append(f"{commodity}: Missing field '{field_name}'")

    # Validate data consistency
    for commodity, commodity_data in supply_demand.items():
        issues = _validate_supply_demand_consistency(commodity, commodity_data)
        result.warnings.extend(issues)

    result.info["commodities_count"] = len(supply_demand)
    result.info["warnings_count"] = len(result.warnings)

    return result


def _field_exists(data: Any, field_name: str) -> bool:
    """Check if a field exists in data (DataFrame or dict)"""
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            # Check columns
            for col in data.columns:
                if field_name.lower() in str(col).lower():
                    return True
            return False
        elif isinstance(data, dict):
            return field_name in data
        return False
    except Exception:
        return False


def _validate_supply_demand_consistency(
    commodity: str,
    data: Any
) -> List[str]:
    """Validate supply/demand data consistency"""
    issues = []

    # This would contain logic to check:
    # - Total supply = production + beginning stocks + imports
    # - Total use = domestic use + exports
    # - Ending stocks = total supply - total use
    # Actual implementation depends on data structure

    return issues


def validate_report_content(content: Any) -> ValidationResult:
    """
    Validate report content completeness

    Args:
        content: ReportContent object

    Returns:
        ValidationResult
    """
    result = ValidationResult(is_valid=True)

    # Check required sections
    required_sections = [
        ("executive_summary", content.executive_summary),
        ("macro_update", content.macro_update),
        ("synthesis_outlook", content.synthesis_outlook),
    ]

    for name, section in required_sections:
        if not section or len(section.strip()) < 50:
            result.warnings.append(f"Section '{name}' is missing or too short")

    # Check commodity sections
    for commodity in ["corn", "wheat", "soybeans"]:
        section = content.commodity_sections.get(commodity)
        if not section or len(section.strip()) < 50:
            result.warnings.append(f"Commodity section '{commodity}' is missing or too short")

    # Check for placeholders
    all_text = [
        content.executive_summary,
        content.macro_update,
        content.weather_update,
        content.synthesis_outlook,
    ]
    all_text.extend(content.commodity_sections.values())

    for text in all_text:
        if text:
            if "[TBD]" in text or "[???]" in text or "PLACEHOLDER" in text:
                result.warnings.append("Report contains placeholders")
                break

    # Check key triggers
    if not content.key_triggers or len(content.key_triggers) < 3:
        result.warnings.append("Key triggers list is missing or incomplete")

    # Check tables
    if not content.price_table_data or len(content.price_table_data.get("futures", [])) == 0:
        result.warnings.append("Price table data is missing")

    result.info["sections_filled"] = sum(1 for _, s in required_sections if s and len(s.strip()) >= 50)
    result.info["commodity_sections_filled"] = len([
        s for s in content.commodity_sections.values()
        if s and len(s.strip()) >= 50
    ])

    # Overall validity
    if len(result.warnings) > 5:
        result.is_valid = False
        result.errors.append("Too many validation warnings - report may be incomplete")

    return result


def validate_date_range(
    start_date: date,
    end_date: date
) -> ValidationResult:
    """Validate a date range"""
    result = ValidationResult(is_valid=True)

    if start_date > end_date:
        result.is_valid = False
        result.errors.append(f"Start date ({start_date}) is after end date ({end_date})")

    if end_date > date.today():
        result.warnings.append(f"End date ({end_date}) is in the future")

    return result


def validate_configuration(config: Any) -> ValidationResult:
    """
    Validate configuration completeness

    Args:
        config: HBWeeklyReportConfig object

    Returns:
        ValidationResult
    """
    result = ValidationResult(is_valid=True)

    # Check data source configuration
    if config.internal_data_source.value == "dropbox":
        if not config.dropbox.enabled:
            result.errors.append("Dropbox is configured as data source but not enabled")
            result.is_valid = False
        elif not config.dropbox.access_token and not config.dropbox.refresh_token:
            result.warnings.append("Dropbox credentials not configured")

    elif config.internal_data_source.value == "database":
        if not config.database.host or not config.database.database:
            result.errors.append("Database is configured as data source but connection not configured")
            result.is_valid = False

    # Check LLM configuration
    if config.llm.enabled:
        if config.llm.provider == "openai" and not config.llm.api_key:
            result.warnings.append("OpenAI LLM enabled but API key not set")
        elif config.llm.provider == "ollama" and not config.llm.ollama_base_url:
            result.warnings.append("Ollama LLM enabled but base URL not set")

    # Check notification configuration
    if config.notifications.enabled:
        if not config.notifications.smtp_server and not config.notifications.slack_webhook_url:
            result.warnings.append("Notifications enabled but no delivery method configured")

    return result
