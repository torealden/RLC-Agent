#!/usr/bin/env python3
"""
Weather Data Models

Structured data models for extracted weather information.
These models standardize the output from various extractors
before passing to the LLM synthesizer.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from enum import Enum


class PrecipitationType(Enum):
    """Types of precipitation."""
    RAIN = "rain"
    SNOW = "snow"
    MIXED = "mixed"
    NONE = "none"


class TrendDirection(Enum):
    """Direction of forecast changes."""
    WETTER = "wetter"
    DRIER = "drier"
    WARMER = "warmer"
    COOLER = "cooler"
    UNCHANGED = "unchanged"


class CropImpact(Enum):
    """Potential crop impact levels."""
    BENEFICIAL = "beneficial"
    NEUTRAL = "neutral"
    MINOR_STRESS = "minor_stress"
    MODERATE_STRESS = "moderate_stress"
    SEVERE_STRESS = "severe_stress"


@dataclass
class PrecipitationForecast:
    """Precipitation forecast for a region/period."""
    amount_low: Optional[float] = None  # inches
    amount_high: Optional[float] = None
    amount_text: str = ""  # Original text like "0.5-1.5 inches"
    precip_type: PrecipitationType = PrecipitationType.RAIN
    coverage: str = ""  # "scattered", "widespread", etc.
    timing: str = ""  # "early week", "weekend", etc.

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["precip_type"] = self.precip_type.value
        return d


@dataclass
class TemperatureForecast:
    """Temperature forecast for a region/period."""
    high_f: Optional[int] = None
    low_f: Optional[int] = None
    anomaly: str = ""  # "above normal", "below normal", etc.
    text: str = ""  # Original description

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class RegionalConditions:
    """Weather conditions for a specific region."""
    region_id: str
    region_name: str

    # Current conditions
    current_conditions: str = ""
    soil_moisture: str = ""  # "adequate", "short", "surplus"

    # Week 1 forecast
    week1_precip: Optional[PrecipitationForecast] = None
    week1_temp: Optional[TemperatureForecast] = None
    week1_summary: str = ""

    # Week 2 forecast
    week2_precip: Optional[PrecipitationForecast] = None
    week2_temp: Optional[TemperatureForecast] = None
    week2_summary: str = ""

    # Extended outlook (6-10 day, 8-14 day)
    extended_outlook: str = ""

    # Crop-specific notes
    crop_notes: Dict[str, str] = field(default_factory=dict)  # {"corn": "good conditions", ...}

    # Key changes from prior update
    changes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        d = {
            "region_id": self.region_id,
            "region_name": self.region_name,
            "current_conditions": self.current_conditions,
            "soil_moisture": self.soil_moisture,
            "week1_summary": self.week1_summary,
            "week2_summary": self.week2_summary,
            "extended_outlook": self.extended_outlook,
            "crop_notes": self.crop_notes,
            "changes": self.changes
        }
        if self.week1_precip:
            d["week1_precip"] = self.week1_precip.to_dict()
        if self.week1_temp:
            d["week1_temp"] = self.week1_temp.to_dict()
        if self.week2_precip:
            d["week2_precip"] = self.week2_precip.to_dict()
        if self.week2_temp:
            d["week2_temp"] = self.week2_temp.to_dict()
        return d


@dataclass
class ForecastChange:
    """A notable change in the forecast."""
    region: str
    change_type: str  # "precipitation", "temperature", "timing"
    direction: TrendDirection
    timeframe: str  # "week 1", "week 2", etc.
    description: str
    magnitude: str = ""  # "slight", "significant", etc.

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["direction"] = self.direction.value
        return d


@dataclass
class WeatherAlert:
    """Weather alert or warning."""
    alert_type: str  # "frost", "heat", "flood", "drought", etc.
    severity: str  # "watch", "warning", "advisory"
    region: str
    timing: str
    description: str
    crop_impact: CropImpact = CropImpact.NEUTRAL

    def to_dict(self) -> Dict:
        d = asdict(self)
        d["crop_impact"] = self.crop_impact.value
        return d


@dataclass
class AttachmentInfo:
    """Information about an email attachment."""
    filename: str
    file_type: str  # "pdf", "image", "audio"
    saved_path: Optional[str] = None
    content_type: str = ""  # "forecast_map", "precip_map", "temp_map", etc.
    extracted_text: str = ""  # If PDF was parsed
    description: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class ExtractedWeatherData:
    """
    Complete extracted data from a weather email.

    This is the standardized format passed to the LLM synthesizer.
    """
    # Metadata
    email_id: str
    email_type: str
    subject: str
    sender: str
    received_at: datetime
    extracted_at: datetime = field(default_factory=datetime.now)

    # Classification
    priority: int = 5
    market_relevance: str = "low"
    sentiment: str = "neutral"

    # Main content
    headline_summary: str = ""  # Key takeaway from subject/first paragraph
    full_text_summary: str = ""  # Summarized body if applicable

    # Structured regional data
    regions: Dict[str, RegionalConditions] = field(default_factory=dict)

    # Key changes/highlights
    forecast_changes: List[ForecastChange] = field(default_factory=list)
    alerts: List[WeatherAlert] = field(default_factory=list)
    key_points: List[str] = field(default_factory=list)

    # Attachments
    attachments: List[AttachmentInfo] = field(default_factory=list)

    # Raw data (for debugging/reference)
    raw_body_preview: str = ""  # First 1000 chars
    extraction_notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "email_id": self.email_id,
            "email_type": self.email_type,
            "subject": self.subject,
            "sender": self.sender,
            "received_at": self.received_at.isoformat() if self.received_at else None,
            "extracted_at": self.extracted_at.isoformat() if self.extracted_at else None,
            "priority": self.priority,
            "market_relevance": self.market_relevance,
            "sentiment": self.sentiment,
            "headline_summary": self.headline_summary,
            "full_text_summary": self.full_text_summary,
            "regions": {k: v.to_dict() for k, v in self.regions.items()},
            "forecast_changes": [fc.to_dict() for fc in self.forecast_changes],
            "alerts": [a.to_dict() for a in self.alerts],
            "key_points": self.key_points,
            "attachments": [a.to_dict() for a in self.attachments],
            "raw_body_preview": self.raw_body_preview,
            "extraction_notes": self.extraction_notes
        }

    def add_region(self, region: RegionalConditions):
        """Add regional conditions."""
        self.regions[region.region_id] = region

    def add_change(self, change: ForecastChange):
        """Add a forecast change."""
        self.forecast_changes.append(change)

    def add_alert(self, alert: WeatherAlert):
        """Add a weather alert."""
        self.alerts.append(alert)

    def add_key_point(self, point: str):
        """Add a key point."""
        if point not in self.key_points:
            self.key_points.append(point)


@dataclass
class WeatherSummaryBatch:
    """
    A batch of extracted weather data ready for synthesis.

    This is what gets passed to the LLM for the final summary.
    """
    generated_at: datetime = field(default_factory=datetime.now)
    emails_processed: int = 0

    # Aggregated data
    emails: List[ExtractedWeatherData] = field(default_factory=list)

    # Summary statistics
    email_types_count: Dict[str, int] = field(default_factory=dict)
    regions_covered: List[str] = field(default_factory=list)
    overall_sentiment: str = "neutral"

    # Merged highlights (from all emails)
    all_key_points: List[str] = field(default_factory=list)
    all_forecast_changes: List[ForecastChange] = field(default_factory=list)
    all_alerts: List[WeatherAlert] = field(default_factory=list)

    # Available attachments
    all_attachments: List[AttachmentInfo] = field(default_factory=list)

    def add_email(self, data: ExtractedWeatherData):
        """Add extracted email data to the batch."""
        self.emails.append(data)
        self.emails_processed += 1

        # Update type counts
        self.email_types_count[data.email_type] = self.email_types_count.get(data.email_type, 0) + 1

        # Merge regions
        for region_id in data.regions.keys():
            if region_id not in self.regions_covered:
                self.regions_covered.append(region_id)

        # Merge highlights
        self.all_key_points.extend(data.key_points)
        self.all_forecast_changes.extend(data.forecast_changes)
        self.all_alerts.extend(data.alerts)
        self.all_attachments.extend(data.attachments)

        # Update overall sentiment
        self._update_sentiment(data.sentiment)

    def _update_sentiment(self, new_sentiment: str):
        """Update overall sentiment based on individual email sentiments."""
        sentiment_weights = {"bullish": 1, "neutral": 0, "bearish": -1}

        # Simple averaging approach
        current_weight = sentiment_weights.get(self.overall_sentiment, 0)
        new_weight = sentiment_weights.get(new_sentiment, 0)

        if self.emails_processed > 1:
            avg_weight = (current_weight * (self.emails_processed - 1) + new_weight) / self.emails_processed
        else:
            avg_weight = new_weight

        if avg_weight > 0.3:
            self.overall_sentiment = "bullish"
        elif avg_weight < -0.3:
            self.overall_sentiment = "bearish"
        else:
            self.overall_sentiment = "neutral"

    def to_dict(self) -> Dict:
        return {
            "generated_at": self.generated_at.isoformat(),
            "emails_processed": self.emails_processed,
            "email_types_count": self.email_types_count,
            "regions_covered": self.regions_covered,
            "overall_sentiment": self.overall_sentiment,
            "all_key_points": list(set(self.all_key_points)),  # Dedupe
            "all_forecast_changes": [fc.to_dict() for fc in self.all_forecast_changes],
            "all_alerts": [a.to_dict() for a in self.all_alerts],
            "all_attachments": [a.to_dict() for a in self.all_attachments],
            "emails": [e.to_dict() for e in self.emails]
        }

    def get_llm_context(self) -> str:
        """
        Generate a condensed context string for the LLM.

        This is the key output - structured data ready for synthesis.
        """
        lines = [
            f"WEATHER EMAIL BATCH - {self.generated_at.strftime('%Y-%m-%d %H:%M')}",
            f"Emails processed: {self.emails_processed}",
            f"Types: {self.email_types_count}",
            f"Regions: {', '.join(self.regions_covered)}",
            f"Overall sentiment: {self.overall_sentiment}",
            ""
        ]

        # Add alerts first (highest priority)
        if self.all_alerts:
            lines.append("=== ALERTS ===")
            for alert in self.all_alerts:
                lines.append(f"  [{alert.severity.upper()}] {alert.region}: {alert.description}")
            lines.append("")

        # Add forecast changes
        if self.all_forecast_changes:
            lines.append("=== FORECAST CHANGES ===")
            for change in self.all_forecast_changes:
                lines.append(f"  {change.region} ({change.timeframe}): {change.direction.value} - {change.description}")
            lines.append("")

        # Add regional summaries
        lines.append("=== REGIONAL CONDITIONS ===")
        for email in self.emails:
            for region_id, region in email.regions.items():
                lines.append(f"\n{region.region_name}:")
                if region.week1_summary:
                    lines.append(f"  Week 1: {region.week1_summary}")
                if region.week2_summary:
                    lines.append(f"  Week 2: {region.week2_summary}")
                if region.changes:
                    lines.append(f"  Changes: {'; '.join(region.changes)}")

        # Add key points
        if self.all_key_points:
            lines.append("\n=== KEY POINTS ===")
            for point in list(set(self.all_key_points))[:10]:
                lines.append(f"  • {point}")

        return "\n".join(lines)


# Helper functions

def parse_precip_amount(text: str) -> PrecipitationForecast:
    """
    Parse precipitation amount from text like "0.5-1.5 inches" or "1-2 inches".
    """
    import re

    forecast = PrecipitationForecast(amount_text=text)

    # Try to extract numeric range
    range_match = re.search(r'(\d+\.?\d*)\s*[-to]+\s*(\d+\.?\d*)\s*(?:inch|in|")', text, re.IGNORECASE)
    if range_match:
        forecast.amount_low = float(range_match.group(1))
        forecast.amount_high = float(range_match.group(2))
        return forecast

    # Try single value
    single_match = re.search(r'(\d+\.?\d*)\s*(?:inch|in|")', text, re.IGNORECASE)
    if single_match:
        forecast.amount_low = float(single_match.group(1))
        forecast.amount_high = float(single_match.group(1))
        return forecast

    return forecast


def determine_crop_impact(
    region_id: str,
    current_month: str,
    conditions: RegionalConditions,
    region_config: Dict
) -> CropImpact:
    """
    Determine potential crop impact based on region, timing, and conditions.

    This uses the critical periods from the config to assess impact.
    """
    critical_periods = region_config.get("critical_periods", {})

    # Find current critical period
    current_period = None
    for period_name, months in critical_periods.items():
        if current_month in months:
            current_period = period_name
            break

    if not current_period:
        return CropImpact.NEUTRAL

    # Assess based on soil moisture and forecasts
    if conditions.soil_moisture == "short":
        if current_period in ["pollination", "pod_fill", "soy_development"]:
            return CropImpact.MODERATE_STRESS
        return CropImpact.MINOR_STRESS

    if conditions.soil_moisture == "surplus":
        if current_period in ["planting", "harvest", "soy_harvest"]:
            return CropImpact.MINOR_STRESS

    return CropImpact.NEUTRAL
