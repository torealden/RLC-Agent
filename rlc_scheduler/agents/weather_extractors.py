#!/usr/bin/env python3
"""
Weather Data Extractors

Type-specific extractors for weather emails:
- TextExtractor: For body-heavy emails (commentary, scheduled updates)
- PDFExtractor: For PDF attachments
- GraphicsHandler: For map/image attachments

These extractors populate the WeatherDataModels with structured data.
"""

import re
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

from weather_data_models import (
    ExtractedWeatherData,
    RegionalConditions,
    PrecipitationForecast,
    TemperatureForecast,
    ForecastChange,
    WeatherAlert,
    AttachmentInfo,
    TrendDirection,
    CropImpact,
    PrecipitationType,
    parse_precip_amount
)

logger = logging.getLogger(__name__)

# Configuration
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
ATTACHMENTS_DIR = BASE_DIR / "data" / "weather_attachments"


class TextExtractor:
    """
    Extracts structured weather data from email body text.

    Handles:
    - Commentary emails (free-form analysis)
    - Scheduled updates (section-based)
    """

    # Section headers to look for
    SECTION_HEADERS = [
        "UNITED STATES",
        "U.S.",
        "BRAZIL",
        "ARGENTINA",
        "AUSTRALIA",
        "EUROPE",
        "CHINA",
        "INDIA",
        "CANADA",
        "RUSSIA",
        "UKRAINE",
        "SUMMARY",
        "KEY CHANGES",
        "COMMENTS",
        "OUTLOOK",
        "WEEK 1",
        "WEEK 2"
    ]

    # Region mapping (header -> region_id)
    REGION_MAP = {
        "UNITED STATES": "us",
        "U.S.": "us",
        "BRAZIL": "brazil",
        "ARGENTINA": "argentina",
        "AUSTRALIA": "australia",
        "EUROPE": "europe",
        "CHINA": "china",
        "INDIA": "india",
        "CANADA": "canada",
        "RUSSIA": "russia",
        "UKRAINE": "ukraine"
    }

    # Sub-region patterns
    SUB_REGIONS = {
        "us": {
            "corn belt": "us_corn_belt",
            "midwest": "us_corn_belt",
            "wheat belt": "us_wheat_belt",
            "plains": "us_wheat_belt",
            "southern plains": "us_wheat_belt",
            "northern plains": "us_wheat_belt",
            "delta": "us_delta",
            "southeast": "us_southeast"
        },
        "brazil": {
            "mato grosso": "brazil_center_west",
            "goias": "brazil_center_west",
            "center-west": "brazil_center_west",
            "parana": "brazil_south",
            "rio grande do sul": "brazil_south",
            "south": "brazil_south"
        },
        "argentina": {
            "buenos aires": "argentina_pampas",
            "cordoba": "argentina_pampas",
            "santa fe": "argentina_pampas",
            "pampas": "argentina_pampas"
        }
    }

    def __init__(self):
        self.precip_patterns = self._compile_precip_patterns()
        self.temp_patterns = self._compile_temp_patterns()
        self.change_patterns = self._compile_change_patterns()

    def _compile_precip_patterns(self) -> List[re.Pattern]:
        """Compile precipitation-related regex patterns."""
        return [
            re.compile(r'(\d+\.?\d*)\s*[-to]+\s*(\d+\.?\d*)\s*(?:inch|in|")', re.IGNORECASE),
            re.compile(r'(\d+\.?\d*)\s*(?:inch|in|")', re.IGNORECASE),
            re.compile(r'(light|moderate|heavy|scattered|widespread)\s+(?:rain|showers|precip)', re.IGNORECASE),
            re.compile(r'(dry|no rain|little rain|minimal precip)', re.IGNORECASE),
        ]

    def _compile_temp_patterns(self) -> List[re.Pattern]:
        """Compile temperature-related regex patterns."""
        return [
            re.compile(r'(\d{1,3})\s*(?:degrees?|°|F)', re.IGNORECASE),
            re.compile(r'(above|below)\s*normal\s*temp', re.IGNORECASE),
            re.compile(r'(hot|warm|cool|cold|mild)\s+(?:temp|conditions|weather)', re.IGNORECASE),
        ]

    def _compile_change_patterns(self) -> List[re.Pattern]:
        """Compile forecast change patterns."""
        return [
            re.compile(r'(wetter|drier|warmer|cooler)\s+(?:than|in)\s+(?:the\s+)?(?:prior|previous|earlier)', re.IGNORECASE),
            re.compile(r'(?:shift|change|adjust|modify)\w*\s+(?:to|toward)\s+(wetter|drier|warmer|cooler)', re.IGNORECASE),
            re.compile(r'(increase|decrease|more|less)\s+(?:rain|precip|precipitation)', re.IGNORECASE),
        ]

    def extract(
        self,
        email_id: str,
        subject: str,
        body: str,
        sender: str,
        received_at: datetime,
        email_type: str,
        classification: Dict = None
    ) -> ExtractedWeatherData:
        """
        Extract structured data from email body.

        Args:
            email_id: Unique email identifier
            subject: Email subject line
            body: Full email body text
            sender: Sender email address
            received_at: When email was received
            email_type: Classified email type
            classification: Full classification dict (optional)

        Returns:
            ExtractedWeatherData with populated fields
        """
        classification = classification or {}

        # Create base data object
        data = ExtractedWeatherData(
            email_id=email_id,
            email_type=email_type,
            subject=subject,
            sender=sender,
            received_at=received_at,
            priority=classification.get("priority", 5),
            market_relevance=classification.get("market_relevance", "low"),
            sentiment=classification.get("sentiment", "neutral"),
            raw_body_preview=body[:1000]
        )

        # Extract headline summary from subject
        data.headline_summary = self._extract_headline(subject)

        # Parse sections if structured email
        if email_type in ["scheduled_update", "outlook_forecast"]:
            sections = self._parse_sections(body)
            for section_name, section_text in sections.items():
                region = self._extract_region_data(section_name, section_text)
                if region:
                    data.add_region(region)
        else:
            # Free-form extraction for commentary
            self._extract_freeform(body, data)

        # Extract forecast changes
        changes = self._extract_changes(body)
        for change in changes:
            data.add_change(change)

        # Extract alerts
        alerts = self._extract_alerts(body)
        for alert in alerts:
            data.add_alert(alert)

        # Extract key points
        key_points = self._extract_key_points(subject, body)
        for point in key_points:
            data.add_key_point(point)

        return data

    def _extract_headline(self, subject: str) -> str:
        """Extract market-relevant headline from subject."""
        # Remove date patterns
        headline = re.sub(r'\s+(?:for\s+)?(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d+,?\s+\d{4}', '', subject, flags=re.IGNORECASE)

        # Remove common prefixes
        headline = re.sub(r'^(?:Evening|Morning|Mid-day)\s+', '', headline)
        headline = re.sub(r'^World Weather\s+', '', headline)

        return headline.strip()

    def _parse_sections(self, body: str) -> Dict[str, str]:
        """
        Parse body into sections based on headers.

        Returns:
            Dict mapping section name to section text
        """
        sections = {}

        # Split by common section headers
        pattern = r'\n\s*(' + '|'.join(re.escape(h) for h in self.SECTION_HEADERS) + r')\s*[\n:]'
        parts = re.split(pattern, body, flags=re.IGNORECASE)

        # parts alternates: [intro, header1, content1, header2, content2, ...]
        current_section = "INTRO"
        current_text = []

        for i, part in enumerate(parts):
            part_upper = part.strip().upper()
            if part_upper in [h.upper() for h in self.SECTION_HEADERS]:
                # Save previous section
                if current_text:
                    sections[current_section] = '\n'.join(current_text)
                current_section = part_upper
                current_text = []
            else:
                current_text.append(part)

        # Save last section
        if current_text:
            sections[current_section] = '\n'.join(current_text)

        return sections

    def _extract_region_data(self, section_name: str, section_text: str) -> Optional[RegionalConditions]:
        """Extract regional conditions from a section."""
        region_id = self.REGION_MAP.get(section_name.upper())
        if not region_id:
            return None

        region = RegionalConditions(
            region_id=region_id,
            region_name=section_name.title()
        )

        # Try to split into week 1 / week 2
        week1_text, week2_text = self._split_by_week(section_text)

        # Extract week 1 data
        if week1_text:
            region.week1_precip = self._extract_precip(week1_text)
            region.week1_temp = self._extract_temp(week1_text)
            region.week1_summary = self._summarize_text(week1_text, max_len=200)

        # Extract week 2 data
        if week2_text:
            region.week2_precip = self._extract_precip(week2_text)
            region.week2_temp = self._extract_temp(week2_text)
            region.week2_summary = self._summarize_text(week2_text, max_len=200)

        # If no week split, use full text
        if not week1_text and not week2_text:
            region.week1_precip = self._extract_precip(section_text)
            region.week1_temp = self._extract_temp(section_text)
            region.week1_summary = self._summarize_text(section_text, max_len=300)

        # Look for sub-regions
        sub_regions = self.SUB_REGIONS.get(region_id, {})
        for sub_name, sub_id in sub_regions.items():
            if sub_name.lower() in section_text.lower():
                # Could create sub-region entries here if needed
                pass

        return region

    def _split_by_week(self, text: str) -> Tuple[str, str]:
        """Split text into week 1 and week 2 portions."""
        week2_markers = [
            r'(?:in\s+)?week\s*2',
            r'6-10\s*day',
            r'second\s*week',
            r'days?\s*6\s*[-through]+\s*10'
        ]

        for marker in week2_markers:
            match = re.search(marker, text, re.IGNORECASE)
            if match:
                return text[:match.start()], text[match.start():]

        return text, ""

    def _extract_precip(self, text: str) -> Optional[PrecipitationForecast]:
        """Extract precipitation forecast from text."""
        for pattern in self.precip_patterns:
            match = pattern.search(text)
            if match:
                return parse_precip_amount(match.group(0))

        # Check for dry conditions
        if re.search(r'\b(?:dry|no\s+rain|little\s+precip)', text, re.IGNORECASE):
            return PrecipitationForecast(
                amount_low=0.0,
                amount_high=0.1,
                amount_text="dry conditions",
                precip_type=PrecipitationType.NONE
            )

        return None

    def _extract_temp(self, text: str) -> Optional[TemperatureForecast]:
        """Extract temperature forecast from text."""
        forecast = TemperatureForecast()

        # Look for specific temps
        temp_matches = re.findall(r'(\d{1,3})\s*(?:degrees?|°|F)', text, re.IGNORECASE)
        if temp_matches:
            temps = [int(t) for t in temp_matches if 0 < int(t) < 130]
            if temps:
                forecast.high_f = max(temps)
                forecast.low_f = min(temps)

        # Look for anomaly description
        if re.search(r'above\s*normal', text, re.IGNORECASE):
            forecast.anomaly = "above normal"
        elif re.search(r'below\s*normal', text, re.IGNORECASE):
            forecast.anomaly = "below normal"
        elif re.search(r'near\s*normal', text, re.IGNORECASE):
            forecast.anomaly = "near normal"

        if forecast.high_f or forecast.anomaly:
            return forecast
        return None

    def _summarize_text(self, text: str, max_len: int = 200) -> str:
        """Create a brief summary of text."""
        # Clean up whitespace
        text = ' '.join(text.split())

        # Take first sentence(s) up to max_len
        sentences = re.split(r'(?<=[.!?])\s+', text)
        summary = ""
        for sentence in sentences:
            if len(summary) + len(sentence) > max_len:
                break
            summary += sentence + " "

        return summary.strip()

    def _extract_freeform(self, body: str, data: ExtractedWeatherData):
        """Extract data from free-form commentary email."""
        # Look for any mentioned regions
        text_lower = body.lower()

        for region_name, region_id in self.REGION_MAP.items():
            if region_name.lower() in text_lower:
                # Extract content around the mention
                pattern = rf'({re.escape(region_name)}.{{0,500}})'
                matches = re.findall(pattern, body, re.IGNORECASE | re.DOTALL)
                if matches:
                    region = RegionalConditions(
                        region_id=region_id,
                        region_name=region_name.title(),
                        week1_summary=self._summarize_text(matches[0], max_len=300)
                    )
                    data.add_region(region)

    def _extract_changes(self, body: str) -> List[ForecastChange]:
        """Extract forecast changes from body."""
        changes = []

        for pattern in self.change_patterns:
            for match in pattern.finditer(body):
                direction_word = match.group(1).lower()

                # Map to TrendDirection
                direction_map = {
                    "wetter": TrendDirection.WETTER,
                    "drier": TrendDirection.DRIER,
                    "warmer": TrendDirection.WARMER,
                    "cooler": TrendDirection.COOLER,
                    "increase": TrendDirection.WETTER,
                    "more": TrendDirection.WETTER,
                    "decrease": TrendDirection.DRIER,
                    "less": TrendDirection.DRIER
                }

                direction = direction_map.get(direction_word, TrendDirection.UNCHANGED)

                # Get surrounding context
                start = max(0, match.start() - 50)
                end = min(len(body), match.end() + 100)
                context = body[start:end]

                changes.append(ForecastChange(
                    region="",  # Would need to determine from context
                    change_type="precipitation" if direction in [TrendDirection.WETTER, TrendDirection.DRIER] else "temperature",
                    direction=direction,
                    timeframe="",
                    description=context.strip()
                ))

        return changes[:5]  # Limit to top 5 changes

    def _extract_alerts(self, body: str) -> List[WeatherAlert]:
        """Extract weather alerts from body."""
        alerts = []

        alert_patterns = [
            (r'(frost|freeze)\s+(?:warning|watch|threat|risk)', "frost", "warning"),
            (r'(heat|hot)\s+(?:wave|stress|advisory)', "heat", "advisory"),
            (r'(flood|flooding)\s+(?:warning|watch|threat)', "flood", "warning"),
            (r'(drought)\s+(?:conditions|concerns|intensif)', "drought", "advisory"),
            (r'(winterkill|winter\s+kill)\s+(?:risk|threat|concern)', "winterkill", "advisory"),
        ]

        for pattern, alert_type, severity in alert_patterns:
            match = re.search(pattern, body, re.IGNORECASE)
            if match:
                # Get context
                start = max(0, match.start() - 30)
                end = min(len(body), match.end() + 150)
                context = body[start:end]

                alerts.append(WeatherAlert(
                    alert_type=alert_type,
                    severity=severity,
                    region="",
                    timing="",
                    description=context.strip()
                ))

        return alerts

    def _extract_key_points(self, subject: str, body: str) -> List[str]:
        """Extract key market-relevant points."""
        key_points = []

        # Subject often contains the key point
        if len(subject) > 10:
            key_points.append(subject)

        # Look for sentences with market-relevant keywords
        market_keywords = [
            "harvest", "planting", "pollination", "yield", "crop",
            "delay", "early", "late", "ahead", "behind",
            "stress", "favorable", "concern", "damage"
        ]

        sentences = re.split(r'(?<=[.!?])\s+', body)
        for sentence in sentences[:20]:  # Check first 20 sentences
            sentence_lower = sentence.lower()
            if any(kw in sentence_lower for kw in market_keywords):
                if 20 < len(sentence) < 200:
                    key_points.append(sentence.strip())

        return key_points[:5]  # Limit to top 5


class PDFExtractor:
    """
    Extracts weather data from PDF attachments.

    Uses pypdf to extract text, then applies TextExtractor logic.
    """

    def __init__(self):
        self.text_extractor = TextExtractor()
        self._check_pdf_support()

    def _check_pdf_support(self):
        """Check if PDF library is available."""
        try:
            from pypdf import PdfReader
            self.pdf_available = True
            self.PdfReader = PdfReader
        except ImportError:
            try:
                from PyPDF2 import PdfReader
                self.pdf_available = True
                self.PdfReader = PdfReader
            except ImportError:
                self.pdf_available = False
                logger.warning("PDF support not available. Install pypdf or PyPDF2.")

    def extract_from_pdf(self, pdf_path: Path) -> str:
        """
        Extract text content from a PDF file.

        Args:
            pdf_path: Path to PDF file

        Returns:
            Extracted text content
        """
        if not self.pdf_available:
            return ""

        try:
            reader = self.PdfReader(str(pdf_path))
            text_parts = []

            for page in reader.pages:
                text = page.extract_text()
                if text:
                    text_parts.append(text)

            return '\n\n'.join(text_parts)

        except Exception as e:
            logger.error(f"Error extracting PDF text from {pdf_path}: {e}")
            return ""

    def extract(
        self,
        email_id: str,
        subject: str,
        pdf_path: Path,
        sender: str,
        received_at: datetime,
        email_type: str,
        body_fallback: str = "",
        classification: Dict = None
    ) -> ExtractedWeatherData:
        """
        Extract weather data from PDF attachment.

        Falls back to body text if PDF extraction fails.
        """
        pdf_text = self.extract_from_pdf(pdf_path)

        if pdf_text:
            # Use PDF content
            data = self.text_extractor.extract(
                email_id=email_id,
                subject=subject,
                body=pdf_text,
                sender=sender,
                received_at=received_at,
                email_type=email_type,
                classification=classification
            )
            data.extraction_notes.append(f"Extracted from PDF: {pdf_path.name}")
        else:
            # Fall back to body
            data = self.text_extractor.extract(
                email_id=email_id,
                subject=subject,
                body=body_fallback,
                sender=sender,
                received_at=received_at,
                email_type=email_type,
                classification=classification
            )
            data.extraction_notes.append("PDF extraction failed, used body fallback")

        return data


class GraphicsHandler:
    """
    Handles weather map and graphics attachments.

    Saves attachments to organized folder structure and catalogs them.
    """

    def __init__(self, output_dir: Path = None):
        self.output_dir = output_dir or ATTACHMENTS_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_attachment(
        self,
        attachment_data: bytes,
        filename: str,
        email_date: datetime,
        email_subject: str
    ) -> AttachmentInfo:
        """
        Process and save an attachment.

        Args:
            attachment_data: Raw attachment bytes
            filename: Original filename
            email_date: Email received date
            email_subject: Email subject for categorization

        Returns:
            AttachmentInfo with saved path and metadata
        """
        # Determine content type from filename and subject
        content_type = self._determine_content_type(filename, email_subject)

        # Create dated folder
        date_folder = self.output_dir / email_date.strftime("%Y-%m-%d")
        date_folder.mkdir(exist_ok=True)

        # Generate unique filename
        timestamp = email_date.strftime("%H%M")
        safe_name = re.sub(r'[^\w\-_.]', '_', filename)
        save_path = date_folder / f"{timestamp}_{safe_name}"

        # Save file
        with open(save_path, 'wb') as f:
            f.write(attachment_data)

        logger.info(f"Saved attachment: {save_path}")

        # Determine file type
        ext = Path(filename).suffix.lower()
        if ext == '.pdf':
            file_type = "pdf"
        elif ext in ['.png', '.jpg', '.jpeg', '.gif']:
            file_type = "image"
        else:
            file_type = "other"

        return AttachmentInfo(
            filename=filename,
            file_type=file_type,
            saved_path=str(save_path),
            content_type=content_type,
            description=f"From: {email_subject}"
        )

    def _determine_content_type(self, filename: str, subject: str) -> str:
        """Determine the content type of an attachment."""
        combined = (filename + " " + subject).lower()

        if "precip" in combined or "rain" in combined:
            return "precipitation_map"
        elif "temp" in combined:
            return "temperature_map"
        elif "outlook" in combined:
            return "outlook_map"
        elif "forecast" in combined:
            return "forecast_map"
        elif "anomal" in combined:
            return "anomaly_map"
        else:
            return "weather_graphic"

    def list_recent_attachments(self, days: int = 7) -> List[AttachmentInfo]:
        """List attachments from recent days."""
        from datetime import timedelta

        attachments = []
        cutoff = datetime.now() - timedelta(days=days)

        for date_folder in self.output_dir.iterdir():
            if not date_folder.is_dir():
                continue

            try:
                folder_date = datetime.strptime(date_folder.name, "%Y-%m-%d")
                if folder_date < cutoff:
                    continue

                for file_path in date_folder.iterdir():
                    ext = file_path.suffix.lower()
                    file_type = "pdf" if ext == ".pdf" else "image" if ext in [".png", ".jpg", ".gif"] else "other"

                    attachments.append(AttachmentInfo(
                        filename=file_path.name,
                        file_type=file_type,
                        saved_path=str(file_path),
                        content_type=self._determine_content_type(file_path.name, "")
                    ))

            except ValueError:
                continue

        return attachments


# Factory function
def get_extractor(email_type: str) -> TextExtractor:
    """Get the appropriate extractor for an email type."""
    # For now, TextExtractor handles all types
    # PDFExtractor is used when attachments are present
    return TextExtractor()


# CLI for testing
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Weather Data Extractors')
    parser.add_argument('--test', action='store_true', help='Run extraction test')

    args = parser.parse_args()

    if args.test:
        # Test extraction
        extractor = TextExtractor()

        test_body = """
        UNITED STATES

        In the Midwest, scattered showers are expected with 0.5-1.0 inches of rain.
        Temperatures will be near normal with highs in the mid-60s F.

        The Southern Plains remain dry with below normal precipitation expected
        through week 2. Wheat conditions are being closely monitored.

        BRAZIL

        Mato Grosso harvest is progressing well with favorable dry conditions.
        Week 2 shows a shift to wetter conditions with 1-2 inches expected.

        ARGENTINA

        Buenos Aires and Cordoba received timely rains last week.
        Week 1 outlook is drier than the prior forecast, which is being
        monitored for stress on late-planted soybeans.
        """

        data = extractor.extract(
            email_id="test123",
            subject="Evening Weather Update -- Comments and Summary",
            body=test_body,
            sender="worldweather@test.com",
            received_at=datetime.now(),
            email_type="scheduled_update"
        )

        print("\nExtraction Test Results")
        print("=" * 50)
        print(f"Email Type: {data.email_type}")
        print(f"Headline: {data.headline_summary}")
        print(f"\nRegions extracted: {list(data.regions.keys())}")

        for region_id, region in data.regions.items():
            print(f"\n{region.region_name}:")
            print(f"  Week 1: {region.week1_summary}")
            if region.week1_precip:
                print(f"  Precip: {region.week1_precip.amount_text}")

        print(f"\nForecast changes: {len(data.forecast_changes)}")
        for change in data.forecast_changes:
            print(f"  - {change.direction.value}: {change.description[:80]}...")

        print(f"\nKey points: {len(data.key_points)}")
        for point in data.key_points[:3]:
            print(f"  - {point[:80]}...")
