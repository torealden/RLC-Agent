#!/usr/bin/env python3
"""
Weather Email Classifier

Classifies World Weather Inc. emails by type and returns the appropriate
extraction strategy. This is Phase 1A of the Weather Intelligence Enhancement.

Email Types:
- commentary: Breaking news, analysis (highest priority)
- scheduled_update: Morning/Mid-day/Evening updates
- outlook_forecast: Forward-looking forecasts with PDFs
- maps_graphics: Visual content (maps, charts)
- audio_update: Audio briefings
- specialty_regional: Niche crop/regional updates
"""

import json
import re
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

# Configuration paths
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
CONFIG_FILE = CONFIG_DIR / "weather_email_types.json"


@dataclass
class EmailClassification:
    """Result of email classification."""
    email_type: str
    confidence: float  # 0.0 to 1.0
    matched_patterns: List[str]
    extraction_strategy: Dict[str, Any]
    priority: int
    market_relevance: str
    regions_detected: List[str] = field(default_factory=list)
    keywords_detected: List[str] = field(default_factory=list)
    sentiment: str = "neutral"  # bullish, bearish, neutral

    def to_dict(self) -> Dict:
        return {
            "email_type": self.email_type,
            "confidence": self.confidence,
            "matched_patterns": self.matched_patterns,
            "extraction_strategy": self.extraction_strategy,
            "priority": self.priority,
            "market_relevance": self.market_relevance,
            "regions_detected": self.regions_detected,
            "keywords_detected": self.keywords_detected,
            "sentiment": self.sentiment
        }


class WeatherEmailClassifier:
    """
    Classifies weather emails by type and determines extraction strategy.

    Usage:
        classifier = WeatherEmailClassifier()
        result = classifier.classify(subject, body_preview)
        print(f"Type: {result.email_type}, Strategy: {result.extraction_strategy}")
    """

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize classifier with configuration."""
        self.config_path = config_path or CONFIG_FILE
        self.config = self._load_config()
        self.email_types = self.config.get("email_types", {})
        self.regions = self.config.get("regions", {})
        self.weather_keywords = self.config.get("weather_keywords", {})

    def _load_config(self) -> Dict:
        """Load classification configuration from JSON file."""
        if not self.config_path.exists():
            logger.warning(f"Config file not found: {self.config_path}")
            return self._get_default_config()

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing config: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict:
        """Return minimal default configuration."""
        return {
            "email_types": {
                "unknown": {
                    "priority": 5,
                    "market_relevance": "low",
                    "extraction": {"primary_source": "body"}
                }
            },
            "regions": {},
            "weather_keywords": {
                "bullish_triggers": ["drought", "dry", "heat", "frost"],
                "bearish_triggers": ["favorable", "ideal", "improving"],
                "neutral": ["normal", "average"]
            }
        }

    def classify(
        self,
        subject: str,
        body_preview: str = "",
        attachments: List[str] = None
    ) -> EmailClassification:
        """
        Classify an email based on subject line and optional body preview.

        Args:
            subject: Email subject line
            body_preview: First ~500 chars of body (optional)
            attachments: List of attachment filenames (optional)

        Returns:
            EmailClassification with type, strategy, and detected features
        """
        attachments = attachments or []

        # Try to match each email type
        matches = []

        for type_name, type_config in self.email_types.items():
            score, matched = self._match_type(subject, type_config)
            if score > 0:
                matches.append((type_name, score, matched, type_config))

        # Sort by score (descending), then by priority (ascending)
        matches.sort(key=lambda x: (-x[1], x[3].get("priority", 5)))

        if matches:
            best_match = matches[0]
            email_type = best_match[0]
            confidence = min(best_match[1], 1.0)
            matched_patterns = best_match[2]
            type_config = best_match[3]
        else:
            # Default to unknown
            email_type = "unknown"
            confidence = 0.0
            matched_patterns = []
            type_config = {"priority": 5, "market_relevance": "low", "extraction": {"primary_source": "body"}}

        # Detect regions mentioned
        regions_detected = self._detect_regions(subject + " " + body_preview)

        # Detect weather keywords and sentiment
        keywords_detected, sentiment = self._detect_keywords_and_sentiment(subject + " " + body_preview)

        # Build extraction strategy
        extraction_strategy = self._build_extraction_strategy(
            type_config.get("extraction", {}),
            attachments
        )

        return EmailClassification(
            email_type=email_type,
            confidence=confidence,
            matched_patterns=matched_patterns,
            extraction_strategy=extraction_strategy,
            priority=type_config.get("priority", 5),
            market_relevance=type_config.get("market_relevance", "low"),
            regions_detected=regions_detected,
            keywords_detected=keywords_detected,
            sentiment=sentiment
        )

    def _match_type(self, subject: str, type_config: Dict) -> Tuple[float, List[str]]:
        """
        Match subject against type patterns.

        Returns:
            Tuple of (score, matched_patterns)
        """
        patterns = type_config.get("subject_patterns", [])
        exclude_patterns = type_config.get("exclude_patterns", [])

        # Check exclusions first
        for exclude in exclude_patterns:
            if re.search(exclude, subject, re.IGNORECASE):
                return 0.0, []

        matched = []
        for pattern in patterns:
            # Handle both simple substring and regex patterns
            try:
                if re.search(pattern, subject, re.IGNORECASE):
                    matched.append(pattern)
            except re.error:
                # Treat as simple substring match if regex fails
                if pattern.lower() in subject.lower():
                    matched.append(pattern)

        if not matched:
            return 0.0, []

        # Score based on number of matches and specificity
        score = len(matched) * 0.3

        # Bonus for longer/more specific patterns
        for pattern in matched:
            if len(pattern) > 20:
                score += 0.2
            elif len(pattern) > 10:
                score += 0.1

        return min(score, 1.0), matched

    def _detect_regions(self, text: str) -> List[str]:
        """Detect agricultural regions mentioned in text."""
        detected = []
        text_lower = text.lower()

        for region_id, region_config in self.regions.items():
            aliases = region_config.get("aliases", [])
            for alias in aliases:
                if alias.lower() in text_lower:
                    if region_id not in detected:
                        detected.append(region_id)
                    break

        return detected

    def _detect_keywords_and_sentiment(self, text: str) -> Tuple[List[str], str]:
        """
        Detect weather keywords and determine overall sentiment.

        Returns:
            Tuple of (keywords_found, sentiment)
        """
        text_lower = text.lower()
        keywords_found = []

        bullish_count = 0
        bearish_count = 0

        for keyword in self.weather_keywords.get("bullish_triggers", []):
            if keyword.lower() in text_lower:
                keywords_found.append(keyword)
                bullish_count += 1

        for keyword in self.weather_keywords.get("bearish_triggers", []):
            if keyword.lower() in text_lower:
                keywords_found.append(keyword)
                bearish_count += 1

        # Determine sentiment
        if bullish_count > bearish_count + 1:
            sentiment = "bullish"
        elif bearish_count > bullish_count + 1:
            sentiment = "bearish"
        else:
            sentiment = "neutral"

        return keywords_found, sentiment

    def _build_extraction_strategy(
        self,
        base_strategy: Dict,
        attachments: List[str]
    ) -> Dict[str, Any]:
        """
        Build extraction strategy based on type config and actual attachments.
        """
        strategy = dict(base_strategy)

        # Analyze attachments
        has_pdf = any(a.lower().endswith('.pdf') for a in attachments)
        has_images = any(
            a.lower().endswith(('.png', '.jpg', '.jpeg', '.gif'))
            for a in attachments
        )

        strategy["attachments"] = {
            "has_pdf": has_pdf,
            "has_images": has_images,
            "files": attachments
        }

        # Adjust source based on what's available
        if strategy.get("primary_source") == "pdf" and not has_pdf:
            strategy["primary_source"] = strategy.get("fallback_source", "body")

        return strategy

    def get_type_info(self, email_type: str) -> Optional[Dict]:
        """Get configuration for a specific email type."""
        return self.email_types.get(email_type)

    def list_types(self) -> List[str]:
        """List all configured email types."""
        return list(self.email_types.keys())

    def get_region_info(self, region_id: str) -> Optional[Dict]:
        """Get configuration for a specific region."""
        return self.regions.get(region_id)

    def get_current_critical_period(self, region_id: str) -> Optional[str]:
        """
        Get the current critical crop period for a region based on current month.

        Returns:
            Description of current critical period, or None
        """
        region = self.regions.get(region_id)
        if not region:
            return None

        current_month = datetime.now().strftime("%B")
        critical_periods = region.get("critical_periods", {})

        for period_name, months in critical_periods.items():
            if current_month in months:
                return period_name

        return None


def classify_email_batch(
    emails: List[Dict[str, Any]],
    classifier: WeatherEmailClassifier = None
) -> List[Dict[str, Any]]:
    """
    Classify a batch of emails.

    Args:
        emails: List of email dicts with 'subject', 'body', and optional 'attachments'
        classifier: Optional pre-initialized classifier

    Returns:
        List of emails with 'classification' field added
    """
    if classifier is None:
        classifier = WeatherEmailClassifier()

    results = []
    for email in emails:
        classification = classifier.classify(
            subject=email.get("subject", ""),
            body_preview=email.get("body", "")[:500],
            attachments=email.get("attachments", [])
        )

        email_with_class = dict(email)
        email_with_class["classification"] = classification.to_dict()
        results.append(email_with_class)

    # Sort by priority
    results.sort(key=lambda x: x["classification"]["priority"])

    return results


# CLI for testing
def main():
    """Test the classifier with sample subjects."""
    import argparse

    parser = argparse.ArgumentParser(description='Weather Email Classifier')
    parser.add_argument('--subject', '-s', help='Subject line to classify')
    parser.add_argument('--test', action='store_true', help='Run test suite')
    parser.add_argument('--list-types', action='store_true', help='List email types')

    args = parser.parse_args()

    classifier = WeatherEmailClassifier()

    if args.list_types:
        print("\nConfigured Email Types:")
        print("-" * 40)
        for type_name in classifier.list_types():
            info = classifier.get_type_info(type_name)
            print(f"  {type_name}:")
            print(f"    Priority: {info.get('priority')}")
            print(f"    Relevance: {info.get('market_relevance')}")
            print(f"    Examples: {info.get('examples', [])[:2]}")
            print()
        return

    if args.test:
        # Test with sample subjects
        test_subjects = [
            "U.S. Wheat Needs Snow Ahead Of Bitter Cold",
            "Evening Weather Update for U.S. and South America -- Comments and Summary",
            "World Weather Outlook for January 21, 2026",
            "Updated 24-Hour North and South America Rainfall and Temperature Maps",
            "Monday Evening Audio Weather Update for Monday, January 19, 2026",
            "Weekly Citrus Weather For Wednesday, January 21, 2025",
            "Mid-day U.S. and South America Weather Update -- Week 1",
            "Portions of Southern Argentina Too Dry; Rain Moistens North",
            "China's Winter Crops In Good Shape; Southeast In Drought",
            "Southern Brazil Dryness Good For Early-Season Harvesting"
        ]

        print("\nEmail Classification Test Results")
        print("=" * 70)

        for subject in test_subjects:
            result = classifier.classify(subject)
            print(f"\nSubject: {subject}")
            print(f"  Type: {result.email_type} (confidence: {result.confidence:.2f})")
            print(f"  Priority: {result.priority}, Relevance: {result.market_relevance}")
            print(f"  Regions: {result.regions_detected}")
            print(f"  Keywords: {result.keywords_detected}, Sentiment: {result.sentiment}")
            print(f"  Strategy: {result.extraction_strategy.get('primary_source', 'body')}")

        return

    if args.subject:
        result = classifier.classify(args.subject)
        print(f"\nClassification Result:")
        print(json.dumps(result.to_dict(), indent=2))
        return

    # Interactive mode
    print("\nWeather Email Classifier - Interactive Mode")
    print("Enter email subjects to classify (Ctrl+C to exit)\n")

    while True:
        try:
            subject = input("Subject: ").strip()
            if not subject:
                continue

            result = classifier.classify(subject)
            print(f"  → Type: {result.email_type}")
            print(f"  → Confidence: {result.confidence:.2f}")
            print(f"  → Regions: {', '.join(result.regions_detected) or 'None detected'}")
            print(f"  → Sentiment: {result.sentiment}")
            print()

        except KeyboardInterrupt:
            print("\nExiting...")
            break


if __name__ == "__main__":
    main()
