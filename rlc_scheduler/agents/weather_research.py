#!/usr/bin/env python3
"""
Weather Research Module

Provides additional context for weather summaries by:
1. Detecting topics that need research (winter kill, drought, etc.)
2. Searching authoritative sources
3. Returning structured context for synthesis

Sources:
- NOAA Climate Prediction Center
- USDA Drought Monitor
- Web search for current conditions
"""

import re
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

# Try to import web search capability
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False


@dataclass
class ResearchTopic:
    """A topic identified for research."""
    topic: str
    keywords: List[str]
    region: str
    urgency: str  # "high", "medium", "low"
    context_needed: str  # Description of what context is needed

    def to_dict(self) -> Dict:
        return {
            "topic": self.topic,
            "keywords": self.keywords,
            "region": self.region,
            "urgency": self.urgency,
            "context_needed": self.context_needed
        }


@dataclass
class ResearchResult:
    """Result from a research query."""
    topic: str
    source: str
    content: str
    url: Optional[str] = None
    retrieved_at: datetime = field(default_factory=datetime.now)
    relevance_score: float = 0.0

    def to_dict(self) -> Dict:
        return {
            "topic": self.topic,
            "source": self.source,
            "content": self.content,
            "url": self.url,
            "retrieved_at": self.retrieved_at.isoformat(),
            "relevance_score": self.relevance_score
        }


# Research topic definitions
RESEARCH_TOPICS = {
    "winter_kill": {
        "triggers": ["winter kill", "winterkill", "freeze damage", "cold damage", "snow cover"],
        "crops": ["wheat", "winter wheat"],
        "regions": ["us_wheat_belt", "us_plains", "europe", "russia", "ukraine"],
        "context_needed": "Temperature thresholds for winter kill, current snow cover status, historical comparisons",
        "search_queries": [
            "winter wheat winterkill risk {region} {year}",
            "wheat cold damage threshold temperature",
            "snow cover protection wheat freezing"
        ],
        "knowledge_base": """
WINTER KILL THRESHOLDS:
- With snow cover (4+ inches): Wheat can survive temps down to -20°F
- Without snow cover: Damage begins at 10-12°F, severe below 0°F
- Crown temperature is key - soil temps matter more than air temps
- Hardened wheat (acclimated) more resistant than non-hardened
- Recovery possible if crown survives - assess in spring

RISK FACTORS:
- Rapid temperature drops (no time to harden)
- Extended periods below 0°F without snow
- Late fall warm spells followed by hard freeze
- Drought-stressed wheat going into winter
"""
    },

    "drought": {
        "triggers": ["drought", "dry conditions", "moisture stress", "below normal precip"],
        "crops": ["corn", "soybeans", "wheat", "cotton"],
        "regions": ["us_corn_belt", "us_wheat_belt", "brazil", "argentina"],
        "context_needed": "Current drought monitor status, soil moisture levels, historical drought comparisons",
        "search_queries": [
            "USDA drought monitor {region} current",
            "{region} soil moisture conditions {year}",
            "drought impact {crop} yield"
        ],
        "knowledge_base": """
DROUGHT IMPACT BY CROP STAGE:
Corn:
- Vegetative: 5-10% yield loss per week of stress
- Pollination (V12-R2): 3-8% yield loss per DAY of severe stress
- Grain fill: 3-5% yield loss per week

Soybeans:
- Flowering (R1-R2): Most sensitive, 40%+ yield loss possible
- Pod fill (R3-R5): 2-4% yield loss per day of stress
- More drought tolerant than corn overall

DROUGHT MONITOR CATEGORIES:
- D0: Abnormally Dry
- D1: Moderate Drought
- D2: Severe Drought
- D3: Extreme Drought
- D4: Exceptional Drought
"""
    },

    "flooding": {
        "triggers": ["flood", "excessive rain", "waterlogged", "ponding", "saturated"],
        "crops": ["corn", "soybeans", "wheat"],
        "regions": ["us_corn_belt", "us_delta", "brazil_south", "argentina"],
        "context_needed": "Flood duration impacts, replant deadlines, yield loss estimates",
        "search_queries": [
            "corn flooding yield loss duration",
            "soybean waterlogged survival days",
            "replant deadline {region} {crop}"
        ],
        "knowledge_base": """
FLOODING SURVIVAL (varies by growth stage and temperature):

Corn:
- Pre-emergence: 24-48 hours at 77°F, longer if cooler
- V6 and later: 24-48 hours, less tolerant than seedlings
- After 4+ days submerged, significant stand loss likely

Soybeans:
- Seedlings: 48-96 hours depending on temperature
- More tolerant than corn at later stages
- Nodule death occurs after 2-3 days, nitrogen fixation impaired

Key factors:
- Temperature: Warmer = faster damage
- Water movement: Flowing water brings O2, standing water worse
- Duration: Most critical factor
"""
    },

    "heat_stress": {
        "triggers": ["heat wave", "heat stress", "high temperatures", "above normal temp", "hot"],
        "crops": ["corn", "soybeans", "wheat", "cotton"],
        "regions": ["us_corn_belt", "us_wheat_belt", "brazil", "argentina", "australia"],
        "context_needed": "Critical temperature thresholds, night temperature importance, duration effects",
        "search_queries": [
            "corn pollination heat stress temperature threshold",
            "night temperature corn yield impact",
            "heat dome {region} crop impact"
        ],
        "knowledge_base": """
HEAT STRESS THRESHOLDS:

Corn Pollination (Critical Period):
- Daytime >95°F: Pollen viability drops significantly
- Night temps >70°F: Reduces kernel set, plant respiration stress
- Combined with drought: Silks dry before pollen shed

Soybeans:
- >95°F: Flower abortion increases
- >100°F: Pod set severely impaired
- Night temps >75°F: Reduces pod retention

Wheat (Heading/Fill):
- >86°F: Accelerates maturity, reduces grain fill
- >95°F: Test weight and protein affected

CRITICAL: Duration matters as much as peak temps
- 3+ consecutive days >100°F = severe impact
- Night temp recovery crucial for plant repair
"""
    },

    "la_nina": {
        "triggers": ["la nina", "la niña", "enso", "pacific cooling"],
        "crops": ["corn", "soybeans", "wheat"],
        "regions": ["us", "brazil", "argentina", "australia"],
        "context_needed": "Current ENSO status, historical La Niña crop impacts by region",
        "search_queries": [
            "la nina forecast {year} strength",
            "la nina impact {region} agriculture",
            "ENSO crop yield correlation"
        ],
        "knowledge_base": """
LA NIÑA TYPICAL IMPACTS:

United States:
- Corn Belt: Variable, often drier in southern portions
- Southern Plains: Drought risk elevated (TX, OK, KS)
- Pacific Northwest: Wetter and cooler

South America:
- Argentina: DROUGHT RISK - La Niña strongly correlated with dry conditions
- Southern Brazil (RS, PR): Drier than normal, reduced yields common
- Central Brazil (MT): Less affected, sometimes beneficial

Australia:
- Eastern Australia: Wetter, can cause flooding
- Good for wheat production

Historical yield impacts:
- 2020-21 La Niña: Argentina corn/soy yields down 10-15%
- 2010-11 La Niña: Similar pattern, Brazil south affected
"""
    },

    "el_nino": {
        "triggers": ["el nino", "el niño", "pacific warming"],
        "crops": ["corn", "soybeans", "wheat"],
        "regions": ["us", "brazil", "argentina", "australia"],
        "context_needed": "Current ENSO status, historical El Niño crop impacts",
        "search_queries": [
            "el nino forecast {year}",
            "el nino agriculture impact {region}"
        ],
        "knowledge_base": """
EL NIÑO TYPICAL IMPACTS:

United States:
- Corn Belt: Often favorable, adequate moisture
- Southern US: Wetter and cooler winters
- Northern Plains: Warmer winters, variable moisture

South America:
- Argentina: Generally FAVORABLE - good moisture
- Southern Brazil: Often beneficial
- Northern Brazil: Can bring drought to northeast

Australia:
- Eastern Australia: Drought risk elevated
- Wheat production often reduced
"""
    },

    "frost": {
        "triggers": ["frost", "freeze warning", "cold snap", "freezing temperatures"],
        "crops": ["corn", "soybeans", "coffee", "citrus", "cotton"],
        "regions": ["us_corn_belt", "brazil_south", "us_delta", "florida"],
        "context_needed": "Crop growth stage vulnerability, frost protection, damage assessment",
        "search_queries": [
            "frost damage {crop} growth stage",
            "{region} freeze forecast",
            "late spring frost crop insurance"
        ],
        "knowledge_base": """
FROST DAMAGE BY CROP:

Corn:
- Pre-V6: Growing point below ground, usually survives
- V6+: Growing point above ground, vulnerable
- 28°F for 2+ hours: Significant leaf damage
- Below 28°F: Potential growing point death

Soybeans:
- Pre-emergence: Can survive light frost
- Emerged: 28°F kills leaves, 25°F kills stems
- Pod fill: Frost can damage developing beans

Coffee (Brazil):
- Light frost (32-34°F): Leaf damage
- Hard frost (<28°F): Branch and trunk damage
- Severe frost: Tree death, multi-year impact

Citrus (Florida):
- 28°F for 4+ hours: Fruit damage
- 24°F: Tree damage possible
"""
    },

    "harvest_progress": {
        "triggers": ["harvest delay", "harvest pace", "harvest progress", "behind average"],
        "crops": ["corn", "soybeans", "wheat", "cotton"],
        "regions": ["us", "brazil", "argentina"],
        "context_needed": "Current progress vs average, quality implications, price impact",
        "search_queries": [
            "USDA crop progress report {crop} harvest",
            "{region} harvest pace {year}",
            "delayed harvest quality impact {crop}"
        ],
        "knowledge_base": """
HARVEST DELAY IMPLICATIONS:

Quality Issues:
- Corn: Increased moisture, drying costs, potential mold
- Soybeans: Shattering risk, moisture variability
- Wheat: Sprouting if wet, falling numbers decline

Market Implications:
- Basis typically strengthens with slow harvest
- Farmer selling pace slows
- Commercials may bid up for immediate needs
- Can push demand to next marketing year

Typical US Harvest Windows:
- Corn: Sept 15 - Nov 30 (peak Oct)
- Soybeans: Sept 10 - Nov 15 (peak Oct)
- Winter Wheat: June 1 - July 31
- Spring Wheat: Aug 1 - Sept 30
"""
    }
}


class WeatherResearchEngine:
    """
    Research engine that identifies topics needing context and retrieves information.
    """

    def __init__(self):
        self.topics = RESEARCH_TOPICS
        self.cache = {}  # Simple in-memory cache
        self.cache_ttl = timedelta(hours=4)  # Cache results for 4 hours

    def identify_topics(
        self,
        text: str,
        regions: List[str] = None
    ) -> List[ResearchTopic]:
        """
        Identify topics in text that would benefit from additional research.

        Args:
            text: Weather text to analyze (subject + body)
            regions: Regions mentioned in the weather data

        Returns:
            List of ResearchTopic objects
        """
        text_lower = text.lower()
        regions = regions or []
        identified = []

        for topic_id, topic_config in self.topics.items():
            # Check for trigger words
            triggers_found = []
            for trigger in topic_config["triggers"]:
                if trigger.lower() in text_lower:
                    triggers_found.append(trigger)

            if not triggers_found:
                continue

            # Check for relevant crops
            crops_mentioned = []
            for crop in topic_config.get("crops", []):
                if crop.lower() in text_lower:
                    crops_mentioned.append(crop)

            # Determine region
            relevant_regions = []
            for region in topic_config.get("regions", []):
                # Check if this topic's region matches any detected region
                if any(region in r or r in region for r in regions):
                    relevant_regions.append(region)

            if not relevant_regions and regions:
                relevant_regions = regions[:2]  # Use first 2 detected regions

            # Determine urgency based on number of triggers and context
            urgency = "low"
            if len(triggers_found) >= 2:
                urgency = "medium"
            if any(word in text_lower for word in ["severe", "significant", "critical", "warning"]):
                urgency = "high"

            identified.append(ResearchTopic(
                topic=topic_id,
                keywords=triggers_found + crops_mentioned,
                region=", ".join(relevant_regions) if relevant_regions else "general",
                urgency=urgency,
                context_needed=topic_config["context_needed"]
            ))

        # Sort by urgency
        urgency_order = {"high": 0, "medium": 1, "low": 2}
        identified.sort(key=lambda x: urgency_order.get(x.urgency, 3))

        return identified

    def get_knowledge_base_context(self, topic_id: str) -> str:
        """Get pre-loaded knowledge base content for a topic."""
        topic_config = self.topics.get(topic_id, {})
        return topic_config.get("knowledge_base", "")

    def research_topic(
        self,
        topic: ResearchTopic,
        use_web_search: bool = True
    ) -> List[ResearchResult]:
        """
        Research a specific topic.

        Args:
            topic: ResearchTopic to research
            use_web_search: Whether to perform web searches

        Returns:
            List of ResearchResult objects
        """
        results = []

        # 1. Always include knowledge base content
        kb_content = self.get_knowledge_base_context(topic.topic)
        if kb_content:
            results.append(ResearchResult(
                topic=topic.topic,
                source="knowledge_base",
                content=kb_content,
                relevance_score=1.0
            ))

        # 2. Check cache for recent web results
        cache_key = f"{topic.topic}_{topic.region}"
        if cache_key in self.cache:
            cached_time, cached_results = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_ttl:
                results.extend(cached_results)
                return results

        # 3. Perform web search if enabled
        if use_web_search and REQUESTS_AVAILABLE:
            web_results = self._web_search(topic)
            results.extend(web_results)

            # Cache the web results
            self.cache[cache_key] = (datetime.now(), web_results)

        return results

    def _web_search(self, topic: ResearchTopic) -> List[ResearchResult]:
        """
        Perform web search for topic.

        Note: This is a placeholder. In production, you would integrate with:
        - Brave Search API
        - Google Custom Search
        - Bing Search API
        - Or a news API
        """
        results = []

        # For now, return structured guidance on what to search
        topic_config = self.topics.get(topic.topic, {})
        search_queries = topic_config.get("search_queries", [])

        current_year = datetime.now().year

        # Format search queries with region and year
        formatted_queries = []
        for query in search_queries[:2]:  # Limit to 2 queries
            formatted = query.format(
                region=topic.region,
                year=current_year,
                crop=topic.keywords[0] if topic.keywords else ""
            )
            formatted_queries.append(formatted)

        if formatted_queries:
            results.append(ResearchResult(
                topic=topic.topic,
                source="search_suggestions",
                content=f"Suggested searches for more context:\n" + "\n".join(f"- {q}" for q in formatted_queries),
                relevance_score=0.5
            ))

        return results

    def get_research_context(
        self,
        text: str,
        regions: List[str] = None,
        max_topics: int = 3
    ) -> Dict[str, Any]:
        """
        Main entry point: analyze text and return research context.

        Args:
            text: Weather text to analyze
            regions: Regions mentioned
            max_topics: Maximum number of topics to research

        Returns:
            Dict with topics identified, results, and formatted context
        """
        # Identify topics
        topics = self.identify_topics(text, regions)[:max_topics]

        if not topics:
            return {
                "topics_identified": [],
                "results": [],
                "context_for_llm": ""
            }

        # Research each topic
        all_results = []
        for topic in topics:
            results = self.research_topic(topic)
            all_results.extend(results)

        # Format context for LLM
        context_lines = [
            "=== ADDITIONAL CONTEXT FROM RESEARCH ===",
            ""
        ]

        for topic in topics:
            context_lines.append(f"TOPIC: {topic.topic.upper().replace('_', ' ')}")
            context_lines.append(f"Urgency: {topic.urgency}")
            context_lines.append(f"Keywords detected: {', '.join(topic.keywords)}")
            context_lines.append("")

            # Add knowledge base content
            kb_content = self.get_knowledge_base_context(topic.topic)
            if kb_content:
                # Truncate if too long
                if len(kb_content) > 1000:
                    kb_content = kb_content[:1000] + "..."
                context_lines.append(kb_content)

            context_lines.append("")
            context_lines.append("-" * 40)
            context_lines.append("")

        return {
            "topics_identified": [t.to_dict() for t in topics],
            "results": [r.to_dict() for r in all_results],
            "context_for_llm": "\n".join(context_lines)
        }


def get_current_crop_context() -> str:
    """
    Get current crop calendar context based on today's date.

    Returns context about what crops are in critical stages.
    """
    month = datetime.now().month

    # Northern hemisphere crop calendar
    nh_context = {
        1: "Winter wheat in dormancy (watch for winterkill). Planning for spring planting.",
        2: "Winter wheat dormancy continues. Early spring fieldwork in south.",
        3: "Winter wheat greenup begins. Corn planting prep in southern Corn Belt.",
        4: "Corn planting begins. Winter wheat jointing. Soybean planting starts late month.",
        5: "CRITICAL: Corn/soybean planting progress. Winter wheat heading.",
        6: "Corn enters critical growth. Winter wheat harvest begins in south.",
        7: "CRITICAL: Corn pollination (most yield-sensitive period). Soybean flowering.",
        8: "Corn grain fill. Soybean pod fill. Spring wheat harvest.",
        9: "Corn maturity. Early harvest begins. Soybean maturity.",
        10: "Harvest in full swing. Winter wheat planting begins.",
        11: "Harvest completion. Winter wheat establishment.",
        12: "Winter wheat dormancy begins. Year-end positioning."
    }

    # Southern hemisphere (Brazil/Argentina)
    sh_context = {
        1: "Brazil soybean pod fill/early harvest (MT). Argentina corn pollination.",
        2: "Brazil soy harvest accelerating. Safrinha corn planting. Argentina development.",
        3: "Brazil soy harvest finishing. Safrinha corn critical development.",
        4: "Safrinha corn development. Argentina harvest begins.",
        5: "Safrinha corn approaching maturity. Argentina harvest.",
        6: "Safrinha harvest. Brazil wheat planting in south.",
        7: "Off-season. Brazil wheat development.",
        8: "Brazil wheat development continues.",
        9: "Brazil planting preparations. Early soy planting MT.",
        10: "CRITICAL: Brazil soybean planting window opens.",
        11: "Brazil soy planting continues. Argentina planting begins.",
        12: "Brazil soy development. Argentina corn/soy planting."
    }

    return f"""CURRENT CROP CALENDAR CONTEXT ({datetime.now().strftime('%B %Y')}):

Northern Hemisphere (US, Europe):
{nh_context.get(month, "N/A")}

Southern Hemisphere (Brazil, Argentina):
{sh_context.get(month, "N/A")}
"""


# CLI for testing
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Weather Research Engine')
    parser.add_argument('--test', action='store_true', help='Run test')
    parser.add_argument('--text', help='Text to analyze')

    args = parser.parse_args()

    engine = WeatherResearchEngine()

    if args.test:
        # Test with sample text
        test_texts = [
            "U.S. Wheat Needs Snow Ahead Of Bitter Cold - Winterkill risk increasing in Southern Plains",
            "Argentina facing dry conditions, La Nina pattern persists through February",
            "Brazil Mato Grosso soybean harvest progressing well, scattered showers causing minor delays",
            "Corn Belt heat wave expected next week, temperatures above 95F during pollination"
        ]

        print("Weather Research Engine Test")
        print("=" * 60)

        for text in test_texts:
            print(f"\nAnalyzing: {text[:60]}...")
            print("-" * 40)

            result = engine.get_research_context(
                text,
                regions=["us_wheat_belt", "argentina", "brazil_center_west", "us_corn_belt"]
            )

            if result["topics_identified"]:
                for topic in result["topics_identified"]:
                    print(f"  Topic: {topic['topic']}")
                    print(f"  Urgency: {topic['urgency']}")
                    print(f"  Keywords: {topic['keywords']}")
            else:
                print("  No research topics identified")
            print()

        # Print crop calendar
        print("\n" + "=" * 60)
        print(get_current_crop_context())

    elif args.text:
        result = engine.get_research_context(args.text)
        print(json.dumps(result, indent=2, default=str))
