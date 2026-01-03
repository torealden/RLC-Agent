"""
Market Research Agent

Handles researching weekly market developments, identifying bullish/bearish
factors, and determining swing catalysts for each commodity.
Uses LLM capabilities and structured data analysis.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple

from ..config.settings import HBWeeklyReportConfig, LLMConfig

logger = logging.getLogger(__name__)


@dataclass
class MarketFactor:
    """A bullish or bearish market factor"""
    description: str
    factor_type: str  # "bullish" or "bearish"
    commodity: str
    source: str  # "data", "news", "analysis", "llm"
    confidence: float = 1.0  # 0-1
    data_support: Optional[str] = None  # Supporting data reference


@dataclass
class SwingFactor:
    """A potential market catalyst"""
    description: str
    commodity: str
    category: str  # "policy", "weather", "demand", "supply", "macro"
    timeframe: str  # "near_term", "medium_term", "long_term"
    probability: Optional[float] = None


@dataclass
class CommodityAnalysis:
    """Complete analysis for a single commodity"""
    commodity: str
    bullish_factors: List[MarketFactor] = field(default_factory=list)
    bearish_factors: List[MarketFactor] = field(default_factory=list)
    swing_factors: List[SwingFactor] = field(default_factory=list)

    # Price context
    price_direction: str = ""  # "up", "down", "flat"
    price_context: str = ""  # Summary of price action

    # Overall sentiment
    overall_sentiment: str = ""  # "bullish", "bearish", "neutral"
    sentiment_score: float = 0.0  # -1 (bearish) to +1 (bullish)

    # LLM-generated content flags
    has_llm_content: bool = False
    llm_confidence: float = 1.0


@dataclass
class MarketResearchResult:
    """Complete market research result"""
    success: bool
    research_date: date
    analyzed_at: datetime = field(default_factory=datetime.utcnow)

    # Per-commodity analysis
    commodity_analyses: Dict[str, CommodityAnalysis] = field(default_factory=dict)

    # Macro context
    macro_developments: List[str] = field(default_factory=list)
    weather_summary: str = ""

    # Key triggers (watchlist)
    key_triggers: List[SwingFactor] = field(default_factory=list)

    # Metadata
    data_sources_used: List[str] = field(default_factory=list)
    llm_calls_made: int = 0
    errors: List[str] = field(default_factory=list)


class MarketResearchAgent:
    """
    Agent for conducting market research and analysis

    Responsibilities:
    - Identify bullish and bearish fundamental developments
    - Determine swing factors/catalysts for each market
    - Generate macro and weather context
    - Compile key triggers watchlist
    """

    def __init__(
        self,
        config: HBWeeklyReportConfig,
        internal_data_agent=None,
        price_data_agent=None
    ):
        """
        Initialize Market Research Agent

        Args:
            config: HB Weekly Report configuration
            internal_data_agent: Agent for internal data access
            price_data_agent: Agent for price data access
        """
        self.config = config
        self.llm_config = config.llm
        self.internal_data_agent = internal_data_agent
        self.price_data_agent = price_data_agent
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # LLM client (lazy initialization)
        self._llm_client = None

        self.logger.info("Initialized MarketResearchAgent")

    @property
    def llm_client(self):
        """Lazy initialization of LLM client"""
        if self._llm_client is None and self.llm_config.enabled:
            self._llm_client = self._create_llm_client()
        return self._llm_client

    def _create_llm_client(self):
        """Create LLM client based on configuration"""
        try:
            if self.llm_config.provider == "openai":
                import openai
                client = openai.OpenAI(api_key=self.llm_config.api_key)
                return client
            elif self.llm_config.provider == "ollama":
                # Use requests for Ollama
                return {"type": "ollama", "base_url": self.llm_config.ollama_base_url}
            elif self.llm_config.provider == "anthropic":
                import anthropic
                client = anthropic.Anthropic(api_key=self.llm_config.api_key)
                return client
            else:
                self.logger.warning(f"Unknown LLM provider: {self.llm_config.provider}")
                return None
        except ImportError as e:
            self.logger.error(f"LLM package not installed: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Failed to create LLM client: {e}")
            return None

    def conduct_research(
        self,
        report_date: date = None,
        commodities: List[str] = None
    ) -> MarketResearchResult:
        """
        Conduct comprehensive market research for all commodities

        Args:
            report_date: Date for the report
            commodities: List of commodities to analyze

        Returns:
            MarketResearchResult with complete analysis
        """
        report_date = report_date or date.today()
        commodities = commodities or self.config.commodities.primary_commodities

        result = MarketResearchResult(
            success=True,
            research_date=report_date,
        )

        self.logger.info(f"Conducting market research for {report_date}, commodities: {commodities}")

        try:
            # Gather data inputs
            internal_data = self._get_internal_data_context()
            price_data = self._get_price_data_context()

            # Analyze each commodity
            for commodity in commodities:
                try:
                    analysis = self._analyze_commodity(
                        commodity,
                        internal_data,
                        price_data,
                        report_date
                    )
                    result.commodity_analyses[commodity] = analysis

                    if analysis.has_llm_content:
                        result.llm_calls_made += 1

                except Exception as e:
                    self.logger.error(f"Error analyzing {commodity}: {e}")
                    result.errors.append(f"{commodity}: {str(e)}")

            # Generate macro context
            result.macro_developments = self._analyze_macro_context(report_date)

            # Generate weather summary
            result.weather_summary = self._analyze_weather_context(report_date)

            # Compile key triggers
            result.key_triggers = self._compile_key_triggers(result.commodity_analyses)

            result.data_sources_used = ["internal_spreadsheet", "price_api"]
            if result.llm_calls_made > 0:
                result.data_sources_used.append("llm_analysis")

        except Exception as e:
            self.logger.error(f"Research failed: {e}", exc_info=True)
            result.success = False
            result.errors.append(str(e))

        return result

    def _get_internal_data_context(self) -> Dict[str, Any]:
        """Get relevant internal data for analysis"""
        if not self.internal_data_agent:
            return {}

        try:
            data_result = self.internal_data_agent.fetch_data()
            if data_result.success:
                return {
                    "supply_demand": data_result.supply_demand,
                    "forecasts": data_result.forecasts,
                }
        except Exception as e:
            self.logger.warning(f"Could not get internal data: {e}")

        return {}

    def _get_price_data_context(self) -> Dict[str, Any]:
        """Get price data for analysis"""
        if not self.price_data_agent:
            return {}

        try:
            price_result = self.price_data_agent.fetch_prices()
            if price_result.success:
                return {"prices": price_result.prices}
        except Exception as e:
            self.logger.warning(f"Could not get price data: {e}")

        return {}

    def _analyze_commodity(
        self,
        commodity: str,
        internal_data: Dict,
        price_data: Dict,
        report_date: date
    ) -> CommodityAnalysis:
        """
        Analyze a single commodity for bullish/bearish factors

        Args:
            commodity: Commodity to analyze
            internal_data: Internal HB data
            price_data: Price data
            report_date: Report date

        Returns:
            CommodityAnalysis with complete analysis
        """
        analysis = CommodityAnalysis(commodity=commodity)

        # Analyze price movement
        price_context = self._analyze_price_movement(commodity, price_data)
        analysis.price_direction = price_context.get("direction", "flat")
        analysis.price_context = price_context.get("summary", "")

        # Identify bullish factors from data
        bullish = self._identify_bullish_factors(commodity, internal_data, price_data)
        analysis.bullish_factors.extend(bullish)

        # Identify bearish factors from data
        bearish = self._identify_bearish_factors(commodity, internal_data, price_data)
        analysis.bearish_factors.extend(bearish)

        # Use LLM to supplement if needed
        min_bullish = self.config.commodities.min_bullish_factors
        min_bearish = self.config.commodities.min_bearish_factors

        if len(analysis.bullish_factors) < min_bullish or len(analysis.bearish_factors) < min_bearish:
            llm_factors = self._get_llm_factors(commodity, internal_data, price_data, report_date)
            if llm_factors:
                analysis.has_llm_content = True

                # Add LLM-identified factors
                for factor in llm_factors.get("bullish", []):
                    if len(analysis.bullish_factors) < min_bullish:
                        analysis.bullish_factors.append(MarketFactor(
                            description=factor,
                            factor_type="bullish",
                            commodity=commodity,
                            source="llm",
                            confidence=0.8
                        ))

                for factor in llm_factors.get("bearish", []):
                    if len(analysis.bearish_factors) < min_bearish:
                        analysis.bearish_factors.append(MarketFactor(
                            description=factor,
                            factor_type="bearish",
                            commodity=commodity,
                            source="llm",
                            confidence=0.8
                        ))

        # Identify swing factors
        analysis.swing_factors = self._identify_swing_factors(commodity, internal_data, report_date)

        # Calculate sentiment
        analysis.sentiment_score = self._calculate_sentiment(analysis)
        if analysis.sentiment_score > 0.2:
            analysis.overall_sentiment = "bullish"
        elif analysis.sentiment_score < -0.2:
            analysis.overall_sentiment = "bearish"
        else:
            analysis.overall_sentiment = "neutral"

        return analysis

    def _analyze_price_movement(
        self,
        commodity: str,
        price_data: Dict
    ) -> Dict[str, str]:
        """Analyze price movement for commodity"""
        prices = price_data.get("prices", {})

        # Find relevant price series
        series_map = {
            "corn": "corn_front_month",
            "wheat": "wheat_hrw_front_month",
            "soybeans": "soybeans_front_month",
            "soybean_meal": "soybean_meal_front_month",
            "soybean_oil": "soybean_oil_front_month",
        }

        series_id = series_map.get(commodity)
        if not series_id or series_id not in prices:
            return {"direction": "flat", "summary": "Price data not available"}

        comparison = prices[series_id]

        # Determine direction
        if comparison.week_change is not None:
            if comparison.week_change > 0:
                direction = "up"
            elif comparison.week_change < 0:
                direction = "down"
            else:
                direction = "flat"
        else:
            direction = "flat"

        # Generate summary
        current = comparison.current.price if comparison.current else None
        week_change = comparison.week_change
        week_change_pct = comparison.week_change_pct

        if current and week_change is not None:
            summary = f"Prices {'rose' if direction == 'up' else 'fell' if direction == 'down' else 'remained flat'}"
            if week_change_pct:
                summary += f" by {abs(week_change_pct):.1f}% over the week"
        else:
            summary = "Price movement data incomplete"

        return {"direction": direction, "summary": summary}

    def _identify_bullish_factors(
        self,
        commodity: str,
        internal_data: Dict,
        price_data: Dict
    ) -> List[MarketFactor]:
        """Identify bullish factors from data"""
        factors = []

        supply_demand = internal_data.get("supply_demand", {}).get(commodity)

        # Check for bullish signals in supply/demand
        if supply_demand is not None:
            # Lower ending stocks = bullish
            # Higher exports = bullish
            # Lower production = bullish
            # These checks would be based on actual data structure
            pass

        # Check for bullish price signals
        prices = price_data.get("prices", {})
        series_map = {
            "corn": "corn_front_month",
            "wheat": "wheat_hrw_front_month",
            "soybeans": "soybeans_front_month",
        }
        series_id = series_map.get(commodity)

        if series_id and series_id in prices:
            comparison = prices[series_id]

            # Strong week-over-week gain
            if comparison.week_change_pct and comparison.week_change_pct > 2:
                factors.append(MarketFactor(
                    description=f"Prices gained {comparison.week_change_pct:.1f}% this week, indicating strengthening demand",
                    factor_type="bullish",
                    commodity=commodity,
                    source="data",
                    data_support=f"Week change: +{comparison.week_change_pct:.1f}%"
                ))

            # Price above year-ago
            if comparison.year_change and comparison.year_change > 0:
                factors.append(MarketFactor(
                    description=f"Prices remain above year-ago levels, supporting producer margins",
                    factor_type="bullish",
                    commodity=commodity,
                    source="data",
                    data_support=f"Year change: +{comparison.year_change:.2f}"
                ))

        return factors

    def _identify_bearish_factors(
        self,
        commodity: str,
        internal_data: Dict,
        price_data: Dict
    ) -> List[MarketFactor]:
        """Identify bearish factors from data"""
        factors = []

        supply_demand = internal_data.get("supply_demand", {}).get(commodity)

        # Check for bearish signals in supply/demand
        if supply_demand is not None:
            # Higher ending stocks = bearish
            # Lower exports = bearish
            # Higher production = bearish
            pass

        # Check for bearish price signals
        prices = price_data.get("prices", {})
        series_map = {
            "corn": "corn_front_month",
            "wheat": "wheat_hrw_front_month",
            "soybeans": "soybeans_front_month",
        }
        series_id = series_map.get(commodity)

        if series_id and series_id in prices:
            comparison = prices[series_id]

            # Week-over-week decline
            if comparison.week_change_pct and comparison.week_change_pct < -2:
                factors.append(MarketFactor(
                    description=f"Prices declined {abs(comparison.week_change_pct):.1f}% this week under selling pressure",
                    factor_type="bearish",
                    commodity=commodity,
                    source="data",
                    data_support=f"Week change: {comparison.week_change_pct:.1f}%"
                ))

            # Price below year-ago
            if comparison.year_change and comparison.year_change < 0:
                factors.append(MarketFactor(
                    description=f"Prices remain below year-ago levels, indicating oversupply concerns",
                    factor_type="bearish",
                    commodity=commodity,
                    source="data",
                    data_support=f"Year change: {comparison.year_change:.2f}"
                ))

        return factors

    def _identify_swing_factors(
        self,
        commodity: str,
        internal_data: Dict,
        report_date: date
    ) -> List[SwingFactor]:
        """Identify swing factors/catalysts for commodity"""
        factors = []

        # Common swing factors by commodity
        commodity_factors = {
            "corn": [
                SwingFactor(
                    description="USDA Export Sales report - changes in China purchasing patterns",
                    commodity="corn",
                    category="demand",
                    timeframe="near_term"
                ),
                SwingFactor(
                    description="Ethanol production trends amid changing fuel policies",
                    commodity="corn",
                    category="demand",
                    timeframe="medium_term"
                ),
            ],
            "wheat": [
                SwingFactor(
                    description="Black Sea region export policy changes",
                    commodity="wheat",
                    category="policy",
                    timeframe="near_term"
                ),
                SwingFactor(
                    description="Weather developments in key growing regions",
                    commodity="wheat",
                    category="weather",
                    timeframe="near_term"
                ),
            ],
            "soybeans": [
                SwingFactor(
                    description="South American weather and crop development",
                    commodity="soybeans",
                    category="weather",
                    timeframe="near_term"
                ),
                SwingFactor(
                    description="China import demand and state reserve purchases",
                    commodity="soybeans",
                    category="demand",
                    timeframe="near_term"
                ),
            ],
            "soybean_meal": [
                SwingFactor(
                    description="Domestic crush margins and processing rates",
                    commodity="soybean_meal",
                    category="supply",
                    timeframe="near_term"
                ),
            ],
            "soybean_oil": [
                SwingFactor(
                    description="Renewable diesel demand and RIN values",
                    commodity="soybean_oil",
                    category="demand",
                    timeframe="medium_term"
                ),
            ],
        }

        factors.extend(commodity_factors.get(commodity, []))

        # Ensure minimum swing factors
        min_swing = self.config.commodities.min_swing_factors
        while len(factors) < min_swing:
            factors.append(SwingFactor(
                description=f"Upcoming USDA reports and market data releases for {commodity}",
                commodity=commodity,
                category="supply",
                timeframe="near_term"
            ))

        return factors[:min_swing]

    def _get_llm_factors(
        self,
        commodity: str,
        internal_data: Dict,
        price_data: Dict,
        report_date: date
    ) -> Optional[Dict[str, List[str]]]:
        """Use LLM to identify additional market factors"""
        if not self.llm_client:
            return None

        try:
            prompt = f"""Analyze the {commodity} market for the week ending {report_date.isoformat()}.

Based on your knowledge of commodity markets and recent developments, provide:
1. Two bullish (supportive) fundamental factors
2. Two bearish (negative) fundamental factors

Format your response as:
BULLISH:
- [Factor 1]
- [Factor 2]

BEARISH:
- [Factor 1]
- [Factor 2]

Be specific and reference actual market fundamentals like exports, production, demand, policy, or weather."""

            if self.llm_config.provider == "openai":
                response = self.llm_client.chat.completions.create(
                    model=self.llm_config.model,
                    messages=[
                        {"role": "system", "content": self.llm_config.style_reference},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.llm_config.temperature,
                    max_tokens=500
                )
                content = response.choices[0].message.content

            elif self.llm_config.provider == "ollama":
                import requests
                response = requests.post(
                    f"{self.llm_config.ollama_base_url}/api/generate",
                    json={
                        "model": self.llm_config.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                    },
                    timeout=60
                )
                content = response.json().get("response", "")

            else:
                return None

            # Parse response
            return self._parse_llm_factors(content)

        except Exception as e:
            self.logger.warning(f"LLM factor generation failed: {e}")
            return None

    def _parse_llm_factors(self, content: str) -> Dict[str, List[str]]:
        """Parse LLM response into structured factors"""
        result = {"bullish": [], "bearish": []}

        lines = content.strip().split("\n")
        current_section = None

        for line in lines:
            line = line.strip()
            if "BULLISH" in line.upper():
                current_section = "bullish"
            elif "BEARISH" in line.upper():
                current_section = "bearish"
            elif line.startswith("-") and current_section:
                factor = line[1:].strip()
                if factor:
                    result[current_section].append(factor)

        return result

    def _calculate_sentiment(self, analysis: CommodityAnalysis) -> float:
        """Calculate overall sentiment score from -1 to +1"""
        bullish_weight = sum(f.confidence for f in analysis.bullish_factors)
        bearish_weight = sum(f.confidence for f in analysis.bearish_factors)

        total = bullish_weight + bearish_weight
        if total == 0:
            return 0.0

        return (bullish_weight - bearish_weight) / total

    def _analyze_macro_context(self, report_date: date) -> List[str]:
        """Analyze macroeconomic factors"""
        # This would integrate with macro data sources
        # Placeholder implementation
        return [
            "US dollar index movements and export competitiveness",
            "Crude oil price trends affecting biofuel economics",
            "Global economic growth indicators",
        ]

    def _analyze_weather_context(self, report_date: date) -> str:
        """Analyze weather context"""
        # This would integrate with weather APIs
        # Placeholder implementation
        return "Weather conditions across major growing regions remain largely favorable for crop development."

    def _compile_key_triggers(
        self,
        analyses: Dict[str, CommodityAnalysis]
    ) -> List[SwingFactor]:
        """Compile key triggers watchlist from all commodity analyses"""
        all_triggers = []

        # Collect all swing factors
        for commodity, analysis in analyses.items():
            all_triggers.extend(analysis.swing_factors)

        # Deduplicate and prioritize
        seen_descriptions = set()
        unique_triggers = []

        for trigger in all_triggers:
            if trigger.description not in seen_descriptions:
                seen_descriptions.add(trigger.description)
                unique_triggers.append(trigger)

        # Add cross-commodity triggers
        unique_triggers.append(SwingFactor(
            description="Upcoming USDA WASDE report - key supply/demand revisions",
            commodity="all",
            category="supply",
            timeframe="near_term"
        ))

        unique_triggers.append(SwingFactor(
            description="Weekly Export Sales and Inspections data",
            commodity="all",
            category="demand",
            timeframe="near_term"
        ))

        # Return top 7 triggers
        return unique_triggers[:7]
