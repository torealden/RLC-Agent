"""
Report Writer Agent

Core agent responsible for generating the narrative content of the HB Weekly Report.
Coordinates data gathering, analysis, and document generation.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

from ..config.settings import HBWeeklyReportConfig, OutputConfig
from .internal_data_agent import InternalDataAgent, InternalDataResult
from .price_data_agent import PriceDataAgent, PriceDataResult
from .market_research_agent import MarketResearchAgent, MarketResearchResult, CommodityAnalysis

logger = logging.getLogger(__name__)


@dataclass
class ReportContent:
    """Complete report content structure"""
    report_date: date
    week_ending: date

    # Sections
    executive_summary: str = ""
    macro_update: str = ""
    weather_update: str = ""

    # Commodity deep dives
    commodity_sections: Dict[str, str] = field(default_factory=dict)

    # Tables data
    price_table_data: Dict[str, List[Dict]] = field(default_factory=dict)
    spread_table_data: List[Dict] = field(default_factory=list)
    international_table_data: List[Dict] = field(default_factory=list)

    # Synthesis
    synthesis_outlook: str = ""
    key_triggers: List[str] = field(default_factory=list)

    # Metadata
    placeholders: List[str] = field(default_factory=list)
    llm_estimates: List[str] = field(default_factory=list)
    data_sources: List[str] = field(default_factory=list)

    # Status
    is_complete: bool = False
    completeness_score: float = 0.0


@dataclass
class WriterResult:
    """Result of report writing operation"""
    success: bool
    report_date: date
    content: Optional[ReportContent] = None
    document_path: Optional[Path] = None

    # Timing
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: float = 0.0

    # Questions for human input
    pending_questions: List[Dict] = field(default_factory=list)

    # Errors
    errors: List[str] = field(default_factory=list)


class ReportWriterAgent:
    """
    Agent for generating HB Weekly Report content

    Coordinates:
    - Data gathering from internal and external sources
    - Market analysis and research
    - Content generation for each section
    - Document assembly
    """

    def __init__(self, config: HBWeeklyReportConfig, db_session_factory=None):
        """
        Initialize Report Writer Agent

        Args:
            config: HB Weekly Report configuration
            db_session_factory: Optional database session factory
        """
        self.config = config
        self.db_session_factory = db_session_factory
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # Initialize sub-agents
        self.internal_data_agent = InternalDataAgent(config, db_session_factory)
        self.price_data_agent = PriceDataAgent(config)
        self.market_research_agent = MarketResearchAgent(
            config,
            self.internal_data_agent,
            self.price_data_agent
        )

        # LLM client for writing (lazy init)
        self._llm_client = None

        self.logger.info("Initialized ReportWriterAgent")

    @property
    def llm_client(self):
        """Lazy initialization of LLM client for writing"""
        if self._llm_client is None and self.config.llm.enabled:
            self._llm_client = self._create_llm_client()
        return self._llm_client

    def _create_llm_client(self):
        """Create LLM client for content generation"""
        try:
            if self.config.llm.provider == "openai":
                import openai
                return openai.OpenAI(api_key=self.config.llm.api_key)
            elif self.config.llm.provider == "ollama":
                return {"type": "ollama", "base_url": self.config.llm.ollama_base_url}
            return None
        except Exception as e:
            self.logger.error(f"Failed to create LLM client: {e}")
            return None

    def generate_report(self, report_date: date = None) -> WriterResult:
        """
        Generate complete report content

        Args:
            report_date: Date for the report (default: today)

        Returns:
            WriterResult with generated content
        """
        report_date = report_date or date.today()
        week_ending = self._calculate_week_ending(report_date)

        result = WriterResult(
            success=False,
            report_date=report_date,
        )

        self.logger.info(f"Generating report for {report_date}, week ending {week_ending}")

        try:
            # Create content container
            content = ReportContent(
                report_date=report_date,
                week_ending=week_ending,
            )

            # Step 1: Gather data
            self.logger.info("Step 1: Gathering data...")
            internal_data = self.internal_data_agent.fetch_data()
            price_data = self.price_data_agent.fetch_prices(report_date)

            content.data_sources = self._compile_data_sources(internal_data, price_data)

            # Check for missing critical data and create questions
            questions = self._identify_missing_data(internal_data, price_data)
            if questions:
                result.pending_questions = questions
                self.logger.info(f"Identified {len(questions)} data questions")

            # Step 2: Conduct market research
            self.logger.info("Step 2: Conducting market research...")
            research = self.market_research_agent.conduct_research(report_date)

            # Step 3: Generate content sections
            self.logger.info("Step 3: Generating content sections...")

            # Executive Summary
            content.executive_summary = self._generate_executive_summary(
                research, price_data
            )

            # Macro and Weather
            content.macro_update = self._generate_macro_section(research)
            content.weather_update = self._generate_weather_section(research)

            # Commodity Deep Dives
            for commodity in self.config.commodities.primary_commodities:
                analysis = research.commodity_analyses.get(commodity)
                if analysis:
                    content.commodity_sections[commodity] = self._generate_commodity_section(
                        commodity, analysis, internal_data, price_data
                    )

            # Price Tables
            if price_data.success:
                table_data = self.price_data_agent.get_price_table_data()
                content.price_table_data = table_data
                content.spread_table_data = table_data.get("spreads", [])
                content.international_table_data = table_data.get("international", [])

            # Synthesis and Outlook
            content.synthesis_outlook = self._generate_synthesis(research)

            # Key Triggers
            content.key_triggers = self._format_key_triggers(research.key_triggers)

            # Calculate completeness
            content.is_complete, content.completeness_score = self._calculate_completeness(content)

            # Track placeholders and LLM estimates
            content.placeholders = self._identify_placeholders(content)
            content.llm_estimates = self._identify_llm_estimates(research)

            result.content = content
            result.success = True
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()

            self.logger.info(
                f"Report generation complete: {content.completeness_score:.1f}% complete, "
                f"{len(content.placeholders)} placeholders, {len(content.llm_estimates)} LLM estimates"
            )

        except Exception as e:
            self.logger.error(f"Report generation failed: {e}", exc_info=True)
            result.errors.append(str(e))
            result.completed_at = datetime.utcnow()
            result.duration_seconds = (result.completed_at - result.started_at).total_seconds()

        return result

    def _calculate_week_ending(self, report_date: date) -> date:
        """Calculate the week ending date (previous Friday)"""
        # Find previous Friday
        days_since_friday = (report_date.weekday() - 4) % 7
        if days_since_friday == 0 and report_date.weekday() != 4:
            days_since_friday = 7
        return report_date - timedelta(days=days_since_friday)

    def _compile_data_sources(
        self,
        internal_data: InternalDataResult,
        price_data: PriceDataResult
    ) -> List[str]:
        """Compile list of data sources used"""
        sources = []

        if internal_data.success:
            sources.append(f"Internal spreadsheet ({internal_data.file_path})")

        if price_data.success:
            sources.append("Market prices via API Manager")
            if price_data.used_previous_day:
                sources.append("Note: Some prices from previous trading day")

        return sources

    def _identify_missing_data(
        self,
        internal_data: InternalDataResult,
        price_data: PriceDataResult
    ) -> List[Dict]:
        """Identify missing data and create questions for human input"""
        questions = []

        # Check internal data
        if not internal_data.success:
            questions.append({
                "question": "Internal spreadsheet data unavailable. Please provide updated HB data file.",
                "category": "missing_data",
                "severity": "high",
            })
        elif internal_data.missing_fields:
            for field in internal_data.missing_fields[:5]:  # Limit to 5
                questions.append({
                    "question": f"Missing data field: {field}. Please provide this value.",
                    "category": "missing_data",
                    "severity": "medium",
                })

        # Check price data
        if not price_data.success:
            questions.append({
                "question": "Price data unavailable. Should the report use cached prices?",
                "category": "missing_data",
                "severity": "high",
            })
        elif price_data.series_failed > 0:
            questions.append({
                "question": f"{price_data.series_failed} price series failed to fetch. Continue with available data?",
                "category": "incomplete_data",
                "severity": "medium",
            })

        return questions

    def _generate_executive_summary(
        self,
        research: MarketResearchResult,
        price_data: PriceDataResult
    ) -> str:
        """Generate executive summary section"""
        if self.llm_client and self.config.llm.enabled:
            return self._generate_executive_summary_llm(research, price_data)
        else:
            return self._generate_executive_summary_template(research, price_data)

    def _generate_executive_summary_template(
        self,
        research: MarketResearchResult,
        price_data: PriceDataResult
    ) -> str:
        """Generate executive summary using templates"""
        sections = []

        for commodity in ["corn", "wheat", "soybeans"]:
            analysis = research.commodity_analyses.get(commodity)
            if analysis:
                # Build summary paragraph
                direction_text = {
                    "up": "rose",
                    "down": "declined",
                    "flat": "held steady"
                }.get(analysis.price_direction, "moved")

                sentiment_text = {
                    "bullish": "supportive fundamentals",
                    "bearish": "headwinds",
                    "neutral": "mixed signals"
                }.get(analysis.overall_sentiment, "developing factors")

                para = (
                    f"{commodity.title()} prices {direction_text} this week amid {sentiment_text}. "
                )

                # Add key factor
                if analysis.bullish_factors:
                    para += f"{analysis.bullish_factors[0].description} "
                if analysis.bearish_factors:
                    para += f"However, {analysis.bearish_factors[0].description.lower()}"

                sections.append(para.strip())

        return "\n\n".join(sections)

    def _generate_executive_summary_llm(
        self,
        research: MarketResearchResult,
        price_data: PriceDataResult
    ) -> str:
        """Generate executive summary using LLM"""
        try:
            # Build context for LLM
            context_parts = []
            for commodity in ["corn", "wheat", "soybeans"]:
                analysis = research.commodity_analyses.get(commodity)
                if analysis:
                    bullish = [f.description for f in analysis.bullish_factors[:2]]
                    bearish = [f.description for f in analysis.bearish_factors[:2]]
                    context_parts.append(
                        f"{commodity.upper()}: Direction={analysis.price_direction}, "
                        f"Sentiment={analysis.overall_sentiment}, "
                        f"Bullish={bullish}, Bearish={bearish}"
                    )

            prompt = f"""Write an executive summary for a weekly agricultural commodity market report.

Market Data:
{chr(10).join(context_parts)}

Weather: {research.weather_summary}

Requirements:
- Write 2-3 short paragraphs covering corn, wheat, and soybeans
- Professional analytical tone
- Reference specific price movements and fundamentals
- Keep it concise (200-300 words total)
- Do not use bullet points"""

            if self.config.llm.provider == "openai":
                response = self.llm_client.chat.completions.create(
                    model=self.config.llm.model,
                    messages=[
                        {"role": "system", "content": self.config.llm.style_reference},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=self.config.llm.temperature,
                    max_tokens=500
                )
                return response.choices[0].message.content.strip()

            elif self.config.llm.provider == "ollama":
                import requests
                response = requests.post(
                    f"{self.config.llm.ollama_base_url}/api/generate",
                    json={
                        "model": self.config.llm.ollama_model,
                        "prompt": prompt,
                        "stream": False,
                    },
                    timeout=60
                )
                return response.json().get("response", "").strip()

        except Exception as e:
            self.logger.warning(f"LLM executive summary failed: {e}")
            return self._generate_executive_summary_template(research, price_data)

    def _generate_macro_section(self, research: MarketResearchResult) -> str:
        """Generate macro update section"""
        developments = research.macro_developments

        if not developments:
            return "Macroeconomic conditions remained largely stable this week with no significant shifts in currency or energy markets that would materially impact agricultural commodity prices."

        section = "Macro factors influencing markets this week include:\n\n"
        for dev in developments:
            section += f"- {dev}\n"

        return section

    def _generate_weather_section(self, research: MarketResearchResult) -> str:
        """Generate weather update section"""
        return research.weather_summary or "Weather conditions across major growing regions remain within normal parameters for this time of year."

    def _generate_commodity_section(
        self,
        commodity: str,
        analysis: CommodityAnalysis,
        internal_data: InternalDataResult,
        price_data: PriceDataResult
    ) -> str:
        """Generate deep dive section for a commodity"""
        sections = []

        # Opening with price context
        sections.append(analysis.price_context)

        # Bullish factors
        if analysis.bullish_factors:
            bullish_text = "Supportive factors for the market include: "
            bullish_points = [f.description for f in analysis.bullish_factors]
            bullish_text += "; ".join(bullish_points) + "."
            sections.append(bullish_text)

        # Bearish factors
        if analysis.bearish_factors:
            bearish_text = "Weighing on prices: "
            bearish_points = [f.description for f in analysis.bearish_factors]
            bearish_text += "; ".join(bearish_points) + "."
            sections.append(bearish_text)

        # Swing factors
        if analysis.swing_factors:
            swing_text = "Key factors to watch: "
            swing_points = [f.description for f in analysis.swing_factors]
            swing_text += " and ".join(swing_points) + "."
            sections.append(swing_text)

        return "\n\n".join(sections)

    def _generate_synthesis(self, research: MarketResearchResult) -> str:
        """Generate synthesis and outlook section"""
        # Summarize overall market sentiment
        sentiments = {
            commodity: analysis.overall_sentiment
            for commodity, analysis in research.commodity_analyses.items()
        }

        bullish_count = sum(1 for s in sentiments.values() if s == "bullish")
        bearish_count = sum(1 for s in sentiments.values() if s == "bearish")

        if bullish_count > bearish_count:
            overall = "cautiously optimistic"
        elif bearish_count > bullish_count:
            overall = "cautious"
        else:
            overall = "balanced"

        synthesis = (
            f"Looking across agricultural commodity markets, the overall outlook remains {overall}. "
        )

        # Add commodity-specific outlook
        for commodity, sentiment in sentiments.items():
            synthesis += f"{commodity.title()} fundamentals appear {sentiment}. "

        synthesis += (
            "\nThe coming weeks will be shaped by export demand developments, "
            "weather patterns in key growing regions, and any policy shifts. "
            "We continue to monitor these catalysts closely."
        )

        return synthesis

    def _format_key_triggers(self, triggers: List) -> List[str]:
        """Format key triggers as bullet points"""
        formatted = []
        for trigger in triggers:
            if hasattr(trigger, 'description'):
                formatted.append(trigger.description)
            elif isinstance(trigger, str):
                formatted.append(trigger)

        return formatted

    def _calculate_completeness(self, content: ReportContent) -> Tuple[bool, float]:
        """Calculate report completeness score"""
        total_sections = 0
        complete_sections = 0

        # Check each section
        checks = [
            ("executive_summary", content.executive_summary),
            ("macro_update", content.macro_update),
            ("weather_update", content.weather_update),
            ("synthesis_outlook", content.synthesis_outlook),
        ]

        for name, section in checks:
            total_sections += 1
            if section and len(section) > 50:
                complete_sections += 1

        # Check commodity sections
        for commodity in self.config.commodities.primary_commodities:
            total_sections += 1
            if commodity in content.commodity_sections and len(content.commodity_sections[commodity]) > 50:
                complete_sections += 1

        # Check tables
        total_sections += 1
        if content.price_table_data and len(content.price_table_data.get("futures", [])) > 0:
            complete_sections += 1

        score = (complete_sections / total_sections) * 100 if total_sections > 0 else 0
        is_complete = score >= 80  # Consider complete if 80%+ sections filled

        return is_complete, score

    def _identify_placeholders(self, content: ReportContent) -> List[str]:
        """Identify sections with placeholders"""
        placeholders = []

        # Check for placeholder indicators in text
        all_text = [
            content.executive_summary,
            content.macro_update,
            content.weather_update,
            content.synthesis_outlook,
        ]
        all_text.extend(content.commodity_sections.values())

        for i, text in enumerate(all_text):
            if text:
                if "[" in text and "]" in text:
                    placeholders.append(f"Section {i}: Contains brackets placeholder")
                if "???" in text or "TBD" in text:
                    placeholders.append(f"Section {i}: Contains TBD marker")

        return placeholders

    def _identify_llm_estimates(self, research: MarketResearchResult) -> List[str]:
        """Identify LLM-generated estimates in the report"""
        estimates = []

        for commodity, analysis in research.commodity_analyses.items():
            if analysis.has_llm_content:
                estimates.append(f"{commodity}: Market factors partially LLM-generated")

            for factor in analysis.bullish_factors + analysis.bearish_factors:
                if factor.source == "llm":
                    estimates.append(f"{commodity}: {factor.description[:50]}...")

        return estimates
