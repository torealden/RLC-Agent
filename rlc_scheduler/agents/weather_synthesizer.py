#!/usr/bin/env python3
"""
Weather Intelligence Synthesizer

Takes structured extracted weather data and generates professional
market-focused weather intelligence briefs using LLM.

Supports:
- Anthropic Claude API (preferred)
- Ollama local models (fallback)
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

# Load environment variables
try:
    from dotenv import load_dotenv
    # Try multiple .env locations
    env_paths = [
        Path(__file__).parent.parent / '.env',
        Path(__file__).parent.parent.parent / '.env',
        Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\.env")
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass

# Import research engine
try:
    from weather_research import WeatherResearchEngine, get_current_crop_context
    RESEARCH_AVAILABLE = True
except ImportError:
    RESEARCH_AVAILABLE = False

logger = logging.getLogger(__name__)

# Configuration
BASE_DIR = Path(__file__).parent.parent
CONFIG_DIR = BASE_DIR / "config"
DOMAIN_KNOWLEDGE_DIR = Path(r"C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\domain_knowledge")


class WeatherSynthesizer:
    """
    Synthesizes weather data into professional market briefs.

    Uses extracted structured data + domain knowledge context
    to generate actionable weather intelligence.
    """

    def __init__(
        self,
        use_claude: bool = True,
        claude_model: str = "claude-sonnet-4-20250514",
        ollama_model: str = "llama3.1:8b",
        ollama_url: str = "http://localhost:11434"
    ):
        """
        Initialize synthesizer.

        Args:
            use_claude: Whether to use Claude API (requires ANTHROPIC_API_KEY)
            claude_model: Claude model to use
            ollama_model: Fallback Ollama model
            ollama_url: Ollama server URL
        """
        self.use_claude = use_claude
        self.claude_model = claude_model
        self.ollama_model = ollama_model
        self.ollama_url = ollama_url

        # Check API availability
        self.anthropic_available = self._check_anthropic()
        self.ollama_available = self._check_ollama()

        if use_claude and not self.anthropic_available:
            logger.warning("Claude API not available, will use Ollama")
            self.use_claude = False

        # Load domain knowledge
        self.crop_calendar = self._load_crop_calendar()
        self.special_situations = self._load_special_situations()

        # Initialize research engine
        if RESEARCH_AVAILABLE:
            self.research_engine = WeatherResearchEngine()
            logger.info("Research engine initialized")
        else:
            self.research_engine = None
            logger.warning("Research engine not available")

    def _check_anthropic(self) -> bool:
        """Check if Anthropic API is available."""
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return False
        try:
            import anthropic
            return True
        except ImportError:
            return False

    def _check_ollama(self) -> bool:
        """Check if Ollama is available."""
        try:
            import requests
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=2)
            return response.status_code == 200
        except Exception:
            return False

    def _load_crop_calendar(self) -> Dict:
        """Load crop calendar context for current month."""
        current_month = datetime.now().strftime("%B")

        # Define critical activities by month
        calendar = {
            "January": {
                "northern_hemisphere": "Winter wheat dormancy; watch for winterkill risk",
                "southern_hemisphere": "Brazil soybean pod fill/early harvest; Argentina corn pollination",
                "key_crops": ["winter wheat", "brazil soybeans", "argentina corn"]
            },
            "February": {
                "northern_hemisphere": "Winter wheat still dormant; soil moisture recharge",
                "southern_hemisphere": "Brazil soybean harvest accelerating; safrinha corn planting",
                "key_crops": ["winter wheat", "brazil soybeans", "safrinha corn"]
            },
            "March": {
                "northern_hemisphere": "Winter wheat greenup begins; early spring fieldwork",
                "southern_hemisphere": "Brazil soybean harvest finishing; safrinha corn development",
                "key_crops": ["winter wheat", "safrinha corn", "argentina soybeans"]
            },
            "April": {
                "northern_hemisphere": "Corn planting begins; winter wheat jointing",
                "southern_hemisphere": "Safrinha corn critical development; Argentina harvest",
                "key_crops": ["corn", "winter wheat", "safrinha corn"]
            },
            "May": {
                "northern_hemisphere": "Corn/soybean planting; winter wheat heading",
                "southern_hemisphere": "Safrinha corn approaching maturity",
                "key_crops": ["corn", "soybeans", "winter wheat", "safrinha corn"]
            },
            "June": {
                "northern_hemisphere": "Planting wrapping up; winter wheat harvest begins",
                "southern_hemisphere": "Safrinha corn harvest; Brazil wheat planting",
                "key_crops": ["corn", "soybeans", "winter wheat"]
            },
            "July": {
                "northern_hemisphere": "CRITICAL: Corn pollination; soybean flowering begins",
                "southern_hemisphere": "Safrinha harvest finishing",
                "key_crops": ["corn", "soybeans"],
                "critical_note": "Corn pollination is the most yield-sensitive period"
            },
            "August": {
                "northern_hemisphere": "Corn grain fill; soybean pod fill",
                "southern_hemisphere": "Off-season; Brazil wheat development",
                "key_crops": ["corn", "soybeans"]
            },
            "September": {
                "northern_hemisphere": "Corn maturity; soybean maturity; early harvest",
                "southern_hemisphere": "Brazil planting preparations",
                "key_crops": ["corn", "soybeans"]
            },
            "October": {
                "northern_hemisphere": "Harvest progress; winter wheat planting",
                "southern_hemisphere": "Brazil soybean planting begins",
                "key_crops": ["corn", "soybeans", "winter wheat", "brazil soybeans"]
            },
            "November": {
                "northern_hemisphere": "Harvest finishing; winter wheat establishment",
                "southern_hemisphere": "Brazil soybean planting; Argentina planting",
                "key_crops": ["winter wheat", "brazil soybeans", "argentina corn"]
            },
            "December": {
                "northern_hemisphere": "Winter wheat dormancy begins",
                "southern_hemisphere": "Brazil soybean development; Argentina corn/soy planting",
                "key_crops": ["winter wheat", "brazil soybeans", "argentina corn"]
            }
        }

        return calendar.get(current_month, {})

    def _load_special_situations(self) -> List[Dict]:
        """Load relevant special situations (historical parallels)."""
        situations = []

        # Check for special situation files
        special_dir = DOMAIN_KNOWLEDGE_DIR / "special_situations"
        if special_dir.exists():
            for file_path in special_dir.glob("*.md"):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        # Extract title and summary
                        lines = content.split('\n')
                        title = lines[0].replace('#', '').strip() if lines else file_path.stem
                        situations.append({
                            "name": title,
                            "file": file_path.name,
                            "preview": content[:500]
                        })
                except Exception as e:
                    logger.warning(f"Could not load {file_path}: {e}")

        return situations

    def _build_system_prompt(self) -> str:
        """Build the system prompt with domain context."""
        current_month = datetime.now().strftime("%B")
        calendar = self.crop_calendar

        prompt = """You are a senior agricultural meteorologist at a commodity trading firm.
Your role is to synthesize weather information into actionable market intelligence.

KEY PRINCIPLES:
1. Focus on MARKET IMPACT, not just weather facts
2. Prioritize information by trading relevance
3. Be specific about regions, timing, and magnitudes
4. Note forecast changes from prior updates
5. Identify potential supply/demand implications

CURRENT CROP CALENDAR CONTEXT:
"""
        if calendar:
            prompt += f"""
Month: {current_month}
Northern Hemisphere: {calendar.get('northern_hemisphere', 'N/A')}
Southern Hemisphere: {calendar.get('southern_hemisphere', 'N/A')}
Key Crops to Watch: {', '.join(calendar.get('key_crops', []))}
"""
            if 'critical_note' in calendar:
                prompt += f"CRITICAL: {calendar['critical_note']}\n"

        prompt += """
OUTPUT FORMAT:
Generate a professional weather intelligence brief with these sections:
1. EXECUTIVE SUMMARY (2-3 sentences, key takeaway)
2. Regional sections (US, BRAZIL, ARGENTINA as applicable)
3. KEY CHANGES FROM PRIOR UPDATE
4. MARKET IMPLICATIONS (if significant)

IMPORTANT GUIDELINES:
- Keep the tone professional and concise
- Use bullet points for clarity
- Do NOT make up specific numbers if not provided in the data
- If ADDITIONAL CONTEXT FROM RESEARCH is provided, integrate relevant
  threshold information and historical context naturally into your analysis
- When discussing risks (winterkill, drought, heat stress), reference the
  specific thresholds and conditions from the research context
- Connect weather conditions to specific crop stages and potential yield impacts
"""
        return prompt

    def _build_user_prompt(self, batch_data: Dict) -> str:
        """Build the user prompt with extracted data."""
        prompt = "Please synthesize the following weather data into a market-focused brief:\n\n"

        # Add the LLM context from the batch
        if "llm_context" in batch_data:
            prompt += batch_data["llm_context"]
        else:
            prompt += json.dumps(batch_data, indent=2, default=str)

        prompt += "\n\nGenerate the weather intelligence brief now."
        return prompt

    def synthesize(self, batch_data: Dict) -> str:
        """
        Synthesize weather data into a professional brief.

        Args:
            batch_data: Output from WeatherSummaryBatch.to_dict() or get_llm_context()

        Returns:
            Formatted weather intelligence brief
        """
        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(batch_data)

        if self.use_claude and self.anthropic_available:
            return self._synthesize_claude(system_prompt, user_prompt)
        elif self.ollama_available:
            return self._synthesize_ollama(system_prompt, user_prompt)
        else:
            logger.error("No LLM backend available")
            return self._synthesize_fallback(batch_data)

    def _synthesize_claude(self, system_prompt: str, user_prompt: str) -> str:
        """Generate synthesis using Claude API."""
        try:
            import anthropic

            client = anthropic.Anthropic()

            message = client.messages.create(
                model=self.claude_model,
                max_tokens=2000,
                system=system_prompt,
                messages=[
                    {"role": "user", "content": user_prompt}
                ]
            )

            return message.content[0].text

        except Exception as e:
            logger.error(f"Claude API error: {e}")
            if self.ollama_available:
                return self._synthesize_ollama(system_prompt, user_prompt)
            return self._synthesize_fallback({})

    def _synthesize_ollama(self, system_prompt: str, user_prompt: str) -> str:
        """Generate synthesis using Ollama."""
        try:
            import requests

            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.ollama_model,
                    "prompt": f"{system_prompt}\n\n{user_prompt}",
                    "stream": False,
                    "options": {
                        "temperature": 0.3,
                        "num_predict": 2000
                    }
                },
                timeout=120
            )

            if response.status_code == 200:
                return response.json().get("response", "")
            else:
                logger.error(f"Ollama error: {response.status_code}")
                return self._synthesize_fallback({})

        except Exception as e:
            logger.error(f"Ollama error: {e}")
            return self._synthesize_fallback({})

    def _synthesize_fallback(self, batch_data: Dict) -> str:
        """Generate basic summary without LLM."""
        lines = [
            "=" * 60,
            "WEATHER INTELLIGENCE BRIEF",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "(Fallback mode - LLM unavailable)",
            "=" * 60,
            ""
        ]

        if isinstance(batch_data, dict):
            if "all_key_points" in batch_data:
                lines.append("KEY POINTS:")
                for point in batch_data["all_key_points"][:5]:
                    lines.append(f"  • {point}")
                lines.append("")

            if "regions_covered" in batch_data:
                lines.append(f"Regions: {', '.join(batch_data['regions_covered'])}")

            if "overall_sentiment" in batch_data:
                lines.append(f"Sentiment: {batch_data['overall_sentiment']}")

        lines.append("")
        lines.append("=" * 60)

        return "\n".join(lines)

    def synthesize_with_research(
        self,
        batch_data: Dict,
        research_topics: List[str] = None,
        regions: List[str] = None
    ) -> str:
        """
        Synthesize with additional research context.

        Identifies topics needing context (winter kill, drought, etc.)
        and adds relevant background information.
        """
        if not self.research_engine:
            return self.synthesize(batch_data)

        # Get the text to analyze for research topics
        llm_context = batch_data.get("llm_context", "")
        key_points = batch_data.get("all_key_points", [])
        analysis_text = llm_context + " " + " ".join(key_points)

        # Get regions from batch data
        if not regions:
            regions = batch_data.get("regions_covered", [])

        # Identify and research topics
        research_result = self.research_engine.get_research_context(
            text=analysis_text,
            regions=regions,
            max_topics=3
        )

        # Add research context to the batch data
        if research_result.get("context_for_llm"):
            enhanced_context = batch_data.get("llm_context", "")
            enhanced_context += "\n\n" + research_result["context_for_llm"]
            batch_data["llm_context"] = enhanced_context

            # Log what was researched
            topics = research_result.get("topics_identified", [])
            if topics:
                logger.info(f"Research added for topics: {[t['topic'] for t in topics]}")

        return self.synthesize(batch_data)

    def get_research_context_only(
        self,
        text: str,
        regions: List[str] = None
    ) -> Dict:
        """
        Get research context without full synthesis.

        Useful for debugging or adding context to other processes.
        """
        if not self.research_engine:
            return {"error": "Research engine not available"}

        return self.research_engine.get_research_context(text, regions)


def format_brief_as_email(brief: str, subject_prefix: str = "Weather Brief") -> Dict[str, str]:
    """
    Format the brief for email sending.

    Returns:
        Dict with 'subject' and 'body' keys
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    time_str = datetime.now().strftime("%H:%M")

    # Determine time of day for subject
    hour = datetime.now().hour
    if hour < 12:
        time_label = "Morning"
    elif hour < 17:
        time_label = "Afternoon"
    else:
        time_label = "Evening"

    return {
        "subject": f"{subject_prefix} - {date_str} {time_label}",
        "body": brief
    }


# CLI for testing
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Weather Intelligence Synthesizer')
    parser.add_argument('--test', action='store_true', help='Run synthesis test')
    parser.add_argument('--use-ollama', action='store_true', help='Force Ollama instead of Claude')

    args = parser.parse_args()

    if args.test:
        # Test with sample data
        test_data = {
            "generated_at": datetime.now().isoformat(),
            "emails_processed": 3,
            "regions_covered": ["us", "brazil", "argentina"],
            "overall_sentiment": "neutral",
            "all_key_points": [
                "U.S. Corn Belt expecting 0.5-1.0 inches this week",
                "Brazil Mato Grosso harvest progressing well",
                "Argentina forecast shifts drier for week 2"
            ],
            "all_forecast_changes": [
                {
                    "region": "Argentina",
                    "direction": "drier",
                    "timeframe": "week 2",
                    "description": "GFS shifts drier than prior run"
                }
            ],
            "llm_context": """
WEATHER EMAIL BATCH - 2026-01-29 08:00
Emails processed: 3
Types: {'scheduled_update': 2, 'commentary': 1}
Regions: us, brazil, argentina
Overall sentiment: neutral

=== FORECAST CHANGES ===
  Argentina (week 2): drier - GFS model shifts drier than prior forecast

=== REGIONAL CONDITIONS ===

United States:
  Week 1: Corn Belt expecting scattered showers, 0.5-1.0 inches. Temps near normal.
  Week 2: Pattern turns drier across the Plains.

Brazil:
  Week 1: Mato Grosso dry, favorable for harvest (15% complete)
  Week 2: Moisture returns, 1-2 inches expected

Argentina:
  Week 1: Recent rains improved soil moisture in Buenos Aires, Cordoba
  Week 2: Drier trend developing, bears watching for late-planted soybeans

=== KEY POINTS ===
  • U.S. winter wheat in dormancy, no winterkill concerns currently
  • Brazil soybean harvest ahead of average pace
  • Argentina week 2 dryness could stress late soybeans
"""
        }

        synthesizer = WeatherSynthesizer(use_claude=not args.use_ollama)

        print("\nGenerating Weather Intelligence Brief...")
        print("=" * 60)

        brief = synthesizer.synthesize(test_data)
        print(brief)

        print("\n" + "=" * 60)
        print("Email format:")
        email = format_brief_as_email(brief)
        print(f"Subject: {email['subject']}")
