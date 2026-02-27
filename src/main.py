#!/usr/bin/env python3
"""
RLC-Agent Main Entry Point

Primary interface for the RLC Ag Economist system.
Designed for Windows desktop wrapper integration.

Usage:
    # Run interactively
    python -m src.main interactive

    # Run daily data collection
    python -m src.main collect --daily

    # Generate weekly report
    python -m src.main report --weekly

    # Start scheduler
    python -m src.main schedule --start

    # Query data
    python -m src.main query "soybean exports to China"

Windows Integration:
    This module exposes functions that can be called from a Windows GUI wrapper.
    See RLCAgent class for the main interface.
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime, date
from typing import Optional, Dict, Any, List

# Ensure src is in path
SRC_ROOT = Path(__file__).parent
PROJECT_ROOT = SRC_ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import __version__, DATA_DIR, CONFIG_DIR, MODELS_DIR


def setup_logging(log_level: str = "INFO", log_file: Optional[Path] = None) -> logging.Logger:
    """Configure logging for the application."""
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    if log_file is None:
        log_file = log_dir / f"rlc_agent_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file),
        ]
    )
    return logging.getLogger("RLC-Agent")


class RLCAgent:
    """
    Main interface for the RLC Ag Economist system.

    This class provides a unified interface for:
    - Data collection from 26+ sources
    - Market analysis and forecasting
    - Report generation
    - Social media content creation
    - Ad hoc consulting queries

    Designed to be wrapped by a Windows GUI application.
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the RLC Agent.

        Args:
            config_path: Optional path to configuration file
        """
        self.logger = setup_logging()
        self.config_path = config_path or CONFIG_DIR / "settings.json"
        self.initialized = False

        self.logger.info(f"RLC Agent v{__version__} initializing...")
        self._load_config()

    def _load_config(self):
        """Load configuration from file or defaults."""
        # TODO: Load from config file
        self.config = {
            "data_dir": str(DATA_DIR),
            "models_dir": str(MODELS_DIR),
            "default_commodities": ["soybeans", "corn", "wheat", "soy_oil", "soy_meal"],
            "default_regions": ["us", "brazil", "argentina", "canada"],
        }
        self.initialized = True
        self.logger.info("Configuration loaded")

    # =========================================================================
    # DATA COLLECTION
    # =========================================================================

    def collect_data(self,
                     source: Optional[str] = None,
                     commodity: Optional[str] = None,
                     date_range: Optional[tuple] = None) -> Dict[str, Any]:
        """
        Collect data from specified sources.

        Args:
            source: Specific source (e.g., "usda_fas", "cftc_cot") or None for all
            commodity: Filter by commodity
            date_range: Tuple of (start_date, end_date)

        Returns:
            Dictionary with collection results
        """
        self.logger.info(f"Collecting data: source={source}, commodity={commodity}")

        if source:
            result = self._get_dispatcher().run_collector(source)
            return {
                "timestamp": datetime.now().isoformat(),
                "source": source,
                "commodity": commodity,
                "success": result.success,
                "records": result.rows_collected,
                "errors": [result.error_message] if result.error_message else [],
            }
        else:
            results = self._get_dispatcher().run_todays_collectors()
            return {
                "timestamp": datetime.now().isoformat(),
                "source": "all_scheduled",
                "commodity": commodity,
                "success": all(r.success for r in results),
                "records": sum(r.rows_collected for r in results),
                "errors": [r.error_message for r in results if r.error_message],
            }

    def run_daily_collection(self) -> Dict[str, Any]:
        """Run the daily data collection workflow."""
        self.logger.info("Starting daily data collection...")

        run_results = self._get_dispatcher().run_todays_collectors()
        return {
            "date": date.today().isoformat(),
            "collections": [
                {
                    "collector": r.collector_name,
                    "status": r.status,
                    "rows": r.rows_collected,
                }
                for r in run_results
            ],
            "success": all(r.success for r in run_results),
        }

    # =========================================================================
    # ANALYSIS
    # =========================================================================

    def analyze_market(self,
                       commodity: str,
                       analysis_type: str = "fundamental") -> Dict[str, Any]:
        """
        Run market analysis for a commodity.

        Args:
            commodity: Commodity to analyze
            analysis_type: Type of analysis (fundamental, technical, seasonal)

        Returns:
            Analysis results
        """
        self.logger.info(f"Analyzing {commodity} ({analysis_type})")

        results = {
            "commodity": commodity,
            "analysis_type": analysis_type,
            "timestamp": datetime.now().isoformat(),
            "insights": [],
        }

        # TODO: Implement using analysis agents

        return results

    def get_price_forecast(self,
                           commodity: str,
                           horizon: str = "1_month") -> Dict[str, Any]:
        """
        Get price forecast for a commodity.

        Args:
            commodity: Commodity to forecast
            horizon: Forecast horizon (1_month, 3_month, 6_month, 12_month)

        Returns:
            Forecast results with confidence intervals
        """
        self.logger.info(f"Generating price forecast: {commodity}, horizon={horizon}")

        results = {
            "commodity": commodity,
            "horizon": horizon,
            "forecast_date": datetime.now().isoformat(),
            "predictions": [],
        }

        # TODO: Implement using price_forecaster

        return results

    # =========================================================================
    # REPORTING
    # =========================================================================

    def generate_weekly_report(self,
                               report_date: Optional[date] = None,
                               output_path: Optional[Path] = None) -> Dict[str, Any]:
        """
        Generate the weekly HB-style commodity report.

        Args:
            report_date: Date for the report (default: today)
            output_path: Where to save the report

        Returns:
            Report generation results
        """
        report_date = report_date or date.today()
        self.logger.info(f"Generating weekly report for {report_date}")

        results = {
            "report_date": report_date.isoformat(),
            "output_path": str(output_path) if output_path else None,
            "success": True,
            "sections": [],
        }

        # TODO: Use HB report orchestrator

        return results

    def generate_market_summary(self,
                                commodities: Optional[List[str]] = None,
                                format: str = "brief") -> str:
        """
        Generate a market summary for specified commodities.

        Args:
            commodities: List of commodities (default: configured defaults)
            format: Output format (brief, detailed, tweet)

        Returns:
            Market summary text
        """
        commodities = commodities or self.config["default_commodities"]
        self.logger.info(f"Generating {format} market summary for {commodities}")

        # TODO: Implement using reporting agents

        return f"Market summary for {', '.join(commodities)}"

    # =========================================================================
    # SOCIAL MEDIA & CONTENT
    # =========================================================================

    def generate_social_content(self,
                                topic: str,
                                platform: str = "twitter",
                                include_chart: bool = False) -> Dict[str, Any]:
        """
        Generate social media content about a topic.

        Args:
            topic: Topic to cover (e.g., "soybean exports", "corn crop progress")
            platform: Target platform (twitter, linkedin, blog)
            include_chart: Whether to generate accompanying chart

        Returns:
            Content ready for posting
        """
        self.logger.info(f"Generating {platform} content about: {topic}")

        results = {
            "topic": topic,
            "platform": platform,
            "content": "",
            "hashtags": [],
            "chart_path": None,
        }

        # TODO: Implement using publishing agents

        return results

    # =========================================================================
    # QUERY INTERFACE
    # =========================================================================

    def query(self, question: str) -> Dict[str, Any]:
        """
        Answer a natural language question about commodity markets.

        This is the main interface for ad hoc consulting queries.

        Args:
            question: Natural language question

        Returns:
            Answer with supporting data
        """
        self.logger.info(f"Processing query: {question}")

        results = {
            "question": question,
            "answer": "",
            "data_sources": [],
            "confidence": 0.0,
            "supporting_data": {},
        }

        # TODO: Implement using master agent with RAG

        return results

    # =========================================================================
    # SCHEDULER
    # =========================================================================

    def _get_dispatcher(self):
        """Get or create the dispatcher instance."""
        if not hasattr(self, '_dispatcher') or self._dispatcher is None:
            from src.dispatcher.dispatcher import Dispatcher
            self._dispatcher = Dispatcher()
        return self._dispatcher

    def start_scheduler(self, background: bool = True) -> bool:
        """
        Start the data collection scheduler.

        Args:
            background: Run in background thread

        Returns:
            True if started successfully
        """
        self.logger.info(f"Starting scheduler (background={background})")
        try:
            self._get_dispatcher().start()
            return True
        except Exception as e:
            self.logger.error(f"Failed to start scheduler: {e}")
            return False

    def stop_scheduler(self) -> bool:
        """Stop the data collection scheduler."""
        self.logger.info("Stopping scheduler")
        if hasattr(self, '_dispatcher') and self._dispatcher:
            self._dispatcher.stop()
        return True

    def get_schedule(self) -> List[Dict[str, Any]]:
        """Get the current collection schedule."""
        status = self._get_dispatcher().get_status()
        return status.get('jobs', [])

    def get_data_freshness(self) -> List[Dict[str, Any]]:
        """Query core.data_freshness view for current data state."""
        from src.services.database.db_config import get_connection
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM core.data_freshness ORDER BY hours_since_collection DESC NULLS LAST")
            return [dict(row) for row in cursor.fetchall()]

    def get_briefing(self) -> List[Dict[str, Any]]:
        """Get unacknowledged events from core.llm_briefing."""
        from src.services.database.db_config import get_connection
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM core.llm_briefing")
            return [dict(row) for row in cursor.fetchall()]


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="RLC Ag Economist - Commodity Market Analysis System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m src.main interactive
    python -m src.main collect --source usda_fas
    python -m src.main report --weekly
    python -m src.main query "What are Brazil soybean exports YTD?"
        """
    )

    parser.add_argument("--version", action="version", version=f"RLC-Agent v{__version__}")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Interactive mode
    interactive_parser = subparsers.add_parser("interactive", help="Run in interactive mode")

    # Collect data
    collect_parser = subparsers.add_parser("collect", help="Collect data")
    collect_parser.add_argument("--source", help="Specific data source")
    collect_parser.add_argument("--commodity", help="Filter by commodity")
    collect_parser.add_argument("--daily", action="store_true", help="Run daily collection")

    # Generate report
    report_parser = subparsers.add_parser("report", help="Generate reports")
    report_parser.add_argument("--weekly", action="store_true", help="Generate weekly report")
    report_parser.add_argument("--date", help="Report date (YYYY-MM-DD)")
    report_parser.add_argument("--output", help="Output path")

    # Query
    query_parser = subparsers.add_parser("query", help="Ask a question")
    query_parser.add_argument("question", help="Question to answer")

    # Schedule
    schedule_parser = subparsers.add_parser("schedule", help="Manage scheduler")
    schedule_parser.add_argument("--start", action="store_true", help="Start scheduler")
    schedule_parser.add_argument("--stop", action="store_true", help="Stop scheduler")
    schedule_parser.add_argument("--status", action="store_true", help="Show schedule status")

    args = parser.parse_args()

    # Initialize agent
    agent = RLCAgent()

    if args.command == "interactive":
        print(f"RLC Ag Economist v{__version__}")
        print("Interactive mode - type 'help' for commands, 'quit' to exit")
        # TODO: Implement REPL

    elif args.command == "collect":
        if args.daily:
            results = agent.run_daily_collection()
        else:
            results = agent.collect_data(source=args.source, commodity=args.commodity)
        print(results)

    elif args.command == "report":
        if args.weekly:
            report_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
            output_path = Path(args.output) if args.output else None
            results = agent.generate_weekly_report(report_date, output_path)
            print(results)

    elif args.command == "query":
        results = agent.query(args.question)
        print(f"\nAnswer: {results.get('answer', 'No answer generated')}")

    elif args.command == "schedule":
        if args.start:
            agent.start_scheduler()
        elif args.stop:
            agent.stop_scheduler()
        elif args.status:
            schedule = agent.get_schedule()
            print("Current schedule:", schedule)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
