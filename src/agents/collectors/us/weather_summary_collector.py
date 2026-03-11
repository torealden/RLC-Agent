"""
Weather Daily Summary Collector

Thin adapter wrapping the Weather Intelligence Agent (rlc_scheduler/agents/)
into the BaseCollector pattern so it can be dispatched on schedule.

Pipeline:
  1. WeatherIntelligenceAgent.process_emails() fetches Gmail, synthesizes brief
  2. This adapter wraps the result into CollectorResult
  3. Saves the LLM-generated summary to bronze.weather_email_extract
  4. Agent's existing SMTP logic handles email delivery
"""

import logging
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

from .base_collector import BaseCollector, CollectorConfig, CollectorResult, DataFrequency

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent


class WeatherSummaryCollector(BaseCollector):
    """
    Adapter that wraps WeatherIntelligenceAgent into BaseCollector interface.

    The underlying agent does the heavy lifting:
    - Fetches weather emails from meteorologist senders via Gmail OAuth
    - Classifies and extracts structured data
    - Synthesizes into a weather intelligence brief via LLM
    - Sends summary email to configured recipients
    """

    def __init__(self, **kwargs):
        config = CollectorConfig(
            source_name='weather_daily_summary',
            source_url='',  # Not HTTP-based
            frequency=DataFrequency.DAILY,
        )
        super().__init__(config)
        self._agent = None

    def _get_agent(self):
        """Lazy-import the weather intelligence agent."""
        if self._agent is None:
            # The agent lives in the legacy scheduler path
            agent_dir = str(PROJECT_ROOT / 'rlc_scheduler' / 'agents')
            if agent_dir not in sys.path:
                sys.path.insert(0, agent_dir)
            from weather_intelligence_agent import WeatherIntelligenceAgent
            self._agent = WeatherIntelligenceAgent()
        return self._agent

    def get_table_name(self) -> str:
        return 'bronze.weather_email_extract'

    def parse_response(self, response_data):
        """The agent already returns structured data."""
        return response_data

    def fetch_data(self, start_date=None, end_date=None, **kwargs) -> CollectorResult:
        """
        Run the weather intelligence agent's email processing pipeline.

        Args:
            start_date: Not used (agent fetches from last processed)
            end_date: Not used
            **kwargs:
                hours_back: Override hours to look back (default 24)
                send_summary: Whether to send email (default True)
                test_mode: If True, skip email sending
        """
        hours_back = kwargs.get('hours_back', 24)
        send_summary = kwargs.get('send_summary', True)
        test_mode = kwargs.get('test_mode', False)

        started_at = datetime.now(timezone.utc)

        try:
            agent = self._get_agent()
            result = agent.process_emails(
                hours_back=hours_back,
                send_summary=send_summary,
                forward_emails=True,
                test_mode=test_mode,
            )

            records_fetched = result.get('emails_processed', 0)
            brief = result.get('brief', '')

            # Save the synthesis to bronze if we got content
            if brief and result.get('success'):
                self._save_to_bronze(brief, result)

            return CollectorResult(
                success=result.get('success', False),
                source=self.config.source_name,
                collected_at=datetime.now(timezone.utc),
                records_fetched=records_fetched,
                data=result,
                response_time_ms=int(
                    (datetime.now(timezone.utc) - started_at).total_seconds() * 1000
                ),
                data_as_of=datetime.now(timezone.utc),
                warnings=[] if result.get('success') else [
                    result.get('error', 'Unknown error')
                ],
            )

        except Exception as e:
            logger.error(f"Weather summary collection failed: {e}", exc_info=True)
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                collected_at=datetime.now(timezone.utc),
                records_fetched=0,
                data=None,
                response_time_ms=int(
                    (datetime.now(timezone.utc) - started_at).total_seconds() * 1000
                ),
                error_message=str(e),
            )

    def _save_to_bronze(self, brief: str, agent_result: dict):
        """Save the weather intelligence brief to bronze.weather_email_extract."""
        try:
            from src.services.database.db_config import get_connection
            with get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO bronze.weather_email_extract
                        (extract_date, brief_text, emails_processed,
                         summary_sent, source, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (extract_date) DO UPDATE SET
                        brief_text = EXCLUDED.brief_text,
                        emails_processed = EXCLUDED.emails_processed,
                        summary_sent = EXCLUDED.summary_sent,
                        updated_at = NOW()
                """, (
                    datetime.now(timezone.utc).date(),
                    brief,
                    agent_result.get('emails_processed', 0),
                    agent_result.get('summary_sent', False),
                    'weather_intelligence_agent',
                ))
                conn.commit()
                logger.info("Saved weather brief to bronze.weather_email_extract")
        except Exception as e:
            # Non-fatal: the email still went out even if DB save fails
            logger.warning(f"Failed to save weather brief to bronze: {e}")
