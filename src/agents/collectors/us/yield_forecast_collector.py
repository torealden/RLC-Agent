"""
Yield Forecast Collector

Adapter wrapping the YieldOrchestrator into BaseCollector pattern
for dispatcher scheduling during the growing season (weeks 18-38, ~May-Sep).

Runs the 3-model ensemble yield prediction pipeline and stores results
in gold.yield_forecast and core.forecasts (via forecast tracker).
"""

import logging
from datetime import datetime, timezone, date

from .base_collector import BaseCollector, CollectorConfig, CollectorResult, DataFrequency

logger = logging.getLogger(__name__)


class YieldForecastCollector(BaseCollector):
    """
    Dispatcher adapter for the yield prediction model.

    During the growing season (weeks 18-38), runs weekly yield forecasts
    for corn, soybeans, winter wheat, and cotton.
    """

    def __init__(self, **kwargs):
        config = CollectorConfig(
            source_name='yield_forecast',
            base_url='',  # Not HTTP-based — uses DB data
            frequency=DataFrequency.WEEKLY,
        )
        super().__init__(config)
        self._orchestrator = None

    def _get_orchestrator(self):
        if self._orchestrator is None:
            from src.models.yield_orchestrator import YieldOrchestrator
            self._orchestrator = YieldOrchestrator()
        return self._orchestrator

    def get_table_name(self) -> str:
        return 'gold.yield_forecast'

    def parse_response(self, response_data):
        return response_data

    def fetch_data(self, start_date=None, end_date=None, **kwargs) -> CollectorResult:
        """
        Run the yield forecast pipeline.

        Only runs during the growing season (weeks 18-38).
        Outside this window, returns success with 0 records.
        """
        started_at = datetime.now(timezone.utc)

        try:
            orchestrator = self._get_orchestrator()
            current_week = date.today().isocalendar()[1]

            # Growing season check (May through September)
            if current_week < 18 or current_week > 38:
                return CollectorResult(
                    success=True,
                    source=self.config.source_name,
                    collected_at=datetime.now(timezone.utc),
                    records_fetched=0,
                    data={'note': f'Off-season (week {current_week}). '
                                  f'Yield models run weeks 18-38.'},
                    response_time_ms=0,
                )

            result = orchestrator.run()

            return CollectorResult(
                success=True,
                source=self.config.source_name,
                collected_at=datetime.now(timezone.utc),
                records_fetched=result.get('total_predictions', 0),
                data=result,
                response_time_ms=int(
                    (datetime.now(timezone.utc) - started_at).total_seconds() * 1000
                ),
                data_as_of=datetime.now(timezone.utc),
                warnings=result.get('alerts', []),
            )

        except Exception as e:
            logger.error(f"Yield forecast failed: {e}", exc_info=True)
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
