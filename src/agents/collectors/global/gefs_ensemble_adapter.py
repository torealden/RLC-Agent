"""
GEFS Ensemble Forecast Collector — BaseCollector adapter.

Wraps the standalone gefs_ensemble_collector.py so it can be dispatched.
Populates silver.weather_forecast_daily with ensemble p10/p50/p90 fields.
"""

import logging
from datetime import date, datetime, timezone

from .base_collector import BaseCollector, CollectorConfig, CollectorResult, DataFrequency
from .gefs_ensemble_collector import GEFSCollector

logger = logging.getLogger(__name__)


class GEFSEnsembleCollector(BaseCollector):
    """Adapter around GEFSCollector (ensemble percentiles)."""

    def __init__(self, **kwargs):
        config = CollectorConfig(
            source_name='gefs_ensemble',
            source_url='s3://noaa-gefs-pds',
            frequency=DataFrequency.DAILY,
        )
        super().__init__(config)
        self._collector = None

    def _get_collector(self):
        if self._collector is None:
            self._collector = GEFSCollector()
        return self._collector

    def get_table_name(self) -> str:
        return 'silver.weather_forecast_daily'

    def parse_response(self, response_data):
        return response_data

    def fetch_data(self, start_date=None, end_date=None, **kwargs) -> CollectorResult:
        started_at = datetime.now(timezone.utc)
        run_date = kwargs.get('run_date', date.today())
        regions = kwargs.get('regions')
        max_lead_days = kwargs.get('max_lead_days', 16)
        n_members = kwargs.get('n_members', 10)

        try:
            collector = self._get_collector()
            forecasts = collector.collect_ensemble_forecast(
                run_date=run_date,
                regions=regions,
                max_lead_days=max_lead_days,
                n_members=n_members,
            )
            saved = collector.save_to_database(forecasts) if forecasts else 0

            return CollectorResult(
                success=saved > 0,
                source=self.config.source_name,
                collected_at=datetime.now(timezone.utc),
                records_fetched=len(forecasts),
                data={'regions': regions, 'saved_rows': saved, 'n_members': n_members},
                response_time_ms=int(
                    (datetime.now(timezone.utc) - started_at).total_seconds() * 1000
                ),
                data_as_of=datetime.now(timezone.utc),
            )
        except Exception as e:
            logger.error(f"GEFS ensemble collection failed: {e}", exc_info=True)
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
