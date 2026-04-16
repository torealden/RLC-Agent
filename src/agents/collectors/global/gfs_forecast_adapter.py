"""
GFS Weather Forecast Collector — BaseCollector adapter.

Wraps the standalone gfs_forecast_collector.py into the BaseCollector pattern
so it can be dispatched on schedule via src/dispatcher/collector_registry.py.

Underlying collector downloads NOAA GFS GRIB2 files (AWS Open Data bucket
`noaa-gfs-bdp-pds`), aggregates to crop regions (CROP_REGION_BOUNDS), and
writes per-region daily forecasts to silver.weather_forecast_daily.
"""

import logging
from datetime import date, datetime, timezone

from .base_collector import BaseCollector, CollectorConfig, CollectorResult, DataFrequency
from .gfs_forecast_collector import GFSCollector

logger = logging.getLogger(__name__)


class GFSForecastCollector(BaseCollector):
    """Adapter around GFSCollector for dispatcher scheduling."""

    def __init__(self, **kwargs):
        config = CollectorConfig(
            source_name='gfs_forecast',
            source_url='s3://noaa-gfs-bdp-pds',
            frequency=DataFrequency.DAILY,
        )
        super().__init__(config)
        self._collector = None

    def _get_collector(self):
        if self._collector is None:
            self._collector = GFSCollector()
        return self._collector

    def get_table_name(self) -> str:
        return 'silver.weather_forecast_daily'

    def parse_response(self, response_data):
        return response_data

    def fetch_data(self, start_date=None, end_date=None, **kwargs) -> CollectorResult:
        """
        Pull latest GFS run, aggregate to crop regions, save to DB.

        kwargs:
            regions: list of region codes (default: all in CROP_REGION_BOUNDS)
            max_lead_days: forecast horizon (default: 16)
            run_date: override forecast date (default: today)
        """
        started_at = datetime.now(timezone.utc)
        run_date = kwargs.get('run_date', date.today())
        regions = kwargs.get('regions')
        max_lead_days = kwargs.get('max_lead_days', 16)

        try:
            collector = self._get_collector()
            forecasts = collector.collect_forecast(
                run_date=run_date,
                regions=regions,
                max_lead_days=max_lead_days,
            )
            saved = collector.save_to_database(forecasts) if forecasts else 0

            return CollectorResult(
                success=saved > 0,
                source=self.config.source_name,
                collected_at=datetime.now(timezone.utc),
                records_fetched=len(forecasts),
                data={'regions': regions, 'saved_rows': saved, 'lead_days': max_lead_days},
                response_time_ms=int(
                    (datetime.now(timezone.utc) - started_at).total_seconds() * 1000
                ),
                data_as_of=datetime.now(timezone.utc),
            )
        except Exception as e:
            logger.error(f"GFS forecast collection failed: {e}", exc_info=True)
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
