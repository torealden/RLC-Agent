"""
NDVI Collector — BaseCollector adapter.

Wraps ndvi_collector.py to dispatch weekly. Pulls USDA Crop Explorer charts
(NDVI, NDVI anomaly, precip, soil moisture, temp) into bronze.ndvi_chart.
"""

import logging
from datetime import datetime, timezone

from .base_collector import BaseCollector, CollectorConfig, CollectorResult, DataFrequency
from .ndvi_collector import NDVICollector as _NDVICollector

logger = logging.getLogger(__name__)


class NDVIChartCollector(BaseCollector):
    """Adapter pulling USDA Crop Explorer charts into bronze.ndvi_chart."""

    def __init__(self, **kwargs):
        config = CollectorConfig(
            source_name='ndvi_charts',
            source_url='https://ipad.fas.usda.gov',
            frequency=DataFrequency.WEEKLY,
        )
        super().__init__(config)
        self._collector = None

    def _get_collector(self):
        if self._collector is None:
            self._collector = _NDVICollector()
        return self._collector

    def get_table_name(self) -> str:
        return 'bronze.ndvi_chart'

    def parse_response(self, response_data):
        return response_data

    def fetch_data(self, start_date=None, end_date=None, **kwargs) -> CollectorResult:
        """
        Pull NDVI + anomaly + precip + soil moisture charts for key commodities/regions.

        kwargs:
            commodities: list (default: corn, soybeans, wheat)
            countries: list (default: US, BR, AR, CN, IN, AU, UA, RU)
        """
        started_at = datetime.now(timezone.utc)
        commodities = kwargs.get('commodities', ['corn', 'soybeans', 'wheat'])
        countries = kwargs.get('countries', ['US', 'BR', 'AR', 'CN', 'IN', 'AU', 'UA', 'RU'])

        try:
            collector = self._get_collector()
            total = 0
            for commodity in commodities:
                charts = collector.download_all_charts(
                    commodity=commodity,
                    countries=countries,
                )
                total += len(charts) if charts else 0

            return CollectorResult(
                success=total > 0,
                source=self.config.source_name,
                collected_at=datetime.now(timezone.utc),
                records_fetched=total,
                data={'commodities': commodities, 'countries': countries},
                response_time_ms=int(
                    (datetime.now(timezone.utc) - started_at).total_seconds() * 1000
                ),
                data_as_of=datetime.now(timezone.utc),
            )
        except Exception as e:
            logger.error(f"NDVI chart collection failed: {e}", exc_info=True)
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
