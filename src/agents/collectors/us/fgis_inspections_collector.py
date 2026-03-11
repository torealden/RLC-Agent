"""
FGIS Export Inspections Collector

Collects weekly grain export inspection data from USDA FGIS via
the Open Ag Transport Data SODA API.

Data source:
- https://agtransport.usda.gov/Exports/Grain-Inspections/sruw-w49i
- SODA API (no key required for limited use)

Tracks actual physical grain leaving US ports — complementary to
FAS export sales (commitments) and Census trade (customs clearance).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

from src.services.database.db_config import get_connection as get_db_connection

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Normalize FGIS grain names to our standard commodity names
GRAIN_MAP = {
    'CORN': 'corn',
    'SOYBEANS': 'soybeans',
    'WHEAT': 'wheat',
    'SORGHUM': 'sorghum',
    'BARLEY': 'barley',
    'OATS': 'oats',
    'RICE': 'rice',
    'RYE': 'rye',
    'SUNFLOWER SEED': 'sunflower',
    'FLAXSEED': 'flaxseed',
}


@dataclass
class FGISConfig(CollectorConfig):
    """FGIS specific configuration"""
    source_name: str = "FGIS Export Inspections"
    source_url: str = "https://agtransport.usda.gov"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.WEEKLY

    # SODA API endpoint
    api_base: str = "https://agtransport.usda.gov/resource/sruw-w49i.json"

    # Default grains
    grains: List[str] = field(default_factory=lambda: [
        'CORN', 'SOYBEANS', 'WHEAT', 'SORGHUM', 'BARLEY'
    ])

    # Rate limiting (SODA throttles at ~1000 req/hr without app token)
    rate_limit_per_minute: int = 30
    timeout: int = 60


class FGISInspectionsCollector(BaseCollector):
    """
    Collector for FGIS weekly grain export inspections.

    FGIS (Federal Grain Inspection Service) provides weekly data on
    grain inspected and weighed for export at US ports. This is the
    most granular view of physical grain movement out of the US.

    No API key required (SODA public endpoint).
    """

    def __init__(self, config: FGISConfig = None):
        config = config or FGISConfig()
        super().__init__(config)
        self.config: FGISConfig = config

    def get_table_name(self) -> str:
        return "fgis_inspections"

    def collect(self, start_date=None, end_date=None, use_cache=True, **kwargs):
        """Override collect to save results to bronze after fetching."""
        result = super().collect(start_date, end_date, use_cache, **kwargs)
        if result.success and result.data is not None and not getattr(result, 'from_cache', False):
            try:
                records = result.data.to_dict('records') if hasattr(result.data, 'to_dict') else result.data
                if records:
                    saved = self.save_to_bronze(records)
                    result.records_fetched = saved
            except Exception as e:
                self.logger.error(f"Bronze save failed (data still returned): {e}")
        return result

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        grains: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch export inspection data from SODA API.

        Args:
            start_date: Start date (default: 4 weeks ago)
            end_date: End date (default: today)
            grains: List of grain names to fetch

        Returns:
            CollectorResult with fetched data
        """
        grains = grains or self.config.grains
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=28))

        all_records = []
        warnings = []

        # SODA API query — fetch in pages of 50000
        offset = 0
        page_size = 50000

        # Build SoQL WHERE clause
        grain_filter = " OR ".join(f"grain='{g}'" for g in grains)
        where = (
            f"date >= '{start_date.isoformat()}' "
            f"AND date <= '{end_date.isoformat()}' "
            f"AND ({grain_filter})"
        )

        while True:
            params = {
                '$where': where,
                '$limit': page_size,
                '$offset': offset,
                '$order': 'date DESC',
            }

            response, error = self._make_request(
                self.config.api_base,
                params=params
            )

            if error:
                if all_records:
                    warnings.append(f"Partial fetch — error at offset {offset}: {error}")
                    break
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"API request failed: {error}"
                )

            if response.status_code != 200:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"HTTP {response.status_code}"
                )

            try:
                data = response.json()
            except Exception as e:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"JSON parse error: {e}"
                )

            if not data:
                break

            for record in data:
                parsed = self._parse_record(record)
                if parsed:
                    all_records.append(parsed)

            if len(data) < page_size:
                break

            offset += page_size

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            period_start=str(start_date),
            period_end=str(end_date),
            warnings=warnings
        )

    def _parse_record(self, record: Dict) -> Optional[Dict]:
        """Parse a single SODA API record."""
        try:
            grain = record.get('grain', '')
            commodity = GRAIN_MAP.get(grain.upper(), grain.lower())

            raw_date = record.get('date', record.get('cert_date', ''))
            inspection_date = raw_date[:10] if raw_date else None

            mt = record.get('mt')
            metric_tons = float(mt) if mt else None

            lbs = record.get('pounds')
            pounds = float(lbs) if lbs else None

            return {
                'inspection_date': inspection_date,
                'week': int(record.get('week', 0)) or None,
                'month': int(record.get('month', 0)) or None,
                'year': int(record.get('year', 0)) or None,
                'grain': grain,
                'commodity': commodity,
                'grade': record.get('grade', ''),
                'grain_class': record.get('class', ''),
                'destination': record.get('destination', ''),
                'port': record.get('port', ''),
                'ams_region': record.get('ams_reg', ''),
                'state': record.get('state', ''),
                'metric_tons': metric_tons,
                'pounds': pounds,
                'carrier_type': record.get('type_carrier_text', ''),
                'source': 'FGIS',
            }
        except Exception as e:
            logger.warning(f"Error parsing FGIS record: {e}")
            return None

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    def save_to_bronze(self, records: list) -> int:
        """Upsert records to bronze.fgis_inspections."""
        if not records:
            return 0
        with get_db_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                cur.execute("""
                    INSERT INTO bronze.fgis_inspections
                        (inspection_date, week, month, year, grain, commodity,
                         grade, grain_class, destination, port, ams_region,
                         state, metric_tons, pounds, carrier_type, source,
                         collected_at)
                    VALUES
                        (%(inspection_date)s, %(week)s, %(month)s, %(year)s,
                         %(grain)s, %(commodity)s, %(grade)s, %(grain_class)s,
                         %(destination)s, %(port)s, %(ams_region)s, %(state)s,
                         %(metric_tons)s, %(pounds)s, %(carrier_type)s,
                         %(source)s, NOW())
                    ON CONFLICT (inspection_date, grain, destination, port,
                                 grain_class, carrier_type, state)
                    DO UPDATE SET
                        metric_tons = EXCLUDED.metric_tons,
                        pounds = EXCLUDED.pounds,
                        collected_at = NOW()
                """, rec)
                count += 1
            conn.commit()
            self.logger.info(f"Saved {count} records to bronze.fgis_inspections")
            return count


def main():
    """CLI for FGIS collector"""
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='FGIS Export Inspections Collector')
    parser.add_argument('command', choices=['fetch', 'backfill', 'test'])
    parser.add_argument('--weeks', type=int, default=4, help='Weeks of history')
    parser.add_argument('--grains', nargs='+', default=['CORN', 'SOYBEANS', 'WHEAT'])
    args = parser.parse_args()

    collector = FGISInspectionsCollector()

    if args.command == 'test':
        success, msg = collector.test_connection()
        print(f"Test: {'PASS' if success else 'FAIL'} - {msg}")
        return

    end_date = date.today()
    start_date = end_date - timedelta(weeks=args.weeks)
    if args.command == 'backfill':
        start_date = date(2024, 1, 1)

    result = collector.collect(
        start_date=start_date,
        end_date=end_date,
        grains=args.grains,
        use_cache=False
    )
    print(f"Success: {result.success}, Records: {result.records_fetched}")
    if result.warnings:
        print(f"Warnings: {result.warnings}")


if __name__ == '__main__':
    main()
