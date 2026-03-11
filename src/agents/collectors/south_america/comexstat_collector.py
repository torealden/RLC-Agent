"""
ComexStat Brazil Trade Collector

Collects Brazilian foreign trade statistics from the ComexStat API
(Ministry of Development, Industry, Commerce and Services - MDIC).

Data source:
- https://comexstat.mdic.gov.br/
- API: https://api-comexstat.mdic.gov.br/general

No API key required. Rate limit: ~1 request per 10 seconds.
Monthly data from 1997 to present.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, date
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

# SH4 heading codes for agricultural commodities
COMEXSTAT_HEADINGS = {
    '1201': 'soybeans',
    '1005': 'corn',
    '1001': 'wheat',
    '1507': 'soybean_oil',
    '2304': 'soybean_meal',
    '1006': 'rice',
    '1007': 'sorghum',
    '5201': 'cotton',
    '1701': 'sugar',
    '0901': 'coffee',
    '1512': 'sunflower_oil',
    '1206': 'sunflower_seed',
    '1514': 'canola_oil',
    '1205': 'canola_seed',
    '2306': 'sunflower_meal',
}

# Reverse: commodity -> heading
COMMODITY_HEADINGS = {v: k for k, v in COMEXSTAT_HEADINGS.items()}


@dataclass
class ComexStatConfig(CollectorConfig):
    """ComexStat specific configuration"""
    source_name: str = "ComexStat Brazil"
    source_url: str = "https://comexstat.mdic.gov.br"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    api_base: str = "https://api-comexstat.mdic.gov.br"

    # Default commodities (SH4 headings)
    headings: List[str] = field(default_factory=lambda: [
        '1201', '1005', '1001', '1507', '2304',  # soy complex + corn + wheat
    ])

    # Rate limit: 1 req per 10 seconds
    rate_limit_per_minute: int = 5
    timeout: int = 60


class ComexStatCollector(BaseCollector):
    """
    Collector for ComexStat Brazilian trade data.

    Provides monthly import/export data from SISCOMEX. Key for tracking:
    - Brazil soybean/corn export volumes by destination (China, EU, etc.)
    - Soy oil/meal export flows
    - Import patterns

    No API key required. Rate limited to ~1 request per 10 seconds.
    """

    def __init__(self, config: ComexStatConfig = None):
        config = config or ComexStatConfig()
        super().__init__(config)
        self.config: ComexStatConfig = config
        # ComexStat has SSL cert issues — disable verification
        self.session.verify = False

    def get_table_name(self) -> str:
        return "comexstat_trade"

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
        flow: str = "export",
        headings: List[str] = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch trade data from ComexStat API.

        Args:
            start_date: Start date (default: Jan of current year)
            end_date: End date (default: current month)
            flow: 'export' or 'import'
            headings: SH4 heading codes to fetch

        Returns:
            CollectorResult with trade data
        """
        headings = headings or self.config.headings
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year, 1, 1)

        period_from = f"{start_date.year}-{start_date.month:02d}"
        period_to = f"{end_date.year}-{end_date.month:02d}"

        all_records = []
        warnings = []

        payload = {
            "flow": flow,
            "monthDetail": True,
            "period": {
                "from": period_from,
                "to": period_to
            },
            "filters": [
                {"filter": "heading", "values": headings}
            ],
            "details": ["country", "ncm", "state"],
            "metrics": ["metricFOB", "metricKG"],
            "language": "en"
        }

        # Add CIF/freight for imports
        if flow == "import":
            payload["metrics"].extend(["metricCIF", "metricFreight"])

        url = f"{self.config.api_base}/general"

        response, error = self._make_request(
            url,
            method="POST",
            json_data=payload,
            timeout=self.config.timeout
        )

        if error:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"API request failed: {error}"
            )

        if response.status_code == 429:
            warnings.append("Rate limited — retry after 10s delay")
            time.sleep(12)
            response, error = self._make_request(
                url, method="POST", json_data=payload
            )
            if error or response.status_code != 200:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"Rate limited and retry failed: {error or response.status_code}"
                )

        if response.status_code != 200:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"HTTP {response.status_code}: {response.text[:200]}"
            )

        try:
            data = response.json()

            if "error" in data:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"API error: {data['error']}"
                )

            rows = data.get("data", {}).get("list", [])

            for row in rows:
                parsed = self._parse_record(row, flow)
                if parsed:
                    all_records.append(parsed)

        except Exception as e:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Parse error: {str(e)}"
            )

        if PANDAS_AVAILABLE and all_records:
            result_df = pd.DataFrame(all_records)
        else:
            result_df = all_records

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=result_df,
            period_start=period_from,
            period_end=period_to,
            warnings=warnings
        )

    def _parse_record(self, row: Dict, flow: str) -> Optional[Dict]:
        """Parse a ComexStat API response row."""
        try:
            ncm_code = str(row.get('coNcm', '')).strip()
            heading = ncm_code[:4] if len(ncm_code) >= 4 else ''
            commodity = COMEXSTAT_HEADINGS.get(heading, '')

            fob = row.get('metricFOB')
            kg = row.get('metricKG')

            return {
                'flow': flow,
                'year': int(row.get('year', 0)) or None,
                'month': int(row.get('monthNumber', 0)) or None,
                'ncm_code': ncm_code,
                'ncm_description': row.get('ncm', ''),
                'heading_code': heading,
                'commodity': commodity,
                'country': row.get('country', ''),
                'state': row.get('state', ''),
                'fob_usd': int(fob) if fob else None,
                'weight_kg': int(kg) if kg else None,
                'cif_usd': int(row.get('metricCIF', 0)) if row.get('metricCIF') else None,
                'freight_usd': int(row.get('metricFreight', 0)) if row.get('metricFreight') else None,
                'source': 'ComexStat',
            }
        except Exception as e:
            logger.warning(f"Error parsing ComexStat record: {e}")
            return None

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    def save_to_bronze(self, records: list) -> int:
        """Upsert records to bronze.comexstat_trade."""
        if not records:
            return 0
        with get_db_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                cur.execute("""
                    INSERT INTO bronze.comexstat_trade
                        (flow, year, month, ncm_code, ncm_description,
                         heading_code, commodity, country, state,
                         fob_usd, weight_kg, cif_usd, freight_usd,
                         source, collected_at)
                    VALUES
                        (%(flow)s, %(year)s, %(month)s, %(ncm_code)s,
                         %(ncm_description)s, %(heading_code)s, %(commodity)s,
                         %(country)s, %(state)s, %(fob_usd)s, %(weight_kg)s,
                         %(cif_usd)s, %(freight_usd)s, %(source)s, NOW())
                    ON CONFLICT (flow, year, month, ncm_code, country, state)
                    DO UPDATE SET
                        fob_usd = EXCLUDED.fob_usd,
                        weight_kg = EXCLUDED.weight_kg,
                        cif_usd = EXCLUDED.cif_usd,
                        freight_usd = EXCLUDED.freight_usd,
                        collected_at = NOW()
                """, rec)
                count += 1
            conn.commit()
            self.logger.info(f"Saved {count} records to bronze.comexstat_trade")
            return count


def main():
    """CLI for ComexStat collector"""
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description='ComexStat Brazil Trade Collector')
    parser.add_argument('command', choices=['exports', 'imports', 'both', 'test'])
    parser.add_argument('--year', type=int, default=date.today().year)
    parser.add_argument('--headings', nargs='+', default=['1201', '1005', '1001', '1507', '2304'])
    args = parser.parse_args()

    collector = ComexStatCollector()

    if args.command == 'test':
        success, msg = collector.test_connection()
        print(f"Test: {'PASS' if success else 'FAIL'} - {msg}")
        return

    flows = ['export', 'import'] if args.command == 'both' else [args.command.rstrip('s')]
    start = date(args.year, 1, 1)
    end = date.today()

    for flow in flows:
        result = collector.collect(
            start_date=start,
            end_date=end,
            flow=flow,
            headings=args.headings,
            use_cache=False
        )
        print(f"{flow}: success={result.success}, records={result.records_fetched}")
        if result.warnings:
            print(f"  Warnings: {result.warnings}")
        if flow != flows[-1]:
            time.sleep(12)


if __name__ == '__main__':
    main()
