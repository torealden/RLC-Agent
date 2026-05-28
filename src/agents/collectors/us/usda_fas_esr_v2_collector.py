"""
USDA FAS Export Sales Report (ESR) v2 collector.

Replaces the v1 ESR logic in usda_fas_collector.py. The v1 API at
`apps.fas.usda.gov/OpenData` returns 403/500 since FAS migrated to the
v2 platform at `api.fas.usda.gov` (announced via
https://apps.fas.usda.gov/opendatawebV2/).

Key differences from v1:
  - Base URL:       api.fas.usda.gov/api          (was apps.fas.usda.gov/OpenData/api)
  - Auth header:    X-Api-Key                     (was API_KEY)
  - Commodity codes: 3-digit shorthand            (was 7-digit, e.g. 801 not 2222000)
  - marketYear:     ENDING calendar year of MY    (was STARTING year)
                    MY 2024/25 was marketYear=2024 in v1, marketYear=2025 in v2.
  - SSL certs:      valid on api.fas.usda.gov     (apps.fas.usda.gov was expired)

Convention note: bronze.fas_export_sales `marketing_year` historically stored
the STARTING year of the MY (v1 convention). To preserve continuity with the
~1.19M existing rows back to 1999, we convert v2 marketYear back to v1
convention at save time: `marketing_year = v2_marketYear - 1`.

Required env: FAS_API_KEY (register at https://apps.fas.usda.gov/opendatawebV2/)

CLI:
    python -m src.agents.collectors.us.usda_fas_esr_v2_collector
    python -m src.agents.collectors.us.usda_fas_esr_v2_collector --my 2026
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import urllib3
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from src.agents.base.base_collector import (
    BaseCollector, CollectorConfig, CollectorResult, DataFrequency, AuthType,
)

# api.fas.usda.gov has a valid cert, but the suppression is cheap and protects
# us against intermittent CA chain hiccups on the RDS host.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

V2_BASE_URL = 'https://api.fas.usda.gov/api'
API_KEY_HEADER = 'X-Api-Key'


# 3-digit ESR commodity codes per the v2 /api/esr/commodities endpoint
# Mapping: internal_name -> {code, description, unit}.
ESR_V2_COMMODITIES: Dict[str, Dict[str, Any]] = {
    'wheat_hrw':       {'code': 101, 'description': 'Wheat - HRW',          'unit': 'MT'},
    'wheat_srw':       {'code': 102, 'description': 'Wheat - SRW',          'unit': 'MT'},
    'wheat_hrs':       {'code': 103, 'description': 'Wheat - HRS',          'unit': 'MT'},
    'wheat_white':     {'code': 104, 'description': 'Wheat - White',        'unit': 'MT'},
    'wheat_durum':     {'code': 105, 'description': 'Wheat - Durum',        'unit': 'MT'},
    'wheat_all':       {'code': 107, 'description': 'All Wheat',            'unit': 'MT'},
    'barley':          {'code': 301, 'description': 'Barley',               'unit': 'MT'},
    'corn':            {'code': 401, 'description': 'Corn',                 'unit': 'MT'},
    'sorghum':         {'code': 701, 'description': 'Sorghum',              'unit': 'MT'},
    'soybeans':        {'code': 801, 'description': 'Soybeans',             'unit': 'MT'},
    'soybean_meal':    {'code': 901, 'description': 'Soybean cake & meal',  'unit': 'MT'},
    'soybean_oil':     {'code': 902, 'description': 'Soybean Oil',          'unit': 'MT'},
    'cottonseed_oil':  {'code': 1203, 'description': 'Cottonseed Oil',      'unit': 'MT'},
    'sunflowerseed_oil': {'code': 1110, 'description': 'Sunflowerseed Oil', 'unit': 'MT'},
    'cotton_upland':   {'code': 1404, 'description': 'All Upland Cotton',   'unit': '480LB BALES'},
    'rice_all':        {'code': 1505, 'description': 'All Rice',            'unit': 'MT'},
}

DEFAULT_COMMODITIES = [
    'corn', 'soybeans', 'soybean_meal', 'soybean_oil',
    'wheat_hrw', 'wheat_srw', 'wheat_hrs',
    'sorghum', 'cotton_upland',
]


# Map FAS country code -> 2-char ISO code, for the small number we need.
# Bronze stores country_code as the FAS numeric code by default for v2 — we
# don't try to maintain an ISO map here.
def _resolve_country_code(record: Dict[str, Any]) -> str:
    """FAS v2 records carry numeric countryCode; we store as string."""
    cc = record.get('countryCode')
    return str(cc) if cc is not None else ''


@dataclass
class ESRv2Config(CollectorConfig):
    source_name: str = 'USDA FAS ESR v2'
    source_url: str = V2_BASE_URL
    auth_type: AuthType = AuthType.NONE  # we set X-Api-Key manually
    frequency: DataFrequency = DataFrequency.WEEKLY
    rate_limit_per_minute: int = 60
    timeout: int = 30
    commodities: List[str] = field(default_factory=lambda: list(DEFAULT_COMMODITIES))


class USDAFASESRv2Collector(BaseCollector):
    """USDA FAS Export Sales Report collector (v2 API)."""

    def __init__(self, commodities: Optional[List[str]] = None, **kwargs):
        config = ESRv2Config()
        if commodities:
            config.commodities = list(commodities)
        super().__init__(config)
        self.api_key = os.environ.get('FAS_API_KEY', '')
        if not self.api_key:
            self.logger.warning("FAS_API_KEY not set — requests will fail with 403")

    # -- BaseCollector abstract methods -----------------------------------

    def get_table_name(self) -> str:
        return 'bronze.fas_export_sales'

    def parse_response(self, response_data: Any) -> Any:
        return response_data

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        marketing_years: Optional[List[int]] = None,
        **kwargs,
    ) -> CollectorResult:
        """Fetch ESR records for configured commodities.

        Args:
            marketing_years: v2 marketYear values (i.e., the ENDING year of
                each MY). Defaults to current MY and previous MY.
        """
        if marketing_years is None:
            # Default: current MY (year that ENDS in current or next calendar
            # year) + one prior year of overlap to catch late corrections.
            now = date.today()
            # Soybean/corn MY ends Aug 31. If today is past Aug 31, current MY ends NEXT year.
            current_my_end = now.year if now.month >= 9 else now.year
            # In v2 convention: marketYear=YYYY means MY that ENDS in YYYY (Aug 31).
            # Today 2026-05-21: latest MY is the one ending 2026-08-31 -> marketYear=2026.
            marketing_years = [current_my_end - 1, current_my_end, current_my_end + 1]
            # Drop years too far in the future to avoid 400s
            marketing_years = [y for y in marketing_years if y <= now.year + 1]

        all_records: List[Dict[str, Any]] = []
        warnings: List[str] = []

        for commodity_key in self.config.commodities:
            if commodity_key not in ESR_V2_COMMODITIES:
                warnings.append(f"Unknown commodity key: {commodity_key}")
                continue
            cinfo = ESR_V2_COMMODITIES[commodity_key]
            code = cinfo['code']

            for my in marketing_years:
                url = f"{V2_BASE_URL}/esr/exports/commodityCode/{code}/allCountries/marketYear/{my}"
                self._respect_rate_limit()
                try:
                    resp = requests.get(
                        url,
                        headers={API_KEY_HEADER: self.api_key},
                        verify=False,
                        timeout=self.config.timeout,
                    )
                except Exception as e:
                    warnings.append(f"{commodity_key} MY{my}: HTTP error {e}")
                    continue

                if resp.status_code != 200:
                    warnings.append(f"{commodity_key} MY{my}: HTTP {resp.status_code} {resp.text[:120]}")
                    continue

                try:
                    payload = resp.json()
                except Exception as e:
                    warnings.append(f"{commodity_key} MY{my}: JSON parse error {e}")
                    continue

                for rec in payload:
                    parsed = self._parse_record(rec, commodity_key, cinfo, my)
                    # _parse_record returns 0, 1, or 2 rows (NMY synthesis)
                    if parsed:
                        all_records.extend(parsed)

                self.logger.info(
                    f"  {commodity_key} (code={code}) MY{my}: "
                    f"{len(payload)} raw records"
                )

        period_end = max((r['week_ending'] for r in all_records), default=None)
        period_start = min((r['week_ending'] for r in all_records), default=None)

        return CollectorResult(
            success=len(all_records) > 0,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=all_records,
            period_start=str(period_start) if period_start else None,
            period_end=str(period_end) if period_end else None,
            warnings=warnings,
            error_message='; '.join(warnings[:3]) if warnings and not all_records else None,
        )

    def _parse_record(
        self,
        rec: Dict[str, Any],
        commodity_key: str,
        cinfo: Dict[str, Any],
        market_year_v2: int,
    ) -> List[Dict[str, Any]]:
        """Map a v2 ESR API record to one or two bronze.fas_export_sales rows.

        Returns:
          - [] on parse error or missing week
          - [current_my_row] for normal records
          - [current_my_row, nmy_row] when the record's nextMYNetSales (forward
            sales booked for the following MY) is non-zero. The synthetic
            NMY row carries those forward-MY figures so they appear in
            gold.export_sales_matrix under marketing_year = current MY + 1
            (otherwise they'd be stranded in the current-MY row's
            prev_my_accumulated column and invisible to NMY spreadsheet tabs).

        Critical convention:
          bronze.fas_export_sales.marketing_year stores the STARTING year of
          the MY (v1 convention). v2 API returns the ENDING year. Convert.
        """
        try:
            week_str = rec.get('weekEndingDate', '')
            if not week_str:
                return []
            week_ending = date.fromisoformat(week_str[:10])

            v1_marketing_year = market_year_v2 - 1
            next_my_net = self._safe_float(rec.get('nextMYNetSales'))
            next_my_outstanding = self._safe_float(rec.get('nextMYOutstandingSales'))

            current_row = {
                'commodity': commodity_key,
                'commodity_code': cinfo['code'],
                'country': '',  # v2 doesn't return countryDescription in this endpoint
                'country_code': _resolve_country_code(rec),
                'region': '',
                'marketing_year': v1_marketing_year,
                'week_ending': week_ending,
                'weekly_exports': self._safe_float(rec.get('weeklyExports')),
                'accumulated_exports': self._safe_float(rec.get('accumulatedExports')),
                'outstanding_sales': self._safe_float(rec.get('outstandingSales')),
                'gross_new_sales': self._safe_float(rec.get('grossNewSales')),
                'net_sales': self._safe_float(rec.get('currentMYNetSales')),
                'prev_my_accumulated': next_my_net,
                'unit': cinfo['unit'],
            }
            out = [current_row]

            # Synthesize an NMY row when forward sales are present. FAS reports
            # forward-MY sales in both (a) a dedicated marketYear=N+1 query and
            # (b) the nextMY* fields on current-MY rows. Early in the season
            # (b) often has data before (a) does, so we always synthesize from
            # (b) — the dedicated NMY query, when it has data, will upsert
            # over our synthetic row via the existing unique constraint on
            # (commodity_code, country_code, marketing_year, week_ending).
            if next_my_net is not None and next_my_net != 0:
                out.append({
                    'commodity': commodity_key,
                    'commodity_code': cinfo['code'],
                    'country': '',
                    'country_code': current_row['country_code'],
                    'region': '',
                    'marketing_year': v1_marketing_year + 1,  # NMY
                    'week_ending': week_ending,
                    'weekly_exports': 0.0,           # no physical exports yet for NMY
                    'accumulated_exports': 0.0,
                    'outstanding_sales': next_my_outstanding,  # cumulative forward outstanding
                    'gross_new_sales': next_my_net,  # gross = net when no cancellations
                    'net_sales': next_my_net,
                    'prev_my_accumulated': None,
                    'unit': cinfo['unit'],
                })
            return out
        except Exception as e:
            self.logger.warning(f"parse_record error for {commodity_key}: {e}")
            return []

    @staticmethod
    def _safe_float(v: Any) -> Optional[float]:
        if v is None or v == '':
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    # -- Persistence ------------------------------------------------------

    def save_to_bronze(self, records: List[Dict[str, Any]]) -> int:
        """Upsert ESR records to bronze.fas_export_sales using its existing
        unique constraint (commodity_code, country_code, marketing_year,
        week_ending).
        """
        if not records:
            return 0

        from src.services.database.db_config import get_connection

        with get_connection() as conn:
            cur = conn.cursor()
            count = 0
            for rec in records:
                cur.execute(
                    """
                    INSERT INTO bronze.fas_export_sales
                        (commodity, commodity_code, country, country_code, region,
                         marketing_year, week_ending,
                         weekly_exports, accumulated_exports, outstanding_sales,
                         gross_new_sales, net_sales, prev_my_accumulated,
                         unit, collected_at)
                    VALUES
                        (%(commodity)s, %(commodity_code)s, %(country)s,
                         %(country_code)s, %(region)s,
                         %(marketing_year)s, %(week_ending)s,
                         %(weekly_exports)s, %(accumulated_exports)s, %(outstanding_sales)s,
                         %(gross_new_sales)s, %(net_sales)s, %(prev_my_accumulated)s,
                         %(unit)s, NOW())
                    ON CONFLICT (commodity_code, country_code, marketing_year, week_ending)
                    DO UPDATE SET
                        weekly_exports      = EXCLUDED.weekly_exports,
                        accumulated_exports = EXCLUDED.accumulated_exports,
                        outstanding_sales   = EXCLUDED.outstanding_sales,
                        gross_new_sales     = EXCLUDED.gross_new_sales,
                        net_sales           = EXCLUDED.net_sales,
                        prev_my_accumulated = EXCLUDED.prev_my_accumulated,
                        collected_at        = NOW()
                    """,
                    rec,
                )
                count += 1
            conn.commit()
            self.logger.info(f"Upserted {count} ESR rows to bronze.fas_export_sales")
            return count

    # -- Required override so the dispatcher persists ---------------------

    def collect(
        self,
        start_date: date = None,
        end_date: date = None,
        use_cache: bool = False,
        **kwargs,
    ) -> CollectorResult:
        """Fetch + persist, committing per commodity to avoid timeout-induced
        loss of work on a slow RDS connection. 30k upserts in one transaction
        was timing out at 10 min; per-commodity batches finish in 10-90s each.
        """
        marketing_years = kwargs.pop('marketing_years', None)
        commodity_keys = list(self.config.commodities)

        total_saved = 0
        all_warnings: List[str] = []
        period_start: Optional[date] = None
        period_end: Optional[date] = None
        any_success = False

        for commodity_key in commodity_keys:
            # Restrict to one commodity per pass
            self.config.commodities = [commodity_key]
            partial = self.fetch_data(marketing_years=marketing_years, **kwargs)
            if partial.warnings:
                all_warnings.extend(partial.warnings)
            if not partial.data:
                continue
            saved = self.save_to_bronze(partial.data)
            total_saved += saved
            any_success = True
            # Track period coverage
            for r in partial.data:
                w = r.get('week_ending')
                if not w:
                    continue
                if period_start is None or w < period_start:
                    period_start = w
                if period_end is None or w > period_end:
                    period_end = w

        # Restore original commodity list so the collector instance is reusable
        self.config.commodities = commodity_keys

        return CollectorResult(
            success=any_success,
            source=self.config.source_name,
            records_fetched=total_saved,
            period_start=str(period_start) if period_start else None,
            period_end=str(period_end) if period_end else None,
            warnings=all_warnings,
            error_message='; '.join(all_warnings[:3]) if all_warnings and not any_success else None,
        )


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description='FAS ESR v2 collector')
    p.add_argument('--my', type=int, action='append',
                   help='Marketing year (v2 convention: ending year). Repeat for multiple.')
    p.add_argument('--commodity', action='append',
                   help='Commodity key from ESR_V2_COMMODITIES. Repeat for multiple.')
    p.add_argument('--dry-run', action='store_true',
                   help='Fetch but do not save.')
    p.add_argument('-v', '--verbose', action='store_true')
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s %(message)s',
    )

    c = USDAFASESRv2Collector(commodities=args.commodity)
    if args.dry_run:
        result = c.fetch_data(marketing_years=args.my)
        print(f"success={result.success}  fetched={result.records_fetched}")
        if result.warnings:
            for w in result.warnings[:5]:
                print(f"  warn: {w}")
    else:
        result = c.collect(**({'marketing_years': args.my} if args.my else {}))
        print(json.dumps({
            'success': result.success,
            'saved': result.records_fetched,
            'period_start': result.period_start,
            'period_end': result.period_end,
            'warnings_count': len(result.warnings or []),
        }, indent=2))


if __name__ == '__main__':
    main()
