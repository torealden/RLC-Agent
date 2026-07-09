"""
EIA Biofuel Feedstock Inputs Collector (v2 API)
===============================================

Pulls the EIA "Inputs to the Production of Biodiesel, Renewable Diesel, and
Other Biofuels" report from the EIA Open Data v2 API and writes it into
``bronze.eia_feedstock_monthly``.

WHY THIS EXISTS
---------------
The feedstock-by-type data used to come only from the Form 819 ``table2.xlsx``
(handled by ``EIABiofuelsForm819Collector``). EIA stopped refreshing the
animal-fat feedstock rows in that xlsx — they froze at Dec 2020. EIA now
exposes the full report (through 2026-04 and counting) via the v2 API at
``petroleum/pnp/feedbiofuel``, split by biodiesel-plant vs renewable-diesel-plant
for the three big vegetable oils and as a combined all-biofuel total for
everything else.

This collector REPLACES the table2.xlsx feedstock source. The Form 819
collector still owns ``table1.xlsx`` (operable capacity) — that path is
untouched; only its table2 feedstock path is retired (see that file).

API STRUCTURE (verified)
------------------------
Single endpoint, single ``process`` code (``YIFBP``, labelled "Feedstock Inputs
to Biodiesel Production" — a misnomer; it covers ALL biofuel production). The
biodiesel vs renewable-diesel split is encoded in the PRODUCT code, and only
for Soybean/Corn/Canola oil:

    EPOOBDSO   Soybean Oil (combined total)     -> Soybean Oil / total
    EPOOBDSOD  Soybean Oil, biodiesel plants    -> Soybean Oil / biodiesel
    EPOOBDSOR  Soybean Oil, renewable-D plants  -> Soybean Oil / renewable_diesel
    (Corn Oil = EPOOBDCNO/D/R, Canola = EPOOBDCO/D/R — same pattern)

Every other feedstock (Tallow, White/Yellow Grease, Poultry, Corn, Grain
Sorghum, ...) has a SINGLE product = the combined all-biofuel total, mapped to
plant_type='total'. Verified: Soybean total (1224) == biodiesel (686) +
renewable_diesel (537) for 2026-04, so 'total' is a NATIVE, redaction-proof
rollup — no synthesis of 'total' is needed.

Values are in MMLB (million pounds). value=None means EIA withheld the cell.

CLOBBER SAFETY
--------------
``bronze.eia_feedstock_monthly`` has a UNIQUE key that INCLUDES source_sheet.
To avoid the downstream rake double-counting an API row alongside an old
table2/oldtable3 row for the same (feedstock, plant_type, period), this
collector:
  1. writes all API rows under source_sheet='api_feedbiofuel' (idempotent UPSERT);
  2. after loading, deletes OLD-source rows that overlap an API row that has a
     non-null value. Old rows the API does NOT cover (Cottonseed Oil, pre-2021
     animal-fat biodiesel history, all-null aggregate categories) are preserved.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base_collector import (
    AuthType, BaseCollector, CollectorConfig, CollectorResult, DataFrequency,
)

logger = logging.getLogger(__name__)


SOURCE_SHEET = "api_feed"  # <=10 chars (source_sheet is varchar(10))
SOURCE_FILE = "api:petroleum/pnp/feedbiofuel"

# EIA product code -> (feedstock_name, plant_type)
# feedstock_name strings match the EXISTING bronze convention the rake reads
# (Soybean Oil / Corn Oil / Canola Oil / Tallow / Yellow Grease / White Grease /
#  Poultry). plant_type in ('biodiesel','renewable_diesel','total').
PRODUCT_MAP: Dict[str, Tuple[str, str]] = {
    # --- three big veg oils: native total + BD-plant + RD-plant split ---
    'EPOOBDSO':   ('Soybean Oil', 'total'),
    'EPOOBDSOD':  ('Soybean Oil', 'biodiesel'),
    'EPOOBDSOR':  ('Soybean Oil', 'renewable_diesel'),
    'EPOOBDCNO':  ('Corn Oil', 'total'),
    'EPOOBDCNOD': ('Corn Oil', 'biodiesel'),
    'EPOOBDCNOR': ('Corn Oil', 'renewable_diesel'),
    'EPOOBDCO':   ('Canola Oil', 'total'),
    'EPOOBDCOD':  ('Canola Oil', 'biodiesel'),
    'EPOOBDCOR':  ('Canola Oil', 'renewable_diesel'),
    # --- animal fats & greases: single combined-total product each ---
    'EPOOBDFSTL': ('Tallow', 'total'),
    'EPOOBDFSWG': ('White Grease', 'total'),
    'EPOOBDFSYG': ('Yellow Grease', 'total'),
    'EPOOBDFSPT': ('Poultry', 'total'),
    'EPOOBDAFO':  ('Other Animal Fats', 'total'),  # EIA "Other-Animal Fats" residual
    # --- ag / grains ---
    'EPOOBDAFC':  ('Corn', 'total'),
    'EPOOBDAFS':  ('Grain Sorghum', 'total'),
    'EPOOBDAFD':  ('Energy Crops', 'total'),
    'EPOOBDAFR':  ('Ag Forestry Residues', 'total'),
    'EPOOBDAFPO': ('Other Ag', 'total'),
    # --- oils / algae / gas ---
    'EPOOBDVOO':  ('Other Vegetable Oil', 'total'),
    'EPOOBDAL':   ('Algae Oil', 'total'),
    'EPOOBDBG':   ('Biogas', 'total'),
    # --- recycled / waste ---
    'EPOOBDRFWM': ('Municipal Solid Waste', 'total'),
    'EPOOBDRFWO': ('Other Recycled', 'total'),
    'EPOOBDRFWY': ('Yard Food Waste', 'total'),
    # --- catch-all ---
    'EPOOBDOB':   ('Other NESOI', 'total'),  # EIA "Other Biofuel feedstocks"
}


@dataclass
class EIAFeedstockAPIConfig(CollectorConfig):
    source_name: str = "EIA Biofuel Feedstock Inputs (v2 API)"
    source_url: str = "https://api.eia.gov/v2/petroleum/pnp/feedbiofuel/data/"
    auth_type: AuthType = AuthType.API_KEY
    frequency: DataFrequency = DataFrequency.MONTHLY
    api_key: Optional[str] = field(default_factory=lambda: os.environ.get('EIA_API_KEY'))


class EIAFeedstockAPICollector(BaseCollector):
    """Collector for biofuel feedstock inputs from the EIA v2 API."""

    ENDPOINT = "https://api.eia.gov/v2/petroleum/pnp/feedbiofuel/data/"

    def __init__(self, config: Optional[EIAFeedstockAPIConfig] = None):
        config = config or EIAFeedstockAPIConfig()
        super().__init__(config)
        self.config: EIAFeedstockAPIConfig = config
        if not self.config.api_key:
            self.logger.warning(
                "No EIA API key configured. Set EIA_API_KEY env var. "
                "Register at https://www.eia.gov/opendata/register.php"
            )

    def get_table_name(self) -> str:
        return "eia_feedstock_monthly"

    def parse_response(self, response_data):
        return response_data

    # ------------------------------------------------------------------
    def _fetch(self, start: Optional[date] = None, end: Optional[date] = None) -> List[Dict[str, Any]]:
        """Fetch all feedstock-input rows (one call, whole report)."""
        params: Dict[str, Any] = {
            'api_key': self.config.api_key,
            'frequency': 'monthly',
            'data[0]': 'value',
            'length': 5000,
        }
        if start:
            params['start'] = start.strftime('%Y-%m')
        if end:
            params['end'] = end.strftime('%Y-%m')

        last_err = None
        for attempt in range(3):
            try:
                r = requests.get(self.ENDPOINT, params=params, timeout=30)
                if r.status_code == 200:
                    return r.json().get('response', {}).get('data', [])
                last_err = f"HTTP {r.status_code}"
            except requests.RequestException as e:
                last_err = str(e)
        self.logger.warning(f"feedbiofuel fetch failed: {last_err}")
        return []

    # ------------------------------------------------------------------
    def _save_to_bronze(self, records: List[Dict[str, Any]]) -> Tuple[int, int, List[str]]:
        """Upsert API rows, then dedupe old-source overlaps. Returns (written, deleted, unmapped)."""
        from src.services.database.db_config import get_connection

        unmapped: set = set()
        written = 0
        deleted = 0
        with get_connection() as conn:
            cur = conn.cursor()
            for rec in records:
                prod = rec.get('product')
                mapped = PRODUCT_MAP.get(prod)
                if not mapped:
                    unmapped.add(f"{prod} ({rec.get('product-name')})")
                    continue
                feedstock_name, plant_type = mapped
                period = rec.get('period')  # 'YYYY-MM'
                if not period:
                    continue
                try:
                    yr, mo = int(period[:4]), int(period[5:7])
                except (ValueError, IndexError):
                    continue

                raw_val = rec.get('value')
                is_withheld = raw_val is None
                quantity = None if is_withheld else float(raw_val)

                cur.execute("""
                    INSERT INTO bronze.eia_feedstock_monthly
                        (year, month, source_sheet, feedstock_name, plant_type,
                         quantity_mil_lbs, is_withheld, is_no_data, source_file, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (year, month, source_sheet, feedstock_name, plant_type)
                    DO UPDATE SET
                        quantity_mil_lbs = EXCLUDED.quantity_mil_lbs,
                        is_withheld      = EXCLUDED.is_withheld,
                        is_no_data       = EXCLUDED.is_no_data,
                        source_file      = EXCLUDED.source_file,
                        collected_at     = NOW()
                """, (
                    yr, mo, SOURCE_SHEET, feedstock_name, plant_type,
                    quantity, is_withheld, False, SOURCE_FILE,
                ))
                written += 1

            # Dedupe: remove stale rows from OTHER sources that an API row now
            # supersedes (non-null only). Preserves rows the API doesn't cover.
            cur.execute("""
                DELETE FROM bronze.eia_feedstock_monthly a
                USING bronze.eia_feedstock_monthly b
                WHERE a.source_sheet <> %s
                  AND b.source_sheet = %s
                  AND a.feedstock_name = b.feedstock_name
                  AND a.plant_type     = b.plant_type
                  AND a.year  = b.year
                  AND a.month = b.month
                  AND b.quantity_mil_lbs IS NOT NULL
            """, (SOURCE_SHEET, SOURCE_SHEET))
            deleted = cur.rowcount
            conn.commit()
        return written, deleted, sorted(unmapped)

    # ------------------------------------------------------------------
    def collect(self, start_date: Optional[date] = None,
                end_date: Optional[date] = None, **kwargs) -> CollectorResult:
        """Dispatcher entry point — MUST override collect() to persist."""
        if not self.config.api_key:
            return CollectorResult(
                success=False, source=self.config.source_name,
                error_message="No EIA_API_KEY configured",
            )

        records = self._fetch(start_date, end_date)
        if not records:
            return CollectorResult(
                success=False, source=self.config.source_name,
                error_message="No records returned from feedbiofuel endpoint",
            )

        written, deleted, unmapped = self._save_to_bronze(records)
        warnings: List[str] = []
        if unmapped:
            warnings.append(f"Unmapped products skipped: {', '.join(unmapped)}")
        self.logger.info(
            f"feedbiofuel: {written} rows upserted, {deleted} stale old-source rows removed"
        )
        return CollectorResult(
            success=written > 0,
            source=self.config.source_name,
            records_fetched=written,
            data={'upserted': written, 'stale_deleted': deleted},
            period_start=datetime.now().isoformat(),
            warnings=warnings,
        )

    # Kept for BaseCollector API symmetry; collect() is the real entry point.
    def fetch_data(self, **kwargs) -> CollectorResult:
        return self.collect(**kwargs)
