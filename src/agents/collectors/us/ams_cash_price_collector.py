"""
USDA AMS Cash Price Collector (Unified)

Collects structured price data from all AMS MARS API reports needed for
the HB Weekly Report and general cash price tracking. Covers:
- Grain prices (corn, soybeans, wheat, sorghum, barley, oats)
- Specialty grains (sunflower, DDGs)
- Livestock (hogs, feeder pigs, choice steers, feeder cattle)
- Cotton spot prices
- Ethanol prices
- Farm inputs (diesel, fertilizer)

Data Pipeline:
  MARS API -> bronze.ams_price_record (one row per price line)
           -> silver.cash_price (grains)
           -> silver.specialty_price (livestock, energy, inputs)

Usage:
    # Collect today's data
    python -m src.agents.collectors.us.ams_cash_price_collector

    # Collect specific date range
    python -m src.agents.collectors.us.ams_cash_price_collector --start 2026-02-20 --end 2026-02-28

    # Collect a single report
    python -m src.agents.collectors.us.ams_cash_price_collector --slug 3192
"""

import os
import sys
import json
import logging
import time
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import requests
from requests.auth import HTTPBasicAuth

# Add project root to path
# File is at src/agents/collectors/us/ams_cash_price_collector.py — 5 levels up
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

from src.agents.base.base_collector import (
    BaseCollector, CollectorConfig, CollectorResult, DataFrequency, AuthType
)

logger = logging.getLogger(__name__)

MARS_BASE_URL = 'https://marsapi.ams.usda.gov/services/v1.2'

# =============================================================================
# REPORT CATALOG — All AMS slug IDs organized by category
# =============================================================================

REPORT_CATALOG: Dict[str, Dict] = {
    # --- Grain (daily, structured via allSections=true) ---
    '3192': {'name': 'IL Grain Processor/Elevator', 'category': 'grain'},
    '3225': {'name': 'NE Grain Country Elevators', 'category': 'grain'},
    '2932': {'name': 'MO Daily Grain Bids', 'category': 'grain'},
    '3046': {'name': 'MN Grain Minneapolis', 'category': 'grain'},
    '3148': {'name': 'PNW/MT Grain', 'category': 'grain'},
    '2771': {'name': 'MT/ND Durum/Barley', 'category': 'grain'},
    '3511': {'name': 'Soybean Meal/Feedstuffs', 'category': 'grain'},

    # --- Grain (added 2026-05-02 for basis-field state coverage) ---
    '2850': {'name': 'IA Daily Cash Grain Bids', 'category': 'grain'},
    '2851': {'name': 'OH Daily Grain Bids', 'category': 'grain'},
    '2886': {'name': 'KS Daily Grain Bids', 'category': 'grain'},
    '2912': {'name': 'CO/NE/WY Elevator Grain Bids', 'category': 'grain'},
    '2928': {'name': 'MS Daily Grain Bids', 'category': 'grain'},
    '2960': {'name': 'AR Daily Grain Bids', 'category': 'grain'},
    '3049': {'name': 'Southern MN Daily Grain Bids', 'category': 'grain'},
    '3088': {'name': 'TN Daily Grain Bids', 'category': 'grain'},
    '3100': {'name': 'OK Daily Grain Bids', 'category': 'grain'},
    '3186': {'name': 'East River SD Grain Market', 'category': 'grain'},
    '3463': {'name': 'IN Daily Grain Bids', 'category': 'grain'},
    '2711': {'name': 'TX High Plains Elevator Grain Bids', 'category': 'grain'},

    # --- Specialty Grain ---
    '2887': {'name': 'ND Sunflower/Specialty', 'category': 'specialty_grain'},

    # --- Ethanol & DDGs (weekly) ---
    '3616': {'name': 'Ethanol & DDGs Report', 'category': 'ethanol_ddgs'},

    # --- Livestock (MARS API) ---
    '2810': {'name': 'National Feeder Pig Report', 'category': 'livestock'},
    '1281': {'name': 'OKC Feeder Cattle Auction', 'category': 'livestock'},
    '3237': {'name': 'WY-NE Direct Cattle Report', 'category': 'livestock'},

    # --- Farm Inputs (bi-weekly) ---
    '3195': {'name': 'Farm Inputs & Fuel', 'category': 'farm_inputs'},

    # NOTE: These reports are NOT in MARS API — they use USDA LMR system:
    #   Hogs (was 2675) -> National Daily Hog Report (LM_HG201) at mpr.datamart.ams.usda.gov
    #   Choice Steers (was 2485) -> 5-Area Cattle (LM_CT169) at mpr.datamart.ams.usda.gov
    #   Cotton (was 3024) -> 3804 exists but has empty detail sections
}

# Categories that route to silver.specialty_price instead of silver.cash_price
SPECIALTY_CATEGORIES = {'livestock', 'ethanol_ddgs', 'cotton', 'farm_inputs', 'specialty_grain'}

# Comprehensive price field mapping — USDA MARS uses MANY naming conventions
# including space-separated names (e.g., "price Min"), underscored, and camelCase.
PRICE_FIELDS = {
    'price': [
        'avg_price', 'wtd_avg_price', 'price_avg', 'price',
        'current_price', 'weighted_avg', 'wtd_avg', 'average',
    ],
    'price_low': [
        'price Min', 'price_Min', 'price_min', 'avg_price_min',
        'price_low', 'low_price', 'low', 'min_price',
    ],
    'price_high': [
        'price Max', 'price_Max', 'price_max', 'avg_price_max',
        'price_high', 'high_price', 'high', 'max_price',
    ],
    'price_avg': [
        'avg_price', 'wtd_avg_price', 'price_avg', 'average_price',
        'weighted_avg', 'wtd_avg', 'weighted_average',
    ],
    'price_year_ago': [
        'avg_price_year_ago', 'wtd_avg_price_yr_ago', 'price_year_ago',
    ],
}

BASIS_FIELDS = {
    'basis': ['basis Min', 'basis_min', 'basis', 'basis_level', 'cash_basis'],
    'basis_low': ['basis Min', 'basis_min', 'basis_low', 'low_basis'],
    'basis_high': ['basis Max', 'basis_max', 'basis_high', 'high_basis'],
    'basis_change': [
        'basis Min Change', 'basis_min_change', 'basis_change', 'change',
    ],
}

# Sections to skip (contain metadata, not price data)
SKIP_SECTIONS = {'Report Header', 'Report Receipts'}


class AMSCashPriceCollector(BaseCollector):
    """
    Unified collector for USDA AMS structured cash prices.

    Fetches data from the MARS API, saves individual price records to
    bronze.ams_price_record, and transforms grains to silver.cash_price
    and everything else to silver.specialty_price.
    """

    def __init__(self, slug_ids: List[str] = None, **kwargs):
        api_key = os.environ.get('USDA_AMS_API_KEY', '')
        config = CollectorConfig(
            source_name='USDA_AMS_CashPrices',
            source_url=MARS_BASE_URL,
            # Use NONE — MARS requires HTTP Basic Auth, not Bearer token.
            # We pass auth explicitly to session.get() calls.
            auth_type=AuthType.NONE,
            frequency=DataFrequency.DAILY,
            rate_limit_per_minute=30,
            timeout=45,
            commodities=['corn', 'soybeans', 'wheat', 'sorghum', 'barley',
                         'oats', 'ethanol', 'ddgs', 'hogs', 'cattle',
                         'cotton', 'sunflower', 'diesel', 'fertilizer'],
        )
        super().__init__(config)
        self.slug_ids = slug_ids or list(REPORT_CATALOG.keys())
        self.auth = HTTPBasicAuth(api_key, '')

    # =========================================================================
    # BaseCollector abstract method implementations
    # =========================================================================

    def get_table_name(self) -> str:
        return 'bronze.ams_price_record'

    def parse_response(self, response_data: Any) -> Any:
        """Parse MARS API response into list of records."""
        return self._parse_mars_response(response_data, '', '')

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch price data from all configured AMS reports.

        Args:
            start_date: Start of date range (default: 10 days ago)
            end_date: End of date range (default: today)

        Returns:
            CollectorResult with total records fetched
        """
        end_date = end_date or date.today()
        start_date = start_date or (end_date - timedelta(days=10))

        date_range = (
            f"{start_date.strftime('%m/%d/%Y')}:{end_date.strftime('%m/%d/%Y')}"
        )

        total_records = 0
        all_records = []
        errors = []

        for slug_id in self.slug_ids:
            info = REPORT_CATALOG.get(slug_id, {'name': slug_id, 'category': 'unknown'})
            self.logger.info(f"Fetching {slug_id} ({info['name']})...")

            try:
                records = self._fetch_report(slug_id, date_range)
                if records:
                    all_records.extend(records)
                    total_records += len(records)
                    self.logger.info(f"  -> {len(records)} records from {slug_id}")
                else:
                    self.logger.info(f"  -> 0 records from {slug_id}")
            except Exception as e:
                self.logger.error(f"  -> Error fetching {slug_id}: {e}")
                errors.append(f"{slug_id}: {e}")

        return CollectorResult(
            success=len(errors) == 0 or total_records > 0,
            source=self.config.source_name,
            records_fetched=total_records,
            data=all_records,
            period_start=str(start_date),
            period_end=str(end_date),
            error_message='; '.join(errors) if errors else None,
            warnings=errors,
        )

    # =========================================================================
    # MARS API methods
    # =========================================================================

    def _fetch_report(self, slug_id: str, date_range: str) -> List[Dict]:
        """Fetch a single report from MARS API and parse into records.

        Uses allSections=true to get structured Report Detail data
        (without it, MARS only returns Report Header narratives).
        """
        url = f"{MARS_BASE_URL}/reports/{slug_id}"
        params = {
            'q': f'report_date={date_range}',
            'allSections': 'true',
        }

        self._respect_rate_limit()

        try:
            resp = self.session.get(
                url, params=params, auth=self.auth,
                timeout=self.config.timeout
            )
        except Exception as e:
            self.logger.error(f"HTTP error for slug {slug_id}: {e}")
            return []

        if resp.status_code != 200:
            self.logger.warning(f"HTTP {resp.status_code} for slug {slug_id}")
            return []

        try:
            data = resp.json()
        except Exception:
            self.logger.error(f"Invalid JSON from slug {slug_id}")
            return []

        return self._parse_mars_response(data, slug_id, REPORT_CATALOG.get(slug_id, {}).get('category', 'unknown'))

    def _parse_mars_response(self, data: Any, slug_id: str, category: str) -> List[Dict]:
        """Parse MARS API response (list of sections) into flat records.

        With allSections=true, MARS returns a list of section dicts.
        We skip Report Header (narrative-only) and process Report Detail
        and other sections that contain structured price data.
        """
        records = []

        if isinstance(data, list):
            for section in data:
                if not isinstance(section, dict):
                    continue
                section_name = section.get('reportSection', '')
                # Skip header/receipts sections — they contain narrative, not price data
                if section_name in SKIP_SECTIONS:
                    continue
                section_results = section.get('results', [])
                for item in section_results:
                    record = self._parse_structured_record(item, slug_id, section_name)
                    if record:
                        records.append(record)

        elif isinstance(data, dict):
            section_name = data.get('reportSection', '')
            if section_name not in SKIP_SECTIONS:
                results = data.get('results', [])
                for item in results:
                    record = self._parse_structured_record(item, slug_id, section_name)
                    if record:
                        records.append(record)

        return records

    def _parse_structured_record(self, item: Dict, slug_id: str, section_name: str) -> Optional[Dict]:
        """
        Parse a single API result item into a standardized record for
        bronze.ams_price_record.

        Adapted from usda_ams_collector_asynch.py field mapping logic.
        """
        try:
            record = {
                'slug_id': slug_id,
                'report_section': section_name,
            }

            # Report date
            report_date = (
                item.get('report_date') or
                item.get('published_date') or
                item.get('date') or
                item.get('report_begin_date') or
                item.get('report_end_date') or
                ''
            )
            record['report_date'] = self._parse_date_str(report_date)
            if not record['report_date']:
                return None

            # Commodity — MARS grain reports use 'commodity' (Corn, Soybeans, Wheat)
            # while farm inputs use 'commod', feedstuffs use 'commodity'
            commodity = (
                item.get('commodity') or
                item.get('commod') or
                item.get('commodity_name') or
                item.get('item') or
                item.get('product') or
                ''
            )
            record['commodity'] = str(commodity).strip()

            # Location — MARS grain reports use 'trade_loc' (e.g., "Central",
            # "Mississippi River", "Kansas City"), farm inputs use
            # 'geographical_location', livestock uses 'market_location_name'
            location = (
                item.get('trade_loc') or
                item.get('region') or
                item.get('location') or
                item.get('geographical_location') or
                item.get('market_location_name') or
                item.get('market_location_city') or
                item.get('location_City') or
                item.get('office_city') or
                ''
            )
            record['location'] = str(location).strip()

            # Grade — grain reports use 'grade' (e.g., "US #1") and 'class'
            # (e.g., "Dark Northern Spring", "Hard Red Winter").
            # Livestock reports use 'class' (Steers/Heifers/Bulls) plus
            # 'muscle_grade' (1, 1-2, 2) which differentiates price tiers.
            grade = item.get('grade') or ''
            wheat_class = item.get('class') or ''
            muscle_grade = item.get('muscle_grade') or ''
            if wheat_class and grade:
                record['grade'] = f"{grade} {wheat_class}".strip()
            elif wheat_class and muscle_grade:
                # Livestock: "Steers 1", "Heifers 1-2", etc.
                record['grade'] = f"{wheat_class} {muscle_grade}".strip()
            else:
                record['grade'] = str(grade or wheat_class).strip()

            # Delivery / weight break
            # For livestock auctions the API returns weight_break_low/high
            # (e.g., 750-800) which identifies the price category.  Store it
            # in delivery_period so the upsert key differentiates weight classes.
            wt_break_lo = item.get('weight_break_low')
            wt_break_hi = item.get('weight_break_high')
            if wt_break_lo is not None and wt_break_hi is not None:
                record['delivery_period'] = f"{int(wt_break_lo)}-{int(wt_break_hi)}"
            else:
                record['delivery_period'] = str(
                    item.get('delivery_period') or item.get('delivery_start') or
                    item.get('del_period') or item.get('current') or ''
                ).strip()
            record['delivery_point'] = str(
                item.get('delivery_point') or item.get('shipping_point') or
                item.get('delivery_location') or ''
            ).strip()
            record['transaction_type'] = str(
                item.get('sale Type') or item.get('sale_type') or
                item.get('transaction_type') or item.get('purchase_type') or ''
            ).strip()
            # Livestock lot description (Unweaned, Thin Fleshed, Fancy, etc.)
            lot_desc = item.get('lot_desc')
            # lot_desc comes as None from API when not applicable — normalise to ''
            if lot_desc is None or str(lot_desc).strip().lower() == 'none':
                lot_desc = ''
            record['product_type'] = str(
                item.get('product_type') or item.get('variety') or
                item.get('material_type') or lot_desc or ''
            ).strip()

            # Price fields
            for target_field, source_fields in PRICE_FIELDS.items():
                for src in source_fields:
                    if src in item and item[src] is not None and item[src] != '':
                        parsed = self._parse_price(item[src])
                        if parsed is not None:
                            record[target_field] = parsed
                            break

            # Basis fields
            for target_field, source_fields in BASIS_FIELDS.items():
                for src in source_fields:
                    if src in item and item[src] is not None and item[src] != '':
                        parsed = self._parse_price(item[src])
                        if parsed is not None:
                            record[target_field] = parsed
                            break

            # Unit — MARS uses 'price_unit' (e.g., "$ Per Bushel", "Per Cwt")
            unit = (
                item.get('price_unit') or
                item.get('unit') or
                item.get('unit_of_measure') or
                item.get('commodity_unit') or
                item.get('basis_unit') or
                ''
            )
            record['unit'] = str(unit).strip()

            # Volume / weight (livestock)
            volume = item.get('head_count') or item.get('volume') or item.get('quantity') or item.get('qty')
            if volume is not None:
                record['volume'] = self._parse_price(volume)

            weight_avg = item.get('avg_weight') or item.get('wtd_avg_wt') or item.get('weight_avg') or item.get('average_weight')
            # Prefer weight_break (category range) over avg_weight (actual avg)
            # so weight_low/high reflect the standard reporting bracket.
            weight_low = item.get('weight_break_low') or item.get('avg_weight_min') or item.get('weight_low') or item.get('min_weight')
            weight_high = item.get('weight_break_high') or item.get('avg_weight_max') or item.get('weight_high') or item.get('max_weight')
            if weight_avg:
                record['weight_avg'] = self._parse_price(weight_avg)
            if weight_low:
                record['weight_low'] = self._parse_price(weight_low)
            if weight_high:
                record['weight_high'] = self._parse_price(weight_high)

            # Raw record for debugging
            record['raw_record'] = json.dumps(item)

            # Skip records with no price data at all
            has_price = any(
                record.get(f) is not None
                for f in ['price', 'price_low', 'price_high', 'price_avg', 'price_mostly']
            )
            if not has_price:
                return None

            return record

        except Exception as e:
            self.logger.error(f"Error parsing record: {e}")
            return None

    @staticmethod
    def _parse_price(price_value: Any) -> Optional[float]:
        """Parse price value from various formats."""
        if price_value is None or price_value == '':
            return None
        try:
            if isinstance(price_value, str):
                price_value = price_value.replace('$', '').replace(',', '').strip()
            return float(price_value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _parse_date_str(date_str: str) -> Optional[str]:
        """Parse date string into YYYY-MM-DD format."""
        if not date_str:
            return None
        for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%Y-%m-%dT%H:%M:%S'):
            try:
                return datetime.strptime(str(date_str).strip(), fmt).strftime('%Y-%m-%d')
            except ValueError:
                continue
        return None

    # =========================================================================
    # Database methods
    # =========================================================================

    def get_connection(self):
        """Get database connection (uses RLC_PG_* env vars, falls back to DB_*)."""
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 required for database operations")
        return psycopg2.connect(
            host=os.environ.get('RLC_PG_HOST', os.environ.get('DB_HOST', 'localhost')),
            port=os.environ.get('RLC_PG_PORT', os.environ.get('DB_PORT', 5432)),
            database=os.environ.get('RLC_PG_DATABASE', os.environ.get('DB_NAME', 'rlc_commodities')),
            user=os.environ.get('RLC_PG_USER', os.environ.get('DB_USER', 'postgres')),
            password=os.environ.get('RLC_PG_PASSWORD', os.environ.get('DB_PASSWORD')),
            sslmode=os.environ.get('RLC_PG_SSLMODE', 'prefer'),
        )

    def save_to_bronze(self, records: List[Dict], conn=None) -> int:
        """
        Save parsed records to bronze.ams_price_record with upsert.

        Args:
            records: List of parsed record dicts
            conn: Database connection (optional, creates one if not provided)

        Returns:
            Number of records upserted
        """
        close_conn = False
        if conn is None:
            conn = self.get_connection()
            close_conn = True

        cur = conn.cursor()
        count = 0

        for rec in records:
            try:
                cur.execute("""
                    INSERT INTO bronze.ams_price_record (
                        slug_id, report_date, report_section,
                        commodity, location, grade,
                        delivery_period, delivery_point,
                        transaction_type, product_type,
                        price, price_low, price_high, price_avg, price_mostly,
                        basis, basis_low, basis_high, basis_change,
                        volume, weight_avg, weight_low, weight_high,
                        unit, raw_record, collected_at
                    ) VALUES (
                        %(slug_id)s, %(report_date)s, %(report_section)s,
                        %(commodity)s, %(location)s, %(grade)s,
                        %(delivery_period)s, %(delivery_point)s,
                        %(transaction_type)s, %(product_type)s,
                        %(price)s, %(price_low)s, %(price_high)s, %(price_avg)s, %(price_mostly)s,
                        %(basis)s, %(basis_low)s, %(basis_high)s, %(basis_change)s,
                        %(volume)s, %(weight_avg)s, %(weight_low)s, %(weight_high)s,
                        %(unit)s, %(raw_record)s, NOW()
                    )
                    ON CONFLICT (
                        slug_id, report_date,
                        COALESCE(report_section, ''),
                        COALESCE(commodity, ''),
                        COALESCE(location, ''),
                        COALESCE(grade, ''),
                        COALESCE(delivery_period, '')
                    )
                    DO UPDATE SET
                        price = EXCLUDED.price,
                        price_low = EXCLUDED.price_low,
                        price_high = EXCLUDED.price_high,
                        price_avg = EXCLUDED.price_avg,
                        price_mostly = EXCLUDED.price_mostly,
                        basis = EXCLUDED.basis,
                        basis_low = EXCLUDED.basis_low,
                        basis_high = EXCLUDED.basis_high,
                        basis_change = EXCLUDED.basis_change,
                        volume = EXCLUDED.volume,
                        weight_avg = EXCLUDED.weight_avg,
                        weight_low = EXCLUDED.weight_low,
                        weight_high = EXCLUDED.weight_high,
                        unit = EXCLUDED.unit,
                        raw_record = EXCLUDED.raw_record,
                        collected_at = NOW()
                """, {
                    'slug_id': rec.get('slug_id'),
                    'report_date': rec.get('report_date'),
                    'report_section': rec.get('report_section') or None,
                    'commodity': rec.get('commodity') or None,
                    'location': rec.get('location') or None,
                    'grade': rec.get('grade') or None,
                    'delivery_period': rec.get('delivery_period') or None,
                    'delivery_point': rec.get('delivery_point') or None,
                    'transaction_type': rec.get('transaction_type') or None,
                    'product_type': rec.get('product_type') or None,
                    'price': rec.get('price'),
                    'price_low': rec.get('price_low'),
                    'price_high': rec.get('price_high'),
                    'price_avg': rec.get('price_avg'),
                    'price_mostly': rec.get('price_mostly'),
                    'basis': rec.get('basis'),
                    'basis_low': rec.get('basis_low'),
                    'basis_high': rec.get('basis_high'),
                    'basis_change': rec.get('basis_change'),
                    'volume': rec.get('volume'),
                    'weight_avg': rec.get('weight_avg'),
                    'weight_low': rec.get('weight_low'),
                    'weight_high': rec.get('weight_high'),
                    'unit': rec.get('unit') or None,
                    'raw_record': rec.get('raw_record'),
                })
                count += 1
            except Exception as e:
                self.logger.warning(f"Error upserting record: {e}")
                conn.rollback()
                continue

        conn.commit()
        self.logger.info(f"Upserted {count} records to bronze.ams_price_record")

        if close_conn:
            conn.close()

        return count

    def transform_to_silver(self, records: List[Dict], conn=None) -> Dict[str, int]:
        """
        Route bronze records to appropriate silver tables.

        Grain prices -> silver.cash_price
        Everything else -> silver.specialty_price

        Args:
            records: List of parsed record dicts
            conn: Database connection

        Returns:
            Dict with counts {'cash_price': N, 'specialty_price': M}
        """
        close_conn = False
        if conn is None:
            conn = self.get_connection()
            close_conn = True

        cur = conn.cursor()
        counts = {'cash_price': 0, 'specialty_price': 0}

        for rec in records:
            slug_id = rec.get('slug_id', '')
            info = REPORT_CATALOG.get(slug_id, {})
            category = info.get('category', 'unknown')

            try:
                if category == 'grain':
                    self._save_grain_to_silver(rec, cur)
                    counts['cash_price'] += 1
                elif category in SPECIALTY_CATEGORIES:
                    self._save_specialty_to_silver(rec, category, cur)
                    counts['specialty_price'] += 1
            except Exception as e:
                self.logger.warning(f"Error saving to silver: {e}")
                conn.rollback()
                continue

        conn.commit()
        self.logger.info(
            f"Silver: {counts['cash_price']} grain, {counts['specialty_price']} specialty"
        )

        if close_conn:
            conn.close()

        return counts

    def _save_grain_to_silver(self, rec: Dict, cur):
        """Insert/update a grain record in silver.cash_price."""
        commodity = self._normalize_grain_commodity(
            rec.get('commodity', ''),
            rec.get('grade', ''),
        )
        location_state = self._extract_state(rec.get('location', ''))
        price = (
            rec.get('price_avg') or rec.get('price') or
            rec.get('price_mostly') or
            self._midpoint(rec.get('price_low'), rec.get('price_high'))
        )

        cur.execute("""
            INSERT INTO silver.cash_price (
                report_date, commodity, location_name, location_state,
                price_cash, price_low, price_high, basis,
                unit, source, slug_id, parsed_at
            ) VALUES (
                %(report_date)s, %(commodity)s, %(location)s, %(state)s,
                %(price)s, %(price_low)s, %(price_high)s, %(basis)s,
                %(unit)s, 'USDA_AMS', %(slug_id)s, NOW()
            )
            ON CONFLICT (report_date, commodity, location_state, slug_id)
            DO UPDATE SET
                price_cash = EXCLUDED.price_cash,
                price_low = EXCLUDED.price_low,
                price_high = EXCLUDED.price_high,
                basis = EXCLUDED.basis,
                parsed_at = NOW()
        """, {
            'report_date': rec['report_date'],
            'commodity': commodity,
            'location': rec.get('location', ''),
            'state': location_state,
            'price': price,
            'price_low': rec.get('price_low'),
            'price_high': rec.get('price_high'),
            'basis': rec.get('basis'),
            'unit': rec.get('unit') or '$/bu',
            'slug_id': rec['slug_id'],
        })

    def _save_specialty_to_silver(self, rec: Dict, category: str, cur):
        """Insert/update a specialty record in silver.specialty_price."""
        price_avg = (
            rec.get('price_avg') or rec.get('price') or
            self._midpoint(rec.get('price_low'), rec.get('price_high'))
        )

        cur.execute("""
            INSERT INTO silver.specialty_price (
                report_date, category, commodity,
                price_low, price_high, price_avg,
                location, unit, source, slug_id, parsed_at
            ) VALUES (
                %(report_date)s, %(category)s, %(commodity)s,
                %(price_low)s, %(price_high)s, %(price_avg)s,
                %(location)s, %(unit)s, 'USDA_AMS', %(slug_id)s, NOW()
            )
            ON CONFLICT (report_date, category, commodity, location)
            DO UPDATE SET
                price_low = EXCLUDED.price_low,
                price_high = EXCLUDED.price_high,
                price_avg = EXCLUDED.price_avg,
                parsed_at = NOW()
        """, {
            'report_date': rec['report_date'],
            'category': category,
            'commodity': rec.get('commodity', ''),
            'price_low': rec.get('price_low'),
            'price_high': rec.get('price_high'),
            'price_avg': price_avg,
            'location': rec.get('location', ''),
            'unit': rec.get('unit', ''),
            'slug_id': rec['slug_id'],
        })

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    def _normalize_grain_commodity(commodity_str: str, grade_str: str = '') -> str:
        """Normalize grain commodity name to standard form.

        Uses both commodity and grade/class fields since MARS reports
        wheat as commodity='Wheat' with class in the grade field.
        """
        c = commodity_str.lower().strip()
        g = grade_str.lower().strip()
        if 'corn' in c:
            return 'corn'
        if 'soybean meal' in c or 'soymeal' in c:
            return 'soybean_meal'
        if 'soybean oil' in c:
            return 'soybean_oil'
        if 'soybean' in c or 'soy' in c:
            return 'soybeans'
        cg = c + ' ' + g  # Combined for wheat class detection
        if 'hard red winter' in cg or 'hrw' in cg:
            return 'wheat_hrw'
        if 'soft red winter' in cg or 'srw' in cg:
            return 'wheat_srw'
        if 'soft white' in cg or 'sww' in cg:
            return 'wheat_sww'
        if 'durum' in cg or 'had' in cg:
            return 'wheat_durum'
        if 'dark northern spring' in cg or 'dns' in cg or 'northern spring' in cg:
            return 'wheat_hrs'
        if 'wheat' in c:
            return 'wheat'
        if 'sorghum' in c or 'milo' in c:
            return 'sorghum'
        if 'barley' in c:
            return 'barley'
        if 'oat' in c:
            return 'oats'
        return c

    @staticmethod
    def _extract_state(location: str) -> str:
        """Extract 2-letter state code from location string."""
        state_map = {
            'illinois': 'IL', 'nebraska': 'NE', 'iowa': 'IA', 'minnesota': 'MN',
            'missouri': 'MO', 'kansas': 'KS', 'montana': 'MT', 'north dakota': 'ND',
            'oregon': 'OR', 'texas': 'TX', 'oklahoma': 'OK', 'indiana': 'IN',
            'ohio': 'OH', 'south dakota': 'SD', 'wisconsin': 'WI',
        }
        loc_lower = location.lower()
        for name, code in state_map.items():
            if name in loc_lower:
                return code
        # Try to find 2-letter state abbreviation
        import re
        m = re.search(r'\b([A-Z]{2})\b', location)
        if m:
            return m.group(1)
        return ''

    @staticmethod
    def _midpoint(low, high):
        """Calculate midpoint of low/high range."""
        if low is not None and high is not None:
            return round((low + high) / 2, 4)
        return low or high

    # =========================================================================
    # Main collection workflow (collect + save to DB)
    # =========================================================================

    def collect_and_save(
        self,
        start_date: date = None,
        end_date: date = None,
    ) -> Dict[str, Any]:
        """
        Full workflow: fetch from API, save to bronze, transform to silver.

        Returns:
            Summary dict with record counts
        """
        result = self.fetch_data(start_date, end_date)

        if not result.success or not result.data:
            self.logger.error(f"Fetch failed: {result.error_message}")
            return {
                'success': False,
                'error': result.error_message,
                'records_fetched': 0,
            }

        records = result.data
        self.logger.info(f"Fetched {len(records)} total records from API")

        conn = self.get_connection()
        try:
            bronze_count = self.save_to_bronze(records, conn)
            silver_counts = self.transform_to_silver(records, conn)

            summary = {
                'success': True,
                'records_fetched': len(records),
                'bronze_saved': bronze_count,
                'silver_cash_price': silver_counts['cash_price'],
                'silver_specialty_price': silver_counts['specialty_price'],
                'period': f"{result.period_start} to {result.period_end}",
                'warnings': result.warnings,
            }
            self.logger.info(f"Collection complete: {json.dumps(summary, indent=2)}")
            return summary

        except Exception as e:
            self.logger.error(f"Database error: {e}")
            conn.rollback()
            return {'success': False, 'error': str(e), 'records_fetched': len(records)}
        finally:
            conn.close()


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='USDA AMS Cash Price Collector')
    parser.add_argument('--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', help='End date (YYYY-MM-DD)')
    parser.add_argument('--slug', help='Collect a single slug ID only')
    parser.add_argument('--dry-run', action='store_true', help='Fetch only, do not save to DB')
    parser.add_argument('--verbose', '-v', action='store_true')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
    )

    start = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else None
    end = datetime.strptime(args.end, '%Y-%m-%d').date() if args.end else None

    slug_ids = [args.slug] if args.slug else None
    collector = AMSCashPriceCollector(slug_ids=slug_ids)

    if args.dry_run:
        result = collector.fetch_data(start, end)
        print(f"\nFetched {result.records_fetched} records")
        if result.data:
            for rec in result.data[:5]:
                print(json.dumps({k: v for k, v in rec.items() if k != 'raw_record'}, indent=2))
            if len(result.data) > 5:
                print(f"... and {len(result.data) - 5} more records")
        if result.warnings:
            print(f"\nWarnings: {result.warnings}")
    else:
        summary = collector.collect_and_save(start, end)
        print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
