"""
USDA NASS Processing Reports Collector

Collects monthly processing/crushing data from NASS:
- Fats and Oils (soybean/vegetable oil production and stocks)
- Grain Crushings and Co-Products (corn for ethanol, dry milling, sorghum)
- Flour Milling Products (wheat flour production by class)
- Peanut Stocks and Processing (peanuts milled, crushed, stocks, usage)
- Soy Crush (soybeans crushed, meal production, oil production/stocks)

These reports provide MONTHLY realized data for S&D balance sheet tracking.

Release Schedule:
- Fats and Oils: ~3rd business day of month (covers 2 months prior)
- Grain Crushings: ~1st business day of month
- Flour Milling: ~1st business day of month
- Peanut Stocks: ~25th of month

NASS API Docs: https://quickstats.nass.usda.gov/api

Data Pipeline:
  NASS API -> bronze.nass_processing (raw) -> silver.monthly_realized (standardized)
                                           -> gold.nass_crush_mapped (spreadsheet-ready)
"""

import os
import sys
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import pandas as pd
    import requests
    from dotenv import load_dotenv
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

logger = logging.getLogger(__name__)

# Load environment
load_dotenv(PROJECT_ROOT / '.env')


# =============================================================================
# NASS SURVEY CONFIGURATIONS
# =============================================================================

# Fats and Oils commodity mappings
# NASS uses commodity_desc='OIL' with class_desc for specific oil types
FATS_OILS_COMMODITIES = {
    'soybean_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'SOYBEAN',
    },
    'cottonseed_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'COTTONSEED',
    },
    'corn_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'CORN',
    },
    'canola_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'CANOLA',
    },
    'sunflower_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'SUNFLOWER',
    },
    'peanut_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'PEANUT',
    },
    'palm_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'PALM',
    },
    'palm_kernel_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'PALM KERNEL',
    },
    'coconut_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'COCONUT',
    },
    'safflower_oil': {
        'commodity_desc': 'OIL',
        'class_desc': 'SAFFLOWER',
    },
}

# Animal fats / greases / rendered meals — the "Low CI" biofuel feedstocks.
# Part of the NASS Fats & Oils report (source_desc='SURVEY'), keyed by
# commodity_desc with no class filter. NASS returns every short_desc variant
# (edible/inedible/technical, choice-white/yellow, feather/meat&bone, etc.)
# in one query. These land in silver.monthly_realized under source
# 'NASS_FATS_OILS' with commodity=commodity_desc.lower().replace(' ','_') and
# attribute=the raw NASS short_desc — matching the one-time
# NASS_FATS_OILS_BACKFILL forms so gold.nass_low_ci_matrix picks them up with
# no view change. See _map_commodity / _map_attribute for the mapping.
ANIMAL_FATS_COMMODITIES = {
    'grease': {'commodity_desc': 'GREASE'},
    'lard': {'commodity_desc': 'LARD'},
    'tallow': {'commodity_desc': 'TALLOW'},
    'poultry_fats': {'commodity_desc': 'POULTRY FATS'},
    'poultry_by-product_meals': {'commodity_desc': 'POULTRY BY-PRODUCT MEALS'},
    'meal': {'commodity_desc': 'MEAL'},  # feather meal + meat & bone meal
}

# Crush, cake/meal, millfeed data — uses different commodity_desc than oil
# Part of the NASS Fats & Oils report but keyed by seed commodity
CRUSH_MEAL_COMMODITIES = {
    # Soybeans
    'soybeans_crushed': {
        'commodity_desc': 'SOYBEANS',
        'class_desc': 'ALL CLASSES',
        'statisticcat_desc': 'CRUSHED',
    },
    'soybean_meal_total': {
        'commodity_desc': 'CAKE & MEAL',
        'class_desc': 'SOYBEAN',
        'statisticcat_desc': 'PRODUCTION',
    },
    'soybean_meal_stocks': {
        'commodity_desc': 'CAKE & MEAL',
        'class_desc': 'SOYBEAN',
        'statisticcat_desc': 'STOCKS',
    },
    'soybean_millfeed_production': {
        'commodity_desc': 'MILLFEED',
        'class_desc': 'SOYBEAN',
        'statisticcat_desc': 'PRODUCTION',
    },
    'soybean_millfeed_stocks': {
        'commodity_desc': 'MILLFEED',
        'class_desc': 'SOYBEAN',
        'statisticcat_desc': 'STOCKS',
    },
    # Canola
    'canola_crushed': {
        'commodity_desc': 'CANOLA',
        'class_desc': 'ALL CLASSES',
        'statisticcat_desc': 'CRUSHED',
    },
    'canola_meal_production': {
        'commodity_desc': 'CAKE & MEAL',
        'class_desc': 'CANOLA',
        'statisticcat_desc': 'PRODUCTION',
    },
    'canola_meal_stocks': {
        'commodity_desc': 'CAKE & MEAL',
        'class_desc': 'CANOLA',
        'statisticcat_desc': 'STOCKS',
    },
    # Cottonseed (commodity_desc=COTTON, class_desc=COTTONSEED for crush)
    'cottonseed_crushed': {
        'commodity_desc': 'COTTON',
        'class_desc': 'COTTONSEED',
        'statisticcat_desc': 'CRUSHED',
    },
    'cottonseed_meal_production': {
        'commodity_desc': 'CAKE & MEAL',
        'class_desc': 'COTTONSEED',
        'statisticcat_desc': 'PRODUCTION',
    },
    'cottonseed_meal_stocks': {
        'commodity_desc': 'CAKE & MEAL',
        'class_desc': 'COTTONSEED',
        'statisticcat_desc': 'STOCKS',
    },
}

# Backward compatibility alias
SOY_CRUSH_COMMODITIES = {k: v for k, v in CRUSH_MEAL_COMMODITIES.items() if k.startswith('soybean')}

# Grain Crushings commodities
# NASS uses statisticcat_desc='USAGE' and the type is in short_desc
GRAIN_CRUSHING_COMMODITIES = {
    'corn_fuel_alcohol': {
        'commodity_desc': 'CORN',
        'statisticcat_desc': 'USAGE',
        'short_desc_filter': 'FUEL ALCOHOL',
    },
    'corn_beverage_alcohol': {
        'commodity_desc': 'CORN',
        'statisticcat_desc': 'USAGE',
        'short_desc_filter': 'BEVERAGE ALCOHOL',
    },
    'corn_industrial_alcohol': {
        'commodity_desc': 'CORN',
        'statisticcat_desc': 'USAGE',
        'short_desc_filter': 'INDUSTRIAL ALCOHOL',
    },
    'sorghum_fuel_alcohol': {
        'commodity_desc': 'SORGHUM',
        'statisticcat_desc': 'USAGE',
        'short_desc_filter': 'FUEL ALCOHOL',
    },
}

# Peanut Stocks and Processing commodities
PEANUT_PROCESSING_COMMODITIES = {
    'peanuts_milled': {
        'commodity_desc': 'PEANUTS',
        'statisticcat_desc': 'MILLED',
    },
    'peanuts_crushed': {
        'commodity_desc': 'PEANUTS',
        'statisticcat_desc': 'CRUSHED',
    },
    'peanuts_stocks': {
        'commodity_desc': 'PEANUTS',
        'statisticcat_desc': 'STOCKS',
    },
    'peanuts_usage': {
        'commodity_desc': 'PEANUTS',
        'statisticcat_desc': 'USAGE',
    },
    'peanuts_production': {
        'commodity_desc': 'PEANUTS',
        'statisticcat_desc': 'PRODUCTION',
    },
    'peanuts_disappearance': {
        'commodity_desc': 'PEANUTS',
        'statisticcat_desc': 'DISAPPEARANCE',
    },
    'peanuts_distribution': {
        'commodity_desc': 'PEANUTS',
        'statisticcat_desc': 'DISTRIBUTION',
    },
}

# Flour Milling commodities
# NASS uses commodity_desc='FLOUR' with class_desc for wheat type
FLOUR_MILLING_COMMODITIES = {
    'wheat_flour_all': {
        'commodity_desc': 'FLOUR',
        'class_desc': 'WHEAT',
    },
    'wheat_flour_excl_durum': {
        'commodity_desc': 'FLOUR',
        'class_desc': 'WHEAT, (EXCL DURUM)',
    },
    'wheat_flour_durum': {
        'commodity_desc': 'FLOUR',
        'class_desc': 'WHEAT, SPRING, DURUM',
    },
    'rye_flour': {
        'commodity_desc': 'FLOUR',
        'class_desc': 'RYE',
    },
}


class NASSProcessingCollector:
    """
    Collector for NASS monthly processing reports.

    Covers:
    - Fats and Oils (vegetable oil production/stocks)
    - Soy Crush (soybeans crushed, meal production, oil production/stocks)
    - Grain Crushings and Co-Products (corn/sorghum processing)
    - Flour Milling Products (wheat flour production)
    - Peanut Stocks and Processing (milled, crushed, stocks, usage)

    Data pipeline: API -> bronze.nass_processing -> silver.monthly_realized
    """

    BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.environ.get('NASS_API_KEY')
        self.logger = logging.getLogger(self.__class__.__name__)

        if not self.api_key:
            self.logger.warning(
                "No NASS API key. Set NASS_API_KEY environment variable. "
                "Register at: https://quickstats.nass.usda.gov/api"
            )

    def collect(self, **kwargs):
        """Dispatcher-compatible entry point. Runs all NASS processing reports
        and returns a CollectorResult-like object."""
        from dataclasses import dataclass, field as dc_field
        from datetime import datetime as dt

        @dataclass
        class _Result:
            success: bool = False
            source: str = 'nass_processing'
            records_fetched: int = 0
            error_message: str = None
            warnings: list = dc_field(default_factory=list)
            collected_at: dt = dc_field(default_factory=dt.now)
            data_as_of: str = None

        result = _Result()
        try:
            year = kwargs.get('year', dt.now().year)
            totals = self.save_to_monthly_realized(report_type='all', year=year)
            errors = totals.pop('errors', [])
            total_bronze = sum(
                v.get('bronze', 0) for v in totals.values() if isinstance(v, dict)
            )
            total_silver = sum(
                v.get('silver', 0) for v in totals.values() if isinstance(v, dict)
            )

            # Success = at least one report type persisted data.
            # Partial failures appear as warnings; full failures set success=False
            # so the dispatcher records status='failed' instead of silently
            # claiming success while the data was rolled back.
            if total_bronze > 0:
                result.success = True
                if errors:
                    result.warnings = [
                        f"{rt}: {msg}" for rt, msg in errors
                    ]
            else:
                result.success = False
                if errors:
                    result.error_message = '; '.join(
                        f"{rt}: {msg}" for rt, msg in errors
                    )
                else:
                    result.error_message = (
                        f"No records persisted for year={year} "
                        f"(API returned nothing for all 5 report types)"
                    )

            result.records_fetched = total_bronze + total_silver
            result.data_as_of = str(year)
            self.logger.info(
                f"Collected {total_bronze} bronze + {total_silver} silver "
                f"across {len(totals)} report types "
                f"({len(errors)} errors)"
            )
        except Exception as e:
            result.error_message = str(e)
            self.logger.error(f"Collection failed: {e}")
        return result

    def _make_request(self, params: Dict) -> Optional[Dict]:
        """Make API request to NASS."""
        params['key'] = self.api_key
        params['format'] = 'JSON'

        try:
            response = requests.get(self.BASE_URL, params=params, timeout=60)

            if response.status_code == 401:
                self.logger.error("Invalid NASS API key")
                return None

            if response.status_code != 200:
                self.logger.error("NASS API error: HTTP {}".format(response.status_code))
                return None

            data = response.json()
            return data

        except requests.exceptions.RequestException as e:
            self.logger.error("Request error: {}".format(e))
            return None
        except Exception as e:
            self.logger.error("Error: {}".format(e))
            return None

    # =========================================================================
    # FATS AND OILS
    # =========================================================================

    def fetch_fats_oils(
        self,
        year: int = None,
        commodities: List[str] = None,
        stat_categories: List[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch Fats and Oils report data.

        Args:
            year: Year to fetch (default: current and prior year)
            commodities: List of oil types (default: all)
            stat_categories: Categories like 'PRODUCTION', 'STOCKS'

        Returns:
            DataFrame with monthly oil production/stocks data
        """
        if not self.api_key:
            return None

        year = year or datetime.now().year
        commodities = commodities or list(FATS_OILS_COMMODITIES.keys())
        stat_categories = stat_categories or ['PRODUCTION', 'STOCKS',
                                               'REMOVAL FOR PROCESSING']

        all_records = []

        for comm_key in commodities:
            if comm_key not in FATS_OILS_COMMODITIES:
                continue

            comm_config = FATS_OILS_COMMODITIES[comm_key]

            for stat_cat in stat_categories:
                params = {
                    'commodity_desc': comm_config['commodity_desc'],
                    'class_desc': comm_config['class_desc'],
                    'statisticcat_desc': stat_cat,
                    'source_desc': 'SURVEY',
                    'freq_desc': 'MONTHLY',
                    'year__GE': str(year - 1),
                    'year__LE': str(year),
                    'agg_level_desc': 'NATIONAL',
                }

                data = self._make_request(params)

                if data and 'data' in data:
                    for item in data['data']:
                        record = self._parse_fats_oils_record(item, comm_key)
                        if record:
                            all_records.append(record)

        if not all_records:
            self.logger.warning("No Fats & Oils data retrieved")
            return None

        df = pd.DataFrame(all_records)
        self.logger.info("Retrieved {} Fats & Oils records".format(len(df)))
        return df

    def _parse_fats_oils_record(self, item: Dict, commodity_key: str) -> Optional[Dict]:
        """Parse a Fats & Oils record."""
        try:
            value_str = str(item.get('Value', '')).replace(',', '')
            if not value_str or value_str == '(D)' or value_str == '(NA)':
                return None

            value = float(value_str)

            # Parse reference period (e.g., "JAN", "FEB", etc.)
            ref_period = item.get('reference_period_desc', '')
            month = self._month_from_period(ref_period)

            return {
                'report_type': 'fats_oils',
                'commodity': commodity_key,
                'commodity_desc': item.get('commodity_desc'),
                'class_desc': item.get('class_desc', ''),
                'year': int(item.get('year', 0)),
                'month': month,
                'reference_period': ref_period,
                'statisticcat': item.get('statisticcat_desc'),
                'short_desc': item.get('short_desc'),
                'domaincat_desc': item.get('domaincat_desc', ''),
                'unit': item.get('unit_desc'),
                'value': value,
                'source': 'NASS_FATS_OILS',
            }
        except Exception as e:
            self.logger.warning("Error parsing record: {}".format(e))
            return None

    # =========================================================================
    # ANIMAL FATS / GREASES / RENDERED MEALS (Low CI feedstocks)
    # =========================================================================

    def fetch_animal_fats(
        self,
        year: int = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch animal-fat / grease / rendered-meal series from the NASS Fats &
        Oils report (GREASE, LARD, TALLOW, POULTRY FATS, POULTRY BY-PRODUCT
        MEALS, MEAL). Queries by commodity_desc with no class/statisticcat
        filter so every short_desc variant (production, stocks, removal for
        processing, incl. edible/inedible/technical splits) comes back in one
        request per commodity.

        Rows are written to silver.monthly_realized under source
        'NASS_FATS_OILS' with commodity=lowercased commodity_desc and
        attribute=the raw NASS short_desc, matching NASS_FATS_OILS_BACKFILL so
        gold.nass_low_ci_matrix refreshes with no view change.

        Args:
            year: Year to fetch (default: current and prior year)

        Returns:
            DataFrame with monthly animal-fat production/stocks/removal data
        """
        if not self.api_key:
            return None

        year = year or datetime.now().year
        all_records = []

        for comm_key, config in ANIMAL_FATS_COMMODITIES.items():
            params = {
                'commodity_desc': config['commodity_desc'],
                'source_desc': 'SURVEY',
                'freq_desc': 'MONTHLY',
                'year__GE': str(year - 1),
                'year__LE': str(year),
                'agg_level_desc': 'NATIONAL',
            }

            data = self._make_request(params)
            if data and 'data' in data:
                for item in data['data']:
                    # Reuse the generic parser; source fixed to NASS_FATS_OILS
                    # so recurring rows merge with the backfill forms.
                    record = self._parse_generic_record(
                        item, 'animal_fats', 'NASS_FATS_OILS'
                    )
                    if record:
                        all_records.append(record)

        if not all_records:
            self.logger.warning("No Animal Fats data retrieved")
            return None

        df = pd.DataFrame(all_records)
        self.logger.info("Retrieved {} Animal Fats records".format(len(df)))
        return df

    # =========================================================================
    # SOY CRUSH (soybeans crushed + soybean meal)
    # =========================================================================

    def fetch_soy_crush(
        self,
        year: int = None,
    ) -> Optional[pd.DataFrame]:
        """
        Fetch all soybean crushing data in one call.

        Combines:
        - Soybeans crushed (commodity_desc=SOYBEANS, statisticcat_desc=CRUSHED)
        - Soybean meal production & stocks (commodity_desc=CAKE & MEAL)
        - Soybean oil production, stocks, refining (commodity_desc=OIL, class_desc=SOYBEAN)

        Args:
            year: Year to fetch (default: current and prior year)

        Returns:
            DataFrame with all soy crush data
        """
        if not self.api_key:
            return None

        year = year or datetime.now().year
        all_records = []

        # 1. All crush/meal commodities (soybeans, canola, cottonseed)
        for comm_key, config in CRUSH_MEAL_COMMODITIES.items():
            params = {
                'commodity_desc': config['commodity_desc'],
                'statisticcat_desc': config['statisticcat_desc'],
                'source_desc': 'SURVEY',
                'freq_desc': 'MONTHLY',
                'year__GE': str(year - 1),
                'year__LE': str(year),
                'agg_level_desc': 'NATIONAL',
            }
            if 'class_desc' in config:
                params['class_desc'] = config['class_desc']

            data = self._make_request(params)
            if data and 'data' in data:
                for item in data['data']:
                    record = self._parse_generic_record(item, 'soy_crush', 'NASS_SOY_CRUSH')
                    if record:
                        all_records.append(record)

        # 2. Soybean oil (production, stocks, removal for processing, usage)
        for stat_cat in ['PRODUCTION', 'STOCKS', 'REMOVAL FOR PROCESSING',
                         'REMOVAL FOR PROCESSING, INEDIBLE USE',
                         'REMOVAL FOR PROCESSING, EDIBLE USE']:
            params = {
                'commodity_desc': 'OIL',
                'class_desc': 'SOYBEAN',
                'statisticcat_desc': stat_cat,
                'source_desc': 'SURVEY',
                'freq_desc': 'MONTHLY',
                'year__GE': str(year - 1),
                'year__LE': str(year),
                'agg_level_desc': 'NATIONAL',
            }

            data = self._make_request(params)
            if data and 'data' in data:
                for item in data['data']:
                    record = self._parse_generic_record(item, 'soy_crush', 'NASS_SOY_CRUSH')
                    if record:
                        all_records.append(record)

        if not all_records:
            self.logger.warning("No Soy Crush data retrieved")
            return None

        df = pd.DataFrame(all_records)
        self.logger.info("Retrieved {} Soy Crush records".format(len(df)))
        return df

    def _parse_generic_record(
        self, item: Dict, report_type: str, source: str
    ) -> Optional[Dict]:
        """Parse any NASS record into a standard format for bronze storage."""
        try:
            value_str = str(item.get('Value', '')).replace(',', '')
            if not value_str or value_str == '(D)' or value_str == '(NA)':
                return None

            value = float(value_str)

            ref_period = item.get('reference_period_desc', '')
            month = self._month_from_period(ref_period)

            # Skip cumulative records
            if 'THRU' in ref_period.upper():
                return None

            return {
                'report_type': report_type,
                'commodity_desc': item.get('commodity_desc', ''),
                'class_desc': item.get('class_desc', ''),
                'statisticcat': item.get('statisticcat_desc', ''),
                'short_desc': item.get('short_desc', ''),
                'domaincat_desc': item.get('domaincat_desc', ''),
                'unit': item.get('unit_desc', ''),
                'year': int(item.get('year', 0)),
                'month': month,
                'reference_period': ref_period,
                'value': value,
                'source': source,
            }
        except Exception as e:
            self.logger.warning("Error parsing record: {}".format(e))
            return None

    # =========================================================================
    # GRAIN CRUSHINGS
    # =========================================================================

    def fetch_grain_crushings(
        self,
        year: int = None,
        include_corn: bool = True,
        include_sorghum: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        Fetch Grain Crushings and Co-Products report data.

        This includes:
        - Corn ground for alcohol (fuel ethanol)
        - Corn for dry milling (grits, meal, flour)
        - Corn for wet milling (sweeteners, starch)
        - Sorghum crushing

        Args:
            year: Year to fetch
            include_corn: Include corn crushing data
            include_sorghum: Include sorghum crushing data

        Returns:
            DataFrame with monthly grain crushing data
        """
        if not self.api_key:
            return None

        year = year or datetime.now().year
        all_records = []

        commodities_to_fetch = []
        if include_corn:
            commodities_to_fetch.extend([
                'corn_fuel_alcohol', 'corn_beverage_alcohol', 'corn_industrial_alcohol'
            ])
        if include_sorghum:
            commodities_to_fetch.append('sorghum_fuel_alcohol')

        for comm_key in commodities_to_fetch:
            if comm_key not in GRAIN_CRUSHING_COMMODITIES:
                continue

            comm_config = GRAIN_CRUSHING_COMMODITIES[comm_key]

            params = {
                'commodity_desc': comm_config['commodity_desc'],
                'statisticcat_desc': comm_config['statisticcat_desc'],
                'source_desc': 'SURVEY',
                'freq_desc': 'MONTHLY',
                'year__GE': str(year - 1),
                'year__LE': str(year),
                'agg_level_desc': 'NATIONAL',
            }

            data = self._make_request(params)

            if data and 'data' in data:
                # Filter by short_desc if specified
                short_desc_filter = comm_config.get('short_desc_filter', '')
                for item in data['data']:
                    if short_desc_filter and short_desc_filter not in item.get('short_desc', ''):
                        continue
                    record = self._parse_grain_crushing_record(item, comm_key)
                    if record:
                        all_records.append(record)

        if not all_records:
            self.logger.warning("No Grain Crushings data retrieved")
            return None

        df = pd.DataFrame(all_records)
        self.logger.info("Retrieved {} Grain Crushings records".format(len(df)))
        return df

    def _parse_grain_crushing_record(self, item: Dict, commodity_key: str) -> Optional[Dict]:
        """Parse a Grain Crushings record."""
        try:
            value_str = str(item.get('Value', '')).replace(',', '')
            if not value_str or value_str == '(D)' or value_str == '(NA)':
                return None

            value = float(value_str)

            ref_period = item.get('reference_period_desc', '')
            month = self._month_from_period(ref_period)

            return {
                'report_type': 'grain_crushings',
                'commodity': commodity_key,
                'commodity_desc': item.get('commodity_desc'),
                'class_desc': item.get('class_desc', ''),
                'year': int(item.get('year', 0)),
                'month': month,
                'reference_period': ref_period,
                'statisticcat': item.get('statisticcat_desc'),
                'short_desc': item.get('short_desc'),
                'domaincat_desc': item.get('domaincat_desc', ''),
                'unit': item.get('unit_desc'),
                'value': value,
                'source': 'NASS_GRAIN_CRUSH',
            }
        except Exception as e:
            self.logger.warning("Error parsing record: {}".format(e))
            return None

    # =========================================================================
    # FLOUR MILLING
    # =========================================================================

    def fetch_flour_milling(
        self,
        year: int = None,
        wheat_classes: List[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch Flour Milling Products report data.

        Args:
            year: Year to fetch
            wheat_classes: List of wheat classes (default: all)

        Returns:
            DataFrame with monthly flour production data
        """
        if not self.api_key:
            return None

        year = year or datetime.now().year
        wheat_classes = wheat_classes or list(FLOUR_MILLING_COMMODITIES.keys())

        all_records = []

        for class_key in wheat_classes:
            if class_key not in FLOUR_MILLING_COMMODITIES:
                continue

            class_config = FLOUR_MILLING_COMMODITIES[class_key]

            params = {
                'commodity_desc': class_config['commodity_desc'],
                'class_desc': class_config['class_desc'],
                'source_desc': 'SURVEY',
                'freq_desc': 'MONTHLY',
                'year__GE': str(year - 1),
                'year__LE': str(year),
                'agg_level_desc': 'NATIONAL',
                'statisticcat_desc': 'PRODUCTION',
            }

            data = self._make_request(params)

            if data and 'data' in data:
                for item in data['data']:
                    record = self._parse_flour_milling_record(item, class_key)
                    if record:
                        all_records.append(record)

        # Additional milling-report series (same report, distinct commodity_desc/statisticcat):
        #   millfeed production (MILLFEED/PRODUCTION, tons) and wheat ground for flour = Food use
        #   (WHEAT/USAGE, bushels). Natural key (commodity_desc, statisticcat, short_desc) keeps
        #   these separate from FLOUR rows.
        for spec in ({'commodity_desc': 'MILLFEED', 'class_desc': 'WHEAT', 'statisticcat_desc': 'PRODUCTION', 'key': 'wheat_millfeed'},
                     {'commodity_desc': 'WHEAT', 'statisticcat_desc': 'USAGE', 'key': 'wheat_ground'}):
            params = {
                'commodity_desc': spec['commodity_desc'],
                'source_desc': 'SURVEY',
                'freq_desc': 'MONTHLY',
                'year__GE': str(year - 1),
                'year__LE': str(year),
                'agg_level_desc': 'NATIONAL',
                'statisticcat_desc': spec['statisticcat_desc'],
            }
            if spec.get('class_desc'):
                params['class_desc'] = spec['class_desc']
            data = self._make_request(params)
            if data and 'data' in data:
                for item in data['data']:
                    record = self._parse_flour_milling_record(item, spec['key'])
                    if record:
                        all_records.append(record)

        if not all_records:
            self.logger.warning("No Flour Milling data retrieved")
            return None

        df = pd.DataFrame(all_records)
        self.logger.info("Retrieved {} Flour Milling records".format(len(df)))
        return df

    def _parse_flour_milling_record(self, item: Dict, class_key: str) -> Optional[Dict]:
        """Parse a Flour Milling record."""
        try:
            value_str = str(item.get('Value', '')).replace(',', '')
            if not value_str or value_str == '(D)' or value_str == '(NA)':
                return None

            value = float(value_str)

            ref_period = item.get('reference_period_desc', '')
            month = self._month_from_period(ref_period)

            return {
                'report_type': 'flour_milling',
                'commodity': class_key,
                'commodity_desc': item.get('commodity_desc'),
                'class_desc': item.get('class_desc', 'ALL CLASSES'),
                'year': int(item.get('year', 0)),
                'month': month,
                'reference_period': ref_period,
                'statisticcat': item.get('statisticcat_desc'),
                'short_desc': item.get('short_desc'),
                'domaincat_desc': item.get('domaincat_desc', ''),
                'unit': item.get('unit_desc'),
                'value': value,
                'source': 'NASS_FLOUR_MILL',
            }
        except Exception as e:
            self.logger.warning("Error parsing record: {}".format(e))
            return None

    # =========================================================================
    # PEANUT STOCKS AND PROCESSING
    # =========================================================================

    def fetch_peanut_processing(
        self,
        year: int = None,
        stat_categories: List[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Fetch Peanut Stocks and Processing report data.

        Args:
            year: Year to fetch (default: current and prior year)
            stat_categories: Categories like 'MILLED', 'CRUSHED', 'STOCKS', 'USAGE'

        Returns:
            DataFrame with monthly peanut processing data
        """
        if not self.api_key:
            return None

        year = year or datetime.now().year
        stat_categories = stat_categories or ['MILLED', 'CRUSHED', 'STOCKS', 'USAGE',
                                               'PRODUCTION', 'DISAPPEARANCE', 'DISTRIBUTION']

        all_records = []

        for stat_cat in stat_categories:
            # NASS reports peanut STOCKS as freq_desc='POINT IN TIME' with
            # reference_period_desc='END OF [MONTH]'. Other peanut series
            # (MILLED/CRUSHED/USAGE etc) are freq_desc='MONTHLY'. Drop the
            # freq filter so both shapes come back; both are valid monthly
            # observations, just different reporting conventions.
            params = {
                'commodity_desc': 'PEANUTS',
                'statisticcat_desc': stat_cat,
                'source_desc': 'SURVEY',
                'year__GE': str(year - 1),
                'year__LE': str(year),
                'agg_level_desc': 'NATIONAL',
            }

            data = self._make_request(params)

            if data and 'data' in data:
                for item in data['data']:
                    # Skip cumulative "AUG THRU [MONTH]" — those are running
                    # totals. Keep "END OF [MONTH]" — those are end-of-month
                    # stocks (which is the point-in-time number we want).
                    ref_period = item.get('reference_period_desc', '').upper()
                    if 'THRU' in ref_period:
                        continue

                    record = self._parse_peanut_record(item, stat_cat)
                    if record:
                        all_records.append(record)

        if not all_records:
            self.logger.warning("No Peanut Processing data retrieved")
            return None

        df = pd.DataFrame(all_records)
        self.logger.info("Retrieved {} Peanut Processing records".format(len(df)))
        return df

    def _parse_peanut_record(self, item: Dict, stat_cat: str) -> Optional[Dict]:
        """Parse a Peanut Stocks and Processing record."""
        try:
            value_str = str(item.get('Value', '')).replace(',', '').strip()
            if not value_str or value_str == '(D)' or value_str == '(NA)':
                return None

            value = float(value_str)

            ref_period = item.get('reference_period_desc', '')
            month = self._month_from_period(ref_period)

            return {
                'report_type': 'peanut_processing',
                'commodity': 'peanuts',
                'commodity_desc': item.get('commodity_desc'),
                'class_desc': item.get('class_desc', ''),
                'year': int(item.get('year', 0)),
                'month': month,
                'reference_period': ref_period,
                'statisticcat': stat_cat,
                'short_desc': item.get('short_desc'),
                'domaincat_desc': item.get('domaincat_desc', ''),
                'unit': item.get('unit_desc'),
                'value': value,
                'source': 'NASS_PEANUT',
            }
        except Exception as e:
            self.logger.warning("Error parsing record: {}".format(e))
            return None

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _month_from_period(self, period: str) -> Optional[int]:
        """Convert NASS period string to month number."""
        period_upper = period.upper().strip()

        # NASS uses 'END OF MAR' / 'END OF MARCH' for point-in-time stocks
        # (peanut stocks report). Strip the prefix so it matches month_map.
        if period_upper.startswith('END OF '):
            period_upper = period_upper[len('END OF '):]

        # Monthly periods
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
            'MAY': 5, 'JUN': 6, 'JUL': 7, 'AUG': 8,
            'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
            'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4,
            'JUNE': 6, 'JULY': 7, 'AUGUST': 8, 'SEPTEMBER': 9,
            'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12
        }

        if period_upper in month_map:
            return month_map[period_upper]

        # Quarterly periods - return end month of quarter
        quarter_map = {
            'JAN THRU MAR': 3, 'APR THRU JUN': 6,
            'JUL THRU SEP': 9, 'OCT THRU DEC': 12,
            'Q1': 3, 'Q2': 6, 'Q3': 9, 'Q4': 12,
            'FIRST QUARTER': 3, 'SECOND QUARTER': 6,
            'THIRD QUARTER': 9, 'FOURTH QUARTER': 12,
        }

        if period_upper in quarter_map:
            return quarter_map[period_upper]

        return None

    # =========================================================================
    # DATABASE METHODS
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

    # -------------------------------------------------------------------------
    # Bronze Layer: Save raw NASS API data
    # -------------------------------------------------------------------------

    def save_to_bronze(
        self,
        df: pd.DataFrame,
        report_type: str,
        conn
    ) -> int:
        """
        Save raw NASS API records to bronze.nass_processing.

        Preserves original API field values without transformation.

        Args:
            df: DataFrame with raw NASS records
            report_type: Report type identifier
            conn: Database connection

        Returns:
            Number of records upserted
        """
        cur = conn.cursor()
        count = 0

        for _, row in df.iterrows():
            try:
                commodity_desc = row.get('commodity_desc', '')
                class_desc = row.get('class_desc', '')
                statisticcat = row.get('statisticcat', '')
                short_desc = row.get('short_desc', '')
                domaincat_desc = row.get('domaincat_desc', '')
                unit = row.get('unit', '')
                year = row.get('year')
                ref_period = row.get('reference_period', '')
                month = row.get('month')
                value = row.get('value')
                source = row.get('source', '')

                if not short_desc or value is None:
                    continue

                cur.execute("""
                    INSERT INTO bronze.nass_processing (
                        commodity_desc, class_desc, statisticcat_desc,
                        short_desc, unit_desc, domaincat_desc,
                        year, reference_period_desc, month,
                        value, report_type, source
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (
                        commodity_desc,
                        COALESCE(class_desc, ''),
                        statisticcat_desc,
                        short_desc,
                        year,
                        COALESCE(month, 0),
                        COALESCE(domaincat_desc, '')
                    ) DO UPDATE SET
                        value = EXCLUDED.value,
                        unit_desc = EXCLUDED.unit_desc,
                        reference_period_desc = EXCLUDED.reference_period_desc,
                        collected_at = NOW()
                """, (
                    commodity_desc, class_desc, statisticcat,
                    short_desc, unit, domaincat_desc,
                    year, ref_period, month,
                    value, report_type, source
                ))
                count += 1

            except Exception as e:
                self.logger.warning("Error inserting bronze row: {}".format(e))
                continue

        self.logger.info("Saved {} records to bronze.nass_processing".format(count))
        return count

    # -------------------------------------------------------------------------
    # Silver Layer: Save standardized data
    # -------------------------------------------------------------------------

    def save_to_monthly_realized(
        self,
        report_type: str = 'all',
        year: int = None,
        conn=None
    ) -> Dict[str, Any]:
        """
        Collect and save processing data to both bronze and silver layers.

        Pipeline: API -> bronze.nass_processing -> silver.monthly_realized

        Commits per report type so a failure in one (e.g., grain_crushings
        API timeout) does not roll back the successful ones (e.g., soy_crush).
        Pre-fix behavior wrapped all five reports in a single transaction and
        a late failure silently discarded earlier writes while the dispatcher
        recorded the run as 'success'.

        Args:
            report_type: 'fats_oils', 'grain_crushings', 'flour_milling',
                        'peanut_processing', 'soy_crush', or 'all'
            year: Year to fetch
            conn: Optional database connection

        Returns:
            Dict with per-report-type results. Each entry has bronze/silver
            counts and optionally an 'error' key. Also includes a top-level
            'errors' list of (report_type, message) pairs.
        """
        year = year or datetime.now().year
        close_conn = False

        if conn is None:
            conn = self.get_connection()
            close_conn = True

        results: Dict[str, Any] = {}
        errors: List[tuple] = []
        session_id = None

        report_specs = [
            ('fats_oils',         self.fetch_fats_oils),
            ('animal_fats',       self.fetch_animal_fats),
            ('soy_crush',         self.fetch_soy_crush),
            ('grain_crushings',   self.fetch_grain_crushings),
            ('flour_milling',     self.fetch_flour_milling),
            ('peanut_processing', self.fetch_peanut_processing),
        ]

        try:
            session_id = self._start_audit_session(conn, report_type, year)
            conn.commit()  # commit audit row so it survives later rollbacks
        except Exception as e:
            self.logger.warning(f"Could not start audit session: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

        for rt_name, fetch_func in report_specs:
            if report_type not in ('all', rt_name):
                continue

            try:
                df = fetch_func(year=year)
            except Exception as e:
                msg = f"fetch failed: {e}"
                self.logger.error(f"[{rt_name}] {msg}")
                errors.append((rt_name, msg))
                results[rt_name] = {'bronze': 0, 'silver': 0, 'error': msg}
                continue

            if df is None or len(df) == 0:
                self.logger.warning(f"[{rt_name}] no records returned by API")
                results[rt_name] = {'bronze': 0, 'silver': 0}
                continue

            try:
                bronze_count = self.save_to_bronze(df, rt_name, conn)
                silver_count = self._save_to_realized(df, rt_name, conn)
                conn.commit()  # per-report commit: isolates failures
                results[rt_name] = {'bronze': bronze_count, 'silver': silver_count}
                self.logger.info(
                    f"[{rt_name}] committed bronze={bronze_count} silver={silver_count}"
                )
            except Exception as e:
                msg = f"save failed: {e}"
                self.logger.error(f"[{rt_name}] {msg}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                errors.append((rt_name, msg))
                results[rt_name] = {'bronze': 0, 'silver': 0, 'error': msg}

        # Recompose oil_stocks from bronze after all reports land. classify() skips oil
        # STOCKS rows (they collide on the silver key); this owns oil_stocks + its
        # crude/once_refined components. Idempotent; never fatal to the collection.
        try:
            from src.agents.collectors.us.oil_stocks_composition import recompute_oil_stocks
            stats = recompute_oil_stocks(conn, apply=True)
            self.logger.info(f"oil_stocks recomposed: {stats}")
        except Exception as e:
            self.logger.warning(f"oil_stocks recompose skipped: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

        # Audit session: complete or fail based on whether anything succeeded
        try:
            if session_id is not None:
                if errors and not any(r.get('bronze', 0) > 0 for r in results.values()):
                    self._fail_audit_session(
                        conn, session_id,
                        '; '.join(f"{rt}: {m}" for rt, m in errors)
                    )
                else:
                    self._complete_audit_session(conn, session_id, results)
                conn.commit()
        except Exception as e:
            self.logger.warning(f"Could not close audit session: {e}")
            try:
                conn.rollback()
            except Exception:
                pass

        if close_conn:
            try:
                conn.close()
            except Exception:
                pass

        results['errors'] = errors
        if errors:
            self.logger.warning(
                f"save_to_monthly_realized completed with {len(errors)} report-type "
                f"error(s): {errors}"
            )
        return results

    def _save_to_realized(
        self,
        df: pd.DataFrame,
        report_type: str,
        conn
    ) -> int:
        """Save DataFrame to silver.monthly_realized."""
        cur = conn.cursor()
        count = 0

        # Map report types to commodity and attribute
        for _, row in df.iterrows():
            try:
                # Determine commodity and attribute based on report type
                commodity = self._map_commodity(row)
                attribute = self._map_attribute(row)

                if not commodity or not attribute:
                    continue

                # Calculate marketing year
                calendar_year = row['year']
                month = row['month']
                marketing_year = self._get_marketing_year(commodity, calendar_year, month)

                cur.execute("""
                    INSERT INTO silver.monthly_realized (
                        commodity, country, marketing_year, month, calendar_year,
                        attribute, realized_value, unit, source, report_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                    ON CONFLICT (commodity, country, marketing_year, month, attribute, source)
                    DO UPDATE SET
                        realized_value = EXCLUDED.realized_value,
                        collected_at = NOW()
                """, (
                    commodity,
                    'US',
                    marketing_year,
                    month,
                    calendar_year,
                    attribute,
                    row['value'],
                    row['unit'],
                    row['source']
                ))
                count += 1

            except Exception as e:
                self.logger.warning("Error inserting row: {}".format(e))
                continue

        return count

    def _map_commodity(self, row: pd.Series) -> Optional[str]:
        """Map NASS commodity to our standard commodity names."""
        # Check commodity_desc first (more reliable for soy crush data)
        commodity_desc = row.get('commodity_desc', '').upper()
        class_desc = row.get('class_desc', '').upper()

        # Animal fats / greases / rendered meals: commodity is the lowercased
        # commodity_desc (grease/lard/tallow/poultry_fats/poultry_by-product_meals/meal),
        # matching NASS_FATS_OILS_BACKFILL. Must precede the generic OIL/CAKE &
        # MEAL/MILLFEED handling below ('MEAL' alone != 'CAKE & MEAL').
        if commodity_desc in (
            'GREASE', 'LARD', 'TALLOW', 'POULTRY FATS',
            'POULTRY BY-PRODUCT MEALS', 'MEAL',
        ):
            return commodity_desc.lower().replace(' ', '_')

        if commodity_desc == 'SOYBEANS':
            return 'soybeans'
        if commodity_desc == 'CANOLA':
            return 'canola'
        if commodity_desc == 'COTTONSEED':
            return 'cottonseed'
        if commodity_desc == 'COTTON' and 'COTTONSEED' in class_desc:
            return 'cottonseed'
        if commodity_desc in ('OIL', 'CAKE & MEAL', 'MILLFEED'):
            if 'SOYBEAN' in class_desc:
                return 'soybeans'
            if 'CANOLA' in class_desc:
                return 'canola'
            if 'COTTONSEED' in class_desc:
                return 'cottonseed'
            if 'SUNFLOWER' in class_desc:
                return 'sunflower'
            # Order matters: PALM KERNEL must be checked before PALM
            if 'PALM KERNEL' in class_desc:
                return 'palm_kernel'
            if class_desc == 'PALM':
                return 'palm'
            if 'COCONUT' in class_desc:
                return 'coconut'
            if 'SAFFLOWER' in class_desc:
                return 'safflower'
            if 'PEANUT' in class_desc:
                return 'peanut'
            if class_desc == 'CORN':
                return 'corn'

        # Fall back to commodity key
        comm = row.get('commodity', '').lower()

        if 'soybean' in comm or 'soy' in comm:
            return 'soybeans'
        elif 'corn' in comm:
            return 'corn'
        elif 'wheat' in comm or 'flour' in comm:
            return 'wheat'
        elif 'sorghum' in comm:
            return 'sorghum'
        elif 'cotton' in comm:
            return 'cottonseed'
        elif 'canola' in comm:
            return 'canola'
        elif 'sunflower' in comm:
            return 'sunflower'
        elif 'peanut' in comm:
            return 'peanuts'

        return None

    def _map_attribute(self, row: pd.Series) -> Optional[str]:
        """Map NASS statistic category to our standard attribute names."""
        stat_cat = row.get('statisticcat', '').upper()
        comm = row.get('commodity', row.get('commodity_desc', '')).lower()
        short_desc = row.get('short_desc', '').upper()
        commodity_desc = row.get('commodity_desc', '').upper()

        # Animal fats / greases / rendered meals: attribute is the raw NASS
        # short_desc (NASS short_descs are uppercase, matching the backfill).
        # gold.nass_low_ci_matrix keys off LIKE patterns on this string.
        if commodity_desc in (
            'GREASE', 'LARD', 'TALLOW', 'POULTRY FATS',
            'POULTRY BY-PRODUCT MEALS', 'MEAL',
        ):
            return row.get('short_desc', '') or None

        # Seeds crushed (soybeans, canola, cottonseed)
        if commodity_desc in ('SOYBEANS', 'CANOLA', 'COTTONSEED') and stat_cat == 'CRUSHED':
            return 'crush'

        # Cake & meal production/stocks (all oilseeds)
        if commodity_desc == 'CAKE & MEAL':
            if stat_cat == 'PRODUCTION':
                if 'ANIMAL FEED' in short_desc:
                    return 'meal_production_feed'
                elif 'EDIBLE PROTEIN' in short_desc:
                    return 'meal_production_edible'
                return 'meal_production'
            if stat_cat == 'STOCKS':
                return 'meal_stocks'

        # Millfeed production/stocks
        if commodity_desc == 'MILLFEED':
            if stat_cat == 'PRODUCTION':
                return 'millfeed_production'
            if stat_cat == 'STOCKS':
                return 'millfeed_stocks'

        # Fats and Oils
        if 'PRODUCTION' in stat_cat and ('oil' in comm or commodity_desc == 'OIL'):
            if 'CRUDE' in short_desc:
                return 'oil_production_crude'
            elif 'REFINED' in short_desc:
                return 'oil_production_refined'
            return 'oil_production'

        if 'STOCKS' in stat_cat and ('oil' in comm or commodity_desc == 'OIL'):
            # Skip at row level. NASS splits oil stocks by refinement stage (crude /
            # once-refined) across several short_descs; collapsing them all to one
            # 'oil_stocks' key here made them collide on the silver upsert key so only the
            # last writer (once-refined, ~5-6x low) survived. oil_stocks_composition.py
            # owns oil_stocks + its crude/once_refined components, recomputed from bronze
            # after ingest (see save_to_monthly_realized). Do NOT re-add a row-level map.
            return None

        # Oil removal for processing (with edible/inedible subcategories)
        if stat_cat == 'REMOVAL FOR PROCESSING, INEDIBLE USE':
            if 'CRUDE' in short_desc:
                return 'oil_crude_inedible_use'
            return 'oil_refined_inedible_use'
        if stat_cat == 'REMOVAL FOR PROCESSING, EDIBLE USE':
            return 'oil_refined_edible_use'
        if 'REMOVAL FOR PROCESSING' in stat_cat:
            if 'ONCE REFINED' in short_desc:
                return 'oil_refined_further_processing'
            return 'oil_removal_for_processing'

        # Grain Crushings
        if 'ALCOHOL' in stat_cat:
            return 'ethanol_grind'
        if 'DRY MILLING' in stat_cat:
            return 'dry_milling'
        if 'WET MILLING' in stat_cat:
            return 'wet_milling'

        # Flour Milling
        if 'PRODUCTION' in stat_cat and 'flour' in comm:
            return 'flour_production'

        # Peanut Processing
        if 'peanut' in comm:
            if 'MILLED' in stat_cat:
                return 'peanuts_milled'
            elif 'CRUSHED' in stat_cat:
                return 'peanuts_crushed'
            elif 'STOCKS' in stat_cat:
                return 'peanuts_stocks'
            elif 'USAGE' in stat_cat:
                return 'peanuts_usage'

        # Generic
        if 'CRUSH' in stat_cat or 'USAGE' in stat_cat:
            return 'crush'

        return stat_cat.lower().replace(' ', '_')

    def _get_marketing_year(
        self,
        commodity: str,
        calendar_year: int,
        month: int
    ) -> int:
        """
        Calculate marketing year from calendar year and month.

        Marketing years:
        - Corn/Soybeans/Sorghum: Sep-Aug (Sep 2025 = MY 2025/26)
        - Wheat: Jun-May (Jun 2025 = MY 2025/26)
        - Peanuts: Aug-Jul (Aug 2025 = MY 2025/26)
        """
        # Handle None month
        if month is None:
            return calendar_year

        if commodity in ('corn', 'soybeans', 'sorghum'):
            # Sep-Aug marketing year
            if month >= 9:
                return calendar_year
            else:
                return calendar_year - 1

        elif commodity == 'wheat':
            # Jun-May marketing year
            if month >= 6:
                return calendar_year
            else:
                return calendar_year - 1

        elif commodity == 'peanuts':
            # Aug-Jul marketing year
            if month >= 8:
                return calendar_year
            else:
                return calendar_year - 1

        # Default to calendar year
        return calendar_year

    # =========================================================================
    # AUDIT LOGGING
    # =========================================================================

    def _start_audit_session(self, conn, report_type: str, year: int) -> Optional[str]:
        """Start an audit transformation session."""
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT audit.start_transformation_session(
                    'BRONZE_TO_SILVER',
                    'nass_processing_collector',
                    'BRONZE',
                    ARRAY['bronze.nass_processing', 'silver.monthly_realized'],
                    %s,
                    'COLLECTOR',
                    '2.0'
                )
            """, ("NASS {} collection for year {}".format(report_type, year),))
            result = cur.fetchone()
            return str(result[0]) if result else None
        except Exception as e:
            self.logger.warning("Could not start audit session: {}".format(e))
            return None

    def _complete_audit_session(
        self, conn, session_id: Optional[str], results: Dict
    ):
        """Complete an audit session with results."""
        if not session_id:
            return
        try:
            cur = conn.cursor()
            total_rows = sum(
                r.get('bronze', 0) + r.get('silver', 0)
                for r in results.values()
                if isinstance(r, dict)
            )
            cur.execute("""
                SELECT audit.complete_transformation_session(%s, 'COMPLETED')
            """, (session_id,))

            # Log the operation
            cur.execute("""
                SELECT audit.log_transformation_operation(
                    %s, 'UPSERT',
                    ARRAY['bronze.nass_processing'],
                    %s,
                    'silver.monthly_realized',
                    %s, %s, 'PYTHON'
                )
            """, (
                session_id,
                "Fetched NASS data, saved to bronze and silver",
                total_rows,
                total_rows
            ))
        except Exception as e:
            self.logger.warning("Could not complete audit session: {}".format(e))

    def _fail_audit_session(self, conn, session_id: Optional[str], error: str):
        """Mark audit session as failed."""
        if not session_id:
            return
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT audit.complete_transformation_session(%s, 'FAILED', %s)
            """, (session_id, error))
        except Exception:
            pass


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for NASS Processing Reports collector."""
    import argparse

    parser = argparse.ArgumentParser(
        description='USDA NASS Processing Reports Collector'
    )

    parser.add_argument(
        'report',
        choices=['fats_oils', 'animal_fats', 'soy_crush', 'grain_crushings',
                 'flour_milling', 'peanut_processing', 'all'],
        help='Report type to fetch'
    )

    parser.add_argument(
        '--year',
        type=int,
        default=datetime.now().year,
        help='Year to fetch (default: current)'
    )

    parser.add_argument(
        '--save-db',
        action='store_true',
        help='Save to bronze.nass_processing + silver.monthly_realized'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output CSV file'
    )

    parser.add_argument(
        '--api-key',
        help='NASS API key (or set NASS_API_KEY env var)'
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s'
    )

    # Set API key if provided
    if args.api_key:
        os.environ['NASS_API_KEY'] = args.api_key

    collector = NASSProcessingCollector()

    if args.save_db:
        print("Collecting {} and saving to database...".format(args.report))
        results = collector.save_to_monthly_realized(
            report_type=args.report,
            year=args.year
        )
        print("Results: {}".format(results))
    else:
        # Fetch and display/save
        df = None
        if args.report == 'fats_oils':
            df = collector.fetch_fats_oils(year=args.year)
        elif args.report == 'animal_fats':
            df = collector.fetch_animal_fats(year=args.year)
        elif args.report == 'soy_crush':
            df = collector.fetch_soy_crush(year=args.year)
        elif args.report == 'grain_crushings':
            df = collector.fetch_grain_crushings(year=args.year)
        elif args.report == 'flour_milling':
            df = collector.fetch_flour_milling(year=args.year)
        elif args.report == 'peanut_processing':
            df = collector.fetch_peanut_processing(year=args.year)
        else:  # all
            dfs = []
            for rpt in ['fats_oils', 'animal_fats', 'soy_crush', 'grain_crushings',
                         'flour_milling', 'peanut_processing']:
                if rpt == 'fats_oils':
                    d = collector.fetch_fats_oils(year=args.year)
                elif rpt == 'animal_fats':
                    d = collector.fetch_animal_fats(year=args.year)
                elif rpt == 'soy_crush':
                    d = collector.fetch_soy_crush(year=args.year)
                elif rpt == 'grain_crushings':
                    d = collector.fetch_grain_crushings(year=args.year)
                elif rpt == 'flour_milling':
                    d = collector.fetch_flour_milling(year=args.year)
                else:
                    d = collector.fetch_peanut_processing(year=args.year)
                if d is not None:
                    dfs.append(d)
            df = pd.concat(dfs, ignore_index=True) if dfs else None

        if df is not None:
            print("\nRetrieved {} records".format(len(df)))
            print("\nSample data:")
            print(df.head(10).to_string())

            if args.output:
                df.to_csv(args.output, index=False)
                print("\nSaved to: {}".format(args.output))
        else:
            print("No data retrieved")


if __name__ == '__main__':
    main()
