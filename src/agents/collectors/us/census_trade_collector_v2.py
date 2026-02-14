"""
US Census Bureau International Trade API Collector V2
======================================================
Enhanced collector that uses the HS codes reference file for comprehensive
trade data collection across all agricultural commodities.

Collects trade data from the US Census Bureau:
- Monthly imports/exports by HS code
- Trade by country/partner
- Supports all commodity categories (grains, oilseeds, oils, meals, biofuels, etc.)

Requires free API key from: https://api.census.gov/data/key_signup.html
"""

import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import requests
from dotenv import load_dotenv

# Load environment
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
load_dotenv(PROJECT_ROOT / '.env')

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# HS CODES REFERENCE LOADER
# =============================================================================

def load_hs_codes_reference() -> Dict:
    """Load HS codes from the domain knowledge reference file."""
    ref_path = PROJECT_ROOT / 'domain_knowledge' / 'data_dictionaries' / 'hs_codes_reference.json'

    if not ref_path.exists():
        logger.warning(f"HS codes reference not found at {ref_path}")
        return {}

    with open(ref_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_hs_codes_for_category(category: str) -> List[str]:
    """
    Get all HS codes for a category.

    Categories: grains, oilseeds, vegetable_oils, protein_meals, animal_fats,
                biofuels, petroleum, livestock_meat
    """
    ref = load_hs_codes_reference()
    categories = ref.get('categories', {})

    if category not in categories:
        logger.warning(f"Unknown category: {category}")
        return []

    codes = []
    cat_data = categories[category]

    for subcategory, subdata in cat_data.items():
        if isinstance(subdata, dict) and 'codes' in subdata:
            for code_info in subdata['codes']:
                hs_code = code_info.get('hs_code', '').replace('.', '')
                if hs_code:
                    codes.append(hs_code)

    return codes


def get_hs_codes_for_commodity(commodity: str) -> List[str]:
    """
    Get HS codes for a specific commodity using quick lookup.

    Examples: corn, soybeans, soybean_oil, ethanol, biodiesel, tallow, etc.
    """
    ref = load_hs_codes_reference()
    quick_lookup = ref.get('quick_lookup', {}).get('by_commodity', {})

    if commodity in quick_lookup:
        return [code.replace('.', '') for code in quick_lookup[commodity]]

    return []


def get_primary_hs_code(commodity: str) -> Optional[str]:
    """Get the primary/most common HS code for a commodity."""
    # First try the known working Census codes
    census_code = CENSUS_SCHEDULE_B_CODES.get(commodity)
    if census_code:
        return census_code

    # Map from reference file commodity names to Census codes
    COMMODITY_TO_CENSUS = {
        'yellow_corn_exports': 'corn_yellow_dent_2',
        'soybean_exports': 'soybeans',
        'wheat_exports': 'wheat',
        'crude_soybean_oil': 'soybean_oil_crude',
        'fuel_ethanol': 'ethanol_denatured',
        'biodiesel_b100': 'biodiesel',
        'inedible_tallow': 'tallow_inedible',
    }

    if commodity in COMMODITY_TO_CENSUS:
        return CENSUS_SCHEDULE_B_CODES.get(COMMODITY_TO_CENSUS[commodity])

    # Fall back to reference file
    ref = load_hs_codes_reference()
    primary_codes = ref.get('quick_lookup', {}).get('primary_codes', {})

    if commodity in primary_codes:
        return primary_codes[commodity].replace('.', '')

    # Fall back to first code in by_commodity
    codes = get_hs_codes_for_commodity(commodity)
    return codes[0] if codes else None


# =============================================================================
# COMMODITY GROUPS FOR SCHEDULED RUNS
# =============================================================================

COMMODITY_GROUPS = {
    'grains': {
        'description': 'Corn, wheat, sorghum, barley, rice',
        'categories': ['grains'],
        'priority': 1,
    },
    'oilseeds': {
        'description': 'Soybeans, canola, sunflower, other oilseeds',
        'categories': ['oilseeds'],
        'priority': 1,
    },
    'oils_meals': {
        'description': 'Vegetable oils and protein meals',
        'categories': ['vegetable_oils', 'protein_meals'],
        'priority': 2,
    },
    'biofuels': {
        'description': 'Ethanol, biodiesel, renewable diesel',
        'categories': ['biofuels'],
        'priority': 2,
    },
    'animal_fats': {
        'description': 'Tallow, lard, yellow grease, UCO',
        'categories': ['animal_fats'],
        'priority': 3,
    },
    'livestock': {
        'description': 'Cattle, hogs, beef, pork, poultry',
        'categories': ['livestock_meat'],
        'priority': 3,
    },
    'cottonseed': {
        'description': 'Cottonseed (seed), cottonseed oil, cottonseed meal',
        'hs_codes': [
            '1207210000', '1207290000',                    # Cottonseed (seed)
            '1512210000', '1512290020', '1512290040',      # Cottonseed oil
            '2306100000',                                   # Cottonseed meal
        ],
        'priority': 2,
    },
    'petroleum': {
        'description': 'Crude oil, refined products, natural gas',
        'categories': ['petroleum'],
        'priority': 4,
    },
}

# Priority commodities for daily monitoring
PRIORITY_COMMODITIES = [
    'yellow_corn_exports',
    'soybean_exports',
    'wheat_exports',
    'soybean_meal',
    'crude_soybean_oil',
    'ddgs',
    'fuel_ethanol',
    'biodiesel_b100',
    'inedible_tallow',
    'yellow_grease',
]


# =============================================================================
# KNOWN WORKING CENSUS SCHEDULE B CODES
# =============================================================================
# These are the actual codes that work with Census API, which may differ from
# international HS codes at the 10-digit level.

CENSUS_SCHEDULE_B_CODES = {
    # Grains
    'corn_yellow_dent_2': '1005902030',      # Yellow Dent Corn US No. 2
    'corn_yellow_dent_3': '1005902035',      # Yellow Dent Corn US No. 3
    'corn_seed': '1005100010',               # Yellow Corn Seed
    'corn': '1005',                          # 4-digit (all corn)
    'wheat': '1001992055',                   # Wheat NESOI (main bulk)
    'wheat_white': '1001992015',             # White Wheat
    'wheat_seed': '1001910000',              # Wheat Seed
    'wheat_4digit': '1001',                  # 4-digit (all wheat)
    'sorghum_seed': '1007100000',            # Grain Sorghum Seed
    'barley': '1003900000',                  # Barley except seed

    # Oilseeds
    'soybeans': '1201900095',                # Soybeans bulk (main export)
    'soybeans_oilstock': '1201900005',       # Soybeans for oil stock
    'soybeans_seed': '1201100000',           # Soybean seeds for sowing
    'soybeans_4digit': '1201',               # 4-digit (all soybeans)
    'canola': '1205100000',                  # Low erucic acid rapeseed/canola
    'sunflower_oilstock': '1206000020',      # Sunflower seeds for oil
    'sunflower_other': '1206000090',         # Sunflower seeds NESOI
    'cotton_seed': '1207290000',             # Cotton seeds except sowing
    'cotton_seed_sowing': '1207210000',      # Cotton seeds for sowing

    # Vegetable Oils
    'soybean_oil_refined': '1507904050',     # Soybean oil fully refined
    'soybean_oil_crude': '1507100000',       # Soybean oil crude
    'palm_oil_refined': '1511900000',        # Palm oil refined
    'palm_oil_crude': '1511100000',          # Palm oil crude
    'sunflower_oil': '1512190020',           # Sunflower oil refined
    'cottonseed_oil_crude': '1512210000',    # Crude cottonseed oil
    'cottonseed_oil_refined': '1512290020',  # Refined cottonseed oil (once refined, edible)
    'cottonseed_oil_fully_refined': '1512290040',  # Refined cottonseed oil (fully refined)
    'canola_oil': '1514190000',              # Rapeseed/canola oil NESOI
    'canola_oil_crude': '1514110000',        # Rapeseed/canola oil crude
    'corn_oil_refined': '1515290040',        # Corn oil fully refined
    'palm_kernel_oil': '1513290000',         # Palm kernel oil refined

    # Meals & Residues
    'soybean_meal': '2304000000',            # Soybean oilcake/meal
    'soybean_meal_bran': '2302500000',       # Bran from legumes (soybean hulls)
    'soybean_flour': '1208100000',           # Soy flour/meal (Chapter 12)
    'sunflower_meal': '2306300000',          # Sunflower seed meal
    'cotton_meal': '2306100000',             # Cotton seed meal
    'canola_meal': '2306490000',             # Rapeseed/canola meal
    'corn_gluten_feed': '2303100010',        # Corn gluten feed
    'corn_gluten_meal': '2303100020',        # Corn gluten meal
    'ddgs': '2303300000',                    # Distillers grains (DDGS)

    # Cotton
    'cotton_raw': '5201009000',              # Cotton not carded
    'cotton_medium': '5201001090',           # Cotton medium staple
    'cotton_pima': '5201002030',             # American Pima cotton

    # Biofuels
    'ethanol_undenatured': '2207100000',     # Undenatured ethyl alcohol
    'ethanol_denatured': '2207200000',       # Denatured ethyl alcohol
    'biodiesel': '3826001000',               # Biodiesel

    # Animal Fats
    'tallow_edible': '1502100020',           # Edible tallow
    'tallow_inedible': '1502100040',         # Inedible tallow
    'lard': '1501100000',                    # Lard
    'yellow_grease': '1501200060',           # Yellow grease
}


def get_census_code(commodity: str) -> Optional[str]:
    """Get working Census Schedule B code for a commodity."""
    return CENSUS_SCHEDULE_B_CODES.get(commodity)


# =============================================================================
# COLLECTOR CLASS
# =============================================================================

@dataclass
class CensusTradeConfig:
    """Census Trade API configuration"""
    source_name: str = "US Census Trade"
    source_url: str = "https://api.census.gov/data/timeseries/intltrade"

    api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get('CENSUS_API_KEY')
    )

    # Rate limiting and retry settings
    requests_per_minute: int = 30
    retry_delay: float = 2.0      # Base delay for exponential backoff
    max_retries: int = 5          # Increased from 3 for better reliability
    request_timeout: int = 60     # Timeout per request in seconds

    # Database
    db_host: str = field(default_factory=lambda: os.environ.get('DB_HOST', 'localhost'))
    db_port: int = field(default_factory=lambda: int(os.environ.get('DB_PORT', '5432')))
    db_name: str = field(default_factory=lambda: os.environ.get('DB_NAME', 'rlc_commodities'))
    db_user: str = field(default_factory=lambda: os.environ.get('DB_USER', 'postgres'))
    db_password: str = field(default_factory=lambda: os.environ.get('DB_PASSWORD', ''))


class CensusTradeCollectorV2:
    """
    Enhanced collector for US Census Bureau International Trade data.

    Uses the HS codes reference file for comprehensive commodity coverage.
    Supports scheduled runs by commodity group.
    """

    def __init__(self, config: CensusTradeConfig = None):
        self.config = config or CensusTradeConfig()
        self.session = requests.Session()
        self._last_request_time = 0

        if not self.config.api_key:
            logger.warning(
                "No Census API key. Register at: https://api.census.gov/data/key_signup.html"
                "\n(API will work without key but limited to 500 calls/day)"
            )

    def _rate_limit(self):
        """Enforce rate limiting."""
        min_interval = 60.0 / self.config.requests_per_minute
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    def _refresh_session(self):
        """Create a fresh session to clear stale connection state."""
        try:
            self.session.close()
        except Exception:
            pass
        self.session = requests.Session()
        logger.debug("Session refreshed")

    def _make_request(
        self,
        url: str,
        params: Dict,
        retries: int = None
    ) -> Tuple[Optional[requests.Response], Optional[str]]:
        """Make API request with rate limiting, exponential backoff, and retries."""
        retries = retries if retries is not None else self.config.max_retries

        for attempt in range(retries):
            self._rate_limit()

            try:
                response = self.session.get(url, params=params, timeout=self.config.request_timeout)

                if response.status_code == 200:
                    return response, None
                elif response.status_code == 204:
                    return None, "No data available"
                elif response.status_code == 429:
                    # Rate limited - exponential backoff with jitter
                    base_wait = self.config.retry_delay * (2 ** attempt)
                    jitter = random.uniform(0, base_wait * 0.5)
                    wait_time = base_wait + jitter
                    logger.warning(f"Rate limited, waiting {wait_time:.1f}s (attempt {attempt + 1})")
                    time.sleep(wait_time)
                    continue
                else:
                    return None, f"HTTP {response.status_code}"

            except requests.exceptions.Timeout:
                # Timeout - exponential backoff with jitter
                base_wait = self.config.retry_delay * (2 ** attempt)
                jitter = random.uniform(0, base_wait * 0.5)
                wait_time = base_wait + jitter
                logger.warning(f"Request timeout (attempt {attempt + 1}/{retries}), waiting {wait_time:.1f}s")
                time.sleep(wait_time)

            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError) as e:
                # Connection errors - refresh session and use exponential backoff
                base_wait = self.config.retry_delay * (2 ** attempt)
                jitter = random.uniform(0, base_wait * 0.5)
                wait_time = base_wait + jitter
                logger.warning(f"Connection error (attempt {attempt + 1}/{retries}): {type(e).__name__}")
                logger.warning(f"Refreshing session and waiting {wait_time:.1f}s before retry")
                self._refresh_session()
                time.sleep(wait_time)

            except requests.exceptions.RequestException as e:
                # Other request errors - exponential backoff
                base_wait = self.config.retry_delay * (2 ** attempt)
                jitter = random.uniform(0, base_wait * 0.5)
                wait_time = base_wait + jitter
                logger.error(f"Request error (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(wait_time)

        return None, "Max retries exceeded"

    def fetch_trade_data(
        self,
        flow: str,
        hs_code: str,
        start_date: date,
        end_date: date,
        partner_country: str = None
    ) -> List[Dict]:
        """
        Fetch trade data for a specific HS code and flow.

        Args:
            flow: 'imports' or 'exports'
            hs_code: 10-digit HS code (no dots)
            start_date: Start date
            end_date: End date
            partner_country: Optional country code filter

        Returns:
            List of trade records
        """
        url = f"{self.config.source_url}/{flow}/hs"

        # Field names differ for imports vs exports
        if flow == 'imports':
            commodity_field = 'I_COMMODITY'
            value_field = 'GEN_VAL_MO'
            qty_field = 'GEN_QY1_MO'
            qty_unit_field = 'GEN_QY1_MO_FLAG'
        else:
            commodity_field = 'E_COMMODITY'
            value_field = 'ALL_VAL_MO'
            qty_field = 'QTY_1_MO'
            qty_unit_field = 'QTY_1_MO_FLAG'

        records = []
        current = date(start_date.year, start_date.month, 1)

        while current <= end_date:
            time_str = f"{current.year}-{current.month:02d}"

            params = {
                'get': f'{value_field},{qty_field},CTY_CODE,CTY_NAME',
                commodity_field: hs_code,
                'time': time_str,
            }

            if self.config.api_key:
                params['key'] = self.config.api_key

            if partner_country:
                params['CTY_CODE'] = partner_country

            response, error = self._make_request(url, params)

            if response and response.status_code == 200:
                try:
                    data = response.json()
                    if data and len(data) > 1:
                        headers = data[0]
                        for row in data[1:]:
                            record = dict(zip(headers, row))
                            records.append({
                                'year': current.year,
                                'month': current.month,
                                'flow': flow,
                                'hs_code': hs_code,
                                'country_code': record.get('CTY_CODE'),
                                'country_name': record.get('CTY_NAME'),
                                'value_usd': self._safe_float(record.get(value_field)),
                                'quantity': self._safe_float(record.get(qty_field)),
                                'source': 'CENSUS_TRADE',
                            })
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON response for {flow}/{hs_code}/{time_str}")
            elif error:
                logger.debug(f"No data for {flow}/{hs_code}/{time_str}: {error}")

            # Move to next month
            if current.month == 12:
                current = date(current.year + 1, 1, 1)
            else:
                current = date(current.year, current.month + 1, 1)

        return records

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert to float."""
        if value is None or value == '' or value == 'null':
            return None
        try:
            return float(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return None

    def collect_commodity_group(
        self,
        group: str,
        start_date: date = None,
        end_date: date = None,
        flow: str = 'both'
    ) -> Dict[str, Any]:
        """
        Collect trade data for a commodity group.

        Args:
            group: Group name from COMMODITY_GROUPS
            start_date: Start date (default: 13 months ago)
            end_date: End date (default: today)
            flow: 'imports', 'exports', or 'both'

        Returns:
            Dict with success status, records, and stats
        """
        if group not in COMMODITY_GROUPS:
            return {'success': False, 'error': f"Unknown group: {group}"}

        group_info = COMMODITY_GROUPS[group]

        # Use direct HS codes if specified, otherwise use category lookup
        hs_codes = []
        if 'hs_codes' in group_info:
            hs_codes = group_info['hs_codes']
        else:
            categories = group_info['categories']
            for category in categories:
                hs_codes.extend(get_hs_codes_for_category(category))

        if not hs_codes:
            return {'success': False, 'error': f"No HS codes found for group: {group}"}

        logger.info(f"Collecting {group}: {len(hs_codes)} HS codes")

        # Set date range
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year - 1, end_date.month, 1)

        # Ensure we don't go before 2013 (API limitation)
        if start_date < date(2013, 1, 1):
            start_date = date(2013, 1, 1)

        flows = ['imports', 'exports'] if flow == 'both' else [flow]

        all_records = []
        stats = {'hs_codes': len(hs_codes), 'flows': len(flows), 'errors': 0}

        for trade_flow in flows:
            for hs_code in hs_codes:
                try:
                    records = self.fetch_trade_data(
                        trade_flow, hs_code, start_date, end_date
                    )
                    all_records.extend(records)
                    logger.debug(f"  {trade_flow}/{hs_code}: {len(records)} records")
                except Exception as e:
                    logger.error(f"Error fetching {trade_flow}/{hs_code}: {e}")
                    stats['errors'] += 1

        stats['total_records'] = len(all_records)

        return {
            'success': True,
            'group': group,
            'records': all_records,
            'stats': stats,
            'period': f"{start_date} to {end_date}"
        }

    def collect_priority_commodities(
        self,
        start_date: date = None,
        end_date: date = None
    ) -> Dict[str, Any]:
        """
        Collect trade data for priority commodities only.
        Used for daily monitoring.
        """
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year, end_date.month, 1)

        all_records = []

        # Map priority commodities to Census codes
        PRIORITY_CODES = {
            'corn': 'corn_yellow_dent_2',
            'wheat': 'wheat',
            'soybeans': 'soybeans',
            'soybean_meal': 'soybean_meal',
            'soybean_meal_bran': 'soybean_meal_bran',
            'soybean_flour': 'soybean_flour',
            'soybean_oil': 'soybean_oil_crude',
            'ddgs': 'ddgs',
            'canola': 'canola',
        }

        for name, census_key in PRIORITY_CODES.items():
            hs_code = CENSUS_SCHEDULE_B_CODES.get(census_key)
            if not hs_code:
                logger.warning(f"No HS code found for {name}")
                continue

            for flow in ['imports', 'exports']:
                records = self.fetch_trade_data(flow, hs_code, start_date, end_date)
                all_records.extend(records)
                logger.info(f"  {flow}/{name}: {len(records)} records")

        return {
            'success': True,
            'records': all_records,
            'commodities': list(PRIORITY_CODES.keys()),
            'period': f"{start_date} to {end_date}"
        }

    def collect_all_commodities(
        self,
        start_date: date = None,
        end_date: date = None,
        flow: str = 'both'
    ) -> Dict[str, Any]:
        """
        Collect trade data for all known Census Schedule B commodities.
        """
        end_date = end_date or date.today()
        start_date = start_date or date(end_date.year - 1, 1, 1)

        if start_date < date(2013, 1, 1):
            start_date = date(2013, 1, 1)

        flows = ['imports', 'exports'] if flow == 'both' else [flow]

        all_records = []
        stats = {'commodities': 0, 'errors': 0}

        for commodity_name, hs_code in CENSUS_SCHEDULE_B_CODES.items():
            # Skip 4-digit codes for detailed collection
            if len(hs_code) <= 4:
                continue

            stats['commodities'] += 1

            for trade_flow in flows:
                try:
                    records = self.fetch_trade_data(
                        trade_flow, hs_code, start_date, end_date
                    )
                    all_records.extend(records)
                    logger.info(f"  {trade_flow}/{commodity_name}: {len(records)} records")
                except Exception as e:
                    logger.error(f"Error fetching {trade_flow}/{commodity_name}: {e}")
                    stats['errors'] += 1

        stats['total_records'] = len(all_records)

        return {
            'success': True,
            'records': all_records,
            'stats': stats,
            'period': f"{start_date} to {end_date}"
        }

    # =========================================================================
    # DATABASE METHODS
    # =========================================================================

    def save_to_bronze(
        self,
        records: List[Dict],
        conn = None
    ) -> Dict[str, int]:
        """
        Save trade records to bronze layer.

        Args:
            records: List of trade record dicts
            conn: Optional database connection

        Returns:
            Dict with insert/update counts
        """
        import psycopg2

        close_conn = False
        if conn is None:
            conn = psycopg2.connect(
                host=self.config.db_host,
                port=self.config.db_port,
                database=self.config.db_name,
                user=self.config.db_user,
                password=self.config.db_password
            )
            close_conn = True

        cursor = conn.cursor()
        counts = {'inserted': 0, 'updated': 0, 'errors': 0}

        for record in records:
            try:
                cursor.execute("""
                    INSERT INTO bronze.census_trade
                    (year, month, flow, hs_code, country_code, country_name,
                     value_usd, quantity, source, collected_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (year, month, flow, hs_code, country_code)
                    DO UPDATE SET
                        country_name = EXCLUDED.country_name,
                        value_usd = EXCLUDED.value_usd,
                        quantity = EXCLUDED.quantity,
                        collected_at = NOW()
                    RETURNING (xmax = 0) as inserted
                """, (
                    record.get('year'),
                    record.get('month'),
                    record.get('flow'),
                    record.get('hs_code'),
                    record.get('country_code'),
                    record.get('country_name'),
                    record.get('value_usd'),
                    record.get('quantity'),
                    record.get('source', 'CENSUS_TRADE')
                ))

                result = cursor.fetchone()
                if result and result[0]:
                    counts['inserted'] += 1
                else:
                    counts['updated'] += 1

            except Exception as e:
                logger.error(f"Error saving record: {e}")
                counts['errors'] += 1
                conn.rollback()

        conn.commit()

        if close_conn:
            cursor.close()
            conn.close()

        logger.info(f"Saved to bronze.census_trade: {counts}")
        return counts

    def collect_and_save_group(
        self,
        group: str,
        start_date: date = None,
        end_date: date = None,
        flow: str = 'both'
    ) -> Dict[str, Any]:
        """
        Collect a commodity group and save to database.

        Args:
            group: Commodity group name
            start_date: Start date
            end_date: End date
            flow: 'imports', 'exports', or 'both'

        Returns:
            Collection result with database stats
        """
        result = self.collect_commodity_group(group, start_date, end_date, flow)

        if not result['success']:
            return result

        db_stats = self.save_to_bronze(result['records'])
        result['db_stats'] = db_stats

        return result


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for Census Trade collector V2"""
    import argparse

    parser = argparse.ArgumentParser(description='Census Trade Data Collector V2')

    parser.add_argument(
        'command',
        choices=['collect', 'group', 'priority', 'list-groups', 'list-codes'],
        help='Command to execute'
    )

    parser.add_argument(
        '--group',
        choices=list(COMMODITY_GROUPS.keys()),
        help='Commodity group to collect'
    )

    parser.add_argument(
        '--category',
        help='Category to list codes for'
    )

    parser.add_argument(
        '--commodity',
        help='Specific commodity name'
    )

    parser.add_argument(
        '--flow',
        choices=['imports', 'exports', 'both'],
        default='both',
        help='Trade flow'
    )

    parser.add_argument(
        '--start-date',
        help='Start date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        help='End date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--save-db',
        action='store_true',
        help='Save data to PostgreSQL bronze layer'
    )

    parser.add_argument(
        '--api-key',
        help='Census API key'
    )

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    if args.api_key:
        os.environ['CENSUS_API_KEY'] = args.api_key

    collector = CensusTradeCollectorV2()

    if args.command == 'list-groups':
        print("\nAvailable commodity groups:")
        for name, info in COMMODITY_GROUPS.items():
            print(f"  {name}: {info['description']}")
        return

    if args.command == 'list-codes':
        if args.category:
            codes = get_hs_codes_for_category(args.category)
            print(f"\nHS codes for category '{args.category}':")
            for code in codes:
                print(f"  {code}")
        elif args.commodity:
            codes = get_hs_codes_for_commodity(args.commodity)
            print(f"\nHS codes for commodity '{args.commodity}':")
            for code in codes:
                print(f"  {code}")
        else:
            print("Specify --category or --commodity")
        return

    # Parse dates
    start_date = None
    end_date = None
    if args.start_date:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    if args.command == 'group':
        if not args.group:
            print("Specify --group")
            return

        if args.save_db:
            result = collector.collect_and_save_group(
                args.group, start_date, end_date, args.flow
            )
        else:
            result = collector.collect_commodity_group(
                args.group, start_date, end_date, args.flow
            )

        print(f"\nResult: {result['success']}")
        print(f"Stats: {result.get('stats')}")
        if 'db_stats' in result:
            print(f"Database: {result['db_stats']}")
        return

    if args.command == 'priority':
        result = collector.collect_priority_commodities(start_date, end_date)
        print(f"\nCollected {len(result['records'])} records for priority commodities")

        if args.save_db:
            db_stats = collector.save_to_bronze(result['records'])
            print(f"Database: {db_stats}")
        return

    if args.command == 'collect':
        if args.commodity:
            hs_codes = get_hs_codes_for_commodity(args.commodity)
            if not hs_codes:
                print(f"No HS codes found for {args.commodity}")
                return

            end_date = end_date or date.today()
            start_date = start_date or date(end_date.year - 1, 1, 1)

            all_records = []
            flows = ['imports', 'exports'] if args.flow == 'both' else [args.flow]

            for flow in flows:
                for hs_code in hs_codes:
                    records = collector.fetch_trade_data(
                        flow, hs_code, start_date, end_date
                    )
                    all_records.extend(records)

            print(f"\nCollected {len(all_records)} records for {args.commodity}")

            if args.save_db and all_records:
                db_stats = collector.save_to_bronze(all_records)
                print(f"Database: {db_stats}")
        else:
            print("Specify --commodity or use 'group' command")


if __name__ == '__main__':
    main()
