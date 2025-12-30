#!/usr/bin/env python3
"""
Pull Census Bureau Trade Data

Downloads monthly import/export data from US Census Bureau API and:
1. Saves to PostgreSQL database
2. Updates Excel model files (using win32com to preserve external links)

Commodities tracked:
- Soybeans (HS 1201)
- Soybean Meal (HS 2304)
- Soybean Oil (HS 1507)

Usage:
    python scripts/pull_census_trade.py --commodity soybeans --years 5
    python scripts/pull_census_trade.py --commodity all --save-to-db --update-excel

    # Test with Models folder outside Dropbox (on Desktop):
    python scripts/pull_census_trade.py --commodity SOYBEANS --years 1 --update-excel --models-path "C:\\Users\\torem\\OneDrive\\Desktop\\Models\\Oilseeds"
"""

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dotenv import load_dotenv

# Add project root to path and load environment variables from project root
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env from project root (not current working directory)
load_dotenv(PROJECT_ROOT / '.env')

# Also load from api Manager/.env if it exists (for database credentials)
api_manager_env = PROJECT_ROOT / 'api Manager' / '.env'
if api_manager_env.exists():
    load_dotenv(api_manager_env)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

# Census API base URL
CENSUS_API_BASE = "https://api.census.gov/data/timeseries/intltrade"

# HS Codes for commodities (6-digit codes from RLC HS codes reference)
# Multiple codes per commodity - will fetch and aggregate data from all codes
# Source: Models/HS codes reference sheet
HS_CODES = {
    'SOYBEANS': ['120110', '120190'],           # 1201.10 - Seed, 1201.90 - Other
    'SOYBEAN_MEAL': ['120810', '230400'],       # 1208.10 - Soy flour/meal, 2304.00 - Oilcake
    'SOYBEAN_HULLS': ['230250'],                # 2302.50 - Soybean hulls (separate)
    'SOYBEAN_MEAL': ['120810', '230400', '230499'],  # 1208.10 - Soy flour/meal, 2304.00/99 - Oilcake
    'SOYBEAN_HULLS': ['230250'],                # 2302.50 - Soybean meal hulls (separate)
    'SOYBEAN_OIL': ['150710', '150790'],        # 1507.10 - Crude, 1507.90 - Other
}

# Alternative 4-digit codes for fallback (GTT codes)
HS_CODES_4DIGIT = {
    'SOYBEANS': ['1201'],
    'SOYBEAN_MEAL': ['1208', '2304'],           # Both chapters for soy flour/meal and oilcake
    'SOYBEAN_MEAL': ['1208', '2304'],  # Both soy flour and oilcake
    'SOYBEAN_HULLS': ['2302'],
    'SOYBEAN_OIL': ['1507'],
}

# Commodity display names
COMMODITY_NAMES = {
    'SOYBEANS': 'Soybeans',
    'SOYBEAN_MEAL': 'Soybean Meal',
    'SOYBEAN_HULLS': 'Soybean Hulls',
    'SOYBEAN_OIL': 'Soybean Oil',
}

# Excel file mappings
EXCEL_FILES = {
    'SOYBEANS': 'Models/Oilseeds/US Soybean Trade.xlsx',
    'SOYBEAN_MEAL': 'Models/Oilseeds/US Soybean Trade.xlsx',
    'SOYBEAN_HULLS': 'Models/Oilseeds/US Soybean Trade.xlsx',
    'SOYBEAN_OIL': 'Models/Oilseeds/US Soybean Trade.xlsx',
}

# Excel sheet names for Census data (matching actual sheet names in workbook)
# Note: Hulls data goes to specific rows within the Soymeal sheets
COMMODITY_SHEETS = {
    'SOYBEANS': {'exports': 'Soybean Exports', 'imports': 'Soybean Imports'},
    'SOYBEAN_MEAL': {'exports': 'Soymeal Exports', 'imports': 'Soymeal Imports'},
    'SOYBEAN_HULLS': {'exports': 'Soymeal Exports', 'imports': 'Soymeal Imports'},  # Same sheet, different rows
    'SOYBEAN_OIL': {'exports': 'Soyoil Exports', 'imports': 'Soyoil Imports'},
}

# Marketing year start months (same as inspections)
MARKETING_YEAR_START = {
    'SOYBEANS': 9,      # September
    'SOYBEAN_MEAL': 10, # October (follows crush)
    'SOYBEAN_HULLS': 10, # October (follows crush, same as meal)
    'SOYBEAN_OIL': 10,  # October (follows crush)
    'CORN': 9,
    'WHEAT': 6,
}

# Conversion factors - Census API reports quantity in KILOGRAMS
# Each commodity has different target units in Excel
# Format: commodity -> (target_unit_name, kg_to_target_multiplier)
UNIT_CONVERSIONS = {
    # Soybeans: Census kg -> 1000 bushels (Excel uses 1000 bu)
    # 1 kg = 0.0367437 bushels, so 1000 bu = 1000/0.0367437 kg = 27,217.7 kg
    'SOYBEANS': ('1000 BU', 0.0367437 / 1000),  # kg to 1000 bushels

    # Soybean Meal: Census kg -> short tons (Excel uses short tons)
    # 1 short ton = 907.185 kg
    'SOYBEAN_MEAL': ('ST', 1 / 907.185),  # kg to short tons

    # Soybean Hulls: same as meal
    'SOYBEAN_HULLS': ('ST', 1 / 907.185),  # kg to short tons

    # Soybean Oil: Census kg -> 1000 pounds (Excel uses 1000 lbs)
    # 1 kg = 2.20462 lbs
    'SOYBEAN_OIL': ('1000 LBS', 2.20462 / 1000),  # kg to 1000 lbs
}

# Special commodity total rows (for commodities that aggregate to a single row)
# Used for hulls which go to a specific row in the Soymeal sheets
COMMODITY_TOTAL_ROWS = {
    'SOYBEAN_HULLS': 294,  # Hulls total row in Soymeal Exports/Imports sheets
}
# =============================================================================
# UNIT CONVERSION FACTORS
# =============================================================================
# US units by commodity type:
#   - Grains (soybeans, corn, wheat): Bushels
#   - Oilseed products (meal, hulls): Short tons
#   - Oils/fats/greases: Pounds
# International: Metric tons for everything

# Conversion constants
KG_PER_MT = 1000.0              # 1 metric ton = 1000 kg
LBS_PER_MT = 2204.62            # 1 metric ton = 2204.62 lbs
LBS_PER_SHORT_TON = 2000.0      # 1 short ton = 2000 lbs
SHORT_TONS_PER_MT = 1.10231     # 1 metric ton = 1.10231 short tons
LBS_PER_BUSHEL_SOYBEANS = 60.0  # 1 bushel soybeans = 60 lbs
BUSHELS_PER_MT_SOYBEANS = 36.7437  # 1 MT = 36.7437 bushels (60 lbs/bu)

# Unit configuration per commodity for US trade files
# Census data comes in KG - these define how to convert for each commodity
# NOTE: Trade sheets use WHOLE NUMBERS (not thousands) - display may show "1,000 short tons" as unit label
US_UNIT_CONFIG = {
    'SOYBEANS': {
        'unit': 'bushels',
        'display_unit': '1000 bushels',
        'kg_to_display': lambda kg: kg / KG_PER_MT * BUSHELS_PER_MT_SOYBEANS / 1000,  # KG -> 1000 bu
    },
    'SOYBEAN_MEAL': {
        'unit': 'short_tons',
        'display_unit': 'short tons',
        'kg_to_display': lambda kg: kg / KG_PER_MT * SHORT_TONS_PER_MT,  # KG -> whole short tons
    },
    'SOYBEAN_HULLS': {
        'unit': 'short_tons',
        'display_unit': 'short tons',
        'kg_to_display': lambda kg: kg / KG_PER_MT * SHORT_TONS_PER_MT,  # KG -> whole short tons
    },
    'SOYBEAN_OIL': {
        'unit': 'pounds',
        'display_unit': '1000 lbs',
        # Census reports quantity in KG for soybean oil
        # Convert: KG -> lbs -> 1000 lbs
        # 1 KG = 2.20462 lbs, divide by 1000 for "thousand lbs"
        'kg_to_display': lambda kg: (kg * LBS_PER_MT / KG_PER_MT) / 1000,  # KG -> 1000 lbs
    },
}

# International trade files use metric tons
INTL_UNIT_CONFIG = {
    'unit': 'metric_tons',
    'display_unit': '1000 tonnes',
    'kg_to_display': lambda kg: kg / 1000000,  # KG -> 1000 MT (million kg)
}

# Map Excel files to unit systems
EXCEL_UNIT_SYSTEM = {
    'US Soybean Trade.xlsx': 'US',      # Uses commodity-specific US units
    'World Rapeseed Trade.xlsx': 'INTL',  # Uses metric tons
    'World Soybean Trade.xlsx': 'INTL',   # Uses metric tons
}

# Fallback price estimates for quantity estimation when Census doesn't provide quantity
# These are approximate average export prices in USD per metric ton
# Used only when quantity data is missing
FALLBACK_PRICES_USD_PER_MT = {
    'SOYBEANS': 450,       # ~$12/bu average
    'SOYBEAN_MEAL': 400,   # ~$360-440/short ton
    'SOYBEAN_HULLS': 200,  # Lower value product
    'SOYBEAN_OIL': 1000,   # ~$0.45/lb average
}

# HS code to specific total row mapping (for commodities where different HS codes
# go to different rows, like crude vs refined oil)
# Format: commodity -> {hs_code: row_number}
HS_CODE_TOTAL_ROWS = {
    'SOYBEAN_OIL': {
        '150710': 11,  # Crude soybean oil total row
        '150790': 17,  # Other/refined soybean oil total row
    },
}

# Destination to row mapping (same as inspections for consistency)
# These match the Excel file layout
DESTINATION_ROWS = {
    # Row 4 is region total for Asia/Oceania
    'CHINA': 88,
    'JAPAN': 89,
    'KOREA': 90,  # South Korea
    'TAIWAN': 91,
    'INDONESIA': 92,
    'MALAYSIA': 93,
    'PHILIPPINES': 94,
    'THAILAND': 95,
    'VIETNAM': 96,
    'INDIA': 97,
    'PAKISTAN': 98,
    'BANGLADESH': 99,
    'SRI LANKA': 100,
    'SINGAPORE': 101,
    'HONG KONG': 102,
    'AUSTRALIA': 103,
    'NEW ZEALAND': 104,

    # European Union (Row 37 is region total)
    'NETHERLANDS': 120,
    'GERMANY': 121,
    'SPAIN': 122,
    'ITALY': 123,
    'PORTUGAL': 124,
    'FRANCE': 125,
    'BELGIUM': 126,
    'UNITED KINGDOM': 127,
    'IRELAND': 128,
    'POLAND': 129,
    'DENMARK': 130,
    'GREECE': 131,
    'AUSTRIA': 132,
    'SWEDEN': 133,
    'FINLAND': 134,
    'CZECH REPUBLIC': 135,
    'HUNGARY': 136,
    'ROMANIA': 137,
    'BULGARIA': 138,
    'CROATIA': 139,
    'SLOVENIA': 140,
    'SLOVAKIA': 141,
    'LITHUANIA': 142,
    'LATVIA': 143,
    'ESTONIA': 144,
    'CYPRUS': 145,
    'MALTA': 146,
    'LUXEMBOURG': 147,

    # Middle East & Africa (Row 59 is region total)
    'EGYPT': 170,
    'MOROCCO': 171,
    'ALGERIA': 172,
    'TUNISIA': 173,
    'TURKEY': 174,
    'ISRAEL': 175,
    'SAUDI ARABIA': 176,
    'UNITED ARAB EMIRATES': 177,
    'JORDAN': 178,
    'LEBANON': 179,
    'IRAQ': 180,
    'IRAN': 181,
    'NIGERIA': 182,
    'SOUTH AFRICA': 183,
    'KENYA': 184,
    'ETHIOPIA': 185,
    'SUDAN': 186,
    'GHANA': 187,
    'SENEGAL': 188,
    'COTE D IVOIRE': 189,
    'MOZAMBIQUE': 190,
    'TANZANIA': 191,

    # Western Hemisphere (Row 74 is region total)
    'MEXICO': 200,
    'CANADA': 201,
    'BRAZIL': 202,
    'ARGENTINA': 203,
    'COLOMBIA': 204,
    'VENEZUELA': 205,
    'PERU': 206,
    'CHILE': 207,
    'ECUADOR': 208,
    'GUATEMALA': 209,
    'HONDURAS': 210,
    'EL SALVADOR': 211,
    'NICARAGUA': 212,
    'COSTA RICA': 213,
    'PANAMA': 214,
    'DOMINICAN REPUBLIC': 215,
    'JAMAICA': 216,
    'HAITI': 217,
    'TRINIDAD AND TOBAGO': 218,
    'CUBA': 219,
    'PUERTO RICO': 220,
    'PARAGUAY': 221,
    'URUGUAY': 222,
    'BOLIVIA': 223,

    # Former Soviet Union (Row 164 is region total)
    'RUSSIA': 250,
    'UKRAINE': 251,
    'KAZAKHSTAN': 252,
    'UZBEKISTAN': 253,
    'BELARUS': 254,
    'GEORGIA': 255,
    'AZERBAIJAN': 256,
    'ARMENIA': 257,
    'MOLDOVA': 258,
    'TURKMENISTAN': 259,
    'KYRGYZSTAN': 260,
    'TAJIKISTAN': 261,
}

# Region totals rows
REGION_TOTAL_ROWS = {
    'ASIA_OCEANIA': 4,
    'EU': 37,
    'MIDDLE_EAST_AFRICA': 59,
    'WESTERN_HEMISPHERE': 74,
    'FSU': 164,
    'WORLD_TOTAL': 290,
}

# Rows that contain SUM formulas - these should NOT be cleared or overwritten
# These are regional totals that sum the country rows below them
SUM_FORMULA_ROWS = {
    4,    # EU
    37,   # Other Europe
    59,   # FSU
    74,   # Asia_Oceana
    164,  # Africa
    231,  # Western Hemisphere
    289,  # World Total SUM formula (preserve this!)
    # Row 290 is external link data, NOT a sum - so it can be cleared/written
    # Rows 291+ are outside clearing range (5-290) but listing for documentation:
    # 293 = Meal total (formula)
    # 294 = Hull total (written by SOYBEAN_HULLS)
    # 295 = Grand total = 293 + 294 (formula)
}

# Map Census aggregate/total names to Excel row numbers
# Census returns aggregates like "TOTAL FOR ALL COUNTRIES", "ASIA", "EUROPE", etc.
# Set to None for aggregates that should be SKIPPED (calculated by formulas in Excel)
CENSUS_AGGREGATE_ROWS = {
    'TOTAL FOR ALL COUNTRIES': 290,  # Row 290 gets Census total (external link data)
    # Skip regional aggregates - they have SUM formulas in Excel:
    'ASIA': None,          # Skip - row 4 has formula
    'EUROPE': None,        # Skip - row 37 has formula
    'AFRICA': None,        # Skip - row 59 has formula
    'NORTH AMERICA': None, # Skip - row 74 has formula
    # Additional aggregates that Census returns - skip these as they're calculated in Excel:
    'OECD': None,
    'APEC': None,
    'NATO': None,
    'LAFTA': None,
    'PACIFIC RIM COUNTRIES': None,
    'TWENTY LATIN AMERICAN REPUBLICS': None,
    'USMCA (NAFTA)': None,
    'CAFTA-DR': None,
    'CACM': None,
    'ASEAN': None,
    'EURO AREA': None,
    'SOUTH AMERICA': None,
    'CENTRAL AMERICA': None,
}

# Census country codes to names mapping (partial - add more as needed)
CENSUS_COUNTRY_CODES = {
    '5700': 'CHINA',
    '5880': 'TAIWAN',
    '5800': 'JAPAN',
    '5801': 'KOREA',  # South Korea
    '5600': 'INDONESIA',
    '5570': 'MALAYSIA',
    '5660': 'PHILIPPINES',
    '5490': 'BANGLADESH',
    '5330': 'INDIA',
    '5350': 'PAKISTAN',
    '5830': 'THAILAND',
    '5850': 'VIETNAM',
    '4210': 'NETHERLANDS',
    '4280': 'GERMANY',
    '4700': 'SPAIN',
    '4759': 'ITALY',
    '4220': 'BELGIUM',
    '4279': 'UNITED KINGDOM',
    '4129': 'FRANCE',
    '4550': 'PORTUGAL',
    '2010': 'MEXICO',
    '1220': 'CANADA',
    '3510': 'BRAZIL',
    '3570': 'ARGENTINA',
    '7320': 'EGYPT',
    '7140': 'MOROCCO',
    '4610': 'TURKEY',
    '5080': 'ISRAEL',
}


# =============================================================================
# API FUNCTIONS
# =============================================================================

def get_api_key() -> Optional[str]:
    """Get Census API key from environment"""
    return os.getenv('CENSUS_API_KEY')


# Track what units Census returns for debugging
_census_units_seen = set()
def test_census_api(api_key: str = None) -> bool:
    """
    Test the Census API connection with a simple request.

    Returns True if the API is working, False otherwise.
    Prints diagnostic information.
    """
    print("\n--- Census API Connection Test ---")

    # Test with a known good query: Soybeans exports for a recent complete month
    test_url = f"{CENSUS_API_BASE}/exports/hs"
    test_params = {
        'get': 'ALL_VAL_MO,CTY_NAME',
        'E_COMMODITY': '120190',
        'time': '2024-01',  # Use a known complete month
    }

    if api_key:
        test_params['key'] = api_key
        print(f"  Using API key: {api_key[:8]}...")
    else:
        print("  No API key (using public access)")

    print(f"  Test URL: {test_url}")
    print(f"  Test params: {test_params}")

    try:
        response = requests.get(test_url, params=test_params, timeout=30)
        print(f"  Response status: {response.status_code}")
        print(f"  Response headers: {dict(response.headers)}")

        if response.status_code == 200:
            # Check content type
            content_type = response.headers.get('Content-Type', '')
            print(f"  Content-Type: {content_type}")

            # Show first 500 chars of response
            content_preview = response.text[:500] if response.text else "(empty)"
            print(f"  Response preview: {content_preview}")

            if 'json' in content_type.lower() or response.text.strip().startswith('['):
                try:
                    data = response.json()
                    print(f"  JSON parsed successfully: {len(data)} rows")
                    if len(data) > 1:
                        print(f"  Sample data: {data[1][:3] if len(data[1]) > 3 else data[1]}")
                    print("  API TEST: SUCCESS")
                    return True
                except Exception as e:
                    print(f"  JSON parse error: {e}")
                    print("  API TEST: FAILED (invalid JSON)")
                    return False
            else:
                print("  API TEST: FAILED (not JSON response)")
                return False
        else:
            print(f"  Response text: {response.text[:500]}")
            print(f"  API TEST: FAILED (status {response.status_code})")
            return False

    except Exception as e:
        print(f"  Request error: {e}")
        print("  API TEST: FAILED (connection error)")
        return False


def fetch_trade_data(
    flow: str,
    hs_code: str,
    year: int,
    month: int,
    api_key: str = None,
    debug: bool = False
    max_retries: int = 3
) -> List[Dict]:
    """
    Fetch trade data from Census API for a specific month

    Args:
        flow: 'exports' or 'imports'
        hs_code: HS code (e.g., '1201' for soybeans)
        year: Year
        month: Month (1-12)
        api_key: Census API key (optional but recommended)
        debug: Print debug info about API response
        max_retries: Maximum number of retry attempts

    Returns:
        List of trade records
    """
    global _census_units_seen
    import time as time_module
    import json

    # Build URL
    url = f"{CENSUS_API_BASE}/{flow}/hs"

    # Field names differ between imports and exports
    # See: https://api.census.gov/data/timeseries/intltrade/exports/hs/variables.html
    # See: https://api.census.gov/data/timeseries/intltrade/imports/hs/variables.html
    # IMPORTANT: Census API returns 0 for BOTH true zeros AND missing values!
    # We must request the FLAG fields to distinguish between them.
    # Flag values: blank = real data, 'A' = suppressed, 'N' = not available
    if flow == 'imports':
        commodity_field = 'I_COMMODITY'
        value_field = 'GEN_VAL_MO'
        # Quantity fields and their corresponding flag fields for imports
        qty_fields = ['GEN_QY1_MO', 'CON_QY1_MO']
        qty_flag_fields = ['GEN_QY1_MO_FLAG', 'CON_QY1_MO_FLAG']
        unit_field = 'UNIT_QY1'
    else:
        commodity_field = 'E_COMMODITY'
        value_field = 'ALL_VAL_MO'
        # Quantity fields and their corresponding flag fields for exports
        qty_fields = ['QTY_1_MO', 'QTY_2_MO']
        qty_flag_fields = ['QTY_1_MO_FLAG', 'QTY_2_MO_FLAG']
        unit_field = 'UNIT_QY1'

    # Build params - request quantity fields
    # NOTE: FLAG fields (QTY_1_MO_FLAG, etc.) may not be available in all API endpoints
    # If they cause errors, we fall back to just the quantity fields
    time_str = f"{year}-{month:02d}"

    # Try with FLAG fields first, fall back without if needed
    # For now, request without FLAGS to avoid API errors - we'll handle missing data differently
    get_fields = f'{value_field},{",".join(qty_fields)},{unit_field},UNIT_QY2,CTY_CODE,CTY_NAME'
    params = {
        'get': get_fields,
        commodity_field: hs_code,
        'time': time_str,
    }

    if api_key:
        params['key'] = api_key

    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 200:
                # Check if response is empty or not JSON
                if not response.text or response.text.strip() == '':
                    logger.warning(f"Empty response for {flow}/{hs_code} {time_str}")
                    return []

                    # Track units seen for debugging
                    unit = record.get(unit_field, '')
                    if unit and unit not in _census_units_seen:
                        _census_units_seen.add(unit)
                        logger.info(f"Census API unit discovered: '{unit}' for HS {hs_code}")

                    records.append({
                        'year': year,
                        'month': month,
                        'date': date(year, month, 1),
                        'flow': flow,
                        'hs_code': hs_code,
                        'country_code': record.get('CTY_CODE', ''),
                        'country_name': clean_country_name(record.get('CTY_NAME', '')),
                        'value_usd': value_usd,
                        'quantity': quantity,
                        'unit': unit,
                    })

                # Debug output for specific HS code/period combinations
                if debug and records:
                    total_qty = sum(r.get('quantity', 0) or 0 for r in records)
                    sample_unit = records[0].get('unit', 'UNKNOWN')
                    print(f"    DEBUG {flow}/{hs_code} {time_str}: {len(records)} countries, total qty={total_qty:,.0f} {sample_unit}")

                return records
        elif response.status_code == 204:
            # No content - no data for this period
            logger.warning(f"API returned 204 for {flow}/{hs_code} {time_str}")
        elif response.status_code == 400:
            # Bad request - log the actual error response
            try:
                error_text = response.text[:200]
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    # Log the actual response content for debugging
                    content_preview = response.text[:500] if response.text else "(empty)"
                    logger.error(f"JSON decode error for {flow}/{hs_code} {time_str}: {e}")
                    print(f"  ERROR: Census API returned non-JSON response:")
                    print(f"  {content_preview}")

                    # Retry on JSON errors (might be transient)
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt  # Exponential backoff: 1, 2, 4 seconds
                        logger.info(f"Retrying in {wait_time}s...")
                        time_module.sleep(wait_time)
                        continue
                    return []

                if data and len(data) > 1:
                    headers = data[0]
                    records = []

                    # Debug: Log the headers and sample data we received
                    if len(data) > 1:
                        logger.info(f"Census API returned headers for {flow}/{hs_code}: {headers}")
                        # Log first row to see quantity values
                        sample = dict(zip(headers, data[1]))
                        qty_info = []
                        for qf in qty_fields:
                            qty_info.append(f"{qf}={sample.get(qf)}")
                        unit_info = f"UNIT_QY1={sample.get('UNIT_QY1')}"
                        logger.info(f"Sample data: {'; '.join(qty_info)}; {unit_info}")

                    for row in data[1:]:
                        record = dict(zip(headers, row))

                        # Parse values
                        value_usd = parse_number(record.get(value_field))

                        # Try each quantity field until we find one with data
                        # Note: Census returns 0 for missing values, so we look for non-zero values
                        quantity = None
                        unit = None
                        for i, qty_field in enumerate(qty_fields):
                            q = parse_number(record.get(qty_field))
                            if q is not None and q > 0:
                                quantity = q
                                # Get corresponding unit
                                if 'QY1' in qty_field or i == 0:
                                    unit = record.get('UNIT_QY1', '')
                                else:
                                    unit = record.get('UNIT_QY2', record.get('UNIT_QY1', ''))
                                break  # Found non-zero data, use it

                        # NORMALIZE QUANTITY TO KG
                        # Census reports different units for different HS codes:
                        # - 150710 (crude SBO): MT (metric tons)
                        # - 150790 (refined SBO): KG (kilograms)
                        # Normalize everything to KG for consistent downstream processing
                        if quantity is not None and unit:
                            unit_upper = unit.upper().strip()
                            if unit_upper in ('MT', 'T', 'METRIC TON', 'METRIC TONS'):
                                # Convert MT to KG (multiply by 1000)
                                quantity = quantity * 1000
                                logger.debug(f"Converted {quantity/1000} MT to {quantity} KG for HS {hs_code}")
                            elif unit_upper in ('LB', 'LBS', 'POUND', 'POUNDS'):
                                # Convert LBS to KG (divide by 2.20462)
                                quantity = quantity / 2.20462
                            # KG stays as-is

                        # If no quantity found but we have value, skip this record's quantity
                        # (we'll still save it to DB with quantity=None)
                        if value_usd is None and quantity is None:
                            continue

                        records.append({
                            'year': year,
                            'month': month,
                            'date': date(year, month, 1),
                            'flow': flow,
                            'hs_code': hs_code,
                            'country_code': record.get('CTY_CODE', ''),
                            'country_name': clean_country_name(record.get('CTY_NAME', '')),
                            'value_usd': value_usd,
                            'quantity': quantity,
                            'unit': unit or '',
                        })

                    return records
                else:
                    # Valid JSON but no data
                    return []

            elif response.status_code == 204:
                # No content - no data for this period (normal for recent months)
                logger.warning(f"API returned 204 for {flow}/{hs_code} {time_str}")
                return []

            elif response.status_code == 400:
                # Bad request - log the actual error response
                error_text = response.text[:300] if response.text else "(empty)"
                logger.warning(f"API returned 400 for {flow}/{hs_code} {time_str}: {error_text}")
                return []

            elif response.status_code in (429, 500, 502, 503, 504):
                # Rate limit or server error - retry with backoff
                if attempt < max_retries - 1:
                    wait_time = 2 ** (attempt + 1)  # 2, 4, 8 seconds
                    logger.warning(f"API returned {response.status_code} for {flow}/{hs_code} {time_str}, retrying in {wait_time}s...")
                    time_module.sleep(wait_time)
                    continue
                else:
                    logger.error(f"API returned {response.status_code} for {flow}/{hs_code} {time_str} after {max_retries} attempts")
                    return []
            else:
                logger.warning(f"API returned {response.status_code} for {flow}/{hs_code} {time_str}")
                return []

        except requests.exceptions.ConnectionError as e:
            # Connection reset, timeout, etc - retry
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                logger.warning(f"Connection error for {flow}/{hs_code} {time_str}: {e}, retrying in {wait_time}s...")
                time_module.sleep(wait_time)
                continue
            else:
                logger.error(f"Connection error for {flow}/{hs_code} {time_str} after {max_retries} attempts: {e}")
                return []

        except requests.RequestException as e:
            logger.error(f"Request failed for {flow}/{hs_code} {time_str}: {e}")
            return []

    return []


def fetch_commodity_data(
    commodity: str,
    start_date: date,
    end_date: date,
    flow: str = 'both',
    api_key: str = None
) -> List[Dict]:
    """
    Fetch all trade data for a commodity over a date range

    Args:
        commodity: Commodity name (SOYBEANS, SOYBEAN_MEAL, SOYBEAN_OIL)
        start_date: Start date
        end_date: End date
        flow: 'exports', 'imports', or 'both'
        api_key: Census API key

    Returns:
        List of all trade records
    """
    # Get HS codes (now lists to support multiple codes per commodity)
    hs_codes_6 = HS_CODES.get(commodity.upper(), [])
    hs_codes_4 = HS_CODES_4DIGIT.get(commodity.upper(), [])

    if not hs_codes_6 and not hs_codes_4:
        logger.error(f"Unknown commodity: {commodity}")
        return []

    flows = ['exports', 'imports'] if flow == 'both' else [flow]
    all_records = []

    # Track which HS codes work for each flow
    working_codes = {}

    # Iterate through months
    current = date(start_date.year, start_date.month, 1)
    total_months = ((end_date.year - start_date.year) * 12 +
                    end_date.month - start_date.month + 1)
    months_processed = 0

    while current <= end_date:
        months_processed += 1

        for trade_flow in flows:
            flow_records = []

            # Use working codes if we've already found them
            if trade_flow in working_codes:
                for hs_code in working_codes[trade_flow]:
                    records = fetch_trade_data(
                        flow=trade_flow,
                        hs_code=hs_code,
                        year=current.year,
                        month=current.month,
                        api_key=api_key
                    )
                    flow_records.extend(records)
            else:
                # Try 6-digit codes first
                codes_that_work = []
                for hs_code in hs_codes_6:
                    records = fetch_trade_data(
                        flow=trade_flow,
                        hs_code=hs_code,
                        year=current.year,
                        month=current.month,
                        api_key=api_key
                    )
                    if records:
                        codes_that_work.append(hs_code)
                        flow_records.extend(records)
                        logger.info(f"Using 6-digit code {hs_code} for {trade_flow}")

                # If no 6-digit codes worked, try 4-digit
                if not flow_records and hs_codes_4:
                    for hs_code in hs_codes_4:
                        records = fetch_trade_data(
                            flow=trade_flow,
                            hs_code=hs_code,
                            year=current.year,
                            month=current.month,
                            api_key=api_key
                        )
                        if records:
                            codes_that_work.append(hs_code)
                            flow_records.extend(records)
                            logger.info(f"Using 4-digit code {hs_code} for {trade_flow}")

                if codes_that_work:
                    working_codes[trade_flow] = codes_that_work

            records = flow_records

            # Add commodity info to records
            for r in records:
                r['commodity'] = commodity.upper()
                r['marketing_year'] = get_marketing_year(commodity, r['date'])

            all_records.extend(records)

        # Progress update every 12 months
        if months_processed % 12 == 0:
            logger.info(f"  Processed {months_processed}/{total_months} months...")

        # Move to next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return all_records


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def parse_number(value: Any) -> Optional[float]:
    """Safely parse a number"""
    if value is None or value == '':
        return None
    try:
        return float(str(value).replace(',', ''))
    except (ValueError, TypeError):
        return None


def estimate_quantity_from_value(value_usd: float, commodity: str) -> Optional[float]:
    """
    Estimate quantity (in kg) from USD value using fallback prices.

    Used when Census API doesn't return quantity data.
    Returns quantity in KG to match Census quantity format.

    Args:
        value_usd: Trade value in USD
        commodity: Commodity name (SOYBEANS, SOYBEAN_MEAL, etc.)

    Returns:
        Estimated quantity in KG, or None if can't estimate
    """
    if value_usd is None or value_usd <= 0:
        return None

    price_per_mt = FALLBACK_PRICES_USD_PER_MT.get(commodity.upper())
    if not price_per_mt:
        return None

    # Calculate quantity in MT, then convert to KG
    quantity_mt = value_usd / price_per_mt
    quantity_kg = quantity_mt * 1000

    return quantity_kg


def clean_country_name(name: str) -> str:
    """Clean and standardize country name to match Excel destination names"""
    if not name:
        return ''

    name = name.upper().strip()

    # Comprehensive Census to Excel name mapping
    replacements = {
        # Asia
        'KOREA, SOUTH': 'KOREA',
        'KOREA, REPUBLIC OF': 'KOREA',
        'REPUBLIC OF KOREA': 'KOREA',
        'KOREA, NORTH': 'NORTH KOREA',  # Exclude from mapping
        'KOREA, DEMOCRATIC PEOPLES REPUBLIC OF': 'NORTH KOREA',
        'DEMOCRATIC PEOPLES REPUBLIC OF KOREA': 'NORTH KOREA',
        'TAIWAN, PROVINCE OF CHINA': 'TAIWAN',
        'CHINA, PEOPLES REPUBLIC OF': 'CHINA',
        'PEOPLES REPUBLIC OF CHINA': 'CHINA',
        'VIETNAM, SOCIALIST REPUBLIC OF': 'VIETNAM',
        'HONG KONG, CHINA': 'HONG KONG',
        'HONG KONG SAR': 'HONG KONG',

        # Europe
        'RUSSIAN FEDERATION': 'RUSSIA',
        'CZECH REPUBLIC': 'CZECH REPUBLIC',
        'CZECHIA': 'CZECH REPUBLIC',
        'UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND': 'UNITED KINGDOM',
        'GREAT BRITAIN': 'UNITED KINGDOM',
        'UK': 'UNITED KINGDOM',
        'HOLLAND': 'NETHERLANDS',
        'FEDERAL REPUBLIC OF GERMANY': 'GERMANY',

        # Middle East
        'IRAN, ISLAMIC REPUBLIC OF': 'IRAN',
        'ISLAMIC REPUBLIC OF IRAN': 'IRAN',
        'UNITED ARAB EMIRATES': 'UNITED ARAB EMIRATES',
        'UAE': 'UNITED ARAB EMIRATES',
        'SYRIAN ARAB REPUBLIC': 'SYRIA',

        # Africa
        'COTE D\'IVOIRE': 'COTE D IVOIRE',
        'IVORY COAST': 'COTE D IVOIRE',
        'DEMOCRATIC REPUBLIC OF THE CONGO': 'DR CONGO',
        'CONGO, DEMOCRATIC REPUBLIC OF': 'DR CONGO',
        'REPUBLIC OF SOUTH AFRICA': 'SOUTH AFRICA',

        # Americas
        'UNITED STATES': 'USA',  # Shouldn't appear but just in case
        'BOLIVIA, PLURINATIONAL STATE OF': 'BOLIVIA',
        'VENEZUELA, BOLIVARIAN REPUBLIC OF': 'VENEZUELA',

        # Other variations Census might use
        'BURMA': 'MYANMAR',
        'REPUBLIC OF THE PHILIPPINES': 'PHILIPPINES',
        'KINGDOM OF THAILAND': 'THAILAND',
        'REPUBLIC OF INDONESIA': 'INDONESIA',
        'FEDERATION OF MALAYSIA': 'MALAYSIA',
        'SOCIALIST REPUBLIC OF VIETNAM': 'VIETNAM',
        'REPUBLIC OF INDIA': 'INDIA',
        'ISLAMIC REPUBLIC OF PAKISTAN': 'PAKISTAN',
        'PEOPLES REPUBLIC OF BANGLADESH': 'BANGLADESH',
        'ARAB REPUBLIC OF EGYPT': 'EGYPT',
        'KINGDOM OF MOROCCO': 'MOROCCO',
        'KINGDOM OF SAUDI ARABIA': 'SAUDI ARABIA',
        'STATE OF ISRAEL': 'ISRAEL',
        'REPUBLIC OF TURKEY': 'TURKEY',
        'TURKIYE': 'TURKEY',
    }

    return replacements.get(name, name)


def get_marketing_year(commodity: str, dt: date) -> str:
    """Get marketing year string for a date"""
    start_month = MARKETING_YEAR_START.get(commodity.upper(), 9)

    if dt.month >= start_month:
        my_start = dt.year
    else:
        my_start = dt.year - 1

    return f"{my_start}/{str(my_start + 1)[2:]}"


def get_destination_region(destination: str) -> str:
    """Map destination country to region"""
    ASIA_OCEANIA = ['CHINA', 'JAPAN', 'KOREA', 'TAIWAN', 'INDONESIA', 'MALAYSIA',
                   'PHILIPPINES', 'THAILAND', 'VIETNAM', 'INDIA', 'PAKISTAN',
                   'BANGLADESH', 'SRI LANKA', 'SINGAPORE', 'HONG KONG',
                   'AUSTRALIA', 'NEW ZEALAND', 'CAMBODIA', 'MYANMAR', 'LAOS']

    EU = ['NETHERLANDS', 'GERMANY', 'SPAIN', 'ITALY', 'PORTUGAL', 'FRANCE',
          'BELGIUM', 'UNITED KINGDOM', 'IRELAND', 'POLAND', 'DENMARK', 'GREECE',
          'AUSTRIA', 'SWEDEN', 'FINLAND', 'CZECH REPUBLIC', 'HUNGARY', 'ROMANIA',
          'BULGARIA', 'CROATIA', 'SLOVENIA', 'SLOVAKIA', 'LITHUANIA', 'LATVIA',
          'ESTONIA', 'CYPRUS', 'MALTA', 'LUXEMBOURG']

    MIDDLE_EAST_AFRICA = ['EGYPT', 'MOROCCO', 'ALGERIA', 'TUNISIA', 'TURKEY',
                          'ISRAEL', 'SAUDI ARABIA', 'UNITED ARAB EMIRATES',
                          'JORDAN', 'LEBANON', 'IRAQ', 'IRAN', 'NIGERIA',
                          'SOUTH AFRICA', 'KENYA', 'ETHIOPIA', 'SUDAN', 'GHANA',
                          'SENEGAL', 'COTE D IVOIRE', 'MOZAMBIQUE', 'TANZANIA']

    WESTERN_HEMISPHERE = ['MEXICO', 'CANADA', 'BRAZIL', 'ARGENTINA', 'COLOMBIA',
                          'VENEZUELA', 'PERU', 'CHILE', 'ECUADOR', 'GUATEMALA',
                          'HONDURAS', 'EL SALVADOR', 'NICARAGUA', 'COSTA RICA',
                          'PANAMA', 'DOMINICAN REPUBLIC', 'JAMAICA', 'HAITI',
                          'TRINIDAD AND TOBAGO', 'CUBA', 'PUERTO RICO',
                          'PARAGUAY', 'URUGUAY', 'BOLIVIA']

    FSU = ['RUSSIA', 'UKRAINE', 'KAZAKHSTAN', 'UZBEKISTAN', 'BELARUS',
           'GEORGIA', 'AZERBAIJAN', 'ARMENIA', 'MOLDOVA', 'TURKMENISTAN',
           'KYRGYZSTAN', 'TAJIKISTAN']

    dest_upper = destination.upper()

    if dest_upper in ASIA_OCEANIA:
        return 'ASIA_OCEANIA'
    elif dest_upper in EU:
        return 'EU'
    elif dest_upper in MIDDLE_EAST_AFRICA:
        return 'MIDDLE_EAST_AFRICA'
    elif dest_upper in WESTERN_HEMISPHERE:
        return 'WESTERN_HEMISPHERE'
    elif dest_upper in FSU:
        return 'FSU'
    else:
        return 'OTHER'


# =============================================================================
# DATA AGGREGATION
# =============================================================================

def aggregate_monthly_by_destination(
    records: List[Dict],
    flow: str = 'exports'
) -> Dict[Tuple, Dict]:
    """
    Aggregate records to monthly totals by destination

    Returns:
        Dict mapping (year, month, destination) to aggregated data
    """
    monthly = defaultdict(lambda: {
        'value_usd': 0,
        'quantity': 0,
        'count': 0,
        'region': None,
        'marketing_year': None
    })

    for r in records:
        if r.get('flow') != flow:
            continue

        key = (r['year'], r['month'], r['country_name'])
        monthly[key]['value_usd'] += r.get('value_usd') or 0
        monthly[key]['quantity'] += r.get('quantity') or 0
        monthly[key]['count'] += 1
        monthly[key]['region'] = get_destination_region(r['country_name'])
        monthly[key]['marketing_year'] = r.get('marketing_year')

    return dict(monthly)


def aggregate_monthly_by_hs_code(
    records: List[Dict],
    flow: str = 'exports'
) -> Dict[Tuple, Dict]:
    """
    Aggregate records to monthly totals by HS code (sum across all countries).
    Used for commodities like soybean oil where crude and refined oil
    go to separate total rows.

    Returns:
        Dict mapping (year, month, hs_code) to aggregated data
    """
    monthly = defaultdict(lambda: {
        'value_usd': 0,
        'quantity': 0,
        'count': 0,
        'marketing_year': None
    })

    for r in records:
        if r.get('flow') != flow:
            continue

        hs_code = r.get('hs_code', '')
        key = (r['year'], r['month'], hs_code)
        monthly[key]['value_usd'] += r.get('value_usd') or 0
        monthly[key]['quantity'] += r.get('quantity') or 0
        monthly[key]['count'] += 1
        monthly[key]['marketing_year'] = r.get('marketing_year')

    return dict(monthly)


def aggregate_by_marketing_year(
    records: List[Dict],
    flow: str = 'exports'
) -> Dict[Tuple, Dict]:
    """
    Aggregate records to marketing year totals by destination
    """
    yearly = defaultdict(lambda: {
        'value_usd': 0,
        'quantity': 0,
        'count': 0,
        'region': None
    })

    for r in records:
        if r.get('flow') != flow:
            continue

        key = (r.get('marketing_year', ''), r['country_name'])
        yearly[key]['value_usd'] += r.get('value_usd') or 0
        yearly[key]['quantity'] += r.get('quantity') or 0
        yearly[key]['count'] += 1
        yearly[key]['region'] = get_destination_region(r['country_name'])

    return dict(yearly)


# =============================================================================
# DATABASE OPERATIONS
# =============================================================================

DEFAULT_DB_PATH = Path(__file__).parent.parent / 'data' / 'census_trade.db'


def get_database_url() -> str:
    """
    Get database connection URL from environment variables.

    Priority:
    1. DATABASE_URL environment variable (if set)
    2. Construct from individual DB_* variables (DB_HOST, DB_NAME, etc.)
    3. Fall back to SQLite (last resort)

    Returns:
        Database connection string
    """
    # Check for explicit DATABASE_URL first
    database_url = os.getenv('DATABASE_URL')
    if database_url:
        return database_url

    # Try to construct from individual variables
    db_type = os.getenv('DB_TYPE', 'postgresql')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'rlc_commodities')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', '')

    if db_type == 'postgresql' and db_host and db_name and db_user:
        # Construct PostgreSQL URL
        if db_password:
            return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        else:
            return f"postgresql://{db_user}@{db_host}:{db_port}/{db_name}"

    # Fall back to SQLite as last resort
    return f'sqlite:///{DEFAULT_DB_PATH}'


def save_to_database(records: List[Dict], connection_string: str = None) -> int:
    """
    Save trade records to database

    Args:
        records: List of trade records
        connection_string: Database connection string

    Returns:
        Number of records saved
    """
    connection_string = connection_string or get_database_url()

    if connection_string.startswith('sqlite'):
        print(f"\nUsing SQLite database: {connection_string.replace('sqlite:///', '')}")
        logger.info(f"Using SQLite database: {connection_string}")
    else:
        # Mask password in log output
        display_url = connection_string
        if '@' in connection_string and ':' in connection_string.split('@')[0]:
            parts = connection_string.split('@')
            user_pass = parts[0].split('://')[-1]
            if ':' in user_pass:
                user = user_pass.split(':')[0]
                display_url = f"postgresql://{user}:****@{parts[1]}"
        print(f"\nSaving {len(records)} records to PostgreSQL: {display_url}")

    try:
        if connection_string.startswith('postgresql'):
            return _save_to_postgresql(records, connection_string)
        elif connection_string.startswith('sqlite'):
            return _save_to_sqlite(records, connection_string)
        else:
            print(f"ERROR: Unsupported database type: {connection_string.split(':')[0]}")
            return 0
    except Exception as e:
        print(f"ERROR: Failed to save to database: {e}")
        logger.error(f"Failed to save to database: {e}")
        import traceback
        traceback.print_exc()
        return 0


def _save_to_postgresql(records: List[Dict], connection_string: str) -> int:
    """Save records to PostgreSQL"""
    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
        return 0

    from urllib.parse import urlparse
    parsed = urlparse(connection_string)

    db_name = parsed.path[1:]
    print(f"Connecting to PostgreSQL: {parsed.hostname}:{parsed.port or 5432}/{db_name}")

    try:
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=db_name,
            user=parsed.username,
            password=parsed.password
        )
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if 'does not exist' in error_msg:
            print(f"\nERROR: Database '{db_name}' does not exist!")
            print(f"Create it first with: psql -U postgres -h localhost -c \"CREATE DATABASE {db_name}\"")
        else:
            print(f"\nERROR connecting to PostgreSQL: {e}")
        return 0

    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS census_trade_records (
            id BIGSERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            commodity VARCHAR(50) NOT NULL,
            hs_code VARCHAR(10) NOT NULL,
            flow VARCHAR(10) NOT NULL,
            country_code VARCHAR(10),
            country_name VARCHAR(100) NOT NULL,
            destination_region VARCHAR(50),
            value_usd NUMERIC(18,2),
            quantity NUMERIC(18,4),
            unit VARCHAR(20),
            marketing_year VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, commodity, flow, country_name)
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_census_date_commodity ON census_trade_records(trade_date, commodity)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_census_flow ON census_trade_records(flow)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_census_country ON census_trade_records(country_name)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_census_my ON census_trade_records(marketing_year)")

    # Create monthly summary table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS census_trade_monthly_summary (
            id BIGSERIAL PRIMARY KEY,
            trade_date DATE NOT NULL,
            commodity VARCHAR(50) NOT NULL,
            flow VARCHAR(10) NOT NULL,
            country_name VARCHAR(100) NOT NULL,
            destination_region VARCHAR(50),
            total_value_usd NUMERIC(18,2),
            total_quantity NUMERIC(18,4),
            marketing_year VARCHAR(10),
            record_count INTEGER,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, commodity, flow, country_name)
        )
    """)

    # Create marketing year summary table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS census_trade_yearly_summary (
            id BIGSERIAL PRIMARY KEY,
            marketing_year VARCHAR(10) NOT NULL,
            commodity VARCHAR(50) NOT NULL,
            flow VARCHAR(10) NOT NULL,
            country_name VARCHAR(100) NOT NULL,
            destination_region VARCHAR(50),
            total_value_usd NUMERIC(18,2),
            total_quantity NUMERIC(18,4),
            record_count INTEGER,
            calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (marketing_year, commodity, flow, country_name)
        )
    """)

    conn.commit()

    # Insert records
    inserted = 0
    for r in records:
        try:
            cursor.execute("""
                INSERT INTO census_trade_records
                (trade_date, year, month, commodity, hs_code, flow,
                 country_code, country_name, destination_region,
                 value_usd, quantity, unit, marketing_year)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trade_date, commodity, flow, country_name)
                DO UPDATE SET
                    value_usd = EXCLUDED.value_usd,
                    quantity = EXCLUDED.quantity,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                r['date'], r['year'], r['month'], r['commodity'], r['hs_code'],
                r['flow'], r.get('country_code'), r['country_name'],
                get_destination_region(r['country_name']),
                r.get('value_usd'), r.get('quantity'), r.get('unit'),
                r.get('marketing_year')
            ))
            inserted += 1
        except psycopg2.Error as e:
            logger.warning(f"Failed to insert record: {e}")

    conn.commit()
    print(f"Inserted/updated {inserted} raw records in PostgreSQL")

    # Update aggregation tables
    _update_postgresql_aggregations(cursor, records)
    conn.commit()

    cursor.close()
    conn.close()

    print(f"SUCCESS: Saved {inserted} records to PostgreSQL with aggregation tables updated")
    return inserted


def _update_postgresql_aggregations(cursor, records: List[Dict]):
    """Update PostgreSQL aggregation tables"""

    # Group by flow for aggregation
    for flow in ['exports', 'imports']:
        flow_records = [r for r in records if r.get('flow') == flow]

        # Monthly aggregation
        monthly = aggregate_monthly_by_destination(flow_records, flow)
        for (year, month, dest), data in monthly.items():
            cursor.execute("""
                INSERT INTO census_trade_monthly_summary
                (trade_date, commodity, flow, country_name, destination_region,
                 total_value_usd, total_quantity, marketing_year, record_count, calculated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (trade_date, commodity, flow, country_name)
                DO UPDATE SET
                    total_value_usd = EXCLUDED.total_value_usd,
                    total_quantity = EXCLUDED.total_quantity,
                    record_count = EXCLUDED.record_count,
                    calculated_at = CURRENT_TIMESTAMP
            """, (
                date(year, month, 1),
                flow_records[0]['commodity'] if flow_records else 'UNKNOWN',
                flow, dest, data['region'],
                data['value_usd'], data['quantity'],
                data['marketing_year'], data['count']
            ))

        # Marketing year aggregation
        yearly = aggregate_by_marketing_year(flow_records, flow)
        for (my, dest), data in yearly.items():
            if not my:
                continue
            cursor.execute("""
                INSERT INTO census_trade_yearly_summary
                (marketing_year, commodity, flow, country_name, destination_region,
                 total_value_usd, total_quantity, record_count, calculated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (marketing_year, commodity, flow, country_name)
                DO UPDATE SET
                    total_value_usd = EXCLUDED.total_value_usd,
                    total_quantity = EXCLUDED.total_quantity,
                    record_count = EXCLUDED.record_count,
                    calculated_at = CURRENT_TIMESTAMP
            """, (
                my, flow_records[0]['commodity'] if flow_records else 'UNKNOWN',
                flow, dest, data['region'],
                data['value_usd'], data['quantity'], data['count']
            ))

    logger.info(f"Updated PostgreSQL aggregations for {len(records)} records")


def _save_to_sqlite(records: List[Dict], connection_string: str) -> int:
    """Save records to SQLite"""
    import sqlite3

    db_path = connection_string.replace('sqlite:///', '')
    os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS census_trade_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            commodity TEXT NOT NULL,
            hs_code TEXT NOT NULL,
            flow TEXT NOT NULL,
            country_code TEXT,
            country_name TEXT NOT NULL,
            destination_region TEXT,
            value_usd REAL,
            quantity REAL,
            unit TEXT,
            marketing_year TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (trade_date, commodity, flow, country_name)
        )
    """)

    # Insert records
    inserted = 0
    for r in records:
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO census_trade_records
                (trade_date, year, month, commodity, hs_code, flow,
                 country_code, country_name, destination_region,
                 value_usd, quantity, unit, marketing_year)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                r['date'].isoformat(), r['year'], r['month'], r['commodity'],
                r['hs_code'], r['flow'], r.get('country_code'), r['country_name'],
                get_destination_region(r['country_name']),
                r.get('value_usd'), r.get('quantity'), r.get('unit'),
                r.get('marketing_year')
            ))
            inserted += 1
        except sqlite3.Error as e:
            logger.warning(f"Failed to insert record: {e}")

    conn.commit()
    print(f"Inserted/updated {inserted} records in SQLite")

    cursor.close()
    conn.close()

    print(f"SUCCESS: Saved {inserted} records to SQLite")
    return inserted


# =============================================================================
# EXCEL UPDATE FUNCTIONS (using win32com to preserve external links)
# =============================================================================

def parse_excel_date_header(cell_value, header_row: int = 2) -> Optional[Tuple[int, int]]:
    """
    Parse a date from an Excel header cell.

    Handles various formats:
    - datetime objects
    - Excel serial date numbers
    - String formats like "Nov-35", "Dec-35" (where 35 = 2035)
    - Skip marketing year formats like "93/94", "94/95"

    Args:
        cell_value: The cell value to parse
        header_row: Which row this is from (for context)

    Returns:
        Tuple of (year, month) or None if not a valid monthly date
    """
    if cell_value is None:
        return None

    # Handle datetime objects
    # Excel may interpret "Nov-35" as November 1935, but we want 2035
    if isinstance(cell_value, datetime):
        year = cell_value.year
        # Fix 2-digit year interpretation: years 1920-1970 should be 2020-2070
        if 1920 <= year < 1970:
            year += 100  # 1935 -> 2035
        return (year, cell_value.month)

    if isinstance(cell_value, date):
        year = cell_value.year
        # Fix 2-digit year interpretation: years 1920-1970 should be 2020-2070
        if 1920 <= year < 1970:
            year += 100  # 1935 -> 2035
        return (year, cell_value.month)

    # Handle Excel serial date numbers
    if isinstance(cell_value, (int, float)) and cell_value > 1000:
        # Excel serial dates are > 1000 for dates after ~1902
        try:
            excel_epoch = datetime(1899, 12, 30)
            dt = excel_epoch + timedelta(days=int(cell_value))
            year = dt.year
            # Fix 2-digit year interpretation if needed
            if 1920 <= year < 1970:
                year += 100
            return (year, dt.month)
        except:
            pass

    # Handle string formats
    if isinstance(cell_value, str):
        cell_str = cell_value.strip()

        # Skip marketing year formats like "93/94", "94/95", "00/01"
        if '/' in cell_str and len(cell_str) <= 7:
            parts = cell_str.split('/')
            if len(parts) == 2 and all(p.isdigit() and len(p) <= 2 for p in parts):
                # This is a marketing year column, skip it
                return None

        # Parse monthly formats like "Nov-35", "Dec-35", "Jan-36"
        # These use 2-digit years where 35 = 2035, not 1935
        for fmt in ['%b-%y', '%b %y', '%B-%y', '%B %y']:
            try:
                dt = datetime.strptime(cell_str, fmt)
                year = dt.year
                # Fix century: years 00-50 are 2000-2050, 51-99 are 1951-1999
                if year < 100:
                    year = 2000 + year if year <= 50 else 1900 + year
                elif year < 1950:
                    # strptime with %y gives 1900s for 00-68 in some Python versions
                    # Fix: if year is 1935, it should be 2035
                    if year < 1970:
                        year += 100  # 1935 -> 2035
                return (year, dt.month)
            except ValueError:
                continue

        # Try other formats
        for fmt in ['%Y-%m', '%m/%Y', '%Y/%m', '%m-%Y']:
            try:
                dt = datetime.strptime(cell_str, fmt)
                return (dt.year, dt.month)
            except ValueError:
                continue

    return None


def find_country_rows(ws, max_row: int = 500) -> Dict[str, int]:
    """
    Dynamically find country/destination rows by scanning column A.

    Args:
        ws: Excel worksheet object
        max_row: Maximum row to scan

    Returns:
        Dict mapping country name (uppercase) to row number
    """
    country_rows = {}

    for row in range(1, max_row + 1):
        cell_value = ws.Cells(row, 1).Value
        if cell_value and isinstance(cell_value, str):
            # Clean the country name
            country = cell_value.strip().upper()

            # Skip header rows, totals, and empty-ish values
            skip_patterns = [
                'EXPORTS', 'IMPORTS', 'MILLION', 'BUSHELS', 'TONNES',
                'TOTAL', 'ACTUAL', 'ADJUSTED', 'SUM', 'HS CODE',
                '120110', '120190', '230400', '150710'  # HS codes
            ]

            if any(pattern in country for pattern in skip_patterns):
                continue

            if len(country) < 3:
                continue

            # Store the row for this country
            country_rows[country] = row

            # Also store normalized versions for common variations
            # EUROPEAN UNION-28 -> also match EUROPEAN UNION
            if '-28' in country:
                base_name = country.replace('-28', '')
                if base_name not in country_rows:
                    country_rows[base_name] = row
            if '-27' in country:
                base_name = country.replace('-27', '')
                if base_name not in country_rows:
                    country_rows[base_name] = row

    return country_rows


def update_excel_file(
    excel_path: Path,
    monthly_data: Dict[Tuple, Dict],
    commodity: str,
    flow: str,
    uses_hs_code_rows: bool = False
) -> bool:
    """
    Update the Excel file with Census trade data using win32com.

    win32com uses Excel's COM interface directly, which preserves external links,
    formulas, and other Excel features that openpyxl corrupts.

    Args:
        excel_path: Path to Excel file
        monthly_data: Monthly data - Dict[(year, month, destination), {value_usd, quantity, ...}]
                      OR Dict[(year, month, hs_code), {...}] if uses_hs_code_rows=True
        commodity: Commodity name (SOYBEANS, SOYBEAN_MEAL, SOYBEAN_OIL)
        flow: 'exports' or 'imports'
        uses_hs_code_rows: If True, data is aggregated by HS code and written to
                           HS code-specific total rows (e.g., crude vs refined oil)

    Returns:
        True if successful
    """
    try:
        import win32com.client
        import pythoncom
    except ImportError:
        print("ERROR: pywin32 not installed. Run: pip install pywin32")
        logger.error("pywin32 not installed")
        return False

    import shutil
    import tempfile
    import time

    # Resolve to absolute path - critical for COM on Windows
    excel_path = Path(excel_path).resolve()

    if not excel_path.exists():
        print(f"ERROR: Excel file not found: {excel_path}")
        logger.error(f"Excel file not found: {excel_path}")
        return False

    # Get sheet name for this commodity/flow
    sheets = COMMODITY_SHEETS.get(commodity.upper())
    if not sheets:
        print(f"ERROR: No sheet mapping for commodity: {commodity}")
        return False

    sheet_name = sheets.get(flow)
    if not sheet_name:
        print(f"ERROR: No sheet for {commodity} {flow}")
        return False

    print(f"  Opening: {excel_path}")

    # For Dropbox/OneDrive files, copy to temp location to avoid sync locks
    temp_path = None
    working_path = excel_path
    is_cloud_sync = any(x in str(excel_path).lower() for x in ['dropbox', 'onedrive', 'google drive'])

    if is_cloud_sync:
        try:
            temp_dir = Path(tempfile.gettempdir())
            temp_path = temp_dir / f"temp_{excel_path.name}"
            print(f"  Cloud sync detected - copying to temp: {temp_path}")
            shutil.copy2(excel_path, temp_path)
            working_path = temp_path
        except Exception as e:
            print(f"  Warning: Could not create temp copy: {e}")
            working_path = excel_path

    excel = None
    wb = None

    try:
        # Initialize COM for this thread
        pythoncom.CoInitialize()

        # Create Excel application instance
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.ScreenUpdating = False

        # Open workbook with UpdateLinks=0 to prevent link update prompts
        # Arguments: Filename, UpdateLinks, ReadOnly, Format, Password
        wb = excel.Workbooks.Open(
            str(working_path),
            UpdateLinks=0,  # Don't update links
            ReadOnly=False
        )

    except Exception as e:
        print(f"ERROR: Failed to open Excel file: {e}")
        print("  TIP: Make sure the file is closed in Excel before running this script.")
        print("  TIP: Try closing all Excel windows and running: taskkill /F /IM EXCEL.EXE")
        print(f"  Path attempted: {working_path}")
        logger.error(f"Failed to open Excel file: {e}")
        if excel:
            try:
                excel.Quit()
            except:
                pass
        pythoncom.CoUninitialize()
        # Clean up temp file if created
        if temp_path and temp_path.exists():
            try:
                temp_path.unlink()
            except:
                pass
        return False

    try:
        # Get the sheet by name
        ws = None
        sheet_names = []
        for i in range(1, wb.Sheets.Count + 1):
            sheet_names.append(wb.Sheets(i).Name)
            if wb.Sheets(i).Name == sheet_name:
                ws = wb.Sheets(i)
                break

        if ws is None:
            print(f"ERROR: Sheet '{sheet_name}' not found in workbook")
            print(f"Available sheets: {sheet_names}")
            logger.error(f"Sheet '{sheet_name}' not found")
            wb.Close(SaveChanges=False)
            excel.Quit()
            pythoncom.CoUninitialize()
            return False

        # Get used range dimensions
        used_range = ws.UsedRange
        max_col = used_range.Columns.Count
        max_row = used_range.Rows.Count

        # Find date columns by scanning row 2 (where monthly dates like "Nov-35" are)
        date_columns = {}
        header_row = 2

        print(f"  Scanning row {header_row} for date columns (up to {max_col} columns)...")

        for col in range(1, max_col + 1):
            cell_value = ws.Cells(header_row, col).Value
            parsed = parse_excel_date_header(cell_value, header_row)
            if parsed:
                year, month = parsed
                # Only include dates from 1990 onwards (skip obvious errors)
                if year >= 1990:
                    date_columns[(year, month)] = col

        print(f"  Found {len(date_columns)} monthly date columns in {sheet_name}")
        logger.info(f"Found {len(date_columns)} date columns in {sheet_name}")

        # Debug: show date range found
        if date_columns:
            years = sorted(set(y for y, m in date_columns.keys()))
            print(f"  Date range: {min(years)} to {max(years)}")
            # Show some dates from the target range (2020-2025)
            recent_dates = sorted([d for d in date_columns.keys() if d[0] >= 2020])[:10]
            if recent_dates:
                print(f"  Recent dates found: {recent_dates}")
            else:
                print(f"  WARNING: No dates found in 2020+ range!")

        # Dynamically find country rows by scanning column A
        print(f"  Scanning column A for country names (up to row {max_row})...")
        country_rows = find_country_rows(ws, max_row)
        print(f"  Found {len(country_rows)} country/destination rows")

        # Debug: show sample country rows
        if country_rows:
            sample_countries = list(country_rows.items())[:10]
            print(f"  Sample country rows: {sample_countries}")

        # Show unit configuration being used
        excel_filename = excel_path.name
        unit_system = EXCEL_UNIT_SYSTEM.get(excel_filename, 'INTL')
        if unit_system == 'US' and commodity.upper() in US_UNIT_CONFIG:
            config = US_UNIT_CONFIG[commodity.upper()]
            print(f"  Unit system: US - {config['display_unit']} ({config['unit']})")
        else:
            print(f"  Unit system: International - {INTL_UNIT_CONFIG['display_unit']}")

        # Debug: show sample data keys (destinations or HS codes depending on mode)
        if uses_hs_code_rows:
            hs_codes_in_data = list(set(hs for (y, m, hs) in list(monthly_data.keys())[:20]))
            print(f"  HS codes in data: {hs_codes_in_data}")
        else:
            sample_dests = list(set(dest for (y, m, dest) in list(monthly_data.keys())[:20]))[:10]
            print(f"  Sample destinations in data: {sample_dests}")

        # Debug: show sample raw quantities from Census (in kg)
        sample_qty = [(k, v.get('quantity')) for k, v in list(monthly_data.items())[:5] if v.get('quantity')]
        if sample_qty:
            print(f"  Sample raw quantities (kg from Census):")
            for key, qty in sample_qty[:3]:
                print(f"    {key}: {qty:,.0f} kg")

        # Get unit conversion for this commodity
        conversion = UNIT_CONVERSIONS.get(commodity.upper())
        if conversion:
            unit_name, multiplier = conversion
            print(f"  Converting from kg to {unit_name} (multiplier: {multiplier:.6f})")
        else:
            unit_name = 'TMT'
            multiplier = 1 / 1000000.0  # Fallback: kg to TMT
            print(f"  WARNING: No conversion for {commodity}, using fallback kg->TMT")

        # Check if this commodity uses a special total row (e.g., hulls)
        special_row = COMMODITY_TOTAL_ROWS.get(commodity.upper())

        # Check if this commodity uses HS code-specific rows (e.g., soybean oil)
        hs_code_rows = HS_CODE_TOTAL_ROWS.get(commodity.upper(), {})

        # Debug: show year distribution of data vs date columns
        data_years = sorted(set(y for (y, m, dest) in monthly_data.keys()))
        column_years = sorted(set(y for y, m in date_columns.keys())) if date_columns else []
        print(f"  Data years: {data_years}")
        print(f"  Column years: {column_years}")

        # Check for overlap
        overlap_years = set(data_years) & set(column_years)
        if not overlap_years:
            print(f"  WARNING: No overlap between data years and column years!")

        # Determine which columns we'll be updating (so we can clear them first)
        columns_to_update = set()
        for (year, month, destination) in monthly_data.keys():
            col = date_columns.get((year, month))
            if col:
                columns_to_update.add(col)

        # Clear columns before writing (remove old projections)
        # Clear rows 5-290 except SUM_FORMULA_ROWS
        if columns_to_update:
            print(f"  Clearing {len(columns_to_update)} columns before writing (rows 5-290, preserving sum rows)...")
            cleared_cells = 0
            # Activate the sheet first (required for Select to work, but we'll use direct assignment)
            ws.Activate()
            for col in columns_to_update:
                for row in range(5, 291):  # Rows 5 to 290 inclusive
                    if row not in SUM_FORMULA_ROWS:
                        # Clear the cell value but preserve formatting
                        # Use direct assignment (simpler and works without Select)
                        cell = ws.Cells(row, col)
                        if cell.Value is not None:
                            cell.Value = None
                            cleared_cells += 1
            print(f"  Cleared {cleared_cells} cells with existing values")

        # Update cells
        updated = 0
        not_found_destinations = set()
        not_found_dates = set()
        matched_destinations = set()
        skipped_aggregates = set()
        zero_values = 0
        estimated_count = 0

        # SPECIAL CASE: SOYBEAN_HULLS
        # For hulls, we only write the TOTAL to row 294 (not individual country breakdowns)
        # The hull total is added to the meal total in the Excel formula
        if commodity.upper() == 'SOYBEAN_HULLS':
            print(f"  SOYBEAN_HULLS: Writing monthly totals to row 294 only")
            print(f"  Hull data records: {len(monthly_data)}")

            # Debug: Show sample hull data
            sample_hull_data = list(monthly_data.items())[:5]
            for (y, m, d), data in sample_hull_data:
                print(f"    {y}-{m:02d} {d}: qty={data.get('quantity')}, val=${data.get('value_usd')}")

            # Aggregate all country data by (year, month) into monthly totals
            hull_monthly_totals = {}
            for (year, month, destination), data in monthly_data.items():
                key = (year, month)
                if key not in hull_monthly_totals:
                    hull_monthly_totals[key] = {'quantity': 0, 'value_usd': 0}
                hull_monthly_totals[key]['quantity'] += data.get('quantity') or 0
                hull_monthly_totals[key]['value_usd'] += data.get('value_usd') or 0

            print(f"  Aggregated to {len(hull_monthly_totals)} monthly totals")

            # Write hull totals to row 294
            hull_row = 294
            for (year, month), data in hull_monthly_totals.items():
                col = date_columns.get((year, month))
                if not col:
                    not_found_dates.add((year, month))
                    continue

                value = data.get('quantity')
                value_usd = data.get('value_usd')

                # Estimate from value if no quantity
                if (not value or value <= 0) and value_usd and value_usd > 0:
                    value = estimate_quantity_from_value(value_usd, 'SOYBEAN_HULLS')

                if value and value > 0:
                    # Use US short tons conversion (same as meal)
                    config = US_UNIT_CONFIG['SOYBEAN_HULLS']
                    converted_value = config['kg_to_display'](value)
                    ws.Cells(hull_row, col).Value = round(converted_value, 3)
                    updated += 1
                    # Debug: Show what we're writing
                    if updated <= 3:
                        print(f"    Writing row {hull_row}, col {col}: {year}-{month:02d} = {round(converted_value, 3)} short tons")
                else:
                    # Debug: Show why we're not writing
                    if len(not_found_dates) < 3:
                        print(f"    Skipping {year}-{month:02d}: quantity={value}, value_usd={value_usd}")

            print(f"  Updated {updated} hull monthly totals in row 294")

            # Save and close for hulls
            wb.Save()
            wb.Close(SaveChanges=True)
            excel.Quit()
            pythoncom.CoUninitialize()

            # Copy back if temp file
            if temp_path and temp_path.exists():
                try:
                    shutil.copy2(temp_path, excel_path)
                    temp_path.unlink()
                except Exception as e:
                    print(f"  Warning: Could not copy back: {e}")

            return True

        # Debug: Check what values we have
        sample_values = []
        for (y, m, d), data in list(monthly_data.items())[:5]:
            sample_values.append((y, m, d, data.get('quantity'), data.get('value_usd')))
        print(f"  Sample data values: {sample_values}")

        # Debug: Show first few writes in detail
        debug_writes = []

        if uses_hs_code_rows and hs_code_rows:
            # HS code handling: write each HS code's total to its specific row
            print(f"  Writing HS code totals to rows: {hs_code_rows}")

            for (year, month, hs_code), data in monthly_data.items():
                # Get row for this HS code
                row = hs_code_rows.get(hs_code)
                if not row:
                    logger.warning(f"No row mapping for HS code {hs_code}")
                    continue

                # Get column for this date
                col = date_columns.get((year, month))
                if not col:
                    not_found_dates.add((year, month))
                    continue

                raw_quantity_kg = data.get('quantity') or 0
                if raw_quantity_kg > 0:
                    converted_value = raw_quantity_kg * multiplier
                    ws.range((row, col)).value = round(converted_value, 3)
                    updated += 1

            # Show summary per HS code
            for hs_code, row in hs_code_rows.items():
                hs_data = [(k, v) for k, v in monthly_data.items() if k[2] == hs_code]
                if hs_data:
                    total = sum(v.get('quantity', 0) or 0 for _, v in hs_data)
                    print(f"    HS {hs_code} -> row {row}: {len(hs_data)} months, total {total * multiplier:,.0f} {unit_name}")

            print(f"  Wrote {updated} HS code totals")

        elif special_row:
            # Special handling: aggregate all countries to a single row per month
            print(f"  Writing aggregated totals to row {special_row}")
            monthly_totals = defaultdict(float)
        for (year, month, destination), data in monthly_data.items():
            dest_upper = destination.upper().strip()

            # First check if this is a Census aggregate that maps to a specific row
            if dest_upper in CENSUS_AGGREGATE_ROWS:
                agg_row = CENSUS_AGGREGATE_ROWS[dest_upper]
                if agg_row is None:
                    # Skip this aggregate - it's calculated in Excel
                    skipped_aggregates.add(dest_upper)
                    continue
                row = agg_row
            else:
                # Get row for this destination (try exact match first)
                row = country_rows.get(dest_upper)

                # Try common name variations if exact match fails
                if not row:
                    for country_name, country_row in country_rows.items():
                        if dest_upper in country_name or country_name in dest_upper:
                            row = country_row
                            break

            if not row:
                not_found_destinations.add(destination)
                continue

            # Skip rows that contain SUM formulas (except row 290 which gets Census aggregate data)
            if row in SUM_FORMULA_ROWS and row != 290:
                skipped_aggregates.add(f"{destination} (row {row} has formula)")
                continue

            matched_destinations.add(destination)

            for (year, month, destination), data in monthly_data.items():
                raw_quantity_kg = data.get('quantity') or 0
                if raw_quantity_kg > 0:
                    monthly_totals[(year, month)] += raw_quantity_kg

            for (year, month), total_kg in monthly_totals.items():
                col = date_columns.get((year, month))
                if not col:
                    not_found_dates.add((year, month))
                    continue

                converted_value = total_kg * multiplier
                ws.range((special_row, col)).value = round(converted_value, 3)
            # Get value to write (use quantity for volume)
            # Census data is in kg - convert based on file type and commodity
            value = data.get('quantity')
            value_usd = data.get('value_usd')
            used_estimate = False

            # If quantity is missing but value_usd is available, estimate quantity
            if (not value or value <= 0) and value_usd and value_usd > 0:
                estimated_qty = estimate_quantity_from_value(value_usd, commodity)
                if estimated_qty:
                    value = estimated_qty
                    used_estimate = True

            if value and value > 0:
                # Determine unit system based on Excel file
                excel_filename = excel_path.name
                unit_system = EXCEL_UNIT_SYSTEM.get(excel_filename, 'INTL')

                if unit_system == 'US' and commodity.upper() in US_UNIT_CONFIG:
                    # Use commodity-specific US conversion
                    config = US_UNIT_CONFIG[commodity.upper()]
                    converted_value = config['kg_to_display'](value)
                else:
                    # Use international metric tons (1000 MT)
                    converted_value = INTL_UNIT_CONFIG['kg_to_display'](value)

                # Direct cell assignment (works without needing to Select)
                ws.Cells(row, col).Value = round(converted_value, 3)
                updated += 1
                if used_estimate:
                    estimated_count += 1

                # Capture debug info for first 5 writes
                if len(debug_writes) < 5:
                    debug_writes.append({
                        'dest': destination,
                        'date': f"{year}-{month:02d}",
                        'row': row,
                        'col': col,
                        'raw_kg': value,
                        'converted': round(converted_value, 3),
                        'estimated': used_estimate
                    })
            else:
                zero_values += 1

            print(f"  Wrote {updated} monthly totals to row {special_row}")
        else:
            # Normal handling: write to individual country rows
            for (year, month, destination), data in monthly_data.items():
                # Get row for this destination
                row = DESTINATION_ROWS.get(destination.upper())
                if not row:
                    not_found_destinations.add(destination)
                    continue

                matched_destinations.add(destination)

                # Get column for this date
                col = date_columns.get((year, month))
                if not col:
                    not_found_dates.add((year, month))
                    continue

                # Get value to write (use quantity for volume)
                # Census API reports quantity in KILOGRAMS
                raw_quantity_kg = data.get('quantity')

                if raw_quantity_kg and raw_quantity_kg > 0:
                    converted_value = raw_quantity_kg * multiplier
                    ws.range((row, col)).value = round(converted_value, 3)
                    updated += 1

            # Debug output
            print(f"  Matched destinations: {len(matched_destinations)}")
            if matched_destinations:
                print(f"    Examples: {list(matched_destinations)[:5]}")
        # Debug output
        print(f"  Matched destinations: {len(matched_destinations)}")

        # Show sample writes
        if debug_writes:
            print(f"  First {len(debug_writes)} writes (verify in Excel):")
            for w in debug_writes:
                est_marker = " [EST]" if w['estimated'] else ""
                print(f"    {w['dest']} {w['date']}: Row {w['row']}, Col {w['col']} = {w['converted']}{est_marker}")
        if matched_destinations:
            print(f"    Examples: {list(matched_destinations)[:5]}")

        if estimated_count > 0:
            print(f"  Used value-based quantity estimates: {estimated_count}")

        if zero_values > 0:
            print(f"  Records with zero/null quantity (even after estimation): {zero_values}")

        if skipped_aggregates:
            print(f"  Skipped Census aggregates (calculated in Excel): {list(skipped_aggregates)[:5]}")

        # Save workbook
        wb.Save()
        print(f"Updated {updated} cells in {sheet_name}")
        logger.info(f"Updated {updated} cells in {sheet_name}")

        # Verify write worked - read back first debug write cell
        if debug_writes:
            test_cell = debug_writes[0]
            verify_value = ws.Cells(test_cell['row'], test_cell['col']).Value
            print(f"  VERIFY: Cell ({test_cell['row']}, {test_cell['col']}) after save = {verify_value}")
            if verify_value != test_cell['converted']:
                print(f"  WARNING: Expected {test_cell['converted']}, got {verify_value}!")

        if not_found_destinations:
            unmapped = list(not_found_destinations)[:10]
            print(f"  Unmapped destinations ({len(not_found_destinations)} total): {unmapped}")
            logger.warning(f"Unmapped destinations: {not_found_destinations}")

        if not_found_dates:
            missing_dates = sorted(list(not_found_dates))[:10]
            print(f"  Dates not in spreadsheet ({len(not_found_dates)} total): {missing_dates}")

        # Close workbook and Excel
        wb.Close(SaveChanges=True)
        excel.Quit()
        pythoncom.CoUninitialize()

        # Small delay to ensure Excel fully releases the file
        time.sleep(0.5)

        # If we used a temp file, copy it back to the original location
        if temp_path and temp_path.exists():
            try:
                print(f"  Copying updated file back to original location...")
                shutil.copy2(temp_path, excel_path)
                temp_path.unlink()  # Clean up temp file
                print(f"  Successfully updated: {excel_path}")
            except Exception as e:
                print(f"  WARNING: Could not copy back to original: {e}")
                print(f"  Updated file saved at: {temp_path}")
                logger.warning(f"Could not copy temp file back: {e}")

        return True

    except Exception as e:
        print(f"ERROR: Failed to update Excel file: {e}")
        logger.error(f"Failed to update Excel file: {e}")
        import traceback
        traceback.print_exc()
        try:
            if wb:
                wb.Close(SaveChanges=False)
            if excel:
                excel.Quit()
            pythoncom.CoUninitialize()
        except:
            pass
        return False


def update_all_excel_sheets(
    records: List[Dict],
    commodity: str,
    models_base: Path
) -> Dict[str, int]:
    """
    Update all relevant Excel sheets for a commodity (both imports and exports)

    Args:
        records: List of trade records
        commodity: Commodity name
        models_base: Base path to Models folder (or custom path like Desktop/Models/Oilseeds)

    Returns:
        Dict with update counts per sheet
    """
    results = {}

    excel_file = EXCEL_FILES.get(commodity.upper())
    if not excel_file:
        print(f"No Excel file configured for {commodity}")
        return results

    # Build the Excel path
    # EXCEL_FILES entries look like "Models/Oilseeds/US Soybean Trade.xlsx"
    # If models_base already ends with "Oilseeds" or similar, adjust path construction
    models_base = Path(models_base)

    # Check if this is a custom path that already contains the subfolder
    # e.g., "C:\Users\torem\OneDrive\Desktop\Models\Oilseeds"
    if models_base.name == 'Oilseeds' or models_base.name == 'Grains':
        # Custom path already includes the subfolder, just need filename
        excel_filename = Path(excel_file).name  # "US Soybean Trade.xlsx"
        excel_path = models_base / excel_filename
    elif 'Models' in str(models_base):
        # Custom path includes "Models" - extract relative path from EXCEL_FILES
        # EXCEL_FILES = "Models/Oilseeds/file.xlsx" -> "Oilseeds/file.xlsx"
        relative_from_models = '/'.join(excel_file.split('/')[1:])  # Remove "Models/"
        excel_path = models_base / relative_from_models
    else:
        # Standard case: models_base is the project root, use full relative path
        excel_path = models_base / excel_file

    # Check if this commodity needs HS code separation (e.g., soybean oil)
    uses_hs_code_rows = commodity.upper() in HS_CODE_TOTAL_ROWS

    for flow in ['exports', 'imports']:
        # Filter records for this flow
        flow_records = [r for r in records if r.get('flow') == flow]
        if not flow_records:
            continue

        # Aggregate differently based on commodity type
        if uses_hs_code_rows:
            # For commodities like soybean oil, aggregate by HS code
            # and write totals to HS code-specific rows
            monthly_data = aggregate_monthly_by_hs_code(flow_records, flow)
            print(f"\n  Using HS code aggregation for {commodity}")
        else:
            # Normal: aggregate by destination country
            monthly_data = aggregate_monthly_by_destination(flow_records, flow)

        # Update Excel
        sheet_name = COMMODITY_SHEETS.get(commodity.upper(), {}).get(flow, flow)
        print(f"\nUpdating {sheet_name}...")

        if update_excel_file(excel_path, monthly_data, commodity, flow, uses_hs_code_rows):
            results[f"{commodity}_{flow}"] = len(monthly_data)
        else:
            results[f"{commodity}_{flow}"] = 0

    return results


# =============================================================================
# MAIN WORKFLOW
# =============================================================================

def run_census_update(
    commodity: str = 'SOYBEANS',
    years: int = 5,
    flow: str = 'both',
    save_to_db: bool = False,
    update_excel: bool = False,
    models_path: str = None
) -> Dict:
    """
    Run the Census trade data update workflow

    Args:
        commodity: Commodity to process (SOYBEANS, SOYBEAN_MEAL, SOYBEAN_OIL, ALL)
        years: Number of years of history to fetch
        flow: 'exports', 'imports', or 'both'
        save_to_db: Save to database
        update_excel: Update Excel files
        models_path: Custom path to Models folder (to test outside Dropbox/OneDrive)

    Returns:
        Results summary
    """
    results = {
        'success': False,
        'records_fetched': 0,
        'records_saved': 0,
        'commodities_processed': [],
    }

    api_key = get_api_key()
    if not api_key:
        print("WARNING: No CENSUS_API_KEY set. API calls will be rate-limited.")
        logger.warning("No Census API key. Rate limited to 500 calls/day.")

    project_root = Path(__file__).parent.parent

    # Calculate date range
    end_date = date.today().replace(day=1) - timedelta(days=1)  # Last complete month
    start_date = date(end_date.year - years, 1, 1)

    # Handle ALL commodities
    if commodity.upper() == 'ALL':
        commodities = list(HS_CODES.keys())
    else:
        commodities = [commodity.upper()]

    all_records = []

    for comm in commodities:
        print(f"\n{'=' * 60}")
        print(f"Fetching {COMMODITY_NAMES.get(comm, comm)} ({flow})")
        print(f"Period: {start_date} to {end_date}")
        print(f"{'=' * 60}")

        records = fetch_commodity_data(
            commodity=comm,
            start_date=start_date,
            end_date=end_date,
            flow=flow,
            api_key=api_key
        )

        if records:
            all_records.extend(records)
            results['commodities_processed'].append(comm)
            logger.info(f"Fetched {len(records)} records for {comm}")

            # Print summary
            print(f"\nFetched {len(records)} records for {comm}")

            # Show top destinations for exports
            if flow in ['exports', 'both']:
                export_records = [r for r in records if r['flow'] == 'exports']
                yearly = aggregate_by_marketing_year(export_records, 'exports')

                # Get latest marketing year
                latest_my = max((my for my, _ in yearly.keys() if my), default=None)
                if latest_my:
                    print(f"\n{latest_my} Export Destinations (top 10):")
                    my_totals = {dest: data['value_usd']
                                for (my, dest), data in yearly.items()
                                if my == latest_my}
                    for dest, value in sorted(my_totals.items(), key=lambda x: -x[1])[:10]:
                        print(f"  {dest}: ${value/1e6:,.1f}M")

    results['records_fetched'] = len(all_records)

    if not all_records:
        print("\nNo records fetched!")
        return results

    # Save to database
    if save_to_db:
        saved = save_to_database(all_records)
        results['records_saved'] = saved

    # Update Excel
    if update_excel:
        print("\n" + "=" * 60)
        print("UPDATING EXCEL FILES")
        print("=" * 60)

        # Determine the base path for Models folder
        if models_path:
            # Use custom models path provided by user
            models_base = Path(models_path)
            print(f"Using custom Models path: {models_base}")
        else:
            # Default: project_root/Models
            models_base = project_root / "Models"
            print(f"Using default Models path: {models_base}")

        for comm in commodities:
            # Get records for this commodity
            comm_records = [r for r in all_records if r.get('commodity') == comm]
            if comm_records:
                excel_results = update_all_excel_sheets(comm_records, comm, models_base)
                for sheet, count in excel_results.items():
                    print(f"  {sheet}: {count} month/destination combinations")

    results['success'] = True
    return results


def debug_census_data(commodity: str, year: int, month: int, flow: str = 'exports'):
    """
    Debug function to analyze Census API data for a specific month.
    Helps verify unit conversions and detect double-counting.
    """
    api_key = get_api_key()

    hs_codes = HS_CODES.get(commodity.upper(), [])
    if not hs_codes:
        print(f"ERROR: Unknown commodity: {commodity}")
        return

    print(f"\n{'=' * 70}")
    print(f"DEBUG: Census {flow} data for {COMMODITY_NAMES.get(commodity, commodity)}")
    print(f"Period: {year}-{month:02d}")
    print(f"HS Codes: {hs_codes}")
    print(f"{'=' * 70}")

    all_records = []
    hs_code_totals = {}

    for hs_code in hs_codes:
        records = fetch_trade_data(
            flow=flow,
            hs_code=hs_code,
            year=year,
            month=month,
            api_key=api_key,
            debug=True
        )

        if records:
            total_qty = sum(r.get('quantity', 0) or 0 for r in records)
            total_value = sum(r.get('value_usd', 0) or 0 for r in records)
            sample_unit = records[0].get('unit', 'UNKNOWN')

            hs_code_totals[hs_code] = {
                'records': len(records),
                'quantity': total_qty,
                'value_usd': total_value,
                'unit': sample_unit,
            }

            print(f"\nHS Code {hs_code}:")
            print(f"  Records: {len(records)}")
            print(f"  Total quantity: {total_qty:,.0f} {sample_unit}")
            print(f"  Total value: ${total_value:,.0f}")

            # Show top 5 destinations
            by_country = defaultdict(float)
            for r in records:
                by_country[r['country_name']] += r.get('quantity', 0) or 0

            print(f"  Top 5 destinations:")
            for dest, qty in sorted(by_country.items(), key=lambda x: -x[1])[:5]:
                print(f"    {dest}: {qty:,.0f} {sample_unit}")

            all_records.extend(records)

    # Check for duplicate countries across HS codes
    if len(hs_codes) > 1:
        print(f"\n{'=' * 70}")
        print("CHECKING FOR OVERLAP BETWEEN HS CODES")
        print(f"{'=' * 70}")

        country_by_hs = defaultdict(set)
        country_qty_by_hs = defaultdict(lambda: defaultdict(float))

        for r in all_records:
            country_by_hs[r['hs_code']].add(r['country_name'])
            country_qty_by_hs[r['hs_code']][r['country_name']] += r.get('quantity', 0) or 0

        # Find countries that appear in multiple HS codes
        if len(hs_codes) == 2:
            hs1, hs2 = hs_codes
            overlap = country_by_hs[hs1] & country_by_hs[hs2]
            only_hs1 = country_by_hs[hs1] - country_by_hs[hs2]
            only_hs2 = country_by_hs[hs2] - country_by_hs[hs1]

            print(f"\nCountries in BOTH {hs1} and {hs2}: {len(overlap)}")
            if overlap:
                print(f"  Overlapping countries (first 10): {list(overlap)[:10]}")
                print(f"\n  Quantities for overlapping countries:")
                for country in sorted(overlap)[:10]:
                    qty1 = country_qty_by_hs[hs1][country]
                    qty2 = country_qty_by_hs[hs2][country]
                    print(f"    {country}: {hs1}={qty1:,.0f}, {hs2}={qty2:,.0f}")

            print(f"\nCountries only in {hs1}: {len(only_hs1)}")
            print(f"Countries only in {hs2}: {len(only_hs2)}")

    # Calculate expected conversion
    print(f"\n{'=' * 70}")
    print("UNIT CONVERSION ANALYSIS")
    print(f"{'=' * 70}")

    if not hs_code_totals:
        print("\nNo data returned from Census API")
        print("(This may be due to network restrictions or invalid date)")
        return

    conversion = UNIT_CONVERSIONS.get(commodity.upper())
    if conversion:
        unit_name, multiplier = conversion
        grand_total_raw = sum(d['quantity'] for d in hs_code_totals.values())
        raw_unit = list(hs_code_totals.values())[0].get('unit', 'UNKNOWN')
        converted_total = grand_total_raw * multiplier

        print(f"\nRaw Census total: {grand_total_raw:,.0f} {raw_unit}")
        print(f"Conversion: × {multiplier:.8f} → {unit_name}")
        print(f"Converted total: {converted_total:,.3f} {unit_name}")

        # For soybean oil, show breakdown by HS code
        if commodity.upper() == 'SOYBEAN_OIL':
            print(f"\nSoybean Oil breakdown (for Excel rows P11 and P17):")
            for hs_code, data in hs_code_totals.items():
                raw_qty = data['quantity']
                converted = raw_qty * multiplier
                if hs_code == '150710':
                    print(f"  HS 150710 (Crude): {raw_qty:,.0f} kg = {converted:,.3f} {unit_name}")
                else:
                    print(f"  HS 150790 (Other): {raw_qty:,.0f} kg = {converted:,.3f} {unit_name}")

    print(f"\n{'=' * 70}")
    print(f"Census API units seen: {_census_units_seen}")
    print(f"{'=' * 70}")


def main():
    """Command-line entry point"""
    parser = argparse.ArgumentParser(
        description='Download and process Census Bureau trade data'
    )

    parser.add_argument(
        '--commodity', '-c',
        default='SOYBEANS',
        choices=['SOYBEANS', 'SOYBEAN_MEAL', 'SOYBEAN_HULLS', 'SOYBEAN_OIL', 'ALL'],
        help='Commodity to process (default: SOYBEANS)'
    )

    parser.add_argument(
        '--years', '-y',
        type=int,
        default=5,
        help='Number of years of history to fetch (default: 5)'
    )

    parser.add_argument(
        '--flow', '-f',
        choices=['exports', 'imports', 'both'],
        default='both',
        help='Trade flow to fetch (default: both)'
    )

    parser.add_argument(
        '--save-to-db',
        action='store_true',
        help='Save data to database'
    )

    parser.add_argument(
        '--update-excel',
        action='store_true',
        help='Update Excel model files'
    )

    parser.add_argument(
        '--debug-month',
        type=str,
        help='Debug a specific month (format: YYYY-MM). Shows detailed Census API analysis.'
        '--test',
        action='store_true',
        help='Test Census API connection before fetching data'
    )

    parser.add_argument(
        '--models-path',
        type=str,
        default=None,
        help='Custom path to Models folder (e.g., C:\\Users\\torem\\Desktop\\Models). Use this to test outside Dropbox.'
    )

    args = parser.parse_args()

    # If debug-month is specified, run debug analysis instead
    if args.debug_month:
        try:
            year, month = map(int, args.debug_month.split('-'))
        except ValueError:
            print("ERROR: --debug-month must be in YYYY-MM format (e.g., 2025-09)")
            return

        debug_census_data(args.commodity, year, month, args.flow.split(',')[0] if ',' in args.flow else ('exports' if args.flow == 'both' else args.flow))
        return

    print("\n" + "=" * 60)
    print("CENSUS BUREAU TRADE DATA COLLECTOR")
    print("=" * 60)

    api_key = get_api_key()

    # Run API test if requested
    if args.test:
        if not test_census_api(api_key):
            print("\nAPI test failed. Please check the error messages above.")
            print("Common issues:")
            print("  - Census API may be temporarily unavailable")
            print("  - Network/firewall issues")
            print("  - API key may be invalid")
            sys.exit(1)
        print("\nAPI test passed. Proceeding with data fetch...\n")

    results = run_census_update(
        commodity=args.commodity,
        years=args.years,
        flow=args.flow,
        save_to_db=args.save_to_db,
        update_excel=args.update_excel,
        models_path=args.models_path
    )

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Success: {results['success']}")
    print(f"Records fetched: {results['records_fetched']}")
    print(f"Records saved to DB: {results['records_saved']}")
    print(f"Commodities: {', '.join(results['commodities_processed'])}")


if __name__ == '__main__':
    main()
