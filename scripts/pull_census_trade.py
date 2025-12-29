#!/usr/bin/env python3
"""
Pull Census Bureau Trade Data

Downloads monthly import/export data from US Census Bureau API and:
1. Saves to PostgreSQL database
2. Updates Excel model files

Commodities tracked:
- Soybeans (HS 1201)
- Soybean Meal (HS 2304)
- Soybean Oil (HS 1507)

Usage:
    python scripts/pull_census_trade.py --commodity soybeans --years 5
    python scripts/pull_census_trade.py --commodity all --save-to-db --update-excel
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
    'SOYBEAN_MEAL': ['230400', '230499'],       # 2304.00 and 2304.99 - Oilcake and meal
    'SOYBEAN_HULLS': ['230250'],                # 2302.50 - Soybean meal hulls (separate)
    'SOYBEAN_OIL': ['150710', '150790'],        # 1507.10 - Crude, 1507.90 - Other
}

# Alternative 4-digit codes for fallback (GTT codes)
HS_CODES_4DIGIT = {
    'SOYBEANS': ['1201'],
    'SOYBEAN_MEAL': ['2304'],
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

# Conversion factors
MT_PER_BUSHEL = {
    'SOYBEANS': 0.0272155,      # 60 lbs/bu
    'SOYBEAN_MEAL': 0.0272155,  # Short tons converted
    'SOYBEAN_HULLS': 0.0272155, # Same as meal
    'SOYBEAN_OIL': None,        # Reported in kg/liters
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


def fetch_trade_data(
    flow: str,
    hs_code: str,
    year: int,
    month: int,
    api_key: str = None
) -> List[Dict]:
    """
    Fetch trade data from Census API for a specific month

    Args:
        flow: 'exports' or 'imports'
        hs_code: HS code (e.g., '1201' for soybeans)
        year: Year
        month: Month (1-12)
        api_key: Census API key (optional but recommended)

    Returns:
        List of trade records
    """
    # Build URL
    url = f"{CENSUS_API_BASE}/{flow}/hs"

    # Field names differ between imports and exports
    # See: https://api.census.gov/data/timeseries/intltrade/exports/hs/variables.html
    # See: https://api.census.gov/data/timeseries/intltrade/imports/hs/variables.html
    if flow == 'imports':
        commodity_field = 'I_COMMODITY'
        value_field = 'GEN_VAL_MO'
        qty_field = 'GEN_QY1_MO'
        unit_field = 'UNIT_QY1'
    else:
        commodity_field = 'E_COMMODITY'
        value_field = 'ALL_VAL_MO'
        qty_field = 'QTY_1_MO'  # Note: exports uses underscore format QTY_1_MO, not QY1_MO
        unit_field = 'UNIT_QY1'

    # Build params
    time_str = f"{year}-{month:02d}"
    params = {
        'get': f'{value_field},{qty_field},{unit_field},CTY_CODE,CTY_NAME',
        commodity_field: hs_code,
        'time': time_str,
    }

    if api_key:
        params['key'] = api_key

    try:
        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 1:
                headers = data[0]
                records = []
                for row in data[1:]:
                    record = dict(zip(headers, row))

                    # Parse values
                    value_usd = parse_number(record.get(value_field))
                    quantity = parse_number(record.get(qty_field))

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
                        'unit': record.get(unit_field, ''),
                    })

                return records
        elif response.status_code == 204:
            # No content - no data for this period
            logger.warning(f"API returned 204 for {flow}/{hs_code} {time_str}")
        elif response.status_code == 400:
            # Bad request - log the actual error response
            try:
                error_text = response.text[:200]
                logger.warning(f"API returned 400 for {flow}/{hs_code} {time_str}: {error_text}")
            except:
                logger.warning(f"API returned 400 for {flow}/{hs_code} {time_str}")
        else:
            logger.warning(f"API returned {response.status_code} for {flow}/{hs_code} {time_str}")

    except requests.RequestException as e:
        logger.error(f"Request failed for {flow}/{hs_code} {time_str}: {e}")

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


def save_to_database(records: List[Dict], connection_string: str = None) -> int:
    """
    Save trade records to database

    Args:
        records: List of trade records
        connection_string: Database connection string

    Returns:
        Number of records saved
    """
    connection_string = connection_string or os.getenv('DATABASE_URL')

    if not connection_string:
        connection_string = f'sqlite:///{DEFAULT_DB_PATH}'
        print(f"\nNo DATABASE_URL set - using SQLite: {DEFAULT_DB_PATH}")
        logger.info(f"Using default SQLite database: {DEFAULT_DB_PATH}")
    else:
        print(f"\nSaving {len(records)} records to database...")

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
# EXCEL UPDATE FUNCTIONS (using xlwings to preserve external links)
# =============================================================================

def update_excel_file(
    excel_path: Path,
    monthly_data: Dict[Tuple, Dict],
    commodity: str,
    flow: str
) -> bool:
    """
    Update the Excel file with Census trade data using xlwings.

    xlwings uses Excel's COM interface, which preserves external links and formulas
    that openpyxl corrupts.

    Args:
        excel_path: Path to Excel file
        monthly_data: Monthly data - Dict[(year, month, destination), {value_usd, quantity, ...}]
        commodity: Commodity name (SOYBEANS, SOYBEAN_MEAL, SOYBEAN_OIL)
        flow: 'exports' or 'imports'

    Returns:
        True if successful
    """
    try:
        import xlwings as xw
    except ImportError:
        print("ERROR: xlwings not installed. Run: pip install xlwings")
        logger.error("xlwings not installed")
        return False

    # Resolve to absolute path - critical for xlwings on Windows
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

    try:
        # Open Excel in the background (visible=False for faster processing)
        # Use app=None to connect to existing Excel or start new one
        app = xw.App(visible=False, add_book=False)
        app.display_alerts = False
        app.screen_updating = False

        # Use str() to convert Path to string for xlwings
        wb = app.books.open(str(excel_path))
    except Exception as e:
        print(f"ERROR: Failed to open Excel file: {e}")
        print("  TIP: Make sure the file is closed in Excel before running this script.")
        print(f"  Path attempted: {excel_path}")
        logger.error(f"Failed to open Excel file: {e}")
        try:
            app.quit()
        except:
            pass
        return False

    try:
        # Get the sheet
        if sheet_name not in [s.name for s in wb.sheets]:
            print(f"ERROR: Sheet '{sheet_name}' not found in workbook")
            print(f"Available sheets: {[s.name for s in wb.sheets]}")
            logger.error(f"Sheet '{sheet_name}' not found")
            wb.close()
            app.quit()
            return False

        ws = wb.sheets[sheet_name]

        # Find date columns by scanning row 3 (header row with dates)
        date_columns = {}
        header_row = 3

        # Get used range to find max column
        used_range = ws.used_range
        max_col = used_range.last_cell.column

        for col in range(1, max_col + 1):
            cell_value = ws.range((header_row, col)).value
            if cell_value:
                try:
                    # xlwings returns datetime objects directly
                    if isinstance(cell_value, datetime):
                        date_columns[(cell_value.year, cell_value.month)] = col
                    elif isinstance(cell_value, date):
                        date_columns[(cell_value.year, cell_value.month)] = col
                    elif isinstance(cell_value, str):
                        # Try parsing string dates
                        for fmt in ['%Y-%m', '%m/%Y', '%b-%y', '%b %Y', '%Y/%m']:
                            try:
                                dt = datetime.strptime(cell_value, fmt)
                                date_columns[(dt.year, dt.month)] = col
                                break
                            except ValueError:
                                continue
                except Exception:
                    continue

        if not date_columns:
            print(f"WARNING: Could not find date columns in row {header_row}")
            print("Trying row 2...")
            header_row = 2
            for col in range(1, max_col + 1):
                cell_value = ws.range((header_row, col)).value
                if cell_value:
                    try:
                        if isinstance(cell_value, datetime):
                            date_columns[(cell_value.year, cell_value.month)] = col
                        elif isinstance(cell_value, date):
                            date_columns[(cell_value.year, cell_value.month)] = col
                    except Exception:
                        continue

        print(f"  Found {len(date_columns)} date columns in {sheet_name}")
        logger.info(f"Found {len(date_columns)} date columns in {sheet_name}")

        # Debug: show sample date columns
        if date_columns:
            sample_dates = list(date_columns.keys())[:5]
            print(f"  Sample date columns: {sample_dates}")

        # Debug: show sample destinations from data
        sample_dests = list(set(dest for (y, m, dest) in list(monthly_data.keys())[:20]))[:10]
        print(f"  Sample destinations in data: {sample_dests}")

        # Update cells
        updated = 0
        not_found_destinations = set()
        not_found_dates = set()
        matched_destinations = set()

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
            # Census data is typically in metric tons for quantity
            value = data.get('quantity')

            if value and value > 0:
                # Convert to thousand metric tons for consistency with inspections
                value_in_tmt = value / 1000.0
                ws.range((row, col)).value = round(value_in_tmt, 3)
                updated += 1

        # Debug output
        print(f"  Matched destinations: {len(matched_destinations)}")
        if matched_destinations:
            print(f"    Examples: {list(matched_destinations)[:5]}")

        # Save workbook
        wb.save()
        print(f"Updated {updated} cells in {sheet_name}")
        logger.info(f"Updated {updated} cells in {sheet_name}")

        if not_found_destinations:
            unmapped = list(not_found_destinations)[:10]
            print(f"  Unmapped destinations ({len(not_found_destinations)} total): {unmapped}")
            logger.warning(f"Unmapped destinations: {not_found_destinations}")

        if not_found_dates:
            print(f"  Dates not in spreadsheet: {len(not_found_dates)}")

        wb.close()
        app.quit()
        return True

    except Exception as e:
        print(f"ERROR: Failed to update Excel file: {e}")
        logger.error(f"Failed to update Excel file: {e}")
        import traceback
        traceback.print_exc()
        try:
            wb.close()
            app.quit()
        except:
            pass
        return False


def update_all_excel_sheets(
    records: List[Dict],
    commodity: str,
    project_root: Path
) -> Dict[str, int]:
    """
    Update all relevant Excel sheets for a commodity (both imports and exports)

    Args:
        records: List of trade records
        commodity: Commodity name
        project_root: Project root path

    Returns:
        Dict with update counts per sheet
    """
    results = {}

    excel_file = EXCEL_FILES.get(commodity.upper())
    if not excel_file:
        print(f"No Excel file configured for {commodity}")
        return results

    excel_path = project_root / excel_file

    for flow in ['exports', 'imports']:
        # Filter records for this flow
        flow_records = [r for r in records if r.get('flow') == flow]
        if not flow_records:
            continue

        # Aggregate monthly by destination
        monthly_data = aggregate_monthly_by_destination(flow_records, flow)

        # Update Excel
        sheet_name = COMMODITY_SHEETS.get(commodity.upper(), {}).get(flow, flow)
        print(f"\nUpdating {sheet_name}...")

        if update_excel_file(excel_path, monthly_data, commodity, flow):
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
    update_excel: bool = False
) -> Dict:
    """
    Run the Census trade data update workflow

    Args:
        commodity: Commodity to process (SOYBEANS, SOYBEAN_MEAL, SOYBEAN_OIL, ALL)
        years: Number of years of history to fetch
        flow: 'exports', 'imports', or 'both'
        save_to_db: Save to database
        update_excel: Update Excel files

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

        for comm in commodities:
            # Get records for this commodity
            comm_records = [r for r in all_records if r.get('commodity') == comm]
            if comm_records:
                excel_results = update_all_excel_sheets(comm_records, comm, project_root)
                for sheet, count in excel_results.items():
                    print(f"  {sheet}: {count} month/destination combinations")

    results['success'] = True
    return results


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

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("CENSUS BUREAU TRADE DATA COLLECTOR")
    print("=" * 60)

    results = run_census_update(
        commodity=args.commodity,
        years=args.years,
        flow=args.flow,
        save_to_db=args.save_to_db,
        update_excel=args.update_excel
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
