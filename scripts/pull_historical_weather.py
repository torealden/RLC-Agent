#!/usr/bin/env python3
"""
Pull Historical Weather Data for Agricultural Locations

Uses Open-Meteo API (free, no key required) to fetch 5 years of daily weather
data for key agricultural regions and stores in PostgreSQL.

Usage:
    python scripts/pull_historical_weather.py
    python scripts/pull_historical_weather.py --years 3
    python scripts/pull_historical_weather.py --location des_moines_ia
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
import time

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Load credentials
credentials_path = project_root / "config" / "credentials.env"
if credentials_path.exists():
    load_dotenv(credentials_path)
else:
    load_dotenv()

try:
    import requests
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Run: pip install requests psycopg2-binary")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class WeatherLocation:
    """Agricultural weather location"""
    id: str
    name: str
    region: str
    lat: float
    lon: float
    commodities: List[str]
    country: str


# Key agricultural locations (same as world_weather_service.py)
AG_LOCATIONS = {
    # US Corn Belt
    'des_moines_ia': WeatherLocation(
        id='des_moines_ia', name="Des Moines, IA", region="US_CORN_BELT",
        lat=41.59, lon=-93.62, commodities=['corn', 'soybeans'], country="US"
    ),
    'champaign_il': WeatherLocation(
        id='champaign_il', name="Champaign, IL", region="US_CORN_BELT",
        lat=40.12, lon=-88.24, commodities=['corn', 'soybeans'], country="US"
    ),
    'lincoln_ne': WeatherLocation(
        id='lincoln_ne', name="Lincoln, NE", region="US_CORN_BELT",
        lat=40.81, lon=-96.70, commodities=['corn', 'soybeans'], country="US"
    ),
    'indianapolis_in': WeatherLocation(
        id='indianapolis_in', name="Indianapolis, IN", region="US_CORN_BELT",
        lat=39.77, lon=-86.16, commodities=['corn', 'soybeans'], country="US"
    ),
    'minneapolis_mn': WeatherLocation(
        id='minneapolis_mn', name="Minneapolis, MN", region="US_CORN_BELT",
        lat=44.98, lon=-93.27, commodities=['corn', 'soybeans', 'wheat'], country="US"
    ),

    # US Wheat Belt
    'dodge_city_ks': WeatherLocation(
        id='dodge_city_ks', name="Dodge City, KS", region="US_WHEAT_BELT",
        lat=37.75, lon=-100.02, commodities=['wheat_hrw'], country="US"
    ),
    'amarillo_tx': WeatherLocation(
        id='amarillo_tx', name="Amarillo, TX", region="US_WHEAT_BELT",
        lat=35.22, lon=-101.83, commodities=['wheat_hrw'], country="US"
    ),
    'oklahoma_city_ok': WeatherLocation(
        id='oklahoma_city_ok', name="Oklahoma City, OK", region="US_WHEAT_BELT",
        lat=35.47, lon=-97.52, commodities=['wheat_hrw'], country="US"
    ),
    'bismarck_nd': WeatherLocation(
        id='bismarck_nd', name="Bismarck, ND", region="US_WHEAT_BELT",
        lat=46.81, lon=-100.78, commodities=['wheat_hrs', 'soybeans'], country="US"
    ),

    # US Delta
    'memphis_tn': WeatherLocation(
        id='memphis_tn', name="Memphis, TN", region="US_DELTA",
        lat=35.15, lon=-90.05, commodities=['cotton', 'soybeans', 'rice'], country="US"
    ),

    # Brazil - Center West (Mato Grosso)
    'sorriso_mt': WeatherLocation(
        id='sorriso_mt', name="Sorriso, MT", region="BRAZIL_CENTER_WEST",
        lat=-12.55, lon=-55.71, commodities=['soybeans', 'corn'], country="BR"
    ),
    'rondonopolis_mt': WeatherLocation(
        id='rondonopolis_mt', name="Rondonópolis, MT", region="BRAZIL_CENTER_WEST",
        lat=-16.47, lon=-54.64, commodities=['soybeans', 'corn'], country="BR"
    ),
    'cuiaba_mt': WeatherLocation(
        id='cuiaba_mt', name="Cuiabá, MT", region="BRAZIL_CENTER_WEST",
        lat=-15.60, lon=-56.10, commodities=['soybeans', 'corn'], country="BR"
    ),

    # Brazil - South
    'londrina_pr': WeatherLocation(
        id='londrina_pr', name="Londrina, PR", region="BRAZIL_SOUTH",
        lat=-23.31, lon=-51.16, commodities=['soybeans', 'corn', 'wheat'], country="BR"
    ),
    'porto_alegre_rs': WeatherLocation(
        id='porto_alegre_rs', name="Porto Alegre, RS", region="BRAZIL_SOUTH",
        lat=-30.03, lon=-51.23, commodities=['soybeans', 'rice', 'wheat'], country="BR"
    ),
    'cascavel_pr': WeatherLocation(
        id='cascavel_pr', name="Cascavel, PR", region="BRAZIL_SOUTH",
        lat=-24.96, lon=-53.46, commodities=['soybeans', 'corn'], country="BR"
    ),

    # Brazil - Northeast (MATOPIBA)
    'barreiras_ba': WeatherLocation(
        id='barreiras_ba', name="Barreiras, BA", region="BRAZIL_NORTHEAST",
        lat=-12.15, lon=-44.99, commodities=['soybeans', 'cotton'], country="BR"
    ),

    # Argentina - Pampas
    'rosario_sf': WeatherLocation(
        id='rosario_sf', name="Rosario, SF", region="ARGENTINA_PAMPAS",
        lat=-32.95, lon=-60.65, commodities=['soybeans', 'corn', 'wheat'], country="AR"
    ),
    'cordoba_ar': WeatherLocation(
        id='cordoba_ar', name="Córdoba", region="ARGENTINA_PAMPAS",
        lat=-31.42, lon=-64.18, commodities=['soybeans', 'corn'], country="AR"
    ),
    'buenos_aires_ar': WeatherLocation(
        id='buenos_aires_ar', name="Buenos Aires", region="ARGENTINA_PAMPAS",
        lat=-34.60, lon=-58.38, commodities=['soybeans', 'wheat', 'corn'], country="AR"
    ),

    # Argentina - North
    'tucuman_ar': WeatherLocation(
        id='tucuman_ar', name="Tucumán", region="ARGENTINA_NORTH",
        lat=-26.82, lon=-65.22, commodities=['soybeans', 'corn', 'sugarcane'], country="AR"
    ),
}


def celsius_to_fahrenheit(celsius: float) -> float:
    """Convert Celsius to Fahrenheit"""
    if celsius is None:
        return None
    return celsius * 9/5 + 32


def kmh_to_mph(kmh: float) -> float:
    """Convert km/h to mph"""
    if kmh is None:
        return None
    return kmh * 0.621371


def fetch_open_meteo_historical(
    lat: float,
    lon: float,
    start_date: date,
    end_date: date
) -> Optional[Dict]:
    """Fetch historical weather data from Open-Meteo API"""

    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat(),
        'daily': [
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'precipitation_hours',
            'wind_speed_10m_max',
            'wind_gusts_10m_max',
            'et0_fao_evapotranspiration',
            'weather_code',
        ],
        'timezone': 'auto',
        'temperature_unit': 'celsius',
        'wind_speed_unit': 'kmh',
        'precipitation_unit': 'mm',
    }

    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Open-Meteo API error: {e}")
        return None


def get_db_connection():
    """Get PostgreSQL database connection"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=int(os.getenv('DB_PORT', 5432)),
        database=os.getenv('DB_NAME', 'rlc_commodities'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', '')
    )


def store_weather_data(location: WeatherLocation, data: Dict) -> int:
    """Store weather data in PostgreSQL"""

    if not data or 'daily' not in data:
        return 0

    daily = data['daily']
    dates = daily.get('time', [])

    if not dates:
        return 0

    records = []
    for i, date_str in enumerate(dates):
        record = (
            location.id,
            location.name,
            location.country,
            location.region,
            location.lat,
            location.lon,
            date_str,
            celsius_to_fahrenheit(daily['temperature_2m_max'][i]) if daily['temperature_2m_max'][i] is not None else None,
            celsius_to_fahrenheit(daily['temperature_2m_min'][i]) if daily['temperature_2m_min'][i] is not None else None,
            celsius_to_fahrenheit(daily['temperature_2m_mean'][i]) if daily['temperature_2m_mean'][i] is not None else None,
            daily['precipitation_sum'][i],
            daily['precipitation_hours'][i],
            kmh_to_mph(daily['wind_speed_10m_max'][i]) if daily['wind_speed_10m_max'][i] is not None else None,
            kmh_to_mph(daily['wind_gusts_10m_max'][i]) if daily['wind_gusts_10m_max'][i] is not None else None,
            None,  # soil_moisture - not available in free API
            None,  # soil_temp - not available in free API
            daily['et0_fao_evapotranspiration'][i],
            daily['weather_code'][i],
            location.commodities,
        )
        records.append(record)

    conn = get_db_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO weather_history (
            location_id, location_name, country, region, lat, lon, date,
            temp_max_f, temp_min_f, temp_mean_f, precipitation_mm,
            precipitation_hours, wind_speed_max_mph, wind_gusts_max_mph,
            soil_moisture_0_7cm, soil_temp_0_7cm_f, et0_mm, weather_code,
            commodities
        ) VALUES %s
        ON CONFLICT (location_id, date) DO UPDATE SET
            temp_max_f = EXCLUDED.temp_max_f,
            temp_min_f = EXCLUDED.temp_min_f,
            temp_mean_f = EXCLUDED.temp_mean_f,
            precipitation_mm = EXCLUDED.precipitation_mm,
            precipitation_hours = EXCLUDED.precipitation_hours,
            wind_speed_max_mph = EXCLUDED.wind_speed_max_mph,
            wind_gusts_max_mph = EXCLUDED.wind_gusts_max_mph,
            et0_mm = EXCLUDED.et0_mm,
            weather_code = EXCLUDED.weather_code
    """

    try:
        execute_values(cursor, insert_sql, records)
        conn.commit()
        inserted = len(records)
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        inserted = 0
    finally:
        cursor.close()
        conn.close()

    return inserted


def pull_historical_weather(
    years: int = 5,
    locations: Optional[List[str]] = None
):
    """Pull historical weather data for all or specified locations"""

    end_date = date.today() - timedelta(days=1)  # Yesterday (today may not be complete)
    start_date = end_date - timedelta(days=years * 365)

    logger.info(f"Pulling weather data from {start_date} to {end_date} ({years} years)")

    # Filter locations if specified
    if locations:
        locs = {k: v for k, v in AG_LOCATIONS.items() if k in locations}
    else:
        locs = AG_LOCATIONS

    total_records = 0

    for loc_id, location in locs.items():
        logger.info(f"Fetching data for {location.name} ({location.country})...")

        # Open-Meteo allows up to 1 year per request for historical data
        # So we need to chunk requests by year
        current_start = start_date
        loc_records = 0

        while current_start < end_date:
            current_end = min(current_start + timedelta(days=365), end_date)

            data = fetch_open_meteo_historical(
                location.lat,
                location.lon,
                current_start,
                current_end
            )

            if data:
                inserted = store_weather_data(location, data)
                loc_records += inserted
                logger.info(f"  {current_start} to {current_end}: {inserted} records")
            else:
                logger.warning(f"  {current_start} to {current_end}: No data returned")

            current_start = current_end + timedelta(days=1)

            # Be nice to the free API
            time.sleep(0.5)

        total_records += loc_records
        logger.info(f"  Total for {location.name}: {loc_records} records")

    return total_records


def main():
    parser = argparse.ArgumentParser(
        description='Pull historical weather data for agricultural locations'
    )
    parser.add_argument('--years', '-y', type=int, default=5,
                       help='Number of years of history (default: 5)')
    parser.add_argument('--location', '-l', nargs='+',
                       help='Specific location(s) to fetch')
    parser.add_argument('--list', action='store_true',
                       help='List available locations')

    args = parser.parse_args()

    if args.list:
        print("\nAvailable agricultural locations:")
        print("-" * 60)
        for loc_id, loc in AG_LOCATIONS.items():
            print(f"  {loc_id:20} {loc.name:25} ({loc.country}) - {', '.join(loc.commodities)}")
        return

    print("\n" + "=" * 60)
    print("HISTORICAL WEATHER DATA PULL")
    print("=" * 60)
    print(f"Years: {args.years}")
    print(f"Locations: {args.location if args.location else 'All'}")
    print("=" * 60 + "\n")

    total = pull_historical_weather(
        years=args.years,
        locations=args.location
    )

    print("\n" + "=" * 60)
    print(f"COMPLETE: {total:,} total weather records stored")
    print("=" * 60)


if __name__ == '__main__':
    main()
