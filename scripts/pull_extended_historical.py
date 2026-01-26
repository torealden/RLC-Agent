#!/usr/bin/env python3
"""
Pull Extended Historical Weather Data
Collects weather data from 2000-2024 for all agricultural locations.

This covers key market events:
- 2012 US drought (major corn/soybean impact)
- 2011 drought
- 2008 flood
- 2018-2019 US-China trade war
- 2020 derecho
- 2021 Brazil drought
- Multiple El Niño/La Niña cycles

Round Lakes Commodities
"""

import json
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests
from dotenv import load_dotenv

# Setup paths
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

load_dotenv(project_root / ".env")

# Database
try:
    import psycopg2
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('HistoricalPull')

# Open-Meteo Archive API
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Database config
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 5432)),
    'database': os.getenv('DB_NAME', 'rlc_commodities'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '')
}


def get_locations():
    """Load locations from JSON config."""
    config_path = project_root / "config" / "weather_locations.json"
    with open(config_path, 'r') as f:
        config = json.load(f)
    return [loc for loc in config['locations'] if loc.get('active', True)]


def fetch_year_data(lat: float, lon: float, year: int, max_retries: int = 5) -> dict:
    """Fetch one year of historical data from Open-Meteo Archive API with retries."""
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"

    # Don't fetch future dates
    today = date.today()
    if date(year, 12, 31) > today:
        end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        'latitude': lat,
        'longitude': lon,
        'start_date': start_date,
        'end_date': end_date,
        'daily': ','.join([
            'temperature_2m_max',
            'temperature_2m_min',
            'temperature_2m_mean',
            'precipitation_sum',
            'precipitation_hours',
            'wind_speed_10m_max',
            'wind_gusts_10m_max',
            'weather_code',
            'et0_fao_evapotranspiration'
        ]),
        'timezone': 'auto'
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(ARCHIVE_URL, params=params, timeout=60)

            if response.status_code == 429:
                # Rate limited - exponential backoff
                wait_time = (2 ** attempt) * 10  # 10, 20, 40, 80, 160 seconds
                logger.warning(f"Rate limited, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 5
                logger.warning(f"Request failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
            else:
                raise

    raise Exception(f"Failed after {max_retries} retries")


def check_existing_data(location_id: str, year: int) -> int:
    """Check how many days of data already exist for a location/year."""
    if not DB_AVAILABLE:
        return 0

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        cursor.execute("""
            SELECT COUNT(*) FROM silver.weather_observation
            WHERE location_id = %s
            AND observation_date BETWEEN %s AND %s
        """, (location_id, start_date, end_date))

        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count

    except Exception as e:
        logger.error(f"Error checking existing data: {e}")
        return 0


def save_to_database(location_id: str, data: dict, batch_id: str):
    """Save historical data to bronze and transform to silver."""
    if not DB_AVAILABLE:
        logger.warning("Database not available")
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    daily = data.get('daily', {})
    dates = daily.get('time', [])

    inserted = 0

    for i, date_str in enumerate(dates):
        obs_date = datetime.strptime(date_str, '%Y-%m-%d').date()

        # Build day-specific JSON
        day_data = {
            'daily': {
                key: [val[i]] if isinstance(val, list) and i < len(val) else val
                for key, val in daily.items()
            },
            'latitude': data.get('latitude'),
            'longitude': data.get('longitude'),
            'timezone': data.get('timezone')
        }

        # Insert into bronze
        try:
            cursor.execute("""
                INSERT INTO bronze.weather_raw (
                    location_id, source, raw_response, observation_date, batch_id, is_processed
                ) VALUES (%s, %s, %s, %s, %s, FALSE)
                ON CONFLICT (location_id, source, observation_date)
                DO UPDATE SET raw_response = EXCLUDED.raw_response, is_processed = FALSE
            """, (location_id, 'open_meteo', json.dumps(day_data), obs_date, batch_id))
            inserted += 1
        except Exception as e:
            logger.error(f"Error inserting {location_id} {date_str}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    return inserted


def transform_batch(batch_id: str):
    """Transform the batch from bronze to silver."""
    if not DB_AVAILABLE:
        return 0

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM bronze.process_weather_to_silver(%s)", (batch_id,))
    result = cursor.fetchone()

    conn.commit()
    cursor.close()
    conn.close()

    return result[0] if result else 0


def main():
    """Main entry point."""
    import argparse
    import uuid

    parser = argparse.ArgumentParser(description='Pull Extended Historical Weather Data')
    parser.add_argument('--start-year', type=int, default=2000, help='Start year (default: 2000)')
    parser.add_argument('--end-year', type=int, default=2024, help='End year (default: 2024)')
    parser.add_argument('--location', type=str, help='Specific location ID (default: all)')
    parser.add_argument('--dry-run', action='store_true', help='Fetch but do not save')

    args = parser.parse_args()

    locations = get_locations()
    if args.location:
        locations = [l for l in locations if l['id'] == args.location]
        if not locations:
            logger.error(f"Location {args.location} not found")
            return 1

    years = list(range(args.start_year, args.end_year + 1))

    print("=" * 70)
    print("EXTENDED HISTORICAL WEATHER DATA PULL")
    print("=" * 70)
    print(f"Years: {args.start_year} - {args.end_year} ({len(years)} years)")
    print(f"Locations: {len(locations)}")
    print(f"Estimated records: ~{len(years) * 365 * len(locations):,}")
    print("=" * 70)
    print()

    total_records = 0
    total_transformed = 0
    failed_locations = []

    for loc in locations:
        loc_id = loc['id']
        loc_name = loc['display_name']
        lat, lon = loc['lat'], loc['lon']

        logger.info(f"Processing {loc_name}...")
        loc_records = 0

        for year in years:
            batch_id = str(uuid.uuid4())

            # Check if we already have data for this year
            existing_count = check_existing_data(loc_id, year)
            expected_days = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365

            # Adjust for current year
            if year == date.today().year:
                expected_days = (date.today() - date(year, 1, 1)).days

            if existing_count >= expected_days * 0.95:  # 95% threshold
                logger.info(f"  {year}: SKIP - already have {existing_count}/{expected_days} days")
                continue

            try:
                # Fetch data
                data = fetch_year_data(lat, lon, year)
                days = len(data.get('daily', {}).get('time', []))

                if args.dry_run:
                    logger.info(f"  {year}: {days} days (dry run)")
                    loc_records += days
                    continue

                # Save to database
                inserted = save_to_database(loc_id, data, batch_id)

                # Transform to silver
                transformed = transform_batch(batch_id)

                logger.info(f"  {year}: {inserted} inserted, {transformed} transformed")
                loc_records += inserted
                total_transformed += transformed

                # Rate limiting - Open-Meteo free tier is limited
                # 10,000 requests/day, ~7 requests/minute recommended
                time.sleep(10)  # 10 seconds between requests

            except Exception as e:
                logger.error(f"  {year}: ERROR - {e}")
                failed_locations.append((loc_id, year, str(e)))

        total_records += loc_records
        logger.info(f"  Total for {loc_name}: {loc_records} records")
        print()

    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Total records collected: {total_records:,}")
    print(f"Total records transformed: {total_transformed:,}")
    print(f"Failed: {len(failed_locations)}")
    if failed_locations:
        print("Failed locations:")
        for loc_id, year, error in failed_locations[:10]:
            print(f"  - {loc_id} ({year}): {error[:50]}")
    print("=" * 70)

    return 0 if not failed_locations else 1


if __name__ == "__main__":
    sys.exit(main())
