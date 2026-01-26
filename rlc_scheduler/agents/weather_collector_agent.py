#!/usr/bin/env python3
"""
Weather Collector Agent
Collects daily weather data for agricultural locations.

Sources:
- OpenWeather API: Current weather and 5-day forecast
- Open-Meteo API: Historical data (free, no key required)

Round Lakes Commodities
"""

import json
import logging
import os
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import requests
from dotenv import load_dotenv

# Load environment variables
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

# Try to import database connectivity
try:
    import psycopg2
    from psycopg2.extras import Json
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

# Import location service
try:
    from src.services.location_service import LocationService, WeatherLocation
    LOCATION_SERVICE_AVAILABLE = True
except ImportError:
    LOCATION_SERVICE_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('WeatherCollector')


@dataclass
class CollectorResult:
    """Result of a collection operation."""
    success: bool
    source: str
    locations_processed: int = 0
    records_collected: int = 0
    errors: List[str] = None
    batch_id: str = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class WeatherCollectorAgent:
    """
    Collects daily weather data for agricultural monitoring locations.

    Features:
    - Fetches current weather from OpenWeather API
    - Fetches historical data from Open-Meteo (free)
    - Stores raw data in bronze.weather_raw
    - Transforms to silver.weather_observation
    - Supports batch collection for all active locations
    """

    # API endpoints
    OPENWEATHER_BASE = "https://api.openweathermap.org/data/2.5"
    OPENWEATHER_ONECALL = "https://api.openweathermap.org/data/3.0/onecall"
    OPEN_METEO_BASE = "https://api.open-meteo.com/v1"

    def __init__(self):
        """Initialize the weather collector."""
        # Check for API key with common env var names
        self.openweather_api_key = os.getenv('OPENWEATHER_API_KEY') or os.getenv('WEATHER_API_KEY')
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'rlc_commodities'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }

        # Load location service
        if LOCATION_SERVICE_AVAILABLE:
            self.location_service = LocationService()
            self.location_service.load_config()
        else:
            self.location_service = None
            logger.warning("Location service not available, using fallback locations")

        # HTTP session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RLC-WeatherCollector/1.0'
        })

        logger.info(f"WeatherCollector initialized")
        logger.info(f"OpenWeather API key configured: {bool(self.openweather_api_key)}")

    def _get_db_connection(self):
        """Get database connection."""
        if not DB_AVAILABLE:
            return None
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return None

    def _get_active_locations(self) -> List[WeatherLocation]:
        """Get all active weather locations."""
        if self.location_service:
            return self.location_service.get_active_locations()

        # Fallback: load from JSON directly
        config_path = project_root / "config" / "weather_locations.json"
        if config_path.exists():
            with open(config_path, 'r') as f:
                config = json.load(f)
            locations = []
            for loc in config.get('locations', []):
                if loc.get('active', True):
                    locations.append(WeatherLocation(
                        id=loc['id'],
                        name=loc['name'],
                        display_name=loc['display_name'],
                        region=loc['region'],
                        country=loc['country'],
                        lat=loc['lat'],
                        lon=loc['lon'],
                        commodities=loc.get('commodities', []),
                        timezone=loc.get('timezone', 'UTC'),
                        active=True
                    ))
            return locations

        logger.error("No location data available")
        return []

    def fetch_openweather_current(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Fetch current weather from OpenWeather API.

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            API response dict or None on error
        """
        if not self.openweather_api_key:
            logger.warning("OpenWeather API key not configured")
            return None

        url = f"{self.OPENWEATHER_BASE}/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key,
            'units': 'metric'  # Celsius
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"OpenWeather API error: {e}")
            return None

    def fetch_openweather_onecall(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Fetch weather from OpenWeather One Call API 3.0.

        Includes:
        - Current weather
        - Minute forecast (1 hour)
        - Hourly forecast (48 hours)
        - Daily forecast (8 days)
        - Government weather alerts

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            API response dict or None on error
        """
        if not self.openweather_api_key:
            logger.warning("OpenWeather API key not configured")
            return None

        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.openweather_api_key,
            'units': 'metric'
        }

        try:
            response = self.session.get(self.OPENWEATHER_ONECALL, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            logger.debug(f"One Call API: {lat},{lon} - alerts: {len(data.get('alerts', []))}")
            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.error("One Call API 3.0 requires subscription - falling back to basic API")
            else:
                logger.error(f"OpenWeather One Call API error: {e}")
            return None
        except Exception as e:
            logger.error(f"OpenWeather One Call API error: {e}")
            return None

    def fetch_open_meteo_current(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Fetch current weather from Open-Meteo API (free, no key).

        Args:
            lat: Latitude
            lon: Longitude

        Returns:
            API response dict or None on error
        """
        url = f"{self.OPEN_METEO_BASE}/forecast"
        params = {
            'latitude': lat,
            'longitude': lon,
            'current_weather': 'true',
            'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,wind_speed_10m_max,weather_code',
            'timezone': 'auto',
            'forecast_days': 1
        }

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Open-Meteo API error: {e}")
            return None

    def fetch_open_meteo_historical(
        self,
        lat: float,
        lon: float,
        start_date: date,
        end_date: date
    ) -> Optional[Dict]:
        """
        Fetch historical weather from Open-Meteo Archive API.

        Args:
            lat: Latitude
            lon: Longitude
            start_date: Start date
            end_date: End date

        Returns:
            API response dict or None on error
        """
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            'latitude': lat,
            'longitude': lon,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'daily': 'temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum,precipitation_hours,wind_speed_10m_max,weather_code,et0_fao_evapotranspiration',
            'timezone': 'auto'
        }

        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Open-Meteo Archive API error: {e}")
            return None

    def save_to_bronze(
        self,
        location_id: str,
        source: str,
        raw_data: Dict,
        observation_date: date,
        batch_id: str
    ) -> bool:
        """
        Save raw weather data to bronze layer.

        Args:
            location_id: Location ID
            source: Data source ('openweather' or 'open_meteo')
            raw_data: Raw API response
            observation_date: Date for the observation
            batch_id: Batch UUID

        Returns:
            True if saved successfully
        """
        conn = self._get_db_connection()
        if not conn:
            logger.warning("Database not available, skipping bronze save")
            return False

        try:
            cursor = conn.cursor()

            sql = """
                INSERT INTO bronze.weather_raw (
                    location_id, source, raw_response, observation_date,
                    collected_at, batch_id, is_processed
                ) VALUES (%s, %s, %s, %s, %s, %s, FALSE)
                ON CONFLICT (location_id, source, observation_date)
                DO UPDATE SET
                    raw_response = EXCLUDED.raw_response,
                    collected_at = EXCLUDED.collected_at,
                    batch_id = EXCLUDED.batch_id,
                    is_processed = FALSE
            """

            cursor.execute(sql, (
                location_id,
                source,
                Json(raw_data),
                observation_date,
                datetime.now(),
                batch_id
            ))

            conn.commit()
            cursor.close()
            conn.close()
            return True

        except Exception as e:
            logger.error(f"Error saving to bronze: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return False

    def save_alerts_to_bronze(
        self,
        location_id: str,
        alerts: List[Dict],
        batch_id: str
    ) -> int:
        """
        Save government weather alerts to bronze layer.

        Args:
            location_id: Location ID
            alerts: List of alert dicts from One Call API
            batch_id: Batch UUID

        Returns:
            Number of alerts saved
        """
        if not alerts:
            return 0

        conn = self._get_db_connection()
        if not conn:
            return 0

        saved_count = 0
        try:
            cursor = conn.cursor()

            for alert in alerts:
                alert_id = f"{alert.get('event', 'unknown')}_{alert.get('start', 0)}"

                sql = """
                    INSERT INTO bronze.weather_alerts_raw (
                        location_id, alert_id, sender_name, event,
                        start_time, end_time, description, tags,
                        raw_data, collected_at, batch_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (location_id, alert_id)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        end_time = EXCLUDED.end_time,
                        raw_data = EXCLUDED.raw_data,
                        collected_at = EXCLUDED.collected_at
                """

                start_time = datetime.fromtimestamp(alert.get('start', 0)) if alert.get('start') else None
                end_time = datetime.fromtimestamp(alert.get('end', 0)) if alert.get('end') else None

                cursor.execute(sql, (
                    location_id,
                    alert_id,
                    alert.get('sender_name'),
                    alert.get('event', 'Unknown'),
                    start_time,
                    end_time,
                    alert.get('description'),
                    alert.get('tags', []),
                    Json(alert),
                    datetime.now(),
                    batch_id
                ))
                saved_count += 1

            conn.commit()
            cursor.close()
            conn.close()

            if saved_count > 0:
                logger.info(f"Saved {saved_count} weather alerts for {location_id}")

            return saved_count

        except Exception as e:
            logger.error(f"Error saving alerts: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return 0

    def process_alerts_to_silver(self) -> int:
        """Process bronze alerts to silver layer."""
        conn = self._get_db_connection()
        if not conn:
            return 0

        try:
            cursor = conn.cursor()

            sql = """
                INSERT INTO silver.weather_alert (
                    location_id, alert_id, event_type, severity, urgency,
                    start_time, end_time, headline, description, bronze_id
                )
                SELECT
                    ba.location_id,
                    ba.alert_id,
                    ba.event,
                    COALESCE(ba.raw_data->>'severity', 'Unknown'),
                    COALESCE(ba.raw_data->>'urgency', 'Unknown'),
                    ba.start_time,
                    ba.end_time,
                    SUBSTRING(ba.description FROM 1 FOR 500),
                    ba.description,
                    ba.id
                FROM bronze.weather_alerts_raw ba
                LEFT JOIN silver.weather_alert sa ON ba.location_id = sa.location_id AND ba.alert_id = sa.alert_id
                WHERE sa.id IS NULL
                ON CONFLICT (location_id, alert_id)
                DO UPDATE SET
                    end_time = EXCLUDED.end_time,
                    description = EXCLUDED.description
            """

            cursor.execute(sql)
            processed = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()

            if processed > 0:
                logger.info(f"Processed {processed} alerts to silver")

            return processed

        except Exception as e:
            logger.error(f"Error processing alerts to silver: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return 0

    def transform_to_silver(self, batch_id: str = None) -> int:
        """
        Transform unprocessed bronze records to silver.

        Args:
            batch_id: Optional batch ID to process (all if None)

        Returns:
            Number of records transformed
        """
        conn = self._get_db_connection()
        if not conn:
            return 0

        try:
            cursor = conn.cursor()

            # Call the transformation function
            cursor.execute(
                "SELECT * FROM bronze.process_weather_to_silver(%s)",
                (batch_id,)
            )
            result = cursor.fetchone()
            processed_count = result[0] if result else 0

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Transformed {processed_count} records to silver")
            return processed_count

        except Exception as e:
            logger.error(f"Error transforming to silver: {e}")
            if conn:
                conn.rollback()
                conn.close()
            return 0

    def collect_current_weather(self, locations: List[WeatherLocation] = None) -> CollectorResult:
        """
        Collect current weather for all (or specified) locations.

        Args:
            locations: List of locations (uses all active if None)

        Returns:
            CollectorResult with collection summary
        """
        batch_id = str(uuid.uuid4())
        today = date.today()

        if locations is None:
            locations = self._get_active_locations()

        logger.info(f"Collecting weather for {len(locations)} locations, batch={batch_id[:8]}")

        result = CollectorResult(
            success=True,
            source='weather_collector',
            batch_id=batch_id
        )

        total_alerts = 0

        for loc in locations:
            try:
                weather_data = None
                source = None
                alerts = []

                # Try One Call API 3.0 first (includes alerts)
                if self.openweather_api_key:
                    onecall_data = self.fetch_openweather_onecall(loc.lat, loc.lon)
                    if onecall_data:
                        weather_data = onecall_data
                        source = 'openweather_onecall'
                        alerts = onecall_data.get('alerts', [])

                        # Save alerts if any
                        if alerts:
                            alerts_saved = self.save_alerts_to_bronze(loc.id, alerts, batch_id)
                            total_alerts += alerts_saved

                # Fall back to basic OpenWeather API
                if not weather_data and self.openweather_api_key:
                    weather_data = self.fetch_openweather_current(loc.lat, loc.lon)
                    if weather_data:
                        source = 'openweather'

                # Fall back to Open-Meteo if OpenWeather failed or not configured
                if not weather_data:
                    weather_data = self.fetch_open_meteo_current(loc.lat, loc.lon)
                    if weather_data:
                        source = 'open_meteo'

                if weather_data:
                    saved = self.save_to_bronze(loc.id, source, weather_data, today, batch_id)
                    if saved:
                        result.records_collected += 1
                    result.locations_processed += 1
                    logger.debug(f"Collected weather for {loc.display_name} (alerts: {len(alerts)})")
                else:
                    result.errors.append(f"Failed to fetch weather for {loc.id}")

            except Exception as e:
                result.errors.append(f"Error collecting {loc.id}: {str(e)}")
                logger.error(f"Error collecting weather for {loc.id}: {e}")

        # Transform to silver
        if result.records_collected > 0:
            transformed = self.transform_to_silver(batch_id)
            logger.info(f"Transformed {transformed} records to silver layer")

        # Process alerts to silver
        if total_alerts > 0:
            self.process_alerts_to_silver()
            logger.info(f"Total alerts collected: {total_alerts}")

        result.success = result.records_collected > 0

        logger.info(
            f"Collection complete: {result.locations_processed} locations, "
            f"{result.records_collected} records, {len(result.errors)} errors"
        )

        return result

    def collect_historical(
        self,
        start_date: date,
        end_date: date = None,
        locations: List[WeatherLocation] = None
    ) -> CollectorResult:
        """
        Collect historical weather data.

        Args:
            start_date: Start date
            end_date: End date (defaults to yesterday)
            locations: Specific locations (uses all active if None)

        Returns:
            CollectorResult with collection summary
        """
        batch_id = str(uuid.uuid4())
        end_date = end_date or (date.today() - timedelta(days=1))

        if locations is None:
            locations = self._get_active_locations()

        logger.info(
            f"Collecting historical weather from {start_date} to {end_date} "
            f"for {len(locations)} locations"
        )

        result = CollectorResult(
            success=True,
            source='weather_collector_historical',
            batch_id=batch_id
        )

        for loc in locations:
            try:
                # Open-Meteo for historical (free)
                weather_data = self.fetch_open_meteo_historical(
                    loc.lat, loc.lon, start_date, end_date
                )

                if weather_data and 'daily' in weather_data:
                    # Save each day separately
                    daily = weather_data['daily']
                    dates = daily.get('time', [])

                    for i, date_str in enumerate(dates):
                        day_data = {
                            'daily': {
                                key: [val[i]] if isinstance(val, list) and i < len(val) else val
                                for key, val in daily.items()
                            },
                            'latitude': weather_data.get('latitude'),
                            'longitude': weather_data.get('longitude'),
                            'timezone': weather_data.get('timezone')
                        }

                        obs_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                        saved = self.save_to_bronze(loc.id, 'open_meteo', day_data, obs_date, batch_id)
                        if saved:
                            result.records_collected += 1

                    result.locations_processed += 1
                    logger.info(f"Collected {len(dates)} days for {loc.display_name}")
                else:
                    result.errors.append(f"No historical data for {loc.id}")

            except Exception as e:
                result.errors.append(f"Error collecting historical {loc.id}: {str(e)}")
                logger.error(f"Error collecting historical for {loc.id}: {e}")

        # Transform to silver
        if result.records_collected > 0:
            transformed = self.transform_to_silver(batch_id)
            logger.info(f"Transformed {transformed} records to silver layer")

        result.success = result.records_collected > 0
        return result

    def run(self, mode: str = 'current', **kwargs) -> CollectorResult:
        """
        Main entry point for the collector.

        Args:
            mode: 'current' for daily collection, 'historical' for backfill
            **kwargs: Additional arguments for the collection mode

        Returns:
            CollectorResult
        """
        logger.info(f"Starting weather collection, mode={mode}")

        if mode == 'current':
            locations = kwargs.get('locations')
            return self.collect_current_weather(locations)
        elif mode == 'historical':
            start_date = kwargs.get('start_date', date.today() - timedelta(days=7))
            end_date = kwargs.get('end_date')
            locations = kwargs.get('locations')
            return self.collect_historical(start_date, end_date, locations)
        else:
            logger.error(f"Unknown mode: {mode}")
            return CollectorResult(
                success=False,
                source='weather_collector',
                errors=[f"Unknown mode: {mode}"]
            )


def main():
    """Main entry point when run as script."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Weather Collector Agent - Collects daily weather for agricultural locations'
    )
    parser.add_argument(
        '--mode', '-m',
        choices=['current', 'historical', 'test'],
        default='current',
        help='Collection mode (default: current, test=fetch and display without DB)'
    )
    parser.add_argument(
        '--start-date', '-s',
        type=str,
        help='Start date for historical mode (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date', '-e',
        type=str,
        help='End date for historical mode (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Fetch but do not save to database'
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=3,
        help='Number of locations to test (default: 3)'
    )
    parser.add_argument(
        '--location',
        type=str,
        help='Specific location ID to collect (for single-location collection)'
    )

    args = parser.parse_args()

    # Test mode - fetch and display a few locations without database
    if args.mode == 'test':
        print("\n" + "=" * 60)
        print("WEATHER COLLECTOR TEST MODE")
        print("=" * 60)

        collector = WeatherCollectorAgent()
        locations = collector._get_active_locations()[:args.limit]

        print(f"\nTesting {len(locations)} locations...\n")

        for loc in locations:
            print(f"--- {loc.display_name} ({loc.id}) ---")
            print(f"    Coordinates: {loc.lat}, {loc.lon}")
            print(f"    Commodities: {', '.join(loc.commodities)}")

            # Try OpenWeather
            if collector.openweather_api_key:
                data = collector.fetch_openweather_current(loc.lat, loc.lon)
                if data:
                    temp = data.get('main', {}).get('temp')
                    desc = data.get('weather', [{}])[0].get('description', 'N/A')
                    print(f"    OpenWeather: {temp}C, {desc}")
                else:
                    print(f"    OpenWeather: FAILED (trying Open-Meteo)")

            # Try Open-Meteo
            data = collector.fetch_open_meteo_current(loc.lat, loc.lon)
            if data:
                current = data.get('current_weather', {})
                daily = data.get('daily', {})
                temp = current.get('temperature', 'N/A')
                temp_max = daily.get('temperature_2m_max', ['N/A'])[0]
                temp_min = daily.get('temperature_2m_min', ['N/A'])[0]
                precip = daily.get('precipitation_sum', ['N/A'])[0]
                print(f"    Open-Meteo: Current={temp}C, High={temp_max}C, Low={temp_min}C, Precip={precip}mm")
            else:
                print(f"    Open-Meteo: FAILED")
            print()

        print("=" * 60)
        print("Test complete!")
        print("=" * 60)
        return

    collector = WeatherCollectorAgent()

    kwargs = {}
    if args.start_date:
        kwargs['start_date'] = datetime.strptime(args.start_date, '%Y-%m-%d').date()
    if args.end_date:
        kwargs['end_date'] = datetime.strptime(args.end_date, '%Y-%m-%d').date()

    # Handle single location collection
    if args.location:
        if collector.location_service:
            loc = collector.location_service.get_location(args.location)
            if loc:
                kwargs['locations'] = [loc]
                print(f"\nCollecting for single location: {loc.display_name}")
            else:
                print(f"Error: Location '{args.location}' not found")
                sys.exit(1)
        else:
            print("Error: Location service not available")
            sys.exit(1)

    result = collector.run(mode=args.mode, **kwargs)

    print("\n" + "=" * 60)
    print("WEATHER COLLECTION RESULTS")
    print("=" * 60)
    print(f"Success: {result.success}")
    print(f"Source: {result.source}")
    print(f"Locations Processed: {result.locations_processed}")
    print(f"Records Collected: {result.records_collected}")
    print(f"Batch ID: {result.batch_id}")
    if result.errors:
        print(f"Errors ({len(result.errors)}):")
        for err in result.errors[:5]:
            print(f"  - {err}")
    print("=" * 60)

    sys.exit(0 if result.success else 1)


if __name__ == "__main__":
    main()
