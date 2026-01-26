"""
City Enrollment Service for Weather Data Pipeline
Handles automatic enrollment of new cities detected by the LLM.

Workflow:
1. Detect new city from email/text
2. Geocode to get coordinates
3. Determine appropriate region
4. Add to location registry (JSON + database)
5. Trigger historical data collection

Round Lakes Commodities
"""

import json
import logging
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# Load environment
project_root = Path(__file__).parent.parent.parent
load_dotenv(project_root / ".env")

logger = logging.getLogger(__name__)

# Import location service
try:
    from src.services.location_service import (
        LocationService, WeatherLocation, get_location_service
    )
    LOCATION_SERVICE_AVAILABLE = True
except ImportError:
    LOCATION_SERVICE_AVAILABLE = False
    logger.warning("Location service not available")


# Region definitions for auto-classification
REGION_DEFINITIONS = {
    "US_CORN_BELT": {
        "bounds": {"lat_min": 37, "lat_max": 48, "lon_min": -100, "lon_max": -80},
        "commodities": ["corn", "soybeans"],
        "timezone": "America/Chicago"
    },
    "US_WHEAT_BELT": {
        "bounds": {"lat_min": 31, "lat_max": 42, "lon_min": -104, "lon_max": -95},
        "commodities": ["wheat", "corn"],
        "timezone": "America/Chicago"
    },
    "US_DELTA": {
        "bounds": {"lat_min": 30, "lat_max": 36, "lon_min": -95, "lon_max": -88},
        "commodities": ["cotton", "rice", "soybeans"],
        "timezone": "America/Chicago"
    },
    "US_NORTHERN_PLAINS": {
        "bounds": {"lat_min": 42, "lat_max": 49, "lon_min": -111, "lon_max": -96},
        "commodities": ["wheat", "barley", "canola"],
        "timezone": "America/Denver"
    },
    "BR_CENTER_WEST": {
        "bounds": {"lat_min": -20, "lat_max": -10, "lon_min": -60, "lon_max": -45},
        "commodities": ["soybeans", "corn", "cotton"],
        "timezone": "America/Sao_Paulo"
    },
    "BR_SOUTH": {
        "bounds": {"lat_min": -32, "lat_max": -20, "lon_min": -55, "lon_max": -45},
        "commodities": ["soybeans", "corn", "wheat"],
        "timezone": "America/Sao_Paulo"
    },
    "AR_PAMPAS": {
        "bounds": {"lat_min": -40, "lat_max": -28, "lon_min": -66, "lon_max": -56},
        "commodities": ["soybeans", "corn", "wheat"],
        "timezone": "America/Argentina/Buenos_Aires"
    },
    "OTHER": {
        "bounds": None,
        "commodities": [],
        "timezone": "UTC"
    }
}


class CityEnrollmentService:
    """
    Service for automatically enrolling new cities in the weather monitoring system.

    When the LLM detects a new city that's not in our registry:
    1. Geocode to get lat/lon
    2. Classify into a region
    3. Add to location registry
    4. Trigger historical data collection
    """

    # OpenWeather Geocoding API
    GEOCODING_URL = "https://api.openweathermap.org/geo/1.0/direct"

    # Nominatim (free fallback)
    NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

    def __init__(self):
        """Initialize the enrollment service."""
        self.api_key = os.getenv('OPENWEATHER_API_KEY') or os.getenv('WEATHER_API_KEY')
        self.location_service = get_location_service() if LOCATION_SERVICE_AVAILABLE else None

        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RLC-CityEnrollment/1.0 (tore.alden@roundlakescommodities.com)'
        })

        logger.info("CityEnrollmentService initialized")

    def geocode_city(self, city_name: str, country_hint: str = None) -> Optional[Dict]:
        """
        Get coordinates for a city name using geocoding API.

        Args:
            city_name: City name (e.g., "St. Louis, MO" or "St. Louis")
            country_hint: Optional country code (e.g., "US", "BR", "AR")

        Returns:
            Dict with lat, lon, name, country, state or None if not found
        """
        # Try OpenWeather first if we have an API key
        if self.api_key:
            result = self._geocode_openweather(city_name, country_hint)
            if result:
                return result

        # Fallback to Nominatim
        return self._geocode_nominatim(city_name, country_hint)

    def _geocode_openweather(self, city_name: str, country_hint: str = None) -> Optional[Dict]:
        """Geocode using OpenWeather API."""
        try:
            query = city_name
            if country_hint:
                query = f"{city_name},{country_hint}"

            response = self.session.get(
                self.GEOCODING_URL,
                params={
                    'q': query,
                    'limit': 1,
                    'appid': self.api_key
                },
                timeout=10
            )
            response.raise_for_status()

            results = response.json()
            if results:
                r = results[0]
                return {
                    'lat': r['lat'],
                    'lon': r['lon'],
                    'name': r.get('name', city_name),
                    'country': r.get('country', country_hint or 'Unknown'),
                    'state': r.get('state', '')
                }
            return None

        except Exception as e:
            logger.warning(f"OpenWeather geocoding failed for {city_name}: {e}")
            return None

    def _geocode_nominatim(self, city_name: str, country_hint: str = None) -> Optional[Dict]:
        """Geocode using Nominatim (OpenStreetMap) as fallback."""
        try:
            query = city_name
            params = {
                'q': query,
                'format': 'json',
                'limit': 1
            }
            if country_hint:
                params['countrycodes'] = country_hint.lower()

            response = self.session.get(
                self.NOMINATIM_URL,
                params=params,
                timeout=10
            )
            response.raise_for_status()

            results = response.json()
            if results:
                r = results[0]
                # Parse display_name to get components
                parts = r.get('display_name', '').split(',')

                return {
                    'lat': float(r['lat']),
                    'lon': float(r['lon']),
                    'name': parts[0].strip() if parts else city_name,
                    'country': self._extract_country_code(r.get('display_name', '')),
                    'state': parts[1].strip() if len(parts) > 1 else ''
                }
            return None

        except Exception as e:
            logger.warning(f"Nominatim geocoding failed for {city_name}: {e}")
            return None

    def _extract_country_code(self, display_name: str) -> str:
        """Extract country code from display name."""
        display_lower = display_name.lower()

        if 'united states' in display_lower or ', us' in display_lower:
            return 'US'
        elif 'brazil' in display_lower or 'brasil' in display_lower:
            return 'BR'
        elif 'argentina' in display_lower:
            return 'AR'
        elif 'canada' in display_lower:
            return 'CA'

        return 'Unknown'

    def classify_region(self, lat: float, lon: float, country: str) -> Tuple[str, List[str], str]:
        """
        Classify a location into a region based on coordinates.

        Args:
            lat: Latitude
            lon: Longitude
            country: Country code

        Returns:
            Tuple of (region_code, commodities, timezone)
        """
        for region_code, region_info in REGION_DEFINITIONS.items():
            if region_code == "OTHER":
                continue

            bounds = region_info['bounds']
            if bounds is None:
                continue

            if (bounds['lat_min'] <= lat <= bounds['lat_max'] and
                bounds['lon_min'] <= lon <= bounds['lon_max']):
                return (
                    region_code,
                    region_info['commodities'],
                    region_info['timezone']
                )

        # Default based on country
        if country == 'US':
            return ('US_OTHER', ['corn', 'soybeans'], 'America/Chicago')
        elif country == 'BR':
            return ('BR_OTHER', ['soybeans', 'corn'], 'America/Sao_Paulo')
        elif country == 'AR':
            return ('AR_OTHER', ['soybeans', 'corn', 'wheat'], 'America/Argentina/Buenos_Aires')

        return ('OTHER', [], 'UTC')

    def generate_location_id(self, name: str, state: str, country: str) -> str:
        """Generate a location ID from name components."""
        # Clean name
        clean_name = re.sub(r'[^a-zA-Z0-9]', '_', name.lower())
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')

        # Add state abbreviation if US
        if country == 'US' and state:
            state_abbrev = self._get_state_abbrev(state)
            return f"{clean_name}_{state_abbrev.lower()}"
        elif state:
            state_clean = re.sub(r'[^a-zA-Z0-9]', '', state.lower())[:2]
            return f"{clean_name}_{state_clean}"

        return clean_name

    def _get_state_abbrev(self, state_name: str) -> str:
        """Get state abbreviation from full name."""
        state_map = {
            'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
            'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
            'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
            'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
            'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
            'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
            'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
            'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
            'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
            'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
            'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
            'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
            'wisconsin': 'WI', 'wyoming': 'WY'
        }

        # Check if already abbreviation
        if len(state_name) == 2:
            return state_name.upper()

        return state_map.get(state_name.lower(), state_name[:2].upper())

    def enroll_city(
        self,
        city_name: str,
        country_hint: str = None,
        pull_historical: bool = True,
        historical_start: date = None
    ) -> Optional[str]:
        """
        Enroll a new city in the weather monitoring system.

        Args:
            city_name: City name to enroll
            country_hint: Optional country code hint
            pull_historical: Whether to trigger historical data collection
            historical_start: Start date for historical data (default: 1 year ago)

        Returns:
            Location ID if successful, None otherwise
        """
        logger.info(f"Enrolling new city: {city_name}")

        # Check if already exists (by fuzzy match)
        if self.location_service:
            matches = self.location_service.fuzzy_match(city_name, threshold=0.85)
            if matches:
                existing_id = matches[0][0]
                logger.info(f"City {city_name} appears to match existing location: {existing_id}")
                return existing_id

        # Geocode the city
        geo = self.geocode_city(city_name, country_hint)
        if not geo:
            logger.error(f"Failed to geocode city: {city_name}")
            return None

        logger.info(f"Geocoded {city_name}: {geo['lat']:.2f}, {geo['lon']:.2f} ({geo['country']})")

        # Classify region
        region, commodities, timezone = self.classify_region(
            geo['lat'], geo['lon'], geo['country']
        )

        # Generate location ID
        location_id = self.generate_location_id(
            geo['name'], geo['state'], geo['country']
        )

        # Check for ID collision
        if self.location_service and self.location_service.location_exists(location_id):
            # Add suffix to make unique
            location_id = f"{location_id}_2"

        # Build display name
        if geo['state'] and geo['country'] == 'US':
            display_name = f"{geo['name']}, {self._get_state_abbrev(geo['state'])}"
        elif geo['state']:
            display_name = f"{geo['name']}, {geo['state']}"
        else:
            display_name = geo['name']

        # Create location object
        location = WeatherLocation(
            id=location_id,
            name=geo['name'],
            display_name=display_name,
            region=region,
            country=geo['country'],
            lat=geo['lat'],
            lon=geo['lon'],
            commodities=commodities,
            timezone=timezone,
            active=True,
            notes=f"Auto-enrolled {datetime.now().strftime('%Y-%m-%d')}"
        )

        # Add to location service
        aliases = [city_name.lower()]
        if geo['name'].lower() != city_name.lower():
            aliases.append(geo['name'].lower())

        if self.location_service:
            # Add to in-memory registry
            self.location_service.add_location(location, aliases)

            # Save to JSON config
            self.location_service.save_config()

            # Add to database
            self.location_service.add_location_to_database(location, aliases)

        logger.info(f"Enrolled location: {location_id} ({display_name}) in region {region}")

        # Trigger historical data collection
        if pull_historical:
            self._trigger_historical_collection(location_id, historical_start)

        return location_id

    def _trigger_historical_collection(self, location_id: str, start_date: date = None):
        """
        Trigger historical data collection for a new location.

        Args:
            location_id: Location ID to collect for
            start_date: Start date (default: 1 year ago)
        """
        if start_date is None:
            start_date = date.today() - timedelta(days=365)

        end_date = date.today() - timedelta(days=1)

        logger.info(f"Triggering historical collection for {location_id}: {start_date} to {end_date}")

        # Path to weather collector
        collector_path = project_root / "rlc_scheduler" / "agents" / "weather_collector_agent.py"

        if not collector_path.exists():
            logger.error(f"Weather collector not found at {collector_path}")
            return

        try:
            # Run collector in subprocess for the specific location
            # Note: We'd need to add --location flag support to the collector
            cmd = [
                sys.executable,
                str(collector_path),
                "--mode", "historical",
                "--start-date", start_date.strftime("%Y-%m-%d"),
                "--end-date", end_date.strftime("%Y-%m-%d"),
                "--location", location_id
            ]

            logger.info(f"Running: {' '.join(cmd)}")

            # Run in background
            subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True
            )

            logger.info(f"Historical collection started for {location_id}")

        except Exception as e:
            logger.error(f"Failed to trigger historical collection: {e}")

    def check_and_enroll_cities(self, city_names: List[str]) -> Dict[str, str]:
        """
        Check a list of cities and enroll any that are new.

        Args:
            city_names: List of city names to check

        Returns:
            Dict mapping city names to their location IDs
        """
        results = {}

        for city in city_names:
            # Try to resolve existing
            if self.location_service:
                existing = self.location_service.resolve_alias(city)
                if existing:
                    results[city] = existing
                    continue

            # Enroll new city
            location_id = self.enroll_city(city)
            if location_id:
                results[city] = location_id

        return results


# Convenience function
_enrollment_service: Optional[CityEnrollmentService] = None


def get_enrollment_service() -> CityEnrollmentService:
    """Get the default enrollment service instance."""
    global _enrollment_service
    if _enrollment_service is None:
        _enrollment_service = CityEnrollmentService()
    return _enrollment_service


def enroll_city(city_name: str, country_hint: str = None, pull_historical: bool = True) -> Optional[str]:
    """
    Convenience function to enroll a city.

    Args:
        city_name: City to enroll
        country_hint: Optional country code
        pull_historical: Whether to pull historical data

    Returns:
        Location ID if successful
    """
    return get_enrollment_service().enroll_city(city_name, country_hint, pull_historical)


if __name__ == "__main__":
    # Test the enrollment service
    logging.basicConfig(level=logging.INFO)

    service = CityEnrollmentService()

    # Test geocoding
    print("\n=== Geocoding Test ===")
    test_cities = ["St. Louis, MO", "Fargo, ND", "Waco, TX"]
    for city in test_cities:
        geo = service.geocode_city(city)
        if geo:
            print(f"{city} -> {geo['lat']:.2f}, {geo['lon']:.2f} ({geo['country']}, {geo['state']})")
        else:
            print(f"{city} -> NOT FOUND")

    # Test region classification
    print("\n=== Region Classification Test ===")
    test_coords = [
        (38.63, -90.20, "US"),  # St. Louis
        (46.88, -96.79, "US"),  # Fargo
        (-12.97, -55.81, "BR"),  # Sorriso
    ]
    for lat, lon, country in test_coords:
        region, commodities, tz = service.classify_region(lat, lon, country)
        print(f"({lat}, {lon}, {country}) -> {region}, {commodities}")

    print("\n=== Enrollment Test (dry run) ===")
    print("To enroll a city, run:")
    print("  python city_enrollment_service.py --enroll 'St. Louis, MO'")
