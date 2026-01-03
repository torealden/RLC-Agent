"""
Weather API Service
Fetches weather data for ranch operations and crop conditions
Round Lakes Commodities
"""

import logging
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger('rlc_master_agent.services.weather')


@dataclass
class WeatherCondition:
    """Represents current weather conditions"""
    location: str
    temperature: float
    feels_like: float
    humidity: int
    description: str
    wind_speed: float
    timestamp: datetime


@dataclass
class WeatherForecast:
    """Represents a weather forecast"""
    location: str
    date: str
    temp_high: float
    temp_low: float
    description: str
    precipitation_chance: float
    conditions: str


class WeatherService:
    """
    Service for fetching weather data using OpenWeatherMap API

    Used for:
    - Ranch operations planning
    - Crop condition monitoring
    - Logistics/transportation planning

    API Documentation: https://openweathermap.org/api
    """

    # Predefined locations for RLC operations
    RLC_LOCATIONS = {
        'headquarters': {'lat': 42.0, 'lon': -93.5},  # Placeholder - update with actual location
        'ranch': {'lat': 42.0, 'lon': -93.5},
        'chicago': {'lat': 41.8781, 'lon': -87.6298},  # Major trading hub
        'kansas_city': {'lat': 39.0997, 'lon': -94.5786},  # Cattle market
        'omaha': {'lat': 41.2565, 'lon': -95.9345},  # Cattle market
    }

    def __init__(self, api_key: str, base_url: str = "https://api.openweathermap.org/data/2.5"):
        """
        Initialize Weather Service

        Args:
            api_key: OpenWeatherMap API key
            base_url: API base URL
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make API request with error handling

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            API response as dictionary
        """
        url = f"{self.base_url}/{endpoint}"
        params = params or {}

        # Add API key
        if self.api_key:
            params['appid'] = self.api_key

        # Default to imperial units for US
        if 'units' not in params:
            params['units'] = 'imperial'

        try:
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Weather API timeout for {endpoint}")
            raise Exception("Weather API request timed out")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Weather API HTTP error: {e}")
            raise Exception(f"Weather API error: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Weather API request failed: {e}")
            raise Exception(f"Weather API request failed: {str(e)}")

    def _resolve_location(self, location: str) -> Dict[str, float]:
        """
        Resolve location name to coordinates

        Args:
            location: Location name or 'lat,lon' string

        Returns:
            Dictionary with lat and lon
        """
        # Check predefined locations
        if location.lower() in self.RLC_LOCATIONS:
            return self.RLC_LOCATIONS[location.lower()]

        # Check if it's coordinates
        if ',' in location:
            try:
                lat, lon = map(float, location.split(','))
                return {'lat': lat, 'lon': lon}
            except ValueError:
                pass

        # Use geocoding API
        try:
            geo_url = f"http://api.openweathermap.org/geo/1.0/direct"
            params = {
                'q': f"{location},US",
                'limit': 1,
                'appid': self.api_key
            }
            response = self.session.get(geo_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()

            if data:
                return {'lat': data[0]['lat'], 'lon': data[0]['lon']}
        except Exception as e:
            logger.warning(f"Geocoding failed for {location}: {e}")

        raise ValueError(f"Could not resolve location: {location}")

    def get_current_weather(self, location: str) -> Dict[str, Any]:
        """
        Get current weather conditions

        Args:
            location: Location name, coordinates, or predefined location key

        Returns:
            Current weather data
        """
        try:
            coords = self._resolve_location(location)
        except ValueError as e:
            return {'status': 'error', 'message': str(e)}

        params = {
            'lat': coords['lat'],
            'lon': coords['lon']
        }

        try:
            data = self._make_request('weather', params)

            return {
                'location': data.get('name', location),
                'coordinates': coords,
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'description': data['weather'][0]['description'],
                'conditions': data['weather'][0]['main'],
                'wind': {
                    'speed': data['wind']['speed'],
                    'direction': data['wind'].get('deg', 0)
                },
                'visibility': data.get('visibility', 0) / 1609.34,  # Convert to miles
                'clouds': data['clouds']['all'],
                'timestamp': datetime.fromtimestamp(data['dt']).isoformat(),
                'sunrise': datetime.fromtimestamp(data['sys']['sunrise']).strftime('%H:%M'),
                'sunset': datetime.fromtimestamp(data['sys']['sunset']).strftime('%H:%M')
            }
        except Exception as e:
            logger.error(f"Failed to get weather for {location}: {e}")
            return {
                'location': location,
                'status': 'error',
                'message': str(e)
            }

    def get_forecast(
        self,
        location: str,
        days: int = 5
    ) -> Dict[str, Any]:
        """
        Get weather forecast

        Args:
            location: Location name or coordinates
            days: Number of days (max 5 for free tier)

        Returns:
            Weather forecast
        """
        try:
            coords = self._resolve_location(location)
        except ValueError as e:
            return {'status': 'error', 'message': str(e)}

        params = {
            'lat': coords['lat'],
            'lon': coords['lon'],
            'cnt': min(days * 8, 40)  # 3-hour intervals, max 40
        }

        try:
            data = self._make_request('forecast', params)

            # Group by day
            daily = {}
            for item in data['list']:
                date = datetime.fromtimestamp(item['dt']).strftime('%Y-%m-%d')
                if date not in daily:
                    daily[date] = {
                        'temps': [],
                        'conditions': [],
                        'precipitation': 0
                    }
                daily[date]['temps'].append(item['main']['temp'])
                daily[date]['conditions'].append(item['weather'][0]['main'])
                if 'pop' in item:
                    daily[date]['precipitation'] = max(
                        daily[date]['precipitation'],
                        item['pop'] * 100
                    )

            # Summarize daily forecasts
            forecasts = []
            for date, info in list(daily.items())[:days]:
                forecasts.append({
                    'date': date,
                    'high': round(max(info['temps']), 1),
                    'low': round(min(info['temps']), 1),
                    'conditions': max(set(info['conditions']), key=info['conditions'].count),
                    'precipitation_chance': round(info['precipitation'], 0)
                })

            return {
                'location': data['city']['name'],
                'coordinates': coords,
                'forecast': forecasts,
                'generated': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get forecast for {location}: {e}")
            return {
                'location': location,
                'status': 'error',
                'message': str(e)
            }

    def get_agricultural_conditions(self, location: str) -> Dict[str, Any]:
        """
        Get weather conditions relevant to agricultural operations

        Args:
            location: Location name

        Returns:
            Agricultural weather assessment
        """
        current = self.get_current_weather(location)
        forecast = self.get_forecast(location, days=5)

        if 'error' in current.get('status', ''):
            return current

        # Assess conditions
        conditions = {
            'location': current['location'],
            'current': current,
            'forecast': forecast.get('forecast', []),
            'assessment': {
                'fieldwork_suitable': self._assess_fieldwork(current, forecast),
                'livestock_concerns': self._assess_livestock(current),
                'precipitation_outlook': self._assess_precipitation(forecast),
                'temperature_trend': self._assess_temperature_trend(forecast)
            }
        }

        return conditions

    def _assess_fieldwork(self, current: Dict, forecast: Dict) -> Dict[str, Any]:
        """Assess suitability for field operations"""
        temp = current.get('temperature', 70)
        wind = current.get('wind', {}).get('speed', 0)
        conditions = current.get('conditions', '')

        suitable = True
        reasons = []

        if temp < 32:
            suitable = False
            reasons.append("Temperature below freezing")
        elif temp > 95:
            suitable = False
            reasons.append("Excessive heat")

        if wind > 25:
            suitable = False
            reasons.append("High wind speeds")

        if conditions.lower() in ['rain', 'thunderstorm', 'snow']:
            suitable = False
            reasons.append(f"Active precipitation: {conditions}")

        # Check forecast for upcoming rain
        upcoming_rain = any(
            f.get('conditions', '').lower() in ['rain', 'thunderstorm']
            for f in forecast.get('forecast', [])[:2]
        )
        if upcoming_rain:
            reasons.append("Rain expected in next 48 hours")

        return {
            'suitable': suitable,
            'reasons': reasons or ['Conditions favorable for field work']
        }

    def _assess_livestock(self, current: Dict) -> Dict[str, Any]:
        """Assess livestock welfare concerns"""
        temp = current.get('temperature', 70)
        humidity = current.get('humidity', 50)
        conditions = current.get('conditions', '')

        concerns = []
        risk_level = 'low'

        # Heat stress assessment
        if temp > 85 and humidity > 60:
            concerns.append("Heat stress risk - ensure adequate water and shade")
            risk_level = 'moderate'
        if temp > 95:
            concerns.append("Extreme heat - minimize cattle movement")
            risk_level = 'high'

        # Cold stress
        if temp < 20:
            concerns.append("Cold stress - check windbreaks and bedding")
            risk_level = 'moderate'
        if temp < 0:
            concerns.append("Extreme cold - risk of frostbite")
            risk_level = 'high'

        # Severe weather
        if conditions.lower() in ['thunderstorm', 'tornado']:
            concerns.append(f"Severe weather alert: {conditions}")
            risk_level = 'high'

        return {
            'risk_level': risk_level,
            'concerns': concerns or ['No immediate livestock weather concerns']
        }

    def _assess_precipitation(self, forecast: Dict) -> Dict[str, Any]:
        """Assess precipitation outlook"""
        forecasts = forecast.get('forecast', [])
        if not forecasts:
            return {'status': 'unknown'}

        total_precip_chance = sum(f.get('precipitation_chance', 0) for f in forecasts)
        rain_days = sum(1 for f in forecasts if f.get('precipitation_chance', 0) > 50)

        return {
            'rain_days_expected': rain_days,
            'average_precip_chance': round(total_precip_chance / len(forecasts), 0),
            'outlook': 'wet' if rain_days >= 3 else 'dry' if rain_days == 0 else 'mixed'
        }

    def _assess_temperature_trend(self, forecast: Dict) -> Dict[str, Any]:
        """Assess temperature trend"""
        forecasts = forecast.get('forecast', [])
        if len(forecasts) < 2:
            return {'trend': 'unknown'}

        first_avg = (forecasts[0].get('high', 0) + forecasts[0].get('low', 0)) / 2
        last_avg = (forecasts[-1].get('high', 0) + forecasts[-1].get('low', 0)) / 2
        diff = last_avg - first_avg

        if diff > 5:
            trend = 'warming'
        elif diff < -5:
            trend = 'cooling'
        else:
            trend = 'stable'

        return {
            'trend': trend,
            'change': round(diff, 1),
            'unit': 'F'
        }

    def get_multiple_locations(self, locations: List[str]) -> Dict[str, Any]:
        """
        Get weather for multiple locations at once

        Args:
            locations: List of location names

        Returns:
            Weather data for all locations
        """
        results = {}
        for location in locations:
            results[location] = self.get_current_weather(location)
        return results

    def list_predefined_locations(self) -> List[Dict[str, Any]]:
        """
        List predefined RLC locations

        Returns:
            List of available predefined locations
        """
        return [
            {'name': name, 'coordinates': coords}
            for name, coords in self.RLC_LOCATIONS.items()
        ]

    def health_check(self) -> Dict[str, Any]:
        """
        Check API connectivity

        Returns:
            Health status
        """
        if not self.api_key:
            return {
                'status': 'unconfigured',
                'message': 'Weather API key not configured'
            }

        try:
            # Try a simple request for Chicago
            params = {
                'lat': 41.8781,
                'lon': -87.6298,
                'appid': self.api_key,
                'units': 'imperial'
            }
            response = self.session.get(
                f"{self.base_url}/weather",
                params=params,
                timeout=10
            )

            return {
                'status': 'healthy' if response.ok else 'degraded',
                'api_key_configured': True,
                'base_url': self.base_url,
                'response_code': response.status_code
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'api_key_configured': bool(self.api_key),
                'base_url': self.base_url
            }
