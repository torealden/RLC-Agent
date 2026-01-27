"""
World Weather Integration Service
Monitors Gmail for World Weather emails, parses content, and generates weather briefs

Workflow:
1. Check Gmail for emails from World Weather (worldweather@bizkc.rr.com)
2. Parse email body and PDF attachments
3. Extract key weather changes by region (US, Brazil, Argentina)
4. Pull supplemental data from OpenWeather, Drought Monitor
5. Generate CC Weather Brief
6. Store in database and optionally send summary

Round Lakes Commodities
"""

import os
import re
import json
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path
from enum import Enum

# Load environment
from dotenv import load_dotenv
project_root = Path(__file__).parent.parent.parent
credentials_path = project_root / "config" / "credentials.env"
if credentials_path.exists():
    load_dotenv(credentials_path)

import requests

logger = logging.getLogger(__name__)


class WeatherRegion(Enum):
    """Weather regions for commodity monitoring"""
    US_CORN_BELT = "us_corn_belt"
    US_WHEAT_BELT = "us_wheat_belt"
    US_DELTA = "us_delta"
    BRAZIL_CENTER_WEST = "brazil_center_west"
    BRAZIL_SOUTH = "brazil_south"
    BRAZIL_NORTHEAST = "brazil_northeast"
    ARGENTINA_PAMPAS = "argentina_pampas"
    ARGENTINA_NORTH = "argentina_north"


@dataclass
class WeatherLocation:
    """A weather monitoring location"""
    name: str
    region: WeatherRegion
    lat: float
    lon: float
    commodities: List[str]
    country: str = "US"


# Key agricultural weather locations
AG_WEATHER_LOCATIONS = {
    # US Corn Belt
    'des_moines_ia': WeatherLocation(
        name="Des Moines, IA", region=WeatherRegion.US_CORN_BELT,
        lat=41.59, lon=-93.62, commodities=['corn', 'soybeans'], country="US"
    ),
    'champaign_il': WeatherLocation(
        name="Champaign, IL", region=WeatherRegion.US_CORN_BELT,
        lat=40.12, lon=-88.24, commodities=['corn', 'soybeans'], country="US"
    ),
    'lincoln_ne': WeatherLocation(
        name="Lincoln, NE", region=WeatherRegion.US_CORN_BELT,
        lat=40.81, lon=-96.70, commodities=['corn', 'soybeans'], country="US"
    ),
    'indianapolis_in': WeatherLocation(
        name="Indianapolis, IN", region=WeatherRegion.US_CORN_BELT,
        lat=39.77, lon=-86.16, commodities=['corn', 'soybeans'], country="US"
    ),
    'minneapolis_mn': WeatherLocation(
        name="Minneapolis, MN", region=WeatherRegion.US_CORN_BELT,
        lat=44.98, lon=-93.27, commodities=['corn', 'soybeans', 'wheat'], country="US"
    ),

    # US Wheat Belt
    'dodge_city_ks': WeatherLocation(
        name="Dodge City, KS", region=WeatherRegion.US_WHEAT_BELT,
        lat=37.75, lon=-100.02, commodities=['wheat_hrw'], country="US"
    ),
    'amarillo_tx': WeatherLocation(
        name="Amarillo, TX", region=WeatherRegion.US_WHEAT_BELT,
        lat=35.22, lon=-101.83, commodities=['wheat_hrw'], country="US"
    ),
    'oklahoma_city_ok': WeatherLocation(
        name="Oklahoma City, OK", region=WeatherRegion.US_WHEAT_BELT,
        lat=35.47, lon=-97.52, commodities=['wheat_hrw'], country="US"
    ),
    'bismarck_nd': WeatherLocation(
        name="Bismarck, ND", region=WeatherRegion.US_WHEAT_BELT,
        lat=46.81, lon=-100.78, commodities=['wheat_hrs', 'soybeans'], country="US"
    ),

    # US Delta (Cotton, Rice)
    'memphis_tn': WeatherLocation(
        name="Memphis, TN", region=WeatherRegion.US_DELTA,
        lat=35.15, lon=-90.05, commodities=['cotton', 'soybeans', 'rice'], country="US"
    ),

    # Brazil - Center West (Mato Grosso)
    'sorriso_mt': WeatherLocation(
        name="Sorriso, MT", region=WeatherRegion.BRAZIL_CENTER_WEST,
        lat=-12.55, lon=-55.71, commodities=['soybeans', 'corn'], country="BR"
    ),
    'rondonopolis_mt': WeatherLocation(
        name="Rondonópolis, MT", region=WeatherRegion.BRAZIL_CENTER_WEST,
        lat=-16.47, lon=-54.64, commodities=['soybeans', 'corn'], country="BR"
    ),
    'cuiaba_mt': WeatherLocation(
        name="Cuiabá, MT", region=WeatherRegion.BRAZIL_CENTER_WEST,
        lat=-15.60, lon=-56.10, commodities=['soybeans', 'corn'], country="BR"
    ),

    # Brazil - South (Paraná, Rio Grande do Sul)
    'londrina_pr': WeatherLocation(
        name="Londrina, PR", region=WeatherRegion.BRAZIL_SOUTH,
        lat=-23.31, lon=-51.16, commodities=['soybeans', 'corn', 'wheat'], country="BR"
    ),
    'porto_alegre_rs': WeatherLocation(
        name="Porto Alegre, RS", region=WeatherRegion.BRAZIL_SOUTH,
        lat=-30.03, lon=-51.23, commodities=['soybeans', 'rice', 'wheat'], country="BR"
    ),
    'cascavel_pr': WeatherLocation(
        name="Cascavel, PR", region=WeatherRegion.BRAZIL_SOUTH,
        lat=-24.96, lon=-53.46, commodities=['soybeans', 'corn'], country="BR"
    ),

    # Brazil - Northeast (Bahia, MATOPIBA)
    'barreiras_ba': WeatherLocation(
        name="Barreiras, BA", region=WeatherRegion.BRAZIL_NORTHEAST,
        lat=-12.15, lon=-44.99, commodities=['soybeans', 'cotton'], country="BR"
    ),

    # Argentina - Pampas
    'rosario_sf': WeatherLocation(
        name="Rosario, SF", region=WeatherRegion.ARGENTINA_PAMPAS,
        lat=-32.95, lon=-60.65, commodities=['soybeans', 'corn', 'wheat'], country="AR"
    ),
    'cordoba_ar': WeatherLocation(
        name="Córdoba", region=WeatherRegion.ARGENTINA_PAMPAS,
        lat=-31.42, lon=-64.18, commodities=['soybeans', 'corn'], country="AR"
    ),
    'buenos_aires_ar': WeatherLocation(
        name="Buenos Aires", region=WeatherRegion.ARGENTINA_PAMPAS,
        lat=-34.60, lon=-58.38, commodities=['soybeans', 'wheat', 'corn'], country="AR"
    ),

    # Argentina - North
    'tucuman_ar': WeatherLocation(
        name="Tucumán", region=WeatherRegion.ARGENTINA_NORTH,
        lat=-26.82, lon=-65.22, commodities=['soybeans', 'corn', 'sugarcane'], country="AR"
    ),
}


@dataclass
class WeatherChange:
    """Represents a weather change extracted from World Weather"""
    region: str
    change_type: str  # 'precipitation', 'temperature', 'outlook'
    direction: str  # 'increased', 'decreased', 'unchanged'
    week: int  # 1 or 2
    details: str
    assessment: str  # World Weather's assessment
    locations: List[str] = field(default_factory=list)


@dataclass
class WorldWeatherReport:
    """Parsed World Weather report"""
    date: datetime
    model_run: str  # 'morning' or 'evening'
    us_changes: List[WeatherChange]
    south_america_changes: List[WeatherChange]
    us_outlook: str
    brazil_outlook: str
    argentina_outlook: str
    raw_text: str
    has_significant_changes: bool = False


@dataclass
class WeatherBrief:
    """Generated CC Weather Brief"""
    date: datetime
    model_run: str
    summary: str
    us_corn_belt: Dict[str, Any]
    us_wheat_belt: Dict[str, Any]
    brazil: Dict[str, Any]
    argentina: Dict[str, Any]
    key_changes: List[str]
    alerts: List[str]
    source_world_weather: Optional[WorldWeatherReport] = None


class WorldWeatherParser:
    """Parses World Weather email content"""

    # Patterns to identify sections
    SECTION_PATTERNS = {
        'us_week1': r'U\.S\.\s*\(Week 1.*?\)\s*(.*?)(?=U\.S\.\s*\(Week 2|SOUTH AMERICA|$)',
        'us_week2': r'U\.S\.\s*\(Week 2.*?\)\s*(.*?)(?=WORLD WEATHER|SOUTH AMERICA|$)',
        'sa_week1': r'SOUTH AMERICA\s*\(Week 1.*?\)\s*(.*?)(?=SOUTH AMERICA\s*\(Week 2|WORLD WEATHER|$)',
        'sa_week2': r'SOUTH AMERICA\s*\(Week 2.*?\)\s*(.*?)(?=WORLD WEATHER|$)',
        'us_outlook': r'UNITED STATES.*?bottom line.*?unchanged\.(.*?)(?=SOUTH AMERICA|$)',
        'brazil_outlook': r'In Brazil,\s*(.*?)(?=In Argentina|$)',
        'argentina_outlook': r'In Argentina,\s*(.*?)(?=World Weather, Inc\.|$)',
    }

    # Keywords indicating precipitation changes
    PRECIP_KEYWORDS = ['precipitation', 'rainfall', 'rain', 'moisture', 'dry', 'wet']
    TEMP_KEYWORDS = ['temperature', 'warm', 'cold', 'heat', 'cool', 'freeze', 'frost']

    def __init__(self):
        pass

    def parse_email(self, email_body: str, email_date: datetime = None) -> WorldWeatherReport:
        """Parse World Weather email body into structured report"""
        if email_date is None:
            email_date = datetime.now()

        # Determine if morning or evening run
        model_run = 'evening' if 'evening' in email_body.lower()[:500] else 'morning'

        # Extract changes
        us_changes = self._extract_us_changes(email_body)
        sa_changes = self._extract_sa_changes(email_body)

        # Extract outlooks
        us_outlook = self._extract_section(email_body, 'us_outlook')
        brazil_outlook = self._extract_section(email_body, 'brazil_outlook')
        argentina_outlook = self._extract_section(email_body, 'argentina_outlook')

        # Determine if significant changes
        has_significant = any(
            'significant' in c.details.lower() and 'no significant' not in c.details.lower()
            for c in us_changes + sa_changes
        )

        return WorldWeatherReport(
            date=email_date,
            model_run=model_run,
            us_changes=us_changes,
            south_america_changes=sa_changes,
            us_outlook=us_outlook or "",
            brazil_outlook=brazil_outlook or "",
            argentina_outlook=argentina_outlook or "",
            raw_text=email_body,
            has_significant_changes=has_significant
        )

    def _extract_section(self, text: str, section: str) -> Optional[str]:
        """Extract a section from the email"""
        pattern = self.SECTION_PATTERNS.get(section)
        if not pattern:
            return None

        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    def _extract_us_changes(self, text: str) -> List[WeatherChange]:
        """Extract US weather changes"""
        changes = []

        # Week 1
        week1_text = self._extract_section(text, 'us_week1') or ""
        changes.extend(self._parse_changes(week1_text, region="US", week=1))

        # Week 2
        week2_text = self._extract_section(text, 'us_week2') or ""
        changes.extend(self._parse_changes(week2_text, region="US", week=2))

        return changes

    def _extract_sa_changes(self, text: str) -> List[WeatherChange]:
        """Extract South America weather changes"""
        changes = []

        # Week 1
        week1_text = self._extract_section(text, 'sa_week1') or ""
        changes.extend(self._parse_changes(week1_text, region="South America", week=1))

        # Week 2
        week2_text = self._extract_section(text, 'sa_week2') or ""
        changes.extend(self._parse_changes(week2_text, region="South America", week=2))

        return changes

    def _parse_changes(self, text: str, region: str, week: int) -> List[WeatherChange]:
        """Parse individual changes from section text"""
        changes = []

        # Split by bullet points
        bullets = re.split(r'[·•]\s*', text)

        for bullet in bullets:
            bullet = bullet.strip()
            if not bullet or len(bullet) < 10:
                continue

            # Determine change type
            change_type = 'precipitation'
            for keyword in self.TEMP_KEYWORDS:
                if keyword in bullet.lower():
                    change_type = 'temperature'
                    break

            # Determine direction
            direction = 'unchanged'
            if 'increased' in bullet.lower() or 'increase' in bullet.lower():
                direction = 'increased'
            elif 'decreased' in bullet.lower() or 'decrease' in bullet.lower():
                direction = 'decreased'
            elif 'no significant' in bullet.lower():
                direction = 'unchanged'

            # Extract locations mentioned
            locations = self._extract_locations(bullet)

            # Look for assessment (usually in next line)
            assessment = ""
            if 'needed' in bullet.lower():
                assessment_match = re.search(r'(The (?:increase|decrease|reduction).*?needed.*?)(?:\n|$)', bullet)
                if assessment_match:
                    assessment = assessment_match.group(1)

            changes.append(WeatherChange(
                region=region,
                change_type=change_type,
                direction=direction,
                week=week,
                details=bullet,
                assessment=assessment,
                locations=locations
            ))

        return changes

    def _extract_locations(self, text: str) -> List[str]:
        """Extract location names from text"""
        locations = []

        # US state patterns
        us_states = [
            'Iowa', 'Illinois', 'Indiana', 'Nebraska', 'Minnesota', 'Ohio',
            'Kansas', 'Oklahoma', 'Texas', 'North Dakota', 'South Dakota',
            'Missouri', 'California', 'Florida', 'Delta'
        ]

        # Brazil locations
        brazil_locations = [
            'Mato Grosso', 'Parana', 'Paraná', 'Rio Grande do Sul',
            'Bahia', 'Minas Gerais', 'Santa Catarina', 'Goias', 'Goiás',
            'Mato Grosso do Sul', 'MATOPIBA'
        ]

        # Argentina locations
        argentina_locations = [
            'Buenos Aires', 'Cordoba', 'Córdoba', 'Santa Fe',
            'Entre Rios', 'La Pampa', 'northeastern Argentina',
            'southern Argentina', 'northern Argentina'
        ]

        all_locations = us_states + brazil_locations + argentina_locations

        for loc in all_locations:
            if loc.lower() in text.lower():
                locations.append(loc)

        return locations


class OpenWeatherService:
    """Service for fetching OpenWeather data"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('WEATHER_API_KEY')
        self.base_url = "https://api.openweathermap.org/data/2.5"

    def get_current(self, lat: float, lon: float) -> Dict[str, Any]:
        """Get current weather for coordinates"""
        if not self.api_key:
            return {'error': 'No API key configured'}

        try:
            response = requests.get(
                f"{self.base_url}/weather",
                params={
                    'lat': lat,
                    'lon': lon,
                    'appid': self.api_key,
                    'units': 'imperial'
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            return {
                'temp': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'humidity': data['main']['humidity'],
                'description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed'],
                'clouds': data['clouds']['all'],
                'timestamp': datetime.fromtimestamp(data['dt']).isoformat()
            }
        except Exception as e:
            logger.error(f"OpenWeather API error: {e}")
            return {'error': str(e)}

    def get_forecast(self, lat: float, lon: float, days: int = 5) -> Dict[str, Any]:
        """Get forecast for coordinates"""
        if not self.api_key:
            return {'error': 'No API key configured'}

        try:
            response = requests.get(
                f"{self.base_url}/forecast",
                params={
                    'lat': lat,
                    'lon': lon,
                    'appid': self.api_key,
                    'units': 'imperial',
                    'cnt': days * 8  # 3-hour intervals
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            # Aggregate by day
            daily = {}
            for item in data['list']:
                dt = datetime.fromtimestamp(item['dt'])
                day = dt.strftime('%Y-%m-%d')
                if day not in daily:
                    daily[day] = {'temps': [], 'precip_prob': [], 'descriptions': []}
                daily[day]['temps'].append(item['main']['temp'])
                daily[day]['precip_prob'].append(item.get('pop', 0) * 100)
                daily[day]['descriptions'].append(item['weather'][0]['main'])

            forecast = []
            for day, info in daily.items():
                forecast.append({
                    'date': day,
                    'high': round(max(info['temps']), 1),
                    'low': round(min(info['temps']), 1),
                    'precip_chance': round(max(info['precip_prob']), 0),
                    'conditions': max(set(info['descriptions']), key=info['descriptions'].count)
                })

            return {'forecast': forecast[:days]}
        except Exception as e:
            logger.error(f"OpenWeather forecast error: {e}")
            return {'error': str(e)}

    def get_region_weather(self, region: WeatherRegion) -> Dict[str, Any]:
        """Get weather for all locations in a region"""
        results = {}
        for loc_id, location in AG_WEATHER_LOCATIONS.items():
            if location.region == region:
                current = self.get_current(location.lat, location.lon)
                results[loc_id] = {
                    'name': location.name,
                    'current': current,
                    'commodities': location.commodities
                }
        return results


class WeatherBriefGenerator:
    """Generates CC Weather Briefs from World Weather + supplemental data"""

    def __init__(self):
        self.parser = WorldWeatherParser()
        self.weather_service = OpenWeatherService()

    def generate_brief(
        self,
        world_weather_email: str,
        email_date: datetime = None,
        fetch_supplemental: bool = True
    ) -> WeatherBrief:
        """Generate a complete weather brief"""

        # Parse World Weather email
        ww_report = self.parser.parse_email(world_weather_email, email_date)

        # Fetch supplemental weather data
        us_corn_belt_weather = {}
        us_wheat_belt_weather = {}
        brazil_weather = {}
        argentina_weather = {}

        if fetch_supplemental:
            # Get current conditions for key regions
            us_corn_belt_weather = self._get_region_summary(WeatherRegion.US_CORN_BELT)
            us_wheat_belt_weather = self._get_region_summary(WeatherRegion.US_WHEAT_BELT)
            brazil_weather = self._get_brazil_summary()
            argentina_weather = self._get_argentina_summary()

        # Generate summary
        summary = self._generate_summary(ww_report)

        # Extract key changes
        key_changes = self._extract_key_changes(ww_report)

        # Generate alerts
        alerts = self._generate_alerts(ww_report)

        return WeatherBrief(
            date=ww_report.date,
            model_run=ww_report.model_run,
            summary=summary,
            us_corn_belt=us_corn_belt_weather,
            us_wheat_belt=us_wheat_belt_weather,
            brazil=brazil_weather,
            argentina=argentina_weather,
            key_changes=key_changes,
            alerts=alerts,
            source_world_weather=ww_report
        )

    def _get_region_summary(self, region: WeatherRegion) -> Dict[str, Any]:
        """Get weather summary for a region"""
        weather_data = self.weather_service.get_region_weather(region)

        if not weather_data:
            return {'status': 'no data'}

        # Calculate averages
        temps = []
        humidities = []
        conditions = []

        for loc_id, data in weather_data.items():
            current = data.get('current', {})
            if 'temp' in current:
                temps.append(current['temp'])
                humidities.append(current.get('humidity', 50))
                conditions.append(current.get('description', ''))

        if temps:
            return {
                'avg_temp': round(sum(temps) / len(temps), 1),
                'temp_range': f"{round(min(temps), 0)}°F - {round(max(temps), 0)}°F",
                'avg_humidity': round(sum(humidities) / len(humidities), 0),
                'conditions': max(set(conditions), key=conditions.count) if conditions else 'unknown',
                'locations': weather_data,
                'status': 'ok'
            }

        return {'status': 'no data'}

    def _get_brazil_summary(self) -> Dict[str, Any]:
        """Get summary for Brazil regions"""
        center_west = self._get_region_summary(WeatherRegion.BRAZIL_CENTER_WEST)
        south = self._get_region_summary(WeatherRegion.BRAZIL_SOUTH)
        northeast = self._get_region_summary(WeatherRegion.BRAZIL_NORTHEAST)

        return {
            'center_west': center_west,
            'south': south,
            'northeast': northeast,
            'status': 'ok'
        }

    def _get_argentina_summary(self) -> Dict[str, Any]:
        """Get summary for Argentina regions"""
        pampas = self._get_region_summary(WeatherRegion.ARGENTINA_PAMPAS)
        north = self._get_region_summary(WeatherRegion.ARGENTINA_NORTH)

        return {
            'pampas': pampas,
            'north': north,
            'status': 'ok'
        }

    def _generate_summary(self, ww_report: WorldWeatherReport) -> str:
        """Generate human-readable summary"""
        lines = [
            f"CC Weather Brief - {ww_report.date.strftime('%B %d, %Y')} ({ww_report.model_run.title()} Run)",
            "=" * 60,
            ""
        ]

        # US Summary
        us_changes_summary = "No significant changes" if not any(
            c.direction != 'unchanged' for c in ww_report.us_changes
        ) else f"{len([c for c in ww_report.us_changes if c.direction != 'unchanged'])} changes noted"

        lines.append(f"UNITED STATES: {us_changes_summary}")
        if ww_report.us_outlook:
            lines.append(f"  Outlook: {ww_report.us_outlook[:200]}...")
        lines.append("")

        # Brazil Summary
        brazil_changes = [c for c in ww_report.south_america_changes if 'brazil' in c.details.lower()]
        brazil_summary = "No significant changes" if not any(
            c.direction != 'unchanged' for c in brazil_changes
        ) else f"{len([c for c in brazil_changes if c.direction != 'unchanged'])} changes noted"

        lines.append(f"BRAZIL: {brazil_summary}")
        if ww_report.brazil_outlook:
            lines.append(f"  Outlook: {ww_report.brazil_outlook[:200]}...")
        lines.append("")

        # Argentina Summary
        argentina_changes = [c for c in ww_report.south_america_changes if 'argentina' in c.details.lower()]
        argentina_summary = "No significant changes" if not any(
            c.direction != 'unchanged' for c in argentina_changes
        ) else f"{len([c for c in argentina_changes if c.direction != 'unchanged'])} changes noted"

        lines.append(f"ARGENTINA: {argentina_summary}")
        if ww_report.argentina_outlook:
            lines.append(f"  Outlook: {ww_report.argentina_outlook[:200]}...")

        return "\n".join(lines)

    def _extract_key_changes(self, ww_report: WorldWeatherReport) -> List[str]:
        """Extract list of key changes"""
        changes = []

        for change in ww_report.us_changes + ww_report.south_america_changes:
            if change.direction != 'unchanged':
                location_str = ", ".join(change.locations) if change.locations else change.region
                changes.append(f"Week {change.week}: {change.change_type.title()} {change.direction} in {location_str}")

        return changes

    def _generate_alerts(self, ww_report: WorldWeatherReport) -> List[str]:
        """Generate weather alerts based on content"""
        alerts = []

        # Check for flooding mentions
        if 'flood' in ww_report.raw_text.lower():
            alerts.append("FLOOD RISK: Potential flooding mentioned in outlook")

        # Check for drought/dry mentions in key areas
        if 'drier-bias' in ww_report.raw_text.lower() or 'net drying' in ww_report.raw_text.lower():
            alerts.append("DRY PATTERN: Drier conditions noted in some areas")

        # Check for significant changes
        if ww_report.has_significant_changes:
            alerts.append("SIGNIFICANT CHANGES: Model run shows notable forecast changes")

        return alerts

    def format_brief_text(self, brief: WeatherBrief) -> str:
        """Format brief as plain text for display/email"""
        lines = [brief.summary, ""]

        # Current conditions
        lines.append("CURRENT CONDITIONS:")
        lines.append("-" * 40)

        if brief.us_corn_belt.get('status') == 'ok':
            lines.append(f"US Corn Belt: {brief.us_corn_belt.get('avg_temp', 'N/A')}°F, "
                        f"{brief.us_corn_belt.get('conditions', 'N/A')}")

        if brief.us_wheat_belt.get('status') == 'ok':
            lines.append(f"US Wheat Belt: {brief.us_wheat_belt.get('avg_temp', 'N/A')}°F, "
                        f"{brief.us_wheat_belt.get('conditions', 'N/A')}")

        if brief.brazil.get('status') == 'ok':
            cw = brief.brazil.get('center_west', {})
            if cw.get('status') == 'ok':
                lines.append(f"Brazil (Mato Grosso): {cw.get('avg_temp', 'N/A')}°F, "
                            f"{cw.get('conditions', 'N/A')}")

        if brief.argentina.get('status') == 'ok':
            pampas = brief.argentina.get('pampas', {})
            if pampas.get('status') == 'ok':
                lines.append(f"Argentina (Pampas): {pampas.get('avg_temp', 'N/A')}°F, "
                            f"{pampas.get('conditions', 'N/A')}")

        lines.append("")

        # Key changes
        if brief.key_changes:
            lines.append("KEY CHANGES:")
            lines.append("-" * 40)
            for change in brief.key_changes:
                lines.append(f"  • {change}")
            lines.append("")

        # Alerts
        if brief.alerts:
            lines.append("⚠️ ALERTS:")
            lines.append("-" * 40)
            for alert in brief.alerts:
                lines.append(f"  • {alert}")

        lines.append("")
        lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("Source: World Weather Inc. + OpenWeather")

        return "\n".join(lines)


class WorldWeatherEmailMonitor:
    """Monitors Gmail for World Weather emails"""

    WORLD_WEATHER_SENDER = "worldweather@bizkc.rr.com"

    def __init__(self, email_agent=None):
        """
        Initialize monitor.

        Args:
            email_agent: Optional EmailAgent instance for Gmail access
        """
        self.email_agent = email_agent
        self.brief_generator = WeatherBriefGenerator()

    def check_for_new_reports(self) -> List[WeatherBrief]:
        """Check Gmail for new World Weather emails and generate briefs"""
        if not self.email_agent:
            logger.warning("No email agent configured - cannot check Gmail")
            return []

        # Search for World Weather emails
        try:
            emails = self.email_agent.search_emails(
                query=f"from:{self.WORLD_WEATHER_SENDER} is:unread"
            )

            briefs = []
            for email in emails:
                try:
                    # Parse email and generate brief
                    brief = self.brief_generator.generate_brief(
                        world_weather_email=email.body,
                        email_date=email.date
                    )
                    briefs.append(brief)

                    # Mark as read
                    # self.email_agent.mark_as_read(email.id)

                except Exception as e:
                    logger.error(f"Error processing World Weather email: {e}")

            return briefs

        except Exception as e:
            logger.error(f"Error checking for World Weather emails: {e}")
            return []

    def process_email_text(self, email_text: str) -> WeatherBrief:
        """Process raw email text (for testing without Gmail)"""
        return self.brief_generator.generate_brief(email_text)


# CLI for testing
def main():
    """Test the World Weather integration"""
    print("=" * 60)
    print("World Weather Integration Test")
    print("=" * 60)

    # Test with sample email (from user's example)
    sample_email = """The Evening Weather Update For December 23, 2025
COMMENTS AND SUMMARY
UNITED STATES
            The evening GFS model run in the United States showed no significant precipitation changes in week 1. In week 2, precipitation was increased across the Delta, northern Florida, and eastern Oklahoma Jan. 3-4. Amounts were also increased in the southeastern states, eastern Florida, and in central California Jan. 5-7. Precipitation was decreased in week 2 across northern Minnesota Jan. 5.
U.S. (Week 1-Discussion of significant model changes)
·         This evening's GFS model run showed no significant precipitation changes in week 1
U.S. (Week 2-Discussion of significant model changes)
·         GFS model run increased precipitation across the Delta, northern Florida, and eastern Oklahoma Jan. 3-4
A majority of the increase was likely necessary
·         GFS model run decreased precipitation across northern Minnesota Jan. 5
The decrease was needed
·          GFS model run increased precipitation in the southeastern states, eastern Florida, and in central California Jan. 5-7
Some of the increase was needed in these areas
World Weather Inc. will not be making any modifications to this morning's official forecast based on the advertised changes on this evening model run
WORLD WEATHER, INC.'S EXPECTATIONS AND IMPACT ON CROPS
The bottom line for the official weather outlook in most production regions is unchanged.
SOUTH AMERICA
            The evening GFS model run in South America showed no significant rainfall changes in week 1. In week 2, rainfall was increased in central and eastern Mato Grosso do Sul of Brazil Jan. 2-4. Rainfall was decreased in week 2 in northeastern and interior southern Argentina and along and near the border of Minas Gerais and Bahia of Brazil Jan. 5-6.
SOUTH AMERICA (Week 1-Discussion of significant model changes)
·         This evening's GFS model run showed no significant rainfall changes in week 1
SOUTH AMERICA (Week 2-Discussion of significant model changes)
·         GFS model run increased rainfall in central and eastern Mato Grosso do Sul of Brazil Jan. 2-4
The increase was necessary
·         GFS model run decreased rainfall in northeastern and interior southern Argentina and along and near the border of Minas Gerais and Bahia of Brazil Jan. 5-6
A majority of the reduction was likely needed in these areas except possibly in northeastern Argentina
World Weather Inc. will not be making any modifications to this morning's official forecast based on the advertised changes on this evening model run.
WORLD WEATHER, INC.'S EXPECTATIONS AND IMPACT ON CROPS
The bottom line for the official weather outlook in most production regions is unchanged.
            In Brazil, conditions will still be mostly favorable. Net drying is expected in the first seven days of the outlook in Bahia and central and northeastern Minas Gerais; however, this shouldn't be much of an issue due to greater rainfall expected in week 2 of the outlook, especially in Minas Gerais. Some localized flooding is a possibility in Rio Grande do Sul, Santa Catarina, and southern Parana due to rain events Thursday and Saturday through Sunday.
            In Argentina, more rounds of rain will still impact the northern half of the nation in the next seven days which will likely lead to some more areas of flooding, especially late Wednesday into Thursday and then Saturday through Sunday. This evening's GFS model run continued to show little to no change with these two upcoming rain events. Southern Argentina will continue to be drier-biased with greater opportunity for fieldwork in-comparison to the north. The drier-bias in southern production areas of Argentina could last through week 2 of the outlook as well; though, completely dry conditions are unlikely.
"""

    # Generate brief
    generator = WeatherBriefGenerator()
    brief = generator.generate_brief(sample_email, datetime.now())

    # Print formatted brief
    print("\n" + generator.format_brief_text(brief))

    # Print raw data for verification
    print("\n" + "=" * 60)
    print("RAW DATA:")
    print("=" * 60)
    print(f"\nKey Changes ({len(brief.key_changes)}):")
    for change in brief.key_changes:
        print(f"  - {change}")

    print(f"\nAlerts ({len(brief.alerts)}):")
    for alert in brief.alerts:
        print(f"  - {alert}")


if __name__ == "__main__":
    main()
