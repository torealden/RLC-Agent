"""
Data Retrieval & Analysis Agent for RLC Master Agent
Fetches and analyzes business and market data
Round Lakes Commodities
"""

import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from services.usda_api import USDAService
from services.census_api import CensusService
from services.weather_api import WeatherService

logger = logging.getLogger('rlc_master_agent.data_agent')


class DataSource(Enum):
    """Available data sources"""
    USDA = "usda"
    CENSUS = "census"
    WEATHER = "weather"
    INTERNAL = "internal"
    COMBINED = "combined"


@dataclass
class DataQuery:
    """Represents a data query"""
    source: DataSource
    query_type: str
    parameters: Dict[str, Any]
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class DataResult:
    """Result of a data query"""
    source: DataSource
    query_type: str
    data: Dict[str, Any]
    success: bool
    error: Optional[str] = None
    cached: bool = False
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class DataAgent:
    """
    Agent responsible for fetching and analyzing data from various sources.

    Integrates:
    - USDA AMS for commodity prices and market reports
    - Census Bureau for trade statistics
    - Weather services for ranch and crop conditions
    - Internal databases for historical analysis

    Features:
    - Unified interface for multiple data sources
    - Caching for frequently accessed data
    - Data transformation and analysis
    - Error handling and fallbacks
    """

    def __init__(self, settings: Optional[Any] = None):
        """
        Initialize Data Agent with configured services

        Args:
            settings: Application settings with API keys
        """
        self.settings = settings
        self._cache: Dict[str, DataResult] = {}
        self._cache_ttl = 300  # 5 minutes default

        # Initialize services
        self._init_services()

        logger.info("Data Agent initialized")

    def _init_services(self):
        """Initialize data service connections"""
        api_config = getattr(self.settings, 'api', None) if self.settings else None

        # USDA Service
        usda_key = api_config.usda_api_key if api_config else ''
        usda_url = api_config.usda_base_url if api_config else 'https://marsapi.ams.usda.gov/services/v1.2'
        self.usda_service = USDAService(api_key=usda_key, base_url=usda_url)

        # Census Service
        census_key = api_config.census_api_key if api_config else ''
        census_url = api_config.census_base_url if api_config else 'https://api.census.gov/data'
        self.census_service = CensusService(api_key=census_key, base_url=census_url)

        # Weather Service
        weather_key = api_config.weather_api_key if api_config else ''
        weather_url = api_config.weather_base_url if api_config else 'https://api.openweathermap.org/data/2.5'
        self.weather_service = WeatherService(api_key=weather_key, base_url=weather_url)

        logger.debug("Data services initialized")

    # -------------------------------------------------------------------------
    # Unified Query Interface
    # -------------------------------------------------------------------------

    def query(
        self,
        source: Union[DataSource, str],
        query_type: str,
        **params
    ) -> DataResult:
        """
        Execute a data query

        Args:
            source: Data source to query
            query_type: Type of query (e.g., 'price', 'exports', 'weather')
            **params: Query parameters

        Returns:
            DataResult with query results
        """
        if isinstance(source, str):
            source = DataSource(source.lower())

        # Check cache
        cache_key = self._get_cache_key(source, query_type, params)
        cached = self._get_from_cache(cache_key)
        if cached:
            logger.debug(f"Cache hit for {source.value}:{query_type}")
            return cached

        # Route to appropriate handler
        try:
            if source == DataSource.USDA:
                result = self._query_usda(query_type, params)
            elif source == DataSource.CENSUS:
                result = self._query_census(query_type, params)
            elif source == DataSource.WEATHER:
                result = self._query_weather(query_type, params)
            elif source == DataSource.INTERNAL:
                result = self._query_internal(query_type, params)
            elif source == DataSource.COMBINED:
                result = self._query_combined(query_type, params)
            else:
                raise ValueError(f"Unknown data source: {source}")

            # Cache successful results
            if result.success:
                self._cache[cache_key] = result

            return result

        except Exception as e:
            logger.error(f"Query failed: {source.value}:{query_type} - {e}")
            return DataResult(
                source=source,
                query_type=query_type,
                data={},
                success=False,
                error=str(e)
            )

    # -------------------------------------------------------------------------
    # USDA Queries
    # -------------------------------------------------------------------------

    def _query_usda(self, query_type: str, params: Dict[str, Any]) -> DataResult:
        """Handle USDA data queries"""
        try:
            if query_type == 'price':
                commodity = params.get('commodity', 'corn')
                date = params.get('date')
                data = self.usda_service.get_commodity_price(commodity, date)

            elif query_type == 'report':
                slug_id = params.get('slug_id') or params.get('report_id')
                if not slug_id:
                    raise ValueError("slug_id or report_id required for report query")
                report = self.usda_service.get_report(
                    slug_id,
                    start_date=params.get('start_date'),
                    end_date=params.get('end_date')
                )
                data = {
                    'slug_id': report.slug_id,
                    'title': report.report_title,
                    'date': report.report_date,
                    'records': report.data
                }

            elif query_type == 'exports' or query_type == 'export_inspections':
                data = self.usda_service.get_export_inspections(
                    commodity=params.get('commodity', 'corn'),
                    start_date=params.get('start_date'),
                    end_date=params.get('end_date')
                )

            elif query_type == 'wasde':
                data = self.usda_service.get_wasde_data(
                    commodity=params.get('commodity')
                )

            elif query_type == 'list_reports':
                reports = self.usda_service.list_available_reports()
                data = {'reports': reports}

            elif query_type == 'search':
                results = self.usda_service.search_reports(params.get('query', ''))
                data = {'results': results}

            else:
                raise ValueError(f"Unknown USDA query type: {query_type}")

            return DataResult(
                source=DataSource.USDA,
                query_type=query_type,
                data=data,
                success='error' not in str(data.get('status', '')).lower()
            )

        except Exception as e:
            return DataResult(
                source=DataSource.USDA,
                query_type=query_type,
                data={},
                success=False,
                error=str(e)
            )

    # -------------------------------------------------------------------------
    # Census Queries
    # -------------------------------------------------------------------------

    def _query_census(self, query_type: str, params: Dict[str, Any]) -> DataResult:
        """Handle Census Bureau data queries"""
        try:
            if query_type == 'exports':
                data = self.census_service.get_exports(
                    commodity=params.get('commodity', 'corn'),
                    partner_country=params.get('country'),
                    year=params.get('year'),
                    month=params.get('month')
                )

            elif query_type == 'imports':
                data = self.census_service.get_imports(
                    commodity=params.get('commodity', 'corn'),
                    source_country=params.get('country'),
                    year=params.get('year'),
                    month=params.get('month')
                )

            elif query_type == 'trade_balance':
                data = self.census_service.get_trade_balance(
                    commodity=params.get('commodity', 'corn'),
                    partner_country=params.get('country'),
                    year=params.get('year')
                )

            elif query_type == 'top_destinations':
                data = self.census_service.get_top_destinations(
                    commodity=params.get('commodity', 'corn'),
                    year=params.get('year'),
                    limit=params.get('limit', 10)
                )

            elif query_type == 'top_sources':
                data = self.census_service.get_top_sources(
                    commodity=params.get('commodity', 'corn'),
                    year=params.get('year'),
                    limit=params.get('limit', 10)
                )

            elif query_type == 'commodities':
                data = {'commodities': self.census_service.list_available_commodities()}

            elif query_type == 'countries':
                data = {'countries': self.census_service.list_countries()}

            else:
                raise ValueError(f"Unknown Census query type: {query_type}")

            return DataResult(
                source=DataSource.CENSUS,
                query_type=query_type,
                data=data,
                success='error' not in str(data.get('status', '')).lower()
            )

        except Exception as e:
            return DataResult(
                source=DataSource.CENSUS,
                query_type=query_type,
                data={},
                success=False,
                error=str(e)
            )

    # -------------------------------------------------------------------------
    # Weather Queries
    # -------------------------------------------------------------------------

    def _query_weather(self, query_type: str, params: Dict[str, Any]) -> DataResult:
        """Handle weather data queries"""
        try:
            location = params.get('location', 'headquarters')

            if query_type == 'current':
                data = self.weather_service.get_current_weather(location)

            elif query_type == 'forecast':
                data = self.weather_service.get_forecast(
                    location=location,
                    days=params.get('days', 5)
                )

            elif query_type == 'agricultural':
                data = self.weather_service.get_agricultural_conditions(location)

            elif query_type == 'multiple':
                locations = params.get('locations', ['headquarters', 'ranch'])
                data = self.weather_service.get_multiple_locations(locations)

            elif query_type == 'locations':
                data = {'locations': self.weather_service.list_predefined_locations()}

            else:
                raise ValueError(f"Unknown weather query type: {query_type}")

            return DataResult(
                source=DataSource.WEATHER,
                query_type=query_type,
                data=data,
                success='error' not in str(data.get('status', '')).lower()
            )

        except Exception as e:
            return DataResult(
                source=DataSource.WEATHER,
                query_type=query_type,
                data={},
                success=False,
                error=str(e)
            )

    # -------------------------------------------------------------------------
    # Internal Database Queries
    # -------------------------------------------------------------------------

    def _query_internal(self, query_type: str, params: Dict[str, Any]) -> DataResult:
        """Handle internal database queries"""
        # This would connect to the commodity_pipeline database
        # For now, return a placeholder

        logger.info(f"Internal query: {query_type} with {params}")

        return DataResult(
            source=DataSource.INTERNAL,
            query_type=query_type,
            data={
                'note': 'Internal database query',
                'query_type': query_type,
                'params': params,
                'suggestion': 'Connect to commodity_pipeline database for historical data'
            },
            success=True
        )

    # -------------------------------------------------------------------------
    # Combined/Multi-Source Queries
    # -------------------------------------------------------------------------

    def _query_combined(self, query_type: str, params: Dict[str, Any]) -> DataResult:
        """Handle queries that combine multiple data sources"""
        try:
            if query_type == 'market_overview':
                return self._get_market_overview(params)

            elif query_type == 'commodity_report':
                return self._get_commodity_report(params)

            elif query_type == 'daily_briefing':
                return self._get_daily_briefing(params)

            else:
                raise ValueError(f"Unknown combined query type: {query_type}")

        except Exception as e:
            return DataResult(
                source=DataSource.COMBINED,
                query_type=query_type,
                data={},
                success=False,
                error=str(e)
            )

    def _get_market_overview(self, params: Dict[str, Any]) -> DataResult:
        """Get a comprehensive market overview"""
        commodities = params.get('commodities', ['corn', 'soybeans', 'wheat', 'cattle'])

        overview = {
            'timestamp': datetime.now().isoformat(),
            'commodities': {}
        }

        for commodity in commodities:
            try:
                price_data = self.usda_service.get_commodity_price(commodity)
                overview['commodities'][commodity] = {
                    'price': price_data,
                    'status': 'available' if price_data.get('price_data') else 'no_data'
                }
            except Exception as e:
                overview['commodities'][commodity] = {
                    'error': str(e),
                    'status': 'error'
                }

        return DataResult(
            source=DataSource.COMBINED,
            query_type='market_overview',
            data=overview,
            success=True
        )

    def _get_commodity_report(self, params: Dict[str, Any]) -> DataResult:
        """Get comprehensive report for a single commodity"""
        commodity = params.get('commodity', 'corn')

        report = {
            'commodity': commodity,
            'timestamp': datetime.now().isoformat(),
            'sections': {}
        }

        # Get price data
        try:
            report['sections']['price'] = self.usda_service.get_commodity_price(commodity)
        except Exception as e:
            report['sections']['price'] = {'error': str(e)}

        # Get export data
        try:
            report['sections']['exports'] = self.census_service.get_top_destinations(commodity)
        except Exception as e:
            report['sections']['exports'] = {'error': str(e)}

        # Get trade balance
        try:
            report['sections']['trade_balance'] = self.census_service.get_trade_balance(commodity)
        except Exception as e:
            report['sections']['trade_balance'] = {'error': str(e)}

        return DataResult(
            source=DataSource.COMBINED,
            query_type='commodity_report',
            data=report,
            success=True
        )

    def _get_daily_briefing(self, params: Dict[str, Any]) -> DataResult:
        """Get daily market and operations briefing"""
        briefing = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'sections': {}
        }

        # Market overview
        try:
            market = self._get_market_overview({'commodities': ['corn', 'soybeans', 'cattle']})
            briefing['sections']['market'] = market.data
        except Exception as e:
            briefing['sections']['market'] = {'error': str(e)}

        # Weather for ranch
        try:
            weather = self.weather_service.get_agricultural_conditions('ranch')
            briefing['sections']['weather'] = weather
        except Exception as e:
            briefing['sections']['weather'] = {'error': str(e)}

        return DataResult(
            source=DataSource.COMBINED,
            query_type='daily_briefing',
            data=briefing,
            success=True
        )

    # -------------------------------------------------------------------------
    # High-Level Query Methods
    # -------------------------------------------------------------------------

    def get_commodity_price(self, commodity: str, date: Optional[str] = None) -> Dict[str, Any]:
        """Convenience method to get commodity price"""
        result = self.query(DataSource.USDA, 'price', commodity=commodity, date=date)
        return result.data

    def get_export_data(
        self,
        commodity: str,
        country: Optional[str] = None,
        year: Optional[int] = None
    ) -> Dict[str, Any]:
        """Convenience method to get export data"""
        result = self.query(
            DataSource.CENSUS,
            'exports',
            commodity=commodity,
            country=country,
            year=year
        )
        return result.data

    def get_weather(self, location: str = 'ranch') -> Dict[str, Any]:
        """Convenience method to get weather"""
        result = self.query(DataSource.WEATHER, 'current', location=location)
        return result.data

    def get_weather_forecast(self, location: str = 'ranch', days: int = 5) -> Dict[str, Any]:
        """Convenience method to get weather forecast"""
        result = self.query(DataSource.WEATHER, 'forecast', location=location, days=days)
        return result.data

    def get_market_overview(
        self,
        commodities: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Convenience method to get market overview"""
        result = self.query(
            DataSource.COMBINED,
            'market_overview',
            commodities=commodities or ['corn', 'soybeans', 'wheat', 'cattle']
        )
        return result.data

    def get_daily_briefing(self) -> Dict[str, Any]:
        """Get daily briefing"""
        result = self.query(DataSource.COMBINED, 'daily_briefing')
        return result.data

    # -------------------------------------------------------------------------
    # Analysis Methods
    # -------------------------------------------------------------------------

    def analyze_price_trend(
        self,
        commodity: str,
        days: int = 30
    ) -> Dict[str, Any]:
        """Analyze price trend for a commodity"""
        # This would analyze historical data
        # For now, return current price with trend analysis placeholder

        current = self.get_commodity_price(commodity)

        return {
            'commodity': commodity,
            'current_price': current.get('price_data', {}),
            'analysis': {
                'period_days': days,
                'trend': 'stable',  # Would calculate from historical data
                'note': 'Connect to historical database for trend analysis'
            }
        }

    def compare_commodities(
        self,
        commodities: List[str]
    ) -> Dict[str, Any]:
        """Compare multiple commodities"""
        comparison = {
            'timestamp': datetime.now().isoformat(),
            'commodities': {}
        }

        for commodity in commodities:
            comparison['commodities'][commodity] = self.get_commodity_price(commodity)

        return comparison

    # -------------------------------------------------------------------------
    # Cache Management
    # -------------------------------------------------------------------------

    def _get_cache_key(
        self,
        source: DataSource,
        query_type: str,
        params: Dict[str, Any]
    ) -> str:
        """Generate cache key for a query"""
        param_str = '_'.join(f"{k}={v}" for k, v in sorted(params.items()))
        return f"{source.value}:{query_type}:{param_str}"

    def _get_from_cache(self, cache_key: str) -> Optional[DataResult]:
        """Get result from cache if valid"""
        if cache_key not in self._cache:
            return None

        cached = self._cache[cache_key]
        age = (datetime.now() - cached.timestamp).total_seconds()

        if age > self._cache_ttl:
            del self._cache[cache_key]
            return None

        cached.cached = True
        return cached

    def clear_cache(self):
        """Clear the data cache"""
        self._cache = {}
        logger.info("Data cache cleared")

    def set_cache_ttl(self, seconds: int):
        """Set cache TTL in seconds"""
        self._cache_ttl = seconds
        logger.info(f"Cache TTL set to {seconds} seconds")

    # -------------------------------------------------------------------------
    # Health & Status
    # -------------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Check health of all data services"""
        return {
            'usda': self.usda_service.health_check(),
            'census': self.census_service.health_check(),
            'weather': self.weather_service.health_check(),
            'cache': {
                'size': len(self._cache),
                'ttl_seconds': self._cache_ttl
            }
        }

    def list_available_sources(self) -> Dict[str, Any]:
        """List available data sources and their capabilities"""
        return {
            'usda': {
                'description': 'USDA Market News API',
                'query_types': ['price', 'report', 'exports', 'wasde', 'list_reports', 'search'],
                'commodities': list(USDAService.REPORT_SLUGS.keys())
            },
            'census': {
                'description': 'US Census Bureau Trade Data',
                'query_types': ['exports', 'imports', 'trade_balance', 'top_destinations', 'top_sources'],
                'commodities': list(CensusService.HS_CODES.keys())
            },
            'weather': {
                'description': 'OpenWeatherMap',
                'query_types': ['current', 'forecast', 'agricultural', 'multiple'],
                'locations': list(WeatherService.RLC_LOCATIONS.keys())
            },
            'combined': {
                'description': 'Multi-source combined queries',
                'query_types': ['market_overview', 'commodity_report', 'daily_briefing']
            }
        }
