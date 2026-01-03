"""
US Census Bureau API Service
Fetches international trade and economic data
Round Lakes Commodities
"""

import logging
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass

logger = logging.getLogger('rlc_master_agent.services.census')


@dataclass
class TradeData:
    """Represents trade statistics"""
    commodity: str
    partner_country: str
    trade_type: str  # 'export' or 'import'
    value: float
    quantity: Optional[float]
    unit: str
    period: str


class CensusService:
    """
    Service for interacting with US Census Bureau APIs

    Supports:
    - International Trade Data (exports/imports)
    - Economic indicators

    API Documentation: https://www.census.gov/data/developers/data-sets.html
    """

    # Common HS codes for agricultural commodities
    HS_CODES = {
        'corn': '1005',
        'wheat': '1001',
        'soybeans': '1201',
        'soybean_oil': '1507',
        'soybean_meal': '2304',
        'beef': '0201',
        'pork': '0203',
        'cattle_live': '0102',
        'hogs_live': '0103',
        'ethanol': '2207',
    }

    # Major trading partners
    COUNTRY_CODES = {
        'china': '5700',
        'mexico': '2010',
        'canada': '1220',
        'japan': '5880',
        'south_korea': '5800',
        'taiwan': '5830',
        'eu': '4000',
        'brazil': '3510',
        'argentina': '3570',
    }

    def __init__(self, api_key: str, base_url: str = "https://api.census.gov/data"):
        """
        Initialize Census API Service

        Args:
            api_key: Census Bureau API key
            base_url: API base URL
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> List[List[str]]:
        """
        Make API request with error handling

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            API response as list of lists (CSV-like format)
        """
        url = f"{self.base_url}/{endpoint}"
        params = params or {}

        # Add API key
        if self.api_key:
            params['key'] = self.api_key

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"Census API timeout for {endpoint}")
            raise Exception("Census API request timed out")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Census API HTTP error: {e}")
            if response.status_code == 204:
                return []
            raise Exception(f"Census API error: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Census API request failed: {e}")
            raise Exception(f"Census API request failed: {str(e)}")

    def _parse_trade_response(self, data: List[List[str]]) -> List[Dict[str, Any]]:
        """
        Parse Census API response into structured data

        Args:
            data: Raw API response (list of lists with headers first)

        Returns:
            List of dictionaries with parsed data
        """
        if not data or len(data) < 2:
            return []

        headers = [h.lower() for h in data[0]]
        results = []

        for row in data[1:]:
            record = dict(zip(headers, row))
            results.append(record)

        return results

    def get_exports(
        self,
        commodity: str,
        partner_country: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get export data for a commodity

        Args:
            commodity: Commodity name or HS code
            partner_country: Optional partner country name or code
            year: Year (defaults to current year)
            month: Month (1-12, optional)

        Returns:
            Export statistics
        """
        # Resolve commodity to HS code
        hs_code = self.HS_CODES.get(commodity.lower(), commodity)

        # Resolve country code
        country_code = None
        if partner_country:
            country_code = self.COUNTRY_CODES.get(partner_country.lower(), partner_country)

        # Default to current/recent year
        if not year:
            year = datetime.now().year

        # Build query
        time_period = f"{year}"
        if month:
            time_period = f"{year}{month:02d}"
            endpoint = f"timeseries/intltrade/exports/hs"
        else:
            endpoint = f"timeseries/intltrade/exports/hs"

        params = {
            'get': 'GEN_VAL_MO,ALL_VAL_MO,CTY_CODE,CTY_NAME,I_COMMODITY,time',
            'I_COMMODITY': hs_code,
            'time': time_period
        }

        if country_code:
            params['CTY_CODE'] = country_code

        try:
            data = self._make_request(endpoint, params)
            parsed = self._parse_trade_response(data)

            return {
                'commodity': commodity,
                'hs_code': hs_code,
                'trade_type': 'export',
                'partner': partner_country or 'all',
                'period': time_period,
                'data': parsed,
                'total_value': sum(float(r.get('gen_val_mo', 0) or 0) for r in parsed)
            }
        except Exception as e:
            logger.error(f"Failed to get export data: {e}")
            return {
                'commodity': commodity,
                'status': 'error',
                'message': str(e)
            }

    def get_imports(
        self,
        commodity: str,
        source_country: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get import data for a commodity

        Args:
            commodity: Commodity name or HS code
            source_country: Optional source country
            year: Year
            month: Month (optional)

        Returns:
            Import statistics
        """
        # Resolve commodity to HS code
        hs_code = self.HS_CODES.get(commodity.lower(), commodity)

        # Resolve country code
        country_code = None
        if source_country:
            country_code = self.COUNTRY_CODES.get(source_country.lower(), source_country)

        if not year:
            year = datetime.now().year

        time_period = f"{year}"
        if month:
            time_period = f"{year}{month:02d}"

        endpoint = f"timeseries/intltrade/imports/hs"

        params = {
            'get': 'GEN_VAL_MO,CON_VAL_MO,CTY_CODE,CTY_NAME,I_COMMODITY,time',
            'I_COMMODITY': hs_code,
            'time': time_period
        }

        if country_code:
            params['CTY_CODE'] = country_code

        try:
            data = self._make_request(endpoint, params)
            parsed = self._parse_trade_response(data)

            return {
                'commodity': commodity,
                'hs_code': hs_code,
                'trade_type': 'import',
                'source': source_country or 'all',
                'period': time_period,
                'data': parsed,
                'total_value': sum(float(r.get('gen_val_mo', 0) or 0) for r in parsed)
            }
        except Exception as e:
            logger.error(f"Failed to get import data: {e}")
            return {
                'commodity': commodity,
                'status': 'error',
                'message': str(e)
            }

    def get_trade_balance(
        self,
        commodity: str,
        partner_country: Optional[str] = None,
        year: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Calculate trade balance for a commodity

        Args:
            commodity: Commodity name
            partner_country: Optional partner country
            year: Year

        Returns:
            Trade balance (exports - imports)
        """
        exports = self.get_exports(commodity, partner_country, year)
        imports = self.get_imports(commodity, partner_country, year)

        export_value = exports.get('total_value', 0)
        import_value = imports.get('total_value', 0)

        return {
            'commodity': commodity,
            'partner': partner_country or 'all',
            'year': year or datetime.now().year,
            'exports': export_value,
            'imports': import_value,
            'balance': export_value - import_value,
            'surplus': export_value > import_value
        }

    def get_top_destinations(
        self,
        commodity: str,
        year: Optional[int] = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get top export destinations for a commodity

        Args:
            commodity: Commodity name
            year: Year
            limit: Number of top destinations to return

        Returns:
            Top destination countries by export value
        """
        exports = self.get_exports(commodity, year=year)

        if 'error' in exports.get('status', ''):
            return exports

        # Aggregate by country
        by_country = {}
        for record in exports.get('data', []):
            country = record.get('cty_name', 'Unknown')
            value = float(record.get('gen_val_mo', 0) or 0)
            by_country[country] = by_country.get(country, 0) + value

        # Sort by value
        sorted_countries = sorted(by_country.items(), key=lambda x: x[1], reverse=True)

        return {
            'commodity': commodity,
            'year': year or datetime.now().year,
            'top_destinations': [
                {'country': c, 'value': v} for c, v in sorted_countries[:limit]
            ],
            'total_destinations': len(sorted_countries)
        }

    def get_top_sources(
        self,
        commodity: str,
        year: Optional[int] = None,
        limit: int = 10
    ) -> Dict[str, Any]:
        """
        Get top import sources for a commodity

        Args:
            commodity: Commodity name
            year: Year
            limit: Number of top sources to return

        Returns:
            Top source countries by import value
        """
        imports = self.get_imports(commodity, year=year)

        if 'error' in imports.get('status', ''):
            return imports

        # Aggregate by country
        by_country = {}
        for record in imports.get('data', []):
            country = record.get('cty_name', 'Unknown')
            value = float(record.get('gen_val_mo', 0) or 0)
            by_country[country] = by_country.get(country, 0) + value

        # Sort by value
        sorted_countries = sorted(by_country.items(), key=lambda x: x[1], reverse=True)

        return {
            'commodity': commodity,
            'year': year or datetime.now().year,
            'top_sources': [
                {'country': c, 'value': v} for c, v in sorted_countries[:limit]
            ],
            'total_sources': len(sorted_countries)
        }

    def list_available_commodities(self) -> List[Dict[str, str]]:
        """
        List commodities with known HS codes

        Returns:
            List of available commodities
        """
        return [
            {'name': name, 'hs_code': code}
            for name, code in self.HS_CODES.items()
        ]

    def list_countries(self) -> List[Dict[str, str]]:
        """
        List major trading partner countries

        Returns:
            List of country codes
        """
        return [
            {'name': name, 'code': code}
            for name, code in self.COUNTRY_CODES.items()
        ]

    def health_check(self) -> Dict[str, Any]:
        """
        Check API connectivity

        Returns:
            Health status
        """
        try:
            # Try a simple query
            endpoint = "timeseries/intltrade/exports/hs"
            params = {
                'get': 'GEN_VAL_MO',
                'I_COMMODITY': '1005',  # Corn
                'time': '2023'
            }
            if self.api_key:
                params['key'] = self.api_key

            response = self.session.get(
                f"{self.base_url}/{endpoint}",
                params=params,
                timeout=10
            )

            return {
                'status': 'healthy' if response.ok else 'degraded',
                'api_key_configured': bool(self.api_key),
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
