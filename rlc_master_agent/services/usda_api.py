"""
USDA Market News API Service
Fetches agricultural commodity prices and market data from USDA AMS
Round Lakes Commodities
"""

import logging
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger('rlc_master_agent.services.usda')


@dataclass
class MarketReport:
    """Represents a USDA market report"""
    slug_id: str
    slug_name: str
    report_title: str
    report_date: str
    data: List[Dict[str, Any]]


class USDAService:
    """
    Service for interacting with USDA AMS Market News API

    API Documentation: https://mymarketnews.ams.usda.gov/mymarketnews-api
    """

    # Common commodity report slugs
    REPORT_SLUGS = {
        'corn': 'LSD_MARS_1815',
        'soybeans': 'LSD_MARS_1822',
        'wheat': 'LSD_MARS_1821',
        'cattle': 'LM_CT105',
        'hogs': 'LM_HG201',
        'ethanol': 'LSD_MARS_2261',
        'ddgs': 'LSD_MARS_1816',
        'soybean_meal': 'LSD_MARS_1823',
        'soybean_oil': 'LSD_MARS_1824',
    }

    def __init__(self, api_key: str, base_url: str = "https://marsapi.ams.usda.gov/services/v1.2"):
        """
        Initialize USDA API Service

        Args:
            api_key: USDA Market News API key
            base_url: API base URL
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}' if api_key else '',
            'Accept': 'application/json'
        })

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

        # Add API key to params if not in header
        if self.api_key and 'api_key' not in params:
            params['api_key'] = self.api_key

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            logger.error(f"USDA API timeout for {endpoint}")
            raise Exception("USDA API request timed out")
        except requests.exceptions.HTTPError as e:
            logger.error(f"USDA API HTTP error: {e}")
            raise Exception(f"USDA API error: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"USDA API request failed: {e}")
            raise Exception(f"USDA API request failed: {str(e)}")

    def get_report(
        self,
        slug_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> MarketReport:
        """
        Get a specific market report by slug ID

        Args:
            slug_id: Report slug ID (e.g., 'LSD_MARS_1815')
            start_date: Start date (MM/DD/YYYY format)
            end_date: End date (MM/DD/YYYY format)

        Returns:
            MarketReport object with data
        """
        params = {}
        if start_date:
            params['report_begin_date'] = start_date
        if end_date:
            params['report_end_date'] = end_date

        endpoint = f"reports/{slug_id}"
        data = self._make_request(endpoint, params)

        # Extract report metadata and results
        results = data.get('results', [])

        return MarketReport(
            slug_id=slug_id,
            slug_name=data.get('slug_name', slug_id),
            report_title=data.get('report_title', ''),
            report_date=data.get('report_date', ''),
            data=results
        )

    def get_commodity_price(
        self,
        commodity: str,
        date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get current price for a commodity

        Args:
            commodity: Commodity name (e.g., 'corn', 'soybeans', 'cattle')
            date: Specific date (MM/DD/YYYY) or None for latest

        Returns:
            Dictionary with price information
        """
        slug_id = self.REPORT_SLUGS.get(commodity.lower())
        if not slug_id:
            available = ', '.join(self.REPORT_SLUGS.keys())
            raise ValueError(f"Unknown commodity '{commodity}'. Available: {available}")

        if date:
            report = self.get_report(slug_id, start_date=date, end_date=date)
        else:
            # Get last 7 days of data to ensure we get something
            end = datetime.now()
            start = end - timedelta(days=7)
            report = self.get_report(
                slug_id,
                start_date=start.strftime('%m/%d/%Y'),
                end_date=end.strftime('%m/%d/%Y')
            )

        if not report.data:
            return {
                'commodity': commodity,
                'status': 'no_data',
                'message': f'No recent data available for {commodity}'
            }

        # Get the most recent record
        latest = report.data[0]

        return {
            'commodity': commodity,
            'report_title': report.report_title,
            'report_date': latest.get('report_date', report.report_date),
            'price_data': {
                'low': latest.get('low_price', latest.get('price_low')),
                'high': latest.get('high_price', latest.get('price_high')),
                'avg': latest.get('avg_price', latest.get('price_avg')),
                'unit': latest.get('unit_of_measure', latest.get('commodity_unit'))
            },
            'location': latest.get('region', latest.get('market_location', '')),
            'raw_data': latest
        }

    def get_wasde_data(self, commodity: Optional[str] = None) -> Dict[str, Any]:
        """
        Get World Agricultural Supply and Demand Estimates (WASDE) data

        Args:
            commodity: Optional specific commodity

        Returns:
            WASDE report data
        """
        # WASDE typically requires specific endpoints or reports
        # This is a placeholder - actual implementation depends on available API endpoints
        logger.info(f"Fetching WASDE data for {commodity or 'all commodities'}")

        # For now, return guidance on getting WASDE data
        return {
            'source': 'USDA WASDE',
            'note': 'WASDE reports are typically available at https://www.usda.gov/oce/commodity/wasde',
            'commodity': commodity,
            'suggestion': 'Use get_commodity_price() for specific market prices'
        }

    def get_export_inspections(
        self,
        commodity: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get export inspection data for grain commodities

        Args:
            commodity: Commodity name
            start_date: Start date
            end_date: End date

        Returns:
            Export inspection data
        """
        # Export inspections endpoint
        params = {
            'commodity': commodity
        }
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date

        try:
            data = self._make_request('exports', params)
            return {
                'commodity': commodity,
                'period': f"{start_date or 'earliest'} to {end_date or 'latest'}",
                'data': data.get('results', [])
            }
        except Exception as e:
            logger.warning(f"Export inspections not available: {e}")
            return {
                'commodity': commodity,
                'status': 'error',
                'message': str(e)
            }

    def list_available_reports(self) -> List[Dict[str, str]]:
        """
        List all available market reports

        Returns:
            List of report metadata
        """
        try:
            data = self._make_request('reports')
            return data.get('results', [])
        except Exception as e:
            logger.error(f"Failed to list reports: {e}")
            # Return our known reports as fallback
            return [
                {'slug_id': slug, 'commodity': name}
                for name, slug in self.REPORT_SLUGS.items()
            ]

    def search_reports(self, query: str) -> List[Dict[str, str]]:
        """
        Search for reports by keyword

        Args:
            query: Search term

        Returns:
            Matching reports
        """
        try:
            data = self._make_request('reports', {'q': query})
            return data.get('results', [])
        except Exception as e:
            logger.warning(f"Search failed: {e}")
            # Fallback to filtering known reports
            query_lower = query.lower()
            return [
                {'slug_id': slug, 'commodity': name}
                for name, slug in self.REPORT_SLUGS.items()
                if query_lower in name.lower()
            ]

    def health_check(self) -> Dict[str, Any]:
        """
        Check API connectivity and key validity

        Returns:
            Health status
        """
        try:
            # Try a simple request
            self._make_request('reports', {'limit': 1})
            return {
                'status': 'healthy',
                'api_key_valid': bool(self.api_key),
                'base_url': self.base_url
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'api_key_valid': bool(self.api_key),
                'base_url': self.base_url
            }
