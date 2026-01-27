"""
Price Data Agent

Handles fetching market prices from the API Manager agent or directly
from USDA AMS. Retrieves current prices, week-ago prices, and year-ago
prices for configured series.
"""

import logging
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..config.settings import HBWeeklyReportConfig, APIManagerConfig, PRICE_SERIES_DEFINITIONS

logger = logging.getLogger(__name__)


@dataclass
class PricePoint:
    """Single price data point"""
    series_id: str
    series_name: str
    price: Optional[float]
    price_date: Optional[date]
    unit: str
    source: str
    is_estimate: bool = False


@dataclass
class PriceComparison:
    """Price comparison across time periods"""
    series_id: str
    series_name: str
    commodity: Optional[str] = None

    # Current price
    current: Optional[PricePoint] = None

    # Week ago price
    week_ago: Optional[PricePoint] = None

    # Year ago price
    year_ago: Optional[PricePoint] = None

    # Calculated changes
    week_change: Optional[float] = None
    week_change_pct: Optional[float] = None
    year_change: Optional[float] = None
    year_change_pct: Optional[float] = None

    # For spreads
    pct_of_full_carry: Optional[float] = None

    unit: str = ""


@dataclass
class PriceDataResult:
    """Result of price data fetch operation"""
    success: bool
    fetched_at: datetime = field(default_factory=datetime.utcnow)

    # Price comparisons by series
    prices: Dict[str, PriceComparison] = field(default_factory=dict)

    # Summary
    series_fetched: int = 0
    series_failed: int = 0
    used_previous_day: bool = False

    # Errors
    errors: List[str] = field(default_factory=list)


class PriceDataAgent:
    """
    Agent for fetching market price data via API Manager or direct API calls

    Handles:
    - Fetching prices from API Manager service
    - Direct USDA AMS API fallback
    - Historical price lookups (week-ago, year-ago)
    - Date adjustment for weekends/holidays
    - Spread calculations including % of full carry
    """

    def __init__(self, config: HBWeeklyReportConfig):
        """
        Initialize Price Data Agent

        Args:
            config: HB Weekly Report configuration
        """
        self.config = config
        self.api_config = config.api_manager
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # HTTP session
        self.session = self._create_session()

        # Price series definitions
        self.series_definitions = PRICE_SERIES_DEFINITIONS

        # Cache
        self._cached_result: Optional[PriceDataResult] = None
        self._cache_timestamp: Optional[datetime] = None

        self.logger.info(f"Initialized PriceDataAgent, API Manager: {self.api_config.base_url}")

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            'User-Agent': 'HB-ReportWriter/1.0',
            'Accept': 'application/json',
        })

        return session

    def fetch_prices(
        self,
        report_date: date = None,
        series_ids: List[str] = None,
        force_refresh: bool = False
    ) -> PriceDataResult:
        """
        Fetch all required price data for the report

        Args:
            report_date: Date for the report (default: today)
            series_ids: Specific series to fetch (default: all configured)
            force_refresh: Bypass cache

        Returns:
            PriceDataResult with all price comparisons
        """
        report_date = report_date or date.today()
        series_ids = series_ids or self.api_config.default_series

        # Check cache
        if not force_refresh and self._cached_result and self._cache_timestamp:
            cache_age = (datetime.utcnow() - self._cache_timestamp).total_seconds()
            if cache_age < 1800:  # 30 minute cache
                self.logger.info("Returning cached price data")
                return self._cached_result

        result = PriceDataResult(success=True)
        errors = []

        # Calculate target dates
        current_date = report_date
        week_ago_date = report_date - timedelta(days=self.api_config.week_ago_days)
        year_ago_date = report_date - timedelta(days=self.api_config.year_ago_days)

        self.logger.info(
            f"Fetching prices for report date {report_date}: "
            f"current={current_date}, week_ago={week_ago_date}, year_ago={year_ago_date}"
        )

        # Fetch each series
        for series_id in series_ids:
            try:
                comparison = self._fetch_series_comparison(
                    series_id,
                    current_date,
                    week_ago_date,
                    year_ago_date
                )
                if comparison:
                    result.prices[series_id] = comparison
                    result.series_fetched += 1

                    if comparison.current and comparison.current.is_estimate:
                        result.used_previous_day = True
                else:
                    result.series_failed += 1
                    errors.append(f"Failed to fetch {series_id}")

            except Exception as e:
                self.logger.error(f"Error fetching {series_id}: {e}")
                result.series_failed += 1
                errors.append(f"{series_id}: {str(e)}")

        result.errors = errors
        result.success = result.series_fetched > 0

        # Update cache
        if result.success:
            self._cached_result = result
            self._cache_timestamp = datetime.utcnow()

        self.logger.info(
            f"Price fetch complete: {result.series_fetched} fetched, "
            f"{result.series_failed} failed"
        )

        return result

    def _fetch_series_comparison(
        self,
        series_id: str,
        current_date: date,
        week_ago_date: date,
        year_ago_date: date
    ) -> Optional[PriceComparison]:
        """Fetch price comparison for a single series"""
        series_def = self.series_definitions.get(series_id, {})

        comparison = PriceComparison(
            series_id=series_id,
            series_name=series_def.get("name", series_id),
            unit=series_def.get("unit", ""),
        )

        # Determine commodity from series
        commodity_map = {
            "corn": ["corn"],
            "wheat": ["wheat", "hrw", "srw"],
            "soybeans": ["soybean", "soybeans"],
            "soybean_meal": ["meal"],
            "soybean_oil": ["oil"],
        }
        for commodity, keywords in commodity_map.items():
            if any(kw in series_id.lower() for kw in keywords):
                comparison.commodity = commodity
                break

        # Try API Manager first
        if self.api_config.enabled:
            current = self._fetch_from_api_manager(series_id, current_date)
            week_ago = self._fetch_from_api_manager(series_id, week_ago_date)
            year_ago = self._fetch_from_api_manager(series_id, year_ago_date)
        else:
            # Direct USDA AMS fallback
            current = self._fetch_from_usda_ams(series_id, current_date)
            week_ago = self._fetch_from_usda_ams(series_id, week_ago_date)
            year_ago = self._fetch_from_usda_ams(series_id, year_ago_date)

        # Handle missing current date (use previous day)
        if current is None or current.price is None:
            adjusted_date = current_date - timedelta(days=1)
            current = self._fetch_from_api_manager(series_id, adjusted_date) if self.api_config.enabled else None
            if current:
                current.is_estimate = True
                self.logger.info(f"Used previous day price for {series_id}")

        comparison.current = current
        comparison.week_ago = week_ago
        comparison.year_ago = year_ago

        # Calculate changes
        if current and current.price is not None:
            if week_ago and week_ago.price is not None:
                comparison.week_change = current.price - week_ago.price
                if week_ago.price != 0:
                    comparison.week_change_pct = (comparison.week_change / week_ago.price) * 100

            if year_ago and year_ago.price is not None:
                comparison.year_change = current.price - year_ago.price
                if year_ago.price != 0:
                    comparison.year_change_pct = (comparison.year_change / year_ago.price) * 100

        # Calculate % of full carry for spreads
        if series_def.get("type") == "spread" and current and current.price is not None:
            comparison.pct_of_full_carry = self._calculate_pct_full_carry(
                current.price,
                self.config.commodities.storage_cost_per_month,
                self.config.commodities.interest_rate_annual
            )

        return comparison

    def _fetch_from_api_manager(
        self,
        series_id: str,
        target_date: date
    ) -> Optional[PricePoint]:
        """Fetch price from API Manager service"""
        try:
            url = f"{self.api_config.base_url}/prices/{series_id}"
            params = {
                "date": target_date.isoformat(),
            }
            headers = {}
            if self.api_config.api_key:
                headers["Authorization"] = f"Bearer {self.api_config.api_key}"

            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.api_config.timeout
            )

            if response.status_code == 200:
                data = response.json()
                return PricePoint(
                    series_id=series_id,
                    series_name=data.get("name", series_id),
                    price=data.get("price"),
                    price_date=date.fromisoformat(data["date"]) if data.get("date") else target_date,
                    unit=data.get("unit", ""),
                    source="API Manager",
                )
            elif response.status_code == 404:
                self.logger.debug(f"No price found for {series_id} on {target_date}")
                return None
            else:
                self.logger.warning(f"API Manager error {response.status_code} for {series_id}")
                return None

        except requests.exceptions.RequestException as e:
            self.logger.warning(f"API Manager request failed for {series_id}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error fetching from API Manager: {e}")
            return None

    def _fetch_from_usda_ams(
        self,
        series_id: str,
        target_date: date
    ) -> Optional[PricePoint]:
        """Fetch price directly from USDA AMS API (fallback)"""
        try:
            # Map series_id to USDA AMS report slug
            # This mapping would be configured based on actual USDA report IDs
            ams_mapping = self._get_usda_ams_mapping(series_id)
            if not ams_mapping:
                return None

            url = f"{self.api_config.usda_ams_base_url}/{ams_mapping['slug_id']}"
            params = {
                "start_date": (target_date - timedelta(days=7)).isoformat(),
                "end_date": target_date.isoformat(),
            }

            if self.api_config.usda_api_key:
                params["api_key"] = self.api_config.usda_api_key

            response = self.session.get(url, params=params, timeout=self.api_config.timeout)

            if response.status_code == 200:
                data = response.json()
                # Parse USDA AMS response format
                if "results" in data and len(data["results"]) > 0:
                    # Get most recent record
                    record = data["results"][-1]
                    price_value = self._extract_price_from_ams_record(record, ams_mapping)

                    if price_value is not None:
                        return PricePoint(
                            series_id=series_id,
                            series_name=ams_mapping.get("name", series_id),
                            price=price_value,
                            price_date=target_date,
                            unit=ams_mapping.get("unit", ""),
                            source="USDA AMS Direct",
                        )

            return None

        except Exception as e:
            self.logger.warning(f"USDA AMS fetch failed for {series_id}: {e}")
            return None

    def _get_usda_ams_mapping(self, series_id: str) -> Optional[Dict]:
        """Get USDA AMS report mapping for a series"""
        # This would be a comprehensive mapping configuration
        # Simplified for this implementation
        mappings = {
            "gulf_corn_fob": {
                "slug_id": "2466",  # Example USDA report ID
                "name": "Gulf Corn FOB",
                "field": "price_low_high_avg",
                "unit": "$/bu",
            },
            "gulf_soybean_fob": {
                "slug_id": "2466",
                "name": "Gulf Soybean FOB",
                "field": "price_low_high_avg",
                "unit": "$/bu",
            },
        }
        return mappings.get(series_id)

    def _extract_price_from_ams_record(self, record: Dict, mapping: Dict) -> Optional[float]:
        """Extract price value from USDA AMS record"""
        try:
            field = mapping.get("field", "price")
            if field in record:
                return float(record[field])
            # Try common field names
            for key in ["low_price", "high_price", "avg_price", "price"]:
                if key in record and record[key] is not None:
                    return float(record[key])
            return None
        except (ValueError, TypeError):
            return None

    def _calculate_pct_full_carry(
        self,
        spread_value: float,
        storage_cost: float,
        interest_rate: float
    ) -> float:
        """
        Calculate percentage of full carry for a spread

        Args:
            spread_value: Spread in cents/bu
            storage_cost: Storage cost per bushel per month
            interest_rate: Annual interest rate (decimal)

        Returns:
            Percentage of full carry
        """
        # Full carry = storage + interest
        # Assuming 3 months between contracts
        months = 3
        storage_component = storage_cost * months * 100  # Convert to cents
        interest_component = interest_rate / 12 * months * 100  # Simplified

        full_carry = storage_component + interest_component

        if full_carry == 0:
            return 0.0

        return (spread_value / full_carry) * 100

    def get_price_table_data(self) -> Dict[str, List[Dict]]:
        """
        Get formatted price data for report tables

        Returns:
            Dict with 'futures', 'spreads', 'international' table data
        """
        if not self._cached_result or not self._cached_result.success:
            self.fetch_prices()

        if not self._cached_result:
            return {"futures": [], "spreads": [], "international": []}

        futures = []
        spreads = []
        international = []

        for series_id, comparison in self._cached_result.prices.items():
            row = {
                "series_id": series_id,
                "name": comparison.series_name,
                "current": comparison.current.price if comparison.current else None,
                "current_date": comparison.current.price_date.isoformat() if comparison.current and comparison.current.price_date else None,
                "week_ago": comparison.week_ago.price if comparison.week_ago else None,
                "year_ago": comparison.year_ago.price if comparison.year_ago else None,
                "week_change": comparison.week_change,
                "week_change_pct": comparison.week_change_pct,
                "year_change": comparison.year_change,
                "year_change_pct": comparison.year_change_pct,
                "unit": comparison.unit,
            }

            series_def = self.series_definitions.get(series_id, {})

            if series_def.get("type") == "spread":
                row["pct_full_carry"] = comparison.pct_of_full_carry
                spreads.append(row)
            elif "fob" in series_id.lower() or "brazil" in series_id.lower():
                international.append(row)
            else:
                futures.append(row)

        return {
            "futures": futures,
            "spreads": spreads,
            "international": international,
        }

    def get_price_for_commodity(self, commodity: str) -> Optional[PriceComparison]:
        """Get the main price comparison for a commodity"""
        if not self._cached_result:
            self.fetch_prices()

        if not self._cached_result:
            return None

        # Map commodity to primary series
        commodity_series = {
            "corn": "corn_front_month",
            "wheat": "wheat_hrw_front_month",
            "soybeans": "soybeans_front_month",
            "soybean_meal": "soybean_meal_front_month",
            "soybean_oil": "soybean_oil_front_month",
        }

        series_id = commodity_series.get(commodity.lower())
        if series_id:
            return self._cached_result.prices.get(series_id)

        return None
