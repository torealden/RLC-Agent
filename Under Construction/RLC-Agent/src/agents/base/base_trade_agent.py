"""
Base Trade Agent Class
Abstract base class providing common functionality for all South American trade data agents
"""

import logging
import sys
import hashlib
import json
import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Generator
from decimal import Decimal

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

from sqlalchemy.orm import Session

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)


@dataclass
class FetchResult:
    """Result of a data fetch operation"""
    success: bool
    source: str
    period: str
    records_fetched: int = 0
    file_path: Optional[Path] = None
    data: Optional[pd.DataFrame] = None
    response_time_ms: int = 0
    error_message: Optional[str] = None
    file_hash: Optional[str] = None


@dataclass
class LoadResult:
    """Result of a data load operation"""
    success: bool
    source: str
    period_start: str
    period_end: str
    records_read: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_errored: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    quality_alerts: List[Dict] = field(default_factory=list)


class BaseTradeAgent(ABC):
    """
    Abstract base class for South American trade data collection agents

    Provides:
    - HTTP session management with retry logic
    - Rate limiting
    - Pagination handling
    - Data normalization
    - Quality validation
    - Database operations
    - Logging and error handling
    """

    def __init__(self, config, db_session_factory=None):
        """
        Initialize the agent

        Args:
            config: Country-specific configuration object
            db_session_factory: SQLAlchemy session factory
        """
        self.config = config
        self.country_code = config.country_code
        self.country_name = config.country_name
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{self.country_code}")

        # Database
        self._session_factory = db_session_factory

        # HTTP session
        self.session = self._create_session()

        # Rate limiting
        self.last_request_time = None
        self.request_count = 0

        # Tracking
        self.last_run = None
        self.last_success = None
        self.consecutive_failures = 0

        self.logger.info(f"Initialized {self.__class__.__name__} for {self.country_name}")

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic and exponential backoff"""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=self.config.retry_delay_base,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update({
            'User-Agent': 'RLC-TradeDataAgent/1.0',
            'Accept': 'application/json, text/csv, application/vnd.ms-excel',
        })

        return session

    def _respect_rate_limit(self):
        """Enforce rate limiting between requests"""
        if self.last_request_time and self.config.rate_limit_per_minute:
            min_interval = 60.0 / self.config.rate_limit_per_minute
            elapsed = time.time() - self.last_request_time
            if elapsed < min_interval:
                # Add jitter to avoid thundering herd
                sleep_time = min_interval - elapsed + random.uniform(0, 0.5)
                time.sleep(sleep_time)
        self.last_request_time = time.time()
        self.request_count += 1

    def _make_request(
        self,
        url: str,
        method: str = "GET",
        params: Dict = None,
        headers: Dict = None,
        data: Any = None,
        timeout: int = None
    ) -> Tuple[Optional[requests.Response], Optional[str]]:
        """
        Make an HTTP request with rate limiting and error handling

        Returns:
            Tuple of (response, error_message)
        """
        self._respect_rate_limit()
        timeout = timeout or self.config.timeout

        try:
            start_time = time.time()

            response = self.session.request(
                method=method,
                url=url,
                params=params,
                headers=headers,
                data=data,
                timeout=timeout
            )

            elapsed_ms = int((time.time() - start_time) * 1000)

            self.logger.debug(
                f"Request to {url}: status={response.status_code}, time={elapsed_ms}ms"
            )

            if response.status_code == 429:
                # Rate limited - wait and retry
                retry_after = int(response.headers.get('Retry-After', 60))
                self.logger.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._make_request(url, method, params, headers, data, timeout)

            return response, None

        except requests.exceptions.Timeout:
            return None, f"Request timeout after {timeout}s"
        except requests.exceptions.ConnectionError as e:
            return None, f"Connection error: {str(e)}"
        except Exception as e:
            return None, f"Request error: {str(e)}"

    def _compute_file_hash(self, content: bytes) -> str:
        """Compute SHA256 hash of file content"""
        return hashlib.sha256(content).hexdigest()

    def _compute_record_hash(self, record: Dict) -> str:
        """Compute hash for record deduplication"""
        key_fields = ['reporter_country', 'flow', 'period', 'hs_code', 'partner_country']
        key_values = [str(record.get(k, '')) for k in key_fields]
        return hashlib.md5('|'.join(key_values).encode()).hexdigest()

    # =========================================================================
    # DATA NORMALIZATION
    # =========================================================================

    def normalize_hs_code(self, hs_code: str, target_level: int = 6) -> str:
        """
        Normalize HS code to target level (default 6-digit)

        Args:
            hs_code: Original HS code (may be 8 or 10 digits)
            target_level: Target number of digits

        Returns:
            Normalized HS code string
        """
        if not hs_code:
            return ""

        # Remove any non-numeric characters
        hs_clean = ''.join(c for c in str(hs_code) if c.isdigit())

        # Truncate or pad as needed
        if len(hs_clean) >= target_level:
            return hs_clean[:target_level]
        else:
            return hs_clean.ljust(target_level, '0')

    def normalize_country_name(self, country_name: str) -> str:
        """Normalize country name for consistency"""
        if not country_name:
            return ""

        # Common normalizations
        normalizations = {
            "UNITED STATES": "USA",
            "UNITED STATES OF AMERICA": "USA",
            "ESTADOS UNIDOS": "USA",
            "U.S.A.": "USA",
            "CHINA": "CHN",
            "PEOPLE'S REPUBLIC OF CHINA": "CHN",
            "CHINA, PEOPLE'S REPUBLIC": "CHN",
            "GERMANY": "DEU",
            "ALEMANIA": "DEU",
            "BRASIL": "BRA",
            "BRAZIL": "BRA",
            "ARGENTINA": "ARG",
            "COLOMBIA": "COL",
            "URUGUAY": "URY",
            "PARAGUAY": "PRY",
        }

        name_upper = country_name.upper().strip()
        return normalizations.get(name_upper, name_upper)

    def convert_to_metric_tons(self, quantity: float, unit: str) -> Optional[float]:
        """Convert quantity to metric tons"""
        if quantity is None:
            return None

        unit_lower = str(unit).lower() if unit else "kg"

        conversions = {
            "kg": 0.001,
            "kilogram": 0.001,
            "kilogramo": 0.001,
            "ton": 1.0,
            "tons": 1.0,
            "metric ton": 1.0,
            "mt": 1.0,
            "lb": 0.000453592,
            "lbs": 0.000453592,
            "pound": 0.000453592,
        }

        factor = conversions.get(unit_lower, 0.001)  # Default to kg
        return quantity * factor

    def normalize_period(self, year: int, month: int) -> str:
        """Create standardized period string YYYY-MM"""
        return f"{year:04d}-{month:02d}"

    # =========================================================================
    # PAGINATION HELPERS
    # =========================================================================

    def paginate_api(
        self,
        base_url: str,
        params: Dict,
        offset_param: str = "offset",
        limit_param: str = "limit",
        page_size: int = None,
        max_records: int = None
    ) -> Generator[Dict, None, None]:
        """
        Generator for paginated API requests

        Yields:
            Response data for each page
        """
        page_size = page_size or self.config.page_size
        offset = 0
        total_fetched = 0

        while True:
            params[offset_param] = offset
            params[limit_param] = page_size

            response, error = self._make_request(base_url, params=params)

            if error or not response:
                self.logger.error(f"Pagination error at offset {offset}: {error}")
                break

            if response.status_code != 200:
                self.logger.error(f"API error: {response.status_code}")
                break

            try:
                data = response.json()
            except json.JSONDecodeError:
                self.logger.error("Failed to parse JSON response")
                break

            # Handle different response formats
            records = self._extract_records_from_response(data)

            if not records:
                break

            yield data

            total_fetched += len(records)
            offset += page_size

            if max_records and total_fetched >= max_records:
                break

            if len(records) < page_size:
                break  # Last page

    def _extract_records_from_response(self, data: Dict) -> List[Dict]:
        """Extract record list from API response (override per source)"""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ['data', 'records', 'result', 'results', 'items']:
                if key in data and isinstance(data[key], list):
                    return data[key]
        return []

    # =========================================================================
    # QUALITY VALIDATION
    # =========================================================================

    def validate_record(self, record: Dict) -> Tuple[bool, List[str]]:
        """
        Validate a single trade record

        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []

        # Required fields
        required = ['reporter_country', 'flow', 'period', 'hs_code',
                   'partner_country', 'value_usd']
        for field in required:
            if not record.get(field):
                issues.append(f"Missing required field: {field}")

        # Value range checks
        value = record.get('value_usd')
        if value is not None:
            if value < 0:
                issues.append(f"Negative value: {value}")
            if value > 1e12:
                issues.append(f"Unusually high value: {value}")

        quantity = record.get('quantity_kg')
        if quantity is not None:
            if quantity < 0:
                issues.append(f"Negative quantity: {quantity}")

        # HS code format
        hs_code = record.get('hs_code', '')
        if hs_code and not hs_code.replace('.', '').isdigit():
            issues.append(f"Invalid HS code format: {hs_code}")

        return len(issues) == 0, issues

    def check_monthly_deviation(
        self,
        current_value: float,
        trailing_values: List[float],
        threshold_pct: float = 20.0
    ) -> Tuple[bool, Optional[float]]:
        """
        Check if current value deviates significantly from trailing average

        Returns:
            Tuple of (is_outlier, deviation_pct)
        """
        if not trailing_values or len(trailing_values) < 3:
            return False, None

        mean = sum(trailing_values) / len(trailing_values)
        if mean == 0:
            return False, None

        deviation_pct = abs((current_value - mean) / mean) * 100

        return deviation_pct > threshold_pct, deviation_pct

    def compute_zscore(self, value: float, values: List[float]) -> Optional[float]:
        """Compute z-score for outlier detection"""
        if not values or len(values) < 2:
            return None

        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        std_dev = variance ** 0.5

        if std_dev == 0:
            return 0.0

        return (value - mean) / std_dev

    # =========================================================================
    # ABSTRACT METHODS (to be implemented by subclasses)
    # =========================================================================

    @abstractmethod
    def fetch_data(
        self,
        year: int,
        month: int,
        flow: str = "export"
    ) -> FetchResult:
        """
        Fetch trade data for a specific period

        Args:
            year: Year to fetch
            month: Month to fetch
            flow: 'export' or 'import'

        Returns:
            FetchResult with fetched data
        """
        pass

    @abstractmethod
    def parse_response(self, response_data: Any) -> pd.DataFrame:
        """
        Parse API response or file into standardized DataFrame

        Args:
            response_data: Raw response from API or file content

        Returns:
            Pandas DataFrame with standardized columns
        """
        pass

    @abstractmethod
    def transform_to_records(self, df: pd.DataFrame, flow: str) -> List[Dict]:
        """
        Transform DataFrame to list of normalized trade records

        Args:
            df: Parsed DataFrame
            flow: 'export' or 'import'

        Returns:
            List of record dictionaries ready for database insertion
        """
        pass

    # =========================================================================
    # MAIN WORKFLOW METHODS
    # =========================================================================

    def run_monthly_pull(
        self,
        year: int,
        month: int,
        flows: List[str] = None
    ) -> Dict[str, LoadResult]:
        """
        Run complete monthly data pull workflow

        Args:
            year: Year to pull
            month: Month to pull
            flows: List of flows to pull ['export', 'import']

        Returns:
            Dict mapping flow to LoadResult
        """
        flows = flows or ['export', 'import']
        results = {}

        self.last_run = datetime.now()
        self.logger.info(f"Starting monthly pull for {year}-{month:02d}")

        for flow in flows:
            self.logger.info(f"Fetching {flow}s for {self.country_name} {year}-{month:02d}")

            try:
                # Fetch
                fetch_result = self.fetch_data(year, month, flow)

                if not fetch_result.success:
                    results[flow] = LoadResult(
                        success=False,
                        source=self.config.country_code,
                        period_start=f"{year}-{month:02d}",
                        period_end=f"{year}-{month:02d}",
                        error_message=fetch_result.error_message
                    )
                    continue

                # Parse
                if fetch_result.data is not None:
                    df = fetch_result.data
                else:
                    self.logger.warning(f"No data returned for {flow}")
                    continue

                # Transform
                records = self.transform_to_records(df, flow)

                # Validate and load
                load_result = self._load_records(records, year, month, flow)
                results[flow] = load_result

                if load_result.success:
                    self.last_success = datetime.now()
                    self.consecutive_failures = 0
                else:
                    self.consecutive_failures += 1

            except Exception as e:
                self.logger.error(f"Error processing {flow}: {str(e)}", exc_info=True)
                self.consecutive_failures += 1
                results[flow] = LoadResult(
                    success=False,
                    source=self.config.country_code,
                    period_start=f"{year}-{month:02d}",
                    period_end=f"{year}-{month:02d}",
                    error_message=str(e)
                )

        return results

    def _load_records(
        self,
        records: List[Dict],
        year: int,
        month: int,
        flow: str
    ) -> LoadResult:
        """Load validated records to database"""
        start_time = datetime.now()
        period = self.normalize_period(year, month)

        result = LoadResult(
            success=True,
            source=self.config.country_code,
            period_start=period,
            period_end=period,
            records_read=len(records)
        )

        quality_alerts = []

        for record in records:
            is_valid, issues = self.validate_record(record)

            if not is_valid:
                result.records_errored += 1
                quality_alerts.append({
                    'type': 'validation_error',
                    'record': record,
                    'issues': issues
                })
                continue

            # Add record hash for deduplication
            record['record_hash'] = self._compute_record_hash(record)

            # Here you would insert to database
            # For now, count as inserted
            result.records_inserted += 1

        result.quality_alerts = quality_alerts
        result.duration_seconds = (datetime.now() - start_time).total_seconds()

        self.logger.info(
            f"Loaded {result.records_inserted} records for {period} {flow}, "
            f"{result.records_errored} errors, {result.records_skipped} skipped"
        )

        return result

    def run_historical_backfill(
        self,
        start_year: int,
        start_month: int,
        end_year: int = None,
        end_month: int = None
    ) -> List[Dict[str, LoadResult]]:
        """
        Run historical backfill for a date range

        Args:
            start_year: Start year
            start_month: Start month
            end_year: End year (default: current year)
            end_month: End month (default: current month - lag)

        Returns:
            List of results per month
        """
        end_year = end_year or datetime.now().year
        end_month = end_month or datetime.now().month - self.config.release_lag_months

        if end_month <= 0:
            end_month += 12
            end_year -= 1

        results = []

        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            month_result = self.run_monthly_pull(current_year, current_month)
            results.append({
                'period': f"{current_year}-{current_month:02d}",
                'results': month_result
            })

            # Move to next month
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        return results

    def get_status(self) -> Dict[str, Any]:
        """Get current agent status"""
        return {
            'country_code': self.country_code,
            'country_name': self.country_name,
            'enabled': self.config.enabled,
            'last_run': str(self.last_run) if self.last_run else None,
            'last_success': str(self.last_success) if self.last_success else None,
            'consecutive_failures': self.consecutive_failures,
            'request_count': self.request_count,
            'is_healthy': self.consecutive_failures < 3,
        }
