"""
Base Collector Class

Abstract base class for all data collectors with common functionality:
- HTTP session management with retry logic
- Rate limiting
- Data caching
- Database operations
- Error handling and logging
"""

import logging
import time
import random
import hashlib
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Union
from enum import Enum

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class DataFrequency(Enum):
    """Data update frequency"""
    REALTIME = "realtime"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    ANNUAL = "annual"


class AuthType(Enum):
    """Authentication type required"""
    NONE = "none"
    API_KEY = "api_key"
    OAUTH = "oauth"
    PAID = "paid"


@dataclass
class CollectorConfig:
    """Configuration for a data collector"""
    source_name: str
    source_url: str
    auth_type: AuthType = AuthType.NONE
    api_key: Optional[str] = None
    api_secret: Optional[str] = None

    # HTTP settings
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay_base: float = 1.0
    rate_limit_per_minute: int = 60

    # Caching
    cache_enabled: bool = True
    cache_directory: Path = field(default_factory=lambda: Path("./data/cache"))
    cache_ttl_hours: int = 24

    # Database
    db_connection_string: Optional[str] = None
    db_table_prefix: str = ""

    # Data settings
    frequency: DataFrequency = DataFrequency.DAILY
    commodities: List[str] = field(default_factory=lambda: ["corn", "soybeans", "wheat"])

    def __post_init__(self):
        if isinstance(self.cache_directory, str):
            self.cache_directory = Path(self.cache_directory)


@dataclass
class CollectorResult:
    """Result of a data collection operation"""
    success: bool
    source: str
    collected_at: datetime = field(default_factory=datetime.now)

    # Data
    records_fetched: int = 0
    data: Optional[Any] = None  # DataFrame or dict

    # Timing
    response_time_ms: int = 0

    # Metadata
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    data_as_of: Optional[str] = None

    # Errors/Warnings
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)

    # Cache info
    from_cache: bool = False
    cache_key: Optional[str] = None


class BaseCollector(ABC):
    """
    Abstract base class for all data source collectors.

    Provides:
    - HTTP session management with retry logic
    - Rate limiting
    - Data caching
    - Common data transformations
    - Database operations
    - Logging and error handling

    Subclasses must implement:
    - fetch_data(): Main data fetching logic
    - parse_response(): Parse API/file response
    - get_table_name(): Database table name
    """

    def __init__(self, config: CollectorConfig):
        """
        Initialize the collector.

        Args:
            config: Collector configuration
        """
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # HTTP session
        self.session = self._create_session()

        # Rate limiting
        self.last_request_time: Optional[float] = None
        self.request_count = 0

        # Tracking
        self.last_run: Optional[datetime] = None
        self.last_success: Optional[datetime] = None
        self.consecutive_failures = 0

        # Cache
        if self.config.cache_enabled:
            self.config.cache_directory.mkdir(parents=True, exist_ok=True)

        self.logger.info(
            f"Initialized {self.__class__.__name__} for {config.source_name}"
        )

    # =========================================================================
    # HTTP SESSION MANAGEMENT
    # =========================================================================

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=self.config.retry_delay_base,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Default headers
        session.headers.update({
            'User-Agent': 'RLC-DataCollector/1.0 (Agricultural Research)',
            'Accept': 'application/json, text/csv, */*',
        })

        # Add API key if configured
        if self.config.auth_type == AuthType.API_KEY and self.config.api_key:
            session.headers['Authorization'] = f'Bearer {self.config.api_key}'

        return session

    def _respect_rate_limit(self):
        """Enforce rate limiting between requests"""
        if self.last_request_time and self.config.rate_limit_per_minute:
            min_interval = 60.0 / self.config.rate_limit_per_minute
            elapsed = time.time() - self.last_request_time
            if elapsed < min_interval:
                sleep_time = min_interval - elapsed + random.uniform(0, 0.2)
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
        json_data: Dict = None,
        timeout: int = None
    ) -> Tuple[Optional[requests.Response], Optional[str]]:
        """
        Make an HTTP request with rate limiting and error handling.

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
                json=json_data,
                timeout=timeout
            )

            elapsed_ms = int((time.time() - start_time) * 1000)

            self.logger.debug(
                f"Request to {url}: status={response.status_code}, time={elapsed_ms}ms"
            )

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                self.logger.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._make_request(url, method, params, headers, data, json_data, timeout)

            return response, None

        except requests.exceptions.Timeout:
            return None, f"Request timeout after {timeout}s"
        except requests.exceptions.ConnectionError as e:
            return None, f"Connection error: {str(e)}"
        except Exception as e:
            return None, f"Request error: {str(e)}"

    # =========================================================================
    # CACHING
    # =========================================================================

    def _get_cache_key(self, *args) -> str:
        """Generate cache key from arguments"""
        key_string = f"{self.config.source_name}|{'|'.join(str(a) for a in args)}"
        return hashlib.md5(key_string.encode()).hexdigest()

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get path to cache file"""
        return self.config.cache_directory / f"{cache_key}.json"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache file is still valid"""
        if not cache_path.exists():
            return False

        file_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age_hours = (datetime.now() - file_time).total_seconds() / 3600

        return age_hours < self.config.cache_ttl_hours

    def _read_cache(self, cache_key: str) -> Optional[Dict]:
        """Read data from cache"""
        cache_path = self._get_cache_path(cache_key)

        if not self._is_cache_valid(cache_path):
            return None

        try:
            with open(cache_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"Error reading cache: {e}")
            return None

    def _write_cache(self, cache_key: str, data: Any):
        """Write data to cache"""
        cache_path = self._get_cache_path(cache_key)

        try:
            # Convert DataFrame to dict if needed
            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                data = data.to_dict(orient='records')

            with open(cache_path, 'w') as f:
                json.dump(data, f, default=str)
        except Exception as e:
            self.logger.warning(f"Error writing cache: {e}")

    # =========================================================================
    # DATA TRANSFORMATION HELPERS
    # =========================================================================

    def normalize_commodity(self, commodity: str) -> str:
        """Normalize commodity name to standard format"""
        normalizations = {
            'CORN': 'corn',
            'MAIZE': 'corn',
            'SOYBEANS': 'soybeans',
            'SOYBEAN': 'soybeans',
            'SOY': 'soybeans',
            'WHEAT': 'wheat',
            'HRW': 'wheat_hrw',
            'SRW': 'wheat_srw',
            'HARD RED WINTER': 'wheat_hrw',
            'SOFT RED WINTER': 'wheat_srw',
            'SOYBEAN MEAL': 'soybean_meal',
            'SOYMEAL': 'soybean_meal',
            'SOYBEAN OIL': 'soybean_oil',
            'SOYOIL': 'soybean_oil',
        }

        upper = commodity.upper().strip()
        return normalizations.get(upper, commodity.lower().strip())

    def parse_date(self, date_str: str, formats: List[str] = None) -> Optional[date]:
        """Parse date string with multiple format attempts"""
        if not date_str:
            return None

        formats = formats or [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y%m%d',
            '%b %d, %Y',
            '%B %d, %Y',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError:
                continue

        self.logger.warning(f"Could not parse date: {date_str}")
        return None

    def convert_to_metric_tons(self, value: float, unit: str) -> Optional[float]:
        """Convert various units to metric tons"""
        if value is None:
            return None

        unit_lower = str(unit).lower() if unit else "mt"

        conversions = {
            'kg': 0.001,
            'kilogram': 0.001,
            'mt': 1.0,
            'metric ton': 1.0,
            'metric tons': 1.0,
            'tonne': 1.0,
            'tonnes': 1.0,
            't': 1.0,
            'thousand mt': 1000.0,
            'tmt': 1000.0,
            '000 mt': 1000.0,
            'mmt': 1_000_000.0,
            'million mt': 1_000_000.0,
            'bushel': None,  # Need commodity-specific
            'bu': None,
        }

        factor = conversions.get(unit_lower)
        if factor is None:
            self.logger.warning(f"Unknown unit: {unit}, assuming MT")
            return value

        return value * factor

    def bushels_to_mt(self, bushels: float, commodity: str) -> float:
        """Convert bushels to metric tons"""
        # Bushels per metric ton by commodity
        bu_per_mt = {
            'corn': 39.368,
            'soybeans': 36.744,
            'wheat': 36.744,
            'wheat_hrw': 36.744,
            'wheat_srw': 36.744,
            'sorghum': 39.368,
            'barley': 45.93,
            'oats': 68.894,
        }

        norm_commodity = self.normalize_commodity(commodity)
        factor = bu_per_mt.get(norm_commodity, 36.744)  # Default to wheat

        return bushels / factor

    # =========================================================================
    # ABSTRACT METHODS (must be implemented by subclasses)
    # =========================================================================

    @abstractmethod
    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch data from the source.

        Args:
            start_date: Start of date range
            end_date: End of date range
            **kwargs: Source-specific parameters

        Returns:
            CollectorResult with fetched data
        """
        pass

    @abstractmethod
    def parse_response(self, response_data: Any) -> Any:
        """
        Parse API response into standardized format.

        Args:
            response_data: Raw response from API

        Returns:
            Parsed data (typically DataFrame or dict)
        """
        pass

    @abstractmethod
    def get_table_name(self) -> str:
        """
        Get database table name for this collector's data.

        Returns:
            Table name string
        """
        pass

    # =========================================================================
    # MAIN WORKFLOW
    # =========================================================================

    def collect(
        self,
        start_date: date = None,
        end_date: date = None,
        use_cache: bool = True,
        **kwargs
    ) -> CollectorResult:
        """
        Main collection workflow with caching.

        Args:
            start_date: Start of date range
            end_date: End of date range
            use_cache: Whether to use cached data
            **kwargs: Source-specific parameters

        Returns:
            CollectorResult with collected data
        """
        self.last_run = datetime.now()

        # Generate cache key
        cache_key = self._get_cache_key(start_date, end_date, *kwargs.values())

        # Check cache
        if use_cache and self.config.cache_enabled:
            cached_data = self._read_cache(cache_key)
            if cached_data:
                self.logger.info(f"Using cached data for {self.config.source_name}")
                return CollectorResult(
                    success=True,
                    source=self.config.source_name,
                    records_fetched=len(cached_data) if isinstance(cached_data, list) else 1,
                    data=cached_data,
                    from_cache=True,
                    cache_key=cache_key
                )

        # Fetch fresh data
        try:
            result = self.fetch_data(start_date, end_date, **kwargs)

            if result.success:
                self.last_success = datetime.now()
                self.consecutive_failures = 0

                # Cache successful results
                if self.config.cache_enabled and result.data is not None:
                    self._write_cache(cache_key, result.data)
                    result.cache_key = cache_key
            else:
                self.consecutive_failures += 1

            return result

        except Exception as e:
            self.consecutive_failures += 1
            self.logger.error(f"Collection error: {e}", exc_info=True)

            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=str(e)
            )

    def get_status(self) -> Dict[str, Any]:
        """Get current collector status"""
        return {
            'source_name': self.config.source_name,
            'source_url': self.config.source_url,
            'auth_type': self.config.auth_type.value,
            'frequency': self.config.frequency.value,
            'last_run': str(self.last_run) if self.last_run else None,
            'last_success': str(self.last_success) if self.last_success else None,
            'consecutive_failures': self.consecutive_failures,
            'request_count': self.request_count,
            'is_healthy': self.consecutive_failures < 3,
            'cache_enabled': self.config.cache_enabled,
        }

    def test_connection(self) -> Tuple[bool, str]:
        """
        Test connection to the data source.

        Returns:
            Tuple of (success, message)
        """
        try:
            response, error = self._make_request(
                self.config.source_url,
                timeout=10
            )

            if error:
                return False, error

            if response.status_code == 200:
                return True, "Connection successful"
            elif response.status_code == 401:
                return False, "Authentication required"
            elif response.status_code == 403:
                return False, "Access forbidden - check credentials"
            else:
                return False, f"HTTP {response.status_code}"

        except Exception as e:
            return False, str(e)
