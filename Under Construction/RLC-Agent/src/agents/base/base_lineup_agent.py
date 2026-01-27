"""
Base Port Lineup Agent Class
Abstract base class providing common functionality for port line-up data collection agents
"""

import logging
import sys
import hashlib
import io
import re
import time
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)


@dataclass
class LineupFetchResult:
    """Result of a port lineup data fetch operation"""
    success: bool
    source: str
    report_week: str  # YYYY-Www format (ISO week)
    report_date: Optional[date] = None
    records_fetched: int = 0
    file_path: Optional[Path] = None
    data: Optional[pd.DataFrame] = None
    raw_content: Optional[bytes] = None
    response_time_ms: int = 0
    error_message: Optional[str] = None
    file_hash: Optional[str] = None


@dataclass
class LineupLoadResult:
    """Result of a port lineup data load operation"""
    success: bool
    source: str
    report_week: str
    report_date: Optional[date] = None
    records_read: int = 0
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_errored: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None
    quality_alerts: List[Dict] = field(default_factory=list)


class BaseLineupAgent(ABC):
    """
    Abstract base class for port line-up data collection agents

    Port line-up data differs from trade flow data in that it represents:
    - Vessel schedules and expected shipments at ports
    - Near-term (weekly) export activity view
    - Volumes by port and commodity
    - Often published as PDFs or industry reports

    Provides:
    - HTTP session management with retry logic
    - PDF download and caching
    - Rate limiting
    - Data normalization for ports and commodities
    - Quality validation
    - Logging and error handling
    """

    # Standard commodity mappings for agricultural products
    COMMODITY_MAPPINGS = {
        # Soybeans
        'soja': 'soybeans',
        'soya': 'soybeans',
        'soybean': 'soybeans',
        'soybeans': 'soybeans',
        'grão de soja': 'soybeans',
        'soja em grão': 'soybeans',

        # Soybean meal
        'farelo de soja': 'soybean_meal',
        'soybean meal': 'soybean_meal',
        'soymeal': 'soybean_meal',
        'meal': 'soybean_meal',

        # Corn
        'milho': 'corn',
        'corn': 'corn',
        'maize': 'corn',

        # Wheat
        'trigo': 'wheat',
        'wheat': 'wheat',

        # Soybean oil
        'óleo de soja': 'soybean_oil',
        'soybean oil': 'soybean_oil',
        'soy oil': 'soybean_oil',

        # Sugar
        'açúcar': 'sugar',
        'sugar': 'sugar',
    }

    # Brazilian port mappings
    BRAZIL_PORT_MAPPINGS = {
        'santos': 'Santos',
        'paranaguá': 'Paranagua',
        'paranagua': 'Paranagua',
        'rio grande': 'Rio Grande',
        'são francisco do sul': 'Sao Francisco do Sul',
        'sao francisco do sul': 'Sao Francisco do Sul',
        'sfds': 'Sao Francisco do Sul',
        'imbituba': 'Imbituba',
        'vitória': 'Vitoria',
        'vitoria': 'Vitoria',
        'são luís': 'Sao Luis',
        'sao luis': 'Sao Luis',
        'itacoatiara': 'Itacoatiara',
        'santarém': 'Santarem',
        'santarem': 'Santarem',
        'barcarena': 'Barcarena',
        'manaus': 'Manaus',
        'aratu': 'Aratu',
        'salvador': 'Salvador',
        'ilhéus': 'Ilheus',
        'ilheus': 'Ilheus',
    }

    def __init__(self, config, db_session_factory=None):
        """
        Initialize the agent

        Args:
            config: Country-specific lineup configuration object
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

        # Cache directory for downloaded files
        self.cache_dir = Path(config.data_directory) / 'lineup_cache' / self.country_code.lower()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Initialized {self.__class__.__name__} for {self.country_name}")

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic and exponential backoff"""
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

        # Set default headers
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 RLC-LineupAgent/1.0',
            'Accept': 'application/pdf, application/octet-stream, */*',
        })

        return session

    def _respect_rate_limit(self):
        """Enforce rate limiting between requests"""
        if self.last_request_time and self.config.rate_limit_per_minute:
            min_interval = 60.0 / self.config.rate_limit_per_minute
            elapsed = time.time() - self.last_request_time
            if elapsed < min_interval:
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
        timeout: int = None,
        stream: bool = False
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
                timeout=timeout,
                stream=stream
            )

            elapsed_ms = int((time.time() - start_time) * 1000)

            self.logger.debug(
                f"Request to {url}: status={response.status_code}, time={elapsed_ms}ms"
            )

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                self.logger.warning(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._make_request(url, method, params, headers, timeout, stream)

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
        key_fields = ['country', 'port', 'commodity', 'report_week']
        key_values = [str(record.get(k, '')) for k in key_fields]
        return hashlib.md5('|'.join(key_values).encode()).hexdigest()

    # =========================================================================
    # DATA NORMALIZATION
    # =========================================================================

    def normalize_commodity(self, commodity: str) -> str:
        """
        Normalize commodity name to standard format

        Args:
            commodity: Raw commodity name (may be in Portuguese, English, etc.)

        Returns:
            Normalized commodity string
        """
        if not commodity:
            return "unknown"

        commodity_lower = commodity.lower().strip()

        # Direct match
        if commodity_lower in self.COMMODITY_MAPPINGS:
            return self.COMMODITY_MAPPINGS[commodity_lower]

        # Partial match
        for key, value in self.COMMODITY_MAPPINGS.items():
            if key in commodity_lower:
                return value

        return commodity_lower.replace(' ', '_')

    def normalize_port(self, port: str, country: str = None) -> str:
        """
        Normalize port name

        Args:
            port: Raw port name
            country: Country code for country-specific mappings

        Returns:
            Normalized port name
        """
        if not port:
            return "Unknown"

        port_lower = port.lower().strip()

        # Brazil ports
        if country == 'BRA' or self.country_code == 'BRA':
            if port_lower in self.BRAZIL_PORT_MAPPINGS:
                return self.BRAZIL_PORT_MAPPINGS[port_lower]

        # Title case for unknown ports
        return port.strip().title()

    def normalize_volume(
        self,
        value: Any,
        unit: str = 'tons'
    ) -> Optional[float]:
        """
        Normalize volume to metric tons

        Args:
            value: Raw volume value (may be string with commas, thousands, etc.)
            unit: Unit of input (tons, thousand_tons, kg)

        Returns:
            Volume in metric tons
        """
        if value is None:
            return None

        try:
            # Handle string formatting
            if isinstance(value, str):
                # Remove commas and spaces
                value = value.replace(',', '').replace(' ', '').strip()
                # Handle European decimal notation
                value = value.replace('.', '').replace(',', '.')
                value = float(value)
            else:
                value = float(value)

            # Convert to tons
            unit_lower = unit.lower()
            if 'thousand' in unit_lower or 'mil' in unit_lower:
                return value * 1000
            elif 'kg' in unit_lower:
                return value / 1000
            else:
                return value

        except (ValueError, TypeError):
            return None

    def get_iso_week(self, dt: date) -> str:
        """
        Get ISO week string from date

        Args:
            dt: Date object

        Returns:
            String in YYYY-Www format (e.g., 2024-W44)
        """
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"

    def parse_week_string(self, week_str: str) -> Tuple[int, int]:
        """
        Parse week string to year and week number

        Args:
            week_str: String like "Week 44/2024", "2024-W44", "44/2024"

        Returns:
            Tuple of (year, week_number)
        """
        patterns = [
            r'(\d{4})-W(\d{1,2})',  # 2024-W44
            r'[Ww]eek\s*(\d{1,2})[/\-](\d{4})',  # Week 44/2024
            r'[Ss]emana\s*(\d{1,2})[/\-](\d{4})',  # Semana 44/2024 (Portuguese)
            r'(\d{1,2})[/\-](\d{4})',  # 44/2024
        ]

        for pattern in patterns:
            match = re.search(pattern, week_str)
            if match:
                groups = match.groups()
                if len(groups[0]) == 4:  # Year first
                    return int(groups[0]), int(groups[1])
                else:  # Week first
                    return int(groups[1]), int(groups[0])

        raise ValueError(f"Could not parse week string: {week_str}")

    # =========================================================================
    # FILE OPERATIONS
    # =========================================================================

    def download_pdf(self, url: str, cache_key: str = None) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Download a PDF file with caching

        Args:
            url: URL to download
            cache_key: Optional cache key for local caching

        Returns:
            Tuple of (content_bytes, error_message)
        """
        # Check cache first
        if cache_key:
            cache_path = self.cache_dir / f"{cache_key}.pdf"
            if cache_path.exists():
                self.logger.info(f"Using cached PDF: {cache_path}")
                return cache_path.read_bytes(), None

        # Download
        response, error = self._make_request(url, stream=True)

        if error:
            return None, error

        if response is None:
            return None, "No response received"

        if response.status_code != 200:
            return None, f"HTTP {response.status_code}: {response.reason}"

        content = response.content

        # Validate it's a PDF
        if not content.startswith(b'%PDF'):
            return None, "Response is not a valid PDF file"

        # Cache the file
        if cache_key:
            cache_path = self.cache_dir / f"{cache_key}.pdf"
            cache_path.write_bytes(content)
            self.logger.info(f"Cached PDF: {cache_path}")

        return content, None

    def clear_cache(self, older_than_days: int = 30):
        """Clear cached files older than specified days"""
        if not self.cache_dir.exists():
            return

        cutoff = datetime.now() - timedelta(days=older_than_days)

        for file_path in self.cache_dir.glob('*'):
            if file_path.is_file():
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if mtime < cutoff:
                    file_path.unlink()
                    self.logger.debug(f"Removed cached file: {file_path}")

    # =========================================================================
    # QUALITY VALIDATION
    # =========================================================================

    def validate_record(self, record: Dict) -> Tuple[bool, List[str]]:
        """
        Validate a single lineup record

        Returns:
            Tuple of (is_valid, list of issues)
        """
        issues = []

        # Required fields
        required = ['country', 'port', 'commodity', 'volume_tons', 'report_week']
        for field_name in required:
            if not record.get(field_name):
                issues.append(f"Missing required field: {field_name}")

        # Volume range checks
        volume = record.get('volume_tons')
        if volume is not None:
            if volume < 0:
                issues.append(f"Negative volume: {volume}")
            if volume > 50_000_000:  # 50 million tons max for a single line item
                issues.append(f"Unusually high volume: {volume}")

        # Week format check
        week = record.get('report_week', '')
        if week and not re.match(r'\d{4}-W\d{2}', week):
            issues.append(f"Invalid week format: {week}")

        return len(issues) == 0, issues

    def check_volume_deviation(
        self,
        current_volume: float,
        historical_volumes: List[float],
        threshold_pct: float = 50.0
    ) -> Tuple[bool, Optional[float]]:
        """
        Check if current volume deviates significantly from historical average

        Returns:
            Tuple of (is_outlier, deviation_pct)
        """
        if not historical_volumes or len(historical_volumes) < 3:
            return False, None

        mean = sum(historical_volumes) / len(historical_volumes)
        if mean == 0:
            return False, None

        deviation_pct = abs((current_volume - mean) / mean) * 100

        return deviation_pct > threshold_pct, deviation_pct

    # =========================================================================
    # ABSTRACT METHODS (to be implemented by subclasses)
    # =========================================================================

    @abstractmethod
    def fetch_data(
        self,
        year: int = None,
        week: int = None,
        report_date: date = None
    ) -> LineupFetchResult:
        """
        Fetch port lineup data for a specific period

        Args:
            year: Year to fetch
            week: ISO week number to fetch
            report_date: Alternative: specific report date

        Returns:
            LineupFetchResult with fetched data
        """
        pass

    @abstractmethod
    def parse_report(self, content: bytes, report_date: date = None) -> pd.DataFrame:
        """
        Parse report content (PDF, Excel, etc.) into DataFrame

        Args:
            content: Raw file content
            report_date: Report date for context

        Returns:
            Pandas DataFrame with parsed data
        """
        pass

    @abstractmethod
    def transform_to_records(self, df: pd.DataFrame, report_week: str) -> List[Dict]:
        """
        Transform DataFrame to list of normalized lineup records

        Args:
            df: Parsed DataFrame
            report_week: Report week in YYYY-Www format

        Returns:
            List of record dictionaries
        """
        pass

    # =========================================================================
    # MAIN WORKFLOW METHODS
    # =========================================================================

    def run_weekly_pull(
        self,
        year: int = None,
        week: int = None
    ) -> LineupLoadResult:
        """
        Run complete weekly lineup data pull workflow

        Args:
            year: Year to pull (default: current year)
            week: Week to pull (default: current week)

        Returns:
            LineupLoadResult with operation results
        """
        # Default to current week
        today = date.today()
        if year is None:
            year, week, _ = today.isocalendar()
        elif week is None:
            week = today.isocalendar()[1]

        report_week = f"{year}-W{week:02d}"

        self.last_run = datetime.now()
        self.logger.info(f"Starting weekly pull for {report_week}")

        try:
            # Fetch
            fetch_result = self.fetch_data(year=year, week=week)

            if not fetch_result.success:
                return LineupLoadResult(
                    success=False,
                    source=self.config.country_code,
                    report_week=report_week,
                    error_message=fetch_result.error_message
                )

            # Parse
            if fetch_result.data is not None:
                df = fetch_result.data
            elif fetch_result.raw_content:
                df = self.parse_report(fetch_result.raw_content, fetch_result.report_date)
            else:
                return LineupLoadResult(
                    success=False,
                    source=self.config.country_code,
                    report_week=report_week,
                    error_message="No data returned"
                )

            # Transform
            records = self.transform_to_records(df, report_week)

            # Validate and load
            load_result = self._load_records(records, report_week)

            if load_result.success:
                self.last_success = datetime.now()
                self.consecutive_failures = 0
            else:
                self.consecutive_failures += 1

            return load_result

        except Exception as e:
            self.logger.error(f"Error processing: {str(e)}", exc_info=True)
            self.consecutive_failures += 1
            return LineupLoadResult(
                success=False,
                source=self.config.country_code,
                report_week=report_week,
                error_message=str(e)
            )

    def _load_records(
        self,
        records: List[Dict],
        report_week: str
    ) -> LineupLoadResult:
        """Load validated records to database"""
        start_time = datetime.now()

        result = LineupLoadResult(
            success=True,
            source=self.config.country_code,
            report_week=report_week,
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
            f"Loaded {result.records_inserted} lineup records for {report_week}, "
            f"{result.records_errored} errors, {result.records_skipped} skipped"
        )

        return result

    def run_historical_backfill(
        self,
        start_year: int,
        start_week: int,
        end_year: int = None,
        end_week: int = None
    ) -> List[LineupLoadResult]:
        """
        Run historical backfill for a week range

        Args:
            start_year: Start year
            start_week: Start week number
            end_year: End year (default: current year)
            end_week: End week (default: current week)

        Returns:
            List of results per week
        """
        today = date.today()
        if end_year is None:
            end_year, end_week, _ = today.isocalendar()
        elif end_week is None:
            end_week = today.isocalendar()[1]

        results = []

        current_year = start_year
        current_week = start_week

        while (current_year < end_year) or (current_year == end_year and current_week <= end_week):
            result = self.run_weekly_pull(current_year, current_week)
            results.append(result)

            # Move to next week
            current_week += 1
            # Get number of weeks in current year
            last_week = date(current_year, 12, 28).isocalendar()[1]
            if current_week > last_week:
                current_week = 1
                current_year += 1

        return results

    def get_status(self) -> Dict[str, Any]:
        """Get current agent status"""
        return {
            'country_code': self.country_code,
            'country_name': self.country_name,
            'agent_type': 'lineup',
            'enabled': self.config.enabled,
            'last_run': str(self.last_run) if self.last_run else None,
            'last_success': str(self.last_success) if self.last_success else None,
            'consecutive_failures': self.consecutive_failures,
            'request_count': self.request_count,
            'is_healthy': self.consecutive_failures < 3,
            'cache_dir': str(self.cache_dir),
        }
