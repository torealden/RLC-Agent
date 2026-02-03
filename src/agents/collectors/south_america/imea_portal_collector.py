"""
IMEA Portal Collector - Authenticated Access
Instituto Mato-Grossense de Economia Agropecuária

Provides authenticated access to IMEA Portal (portal.imea.com.br) for:
- Historical price series
- Production estimates and forecasts
- Crop condition reports
- Cost of production data
- Supply/demand balances

This collector uses legitimate portal access with user credentials.
Credentials are stored securely in environment variables.

Data source:
- https://portal.imea.com.br (authenticated)
- https://api1.imea.com.br (API backend)

Authentication:
- Free registration at portal.imea.com.br
- Credentials stored in IMEA_PORTAL_EMAIL and IMEA_PORTAL_PASSWORD env vars

Round Lakes Commodities - Commodities Data Pipeline
"""

import hashlib
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from enum import Enum

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CACHE_DIR = DATA_DIR / "cache" / "imea_portal"
DB_PATH = DATA_DIR / "rlc_commodities.db"

# IMEA Portal URLs
IMEA_URLS = {
    'portal': 'https://portal.imea.com.br',
    'api': 'https://api1.imea.com.br',
    'login': 'https://api1.imea.com.br/api/v2/login',
    'series': 'https://api1.imea.com.br/api/v2/mobile/series',
    'cotacoes': 'https://api1.imea.com.br/api/v2/mobile/cadeias/{chain_id}/cotacoes',
    'historico': 'https://api1.imea.com.br/api/v2/mobile/series/{series_id}/historico',
}

# IMEA commodity chain IDs
IMEA_CHAINS = {
    'soja': {'id': 4, 'en_name': 'soybeans'},
    'milho': {'id': 3, 'en_name': 'corn'},
    'algodao': {'id': 1, 'en_name': 'cotton'},
    'boi': {'id': 2, 'en_name': 'cattle'},
    'leite': {'id': 5, 'en_name': 'milk'},
}

# Update frequencies
class UpdateFrequency(Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass
class IMEAPortalConfig:
    """Configuration for IMEA Portal authenticated collector"""

    source_name: str = "IMEA_PORTAL"
    database_path: Path = field(default_factory=lambda: DB_PATH)
    cache_dir: Path = field(default_factory=lambda: CACHE_DIR)

    # Authentication (from environment)
    email: Optional[str] = None
    password: Optional[str] = None

    # HTTP settings
    timeout: int = 60
    retry_attempts: int = 3
    rate_limit_per_minute: int = 30

    # Session settings
    session_timeout_hours: int = 24

    # Data settings
    commodities: List[str] = field(default_factory=lambda: [
        'soybeans', 'corn', 'cotton'
    ])

    def __post_init__(self):
        # Load credentials from environment if not provided
        if not self.email:
            self.email = os.getenv('IMEA_PORTAL_EMAIL')
        if not self.password:
            self.password = os.getenv('IMEA_PORTAL_PASSWORD')

        # Create cache directory
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def has_credentials(self) -> bool:
        return bool(self.email and self.password)


@dataclass
class CollectionResult:
    """Result of a data collection operation"""
    success: bool
    source: str
    records_fetched: int = 0
    records_inserted: int = 0
    data: Optional[Any] = None
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    collected_at: datetime = field(default_factory=datetime.now)
    ingest_run_id: Optional[str] = None


class IMEAPortalCollector:
    """
    Authenticated collector for IMEA Portal.

    Provides access to:
    - Historical price series (cotações)
    - Production estimates
    - Crop progress data
    - Cost of production
    - Supply/demand balances

    Authentication:
    - Uses free portal.imea.com.br registration
    - Credentials stored in environment variables
    - Session tokens cached for efficiency

    Usage:
        collector = IMEAPortalCollector()

        # Authenticate
        if collector.authenticate():
            # Download historical prices
            result = collector.fetch_historical_prices('soybeans')

            # Get all available series
            series = collector.get_available_series()
    """

    def __init__(self, config: IMEAPortalConfig = None):
        self.config = config or IMEAPortalConfig()
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # Session management
        self.session = self._create_session()
        self.auth_token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        self.user_info: Optional[Dict] = None

        # Token cache file
        self.token_cache_path = self.config.cache_dir / ".imea_session"

        # Try to load cached session
        self._load_cached_session()

        self.logger.info(f"IMEAPortalCollector initialized. Credentials: {'configured' if self.config.has_credentials else 'not configured'}")

    def _create_session(self) -> requests.Session:
        """Create HTTP session with retry logic"""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.config.retry_attempts,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST"],
            raise_on_status=False,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            'User-Agent': 'IMEA-Digital/2.0 (RLC-Commodities)',
            'Accept': 'application/json',
            'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
            'Content-Type': 'application/json',
        })

        return session

    # =========================================================================
    # AUTHENTICATION
    # =========================================================================

    def authenticate(self, force: bool = False) -> bool:
        """
        Authenticate with IMEA Portal.

        Args:
            force: Force re-authentication even if token is valid

        Returns:
            True if authentication successful
        """
        # Check if we have a valid cached session
        if not force and self._is_session_valid():
            self.logger.info("Using cached authentication session")
            return True

        if not self.config.has_credentials:
            self.logger.error(
                "IMEA Portal credentials not configured. "
                "Set IMEA_PORTAL_EMAIL and IMEA_PORTAL_PASSWORD environment variables."
            )
            return False

        self.logger.info(f"Authenticating with IMEA Portal as {self.config.email}")

        try:
            # Login request
            response = self.session.post(
                IMEA_URLS['login'],
                json={
                    'email': self.config.email,
                    'senha': self.config.password,  # Portuguese: password
                },
                timeout=self.config.timeout
            )

            if response.status_code == 200:
                data = response.json()

                # Extract token (structure may vary)
                if 'token' in data:
                    self.auth_token = data['token']
                elif 'access_token' in data:
                    self.auth_token = data['access_token']
                elif 'data' in data and 'token' in data['data']:
                    self.auth_token = data['data']['token']
                else:
                    # Try to find token in response
                    self.auth_token = data.get('jwt') or data.get('sessionToken')

                if self.auth_token:
                    self.token_expires = datetime.now() + timedelta(hours=self.config.session_timeout_hours)
                    self.user_info = data.get('user') or data.get('usuario')

                    # Update session headers
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.auth_token}'
                    })

                    # Cache the session
                    self._save_session_cache()

                    self.logger.info("Authentication successful")
                    return True
                else:
                    self.logger.error(f"No token in response: {data}")
                    return False

            elif response.status_code == 401:
                self.logger.error("Authentication failed: Invalid credentials")
                return False
            else:
                self.logger.error(f"Authentication failed: HTTP {response.status_code}")
                return False

        except requests.exceptions.Timeout:
            self.logger.error(f"Authentication timeout after {self.config.timeout}s")
            return False
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Authentication request failed: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Authentication error: {e}")
            return False

    def _is_session_valid(self) -> bool:
        """Check if current session is valid"""
        if not self.auth_token:
            return False
        if not self.token_expires:
            return False
        if datetime.now() >= self.token_expires:
            self.logger.info("Session token expired")
            return False
        return True

    def _save_session_cache(self):
        """Save session token to cache file"""
        try:
            cache_data = {
                'token': self.auth_token,
                'expires': self.token_expires.isoformat() if self.token_expires else None,
                'email': self.config.email,
                'user_info': self.user_info,
            }
            with open(self.token_cache_path, 'w') as f:
                json.dump(cache_data, f)
            # Secure the file
            self.token_cache_path.chmod(0o600)
        except Exception as e:
            self.logger.warning(f"Failed to cache session: {e}")

    def _load_cached_session(self):
        """Load cached session token"""
        try:
            if self.token_cache_path.exists():
                with open(self.token_cache_path, 'r') as f:
                    cache_data = json.load(f)

                # Check if cache is for same user
                if cache_data.get('email') != self.config.email:
                    return

                # Check if token expired
                expires_str = cache_data.get('expires')
                if expires_str:
                    expires = datetime.fromisoformat(expires_str)
                    if datetime.now() < expires:
                        self.auth_token = cache_data.get('token')
                        self.token_expires = expires
                        self.user_info = cache_data.get('user_info')

                        # Update session headers
                        if self.auth_token:
                            self.session.headers.update({
                                'Authorization': f'Bearer {self.auth_token}'
                            })
                            self.logger.info("Loaded cached session")
        except Exception as e:
            self.logger.debug(f"No cached session available: {e}")

    def logout(self):
        """Clear session and logout"""
        self.auth_token = None
        self.token_expires = None
        self.user_info = None

        if 'Authorization' in self.session.headers:
            del self.session.headers['Authorization']

        if self.token_cache_path.exists():
            self.token_cache_path.unlink()

        self.logger.info("Logged out")

    # =========================================================================
    # DATA FETCHING
    # =========================================================================

    def get_available_series(self) -> CollectionResult:
        """
        Get list of available data series from IMEA.

        Returns:
            CollectionResult with list of available series
        """
        if not self._ensure_authenticated():
            return CollectionResult(
                success=False,
                source=self.config.source_name,
                error_message="Authentication required"
            )

        try:
            response = self.session.get(
                IMEA_URLS['series'],
                timeout=self.config.timeout
            )

            if response.status_code == 200:
                data = response.json()
                series_list = data if isinstance(data, list) else data.get('data', [])

                return CollectionResult(
                    success=True,
                    source=self.config.source_name,
                    records_fetched=len(series_list),
                    data=series_list
                )
            else:
                return CollectionResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"HTTP {response.status_code}"
                )

        except Exception as e:
            return CollectionResult(
                success=False,
                source=self.config.source_name,
                error_message=str(e)
            )

    def fetch_historical_prices(
        self,
        commodity: str,
        start_date: date = None,
        end_date: date = None
    ) -> CollectionResult:
        """
        Fetch historical price series for a commodity.

        Args:
            commodity: Commodity name (soybeans, corn, cotton, cattle, milk)
            start_date: Start date for historical data
            end_date: End date for historical data

        Returns:
            CollectionResult with price data
        """
        if not self._ensure_authenticated():
            return CollectionResult(
                success=False,
                source=self.config.source_name,
                error_message="Authentication required"
            )

        # Map commodity to chain ID
        chain_info = None
        for pt_name, info in IMEA_CHAINS.items():
            if info['en_name'] == commodity.lower():
                chain_info = info
                break

        if not chain_info:
            return CollectionResult(
                success=False,
                source=self.config.source_name,
                error_message=f"Unknown commodity: {commodity}. Valid: {[c['en_name'] for c in IMEA_CHAINS.values()]}"
            )

        chain_id = chain_info['id']

        try:
            url = IMEA_URLS['cotacoes'].format(chain_id=chain_id)

            params = {}
            if start_date:
                params['data_inicial'] = start_date.strftime('%Y-%m-%d')
            if end_date:
                params['data_final'] = end_date.strftime('%Y-%m-%d')

            self.logger.info(f"Fetching {commodity} prices from IMEA Portal (chain {chain_id})")

            response = self.session.get(
                url,
                params=params if params else None,
                timeout=self.config.timeout
            )

            if response.status_code == 200:
                data = response.json()

                # Handle different response structures
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    records = data.get('data', data.get('cotacoes', []))
                else:
                    records = []

                # Enrich records
                for record in records:
                    record['commodity'] = commodity
                    record['source'] = 'IMEA_PORTAL'
                    record['collected_at'] = datetime.now().isoformat()

                # Store in database
                inserted = self._store_price_records(records, commodity)

                return CollectionResult(
                    success=len(records) > 0,
                    source=self.config.source_name,
                    records_fetched=len(records),
                    records_inserted=inserted,
                    data=records
                )

            elif response.status_code == 401:
                # Token expired, try to re-authenticate
                self.logger.warning("Token expired, re-authenticating...")
                if self.authenticate(force=True):
                    return self.fetch_historical_prices(commodity, start_date, end_date)
                else:
                    return CollectionResult(
                        success=False,
                        source=self.config.source_name,
                        error_message="Re-authentication failed"
                    )
            else:
                return CollectionResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"HTTP {response.status_code}: {response.text[:200]}"
                )

        except Exception as e:
            self.logger.error(f"Error fetching prices: {e}")
            return CollectionResult(
                success=False,
                source=self.config.source_name,
                error_message=str(e)
            )

    def fetch_all_commodities(
        self,
        start_date: date = None,
        end_date: date = None
    ) -> Dict[str, CollectionResult]:
        """
        Fetch historical prices for all configured commodities.

        Returns:
            Dictionary of commodity -> CollectionResult
        """
        results = {}

        for commodity in self.config.commodities:
            self.logger.info(f"Fetching {commodity}...")
            results[commodity] = self.fetch_historical_prices(
                commodity, start_date, end_date
            )

            # Rate limiting
            time.sleep(60 / self.config.rate_limit_per_minute)

        return results

    def fetch_series_data(
        self,
        series_id: int,
        start_date: date = None,
        end_date: date = None
    ) -> CollectionResult:
        """
        Fetch data for a specific series by ID.

        Args:
            series_id: IMEA series identifier
            start_date: Start date
            end_date: End date

        Returns:
            CollectionResult with series data
        """
        if not self._ensure_authenticated():
            return CollectionResult(
                success=False,
                source=self.config.source_name,
                error_message="Authentication required"
            )

        try:
            url = IMEA_URLS['historico'].format(series_id=series_id)

            params = {}
            if start_date:
                params['data_inicial'] = start_date.strftime('%Y-%m-%d')
            if end_date:
                params['data_final'] = end_date.strftime('%Y-%m-%d')

            response = self.session.get(
                url,
                params=params if params else None,
                timeout=self.config.timeout
            )

            if response.status_code == 200:
                data = response.json()
                records = data if isinstance(data, list) else data.get('data', [])

                return CollectionResult(
                    success=True,
                    source=self.config.source_name,
                    records_fetched=len(records),
                    data=records
                )
            else:
                return CollectionResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"HTTP {response.status_code}"
                )

        except Exception as e:
            return CollectionResult(
                success=False,
                source=self.config.source_name,
                error_message=str(e)
            )

    def _ensure_authenticated(self) -> bool:
        """Ensure we have valid authentication"""
        if self._is_session_valid():
            return True
        return self.authenticate()

    # =========================================================================
    # DATABASE STORAGE
    # =========================================================================

    def _store_price_records(self, records: List[Dict], commodity: str) -> int:
        """Store price records in database"""
        if not records:
            return 0

        # Initialize schema
        self._initialize_schema()

        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        inserted = 0
        for record in records:
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO imea_portal_prices
                    (commodity, price_date, price_value, price_unit, location,
                     variation_pct, raw_data, source, collected_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    commodity,
                    record.get('data') or record.get('date') or record.get('price_date'),
                    record.get('valor') or record.get('value') or record.get('preco'),
                    record.get('unidade') or record.get('unit', 'BRL/saca'),
                    record.get('praca') or record.get('location', 'MT'),
                    record.get('variacao') or record.get('variation'),
                    json.dumps(record),
                    'IMEA_PORTAL',
                    record.get('collected_at', datetime.now().isoformat()),
                ))
                inserted += 1
            except Exception as e:
                self.logger.warning(f"Error inserting record: {e}")

        conn.commit()
        conn.close()

        self.logger.info(f"Stored {inserted} price records for {commodity}")
        return inserted

    def _initialize_schema(self):
        """Create database tables if they don't exist"""
        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        # IMEA Portal Prices table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS imea_portal_prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity TEXT NOT NULL,
                price_date DATE NOT NULL,
                price_value REAL,
                price_unit TEXT,
                location TEXT,
                variation_pct REAL,
                raw_data TEXT,
                source TEXT DEFAULT 'IMEA_PORTAL',
                collected_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity, price_date, location)
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_imea_portal_commodity_date
            ON imea_portal_prices(commodity, price_date)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_imea_portal_date
            ON imea_portal_prices(price_date)
        """)

        conn.commit()
        conn.close()

    # =========================================================================
    # SCHEDULED COLLECTION
    # =========================================================================

    def schedule_collection(
        self,
        frequency: UpdateFrequency = UpdateFrequency.DAILY,
        commodities: List[str] = None
    ) -> Dict:
        """
        Get schedule configuration for recurring data collection.

        This returns a configuration that can be used with a scheduler
        (e.g., cron, APScheduler, Celery) to run periodic collections.

        Args:
            frequency: How often to collect data
            commodities: List of commodities to collect (default: all configured)

        Returns:
            Schedule configuration dictionary
        """
        commodities = commodities or self.config.commodities

        # Determine cron expression based on frequency
        if frequency == UpdateFrequency.DAILY:
            # Run at 6 PM Brazil time (when daily data is typically available)
            cron_expression = "0 18 * * *"
            description = "Daily at 6:00 PM (BRT)"
        elif frequency == UpdateFrequency.WEEKLY:
            # Run on Fridays at 6 PM (after weekly bulletin)
            cron_expression = "0 18 * * 5"
            description = "Weekly on Friday at 6:00 PM (BRT)"
        elif frequency == UpdateFrequency.MONTHLY:
            # Run on 1st of month at 6 PM
            cron_expression = "0 18 1 * *"
            description = "Monthly on 1st at 6:00 PM (BRT)"
        else:
            cron_expression = "0 18 * * *"
            description = "Daily at 6:00 PM (BRT)"

        return {
            'collector': 'IMEAPortalCollector',
            'frequency': frequency.value,
            'cron_expression': cron_expression,
            'timezone': 'America/Cuiaba',  # Mato Grosso timezone
            'description': description,
            'commodities': commodities,
            'method': 'fetch_all_commodities',
            'requires_auth': True,
        }

    def run_scheduled_collection(self) -> Dict[str, CollectionResult]:
        """
        Run a scheduled collection job.

        This is the method that should be called by the scheduler.
        It authenticates, fetches all configured commodities, and
        returns the results.

        Returns:
            Dictionary of results by commodity
        """
        self.logger.info("=" * 50)
        self.logger.info("Starting scheduled IMEA Portal collection")
        self.logger.info("=" * 50)

        # Authenticate
        if not self.authenticate():
            self.logger.error("Scheduled collection failed: Authentication error")
            return {
                'error': CollectionResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="Authentication failed"
                )
            }

        # Fetch all commodities
        # Default to last 30 days to get recent updates
        end_date = date.today()
        start_date = end_date - timedelta(days=30)

        results = self.fetch_all_commodities(start_date, end_date)

        # Log summary
        total_fetched = sum(r.records_fetched for r in results.values() if r.success)
        total_inserted = sum(r.records_inserted for r in results.values() if r.success)
        successful = sum(1 for r in results.values() if r.success)

        self.logger.info("=" * 50)
        self.logger.info(f"Scheduled collection complete")
        self.logger.info(f"  Commodities: {successful}/{len(results)} successful")
        self.logger.info(f"  Records fetched: {total_fetched}")
        self.logger.info(f"  Records inserted: {total_inserted}")
        self.logger.info("=" * 50)

        return results

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def get_status(self) -> Dict:
        """Get collector status"""
        conn = sqlite3.connect(str(self.config.database_path))
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT COUNT(*) FROM imea_portal_prices")
            record_count = cursor.fetchone()[0]
        except:
            record_count = 0

        try:
            cursor.execute("SELECT MAX(price_date) FROM imea_portal_prices")
            latest_date = cursor.fetchone()[0]
        except:
            latest_date = None

        try:
            cursor.execute("SELECT MAX(collected_at) FROM imea_portal_prices")
            last_collection = cursor.fetchone()[0]
        except:
            last_collection = None

        conn.close()

        return {
            'source': self.config.source_name,
            'authenticated': self._is_session_valid(),
            'credentials_configured': self.config.has_credentials,
            'user': self.config.email if self.config.has_credentials else None,
            'record_count': record_count,
            'latest_data_date': latest_date,
            'last_collection': last_collection,
            'database_path': str(self.config.database_path),
            'commodities': self.config.commodities,
        }

    def get_latest_prices(self, commodity: str = None) -> Optional[Dict]:
        """Get latest prices from database"""
        conn = sqlite3.connect(str(self.config.database_path))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        try:
            if commodity:
                cursor.execute("""
                    SELECT * FROM imea_portal_prices
                    WHERE commodity = ?
                    ORDER BY price_date DESC
                    LIMIT 1
                """, (commodity,))
            else:
                cursor.execute("""
                    SELECT * FROM imea_portal_prices
                    ORDER BY price_date DESC
                    LIMIT 10
                """)

            rows = cursor.fetchall()
            if rows:
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.warning(f"Error getting latest prices: {e}")
        finally:
            conn.close()

        return None


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for IMEA Portal Collector"""
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser(
        description='IMEA Portal Authenticated Data Collector'
    )

    parser.add_argument(
        'command',
        choices=['auth', 'series', 'prices', 'all', 'status', 'schedule', 'run'],
        help='Command to execute'
    )

    parser.add_argument(
        '--commodity', '-c',
        help='Commodity to fetch (soybeans, corn, cotton, cattle, milk)'
    )

    parser.add_argument(
        '--start-date',
        help='Start date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--end-date',
        help='End date (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--email',
        help='IMEA Portal email (or set IMEA_PORTAL_EMAIL env var)'
    )

    parser.add_argument(
        '--password',
        help='IMEA Portal password (or set IMEA_PORTAL_PASSWORD env var)'
    )

    args = parser.parse_args()

    # Create config with CLI args
    config = IMEAPortalConfig(
        email=args.email,
        password=args.password
    )

    collector = IMEAPortalCollector(config)

    if args.command == 'auth':
        print("\nTesting authentication...")
        if collector.authenticate():
            print("Authentication successful!")
            print(f"User: {collector.user_info}")
        else:
            print("Authentication failed!")
            print("Make sure IMEA_PORTAL_EMAIL and IMEA_PORTAL_PASSWORD are set.")

    elif args.command == 'series':
        print("\nFetching available series...")
        result = collector.get_available_series()
        if result.success:
            print(f"Found {result.records_fetched} series")
            if result.data:
                for series in result.data[:10]:
                    print(f"  - {series}")
        else:
            print(f"Failed: {result.error_message}")

    elif args.command == 'prices':
        commodity = args.commodity or 'soybeans'
        start_date = date.fromisoformat(args.start_date) if args.start_date else None
        end_date = date.fromisoformat(args.end_date) if args.end_date else None

        print(f"\nFetching {commodity} prices...")
        result = collector.fetch_historical_prices(commodity, start_date, end_date)

        if result.success:
            print(f"Fetched {result.records_fetched} records")
            print(f"Inserted {result.records_inserted} records")
        else:
            print(f"Failed: {result.error_message}")

    elif args.command == 'all':
        print("\nFetching all commodities...")
        results = collector.fetch_all_commodities()

        for commodity, result in results.items():
            status = "OK" if result.success else "FAIL"
            print(f"  {commodity}: {status} ({result.records_fetched} records)")

    elif args.command == 'status':
        status = collector.get_status()
        print("\nIMEA Portal Collector Status:")
        for key, value in status.items():
            print(f"  {key}: {value}")

    elif args.command == 'schedule':
        schedule = collector.schedule_collection()
        print("\nSchedule Configuration:")
        for key, value in schedule.items():
            print(f"  {key}: {value}")

    elif args.command == 'run':
        print("\nRunning scheduled collection...")
        results = collector.run_scheduled_collection()

        print("\nResults:")
        for commodity, result in results.items():
            status = "OK" if result.success else "FAIL"
            print(f"  {commodity}: {status}")


if __name__ == '__main__':
    main()
