"""
Base Collector Class for RLC Environmental/Regulatory Data

Abstract base class providing:
- HTTP requests with retry/exponential backoff
- Rate limiting
- Structured JSON-lines logging
- Raw response archiving
- Multi-sheet Excel workbook generation
- Run lifecycle management (startup → collect → shutdown)
"""

import hashlib
import json
import logging
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import openpyxl
    from openpyxl.utils import get_column_letter
    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

from .logging_utils import (
    LogAction, generate_run_id, setup_collector_logger, make_log_record
)


@dataclass
class CollectorConfig:
    """Configuration for a data collector."""
    collector_name: str
    collector_version: str
    description: str = ''
    base_url: str = ''
    requires_api_key: bool = False
    api_key: Optional[str] = None
    rate_limit_delay: float = 1.0
    max_retries: int = 3
    backoff_multiplier: int = 2
    timeout: int = 30
    output_dir: str = ''
    raw_json_dir: str = ''
    log_dir: str = ''


@dataclass
class CollectorResult:
    """Result of a collection run."""
    success: bool = False
    run_id: str = ''
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_facilities: int = 0
    records_new: int = 0
    records_updated: int = 0
    api_calls_made: int = 0
    errors: List[str] = field(default_factory=list)
    output_file: str = ''


class BaseCollector(ABC):
    """
    Abstract base class for all RLC data collectors.

    Subclasses must implement:
        collect() -> CollectorResult
    """

    def __init__(self, config: CollectorConfig):
        self.config = config
        self.run_id = generate_run_id()
        self.started_at = datetime.now()
        self._api_call_count = 0
        self._api_metadata = []  # Track all API calls for metadata sheet
        self._errors = []

        # Ensure directories exist
        for dir_path in [config.output_dir, config.raw_json_dir, config.log_dir]:
            if dir_path:
                Path(dir_path).mkdir(parents=True, exist_ok=True)

        # Set up logging
        self.logger = setup_collector_logger(
            name=config.collector_name,
            log_dir=config.log_dir,
            run_id=self.run_id,
        )

        # HTTP session
        self.session = requests.Session()
        self._last_request_time = 0

    # =========================================================================
    # HTTP REQUEST METHODS
    # =========================================================================

    def _make_request(self, url, params=None, method='GET'):
        """
        Make an HTTP request with rate limiting and retry/backoff.

        Args:
            url: Request URL
            params: Query parameters
            method: HTTP method

        Returns:
            (response, error_message) tuple. Response is None on failure.
        """
        params = params or {}

        for attempt in range(self.config.max_retries):
            # Rate limiting
            self._respect_rate_limit()

            start_time = time.time()
            try:
                response = self.session.request(
                    method, url, params=params,
                    timeout=self.config.timeout
                )
                duration = time.time() - start_time
                self._api_call_count += 1

                # Calculate response hash
                response_hash = self._hash_response(response.text)

                # Log the API call
                log_details = {
                    'description': '{} {} ({:.1f}s)'.format(
                        method, url.split('/')[-1], duration),
                    'source_type': 'API',
                    'source_endpoint': url,
                    'source_params': params,
                    'http_status': response.status_code,
                    'response_size_bytes': len(response.text),
                    'response_hash': response_hash,
                }
                make_log_record(
                    self.logger, logging.INFO, LogAction.API_CALL,
                    log_details, duration_seconds=round(duration, 2)
                )

                # Track for metadata sheet
                self._api_metadata.append({
                    'timestamp': datetime.now().isoformat(),
                    'endpoint': url,
                    'parameters': json.dumps(params),
                    'http_status': response.status_code,
                    'records_returned': None,  # Set by caller
                    'response_time_seconds': round(duration, 2),
                    'response_hash': response_hash,
                })

                # Handle rate limiting
                if response.status_code == 429:
                    wait = self.config.rate_limit_delay * (
                        self.config.backoff_multiplier ** attempt)
                    make_log_record(
                        self.logger, logging.WARNING, LogAction.API_CALL,
                        {'description': 'Rate limited (429), waiting {:.0f}s'.format(wait)},
                    )
                    time.sleep(wait)
                    continue

                # Handle server errors
                if response.status_code >= 500:
                    wait = self.config.rate_limit_delay * (
                        self.config.backoff_multiplier ** attempt)
                    make_log_record(
                        self.logger, logging.WARNING, LogAction.API_CALL,
                        {'description': 'Server error ({}), waiting {:.0f}s'.format(
                            response.status_code, wait)},
                    )
                    time.sleep(wait)
                    continue

                return response, None

            except requests.exceptions.Timeout:
                duration = time.time() - start_time
                wait = self.config.rate_limit_delay * (
                    self.config.backoff_multiplier ** attempt)
                make_log_record(
                    self.logger, logging.WARNING, LogAction.ERROR,
                    {'description': 'Request timeout after {:.1f}s, retry {}/{}'.format(
                        duration, attempt + 1, self.config.max_retries)},
                )
                time.sleep(wait)

            except requests.exceptions.ConnectionError as e:
                wait = self.config.rate_limit_delay * (
                    self.config.backoff_multiplier ** attempt)
                make_log_record(
                    self.logger, logging.WARNING, LogAction.ERROR,
                    {'description': 'Connection error: {}, retry {}/{}'.format(
                        str(e)[:200], attempt + 1, self.config.max_retries)},
                )
                # Refresh session on connection errors
                try:
                    self.session.close()
                except Exception:
                    pass
                self.session = requests.Session()
                time.sleep(wait)

            except Exception as e:
                error_msg = 'Unexpected error: {}'.format(str(e)[:300])
                make_log_record(
                    self.logger, logging.ERROR, LogAction.ERROR,
                    {'description': error_msg, 'error_message': str(e)},
                )
                self._errors.append(error_msg)
                return None, error_msg

        error_msg = 'Max retries ({}) exceeded for {}'.format(
            self.config.max_retries, url)
        self._errors.append(error_msg)
        make_log_record(
            self.logger, logging.ERROR, LogAction.ERROR,
            {'description': error_msg},
        )
        return None, error_msg

    def _respect_rate_limit(self):
        """Enforce minimum delay between requests."""
        if self._last_request_time:
            elapsed = time.time() - self._last_request_time
            if elapsed < self.config.rate_limit_delay:
                time.sleep(self.config.rate_limit_delay - elapsed)
        self._last_request_time = time.time()

    @staticmethod
    def _hash_response(text):
        """SHA-256 hash of response body."""
        return 'sha256:' + hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]

    # =========================================================================
    # RAW RESPONSE ARCHIVING
    # =========================================================================

    def _save_raw_response(self, endpoint_name, identifier, data):
        """
        Save raw API response to JSON file.

        Args:
            endpoint_name: Short name for the endpoint (e.g., 'air_facilities')
            identifier: Distinguishing label (e.g., NAICS code or registry ID)
            data: Response data (dict, list, or string)
        """
        if not self.config.raw_json_dir:
            return

        raw_dir = Path(self.config.raw_json_dir)
        raw_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = '{}_{}_{}.json'.format(endpoint_name, identifier, timestamp)
        filepath = raw_dir / filename

        with open(str(filepath), 'w', encoding='utf-8') as f:
            if isinstance(data, str):
                f.write(data)
            else:
                json.dump(data, f, indent=2, default=str)

    # =========================================================================
    # EXCEL OUTPUT
    # =========================================================================

    def _create_excel_workbook(self, sheets, output_path):
        """
        Create a multi-sheet Excel workbook.

        Args:
            sheets: Dict of {sheet_name: pd.DataFrame}
            output_path: Path to save the Excel file

        Returns:
            Path to the saved file
        """
        if not OPENPYXL_AVAILABLE or not PANDAS_AVAILABLE:
            raise ImportError('openpyxl and pandas required for Excel output')

        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(str(output_path), engine='openpyxl') as writer:
            for sheet_name, df in sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

                # Auto-fit column widths
                ws = writer.sheets[sheet_name]
                for col_idx, col_name in enumerate(df.columns, 1):
                    max_len = max(
                        len(str(col_name)),
                        df[col_name].astype(str).str.len().max() if len(df) > 0 else 0
                    )
                    # Cap at 50 characters
                    adjusted_width = min(max_len + 2, 50)
                    ws.column_dimensions[get_column_letter(col_idx)].width = adjusted_width

                # Freeze header row
                ws.freeze_panes = 'A2'

        return str(output_path)

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def run(self):
        """
        Execute the full collector lifecycle.

        STARTUP -> collect() -> SHUTDOWN
        """
        # Log startup
        make_log_record(
            self.logger, logging.INFO, LogAction.STARTUP,
            {
                'description': 'Starting {} v{} (run: {})'.format(
                    self.config.collector_name,
                    self.config.collector_version,
                    self.run_id),
                'python_version': sys.version.split()[0],
                'collector_version': self.config.collector_version,
                'config': {
                    'base_url': self.config.base_url,
                    'rate_limit_delay': self.config.rate_limit_delay,
                    'max_retries': self.config.max_retries,
                    'timeout': self.config.timeout,
                },
            }
        )

        # Run collection
        try:
            result = self.collect()
        except Exception as e:
            make_log_record(
                self.logger, logging.CRITICAL, LogAction.ERROR,
                {
                    'description': 'Fatal error: {}'.format(str(e)),
                    'error_message': str(e),
                },
            )
            result = CollectorResult(
                success=False,
                run_id=self.run_id,
                started_at=self.started_at,
                completed_at=datetime.now(),
                errors=[str(e)],
            )

        # Log shutdown
        result.run_id = self.run_id
        result.started_at = self.started_at
        result.completed_at = datetime.now()
        result.api_calls_made = self._api_call_count

        duration = (result.completed_at - self.started_at).total_seconds()
        status = 'SUCCESS' if result.success else (
            'PARTIAL_SUCCESS' if result.total_facilities > 0 else 'FAILURE')

        make_log_record(
            self.logger, logging.INFO, LogAction.SHUTDOWN,
            {
                'description': 'COMPLETE: {} unique facilities, {} API calls, {:.0f}s runtime — {}'.format(
                    result.total_facilities, self._api_call_count,
                    duration, status),
                'total_facilities': result.total_facilities,
                'records_new': result.records_new,
                'api_calls_made': self._api_call_count,
                'errors_count': len(result.errors),
                'runtime_seconds': round(duration, 1),
                'status': status,
                'output_file': result.output_file,
            },
            duration_seconds=round(duration, 1),
        )

        # Close session
        try:
            self.session.close()
        except Exception:
            pass

        return result

    @abstractmethod
    def collect(self):
        """
        Main collection logic. Must be implemented by subclasses.

        Returns:
            CollectorResult with collection outcomes
        """
        raise NotImplementedError
