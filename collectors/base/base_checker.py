"""
Base Checker Class for RLC Verification Agents

Abstract base class for data verification agents that independently
verify collected data against source APIs.

Pattern:
1. Read the collector's JSON-lines log
2. Extract DATA_SAVE / DATA_UPDATE entries
3. Make fresh API calls to verify saved data
4. Report discrepancies with severity classification
"""

import json
import logging
import random
import sys
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

from .logging_utils import (
    LogAction, generate_run_id, setup_collector_logger, make_log_record
)


@dataclass
class CheckerConfig:
    """Configuration for a verification agent."""
    checker_name: str
    log_dir: str
    mode: str = 'sample'          # 'sample' or 'full'
    sample_percentage: int = 20
    rate_limit_delay: float = 1.0
    timeout: int = 30
    severity_rules: Dict[str, List[str]] = field(default_factory=dict)
    # severity_rules = {'HIGH': ['FacName', ...], 'MEDIUM': [...], 'LOW': [...]}


@dataclass
class CheckerResult:
    """Result of a verification run."""
    success: bool = False
    run_id: str = ''
    collector_run_id: str = ''
    collector_log_file: str = ''
    records_checked: int = 0
    records_matched: int = 0
    records_mismatched: int = 0
    records_source_unavailable: int = 0
    high_severity_count: int = 0
    mismatches: List[Dict] = field(default_factory=list)


class BaseChecker(ABC):
    """
    Abstract base class for verification agents.

    Subclasses must implement:
        check(collector_log_path) -> CheckerResult
        _fetch_fresh_data(record_id) -> Dict
    """

    def __init__(self, config: CheckerConfig):
        self.config = config
        self.run_id = generate_run_id()

        self.logger = setup_collector_logger(
            name=config.checker_name,
            log_dir=config.log_dir,
            run_id=self.run_id,
        )

        self.session = requests.Session()

    # =========================================================================
    # LOG READING
    # =========================================================================

    def _read_collector_log(self, log_path):
        """
        Read a collector's JSON-lines log file.

        Args:
            log_path: Path to the log file

        Returns:
            List of parsed log entry dicts
        """
        entries = []
        with open(str(log_path), 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return entries

    def _extract_verification_targets(self, log_entries):
        """
        Extract DATA_SAVE and DATA_UPDATE entries for verification.

        Args:
            log_entries: List of parsed log entries

        Returns:
            List of dicts with: record_id, facility_name, source_endpoint,
            verification_url, saved_values
        """
        targets = []
        for entry in log_entries:
            action = entry.get('action', '')
            if action not in ('DATA_SAVE', 'DATA_UPDATE'):
                continue

            details = entry.get('details', {})
            record_ids = details.get('affected_record_ids', [])
            for rid in record_ids:
                targets.append({
                    'record_id': rid,
                    'facility_name': details.get('facility_name', ''),
                    'source_endpoint': details.get('source_endpoint', ''),
                    'verification_url': details.get('verification_url', ''),
                    'saved_values': details.get('new_values', {}),
                    'action': action,
                })

        return targets

    def _find_latest_log(self, log_dir, prefix):
        """
        Find the most recent log file matching a prefix.

        Args:
            log_dir: Directory to search
            prefix: Log file name prefix (e.g., 'epa_echo_collector')

        Returns:
            Path to latest log file, or None
        """
        log_dir = Path(log_dir)
        if not log_dir.exists():
            return None

        logs = sorted(
            [f for f in log_dir.glob('{}_*.log'.format(prefix))],
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        return logs[0] if logs else None

    # =========================================================================
    # SAMPLING
    # =========================================================================

    def _select_sample(self, targets):
        """
        Select verification targets based on mode.

        Args:
            targets: Full list of verification targets

        Returns:
            Subset of targets (all if mode='full', random sample if mode='sample')
        """
        if self.config.mode == 'full' or not targets:
            return targets

        sample_size = max(1, int(len(targets) * self.config.sample_percentage / 100))
        return random.sample(targets, min(sample_size, len(targets)))

    # =========================================================================
    # SEVERITY CLASSIFICATION
    # =========================================================================

    def _classify_severity(self, field_name):
        """
        Classify mismatch severity for a given field.

        Args:
            field_name: Name of the mismatched field

        Returns:
            'HIGH', 'MEDIUM', or 'LOW'
        """
        for severity in ('HIGH', 'MEDIUM', 'LOW'):
            if field_name in self.config.severity_rules.get(severity, []):
                return severity
        return 'LOW'

    # =========================================================================
    # VERIFICATION HELPERS
    # =========================================================================

    def _compare_values(self, saved, fresh, fields_to_check=None):
        """
        Compare saved values against fresh values.

        Args:
            saved: Dict of saved field values
            fresh: Dict of fresh field values from source
            fields_to_check: Optional list of fields to compare

        Returns:
            List of mismatch dicts with: field, saved_value, source_value, severity
        """
        mismatches = []
        fields = fields_to_check or set(saved.keys()) | set(fresh.keys())

        for field_name in fields:
            saved_val = str(saved.get(field_name, '')).strip()
            fresh_val = str(fresh.get(field_name, '')).strip()

            if saved_val != fresh_val:
                # Skip comparisons where either side is empty
                # (empty source = API doesn't provide this field)
                if not saved_val or not fresh_val:
                    continue

                mismatches.append({
                    'field': field_name,
                    'saved_value': saved_val,
                    'source_value': fresh_val,
                    'severity': self._classify_severity(field_name),
                })

        return mismatches

    def _make_verification_request(self, url, params=None):
        """
        Make an HTTP request for verification purposes.

        Similar to BaseCollector._make_request but simpler (single attempt
        with one retry).
        """
        try:
            time.sleep(self.config.rate_limit_delay)
            response = self.session.get(
                url, params=params, timeout=self.config.timeout
            )
            if response.status_code == 200:
                return response, None
            return None, 'HTTP {}'.format(response.status_code)
        except requests.exceptions.Timeout:
            return None, 'Timeout'
        except requests.exceptions.ConnectionError as e:
            return None, 'Connection error: {}'.format(str(e)[:200])
        except Exception as e:
            return None, str(e)

    # =========================================================================
    # LIFECYCLE
    # =========================================================================

    def run(self, collector_log_path=None):
        """
        Execute the full checker lifecycle.

        Args:
            collector_log_path: Path to the collector log to verify.
                If None, finds the latest log.
        """
        make_log_record(
            self.logger, logging.INFO, LogAction.VERIFICATION_START,
            {
                'description': 'Starting {} (run: {}, mode: {})'.format(
                    self.config.checker_name, self.run_id, self.config.mode),
                'mode': self.config.mode,
                'sample_percentage': self.config.sample_percentage,
            }
        )

        try:
            result = self.check(collector_log_path)
        except Exception as e:
            make_log_record(
                self.logger, logging.CRITICAL, LogAction.ERROR,
                {'description': 'Fatal checker error: {}'.format(str(e))},
            )
            result = CheckerResult(success=False, run_id=self.run_id)

        result.run_id = self.run_id

        # Summary
        level = logging.WARNING if result.high_severity_count > 0 else logging.INFO
        make_log_record(
            self.logger, level, LogAction.SHUTDOWN,
            {
                'description': 'Checked {}: {} match, {} mismatch ({} HIGH), {} unavailable'.format(
                    result.records_checked, result.records_matched,
                    result.records_mismatched, result.high_severity_count,
                    result.records_source_unavailable),
                'records_checked': result.records_checked,
                'records_matched': result.records_matched,
                'records_mismatched': result.records_mismatched,
                'high_severity_count': result.high_severity_count,
                'records_source_unavailable': result.records_source_unavailable,
            }
        )

        try:
            self.session.close()
        except Exception:
            pass

        return result

    @abstractmethod
    def check(self, collector_log_path=None):
        """
        Main verification logic. Must be implemented by subclasses.

        Args:
            collector_log_path: Path to collector log, or None for latest

        Returns:
            CheckerResult
        """
        raise NotImplementedError

    @abstractmethod
    def _fetch_fresh_data(self, record_id):
        """
        Fetch current data for a facility from the source API.

        Args:
            record_id: Facility identifier (e.g., FRS Registry ID)

        Returns:
            Dict of field values, or None if unavailable
        """
        raise NotImplementedError
