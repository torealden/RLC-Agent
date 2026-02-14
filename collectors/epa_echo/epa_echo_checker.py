"""
EPA ECHO Data Verification Agent

Independently verifies collected EPA ECHO facility data by making fresh
API calls to the DFR endpoint and comparing against saved values.

Two modes:
  - sample (default): Verify a random 20% of collected facilities
  - full: Verify every facility

Usage:
    python epa_echo_checker.py                          # Latest log, sample mode
    python epa_echo_checker.py --mode full              # Latest log, verify all
    python epa_echo_checker.py --collector-log <path>   # Specific log file
"""

import argparse
import json
import logging
import random
import sys
import time
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from collectors.base.base_checker import BaseChecker, CheckerConfig, CheckerResult
from collectors.base.logging_utils import LogAction, make_log_record


class EPAEchoChecker(BaseChecker):
    """
    Verification agent for EPA ECHO facility data.

    Reads the collector's JSON-lines log, extracts DATA_SAVE entries,
    then makes fresh DFR API calls to verify saved facility values.
    """

    def __init__(self, config_path=None):
        if config_path is None:
            config_path = Path(__file__).parent / 'config.json'
        with open(str(config_path), 'r') as f:
            full_config = json.load(f)

        api_cfg = full_config['api']
        paths = full_config['paths']
        checker_cfg = full_config['checker']
        project_root = Path(__file__).parent.parent.parent

        config = CheckerConfig(
            checker_name='epa_echo_checker',
            log_dir=str(project_root / paths['checker_log_dir']),
            mode=checker_cfg.get('default_mode', 'sample'),
            sample_percentage=checker_cfg.get('sample_percentage', 20),
            rate_limit_delay=api_cfg['rate_limit_delay_seconds'],
            timeout=api_cfg['timeout_seconds'],
            severity_rules=checker_cfg.get('mismatch_severity', {}),
        )

        super().__init__(config)

        # Same User-Agent as collector to avoid bot detection
        self.session.headers.update({
            'User-Agent': 'RLC-Agent/1.0 (Agricultural Research; contact@rlcag.com)',
        })

        self.base_url = api_cfg['base_url']
        self.endpoints = api_cfg['endpoints']
        self.collector_log_dir = str(project_root / paths['collector_log_dir'])

        # DFR-specific pacing (same as collector)
        self._dfr_delay = 3.0
        self._dfr_jitter = 1.5

    # =========================================================================
    # FRESH DATA FETCHING
    # =========================================================================

    def _fetch_fresh_data(self, record_id):
        """
        Fetch current facility data from the DFR endpoint.

        DFR structure: facility details are in Permits[0] (the FRS record),
        not at the top level. Name, coordinates, etc. are nested there.

        Args:
            record_id: FRS Registry ID

        Returns:
            Dict of field values for comparison, or None
        """
        url = self.base_url + self.endpoints['detailed_facility_report']
        params = {
            'p_id': str(record_id),
            'output': 'JSON',
        }

        # DFR-specific pacing
        delay = self._dfr_delay + random.uniform(0, self._dfr_jitter)
        time.sleep(delay)

        response, error = self._make_verification_request(url, params)
        if error or response is None:
            return None

        try:
            data = response.json()
            results = data.get('Results', {})

            # Check for robotic query block
            if 'Error' in results:
                make_log_record(
                    self.logger, logging.WARNING, LogAction.ERROR,
                    {'description': 'DFR blocked for {} (robotic query detection)'.format(
                        record_id)},
                )
                return None

            # DFR puts facility info in the Permits array — the FRS entry
            # (Statute='') or the first CAA entry has the canonical name/coords
            permits = results.get('Permits', [])
            fresh = {}

            for permit in permits:
                if not isinstance(permit, dict):
                    continue
                # Use the first permit with a facility name
                fac_name = permit.get('FacilityName', '')
                if fac_name and 'AIRName' not in fresh:
                    fresh['AIRName'] = fac_name
                # Coordinates from any permit entry
                lat = permit.get('Latitude', '')
                lon = permit.get('Longitude', '')
                if lat and 'FacLat' not in fresh:
                    fresh['FacLat'] = str(lat)
                if lon and 'FacLong' not in fresh:
                    fresh['FacLong'] = str(lon)

            return fresh if fresh else None

        except Exception as e:
            make_log_record(
                self.logger, logging.WARNING, LogAction.ERROR,
                {'description': 'DFR parse error for {}: {}'.format(record_id, e)},
            )
            return None

    # =========================================================================
    # MAIN CHECK LOGIC
    # =========================================================================

    def check(self, collector_log_path=None):
        """
        Main verification workflow.

        1. Find/read collector log
        2. Extract DATA_SAVE entries
        3. Select sample (or all)
        4. For each target: fresh DFR call → compare → log result
        5. Return CheckerResult
        """
        result = CheckerResult()

        # 1. Find collector log
        if collector_log_path:
            log_path = Path(collector_log_path)
        else:
            log_path = self._find_latest_log(
                self.collector_log_dir, 'epa_echo_collector'
            )

        if log_path is None or not log_path.exists():
            make_log_record(
                self.logger, logging.ERROR, LogAction.ERROR,
                {'description': 'No collector log found in {}'.format(
                    self.collector_log_dir)},
            )
            return result

        result.collector_log_file = str(log_path)
        make_log_record(
            self.logger, logging.INFO, LogAction.VERIFICATION_START,
            {'description': 'Reading collector log: {}'.format(log_path.name)},
        )

        # 2. Read log and extract targets
        entries = self._read_collector_log(log_path)
        targets = self._extract_verification_targets(entries)

        # Extract collector run_id from log
        for entry in entries:
            rid = entry.get('run_id', '')
            if rid:
                result.collector_run_id = rid
                break

        make_log_record(
            self.logger, logging.INFO, LogAction.VERIFICATION_START,
            {'description': 'Found {} verification targets (collector run: {})'.format(
                len(targets), result.collector_run_id)},
        )

        if not targets:
            make_log_record(
                self.logger, logging.WARNING, LogAction.ERROR,
                {'description': 'No DATA_SAVE/DATA_UPDATE entries in collector log'},
            )
            result.success = True
            return result

        # 3. Select sample
        selected = self._select_sample(targets)
        make_log_record(
            self.logger, logging.INFO, LogAction.VERIFICATION_START,
            {'description': 'Verifying {} of {} facilities (mode: {})'.format(
                len(selected), len(targets), self.config.mode)},
        )

        # 4. Verify each target
        for target in selected:
            record_id = target['record_id']
            facility_name = target.get('facility_name', '')
            saved_values = target.get('saved_values', {})

            make_log_record(
                self.logger, logging.INFO, LogAction.VERIFICATION_RESULT,
                {'description': 'Checking: {} ({})'.format(facility_name, record_id)},
            )

            # Fetch fresh data
            fresh = self._fetch_fresh_data(record_id)

            if fresh is None:
                result.records_source_unavailable += 1
                make_log_record(
                    self.logger, logging.WARNING, LogAction.VERIFICATION_RESULT,
                    {'description': 'Source unavailable: {} ({})'.format(
                        facility_name, record_id)},
                )
                result.records_checked += 1
                continue

            # Compare
            mismatches = self._compare_values(saved_values, fresh)

            result.records_checked += 1

            if not mismatches:
                result.records_matched += 1
                make_log_record(
                    self.logger, logging.INFO, LogAction.VERIFICATION_RESULT,
                    {
                        'description': 'MATCH: {} ({})'.format(
                            facility_name, record_id),
                        'record_id': record_id,
                        'status': 'match',
                    },
                )
            else:
                result.records_mismatched += 1
                high_count = sum(1 for m in mismatches if m['severity'] == 'HIGH')
                result.high_severity_count += high_count

                result.mismatches.append({
                    'record_id': record_id,
                    'facility_name': facility_name,
                    'mismatches': mismatches,
                })

                level = logging.WARNING if high_count > 0 else logging.INFO
                make_log_record(
                    self.logger, level, LogAction.VERIFICATION_RESULT,
                    {
                        'description': 'MISMATCH: {} ({}) — {} fields ({} HIGH)'.format(
                            facility_name, record_id,
                            len(mismatches), high_count),
                        'record_id': record_id,
                        'status': 'mismatch',
                        'mismatches': mismatches,
                    },
                )

        result.success = True
        return result


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='EPA ECHO Data Verification Agent'
    )
    parser.add_argument(
        '--mode', '-m',
        choices=['sample', 'full'],
        default=None,
        help='Verification mode (default: from config, typically "sample")'
    )
    parser.add_argument(
        '--collector-log', '-l',
        default=None,
        help='Path to specific collector log file to verify'
    )
    parser.add_argument(
        '--latest',
        action='store_true',
        help='Verify the most recent collector log (default behavior)'
    )

    args = parser.parse_args()

    checker = EPAEchoChecker()

    # Override mode if specified
    if args.mode:
        checker.config.mode = args.mode

    result = checker.run(collector_log_path=args.collector_log)

    # Print summary
    print()
    print('=== Verification Summary ===')
    print('Checker run:    {}'.format(result.run_id))
    print('Collector run:  {}'.format(result.collector_run_id))
    print('Log verified:   {}'.format(result.collector_log_file))
    print()
    print('Checked:        {}'.format(result.records_checked))
    print('Matched:        {}'.format(result.records_matched))
    print('Mismatched:     {}'.format(result.records_mismatched))
    print('  HIGH severity:{}'.format(result.high_severity_count))
    print('Unavailable:    {}'.format(result.records_source_unavailable))
    print()

    if result.mismatches:
        print('--- Mismatches ---')
        for entry in result.mismatches:
            print('{} ({}):'.format(
                entry['facility_name'], entry['record_id']))
            for m in entry['mismatches']:
                print('  [{}] {}: saved="{}" source="{}"'.format(
                    m['severity'], m['field'],
                    m['saved_value'], m['source_value']))
        print()

    print('Checker log: {}'.format(checker.logger.log_file_path))

    if result.high_severity_count > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
