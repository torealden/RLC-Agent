"""
EPA ECHO Soybean/Oilseed Processor Data Collector

Retrieves facility-level data on US soybean and oilseed crushing plants
from the EPA's ECHO (Enforcement and Compliance History Online) database.

The ECHO API is fully public — no API key required.

Query Strategy:
  1. Search by SIC codes (2075, 2076) via air_rest_services — reliable NAICS
     filtering is not supported by the API, but SIC filtering works.
  2. For each SIC code, get_facilities returns a QueryID + summary.
  3. Use get_download with the QueryID to retrieve CSV facility data.
  4. For each unique facility, call the DFR endpoint for full detail.
  5. Deduplicate by FRS Registry ID across all searches.

Usage:
    python epa_echo_collector.py
    python epa_echo_collector.py --profile soybean_oilseed
    python epa_echo_collector.py --profile ethanol
    python epa_echo_collector.py --test  # Single-facility test mode
"""

import argparse
import csv
import io
import json
import logging
import random
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print('ERROR: pandas required. pip install pandas')
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from collectors.base.base_collector import BaseCollector, CollectorConfig, CollectorResult
from collectors.base.logging_utils import LogAction, make_log_record


class EPAEchoCollector(BaseCollector):
    """
    Collector for EPA ECHO facility data.

    Designed for reuse across facility types — the search_profile parameter
    selects which SIC/NAICS codes to query. Default: soybean_oilseed.
    """

    def __init__(self, profile_name='soybean_oilseed', config_path=None):
        # Load config
        if config_path is None:
            config_path = Path(__file__).parent / 'config.json'
        with open(str(config_path), 'r') as f:
            self.full_config = json.load(f)

        self.profile_name = profile_name
        self.profile = self.full_config['search_profiles'][profile_name]

        api_cfg = self.full_config['api']
        paths = self.full_config['paths']

        # Resolve paths relative to project root
        project_root = Path(__file__).parent.parent.parent

        config = CollectorConfig(
            collector_name=self.full_config['collector_name'],
            collector_version=self.full_config['collector_version'],
            description=self.profile['description'],
            base_url=api_cfg['base_url'],
            requires_api_key=api_cfg['requires_api_key'],
            rate_limit_delay=api_cfg['rate_limit_delay_seconds'],
            max_retries=api_cfg['max_retries'],
            backoff_multiplier=api_cfg['backoff_multiplier'],
            timeout=api_cfg['timeout_seconds'],
            output_dir=str(project_root / paths['output_dir']),
            raw_json_dir=str(project_root / paths['raw_json_dir']),
            log_dir=str(project_root / paths['collector_log_dir']),
        )

        super().__init__(config)

        # Set a reasonable User-Agent so EPA doesn't flag us as a bot
        self.session.headers.update({
            'User-Agent': 'RLC-Agent/1.0 (Agricultural Research; contact@rlcag.com)',
        })

        self.base_url = api_cfg['base_url']
        self.endpoints = api_cfg['endpoints']
        self.download_columns = self.full_config.get('download_columns', '')

        # DFR calls need slower pacing to avoid EPA's robotic query detection
        # Search/download calls are fine at 1s (only a few per run)
        self._dfr_delay = 3.0       # Base delay between DFR calls
        self._dfr_jitter = 1.5      # Random jitter added to delay

        # Track all facilities found across searches
        self._all_facilities = {}   # registry_id -> facility dict
        self._search_coverage = []  # Track which search found each facility

    # =========================================================================
    # API QUERY METHODS
    # =========================================================================

    def _search_by_sic(self, sic_code):
        """
        Search air facilities by SIC code.

        Two-step pattern:
          1. get_facilities → QueryID + row count
          2. get_download → CSV facility data

        Args:
            sic_code: SIC code string (e.g., '2075')

        Returns:
            List of facility dicts parsed from CSV
        """
        search_url = self.base_url + self.endpoints['air_facility_search']
        download_url = self.base_url + self.endpoints['air_download']

        # Step 1: Get QueryID
        params = {
            'output': 'JSON',
            'p_sic': sic_code,
        }

        make_log_record(
            self.logger, logging.INFO, LogAction.API_CALL,
            {'description': 'Searching Air Facilities: SIC {} ({})...'.format(
                sic_code, self.profile.get('sic_descriptions', {}).get(sic_code, ''))},
        )

        response, error = self._make_request(search_url, params)
        if error or response is None:
            make_log_record(
                self.logger, logging.ERROR, LogAction.ERROR,
                {'description': 'Search failed for SIC {}: {}'.format(sic_code, error)},
            )
            return []

        try:
            data = response.json()
        except Exception as e:
            make_log_record(
                self.logger, logging.ERROR, LogAction.ERROR,
                {'description': 'JSON parse error for SIC {}: {}'.format(sic_code, e)},
            )
            return []

        results = data.get('Results', {})

        # Check for API error
        if 'Error' in results:
            make_log_record(
                self.logger, logging.ERROR, LogAction.ERROR,
                {'description': 'API error for SIC {}: {}'.format(sic_code, results['Error'])},
            )
            return []

        query_id = results.get('QueryID')
        query_rows = results.get('QueryRows', 0)

        if isinstance(query_rows, str):
            query_rows = int(query_rows.replace(',', ''))

        make_log_record(
            self.logger, logging.INFO, LogAction.API_CALL,
            {'description': '  -> {} facilities found (QueryID: {})'.format(
                query_rows, query_id)},
        )

        # Save raw response
        self._save_raw_response('air_search', 'sic_{}'.format(sic_code), data)

        if not query_id or query_rows == 0:
            return []

        # Step 2: Download CSV data
        download_params = {
            'output': 'CSV',
            'qid': str(query_id),
        }
        if self.download_columns:
            download_params['qcolumns'] = self.download_columns

        response2, error2 = self._make_request(download_url, download_params)
        if error2 or response2 is None:
            make_log_record(
                self.logger, logging.ERROR, LogAction.ERROR,
                {'description': 'CSV download failed for SIC {}: {}'.format(sic_code, error2)},
            )
            return []

        # Parse CSV
        facilities = []
        try:
            reader = csv.DictReader(io.StringIO(response2.text))
            for row in reader:
                # Clean up whitespace in all values
                cleaned = {}
                for k, v in row.items():
                    cleaned[k.strip()] = v.strip() if v else ''
                facilities.append(cleaned)
        except Exception as e:
            make_log_record(
                self.logger, logging.ERROR, LogAction.ERROR,
                {'description': 'CSV parse error for SIC {}: {}'.format(sic_code, e)},
            )
            return []

        # Save raw CSV
        self._save_raw_response('air_download', 'sic_{}'.format(sic_code), response2.text)

        # Update last metadata entry with record count
        if self._api_metadata:
            self._api_metadata[-1]['records_returned'] = len(facilities)

        make_log_record(
            self.logger, logging.INFO, LogAction.API_CALL,
            {'description': '  -> Downloaded {} facility records'.format(len(facilities))},
        )

        return facilities

    def _get_facility_detail(self, registry_id):
        """
        Get detailed facility report from DFR endpoint.

        Uses slower pacing than search calls to avoid EPA's robotic
        query detection. Retries with exponential backoff if blocked.

        Args:
            registry_id: FRS Registry ID

        Returns:
            Dict with detailed facility data, or None
        """
        url = self.base_url + self.endpoints['detailed_facility_report']
        params = {
            'p_id': str(registry_id),
            'output': 'JSON',
        }

        # DFR-specific delay with jitter (overrides base rate limit)
        delay = self._dfr_delay + random.uniform(0, self._dfr_jitter)
        time.sleep(delay)

        for attempt in range(3):
            response, error = self._make_request(url, params)
            if error or response is None:
                return None

            try:
                data = response.json()
                results = data.get('Results', {})

                # Check for robotic query block
                if 'Error' in results:
                    error_msg = results.get('Error', {})
                    if isinstance(error_msg, dict):
                        error_msg = error_msg.get('ErrorMessage', '')
                    if 'robotic' in str(error_msg).lower():
                        wait = 30 * (2 ** attempt)  # 30s, 60s, 120s
                        make_log_record(
                            self.logger, logging.WARNING, LogAction.ERROR,
                            {'description': 'Robotic query block for {}. '
                             'Waiting {}s before retry {}/3'.format(
                                 registry_id, wait, attempt + 1)},
                        )
                        time.sleep(wait)
                        continue
                    return None

                self._save_raw_response('dfr', registry_id, data)
                return results
            except Exception:
                return None

        make_log_record(
            self.logger, logging.ERROR, LogAction.ERROR,
            {'description': 'DFR blocked after 3 retries for {}'.format(registry_id)},
        )
        return None

    def _fetch_metadata(self):
        """
        Fetch API metadata to generate data dictionary.

        Returns:
            List of field definition dicts
        """
        url = self.base_url + self.endpoints['metadata']
        response, error = self._make_request(url, {'output': 'JSON'})
        if error or response is None:
            return []

        try:
            data = response.json()
            results = data.get('Results', {})
            self._save_raw_response('metadata', 'air_facilities', data)

            # Parse metadata entries
            fields = []
            for entry in results.get('ResultColumns', []):
                fields.append({
                    'column_id': entry.get('ColumnID', ''),
                    'object_name': entry.get('ObjectName', ''),
                    'column_name': entry.get('ColumnName', ''),
                    'description': entry.get('Description', ''),
                    'data_type': entry.get('DataType', ''),
                    'data_length': entry.get('DataLength', ''),
                })

            # Also save as data dictionary
            dict_path = (
                Path(__file__).parent.parent.parent /
                'domain_knowledge' / 'data_dictionaries' /
                'epa_echo_air_facility.json'
            )
            dict_path.parent.mkdir(parents=True, exist_ok=True)
            with open(str(dict_path), 'w', encoding='utf-8') as f:
                json.dump({
                    'source': 'EPA ECHO air_rest_services.metadata',
                    'fetched_at': datetime.now().isoformat(),
                    'fields': fields,
                }, f, indent=2)

            make_log_record(
                self.logger, logging.INFO, LogAction.DATA_SAVE,
                {'description': 'Saved data dictionary ({} fields) to {}'.format(
                    len(fields), str(dict_path))},
            )

            return fields
        except Exception as e:
            make_log_record(
                self.logger, logging.WARNING, LogAction.ERROR,
                {'description': 'Metadata parse error: {}'.format(e)},
            )
            return []

    # =========================================================================
    # DEDUPLICATION
    # =========================================================================

    def _merge_facility(self, facility, sic_code, source='air_facility_search'):
        """
        Merge a facility into the master dict, deduplicating by RegistryID.

        Args:
            facility: Dict of facility fields from CSV
            sic_code: SIC code that found this facility
            source: Search endpoint name
        """
        registry_id = facility.get('RegistryID', '').strip()
        if not registry_id:
            return

        if registry_id not in self._all_facilities:
            # New facility
            self._all_facilities[registry_id] = facility.copy()
            self._all_facilities[registry_id]['_found_by_sic'] = set()
            self._all_facilities[registry_id]['_found_by_naics'] = set()
            self._all_facilities[registry_id]['_found_by_endpoints'] = set()

        fac = self._all_facilities[registry_id]
        fac['_found_by_sic'].add(sic_code)
        fac['_found_by_endpoints'].add(source)

        # Track NAICS codes
        naics = facility.get('AIRNAICS', '')
        if naics:
            for code in naics.split():
                fac['_found_by_naics'].add(code)

        # Track for coverage table
        self._search_coverage.append({
            'registry_id': registry_id,
            'facility_name': facility.get('AIRName', ''),
            'search_type': 'SIC',
            'search_code': sic_code,
            'endpoint': source,
        })

    # =========================================================================
    # DFR ENRICHMENT
    # =========================================================================

    def _enrich_with_dfr(self, facility, registry_id):
        """
        Enrich a facility record with data from the DFR endpoint.

        Extracts cross-reference IDs (TRI, GHG, RCRA, NPDES) and other
        detail not available in the search results.
        """
        dfr = self._get_facility_detail(registry_id)
        if not dfr:
            return

        # Extract permit/program IDs from DFR
        permits = dfr.get('Permits', [])
        permit_ids = {
            'caa_ids': [],
            'npdes_ids': [],
            'rcra_ids': [],
            'sdwa_ids': [],
        }
        for permit in permits:
            if isinstance(permit, dict):
                statute = permit.get('Statute', '')
                permit_id = permit.get('SourceID', permit.get('PermitID', ''))
                if not permit_id:
                    continue
                if statute == 'CAA':
                    permit_ids['caa_ids'].append(permit_id)
                elif statute == 'CWA':
                    permit_ids['npdes_ids'].append(permit_id)
                elif statute == 'RCRA':
                    permit_ids['rcra_ids'].append(permit_id)
                elif statute == 'SDWIS':
                    permit_ids['sdwa_ids'].append(permit_id)

        facility['CAAPermitIDs'] = '; '.join(permit_ids['caa_ids'])
        facility['NPDESPermitIDs'] = '; '.join(permit_ids['npdes_ids'])
        facility['RCRAHandlerIDs'] = '; '.join(permit_ids['rcra_ids'])

        # Extract TRI and GHG IDs from permits
        # TRI uses statute 'EP313', GHG uses NAICS source 'GHGRP'
        tri_id = ''
        for permit in permits:
            if isinstance(permit, dict):
                if permit.get('Statute', '') == 'EP313':
                    tri_id = permit.get('SourceID', '')
                    break
        facility['TRIFacilityID'] = tri_id

        # GHG ID from NAICS sources (GHGRP system)
        ghg_id = ''
        naics_data = dfr.get('NAICS', {})
        if isinstance(naics_data, dict):
            for src in naics_data.get('Sources', []):
                for code_entry in src.get('NAICSCodes', []):
                    if code_entry.get('EPASystem', '') == 'GHGRP':
                        # The SourceID for GHGRP is the GHG reporter ID
                        ghg_id = code_entry.get('SourceID', '')
                        break
                if ghg_id:
                    break
        facility['GHGReporterID'] = ghg_id

        # Extract NAICS codes from DFR (nested: NAICS.Sources[].NAICSCodes[])
        naics_data = dfr.get('NAICS', {})
        if isinstance(naics_data, dict):
            naics_codes = set()
            for src in naics_data.get('Sources', []):
                for code_entry in src.get('NAICSCodes', []):
                    code = code_entry.get('NAICSCode', '')
                    if code:
                        naics_codes.add(code)
            if naics_codes:
                facility['DFR_NAICS'] = ' '.join(sorted(naics_codes))

        # Extract SIC codes from DFR (nested: SIC.Sources[].SICCodes[])
        sic_data = dfr.get('SIC', {})
        if isinstance(sic_data, dict):
            sic_codes = set()
            for src in sic_data.get('Sources', []):
                for code_entry in src.get('SICCodes', []):
                    code = code_entry.get('SICCode', '')
                    if code:
                        sic_codes.add(code)
            if sic_codes:
                facility['DFR_SIC'] = ' '.join(sorted(sic_codes))

        # Compliance summary (nested: ComplianceSummary.Source[])
        compliance = dfr.get('ComplianceSummary', {})
        if isinstance(compliance, dict):
            sources = compliance.get('Source', [])
            statuses = []
            for src in sources:
                if isinstance(src, dict):
                    statute = src.get('Statute', '')
                    snc = src.get('CurrentSNC', '')
                    qtrs = src.get('QtrsInNC', '0')
                    statuses.append('{}: SNC={}, QtrsNC={}'.format(statute, snc, qtrs))
            facility['ComplianceStatus'] = '; '.join(statuses)

        # Enforcement summary (nested: EnforcementComplianceSummaries.Summaries[])
        enforcement = dfr.get('EnforcementComplianceSummaries', {})
        if isinstance(enforcement, dict):
            summaries = enforcement.get('Summaries', [])
            actions = []
            for s in summaries:
                if isinstance(s, dict):
                    statute = s.get('Statute', '')
                    status = s.get('CurrentStatus', '')
                    formal = s.get('FormalActions', '')
                    if status:
                        actions.append('{}: {}'.format(statute, status))
            facility['EnforcementActions'] = '; '.join(actions)

    # =========================================================================
    # OUTPUT BUILDING
    # =========================================================================

    def _build_facilities_df(self):
        """Build the main Facilities DataFrame."""
        rows = []
        now = datetime.now().isoformat()

        for registry_id, fac in self._all_facilities.items():
            rows.append({
                'frs_registry_id': registry_id,
                'facility_name': fac.get('AIRName', ''),
                'street_address': fac.get('AIRStreet', ''),
                'city': fac.get('AIRCity', ''),
                'state': fac.get('AIRState', ''),
                'zip_code': fac.get('AIRZip', ''),
                'county_name': fac.get('AIRCounty', ''),
                'county_fips': fac.get('FacFIPSCode', ''),
                'epa_region': fac.get('AIREPARegion', ''),
                'latitude': fac.get('FacLat', ''),
                'longitude': fac.get('FacLong', ''),
                'sic_codes': fac.get('FacSICCodes', ''),
                'naics_codes': fac.get('AIRNAICS', ''),
                'dfr_naics': fac.get('DFR_NAICS', ''),
                'dfr_sic': fac.get('DFR_SIC', ''),
                'operating_status': fac.get('AIRStatus', ''),
                'air_programs': fac.get('AIRPrograms', ''),
                'air_classification': fac.get('AIRClassification', ''),
                'air_universe': fac.get('AIRUniverse', ''),
                'source_id': fac.get('SourceID', ''),
                'caa_permit_ids': fac.get('CAAPermitIDs', ''),
                'npdes_permit_ids': fac.get('NPDESPermitIDs', ''),
                'rcra_handler_ids': fac.get('RCRAHandlerIDs', ''),
                'tri_facility_id': fac.get('TRIFacilityID', ''),
                'ghg_reporter_id': fac.get('GHGReporterID', ''),
                'compliance_status': fac.get('ComplianceStatus', ''),
                'enforcement_actions': fac.get('EnforcementActions', ''),
                'collected_at': now,
                'source_endpoint': 'air_rest_services',
                'collector_version': self.config.collector_version,
                'run_id': self.run_id,
            })

        return pd.DataFrame(rows)

    def _build_coverage_df(self):
        """Build the Search Coverage DataFrame."""
        # Summarize per facility
        coverage = {}
        for entry in self._search_coverage:
            rid = entry['registry_id']
            if rid not in coverage:
                coverage[rid] = {
                    'frs_registry_id': rid,
                    'facility_name': entry['facility_name'],
                    'found_by_sic': set(),
                    'found_by_endpoints': set(),
                }
            coverage[rid]['found_by_sic'].add(entry['search_code'])
            coverage[rid]['found_by_endpoints'].add(entry['endpoint'])

        rows = []
        for rid, info in coverage.items():
            fac = self._all_facilities.get(rid, {})
            rows.append({
                'frs_registry_id': rid,
                'facility_name': info['facility_name'],
                'found_by_sic': ', '.join(sorted(info['found_by_sic'])),
                'naics_codes': fac.get('AIRNAICS', ''),
                'found_by_endpoints': ', '.join(sorted(info['found_by_endpoints'])),
            })

        return pd.DataFrame(rows)

    def _build_metadata_df(self):
        """Build the API Metadata DataFrame."""
        return pd.DataFrame(self._api_metadata)

    # =========================================================================
    # MAIN COLLECTION LOGIC
    # =========================================================================

    def collect(self):
        """
        Main collection workflow.

        1. Fetch API metadata → data dictionary
        2. Search by each SIC code
        3. Deduplicate by FRS Registry ID
        4. Enrich each facility with DFR detail
        5. Build and save Excel workbook
        """
        result = CollectorResult()

        # 1. Fetch metadata / data dictionary
        make_log_record(
            self.logger, logging.INFO, LogAction.API_CALL,
            {'description': 'Fetching API metadata for data dictionary...'},
        )
        dict_fields = self._fetch_metadata()

        # 2. Search by SIC codes
        sic_codes = self.profile.get('sic_codes', [])
        for sic_code in sic_codes:
            facilities = self._search_by_sic(sic_code)
            for fac in facilities:
                self._merge_facility(fac, sic_code)

        unique_count = len(self._all_facilities)
        make_log_record(
            self.logger, logging.INFO, LogAction.VALIDATION,
            {'description': '{} unique facilities after deduplication (from {} total search results)'.format(
                unique_count, len(self._search_coverage))},
        )

        if unique_count == 0:
            result.success = False
            result.errors.append('No facilities found')
            return result

        # 3. Enrich with DFR detail
        total = len(self._all_facilities)
        make_log_record(
            self.logger, logging.INFO, LogAction.API_CALL,
            {'description': 'Fetching Detailed Facility Reports: 0/{} complete'.format(total)},
        )

        for i, (registry_id, facility) in enumerate(self._all_facilities.items()):
            self._enrich_with_dfr(facility, registry_id)

            # Log data save
            make_log_record(
                self.logger, logging.INFO, LogAction.DATA_SAVE,
                {
                    'description': 'Saved: {} ({})'.format(
                        facility.get('AIRName', ''), facility.get('AIRState', '')),
                    'affected_record_ids': [registry_id],
                    'facility_name': facility.get('AIRName', ''),
                    'source_endpoint': self.base_url + self.endpoints['detailed_facility_report'],
                    'verification_url': 'https://echo.epa.gov/detailed-facility-report?fid={}'.format(
                        registry_id),
                    'new_values': {
                        'AIRName': facility.get('AIRName', ''),
                        'AIRStatus': facility.get('AIRStatus', ''),
                        'FacLat': facility.get('FacLat', ''),
                        'FacLong': facility.get('FacLong', ''),
                    },
                },
            )

            # Progress logging
            if (i + 1) % 10 == 0 or (i + 1) == total:
                make_log_record(
                    self.logger, logging.INFO, LogAction.API_CALL,
                    {'description': 'Fetching Detailed Facility Reports: {}/{} complete'.format(
                        i + 1, total)},
                )

        # 4. Validate
        for rid, fac in self._all_facilities.items():
            lat = fac.get('FacLat', '')
            lon = fac.get('FacLong', '')
            if not lat or not lon:
                make_log_record(
                    self.logger, logging.WARNING, LogAction.VALIDATION,
                    {'description': 'Missing lat/long: {} ({})'.format(
                        fac.get('AIRName', ''), rid)},
                )

            sic = fac.get('FacSICCodes', '')
            naics = fac.get('AIRNAICS', '')
            if not sic and not naics:
                make_log_record(
                    self.logger, logging.WARNING, LogAction.VALIDATION,
                    {'description': 'Missing both SIC and NAICS: {} ({})'.format(
                        fac.get('AIRName', ''), rid)},
                )

        # 5. Build Excel output
        facilities_df = self._build_facilities_df()
        coverage_df = self._build_coverage_df()
        metadata_df = self._build_metadata_df()
        dictionary_df = pd.DataFrame(dict_fields) if dict_fields else pd.DataFrame()

        date_str = datetime.now().strftime('%Y-%m-%d')
        filename = 'epa_echo_{}_facilities_{}.xlsx'.format(
            self.profile_name, date_str)
        output_path = Path(self.config.output_dir) / filename

        sheets = {
            'Facilities': facilities_df,
            'Search Coverage': coverage_df,
            'API Metadata': metadata_df,
            'Data Dictionary': dictionary_df,
        }

        saved_path = self._create_excel_workbook(sheets, output_path)

        make_log_record(
            self.logger, logging.INFO, LogAction.DATA_SAVE,
            {'description': 'Output: {}'.format(saved_path)},
        )

        result.success = True
        result.total_facilities = unique_count
        result.records_new = unique_count
        result.output_file = saved_path
        result.errors = self._errors

        return result


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='EPA ECHO Facility Data Collector'
    )
    parser.add_argument(
        '--profile', '-p',
        default='soybean_oilseed',
        help='Search profile (default: soybean_oilseed). '
             'Options: soybean_oilseed, ethanol, biodiesel_renewable_diesel, wheat_milling'
    )
    parser.add_argument(
        '--test', '-t',
        action='store_true',
        help='Test mode: search for a single known facility (Cargill, Sidney OH)'
    )

    args = parser.parse_args()

    if args.test:
        print('=== TEST MODE: Single facility search ===')
        # Quick test with Cargill Sidney OH
        import requests as req
        base = 'https://echodata.epa.gov/echo/'

        # Search
        r = req.get(base + 'air_rest_services.get_facilities', params={
            'output': 'JSON', 'p_fn': 'CARGILL', 'p_st': 'OH', 'p_sic': '2075',
        }, timeout=30)
        data = r.json().get('Results', {})
        qid = data.get('QueryID')
        qrows = data.get('QueryRows', 0)
        print('Search: {} results, QueryID={}'.format(qrows, qid))

        if qid:
            r2 = req.get(base + 'air_rest_services.get_download', params={
                'output': 'CSV', 'qid': str(qid),
                'qcolumns': '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30',
            }, timeout=30)
            reader_rows = list(csv.DictReader(io.StringIO(r2.text)))
            for row in reader_rows:
                print('  {} | {} {} | SIC={} | NAICS={} | Status={}'.format(
                    row.get('AIRName', ''), row.get('AIRCity', ''),
                    row.get('AIRState', ''), row.get('FacSICCodes', ''),
                    row.get('AIRNAICS', ''), row.get('AIRStatus', '')))

        # Get RegistryID from CSV for DFR test
        registry_id = None
        for row in reader_rows:
            registry_id = row.get('RegistryID', '').strip()
            if registry_id:
                break

        print()
        print('DFR test (RegistryID: {}):'.format(registry_id))
        if not registry_id:
            print('  No RegistryID in CSV — skipping DFR test')
            return
        r3 = req.get(base + 'dfr_rest_services.get_dfr', params={
            'p_id': registry_id, 'output': 'JSON',
        }, timeout=30)
        dfr = r3.json().get('Results', {})

        # NAICS (nested)
        naics_data = dfr.get('NAICS', {})
        if isinstance(naics_data, dict):
            for src in naics_data.get('Sources', []):
                for c in src.get('NAICSCodes', []):
                    print('  NAICS: {} - {} ({})'.format(
                        c.get('NAICSCode', ''), c.get('NAICSDesc', ''),
                        c.get('EPASystem', '')))

        # SIC (nested)
        sic_data = dfr.get('SIC', {})
        if isinstance(sic_data, dict):
            for src in sic_data.get('Sources', []):
                for c in src.get('SICCodes', []):
                    print('  SIC: {} - {} ({})'.format(
                        c.get('SICCode', ''), c.get('SICDesc', ''),
                        c.get('EPASystem', '')))

        permits = dfr.get('Permits', [])
        print('  Permits: {} entries'.format(len(permits)))
        for p in permits:
            if isinstance(p, dict) and p.get('Statute', ''):
                print('    {} - {} ({})'.format(
                    p.get('Statute', ''), p.get('SourceID', ''),
                    p.get('FacilityName', '')))
        return

    # Full collection run
    collector = EPAEchoCollector(profile_name=args.profile)
    result = collector.run()

    if result.success:
        print()
        print('Output: {}'.format(result.output_file))
        print('Log: {}'.format(collector.logger.log_file_path))
    else:
        print()
        print('Collection failed. Errors:')
        for err in result.errors:
            print('  - {}'.format(err))
        sys.exit(1)


if __name__ == '__main__':
    main()
