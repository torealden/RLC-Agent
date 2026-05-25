"""
EPA ECHO Enrich-by-FRS Collector

Phase 2 of the architecture flip (Task #66). Replaces the four SIC-sweep
collectors (epa_echo_{oilseed,ethanol,biodiesel,milling}) that each do
1,000-1,600-facility daily sweeps with ~80% false positives and 9 hours
of total daily API time.

This collector:
  1. Reads the curated facility list from gold.facility_map_with_frs
     (silver.facility_map LEFT JOIN silver.facility_frs_xref).
  2. For each row that has a frs_registry_id, calls EPA ECHO's DFR endpoint
     directly. No SIC sweep, no false positives.
  3. Upserts the result into bronze.epa_echo_facility (existing table).
  4. Records any change to operating_status / compliance_status /
     enforcement_actions in bronze.epa_echo_facility_audit (state-change log).

Expected runtime: ~30 minutes (2,000 facilities × ~1s throttle) vs
9 hours for the four old collectors combined.

Wiring: register as 'epa_echo_enrich_by_frs' in collector_registry,
schedule once daily. The four old SIC-sweep entries should be disabled
once this is proven reliable (left in place for now so we can compare).
"""

from __future__ import annotations

import json
import logging
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from src.agents.base.base_collector import (
    BaseCollector, CollectorConfig, CollectorResult, DataFrequency, AuthType,
)
from src.services.database.db_config import get_connection

logger = logging.getLogger(__name__)


ECHO_BASE = "https://echodata.epa.gov/echo"
DFR_PATH = "/dfr_rest_services.get_dfr"

# Fields the audit table watches. Only inserted into the audit log when the
# value changes vs the prior row in bronze.epa_echo_facility.
AUDITED_FIELDS = (
    'operating_status',
    'compliance_status',
    'enforcement_actions',
    'air_classification',
)


# -----------------------------------------------------------------------------
# DFR helpers
# -----------------------------------------------------------------------------

def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    return s


def _parse_dfr(dfr_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Turn the EPA DFR JSON 'Results' block into the column dict that
    bronze.epa_echo_facility expects.

    Key paths confirmed against 5 live samples 2026-05-25:
    - Top-level facility identity (name/address/lat/lon) lives in the
      first Permits[] entry (EPASystem='FRS'). NOT in any top-level
      AIRName/AIRStreet/etc field — that's the AIR search endpoint shape,
      not DFR.
    - Operating status lives on the Permits[] entry where
      Statute='CAA' AND EPASystem='ICIS-Air'. The GHGRP permit has its
      own FacilityStatus but it's a reporting-year description, not a
      site operating status.
    """
    out: Dict[str, Any] = {}
    permits = dfr_results.get('Permits') or []

    # Pick the FRS root permit for facility identity, and the CAA/ICIS-Air
    # permit for operating status.
    frs_permit: Dict[str, Any] = next(
        (p for p in permits if isinstance(p, dict) and p.get('EPASystem') == 'FRS'),
        permits[0] if permits and isinstance(permits[0], dict) else {},
    )
    air_permit: Dict[str, Any] = next(
        (p for p in permits if isinstance(p, dict)
         and p.get('Statute') == 'CAA' and p.get('EPASystem') == 'ICIS-Air'),
        {},
    )

    out['facility_name']      = frs_permit.get('FacilityName', '')
    out['street_address']     = frs_permit.get('FacilityStreet', '')
    out['city']               = frs_permit.get('FacilityCity', '')
    out['state']              = frs_permit.get('FacilityState', '')
    out['zip_code']           = frs_permit.get('FacilityZip', '')
    out['county_name']        = frs_permit.get('FacilityCountyName', '')
    out['county_fips']        = frs_permit.get('FacilityFipsCode', '')
    out['epa_region']         = frs_permit.get('EPARegion', '')
    # Lat/lon: prefer SpatialMetadata (NAD83), fall back to first permit
    spatial = dfr_results.get('SpatialMetadata') or {}
    out['latitude']           = spatial.get('Latitude83') or frs_permit.get('Latitude')
    out['longitude']          = spatial.get('Longitude83') or frs_permit.get('Longitude')
    out['operating_status']   = air_permit.get('FacilityStatus', '')
    out['air_universe']       = air_permit.get('Universe', '')
    # air_programs and air_classification do NOT come from DFR — they're only
    # in the AIR search endpoint. Omit them from the parsed dict so the
    # upsert preserves whatever the prior SIC-sweep collector wrote.

    # Permits
    permits = dfr_results.get('Permits', []) or []
    caa, npdes, rcra, sdwa = [], [], [], []
    tri_id = ''
    for p in permits:
        if not isinstance(p, dict):
            continue
        statute = p.get('Statute', '')
        permit_id = p.get('SourceID') or p.get('PermitID') or ''
        if not permit_id:
            continue
        if statute == 'CAA': caa.append(permit_id)
        elif statute == 'CWA': npdes.append(permit_id)
        elif statute == 'RCRA': rcra.append(permit_id)
        elif statute == 'SDWIS': sdwa.append(permit_id)
        elif statute == 'EP313' and not tri_id:
            tri_id = permit_id
    out['caa_permit_ids'] = '; '.join(caa)
    out['npdes_permit_ids'] = '; '.join(npdes)
    out['rcra_handler_ids'] = '; '.join(rcra)
    out['tri_facility_id'] = tri_id

    # NAICS / SIC from DFR (nested)
    naics_data = dfr_results.get('NAICS', {}) or {}
    naics_codes = set()
    ghg_id = ''
    for src in (naics_data.get('Sources') or []):
        for ce in (src.get('NAICSCodes') or []):
            code = ce.get('NAICSCode', '')
            if code:
                naics_codes.add(code)
            if ce.get('EPASystem') == 'GHGRP' and not ghg_id:
                ghg_id = ce.get('SourceID', '')
    out['dfr_naics'] = ' '.join(sorted(naics_codes))
    out['ghg_reporter_id'] = ghg_id

    sic_data = dfr_results.get('SIC', {}) or {}
    sic_codes = set()
    for src in (sic_data.get('Sources') or []):
        for ce in (src.get('SICCodes') or []):
            code = ce.get('SICCode', '')
            if code:
                sic_codes.add(code)
    out['dfr_sic'] = ' '.join(sorted(sic_codes))

    # Compliance summary
    statuses: List[str] = []
    cs = dfr_results.get('ComplianceSummary', {}) or {}
    for src in (cs.get('Source') or []):
        if isinstance(src, dict):
            statute = src.get('Statute', '')
            snc = src.get('CurrentSNC', '')
            qtrs = src.get('QtrsInNC', '0')
            statuses.append(f"{statute}: SNC={snc}, QtrsNC={qtrs}")
    out['compliance_status'] = '; '.join(statuses)

    # Enforcement
    actions: List[str] = []
    es = dfr_results.get('EnforcementComplianceSummaries', {}) or {}
    for s in (es.get('Summaries') or []):
        if isinstance(s, dict):
            statute = s.get('Statute', '')
            status = s.get('CurrentStatus', '')
            if status:
                actions.append(f"{statute}: {status}")
    out['enforcement_actions'] = '; '.join(actions)

    return out


# -----------------------------------------------------------------------------
# Config + collector
# -----------------------------------------------------------------------------

@dataclass
class EpaEchoEnrichByFrsConfig(CollectorConfig):
    source_name: str = "EPA ECHO (enrich-by-FRS)"
    source_url: str = ECHO_BASE
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.DAILY
    rate_limit_per_minute: int = 60  # 1 call / second nominal
    dfr_delay_sec: float = 1.0
    dfr_jitter_sec: float = 0.5
    timeout: int = 60
    # Optional: limit run to specific industry_codes for testing
    industry_codes: Optional[List[str]] = None
    # Optional: cap rows (for smoke tests)
    max_rows: Optional[int] = None


class EpaEchoEnrichByFrsCollector(BaseCollector):
    """
    Daily FRS-driven enrichment of bronze.epa_echo_facility.

    Replaces the 4 SIC-sweep collectors. Reads FRS IDs from the
    curated facility map, hits DFR directly, upserts + audits.
    """

    def __init__(self, config: Optional[EpaEchoEnrichByFrsConfig] = None):
        config = config or EpaEchoEnrichByFrsConfig()
        super().__init__(config)
        self.config: EpaEchoEnrichByFrsConfig = config
        self.session = _make_session()
        self.run_id = f"echo_frs_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"

    def get_table_name(self) -> str:
        return "epa_echo_facility"

    def parse_response(self, raw_data: Any, **kwargs) -> Any:
        return _parse_dfr(raw_data) if raw_data else None

    # --- main pipeline ---

    def _select_facility_list(self) -> List[Tuple[str, str, str]]:
        """Return list of (facility_id, frs_registry_id, industry_code)."""
        sql = """
            SELECT fm.facility_id, x.frs_registry_id, fm.industry_code
            FROM silver.facility_map fm
            JOIN silver.facility_frs_xref x ON x.facility_id = fm.facility_id
            WHERE x.frs_registry_id IS NOT NULL
              AND x.match_confidence >= 0.70
        """
        params: List[Any] = []
        if self.config.industry_codes:
            sql += " AND fm.industry_code = ANY(%s)"
            params.append(self.config.industry_codes)
        sql += " ORDER BY fm.facility_id"
        if self.config.max_rows:
            sql += f" LIMIT {int(self.config.max_rows)}"

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params if params else None)
                rows = cur.fetchall()
        return [(r['facility_id'], r['frs_registry_id'], r.get('industry_code') or '') for r in rows]

    def _fetch_dfr(self, frs_id: str) -> Optional[Dict[str, Any]]:
        """Hit EPA DFR for one FRS ID with throttle + simple retry."""
        url = ECHO_BASE + DFR_PATH
        params = {"p_id": str(frs_id), "output": "JSON"}
        time.sleep(self.config.dfr_delay_sec + random.uniform(0, self.config.dfr_jitter_sec))
        for attempt in range(3):
            try:
                r = self.session.get(url, params=params, timeout=self.config.timeout)
                if r.status_code != 200:
                    self.logger.warning(f"DFR {frs_id} HTTP {r.status_code}, attempt {attempt+1}")
                    time.sleep(5 * (attempt + 1))
                    continue
                data = r.json()
                results = data.get('Results', {})
                if 'Error' in results:
                    err = results['Error']
                    if isinstance(err, dict): err = err.get('ErrorMessage', '')
                    if 'robotic' in str(err).lower():
                        wait = 30 * (2 ** attempt)
                        self.logger.warning(f"DFR {frs_id} robotic-block, waiting {wait}s")
                        time.sleep(wait)
                        continue
                    self.logger.warning(f"DFR {frs_id} returned Error: {err}")
                    return None
                return results
            except Exception as e:
                self.logger.warning(f"DFR {frs_id} exception attempt {attempt+1}: {e}")
                time.sleep(5 * (attempt + 1))
        return None

    def _get_prior_state(self, frs_id: str) -> Dict[str, Any]:
        """Read current bronze row for change detection."""
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT facility_name, state, {', '.join(AUDITED_FIELDS)}
                    FROM bronze.epa_echo_facility WHERE frs_registry_id = %s
                """, (frs_id,))
                r = cur.fetchone()
        return dict(r) if r else {}

    def _upsert_and_audit(self, frs_id: str, parsed: Dict[str, Any], prior: Dict[str, Any]) -> int:
        """Upsert into bronze.epa_echo_facility + write audit rows for changes."""
        cols = list(parsed.keys()) + ['frs_registry_id', 'collected_at']
        vals = [parsed[k] for k in parsed.keys()] + [frs_id, datetime.utcnow()]
        placeholders = ','.join(['%s'] * len(cols))

        # Build ON CONFLICT update clause
        update_set = ', '.join(
            f"{c} = EXCLUDED.{c}" for c in parsed.keys()
        ) + ", collected_at = EXCLUDED.collected_at"

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO bronze.epa_echo_facility ({', '.join(cols)}) "
                    f"VALUES ({placeholders}) "
                    f"ON CONFLICT (frs_registry_id) DO UPDATE SET {update_set}",
                    vals,
                )
                # Write audit rows for any AUDITED_FIELDS that changed
                audit_rows = []
                for f in AUDITED_FIELDS:
                    new_val = parsed.get(f) or ''
                    old_val = prior.get(f) or ''
                    if new_val != old_val:
                        audit_rows.append((
                            frs_id,
                            parsed.get('facility_name') or prior.get('facility_name'),
                            parsed.get('state') or prior.get('state'),
                            f,
                            old_val or None,
                            new_val,
                            self.run_id,
                        ))
                if audit_rows:
                    cur.executemany(
                        "INSERT INTO bronze.epa_echo_facility_audit "
                        "(frs_registry_id, facility_name, state, field, "
                        " previous_value, new_value, collector_run_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        audit_rows,
                    )
            conn.commit()
        return len(audit_rows)

    def fetch_data(self, **kwargs) -> CollectorResult:
        targets = self._select_facility_list()
        self.logger.info(f"[ECHO-FRS] enriching {len(targets)} facilities, run_id={self.run_id}")

        ok = 0
        failed: List[str] = []
        audit_total = 0
        first_seen = 0

        for i, (facility_id, frs_id, _industry) in enumerate(targets, 1):
            if i % 100 == 0:
                self.logger.info(f"[ECHO-FRS] progress {i}/{len(targets)}, ok={ok}, failed={len(failed)}, audited={audit_total}")
            results = self._fetch_dfr(frs_id)
            if not results:
                failed.append(frs_id)
                continue
            parsed = _parse_dfr(results)
            prior = self._get_prior_state(frs_id)
            if not prior:
                first_seen += 1
            audited = self._upsert_and_audit(frs_id, parsed, prior)
            audit_total += audited
            ok += 1

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            collected_at=datetime.utcnow(),
            records_fetched=ok,
            data={
                "ok": ok,
                "failed": failed,
                "audit_rows_written": audit_total,
                "first_seen_facilities": first_seen,
                "total_targets": len(targets),
                "run_id": self.run_id,
            },
            warnings=[f"{len(failed)} DFR failures"] if failed else [],
        )

    def save_to_bronze(self, result: CollectorResult) -> int:
        # Persistence happens inside fetch_data per facility (so audit log
        # stays consistent with the DFR fetch ordering and we don't need
        # to hold 2,000 rows in memory). Just report what was done.
        return (result.data or {}).get("ok", 0)

    def collect(self, **kwargs) -> CollectorResult:
        return self.fetch_data(**kwargs)


def smoke(industry_codes: Optional[List[str]] = None, max_rows: int = 5) -> Dict[str, Any]:
    """End-to-end smoke test on a tiny subset."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = EpaEchoEnrichByFrsConfig(industry_codes=industry_codes, max_rows=max_rows)
    c = EpaEchoEnrichByFrsCollector(config=cfg)
    r = c.collect()
    print(f"success={r.success}, fetched={r.records_fetched}")
    print(f"data={r.data}")
    return r.data


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--smoke", action="store_true")
    p.add_argument("--max-rows", type=int)
    p.add_argument("--industry", action="append", help="Filter to these industry_code values")
    args = p.parse_args()

    if args.smoke:
        smoke(industry_codes=args.industry, max_rows=args.max_rows or 5)
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        cfg = EpaEchoEnrichByFrsConfig(industry_codes=args.industry, max_rows=args.max_rows)
        EpaEchoEnrichByFrsCollector(config=cfg).collect()
