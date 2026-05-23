"""
USITC DataWeb v2 collector.

Backfills US trade flows (exports + imports) at HS-10 monthly granularity
back to 1989, beyond the Census Bureau API's 2013 floor for HS-10 detail.

Source: https://dataweb.usitc.gov (Akamai-fronted Spring Boot API)
Spec:   data/spec_sheets/census_api_endpoints.docx (OpenAPI v0.0.1)
Endpoint: POST /dataweb/api/v2/report2/runReport
Auth:   Authorization: Bearer <USITC_DATAWEB_TOKEN> + X-XSRF-TOKEN header
        (warm GET first to capture the XSRF cookie)

Tagged: source='USITC_DATAWEB' in bronze.census_trade. Attribution required
on republish per USITC terms (reference.data_source_attribution lookup).

Token rotation: JWT expires; calendar reminder set 2026-09-15.

CLI:
    python -m src.agents.collectors.us.usitc_dataweb_collector --year 2012 --hs 1201
    python -m src.agents.collectors.us.usitc_dataweb_collector --backfill 1994-2012
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


BASE_URL = "https://datawebws.usitc.gov/dataweb"
RUN_REPORT_PATH = "/api/v2/report2/runReport"
GLOBAL_VARS_PATH = "/api/v2/query/getGlobalVars"   # warm-up + XSRF capture
COUNTRIES_PATH = "/api/v2/country/getAllCountries"

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Origin": "https://dataweb.usitc.gov",
    "Referer": "https://dataweb.usitc.gov/",
    "Accept": "application/json, text/plain, */*",
}


# -----------------------------------------------------------------------------
# Auth + session helpers
# -----------------------------------------------------------------------------

def _make_session(token: str) -> Tuple[requests.Session, str]:
    """Open a session, do a warm GET, return session + XSRF cookie value."""
    s = requests.Session()
    s.verify = False
    s.headers.update(BROWSER_HEADERS)
    s.headers["Authorization"] = f"Bearer {token}"

    r = s.get(BASE_URL + GLOBAL_VARS_PATH, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"USITC DataWeb warm-up failed ({r.status_code}): {r.text[:200]}")

    xsrf = None
    for c in s.cookies:
        if c.name == "XSRF-TOKEN":
            xsrf = c.value  # last write wins; that matches Spring's expectation
    if not xsrf:
        raise RuntimeError("USITC DataWeb: no XSRF-TOKEN cookie returned")
    return s, xsrf


def _is_maintenance(r: requests.Response) -> bool:
    """USITC's maintenance page returns 503 with the HTML site shell."""
    if r.status_code != 503:
        return False
    return "Site under maintenance" in r.text or "under maintenance" in r.text.lower()


# -----------------------------------------------------------------------------
# Request body builder
# -----------------------------------------------------------------------------

def build_run_report_body(
    *,
    trade_type: str,           # "Export" or "Import"
    hs_codes: List[str],       # e.g., ["1201", "1507", "2304"]
    year: int,
    classification: str = "HTS",
) -> Dict[str, Any]:
    """
    Build a minimal-but-complete runReport payload.

    Returns monthly data for the given year, all partner countries (not
    aggregated), at HS-10 granularity.
    """
    return {
        "tradeType": trade_type,
        "classificationSystem": classification,
        "dataToReport": ["GEN_VAL_MO", "GEN_QY1_MO"],
        "scale": 1,
        "timeframeSelectType": "fullYears",
        "years": [str(year)],
        "startMonth": "01",
        "endMonth": "12",
        "timeline": "Monthly",
        # Commodities — manual list at the requested granularity
        "commoditySelectType": "manual",
        "commoditiesManual": ",".join(hs_codes),
        "commodities": [],
        "commoditiesGroupsSystem": [],
        "commoditiesGroupsUser": [],
        "commoditiesAgg": "false",
        "granularity": 10,
        "searchGranularity": len(hs_codes[0]) if hs_codes else 4,
        "groupGranularity": 10,
        # Countries — all, in detail
        "countries": [],
        "countriesSelectType": "all",
        "countriesAgg": "false",
        "countriesGroupsSystem": [],
        "countriesGroupsUser": [],
        # Programs — n/a for our use case
        "importPrograms": [],
        "importProgramsAgg": "false",
        "programsSelectType": "all",
        "extendedImportPrograms": [],
        "extendedImportProgramsAgg": "false",
        "rateProvisionCodes": [],
        "rateProvisionCodesAgg": "false",
        "provisionCodesSelectType": "all",
        # Districts — n/a
        "districts": [],
        "districtsAgg": "true",
        "districtsSelectType": "all",
        # Output
        "sortOrder": [],
        "columnOrder": [],
        "exportRawData": True,
        "suppressZeroValues": True,
        "displayCommodityList": False,
    }


# -----------------------------------------------------------------------------
# Response parser
# -----------------------------------------------------------------------------

def parse_run_report(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Turn the runReport JSON into flat rows for bronze.census_trade.

    The runReport response shape (subject to verification once API is back from
    maintenance) is roughly:
        {
          "tables": [
            {
              "results": [
                {
                  "commodityNumber": "1201",
                  "commodityDescription": "...",
                  "countryNumber": "5310",
                  "countryName": "Afghanistan",
                  "data": [
                    {"year": "2012", "month": "01", "value": 12345.0, "quantity": 67.8},
                    ...
                  ]
                },
                ...
              ]
            }
          ]
        }
    Returns: list of dicts ready for INSERT into bronze.census_trade with
    source='USITC_DATAWEB'.
    """
    rows: List[Dict[str, Any]] = []
    tables = payload.get("tables") or payload.get("results") or []
    if isinstance(tables, dict):
        tables = [tables]
    for table in tables:
        for entry in table.get("results", []) if isinstance(table, dict) else []:
            hs = str(entry.get("commodityNumber") or entry.get("commodity") or "").strip()
            country_code = str(entry.get("countryISO3") or entry.get("countryCode") or "").strip()
            country_name = str(entry.get("countryName") or "").strip()
            for d in entry.get("data", []):
                year = int(d.get("year") or 0) or None
                month = int(d.get("month") or 0) or None
                if not year or not month:
                    continue
                value = d.get("value")
                qty = d.get("quantity")
                rows.append({
                    "year": year,
                    "month": month,
                    "hs_code": hs,
                    "country_code": country_code,
                    "country_name": country_name,
                    "value_usd": float(value) if value is not None else None,
                    "quantity": float(qty) if qty is not None else None,
                    "source": "USITC_DATAWEB",
                })
    return rows


# -----------------------------------------------------------------------------
# Persistence
# -----------------------------------------------------------------------------

def persist_rows(rows: List[Dict[str, Any]], flow: str) -> int:
    """Insert into bronze.census_trade with source='USITC_DATAWEB'."""
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            tuples = [(
                r["year"], r["month"], flow, r["hs_code"], r["country_code"],
                r["country_name"], r["value_usd"], r["quantity"], r["source"]
            ) for r in rows]
            args = ",".join(
                cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", t).decode("utf-8") for t in tuples
            )
            cur.execute(
                "INSERT INTO bronze.census_trade "
                "(year, month, flow, hs_code, country_code, country_name, "
                " value_usd, quantity, source) VALUES " + args
            )
            inserted = cur.rowcount
        conn.commit()
    return inserted


# -----------------------------------------------------------------------------
# Collector class
# -----------------------------------------------------------------------------

@dataclass
class USITCDataWebConfig(CollectorConfig):
    source_name: str = "USITC_DATAWEB"
    source_url: str = BASE_URL
    auth_type: AuthType = AuthType.API_KEY
    frequency: DataFrequency = DataFrequency.MONTHLY
    rate_limit_per_minute: int = 20
    timeout: int = 180
    # Default HS codes — full set lives in bronze.census_trade; can be overridden
    hs_codes: List[str] = field(default_factory=lambda: ["1201"])
    years: List[int] = field(default_factory=lambda: [2012])
    trade_types: List[str] = field(default_factory=lambda: ["Export", "Import"])


class USITCDataWebCollector(BaseCollector):
    """
    Hits USITC DataWeb runReport for HS-10 monthly trade flows.

    Designed to backfill bronze.census_trade prior to Jan 2013 (Census API floor).
    """

    def __init__(self, config: Optional[USITCDataWebConfig] = None):
        config = config or USITCDataWebConfig()
        super().__init__(config)
        self.config: USITCDataWebConfig = config
        self.token = os.environ.get("USITC_DATAWEB_TOKEN")
        if not self.token:
            raise RuntimeError("USITC_DATAWEB_TOKEN not set in env")

    def get_table_name(self) -> str:
        return "census_trade"

    def parse_response(self, raw_data: Any, **kwargs) -> Any:
        return parse_run_report(raw_data)

    def _post_with_retry(
        self, session: requests.Session, xsrf: str, body: Dict[str, Any],
        max_maintenance_retries: int = 3, retry_sleep_sec: int = 600,
    ) -> Dict[str, Any]:
        post_headers = {"Content-Type": "application/json", "X-XSRF-TOKEN": xsrf}
        url = BASE_URL + RUN_REPORT_PATH

        for attempt in range(max_maintenance_retries):
            r = session.post(url, headers=post_headers, json=body, timeout=self.config.timeout)
            if r.status_code == 200:
                try:
                    return r.json()
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"USITC DataWeb returned 200 but non-JSON body: {e}; preview {r.text[:200]}")
            if _is_maintenance(r):
                logger.warning(f"[USITC] under maintenance — sleeping {retry_sleep_sec}s (attempt {attempt+1}/{max_maintenance_retries})")
                time.sleep(retry_sleep_sec)
                # refresh XSRF for next attempt
                session, xsrf = _make_session(self.token)
                post_headers["X-XSRF-TOKEN"] = xsrf
                continue
            raise RuntimeError(f"USITC DataWeb error {r.status_code}: {r.text[:400]}")

        raise RuntimeError("USITC DataWeb under maintenance for too long; aborting batch")

    def fetch_data(
        self, *, year: int, hs_codes: List[str], trade_type: str, **kwargs
    ) -> CollectorResult:
        session, xsrf = _make_session(self.token)
        body = build_run_report_body(trade_type=trade_type, hs_codes=hs_codes, year=year)
        logger.info(f"[USITC] {trade_type} {year} HS={hs_codes[:3]}{'…' if len(hs_codes)>3 else ''}")
        payload = self._post_with_retry(session, xsrf, body)
        rows = parse_run_report(payload)
        return CollectorResult(
            success=True,
            source=self.config.source_name,
            collected_at=datetime.utcnow(),
            records_fetched=len(rows),
            data={"rows": rows, "trade_type": trade_type, "year": year, "hs_codes": hs_codes},
        )

    def save_to_bronze(self, result: CollectorResult) -> int:
        if not result or not result.data:
            return 0
        rows = result.data.get("rows", [])
        flow = "exports" if result.data["trade_type"] == "Export" else "imports"
        return persist_rows(rows, flow)

    def collect(self, **kwargs) -> CollectorResult:
        year = kwargs.get("year") or (self.config.years[0] if self.config.years else 2012)
        hs_codes = kwargs.get("hs_codes") or self.config.hs_codes
        trade_type = kwargs.get("trade_type") or self.config.trade_types[0]
        result = self.fetch_data(year=year, hs_codes=hs_codes, trade_type=trade_type)
        if result.success and result.records_fetched > 0:
            inserted = self.save_to_bronze(result)
            result.data["rows_persisted"] = inserted
        return result


# -----------------------------------------------------------------------------
# Smoke + CLI
# -----------------------------------------------------------------------------

def smoke() -> Dict[str, Any]:
    """Tiny end-to-end test: HS 1201, 2012, US exports. Returns parsed rows."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    c = USITCDataWebCollector()
    result = c.fetch_data(year=2012, hs_codes=["1201"], trade_type="Export")
    sample = result.data["rows"][:3]
    print(f"Fetched {result.records_fetched} rows; first 3:")
    for r in sample:
        print(f"  {r}")
    return {"count": result.records_fetched, "sample": sample}


def _cli() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2012)
    p.add_argument("--hs", default="1201", help="comma-separated HS prefixes")
    p.add_argument("--trade", choices=["Export", "Import"], default="Export")
    p.add_argument("--backfill", help="year range like 1994-2012 (overrides --year)")
    p.add_argument("--smoke", action="store_true")
    p.add_argument("--no-persist", action="store_true")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.smoke:
        smoke()
        return

    c = USITCDataWebCollector()
    hs_codes = [s.strip() for s in args.hs.split(",")]
    years: List[int] = []
    if args.backfill:
        a, b = args.backfill.split("-")
        years = list(range(int(a), int(b) + 1))
    else:
        years = [args.year]

    total = 0
    for y in years:
        for tt in ("Export", "Import") if args.trade is None else [args.trade]:
            result = c.fetch_data(year=y, hs_codes=hs_codes, trade_type=tt)
            if args.no_persist:
                print(f"  {tt} {y}: {result.records_fetched} rows (not persisted)")
            else:
                ins = c.save_to_bronze(result)
                total += ins
                print(f"  {tt} {y}: fetched {result.records_fetched}, persisted {ins}")
    print(f"Done. Total persisted: {total}")


if __name__ == "__main__":
    _cli()
