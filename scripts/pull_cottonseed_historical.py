#!/usr/bin/env python3
"""
Pull Historical Cottonseed Complex Trade Data from US Census Bureau API.

Commodities:
  - COTTONSEED (seed):    HS 1207210000, 1207290000
  - COTTONSEED_OIL:       HS 1512210000, 1512290020, 1512290040
  - COTTONSEED_MEAL:      HS 2306100000

Pulls monthly exports and imports with country-level detail from 2013
(earliest Census API availability) to present. Saves to CSV.

Usage:
    python scripts/pull_cottonseed_historical.py
    python scripts/pull_cottonseed_historical.py --start-year 2013 --save-to-db
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import time as time_module
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3 import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")
api_manager_env = PROJECT_ROOT / "api Manager" / ".env"
if api_manager_env.exists():
    load_dotenv(api_manager_env)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

CENSUS_API_BASE = "https://api.census.gov/data/timeseries/intltrade"

# Output directory
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "cottonseed_historical"


# ---------------------------------------------------------------------------
# Commodity configuration
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CommodityConfig:
    name: str
    group: str
    hs_codes: Tuple[str, ...]
    # KG → target unit multiplier
    kg_to_target: float
    target_unit: str


COMMODITIES: Dict[str, CommodityConfig] = {
    "COTTONSEED": CommodityConfig(
        name="Cottonseed (seed)",
        group="COTTONSEED",
        hs_codes=("1207210000", "1207290000"),
        kg_to_target=1.0 / 907.185,       # KG → Short Tons
        target_unit="Short Tons",
    ),
    "COTTONSEED_OIL": CommodityConfig(
        name="Cottonseed Oil",
        group="COTTONSEED_OIL",
        hs_codes=("1512210000", "1512290020", "1512290040"),
        kg_to_target=2.20462 / 1000,       # KG → 000 Pounds
        target_unit="000 Pounds",
    ),
    "COTTONSEED_MEAL": CommodityConfig(
        name="Cottonseed Meal",
        group="COTTONSEED_MEAL",
        hs_codes=("2306100000",),
        kg_to_target=1.0 / 907.185,        # KG → Short Tons
        target_unit="Short Tons",
    ),
}


# ---------------------------------------------------------------------------
# HTTP session with retries
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ---------------------------------------------------------------------------
# Census API helpers
# ---------------------------------------------------------------------------
def fetch_all_years(
    session: requests.Session,
    commodity: CommodityConfig,
    flow: str,
    start_year: int,
    end_year: int,
    api_key: Optional[str],
) -> List[Dict]:
    """Fetch the full date range of trade data in a single API call per HS code."""
    url = f"{CENSUS_API_BASE}/{flow}/hs"

    if flow == "imports":
        commodity_field = "I_COMMODITY"
        value_field = "GEN_VAL_MO"
        qty_fields = ["GEN_QY1_MO", "CON_QY1_MO"]
    else:
        commodity_field = "E_COMMODITY"
        value_field = "ALL_VAL_MO"
        qty_fields = ["QTY_1_MO", "QTY_2_MO"]

    time_range = f"from {start_year}-01 to {end_year}-12"
    records: List[Dict] = []

    for hs_code in commodity.hs_codes:
        get_fields = f"{value_field},{','.join(qty_fields)},UNIT_QY1,CTY_CODE,CTY_NAME,YEAR,MONTH"
        params = {
            "get": get_fields,
            commodity_field: hs_code,
            "time": time_range,
            "COMM_LVL": "HS10",
        }
        if api_key:
            params["key"] = api_key

        logger.info("  Fetching %s %s %s (%s-%s)...", commodity.group, flow, hs_code, start_year, end_year)

        try:
            resp = session.get(url, params=params, timeout=120)
        except requests.exceptions.RequestException as exc:
            logger.warning("Request error %s %s %s: %s", commodity.group, flow, hs_code, exc)
            continue

        if resp.status_code == 204 or not resp.text.strip():
            logger.info("    No data for %s %s %s", commodity.group, flow, hs_code)
            continue

        if resp.status_code != 200:
            logger.warning("HTTP %d for %s %s %s: %s",
                           resp.status_code, commodity.group, flow, hs_code,
                           resp.text[:200] if resp.text else "")
            if resp.status_code == 429:
                time_module.sleep(10)
            continue

        try:
            raw = resp.json()
        except Exception:
            logger.warning("JSON decode error for %s %s %s", commodity.group, flow, hs_code)
            continue

        if not raw or len(raw) <= 1:
            continue

        headers = raw[0]
        hmap = {name: idx for idx, name in enumerate(headers)}

        for row in raw[1:]:
            # Parse YEAR and MONTH from response
            try:
                rec_year = int(row[hmap["YEAR"]])
                rec_month = int(row[hmap["MONTH"]])
            except (KeyError, ValueError, TypeError):
                continue

            # Extract quantity (try each qty field)
            qty_kg = None
            unit = row[hmap["UNIT_QY1"]] if "UNIT_QY1" in hmap else None
            for qf in qty_fields:
                if qf in hmap:
                    val = row[hmap[qf]]
                    if val and str(val).strip() not in ("", "0", "None", "null"):
                        try:
                            qty_kg = float(val)
                            if qty_kg > 0:
                                break
                        except (TypeError, ValueError):
                            continue

            # Extract value
            value_usd = None
            if value_field in hmap:
                try:
                    value_usd = float(row[hmap[value_field]])
                except (TypeError, ValueError):
                    pass

            # Country info
            cty_code = row[hmap["CTY_CODE"]] if "CTY_CODE" in hmap else ""
            cty_name = row[hmap["CTY_NAME"]] if "CTY_NAME" in hmap else ""

            # Convert quantity
            converted_qty = None
            if qty_kg is not None and qty_kg > 0:
                converted_qty = qty_kg * commodity.kg_to_target

            records.append({
                "commodity_group": commodity.group,
                "hs_code": hs_code,
                "flow": flow,
                "year": rec_year,
                "month": rec_month,
                "country_code": cty_code,
                "country_name": cty_name,
                "quantity_kg": qty_kg,
                "quantity_converted": converted_qty,
                "target_unit": commodity.target_unit,
                "value_usd": value_usd,
                "reported_unit": unit,
            })

        logger.info("    %s %s %s: %d records", commodity.group, flow, hs_code, len([r for r in records if r["hs_code"] == hs_code]))
        # Rate limit between HS codes
        time_module.sleep(1.0)

    return records


# ---------------------------------------------------------------------------
# Main collection loop
# ---------------------------------------------------------------------------
def collect_all(
    start_year: int,
    end_year: int,
    api_key: Optional[str],
    flows: List[str],
    commodities: Optional[Dict[str, CommodityConfig]] = None,
) -> Dict[str, List[Dict]]:
    """Collect all cottonseed complex trade data from Census API."""
    session = build_session()
    results: Dict[str, List[Dict]] = {}
    if commodities is None:
        commodities = COMMODITIES

    for comm_key, commodity in commodities.items():
        logger.info("=" * 60)
        logger.info("Collecting %s (%s)", commodity.name, ", ".join(commodity.hs_codes))
        logger.info("=" * 60)

        all_records: List[Dict] = []

        for flow in flows:
            recs = fetch_all_years(session, commodity, flow, start_year, end_year, api_key)
            all_records.extend(recs)

        years_found = sorted(set(r["year"] for r in all_records)) if all_records else []
        results[comm_key] = all_records
        logger.info("  %s: total %d records collected, years: %s", comm_key, len(all_records),
                     f"{years_found[0]}-{years_found[-1]}" if years_found else "none")

    return results


def save_to_csv(results: Dict[str, List[Dict]], output_dir: Path) -> Dict[str, Path]:
    """Save results to CSV files, one per commodity+flow."""
    output_dir.mkdir(parents=True, exist_ok=True)
    saved_files: Dict[str, Path] = {}

    fieldnames = [
        "commodity_group", "hs_code", "flow", "year", "month",
        "country_code", "country_name",
        "quantity_kg", "quantity_converted", "target_unit",
        "value_usd", "reported_unit",
    ]

    for comm_key, records in results.items():
        if not records:
            logger.warning("No records for %s", comm_key)
            continue

        # Split by flow
        for flow in ("exports", "imports"):
            flow_records = [r for r in records if r["flow"] == flow]
            if not flow_records:
                continue

            # Sort by year, month, country
            flow_records.sort(key=lambda r: (r["year"], r["month"], r["country_name"]))

            filename = f"{comm_key.lower()}_{flow}_historical.csv"
            filepath = output_dir / filename

            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flow_records)

            saved_files[f"{comm_key}_{flow}"] = filepath
            logger.info("  Saved %s: %d records -> %s", f"{comm_key}_{flow}", len(flow_records), filepath)

    return saved_files


def save_to_database(results: Dict[str, List[Dict]]) -> int:
    """Save results to bronze.census_trade table."""
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        logger.error("psycopg2 not installed; skipping database save")
        return 0

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.warning("DATABASE_URL not set; skipping database save")
        return 0

    conn = psycopg2.connect(database_url)
    total_inserted = 0

    try:
        with conn.cursor() as cur:
            for comm_key, records in results.items():
                if not records:
                    continue

                rows = []
                for r in records:
                    rows.append((
                        r["year"],
                        r["month"],
                        r["flow"],
                        r["hs_code"],
                        r["country_code"],
                        r["country_name"],
                        r["value_usd"],
                        r["quantity_kg"],
                        "CENSUS_TRADE",
                    ))

                if rows:
                    execute_values(
                        cur,
                        """INSERT INTO bronze.census_trade
                           (year, month, flow, hs_code, country_code, country_name, value_usd, quantity, source)
                           VALUES %s
                           ON CONFLICT (year, month, flow, hs_code, country_code)
                           DO UPDATE SET
                               value_usd = EXCLUDED.value_usd,
                               quantity = EXCLUDED.quantity,
                               collected_at = NOW()
                        """,
                        rows,
                    )
                    total_inserted += len(rows)
                    logger.info("  Database: upserted %d records for %s", len(rows), comm_key)

            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Database error: %s", e)
        raise
    finally:
        conn.close()

    return total_inserted


def print_summary(results: Dict[str, List[Dict]]) -> None:
    """Print a summary of collected data."""
    print("\n" + "=" * 70)
    print("COTTONSEED COMPLEX HISTORICAL TRADE DATA - COLLECTION SUMMARY")
    print("=" * 70)

    for comm_key, records in results.items():
        if not records:
            print(f"\n{comm_key}: No data collected")
            continue

        exports = [r for r in records if r["flow"] == "exports"]
        imports = [r for r in records if r["flow"] == "imports"]

        exp_years = sorted(set(r["year"] for r in exports)) if exports else []
        imp_years = sorted(set(r["year"] for r in imports)) if imports else []

        exp_countries = len(set(r["country_code"] for r in exports)) if exports else 0
        imp_countries = len(set(r["country_code"] for r in imports)) if imports else 0

        config = COMMODITIES[comm_key]
        print(f"\n{config.name} ({comm_key})")
        print(f"  HS Codes: {', '.join(config.hs_codes)}")
        print(f"  Target unit: {config.target_unit}")
        print(f"  Exports: {len(exports):,} records | {exp_years[0]}-{exp_years[-1]} | {exp_countries} countries" if exports else "  Exports: none")
        print(f"  Imports: {len(imports):,} records | {imp_years[0]}-{imp_years[-1]} | {imp_countries} countries" if imports else "  Imports: none")

    total = sum(len(r) for r in results.values())
    print(f"\nTotal records: {total:,}")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull historical cottonseed complex trade data from Census Bureau.")
    parser.add_argument("--start-year", type=int, default=2013, help="Start year (Census API starts at 2013)")
    parser.add_argument("--end-year", type=int, default=date.today().year, help="End year")
    parser.add_argument("--flow", choices=["imports", "exports", "both"], default="both")
    parser.add_argument("--commodity", choices=list(COMMODITIES.keys()) + ["ALL"], default="ALL")
    parser.add_argument("--api-key", default=os.getenv("CENSUS_API_KEY"))
    parser.add_argument("--save-to-db", action="store_true", help="Also save to bronze.census_trade")
    parser.add_argument("--output-dir", type=str, default=str(OUTPUT_DIR))
    args = parser.parse_args()

    if not args.api_key:
        logger.warning("No CENSUS_API_KEY found. API calls may be rate-limited (500/day).")

    flows = ["imports", "exports"] if args.flow == "both" else [args.flow]

    # Filter commodities if specific one requested
    commodities_to_pull = COMMODITIES
    if args.commodity != "ALL":
        commodities_to_pull = {args.commodity: COMMODITIES[args.commodity]}

    logger.info("Pulling cottonseed complex trade data: %d-%d, flows=%s", args.start_year, args.end_year, flows)
    logger.info("Commodities: %s", ", ".join(commodities_to_pull.keys()))

    results = collect_all(args.start_year, args.end_year, args.api_key, flows, commodities_to_pull)

    # Save to CSV
    output_dir = Path(args.output_dir)
    saved = save_to_csv(results, output_dir)
    for key, path in saved.items():
        print(f"  CSV: {key} -> {path}")

    # Optionally save to database
    if args.save_to_db:
        count = save_to_database(results)
        print(f"  Database: {count:,} records upserted")

    print_summary(results)


if __name__ == "__main__":
    main()
