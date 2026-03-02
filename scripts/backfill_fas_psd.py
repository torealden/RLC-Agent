"""
Historical FAS PSD Backfill Script

Pulls all historical Production, Supply, and Distribution data from the
USDA FAS API and inserts pivoted balance-sheet rows into bronze.fas_psd.

The FAS PSD API returns attribute-level records (one record per S&D attribute
per commodity/country/year). This script pivots those into one row per
(commodity, country, marketing_year, month) matching the bronze.fas_psd schema.

Usage:
    # Full historical pull (1960-2025, all commodities)
    python scripts/backfill_fas_psd.py

    # Specific year range
    python scripts/backfill_fas_psd.py --start-year 2000 --end-year 2025

    # Specific commodities only
    python scripts/backfill_fas_psd.py --commodities corn,soybeans,wheat

    # Resume from where it left off
    python scripts/backfill_fas_psd.py --resume

    # Dry run (fetch but don't insert)
    python scripts/backfill_fas_psd.py --dry-run

    # Skip world aggregate (faster)
    python scripts/backfill_fas_psd.py --skip-world

API:  https://api.fas.usda.gov
Auth: X-Api-Key header (FAS_API_KEY env var)
Rate: 1,000 requests/hour with registered key
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Add project root to path so we can import db_config
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.services.database.db_config import get_connection

logger = logging.getLogger("backfill_fas_psd")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PSD_API_BASE = "https://api.fas.usda.gov"

# Attribute ID -> bronze.fas_psd column name
# These are the standard PSD attribute IDs from /api/psd/commodityAttributes
ATTR_MAP: Dict[int, str] = {
    1:   "area_planted",
    4:   "area_harvested",
    7:   "crush",
    20:  "beginning_stocks",
    28:  "production",
    57:  "imports",
    81:  "ty_imports",
    86:  "total_supply",
    88:  "exports",
    113: "ty_exports",
    125: "domestic_consumption",
    130: "feed_dom_consumption",
    176: "ending_stocks",
    178: "total_distribution",
    184: "yield",
    192: "fsi_consumption",
}

# All balance-sheet columns in bronze.fas_psd (insertion order)
BALANCE_COLS = [
    "area_planted", "area_harvested", "yield",
    "beginning_stocks", "production", "imports", "total_supply",
    "feed_dom_consumption", "fsi_consumption", "crush",
    "domestic_consumption", "exports", "total_distribution",
    "ending_stocks", "ty_imports", "ty_exports",
]

# PSD commodity codes -- mirrors usda_wasde_collector.py
PSD_COMMODITY_CODES: Dict[str, Dict[str, str]] = {
    "corn":              {"code": "0440000", "name": "Corn"},
    "wheat":             {"code": "0410000", "name": "Wheat"},
    "rice":              {"code": "0422110", "name": "Rice, Milled"},
    "barley":            {"code": "0430000", "name": "Barley"},
    "sorghum":           {"code": "0459100", "name": "Sorghum"},
    "soybeans":          {"code": "2222000", "name": "Soybeans"},
    "rapeseed":          {"code": "2226000", "name": "Oilseed, Rapeseed"},
    "sunflowerseed":     {"code": "2224000", "name": "Oilseed, Sunflowerseed"},
    "peanuts":           {"code": "2221000", "name": "Oilseed, Peanut"},
    "cottonseed":        {"code": "2223000", "name": "Oilseed, Cottonseed"},
    "soybean_meal":      {"code": "0813100", "name": "Meal, Soybean"},
    "soybean_oil":       {"code": "4232000", "name": "Oil, Soybean"},
    "palm_oil":          {"code": "4243000", "name": "Oil, Palm"},
    "palm_kernel_oil":   {"code": "4244000", "name": "Oil, Palm Kernel"},
    "rapeseed_oil":      {"code": "4239100", "name": "Oil, Rapeseed"},
    "rapeseed_meal":     {"code": "0813600", "name": "Meal, Rapeseed"},
    "sunflowerseed_oil": {"code": "4236000", "name": "Oil, Sunflowerseed"},
    "sunflowerseed_meal":{"code": "0813500", "name": "Meal, Sunflowerseed"},
    "cottonseed_oil":    {"code": "4233000", "name": "Oil, Cottonseed"},
    "cottonseed_meal":   {"code": "0813300", "name": "Meal, Cottonseed"},
    "cotton":            {"code": "2631000", "name": "Cotton"},
    "sugar":             {"code": "0612000", "name": "Sugar, Centrifugal"},
}

PROGRESS_FILE = PROJECT_ROOT / "data" / "backfill_fas_psd_progress.json"

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def build_session(api_key: str) -> requests.Session:
    """Build a requests session with retry logic and API key header."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "RLC-Agent-Backfill/1.0",
    })
    if api_key:
        session.headers["X-Api-Key"] = api_key

    return session


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Track backfill progress with resume support."""

    def __init__(self, path: Path = PROGRESS_FILE):
        self.path = path
        self.data = {
            "started_at": datetime.now().isoformat(),
            "completed": {},  # {commodity: [year1, year2, ...]}
            "stats": {
                "api_calls": 0,
                "rows_inserted": 0,
                "rows_skipped": 0,
                "errors": 0,
            },
        }

    def load(self) -> bool:
        """Load progress from file. Returns True if file existed."""
        if self.path.exists():
            with open(self.path, "r") as f:
                self.data = json.load(f)
            logger.info(
                "Resumed from progress file: %d API calls, %d rows inserted so far",
                self.data["stats"]["api_calls"],
                self.data["stats"]["rows_inserted"],
            )
            return True
        return False

    def save(self):
        """Save current progress to file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2, default=str)

    def is_done(self, commodity: str, year: int) -> bool:
        """Check if a commodity/year pair is already completed."""
        return year in self.data.get("completed", {}).get(commodity, [])

    def mark_done(self, commodity: str, year: int):
        """Mark a commodity/year pair as completed."""
        self.data.setdefault("completed", {}).setdefault(commodity, [])
        if year not in self.data["completed"][commodity]:
            self.data["completed"][commodity].append(year)

    def add_api_call(self):
        self.data["stats"]["api_calls"] += 1

    def add_rows(self, n: int):
        self.data["stats"]["rows_inserted"] += n

    def add_skipped(self, n: int):
        self.data["stats"]["rows_skipped"] += n

    def add_error(self):
        self.data["stats"]["errors"] += 1


# ---------------------------------------------------------------------------
# API fetching
# ---------------------------------------------------------------------------

def fetch_psd_data(
    session: requests.Session,
    commodity_code: str,
    endpoint: str,
    marketing_year: int,
    tracker: ProgressTracker,
    delay: float,
) -> Optional[list]:
    """
    Fetch PSD data from the FAS API for one commodity/endpoint/year.

    Args:
        endpoint: "country/all" or "world"
        delay: seconds to sleep after the call (rate limiting)

    Returns:
        List of API response records, or None on error.
    """
    if endpoint == "world":
        url = f"{PSD_API_BASE}/api/psd/commodity/{commodity_code}/world/year/{marketing_year}"
    else:
        url = f"{PSD_API_BASE}/api/psd/commodity/{commodity_code}/country/all/year/{marketing_year}"

    tracker.add_api_call()

    try:
        resp = session.get(url, timeout=60)
    except requests.RequestException as e:
        logger.warning("HTTP error fetching %s: %s", url, e)
        tracker.add_error()
        time.sleep(delay)
        return None

    time.sleep(delay)

    if resp.status_code == 429:
        # Rate limited -- back off exponentially
        logger.warning("Rate limited (429). Backing off 60s...")
        time.sleep(60)
        tracker.add_error()
        return None

    if resp.status_code == 401:
        logger.error("API key invalid or missing (401). Set FAS_API_KEY env var.")
        tracker.add_error()
        return None

    if resp.status_code != 200:
        logger.warning("HTTP %d for %s", resp.status_code, url)
        tracker.add_error()
        return None

    try:
        data = resp.json()
        if not isinstance(data, list):
            logger.warning("Unexpected response type (%s) for %s", type(data).__name__, url)
            return None
        return data
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning("JSON parse error for %s: %s", url, e)
        tracker.add_error()
        return None


# ---------------------------------------------------------------------------
# Pivot attribute records into balance-sheet rows
# ---------------------------------------------------------------------------

def pivot_records(
    records: list,
    commodity: str,
    commodity_code: str,
    report_date: date,
    is_world: bool = False,
) -> List[dict]:
    """
    Pivot API attribute-level records into bronze.fas_psd-shaped rows.

    API records have: countryCode, marketYear, calendarYear, month,
    attributeId, value, unitId, etc.

    We group by (countryCode, marketYear, month) and pivot attributeId -> column.
    """
    # Group records by (country_code, marketing_year, month)
    groups: Dict[tuple, dict] = {}

    for rec in records:
        attr_id = rec.get("attributeId")
        if attr_id is None:
            continue

        col = ATTR_MAP.get(attr_id)
        if col is None:
            continue  # attribute we don't track

        country_code = "WD" if is_world else (rec.get("countryCode") or "")
        country_name = "World" if is_world else (rec.get("countryDescription") or country_code)
        my = rec.get("marketYear")
        month = rec.get("month")
        cal_year = rec.get("calendarYear")

        if my is None:
            continue

        key = (country_code, my, month)

        if key not in groups:
            groups[key] = {
                "commodity": commodity,
                "commodity_code": commodity_code,
                "country": country_name,
                "country_code": country_code,
                "marketing_year": my,
                "calendar_year": cal_year,
                "month": month,
                "report_date": report_date,
                "unit": rec.get("unitDescription", "1000 MT"),
            }
            # Initialize all balance-sheet columns to None
            for c in BALANCE_COLS:
                groups[key][c] = None

        # Set the value for this attribute
        val = rec.get("value")
        if val is not None:
            try:
                groups[key][col] = float(val)
            except (ValueError, TypeError):
                pass

    return list(groups.values())


# ---------------------------------------------------------------------------
# Database insertion
# ---------------------------------------------------------------------------

INSERT_SQL = """
    INSERT INTO bronze.fas_psd (
        commodity, commodity_code, country, country_code,
        marketing_year, calendar_year, month, report_date,
        area_planted, area_harvested, yield,
        beginning_stocks, production, imports, total_supply,
        feed_dom_consumption, fsi_consumption, crush,
        domestic_consumption, exports, total_distribution,
        ending_stocks, ty_imports, ty_exports,
        unit
    ) VALUES (
        %(commodity)s, %(commodity_code)s, %(country)s, %(country_code)s,
        %(marketing_year)s, %(calendar_year)s, %(month)s, %(report_date)s,
        %(area_planted)s, %(area_harvested)s, %(yield)s,
        %(beginning_stocks)s, %(production)s, %(imports)s, %(total_supply)s,
        %(feed_dom_consumption)s, %(fsi_consumption)s, %(crush)s,
        %(domestic_consumption)s, %(exports)s, %(total_distribution)s,
        %(ending_stocks)s, %(ty_imports)s, %(ty_exports)s,
        %(unit)s
    )
    ON CONFLICT (commodity_code, country_code, marketing_year, month, report_date)
    DO NOTHING
"""


def insert_rows(rows: List[dict], tracker: ProgressTracker, dry_run: bool = False) -> int:
    """Insert pivoted rows into bronze.fas_psd. Returns count of rows inserted."""
    if not rows:
        return 0

    if dry_run:
        tracker.add_skipped(len(rows))
        return 0

    inserted = 0
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            for row in rows:
                try:
                    cursor.execute(INSERT_SQL, row)
                    # statusmessage is "INSERT 0 1" on success, "INSERT 0 0" on conflict skip
                    if hasattr(cursor, "statusmessage") and cursor.statusmessage == "INSERT 0 1":
                        inserted += 1
                    elif hasattr(cursor, "rowcount") and cursor.rowcount > 0:
                        inserted += 1
                except Exception as e:
                    logger.warning("Row insert error (%s/%s/MY%s): %s",
                                   row.get("commodity"), row.get("country_code"),
                                   row.get("marketing_year"), e)
                    conn.rollback()
                    tracker.add_error()
    except Exception as e:
        logger.error("Database connection error: %s", e)
        tracker.add_error()
        return 0

    tracker.add_rows(inserted)
    return inserted


# ---------------------------------------------------------------------------
# Main backfill loop
# ---------------------------------------------------------------------------

def run_backfill(args):
    """Main backfill orchestration."""

    # --- API key ---
    api_key = os.environ.get("FAS_API_KEY") or os.environ.get("USDA_FAS_API_KEY", "")
    if not api_key:
        logger.error(
            "No API key found. Set FAS_API_KEY env var. "
            "Register free at https://api.fas.usda.gov"
        )
        sys.exit(1)

    session = build_session(api_key)

    # --- Commodities ---
    if args.commodities:
        commodity_names = [c.strip() for c in args.commodities.split(",")]
        for name in commodity_names:
            if name not in PSD_COMMODITY_CODES:
                logger.error("Unknown commodity: %s", name)
                logger.info("Available: %s", ", ".join(sorted(PSD_COMMODITY_CODES.keys())))
                sys.exit(1)
    else:
        commodity_names = list(PSD_COMMODITY_CODES.keys())

    # --- Year range ---
    start_year = args.start_year
    end_year = args.end_year
    years = list(range(start_year, end_year + 1))

    # --- Rate limiting ---
    # Default: 1000 req/hr -> 3.6s between calls
    rate_limit = args.rate_limit or 1000
    delay = 3600.0 / rate_limit

    # --- Report date ---
    report_date = date.today()

    # --- Progress tracking ---
    tracker = ProgressTracker()
    if args.resume:
        tracker.load()

    # --- Ctrl+C handler ---
    interrupted = False

    def handle_interrupt(signum, frame):
        nonlocal interrupted
        if interrupted:
            logger.warning("Second interrupt -- exiting immediately")
            sys.exit(1)
        interrupted = True
        logger.info("Interrupt received. Saving progress and exiting after current request...")

    signal.signal(signal.SIGINT, handle_interrupt)

    # --- Estimate ---
    total_pairs = len(commodity_names) * len(years)
    calls_per_pair = 1 if args.skip_world else 2
    skip_count = sum(
        1 for c in commodity_names for y in years if tracker.is_done(c, y)
    )
    remaining = total_pairs - skip_count
    est_calls = remaining * calls_per_pair
    est_minutes = est_calls * delay / 60

    logger.info("=" * 60)
    logger.info("FAS PSD Historical Backfill")
    logger.info("=" * 60)
    logger.info("Commodities: %d (%s)", len(commodity_names),
                ", ".join(commodity_names[:5]) + ("..." if len(commodity_names) > 5 else ""))
    logger.info("Years: %d-%d (%d years)", start_year, end_year, len(years))
    logger.info("World aggregate: %s", "no" if args.skip_world else "yes")
    logger.info("Dry run: %s", "yes" if args.dry_run else "no")
    logger.info("Rate limit: %d req/hr (%.1fs delay)", rate_limit, delay)
    logger.info("Report date: %s", report_date)
    logger.info("Estimated API calls: ~%d (%.0f min)", est_calls, est_minutes)
    if skip_count:
        logger.info("Skipping %d already-completed commodity/year pairs", skip_count)
    logger.info("=" * 60)

    # --- Main loop ---
    pair_num = 0
    for commodity in commodity_names:
        if interrupted:
            break

        info = PSD_COMMODITY_CODES[commodity]
        code = info["code"]

        for year in years:
            if interrupted:
                break

            pair_num += 1

            if tracker.is_done(commodity, year):
                continue

            logger.info("[%d/%d] %s MY%d (code=%s)...",
                        pair_num, total_pairs, commodity, year, code)

            # --- Fetch country/all ---
            records = fetch_psd_data(session, code, "country/all", year, tracker, delay)

            rows_this_pair = 0

            if records:
                pivoted = pivot_records(records, commodity, code, report_date, is_world=False)
                n = insert_rows(pivoted, tracker, dry_run=args.dry_run)
                rows_this_pair += n
                if args.dry_run:
                    logger.info("  country/all: %d records -> %d rows (dry run)", len(records), len(pivoted))
                else:
                    logger.info("  country/all: %d records -> %d rows pivoted, %d inserted", len(records), len(pivoted), n)
            elif records is not None and len(records) == 0:
                logger.info("  country/all: no data for %s MY%d", commodity, year)
            else:
                logger.info("  country/all: fetch failed")

            # --- Fetch world aggregate ---
            if not args.skip_world and not interrupted:
                world_records = fetch_psd_data(session, code, "world", year, tracker, delay)
                if world_records:
                    pivoted_w = pivot_records(world_records, commodity, code, report_date, is_world=True)
                    n_w = insert_rows(pivoted_w, tracker, dry_run=args.dry_run)
                    rows_this_pair += n_w
                    if args.dry_run:
                        logger.info("  world: %d records -> %d rows (dry run)", len(world_records), len(pivoted_w))
                    else:
                        logger.info("  world: %d records -> %d rows pivoted, %d inserted", len(world_records), len(pivoted_w), n_w)

            # Mark done (even if 0 rows -- means no data exists for that pair)
            if not interrupted:
                tracker.mark_done(commodity, year)

            # Save progress periodically (every 10 pairs)
            if pair_num % 10 == 0:
                tracker.save()

    # --- Final save ---
    tracker.save()

    # --- Summary ---
    stats = tracker.data["stats"]
    logger.info("=" * 60)
    logger.info("Backfill %s", "interrupted" if interrupted else "complete")
    logger.info("API calls:     %d", stats["api_calls"])
    logger.info("Rows inserted: %d", stats["rows_inserted"])
    if stats["rows_skipped"]:
        logger.info("Rows skipped:  %d (dry run)", stats["rows_skipped"])
    logger.info("Errors:        %d", stats["errors"])
    logger.info("Progress saved to: %s", tracker.path)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Backfill bronze.fas_psd with historical FAS PSD data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pull (1960-2025, all 22 commodities, ~3 hours)
  python scripts/backfill_fas_psd.py

  # Core grains, recent years (~10 min)
  python scripts/backfill_fas_psd.py --commodities corn,soybeans,wheat --start-year 2020 --skip-world

  # Dry run to verify pivoting
  python scripts/backfill_fas_psd.py --commodities corn --start-year 2024 --dry-run

  # Resume after interruption
  python scripts/backfill_fas_psd.py --resume
        """,
    )

    parser.add_argument(
        "--start-year", type=int, default=1960,
        help="First marketing year to fetch (default: 1960)",
    )
    parser.add_argument(
        "--end-year", type=int, default=date.today().year,
        help="Last marketing year to fetch (default: current year)",
    )
    parser.add_argument(
        "--commodities", type=str, default=None,
        help="Comma-separated commodity names (default: all 22)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume from progress file (skip completed commodity/year pairs)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and pivot but do not insert into database",
    )
    parser.add_argument(
        "--skip-world", action="store_true",
        help="Skip world aggregate endpoint (halves API calls)",
    )
    parser.add_argument(
        "--rate-limit", type=int, default=None,
        help="Max requests per hour (default: 1000)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # --- Logging ---
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    run_backfill(args)


if __name__ == "__main__":
    main()
