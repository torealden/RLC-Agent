"""
EIA Petroleum & Biofuels Agent
==============================
Fetches energy data from the US Energy Information Administration (EIA) API v2.

Data includes:
- Weekly Petroleum Status Report (crude, gasoline, distillate, ethanol)
- Daily spot prices (WTI, Brent, RBOB, ULSD, Henry Hub)
- Monthly biofuels data (biodiesel)

Usage:
    python eia_petroleum_agent.py                    # Fetch latest data
    python eia_petroleum_agent.py --backfill 30     # Backfill last 30 days
    python eia_petroleum_agent.py --series crude_oil_stocks  # Single series
    python eia_petroleum_agent.py --test            # Test API connection

Requires:
    - EIA API key in environment variable EIA_API_KEY
    - PostgreSQL connection string in DATABASE_URL (or individual vars)
"""

import os
import sys
import json
import hashlib
import logging
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from decimal import Decimal
import time

# Third-party imports
try:
    import requests
    import yaml
    import psycopg2
    from psycopg2.extras import Json, execute_values
except ImportError as e:
    print(f"Missing required package: {e}")
    print("Install with: pip install requests pyyaml psycopg2-binary")
    sys.exit(1)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Try multiple locations for .env file
    env_paths = [
        Path(__file__).parent.parent / ".env",  # commodity_pipeline/.env
        Path(__file__).parent.parent.parent / "RLC-Agent" / ".env",  # RLC-Agent/.env
        Path.home() / "RLC Dropbox" / "RLC Team Folder" / "RLC Documents" / "LLM Model and Documents" / "Projects" / "RLC-Agent" / ".env"
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            break
except ImportError:
    pass  # dotenv not installed, rely on system environment

# =============================================================================
# Configuration
# =============================================================================

# Paths
SCRIPT_DIR = Path(__file__).parent
CONFIG_DIR = SCRIPT_DIR.parent / "config"
LOG_DIR = SCRIPT_DIR.parent / "logs"
DATA_DIR = SCRIPT_DIR.parent / "data"

# Ensure directories exist
LOG_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
(DATA_DIR / "raw").mkdir(exist_ok=True)
(DATA_DIR / "processed").mkdir(exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "eia_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EIA_Agent")

# API Configuration
EIA_API_BASE = "https://api.eia.gov/v2"
EIA_API_KEY = os.environ.get("EIA_API_KEY")

# Database Configuration
DATABASE_URL = os.environ.get("DATABASE_URL")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "rlc_commodities")
DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class EIAResponse:
    """Structured response from EIA API."""
    series_id: str
    category: str
    data: List[Dict]
    api_route: str
    params: Dict
    fetch_timestamp: datetime
    record_count: int


@dataclass
class IngestionResult:
    """Result of an ingestion operation."""
    series_id: str
    status: str  # 'success', 'skipped', 'failed'
    records_fetched: int = 0
    records_inserted: int = 0
    records_skipped: int = 0
    error_message: Optional[str] = None


# =============================================================================
# Database Connection
# =============================================================================

def get_db_connection():
    """Get PostgreSQL database connection."""
    if DATABASE_URL:
        return psycopg2.connect(DATABASE_URL)
    else:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )


def ensure_schemas(conn):
    """Ensure required schemas exist."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver")
        cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
    conn.commit()


# =============================================================================
# EIA API Client
# =============================================================================

class EIAClient:
    """Client for EIA API v2."""

    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("EIA_API_KEY environment variable not set")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json"
        })

    def fetch_series(
        self,
        route: str,
        params: Dict,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 5000
    ) -> Dict:
        """
        Fetch data from EIA API.

        Args:
            route: API route (e.g., '/petroleum/sum/sndw/data')
            params: Query parameters including facets
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Max records to return

        Returns:
            Raw API response as dict
        """
        url = f"{EIA_API_BASE}{route}"

        # Build query params
        query_params = {
            "api_key": self.api_key,
            "length": limit,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc"
        }

        # Add data columns
        if "data" in params:
            for i, col in enumerate(params["data"]):
                query_params[f"data[{i}]"] = col

        # Add facets
        if "facets" in params:
            for facet_name, facet_values in params["facets"].items():
                if isinstance(facet_values, list):
                    for i, val in enumerate(facet_values):
                        query_params[f"facets[{facet_name}][{i}]"] = val
                else:
                    query_params[f"facets[{facet_name}][0]"] = facet_values

        # Add date filters
        if start_date:
            query_params["start"] = start_date.strftime("%Y-%m-%d")
        if end_date:
            query_params["end"] = end_date.strftime("%Y-%m-%d")

        logger.debug(f"Fetching: {url}")
        logger.debug(f"Params: {query_params}")

        response = self.session.get(url, params=query_params, timeout=60)
        response.raise_for_status()

        return response.json()

    def test_connection(self) -> bool:
        """Test API connection and key validity."""
        try:
            # Simple test query
            response = self.session.get(
                f"{EIA_API_BASE}/petroleum/sum/sndw",
                params={"api_key": self.api_key},
                timeout=10
            )
            response.raise_for_status()
            logger.info("EIA API connection test successful")
            return True
        except Exception as e:
            logger.error(f"EIA API connection test failed: {e}")
            return False


# =============================================================================
# Data Processing
# =============================================================================

def load_series_config() -> Dict:
    """Load series configuration from YAML file."""
    config_path = CONFIG_DIR / "eia_series.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def compute_hash(data: Any) -> str:
    """Compute SHA-256 hash of data for deduplication."""
    json_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(json_str.encode()).hexdigest()


def extract_report_date(record: Dict) -> Optional[date]:
    """Extract report date from EIA record."""
    period = record.get("period")
    if not period:
        return None

    # Handle different period formats
    try:
        if len(period) == 10:  # YYYY-MM-DD
            return datetime.strptime(period, "%Y-%m-%d").date()
        elif len(period) == 7:  # YYYY-MM
            return datetime.strptime(period + "-01", "%Y-%m-%d").date()
        elif len(period) == 4:  # YYYY
            return datetime.strptime(period + "-01-01", "%Y-%m-%d").date()
    except ValueError:
        logger.warning(f"Could not parse period: {period}")

    return None


# =============================================================================
# Bronze Layer - Raw Ingestion
# =============================================================================

def ingest_to_bronze(
    conn,
    series_id: str,
    category: str,
    api_response: Dict,
    api_route: str,
    api_params: Dict
) -> int:
    """
    Store raw API response in bronze layer.

    Returns number of records inserted.
    """
    data = api_response.get("response", {}).get("data", [])
    if not data:
        logger.warning(f"No data in response for {series_id}")
        return 0

    records_inserted = 0

    with conn.cursor() as cur:
        for record in data:
            report_date = extract_report_date(record)
            if not report_date:
                continue

            payload = {
                "series_id": series_id,
                "record": record,
                "api_metadata": api_response.get("response", {}).get("total", 0)
            }
            file_hash = compute_hash(payload)

            try:
                cur.execute("""
                    INSERT INTO bronze.eia_raw_ingestion
                    (source_system, series_id, category, report_date, raw_payload,
                     api_route, api_params, record_count, file_hash)
                    VALUES ('eia', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source_system, series_id, report_date, file_hash)
                    DO NOTHING
                    RETURNING id
                """, (
                    series_id,
                    category,
                    report_date,
                    Json(payload),
                    api_route,
                    Json(api_params),
                    1,
                    file_hash
                ))

                result = cur.fetchone()
                if result:
                    records_inserted += 1

            except Exception as e:
                logger.error(f"Error inserting record: {e}")
                continue

    conn.commit()
    return records_inserted


# =============================================================================
# Silver Layer - Transformation
# =============================================================================

def transform_petroleum_weekly(conn, bronze_records: List[Dict]) -> int:
    """
    Transform bronze petroleum data to silver weekly table.

    This aggregates multiple series into a single weekly row.
    """
    # Group records by period_date
    by_period = {}
    for rec in bronze_records:
        period = rec.get("period")
        if period:
            if period not in by_period:
                by_period[period] = {}
            series = rec.get("series_id", "unknown")
            by_period[period][series] = rec.get("value")

    records_inserted = 0

    with conn.cursor() as cur:
        for period_str, values in by_period.items():
            try:
                period_date = datetime.strptime(period_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            # Map series to columns
            cur.execute("""
                INSERT INTO silver.eia_petroleum_weekly
                (period_date, report_date, crude_stocks_total_kb, crude_production_kbd,
                 crude_imports_kbd, refinery_inputs_kbd, gasoline_stocks_kb,
                 gasoline_demand_kbd, distillate_stocks_kb, ethanol_production_kbd,
                 ethanol_stocks_kb, validation_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'validated')
                ON CONFLICT (period_date)
                DO UPDATE SET
                    crude_stocks_total_kb = COALESCE(EXCLUDED.crude_stocks_total_kb, silver.eia_petroleum_weekly.crude_stocks_total_kb),
                    crude_production_kbd = COALESCE(EXCLUDED.crude_production_kbd, silver.eia_petroleum_weekly.crude_production_kbd),
                    crude_imports_kbd = COALESCE(EXCLUDED.crude_imports_kbd, silver.eia_petroleum_weekly.crude_imports_kbd),
                    refinery_inputs_kbd = COALESCE(EXCLUDED.refinery_inputs_kbd, silver.eia_petroleum_weekly.refinery_inputs_kbd),
                    gasoline_stocks_kb = COALESCE(EXCLUDED.gasoline_stocks_kb, silver.eia_petroleum_weekly.gasoline_stocks_kb),
                    gasoline_demand_kbd = COALESCE(EXCLUDED.gasoline_demand_kbd, silver.eia_petroleum_weekly.gasoline_demand_kbd),
                    distillate_stocks_kb = COALESCE(EXCLUDED.distillate_stocks_kb, silver.eia_petroleum_weekly.distillate_stocks_kb),
                    ethanol_production_kbd = COALESCE(EXCLUDED.ethanol_production_kbd, silver.eia_petroleum_weekly.ethanol_production_kbd),
                    ethanol_stocks_kb = COALESCE(EXCLUDED.ethanol_stocks_kb, silver.eia_petroleum_weekly.ethanol_stocks_kb),
                    updated_ts = NOW()
            """, (
                period_date,
                date.today(),
                values.get("crude_oil_stocks"),
                values.get("crude_oil_production"),
                values.get("crude_oil_imports"),
                values.get("refinery_inputs"),
                values.get("gasoline_stocks"),
                values.get("gasoline_demand"),
                values.get("distillate_stocks"),
                values.get("ethanol_production"),
                values.get("ethanol_stocks")
            ))
            records_inserted += 1

    conn.commit()
    return records_inserted


def transform_spot_prices(conn, bronze_records: List[Dict]) -> int:
    """Transform bronze price data to silver daily prices table."""
    records_inserted = 0

    with conn.cursor() as cur:
        for rec in bronze_records:
            try:
                price_date = datetime.strptime(rec["period"], "%Y-%m-%d").date()
            except (ValueError, KeyError):
                continue

            series = rec.get("series_id", "")
            value = rec.get("value")

            if value is None:
                continue

            # Determine which column to update based on series
            column_map = {
                "wti_spot": "wti_spot_bbl",
                "brent_spot": "brent_spot_bbl",
                "rbob_gasoline": "rbob_gasoline_gal",
                "ulsd_diesel": "ulsd_diesel_gal",
                "heating_oil": "heating_oil_gal",
                "henry_hub_spot": "henry_hub_mmbtu"
            }

            column = column_map.get(series)
            if not column:
                continue

            # Upsert the value
            cur.execute(f"""
                INSERT INTO silver.eia_spot_prices_daily (price_date, {column}, validation_status)
                VALUES (%s, %s, 'validated')
                ON CONFLICT (price_date)
                DO UPDATE SET {column} = EXCLUDED.{column}, created_ts = NOW()
            """, (price_date, value))
            records_inserted += 1

        # Calculate spreads and cracks after inserting raw prices
        cur.execute("""
            UPDATE silver.eia_spot_prices_daily
            SET
                wti_brent_spread = brent_spot_bbl - wti_spot_bbl,
                gasoline_crack_bbl = (rbob_gasoline_gal * 42) - wti_spot_bbl,
                diesel_crack_bbl = (ulsd_diesel_gal * 42) - wti_spot_bbl
            WHERE wti_spot_bbl IS NOT NULL
              AND (wti_brent_spread IS NULL OR gasoline_crack_bbl IS NULL OR diesel_crack_bbl IS NULL)
        """)

    conn.commit()
    return records_inserted


# =============================================================================
# Main Agent Logic
# =============================================================================

class EIAPetroleumAgent:
    """Main agent class for EIA data ingestion."""

    def __init__(self):
        self.client = EIAClient(EIA_API_KEY)
        self.config = load_series_config()
        self.conn = None

    def connect_db(self):
        """Establish database connection."""
        self.conn = get_db_connection()
        ensure_schemas(self.conn)
        logger.info("Database connection established")

    def close_db(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def fetch_series(
        self,
        series_id: str,
        category: str,
        series_config: Dict,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> IngestionResult:
        """
        Fetch and ingest a single series.

        Returns IngestionResult with status and counts.
        """
        result = IngestionResult(series_id=series_id, status="pending")

        try:
            route = series_config["route"]
            params = series_config.get("params", {})

            logger.info(f"Fetching {series_id} from {route}")

            # Fetch from API
            api_response = self.client.fetch_series(
                route=route,
                params=params,
                start_date=start_date,
                end_date=end_date
            )

            data = api_response.get("response", {}).get("data", [])
            result.records_fetched = len(data)

            if not data:
                result.status = "skipped"
                result.error_message = "No data returned from API"
                return result

            # Store in bronze
            result.records_inserted = ingest_to_bronze(
                self.conn,
                series_id,
                category,
                api_response,
                route,
                params
            )

            # Log to file for debugging
            raw_file = DATA_DIR / "raw" / f"{series_id}_{date.today().isoformat()}.json"
            with open(raw_file, "w") as f:
                json.dump(api_response, f, indent=2, default=str)

            result.status = "success"
            logger.info(f"  Fetched {result.records_fetched}, inserted {result.records_inserted}")

        except requests.exceptions.HTTPError as e:
            result.status = "failed"
            result.error_message = f"HTTP error: {e}"
            logger.error(f"HTTP error fetching {series_id}: {e}")

        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            logger.exception(f"Error fetching {series_id}: {e}")

        return result

    def run(
        self,
        categories: Optional[List[str]] = None,
        series_filter: Optional[str] = None,
        backfill_days: int = 0
    ) -> List[IngestionResult]:
        """
        Run the agent to fetch all configured series.

        Args:
            categories: Optional list of categories to fetch (e.g., ['petroleum', 'biofuels'])
            series_filter: Optional single series ID to fetch
            backfill_days: If > 0, fetch historical data going back N days

        Returns:
            List of IngestionResult for each series
        """
        results = []

        # Calculate date range
        end_date = date.today()
        start_date = None
        if backfill_days > 0:
            start_date = end_date - timedelta(days=backfill_days)
            logger.info(f"Backfilling from {start_date} to {end_date}")

        # Connect to database
        self.connect_db()

        try:
            # Iterate through configured categories and series
            for category, series_dict in self.config.items():
                if category in ("api_base_url",):  # Skip metadata
                    continue

                if categories and category not in categories:
                    continue

                if not isinstance(series_dict, dict):
                    continue

                logger.info(f"Processing category: {category}")

                for series_id, series_config in series_dict.items():
                    if not isinstance(series_config, dict):
                        continue

                    if series_filter and series_id != series_filter:
                        continue

                    result = self.fetch_series(
                        series_id=series_id,
                        category=category,
                        series_config=series_config,
                        start_date=start_date,
                        end_date=end_date
                    )
                    results.append(result)

                    # Rate limiting - be nice to the API
                    time.sleep(0.5)

            # Log summary
            success_count = sum(1 for r in results if r.status == "success")
            failed_count = sum(1 for r in results if r.status == "failed")
            total_records = sum(r.records_inserted for r in results)

            logger.info(f"Ingestion complete: {success_count} succeeded, {failed_count} failed, {total_records} records inserted")

            # Log to meta table
            self._log_ingestion_run(results)

        finally:
            self.close_db()

        return results

    def _log_ingestion_run(self, results: List[IngestionResult]):
        """Log ingestion run to meta table."""
        try:
            with self.conn.cursor() as cur:
                for result in results:
                    cur.execute("""
                        INSERT INTO meta.eia_ingestion_log
                        (series_id, category, status, records_fetched, records_inserted,
                         records_skipped, error_message, completed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        result.series_id,
                        "petroleum",  # TODO: track actual category
                        result.status,
                        result.records_fetched,
                        result.records_inserted,
                        result.records_skipped,
                        result.error_message
                    ))
            self.conn.commit()
        except Exception as e:
            logger.error(f"Failed to log ingestion run: {e}")


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="EIA Petroleum & Biofuels Data Agent")
    parser.add_argument("--test", action="store_true", help="Test API connection")
    parser.add_argument("--series", type=str, help="Fetch single series by ID")
    parser.add_argument("--category", type=str, nargs="+", help="Fetch specific categories")
    parser.add_argument("--backfill", type=int, default=0, help="Backfill N days of history")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't store")

    args = parser.parse_args()

    # Check API key
    if not EIA_API_KEY:
        print("ERROR: EIA_API_KEY environment variable not set")
        print("Get your free API key at: https://www.eia.gov/opendata/register.php")
        sys.exit(1)

    # Test mode
    if args.test:
        client = EIAClient(EIA_API_KEY)
        if client.test_connection():
            print("API connection successful!")

            # Also test a sample query
            print("\nTesting sample query (WTI spot price)...")
            try:
                response = client.fetch_series(
                    route="/petroleum/pri/spt/data",
                    params={
                        "data": ["value"],
                        "facets": {"product": ["EPCWTI"]}
                    },
                    limit=5
                )
                data = response.get("response", {}).get("data", [])
                if data:
                    print(f"Latest WTI prices:")
                    for rec in data[:5]:
                        print(f"  {rec.get('period')}: ${rec.get('value')}/barrel")
                else:
                    print("No data returned")
            except Exception as e:
                print(f"Sample query failed: {e}")
        else:
            print("API connection failed!")
            sys.exit(1)
        return

    # Run agent
    agent = EIAPetroleumAgent()

    try:
        results = agent.run(
            categories=args.category,
            series_filter=args.series,
            backfill_days=args.backfill
        )

        # Print summary
        print("\n" + "=" * 60)
        print("EIA INGESTION SUMMARY")
        print("=" * 60)
        for result in results:
            status_icon = "v" if result.status == "success" else "x" if result.status == "failed" else "-"
            print(f"[{status_icon}] {result.series_id}: {result.records_inserted} records")
            if result.error_message:
                print(f"    Error: {result.error_message}")

    except Exception as e:
        logger.exception(f"Agent failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
