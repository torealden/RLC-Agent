"""
USDA WASDE (World Agricultural Supply and Demand Estimates) Collector

Collects monthly WASDE report data from the USDA OCE CSV endpoint.
WASDE is released around the 12th of each month at noon ET.

CSV URL pattern:
    https://www.usda.gov/sites/default/files/documents/oce-wasde-report-data-YYYY-MM.csv

CSV columns:
    WasdeNumber, ReportDate, ReportTitle, Attribute, ReliabilityProjection,
    Commodity, Region, MarketYear, ProjEstFlag, AnnualQuarterFlag, Value,
    Unit, ReleaseDate, ReleaseTime, ForecastYear, ForecastMonth

Data source: USDA Office of the Chief Economist
https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report
"""

import io
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType,
)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


# Base URL for monthly WASDE CSV downloads
WASDE_CSV_BASE_URL = (
    "https://www.usda.gov/sites/default/files/documents/oce-wasde-report-data"
)

# WASDE table number mapping from ReportTitle patterns to table IDs
# These correspond to bronze.wasde_table_def entries
REPORT_TITLE_TO_TABLE_ID: Dict[str, str] = {
    "U.S. Wheat Supply and Use": "01",
    "U.S. Coarse Grains Supply and Use": "03",
    "U.S. Corn Supply and Use": "04",
    "U.S. Sorghum Supply and Use": "05",
    "U.S. Barley Supply and Use": "06",
    "U.S. Oats Supply and Use": "07",
    "U.S. Rice Supply and Use": "08",
    "U.S. Oilseed Supply and Use": "09",
    "U.S. Soybeans and Products Supply and Use": "10",
    "World Wheat Supply and Use": "11",
    "World Coarse Grains Supply and Use": "12",
    "World Corn Supply and Use": "13",
    "World Rice Supply and Use": "14",
    "World Oilseed Supply and Use": "15",
    "World Soybean Supply and Use": "16",
    "U.S. Sugar Supply and Use": "17",
    "U.S. Cotton Supply and Use": "19",
}

# Attribute to row_category mapping for standardization
ATTRIBUTE_TO_CATEGORY: Dict[str, str] = {
    "Area Planted": "area",
    "Area Harvested": "area",
    "Yield per Harvested Acre": "yield",
    "Beginning Stocks": "supply",
    "Production": "supply",
    "Imports": "supply",
    "Supply, Total": "supply",
    "Total Supply": "supply",
    "Feed and Residual": "demand",
    "Food, Seed, and Industrial": "demand",
    "Ethanol and by-products": "demand",
    "Ethanol & by-products": "demand",
    "Domestic, Total": "demand",
    "Exports": "demand",
    "Use, Total": "demand",
    "Total Use": "demand",
    "Ending Stocks": "stocks",
    "Avg. Farm Price": "price",
    "Farm Price": "price",
    "Crushings": "demand",
    "Seed": "demand",
    "Residual": "demand",
}

# Commodities tracked in WASDE that we care about
WASDE_COMMODITIES = [
    "Corn",
    "Wheat",
    "Soybeans",
    "Soybean Oil",
    "Soybean Meal",
    "Sorghum",
    "Barley",
    "Oats",
    "Rice",
    "Cotton",
    "Sugar (Centrifugal)",
]


@dataclass
class USDAWASPEConfig(CollectorConfig):
    """USDA WASDE-specific configuration."""

    source_name: str = "USDA WASDE"
    source_url: str = WASDE_CSV_BASE_URL
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY
    rate_limit_per_minute: int = 10
    timeout: int = 60

    # WASDE-specific settings
    commodities: List[str] = field(default_factory=lambda: [
        "corn", "wheat", "soybeans", "soybean_oil", "soybean_meal",
        "cotton", "sorghum", "rice",
    ])

    # Which regions to collect (us, world, or both)
    regions: List[str] = field(default_factory=lambda: ["United States", "World"])

    # How many months of historical data to fetch on backfill
    backfill_months: int = 12


class USDAWASPECollector(BaseCollector):
    """
    Collector for USDA WASDE (World Agricultural Supply and Demand Estimates).

    Downloads the monthly WASDE CSV from USDA.gov and parses it into a
    standardized format matching the bronze.wasde_cell schema.

    Features:
    - Downloads monthly CSV from predictable URL pattern
    - Parses all commodity balance sheets (US and World)
    - Maps to bronze.wasde_cell schema (release_id, table_id, row_id, etc.)
    - Supports backfill of historical months
    - Calculates month-over-month revisions
    """

    def __init__(self, config: USDAWASPEConfig = None):
        config = config or USDAWASPEConfig()
        super().__init__(config)
        self.config: USDAWASPEConfig = config

    def get_table_name(self) -> str:
        return "bronze.wasde_cell"

    def _build_csv_url(self, year: int, month: int) -> str:
        """Build the WASDE CSV download URL for a given year/month."""
        return f"{WASDE_CSV_BASE_URL}-{year}-{month:02d}.csv"

    def _normalize_commodity(self, commodity_str: str) -> Optional[str]:
        """Normalize WASDE commodity name to our standard codes."""
        if not commodity_str:
            return None
        norm = commodity_str.strip().lower()
        mappings = {
            "corn": "corn",
            "wheat": "wheat",
            "soybeans": "soybeans",
            "soybean oil": "soybean_oil",
            "soybean meal": "soybean_meal",
            "sorghum": "sorghum",
            "barley": "barley",
            "oats": "oats",
            "rice": "rice",
            "rice, milled": "rice",
            "cotton": "cotton",
            "sugar (centrifugal)": "sugar",
            "sugar": "sugar",
            "upland cotton": "cotton",
        }
        return mappings.get(norm, norm.replace(" ", "_"))

    def _map_table_id(self, report_title: str) -> Optional[str]:
        """Map a ReportTitle to a WASDE table number."""
        if not report_title:
            return None
        # Exact match first
        if report_title in REPORT_TITLE_TO_TABLE_ID:
            return REPORT_TITLE_TO_TABLE_ID[report_title]
        # Fuzzy match: check if any key is contained in the title
        title_lower = report_title.lower()
        for pattern, table_id in REPORT_TITLE_TO_TABLE_ID.items():
            if pattern.lower() in title_lower:
                return table_id
        return None

    def _determine_projection_type(self, proj_est_flag: str) -> str:
        """Map ProjEstFlag to standardized projection type."""
        if not proj_est_flag:
            return "actual"
        flag = str(proj_est_flag).strip().lower()
        if "proj" in flag:
            return "proj"
        elif "est" in flag:
            return "est"
        return "actual"

    def _clean_value(self, value_str: Any) -> Optional[float]:
        """Parse a CSV value field into a numeric float."""
        if value_str is None:
            return None
        val = str(value_str).strip()
        if val in ("", "-", "--", "NA", "N/A", "nan", "None"):
            return None
        # Remove commas
        val = val.replace(",", "")
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _make_row_id(self, attribute: str) -> str:
        """Convert an Attribute label to a standardized row_id."""
        if not attribute:
            return "unknown"
        return (
            attribute.strip()
            .lower()
            .replace(",", "")
            .replace(".", "")
            .replace("  ", " ")
            .replace(" ", "_")
        )

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs,
    ) -> CollectorResult:
        """
        Fetch WASDE data from USDA CSV endpoint.

        Args:
            start_date: Earliest month to fetch (default: current month)
            end_date: Latest month to fetch (default: current month)
            **kwargs:
                year: Specific year to fetch
                month: Specific month to fetch
                backfill: If True, fetch backfill_months of history

        Returns:
            CollectorResult with parsed WASDE records
        """
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas is required for WASDE CSV parsing",
            )

        # Determine which months to fetch
        months_to_fetch = self._resolve_months(start_date, end_date, **kwargs)

        all_records = []
        warnings = []
        releases_found = []

        for year, month in months_to_fetch:
            url = self._build_csv_url(year, month)
            self.logger.info(f"Fetching WASDE CSV: {url}")

            response, error = self._make_request(url, timeout=self.config.timeout)

            if error:
                warnings.append(f"{year}-{month:02d}: {error}")
                continue

            if response.status_code == 404:
                warnings.append(
                    f"{year}-{month:02d}: CSV not yet available (404)"
                )
                continue

            if response.status_code != 200:
                warnings.append(
                    f"{year}-{month:02d}: HTTP {response.status_code}"
                )
                continue

            try:
                records, release_info = self._parse_csv(
                    response.text, year, month
                )
                all_records.extend(records)
                if release_info:
                    releases_found.append(release_info)
                self.logger.info(
                    f"  Parsed {len(records)} cells from {year}-{month:02d} "
                    f"(WASDE #{release_info.get('wasde_number', '?')})"
                )
            except Exception as e:
                warnings.append(f"{year}-{month:02d}: Parse error - {e}")
                self.logger.error(f"Parse error for {year}-{month:02d}: {e}")

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No WASDE data retrieved from any month",
                warnings=warnings,
            )

        # Build DataFrame
        df = pd.DataFrame(all_records)

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=df,
            period_start=(
                f"{months_to_fetch[0][0]}-{months_to_fetch[0][1]:02d}"
            ),
            period_end=(
                f"{months_to_fetch[-1][0]}-{months_to_fetch[-1][1]:02d}"
            ),
            data_as_of=releases_found[-1].get("release_date")
            if releases_found
            else None,
            warnings=warnings,
        )

    def _resolve_months(
        self,
        start_date: date = None,
        end_date: date = None,
        **kwargs,
    ) -> List[tuple]:
        """Determine which (year, month) pairs to fetch."""
        # Explicit year/month
        if kwargs.get("year") and kwargs.get("month"):
            return [(int(kwargs["year"]), int(kwargs["month"]))]

        # Backfill mode
        if kwargs.get("backfill"):
            today = date.today()
            months = []
            for i in range(self.config.backfill_months):
                d = today - timedelta(days=30 * i)
                months.append((d.year, d.month))
            return sorted(set(months))

        # Date range
        if start_date and end_date:
            months = []
            current = start_date.replace(day=1)
            end = end_date.replace(day=1)
            while current <= end:
                months.append((current.year, current.month))
                # Advance to next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)
            return months

        # Default: current month
        today = date.today()
        return [(today.year, today.month)]

    def _parse_csv(
        self, csv_text: str, year: int, month: int
    ) -> tuple:
        """
        Parse WASDE CSV text into cell records matching bronze.wasde_cell schema.

        Returns:
            Tuple of (list of cell dicts, release_info dict)
        """
        df = pd.read_csv(io.StringIO(csv_text), dtype=str)

        # Normalize column names (handle possible whitespace/casing variations)
        df.columns = df.columns.str.strip()

        # Extract release metadata from first row
        release_info = {}
        if len(df) > 0:
            first_row = df.iloc[0]
            release_info = {
                "wasde_number": first_row.get("WasdeNumber", ""),
                "report_date": first_row.get("ReportDate", ""),
                "release_date": first_row.get("ReleaseDate", ""),
                "release_time": first_row.get("ReleaseTime", ""),
                "forecast_year": first_row.get("ForecastYear", ""),
                "forecast_month": first_row.get("ForecastMonth", ""),
            }

        records = []
        for _, row in df.iterrows():
            report_title = str(row.get("ReportTitle", "")).strip()
            attribute = str(row.get("Attribute", "")).strip()
            commodity = str(row.get("Commodity", "")).strip()
            region = str(row.get("Region", "")).strip()
            market_year = str(row.get("MarketYear", "")).strip()
            value_text = str(row.get("Value", "")).strip()
            unit = str(row.get("Unit", "")).strip()
            proj_est_flag = str(row.get("ProjEstFlag", "")).strip()

            # Map to table_id
            table_id = self._map_table_id(report_title)

            # Build standardized cell record
            record = {
                # Release identification
                "wasde_number": int(row.get("WasdeNumber", 0) or 0),
                "report_date_text": str(row.get("ReportDate", "")),
                "release_date": str(row.get("ReleaseDate", "")),
                "release_time": str(row.get("ReleaseTime", "")),
                "forecast_year": str(row.get("ForecastYear", "")),
                "forecast_month": str(row.get("ForecastMonth", "")),
                # Cell location
                "table_id": table_id or "",
                "report_title": report_title,
                "row_id": self._make_row_id(attribute),
                "row_label": attribute,
                "column_id": market_year,
                "column_label": f"{market_year} {proj_est_flag}".strip(),
                # Commodity/region
                "commodity": self._normalize_commodity(commodity),
                "commodity_raw": commodity,
                "region": region,
                # Marketing year context
                "marketing_year": market_year,
                "projection_type": self._determine_projection_type(
                    proj_est_flag
                ),
                "proj_est_flag": proj_est_flag,
                "annual_quarter_flag": str(
                    row.get("AnnualQuarterFlag", "")
                ).strip(),
                # Values
                "value_text": value_text,
                "value_numeric": self._clean_value(value_text),
                "value_unit_text": unit,
                # Quality flags
                "is_numeric": self._clean_value(value_text) is not None,
                "reliability_projection": str(
                    row.get("ReliabilityProjection", "")
                ).strip(),
                # Row classification
                "row_category": ATTRIBUTE_TO_CATEGORY.get(attribute, "other"),
            }

            records.append(record)

        return records, release_info

    def parse_response(self, response_data: Any) -> Any:
        """Parse response - main parsing happens in _parse_csv."""
        return response_data

    # =========================================================================
    # Convenience methods
    # =========================================================================

    def get_latest_wasde(self) -> CollectorResult:
        """Fetch the most recent WASDE report."""
        today = date.today()
        # Try current month first, fall back to previous month
        result = self.collect(year=today.year, month=today.month)
        if not result.success and today.day < 15:
            # Before the 12th, current month may not be available yet
            prev = today - timedelta(days=30)
            result = self.collect(year=prev.year, month=prev.month)
        return result

    def get_us_balance_sheet(
        self, commodity: str, year: int = None, month: int = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get a US commodity balance sheet from the latest WASDE.

        Args:
            commodity: Commodity name (corn, soybeans, wheat, etc.)
            year: Report year (default: latest)
            month: Report month (default: latest)

        Returns:
            Dict with balance sheet rows keyed by attribute
        """
        if year and month:
            result = self.collect(year=year, month=month)
        else:
            result = self.get_latest_wasde()

        if not result.success or result.data is None:
            return None

        df = result.data
        norm = self._normalize_commodity(commodity)

        # Filter to US data for this commodity
        mask = (df["commodity"] == norm) & (
            df["region"].str.contains("United States", case=False, na=False)
        )
        us_data = df[mask]

        if us_data.empty:
            return None

        # Pivot into balance sheet format
        balance_sheet = {}
        for _, row in us_data.iterrows():
            my = row["marketing_year"]
            attr = row["row_label"]
            if my not in balance_sheet:
                balance_sheet[my] = {}
            balance_sheet[my][attr] = {
                "value": row["value_numeric"],
                "unit": row["value_unit_text"],
                "projection_type": row["projection_type"],
            }

        return balance_sheet

    def get_revision_summary(
        self,
        current_year: int,
        current_month: int,
        commodity: str = None,
    ) -> Optional[List[Dict]]:
        """
        Compare current WASDE to previous month and return revisions.

        Args:
            current_year: Year of current report
            current_month: Month of current report
            commodity: Optional commodity filter

        Returns:
            List of revision dicts with previous_value, current_value, change
        """
        # Get current and previous month
        current = self.collect(year=current_year, month=current_month)

        if current_month == 1:
            prev_year, prev_month = current_year - 1, 12
        else:
            prev_year, prev_month = current_year, current_month - 1

        previous = self.collect(year=prev_year, month=prev_month)

        if not current.success or not previous.success:
            return None

        df_curr = current.data
        df_prev = previous.data

        if commodity:
            norm = self._normalize_commodity(commodity)
            df_curr = df_curr[df_curr["commodity"] == norm]
            df_prev = df_prev[df_prev["commodity"] == norm]

        # Join on natural key: table_id + row_id + marketing_year
        merge_keys = ["table_id", "row_id", "marketing_year", "commodity"]
        merged = pd.merge(
            df_curr[merge_keys + ["value_numeric", "row_label", "value_unit_text"]],
            df_prev[merge_keys + ["value_numeric"]],
            on=merge_keys,
            suffixes=("_current", "_previous"),
            how="inner",
        )

        # Calculate changes
        merged["change"] = merged["value_numeric_current"] - merged["value_numeric_previous"]
        merged["change_pct"] = (
            merged["change"] / merged["value_numeric_previous"].abs() * 100
        ).round(2)

        # Filter to rows that actually changed
        changed = merged[merged["change"].abs() > 0.001]

        return changed.to_dict(orient="records")


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for WASDE collector."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="USDA WASDE Data Collector")

    parser.add_argument(
        "command",
        choices=["fetch", "latest", "balance", "revisions", "test"],
        help="Command to execute",
    )
    parser.add_argument("--year", "-y", type=int, help="Report year")
    parser.add_argument("--month", "-m", type=int, help="Report month")
    parser.add_argument(
        "--commodity", "-c", default="corn", help="Commodity (default: corn)"
    )
    parser.add_argument(
        "--backfill", action="store_true", help="Fetch historical data"
    )
    parser.add_argument("--output", "-o", help="Output file path")

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    collector = USDAWASPECollector()

    if args.command == "test":
        # Test connection to WASDE CSV endpoint
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == "latest":
        result = collector.get_latest_wasde()
        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")
        if result.warnings:
            print(f"Warnings: {result.warnings}")
        if result.success and PANDAS_AVAILABLE and hasattr(result.data, "head"):
            print(f"\nSample data (first 10 rows):")
            print(result.data.head(10).to_string())
        return

    if args.command == "fetch":
        kwargs = {}
        if args.year:
            kwargs["year"] = args.year
        if args.month:
            kwargs["month"] = args.month
        if args.backfill:
            kwargs["backfill"] = True

        result = collector.collect(**kwargs)
        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")
        if result.warnings:
            print(f"Warnings: {result.warnings}")

        if args.output and result.data is not None:
            if PANDAS_AVAILABLE and hasattr(result.data, "to_csv"):
                result.data.to_csv(args.output, index=False)
                print(f"Saved to: {args.output}")
        elif result.success and PANDAS_AVAILABLE and hasattr(result.data, "head"):
            print(f"\nSample data (first 10 rows):")
            print(result.data.head(10).to_string())
        return

    if args.command == "balance":
        bs = collector.get_us_balance_sheet(
            args.commodity, year=args.year, month=args.month
        )
        if bs:
            print(json.dumps(bs, indent=2, default=str))
        else:
            print(f"No balance sheet found for {args.commodity}")
        return

    if args.command == "revisions":
        if not args.year or not args.month:
            print("--year and --month required for revisions")
            return
        revisions = collector.get_revision_summary(
            args.year, args.month, args.commodity
        )
        if revisions:
            print(json.dumps(revisions, indent=2, default=str))
        else:
            print("No revisions found")
        return


if __name__ == "__main__":
    main()
