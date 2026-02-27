"""
USDA WASDE Collector (via FAS PSD API)

Collects WASDE supply/demand balance sheet data through the FAS Production,
Supply, and Distribution (PSD) API. PSD is the underlying database that
WASDE reports draw from -- updated monthly at noon ET on WASDE release day.

API base:  https://api.fas.usda.gov
Auth:      X-Api-Key header (register free at https://api.data.gov/signup/)

Key endpoints:
    /api/psd/commodity/{code}/country/{cc}/year/{my}  - single country
    /api/psd/commodity/{code}/country/all/year/{my}    - all countries
    /api/psd/commodity/{code}/world/year/{my}          - world aggregate
    /api/psd/commodity/{code}/dataReleaseDates          - last update dates

Reference:
    /api/psd/commodities          - list commodity codes
    /api/psd/countries             - list country codes
    /api/psd/commodityAttributes   - list attribute IDs
    /api/psd/unitsOfMeasure        - list unit IDs

See: domain_knowledge/data_dictionaries/usda_fas_psd_api_reference.json
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple

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


# ============================================================================
# PSD commodity codes -- full WASDE coverage plus biofuel-relevant oils
# ============================================================================
PSD_COMMODITY_CODES: Dict[str, Dict[str, str]] = {
    # Grains
    "corn":             {"code": "0440000", "name": "Corn",              "unit": "1000 MT"},
    "wheat":            {"code": "0410000", "name": "Wheat",             "unit": "1000 MT"},
    "rice":             {"code": "0422110", "name": "Rice, Milled",      "unit": "1000 MT"},
    "barley":           {"code": "0430000", "name": "Barley",            "unit": "1000 MT"},
    "sorghum":          {"code": "0459100", "name": "Sorghum",           "unit": "1000 MT"},
    # Oilseeds
    "soybeans":         {"code": "2222000", "name": "Soybeans",          "unit": "1000 MT"},
    "rapeseed":         {"code": "1205000", "name": "Rapeseed (Canola)", "unit": "1000 MT"},
    "sunflowerseed":    {"code": "1206000", "name": "Sunflowerseed",    "unit": "1000 MT"},
    "peanuts":          {"code": "1202000", "name": "Peanuts",           "unit": "1000 MT"},
    "cottonseed":       {"code": "1207200", "name": "Cottonseed",        "unit": "1000 MT"},
    # Oilseed products
    "soybean_meal":     {"code": "2304000", "name": "Meal, Soybean",    "unit": "1000 MT"},
    "soybean_oil":      {"code": "1507000", "name": "Oil, Soybean",     "unit": "1000 MT"},
    "palm_oil":         {"code": "1511000", "name": "Oil, Palm",         "unit": "1000 MT"},
    "palm_kernel_oil":  {"code": "1513200", "name": "Oil, Palm Kernel",  "unit": "1000 MT"},
    "rapeseed_oil":     {"code": "1514000", "name": "Oil, Rapeseed",     "unit": "1000 MT"},
    "rapeseed_meal":    {"code": "2306400", "name": "Meal, Rapeseed",    "unit": "1000 MT"},
    "sunflowerseed_oil":{"code": "1512000", "name": "Oil, Sunflowerseed","unit": "1000 MT"},
    "sunflowerseed_meal":{"code":"2306300", "name": "Meal, Sunflowerseed","unit":"1000 MT"},
    "cottonseed_oil":   {"code": "1512200", "name": "Oil, Cottonseed",   "unit": "1000 MT"},
    "cottonseed_meal":  {"code": "2306100", "name": "Meal, Cottonseed",  "unit": "1000 MT"},
    # Fiber
    "cotton":           {"code": "2631000", "name": "Cotton",            "unit": "1000 480-lb Bales"},
    # Sugar
    "sugar":            {"code": "1701000", "name": "Sugar, Centrifugal","unit": "1000 MT"},
}

# Key countries tracked in WASDE analysis
PSD_COUNTRY_CODES: Dict[str, str] = {
    "US": "United States",
    "BR": "Brazil",
    "AR": "Argentina",
    "CN": "China",
    "EU": "European Union",
    "RU": "Russia",
    "UA": "Ukraine",
    "AU": "Australia",
    "CA": "Canada",
    "IN": "India",
    "ID": "Indonesia",
    "MY": "Malaysia",
    "TH": "Thailand",
    "MX": "Mexico",
    "EG": "Egypt",
    "JP": "Japan",
    "KR": "Korea, South",
    "PK": "Pakistan",
    "VN": "Vietnam",
    "PH": "Philippines",
    "NG": "Nigeria",
    "ZA": "South Africa",
    "KZ": "Kazakhstan",
    "PY": "Paraguay",
    "TR": "Turkey",
}

# Default commodity list for a standard WASDE pull
DEFAULT_WASDE_COMMODITIES = [
    "corn", "wheat", "soybeans", "soybean_meal", "soybean_oil",
    "rice", "sorghum", "barley", "cotton", "sugar",
    "palm_oil", "rapeseed", "rapeseed_oil", "sunflowerseed_oil",
]

# Default country list for a standard WASDE pull
DEFAULT_WASDE_COUNTRIES = [
    "US", "BR", "AR", "CN", "EU", "RU", "UA", "AU", "CA", "IN",
    "ID", "MY",
]

# PSD API base URL
PSD_API_BASE = "https://api.fas.usda.gov"


@dataclass
class USDAWASPEConfig(CollectorConfig):
    """USDA WASDE / PSD collector configuration."""

    source_name: str = "USDA WASDE"
    source_url: str = PSD_API_BASE
    auth_type: AuthType = AuthType.API_KEY
    frequency: DataFrequency = DataFrequency.MONTHLY
    timeout: int = 60
    rate_limit_per_minute: int = 30  # conservative; registered key allows 1000/hr

    # API key from api.data.gov (free registration)
    api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("USDA_FAS_API_KEY", "")
    )

    # Which commodities to fetch
    commodities: List[str] = field(default_factory=lambda: list(DEFAULT_WASDE_COMMODITIES))

    # Which countries to fetch (use "all" for every country, or "world" for aggregate)
    countries: List[str] = field(default_factory=lambda: list(DEFAULT_WASDE_COUNTRIES))

    # Which marketing years to fetch
    marketing_years: List[int] = field(default_factory=lambda: [
        date.today().year - 1, date.today().year,
    ])

    # Whether to also pull world aggregate
    include_world: bool = True


class USDAWASPECollector(BaseCollector):
    """
    Collector for WASDE data via the FAS PSD API.

    PSD is the canonical database underlying WASDE reports. It is updated
    on WASDE release day (typically the 12th of each month at noon ET)
    and contains complete global S&D balance sheets for all commodities
    covered in WASDE -- including palm oil, rapeseed, sunflowerseed, etc.
    that are absent from the USDA OCE CSV download.

    Features:
    - Fetches structured JSON from PSD API (no PDF/CSV parsing)
    - Comprehensive commodity coverage (grains + oilseeds + oils + fiber + sugar)
    - Global country coverage (US, Brazil, Argentina, China, EU, etc.)
    - World aggregate data
    - Release-date detection to know when new data is available
    - CLI for testing and ad-hoc queries
    """

    def __init__(self, config: USDAWASPEConfig = None):
        config = config or USDAWASPEConfig()
        super().__init__(config)
        self.config: USDAWASPEConfig = config

        # Override the default auth header -- PSD uses X-Api-Key, not Bearer
        if self.config.api_key:
            self.session.headers.pop("Authorization", None)
            self.session.headers["X-Api-Key"] = self.config.api_key

    def get_table_name(self) -> str:
        return "bronze.fas_psd"

    # =========================================================================
    # PSD API helpers
    # =========================================================================

    def _psd_url(self, *path_parts: str) -> str:
        """Build a PSD API URL from path parts."""
        path = "/".join(str(p) for p in path_parts)
        return f"{PSD_API_BASE}/api/psd/{path}"

    def _safe_float(self, value: Any) -> Optional[float]:
        """Safely convert a value to float."""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any) -> Optional[int]:
        """Safely convert a value to int."""
        if value is None or value == "":
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    # =========================================================================
    # Data fetching
    # =========================================================================

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        commodities: List[str] = None,
        countries: List[str] = None,
        marketing_years: List[int] = None,
        **kwargs,
    ) -> CollectorResult:
        """
        Fetch PSD supply/demand data from the FAS API.

        Args:
            start_date / end_date:  Ignored (PSD is by marketing year, not date range).
            commodities:  List of commodity keys (default: config.commodities).
            countries:    List of 2-letter country codes, or ["all"] or ["world"]
                          (default: config.countries).
            marketing_years:  List of marketing year ints (default: config.marketing_years).

        Returns:
            CollectorResult with parsed S&D records as a DataFrame.
        """
        commodities = commodities or self.config.commodities
        countries = countries or self.config.countries
        marketing_years = marketing_years or self.config.marketing_years

        if not self.config.api_key:
            self.logger.warning(
                "No USDA_FAS_API_KEY set. Register free at https://api.data.gov/signup/ "
                "and set USDA_FAS_API_KEY env var."
            )

        all_records: List[Dict] = []
        warnings: List[str] = []

        for commodity in commodities:
            if commodity not in PSD_COMMODITY_CODES:
                warnings.append(f"Unknown commodity: {commodity}")
                continue

            commodity_info = PSD_COMMODITY_CODES[commodity]
            code = commodity_info["code"]

            for my in marketing_years:
                # --- per-country queries ---
                for cc in countries:
                    records, warn = self._fetch_commodity_country_year(
                        commodity, code, cc, my
                    )
                    all_records.extend(records)
                    if warn:
                        warnings.append(warn)

                # --- world aggregate ---
                if self.config.include_world:
                    records, warn = self._fetch_commodity_world_year(
                        commodity, code, my
                    )
                    all_records.extend(records)
                    if warn:
                        warnings.append(warn)

        if not all_records:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="No PSD data retrieved",
                warnings=warnings,
            )

        # Build DataFrame
        if PANDAS_AVAILABLE:
            df = pd.DataFrame(all_records)
        else:
            df = all_records

        return CollectorResult(
            success=True,
            source=self.config.source_name,
            records_fetched=len(all_records),
            data=df,
            data_as_of=date.today().isoformat(),
            warnings=warnings,
        )

    def _fetch_commodity_country_year(
        self,
        commodity: str,
        code: str,
        country_code: str,
        marketing_year: int,
    ) -> Tuple[List[Dict], Optional[str]]:
        """Fetch one commodity/country/year from PSD API and parse response."""
        if country_code.lower() == "all":
            url = self._psd_url("commodity", code, "country", "all", "year", marketing_year)
        else:
            url = self._psd_url("commodity", code, "country", country_code, "year", marketing_year)

        response, error = self._make_request(url)

        if error:
            return [], f"{commodity}/{country_code}/MY{marketing_year}: {error}"

        if response.status_code == 401:
            return [], f"{commodity}: API key invalid or missing (HTTP 401)"

        if response.status_code == 429:
            return [], f"{commodity}: Rate limited (HTTP 429)"

        if response.status_code != 200:
            return [], f"{commodity}/{country_code}/MY{marketing_year}: HTTP {response.status_code}"

        try:
            data = response.json()
            records = [
                self._parse_psd_record(rec, commodity)
                for rec in data
                if rec is not None
            ]
            records = [r for r in records if r is not None]
            return records, None
        except Exception as e:
            return [], f"{commodity}/{country_code}/MY{marketing_year}: parse error - {e}"

    def _fetch_commodity_world_year(
        self,
        commodity: str,
        code: str,
        marketing_year: int,
    ) -> Tuple[List[Dict], Optional[str]]:
        """Fetch world aggregate for a commodity/year."""
        url = self._psd_url("commodity", code, "world", "year", marketing_year)

        response, error = self._make_request(url)

        if error:
            return [], f"{commodity}/world/MY{marketing_year}: {error}"

        if response.status_code != 200:
            return [], f"{commodity}/world/MY{marketing_year}: HTTP {response.status_code}"

        try:
            data = response.json()
            records = []
            for rec in data:
                parsed = self._parse_psd_record(rec, commodity)
                if parsed:
                    parsed["country_code"] = "WD"
                    parsed["country"] = "World"
                    records.append(parsed)
            return records, None
        except Exception as e:
            return [], f"{commodity}/world/MY{marketing_year}: parse error - {e}"

    def _parse_psd_record(
        self, record: Dict, commodity: str
    ) -> Optional[Dict]:
        """
        Parse a single PSD API response record into standardised format.

        PSD response fields (per reference doc):
            commodityCode, countryCode, marketYear, attributeId,
            value, unitId
        """
        try:
            commodity_info = PSD_COMMODITY_CODES.get(commodity, {})
            country_code = record.get("countryCode", "")
            country_name = PSD_COUNTRY_CODES.get(country_code, country_code)

            return {
                "commodity": commodity,
                "commodity_code": record.get("commodityCode", commodity_info.get("code", "")),
                "country_code": country_code,
                "country": record.get("countryDescription", country_name),
                "marketing_year": self._safe_int(record.get("marketYear")),
                "attribute_id": self._safe_int(record.get("attributeId")),
                "attribute_name": record.get("attributeName", ""),
                "value": self._safe_float(record.get("value")),
                "unit_id": self._safe_int(record.get("unitId")),
                "unit": record.get("unitDescription", commodity_info.get("unit", "1000 MT")),
                "month": record.get("month"),

                # Convenience fields matching bronze.fas_psd schema
                "beginning_stocks": self._safe_float(record.get("beginningStocks")),
                "production": self._safe_float(record.get("production")),
                "imports": self._safe_float(record.get("imports")),
                "total_supply": self._safe_float(record.get("totalSupply")),
                "domestic_consumption": self._safe_float(record.get("domesticConsumption")),
                "feed_dom_consumption": self._safe_float(record.get("feedDomConsumption")),
                "fsi_consumption": self._safe_float(record.get("fsiConsumption")),
                "crush": self._safe_float(record.get("crush")),
                "exports": self._safe_float(record.get("exports")),
                "ending_stocks": self._safe_float(record.get("endingStocks")),
                "total_distribution": self._safe_float(record.get("totalDistribution")),
                "area_harvested": self._safe_float(record.get("areaHarvested")),
                "yield_per_hectare": self._safe_float(record.get("yieldPerHectare")),

                "source": "USDA_FAS_PSD",
            }
        except Exception as e:
            self.logger.warning(f"Error parsing PSD record: {e}")
            return None

    def parse_response(self, response_data: Any) -> Any:
        """Parse response -- main parsing happens in _parse_psd_record."""
        return response_data

    # =========================================================================
    # Release-date detection
    # =========================================================================

    def get_data_release_dates(self, commodity: str = "corn") -> Optional[List[str]]:
        """
        Check when PSD data was last updated for a commodity.

        Useful for detecting fresh WASDE data availability.
        """
        code = PSD_COMMODITY_CODES.get(commodity, {}).get("code")
        if not code:
            return None

        url = self._psd_url("commodity", code, "dataReleaseDates")
        response, error = self._make_request(url)

        if error or response.status_code != 200:
            return None

        try:
            return response.json()
        except Exception:
            return None

    # =========================================================================
    # Convenience methods
    # =========================================================================

    def get_us_balance_sheet(
        self, commodity: str, marketing_year: int = None
    ) -> Optional[Dict]:
        """
        Fetch US S&D balance sheet for a commodity.

        Args:
            commodity:      Commodity key (corn, soybeans, wheat, etc.)
            marketing_year: Marketing year (default: current)

        Returns:
            Dict with S&D attributes and values.
        """
        my = marketing_year or date.today().year
        code = PSD_COMMODITY_CODES.get(commodity, {}).get("code")
        if not code:
            return None

        records, _ = self._fetch_commodity_country_year(commodity, code, "US", my)
        if not records:
            return None

        # Collapse into single balance sheet dict
        bs = {"commodity": commodity, "country": "US", "marketing_year": my}
        for rec in records:
            for key in [
                "beginning_stocks", "production", "imports", "total_supply",
                "domestic_consumption", "feed_dom_consumption", "fsi_consumption",
                "crush", "exports", "ending_stocks", "total_distribution",
                "area_harvested", "yield_per_hectare",
            ]:
                if rec.get(key) is not None:
                    bs[key] = rec[key]
            # Also capture attribute-based fields
            attr = rec.get("attribute_name", "")
            val = rec.get("value")
            if attr and val is not None:
                bs[attr] = val

        return bs

    def get_global_production_ranking(
        self, commodity: str, marketing_year: int = None, top_n: int = 10
    ) -> Optional[List[Dict]]:
        """
        Fetch production rankings for a commodity across all countries.

        Args:
            commodity:      Commodity key.
            marketing_year: Marketing year (default: current).
            top_n:          Number of top producers to return.
        """
        my = marketing_year or date.today().year
        code = PSD_COMMODITY_CODES.get(commodity, {}).get("code")
        if not code:
            return None

        records, _ = self._fetch_commodity_country_year(commodity, code, "all", my)
        if not records:
            return None

        if PANDAS_AVAILABLE:
            df = pd.DataFrame(records)
            # Filter to rows with production data
            prod = df[df["production"].notna() & (df["production"] > 0)]
            prod = prod.sort_values("production", ascending=False).head(top_n)
            return prod[["country", "country_code", "production", "exports", "ending_stocks"]].to_dict(orient="records")

        # Non-pandas fallback
        with_prod = [r for r in records if r.get("production") and r["production"] > 0]
        with_prod.sort(key=lambda r: r["production"], reverse=True)
        return with_prod[:top_n]

    # =========================================================================
    # Reference data
    # =========================================================================

    def list_commodities(self) -> Optional[List[Dict]]:
        """Fetch the full list of PSD commodity codes."""
        url = self._psd_url("commodities")
        response, error = self._make_request(url)
        if error or response.status_code != 200:
            return None
        return response.json()

    def list_countries(self) -> Optional[List[Dict]]:
        """Fetch the full list of PSD country codes."""
        url = self._psd_url("countries")
        response, error = self._make_request(url)
        if error or response.status_code != 200:
            return None
        return response.json()

    def list_attributes(self) -> Optional[List[Dict]]:
        """Fetch the full list of PSD attribute codes."""
        url = self._psd_url("commodityAttributes")
        response, error = self._make_request(url)
        if error or response.status_code != 200:
            return None
        return response.json()


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command-line interface for WASDE / PSD collector."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="USDA WASDE (PSD API) Collector")

    parser.add_argument(
        "command",
        choices=["fetch", "balance", "ranking", "releases", "commodities", "countries", "test"],
        help="Command to execute",
    )
    parser.add_argument("--commodity", "-c", default="corn", help="Commodity key")
    parser.add_argument("--country", default=None, help="Country code (2-letter)")
    parser.add_argument("--year", "-y", type=int, default=None, help="Marketing year")
    parser.add_argument("--output", "-o", help="Output file path (CSV or JSON)")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    collector = USDAWASPECollector()

    if args.command == "test":
        success, message = collector.test_connection()
        print(f"Connection test: {'PASS' if success else 'FAIL'} - {message}")
        return

    if args.command == "commodities":
        data = collector.list_commodities()
        print(json.dumps(data, indent=2) if data else "Failed to fetch commodities")
        return

    if args.command == "countries":
        data = collector.list_countries()
        print(json.dumps(data, indent=2) if data else "Failed to fetch countries")
        return

    if args.command == "releases":
        dates = collector.get_data_release_dates(args.commodity)
        print(json.dumps(dates, indent=2) if dates else "Failed to fetch release dates")
        return

    if args.command == "balance":
        bs = collector.get_us_balance_sheet(args.commodity, args.year)
        print(json.dumps(bs, indent=2, default=str) if bs else f"No data for {args.commodity}")
        return

    if args.command == "ranking":
        ranking = collector.get_global_production_ranking(args.commodity, args.year)
        if ranking:
            print(json.dumps(ranking, indent=2, default=str))
        else:
            print(f"No ranking data for {args.commodity}")
        return

    if args.command == "fetch":
        commodities = [args.commodity] if args.commodity else None
        countries = [args.country] if args.country else None
        years = [args.year] if args.year else None

        result = collector.collect(
            commodities=commodities,
            countries=countries,
            marketing_years=years,
        )
        print(f"Success: {result.success}")
        print(f"Records: {result.records_fetched}")
        if result.warnings:
            print(f"Warnings: {result.warnings}")

        if args.output and result.data is not None:
            if PANDAS_AVAILABLE and hasattr(result.data, "to_csv"):
                if args.output.endswith(".csv"):
                    result.data.to_csv(args.output, index=False)
                else:
                    result.data.to_json(args.output, orient="records", indent=2)
                print(f"Saved to: {args.output}")
        elif result.success and PANDAS_AVAILABLE and hasattr(result.data, "head"):
            print(f"\nSample data (first 10 rows):")
            cols = ["commodity", "country_code", "marketing_year",
                    "production", "exports", "ending_stocks"]
            avail = [c for c in cols if c in result.data.columns]
            print(result.data[avail].head(10).to_string())


if __name__ == "__main__":
    main()
