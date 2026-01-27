"""
Internal Data Agent

Handles fetching and parsing internal HB spreadsheet data from Dropbox
or database sources. Provides a unified interface for supply/demand data,
forecasts, and other internal estimates.
"""

import logging
import hashlib
import io
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import pandas as pd

from ..config.settings import (
    HBWeeklyReportConfig,
    DataSourceType,
    DropboxConfig,
    DatabaseConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class InternalDataResult:
    """Result of internal data fetch operation"""
    success: bool
    source: DataSourceType
    fetched_at: datetime = field(default_factory=datetime.utcnow)

    # Data
    supply_demand: Optional[Dict[str, pd.DataFrame]] = None  # commodity -> DataFrame
    forecasts: Optional[Dict[str, pd.DataFrame]] = None
    raw_data: Optional[pd.DataFrame] = None

    # Metadata
    file_path: Optional[str] = None
    file_hash: Optional[str] = None
    last_modified: Optional[datetime] = None

    # Validation
    missing_fields: List[str] = field(default_factory=list)
    validation_warnings: List[str] = field(default_factory=list)
    error_message: Optional[str] = None


class InternalDataAgent:
    """
    Agent for retrieving internal HB data from spreadsheets or database

    Supports:
    - Dropbox: Downloads and parses Excel files
    - Database: Queries structured tables (future)

    Provides unified interface regardless of source.
    """

    def __init__(self, config: HBWeeklyReportConfig, db_session_factory=None):
        """
        Initialize Internal Data Agent

        Args:
            config: HB Weekly Report configuration
            db_session_factory: SQLAlchemy session factory for database access
        """
        self.config = config
        self.db_session_factory = db_session_factory
        self.logger = logging.getLogger(f"{self.__class__.__name__}")

        # Dropbox client (lazy initialization)
        self._dropbox_client = None

        # Cache
        self._cached_data: Optional[InternalDataResult] = None
        self._cache_timestamp: Optional[datetime] = None

        self.logger.info(f"Initialized InternalDataAgent with source: {config.internal_data_source.value}")

    @property
    def dropbox_client(self):
        """Lazy initialization of Dropbox client"""
        if self._dropbox_client is None and self.config.dropbox.enabled:
            self._dropbox_client = self._create_dropbox_client()
        return self._dropbox_client

    def _create_dropbox_client(self):
        """Create Dropbox client with authentication"""
        try:
            import dropbox
            from dropbox.exceptions import AuthError

            dbx_config = self.config.dropbox

            # Try access token first, then refresh token
            if dbx_config.access_token:
                client = dropbox.Dropbox(dbx_config.access_token)
            elif dbx_config.refresh_token and dbx_config.app_key:
                client = dropbox.Dropbox(
                    oauth2_refresh_token=dbx_config.refresh_token,
                    app_key=dbx_config.app_key,
                    app_secret=dbx_config.app_secret,
                )
            else:
                self.logger.warning("Dropbox credentials not configured")
                return None

            # Test connection
            try:
                client.users_get_current_account()
                self.logger.info("Dropbox client connected successfully")
            except AuthError as e:
                self.logger.error(f"Dropbox authentication failed: {e}")
                return None

            return client

        except ImportError:
            self.logger.error("dropbox package not installed. Run: pip install dropbox")
            return None
        except Exception as e:
            self.logger.error(f"Failed to create Dropbox client: {e}")
            return None

    def fetch_data(self, force_refresh: bool = False) -> InternalDataResult:
        """
        Fetch internal data from configured source

        Args:
            force_refresh: Bypass cache and fetch fresh data

        Returns:
            InternalDataResult with fetched data
        """
        # Check cache
        if not force_refresh and self._cached_data and self._cache_timestamp:
            cache_age = (datetime.utcnow() - self._cache_timestamp).total_seconds()
            if cache_age < 3600:  # 1 hour cache
                self.logger.info("Returning cached internal data")
                return self._cached_data

        # Fetch based on configured source
        if self.config.internal_data_source == DataSourceType.DROPBOX:
            result = self._fetch_from_dropbox()
        elif self.config.internal_data_source == DataSourceType.DATABASE:
            result = self._fetch_from_database()
        else:
            result = InternalDataResult(
                success=False,
                source=self.config.internal_data_source,
                error_message=f"Unsupported data source: {self.config.internal_data_source}"
            )

        # Update cache
        if result.success:
            self._cached_data = result
            self._cache_timestamp = datetime.utcnow()

        return result

    def _fetch_from_dropbox(self) -> InternalDataResult:
        """Fetch data from Dropbox spreadsheet"""
        result = InternalDataResult(
            success=False,
            source=DataSourceType.DROPBOX
        )

        if not self.dropbox_client:
            result.error_message = "Dropbox client not available"
            return result

        try:
            import dropbox

            dbx_config = self.config.dropbox
            file_path = f"{dbx_config.data_folder}/{dbx_config.hb_spreadsheet_name}"

            self.logger.info(f"Fetching spreadsheet from Dropbox: {file_path}")

            # Download file
            try:
                metadata, response = self.dropbox_client.files_download(file_path)
            except dropbox.exceptions.ApiError as e:
                # Try alternate path patterns
                alt_paths = [
                    f"{dbx_config.root_folder}/{dbx_config.hb_spreadsheet_name}",
                    f"/rlc documents/{dbx_config.hb_spreadsheet_name}",
                ]
                found = False
                for alt_path in alt_paths:
                    try:
                        metadata, response = self.dropbox_client.files_download(alt_path)
                        file_path = alt_path
                        found = True
                        break
                    except:
                        continue

                if not found:
                    result.error_message = f"File not found: {file_path}"
                    return result

            # Get file content
            file_content = response.content
            result.file_hash = hashlib.sha256(file_content).hexdigest()
            result.file_path = file_path
            result.last_modified = metadata.server_modified

            # Parse Excel file
            excel_buffer = io.BytesIO(file_content)
            result = self._parse_spreadsheet(excel_buffer, result)

            return result

        except Exception as e:
            self.logger.error(f"Error fetching from Dropbox: {e}", exc_info=True)
            result.error_message = str(e)
            return result

    def _fetch_from_database(self) -> InternalDataResult:
        """Fetch data from database (future implementation)"""
        result = InternalDataResult(
            success=False,
            source=DataSourceType.DATABASE
        )

        if not self.db_session_factory:
            result.error_message = "Database session factory not configured"
            return result

        try:
            # Future: Query database for internal data
            # This will be implemented when database migration occurs

            self.logger.info("Database fetch not yet implemented - using placeholder")

            # For now, return empty but successful result
            result.success = True
            result.supply_demand = {}
            result.forecasts = {}
            result.validation_warnings.append("Database source not yet implemented")

            return result

        except Exception as e:
            self.logger.error(f"Error fetching from database: {e}", exc_info=True)
            result.error_message = str(e)
            return result

    def _parse_spreadsheet(self, excel_buffer: io.BytesIO, result: InternalDataResult) -> InternalDataResult:
        """Parse Excel spreadsheet into structured data"""
        try:
            dbx_config = self.config.dropbox
            commodity_config = self.config.commodities

            # Read all sheets
            excel_file = pd.ExcelFile(excel_buffer)
            available_sheets = excel_file.sheet_names

            self.logger.info(f"Available sheets: {available_sheets}")

            # Parse Supply/Demand sheet
            supply_demand = {}
            if dbx_config.supply_demand_sheet in available_sheets:
                sd_df = pd.read_excel(excel_buffer, sheet_name=dbx_config.supply_demand_sheet)
                supply_demand = self._parse_supply_demand_sheet(sd_df, commodity_config)
            else:
                result.validation_warnings.append(f"Sheet '{dbx_config.supply_demand_sheet}' not found")

            # Parse Forecasts sheet
            forecasts = {}
            if dbx_config.forecast_sheet in available_sheets:
                fc_df = pd.read_excel(excel_buffer, sheet_name=dbx_config.forecast_sheet)
                forecasts = self._parse_forecast_sheet(fc_df, commodity_config)
            else:
                result.validation_warnings.append(f"Sheet '{dbx_config.forecast_sheet}' not found")

            # Parse Price Data sheet
            price_data = None
            if dbx_config.price_data_sheet in available_sheets:
                price_data = pd.read_excel(excel_buffer, sheet_name=dbx_config.price_data_sheet)

            # Validate data completeness
            missing = self._validate_data_completeness(supply_demand, commodity_config)
            result.missing_fields = missing

            result.supply_demand = supply_demand
            result.forecasts = forecasts
            result.raw_data = price_data
            result.success = True

            self.logger.info(
                f"Parsed spreadsheet: {len(supply_demand)} commodities, "
                f"{len(missing)} missing fields, {len(result.validation_warnings)} warnings"
            )

            return result

        except Exception as e:
            self.logger.error(f"Error parsing spreadsheet: {e}", exc_info=True)
            result.error_message = f"Parse error: {str(e)}"
            return result

    def _parse_supply_demand_sheet(
        self,
        df: pd.DataFrame,
        commodity_config
    ) -> Dict[str, pd.DataFrame]:
        """Parse supply/demand data for each commodity"""
        supply_demand = {}

        # Common column name patterns
        col_patterns = {
            "production": ["production", "prod", "output"],
            "beginning_stocks": ["beginning stocks", "beg stocks", "opening stocks", "beginning_stocks"],
            "total_supply": ["total supply", "supply", "total_supply"],
            "exports": ["exports", "export", "exp"],
            "ending_stocks": ["ending stocks", "end stocks", "ending_stocks"],
            "stocks_to_use": ["stocks to use", "s/u", "stocks/use", "stocks_to_use"],
        }

        for commodity in commodity_config.primary_commodities:
            try:
                # Try to find commodity section in DataFrame
                # This will depend on actual spreadsheet structure
                commodity_df = self._extract_commodity_section(df, commodity, col_patterns)
                if commodity_df is not None and not commodity_df.empty:
                    supply_demand[commodity] = commodity_df
            except Exception as e:
                self.logger.warning(f"Could not parse {commodity} from supply/demand: {e}")

        return supply_demand

    def _extract_commodity_section(
        self,
        df: pd.DataFrame,
        commodity: str,
        col_patterns: Dict[str, List[str]]
    ) -> Optional[pd.DataFrame]:
        """Extract data for a specific commodity from the supply/demand sheet"""
        # This is a simplified implementation - actual parsing depends on sheet format
        # The real implementation would need to handle various spreadsheet layouts

        try:
            # Look for commodity name in the DataFrame
            commodity_variations = [
                commodity.lower(),
                commodity.upper(),
                commodity.title(),
                commodity.replace("_", " "),
            ]

            # Try to find the commodity section
            for idx, row in df.iterrows():
                row_str = str(row.values).lower()
                if any(var in row_str for var in commodity_variations):
                    # Found potential start of commodity section
                    # Extract relevant rows
                    section_start = idx
                    section_end = min(idx + 20, len(df))  # Assume max 20 rows per commodity

                    section_df = df.iloc[section_start:section_end].copy()
                    return section_df

            return None

        except Exception as e:
            self.logger.warning(f"Error extracting {commodity} section: {e}")
            return None

    def _parse_forecast_sheet(
        self,
        df: pd.DataFrame,
        commodity_config
    ) -> Dict[str, pd.DataFrame]:
        """Parse forecast data for each commodity"""
        forecasts = {}

        for commodity in commodity_config.primary_commodities:
            try:
                # Similar parsing logic as supply/demand
                commodity_df = self._extract_commodity_section(df, commodity, {})
                if commodity_df is not None and not commodity_df.empty:
                    forecasts[commodity] = commodity_df
            except Exception as e:
                self.logger.warning(f"Could not parse {commodity} forecasts: {e}")

        return forecasts

    def _validate_data_completeness(
        self,
        supply_demand: Dict[str, pd.DataFrame],
        commodity_config
    ) -> List[str]:
        """Validate that all required data fields are present"""
        missing = []

        for commodity in commodity_config.primary_commodities:
            if commodity not in supply_demand:
                missing.append(f"{commodity}: entire section missing")
                continue

            required_fields = commodity_config.supply_demand_fields.get(commodity, [])
            commodity_df = supply_demand[commodity]

            for field in required_fields:
                # Check if field exists in the DataFrame (simplified check)
                field_found = False
                for col in commodity_df.columns:
                    if field.lower() in str(col).lower():
                        field_found = True
                        break

                if not field_found:
                    missing.append(f"{commodity}.{field}")

        return missing

    def get_supply_demand_value(
        self,
        commodity: str,
        field: str,
        marketing_year: str = None
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        Get a specific supply/demand value

        Args:
            commodity: Commodity name
            field: Field name (e.g., "production", "exports")
            marketing_year: Optional marketing year (e.g., "2024/25")

        Returns:
            Tuple of (value, source_description)
        """
        if not self._cached_data or not self._cached_data.success:
            # Fetch data if not cached
            self.fetch_data()

        if not self._cached_data or not self._cached_data.supply_demand:
            return None, "No data available"

        commodity_data = self._cached_data.supply_demand.get(commodity.lower())
        if commodity_data is None:
            return None, f"No data for commodity: {commodity}"

        # Search for field value (simplified - depends on actual data structure)
        try:
            for col in commodity_data.columns:
                if field.lower() in str(col).lower():
                    # Get the most recent value or specific year
                    values = commodity_data[col].dropna()
                    if len(values) > 0:
                        return float(values.iloc[-1]), f"Internal spreadsheet ({self._cached_data.file_path})"
        except Exception as e:
            self.logger.warning(f"Error getting {commodity}.{field}: {e}")

        return None, f"Field not found: {field}"

    def get_hb_vs_usda_comparison(
        self,
        commodity: str,
        field: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get comparison of HB estimate vs USDA

        Returns:
            Dict with 'hb_value', 'usda_value', 'difference', 'pct_difference'
        """
        hb_value, _ = self.get_supply_demand_value(commodity, field)

        if hb_value is None:
            return None

        # USDA comparison would come from separate source
        # This is a placeholder for the actual comparison logic
        return {
            "hb_value": hb_value,
            "usda_value": None,  # Would be fetched from USDA source
            "difference": None,
            "pct_difference": None,
        }

    def get_data_freshness(self) -> Dict[str, Any]:
        """Get information about data freshness"""
        if not self._cached_data:
            return {"status": "no_data", "last_fetch": None}

        return {
            "status": "available" if self._cached_data.success else "error",
            "last_fetch": self._cache_timestamp.isoformat() if self._cache_timestamp else None,
            "file_modified": self._cached_data.last_modified.isoformat() if self._cached_data.last_modified else None,
            "file_hash": self._cached_data.file_hash,
            "missing_fields_count": len(self._cached_data.missing_fields),
            "warnings_count": len(self._cached_data.validation_warnings),
        }
