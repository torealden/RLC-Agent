"""
USDA ERS Food Expenditure Series Collector

Collects monthly food sales data with taxes and tips from the USDA Economic
Research Service Food Expenditure Series.

Data Source: https://www.ers.usda.gov/data-products/food-expenditure-series
Direct CSV: https://www.ers.usda.gov/media/5200/monthly-sales-of-food-with-taxes-and-tips-for-all-purchasers-by-outlet-type.csv

Release Schedule:
- Monthly data: Released June through February (17th-21st of each month)
- Annual data: Released each June

Data includes:
- Monthly sales of food with taxes and tips
- Food at home (FAH) vs food away from home (FAFH)
- Breakdown by outlet type (grocery stores, restaurants, etc.)

Usage:
    from src.agents.collectors.us.ers_food_expenditure_collector import ERSFoodExpenditureCollector

    collector = ERSFoodExpenditureCollector()
    result = collector.collect()

    # Or with custom config
    config = FoodExpenditureConfig(cache_enabled=False)
    collector = ERSFoodExpenditureCollector(config)
    result = collector.fetch_data()
"""

import io
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import hashlib

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from .base_collector import (
    BaseCollector,
    CollectorConfig,
    CollectorResult,
    DataFrequency,
    AuthType
)

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class FoodExpenditureConfig(CollectorConfig):
    """USDA ERS Food Expenditure Series configuration"""
    source_name: str = "USDA ERS Food Expenditure"
    source_url: str = "https://www.ers.usda.gov/data-products/food-expenditure-series"
    auth_type: AuthType = AuthType.NONE
    frequency: DataFrequency = DataFrequency.MONTHLY

    # Direct download URLs for CSV files
    monthly_sales_csv_url: str = (
        "https://www.ers.usda.gov/media/5200/"
        "monthly-sales-of-food-with-taxes-and-tips-for-all-purchasers-by-outlet-type.csv"
    )

    # Alternative URLs (ERS sometimes changes these)
    alternative_urls: List[str] = field(default_factory=lambda: [
        "https://www.ers.usda.gov/webdocs/DataFiles/37549/monthly_sales.csv",
        "https://www.ers.usda.gov/webdocs/DataFiles/37549/FoodExpendituresMonthly.csv",
    ])

    # Expected outlet types in the data
    outlet_types: List[str] = field(default_factory=lambda: [
        "Grocery stores",
        "Other food stores",
        "Food services and drinking places",
        "Full-service restaurants",
        "Limited-service restaurants",
        "Hotels and motels",
        "Retail stores",
        "Recreation places",
        "Schools and colleges",
        "Total food at home",
        "Total food away from home",
        "Total food",
    ])


# =============================================================================
# COLLECTOR CLASS
# =============================================================================

class ERSFoodExpenditureCollector(BaseCollector):
    """
    Collector for USDA ERS Food Expenditure Series data.

    Fetches and parses monthly food sales data with taxes and tips,
    broken down by outlet type and food category.

    Features:
    - Automatic CSV parsing with flexible column detection
    - Support for multiple URL patterns (ERS changes URLs periodically)
    - Local file fallback for when web access is blocked
    - Caching with configurable TTL
    - Idempotent database upserts
    """

    def __init__(self, config: FoodExpenditureConfig = None):
        config = config or FoodExpenditureConfig()
        super().__init__(config)
        self.config: FoodExpenditureConfig = config

    def get_table_name(self) -> str:
        """Return the bronze table name for this data"""
        return "bronze.ers_food_sales_monthly"

    def test_connection(self) -> Tuple[bool, str]:
        """Test connection to ERS data source"""
        urls_to_try = [self.config.monthly_sales_csv_url] + self.config.alternative_urls

        for url in urls_to_try:
            try:
                response, error = self._make_request(url, timeout=15)
                if response and response.status_code == 200:
                    return True, f"Connection successful to {url}"
            except Exception:
                continue

        return False, "Could not connect to any ERS Food Expenditure URLs"

    def fetch_data(
        self,
        start_date: date = None,
        end_date: date = None,
        local_file_path: str = None,
        **kwargs
    ) -> CollectorResult:
        """
        Fetch Food Expenditure data from USDA ERS.

        Args:
            start_date: Filter data from this date (optional)
            end_date: Filter data to this date (optional)
            local_file_path: Path to local CSV file (if web download fails)

        Returns:
            CollectorResult with parsed data
        """
        if not PANDAS_AVAILABLE:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="pandas required for CSV parsing"
            )

        start_time = datetime.now()
        csv_content = None
        source_url = None

        # Try downloading from web first
        if not local_file_path:
            csv_content, source_url = self._download_csv()

        # Fall back to local file if provided
        if csv_content is None and local_file_path:
            logger.info(f"Using local file: {local_file_path}")
            try:
                with open(local_file_path, 'rb') as f:
                    csv_content = f.read()
                source_url = f"file://{local_file_path}"
            except Exception as e:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message=f"Failed to read local file: {e}"
                )

        if csv_content is None:
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message="Failed to obtain CSV data from web or local file"
            )

        # Parse the CSV content
        try:
            records, warnings = self._parse_csv(csv_content, source_url)

            if not records:
                return CollectorResult(
                    success=False,
                    source=self.config.source_name,
                    error_message="No records parsed from CSV",
                    warnings=warnings
                )

            # Filter by date if specified
            if start_date or end_date:
                records = self._filter_by_date(records, start_date, end_date)

            # Convert to DataFrame
            result_df = pd.DataFrame(records)

            elapsed_ms = int((datetime.now() - start_time).total_seconds() * 1000)

            return CollectorResult(
                success=True,
                source=self.config.source_name,
                records_fetched=len(records),
                data=result_df,
                response_time_ms=elapsed_ms,
                warnings=warnings,
                data_as_of=datetime.now().strftime("%Y-%m-%d")
            )

        except Exception as e:
            logger.error(f"CSV parsing error: {e}", exc_info=True)
            return CollectorResult(
                success=False,
                source=self.config.source_name,
                error_message=f"CSV parsing error: {e}"
            )

    def _download_csv(self) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Attempt to download CSV from known URLs.

        Returns:
            Tuple of (content_bytes, source_url) or (None, None) if all fail
        """
        urls_to_try = [self.config.monthly_sales_csv_url] + self.config.alternative_urls

        for url in urls_to_try:
            logger.info(f"Attempting download from: {url}")
            try:
                response, error = self._make_request(url, timeout=60)

                if error:
                    logger.warning(f"Request error for {url}: {error}")
                    continue

                if response.status_code == 200:
                    content_type = response.headers.get('content-type', '').lower()
                    if 'csv' in content_type or 'text' in content_type or response.content:
                        logger.info(f"Successfully downloaded from {url}")
                        return response.content, url

                elif response.status_code == 403:
                    logger.warning(f"Access forbidden (403) for {url}")

                else:
                    logger.warning(f"HTTP {response.status_code} for {url}")

            except Exception as e:
                logger.warning(f"Exception downloading from {url}: {e}")

        return None, None

    def _parse_csv(
        self,
        content: bytes,
        source_url: str
    ) -> Tuple[List[Dict], List[str]]:
        """
        Parse CSV content into records.

        Handles various CSV formats that ERS might use:
        - Wide format (months as columns)
        - Long format (month as a column)
        - With or without header rows

        Returns:
            Tuple of (records_list, warnings_list)
        """
        records = []
        warnings = []

        # Try to decode the content
        try:
            text_content = content.decode('utf-8')
        except UnicodeDecodeError:
            text_content = content.decode('latin-1')

        # Try to read with pandas, handling various formats
        df = None

        # Try standard CSV read first
        try:
            df = pd.read_csv(io.StringIO(text_content))
            logger.info(f"Parsed CSV with {len(df)} rows and columns: {list(df.columns)}")
        except Exception as e:
            warnings.append(f"Standard CSV parse failed: {e}")

        # If that failed, try with header detection
        if df is None or df.empty:
            try:
                # Find the header row by looking for 'year' or 'month' or date-like values
                lines = text_content.strip().split('\n')
                header_row = 0
                for i, line in enumerate(lines[:20]):  # Check first 20 lines
                    lower_line = line.lower()
                    if 'year' in lower_line or 'month' in lower_line or 'outlet' in lower_line:
                        header_row = i
                        break

                df = pd.read_csv(io.StringIO(text_content), skiprows=header_row)
                logger.info(f"Parsed CSV with header at row {header_row}")
            except Exception as e:
                warnings.append(f"Header detection parse failed: {e}")

        if df is None or df.empty:
            return [], warnings + ["Could not parse CSV content"]

        # Detect format and extract records
        columns_lower = [str(c).lower().strip() for c in df.columns]

        # Check if this is wide format (months as columns)
        month_columns = self._detect_month_columns(df.columns)

        if month_columns:
            # Wide format: pivot to long
            records, parse_warnings = self._parse_wide_format(df, month_columns, source_url)
        else:
            # Long format: direct extraction
            records, parse_warnings = self._parse_long_format(df, source_url)

        warnings.extend(parse_warnings)
        return records, warnings

    def _detect_month_columns(self, columns) -> List[Tuple[str, int, int]]:
        """
        Detect if columns represent months (wide format).

        Returns list of (column_name, year, month) tuples.
        """
        month_columns = []

        # Common month patterns
        month_names = {
            'jan': 1, 'january': 1,
            'feb': 2, 'february': 2,
            'mar': 3, 'march': 3,
            'apr': 4, 'april': 4,
            'may': 5,
            'jun': 6, 'june': 6,
            'jul': 7, 'july': 7,
            'aug': 8, 'august': 8,
            'sep': 9, 'sept': 9, 'september': 9,
            'oct': 10, 'october': 10,
            'nov': 11, 'november': 11,
            'dec': 12, 'december': 12,
        }

        for col in columns:
            col_str = str(col).strip()
            col_lower = col_str.lower()

            # Try pattern like "Jan 2024" or "January 2024"
            for month_name, month_num in month_names.items():
                if month_name in col_lower:
                    # Extract year
                    year_match = re.search(r'(19|20)\d{2}', col_str)
                    if year_match:
                        year = int(year_match.group())
                        month_columns.append((col, year, month_num))
                        break

            # Try pattern like "2024-01" or "2024/01"
            date_match = re.match(r'(19|20)(\d{2})[-/](\d{1,2})', col_str)
            if date_match:
                year = int(date_match.group(1) + date_match.group(2))
                month = int(date_match.group(3))
                if 1 <= month <= 12:
                    month_columns.append((col, year, month))

        return month_columns

    def _parse_wide_format(
        self,
        df: pd.DataFrame,
        month_columns: List[Tuple[str, int, int]],
        source_url: str
    ) -> Tuple[List[Dict], List[str]]:
        """Parse wide format CSV where months are columns."""
        records = []
        warnings = []

        # Find the outlet type column
        outlet_col = None
        for col in df.columns:
            col_lower = str(col).lower()
            if 'outlet' in col_lower or 'category' in col_lower or 'type' in col_lower:
                outlet_col = col
                break

        if outlet_col is None:
            # Use first column as outlet type
            outlet_col = df.columns[0]
            warnings.append(f"Using first column '{outlet_col}' as outlet type")

        # Process each row
        for idx, row in df.iterrows():
            outlet_type = str(row[outlet_col]).strip()

            if not outlet_type or outlet_type.lower() in ['nan', 'none', '']:
                continue

            # Extract values for each month column
            for col_name, year, month in month_columns:
                value = row.get(col_name)

                if pd.isna(value):
                    continue

                # Parse numeric value
                numeric_value = self._parse_numeric_value(value)

                if numeric_value is not None:
                    records.append({
                        'year': year,
                        'month': month,
                        'outlet_type': outlet_type,
                        'purchaser_type': 'All purchasers',
                        'food_category': self._categorize_outlet(outlet_type),
                        'sales_value': numeric_value,
                        'raw_value_text': str(value),
                        'raw_row_data': json.dumps(row.to_dict(), default=str),
                        'source_file': source_url,
                        'source_row_number': idx + 1,
                        'data_revision': datetime.now().strftime('%B %Y'),
                    })

        return records, warnings

    def _parse_long_format(
        self,
        df: pd.DataFrame,
        source_url: str
    ) -> Tuple[List[Dict], List[str]]:
        """Parse long format CSV where each row is one observation."""
        records = []
        warnings = []

        # Normalize column names
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'year' in col_lower:
                col_map['year'] = col
            elif 'month' in col_lower:
                col_map['month'] = col
            elif 'outlet' in col_lower or 'category' in col_lower or 'type' in col_lower:
                col_map['outlet_type'] = col
            elif 'value' in col_lower or 'sales' in col_lower or 'amount' in col_lower:
                col_map['sales_value'] = col
            elif 'purchaser' in col_lower:
                col_map['purchaser_type'] = col

        # Validate required columns
        if 'year' not in col_map:
            warnings.append("No 'year' column found")
            return [], warnings

        # Process each row
        for idx, row in df.iterrows():
            try:
                year_val = row.get(col_map.get('year'))
                if pd.isna(year_val):
                    continue

                year = int(year_val)
                month = int(row.get(col_map.get('month'), 1)) if 'month' in col_map else 1

                outlet_type = str(row.get(col_map.get('outlet_type', df.columns[1]), 'Unknown'))
                purchaser_type = str(row.get(col_map.get('purchaser_type', 'All purchasers'), 'All purchasers'))

                # Get sales value from any value-like column
                sales_value = None
                raw_value = None
                for col in df.columns:
                    if col in col_map.values():
                        continue
                    val = row.get(col)
                    if pd.notna(val):
                        numeric = self._parse_numeric_value(val)
                        if numeric is not None:
                            sales_value = numeric
                            raw_value = str(val)
                            break

                if 'sales_value' in col_map:
                    val = row.get(col_map['sales_value'])
                    sales_value = self._parse_numeric_value(val)
                    raw_value = str(val)

                if sales_value is not None:
                    records.append({
                        'year': year,
                        'month': month,
                        'outlet_type': outlet_type.strip(),
                        'purchaser_type': purchaser_type.strip(),
                        'food_category': self._categorize_outlet(outlet_type),
                        'sales_value': sales_value,
                        'raw_value_text': raw_value,
                        'raw_row_data': json.dumps(row.to_dict(), default=str),
                        'source_file': source_url,
                        'source_row_number': idx + 1,
                        'data_revision': datetime.now().strftime('%B %Y'),
                    })

            except Exception as e:
                warnings.append(f"Row {idx}: {e}")
                continue

        return records, warnings

    def _parse_numeric_value(self, value: Any) -> Optional[float]:
        """Parse a value to numeric, handling various formats."""
        if pd.isna(value):
            return None

        if isinstance(value, (int, float)):
            return float(value)

        # Try string parsing
        try:
            val_str = str(value).strip()

            # Remove common formatting
            val_str = val_str.replace(',', '')
            val_str = val_str.replace('$', '')
            val_str = val_str.replace('%', '')

            # Handle parentheses for negative numbers
            if val_str.startswith('(') and val_str.endswith(')'):
                val_str = '-' + val_str[1:-1]

            # Handle text markers
            if val_str.lower() in ['na', 'n/a', '--', '-', '.', '']:
                return None

            return float(val_str)

        except (ValueError, TypeError):
            return None

    def _categorize_outlet(self, outlet_type: str) -> str:
        """Categorize outlet type into food at home vs away from home."""
        outlet_lower = outlet_type.lower()

        fah_keywords = ['grocery', 'supermarket', 'food store', 'warehouse', 'club']
        fafh_keywords = ['restaurant', 'hotel', 'school', 'hospital', 'vending',
                         'drinking', 'cafeteria', 'catering', 'recreation']

        for keyword in fah_keywords:
            if keyword in outlet_lower:
                return 'Food at home'

        for keyword in fafh_keywords:
            if keyword in outlet_lower:
                return 'Food away from home'

        if 'at home' in outlet_lower or 'fah' in outlet_lower:
            return 'Food at home'

        if 'away' in outlet_lower or 'fafh' in outlet_lower:
            return 'Food away from home'

        if 'total' in outlet_lower:
            return 'Total'

        return 'Other'

    def _filter_by_date(
        self,
        records: List[Dict],
        start_date: Optional[date],
        end_date: Optional[date]
    ) -> List[Dict]:
        """Filter records by date range."""
        filtered = []

        for record in records:
            year = record.get('year')
            month = record.get('month', 1)

            try:
                record_date = date(year, month, 1)

                if start_date and record_date < start_date:
                    continue
                if end_date and record_date > end_date:
                    continue

                filtered.append(record)

            except (ValueError, TypeError):
                # Include records with invalid dates
                filtered.append(record)

        return filtered

    def parse_response(self, response_data: Any) -> Any:
        """Required by BaseCollector - handled in fetch_data"""
        return response_data

    def save_to_database(
        self,
        records: List[Dict],
        db_connection,
        ingest_run_id: str = None
    ) -> Tuple[int, List[str]]:
        """
        Save records to the bronze database table.

        Args:
            records: List of record dictionaries
            db_connection: Database connection (SQLAlchemy or psycopg2)
            ingest_run_id: UUID for tracking this ingestion run

        Returns:
            Tuple of (records_saved, errors_list)
        """
        saved = 0
        errors = []

        upsert_sql = """
            INSERT INTO bronze.ers_food_sales_monthly (
                year, month, outlet_type, purchaser_type, food_category,
                sales_value, raw_value_text, raw_row_data,
                source_file, source_row_number, data_revision, ingest_run_id
            ) VALUES (
                %(year)s, %(month)s, %(outlet_type)s, %(purchaser_type)s, %(food_category)s,
                %(sales_value)s, %(raw_value_text)s, %(raw_row_data)s,
                %(source_file)s, %(source_row_number)s, %(data_revision)s, %(ingest_run_id)s
            )
            ON CONFLICT (year, month, outlet_type,
                         COALESCE(purchaser_type, 'ALL'),
                         COALESCE(food_category, 'TOTAL'))
            DO UPDATE SET
                sales_value = EXCLUDED.sales_value,
                raw_value_text = EXCLUDED.raw_value_text,
                raw_row_data = EXCLUDED.raw_row_data,
                data_revision = EXCLUDED.data_revision,
                ingest_run_id = EXCLUDED.ingest_run_id,
                updated_at = NOW()
        """

        for record in records:
            try:
                record['ingest_run_id'] = ingest_run_id

                cursor = db_connection.cursor()
                cursor.execute(upsert_sql, record)
                saved += 1

            except Exception as e:
                errors.append(f"Row {record.get('source_row_number')}: {e}")

        db_connection.commit()
        return saved, errors


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Command line interface for the Food Expenditure Collector."""
    import argparse

    parser = argparse.ArgumentParser(
        description='Collect USDA ERS Food Expenditure Series data'
    )
    parser.add_argument(
        '--local-file', '-f',
        help='Path to local CSV file (if web download unavailable)'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output file path (CSV or JSON)'
    )
    parser.add_argument(
        '--no-cache',
        action='store_true',
        help='Disable caching'
    )
    parser.add_argument(
        '--test-connection',
        action='store_true',
        help='Test connection to ERS data source'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose logging'
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create collector
    config = FoodExpenditureConfig(cache_enabled=not args.no_cache)
    collector = ERSFoodExpenditureCollector(config)

    # Test connection if requested
    if args.test_connection:
        success, message = collector.test_connection()
        print(f"Connection test: {'SUCCESS' if success else 'FAILED'}")
        print(f"Message: {message}")
        return

    # Run collection
    print(f"\n{'='*60}")
    print("USDA ERS Food Expenditure Series Collector")
    print(f"{'='*60}\n")

    result = collector.fetch_data(local_file_path=args.local_file)

    print(f"Success: {result.success}")
    print(f"Records fetched: {result.records_fetched}")
    print(f"Response time: {result.response_time_ms}ms")

    if result.warnings:
        print(f"\nWarnings:")
        for w in result.warnings[:5]:
            print(f"  - {w}")
        if len(result.warnings) > 5:
            print(f"  ... and {len(result.warnings) - 5} more")

    if result.error_message:
        print(f"\nError: {result.error_message}")

    # Save output if requested
    if args.output and result.data is not None and PANDAS_AVAILABLE:
        df = result.data
        if args.output.endswith('.csv'):
            df.to_csv(args.output, index=False)
        elif args.output.endswith('.json'):
            df.to_json(args.output, orient='records', indent=2)
        else:
            df.to_csv(args.output, index=False)
        print(f"\nSaved to: {args.output}")

    # Show sample data
    if result.success and result.data is not None and PANDAS_AVAILABLE:
        print(f"\nSample data (first 5 rows):")
        print(result.data.head())
        print(f"\nColumns: {list(result.data.columns)}")
        print(f"\nDate range: {result.data['year'].min()}-{result.data['month'].min():02d} to "
              f"{result.data['year'].max()}-{result.data['month'].max():02d}")


if __name__ == '__main__':
    main()
