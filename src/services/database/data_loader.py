"""
Universal Data Loader

Loads data from various Excel/CSV formats into the commodity database.
Handles:
- Flattened format (Country, Commodity, Metric, Year, Value)
- Wide format (metrics as columns, dates as rows)
- Time series format (date, value1, value2, ...)
- Multi-sheet Excel workbooks

Usage:
    from database.data_loader import DataLoader

    loader = DataLoader("./data/commodities.db")
    loader.load_excel("path/to/file.xlsx")
    loader.load_folder("path/to/models/folder")
"""

import os
import sqlite3
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import re

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    raise ImportError("pandas is required for DataLoader")


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class LoadResult:
    """Result of a data load operation"""
    filename: str
    records_imported: int
    records_updated: int
    records_failed: int
    target_table: str
    duration_seconds: float
    errors: List[str]


class DataLoader:
    """
    Universal data loader for commodity market data.

    Auto-detects file format and loads into appropriate database table.
    """

    # Known column name mappings
    COLUMN_MAPPINGS = {
        # Country variations
        'country': 'country_name',
        'country_code': 'country_code',
        'region': 'country_name',
        'market': 'country_name',

        # Commodity variations
        'commodity': 'commodity_code',
        'commodity_code': 'commodity_code',
        'product': 'commodity_code',

        # Date/Time variations
        'date': 'report_date',
        'trade_date': 'trade_date',
        'report_date': 'report_date',
        'week_ending': 'week_ending',
        'year': 'calendar_year',
        'month': 'calendar_month',
        'marketing_year': 'marketing_year',
        'my': 'marketing_year',

        # Metric variations
        'statistic': 'metric',
        'metric': 'metric',
        'indicator': 'metric',
        'attribute': 'metric',
        'data_type': 'metric',

        # Value variations
        'value': 'value',
        'amount': 'value',
        'quantity': 'value',

        # Unit variations
        'unit': 'unit',
        'units': 'unit',
        'uom': 'unit',
    }

    # Commodity code normalizations
    COMMODITY_NORMALIZATIONS = {
        'soybean': 'SOYBEANS',
        'soybeans': 'SOYBEANS',
        'soy': 'SOYBEANS',
        'soybean oil': 'SOYBEAN_OIL',
        'soyoil': 'SOYBEAN_OIL',
        'soybean meal': 'SOYBEAN_MEAL',
        'soymeal': 'SOYBEAN_MEAL',
        'corn': 'CORN',
        'maize': 'CORN',
        'wheat': 'WHEAT_SRW',
        'wheat srw': 'WHEAT_SRW',
        'wheat hrw': 'WHEAT_HRW',
        'canola': 'CANOLA',
        'rapeseed': 'CANOLA',
        'palm oil': 'PALM_OIL',
        'palmoil': 'PALM_OIL',
        'cotton': 'COTTON',
        'sugar': 'SUGAR',
        'coffee': 'COFFEE',
        'crude oil': 'CRUDE_OIL',
        'crude': 'CRUDE_OIL',
        'wti': 'CRUDE_OIL',
        'natural gas': 'NATURAL_GAS',
        'natgas': 'NATURAL_GAS',
        'ethanol': 'ETHANOL',
        'biodiesel': 'BIODIESEL',
    }

    # Metric normalizations
    METRIC_NORMALIZATIONS = {
        'exports': 'EXPORTS',
        'export': 'EXPORTS',
        'imports': 'IMPORTS',
        'import': 'IMPORTS',
        'production': 'PRODUCTION',
        'crush': 'CRUSH',
        'domestic use': 'DOMESTIC_USE',
        'domestic usage': 'DOMESTIC_USE',
        'ending stocks': 'ENDING_STOCKS',
        'end-of-month stocks': 'ENDING_STOCKS',
        'beginning stocks': 'BEGINNING_STOCKS',
        'yield': 'YIELD',
        'harvested area': 'HARVESTED_AREA',
        'planted area': 'PLANTED_AREA',
        'total supply': 'TOTAL_SUPPLY',
        'total use': 'TOTAL_USE',
    }

    def __init__(self, db_path: str = "./data/commodities.db"):
        """Initialize the data loader"""
        self.db_path = db_path
        self.conn = None
        self._ensure_database()

    def _ensure_database(self):
        """Ensure database exists with schema"""
        from .schema import create_database
        if not Path(self.db_path).exists():
            create_database(self.db_path)

    def _get_connection(self) -> sqlite3.Connection:
        """Get database connection"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names"""
        # Lowercase all column names
        df.columns = [str(c).lower().strip() for c in df.columns]

        # Apply mappings
        rename_map = {}
        for col in df.columns:
            if col in self.COLUMN_MAPPINGS:
                rename_map[col] = self.COLUMN_MAPPINGS[col]

        if rename_map:
            df = df.rename(columns=rename_map)

        return df

    def _normalize_commodity(self, value: str) -> str:
        """Normalize commodity name to standard code"""
        if pd.isna(value):
            return None
        value_lower = str(value).lower().strip()
        return self.COMMODITY_NORMALIZATIONS.get(value_lower, value.upper().replace(' ', '_'))

    def _normalize_metric(self, value: str) -> str:
        """Normalize metric name"""
        if pd.isna(value):
            return None
        value_lower = str(value).lower().strip()
        return self.METRIC_NORMALIZATIONS.get(value_lower, value.upper().replace(' ', '_'))

    def _detect_format(self, df: pd.DataFrame) -> str:
        """
        Detect the data format of a DataFrame.

        Returns one of:
        - 'flattened': Has country, commodity, metric, value columns
        - 'timeseries': Has date column and multiple value columns
        - 'wide': Metrics/dates as columns
        - 'unknown': Cannot determine format
        """
        cols_lower = [str(c).lower() for c in df.columns]

        # Check for flattened format
        flattened_indicators = ['country', 'commodity', 'statistic', 'metric', 'value']
        if sum(1 for ind in flattened_indicators if any(ind in c for c in cols_lower)) >= 3:
            return 'flattened'

        # Check for time series format
        date_cols = [c for c in cols_lower if 'date' in c or c in ['year', 'month', 'week']]
        if date_cols and len(df.columns) > 2:
            return 'timeseries'

        # Check for wide format (years as columns)
        year_cols = [c for c in df.columns if re.match(r'^\d{4}(/\d{2})?$', str(c))]
        if len(year_cols) > 3:
            return 'wide'

        return 'unknown'

    def _load_flattened(
        self,
        df: pd.DataFrame,
        source: str = None
    ) -> Tuple[int, int, List[str]]:
        """Load data in flattened format into fundamentals table"""
        conn = self._get_connection()
        cursor = conn.cursor()

        df = self._normalize_columns(df)

        # Normalize values
        if 'commodity_code' in df.columns:
            df['commodity_code'] = df['commodity_code'].apply(self._normalize_commodity)
        if 'metric' in df.columns:
            df['metric'] = df['metric'].apply(self._normalize_metric)

        imported = 0
        updated = 0
        errors = []

        for _, row in df.iterrows():
            try:
                # Build record
                record = {
                    'commodity_code': row.get('commodity_code'),
                    'country_code': row.get('country_code'),
                    'country_name': row.get('country_name'),
                    'marketing_year': str(row.get('marketing_year', row.get('calendar_year', ''))),
                    'calendar_year': int(row['calendar_year']) if pd.notna(row.get('calendar_year')) else None,
                    'calendar_month': int(row['calendar_month']) if pd.notna(row.get('calendar_month')) else None,
                    'metric': row.get('metric'),
                    'value': float(row['value']) if pd.notna(row.get('value')) else None,
                    'unit': row.get('unit'),
                    'source': source or row.get('source'),
                }

                # Skip if no value
                if record['value'] is None:
                    continue

                # Insert or update
                cursor.execute("""
                    INSERT INTO fundamentals
                    (commodity_code, country_code, country_name, marketing_year,
                     calendar_year, calendar_month, metric, value, unit, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(commodity_code, country_code, marketing_year, calendar_month, metric, source)
                    DO UPDATE SET value = excluded.value
                """, (
                    record['commodity_code'],
                    record['country_code'],
                    record['country_name'],
                    record['marketing_year'],
                    record['calendar_year'],
                    record['calendar_month'],
                    record['metric'],
                    record['value'],
                    record['unit'],
                    record['source'],
                ))

                if cursor.rowcount > 0:
                    imported += 1

            except Exception as e:
                errors.append(f"Row error: {e}")

        conn.commit()
        return imported, updated, errors

    def _load_timeseries(
        self,
        df: pd.DataFrame,
        table_name: str,
        source: str = None
    ) -> Tuple[int, int, List[str]]:
        """Load time series data"""
        conn = self._get_connection()

        df = self._normalize_columns(df)

        # Identify date column
        date_col = None
        for col in df.columns:
            if 'date' in col.lower():
                date_col = col
                break

        if not date_col:
            return 0, 0, ["No date column found"]

        # Convert date column
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

        # Add source column
        df['source'] = source

        # Write to database
        try:
            df.to_sql(table_name, conn, if_exists='append', index=False)
            return len(df), 0, []
        except Exception as e:
            return 0, 0, [str(e)]

    def load_excel(
        self,
        filepath: str,
        sheet_name: str = None,
        target_table: str = None,
        source_name: str = None
    ) -> LoadResult:
        """
        Load an Excel file into the database.

        Args:
            filepath: Path to Excel file
            sheet_name: Specific sheet to load (default: all sheets)
            target_table: Force loading to specific table
            source_name: Source identifier

        Returns:
            LoadResult with import statistics
        """
        start_time = datetime.now()
        filepath = Path(filepath)

        if not filepath.exists():
            return LoadResult(
                filename=str(filepath),
                records_imported=0,
                records_updated=0,
                records_failed=0,
                target_table='',
                duration_seconds=0,
                errors=[f"File not found: {filepath}"]
            )

        source_name = source_name or filepath.stem
        total_imported = 0
        total_updated = 0
        all_errors = []

        try:
            xls = pd.ExcelFile(filepath)
            sheets = [sheet_name] if sheet_name else xls.sheet_names

            for sheet in sheets:
                logger.info(f"Loading sheet: {sheet}")
                df = pd.read_excel(xls, sheet_name=sheet)

                if df.empty:
                    continue

                # Detect format
                data_format = self._detect_format(df)
                logger.info(f"  Detected format: {data_format}")

                # Load based on format
                if data_format == 'flattened':
                    imported, updated, errors = self._load_flattened(df, source_name)
                    target = 'fundamentals'
                elif data_format == 'timeseries':
                    imported, updated, errors = self._load_timeseries(
                        df, target_table or 'raw_timeseries', source_name
                    )
                    target = target_table or 'raw_timeseries'
                else:
                    errors = [f"Unknown format for sheet {sheet}"]
                    imported = updated = 0
                    target = ''

                total_imported += imported
                total_updated += updated
                all_errors.extend(errors)

                logger.info(f"  Imported: {imported}, Errors: {len(errors)}")

        except Exception as e:
            all_errors.append(str(e))

        duration = (datetime.now() - start_time).total_seconds()

        # Log import
        self._log_import(
            filename=str(filepath),
            source_type='excel',
            records_imported=total_imported,
            records_failed=len(all_errors),
            status='success' if not all_errors else 'partial',
            errors=all_errors
        )

        return LoadResult(
            filename=str(filepath),
            records_imported=total_imported,
            records_updated=total_updated,
            records_failed=len(all_errors),
            target_table=target_table or 'auto',
            duration_seconds=duration,
            errors=all_errors
        )

    def load_csv(
        self,
        filepath: str,
        target_table: str = None,
        source_name: str = None
    ) -> LoadResult:
        """Load a CSV file into the database"""
        start_time = datetime.now()
        filepath = Path(filepath)

        source_name = source_name or filepath.stem
        errors = []

        try:
            df = pd.read_csv(filepath)

            if df.empty:
                return LoadResult(
                    filename=str(filepath),
                    records_imported=0,
                    records_updated=0,
                    records_failed=0,
                    target_table='',
                    duration_seconds=0,
                    errors=["Empty file"]
                )

            data_format = self._detect_format(df)

            if data_format == 'flattened':
                imported, updated, errors = self._load_flattened(df, source_name)
                target = 'fundamentals'
            elif data_format == 'timeseries':
                imported, updated, errors = self._load_timeseries(
                    df, target_table or 'raw_timeseries', source_name
                )
                target = target_table or 'raw_timeseries'
            else:
                return LoadResult(
                    filename=str(filepath),
                    records_imported=0,
                    records_updated=0,
                    records_failed=0,
                    target_table='',
                    duration_seconds=0,
                    errors=["Unknown data format"]
                )

        except Exception as e:
            errors.append(str(e))
            imported = updated = 0
            target = ''

        duration = (datetime.now() - start_time).total_seconds()

        return LoadResult(
            filename=str(filepath),
            records_imported=imported,
            records_updated=updated,
            records_failed=len(errors),
            target_table=target,
            duration_seconds=duration,
            errors=errors
        )

    def load_folder(
        self,
        folder_path: str,
        recursive: bool = True
    ) -> List[LoadResult]:
        """
        Load all Excel/CSV files from a folder.

        Args:
            folder_path: Path to folder
            recursive: Search subfolders

        Returns:
            List of LoadResult for each file
        """
        folder = Path(folder_path)
        results = []

        if not folder.exists():
            logger.error(f"Folder not found: {folder}")
            return results

        # Find all data files
        patterns = ['*.xlsx', '*.xls', '*.csv']
        files = []

        for pattern in patterns:
            if recursive:
                files.extend(folder.rglob(pattern))
            else:
                files.extend(folder.glob(pattern))

        logger.info(f"Found {len(files)} files in {folder}")

        for filepath in files:
            logger.info(f"Loading: {filepath}")

            if filepath.suffix.lower() in ['.xlsx', '.xls']:
                result = self.load_excel(filepath)
            else:
                result = self.load_csv(filepath)

            results.append(result)

            if result.errors:
                for err in result.errors[:3]:
                    logger.warning(f"  Error: {err}")

        # Summary
        total_imported = sum(r.records_imported for r in results)
        total_failed = sum(r.records_failed for r in results)
        logger.info(f"Total: {total_imported} records imported, {total_failed} failed")

        return results

    def _log_import(
        self,
        filename: str,
        source_type: str,
        records_imported: int,
        records_failed: int,
        status: str,
        errors: List[str]
    ):
        """Log import to database"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO data_imports
            (filename, source_type, records_imported, records_failed, status, error_message, import_completed)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            filename,
            source_type,
            records_imported,
            records_failed,
            status,
            '; '.join(errors[:5]) if errors else None,
            datetime.now()
        ))

        conn.commit()

    def get_import_history(self, limit: int = 20) -> pd.DataFrame:
        """Get recent import history"""
        conn = self._get_connection()
        return pd.read_sql(f"""
            SELECT * FROM data_imports
            ORDER BY import_completed DESC
            LIMIT {limit}
        """, conn)

    def get_table_stats(self) -> pd.DataFrame:
        """Get row counts for all tables"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """)

        tables = [row[0] for row in cursor.fetchall()]
        stats = []

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            stats.append({'table': table, 'rows': count})

        return pd.DataFrame(stats)

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None


# Convenience function
def load_historical_data(folder_path: str, db_path: str = "./data/commodities.db"):
    """
    Quick function to load all historical data from a folder.

    Args:
        folder_path: Path to folder with Excel/CSV files
        db_path: Path to database

    Returns:
        Summary of import results
    """
    loader = DataLoader(db_path)
    results = loader.load_folder(folder_path)

    # Print summary
    print("\n" + "=" * 60)
    print("IMPORT SUMMARY")
    print("=" * 60)

    total_imported = 0
    total_failed = 0

    for result in results:
        status = "OK" if not result.errors else "WARN"
        print(f"[{status}] {result.filename}: {result.records_imported} records")
        total_imported += result.records_imported
        total_failed += result.records_failed

    print("-" * 60)
    print(f"Total: {total_imported} imported, {total_failed} failed")
    print("\nTable Statistics:")
    print(loader.get_table_stats().to_string(index=False))

    loader.close()
    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        folder = sys.argv[1]
        load_historical_data(folder)
    else:
        print("Usage: python data_loader.py <folder_path>")
        print("\nExample: python data_loader.py 'C:/Users/torem/Dropbox/RLC Documents/Models'")
