#!/usr/bin/env python3
"""
Brazil Soybean Weekly Report - Data Ingestion

This script loads raw data files (CSV, XLS) from the Dropbox sync folder
or local raw directory and standardizes them for the weekly data pack.

Usage:
    # Load all available data for current week
    python ingest_brazil_soy_data.py

    # Load specific source
    python ingest_brazil_soy_data.py --source cepea_paranagua

    # Load from specific date
    python ingest_brazil_soy_data.py --date 2026-01-22

    # Validate only (don't write processed files)
    python ingest_brazil_soy_data.py --validate-only
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import json
import glob

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.brazil_soy_report.brazil_soy_config import (
    DATA_SOURCES,
    RAW_DIR,
    PROCESSED_DIR,
    get_report_week_dates,
    get_expected_filename,
    VALIDATION_RULES,
)

# Optional imports
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("WARNING: pandas not available. Install with: pip install pandas openpyxl")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# FILE DISCOVERY
# =============================================================================

def find_raw_files(source: str, start_date: date, end_date: date) -> List[Path]:
    """
    Find raw files for a source within date range.

    Looks for files matching the naming convention:
    YYYY-MM-DD__SOURCE__*.ext
    """
    source_config = DATA_SOURCES.get(source)
    if not source_config:
        logger.error(f"Unknown source: {source}")
        return []

    found_files = []

    # Search in RAW_DIR for matching files
    patterns = [
        f"*{source_config.name}*",
        f"*{source.upper()}*",
    ]

    for pattern in patterns:
        for ext in ['.csv', '.xls', '.xlsx']:
            for fpath in RAW_DIR.glob(f"*{pattern}*{ext}"):
                # Check if date in filename is within range
                fname = fpath.name
                try:
                    # Extract date from YYYY-MM-DD__ prefix
                    if fname[:10].replace('-', '').isdigit():
                        file_date = datetime.strptime(fname[:10], "%Y-%m-%d").date()
                        if start_date <= file_date <= end_date:
                            found_files.append(fpath)
                    else:
                        # No date prefix - include anyway
                        found_files.append(fpath)
                except ValueError:
                    # Can't parse date - include file for manual review
                    found_files.append(fpath)

    return list(set(found_files))


def find_latest_file(source: str) -> Optional[Path]:
    """Find the most recent file for a source."""
    source_config = DATA_SOURCES.get(source)
    if not source_config:
        return None

    pattern = source_config.file_pattern
    files = list(RAW_DIR.glob(pattern))

    if not files:
        # Try broader pattern
        files = list(RAW_DIR.glob(f"*{source_config.name}*"))

    if not files:
        return None

    # Sort by modification time, newest first
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files[0]


# =============================================================================
# DATA LOADING
# =============================================================================

def load_cepea_paranagua(filepath: Path) -> Optional[pd.DataFrame]:
    """
    Load CEPEA Paranagua price data from XLS.

    Expected columns: Data, À vista R$, À prazo R$, ...
    We use: Data, À vista R$
    """
    if not PANDAS_AVAILABLE:
        logger.error("pandas required for Excel files")
        return None

    try:
        # Try reading as Excel
        df = pd.read_excel(filepath, sheet_name=0)

        # Normalize column names (handle variations)
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'data' in col_lower:
                col_map[col] = 'date'
            elif 'vista' in col_lower and 'r$' in col_lower:
                col_map[col] = 'price_brl'
            elif 'vista' in col_lower:
                col_map[col] = 'price_brl'

        df = df.rename(columns=col_map)

        # Ensure required columns
        if 'date' not in df.columns or 'price_brl' not in df.columns:
            logger.error(f"Missing required columns. Found: {df.columns.tolist()}")
            return None

        # Parse dates
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])

        # Parse prices (handle Brazilian format: 1.234,56)
        if df['price_brl'].dtype == object:
            df['price_brl'] = (
                df['price_brl']
                .astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .astype(float)
            )

        # Add metadata
        df['source'] = 'CEPEA'
        df['series'] = 'soy_paranagua_cash'
        df['unit'] = 'BRL/60kg'

        logger.info(f"Loaded {len(df)} rows from CEPEA Paranagua")
        return df[['date', 'price_brl', 'source', 'series', 'unit']]

    except Exception as e:
        logger.error(f"Error loading CEPEA file: {e}")
        return None


def load_cbot_futures(filepath: Path) -> Optional[pd.DataFrame]:
    """
    Load CBOT Soy Futures from Barchart CSV.

    Expected columns: Date, Contract, Open, High, Low, Settle, Volume, ...
    We use: Date, Contract, Settle
    """
    if not PANDAS_AVAILABLE:
        logger.error("pandas required")
        return None

    try:
        df = pd.read_csv(filepath)

        # Normalize columns
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

        # Map common variations
        col_map = {
            'time': 'date',
            'last': 'settle',
            'close': 'settle',
            'settlement': 'settle',
            'symbol': 'contract',
        }
        df = df.rename(columns=col_map)

        # Ensure date column
        date_cols = [c for c in df.columns if 'date' in c]
        if date_cols:
            df['date'] = pd.to_datetime(df[date_cols[0]], errors='coerce')
        else:
            logger.error("No date column found in CBOT file")
            return None

        # Ensure settle column
        if 'settle' not in df.columns:
            logger.error("No settle/close column found")
            return None

        # Filter for soybean contracts (ZS, S)
        if 'contract' in df.columns:
            df = df[df['contract'].str.contains('ZS|^S[FGHJKMNQUVXZ]', regex=True, na=False)]

        df = df.dropna(subset=['date', 'settle'])

        # Add metadata
        df['source'] = 'CBOT'
        df['series'] = 'soy_futures'
        df['unit'] = 'USc/bu'

        # Rename settle to price
        df = df.rename(columns={'settle': 'price_usc'})

        logger.info(f"Loaded {len(df)} rows from CBOT futures")
        return df[['date', 'contract', 'price_usc', 'source', 'series', 'unit']]

    except Exception as e:
        logger.error(f"Error loading CBOT file: {e}")
        return None


def load_imea_mt(filepath: Path) -> Optional[pd.DataFrame]:
    """
    Load IMEA Mato Grosso soy prices.

    Filter for: Indicador = 'Preço soja disponível compra' (spot price)
    """
    if not PANDAS_AVAILABLE:
        return None

    try:
        df = pd.read_excel(filepath, sheet_name=0)

        # Normalize columns
        col_map = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if 'data' in col_lower:
                col_map[col] = 'date'
            elif 'indicador' in col_lower:
                col_map[col] = 'indicator'
            elif 'preço' in col_lower or 'preco' in col_lower:
                col_map[col] = 'price_brl'
            elif 'valor' in col_lower:
                col_map[col] = 'price_brl'

        df = df.rename(columns=col_map)

        # Filter for spot price indicator
        if 'indicator' in df.columns:
            spot_mask = df['indicator'].str.contains(
                'disponível|disponivel|compra|spot',
                case=False, na=False
            )
            df = df[spot_mask]

        # Parse date
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])

        # Parse price (Brazilian format)
        if 'price_brl' in df.columns and df['price_brl'].dtype == object:
            df['price_brl'] = (
                df['price_brl']
                .astype(str)
                .str.replace('.', '', regex=False)
                .str.replace(',', '.', regex=False)
                .astype(float)
            )

        df['source'] = 'IMEA'
        df['series'] = 'soy_mt_spot'
        df['unit'] = 'BRL/sc'

        logger.info(f"Loaded {len(df)} rows from IMEA MT")
        return df[['date', 'price_brl', 'source', 'series', 'unit']]

    except Exception as e:
        logger.error(f"Error loading IMEA file: {e}")
        return None


def load_anec_exports(filepath: Path) -> Optional[pd.DataFrame]:
    """
    Load ANEC export data from manual CSV.

    Expected: Week, Soy_MMT, Meal_MMT, YTD_2025, YTD_2026
    """
    if not PANDAS_AVAILABLE:
        return None

    try:
        df = pd.read_csv(filepath)
        df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

        # Add metadata
        df['source'] = 'ANEC'
        df['series'] = 'soy_exports'
        df['unit'] = 'MMT'

        logger.info(f"Loaded {len(df)} rows from ANEC exports")
        return df

    except Exception as e:
        logger.error(f"Error loading ANEC file: {e}")
        return None


def load_noaa_weather(filepath: Path) -> Optional[pd.DataFrame]:
    """
    Load NOAA weather signal from manual CSV.

    Expected: Week, Signal (dry/neutral/wet), Notes
    """
    if not PANDAS_AVAILABLE:
        return None

    try:
        df = pd.read_csv(filepath)
        df.columns = [c.strip().lower() for c in df.columns]

        df['source'] = 'NOAA'
        df['series'] = 'brazil_weather_signal'

        logger.info(f"Loaded {len(df)} rows from NOAA weather")
        return df

    except Exception as e:
        logger.error(f"Error loading NOAA file: {e}")
        return None


# Loader dispatch
LOADERS = {
    'cepea_paranagua': load_cepea_paranagua,
    'cbot_futures': load_cbot_futures,
    'imea_mt': load_imea_mt,
    'anec_exports': load_anec_exports,
    'noaa_weather': load_noaa_weather,
}


# =============================================================================
# VALIDATION
# =============================================================================

def validate_data(source: str, df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate loaded data against configured rules.

    Returns: (is_valid, list_of_warnings)
    """
    warnings = []
    rules = VALIDATION_RULES.get(source, {})

    if df is None or len(df) == 0:
        return False, ["No data loaded"]

    # Check for price column
    price_col = None
    for col in ['price_brl', 'price_usc', 'value']:
        if col in df.columns:
            price_col = col
            break

    if price_col and rules:
        prices = df[price_col].dropna()

        # Check min/max bounds
        if 'min_value' in rules and prices.min() < rules['min_value']:
            warnings.append(f"Price below minimum: {prices.min()} < {rules['min_value']}")

        if 'max_value' in rules and prices.max() > rules['max_value']:
            warnings.append(f"Price above maximum: {prices.max()} > {rules['max_value']}")

        # Check for suspicious daily changes
        if 'max_daily_change_pct' in rules and len(prices) > 1:
            pct_changes = prices.pct_change().abs() * 100
            max_change = pct_changes.max()
            if max_change > rules['max_daily_change_pct']:
                warnings.append(f"Large daily change: {max_change:.1f}% > {rules['max_daily_change_pct']}%")

    # Check for duplicates
    if 'date' in df.columns:
        dupe_count = df.duplicated(subset=['date']).sum()
        if dupe_count > 0:
            warnings.append(f"{dupe_count} duplicate dates found")

    is_valid = len([w for w in warnings if 'below' in w.lower() or 'above' in w.lower()]) == 0
    return is_valid, warnings


# =============================================================================
# MAIN INGESTION WORKFLOW
# =============================================================================

def ingest_source(
    source: str,
    start_date: date,
    end_date: date,
    validate_only: bool = False
) -> Dict[str, Any]:
    """
    Ingest data for a single source.

    Returns dict with status, data summary, and any warnings.
    """
    result = {
        'source': source,
        'status': 'not_found',
        'rows': 0,
        'date_range': None,
        'warnings': [],
        'file_path': None,
    }

    # Find files
    files = find_raw_files(source, start_date, end_date)

    if not files:
        # Try finding latest file regardless of date
        latest = find_latest_file(source)
        if latest:
            files = [latest]
            result['warnings'].append(f"Using latest file (not dated): {latest.name}")

    if not files:
        result['warnings'].append(f"No files found for {source}")
        return result

    # Use most recent file
    filepath = files[0]
    result['file_path'] = str(filepath)

    # Load data
    loader = LOADERS.get(source)
    if not loader:
        result['status'] = 'no_loader'
        result['warnings'].append(f"No loader implemented for {source}")
        return result

    df = loader(filepath)

    if df is None or len(df) == 0:
        result['status'] = 'load_failed'
        return result

    # Validate
    is_valid, warnings = validate_data(source, df)
    result['warnings'].extend(warnings)
    result['rows'] = len(df)

    if 'date' in df.columns:
        result['date_range'] = {
            'min': str(df['date'].min()),
            'max': str(df['date'].max()),
        }

    if not validate_only and PANDAS_AVAILABLE:
        # Save processed data
        output_file = PROCESSED_DIR / f"{source}_{end_date.strftime('%Y%m%d')}.csv"
        df.to_csv(output_file, index=False)
        result['output_file'] = str(output_file)

    result['status'] = 'valid' if is_valid else 'warnings'
    return result


def ingest_all(
    report_date: date = None,
    validate_only: bool = False
) -> Dict[str, Any]:
    """
    Ingest all data sources for a report week.

    Returns summary of all ingestion results.
    """
    week = get_report_week_dates(report_date)
    start_date = week['start_wed']
    end_date = week['end_wed']

    logger.info(f"Ingesting data for week: {week['week_label']}")

    results = {
        'report_week': week['week_label'],
        'start_date': str(start_date),
        'end_date': str(end_date),
        'timestamp': datetime.now().isoformat(),
        'sources': {},
    }

    for source in DATA_SOURCES:
        logger.info(f"Processing {source}...")
        result = ingest_source(source, start_date, end_date, validate_only)
        results['sources'][source] = result

        # Print status
        status_icon = {
            'valid': '[OK]',
            'warnings': '[WARN]',
            'not_found': '[MISSING]',
            'load_failed': '[FAIL]',
            'no_loader': '[SKIP]',
        }.get(result['status'], '[?]')

        print(f"  {status_icon} {source}: {result['rows']} rows")
        for w in result['warnings']:
            print(f"      - {w}")

    # Summary
    found = sum(1 for r in results['sources'].values() if r['status'] in ['valid', 'warnings'])
    missing = sum(1 for r in results['sources'].values() if r['status'] == 'not_found')

    print()
    print(f"Summary: {found} sources loaded, {missing} missing")

    # Save ingestion manifest
    manifest_file = PROCESSED_DIR / f"ingestion_manifest_{end_date.strftime('%Y%m%d')}.json"
    with open(manifest_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)

    logger.info(f"Manifest saved to: {manifest_file}")

    return results


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Ingest raw data files for Brazil Soybean Weekly Report'
    )
    parser.add_argument(
        '--source',
        choices=list(DATA_SOURCES.keys()),
        help='Specific source to ingest (default: all)'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Report date (YYYY-MM-DD). Default: latest Wednesday'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Validate data without saving processed files'
    )
    parser.add_argument(
        '--list-sources',
        action='store_true',
        help='List all configured data sources'
    )

    args = parser.parse_args()

    if args.list_sources:
        print("Configured Data Sources:")
        print("=" * 60)
        for key, cfg in DATA_SOURCES.items():
            print(f"\n{key}:")
            print(f"  Name: {cfg.name}")
            print(f"  Description: {cfg.description}")
            print(f"  Frequency: {cfg.frequency}")
            print(f"  Unit: {cfg.unit}")
            print(f"  File pattern: {cfg.file_pattern}")
            print(f"  Notes: {cfg.notes}")
        return

    # Parse date
    report_date = None
    if args.date:
        report_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    # Run ingestion
    if args.source:
        week = get_report_week_dates(report_date)
        result = ingest_source(
            args.source,
            week['start_wed'],
            week['end_wed'],
            args.validate_only
        )
        print(json.dumps(result, indent=2, default=str))
    else:
        ingest_all(report_date, args.validate_only)


if __name__ == '__main__':
    main()
