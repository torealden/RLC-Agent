#!/usr/bin/env python3
"""
Pull Statistics Canada Trade Data

Downloads trade data from Statistics Canada Web Data Service (WDS) API and:
1. Saves to PostgreSQL database
2. Updates Excel model files (using win32com to preserve external links)

Key Tables:
- 32-10-0008-01: Exports of grains, by final destination
- 32-10-0013-01: Supply and disposition of grains in Canada
- 32-10-0359-01: Areas, yield, production of principal field crops

The WDS API is open access (no API key required).
Rate limits: 25 requests/second per IP

Usage:
    python scripts/pull_statcan_trade.py --table 32-10-0008-01
    python scripts/pull_statcan_trade.py --table exports --update-excel
    pip install stats-can  # Install the helper library
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dotenv import load_dotenv

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / '.env')

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip install requests")
    sys.exit(1)

# Try to import stats_can library (optional but recommended)
try:
    import stats_can
    HAS_STATS_CAN = True
except ImportError:
    HAS_STATS_CAN = False
    print("NOTE: stats_can library not installed. Using direct API calls.")
    print("      For easier access, run: pip install stats-can")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

# Statistics Canada WDS API base URL
STATCAN_API_BASE = "https://www150.statcan.gc.ca/t1/wds/rest"

# Key table IDs for grain/oilseed trade
# Format: Product ID (PID) without dashes, e.g., 32-10-0008-01 -> 32100008
STATCAN_TABLES = {
    'GRAIN_EXPORTS_BY_DEST': {
        'table_id': '32-10-0008-01',
        'pid': '32100008',
        'name': 'Exports of grains, by final destination',
        'frequency': 'annual',
        'commodities': ['Wheat', 'Durum wheat', 'Oats', 'Barley', 'Rye', 'Flaxseed',
                       'Canola', 'Soybeans', 'Corn', 'Peas', 'Lentils', 'Chickpeas'],
    },
    'SUPPLY_DISPOSITION': {
        'table_id': '32-10-0013-01',
        'pid': '32100013',
        'name': 'Supply and disposition of grains in Canada',
        'frequency': 'quarterly',
        'commodities': ['Wheat', 'Oats', 'Barley', 'Corn', 'Soybeans', 'Canola'],
    },
    'FIELD_CROPS': {
        'table_id': '32-10-0359-01',
        'pid': '32100359',
        'name': 'Estimated areas, yield, production of principal field crops',
        'frequency': 'annual',
    },
    'GRAIN_STOCKS': {
        'table_id': '32-10-0007-01',
        'pid': '32100007',
        'name': 'Stocks of principal field crops',
        'frequency': 'quarterly',
    },
}

# Alias for convenience
TABLE_ALIASES = {
    'exports': 'GRAIN_EXPORTS_BY_DEST',
    'supply': 'SUPPLY_DISPOSITION',
    'crops': 'FIELD_CROPS',
    'stocks': 'GRAIN_STOCKS',
}

# Excel file mappings for Canadian data
EXCEL_FILES = {
    'CANOLA': 'Models/Oilseeds/World Rapeseed Trade.xlsx',
    'WHEAT': 'Models/Grains/World Wheat Trade.xlsx',
    'SOYBEANS': 'Models/Oilseeds/World Soybean Trade.xlsx',
}

# Sheet names for Canadian trade data
COMMODITY_SHEETS = {
    'CANOLA': {
        'exports': 'Canada Seed Exports',
        'imports': 'Canada Seed Imports',
    },
    'WHEAT': {
        'exports': 'Canada Exports',
        'imports': 'Canada Imports',
    },
    'SOYBEANS': {
        'exports': 'Canada Exports',
        'imports': 'Canada Imports',
    },
}

# Marketing year start months for Canadian crops
MARKETING_YEAR_START = {
    'CANOLA': 8,     # August
    'WHEAT': 8,      # August
    'BARLEY': 8,     # August
    'OATS': 8,       # August
    'SOYBEANS': 9,   # September
    'CORN': 9,       # September
    'FLAXSEED': 8,   # August
    'PEAS': 8,       # August
    'LENTILS': 8,    # August
}


# =============================================================================
# API FUNCTIONS
# =============================================================================

def get_all_cubes_list() -> List[Dict]:
    """
    Get list of all available data cubes (tables) from StatsCan.

    Returns:
        List of cube metadata dictionaries
    """
    url = f"{STATCAN_API_BASE}/getAllCubesListLite"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to get cubes list: {e}")
        return []


def get_cube_metadata(pid: str) -> Optional[Dict]:
    """
    Get metadata for a specific cube/table.

    Args:
        pid: Product ID (e.g., '32100008' for table 32-10-0008-01)

    Returns:
        Cube metadata dictionary or None
    """
    url = f"{STATCAN_API_BASE}/getCubeMetadata"

    try:
        # WDS uses POST with JSON body for most methods
        response = requests.post(
            url,
            json=[{"productId": int(pid)}],
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        if data and len(data) > 0 and data[0].get('status') == 'SUCCESS':
            return data[0].get('object')
        else:
            logger.warning(f"No metadata found for PID {pid}")
            return None

    except requests.RequestException as e:
        logger.error(f"Failed to get cube metadata for {pid}: {e}")
        return None


def get_series_info_from_cube(pid: str) -> List[Dict]:
    """
    Get all series (vectors) available in a cube.

    Args:
        pid: Product ID

    Returns:
        List of series info dictionaries
    """
    url = f"{STATCAN_API_BASE}/getSeriesInfoFromCubePidCoord"

    try:
        response = requests.post(
            url,
            json=[{"productId": int(pid), "coordinate": "1.1.1.1"}],
            headers={'Content-Type': 'application/json'},
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to get series info for {pid}: {e}")
        return []


def get_data_from_vectors(vector_ids: List[str], num_periods: int = 10) -> List[Dict]:
    """
    Get data for specific vectors for the latest N periods.

    Args:
        vector_ids: List of vector IDs (e.g., ['v123456', 'v789012'])
        num_periods: Number of latest periods to retrieve

    Returns:
        List of data dictionaries
    """
    url = f"{STATCAN_API_BASE}/getDataFromVectorsAndLatestNPeriods"

    # Format vector IDs (remove 'v' prefix if present)
    formatted_vectors = []
    for v in vector_ids:
        vid = str(v).replace('v', '').replace('V', '')
        formatted_vectors.append({"vectorId": int(vid), "latestN": num_periods})

    try:
        response = requests.post(
            url,
            json=formatted_vectors,
            headers={'Content-Type': 'application/json'},
            timeout=60
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to get vector data: {e}")
        return []


def get_bulk_vector_data_by_range(
    vector_ids: List[str],
    start_date: str,
    end_date: str
) -> List[Dict]:
    """
    Get data for vectors within a date range.

    Args:
        vector_ids: List of vector IDs
        start_date: Start date (YYYY-MM-DDTHH:MM format)
        end_date: End date (YYYY-MM-DDTHH:MM format)

    Returns:
        List of data dictionaries
    """
    url = f"{STATCAN_API_BASE}/getBulkVectorDataByRange"

    # Format vector IDs
    formatted_vectors = [str(v).replace('v', '').replace('V', '') for v in vector_ids]

    payload = {
        "vectorIds": formatted_vectors,
        "startDataPointReleaseDate": start_date,
        "endDataPointReleaseDate": end_date
    }

    try:
        response = requests.post(
            url,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=120
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to get bulk vector data: {e}")
        return []


def get_full_table_download(pid: str, csv_format: bool = True) -> Optional[str]:
    """
    Get URL for full table download.

    Args:
        pid: Product ID
        csv_format: If True, return CSV URL; else return SDMX URL

    Returns:
        Download URL or None
    """
    url = f"{STATCAN_API_BASE}/getFullTableDownloadCSV/{pid}/en" if csv_format else \
          f"{STATCAN_API_BASE}/getFullTableDownloadSDMX/{pid}"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get('status') == 'SUCCESS':
            return data.get('object')
        return None
    except requests.RequestException as e:
        logger.error(f"Failed to get download URL for {pid}: {e}")
        return None


def download_full_table(pid: str, output_dir: Path = None) -> Optional[Path]:
    """
    Download the full table as a CSV ZIP file.

    Args:
        pid: Product ID
        output_dir: Directory to save the file

    Returns:
        Path to downloaded file or None
    """
    download_url = get_full_table_download(pid, csv_format=True)

    if not download_url:
        logger.error(f"Could not get download URL for {pid}")
        return None

    output_dir = output_dir or (PROJECT_ROOT / 'data' / 'statcan')
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{pid}.zip"

    try:
        print(f"Downloading table {pid}...")
        print(f"  URL: {download_url}")

        response = requests.get(download_url, timeout=300, stream=True)
        response.raise_for_status()

        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"  Saved to: {output_file}")
        return output_file

    except requests.RequestException as e:
        logger.error(f"Failed to download table {pid}: {e}")
        return None


# =============================================================================
# STATS_CAN LIBRARY FUNCTIONS (if available)
# =============================================================================

def fetch_table_with_library(table_id: str) -> Optional[Any]:
    """
    Fetch table data using the stats_can library.

    Args:
        table_id: Table ID in format '32-10-0008-01'

    Returns:
        DataFrame or None
    """
    if not HAS_STATS_CAN:
        logger.warning("stats_can library not available")
        return None

    try:
        # stats_can can read tables directly into pandas DataFrames
        import pandas as pd

        # Download and read the table
        df = stats_can.sc.table_to_df(table_id)
        return df

    except Exception as e:
        logger.error(f"Failed to fetch table {table_id} with stats_can: {e}")
        return None


def get_vectors_with_library(vectors: List[str], periods: int = 10) -> Optional[Any]:
    """
    Get vector data using the stats_can library.

    Args:
        vectors: List of vector IDs
        periods: Number of periods

    Returns:
        DataFrame or None
    """
    if not HAS_STATS_CAN:
        return None

    try:
        df = stats_can.sc.vectors_to_df(vectors, periods=periods)
        return df
    except Exception as e:
        logger.error(f"Failed to get vectors with stats_can: {e}")
        return None


# =============================================================================
# DATA PROCESSING
# =============================================================================

def parse_grain_exports_data(data: List[Dict]) -> Dict[Tuple, Dict]:
    """
    Parse grain exports by destination data into a usable format.

    Args:
        data: Raw API response data

    Returns:
        Dict mapping (year, commodity, destination) to export values
    """
    exports = {}

    for item in data:
        if item.get('status') != 'SUCCESS':
            continue

        obj = item.get('object', {})
        vector_data = obj.get('vectorDataPoint', [])

        for point in vector_data:
            ref_period = point.get('refPer', '')  # e.g., '2023-01-01'
            value = point.get('value')

            if ref_period and value is not None:
                try:
                    year = int(ref_period[:4])
                    # Would need to parse commodity and destination from vector metadata
                    # This is simplified - actual implementation needs coordinate mapping
                    exports[(year,)] = {'value': value}
                except (ValueError, IndexError):
                    continue

    return exports


def get_marketing_year(commodity: str, dt: date) -> str:
    """Get marketing year string for a date"""
    start_month = MARKETING_YEAR_START.get(commodity.upper(), 8)

    if dt.month >= start_month:
        my_start = dt.year
    else:
        my_start = dt.year - 1

    return f"{my_start}/{str(my_start + 1)[2:]}"


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def test_api_connection() -> bool:
    """Test the StatsCan API connection."""
    print("\n--- Statistics Canada API Connection Test ---")

    # Test 1: Get cubes list
    print("\n1. Testing getAllCubesListLite...")
    cubes = get_all_cubes_list()
    if cubes:
        print(f"   SUCCESS: Found {len(cubes)} data cubes")
    else:
        print("   FAILED: Could not retrieve cubes list")
        return False

    # Test 2: Get metadata for grain exports table
    print("\n2. Testing getCubeMetadata for grain exports (32100008)...")
    metadata = get_cube_metadata('32100008')
    if metadata:
        print(f"   SUCCESS: {metadata.get('productId')} - {metadata.get('cubeTitleEn', 'N/A')}")
    else:
        print("   FAILED: Could not retrieve metadata")
        return False

    # Test 3: Get download URL
    print("\n3. Testing getFullTableDownloadCSV...")
    download_url = get_full_table_download('32100008')
    if download_url:
        print(f"   SUCCESS: Download URL available")
        print(f"   URL: {download_url[:80]}...")
    else:
        print("   FAILED: Could not get download URL")

    print("\n--- API Test Complete ---")
    return True


def list_available_tables():
    """List all available StatsCan tables configured in this script."""
    print("\n=== Available Statistics Canada Tables ===\n")

    for key, info in STATCAN_TABLES.items():
        print(f"Key: {key}")
        print(f"  Table ID: {info['table_id']}")
        print(f"  Name: {info['name']}")
        print(f"  Frequency: {info['frequency']}")
        if 'commodities' in info:
            print(f"  Commodities: {', '.join(info['commodities'][:5])}...")
        print()

    print("Aliases:")
    for alias, key in TABLE_ALIASES.items():
        print(f"  --table {alias}  ->  {key}")


def run_statcan_update(
    table_key: str,
    download: bool = False,
    update_excel: bool = False
) -> Dict:
    """
    Run the StatsCan data update workflow.

    Args:
        table_key: Table key or alias (e.g., 'exports', 'GRAIN_EXPORTS_BY_DEST')
        download: Download full table CSV
        update_excel: Update Excel files

    Returns:
        Results summary
    """
    results = {
        'success': False,
        'table': None,
        'records_fetched': 0,
        'file_downloaded': None,
    }

    # Resolve alias
    if table_key.lower() in TABLE_ALIASES:
        table_key = TABLE_ALIASES[table_key.lower()]

    table_info = STATCAN_TABLES.get(table_key.upper())
    if not table_info:
        print(f"ERROR: Unknown table: {table_key}")
        print("Use --list to see available tables")
        return results

    results['table'] = table_info['table_id']

    print(f"\n{'=' * 60}")
    print(f"STATISTICS CANADA: {table_info['name']}")
    print(f"Table: {table_info['table_id']}")
    print(f"{'=' * 60}")

    # Get metadata
    print("\nFetching table metadata...")
    metadata = get_cube_metadata(table_info['pid'])
    if metadata:
        print(f"  Title: {metadata.get('cubeTitleEn', 'N/A')}")
        print(f"  Start: {metadata.get('cubeStartDate', 'N/A')}")
        print(f"  End: {metadata.get('cubeEndDate', 'N/A')}")
        print(f"  Frequency: {metadata.get('frequencyCode', 'N/A')}")

    # Download full table if requested
    if download:
        print("\nDownloading full table...")
        downloaded_file = download_full_table(table_info['pid'])
        if downloaded_file:
            results['file_downloaded'] = str(downloaded_file)
            results['success'] = True
            print(f"\nSUCCESS: Table downloaded to {downloaded_file}")
            print("\nTo extract and view:")
            print(f"  unzip {downloaded_file}")
            print(f"  # CSV file will contain the full data")

    # TODO: Add Excel update logic once we understand the data format
    if update_excel:
        print("\nExcel update not yet implemented for StatsCan data")
        print("Need to first download and analyze the table structure")

    return results


def main():
    """Command-line entry point"""
    parser = argparse.ArgumentParser(
        description='Download Statistics Canada trade data via WDS API'
    )

    parser.add_argument(
        '--table', '-t',
        help='Table to fetch (e.g., exports, supply, stocks, or full ID like 32-10-0008-01)'
    )

    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List available tables'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Test API connection'
    )

    parser.add_argument(
        '--download', '-d',
        action='store_true',
        help='Download full table as CSV'
    )

    parser.add_argument(
        '--update-excel',
        action='store_true',
        help='Update Excel model files'
    )

    parser.add_argument(
        '--vectors', '-v',
        nargs='+',
        help='Specific vector IDs to fetch (e.g., v123456 v789012)'
    )

    parser.add_argument(
        '--periods', '-p',
        type=int,
        default=10,
        help='Number of periods to fetch for vectors (default: 10)'
    )

    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("STATISTICS CANADA WDS DATA COLLECTOR")
    print("=" * 60)
    print("API: https://www150.statcan.gc.ca/t1/wds/rest")
    print("No API key required (open access)")

    if args.list:
        list_available_tables()
        return

    if args.test:
        if not test_api_connection():
            sys.exit(1)
        return

    if args.vectors:
        print(f"\nFetching {len(args.vectors)} vectors for {args.periods} periods...")

        if HAS_STATS_CAN:
            df = get_vectors_with_library(args.vectors, args.periods)
            if df is not None:
                print(f"\nData shape: {df.shape}")
                print(df.head(10))
        else:
            data = get_data_from_vectors(args.vectors, args.periods)
            print(f"\nReceived {len(data)} responses")
            for item in data[:3]:
                print(json.dumps(item, indent=2)[:500])
        return

    if args.table:
        results = run_statcan_update(
            table_key=args.table,
            download=args.download,
            update_excel=args.update_excel
        )

        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        print(f"Table: {results.get('table')}")
        print(f"Success: {results.get('success')}")
        if results.get('file_downloaded'):
            print(f"Downloaded: {results.get('file_downloaded')}")
        return

    # Default: show help
    parser.print_help()
    print("\n\nExamples:")
    print("  python scripts/pull_statcan_trade.py --test")
    print("  python scripts/pull_statcan_trade.py --list")
    print("  python scripts/pull_statcan_trade.py --table exports --download")
    print("  python scripts/pull_statcan_trade.py --vectors v123456 v789012 --periods 20")


if __name__ == '__main__':
    main()
