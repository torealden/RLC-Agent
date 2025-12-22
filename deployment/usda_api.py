#!/usr/bin/env python3
"""
USDA FAS API Integration

Provides access to USDA Foreign Agricultural Service data:
- PSD (Production, Supply, and Distribution) database
- Export Sales Report (ESR)
- Global Agricultural Trade System (GATS)

API Documentation: https://apps.fas.usda.gov/opendatawebV2/#/home

To get an API key:
1. Go to https://api.data.gov/signup/
2. Sign up with your email
3. Save the key to environment variable USDA_API_KEY or config file

Usage:
    python deployment/usda_api.py --commodities           # List all commodities
    python deployment/usda_api.py --countries             # List all countries
    python deployment/usda_api.py --query soybeans china  # Query specific data
    python deployment/usda_api.py --download soybeans     # Download full commodity data
"""

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

import requests

# API Configuration
API_BASE_URL = "https://apps.fas.usda.gov/OpenData/api"
DEFAULT_API_KEY = os.environ.get("USDA_API_KEY", "")

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "rlc_commodities.db"
CACHE_DIR = DATA_DIR / "usda_cache"


class USDAApi:
    """Client for USDA FAS OpenData API."""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or DEFAULT_API_KEY
        if not self.api_key:
            print("WARNING: No API key set. Get one at https://api.data.gov/signup/")
            print("Set environment variable USDA_API_KEY or pass to constructor.")

        self.session = requests.Session()
        self.session.headers.update({
            "API_KEY": self.api_key,
            "Accept": "application/json"
        })

        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _get(self, endpoint: str, params: Dict = None) -> Dict:
        """Make GET request to API."""
        url = f"{API_BASE_URL}/{endpoint}"

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API Error: {e}")
            return {}

    def get_commodities(self) -> List[Dict]:
        """Get list of all available commodities."""
        cache_file = CACHE_DIR / "commodities.json"

        # Use cache if less than 24 hours old
        if cache_file.exists():
            age = time.time() - cache_file.stat().st_mtime
            if age < 86400:  # 24 hours
                with open(cache_file) as f:
                    return json.load(f)

        data = self._get("psd/commodities")

        if data:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)

        return data

    def get_countries(self) -> List[Dict]:
        """Get list of all countries."""
        cache_file = CACHE_DIR / "countries.json"

        if cache_file.exists():
            age = time.time() - cache_file.stat().st_mtime
            if age < 86400:
                with open(cache_file) as f:
                    return json.load(f)

        data = self._get("psd/countries")

        if data:
            with open(cache_file, 'w') as f:
                json.dump(data, f, indent=2)

        return data

    def get_units_of_measure(self) -> List[Dict]:
        """Get list of units of measure."""
        return self._get("psd/unitsOfMeasure")

    def get_commodity_data(self, commodity_code: str, country_code: str = None,
                           market_year: int = None) -> List[Dict]:
        """
        Get PSD data for a specific commodity.

        Args:
            commodity_code: e.g., "0813800" for Soybeans
            country_code: e.g., "CH" for China (optional)
            market_year: e.g., 2023 (optional)

        Returns:
            List of data records
        """
        params = {"commodityCode": commodity_code}

        if country_code:
            params["countryCode"] = country_code

        if market_year:
            params["marketYear"] = market_year

        return self._get("psd/commodity", params)

    def search_commodity(self, name: str) -> List[Dict]:
        """Search for commodity by name."""
        commodities = self.get_commodities()
        name_lower = name.lower()

        matches = [c for c in commodities
                   if name_lower in c.get('commodityName', '').lower()]

        return matches

    def search_country(self, name: str) -> List[Dict]:
        """Search for country by name."""
        countries = self.get_countries()
        name_lower = name.lower()

        matches = [c for c in countries
                   if name_lower in c.get('countryName', '').lower()]

        return matches

    def download_commodity_data(self, commodity_name: str,
                                 save_to_db: bool = True) -> List[Dict]:
        """
        Download all data for a commodity and optionally save to database.

        Args:
            commodity_name: e.g., "soybeans", "corn", "wheat"
            save_to_db: If True, save to SQLite database

        Returns:
            List of data records
        """
        # Find commodity code
        matches = self.search_commodity(commodity_name)
        if not matches:
            print(f"Commodity not found: {commodity_name}")
            return []

        commodity = matches[0]
        commodity_code = commodity.get('commodityCode')
        print(f"Found: {commodity.get('commodityName')} ({commodity_code})")

        # Get all data
        data = self.get_commodity_data(commodity_code)

        if not data:
            print("No data returned from API")
            return []

        print(f"Downloaded {len(data)} records")

        if save_to_db:
            self._save_to_database(data, commodity_name)

        return data

    def _save_to_database(self, data: List[Dict], source_name: str):
        """Save USDA data to the commodity database."""
        conn = sqlite3.connect(str(DB_PATH))
        cursor = conn.cursor()

        # Create USDA-specific table if needed
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usda_psd_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commodity_code TEXT,
                commodity_name TEXT,
                country_code TEXT,
                country_name TEXT,
                market_year INTEGER,
                attribute_id INTEGER,
                attribute_description TEXT,
                value REAL,
                unit_description TEXT,
                source TEXT,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(commodity_code, country_code, market_year, attribute_id)
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_usda_commodity
            ON usda_psd_data(commodity_name)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_usda_country
            ON usda_psd_data(country_name)
        """)

        # Insert data
        inserted = 0
        for record in data:
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO usda_psd_data
                    (commodity_code, commodity_name, country_code, country_name,
                     market_year, attribute_id, attribute_description, value,
                     unit_description, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get('commodityCode'),
                    record.get('commodityName'),
                    record.get('countryCode'),
                    record.get('countryName'),
                    record.get('marketYear'),
                    record.get('attributeId'),
                    record.get('attributeDescription'),
                    record.get('value'),
                    record.get('unitDescription'),
                    f"USDA PSD API - {source_name}"
                ))
                inserted += 1
            except Exception as e:
                print(f"Insert error: {e}")

        conn.commit()
        conn.close()

        print(f"Saved {inserted} records to database")


def print_commodities(api: USDAApi):
    """Print list of available commodities."""
    commodities = api.get_commodities()

    print("\n" + "="*60)
    print("  USDA PSD COMMODITIES")
    print("="*60 + "\n")

    # Group by category
    oilseeds = []
    grains = []
    other = []

    for c in commodities:
        name = c.get('commodityName', '').lower()
        if any(x in name for x in ['soy', 'sunflower', 'rape', 'canola', 'peanut',
                                    'palm', 'coconut', 'cotton', 'olive']):
            oilseeds.append(c)
        elif any(x in name for x in ['corn', 'wheat', 'rice', 'barley', 'oat', 'sorghum']):
            grains.append(c)
        else:
            other.append(c)

    print("  OILSEEDS:")
    for c in sorted(oilseeds, key=lambda x: x.get('commodityName', '')):
        print(f"    {c.get('commodityCode')}: {c.get('commodityName')}")

    print("\n  GRAINS:")
    for c in sorted(grains, key=lambda x: x.get('commodityName', '')):
        print(f"    {c.get('commodityCode')}: {c.get('commodityName')}")

    print(f"\n  Total commodities: {len(commodities)}")


def print_countries(api: USDAApi):
    """Print list of countries."""
    countries = api.get_countries()

    print("\n" + "="*60)
    print("  USDA PSD COUNTRIES")
    print("="*60 + "\n")

    for c in sorted(countries, key=lambda x: x.get('countryName', '')):
        print(f"    {c.get('countryCode')}: {c.get('countryName')}")

    print(f"\n  Total countries: {len(countries)}")


def query_data(api: USDAApi, commodity: str, country: str = None):
    """Query and display data."""
    matches = api.search_commodity(commodity)
    if not matches:
        print(f"Commodity not found: {commodity}")
        return

    commodity_info = matches[0]
    print(f"\nCommodity: {commodity_info.get('commodityName')}")
    print(f"Code: {commodity_info.get('commodityCode')}")

    country_code = None
    if country:
        country_matches = api.search_country(country)
        if country_matches:
            country_info = country_matches[0]
            country_code = country_info.get('countryCode')
            print(f"Country: {country_info.get('countryName')} ({country_code})")

    data = api.get_commodity_data(
        commodity_info.get('commodityCode'),
        country_code=country_code
    )

    if not data:
        print("No data returned")
        return

    print(f"\nReturned {len(data)} records")

    # Show sample
    print("\nSample records:")
    for record in data[:10]:
        print(f"  {record.get('countryName')} {record.get('marketYear')}: "
              f"{record.get('attributeDescription')} = {record.get('value')} "
              f"{record.get('unitDescription')}")


def main():
    parser = argparse.ArgumentParser(description='USDA FAS API Client')
    parser.add_argument('--api-key', type=str, help='USDA API key')
    parser.add_argument('--commodities', action='store_true', help='List all commodities')
    parser.add_argument('--countries', action='store_true', help='List all countries')
    parser.add_argument('--query', nargs='+', help='Query: commodity [country]')
    parser.add_argument('--download', type=str, help='Download commodity data to database')
    parser.add_argument('--search', type=str, help='Search for commodity')

    args = parser.parse_args()

    api = USDAApi(api_key=args.api_key)

    if args.commodities:
        print_commodities(api)
    elif args.countries:
        print_countries(api)
    elif args.query:
        commodity = args.query[0]
        country = args.query[1] if len(args.query) > 1 else None
        query_data(api, commodity, country)
    elif args.download:
        api.download_commodity_data(args.download)
    elif args.search:
        matches = api.search_commodity(args.search)
        print(f"\nSearch results for '{args.search}':")
        for m in matches:
            print(f"  {m.get('commodityCode')}: {m.get('commodityName')}")
    else:
        parser.print_help()
        print("\n" + "="*60)
        print("  QUICK START")
        print("="*60)
        print("\n  1. Get API key: https://api.data.gov/signup/")
        print("  2. Set environment variable: export USDA_API_KEY=your_key")
        print("  3. Test: python deployment/usda_api.py --commodities")
        print()


if __name__ == '__main__':
    main()
