#!/usr/bin/env python3
"""
ERS Feed Grains Data Collector

Automatically downloads the USDA ERS Feed Grains Database yearbook tables
from the official ERS website.

Data Source: https://www.ers.usda.gov/data-products/feed-grains-database/
Release Schedule: Monthly, typically around the 13th-15th

Usage:
    python scripts/collectors/ers_feed_grains_collector.py [--force]

The collector will:
1. Check if new data is available (comparing file dates)
2. Download the Feed Grains Yearbook Tables Excel file
3. Archive the previous version
4. Trigger the ingestion pipeline
"""

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ers_feed_grains_collector')

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "Models" / "Data"
ARCHIVE_DIR = DATA_DIR / "archive"
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
CONFIG_PATH = PROJECT_ROOT / "config" / "data_sources.json"

# ERS Feed Grains URLs
ERS_BASE_URL = "https://www.ers.usda.gov"
ERS_FEED_GRAINS_PAGE = "https://www.ers.usda.gov/data-products/feed-grains-database/"
ERS_YEARBOOK_DOWNLOAD = "https://www.ers.usda.gov/webdocs/DataFiles/50048/FeedGrainsYearbookTables2024.xlsx"

# Expected filename patterns
YEARBOOK_PATTERN = re.compile(r'FeedGrainsYearbookTables(\d{4})\.xlsx', re.IGNORECASE)


class ERSFeedGrainsCollector:
    """Collects Feed Grains data from USDA ERS."""

    def __init__(self, force: bool = False):
        self.force = force
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RLC-Agent/1.0 (Agricultural Data Research; contact@example.com)'
        })

        # Ensure directories exist
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

        # Load or create config
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load data source configuration."""
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH) as f:
                return json.load(f)
        return {
            "ers_feed_grains": {
                "last_download": None,
                "last_file_hash": None,
                "last_file_date": None,
                "download_count": 0
            }
        }

    def _save_config(self):
        """Save configuration."""
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, 'w') as f:
            json.dump(self.config, f, indent=2, default=str)

    def _get_file_hash(self, filepath: Path) -> str:
        """Calculate MD5 hash of a file."""
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hasher.update(chunk)
        return hasher.hexdigest()

    def _download_with_retry(self, url: str, max_retries: int = 4) -> Optional[bytes]:
        """Download URL with exponential backoff retry."""
        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading: {url} (attempt {attempt + 1}/{max_retries})")
                response = self.session.get(url, timeout=60)
                response.raise_for_status()
                return response.content
            except requests.RequestException as e:
                wait_time = 2 ** (attempt + 1)  # 2, 4, 8, 16 seconds
                logger.warning(f"Download failed: {e}. Retrying in {wait_time}s...")
                if attempt < max_retries - 1:
                    time.sleep(wait_time)
        logger.error(f"Failed to download after {max_retries} attempts")
        return None

    def find_yearbook_download_url(self) -> Optional[Tuple[str, str]]:
        """
        Scrape the ERS Feed Grains page to find the current yearbook download URL.
        Returns (url, filename) or None if not found.
        """
        logger.info("Checking ERS Feed Grains page for latest data...")

        try:
            response = self.session.get(ERS_FEED_GRAINS_PAGE, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # Look for download links
            for link in soup.find_all('a', href=True):
                href = link['href']

                # Check for yearbook table pattern
                if 'yearbooktables' in href.lower() and href.endswith('.xlsx'):
                    full_url = urljoin(ERS_BASE_URL, href)
                    filename = href.split('/')[-1]
                    logger.info(f"Found yearbook download: {filename}")
                    return full_url, filename

            # Fallback: try known URL pattern with current year
            current_year = datetime.now().year
            for year in [current_year, current_year - 1]:
                test_url = f"https://www.ers.usda.gov/webdocs/DataFiles/50048/FeedGrainsYearbookTables{year}.xlsx"
                try:
                    head_response = self.session.head(test_url, timeout=10)
                    if head_response.status_code == 200:
                        logger.info(f"Found yearbook at known URL: {test_url}")
                        return test_url, f"FeedGrainsYearbookTables{year}.xlsx"
                except:
                    pass

        except Exception as e:
            logger.error(f"Error finding download URL: {e}")

        return None

    def check_for_updates(self) -> bool:
        """
        Check if new data is available.
        Returns True if we should download.
        """
        if self.force:
            logger.info("Force download requested")
            return True

        # Check last download date
        last_download = self.config.get("ers_feed_grains", {}).get("last_download")
        if last_download:
            last_dt = datetime.fromisoformat(last_download)
            days_since = (datetime.now() - last_dt).days

            # ERS updates monthly, don't check more than daily
            if days_since < 1:
                logger.info(f"Last download was {days_since} days ago, skipping check")
                return False

        return True

    def download_yearbook(self) -> Optional[Path]:
        """
        Download the Feed Grains Yearbook Tables.
        Returns path to downloaded file or None.
        """
        # Find the download URL
        result = self.find_yearbook_download_url()
        if not result:
            logger.error("Could not find yearbook download URL")
            return None

        url, filename = result

        # Download the file
        content = self._download_with_retry(url)
        if not content:
            return None

        # Calculate hash to check if file changed
        new_hash = hashlib.md5(content).hexdigest()
        old_hash = self.config.get("ers_feed_grains", {}).get("last_file_hash")

        if new_hash == old_hash and not self.force:
            logger.info("File unchanged since last download, skipping")
            return None

        # Archive old file if exists
        target_path = DATA_DIR / "US Feed Grains Outlook.xlsx"
        if target_path.exists():
            archive_name = f"US Feed Grains Outlook - {datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            archive_path = ARCHIVE_DIR / archive_name
            shutil.move(target_path, archive_path)
            logger.info(f"Archived previous file to: {archive_path}")

        # Save new file
        with open(target_path, 'wb') as f:
            f.write(content)
        logger.info(f"Downloaded new file: {target_path}")

        # Update config
        self.config.setdefault("ers_feed_grains", {})
        self.config["ers_feed_grains"]["last_download"] = datetime.now().isoformat()
        self.config["ers_feed_grains"]["last_file_hash"] = new_hash
        self.config["ers_feed_grains"]["download_count"] = self.config["ers_feed_grains"].get("download_count", 0) + 1
        self._save_config()

        return target_path

    def run_ingestion(self, filepath: Path) -> bool:
        """Run the ingestion pipeline on the downloaded file."""
        logger.info("Running ingestion pipeline...")

        try:
            # Import and run the ingestor
            sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
            from ingest_feed_grains_data import FeedGrainsIngestor, create_sqlite_tables

            conn = sqlite3.connect(str(DB_PATH))
            create_sqlite_tables(conn)

            ingestor = FeedGrainsIngestor(conn)
            ingestor.ingest_all()

            conn.commit()
            conn.close()

            logger.info(f"Ingestion complete: {ingestor.stats}")
            return True

        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            return False

    def collect(self) -> dict:
        """
        Main collection method.
        Returns status dict with results.
        """
        result = {
            "success": False,
            "message": "",
            "downloaded_file": None,
            "ingested": False,
            "timestamp": datetime.now().isoformat()
        }

        # Check if we should download
        if not self.check_for_updates():
            result["message"] = "No update needed"
            result["success"] = True
            return result

        # Download the file
        filepath = self.download_yearbook()
        if filepath:
            result["downloaded_file"] = str(filepath)

            # Run ingestion
            if self.run_ingestion(filepath):
                result["ingested"] = True
                result["success"] = True
                result["message"] = "Successfully downloaded and ingested new data"
            else:
                result["message"] = "Downloaded but ingestion failed"
        else:
            result["message"] = "No new data available or download failed"
            result["success"] = True  # Not an error if no new data

        return result


class CensusTradeCollector:
    """
    Collects monthly trade data from Census Bureau.
    This data is available via their API or data portal.
    """

    CENSUS_API_BASE = "https://api.census.gov/data/timeseries/intltrade"

    def __init__(self):
        self.session = requests.Session()
        self.api_key = os.environ.get("CENSUS_API_KEY")

    def collect_corn_exports(self, year: int, month: int) -> Optional[dict]:
        """
        Collect corn export data for a specific month.
        HS Code 100590 = Corn (maize), other than seed corn
        """
        logger.info(f"Collecting corn exports for {year}-{month:02d}")

        # Census Trade API endpoint for exports
        url = f"{self.CENSUS_API_BASE}/exports/hs"

        params = {
            "get": "CTY_CODE,CTY_NAME,ALL_VAL_MO,QTY_1_MO,UNIT_QY1",
            "COMM_LVL": "HS6",
            "HS_ID": "100590",
            "YEAR": year,
            "MONTH": month,
        }

        if self.api_key:
            params["key"] = self.api_key

        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Census API error: {e}")
            return None

    def collect_recent_months(self, months_back: int = 3) -> list:
        """Collect data for recent months."""
        results = []
        today = datetime.now()

        for i in range(months_back):
            target_date = today - timedelta(days=30 * (i + 2))  # Census data has ~2 month lag
            data = self.collect_corn_exports(target_date.year, target_date.month)
            if data:
                results.append({
                    "year": target_date.year,
                    "month": target_date.month,
                    "data": data
                })

        return results


def get_release_schedule() -> dict:
    """
    Returns the typical release schedule for agricultural data.
    """
    return {
        "ers_feed_grains": {
            "name": "ERS Feed Grains Database",
            "frequency": "monthly",
            "typical_release_day": 13,  # Usually mid-month
            "url": "https://www.ers.usda.gov/data-products/feed-grains-database/",
            "description": "Yearbook tables with prices, balance sheets, trade"
        },
        "wasde": {
            "name": "World Agricultural Supply and Demand Estimates",
            "frequency": "monthly",
            "typical_release_day": 12,  # Usually around 12th
            "url": "https://www.usda.gov/oce/commodity/wasde",
            "description": "Supply/demand projections"
        },
        "nass_crop_production": {
            "name": "NASS Crop Production",
            "frequency": "monthly",
            "typical_release_day": 10,
            "url": "https://www.nass.usda.gov/Publications/Reports_By_Date/",
            "description": "US crop production estimates"
        },
        "census_trade": {
            "name": "Census Bureau Trade Data",
            "frequency": "monthly",
            "typical_release_day": 5,  # ~5th of month, 2-month lag
            "url": "https://usatrade.census.gov/",
            "description": "Import/export data by HS code"
        },
        "export_sales": {
            "name": "USDA Export Sales",
            "frequency": "weekly",
            "release_day": "Thursday",
            "url": "https://apps.fas.usda.gov/export-sales/",
            "description": "Weekly export sales and shipments"
        }
    }


def main():
    parser = argparse.ArgumentParser(description='Collect ERS Feed Grains data')
    parser.add_argument('--force', action='store_true', help='Force download even if data seems current')
    parser.add_argument('--dry-run', action='store_true', help='Check for updates without downloading')
    parser.add_argument('--schedule', action='store_true', help='Show release schedule')
    args = parser.parse_args()

    if args.schedule:
        print("\n=== Agricultural Data Release Schedule ===\n")
        for source, info in get_release_schedule().items():
            print(f"{info['name']}:")
            print(f"  Frequency: {info['frequency']}")
            if 'typical_release_day' in info:
                print(f"  Typical release: Day {info['typical_release_day']} of month")
            if 'release_day' in info:
                print(f"  Release day: {info['release_day']}")
            print(f"  URL: {info['url']}")
            print()
        return

    if args.dry_run:
        collector = ERSFeedGrainsCollector()
        result = collector.find_yearbook_download_url()
        if result:
            url, filename = result
            print(f"Found: {filename}")
            print(f"URL: {url}")
        else:
            print("Could not find download URL")
        return

    # Run collection
    collector = ERSFeedGrainsCollector(force=args.force)
    result = collector.collect()

    print(f"\n=== Collection Result ===")
    print(f"Success: {result['success']}")
    print(f"Message: {result['message']}")
    if result['downloaded_file']:
        print(f"File: {result['downloaded_file']}")
    print(f"Ingested: {result['ingested']}")


if __name__ == '__main__':
    main()
