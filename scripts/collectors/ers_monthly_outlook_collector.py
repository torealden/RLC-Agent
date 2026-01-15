#!/usr/bin/env python3
"""
ERS Monthly Outlook Collector

Automatically downloads the USDA ERS monthly outlook reports for:
- Oil Crops Outlook (OCS)
- Feed Grains Outlook (FDS)

These are released monthly and contain updated supply/demand projections.

Data Sources:
- Oil Crops: https://www.ers.usda.gov/publications/pub-details?pubid={pubid}
- Feed Grains: https://www.ers.usda.gov/publications/pub-details?pubid={pubid}

Release Schedule: Monthly, typically mid-month

Usage:
    python scripts/collectors/ers_monthly_outlook_collector.py --source oil_crops
    python scripts/collectors/ers_monthly_outlook_collector.py --source feed_grains
    python scripts/collectors/ers_monthly_outlook_collector.py --all

The collector will:
1. Check the ERS publications page for the latest release
2. Download the Excel tables file
3. Archive the previous version
4. Optionally trigger the ingestion pipeline
"""

import argparse
import hashlib
import json
import logging
import re
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('ers_monthly_outlook')

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "Models" / "Data"
ARCHIVE_DIR = DATA_DIR / "archive"
DB_PATH = PROJECT_ROOT / "data" / "rlc_commodities.db"
CONFIG_PATH = PROJECT_ROOT / "config" / "data_sources.json"

# ERS Monthly Outlook configurations
OUTLOOK_SOURCES = {
    "oil_crops": {
        "name": "Oil Crops Outlook",
        "code": "OCS",
        "pub_page_template": "https://www.ers.usda.gov/publications/pub-details?pubid={pubid}",
        "excel_template": "https://ers.usda.gov/sites/default/files/_laserfiche/outlooks/{folder_id}/oiltables.xlsx",
        "pdf_template": "https://ers.usda.gov/sites/default/files/_laserfiche/outlooks/{folder_id}/{code}-{year_letter}.pdf",
        "local_filename": "oiltables_{year}{month}.xlsx",
        "current_pubid": 113677,  # January 2026
        "current_folder": 113678,
        "release_day": 14,  # Approximate day of month
        "ingest_script": "scripts/ingest_oil_crops_outlook.py"
    },
    "feed_grains": {
        "name": "Feed Grains Outlook",
        "code": "FDS",
        "pub_page_template": "https://www.ers.usda.gov/publications/pub-details?pubid={pubid}",
        "excel_template": "https://ers.usda.gov/sites/default/files/_laserfiche/outlooks/{folder_id}/FGOutlook-tables.xlsx",
        "pdf_template": "https://ers.usda.gov/sites/default/files/_laserfiche/outlooks/{folder_id}/{code}-{year_letter}.pdf",
        "local_filename": "FGOutlook-tables_{year}{month}.xlsx",
        "current_pubid": 113682,  # January 2026
        "current_folder": 113683,
        "release_day": 14,
        "ingest_script": "scripts/ingest_feed_grains_outlook.py"
    }
}

# Month letter codes (A=Jan, B=Feb, ..., L=Dec)
MONTH_LETTERS = {
    1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F',
    7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L'
}


class ERSMonthlyOutlookCollector:
    """Collects monthly outlook data from USDA ERS."""

    def __init__(self, source: str, force: bool = False):
        if source not in OUTLOOK_SOURCES:
            raise ValueError(f"Unknown source: {source}. Valid: {list(OUTLOOK_SOURCES.keys())}")

        self.source = source
        self.source_config = OUTLOOK_SOURCES[source]
        self.force = force

        if HAS_REQUESTS:
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'RLC-Agent/1.0 (Agricultural Data Research)'
            })
        else:
            self.session = None

        # Ensure directories exist
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

        # Load config
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load data source configuration."""
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH) as f:
                config = json.load(f)
        else:
            config = {}

        # Ensure source entry exists
        if self.source not in config:
            config[self.source] = {
                "last_download": None,
                "last_file_hash": None,
                "last_pubid": self.source_config["current_pubid"],
                "last_folder": self.source_config["current_folder"],
                "download_count": 0
            }
        return config

    def _save_config(self):
        """Save configuration."""
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_PATH, 'w') as f:
            json.dump(self.config, f, indent=2, default=str)

    def _get_file_hash(self, filepath: Path) -> str:
        """Calculate MD5 hash of a file."""
        md5 = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                md5.update(chunk)
        return md5.hexdigest()

    def _get_year_letter(self, year: int, month: int) -> str:
        """Get the year-letter code (e.g., '26A' for Jan 2026)."""
        return f"{year % 100}{MONTH_LETTERS[month]}"

    def _estimate_next_pubid(self) -> Tuple[int, int]:
        """Estimate the next publication ID based on current date."""
        now = datetime.now()
        current_year = now.year
        current_month = now.month

        # Base: January 2026 pubids
        base_year = 2026
        base_month = 1
        base_pubid = self.source_config["current_pubid"]
        base_folder = self.source_config["current_folder"]

        # Calculate months since base
        months_diff = (current_year - base_year) * 12 + (current_month - base_month)

        # Estimate pubid increment (typically 5-10 per month for all ERS publications)
        # This is an approximation - actual checking will verify
        estimated_pubid = base_pubid + (months_diff * 6)
        estimated_folder = base_folder + (months_diff * 6)

        return estimated_pubid, estimated_folder

    def check_for_update(self) -> Optional[Dict]:
        """Check if a new outlook report is available."""
        if not HAS_REQUESTS:
            logger.error("requests library not available")
            return None

        logger.info(f"Checking for new {self.source_config['name']}...")

        # Try the known current URLs first
        pubid = self.config[self.source].get("last_pubid", self.source_config["current_pubid"])
        folder = self.config[self.source].get("last_folder", self.source_config["current_folder"])

        # Try incrementing to find new release
        for offset in range(0, 20, 1):
            test_pubid = pubid + offset
            test_folder = folder + offset

            excel_url = self.source_config["excel_template"].format(folder_id=test_folder)

            try:
                response = self.session.head(excel_url, timeout=10, allow_redirects=True)
                if response.status_code == 200:
                    content_length = response.headers.get('Content-Length', 0)
                    if int(content_length) > 10000:  # Minimum reasonable file size
                        logger.info(f"Found valid file at pubid offset +{offset}")
                        return {
                            "pubid": test_pubid,
                            "folder": test_folder,
                            "excel_url": excel_url,
                            "content_length": content_length
                        }
            except requests.RequestException as e:
                logger.debug(f"Error checking pubid {test_pubid}: {e}")
                continue

            time.sleep(0.5)  # Be polite to the server

        logger.info("No new release found")
        return None

    def download(self, url: str, target_path: Path) -> bool:
        """Download a file with retry logic."""
        if not HAS_REQUESTS:
            logger.error("requests library not available")
            return False

        max_retries = 4
        retry_delays = [2, 4, 8, 16]

        for attempt in range(max_retries):
            try:
                logger.info(f"Downloading: {url}")
                response = self.session.get(url, timeout=60, stream=True)
                response.raise_for_status()

                with open(target_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

                logger.info(f"Downloaded to: {target_path}")
                return True

            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    delay = retry_delays[attempt]
                    logger.warning(f"Download failed, retrying in {delay}s: {e}")
                    time.sleep(delay)
                else:
                    logger.error(f"Download failed after {max_retries} attempts: {e}")
                    return False

        return False

    def archive_existing(self, filepath: Path):
        """Archive an existing file before replacing."""
        if filepath.exists():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"{filepath.stem}_{timestamp}{filepath.suffix}"
            archive_path = ARCHIVE_DIR / archive_name
            shutil.copy2(filepath, archive_path)
            logger.info(f"Archived: {archive_path}")

    def collect(self) -> Optional[Path]:
        """Main collection process."""
        logger.info(f"Starting collection for {self.source_config['name']}")

        # Check for updates
        update_info = self.check_for_update()

        if not update_info and not self.force:
            logger.info("No new data available")
            return None

        if not update_info:
            # Use last known URLs for forced download
            folder = self.config[self.source].get("last_folder", self.source_config["current_folder"])
            update_info = {
                "folder": folder,
                "excel_url": self.source_config["excel_template"].format(folder_id=folder),
                "pubid": self.config[self.source].get("last_pubid", self.source_config["current_pubid"])
            }

        # Generate local filename with date
        now = datetime.now()
        local_filename = self.source_config["local_filename"].format(
            year=now.year,
            month=f"{now.month:02d}"
        )
        target_path = DATA_DIR / local_filename

        # Archive existing file
        self.archive_existing(target_path)

        # Download new file
        if not self.download(update_info["excel_url"], target_path):
            return None

        # Verify download
        if not target_path.exists() or target_path.stat().st_size < 10000:
            logger.error("Downloaded file is invalid or too small")
            return None

        # Update config
        file_hash = self._get_file_hash(target_path)
        self.config[self.source].update({
            "last_download": datetime.now().isoformat(),
            "last_file_hash": file_hash,
            "last_pubid": update_info.get("pubid"),
            "last_folder": update_info.get("folder"),
            "download_count": self.config[self.source].get("download_count", 0) + 1
        })
        self._save_config()

        logger.info(f"Collection complete: {target_path}")
        return target_path

    def get_status(self) -> Dict:
        """Get current status of this data source."""
        return {
            "source": self.source,
            "name": self.source_config["name"],
            "last_download": self.config[self.source].get("last_download"),
            "last_pubid": self.config[self.source].get("last_pubid"),
            "download_count": self.config[self.source].get("download_count", 0),
            "release_day": self.source_config["release_day"]
        }


def log_collection(source: str, success: bool, filepath: Optional[Path] = None):
    """Log collection attempt to database."""
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS collection_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                success INTEGER NOT NULL,
                filepath TEXT,
                notes TEXT
            )
        """)
        conn.execute(
            "INSERT INTO collection_log (source, success, filepath) VALUES (?, ?, ?)",
            (source, 1 if success else 0, str(filepath) if filepath else None)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not log to database: {e}")


def main():
    parser = argparse.ArgumentParser(description='Collect ERS monthly outlook data')
    parser.add_argument('--source', choices=list(OUTLOOK_SOURCES.keys()),
                        help='Specific source to collect')
    parser.add_argument('--all', action='store_true', help='Collect all sources')
    parser.add_argument('--force', action='store_true', help='Force download even if no update detected')
    parser.add_argument('--status', action='store_true', help='Show status of all sources')
    args = parser.parse_args()

    if args.status:
        print("\nERS Monthly Outlook Status")
        print("=" * 60)
        for source_name in OUTLOOK_SOURCES:
            collector = ERSMonthlyOutlookCollector(source_name)
            status = collector.get_status()
            print(f"\n{status['name']}:")
            print(f"  Last download: {status['last_download'] or 'Never'}")
            print(f"  Last pub ID: {status['last_pubid']}")
            print(f"  Total downloads: {status['download_count']}")
            print(f"  Release day: ~{status['release_day']}th of month")
        return

    sources_to_collect = []
    if args.all:
        sources_to_collect = list(OUTLOOK_SOURCES.keys())
    elif args.source:
        sources_to_collect = [args.source]
    else:
        parser.print_help()
        return

    for source in sources_to_collect:
        print(f"\n{'='*60}")
        print(f"Collecting: {OUTLOOK_SOURCES[source]['name']}")
        print(f"{'='*60}")

        collector = ERSMonthlyOutlookCollector(source, force=args.force)
        filepath = collector.collect()

        log_collection(source, filepath is not None, filepath)

        if filepath:
            print(f"SUCCESS: Downloaded to {filepath}")
        else:
            print("No new data or download failed")


if __name__ == '__main__':
    main()
