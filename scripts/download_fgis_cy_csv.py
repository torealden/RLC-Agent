"""
Download the latest FGIS Export Grain Report CY{year}.csv and reload bronze.

FGIS publishes the calendar-year CSV at
  https://fgisonline.ams.usda.gov/ExportGrainReport/CY{YYYY}.csv

The file is updated every Monday (after the AMS WA_GR101 weekly grain
inspection release at https://www.ams.usda.gov/mnreports/wa_gr101.txt).
This script:

  1. Downloads the current CY{year}.csv to data/raw/cross_commodity/
  2. Backs up the prior version with a date stamp
  3. Calls load_fgis_inspections.py to refresh bronze.fgis_inspections_history

Usage:
  python scripts/download_fgis_cy_csv.py              # current calendar year
  python scripts/download_fgis_cy_csv.py --year 2025  # specific year
  python scripts/download_fgis_cy_csv.py --no-load    # skip the reload step

Scheduled task suggestion:
  schtasks /Create /TN "\\RLC\\FGIS Inspections Weekly" /SC WEEKLY /D MON \\
    /ST 08:30 /TR "python C:\\dev\\RLC-Agent\\scripts\\download_fgis_cy_csv.py"
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger(__name__)


FGIS_URL_TEMPLATE = "https://fgisonline.ams.usda.gov/ExportGrainReport/CY{year}.csv"
TARGET_DIR = PROJECT_ROOT / "data" / "raw" / "cross_commodity"


def download_year(year: int, target_dir: Path = TARGET_DIR) -> Path:
    """Download CY{year}.csv. Returns the path written."""
    url = FGIS_URL_TEMPLATE.format(year=year)
    target = target_dir / f"CY{year}.csv"
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        backup = target.with_suffix(f".csv.bak.{date.today().isoformat()}")
        shutil.copy2(target, backup)
        logger.info(f"Backed up existing CY{year}.csv -> {backup.name}")

    logger.info(f"Downloading {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    target.write_bytes(r.content)
    size_kb = len(r.content) / 1024
    line_count = r.text.count('\n')
    logger.info(f"Saved {target.name} — {size_kb:,.1f} KB, ~{line_count:,} lines")
    return target


def reload_bronze(year: int) -> int:
    """Call the existing loader script as a subprocess. Returns its exit code."""
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "load_fgis_inspections.py"),
        "--years", str(year),
    ]
    logger.info(f"Reloading bronze: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    return proc.returncode


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=date.today().year)
    p.add_argument("--no-load", action="store_true",
                   help="Just download — skip the bronze reload step")
    args = p.parse_args()

    try:
        download_year(args.year)
    except requests.HTTPError as e:
        logger.error(f"Download failed: {e}")
        return 2
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 3

    if not args.no_load:
        rc = reload_bronze(args.year)
        if rc != 0:
            logger.error(f"Bronze reload exited with code {rc}")
            return rc

    logger.info("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
