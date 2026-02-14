"""
Update USDA FAS Crop Production Maps with Archiving

This script:
1. Archives existing maps with date stamps
2. Downloads new versions from USDA FAS IPAD
3. Logs changes for tracking

Can be scheduled for annual refresh (recommended: October after harvest estimates)

Usage:
    python scripts/update_crop_maps.py --all           # Update all maps
    python scripts/update_crop_maps.py --country us    # Update specific country
    python scripts/update_crop_maps.py --dry-run       # Show what would be updated
"""

import os
import sys
import json
import logging
import argparse
import shutil
from pathlib import Path
from datetime import datetime
import hashlib

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import the download script
from scripts.download_crop_maps import (
    CROP_MAPS, OUTPUT_DIR, ARCHIVE_DIR, BASE_URL,
    download_file, archive_existing_map
)

# Setup logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"crop_map_update_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_file_hash(filepath: Path) -> str:
    """Calculate MD5 hash of a file."""
    if not filepath.exists():
        return ""
    return hashlib.md5(filepath.read_bytes()).hexdigest()


def check_for_updates(country: str, dry_run: bool = False) -> dict:
    """Check if maps have changed for a country."""
    if country not in CROP_MAPS:
        logger.warning(f"Unknown country: {country}")
        return {"country": country, "checked": 0, "updated": 0, "unchanged": 0, "failed": 0}

    results = {"country": country, "checked": 0, "updated": 0, "unchanged": 0, "failed": 0, "details": []}
    maps = CROP_MAPS[country]
    country_dir = OUTPUT_DIR / country

    for url_path, filename in maps:
        results["checked"] += 1
        output_path = country_dir / filename
        url = f"{BASE_URL}{url_path}"

        try:
            # Download to temp location
            import tempfile
            import requests

            response = requests.get(url, timeout=30)
            if response.status_code != 200:
                results["failed"] += 1
                results["details"].append({"file": filename, "status": "failed", "reason": f"HTTP {response.status_code}"})
                continue

            # Compare with existing
            new_hash = hashlib.md5(response.content).hexdigest()
            old_hash = get_file_hash(output_path)

            if new_hash == old_hash:
                results["unchanged"] += 1
                results["details"].append({"file": filename, "status": "unchanged"})
            else:
                results["updated"] += 1
                if not dry_run:
                    # Archive old version
                    if output_path.exists():
                        archive_existing_map(output_path)
                    # Save new version
                    country_dir.mkdir(parents=True, exist_ok=True)
                    output_path.write_bytes(response.content)
                    results["details"].append({"file": filename, "status": "updated", "old_hash": old_hash, "new_hash": new_hash})
                    logger.info(f"Updated: {country}/{filename}")
                else:
                    results["details"].append({"file": filename, "status": "would_update", "old_hash": old_hash, "new_hash": new_hash})
                    logger.info(f"Would update: {country}/{filename}")

        except Exception as e:
            results["failed"] += 1
            results["details"].append({"file": filename, "status": "error", "reason": str(e)})
            logger.error(f"Error checking {country}/{filename}: {e}")

    return results


def update_all_maps(dry_run: bool = False) -> list:
    """Update maps for all countries."""
    all_results = []

    for country in CROP_MAPS.keys():
        logger.info(f"Checking {country}...")
        results = check_for_updates(country, dry_run)
        all_results.append(results)

        # Be nice to the server
        import time
        time.sleep(0.5)

    return all_results


def generate_report(results: list) -> str:
    """Generate a summary report of updates."""
    lines = [
        "=" * 60,
        "CROP MAP UPDATE REPORT",
        f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "=" * 60,
        ""
    ]

    total_checked = sum(r["checked"] for r in results)
    total_updated = sum(r["updated"] for r in results)
    total_unchanged = sum(r["unchanged"] for r in results)
    total_failed = sum(r["failed"] for r in results)

    lines.append(f"Total maps checked: {total_checked}")
    lines.append(f"Updated: {total_updated}")
    lines.append(f"Unchanged: {total_unchanged}")
    lines.append(f"Failed: {total_failed}")
    lines.append("")

    if total_updated > 0:
        lines.append("UPDATED MAPS:")
        for r in results:
            for d in r["details"]:
                if d["status"] in ["updated", "would_update"]:
                    lines.append(f"  - {r['country']}/{d['file']}")
        lines.append("")

    if total_failed > 0:
        lines.append("FAILED MAPS:")
        for r in results:
            for d in r["details"]:
                if d["status"] in ["failed", "error"]:
                    lines.append(f"  - {r['country']}/{d['file']}: {d.get('reason', 'unknown')}")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Update USDA FAS Crop Production Maps')
    parser.add_argument('--country', '-c', type=str, help='Update specific country only')
    parser.add_argument('--all', '-a', action='store_true', help='Update all countries')
    parser.add_argument('--dry-run', '-n', action='store_true', help='Check for updates without downloading')
    parser.add_argument('--report', '-r', type=str, help='Save report to file')
    args = parser.parse_args()

    if not args.all and not args.country:
        parser.print_help()
        print("\nError: Specify --all or --country")
        sys.exit(1)

    logger.info("Starting crop map update check...")

    if args.country:
        results = [check_for_updates(args.country, args.dry_run)]
    else:
        results = update_all_maps(args.dry_run)

    report = generate_report(results)
    print(report)

    if args.report:
        Path(args.report).write_text(report)
        logger.info(f"Report saved to {args.report}")

    # Save update log
    log_file = LOG_DIR / f"crop_map_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(log_file, 'w') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "dry_run": args.dry_run,
            "results": results
        }, f, indent=2)
    logger.info(f"Update log saved to {log_file}")


if __name__ == "__main__":
    main()
