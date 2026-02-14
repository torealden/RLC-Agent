"""
Download USDA FAS Crop Production Maps

Downloads all available crop production maps from USDA FAS IPAD
and organizes them into the domain_knowledge/crop_maps folder.

Naming convention: {country}_{commodity}.png/jpg (lowercase, underscores)

Updated: January 2026 - New URL patterns from USDA site reorganization
"""

import os
import sys
import requests
import time
import shutil
import argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin

# Base URLs
BASE_URL = "https://ipad.fas.usda.gov"

# Output directory
OUTPUT_DIR = Path(__file__).parent.parent / "domain_knowledge" / "crop_maps"
ARCHIVE_DIR = OUTPUT_DIR / "_archive"

# Map definitions: (folder_name, [(url_path, output_filename), ...])
# Updated January 2026 with verified working URLs
CROP_MAPS = {
    # =========================================================================
    # UNITED STATES - Major producer of corn, soybeans, wheat
    # =========================================================================
    "us": [
        ("/rssiws/al/crop_production_maps/US/USA_Corn.png", "us_corn.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Soybean.png", "us_soybean.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Wheat.png", "us_wheat.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Cotton.png", "us_cotton.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Rice.png", "us_rice.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Sorghum.png", "us_sorghum.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Barley.png", "us_barley.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Oats.png", "us_oats.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Peanut.png", "us_peanut.png"),
        ("/rssiws/al/crop_production_maps/US/USA_Sunflowerseed.png", "us_sunflowerseed.png"),
    ],

    # =========================================================================
    # CHINA - Major producer of corn, rice, wheat, soybeans
    # =========================================================================
    "china": [
        ("/rssiws/al/crop_production_maps/China/China_corn.jpg", "china_corn.jpg"),
        ("/rssiws/al/crop_production_maps/China/China_soybean.jpg", "china_soybean.jpg"),
        ("/rssiws/al/crop_production_maps/China/China_wheat.jpg", "china_wheat.jpg"),
        ("/rssiws/al/crop_production_maps/China/China_cotton.jpg", "china_cotton.jpg"),
        ("/rssiws/al/crop_production_maps/China/China_peanut.jpg", "china_peanut.jpg"),
        ("/rssiws/al/crop_production_maps/China/China_rapeseed.jpg", "china_rapeseed.jpg"),
    ],

    # =========================================================================
    # AUSTRALIA - Major wheat and barley exporter
    # =========================================================================
    "australia": [
        ("/rssiws/al/crop_production_maps/Australia/Australia_Wheat.jpg", "australia_wheat.jpg"),
        ("/rssiws/al/crop_production_maps/Australia/Australia_Barley.jpg", "australia_barley.jpg"),
        ("/rssiws/al/crop_production_maps/Australia/Australia_Cotton.jpg", "australia_cotton.jpg"),
        ("/rssiws/al/crop_production_maps/Australia/Australia_Sorghum.jpg", "australia_sorghum.jpg"),
        ("/rssiws/al/crop_production_maps/Australia/Australia_Rapeseed.jpg", "australia_rapeseed.jpg"),
        ("/rssiws/al/crop_production_maps/Australia/Australia_canola.jpg", "australia_canola.jpg"),
    ],

    # =========================================================================
    # UKRAINE - Major grain and oilseed exporter
    # =========================================================================
    "ukraine": [
        ("/rssiws/al/crop_production_maps/Ukraine/Ukraine_Corn.jpg", "ukraine_corn.jpg"),
        ("/rssiws/al/crop_production_maps/Ukraine/Ukraine_wheat.jpg", "ukraine_wheat.jpg"),
        ("/rssiws/al/crop_production_maps/Ukraine/Ukraine_barley.jpg", "ukraine_barley.jpg"),
        ("/rssiws/al/crop_production_maps/Ukraine/Ukraine_soybean.jpg", "ukraine_soybean.jpg"),
        ("/rssiws/al/crop_production_maps/Ukraine/Ukraine_sunflowerseed.jpg", "ukraine_sunflowerseed.jpg"),
        ("/rssiws/al/crop_production_maps/Ukraine/Ukraine_rapeseed.jpg", "ukraine_rapeseed.jpg"),
    ],

    # =========================================================================
    # MEXICO - Corn, wheat, sorghum
    # =========================================================================
    "mexico": [
        ("/rssiws/al/crop_production_maps/Mexico/Municipality/Mexico_Corn.png", "mexico_corn.png"),
        ("/rssiws/al/crop_production_maps/Mexico/Municipality/Mexico_wheat.png", "mexico_wheat.png"),
        ("/rssiws/al/crop_production_maps/Mexico/Municipality/Mexico_Sorghum.png", "mexico_sorghum.png"),
    ],

    # =========================================================================
    # JAPAN - Rice, wheat
    # =========================================================================
    "japan": [
        ("/rssiws/al/crop_production_maps/Japan/Japan_rice.jpg", "japan_rice.jpg"),
        ("/rssiws/al/crop_production_maps/Japan/Japan_wheat.jpg", "japan_wheat.jpg"),
    ],

    # =========================================================================
    # RUSSIA - Major wheat and grain exporter
    # =========================================================================
    "russia": [
        ("/rssiws/al/crop_production_maps/Russia/Russia_Total_Barley.jpg", "russia_barley.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Spring_Barley.jpg", "russia_spring_barley.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Winter_Barley.jpg", "russia_winter_barley.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Corn.jpg", "russia_corn.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_millet.jpg", "russia_millet.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Oats.jpg", "russia_oats.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Rye.jpg", "russia_rye.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Soybean.jpg", "russia_soybean.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_sunflowerseed.jpg", "russia_sunflowerseed.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Total_Rapeseed.jpg", "russia_rapeseed.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Spring_Rapeseed.jpg", "russia_spring_rapeseed.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Winter_Rapeseed.jpg", "russia_winter_rapeseed.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Total_Wheat.jpg", "russia_wheat.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Spring_Wheat.jpg", "russia_spring_wheat.jpg"),
        ("/rssiws/al/crop_production_maps/Russia/Russia_Winter_Wheat.jpg", "russia_winter_wheat.jpg"),
    ],

    # =========================================================================
    # INDIA - Major rice, wheat, cotton producer
    # =========================================================================
    "india": [
        ("/rssiws/al/crop_production_maps/sasia/IND_Corn.png", "india_corn.png"),
        ("/rssiws/al/crop_production_maps/sasia/IND_Wheat.png", "india_wheat.png"),
        ("/rssiws/al/crop_production_maps/sasia/IND_Rice.png", "india_rice.png"),
        ("/rssiws/al/crop_production_maps/sasia/IND_Cotton.png", "india_cotton.png"),
        ("/rssiws/al/crop_production_maps/sasia/IND_Soybean.png", "india_soybean.png"),
        ("/rssiws/al/crop_production_maps/sasia/IND_Rapeseed.png", "india_rapeseed.png"),
        ("/rssiws/al/crop_production_maps/sasia/IND_Peanut.png", "india_peanut.png"),
        ("/rssiws/al/crop_production_maps/sasia/India_Sorghum.png", "india_sorghum.png"),
        ("/rssiws/al/crop_production_maps/sasia/India_Sunflowerseed.png", "india_sunflowerseed.png"),
    ],

    # =========================================================================
    # BRAZIL - Soybeans, corn, cotton, coffee
    # =========================================================================
    "brazil": [
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Soybean.png", "brazil_soybean.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Total_Corn.png", "brazil_corn.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_First_Season_Corn.png", "brazil_first_season_corn.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Second_Season_Corn.png", "brazil_second_season_corn.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Cotton.png", "brazil_cotton.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Rice.png", "brazil_rice.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Wheat.png", "brazil_wheat.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Sugarcane.png", "brazil_sugarcane.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Arabica_Coffee.png", "brazil_arabica_coffee.png"),
        ("/rssiws/al/crop_production_maps/Brazil/Municipality/Brazil_Robusta_Coffee.png", "brazil_robusta_coffee.png"),
    ],

    # =========================================================================
    # ARGENTINA - Soybeans, corn, wheat
    # =========================================================================
    "argentina": [
        ("/rssiws/al/crop_production_maps/ssa/AR_Delegation/Argentina_Soybean.png", "argentina_soybean.png"),
        ("/rssiws/al/crop_production_maps/ssa/AR_Delegation/Argentina_Corn.png", "argentina_corn.png"),
        ("/rssiws/al/crop_production_maps/ssa/AR_Delegation/Argentina_Wheat.png", "argentina_wheat.png"),
        ("/rssiws/al/crop_production_maps/ssa/AR_Delegation/Argentina_Sunflowerseed.png", "argentina_sunflowerseed.png"),
        ("/rssiws/al/crop_production_maps/ssa/AR_Delegation/Argentina_Barley.png", "argentina_barley.png"),
    ],

    # =========================================================================
    # EUROPEAN UNION
    # =========================================================================
    "eu": [
        ("/rssiws/al/crop_production_maps/Europe/EU_Barley.png", "eu_barley.png"),
        ("/rssiws/al/crop_production_maps/Europe/EU_Corn.png", "eu_corn.png"),
        ("/rssiws/al/crop_production_maps/Europe/EU_Oats.png", "eu_oats.png"),
        ("/rssiws/al/crop_production_maps/Europe/EU_Rapeseed.png", "eu_rapeseed.png"),
        ("/rssiws/al/crop_production_maps/Europe/EU_Rye.png", "eu_rye.png"),
        ("/rssiws/al/crop_production_maps/Europe/EU_Sunflowerseed.png", "eu_sunflowerseed.png"),
        ("/rssiws/al/crop_production_maps/Europe/EU_Wheat.png", "eu_wheat.png"),
    ],

    # =========================================================================
    # CANADA
    # =========================================================================
    "canada": [
        ("/rssiws/al/crop_production_maps/Canada/Canada_Wheat.png", "canada_wheat.png"),
        ("/rssiws/al/crop_production_maps/Canada/Canada_Corn.png", "canada_corn.png"),
        ("/rssiws/al/crop_production_maps/Canada/Canada_Barley.png", "canada_barley.png"),
        ("/rssiws/al/crop_production_maps/Canada/Canada_Rapeseed.png", "canada_rapeseed.png"),
        ("/rssiws/al/crop_production_maps/Canada/Canada_Soybean.png", "canada_soybean.png"),
        ("/rssiws/al/crop_production_maps/Canada/Canada_Oats.png", "canada_oats.png"),
    ],

    # =========================================================================
    # TURKEY
    # =========================================================================
    "turkey": [
        ("/rssiws/al/crop_production_maps/metu/Turkey_Barley.png", "turkey_barley.png"),
        ("/rssiws/al/crop_production_maps/metu/Turkey_Corn.png", "turkey_corn.png"),
        ("/rssiws/al/crop_production_maps/metu/Turkey_Cotton.png", "turkey_cotton.png"),
        ("/rssiws/al/crop_production_maps/metu/Turkey_wheat.jpg", "turkey_wheat.jpg"),
        ("/rssiws/al/crop_production_maps/metu/Turkey_Sunflowerseed.png", "turkey_sunflowerseed.png"),
    ],

    # =========================================================================
    # SOUTH AFRICA
    # =========================================================================
    "south_africa": [
        ("/rssiws/al/crop_production_maps/safrica/SouthAfrica_Corn.png", "south_africa_corn.png"),
        ("/rssiws/al/crop_production_maps/safrica/SouthAfrica_Sorghum.png", "south_africa_sorghum.png"),
        ("/rssiws/al/crop_production_maps/safrica/SouthAfrica_Soybean.png", "south_africa_soybean.png"),
        ("/rssiws/al/crop_production_maps/safrica/SouthAfrica_Sunflowerseed.png", "south_africa_sunflowerseed.png"),
        ("/rssiws/al/crop_production_maps/safrica/SouthAfrica_Wheat.png", "south_africa_wheat.png"),
    ],

    # =========================================================================
    # PAKISTAN
    # =========================================================================
    "pakistan": [
        ("/rssiws/al/crop_production_maps/sasia/Pakistan_Cotton.png", "pakistan_cotton.png"),
        ("/rssiws/al/crop_production_maps/sasia/Pakistan_Rice.png", "pakistan_rice.png"),
        ("/rssiws/al/crop_production_maps/sasia/Pakistan_Wheat.png", "pakistan_wheat.png"),
    ],

    # =========================================================================
    # BANGLADESH
    # =========================================================================
    "bangladesh": [
        ("/rssiws/al/crop_production_maps/sasia/Bangladesh_Wheat.png", "bangladesh_wheat.png"),
        ("/rssiws/al/crop_production_maps/sasia/Bangladesh_Rice.png", "bangladesh_rice.png"),
    ],

    # =========================================================================
    # SOUTHEAST ASIA
    # =========================================================================
    "indonesia": [
        ("/rssiws/al/crop_production_maps/seasia/Indonesia_corn.png", "indonesia_corn.png"),
        ("/rssiws/al/crop_production_maps/seasia/Indonesia_palm_oil.png", "indonesia_palm_oil.png"),
        ("/rssiws/al/crop_production_maps/seasia/Indonesia_Peanut.png", "indonesia_peanut.png"),
        ("/rssiws/al/crop_production_maps/seasia/Indonesia_rice.png", "indonesia_rice.png"),
        ("/rssiws/al/crop_production_maps/seasia/Indonesia_Soybean.png", "indonesia_soybean.png"),
    ],

    "malaysia": [
        ("/rssiws/al/crop_production_maps/seasia/Malaysia_Palm_Oil.png", "malaysia_palm_oil.png"),
        ("/rssiws/al/crop_production_maps/seasia/Malaysia_Rice.png", "malaysia_rice.png"),
    ],

    "thailand": [
        ("/rssiws/al/crop_production_maps/seasia/TH_Corn.png", "thailand_corn.png"),
        ("/rssiws/al/crop_production_maps/seasia/TH_Palm_Oil.png", "thailand_palm_oil.png"),
        ("/rssiws/al/crop_production_maps/seasia/TH_Rice.png", "thailand_rice.png"),
        ("/rssiws/al/crop_production_maps/seasia/TH_Soybean.png", "thailand_soybean.png"),
    ],

    "vietnam": [
        ("/rssiws/al/crop_production_maps/seasia/Vietnam_Corn.png", "vietnam_corn.png"),
        ("/rssiws/al/crop_production_maps/seasia/Vietnam_Peanut.png", "vietnam_peanut.png"),
        ("/rssiws/al/crop_production_maps/seasia/Vietnam_Soybean.png", "vietnam_soybean.png"),
        ("/rssiws/al/crop_production_maps/seasia/Vietnam_Total_Rice.png", "vietnam_rice.png"),
    ],

    "philippines": [
        ("/rssiws/al/crop_production_maps/seasia/Phil_Total_Corn.png", "philippines_corn.png"),
        ("/rssiws/al/crop_production_maps/seasia/Phil_Palm_Oil.png", "philippines_palm_oil.png"),
        ("/rssiws/al/crop_production_maps/seasia/Phil_Total_Peanut.png", "philippines_peanut.png"),
        ("/rssiws/al/crop_production_maps/seasia/Phil_Total_Rice.png", "philippines_rice.png"),
        ("/rssiws/al/crop_production_maps/seasia/Phil_Total_Soybean.png", "philippines_soybean.png"),
    ],

    "burma": [
        ("/rssiws/al/crop_production_maps/seasia/Burma_Corn.png", "burma_corn.png"),
        ("/rssiws/al/crop_production_maps/seasia/Burma_cotton.png", "burma_cotton.png"),
        ("/rssiws/al/crop_production_maps/seasia/Burma_Peanut.png", "burma_peanut.png"),
        ("/rssiws/al/crop_production_maps/seasia/Burma_rice.png", "burma_rice.png"),
        ("/rssiws/al/crop_production_maps/seasia/Burma_Soybean.png", "burma_soybean.png"),
    ],

    "cambodia": [
        ("/rssiws/al/crop_production_maps/seasia/Cambodia_corn.png", "cambodia_corn.png"),
        ("/rssiws/al/crop_production_maps/seasia/Cambodia_Rice.png", "cambodia_rice.png"),
    ],

    "laos": [
        ("/rssiws/al/crop_production_maps/seasia/Laos_Corn.png", "laos_corn.png"),
        ("/rssiws/al/crop_production_maps/seasia/Laos_Rice.png", "laos_rice.png"),
    ],

    # =========================================================================
    # WEST AFRICA
    # =========================================================================
    "nigeria": [
        ("/rssiws/al/crop_production_maps/wafrica/Nigeria_Corn.png", "nigeria_corn.png"),
        ("/rssiws/al/crop_production_maps/wafrica/Nigeria_Millet.png", "nigeria_millet.png"),
        ("/rssiws/al/crop_production_maps/wafrica/Nigeria_Rice.png", "nigeria_rice.png"),
        ("/rssiws/al/crop_production_maps/wafrica/Nigeria_Sorghum.png", "nigeria_sorghum.png"),
    ],
}


def archive_existing_map(file_path: Path) -> bool:
    """Archive an existing map file with timestamp."""
    if not file_path.exists():
        return False

    # Create archive directory if needed
    archive_country_dir = ARCHIVE_DIR / file_path.parent.name
    archive_country_dir.mkdir(parents=True, exist_ok=True)

    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y%m%d")
    stem = file_path.stem
    suffix = file_path.suffix
    archive_name = f"{stem}_{timestamp}{suffix}"
    archive_path = archive_country_dir / archive_name

    # Don't archive if we already have one from today
    if archive_path.exists():
        return False

    shutil.copy2(file_path, archive_path)
    return True


def download_file(url: str, output_path: Path, archive: bool = False) -> bool:
    """Download a file from URL to output path."""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            # Archive existing file if requested
            if archive and output_path.exists():
                archive_existing_map(output_path)

            output_path.write_bytes(response.content)
            return True
        else:
            return False
    except Exception as e:
        print(f"  Error: {e}")
        return False


def main():
    """Download all crop production maps."""
    parser = argparse.ArgumentParser(description='Download USDA FAS Crop Production Maps')
    parser.add_argument('--country', '-c', type=str, help='Download maps for specific country only')
    parser.add_argument('--force', '-f', action='store_true', help='Force re-download even if file exists')
    parser.add_argument('--archive', '-a', action='store_true', help='Archive existing maps before downloading')
    parser.add_argument('--list', '-l', action='store_true', help='List available countries')
    args = parser.parse_args()

    if args.list:
        print("Available countries:")
        for country in sorted(CROP_MAPS.keys()):
            print(f"  {country} ({len(CROP_MAPS[country])} maps)")
        return

    print("USDA FAS Crop Production Map Downloader")
    print("=" * 60)

    total_downloaded = 0
    total_failed = 0
    total_skipped = 0

    countries_to_process = [args.country] if args.country else CROP_MAPS.keys()

    for country in countries_to_process:
        if country not in CROP_MAPS:
            print(f"Unknown country: {country}")
            continue

        maps = CROP_MAPS[country]
        country_dir = OUTPUT_DIR / country
        country_dir.mkdir(parents=True, exist_ok=True)

        print(f"\n{country.upper()}:")

        for url_path, filename in maps:
            output_path = country_dir / filename

            # Skip if already exists (unless force)
            if output_path.exists() and not args.force:
                print(f"  [SKIP] {filename} (already exists)")
                total_skipped += 1
                continue

            url = urljoin(BASE_URL, url_path)
            print(f"  Downloading {filename}...", end=" ", flush=True)

            if download_file(url, output_path, archive=args.archive):
                print("OK")
                total_downloaded += 1
            else:
                print("FAILED")
                total_failed += 1

            # Be nice to the server
            time.sleep(0.3)

    print("\n" + "=" * 60)
    print(f"Download complete!")
    print(f"  Downloaded: {total_downloaded}")
    print(f"  Skipped: {total_skipped}")
    print(f"  Failed: {total_failed}")


if __name__ == "__main__":
    main()
