# collectors/epa_echo/illinois_capacity_collector.py
"""
Illinois IEPA Title V Permit Capacity Collector

This script downloads Title V operating permits from the Illinois EPA Document Explorer using Selenium and extracts equipment-level capacity data for soybean/oilseed processing facilities.
It follows the pattern of iowa_capacity_collector.py for generalizability.

Dependencies:
- selenium
- webdriver-manager
- pdfplumber
- openpyxl
- pandas
- re

Install with: pip install selenium webdriver-manager pdfplumber openpyxl pandas

Note: Selenium is used because the Document Explorer is a dynamic web app. Update selectors based on page inspection.
For scaling, the downloader is state-specific; parser is modular.

"""

import os
import argparse
import logging
from datetime import datetime
import time
from typing import Dict, Any, List

import pdfplumber
import pandas as pd
import json
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Facility mapping - BOA IDs from research; update as needed
IL_SOY_PERMITS = {
    'adm_decatur': {
        'name': 'Archer Daniels Midland Co',
        'city': 'Decatur',
        'county': 'Macon',
        'frs_registry_id': '110000352731',
        'boa_id': '115015AAE',
        'permit_number': '',
        'permit_url': '',
    },
    'bunge_cairo': {
        'name': 'Bunge North America Inc',
        'city': 'Cairo',
        'county': 'Alexander',
        'frs_registry_id': '',  # Update if known
        'boa_id': '003005AAI',
        'permit_number': '',
        'permit_url': '',
    },
    'adm_quincy': {
        'name': 'ADM Quincy',
        'city': 'Quincy',
        'county': 'Adams',
        'frs_registry_id': '',  # Update if known
        'boa_id': '001815MF',
        'permit_number': '',
        'permit_url': '',
    },
    'cargill_bloomington': {
        'name': 'Cargill Inc',
        'city': 'Bloomington',
        'county': 'McLean',
        'frs_registry_id': '',  # Update if known
        'boa_id': '113030019',  # Inferred from permit number; verify
        'permit_number': '96030019',
        'permit_url': '',
    },
    'incobrasa_gilman': {
        'name': 'Incobrasa Industries Ltd',
        'city': 'Gilman',
        'county': 'Iroquois',
        'frs_registry_id': '',  # Update if known
        'boa_id': '075805AAA',  # Placeholder; verify from IEPA or ECHO
        'permit_number': '',
        'permit_url': '',
    },
}

RAW_DIR = 'collectors/epa_echo/raw'
OUTPUT_DIR = 'collectors/epa_echo/output'
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Keyword lists
CRUSH_KEYWORDS = ['extraction', 'extractor', 'flaker', 'flaking', 'conditioning', 'dehulling', 'cracking', 'soybean processing']
REFINERY_KEYWORDS = ['oil refining', 'bleaching', 'deodorizing', 'degumming', 'caustic refining', 'hydrogenation']
BIODIESEL_KEYWORDS = ['biodiesel', 'transesterification', 'methanol', 'glycerin', 'renewable diesel']
REFINING_TYPES = {
    'RBD': ['refined', 'bleached', 'deodorized'],
    'RB': ['refined', 'bleached'],
    'Degummed': ['degummed'],
    'Caustic Refined': ['caustic', 'refined'],
}

CAPACITY_REGEX = r'(\d+\.?\d*)\s*(tons/hour|bushels/day|bushels/hour|million gallons/year|MGY|tons/year|lb/hr)'

class TitleVCapacityParser:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.text = ''
        self.equipment = []
        self.crush_capacity = 0.0
        self.crush_units = 0
        self.crush_description = ''
        self.crush_bushels_day = 0
        self.refinery_capacity_hour = 0.0
        self.refinery_capacity_year = 0.0
        self.refinery_units = 0
        self.refinery_description = ''
        self.refining_type = ''
        self.biodiesel_capacity = 0.0
        self.biodiesel_units = 0
        self.total_units = 0

    def parse(self):
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                self.text += page.extract_text() or ''

        lines = self.text.split('\n')
        in_equipment_section = False
        current_unit = None

        for line in lines:
            if 'EMISSION UNITS' in line or 'SOURCE SUMMARY' in line:
                in_equipment_section = True
            if in_equipment_section:
                if 'EU' in line or 'emission unit' in line.lower():
                    current_unit = {'description': line, 'capacity': 0, 'unit': '', 'type': 'other'}
                    self.equipment.append(current_unit)
                    self.total_units += 1

                match = re.search(CAPACITY_REGEX, line, re.I)
                if match and current_unit:
                    value = float(match.group(1))
                    unit = match.group(2).lower()
                    current_unit['capacity'] = value
                    current_unit['unit'] = unit

                    lower_line = line.lower()
                    if any(k in lower_line for k in CRUSH_KEYWORDS):
                        current_unit['type'] = 'crush'
                        self.crush_units += 1
                        if unit == 'tons/hour' and value > self.crush_capacity:
                            self.crush_capacity = value
                            self.crush_description += line + '; '
                        elif unit == 'bushels/day' and value > self.crush_bushels_day:
                            self.crush_bushels_day = value
                    elif any(k in lower_line for k in REFINERY_KEYWORDS):
                        current_unit['type'] = 'refinery'
                        self.refinery_units += 1
                        if unit == 'tons/hour' and value > self.refinery_capacity_hour:
                            self.refinery_capacity_hour = value
                            self.refinery_description += line + '; '
                        elif unit == 'tons/year' and value > self.refinery_capacity_year:
                            self.refinery_capacity_year = value
                            self.refinery_description += line + '; '
                        for rtype, keywords in REFINING_TYPES.items():
                            if all(k in lower_line for k in keywords):
                                self.refining_type = rtype
                                break
                    elif any(k in lower_line for k in BIODIESEL_KEYWORDS):
                        current_unit['type'] = 'biodiesel'
                        self.biodiesel_units += 1
                        if unit in ['million gallons/year', 'mgy'] and value > self.biodiesel_capacity:
                            self.biodiesel_capacity = value

                if 'END OF SECTION' in line or 'SECTION B' in line:
                    in_equipment_section = False

def download_permits(facilities: List[str]):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    for key in facilities:
        info = IL_SOY_PERMITS[key]
        raw_path = os.path.join(RAW_DIR, f"il_{key}_titlev.pdf")
        if os.path.exists(raw_path) and os.path.getsize(raw_path) > 10000:
            logger.info(f"Skipping download for {key}, valid file exists")
            continue

        try:
            driver.get("https://webapps.illinois.gov/EPA/DocumentExplorer/Attributes")

            wait = WebDriverWait(driver, 10)
            # Fill search form - update selectors from inspection
            boa_input = wait.until(EC.presence_of_element_located((By.ID, 'boaId')))  # Update ID, e.g., 'txtBoaId'
            boa_input.send_keys(info['boa_id'])

            search_button = driver.find_element(By.ID, 'btnSearch')  # Update
            search_button.click()

            # Wait for results, click on the facility
            facility_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, info['name'])))  # Or XPath for row
            facility_link.click()

            # In facility page, find documents, look for latest Title V permit PDF
            documents_table = wait.until(EC.presence_of_element_located((By.ID, 'documentsTable')))  # Update
            rows = documents_table.find_elements(By.TAG_NAME, 'tr')
            latest_permit_url = None
            latest_date = None
            for row in rows[1:]:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if 'Title V' in cells[0].text and 'Operating' in cells[0].text:  # Type column
                    date_str = cells[1].text  # Issue date
                    date = datetime.strptime(date_str, '%m/%d/%Y')  # Adjust format
                    if not latest_date or date > latest_date:
                        latest_date = date
                        link = row.find_element(By.LINK_TEXT, 'Download PDF')  # Update
                        latest_permit_url = link.get_attribute('href')
                        info['permit_number'] = cells[2].text

            if latest_permit_url:
                info['permit_url'] = latest_permit_url
                # Download using requests
                response = requests.get(latest_permit_url)
                if response.status_code == 200:
                    with open(raw_path, 'wb') as f:
                        f.write(response.content)
                    logger.info(f"Downloaded permit for {key} to {raw_path}")
                else:
                    logger.error(f"Failed to download {latest_permit_url}")
            else:
                logger.warning(f"No Title V permit found for {key}")

            time.sleep(1.5)

        except Exception as e:
            logger.error(f"Error downloading for {key}: {str(e)} - Check selectors or site changes")
        finally:
            driver.quit()

# parse_permits, output_results, main similar to previous

def parse_permits(facilities: List[str]):
    results = {}
    all_equipment = []
    for key in facilities:
        raw_path = os.path.join(RAW_DIR, f"il_{key}_titlev.pdf")
        if not os.path.exists(raw_path):
            logger.warning(f"No PDF for {key}, skipping parse")
            continue

        parser = TitleVCapacityParser(raw_path)
        parser.parse()

        info = IL_SOY_PERMITS[key]
        result = {
            'facility_name': info['name'],
            'permit_number': info['permit_number'],
            'facility_description': 'Soybean Oil Mills (SIC 2075)',
            'crush_capacity_tons_per_hour': parser.crush_capacity,
            'crush_capacity_bushels_per_day': parser.crush_bushels_day,
            'crush_description': parser.crush_description,
            'has_refinery': parser.refinery_units > 0,
            'refinery_capacity_tons_per_hour': parser.refinery_capacity_hour,
            'refinery_capacity_tons_per_year': parser.refinery_capacity_year,
            'refinery_description': parser.refinery_description,
            'refining_type': parser.refining_type,
            'has_biodiesel': parser.biodiesel_units > 0,
            'biodiesel_capacity_mgy': parser.biodiesel_capacity,
            'total_emission_units_found': parser.total_units,
            'crush_units': parser.crush_units,
            'refinery_units': parser.refinery_units,
            'biodiesel_units': parser.biodiesel_units
        }
        results[key] = result
        logger.info(f"Parsed {key}: Crush {result['crush_capacity_tons_per_hour']} t/h, Refinery {result['has_refinery']}, Biodiesel {result['has_biodiesel']}")

        # Collect equipment
        for eq in parser.equipment:
            all_equipment.append({
                'Facility': key,
                'Description': eq['description'],
                'Capacity': eq['capacity'],
                'Unit': eq['unit'],
                'Type': eq['type']
            })

    return results, all_equipment

def output_results(results: Dict[str, Any], all_equipment: List[Dict]):
    date_str = datetime.now().strftime('%Y-%m-%d')
    json_path = os.path.join(OUTPUT_DIR, f'illinois_soy_capacity_{date_str}.json')
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Wrote JSON to {json_path}")

    excel_path = os.path.join(OUTPUT_DIR, f'illinois_soy_capacity_{date_str}.xlsx')
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        pd.DataFrame(results).T.to_excel(writer, sheet_name='Capacity Summary')
        pd.DataFrame(all_equipment).to_excel(writer, sheet_name='Equipment Detail', index=False)
    logger.info(f"Wrote Excel to {excel_path}")

def main():
    parser = argparse.ArgumentParser(description='Illinois Capacity Collector')
    parser.add_argument('--download-only', action='store_true', help='Only download PDFs')
    parser.add_argument('--parse-only', action='store_true', help='Only parse existing PDFs')
    parser.add_argument('--facility', type=str, help='Single facility key (e.g., adm_decatur)')
    args = parser.parse_args()

    facilities = [args.facility] if args.facility else list(IL_SOY_PERMITS.keys())

    if not args.parse_only:
        download_permits(facilities)

    if not args.download_only:
        results, all_equipment = parse_permits(facilities)
        output_results(results, all_equipment)

    # Summary table
    print("\nSummary:")
    for key in facilities:
        if key in results:
            r = results[key]
            print(f"{key}: Crush {r['crush_capacity_tons_per_hour']} t/h, Refinery {r['refinery_capacity_tons_per_hour']} t/h, Biodiesel {r['biodiesel_capacity_mgy']} MGY")

if __name__ == '__main__':
    main()

# Notes:
# - Update Selenium selectors by inspecting the Document Explorer page (e.g., ID for BOA input, search button, etc.).
# - If site changes, adjust the code.
# - For missing BOA IDs, search IEPA or ECHO for the facility to find them.
# - Parser may need tuning for Illinois permit formatting (e.g., section headers, capacity phrasing).
# - To load to DB, update scripts/load_echo_capacity_data.py with 'IL'.