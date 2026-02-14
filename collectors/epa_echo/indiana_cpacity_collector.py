# collectors/epa_echo/indiana_capacity_collector.py
"""
Indiana IDEM Title V Permit Capacity Collector - Simplified Version (No Selenium)

This version uses direct public PDF URLs from IDEM's permits.air.idem.in.gov site.
No browser automation, no webdriver_manager, no CAATS scraping required.

Just run it — it will download the 5 available permits and parse them.

Dependencies (all lightweight):
pip install pdfplumber requests pandas openpyxl

"""

import os
import argparse
import logging
from datetime import datetime
import time
import json
import re
from typing import Dict, List, Any

import requests
import pdfplumber
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Direct public PDF URLs (latest available from IDEM as of Feb 2026)
# Note: For White River Seymour, no direct PDF found; skip or add manually if available
IN_SOY_PERMITS = {
    'adm_frankfort': {
        'name': 'Archer Daniels Midland Company',
        'city': 'Frankfort',
        'permit_number': 'T023-47279-00011',  # 2024 renewal
        'permit_url': 'https://permits.air.idem.in.gov/47279f.pdf',
    },
    'bunge_morristown': {
        'name': 'Bunge North America (East), LLC',
        'city': 'Morristown',
        'permit_number': 'T145-36069-00035',
        'permit_url': 'https://permits.air.idem.in.gov/36883d.pdf',
    },
    'bunge_decatur': {
        'name': 'Bunge North America East LLC',
        'city': 'Decatur',
        'permit_number': 'T001-23640-00005',
        'permit_url': 'https://permits.air.idem.in.gov/28224d.pdf',
    },
    'cargill_lafayette': {
        'name': 'Cargill Inc Soybean Processing Division',
        'city': 'Lafayette',
        'permit_number': 'T157-34376-00038',
        'permit_url': 'https://permits.air.idem.in.gov/34376f.pdf',
    },
    'louis_dreyfus_claypool': {
        'name': 'Louis Dreyfus Company Agricultural Products LLC',
        'city': 'Claypool',
        'permit_number': 'T085-36889-00102',
        'permit_url': 'https://permits.air.idem.in.gov/39729d.pdf',
    },
    'white_river_seymour': {
        'name': 'White River Seymour LLC',
        'city': 'Seymour',
        'permit_number': 'Unknown (no recent public Title V PDF found)',
        'permit_url': None,  # Manual download needed if required
    },
}

RAW_DIR = 'collectors/epa_echo/raw'
OUTPUT_DIR = 'collectors/epa_echo/output'
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Keywords & regex tuned for Indiana permits
CRUSH_KEYWORDS = ['soybean', 'extraction', 'extractor', 'flaker', 'flaking', 'conditioning', 'dehulling', 'cracking', 'crush', 'throughput']
REFINERY_KEYWORDS = ['refining', 'bleaching', 'deodorizing', 'degumming', 'caustic', 'RBD', 'RB']
BIODIESEL_KEYWORDS = ['biodiesel', 'transesterification', 'methanol', 'glycerin']
REFINING_TYPES = {'RBD': ['refined', 'bleached', 'deodorized'], 'RB': ['refined', 'bleached'], 'Degummed': ['degummed']}

CAPACITY_REGEX = r'(\d{1,3}(?:,\d{3})*|\d+\.?\d*)\s*(tons? per hour|tons?/hour|bushels? per day|bushels?/day|million gallons per year|MGY|tons? per year)'

class TitleVCapacityParser:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.text = ''
        self.equipment: List[Dict] = []
        self.crush_capacity_hour = 0.0
        self.crush_bushels_day = 0
        self.crush_description = ''
        self.refinery_capacity_hour = 0.0
        self.refinery_capacity_year = 0.0
        self.refinery_description = ''
        self.refining_type = ''
        self.biodiesel_capacity_mgy = 0.0
        self.total_units = 0
        self.crush_units = 0
        self.refinery_units = 0
        self.biodiesel_units = 0

    def parse(self):
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text(layout=True) or ''
                self.text += text + '\n'

        lines = self.text.split('\n')
        current_unit = None

        for line in lines:
            line = line.strip()
            if not line: continue

            # New emission unit
            if re.search(r'^(EU|EP|Source|One \(1\)|Two \(2\))', line, re.I):
                if current_unit:
                    self.equipment.append(current_unit)
                current_unit = {'description': line, 'capacity': 0.0, 'unit': '', 'type': 'other'}
                self.total_units += 1

            if current_unit:
                current_unit['description'] += ' ' + line
                lower = current_unit['description'].lower()

                for val_str, unit in re.findall(CAPACITY_REGEX, line, re.I):
                    try:
                        value = float(val_str.replace(',', ''))
                        unit_norm = unit.lower().replace(' per ', '/')
                        current_unit['capacity'] = value
                        current_unit['unit'] = unit_norm

                        if any(k in lower for k in CRUSH_KEYWORDS):
                            current_unit['type'] = 'crush'
                            self.crush_units += 1
                            self.crush_description += line + '; '
                            if 'tons/hour' in unit_norm and value > self.crush_capacity_hour:
                                self.crush_capacity_hour = value
                            elif 'bushels/day' in unit_norm and value > self.crush_bushels_day:
                                self.crush_bushels_day = value
                        elif any(k in lower for k in REFINERY_KEYWORDS):
                            current_unit['type'] = 'refinery'
                            self.refinery_units += 1
                            self.refinery_description += line + '; '
                            if 'tons/hour' in unit_norm and value > self.refinery_capacity_hour:
                                self.refinery_capacity_hour = value
                            elif 'tons/year' in unit_norm and value > self.refinery_capacity_year:
                                self.refinery_capacity_year = value
                            for rtype, kw in REFINING_TYPES.items():
                                if all(k in lower for k in kw):
                                    self.refining_type = rtype
                                    break
                        elif any(k in lower for k in BIODIESEL_KEYWORDS):
                            current_unit['type'] = 'biodiesel'
                            self.biodiesel_units += 1
                            if 'million gallons/year' in unit_norm or 'mgy' in unit_norm:
                                self.biodiesel_capacity_mgy = value
                    except:
                        continue

        if current_unit:
            self.equipment.append(current_unit)

        self.crush_description = self.crush_description.strip('; ')
        self.refinery_description = self.refinery_description.strip('; ')

def download_permits(facilities: List[str]):
    for key in facilities:
        info = IN_SOY_PERMITS[key]
        if not info.get('permit_url'):
            logger.warning(f"No URL for {key} — skip")
            continue
        raw_path = os.path.join(RAW_DIR, f"in_{key}_titlev.pdf")
        if os.path.exists(raw_path) and os.path.getsize(raw_path) > 10000:
            logger.info(f"✓ {key} already downloaded")
            continue

        try:
            resp = requests.get(info['permit_url'], headers={'User-Agent': 'Mozilla/5.0'}, timeout=30)
            if resp.status_code == 200 and b'%PDF' in resp.content[:10]:
                with open(raw_path, 'wb') as f:
                    f.write(resp.content)
                logger.info(f"✓ Downloaded {key}")
            else:
                logger.error(f"✗ Failed {key}: {resp.status_code}")
            time.sleep(1.5)
        except Exception as e:
            logger.error(f"✗ Error downloading {key}: {e}")

def parse_permits(facilities: List[str]):
    results = {}
    equipment_detail = []
    for key in facilities:
        raw_path = os.path.join(RAW_DIR, f"in_{key}_titlev.pdf")
        if not os.path.exists(raw_path):
            continue
        parser = TitleVCapacityParser(raw_path)
        parser.parse()
        info = IN_SOY_PERMITS[key]
        result = {
            'facility_name': info['name'],
            'permit_number': info['permit_number'],
            'facility_description': 'Soybean Oil Mills (SIC 2075)',
            'crush_capacity_tons_per_hour': parser.crush_capacity_hour,
            'crush_capacity_bushels_per_day': parser.crush_bushels_day,
            'crush_description': parser.crush_description,
            'has_refinery': parser.refinery_units > 0,
            'refinery_capacity_tons_per_hour': parser.refinery_capacity_hour,
            'refinery_capacity_tons_per_year': parser.refinery_capacity_year,
            'refinery_description': parser.refinery_description,
            'refining_type': parser.refining_type,
            'has_biodiesel': parser.biodiesel_units > 0,
            'biodiesel_capacity_mgy': parser.biodiesel_capacity_mgy,
            'total_emission_units_found': parser.total_units,
            'crush_units': parser.crush_units,
            'refinery_units': parser.refinery_units,
            'biodiesel_units': parser.biodiesel_units,
        }
        results[key] = result
        for eq in parser.equipment:
            equipment_detail.append({'Facility': key, **eq})

        logger.info(f"Parsed {key}: Crush {parser.crush_capacity_hour:.1f} t/h, Refinery {parser.refinery_capacity_hour:.1f} t/h")

    return results, equipment_detail

def output_results(results: Dict, equipment: List):
    date_str = datetime.now().strftime('%Y-%m-%d')
    json_path = os.path.join(OUTPUT_DIR, f'indiana_soy_capacity_{date_str}.json')
    with open(json_path, 'w') as f:
        json.dump(results, f, indent=2)

    excel_path = os.path.join(OUTPUT_DIR, f'indiana_soy_capacity_{date_str}.xlsx')
    with pd.ExcelWriter(excel_path) as writer:
        pd.DataFrame.from_dict(results, orient='index').to_excel(writer, sheet_name='Capacity Summary')
        pd.DataFrame(equipment).to_excel(writer, sheet_name='Equipment Detail', index=False)

    logger.info(f"✓ Output saved: {json_path} and {excel_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--download-only', action='store_true')
    parser.add_argument('--parse-only', action='store_true')
    parser.add_argument('--facility', type=str, help='e.g. adm_frankfort')
    args = parser.parse_args()

    facilities = [args.facility] if args.facility else list(IN_SOY_PERMITS.keys())

    if not args.parse_only:
        download_permits(facilities)
    if not args.download_only:
        results, equipment = parse_permits(facilities)
        output_results(results, equipment)

    print("\n=== SUMMARY ===")
    for key in results:
        r = results[key]
        print(f"{key}: {r['crush_capacity_tons_per_hour']:.1f} t/h crush | Refinery: {r['has_refinery']} | Biodiesel: {r['has_biodiesel']}")

if __name__ == '__main__':
    main()