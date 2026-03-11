"""
AMS MARS API Explorer

Probes all AMS report slug IDs needed for HB Weekly Report to determine
which return structured price data vs. narrative-only vs. empty responses.

Usage:
    python scripts/explore_ams_api.py
    python scripts/explore_ams_api.py --slug 3192    # Probe a single report
    python scripts/explore_ams_api.py --verbose       # Show sample records
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MARS_BASE_URL = 'https://marsapi.ams.usda.gov/services/v1.2'

# All slug IDs referenced in HB cash price spreadsheet Sheet2
HB_REPORT_CATALOG = {
    '3192': {'name': 'IL Grain - Processor/Elevator', 'category': 'grain', 'hb_items': ['Corn Processor IL', 'Soybeans Processor IL']},
    '3225': {'name': 'NE Grain - Country Elevators', 'category': 'grain', 'hb_items': ['Corn Omaha NE']},
    '2932': {'name': 'National Grain/Oilseed - Terminals', 'category': 'grain', 'hb_items': ['Soybeans StL', 'Wheat HRW KC', 'Wheat SRW StL', 'Sorghum KC']},
    '3046': {'name': 'MN Grain - Minneapolis', 'category': 'grain', 'hb_items': ['Wheat DNS Mpls', 'Oats Mpls']},
    '3148': {'name': 'PNW/MT Grain', 'category': 'grain', 'hb_items': ['Wheat SWW Portland', 'Wheat Durum MT']},
    '2771': {'name': 'MT/ND Grain - Durum/Barley', 'category': 'grain', 'hb_items': ['Wheat Durum MT', 'Barley MT']},
    '3511': {'name': 'Soybean Meal Prices', 'category': 'grain', 'hb_items': ['Soybean Meal Decatur']},
    '2887': {'name': 'ND Sunflower', 'category': 'specialty_grain', 'hb_items': ['Sunflower Fargo ND']},
    '3616': {'name': 'Ethanol & DDGs Report', 'category': 'ethanol_ddgs', 'hb_items': ['DDGs NE IA', 'DDGs NW IA', 'Ethanol IA']},
    '2675': {'name': 'National Hog Report', 'category': 'livestock_hogs', 'hb_items': ['Hogs National Base']},
    '2810': {'name': 'National Feeder Pig Report', 'category': 'livestock_pigs', 'hb_items': ['Feeder Pigs 40lb']},
    '2485': {'name': 'NE Cattle - Choice Steers', 'category': 'livestock_cattle', 'hb_items': ['Choice Steers NE']},
    '1281': {'name': 'OKC Feeder Cattle Auction', 'category': 'livestock_feeders', 'hb_items': ['Feeder 750-800', 'Feeder 500-550', 'Feeder 450-500']},
    '3195': {'name': 'Farm Inputs & Fuel', 'category': 'farm_inputs', 'hb_items': ['Diesel Midwest', 'DAP Tampa', 'Urea NOLA', 'UAN NOLA']},
    '3024': {'name': 'Cotton Daily Spot', 'category': 'cotton', 'hb_items': ['Cotton ET-ST 41-4-34']},
    '2850': {'name': 'Daily Grain/Soybean Review', 'category': 'grain', 'hb_items': []},
    '2886': {'name': 'Sunflower Seed', 'category': 'specialty_grain', 'hb_items': []},
}


def probe_report(slug_id: str, api_key: str, verbose: bool = False) -> Dict:
    """Probe a single AMS report and return its structure summary."""
    auth = HTTPBasicAuth(api_key, '')

    # Query last 10 days to ensure we capture weekly reports
    end_date = datetime.now()
    start_date = end_date - timedelta(days=10)
    date_range = f"{start_date.strftime('%m/%d/%Y')}:{end_date.strftime('%m/%d/%Y')}"

    url = f"{MARS_BASE_URL}/reports/{slug_id}"
    params = {'q': f'report_date={date_range}'}

    result = {
        'slug_id': slug_id,
        'info': HB_REPORT_CATALOG.get(slug_id, {}),
        'status': None,
        'response_type': None,
        'sections': [],
        'total_records': 0,
        'has_structured_prices': False,
        'has_narrative': False,
        'sample_fields': [],
        'price_fields_found': [],
        'error': None,
    }

    try:
        resp = requests.get(url, params=params, auth=auth, timeout=30)
        result['status'] = resp.status_code

        if resp.status_code != 200:
            result['error'] = f"HTTP {resp.status_code}"
            return result

        data = resp.json()
        result['response_type'] = type(data).__name__

        if isinstance(data, list):
            # List of sections — standard MARS response
            for section in data:
                if not isinstance(section, dict):
                    continue
                sec_name = section.get('reportSection', 'Unknown')
                results = section.get('results', [])
                stats = section.get('stats', {})
                returned = stats.get('returnedRows', len(results))

                sec_info = {
                    'name': sec_name,
                    'record_count': returned,
                    'fields': [],
                    'price_fields': [],
                    'has_narrative': False,
                }

                if results:
                    sample = results[0]
                    sec_info['fields'] = sorted(sample.keys())

                    # Check for price fields
                    price_indicators = [
                        'price', 'low', 'high', 'avg', 'wtd_avg', 'weighted_avg',
                        'basis', 'price_low', 'price_high', 'price_avg', 'price_mostly',
                    ]
                    for f in sample.keys():
                        fl = f.lower()
                        if any(p in fl for p in price_indicators):
                            val = sample[f]
                            sec_info['price_fields'].append(f"{f}={val}")

                    if sec_info['price_fields']:
                        result['has_structured_prices'] = True
                        result['price_fields_found'].extend(sec_info['price_fields'])

                    # Check for narrative text
                    for f in ['report_narrative', 'narrative', 'comments', 'report_text']:
                        if f in sample and sample[f]:
                            sec_info['has_narrative'] = True
                            result['has_narrative'] = True

                    if verbose and results:
                        sec_info['sample_record'] = results[0]

                result['total_records'] += returned
                result['sections'].append(sec_info)

                if results:
                    result['sample_fields'] = sec_info['fields']

        elif isinstance(data, dict):
            # Single dict response
            results = data.get('results', [])
            result['total_records'] = len(results)
            if results:
                result['sample_fields'] = sorted(results[0].keys())
                result['has_structured_prices'] = any(
                    any(p in f.lower() for p in ['price', 'low', 'high', 'avg'])
                    for f in results[0].keys()
                )

    except requests.exceptions.Timeout:
        result['error'] = 'Timeout'
    except Exception as e:
        result['error'] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(description='Explore AMS MARS API reports')
    parser.add_argument('--slug', help='Probe a single slug ID')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show sample records')
    args = parser.parse_args()

    api_key = os.getenv('USDA_AMS_API_KEY')
    if not api_key:
        print("ERROR: Set USDA_AMS_API_KEY in .env file")
        sys.exit(1)

    slugs = [args.slug] if args.slug else sorted(HB_REPORT_CATALOG.keys())

    print(f"\n{'='*90}")
    print(f"AMS MARS API Explorer — Probing {len(slugs)} reports")
    print(f"{'='*90}\n")

    results = []
    for slug_id in slugs:
        info = HB_REPORT_CATALOG.get(slug_id, {})
        name = info.get('name', 'Unknown')
        print(f"  Probing {slug_id} ({name})...", end=' ', flush=True)

        result = probe_report(slug_id, api_key, args.verbose)
        results.append(result)

        if result['error']:
            print(f"ERROR: {result['error']}")
        elif result['has_structured_prices']:
            print(f"OK — {result['total_records']} records, STRUCTURED PRICES")
        elif result['has_narrative']:
            print(f"OK — {result['total_records']} records, NARRATIVE ONLY")
        else:
            print(f"OK — {result['total_records']} records, NO PRICES FOUND")

    # Summary table
    print(f"\n{'='*90}")
    print(f"{'Slug':>6}  {'Category':<18} {'Records':>8}  {'Structured':>10}  {'Narrative':>10}  {'Sections':>8}")
    print(f"{'-'*90}")

    structured = []
    narrative_only = []
    no_data = []

    for r in results:
        info = r['info']
        cat = info.get('category', '?')
        sections_str = ','.join(s['name'][:15] for s in r['sections'][:3])

        print(f"{r['slug_id']:>6}  {cat:<18} {r['total_records']:>8}  "
              f"{'YES' if r['has_structured_prices'] else 'no':>10}  "
              f"{'YES' if r['has_narrative'] else 'no':>10}  "
              f"{len(r['sections']):>8}")

        if r['has_structured_prices']:
            structured.append(r['slug_id'])
        elif r['has_narrative']:
            narrative_only.append(r['slug_id'])
        else:
            no_data.append(r['slug_id'])

    print(f"\n{'='*90}")
    print(f"Structured data:  {len(structured)} reports — {', '.join(structured)}")
    print(f"Narrative only:   {len(narrative_only)} reports — {', '.join(narrative_only)}")
    print(f"No useful data:   {len(no_data)} reports — {', '.join(no_data)}")

    if args.verbose:
        print(f"\n{'='*90}")
        print("DETAILED SECTION INFO")
        print(f"{'='*90}")
        for r in results:
            print(f"\n--- {r['slug_id']} ({r['info'].get('name', '?')}) ---")
            for sec in r['sections']:
                print(f"  Section: {sec['name']} ({sec['record_count']} records)")
                if sec['price_fields']:
                    print(f"    Price fields: {', '.join(sec['price_fields'][:8])}")
                if sec.get('fields'):
                    print(f"    All fields: {', '.join(sec['fields'][:15])}")
                if sec.get('sample_record'):
                    print(f"    Sample: {json.dumps(sec['sample_record'], indent=2)[:500]}")


if __name__ == '__main__':
    main()
