"""
EPA Pathway Determination Letter Collector
==========================================
Downloads and parses EPA renewable fuel pathway determination letters.

Source: https://www.epa.gov/renewable-fuel-standard/approved-pathways-renewable-fuel

Usage:
    python epa_pathway_collector.py --scrape           # Scrape HTML index only
    python epa_pathway_collector.py --download         # Download all PDFs
    python epa_pathway_collector.py --parse            # Parse all downloaded PDFs
    python epa_pathway_collector.py --all              # Scrape + download + parse
    python epa_pathway_collector.py --parse-one FILE   # Parse a single PDF (debug)
"""

import os
import re
import sys
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

# =============================================================================
# Configuration
# =============================================================================

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'rlc_commodities',
    'user': 'postgres',
    'password': 'SoupBoss1',
}

EPA_URL = 'https://www.epa.gov/renewable-fuel-standard/approved-pathways-renewable-fuel'
PDF_DIR = Path(__file__).resolve().parent.parent.parent / 'data' / 'epa_pathways' / 'pdfs'
LOG_PATH = Path(__file__).resolve().parent / 'epa_pathway_collector.log'
DOWNLOAD_DELAY = 1.0  # seconds between PDF downloads

USER_AGENT = 'Mozilla/5.0 (RLC-Agent Data Collector)'

# US state abbreviation lookup
STATE_ABBREV = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN',
    'mississippi': 'MS', 'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE',
    'nevada': 'NV', 'new hampshire': 'NH', 'new jersey': 'NJ',
    'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC',
    'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK', 'oregon': 'OR',
    'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA',
    'west virginia': 'WV', 'wisconsin': 'WI', 'wyoming': 'WY',
    'district of columbia': 'DC',
}

# Reverse lookup: abbreviation -> full name
STATE_FULL = {v: k.title() for k, v in STATE_ABBREV.items()}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# =============================================================================
# Phase 1: Scrape HTML Index
# =============================================================================

def scrape_index():
    """Scrape the EPA approved pathways page and load index to database."""
    logger.info(f"Scraping {EPA_URL}")
    resp = requests.get(EPA_URL, timeout=60, headers={'User-Agent': USER_AGENT})
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, 'lxml')
    tables = soup.find_all('table')

    if len(tables) < 3:
        logger.error(f"Expected 3 tables, found {len(tables)}")
        sys.exit(1)

    # Table 0: Generally Applicable Pathways (Rows A-T)
    gap_rows = parse_generally_applicable(tables[0])
    logger.info(f"Generally applicable pathways: {len(gap_rows)} rows")

    # Table 1: Non-EP3 determinations
    non_ep3 = parse_determination_table(tables[1], 'non_ep3')
    logger.info(f"Non-EP3 determinations: {len(non_ep3)}")

    # Table 2: EP3 determinations
    ep3 = parse_determination_table(tables[2], 'ep3')
    logger.info(f"EP3 determinations: {len(ep3)}")

    all_determinations = non_ep3 + ep3

    # Save to database
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Upsert generally applicable pathways
    cursor.execute("DELETE FROM reference.epa_generally_applicable_pathways")
    for row in gap_rows:
        cursor.execute("""
            INSERT INTO reference.epa_generally_applicable_pathways
                (row_letter, fuel_type, feedstock, production_process, d_code)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['row_letter'], row['fuel_type'], row['feedstock'],
              row['production_process'], row['d_code']))
    logger.info(f"  Loaded {len(gap_rows)} generally applicable pathways")

    # Upsert determination index
    inserted, updated = 0, 0
    for det in all_determinations:
        cursor.execute("""
            INSERT INTO bronze.epa_pathway_index
                (determination_name, category, fuel_type, feedstock,
                 d_code, determination_date, pdf_url, pdf_filename, collected_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (pdf_url) DO UPDATE SET
                determination_name = EXCLUDED.determination_name,
                fuel_type = EXCLUDED.fuel_type,
                feedstock = EXCLUDED.feedstock,
                d_code = EXCLUDED.d_code,
                determination_date = EXCLUDED.determination_date,
                collected_at = NOW()
            RETURNING (xmax = 0) AS is_insert
        """, (det['name'], det['category'], det['fuel_type'], det['feedstock'],
              det['d_code'], det['date'], det['pdf_url'], det['pdf_filename']))
        result = cursor.fetchone()
        if result and result[0]:
            inserted += 1
        else:
            updated += 1

    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Index: {inserted} inserted, {updated} updated")
    print(f"\nScraped {len(all_determinations)} determination letters")
    print(f"  Non-EP3: {len(non_ep3)}")
    print(f"  EP3:     {len(ep3)}")
    print(f"  Inserted: {inserted}, Updated: {updated}")


def parse_generally_applicable(table):
    """Parse Table 0: Generally Applicable Pathways (Rows A-T)."""
    rows = []
    for tr in table.find_all('tr')[1:]:  # skip header
        cells = tr.find_all(['td', 'th'])
        if len(cells) < 5:
            continue
        text = [c.get_text(strip=True) for c in cells]
        if not text[0] or len(text[0]) > 2:
            continue
        rows.append({
            'row_letter': text[0][0],
            'fuel_type': text[1],
            'feedstock': text[2],
            'production_process': text[3],
            'd_code': text[4],
        })
    return rows


def parse_determination_table(table, category):
    """Parse a determination table (Non-EP3 or EP3)."""
    determinations = []
    for tr in table.find_all('tr')[1:]:  # skip header
        cells = tr.find_all('td')
        if len(cells) < 5:
            continue

        # First cell has the PDF link
        link = cells[0].find('a')
        if not link:
            continue

        href = link.get('href', '')
        if not href.endswith('.pdf'):
            continue

        # Make absolute URL
        if href.startswith('/'):
            href = 'https://www.epa.gov' + href

        name = link.get_text(strip=True)
        # Clean name: remove "(pdf)", "(11/2025)", etc.
        name = re.sub(r'\s*\(pdf\)', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*\(\d{1,2}/\d{4}\)', '', name)
        name = name.strip()

        fuel_type = cells[1].get_text(strip=True) if len(cells) > 1 else ''
        feedstock = cells[2].get_text(strip=True) if len(cells) > 2 else ''
        d_code = cells[3].get_text(strip=True) if len(cells) > 3 else ''
        date_str = cells[4].get_text(strip=True) if len(cells) > 4 else ''

        # Parse date
        det_date = None
        if date_str:
            for fmt in ('%m/%d/%Y', '%m/%d/%y', '%B %d, %Y', '%b %d, %Y'):
                try:
                    det_date = datetime.strptime(date_str, fmt).date()
                    break
                except ValueError:
                    continue

        # Extract filename from URL
        pdf_filename = href.split('/')[-1]

        determinations.append({
            'name': name,
            'category': category,
            'fuel_type': fuel_type,
            'feedstock': feedstock,
            'd_code': d_code,
            'date': det_date,
            'pdf_url': href,
            'pdf_filename': pdf_filename,
        })

    return determinations


# =============================================================================
# Phase 2: Download PDFs
# =============================================================================

def download_pdfs():
    """Download all PDFs that haven't been downloaded yet."""
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute("""
        SELECT id, pdf_url, pdf_filename
        FROM bronze.epa_pathway_index
        WHERE pdf_downloaded = FALSE
        ORDER BY id
    """)
    pending = cursor.fetchall()

    if not pending:
        logger.info("All PDFs already downloaded")
        print("All PDFs already downloaded.")
        cursor.close()
        conn.close()
        return

    logger.info(f"Downloading {len(pending)} PDFs to {PDF_DIR}")
    downloaded, errors = 0, 0

    for row in pending:
        idx_id = row['id']
        url = row['pdf_url']
        filename = row['pdf_filename']
        dest = PDF_DIR / filename

        try:
            if dest.exists():
                # Already on disk, just mark as downloaded
                cursor.execute(
                    "UPDATE bronze.epa_pathway_index SET pdf_downloaded = TRUE WHERE id = %s",
                    (idx_id,))
                conn.commit()
                downloaded += 1
                continue

            resp = requests.get(url, timeout=60, headers={'User-Agent': USER_AGENT})
            resp.raise_for_status()

            with open(dest, 'wb') as f:
                f.write(resp.content)

            cursor.execute(
                "UPDATE bronze.epa_pathway_index SET pdf_downloaded = TRUE WHERE id = %s",
                (idx_id,))
            conn.commit()

            downloaded += 1
            logger.info(f"  [{downloaded}/{len(pending)}] {filename} ({len(resp.content):,} bytes)")

            time.sleep(DOWNLOAD_DELAY)

        except Exception as e:
            errors += 1
            logger.error(f"  Error downloading {filename}: {e}")

    cursor.close()
    conn.close()

    logger.info(f"Download complete: {downloaded} downloaded, {errors} errors")
    print(f"\nDownloaded {downloaded} PDFs ({errors} errors)")


# =============================================================================
# Phase 3: Parse PDFs
# =============================================================================

def parse_pdf(filepath):
    """Extract structured data from a single EPA determination letter PDF.

    Returns a dict with all extracted fields, or None on failure.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        logger.error(f"PDF not found: {filepath}")
        return None

    try:
        reader = PdfReader(filepath)
    except Exception as e:
        logger.error(f"Cannot read PDF {filepath.name}: {e}")
        return None

    # Extract all text
    pages_text = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ''
            pages_text.append(text)
        except Exception:
            pages_text.append('')

    full_text = '\n\n'.join(pages_text)
    if not full_text.strip():
        logger.warning(f"No text extracted from {filepath.name}")
        return {'full_text': '', 'page_count': len(reader.pages),
                'parse_confidence': 'low', 'parse_notes': 'No text extracted'}

    result = {
        'full_text': full_text,
        'page_count': len(reader.pages),
        'parse_confidence': 'high',
        'parse_notes': '',
    }

    notes = []

    # --- Parse Page 1 header block ---
    page1 = pages_text[0] if pages_text else ''
    lines = [l.strip() for l in page1.split('\n') if l.strip()]

    # Date: first non-empty line that looks like a date
    result['determination_date'] = None
    for line in lines[:5]:
        m = re.match(r'^((?:January|February|March|April|May|June|July|August|'
                     r'September|October|November|December)\s+\d{1,2},?\s+\d{4})', line)
        if m:
            try:
                result['determination_date'] = datetime.strptime(
                    m.group(1).replace(',', ''), '%B %d %Y').date()
            except ValueError:
                pass
            break

    # Recipient block: lines between date and "Dear"
    dear_idx = None
    for i, line in enumerate(lines):
        if line.lower().startswith('dear '):
            dear_idx = i
            break

    if dear_idx and dear_idx > 1:
        recipient_lines = lines[1:dear_idx]
        _parse_recipient(result, recipient_lines)
    else:
        notes.append('Could not locate recipient block')

    # --- Parse body text for key fields ---

    # D-code
    d_match = re.search(r'D\s*code\s*(\d)', full_text)
    result['d_code'] = int(d_match.group(1)) if d_match else None

    # Fuel types
    result['fuel_types'] = _extract_fuel_types(full_text)

    # Feedstocks
    result['feedstocks'] = _extract_feedstocks(full_text)

    # Facility location
    _extract_facility_location(result, full_text)

    # Production process
    result['production_process'] = _extract_production_process(full_text)

    # Process energy sources
    result['process_energy_sources'] = _extract_energy_sources(full_text)

    # Pathway name (e.g., "Scott Petroleum Greenville Process")
    result['pathway_name'] = _extract_pathway_name(full_text)

    # --- GHG Results (deeper pages) ---
    _extract_ghg_results(result, full_text)

    # Table 1 row reference
    row_match = re.search(r'Row\s+([A-T])\s+of\s+Table\s+1', full_text)
    result['table1_row_reference'] = row_match.group(1) if row_match else None

    # Assess parse confidence
    key_fields = ['company_name', 'facility_city', 'facility_state', 'd_code',
                  'production_process']
    filled = sum(1 for f in key_fields if result.get(f))
    if filled >= 4:
        result['parse_confidence'] = 'high'
    elif filled >= 2:
        result['parse_confidence'] = 'medium'
    else:
        result['parse_confidence'] = 'low'
        notes.append(f'Only {filled}/{len(key_fields)} key fields extracted')

    result['parse_notes'] = '; '.join(notes) if notes else None
    return result


def _parse_recipient(result, lines):
    """Parse recipient name, title, company, and address from header lines."""
    result['recipient_name'] = None
    result['recipient_title'] = None
    result['company_name'] = None
    result['mailing_address'] = None

    if not lines:
        return

    # First line is usually the name (Mr./Ms./Dr. ...)
    name_line = lines[0]
    name_match = re.match(r'^(Mr\.|Ms\.|Mrs\.|Dr\.)\s+(.+)', name_line)
    if name_match:
        result['recipient_name'] = name_line
    else:
        result['recipient_name'] = name_line

    # Look for company name (usually contains Inc., LLC, Corp., LP, etc.)
    company_patterns = [
        r'(?:Inc\.|LLC|Corp\.|Corporation|L\.?P\.?|Company|Co\.|Ltd\.)',
        r'(?:Biorefining|Renewables|Energy|Ethanol|Biofuels|Petroleum)',
    ]
    for line in lines[1:]:
        for pat in company_patterns:
            if re.search(pat, line, re.IGNORECASE):
                result['company_name'] = line.strip()
                break
        if result['company_name']:
            break

    # If no company found via patterns, try the line after the name
    if not result['company_name'] and len(lines) > 2:
        # Title is usually line 2, company is line 3
        if len(lines) > 3:
            result['recipient_title'] = lines[1]
            result['company_name'] = lines[2]
        else:
            result['company_name'] = lines[1]

    # Title: line between name and company that looks like a title
    if not result['recipient_title']:
        title_keywords = ['president', 'manager', 'director', 'officer', 'vp',
                          'vice president', 'ceo', 'coo', 'general counsel',
                          'chief', 'head', 'senior']
        for line in lines[1:]:
            if any(kw in line.lower() for kw in title_keywords):
                result['recipient_title'] = line.strip()
                break

    # Address: remaining lines (street, city/state/zip)
    addr_lines = []
    for line in lines:
        if line == result.get('recipient_name') or line == result.get('recipient_title') \
                or line == result.get('company_name'):
            continue
        # Looks like an address line (number + street, or city + state + zip)
        if re.search(r'\d', line):
            addr_lines.append(line)
    result['mailing_address'] = '\n'.join(addr_lines) if addr_lines else None


def _extract_fuel_types(text):
    """Extract fuel types from the determination letter text.

    Focuses on the first paragraph (the "Dear" paragraph) which explicitly
    states what fuels were approved, to avoid picking up context mentions
    of other fuels from prior rulemakings.
    """
    # Extract the "Dear" paragraph — the first ~1500 chars of the body
    # which states "generation of [fuel type] (D code N) RINs for [fuels]"
    dear_section = text[:2500]

    fuel_keywords = [
        'biodiesel', 'renewable diesel', 'heating oil', 'ethanol',
        'renewable gasoline', 'gasoline blendstock', 'jet fuel',
        'renewable jet fuel', 'sustainable aviation fuel',
        'naphtha', 'butanol', 'dimethyl ether',
        'compressed natural gas', 'liquefied natural gas',
        'renewable electricity', 'cellulosic ethanol',
        'cellulosic diesel',
    ]

    # First try: extract from "RINs for [fuel list]" pattern
    # Note: text often has "RINs)" or "RINs )" before "for"
    m = re.search(r'RINs?\)?\s+for\s+(.{10,300}?)(?:produced|at\s+(?:their|your|the))',
                  dear_section, re.IGNORECASE | re.DOTALL)
    if m:
        fuel_snippet = m.group(1).lower()
    else:
        # Fallback: extract from "generation of [fuels] (D code" pattern
        m = re.search(r'generation of\s+(.{10,200}?)\s*\(?D\s*code',
                      dear_section, re.IGNORECASE | re.DOTALL)
        if m:
            fuel_snippet = m.group(1).lower()
        else:
            fuel_snippet = dear_section[:1200].lower()

    found = set()
    for fuel in fuel_keywords:
        if fuel in fuel_snippet:
            found.add(fuel.title())

    # Normalize
    normalized = set()
    for f in found:
        if f == 'Jet Fuel' or f == 'Renewable Jet Fuel':
            normalized.add('Renewable Jet Fuel')
        elif f == 'Gasoline Blendstock' or f == 'Renewable Gasoline':
            normalized.add('Renewable Gasoline')
        elif f == 'Compressed Natural Gas' or f == 'Liquefied Natural Gas':
            normalized.add('Renewable CNG/LNG')
        else:
            normalized.add(f)

    return sorted(normalized) if normalized else None


def _extract_feedstocks(text):
    """Extract feedstock names from the determination letter.

    Focuses on the first paragraph and "from X feedstock" patterns
    to avoid picking up context mentions of other feedstocks.
    """
    # Focus on the "Dear" paragraph for the actual approved feedstock
    dear_section = text[:2500]

    feedstock_keywords = [
        'soybean oil', 'corn starch', 'cottonseed oil', 'canola oil',
        'corn oil', 'distillers corn oil', 'palm oil', 'camelina oil',
        'tallow', 'yellow grease', 'used cooking oil', 'white grease',
        'poultry fat', 'waste oils', 'animal fat',
        'biogas', 'renewable natural gas', 'landfill gas',
        'corn stover', 'switchgrass', 'wood waste', 'cellulosic biomass',
        'sugarcane', 'grain sorghum', 'municipal solid waste',
        'food waste', 'yard waste', 'algae',
    ]

    found = set()

    # Primary: "from X feedstock" or "using X as feedstock" in first paragraph
    for pat in [r'from\s+(.{5,80}?)\s+feedstock',
                r'using\s+(.{5,80}?)\s+as\s+feedstock',
                r'produced\s+from\s+(.{5,80}?)\s+(?:through|at|via)']:
        m = re.search(pat, dear_section, re.IGNORECASE)
        if m:
            fs_name = re.sub(r'\s+', ' ', m.group(1).strip())
            # Strip trailing "feedstock" or "oil feedstock" if captured
            fs_name = re.sub(r'\s+feedstock$', '', fs_name, flags=re.IGNORECASE)
            if len(fs_name) < 80 and len(fs_name) > 2:
                found.add(fs_name.title())

    # Secondary: keyword search only if no regex match found
    if not found:
        dear_lower = dear_section[:1200].lower()
        for fs in feedstock_keywords:
            if fs in dear_lower:
                found.add(fs.title())

    return sorted(found) if found else None


def _extract_facility_location(result, text):
    """Extract facility city and state from the letter text."""
    result['facility_city'] = None
    result['facility_state'] = None
    result['facility_state_full'] = None
    result['facility_name'] = None

    # Pattern: "facility in/located in City, State"
    patterns = [
        r'facility\s+(?:located\s+)?in\s+([A-Z][a-zA-Z\s.]+?),\s*([A-Z][a-zA-Z\s]+?)[\s.,(\n]',
        r'facility\s+in\s+([A-Z][a-zA-Z\s.]+?),\s*([A-Z]{2})\b',
        r'production facility located in\s+([A-Z][a-zA-Z\s.]+?),\s*([A-Z][a-zA-Z\s]+?)[\s.,(\n]',
    ]

    for pat in patterns:
        m = re.search(pat, text[:5000])
        if m:
            city = m.group(1).strip().rstrip(',.')
            state = m.group(2).strip().rstrip(',.')

            result['facility_city'] = city

            # Check if state is abbreviation or full name
            if len(state) == 2 and state.upper() in STATE_FULL:
                result['facility_state'] = state.upper()
                result['facility_state_full'] = STATE_FULL[state.upper()]
            elif state.lower() in STATE_ABBREV:
                result['facility_state'] = STATE_ABBREV[state.lower()]
                result['facility_state_full'] = state.title()
            else:
                result['facility_state_full'] = state
                # Try partial match
                for full, abbr in STATE_ABBREV.items():
                    if state.lower().startswith(full[:4]):
                        result['facility_state'] = abbr
                        break
            break

    # Also try to extract from mailing address if not found
    if not result['facility_state'] and result.get('mailing_address'):
        addr = result['mailing_address']
        state_match = re.search(r',\s*([A-Z]{2})\s+\d{5}', addr)
        if state_match:
            abbr = state_match.group(1)
            if abbr in STATE_FULL:
                result['facility_state'] = abbr
                result['facility_state_full'] = STATE_FULL[abbr]
            # City from address
            city_match = re.search(r'\n([A-Za-z\s.]+),\s*[A-Z]{2}\s+\d{5}', addr)
            if city_match and not result['facility_city']:
                result['facility_city'] = city_match.group(1).strip()


def _extract_production_process(text):
    """Extract the production process description."""
    process_keywords = {
        'transesterification': 'Transesterification',
        'dry mill': 'Dry Mill',
        'wet mill': 'Wet Mill',
        'hydrotreating': 'Hydrotreating',
        'hydroprocessing': 'Hydroprocessing',
        'fluid catalytic cracking': 'Fluid Catalytic Cracking (FCC)',
        'co-processing': 'Co-processing',
        'co processing': 'Co-processing',
        'fermentation': 'Fermentation',
        'fischer-tropsch': 'Fischer-Tropsch',
        'gasification': 'Gasification',
        'pyrolysis': 'Pyrolysis',
        'anaerobic digestion': 'Anaerobic Digestion',
        'alcohol-to-jet': 'Alcohol-to-Jet',
        'hefa': 'HEFA (Hydroprocessed Esters and Fatty Acids)',
    }

    text_lower = text[:8000].lower()
    for keyword, label in process_keywords.items():
        if keyword in text_lower:
            return label

    # Try regex pattern
    m = re.search(r'through\s+(?:a\s+|the\s+)?(.{10,80}?)\s+process', text[:5000],
                  re.IGNORECASE)
    if m:
        process = m.group(1).strip()
        if len(process) < 80:
            return process

    return None


def _extract_energy_sources(text):
    """Extract process energy sources."""
    energy_keywords = {
        'natural gas': 'Natural Gas',
        'grid electricity': 'Grid Electricity',
        'electricity': 'Electricity',
        'biomass': 'Biomass',
        'biogas': 'Biogas',
        'coal': 'Coal',
        'landfill gas': 'Landfill Gas',
        'solar': 'Solar',
        'wind': 'Wind',
    }

    # Look for "using X for process energy" or "X and Y for process energy"
    energy_section = re.search(
        r'(?:using|uses?)\s+(.{10,200}?)\s+for\s+process\s+energy',
        text[:10000], re.IGNORECASE)

    found = set()
    if energy_section:
        snippet = energy_section.group(1).lower()
        for keyword, label in energy_keywords.items():
            if keyword in snippet:
                found.add(label)
    else:
        # Fallback: search broader context
        text_lower = text[:10000].lower()
        if 'natural gas' in text_lower and 'process energy' in text_lower:
            found.add('Natural Gas')
        if ('electricity' in text_lower or 'grid electricity' in text_lower) \
                and 'process energy' in text_lower:
            found.add('Electricity')

    # Deduplicate: if we have Grid Electricity and Electricity, keep Grid Electricity
    if 'Grid Electricity' in found and 'Electricity' in found:
        found.discard('Electricity')

    return sorted(found) if found else None


def _extract_pathway_name(text):
    """Extract the named pathway (e.g., 'Scott Petroleum Greenville Process')."""
    # Look for quoted pathway names
    m = re.search(r'["\u201c]([^"\u201d]{10,100}?Process)["\u201d]', text[:5000])
    if m:
        return m.group(1).strip()

    # Try "the X Pathways" or "the X Process"
    m = re.search(r'the\s+\u201c([^"\u201d]{10,100}?(?:Process|Pathways?))\u201d',
                  text[:5000])
    if m:
        return m.group(1).strip()

    return None


def _extract_ghg_results(result, text):
    """Extract GHG reduction percentage and lifecycle emissions.

    Searches from later pages (results/conclusion section) to avoid
    matching threshold requirements like "20 percent reduction requirement".
    """
    result['ghg_reduction_pct'] = None
    result['lifecycle_ghg_gco2e_mj'] = None
    result['ghg_baseline_gco2e_mj'] = None

    # GHG reduction percentage — search ALL matches and pick the most likely
    # The actual result is usually near "Percent Reduction Relative to" table
    # or "exceeds the CAA X% GHG reduction threshold"
    candidates = []

    # Best pattern: table row "Percent Reduction Relative to ... X%"
    for m in re.finditer(r'Percent Reduction.*?(\d{2,3})\s*%', text,
                         re.IGNORECASE):
        candidates.append(('table', float(m.group(1)), m.start()))

    # Good pattern: "by X percent" near "baseline" or "compared to"
    for m in re.finditer(
            r'(?:compared to|relative to).*?(?:by|of)\s+(\d{2,3})\s*(?:percent|%)',
            text, re.IGNORECASE | re.DOTALL):
        pct = float(m.group(1))
        if 15 <= pct <= 99:
            candidates.append(('compared', pct, m.start()))

    # Good pattern: "exceeds the CAA X% GHG reduction threshold"
    for m in re.finditer(
            r'exceeds?\s+the\s+CAA\s+(\d{2,3})\s*%?\s+(?:percent\s+)?GHG\s+reduction',
            text, re.IGNORECASE):
        candidates.append(('exceeds', float(m.group(1)), m.start()))

    # Pattern: "reduces lifecycle ... by X percent" (not "requirement")
    for m in re.finditer(
            r'reduces?\s+lifecycle.*?by\s+(\d{2,3})\s+percent',
            text, re.IGNORECASE | re.DOTALL):
        # Check it's not "reduction requirement"
        context = text[max(0, m.start()-50):m.end()+50]
        if 'requirement' not in context.lower():
            candidates.append(('reduces', float(m.group(1)), m.start()))

    if candidates:
        # Prefer 'table' matches, then later-occurring matches
        candidates.sort(key=lambda c: (
            0 if c[0] == 'table' else 1,
            -c[2]  # prefer later occurrences
        ))
        result['ghg_reduction_pct'] = candidates[0][1]

    # Lifecycle GHG emissions (gCO2e/MJ) — from results table
    m = re.search(r'Total Lifecycle GHG Emissions\s+(\d{1,3})', text)
    if m:
        val = float(m.group(1))
        if 5 <= val <= 150:
            result['lifecycle_ghg_gco2e_mj'] = val

    if not result['lifecycle_ghg_gco2e_mj']:
        m = re.search(r'(?:Total\s+)?[Ll]ifecycle\s+GHG\s+[Ee]missions\s+(\d{1,3})',
                      text)
        if m:
            val = float(m.group(1))
            if 5 <= val <= 150:
                result['lifecycle_ghg_gco2e_mj'] = val

    # Baseline
    m = re.search(r'(?:petroleum|diesel|gasoline)\s+baseline\s+of\s+'
                  r'(\d{2,3}\.?\d?)\s+gCO2e', text, re.IGNORECASE)
    if m:
        result['ghg_baseline_gco2e_mj'] = float(m.group(1))


# =============================================================================
# Phase 3b: Parse all PDFs
# =============================================================================

def parse_all_pdfs():
    """Parse all downloaded but unparsed PDFs."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute("""
        SELECT id, pdf_filename
        FROM bronze.epa_pathway_index
        WHERE pdf_downloaded = TRUE AND pdf_parsed = FALSE
        ORDER BY id
    """)
    pending = cursor.fetchall()

    if not pending:
        logger.info("All downloaded PDFs already parsed")
        print("All downloaded PDFs already parsed.")
        cursor.close()
        conn.close()
        return

    logger.info(f"Parsing {len(pending)} PDFs")
    parsed, errors = 0, 0
    confidence_counts = {'high': 0, 'medium': 0, 'low': 0}

    for row in pending:
        idx_id = row['id']
        filename = row['pdf_filename']
        filepath = PDF_DIR / filename

        if not filepath.exists():
            logger.warning(f"  PDF not found on disk: {filename}")
            errors += 1
            continue

        result = parse_pdf(filepath)
        if result is None:
            errors += 1
            cursor.execute(
                "UPDATE bronze.epa_pathway_index SET pdf_parsed = TRUE WHERE id = %s",
                (idx_id,))
            conn.commit()
            continue

        # Save to bronze.epa_pathway_detail
        try:
            cursor.execute("""
                INSERT INTO bronze.epa_pathway_detail
                    (pathway_index_id, recipient_name, recipient_title, company_name,
                     mailing_address, facility_name, facility_city, facility_state,
                     facility_state_full, fuel_types, feedstocks, d_code,
                     production_process, process_energy_sources,
                     ghg_reduction_pct, lifecycle_ghg_gco2e_mj, ghg_baseline_gco2e_mj,
                     table1_row_reference, pathway_name,
                     full_text, page_count, parsed_at, parse_confidence, parse_notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
                ON CONFLICT (pathway_index_id) DO UPDATE SET
                    recipient_name = EXCLUDED.recipient_name,
                    recipient_title = EXCLUDED.recipient_title,
                    company_name = EXCLUDED.company_name,
                    mailing_address = EXCLUDED.mailing_address,
                    facility_name = EXCLUDED.facility_name,
                    facility_city = EXCLUDED.facility_city,
                    facility_state = EXCLUDED.facility_state,
                    facility_state_full = EXCLUDED.facility_state_full,
                    fuel_types = EXCLUDED.fuel_types,
                    feedstocks = EXCLUDED.feedstocks,
                    d_code = EXCLUDED.d_code,
                    production_process = EXCLUDED.production_process,
                    process_energy_sources = EXCLUDED.process_energy_sources,
                    ghg_reduction_pct = EXCLUDED.ghg_reduction_pct,
                    lifecycle_ghg_gco2e_mj = EXCLUDED.lifecycle_ghg_gco2e_mj,
                    ghg_baseline_gco2e_mj = EXCLUDED.ghg_baseline_gco2e_mj,
                    table1_row_reference = EXCLUDED.table1_row_reference,
                    pathway_name = EXCLUDED.pathway_name,
                    full_text = EXCLUDED.full_text,
                    page_count = EXCLUDED.page_count,
                    parsed_at = NOW(),
                    parse_confidence = EXCLUDED.parse_confidence,
                    parse_notes = EXCLUDED.parse_notes
            """, (
                idx_id,
                result.get('recipient_name'),
                result.get('recipient_title'),
                result.get('company_name'),
                result.get('mailing_address'),
                result.get('facility_name'),
                result.get('facility_city'),
                result.get('facility_state'),
                result.get('facility_state_full'),
                result.get('fuel_types'),
                result.get('feedstocks'),
                result.get('d_code'),
                result.get('production_process'),
                result.get('process_energy_sources'),
                result.get('ghg_reduction_pct'),
                result.get('lifecycle_ghg_gco2e_mj'),
                result.get('ghg_baseline_gco2e_mj'),
                result.get('table1_row_reference'),
                result.get('pathway_name'),
                result.get('full_text'),
                result.get('page_count'),
                result.get('parse_confidence', 'low'),
                result.get('parse_notes'),
            ))

            cursor.execute(
                "UPDATE bronze.epa_pathway_index SET pdf_parsed = TRUE WHERE id = %s",
                (idx_id,))
            conn.commit()

            conf = result.get('parse_confidence', 'low')
            confidence_counts[conf] = confidence_counts.get(conf, 0) + 1
            parsed += 1
            logger.info(f"  [{parsed}/{len(pending)}] {filename} -> {conf}")

        except Exception as e:
            logger.error(f"  Error saving {filename}: {e}")
            conn.rollback()
            errors += 1

    cursor.close()
    conn.close()

    logger.info(f"Parse complete: {parsed} parsed, {errors} errors")
    print(f"\nParsed {parsed} PDFs ({errors} errors)")
    print(f"  Confidence: {confidence_counts}")


def parse_one_pdf(filepath):
    """Parse a single PDF and print results (for debugging)."""
    result = parse_pdf(filepath)
    if result is None:
        print("Failed to parse PDF")
        return

    print(f"\n{'='*60}")
    print(f"File: {filepath}")
    print(f"Pages: {result.get('page_count')}")
    print(f"Confidence: {result.get('parse_confidence')}")
    if result.get('parse_notes'):
        print(f"Notes: {result['parse_notes']}")
    print(f"{'='*60}")

    fields = [
        ('Company', 'company_name'),
        ('Recipient', 'recipient_name'),
        ('Title', 'recipient_title'),
        ('Address', 'mailing_address'),
        ('Facility City', 'facility_city'),
        ('Facility State', 'facility_state'),
        ('D-Code', 'd_code'),
        ('Fuel Types', 'fuel_types'),
        ('Feedstocks', 'feedstocks'),
        ('Process', 'production_process'),
        ('Energy Sources', 'process_energy_sources'),
        ('GHG Reduction', 'ghg_reduction_pct'),
        ('Lifecycle GHG', 'lifecycle_ghg_gco2e_mj'),
        ('GHG Baseline', 'ghg_baseline_gco2e_mj'),
        ('Pathway Name', 'pathway_name'),
        ('Table 1 Row', 'table1_row_reference'),
    ]

    for label, key in fields:
        val = result.get(key)
        if val is not None:
            if key == 'ghg_reduction_pct':
                print(f"  {label}: {val}%")
            elif key in ('lifecycle_ghg_gco2e_mj', 'ghg_baseline_gco2e_mj'):
                print(f"  {label}: {val} gCO2e/MJ")
            elif key == 'mailing_address':
                print(f"  {label}: {val.replace(chr(10), ', ')}")
            else:
                print(f"  {label}: {val}")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='EPA Pathway Determination Letter Collector')
    parser.add_argument('--scrape', action='store_true',
                        help='Scrape HTML index from EPA website')
    parser.add_argument('--download', action='store_true',
                        help='Download all PDFs')
    parser.add_argument('--parse', action='store_true',
                        help='Parse all downloaded PDFs')
    parser.add_argument('--all', action='store_true',
                        help='Scrape + download + parse')
    parser.add_argument('--parse-one', metavar='FILE',
                        help='Parse a single PDF (debug mode)')
    args = parser.parse_args()

    if not any([args.scrape, args.download, args.parse, args.all, args.parse_one]):
        parser.print_help()
        sys.exit(1)

    if args.parse_one:
        parse_one_pdf(args.parse_one)
        return

    if args.all or args.scrape:
        scrape_index()

    if args.all or args.download:
        download_pdfs()

    if args.all or args.parse:
        parse_all_pdfs()


if __name__ == '__main__':
    main()
