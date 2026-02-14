# Prompt: Build Nebraska NDEE Title V Permit Capacity Collector

## Your Mission

Build a Python script (`collectors/epa_echo/nebraska_capacity_collector.py`) that downloads Title V operating permits from the **Nebraska Department of Environment and Energy (NDEE)** and extracts equipment-level capacity data for soybean/oilseed processing facilities (crush plants, refineries, biodiesel plants). The script must follow the exact same pattern as the working Iowa collector at `collectors/epa_echo/iowa_capacity_collector.py`.

This is part of a multi-state facility capacity database. Iowa is already complete and loaded. Nebraska is the second state. The script you build will be reused (with modifications) for every other state, and also for **other industries** (ethanol plants, biodiesel-only plants) — so build it to be generalizable.

---

## What We're Extracting

From each facility's Title V operating permit PDF, we need:

1. **Crush capacity** — The maximum rated throughput of the soybean extraction/processing line
   - Measured in: tons/hour, bushels/day, or bushels/hour
   - Look for: extraction, extractor, flaker, flaking, conditioning, dehulling, cracking equipment
   - **IMPORTANT**: Distinguish between *processing* equipment (extraction, flaking, conditioning) and *handling* equipment (grain receiving, truck unloading, meal loadout). Only processing equipment rates represent true crush capacity.

2. **Refinery capacity** — Oil refining throughput, if present
   - Measured in: tons/hour or tons/year
   - Look for: oil refining, bleaching, deodorizing, degumming, caustic refining, hydrogenation
   - Classify refining type: RBD (Refined/Bleached/Deodorized), RB (Refined/Bleached), Degummed, Caustic Refined

3. **Biodiesel capacity** — Biodiesel/renewable fuel production, if present
   - Measured in: million gallons/year (MGY)
   - Look for: biodiesel, transesterification, methanol, glycerin, renewable diesel

4. **Equipment inventory** — Every emission unit with a rated capacity, for audit purposes

---

## Nebraska NDEE Permit System — What We Know

### Primary Interface: NDEE Permit Search
- **Search page**: `https://deq-iis.ne.gov/zs/permit/main_search.php`
- **Results page**: `https://deq-iis.ne.gov/zs/permit/main_result.php` (HTML with Dojo toolkit JS grid)
- **JSON API**: POST to `https://deq-iis.ne.gov/zs/permit/result_result.php` returns JSON

### JSON API Details (CRITICAL)
The NDEE permit search has a JSON endpoint. Here's what we discovered:

```
POST https://deq-iis.ne.gov/zs/permit/result_result.php
Content-Type: application/x-www-form-urlencoded
```

**WARNING**: The name filter parameter does NOT work server-side. Every POST request returns the **entire database** of ~13,050 permit records regardless of what name you pass. The filtering happens client-side in the Dojo JavaScript grid. Your script must:
1. Download the full ~13,050-record JSON dump
2. Filter locally for our target facilities

**JSON record structure** (each record has these fields):
```json
{
  "DBNAME": "NE-AIR",
  "PERMIT": "PERMIT_NUMBER_HERE",
  "AUTTYP": "Title V",
  "COUNTY": "County Name",
  "CITY": "City Name",
  "NAME": "FACILITY NAME",
  "ADDRESS": "Street Address",
  "MODSQ": 12345
}
```

**Key field**: `MODSQ` — This is the unique identifier used to access the permit detail page.

### Permit Detail & Download
- **Detail page**: `https://deq-iis.ne.gov/zs/permit/main_detail.php?modsq={MODSQ}`
- **Download endpoint**: `https://deq-iis.ne.gov/zs/permit/download_result.php` — This likely serves permit PDF documents. You will need to inspect the detail page HTML to find the exact download links for PDF files. The detail page probably contains links to permit documents.

### Alternative: Direct Source IDs from ECHO
Each Nebraska facility in EPA ECHO has a `source_id` that follows the pattern `NE0000003XXXXXXXXX` or `NELLC0003XXXXXXXXX`. These may correspond to NDEE permit identifiers. Cross-reference these with the JSON API `PERMIT` field.

---

## Nebraska Target Facilities (from EPA ECHO database)

These 15 facilities are already in our `bronze.epa_echo_facility` table. **Focus on the operating SIC 2075/2076 facilities first** (the primary crushers). Permanently closed facilities can be skipped.

### Operating Facilities (Priority)

| FRS Registry ID | Facility Name | City | County | SIC | NAICS | Source ID | Status |
|----------------|---------------|------|--------|-----|-------|-----------|--------|
| 110002382680 | ADM SOYBEAN OIL EXTRACTION PLANT | LINCOLN | Lancaster | 2075, 2079 | 311222 | NELLC0003110900011 | Operating |
| 110015682260 | ADM MILLING | LINCOLN | Lancaster | 2041, 5153, 2047, 2048, 2075 | 311111, 311211 | NELLC0003110900003 | Operating |
| 110000447437 | ARCHER DANIELS MIDLAND CO | FREMONT | Dodge | 2075 | 311224 | NE0000003105300018 | Operating |
| 110000724306 | BRUNING GRAIN & FEED CO | BRUNING | Thayer | 5153, 4221, 2075 | 424510 | NE0000003116900005 | Operating |
| 110045416974 | ECO-ENERGY DISTRB-BEATRICE LLC | BEATRICE | Gage | 2075, 2869 | 325193 | NE0000003106700085 | Operating |
| 110041346843 | FRONTIER COOPERATIVE | COLUMBUS | Platte | 2075 | 311119 | NE0000003114100103 | Operating |
| 110040498235 | INGREDION INCORPORATED | SOUTH SIOUX CITY | Dakota | 2075, 2076 | 311224 | NE0000003104300032 | Operating |
| 110007131004 | KANSAS ORGANIC PRODUCERS ASSN | DU BOIS | Pawnee | 2075 | 311224 | NE0000003113300018 | Operating |
| 110002441947 | NEBRASKA SOYBEAN PROCESSING | SCRIBNER | Dodge | 2075, 4221 | 311224 | NE0000003105300070 | Operating |
| 110000497686 | RICHARDSON MILLING INC | SOUTH SIOUX CITY | Dakota | 2041, 2043, 2076, 5141, 4221, 2099 | 311211 | NE0000003104300020 | Operating |

### Planned Facilities (Include if permits found)

| FRS Registry ID | Facility Name | City | County | SIC | NAICS | Source ID | Status |
|----------------|---------------|------|--------|-----|-------|-----------|--------|
| 110071344603 | AG PROCESSING INC | DAVID CITY | Butler | 2075 | 311224 | NE0000003102300054 | Planned |
| 110071274379 | NORFOLK CRUSH LLC | NORFOLK | Madison | 2075 | 311224 | NE0000003111900119 | Planned |

### Permanently Closed (Skip unless needed for historical)

| FRS Registry ID | Facility Name | City | SIC | Status |
|----------------|---------------|------|-----|--------|
| 110000448472 | AGP CORN PROCESSING INC | HASTINGS | 2869, 2075, 2085 | Permanently Closed |
| 110040499788 | REPUBLICAN VALLEY BIOFUELS LLC | ARAPAHOE | 2075, 2076, 2869 | Permanently Closed |
| 110033138575 | RVBF-SUN OIL | ARAPAHOE | 2075, 2076 | Permanently Closed |

---

## SIC Code Reference

- **2075** — Soybean Oil Mills (primary crush plants)
- **2076** — Vegetable Oil Mills, Except Corn, Cottonseed, and Soybean
- **2079** — Shortening, Table Oils, Margarine (oil refining)
- **2041** — Flour and Other Grain Mill Products
- **2046** — Wet Corn Milling
- **2048** — Prepared Feeds
- **2869** — Industrial Organic Chemicals (often biodiesel)
- **5153** — Grain and Field Beans (wholesale)

---

## Step-by-Step Implementation Guide

### Step 1: Download the NDEE Permit Database

```python
import requests
import json

def download_ndee_permits():
    """Download full NDEE permit database via JSON API."""
    url = 'https://deq-iis.ne.gov/zs/permit/result_result.php'

    # The server ignores filter parameters and returns everything
    # POST with minimal params
    response = requests.post(url, data={}, timeout=120)

    # Response is JSON array of ~13,050 records
    all_permits = response.json()

    # Filter for Title V air permits
    title_v = [p for p in all_permits if p.get('AUTTYP', '').strip() == 'Title V']

    return title_v
```

### Step 2: Match NDEE Permits to Our Target Facilities

Match using facility name and/or city. The NDEE `NAME` field should match (case-insensitive) against our ECHO facility names. Also try matching the NDEE `PERMIT` field against the ECHO `source_id` or `caa_permit_ids`.

Build a mapping dict like Iowa's `IOWA_SOY_PERMITS`:

```python
NE_SOY_PERMITS = {
    'adm_lincoln_extraction': {
        'name': 'ADM Soybean Oil Extraction Plant',
        'city': 'Lincoln',
        'frs_registry_id': '110002382680',
        'source_id': 'NELLC0003110900011',
        # These fields to be filled after matching:
        'permit_number': '',   # from NDEE JSON
        'modsq': 0,           # from NDEE JSON — needed for detail/download
    },
    # ... more facilities
}
```

### Step 3: Access Permit Detail Pages

For each matched facility, fetch the detail page:
```
GET https://deq-iis.ne.gov/zs/permit/main_detail.php?modsq={MODSQ}
```

Parse the HTML to find links to downloadable permit PDF documents. Look for:
- Direct PDF links (`.pdf` extensions)
- Download links pointing to `download_result.php` with parameters
- Document attachment sections

### Step 4: Download Permit PDFs

Save each PDF to `collectors/epa_echo/raw/ne_{facility_key}_titlev.pdf`

Be polite to the server:
- Use a standard User-Agent header
- Add 1.5-second delays between downloads
- Check file size > 10KB to verify successful download
- Skip already-downloaded files

### Step 5: Parse PDFs with pdfplumber

Use the `TitleVCapacityParser` class from `iowa_capacity_collector.py` as your starting point. Title V permits across states follow a similar federal format, but Nebraska may have state-specific variations:

**Nebraska-specific considerations:**
- NDEE may use different section headers than Iowa DNR
- Equipment table format may differ (column order, labels)
- Capacity units may vary (tons/hr vs tons/hour, bu/hr vs bushels/hour)
- Look for "Emission Unit" or "Emission Point" sections
- Operating limits may be in different sections

**Adapt the parser if needed**, but start with the Iowa parser and only modify what's necessary. The core logic (regex for capacity values, keyword classification, priority hierarchy) should transfer.

### Step 6: Output JSON + Excel

Output format must match Iowa exactly for the database loader:

**JSON** (`collectors/epa_echo/output/nebraska_soy_capacity_YYYY-MM-DD.json`):
```json
{
  "adm_lincoln_extraction": {
    "facility_name": "ADM Soybean Oil Extraction Plant",
    "permit_number": "PERMIT_NUMBER",
    "facility_description": "Soybean Oil Mills (SIC 2075)",
    "crush_capacity_tons_per_hour": 250.0,
    "crush_capacity_bushels_per_day": 0,
    "crush_description": "extraction; flaker",
    "has_refinery": true,
    "refinery_capacity_tons_per_hour": 7.5,
    "refinery_capacity_tons_per_year": 0,
    "refinery_description": "bleaching; deodorizing",
    "refining_type": "RBD",
    "has_biodiesel": false,
    "biodiesel_capacity_mgy": 0,
    "total_emission_units_found": 85,
    "crush_units": 12,
    "refinery_units": 4,
    "biodiesel_units": 0
  }
}
```

**Excel** (`collectors/epa_echo/output/nebraska_soy_capacity_YYYY-MM-DD.xlsx`):
- Sheet 1: "Capacity Summary" — one row per facility
- Sheet 2: "Equipment Detail" — every emission unit with capacity, for audit

### Step 7: Load into Database

The existing loader script (`scripts/load_echo_capacity_data.py`) already supports multi-state data. After your collector produces the JSON, add a `load_nebraska_capacity()` function to the loader (or generalize the existing `load_iowa_capacity()` to accept any state).

The database tables are:
- `bronze.permit_capacity` — one row per facility-permit (natural key: `state, permit_number`)
- `bronze.permit_emission_unit` — one row per equipment (many per facility)

Both tables already have `state` column. Just use `state='NE'`.

---

## Quality Requirements

### Logging
- Print status for each facility: downloading, parsing, capacity found
- Log warnings for facilities where no capacity data was extracted
- Log errors with traceback for any parsing failures
- Summary table at the end showing all facilities and their capacities

### Validation
- Cross-check that extracted permit numbers appear in the NDEE JSON data
- Verify crush capacity values are reasonable (typically 50-500 tons/hour for soy crushers)
- Flag any facility with SIC 2075 but 0 crush capacity (may indicate parsing issue)
- Count emission units per facility — most soy crushers have 20-100+ emission units

### Idempotency
- Skip already-downloaded PDFs (check file exists and size > 10KB)
- JSON output overwrites cleanly
- Database upserts use ON CONFLICT (state, permit_number) DO UPDATE

### CLI Interface
```
python nebraska_capacity_collector.py                     # Full pipeline
python nebraska_capacity_collector.py --download-only     # Just download PDFs
python nebraska_capacity_collector.py --parse-only        # Parse existing PDFs
python nebraska_capacity_collector.py --facility "ADM"    # Single facility
```

---

## Industry Generalization Notes

This script should be designed so it can later be adapted for:

1. **Ethanol plants** — SIC 2085 (Industrial Ethanol), 2869 (Industrial Organic Chemicals)
   - Key equipment: fermentation, distillation, DDGS dryer
   - Capacity measured in: million gallons/year (MGY), bushels/year

2. **Biodiesel-only plants** — SIC 2911, 2899
   - Key equipment: transesterification, methanol recovery, glycerin processing
   - Capacity measured in: MGY

3. **Corn milling** — SIC 2046
   - Key equipment: steeping, milling, drying
   - Capacity measured in: bushels/day

To support this, keep the keyword classification system modular. The Iowa collector has separate keyword lists (`CRUSH_KEYWORDS`, `REFINERY_KEYWORDS`, `BIODIESEL_KEYWORDS`) that can be extended or swapped.

---

## Key Files for Reference

All files are relative to project root: `C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent`

| File | What It Contains |
|------|-----------------|
| `collectors/epa_echo/iowa_capacity_collector.py` | **THE PATTERN TO FOLLOW** — complete Iowa pipeline (download + parse + Excel/JSON output) |
| `collectors/epa_echo/output/iowa_soy_capacity_2026-02-11.json` | Example of expected JSON output format |
| `scripts/load_echo_capacity_data.py` | Database loader (already supports multi-state) |
| `database/migrations/024_epa_echo_facility_capacity.sql` | Database schema — tables and gold views |
| `collectors/epa_echo/epa_echo_collector.py` | ECHO facility collector (for reference on how ECHO data was gathered) |

---

## Expected Results

Based on industry knowledge, Nebraska's major soy crushers include:

| Facility | City | Expected Capacity |
|----------|------|-------------------|
| ADM Soybean Oil Extraction | Lincoln | Large — ADM's major NE plant |
| Archer Daniels Midland | Fremont | Medium-large crush |
| Nebraska Soybean Processing | Scribner | Medium crush — independent processor |
| Ingredion | South Sioux City | Likely oilseed processing (SIC 2075/2076) |
| AG Processing Inc | David City | New/planned AGP facility |
| Norfolk Crush LLC | Norfolk | New/planned facility |

Nebraska total crush capacity is estimated at 500-800 million bushels/year industry-wide. Your extracted numbers should be in a reasonable range.

---

## Troubleshooting

1. **If the JSON API changes or stops working**: Fall back to scraping the HTML result page at `main_result.php`. The Dojo grid loads data dynamically but the initial HTML may contain some records.

2. **If permit PDFs aren't directly downloadable**: The detail page may require navigating through multiple links. Use the `requests` session to maintain cookies.

3. **If the parser extracts garbage**: Check for multi-column PDF layouts. pdfplumber handles these, but you may need to use `page.extract_tables()` instead of `page.extract_text()` for tabular permit sections.

4. **If capacity values seem too high or too low**: Nebraska permits may express capacity in different units than Iowa (e.g., lb/hr instead of tons/hr, or metric tons instead of short tons). Check the unit field carefully.

---

## Python Dependencies

```
pip install pdfplumber requests openpyxl pandas python-dotenv psycopg2-binary
```

## Final Deliverables

1. `collectors/epa_echo/nebraska_capacity_collector.py` — Main collector script
2. Downloaded PDFs in `collectors/epa_echo/raw/` (prefixed with `ne_`)
3. `collectors/epa_echo/output/nebraska_soy_capacity_YYYY-MM-DD.json`
4. `collectors/epa_echo/output/nebraska_soy_capacity_YYYY-MM-DD.xlsx`
5. Updated `scripts/load_echo_capacity_data.py` with Nebraska support (or a new generic `load_state_capacity()`)
