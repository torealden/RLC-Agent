# Prompt: Build Indiana IDEM Title V Permit Capacity Collector

## Your Mission

Build a Python script (`collectors/epa_echo/indiana_capacity_collector.py`) that downloads Title V operating permits from the **Indiana Department of Environmental Management (IDEM)** and extracts equipment-level capacity data for soybean/oilseed processing facilities (crush plants, refineries, biodiesel plants). The script must follow the exact same pattern as the working Iowa collector at `collectors/epa_echo/iowa_capacity_collector.py`.

This is part of a multi-state facility capacity database. Iowa is already complete and loaded. Indiana is one of the next states. The script you build will be reused (with modifications) for every other state, and also for **other industries** (ethanol plants, biodiesel-only plants) — so build it to be generalizable.

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

## Indiana IDEM Permit System — What We Know

### 1. CAATS — Compliance and Air Tracking System
- **URL**: `https://caats.idem.in.gov/`
- **Technology**: PrimeFaces (Java JSF framework) web interface
- **Capabilities**: Search by SIC code, facility name, county, source ID
- **Key feature**: "View, download and print draft and final permit documents"
- **SIC code search**: Supports searching by SIC code (use 2075 for soybean oil mills)

### How CAATS Works (PrimeFaces)
PrimeFaces-based applications use AJAX form submissions. To interact programmatically:

1. **Initial page load**: GET `https://caats.idem.in.gov/` — captures session cookies + JSF ViewState token
2. **Search submission**: POST form data including:
   - `javax.faces.ViewState` (from page HTML)
   - Search parameters (SIC code, facility name, etc.)
   - `javax.faces.partial.ajax=true` for AJAX mode
3. **Results**: Returned as HTML fragments or XML partial updates
4. **Pagination**: PrimeFaces DataTable pagination via AJAX POST

**Approach options:**
- **Option A (Preferred)**: Use `requests` + BeautifulSoup to POST the search form and parse HTML results. You'll need to extract the `ViewState` token from the initial page load.
- **Option B**: Use Selenium/Playwright if the AJAX interaction proves too complex for raw requests.
- **Option C**: If CAATS proves intractable, try the alternative sources below.

### 2. IDEM Virtual File Cabinet
- **URL**: `https://www.in.gov/idem/legal/public-records/virtual-file-cabinet`
- **Description**: Repository for public records including permit documents
- **May contain**: Downloadable permit PDFs organized by facility or permit number

### 3. IDEM Pending and Issued Permits
- **URL**: `https://www.in.gov/idem/airpermit/information-about/pending-and-issued-permits/`
- **Description**: Lists recently pending and issued air permits
- **Useful for**: Finding recent permit renewals for our target facilities

### 4. IDEM Source IDs (from ECHO)
Every Indiana facility in ECHO has a `source_id` following the pattern `IN0000001XXXXXXXXX`. These are IDEM's internal identifiers and can be used to search CAATS directly.

Example source IDs for our target facilities:
- ADM Frankfort: `IN0000001802300011`
- Bunge Morristown: `IN0000001814500035`
- Bunge Decatur: `IN0000001800100005`
- Cargill Lafayette: `IN0000001815700038`
- Louis Dreyfus Claypool: `IN0000001808500102`
- White River Seymour: `IN0000001807100018`

These source IDs are likely searchable in CAATS and may appear in permit document URLs.

---

## Indiana Target Facilities (from EPA ECHO database)

These 14 facilities are already in our `bronze.epa_echo_facility` table. **Focus on the operating SIC 2075 facilities** (active crushers).

### Operating Facilities (Priority — Active Crushers)

| FRS Registry ID | Facility Name | City | County | SIC | NAICS | Source ID | CAA Permit IDs |
|----------------|---------------|------|--------|-----|-------|-----------|----------------|
| 110000592047 | ARCHER DANIELS MIDLAND COMPANY | FRANKFORT | Clinton | 2075 | 311222 | IN0000001802300011 | IN0000001802300011; CEDRI10001284; 8165111; 1003215 |
| 110000396624 | BUNGE NORTH AMERICA (EAST), LLC | MORRISTOWN | Shelby | 2075 | 311222 | IN0000001814500035 | IN0000001814500035; CEDRI10250513; 8201411; 1002433 |
| 110000400325 | BUNGE NORTH AMERICA EAST LLC | DECATUR | Adams | 2075, 2048, 5153 | 311222, 311224 | IN0000001800100005 | IN0000001800100005; CEDRI81466; 8223211 |
| 110017418098 | CARGILL INC SOYBEAN PROCESSING DIVISION | LAFAYETTE | Tippecanoe | 2075 | 311222 | IN0000001815700038 | IN0000001815700038; CEDRI90156; CEDRI10166527; 4917411; 1011980 |
| 110058500616 | LOUIS DREYFUS COMPANY AGRICULTURAL PRODUCTS LLC | CLAYPOOL | Kosciusko | 5169, 2075 | 311225 | IN0000001808500102 | IN0000001808500102; CEDRI102703; 15006311; 1002988 |
| 110025333253 | WHITE RIVER SEYMOUR LLC | SEYMOUR | Jackson | 2075 | 311224 | IN0000001807100018 | IN0000001807100018; CEDRI93625; 10715811 |

### Operating (Non-Crusher — Include if capacity found)

| FRS Registry ID | Facility Name | City | County | SIC | NAICS | Source ID | Notes |
|----------------|---------------|------|--------|-----|-------|-----------|-------|
| 110014325300 | SOLAE, INC. | REMINGTON | Jasper | 2066, 2879, 2099, 2075 | 111998 | IN0000001807300011 | Soy protein isolate, not a crusher |

### Permanently Closed (Skip unless needed)

| FRS Registry ID | Facility Name | City | SIC | Status |
|----------------|---------------|------|-----|--------|
| 110003074002 | BUNGE NORTH AMERICA (EAST), LLC | INDIANAPOLIS | 2048, 2075, 5153 | Permanently Closed |
| 110007396415 | CON AGRA SOYBEAN PROCESSING COMPANY | WEST FRANKLIN | 2075 | Permanently Closed |
| 110000403661 | CONSOLIDATED GRAIN & BARGE COMPANY | MOUNT VERNON | 2075, 5153 | Permanently Closed |
| 110007396424 | CSE PROCESSING, LLC - COMB | NEW HAVEN | 2075 | Permanently Closed |
| 110000742064 | PHM BRANDS, LLC DBA VIO-COOP | MICHIGAN CITY | 2041, 2076 | Permanently Closed |
| 110058812539 | ROSE ACRES FARMS - CORT ACRE EGG FARM | SEYMOUR | 2075 | No Operating Status |
| 110040494514 | ULTRA SOY OF AMERICA, LLC | SOUTH MILFORD | 2075 | Permanently Closed |

---

## SIC Code Reference

- **2075** — Soybean Oil Mills (primary crush plants)
- **2076** — Vegetable Oil Mills, Except Corn, Cottonseed, and Soybean
- **2079** — Shortening, Table Oils, Margarine (oil refining)
- **2041** — Flour and Other Grain Mill Products
- **2048** — Prepared Feeds
- **2066** — Chocolate and Cocoa Products
- **2869** — Industrial Organic Chemicals (often biodiesel)
- **5153** — Grain and Field Beans (wholesale)
- **5169** — Chemicals and Allied Products (wholesale)

---

## Step-by-Step Implementation Guide

### Step 1: Access CAATS and Find Permits

**Option A: Direct CAATS Interaction**

```python
import requests
from bs4 import BeautifulSoup

def search_caats_by_source_id(source_id):
    """Search CAATS for a facility using its IDEM Source ID."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    })

    # Step 1: Load the CAATS page to get session + ViewState
    resp = session.get('https://caats.idem.in.gov/', timeout=30)
    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract javax.faces.ViewState
    viewstate = soup.find('input', {'name': 'javax.faces.ViewState'})
    if viewstate:
        vs_value = viewstate['value']

    # Step 2: POST search with source ID
    # You'll need to inspect the CAATS page HTML to find:
    # - The form action URL
    # - The input field names for source ID search
    # - Any required hidden fields (PrimeFaces component IDs)
    # ...

    # Step 3: Parse results for permit document links
    # ...
```

**Option B: Search by SIC Code**

Instead of searching facility-by-facility, search for ALL SIC 2075 permits in Indiana:
```python
# POST search with SIC code = 2075
# This should return all soybean oil mill permits in one query
```

**Option C: If CAATS is too complex, use Source IDs to construct direct URLs**

Some state permit systems allow direct URL access if you know the facility's source ID:
```
https://caats.idem.in.gov/facility/{SOURCE_ID}
https://caats.idem.in.gov/search?sourceId=IN0000001802300011
```

Try various URL patterns. Also try the Virtual File Cabinet for direct PDF access.

### Step 2: Build the Facility-Permit Mapping

Create a mapping dict like Iowa's:

```python
IN_SOY_PERMITS = {
    'adm_frankfort': {
        'name': 'Archer Daniels Midland Company',
        'city': 'Frankfort',
        'county': 'Clinton',
        'frs_registry_id': '110000592047',
        'source_id': 'IN0000001802300011',
        # Fill after discovering:
        'permit_number': '',
        'permit_url': '',    # Direct URL to permit PDF
    },
    'bunge_morristown': {
        'name': 'Bunge North America (East), LLC',
        'city': 'Morristown',
        'county': 'Shelby',
        'frs_registry_id': '110000396624',
        'source_id': 'IN0000001814500035',
        'permit_number': '',
        'permit_url': '',
    },
    'bunge_decatur': {
        'name': 'Bunge North America East LLC',
        'city': 'Decatur',
        'county': 'Adams',
        'frs_registry_id': '110000400325',
        'source_id': 'IN0000001800100005',
        'permit_number': '',
        'permit_url': '',
    },
    'cargill_lafayette': {
        'name': 'Cargill Inc Soybean Processing Division',
        'city': 'Lafayette',
        'county': 'Tippecanoe',
        'frs_registry_id': '110017418098',
        'source_id': 'IN0000001815700038',
        'permit_number': '',
        'permit_url': '',
    },
    'louis_dreyfus_claypool': {
        'name': 'Louis Dreyfus Company Agricultural Products LLC',
        'city': 'Claypool',
        'county': 'Kosciusko',
        'frs_registry_id': '110058500616',
        'source_id': 'IN0000001808500102',
        'permit_number': '',
        'permit_url': '',
    },
    'white_river_seymour': {
        'name': 'White River Seymour LLC',
        'city': 'Seymour',
        'county': 'Jackson',
        'frs_registry_id': '110025333253',
        'source_id': 'IN0000001807100018',
        'permit_number': '',
        'permit_url': '',
    },
}
```

### Step 3: Download Permit PDFs

Save each PDF to `collectors/epa_echo/raw/in_{facility_key}_titlev.pdf`

Be polite to the server:
- Use a standard User-Agent header
- Add 1.5-second delays between downloads
- Check file size > 10KB to verify successful download
- Skip already-downloaded files

### Step 4: Parse PDFs with pdfplumber

Use the `TitleVCapacityParser` class from `iowa_capacity_collector.py` as your starting point. Title V permits follow a federal template but Indiana (IDEM) may have state-specific variations:

**Indiana-specific considerations:**
- IDEM permits may use different section headers than Iowa DNR
- Indiana may list "Source" instead of "Emission Point"
- Equipment rated capacity may be in the "Source Description" section
- Operating conditions/limits may have different formatting
- IDEM source IDs follow format: `IN0000001XXXXXXXXX`

**Adapt the parser if needed**, but start with the Iowa parser and only modify what's necessary. The core logic (regex for capacity values, keyword classification, priority hierarchy) should transfer.

### Step 5: Output JSON + Excel

Output format must match Iowa exactly for the database loader:

**JSON** (`collectors/epa_echo/output/indiana_soy_capacity_YYYY-MM-DD.json`):
```json
{
  "adm_frankfort": {
    "facility_name": "Archer Daniels Midland Company",
    "permit_number": "PERMIT_NUMBER",
    "facility_description": "Soybean Oil Mills (SIC 2075)",
    "crush_capacity_tons_per_hour": 300.0,
    "crush_capacity_bushels_per_day": 0,
    "crush_description": "extraction; flaker; conditioning",
    "has_refinery": true,
    "refinery_capacity_tons_per_hour": 12.0,
    "refinery_capacity_tons_per_year": 0,
    "refinery_description": "bleaching; deodorizing",
    "refining_type": "RBD",
    "has_biodiesel": false,
    "biodiesel_capacity_mgy": 0,
    "total_emission_units_found": 75,
    "crush_units": 15,
    "refinery_units": 5,
    "biodiesel_units": 0
  }
}
```

**Excel** (`collectors/epa_echo/output/indiana_soy_capacity_YYYY-MM-DD.xlsx`):
- Sheet 1: "Capacity Summary" — one row per facility
- Sheet 2: "Equipment Detail" — every emission unit with capacity, for audit

### Step 6: Load into Database

The existing loader script (`scripts/load_echo_capacity_data.py`) already supports multi-state data. After your collector produces the JSON, add a `load_indiana_capacity()` function to the loader (or generalize the existing `load_iowa_capacity()` to accept any state).

The database tables are:
- `bronze.permit_capacity` — one row per facility-permit (natural key: `state, permit_number`)
- `bronze.permit_emission_unit` — one row per equipment (many per facility)

Both tables already have `state` column. Just use `state='IN'`.

---

## Quality Requirements

### Logging
- Print status for each facility: downloading, parsing, capacity found
- Log warnings for facilities where no capacity data was extracted
- Log errors with traceback for any parsing failures
- Summary table at the end showing all facilities and their capacities

### Validation
- Verify crush capacity values are reasonable (typically 50-500 tons/hour for soy crushers)
- Flag any facility with SIC 2075 but 0 crush capacity (may indicate parsing issue)
- Count emission units per facility — most soy crushers have 20-100+ emission units
- Cross-reference permit numbers with IDEM source IDs

### Idempotency
- Skip already-downloaded PDFs (check file exists and size > 10KB)
- JSON output overwrites cleanly
- Database upserts use ON CONFLICT (state, permit_number) DO UPDATE

### CLI Interface
```
python indiana_capacity_collector.py                     # Full pipeline
python indiana_capacity_collector.py --download-only     # Just download PDFs
python indiana_capacity_collector.py --parse-only        # Parse existing PDFs
python indiana_capacity_collector.py --facility "ADM"    # Single facility
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

Based on industry knowledge, Indiana's major soy crushers include:

| Facility | City | Expected Profile |
|----------|------|-----------------|
| ADM | Frankfort | Large crush + likely refinery (ADM typically has integrated operations) |
| Bunge | Morristown | Medium-large crush (Bunge's Indiana operations) |
| Bunge | Decatur | Large crush + feed operations (multi-SIC: 2075, 2048, 5153) |
| Cargill | Lafayette | Large crush (Cargill soybean processing division) |
| Louis Dreyfus | Claypool | Medium crush (LDC agricultural products) |
| White River | Seymour | Medium crush (independent processor) |

Indiana total crush capacity is significant — it's a top-5 soybean crushing state. The 6 active facilities likely represent 300-600 million bushels/year combined.

---

## Troubleshooting

1. **If CAATS PrimeFaces interaction is too complex**: Try these alternatives:
   - Search for "Indiana IDEM Title V permit" + facility name on Google — some permits may be publicly hosted
   - Try the IDEM Virtual File Cabinet for direct document access
   - Use the CAA Permit IDs from ECHO (e.g., `CEDRI10001284`) — some may be searchable in EPA's CEDRI system at `https://www.epa.gov/electronic-reporting-air-emissions/cedri`
   - Contact IDEM directly: their air permits office may have a direct download portal

2. **If permit PDFs aren't standard Title V format**: IDEM may use a different permit format than Iowa DNR. If so, you'll need to adapt the parser regex patterns. Look for "rated capacity", "maximum capacity", "equipment description" sections.

3. **If the parser extracts garbage**: Check for multi-column PDF layouts. pdfplumber handles these, but you may need to use `page.extract_tables()` instead of `page.extract_text()` for tabular permit sections.

4. **If capacity values seem too high or too low**: Indiana permits may express capacity in different units than Iowa (e.g., lb/hr instead of tons/hr, or metric tons instead of short tons). Check the unit field carefully.

5. **If you can't find the permit for a facility**: Check if the facility has been renamed or merged. ADM and Bunge frequently acquire and rename plants. The ECHO `caa_permit_ids` field may contain multiple permit IDs — try each one.

---

## Python Dependencies

```
pip install pdfplumber requests openpyxl pandas python-dotenv psycopg2-binary beautifulsoup4
```

## Final Deliverables

1. `collectors/epa_echo/indiana_capacity_collector.py` — Main collector script
2. Downloaded PDFs in `collectors/epa_echo/raw/` (prefixed with `in_`)
3. `collectors/epa_echo/output/indiana_soy_capacity_YYYY-MM-DD.json`
4. `collectors/epa_echo/output/indiana_soy_capacity_YYYY-MM-DD.xlsx`
5. Updated `scripts/load_echo_capacity_data.py` with Indiana support (or a new generic `load_state_capacity()`)
6. Notes on any CAATS API endpoints discovered (document these in comments for reuse)
