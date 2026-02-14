# Illinois Capacity Collector — Context for Next Session

## What Was Just Completed (This Session)

### Database Schema — DONE
- **Migration 024** created and applied: `database/migrations/024_epa_echo_facility_capacity.sql`
- 3 bronze tables: `bronze.epa_echo_facility` (200 records), `bronze.permit_capacity` (17 Iowa records), `bronze.permit_emission_unit` (648 records)
- 3 gold views: `gold.facility_capacity`, `gold.state_crush_capacity`, `gold.crush_capacity_ranking`
- **Loader script:** `scripts/load_echo_capacity_data.py` — loads Excel/JSON into bronze tables
- Iowa data fully loaded and verified. 13/17 FRS IDs matched. Gold views working.

### Iowa Capacity Collector — DONE (Previous Session)
- `collectors/epa_echo/iowa_capacity_collector.py` — full pipeline
- Downloads Title V permit PDFs from Iowa DNR (`iowadnr.gov/media/{MEDIA_ID}/download`)
- Parses with pdfplumber, classifies equipment, outputs Excel + JSON
- Iowa results: 17 facilities, 10 with crush capacity, 1,482 tph total, 6 refineries

## What We're Working On Now: Illinois

### Goal
Build `collectors/epa_echo/illinois_capacity_collector.py` following the same pattern as Iowa.

### Illinois ECHO Facilities (23 in database)
From `bronze.epa_echo_facility WHERE state = 'IL'`:
- ADM COMPANY
- ADM QUINCY
- ARCHER DANIELS MIDLAND - VITAMIN 5 PLANT
- ARCHER DANIELS MIDLAND CO
- BUNGE NORTH AMERICA INC
- BUNGE OILS INC
- CARGILL INC
- CORBION BIOTECH INC
- DEVANSOY LLC
- GRAIN CRAFT CORN LLC
- INCOBRASA INDUSTRIES LTD
- MARQUIS RENEWABLE OILS LLC
- NORTH STAR FEEDS LLC
- PIONEER HI-BRED INTERNATIONAL INC
- SOLAE LLC
- WABASH VALLEY ASPHAL

### Illinois Permit System Research (In Progress)
We researched how to access Illinois EPA Title V permits. Key findings:

1. **IEPA Document Explorer** — `https://webapps.illinois.gov/EPA/DocumentExplorer/Attributes`
   - Search interface for air permits by facility name, address, city, county
   - Also searchable by IEPA Bureau ID (format: 6 digits + 3 letters, e.g., "031600ABC")
   - **Problem:** JavaScript-based, uses ASP.NET MVC with `data-ajax` attributes
   - Form action: `/EPA/DocumentExplorer/Attributes/Search?Length=10`
   - The search form ID is `#attributeSearchForm`
   - We could NOT get API responses via simple GET/POST requests — needs JS execution
   - The app bundle JS is minimal (~9KB), uses `data-ajax-begin`, `data-ajax-complete` patterns

2. **Bureau of Air Permit Notice Archive** — `https://epa.illinois.gov/public-notices/boa-notices/archive.html`
   - Searchable by: facility name, city, county, permit type, BOA ID, permit number, **SIC code**, date range
   - Covers permits with public notice since January 1, 2017
   - Also JavaScript-based search form — not easily scriptable with simple requests

3. **EPA Region 5 access page** — `https://www.epa.gov/caa-permitting/access-state-issued-permits-region-5`
   - Points to the BOA archive above for post-2017 permits
   - For pre-2015 permits: contact Brad Frost (brad.frost@illinois.gov, 217-782-7027)

### What Hasn't Been Tried Yet
- **Selenium/Playwright** to drive the IEPA Document Explorer JS interface
- **Direct Bureau of Air IEPA Bureau IDs** — if we can find the 6-digit+3-letter Bureau IDs for our 23 IL facilities, we can search the Document Explorer directly
- **ECHO data may contain IEPA IDs** — check `caa_permit_ids` or `source_id` fields in our ECHO data for Illinois facilities; these might map to IEPA Bureau IDs
- **FOIA request** — last resort, request all Title V permits for SIC 2075/2076 facilities
- **Try fetching the BOA archive as a POST form** — it may accept form data differently than the Document Explorer

### Recommended Next Steps
1. **Check if ECHO data already has IEPA Bureau IDs** for IL facilities (look at `caa_permit_ids`, `source_id` columns)
2. **Try the BOA archive search** with POST form data for SIC code 2075
3. If those fail, **use Selenium** to drive the Document Explorer and download permits
4. Once PDFs are obtained, the existing `TitleVCapacityParser` from `iowa_capacity_collector.py` should work for IL permits too (same Title V format)
5. Build `illinois_capacity_collector.py` with IL-specific permit mapping dict
6. Run loader to populate `bronze.permit_capacity` with state='IL'

## Key Files
- `collectors/epa_echo/iowa_capacity_collector.py` — **pattern to follow** for IL collector
- `collectors/epa_echo/epa_echo_collector.py` — has ECHO facility data
- `collectors/epa_echo/output/epa_echo_soybean_oilseed_facilities_2026-02-11.xlsx` — ECHO Excel with IL facility details
- `scripts/load_echo_capacity_data.py` — loader (already supports multi-state, just needs IL JSON)
- `database/migrations/024_epa_echo_facility_capacity.sql` — schema already supports multi-state

## User Intent
"We are going to need them all" — meaning all major soybean crushing states. Illinois first (23 facilities), then OH, IN, NE, MO, MN, KS, ND, etc. The database schema and loader are already built for multi-state. Just need state-specific collectors.
