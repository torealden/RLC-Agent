# Claude Code Prompt: EPA ECHO Soybean Processor Data Collector

## Project Context

You are building the first data collector for Round Lakes Companies (RLC), a commodity market analysis firm. This collector retrieves facility-level data on all US soybean and oilseed crushing plants from the EPA's ECHO (Enforcement and Compliance History Online) database. This is the first of many collectors we will build — the same architecture will be reused for biodiesel/renewable diesel plants, ethanol plants, non-ethanol corn-processing plants, and wheat-milling plants. **Design everything to be modular and reusable.**

The collector does NOT require an API key. The ECHO REST API is completely public with no authentication.

---

## Part 1: The ECHO Data Collector Script

### API Details

**Base URL:** `https://echodata.epa.gov/echo/`

**Endpoints to query (in order):**

#### 1. Air Facility Search (Primary — most soybean plants are CAA-regulated)
```
GET https://echodata.epa.gov/echo/air_rest_services.get_facilities
```
Parameters:
- `output=JSON`
- `p_nai=311224` (NAICS code for Soybean and Other Oilseed Processing)
- `p_act=Y` (active facilities only — run a second pass with `p_act=N` for inactive/closed)
- `responseset=0` (first page; increment for pagination)
- `p_qcolumns=` (specify column IDs for the data fields we need — see below)

Also search with legacy SIC codes in a separate call:
- `p_sic=2075` (Soybean Oil Mills)
- `p_sic=2076` (Vegetable Oil Mills, Except Corn, Cottonseed, and Soybean)

#### 2. All Media Programs Facility Search (catches facilities not in CAA)
```
GET https://echodata.epa.gov/echo/echo_rest_services.get_facilities
```
Same parameters as above, same NAICS/SIC codes.

#### 3. Detailed Facility Report (for each facility found)
```
GET https://echodata.epa.gov/echo/dfr_rest_services.get_dfr
```
Parameters:
- `p_id=<REGISTRY_ID>` (the FRS Registry ID from the facility search)
- `output=JSON`

#### 4. NAICS 2022 Update
The NAICS 2022 revision split 311224 into:
- **311222** — Soybean Processing (soybeans only)
- **311223** — Other Oilseed Processing (cottonseed, canola, sunflower, etc.)

Search all three codes (311224, 311222, 311223) to ensure complete coverage regardless of which NAICS vintage a facility was registered under. Also search 311225 (Fats and Oils Refining and Blending) as some integrated crush+refinery operations may be classified there.

### Data Fields to Collect

For every facility, collect ALL available fields. At minimum, we need:

| Field | Purpose |
|-------|---------|
| FRS Registry ID | Universal EPA cross-reference key |
| Facility Name | Identification |
| Street Address, City, State, ZIP | Location |
| Latitude, Longitude | GIS mapping |
| County (FIPS) | Regional analysis |
| EPA Region | Regulatory jurisdiction |
| Owner/Operator Name | Company identification |
| SIC Code(s) | Industry classification (legacy) |
| NAICS Code(s) | Industry classification (current) |
| Operating Status | Active/Inactive/Closed |
| CAA Permit ID (Source ID) | Air permit cross-reference |
| NPDES Permit ID | Water permit cross-reference |
| RCRA Handler ID | Hazardous waste cross-reference |
| TRI Facility ID | Toxics release cross-reference |
| GHG Reporter ID | Greenhouse gas cross-reference |
| Permit Issuing Agency | State vs. EPA |
| Air Programs (SIP, NSPS, NESHAP, PSD, Title V) | Regulatory programs |
| Last Inspection Date | Compliance monitoring |
| Compliance Status (3-year quarters) | Operational indicator |
| Any enforcement actions | Operational risk indicator |

### Script Requirements

Write a Python script: `epa_echo_collector.py`

```
Location: C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\collectors\epa_echo\
```

**Dependencies:** `requests`, `pandas`, `openpyxl`, `json`, `logging`, `datetime`, `time`, `hashlib`

**Behavior:**

1. **Query all relevant endpoints** with all NAICS codes (311224, 311222, 311223, 311225) and SIC codes (2075, 2076) across both the Air Facility Search and All Media Programs search.

2. **Deduplicate results** by FRS Registry ID. Many facilities will appear in multiple searches. Keep all records but flag which searches returned each facility.

3. **Handle pagination.** The ECHO API returns results in pages. The response JSON includes a `QueryRows` count. If results exceed one page, iterate using `responseset` parameter (0-indexed, typically 20 or 25 results per page). Check the API response for the exact page size.

4. **Rate limiting.** Add a 1-second delay between API calls. The ECHO API doesn't publish rate limits, but be respectful. If a 429 or 503 is returned, implement exponential backoff (wait 5s, 10s, 30s, then fail with logged error).

5. **For each unique facility**, call the Detailed Facility Report endpoint to get the full cross-reference IDs (TRI, GHG, RCRA, etc.) and any additional detail not in the search results.

6. **Save results to Excel** with the following worksheets:

   - **Facilities** — One row per facility, all fields, deduplicated by FRS ID
   - **Search Coverage** — Which NAICS/SIC code and endpoint returned each facility (for audit)
   - **API Metadata** — Timestamp of each API call, endpoint URL, HTTP status code, record count returned, response hash
   - **Data Dictionary** — Column names, descriptions, data types, and source endpoint

   Save to: `C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\collectors\epa_echo\output\epa_echo_soybean_facilities_{YYYY-MM-DD}.xlsx`

7. **Also save raw JSON responses** to: `C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\collectors\epa_echo\raw\{endpoint}_{naics}_{timestamp}.json`

---

## Part 2: Logging Architecture

### CRITICAL — This logging pattern applies to ALL collectors and agents built for RLC.

Every collector and agent must use a standardized logging system. There is a separate log-reading script that monitors these logs and sends email alerts when problems are detected. The log format must be consistent across all programs.

### Log File Location
```
C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\logs\collectors\epa_echo\
```

### Log File Naming
```
epa_echo_collector_{YYYY-MM-DD}_{HH-MM-SS}.log
```

### Log Format (Structured JSON Lines)

Every log entry must be a single JSON line with these fields:

```json
{
  "timestamp": "2026-02-11T14:30:00.000Z",
  "level": "INFO|WARNING|ERROR|CRITICAL",
  "collector": "epa_echo_collector",
  "action": "API_CALL|DATA_SAVE|DATA_UPDATE|DATA_DELETE|VALIDATION|STARTUP|SHUTDOWN|ERROR",
  "details": {
    "description": "Human-readable description of what happened",
    "source_type": "API",
    "source_endpoint": "https://echodata.epa.gov/echo/air_rest_services.get_facilities?p_nai=311224&p_act=Y&output=JSON",
    "source_params": {"p_nai": "311224", "p_act": "Y"},
    "http_status": 200,
    "records_returned": 45,
    "records_new": 3,
    "records_updated": 2,
    "records_unchanged": 40,
    "response_hash": "sha256:abc123...",
    "database_table": "bronze.epa_echo_facilities",
    "affected_record_ids": ["110071843949", "110000491354"],
    "previous_values": {"field": "old_value"},
    "new_values": {"field": "new_value"},
    "verification_url": "https://echo.epa.gov/detailed-facility-report?fid=110071843949",
    "error_message": null,
    "stack_trace": null
  },
  "duration_seconds": 2.3,
  "run_id": "uuid-for-this-entire-run"
}
```

### Logging Rules

1. **STARTUP**: Log when the collector begins, including the run_id (UUID generated at start), Python version, script version, and all configuration parameters.

2. **API_CALL**: Log every single HTTP request — the full URL (with parameters), HTTP status code, response size in bytes, response time, number of records returned, and a SHA-256 hash of the response body. If the call fails, log the error and any retry attempts.

3. **DATA_SAVE** (new record): Log the FRS ID, facility name, source endpoint that provided the data, and the verification URL where a human or checking agent can verify the data. Example verification URL: `https://echo.epa.gov/detailed-facility-report?fid={REGISTRY_ID}`

4. **DATA_UPDATE** (changed record): Log the FRS ID, which fields changed, the previous values, the new values, the source endpoint, and the verification URL. This is critical — we need to know exactly what changed and why.

5. **VALIDATION**: Log any data quality checks performed (e.g., "Facility X has no lat/long coordinates", "Facility Y has NAICS 311224 but SIC code doesn't match expected 2075").

6. **ERROR**: Log all errors with full stack traces, the context of what was being attempted, and whether the error is recoverable.

7. **SHUTDOWN**: Log summary statistics — total facilities found, new records, updated records, errors encountered, total runtime, and a final status (SUCCESS, PARTIAL_SUCCESS, FAILURE).

### Console Output

In addition to the JSON log file, print human-readable progress to the console:
```
[2026-02-11 14:30:00] Starting EPA ECHO Soybean Processor Collector (run: abc123)
[2026-02-11 14:30:01] Querying Air Facility Search: NAICS 311224 (active)...
[2026-02-11 14:30:03] → 47 facilities returned (page 1/1)
[2026-02-11 14:30:04] Querying Air Facility Search: NAICS 311224 (inactive)...
[2026-02-11 14:30:06] → 12 facilities returned (page 1/1)
...
[2026-02-11 14:35:00] Fetching Detailed Facility Reports: 0/59 complete
[2026-02-11 14:35:30] Fetching Detailed Facility Reports: 20/59 complete
...
[2026-02-11 14:38:00] COMPLETE: 63 unique facilities found (47 active, 12 inactive, 4 duplicate removals)
[2026-02-11 14:38:00] Output: epa_echo_soybean_facilities_2026-02-11.xlsx
[2026-02-11 14:38:00] Log: epa_echo_collector_2026-02-11_14-30-00.log
```

---

## Part 3: Data Verification Agent (Checking Agent)

For every collector, write a companion checking agent. This one is: `epa_echo_checker.py`

```
Location: C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\collectors\epa_echo\
```

### Purpose

After the collector saves data (to spreadsheet now, to the bronze database later), the checker independently verifies that the saved data matches the source. It does this by:

1. **Reading the collector's log file** to find all DATA_SAVE and DATA_UPDATE entries.
2. **Extracting the source endpoints and verification URLs** from those log entries.
3. **Making fresh API calls** to the original source endpoints to pull the current data.
4. **Comparing** the freshly-pulled data against the data that was saved.
5. **Reporting discrepancies** in its own log file.

### Checker Log Location
```
C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\logs\checkers\epa_echo\
```

### Checker Log Naming
```
epa_echo_checker_{YYYY-MM-DD}_{HH-MM-SS}.log
```

### Checker Behavior

```json
{
  "timestamp": "2026-02-11T15:00:00.000Z",
  "level": "INFO",
  "checker": "epa_echo_checker",
  "action": "VERIFICATION_RESULT",
  "details": {
    "collector_run_id": "uuid-of-the-collector-run-being-checked",
    "collector_log_file": "epa_echo_collector_2026-02-11_14-30-00.log",
    "record_id": "110071843949",
    "facility_name": "CARGILL INC - SIDNEY",
    "verification_source": "https://echodata.epa.gov/echo/dfr_rest_services.get_dfr?p_id=110071843949&output=JSON",
    "verification_status": "MATCH|MISMATCH|SOURCE_UNAVAILABLE|ERROR",
    "mismatched_fields": [
      {
        "field": "OperatingStatus",
        "saved_value": "Active",
        "source_value": "Inactive",
        "severity": "HIGH"
      }
    ],
    "records_checked": 63,
    "records_matched": 61,
    "records_mismatched": 1,
    "records_source_unavailable": 1
  }
}
```

### Checker Rules

1. **Run automatically** after the collector completes, OR on a schedule, OR manually.
2. **Sample or full check**: Accept a `--mode full` or `--mode sample` flag. Sample mode checks 20% of records (randomly selected). Full mode checks every record.
3. **Severity levels for mismatches**:
   - **HIGH**: Facility name, owner, operating status, lat/long changed → likely real change or data error
   - **MEDIUM**: Permit IDs, compliance status changed → may be routine update
   - **LOW**: Formatting differences, whitespace, case sensitivity
4. **If a HIGH severity mismatch is found**, the checker log should set level to `WARNING` so the log-reading email script catches it.
5. **If the source API is unavailable** (timeout, 500 error, etc.), log it as `WARNING` with `SOURCE_UNAVAILABLE` status — do not treat it as a mismatch.

---

## Part 4: Configuration File

Create a shared configuration file used by both the collector and checker:

```
Location: C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\collectors\epa_echo\config.json
```

```json
{
  "collector_name": "epa_echo",
  "collector_version": "1.0.0",
  "description": "EPA ECHO facility data for soybean and oilseed processors",

  "api": {
    "base_url": "https://echodata.epa.gov/echo/",
    "endpoints": {
      "air_facility_search": "air_rest_services.get_facilities",
      "all_media_search": "echo_rest_services.get_facilities",
      "detailed_facility_report": "dfr_rest_services.get_dfr"
    },
    "requires_api_key": false,
    "rate_limit_delay_seconds": 1.0,
    "max_retries": 3,
    "backoff_multiplier": 2,
    "timeout_seconds": 30
  },

  "search_codes": {
    "naics": ["311224", "311222", "311223", "311225"],
    "sic": ["2075", "2076"],
    "naics_descriptions": {
      "311224": "Soybean and Other Oilseed Processing (NAICS 2017)",
      "311222": "Soybean Processing (NAICS 2022)",
      "311223": "Other Oilseed Processing (NAICS 2022)",
      "311225": "Fats and Oils Refining and Blending"
    },
    "sic_descriptions": {
      "2075": "Soybean Oil Mills",
      "2076": "Vegetable Oil Mills, Except Corn, Cottonseed, and Soybean"
    }
  },

  "paths": {
    "output_dir": "C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent\\collectors\\epa_echo\\output",
    "raw_json_dir": "C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent\\collectors\\epa_echo\\raw",
    "collector_log_dir": "C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent\\logs\\collectors\\epa_echo",
    "checker_log_dir": "C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent\\logs\\checkers\\epa_echo"
  },

  "checker": {
    "default_mode": "sample",
    "sample_percentage": 20,
    "mismatch_severity": {
      "HIGH": ["FacName", "FacAddr", "OperatingStatus", "FacLat", "FacLong", "OwnerOperator"],
      "MEDIUM": ["CAAPermitIDs", "NPDESPermitIDs", "ComplianceStatus", "LastInspectionDate"],
      "LOW": ["FacCounty", "EPARegion", "SICCode", "NAICSCode"]
    }
  }
}
```

---

## Part 5: Future-Proofing and Reusability

### This is the first of many collectors. Design with these principles:

1. **Abstract the base collector class.** Create a `BaseCollector` class in a shared module at:
   ```
   C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\collectors\base\
   ```
   This base class should handle:
   - Logging setup (JSON-lines format, console output)
   - Run ID generation
   - HTTP request wrapper with retry/backoff
   - Rate limiting
   - Excel output generation
   - Raw response archiving
   - Configuration loading
   - Startup/shutdown logging

   The EPA ECHO collector inherits from this base class and only implements the API-specific logic.

2. **Abstract the base checker class.** Create a `BaseChecker` class that handles:
   - Reading collector logs
   - Extracting verification targets
   - Comparing saved vs. source data
   - Generating checker logs
   - Severity classification

3. **Directory structure:**
   ```
   RLC-Agent/
   ├── collectors/
   │   ├── base/
   │   │   ├── __init__.py
   │   │   ├── base_collector.py
   │   │   ├── base_checker.py
   │   │   └── logging_utils.py
   │   ├── epa_echo/
   │   │   ├── __init__.py
   │   │   ├── config.json
   │   │   ├── epa_echo_collector.py
   │   │   ├── epa_echo_checker.py
   │   │   ├── output/
   │   │   └── raw/
   │   ├── [future: state_air_permits/]
   │   ├── [future: epa_emts/]
   │   └── [future: usda_nass/]
   ├── logs/
   │   ├── collectors/
   │   │   └── epa_echo/
   │   └── checkers/
   │       └── epa_echo/
   └── domain_knowledge/
       └── data_dictionaries/
           ├── epa_echo_air_facility.json    ← Generate this from API metadata
           └── epa_*.json                    ← Existing RIN/EMTS dictionaries
   ```

4. **Generate a data dictionary** from the ECHO API metadata endpoint. The Air Facility Search has a metadata call:
   ```
   GET https://echodata.epa.gov/echo/air_rest_services.metadata?output=JSON
   ```
   Save the response as a data dictionary JSON file alongside the existing `epa_*` files in the data_dictionaries folder.

---

## Part 6: Database Preparation Notes (Do Not Implement Yet)

**Do not build the database integration yet.** We will design the schema after we see what data ECHO actually returns. However, when the collector saves data to the spreadsheet, structure it as if it were going into a database:

- Every row should have a unique primary key (FRS Registry ID)
- Include metadata columns: `collected_at` (timestamp), `source_endpoint`, `collector_version`, `run_id`
- Use consistent data types (dates as ISO 8601, coordinates as decimal degrees, booleans as TRUE/FALSE)
- Null values should be empty cells, not "N/A" or "None" strings

When we do build the database layer, every interaction will use the same logging pattern described in Part 2. The log entries for database operations will include:
- The SQL statement or ORM operation performed
- The table and record IDs affected
- For UPDATEs: previous values and new values
- The source endpoint or file that triggered the write
- A verification URL or method for the checking agent

---

## Part 7: Testing

Before running against the live API:

1. **Test with a single known facility first.** Use Cargill Sidney, OH — we know this is an active soybean processing plant. Search by facility name `p_fn=CARGILL` and state `p_st=OH` to verify the API returns expected data.

2. **Test pagination** by querying a broad NAICS code (e.g., 311 — all food manufacturing) with a small page size to confirm pagination logic works.

3. **Test the checker** by intentionally modifying one field in the saved data and confirming the checker catches it.

4. **Test error handling** by using an invalid endpoint URL and confirming the retry/backoff and error logging work correctly.

---

## Summary of Deliverables

| File | Description |
|------|-------------|
| `collectors/base/base_collector.py` | Abstract base class for all collectors |
| `collectors/base/base_checker.py` | Abstract base class for all checkers |
| `collectors/base/logging_utils.py` | Shared logging utilities |
| `collectors/epa_echo/config.json` | Configuration for this collector |
| `collectors/epa_echo/epa_echo_collector.py` | The ECHO data collector |
| `collectors/epa_echo/epa_echo_checker.py` | The ECHO data verification agent |
| `domain_knowledge/data_dictionaries/epa_echo_air_facility.json` | Auto-generated data dictionary |

Run the collector with:
```bash
python epa_echo_collector.py
```

Run the checker with:
```bash
python epa_echo_checker.py --collector-log <path_to_collector_log> --mode full
```

Or run both in sequence:
```bash
python epa_echo_collector.py && python epa_echo_checker.py --latest --mode sample
```
