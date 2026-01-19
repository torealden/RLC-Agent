# API Credentials & Registration Guide

This document lists all API keys, credentials, and registrations needed to run the full HB Weekly Report data collection system.

---

## Quick Summary

| Source | Auth Type | Required? | Registration URL |
|--------|-----------|-----------|------------------|
| USDA FAS OpenDataWeb | None | No | N/A |
| CFTC COT | None | No | N/A |
| US Drought Monitor | None | No | N/A |
| USDA NASS Quick Stats | API Key (Free) | **Yes** | https://quickstats.nass.usda.gov/api |
| EIA Petroleum | API Key (Free) | **Yes** | https://www.eia.gov/opendata/register.php |
| Eurostat/EC AGRI | API Key (Free) | Optional | https://agridata.ec.europa.eu/ |
| CME DataMine | Paid | Optional | https://www.cmegroup.com/market-data/datamine-api.html |

---

## Free - No Registration Required

### 1. USDA FAS OpenDataWeb
- **Status:** Ready to use
- **Data:** Export sales, PSD data
- **Notes:** No API key needed, may have rate limits

### 2. CFTC Commitments of Traders
- **Status:** Ready to use
- **Data:** Weekly positioning data
- **Notes:** Public API via Socrata

### 3. US Drought Monitor
- **Status:** Ready to use
- **Data:** Weekly drought conditions
- **Notes:** CSV/JSON downloads available

### 4. CONAB (Brazil)
- **Status:** Ready to use (limited)
- **Data:** Brazil crop estimates
- **Notes:** PDF reports, some Excel files

### 5. ABIOVE (Brazil)
- **Status:** Ready to use
- **Data:** Brazil soy crush statistics
- **Notes:** Excel file downloads

---

## Free - Registration Required

### 1. USDA NASS Quick Stats API
**REQUIRED FOR:** Crop progress/condition data

**Registration:**
1. Go to: https://quickstats.nass.usda.gov/api
2. Click "Request API Key"
3. Enter your email and information
4. Receive API key via email

**Environment Variable:**
```bash
export NASS_API_KEY="your_api_key_here"
```

**Estimated Time:** 5 minutes

---

### 2. EIA Open Data API
**REQUIRED FOR:** Weekly ethanol production/stocks data

**Registration:**
1. Go to: https://www.eia.gov/opendata/register.php
2. Fill out registration form
3. Receive API key via email

**Environment Variable:**
```bash
export EIA_API_KEY="your_api_key_here"
```

**Estimated Time:** 5 minutes

---

### 3. Eurostat/European Commission AGRI
**OPTIONAL FOR:** EU production and trade data

**Registration:**
1. Go to: https://agridata.ec.europa.eu/
2. Navigate to "API Documentation"
3. Register for machine-to-machine access

**Environment Variable:**
```bash
export EUROSTAT_API_KEY="your_api_key_here"
```

---

## Paid Services (Optional)

### 1. CME Group DataMine
**USE CASE:** Real-time or historical futures prices

**Alternatives for Free:**
- Use CME Daily Bulletin (delayed)
- Web scrape settlement prices
- Use Databento (pay-per-use, cheaper)

**Registration:**
https://www.cmegroup.com/market-data/datamine-api.html

---

### 2. Barchart cmdty API
**USE CASE:** Consolidated commodity data

**Alternatives for Free:**
- Use individual source collectors
- Most data available from free sources

---

### 3. DTN/Progressive Farmer
**USE CASE:** Cash prices, basis data

**Alternatives:**
- USDA AMS data (already implemented)
- Regional elevator quotes (manual)

---

## Environment Variables Template

Create a `.env` file in the project root:

```bash
# =============================================================================
# HB WEEKLY REPORT - API CREDENTIALS
# =============================================================================

# USDA NASS Quick Stats API (Required for crop progress)
# Register at: https://quickstats.nass.usda.gov/api
NASS_API_KEY=

# EIA Open Data API (Required for ethanol data)
# Register at: https://www.eia.gov/opendata/register.php
EIA_API_KEY=

# Eurostat API (Optional - EU data)
EUROSTAT_API_KEY=

# CME DataMine (Optional - paid)
CME_API_KEY=
CME_API_SECRET=

# Dropbox (for document upload)
DROPBOX_ACCESS_TOKEN=
DROPBOX_REFRESH_TOKEN=
DROPBOX_APP_KEY=
DROPBOX_APP_SECRET=

# Anthropic Claude API (for LLM analysis)
ANTHROPIC_API_KEY=
```

---

## Collector-Specific Setup

### CFTC COT Collector
```python
# No configuration needed - works out of the box
from data_collectors.collectors.cftc_cot_collector import CFTCCOTCollector
collector = CFTCCOTCollector()
result = collector.collect()
```

### USDA FAS Collector
```python
# No configuration needed - works out of the box
from data_collectors.collectors.usda_fas_collector import USDATFASCollector
collector = USDATFASCollector()
result = collector.collect(data_type="export_sales")
```

### EIA Ethanol Collector
```python
import os
os.environ['EIA_API_KEY'] = 'your_key_here'

from data_collectors.collectors.eia_ethanol_collector import EIAEthanolCollector
collector = EIAEthanolCollector()
result = collector.collect()
```

### Drought Collector
```python
# No configuration needed - works out of the box
from data_collectors.collectors.drought_collector import DroughtCollector
collector = DroughtCollector()
result = collector.collect()
```

---

## Data Source Priority

For the HB Weekly Report, these are the priorities:

### Critical (Required)
1. **USDA FAS Export Sales** - Weekly export data (FREE)
2. **CFTC COT** - Positioning data (FREE)
3. **EIA Ethanol** - Corn demand proxy (FREE API KEY)

### Important
4. **USDA NASS** - Crop conditions (FREE API KEY)
5. **Drought Monitor** - Weather impact (FREE)
6. **USDA WASDE** - Supply/demand (FREE via FAS)

### Nice to Have
7. **MPOB** - Palm oil (FREE, scraping)
8. **Brazil/Argentina** - South America (FREE, some scraping)
9. **Eurostat** - EU data (FREE API KEY)

---

## Testing Credentials

After setting up credentials, test each collector:

```bash
# Test CFTC (no key needed)
python -m commodity_pipeline.data_collectors.collectors.cftc_cot_collector test

# Test USDA FAS (no key needed)
python -m commodity_pipeline.data_collectors.collectors.usda_fas_collector test

# Test EIA (requires EIA_API_KEY)
python -m commodity_pipeline.data_collectors.collectors.eia_ethanol_collector test

# Test Drought (no key needed)
python -m commodity_pipeline.data_collectors.collectors.drought_collector test
```

---

## Next Steps

1. **Register for required API keys:**
   - [ ] USDA NASS: https://quickstats.nass.usda.gov/api
   - [ ] EIA: https://www.eia.gov/opendata/register.php

2. **Add to environment:**
   - Copy `.env.template` to `.env`
   - Add your API keys

3. **Test collectors:**
   - Run test commands above
   - Verify data is flowing

4. **Configure scheduler:**
   - Set up Tuesday report generation
   - Configure data refresh timing

---

## Support

If you encounter issues:
1. Check that API keys are correctly set in environment
2. Verify network access to data sources
3. Check rate limits (some sources throttle requests)
4. Review collector logs for specific errors
