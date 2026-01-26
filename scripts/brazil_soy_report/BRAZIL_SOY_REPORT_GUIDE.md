# Brazil Soybean Weekly Report - Complete Guide

This guide walks you through running the Brazil Soybean Weekly Report pipeline from start to finish. **Assumes you're new to this** - every step is explained.

---

## Table of Contents

1. [Quick Start (TL;DR)](#quick-start)
2. [What This Pipeline Does](#what-this-pipeline-does)
3. [Directory Structure](#directory-structure)
4. [Step A: Environment Setup](#step-a-environment-setup)
5. [Step B: Database Setup](#step-b-database-setup)
6. [Step C: Collecting Raw Data](#step-c-collecting-raw-data)
7. [Step D: Running the Pipeline](#step-d-running-the-pipeline)
8. [Step E: Using the Output with LLM](#step-e-using-the-output-with-llm)
9. [Step F: Quality Assurance](#step-f-quality-assurance)
10. [Troubleshooting](#troubleshooting)
11. [Data Source Reference](#data-source-reference)

---

## Quick Start

If you're in a hurry, here's the minimal workflow:

```bash
# 1. Navigate to repo
cd /home/user/RLC-Agent

# 2. Activate virtual environment
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# 3. Put your raw data files in:
#    data/brazil_soy/raw/

# 4. Run the pipeline
python scripts/brazil_soy_report/run_brazil_soy_report.py

# 5. Find outputs in:
#    data/brazil_soy/output/
```

---

## What This Pipeline Does

**Input**: Raw data files (CSV, XLS) from your Dropbox folder

**Processing**:
1. Reads and validates raw files
2. Standardizes data formats
3. Calculates Wed-to-Wed price changes
4. Generates charts (PNG)
5. Creates JSON data pack for LLM

**Output**:
- `weekly_data_pack_YYYYMMDD.json` - All data for the LLM
- `llm_prompt_YYYYMMDD.txt` - Ready-to-use prompt
- `cepea_paranagua_YYYYMMDD.png` - Price chart
- `cbot_vs_cepea_YYYYMMDD.png` - Comparison chart
- `exports_ytd_YYYYMMDD.png` - Exports bar chart

---

## Directory Structure

```
RLC-Agent/
├── scripts/brazil_soy_report/          # Pipeline code
│   ├── brazil_soy_config.py            # Configuration
│   ├── ingest_brazil_soy_data.py       # Data loading
│   ├── weekly_data_pack.py             # JSON generator
│   ├── brazil_soy_charts.py            # Chart generator
│   ├── run_brazil_soy_report.py        # Master script
│   └── BRAZIL_SOY_REPORT_GUIDE.md      # This file
│
├── data/brazil_soy/                    # Data directory
│   ├── raw/                            # Put raw files here
│   ├── processed/                      # Cleaned data (auto-generated)
│   └── output/                         # Final outputs (auto-generated)
│
├── database/                           # Database schemas
│   └── sql/                            # PostgreSQL migrations
│
└── .env                                # Your API keys (copy from .env.example)
```

---

## Step A: Environment Setup

### A.1 Install Python Dependencies

```bash
cd /home/user/RLC-Agent

# Create virtual environment (if not exists)
python -m venv venv

# Activate it
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Additional packages for charts
pip install matplotlib openpyxl xlrd
```

### A.2 Verify Installation

```bash
python -c "import pandas; import matplotlib; print('OK')"
```

### A.3 Create Environment File

```bash
# Copy the example
cp .env.example .env

# Edit with your settings (database, API keys)
nano .env  # or use any editor
```

**Minimum .env settings for this pipeline**:
```env
# Database (optional - pipeline works without DB)
DATABASE_URL=postgresql://user:password@localhost:5432/rlc_commodities

# Directories
DATA_CACHE_DIR=./data/cache
REPORTS_OUTPUT_DIR=./data/reports
```

---

## Step B: Database Setup

> **Note**: The Brazil Soy Report can run WITHOUT a database. It reads directly from CSV/XLS files and outputs JSON. The database is for longer-term historical storage.

### B.1 Using PostgreSQL (Recommended for Production)

```bash
# 1. Install PostgreSQL
# Ubuntu: sudo apt install postgresql
# Mac: brew install postgresql

# 2. Create database
createdb rlc_commodities

# 3. Run migrations (in order)
psql -d rlc_commodities -f database/sql/00_init.sql
psql -d rlc_commodities -f database/sql/01_schemas.sql
psql -d rlc_commodities -f database/sql/02_core_dimensions.sql
psql -d rlc_commodities -f database/sql/03_audit_tables.sql
psql -d rlc_commodities -f database/sql/05_silver_observation.sql
psql -d rlc_commodities -f database/sql/06_gold_views.sql

# 4. Verify
psql -d rlc_commodities -c "\dt bronze.*"
```

### B.2 Using SQLite (Development/Testing)

No setup needed. The system auto-creates `data/rlc_commodities.db` when needed.

### B.3 Test Connection

```bash
python deployment/db_config.py
```

---

## Step C: Collecting Raw Data

### C.1 Where to Get Data

| Source | URL | What to Download |
|--------|-----|------------------|
| **CEPEA** | https://www.cepea.esalq.usp.br | Soja Paranaguá (daily XLS) |
| **IMEA** | https://www.imea.com.br | MT soy prices (weekly XLS) |
| **CONAB** | https://portaldeinformacoes.conab.gov.br | Monthly prices CSV |
| **CBOT** | https://www.barchart.com | Soy futures settlements CSV |
| **ANEC** | Manual from PDF | Create structured CSV |
| **NOAA** | https://www.cpc.ncep.noaa.gov | Save PNG, create signal CSV |

### C.2 File Naming Convention

**Format**: `YYYY-MM-DD__SOURCE__CONTENT__DETAIL.ext`

**Examples**:
```
2026-01-22__CEPEA__SOY_PARANAGUA_RS_SC_DAILY.xls
2026-01-22__CEPEA__USDBRL_MONTHLY.xls
2026-01-22__IMEA__MT_SOY_PRICES_RS_SC.xls
2026-01-22__CONAB__SOY_PRICES_BY_STATE_RS_SC.csv
2026-01-22__CBOT__SOY_FUTURES_SETTLEMENTS_BARCHART.csv
2026-01-22__ANEC__SOY_SHIPMENTS_WEEK04_2026.csv
2026-01-22__NOAA__BRAZIL_WEATHER_SIGNAL.csv
```

### C.3 Where to Put Files

```
data/brazil_soy/raw/
├── 2026-01-22__CEPEA__SOY_PARANAGUA_RS_SC_DAILY.xls
├── 2026-01-22__CBOT__SOY_FUTURES_SETTLEMENTS_BARCHART.csv
├── 2026-01-22__IMEA__MT_SOY_PRICES_RS_SC.xls
├── 2026-01-22__ANEC__SOY_SHIPMENTS_WEEK04_2026.csv
└── 2026-01-22__NOAA__BRAZIL_WEATHER_SIGNAL.csv
```

### C.4 Manual Data Templates

**ANEC Exports CSV** (create manually from PDF):
```csv
Week,Soy_MMT,Meal_MMT,YTD_2025,YTD_2026
W01,1.2,0.4,1.2,1.5
W02,1.5,0.5,2.7,3.0
W03,1.8,0.6,4.5,4.8
```

**NOAA Weather Signal CSV** (create manually):
```csv
Week,Signal,Notes
W04,neutral,Near-normal precipitation expected across key soy regions
```

### C.5 Check File Availability

```bash
python scripts/brazil_soy_report/run_brazil_soy_report.py --check-files
```

---

## Step D: Running the Pipeline

### D.1 Full Pipeline (Recommended)

```bash
cd /home/user/RLC-Agent
source venv/bin/activate

# Run everything
python scripts/brazil_soy_report/run_brazil_soy_report.py
```

**Output**:
```
============================================================
 BRAZIL SOYBEAN WEEKLY REPORT
============================================================
Report Week: Jan 15 - Jan 22, 2026
Start: 2026-01-15 (Wednesday)
End:   2026-01-22 (Wednesday)

[Step 1] Ingesting raw data files
----------------------------------------
  [OK] cepea_paranagua: 7 rows
  [OK] cbot_futures: 5 rows
  [WARN] imea_mt: [Data incomplete]
  [MISSING] anec_exports

[Step 2] Generating weekly data pack (JSON)
----------------------------------------
  Sources loaded: 2
  Sources missing: 4

[Step 3] Generating charts (PNG)
----------------------------------------
  Charts created: 1/3

------------------------------------------------------------
 OUTPUT FILES
------------------------------------------------------------
  weekly_data_pack_20260122.json (12.3 KB)
  llm_prompt_20260122.txt (1.8 KB)
  cepea_paranagua_20260122.png (45.2 KB)
```

### D.2 Pipeline Options

```bash
# Dry run (validate only, no outputs)
python scripts/brazil_soy_report/run_brazil_soy_report.py --dry-run

# Skip charts (faster)
python scripts/brazil_soy_report/run_brazil_soy_report.py --no-charts

# Specific date
python scripts/brazil_soy_report/run_brazil_soy_report.py --date 2026-01-15
```

### D.3 Run Individual Steps

```bash
# Just ingestion
python scripts/brazil_soy_report/ingest_brazil_soy_data.py

# Just data pack
python scripts/brazil_soy_report/weekly_data_pack.py

# Just charts
python scripts/brazil_soy_report/brazil_soy_charts.py
```

---

## Step E: Using the Output with LLM

### E.1 The Data Pack

Open `data/brazil_soy/output/weekly_data_pack_YYYYMMDD.json`:

```json
{
  "metadata": {
    "report_type": "Brazil Soybean Weekly Report",
    "report_week": "Jan 15 - Jan 22, 2026",
    "sources_loaded": ["cepea_paranagua", "cbot_futures"],
    "sources_missing": ["imea_mt", "anec_exports"]
  },
  "price_snapshot": {
    "prices": {
      "cepea_paranagua": {
        "weekly_change": {
          "start_value": 142.50,
          "end_value": 145.20,
          "change": 2.70,
          "change_pct": 1.89,
          "direction": "up"
        }
      }
    }
  },
  "llm_prompt": "You are writing the Brazil Soybean Weekly Report..."
}
```

### E.2 Feed to LLM

**Option A: Use the prompt file directly**

```bash
cat data/brazil_soy/output/llm_prompt_20260122.txt
```

Copy this prompt to Claude, ChatGPT, or your local Ollama instance.

**Option B: Programmatic (Python)**

```python
import json
from pathlib import Path

# Load data pack
with open("data/brazil_soy/output/weekly_data_pack_20260122.json") as f:
    data = json.load(f)

# Get the prompt
prompt = data['llm_prompt']

# Send to your LLM
# Example with Anthropic:
import anthropic
client = anthropic.Anthropic()
response = client.messages.create(
    model="claude-sonnet-4-20250514",
    max_tokens=1024,
    messages=[{"role": "user", "content": prompt}]
)
print(response.content[0].text)
```

**Option C: Use existing RLC report infrastructure**

```bash
python -m src.main report --weekly --template brazil_soy
```

### E.3 Assemble Final Report

1. **Get LLM narrative** from the prompt
2. **Insert charts** (PNG files) into your template
3. **Export to PDF** using Word, Google Docs, or Python

---

## Step F: Quality Assurance

### F.1 Validation Checklist

Before distributing the report:

- [ ] **Price values** match source files
- [ ] **Units** are correct (BRL/60kg, USc/bu, BRL/sc)
- [ ] **Week dates** are correct (Wednesday to Wednesday)
- [ ] **YoY comparisons** use correct year labels
- [ ] **Charts** render properly with correct scales
- [ ] **No "placeholder" or "TBD"** text in final output
- [ ] **LLM text** doesn't invent numbers not in data pack

### F.2 Common Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| "No data loaded" | Files not in raw/ folder | Check file location and naming |
| Wrong week dates | Running on wrong day | Use `--date` to specify |
| Missing prices | CEPEA file format changed | Check column names in XLS |
| Charts blank | No data in date range | Verify data covers report week |

### F.3 Automated Validation

The pipeline runs these checks automatically:
- Price bounds (e.g., CEPEA 50-300 BRL)
- Large daily changes (>10% flagged)
- Duplicate dates
- Missing required columns

---

## Troubleshooting

### "ModuleNotFoundError: No module named 'pandas'"

```bash
pip install pandas openpyxl xlrd
```

### "No files found for source X"

1. Check files are in `data/brazil_soy/raw/`
2. Verify file naming matches pattern
3. Run `--check-files` to diagnose

### "matplotlib not available"

```bash
pip install matplotlib
```

### Database connection errors

The Brazil Soy Report doesn't require a database. If you're seeing DB errors, they're from other parts of the system.

### Wrong Wednesday dates

The pipeline calculates the most recent Wednesday. To target a specific week:

```bash
python scripts/brazil_soy_report/run_brazil_soy_report.py --date 2026-01-15
```

---

## Data Source Reference

### CEPEA Paranaguá (Daily Cash)

- **URL**: https://www.cepea.esalq.usp.br/br/indicador/soja.aspx
- **Update**: Daily (business days)
- **Use**: "À vista R$" column (cash price)
- **Ignore**: "À prazo" (forward prices)
- **Format**: XLS with date and price columns

### IMEA Mato Grosso (Weekly)

- **URL**: https://www.imea.com.br/imea-site/indicador-soja
- **Update**: Weekly
- **Use**: "Preço soja disponível compra" (spot price)
- **Ignore**: Parity indicators, contract prices
- **Format**: XLS

### CONAB Prices (Monthly)

- **URL**: https://portaldeinformacoes.conab.gov.br
- **Update**: Monthly
- **Use**: MT column for MVP
- **Note**: Context only, not for weekly change

### CBOT Futures (Daily)

- **URL**: https://www.barchart.com/futures/quotes/ZS*0
- **Download**: Settlements CSV
- **Use**: Settle price, nearby contract
- **Format**: Standard Barchart CSV

### ANEC Exports (Weekly)

- **URL**: https://www.anec.com.br (PDF reports)
- **Update**: Weekly
- **Process**: Manually extract to CSV
- **Key data**: Weekly MMT, YTD comparisons

### NOAA Weather (Weekly)

- **URL**: https://www.cpc.ncep.noaa.gov
- **Download**: Brazil 7-day precipitation map (PNG)
- **Process**: Create CSV with signal (dry/neutral/wet)

---

## Summary

**Weekly Workflow**:

1. **Wednesday evening**: Download raw files from sources
2. **Place files** in `data/brazil_soy/raw/` with correct naming
3. **Run pipeline**: `python scripts/brazil_soy_report/run_brazil_soy_report.py`
4. **Review outputs** in `data/brazil_soy/output/`
5. **Feed prompt to LLM** to generate narrative
6. **Assemble final report** with text + charts
7. **QA check** before distribution

**Files created by this setup**:
- `brazil_soy_config.py` - Configuration
- `ingest_brazil_soy_data.py` - Data loader
- `weekly_data_pack.py` - JSON generator
- `brazil_soy_charts.py` - Chart generator
- `run_brazil_soy_report.py` - Master script

---

*Last updated: January 2026*
