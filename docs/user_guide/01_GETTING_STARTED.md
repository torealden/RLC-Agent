# Part 1: Getting Started

[← Back to Table of Contents](00_COVER_AND_TOC.md)

---

## 1.1 Platform Overview

The RLC Commodities Intelligence Platform is an automated system for collecting, processing, and analyzing agricultural commodity market data. It provides:

- **Automated Data Collection** — Scheduled collection from 25+ government and market data sources
- **Unified Database** — All data standardized into a single PostgreSQL database
- **Quality Monitoring** — Real-time dashboards showing data freshness and collection status
- **Analysis Tools** — Power BI integration and LLM-assisted analysis
- **Report Generation** — Automated weekly market reports

### What Data Is Available?

| Category | Sources | Update Frequency |
|----------|---------|------------------|
| US Supply & Demand | USDA WASDE, NASS, ERS | Weekly/Monthly |
| Trade Flows | Census Bureau, USDA FAS | Weekly/Monthly |
| Prices | CME, USDA AMS | Daily |
| Positioning | CFTC COT Reports | Weekly (Friday) |
| Energy | EIA Ethanol, Petroleum | Weekly |
| Weather | NOAA, Drought Monitor | Daily/Weekly |
| South America | CONAB, IMEA, ABIOVE | Monthly |
| Global | FAOSTAT, MPOB | Monthly |

### Key Commodities Tracked

**Grains:** Corn, Wheat (HRW, SRW, HRS), Soybeans, Sorghum, Barley, Oats

**Oilseeds & Products:** Soybeans, Soybean Meal, Soybean Oil, Canola, Sunflower, Palm Oil

**Energy:** Ethanol, Biodiesel, Crude Oil, Natural Gas

**Other:** Cotton, Rice, Sugar, Livestock (Cattle, Hogs)

---

## 1.2 System Architecture

The platform follows a **medallion architecture** with three data layers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                    │
│  USDA WASDE │ USDA NASS │ Census │ CFTC │ EIA │ CME │ CONAB │ Weather       │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            COLLECTORS                                        │
│  Python agents that fetch data from APIs, handle rate limits, log requests  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER (Raw)                                   │
│  • Data stored exactly as received from source                               │
│  • Full audit trail preserved                                                │
│  • Enables reprocessing if needed                                            │
│  Tables: bronze.wasde_cell, bronze.census_trade_raw, bronze.cftc_raw, etc.  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SILVER LAYER (Standardized)                           │
│  • Consistent format: (series_id, observation_time, value)                   │
│  • Units normalized, data cleaned                                            │
│  • Quality flags applied                                                     │
│  Tables: silver.observation, silver.trade_flow, silver.price                │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         GOLD LAYER (Analysis-Ready)                          │
│  • Pre-built views for common analyses                                       │
│  • Aggregations, calculations, pivots                                        │
│  • Optimized for Power BI and reporting                                      │
│  Views: gold.us_corn_balance_sheet, gold.trade_summary, gold.wasde_changes  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
              ┌──────────┐  ┌──────────┐  ┌──────────────┐
              │ Power BI │  │  Reports │  │ LLM Analysis │
              └──────────┘  └──────────┘  └──────────────┘
```

**[GRAPHIC: System Architecture Diagram]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#architecture-diagram) for graphic specifications*

### Why This Architecture?

| Layer | Purpose | Who Uses It |
|-------|---------|-------------|
| Bronze | Preserve raw data for audit/reprocessing | System only |
| Silver | Single source of truth for analysis | Developers, LLM |
| Gold | Business-ready views, Excel-compatible | Analysts, Power BI |

💡 **Key Principle:** Data flows one direction (Bronze → Silver → Gold). Analysts never need to touch Bronze tables.

---

## 1.3 Installation

### Prerequisites

Before starting, ensure you have:

| Requirement | Version | How to Check |
|-------------|---------|--------------|
| Python | 3.9 or higher | `python --version` |
| Git | Any recent version | `git --version` |
| PostgreSQL client | Optional (for direct queries) | `psql --version` |
| Power BI Desktop | Latest | Windows only |

### Step 1: Clone the Repository

```bash
git clone https://github.com/your-org/RLC-Agent.git
cd RLC-Agent
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

This installs all required packages including:
- `pandas`, `numpy` — Data processing
- `sqlalchemy`, `psycopg2-binary` — Database connectivity
- `streamlit` — Operations dashboard
- `python-dotenv` — Configuration management
- `requests`, `aiohttp` — API clients

### Step 3: Obtain Required Files

Certain files contain credentials and are not stored in Git. You must obtain these from an existing team member:

| File | Location | Purpose |
|------|----------|---------|
| `.env` | Repository root | Database connection, API keys |
| `.env` | `dashboards/ops/` | Dashboard database connection |
| `credentials.json` | Repository root | Google API OAuth (if using email/calendar) |

See [Appendix A](APPENDIX_A_FILE_LIST.md) for the complete file list.

### Step 4: Verify Installation

```bash
# Test Python imports
python -c "import pandas; import sqlalchemy; import streamlit; print('All packages OK')"

# Test database connection (after configuration)
python -c "from dashboards.ops.db import get_connection; print('Database OK')"
```

---

## 1.4 Configuration

### Environment File Setup

The platform uses `.env` files to store configuration. These files are **not** stored in Git for security reasons.

#### Main Configuration File

Create `C:\dev\RLC-Agent\.env` with the following content:

```ini
# =============================================================================
# RLC PLATFORM CONFIGURATION
# =============================================================================

# -----------------------------------------------------------------------------
# DATABASE CONNECTION (Required)
# -----------------------------------------------------------------------------
RLC_PG_HOST=your-database-host.com
RLC_PG_PORT=5432
RLC_PG_DATABASE=rlc_commodities
RLC_PG_USER=your_username
RLC_PG_PASSWORD=your_password

# Alternate format (some scripts use this)
DATABASE_URL=postgresql://your_username:your_password@your-database-host.com:5432/rlc_commodities

# -----------------------------------------------------------------------------
# API KEYS (Required for data collection)
# -----------------------------------------------------------------------------
# USDA NASS QuickStats
NASS_API_KEY=your_nass_key

# US Energy Information Administration
EIA_API_KEY=your_eia_key

# US Census Bureau
CENSUS_API_KEY=your_census_key

# -----------------------------------------------------------------------------
# OPTIONAL SERVICES
# -----------------------------------------------------------------------------
# Tavily (web search for LLM)
TAVILY_API_KEY=your_tavily_key

# Notion (knowledge base)
NOTION_API_KEY=your_notion_key

# Dropbox (report distribution)
DROPBOX_ACCESS_TOKEN=your_dropbox_token
```

⚠️ **Security Warning:** Never commit `.env` files to Git. They contain sensitive credentials.

#### Dashboard Configuration File

Create `C:\dev\RLC-Agent\dashboards\ops\.env`:

```ini
# Dashboard Database Connection
RLC_PG_HOST=your-database-host.com
RLC_PG_PORT=5432
RLC_PG_DATABASE=rlc_commodities
RLC_PG_USER=your_username
RLC_PG_PASSWORD=your_password
```

💡 **Tip:** Copy the database settings from the main `.env` file.

### Obtaining API Keys

| Service | Registration URL | Notes |
|---------|-----------------|-------|
| USDA NASS | https://quickstats.nass.usda.gov/api | Free, instant |
| EIA | https://www.eia.gov/opendata/register.php | Free, instant |
| Census | https://api.census.gov/data/key_signup.html | Free, instant |
| Tavily | https://tavily.com | Free tier available |

See [Appendix B](APPENDIX_B_API_KEYS.md) for detailed registration instructions.

---

## 1.5 Verifying Your Setup

After configuration, run these verification steps:

### Test 1: Database Connection

```bash
cd C:\dev\RLC-Agent
python -c "
from dashboards.ops.db import get_connection
with get_connection() as conn:
    cur = conn.cursor()
    cur.execute('SELECT 1')
    print('Database connection: OK')
"
```

**Expected output:** `Database connection: OK`

### Test 2: Query Data

```bash
python -c "
from dashboards.ops.db import query_df
df = query_df('SELECT COUNT(*) as n FROM silver.observation')
print(f'Observations in database: {df.iloc[0][\"n\"]:,}')
"
```

**Expected output:** A count of observations (e.g., `Observations in database: 1,234,567`)

### Test 3: Launch Operations Dashboard

```bash
streamlit run dashboards/ops/app.py
```

Or double-click: `scripts\launch_data_dashboard.bat`

**Expected result:** Browser opens with the Operations Dashboard showing system health.

**[GRAPHIC: Operations Dashboard Screenshot]**
*See [Appendix E](APPENDIX_E_GRAPHICS.md#dashboard-screenshot) for graphic specifications*

### Test 4: Verify Power BI Connection

1. Open Power BI Desktop
2. Click **Get Data** > **PostgreSQL database**
3. Enter your database host and credentials
4. Browse to `gold` schema
5. Preview `us_corn_balance_sheet` view

**Expected result:** You should see corn supply/demand data in the preview.

---

## Troubleshooting Installation

| Problem | Cause | Solution |
|---------|-------|----------|
| `ModuleNotFoundError` | Package not installed | Run `pip install -r requirements.txt` |
| `Connection refused` | Database not reachable | Check `RLC_PG_HOST` in `.env`, verify network |
| `Authentication failed` | Wrong credentials | Verify `RLC_PG_USER` and `RLC_PG_PASSWORD` |
| `Relation does not exist` | Database not initialized | Contact admin to verify schema exists |
| Dashboard shows errors | Missing `.env` in dashboards/ops | Copy `.env` file to `dashboards/ops/` |

---

## Next Steps

Now that your system is set up:

1. **Explore the data** → [Part 2: Understanding the Data](02_UNDERSTANDING_DATA.md)
2. **Monitor collections** → [Part 3: Daily Operations](03_DAILY_OPERATIONS.md)
3. **Build dashboards** → [Part 4: Working with Power BI](04_POWER_BI.md)

---

[← Back to Table of Contents](00_COVER_AND_TOC.md) | [Next: Understanding the Data →](02_UNDERSTANDING_DATA.md)
