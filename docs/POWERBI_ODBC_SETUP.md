# Power BI ODBC Direct Connection Guide
## SQLite Today, PostgreSQL Tomorrow

---

## Overview

This guide establishes a **live ODBC connection** between Power BI and your RLC database. Unlike Excel/CSV exports, this approach:

- **Real-time data**: No manual export/import cycle
- **Automatic refresh**: Schedule updates in Power BI Service
- **Single source of truth**: Everyone sees the same data
- **Scalable**: Same process works for PostgreSQL migration

---

## Part 1: SQLite ODBC Setup (Current)

### Step 1: Download SQLite ODBC Driver

1. Go to: http://www.ch-werner.de/sqliteodbc/
2. Download the appropriate version:
   - **64-bit Windows**: `sqliteodbc_w64.exe`
   - **32-bit Windows**: `sqliteodbc.exe`

   > **Important**: Match your Power BI Desktop architecture. Most modern installs are 64-bit.

3. Run the installer with default settings

### Step 2: Create the ODBC Data Source (DSN)

#### Open ODBC Administrator

1. Press `Windows + R`
2. Type:
   - For 64-bit: `odbcad64.exe`
   - For 32-bit: `odbcad32.exe`
3. Press Enter

#### Create System DSN

1. Click the **System DSN** tab (not User DSN - System DSN works for all users)
2. Click **Add...**
3. Select **SQLite3 ODBC Driver** from the list
4. Click **Finish**

#### Configure the DSN

```
Data Source Name:    RLC_Commodities
Description:         RLC Commodity Balance Sheet Database
Database Name:       [Browse to your database file]
                     Example: C:\Users\YourName\RLC-Agent\data\rlc_commodities.db
```

**Advanced Settings** (click the button):
```
☑ Load Extension         (leave unchecked unless needed)
☑ No TXN                  (check this - improves read performance)
☐ Step API                (leave unchecked)
☐ No WCHAR               (leave unchecked)
☐ OEMCP                  (leave unchecked)
☐ Long Names             (leave unchecked)
Sync Mode:              NORMAL
```

5. Click **OK** to save

### Step 3: Test the Connection

1. In ODBC Administrator, select your new DSN
2. Click **Configure**
3. Click **Test** (if available) or proceed to Power BI

---

## Part 2: Connect Power BI to ODBC

### Step 1: Open Power BI Desktop

1. Launch **Power BI Desktop**
2. Click **Get Data** (Home ribbon)
3. Search for **ODBC**
4. Select **ODBC** and click **Connect**

### Step 2: Select Your DSN

1. In the dropdown, select: `RLC_Commodities`
2. Leave **Advanced options** empty for now
3. Click **OK**

### Step 3: Authenticate

1. Select **Default or Custom** in the left panel
2. Leave username/password blank (SQLite doesn't require auth)
3. Click **Connect**

### Step 4: Select Tables

You'll see a Navigator with available tables:

| Table | Description | Records |
|-------|-------------|---------|
| `commodity_balance_sheets` | Main data: S&D by country/year | 104,768 |
| `forecasts` | Your recorded predictions | Growing |
| `actuals` | Reported actual values | Growing |
| `forecast_actual_pairs` | Matched forecast/actual with errors | Growing |
| `accuracy_metrics` | Computed accuracy statistics | Growing |

**Recommended**: Select `commodity_balance_sheets` first.

4. Click **Load** (or **Transform Data** if you want to clean first)

---

## Part 3: Data Model Setup in Power BI

### Create a Date Table

Power BI works best with a proper date table. Create one:

1. Go to **Modeling** → **New Table**
2. Enter this DAX:

```dax
DateTable =
ADDCOLUMNS(
    CALENDAR(DATE(1960, 1, 1), DATE(2030, 12, 31)),
    "Year", YEAR([Date]),
    "Month", MONTH([Date]),
    "MonthName", FORMAT([Date], "MMMM"),
    "Quarter", "Q" & QUARTER([Date]),
    "MarketingYear",
        IF(MONTH([Date]) >= 9,
            YEAR([Date]) & "/" & RIGHT(YEAR([Date]) + 1, 2),
            YEAR([Date]) - 1 & "/" & RIGHT(YEAR([Date]), 2)
        )
)
```

### Create Relationships

1. Go to **Model** view (left sidebar)
2. Create relationships:
   - `DateTable[MarketingYear]` → `commodity_balance_sheets[marketing_year]`

### Create Key Measures

Add these measures for analysis:

```dax
// Total value (generic aggregation)
Total Value = SUM(commodity_balance_sheets[value])

// Production total
Production =
CALCULATE(
    SUM(commodity_balance_sheets[value]),
    commodity_balance_sheets[metric] = "Production"
)

// Ending Stocks
Ending Stocks =
CALCULATE(
    SUM(commodity_balance_sheets[value]),
    commodity_balance_sheets[metric] = "Ending Stocks"
)

// Stocks-to-Use Ratio (%)
Stocks to Use Ratio =
DIVIDE(
    [Ending Stocks],
    [Total Disappearance],
    0
) * 100

// Year-over-Year Change
YoY Change =
VAR CurrentYear = SELECTEDVALUE(commodity_balance_sheets[marketing_year])
VAR PriorYear = ... // requires parsing marketing_year
RETURN
    [Production] - [Prior Year Production]
```

---

## Part 4: PostgreSQL Migration Path (Future)

When you're ready to scale to PostgreSQL:

### Step 1: Install PostgreSQL ODBC Driver

1. Download from: https://www.postgresql.org/ftp/odbc/versions/msi/
2. Install **psqlodbc_x64.msi** (64-bit)

### Step 2: Create PostgreSQL DSN

In ODBC Administrator:

```
Data Source:     RLC_Commodities_PG
Description:     RLC PostgreSQL Database
Database:        rlc_commodities
Server:          localhost (or your server IP)
Port:            5432
User Name:       rlc_user
Password:        [your password]
SSL Mode:        prefer (or require for production)
```

### Step 3: Update Power BI Connection

1. In Power BI, go to **Home** → **Transform Data**
2. Click **Data source settings**
3. Select the old SQLite connection
4. Click **Change Source**
5. Select the new PostgreSQL DSN

All your reports and measures will automatically use the new source!

### Data Migration Script

When ready, use this to migrate SQLite → PostgreSQL:

```python
# deployment/migrate_to_postgres.py
import sqlite3
import psycopg2
import pandas as pd

# Connect to both databases
sqlite_conn = sqlite3.connect('data/rlc_commodities.db')
pg_conn = psycopg2.connect(
    host="localhost",
    database="rlc_commodities",
    user="rlc_user",
    password="your_password"
)

# Migrate each table
tables = ['commodity_balance_sheets', 'forecasts', 'actuals',
          'forecast_actual_pairs', 'accuracy_metrics']

for table in tables:
    df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
    df.to_sql(table, pg_conn, if_exists='replace', index=False)
    print(f"Migrated {table}: {len(df):,} rows")
```

---

## Part 5: Visualization Design Philosophy

### The E=MC² Principle

> "Everything should be made as simple as possible, but not simpler." — Einstein

The most powerful visualizations share these traits:

1. **One clear message per visual** - What should the viewer conclude?
2. **Minimal ink, maximum data** - Remove every non-essential element
3. **Preattentive attributes** - Use color, size, position strategically
4. **Guided attention** - Lead the eye to the insight

---

## Part 6: Signature Visualizations for RLC

### Visualization 1: The Balance Sheet Waterfall

**Purpose**: Show how supply flows to demand in one glance.

```
SUPPLY                          DEMAND
═══════                         ═══════

┌──────────┐
│Beginning │
│ Stocks   │───────────────────→ Crush ────────────┐
│  45 MMT  │                      82 MMT           │
└──────────┘                                       │
     ↓                                             │
┌──────────┐                    Food/Feed ─────────┤
│Production│───────────────────→   35 MMT          │
│ 155 MMT  │                                       │
└──────────┘                    Exports ───────────┤
     ↓                            62 MMT           │
┌──────────┐                                       │
│ Imports  │                    Ending ────────────┘
│   2 MMT  │───────────────────→ Stocks
└──────────┘                      23 MMT
═══════════                     ═══════════
Total: 202                      Total: 202
```

**Power BI Implementation**:

1. Use the **Waterfall Chart** visual
2. Category: Create a calculated column with ordered supply/demand items
3. Y-axis: Value
4. Configure:
   - Starting value: Beginning Stocks
   - Increases: Production, Imports
   - Decreases: Crush, Food/Feed, Exports
   - End value: Ending Stocks

**DAX for ordered waterfall**:
```dax
Waterfall Order =
SWITCH(
    commodity_balance_sheets[metric],
    "Beginning Stocks", 1,
    "Production", 2,
    "Imports", 3,
    "Total Supply", 4,
    "Crush", 5,
    "Food", 6,
    "Feed", 7,
    "Exports", 8,
    "Ending Stocks", 9,
    99
)
```

---

### Visualization 2: The Stocks-to-Use Heatmap

**Purpose**: Instantly see tight vs. comfortable supply across all markets.

```
           2020/21   2021/22   2022/23   2023/24   2024/25
          ─────────────────────────────────────────────────
US        ██████████ ████████  ██████    ████      ██████
Brazil    ████████   ██████████████████  ████████  ████████
Argentina ████       ██████    ████████  ██████████████████
EU        ██████     ████      ██        ████      ██████
China     ██████████████████████████████████████████████████

Legend: █ = Higher S/U (comfortable)    = Lower S/U (tight)
```

**Power BI Implementation**:

1. Use **Matrix** visual
2. Rows: Country
3. Columns: Marketing Year
4. Values: Stocks-to-Use Ratio (your measure)
5. Apply **Conditional Formatting**:
   - Background color → Rules:
     - < 10%: Deep Red (#B71C1C)
     - 10-15%: Orange (#E65100)
     - 15-20%: Yellow (#F9A825)
     - 20-30%: Light Green (#7CB342)
     - > 30%: Deep Green (#2E7D32)

**Why it works**: Your eye immediately spots supply stress without reading numbers.

---

### Visualization 3: The Price-Fundamentals Scatter

**Purpose**: Reveal the relationship between stocks and prices.

```
Price
($/bu)
  │
18├                              ●2012 (drought)
  │
16├                    ●2021
  │
14├              ●2022    ●2023
  │        ●2019
12├    ●2018         ●2020          ●2024
  │●2017
10├──────────────────────────────────────────────
  │
  └────────────────────────────────────────────── S/U Ratio
        5%    10%    15%    20%    25%    30%

  Insight: Prices rise exponentially as S/U drops below 10%
```

**Power BI Implementation**:

1. Use **Scatter Chart**
2. X-axis: Stocks-to-Use Ratio
3. Y-axis: Average Price
4. Details: Marketing Year
5. Add a **Trend Line** (Analytics pane)
6. Consider adding a **Reference Line** at 10% S/U

**Enhancement**: Add a third dimension with bubble size = Production volume

---

### Visualization 4: The 5-Year Range Chart

**Purpose**: Show current values in historical context.

```
                    Production (MMT)
    │
140 ├─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ 5-yr max
    │           ╭───────────╮
130 ├         ╱              ╲
    │  ████████                ████████████  Current Year
120 ├████                              ████
    │                                      ████
110 ├─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ 5-yr min
    │
    └─────────────────────────────────────────────────
       Sep  Oct  Nov  Dec  Jan  Feb  Mar  Apr  May
```

**Power BI Implementation**:

1. Create measures for 5-yr min, max, and average:
```dax
5Yr Max =
CALCULATE(
    MAX(commodity_balance_sheets[value]),
    FILTER(
        ALL(commodity_balance_sheets),
        commodity_balance_sheets[marketing_year] >= YEAR(TODAY()) - 5
    )
)
```

2. Use **Area Chart** with:
   - Band between min and max (shaded)
   - Line for 5-year average
   - Bold line for current year

---

### Visualization 5: Crush Economics Dashboard

**Purpose**: Show profitability at a glance for management decisions.

```
┌─────────────────────────────────────────────────────────────────┐
│                    SOYBEAN CRUSH ECONOMICS                       │
│                         December 2024                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐     │
│   │  SOYBEANS   │      │  SOY MEAL   │      │  SOY OIL    │     │
│   │             │      │             │      │             │     │
│   │   $10.25    │  →   │   $295.50   │  +   │   $0.4250   │     │
│   │   per bu    │      │   per ton   │      │   per lb    │     │
│   │             │      │             │      │             │     │
│   │   ▼ 2.1%    │      │   ▲ 1.5%    │      │   ▲ 3.2%    │     │
│   └─────────────┘      └─────────────┘      └─────────────┘     │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   GROSS PROCESSING VALUE (GPV)              CRUSH MARGIN        │
│   ═══════════════════════════               ════════════        │
│                                                                  │
│   Meal Value:  $295.50 × 0.0225 = $ 6.65                        │
│   Oil Value:   $0.4250 × 11.5   = $ 4.89    ┌───────────┐       │
│   ─────────────────────────────────────     │           │       │
│   GPV (per bushel):              $11.54     │   $1.29   │       │
│   Soybean Cost:                  $10.25     │  per bu   │       │
│   ─────────────────────────────────────     │           │       │
│   GROSS MARGIN:                  $ 1.29     │   ▲ 12%   │       │
│                                             └───────────┘       │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   MARGIN HISTORY (Last 12 Months)                               │
│   ─────────────────────────────────────────────────────         │
│     $2.00 ┤          ╭╮                                         │
│           │    ╭────╯ ╰╮                                        │
│     $1.50 ┤╭──╯        ╰──╮     ╭──╮                            │
│           │                ╰───╯    ╰──╮                        │
│     $1.00 ┤                            ╰────●                   │
│           │                                                      │
│     $0.50 ┤─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ Breakeven             │
│           └─────────────────────────────────────────            │
│           Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Power BI Implementation**:

1. Create DAX measures for each component:
```dax
Meal Value Per Bushel =
[Soy Meal Price] * 0.0225  // tons of meal per bushel

Oil Value Per Bushel =
[Soy Oil Price] * 11.5     // lbs of oil per bushel

GPV = [Meal Value Per Bushel] + [Oil Value Per Bushel]

Crush Margin = [GPV] - [Soybean Price]
```

2. Use **Card** visuals for the key numbers
3. Use **Line Chart** for margin history
4. Add a **Constant Line** at $0.50 for breakeven

---

### Visualization 6: Biofuel Credit Profitability

**Purpose**: Track RIN/LCFS credit values against biodiesel economics.

```
┌─────────────────────────────────────────────────────────────────┐
│                BIODIESEL ECONOMICS + CREDITS                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  COST STRUCTURE                    │    CREDIT VALUES           │
│  ════════════════                  │    ═════════════           │
│                                    │                            │
│  ┌─────────────────────────┐       │    D4 RIN      $1.05      │
│  │█████████████████████████│       │    ━━━━━━      ▲ 5%       │
│  │       Feedstock         │       │                            │
│  │         75%             │       │    LCFS       $125.00     │
│  ├─────────────────────────┤       │    ━━━━       ▼ 2%        │
│  │████████                 │       │                            │
│  │ Processing              │       │    BTC         $1.00      │
│  │    20%                  │       │    ━━━        (expires)   │
│  ├─────────────────────────┤       │                            │
│  │███                      │       ├────────────────────────────│
│  │Other                    │       │                            │
│  │ 5%                      │       │    TOTAL CREDIT VALUE     │
│  └─────────────────────────┘       │    ══════════════════     │
│                                    │       $2.30 / gal         │
│                                    │                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  NET MARGIN = Biodiesel Price + Credits - Costs                 │
│             = $3.50 + $2.30 - $4.80 = $1.00 / gallon            │
│                                                                  │
│  ════════════════════════════════════════════════════════════   │
│                                                                  │
│  [        MARGIN WATERFALL CHART SHOWING BUILD-UP         ]     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

### Visualization 7: Global Trade Flow Map

**Purpose**: Visualize export flows from origins to destinations.

**Power BI Implementation**:

1. Use the **Map** or **Filled Map** visual
2. Or use the **Flow Map** custom visual from AppSource
3. Data structure needed:
   - Origin (lat/long or country)
   - Destination (lat/long or country)
   - Trade volume (line thickness)

```dax
// Create a trade flows table
TradeFlows =
SUMMARIZE(
    commodity_balance_sheets,
    commodity_balance_sheets[country],
    "Exports", CALCULATE(SUM([value]), [metric] = "Exports"),
    "Imports", CALCULATE(SUM([value]), [metric] = "Imports")
)
```

---

## Part 7: Design Principles Checklist

Before publishing any visual, ask:

### Clarity
- [ ] Can someone understand the message in 5 seconds?
- [ ] Is the title a complete sentence describing the insight?
- [ ] Are axes clearly labeled with units?

### Simplicity
- [ ] Have I removed all non-essential gridlines?
- [ ] Are there any chart elements that don't add information?
- [ ] Is the color palette limited (2-3 colors max)?

### Accuracy
- [ ] Does the visual start at zero (for bar charts)?
- [ ] Are comparisons fair (same scale, same time period)?
- [ ] Is the data source and date clearly noted?

### Impact
- [ ] Does color draw attention to the key insight?
- [ ] Is the most important number the largest?
- [ ] Would this work in black and white (for printing)?

---

## Part 8: RLC Color Palette

Use these colors consistently across all visuals:

| Usage | Color | Hex | When to Use |
|-------|-------|-----|-------------|
| Primary | Navy Blue | `#1A365D` | Main data series, titles |
| Secondary | Steel Blue | `#4A6FA5` | Secondary data series |
| Accent | Gold | `#C6963C` | Highlights, key values |
| Positive | Forest Green | `#2D5016` | Gains, surpluses, good news |
| Negative | Crimson | `#8B1A1A` | Losses, deficits, concerns |
| Neutral | Slate Gray | `#64748B` | Gridlines, secondary text |
| Background | Off-White | `#F8FAFC` | Dashboard background |

### Creating the Theme in Power BI

1. Go to **View** → **Themes** → **Customize current theme**
2. Set colors as above
3. Save as **RLC_Theme.json**

Or create a theme file directly:

```json
{
  "name": "RLC Commodities",
  "dataColors": [
    "#1A365D",
    "#4A6FA5",
    "#C6963C",
    "#2D5016",
    "#8B1A1A",
    "#64748B"
  ],
  "background": "#F8FAFC",
  "foreground": "#1A365D",
  "tableAccent": "#C6963C"
}
```

Save as `RLC_Theme.json` and import via View → Themes → Browse for themes.

---

## Part 9: Scheduled Refresh Setup

Once your reports are built:

### Power BI Service Setup

1. **Publish** your report to Power BI Service
2. Go to your workspace
3. Click the **dataset** (not the report)
4. Click **Settings** (gear icon)
5. Expand **Scheduled refresh**

### For ODBC/On-Premise Data

You need the **Power BI Gateway**:

1. Download from: https://powerbi.microsoft.com/gateway/
2. Install on a machine that has:
   - Access to your SQLite database (or PostgreSQL later)
   - Always-on connection
3. Sign in with your Power BI account
4. Register your data source

### Refresh Schedule

- Set refresh frequency (e.g., daily at 6 AM)
- Power BI Service will pull fresh data automatically

---

## Troubleshooting

### "Driver not found" in Power BI
- Ensure 64-bit driver for 64-bit Power BI
- Restart Power BI after installing driver

### "File in use" error
- SQLite allows only one writer at a time
- Stop any Python scripts accessing the database
- Or use PostgreSQL (supports concurrent access)

### "Connection timeout"
- For large queries, increase ODBC timeout
- Consider creating summary views in SQLite

### Data not refreshing
- Check Gateway status in Power BI Service
- Verify credentials haven't expired
- Check network connectivity to database

---

## Quick Reference

### ODBC Connection String (Advanced)
```
Driver={SQLite3 ODBC Driver};Database=C:\path\to\rlc_commodities.db;
```

### PostgreSQL Connection String (Future)
```
Driver={PostgreSQL ODBC Driver(UNICODE)};Server=localhost;Port=5432;Database=rlc_commodities;Uid=rlc_user;Pwd=password;
```

### Essential DAX Functions
| Function | Purpose |
|----------|---------|
| `CALCULATE()` | Filter context modification |
| `DIVIDE()` | Safe division (handles /0) |
| `SWITCH()` | Conditional logic |
| `SUMX()` | Row-by-row calculation |
| `FILTER()` | Dynamic filtering |

---

*Guide created for RLC-Agent | Updated: December 2024*
