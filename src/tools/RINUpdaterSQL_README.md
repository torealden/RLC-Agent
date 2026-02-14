# RIN Updater - Excel Data Import from PostgreSQL

## Files

| File | Purpose |
|------|---------|
| `RINUpdaterSQL.bas` | VBA module — import into a standard module |
| `RINWorkbookEvents.bas` | ThisWorkbook event code — paste into ThisWorkbook |
| `rin_gold_views.sql` | SQL to create the gold matrix views (run once) |

## Installation

### 1. Create the Gold Views (one-time)

```bash
psql -U postgres -d rlc_commodities -f src/tools/rin_gold_views.sql
```

This creates three views:
- `gold.rin_monthly_matrix` — Monthly RIN generation pivoted by D-code
- `gold.rin_annual_balance` — Annual gen/ret/avail combined by D-code
- `gold.d4_fuel_matrix` — D4 fuel production pivoted by fuel type

### 2. Create the Excel Workbook

Create `us_rin_data.xlsm` with 4 sheets (names must match exactly):

**Sheet 1: "RIN Monthly"** — Monthly time series
| Row | Content |
|-----|---------|
| 3 | Headers: `Date` \| `D3 Cellulosic` \| `D4 BBD` \| `D5 Advanced` \| `D6 Renewable` \| `D7 Cel. Diesel` \| `Total RINs` \| `D3 Volume` \| `D4 Volume` \| `D5 Volume` \| `D6 Volume` \| `D7 Volume` \| `Total Volume` |
| 4 | Units: ` ` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `Gallons` \| `Gallons` \| `Gallons` \| `Gallons` \| `Gallons` \| `Gallons` |
| 5+ | Data: Col A = 1st-of-month date (e.g., 1/1/2010), Cols B-F and H-L = data from DB, Cols G and M = SUM formulas |

- Col A: Date (formatted as 1st of month, e.g., `1/1/2010`, `2/1/2010`, ...)
- Cols B-F: RIN quantities by D-code (filled by updater)
- Col G: `=SUM(B5:F5)` — Total RINs formula
- Cols H-L: Batch volumes by D-code (filled by updater)
- Col M: `=SUM(H5:L5)` — Total Volume formula

**Sheet 2: "Annual Generation"** — Annual totals
| Row | Content |
|-----|---------|
| 3 | Headers: `Year` \| `D3 Cellulosic` \| `D4 BBD` \| `D5 Advanced` \| `D6 Renewable` \| `D7 Cel. Diesel` \| `Total RINs` \| `Total Advanced` |
| 4 | Units: ` ` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `RINs` |
| 5+ | Data: Col A = Year (integer, e.g., 2010, 2011, ...), Cols B-H = data from DB |

**Sheet 3: "RIN Balance"** — Annual balance by D-code
| Row | Content |
|-----|---------|
| 2 | Category: ` ` \| `D3 Cellulosic` \| ` ` \| ` ` \| `D4 BBD` \| ` ` \| ` ` \| `D5 Advanced` \| ` ` \| ` ` \| `D6 Renewable` \| ` ` \| ` ` \| `Totals` \| ` ` \| ` ` |
| 3 | Headers: `Year` \| `Generated` \| `Retired` \| `Available` \| `Generated` \| `Retired` \| `Available` \| `Generated` \| `Retired` \| `Available` \| `Generated` \| `Retired` \| `Available` \| `Generated` \| `Retired` \| `Available` |
| 4 | Units: all `RINs` |
| 5+ | Data: Col A = Year (integer), Cols B-P = data from DB |

**Sheet 4: "D4 Fuel Mix"** — D4 fuel production breakdown
| Row | Content |
|-----|---------|
| 3 | Headers: `Year` \| `Biodiesel` \| `RD (EV 1.7)` \| `RD (EV 1.6)` \| `Ren Jet` \| `Other` \| `Total Vol` \| `Biodiesel` \| `RD (EV 1.7)` \| `RD (EV 1.6)` \| `Ren Jet` \| `Other` \| `Total RINs` |
| 4 | Units: ` ` \| `Gallons` \| `Gallons` \| `Gallons` \| `Gallons` \| `Gallons` \| `Gallons` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `RINs` \| `RINs` |
| 5+ | Data: Col A = Year (integer), Cols B-F and H-L = data from DB, Cols G and M = SUM formulas |

### 3. Import VBA Code

1. Open the workbook, press `Alt+F11` to open VBA Editor
2. **Tools > References** — check "Microsoft ActiveX Data Objects 6.1 Library"
3. Right-click on the project > **Import File** > select `RINUpdaterSQL.bas`
4. Double-click **ThisWorkbook** in the Project Explorer
5. Paste the contents of `RINWorkbookEvents.bas`
6. Save as `.xlsm` (macro-enabled)

### 4. Prerequisites

- PostgreSQL ODBC driver (psqlODBC x64) — download from https://www.postgresql.org/ftp/odbc/versions/msi/
- PostgreSQL database `rlc_commodities` running on localhost:5432

## Usage

| Shortcut | Action |
|----------|--------|
| `Ctrl+R` | Quick update — latest 6 months (monthly) + all years (annual) |
| `Ctrl+Shift+R` | Custom — choose how many months to update |

## How It Works

1. Connects to PostgreSQL via ODBC (single shared connection)
2. Queries each gold view for the relevant data
3. For each row returned, finds the matching row in the spreadsheet by scanning Column A
4. Writes data values to the correct columns; skips formula columns (totals, YoY%, etc.)
5. Reports total cells updated across all sheets

## Database Schema

| Gold View | Sheet | Data |
|-----------|-------|------|
| `gold.rin_monthly_matrix` | RIN Monthly | Monthly D3-D7 RINs + batch volumes |
| `gold.rin_generation_summary` | Annual Generation | Annual generation totals by D-code |
| `gold.rin_annual_balance` | RIN Balance | Annual gen/ret/avail by D-code |
| `gold.d4_fuel_matrix` | D4 Fuel Mix | D4 fuel production by fuel type |

## Keyboard Shortcut Reference (All Updaters)

| Shortcut | Updater | File |
|----------|---------|------|
| `Ctrl+U` / `Ctrl+Shift+U` | Crush (NASS) | CrushUpdaterSQL.bas |
| `Ctrl+I` / `Ctrl+Shift+I` | Trade (Census) | TradeUpdaterSQL.bas |
| `Ctrl+R` / `Ctrl+Shift+R` | RIN (EPA) | RINUpdaterSQL.bas |
| `Ctrl+E` / `Ctrl+Shift+E` | EIA Feedstock | FeedstockUpdaterSQL.bas |
