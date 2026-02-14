# EIA Feedstock Updater - Excel Data Import from PostgreSQL

## Files

| File | Purpose |
|------|---------|
| `FeedstockUpdaterSQL.bas` | VBA module — import into a standard module |
| `FeedstockWorkbookEvents.bas` | ThisWorkbook event code — paste into ThisWorkbook |

## Installation

### 1. Create the Excel Workbook

Create `us_feedstock_data.xlsm` with 2 sheets (names must match exactly):

**Sheet 1: "Ethanol Weekly"** — Weekly ethanol data
| Row | Content |
|-----|---------|
| 3 | Headers: `Week Ending` \| `Production` \| `Stocks` \| `Blender Input` \| `Balance` |
| 4 | Units: ` ` \| `kbd` \| `kb` \| `kbd` \| `kbd` |
| 5+ | Data: Col A = week-ending date (e.g., 1/3/2020), Cols B-E = data from DB |

- Col A: Week-ending date (Friday dates from EIA)
- Col B: Ethanol production (thousand barrels/day)
- Col C: Ethanol stocks (thousand barrels)
- Col D: Ethanol blender input (thousand barrels/day)
- Col E: Ethanol balance (production minus blender input)

**Sheet 2: "Petroleum Weekly"** — Weekly petroleum context
| Row | Content |
|-----|---------|
| 3 | Headers: `Week Ending` \| `Crude Total` \| `Crude ex-SPR` \| `SPR` \| `Gasoline` \| `Distillate` \| `Crude Prod` \| `Crude Imports` \| `Refinery Inputs` \| `Refinery Util` \| `Gas Days Supply` |
| 4 | Units: ` ` \| `kb` \| `kb` \| `kb` \| `kb` \| `kb` \| `kbd` \| `kbd` \| `kbd` \| `%` \| `days` |
| 5+ | Data: Col A = week-ending date, Cols B-K = data from DB |

### 2. Import VBA Code

1. Open the workbook, press `Alt+F11` to open VBA Editor
2. **Tools > References** — check "Microsoft ActiveX Data Objects 6.1 Library"
3. Right-click on the project > **Import File** > select `FeedstockUpdaterSQL.bas`
4. Double-click **ThisWorkbook** in the Project Explorer
5. Paste the contents of `FeedstockWorkbookEvents.bas`
6. Save as `.xlsm` (macro-enabled)

### 3. Prerequisites

- PostgreSQL ODBC driver (psqlODBC x64)
- PostgreSQL database `rlc_commodities` running on localhost:5432

## Usage

| Shortcut | Action |
|----------|--------|
| `Ctrl+E` | Quick update — latest 26 weeks (~6 months) |
| `Ctrl+Shift+E` | Custom — choose how many weeks to update |

## How It Works

Same pattern as CrushUpdaterSQL and RINUpdaterSQL:
1. Connects to PostgreSQL via ODBC
2. Queries gold views for weekly data
3. Finds matching rows by exact date match in Column A
4. Writes data values, skips formula columns
5. Reports total cells updated

## Database Schema

| Gold View | Sheet | Data |
|-----------|-------|------|
| `gold.eia_ethanol_weekly` | Ethanol Weekly | Production, stocks, blending |
| `gold.eia_petroleum_weekly` | Petroleum Weekly | Crude, gasoline, diesel, refinery |

## Future Expansion

When the EIA Monthly Biofuels Capacity and Feedstock data is collected into the database, add these sheets:

**"Feedstock Inputs"** — Monthly feedstock consumption by type
- Soybean oil, corn oil, canola oil, palm oil, tallow, UCO, etc.
- Source: future `gold.eia_feedstock_matrix` view

**"Biofuel Production"** — Monthly biodiesel/RD production
- Biodiesel production (kbd), RD production (kbd), stocks
- Source: future `gold.eia_biofuel_production_matrix` view

The VBA module is designed to be easily extended — just add a new sheet updater function following the existing pattern and call it from `UpdateFromDatabase()`.

## Keyboard Shortcut Reference (All Updaters)

| Shortcut | Updater | File |
|----------|---------|------|
| `Ctrl+U` / `Ctrl+Shift+U` | Crush (NASS) | CrushUpdaterSQL.bas |
| `Ctrl+I` / `Ctrl+Shift+I` | Trade (Census) | TradeUpdaterSQL.bas |
| `Ctrl+R` / `Ctrl+Shift+R` | RIN (EPA) | RINUpdaterSQL.bas |
| `Ctrl+E` / `Ctrl+Shift+E` | EIA Feedstock | FeedstockUpdaterSQL.bas |
