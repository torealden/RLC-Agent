# Biofuel Holding Sheet — VBA Setup Guide

## Files

| File | Location | Purpose |
|------|----------|---------|
| `BiofuelDataUpdater.bas` | `src\tools\` | Main VBA module — PostgreSQL queries for 9 data sheets |
| `BiofuelWorkbookEvents.bas` | `src\tools\` | Reference for ThisWorkbook event code |
| `biofuel_schema.sql` | `src\tools\` | PostgreSQL schema (bronze + gold layers) |

## Installation

Same process as TradeUpdaterSQL and CrushUpdaterSQL:

1. Save `us_biofuel_holding_sheet.xlsx` as **.xlsm** (macro-enabled)
2. **Alt+F11** → File → Import File → select `BiofuelDataUpdater.bas`
3. Double-click **ThisWorkbook** → paste the two Subs from `BiofuelWorkbookEvents.bas`
4. **Tools → References** → check **Microsoft ActiveX Data Objects 6.1 Library**
5. psqlODBC 64-bit must be installed (same driver as Trade and Crush updaters)

## Usage

| Shortcut | Action |
|----------|--------|
| **Ctrl+B** | Quick update — latest 6 months (with confirmation dialog) |
| **Ctrl+Shift+B** | Custom — enter month count (0 = all available data) |

Completion dialog reports sheets updated and total cells written.

## How It Works

Identical pattern to CrushUpdaterSQL:
- Rows = months (date serial in column A), columns = data attributes
- `FindRowForDate` scans column A matching year/month (handles both Date and serial number)
- Formula columns (totals, YoY%, blend rates, conversions) are never touched
- Each sheet updater returns a cell count; orchestrator sums them for the report
- Single ADODB connection shared across all 9 sheet queries
- Queries hit `gold.*` tables/views via `(year, month) IN (SELECT DISTINCT ... LIMIT N)`

## Database Schema

Follows the bronze → silver → gold medallion pattern:

| Layer | Tables | Who Writes |
|-------|--------|------------|
| `bronze.*` | Raw collector output (append-only) | Python collectors |
| `silver.*` | Normalized, quality-checked | Transform scripts (future) |
| `gold.*` | VBA-ready pivoted tables | Initially collectors; later views over silver |

Run `biofuel_schema.sql` to create all tables:
```bash
psql -U postgres -d rlc_commodities -f biofuel_schema.sql
```

## Gold Tables → Sheet Mapping

| Gold Table | VBA Sheet | Key Columns |
|------------|-----------|-------------|
| `gold.rin_generation` | RIN Generation | D3-D7 RINs, D4 breakout, physical volumes |
| `gold.rin_separation` | RIN Sep & Available | Separation D3-D7, Available D3-D7 |
| `gold.rin_retirement` | RIN Retirement | Retirement D3-D7, RVO target |
| `gold.eia_feedstock` | Feedstock Consumption | 14 feedstock types (mil lbs), plant totals |
| `gold.eia_feedstock_plant_type` | Feedstock by Plant Type | SBO/Corn/Tallow/UCO split by BD vs RD |
| `gold.eia_biofuel_production` | Biofuel Production | Ethanol/BD/RD/Other kbd, stocks kbbl |
| `gold.eia_biofuel_trade` | Biofuel Trade | Ethanol/BD/RD imports & exports |
| `gold.eia_biofuel_capacity` | Capacity | Plant-level: company, location, MMgy |
| `gold.eia_blending_context` | Blending Context | Gasoline/diesel prod, demand, stocks |
