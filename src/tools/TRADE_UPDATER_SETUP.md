# Trade Updater Setup Guide

## Overview

The Trade Updater system automates pulling trade data from PostgreSQL into Excel spreadsheets using Ctrl+I.

## Files

| File | Purpose |
|------|---------|
| `database/schemas/014_trade_reference_tables.sql` | Silver layer reference tables (countries, commodities, regions) |
| `database/views/07_trade_export_views.sql` | Gold layer views for trade export |
| `src/tools/excel_trade_updater.py` | Python script that queries DB and updates Excel |
| `src/tools/TradeUpdater.bas` | VBA module for Excel keyboard integration |

## Step 1: Install Database Tables and Views

Open PowerShell and run:

```powershell
# Create the reference tables (silver layer)
psql -d rlc_commodities -f "C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\database\schemas\014_trade_reference_tables.sql"

# Create the export views (gold layer)
psql -d rlc_commodities -f "C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\database\views\07_trade_export_views.sql"
```

Verify installation:
```sql
-- In psql, check tables exist
\dt silver.trade_*

-- Check views exist
\dv gold.trade_*

-- Test the main view
SELECT COUNT(*) FROM gold.trade_export_matrix;
```

## Step 2: Install Python Dependencies

```powershell
pip install psycopg2-binary openpyxl
```

## Step 3: Test Python Script

```powershell
# Navigate to tools directory
cd "C:\Users\torem\RLC Dropbox\RLC Team Folder\RLC-Agent\src\tools"

# Test with dry run (check connection)
python excel_trade_updater.py --help

# Test with actual file
python excel_trade_updater.py --file "path/to/trade.xlsx" --sheet "Soybean Exports" --months "2024-10,2024-11,2024-12"
```

## Step 4: Install VBA Module in Excel

1. Open your trade spreadsheet in Excel
2. Press **Alt+F11** to open VBA Editor
3. Go to **File > Import File**
4. Select `src/tools/TradeUpdater.bas`
5. Save workbook as `.xlsm` (macro-enabled)

### Auto-Enable Shortcuts

Add this code to **ThisWorkbook** module (in VBA Editor, double-click "ThisWorkbook" in Project Explorer):

```vba
Private Sub Workbook_Open()
    AssignKeyboardShortcuts
End Sub

Private Sub Workbook_BeforeClose(Cancel As Boolean)
    RemoveKeyboardShortcuts
End Sub
```

## Usage

| Shortcut | Action |
|----------|--------|
| **Ctrl+I** | Quick update - last 3 months of data |
| **Ctrl+Shift+I** | Custom update - prompts for date range |

## Sheet Name Detection

The script automatically detects commodity and flow type from sheet names:

| Sheet Name Contains | Commodity | Flow |
|---------------------|-----------|------|
| "soybean export" | SOYBEANS | EXPORT |
| "soybean import" | SOYBEANS | IMPORT |
| "soybean meal" | SOYBEAN_MEAL | - |
| "soybean oil" | SOYBEAN_OIL | - |
| "corn" or "maize" | CORN | - |
| "wheat" | WHEAT | - |
| "ddgs" | DDGS | - |

## Troubleshooting

### "Table not found" error
Run the SQL files again to create tables/views.

### "No data found" error
Check that `bronze.census_trade` has data for the requested months:
```sql
SELECT DISTINCT year, month FROM bronze.census_trade ORDER BY year, month;
```

### Python not found
Update `PYTHON_PATH` in `TradeUpdater.bas` to your Python installation path.

### Connection refused
Ensure PostgreSQL is running and accepts connections on localhost:5432.

## Database Schema

```
bronze.census_trade (raw Census trade data)
    ↓
silver.trade_country_reference (country mapping)
silver.trade_commodity_reference (HS codes, conversions)
    ↓
gold.trade_export_mapped (joined data)
    ↓
gold.trade_export_matrix (final matrix for Excel)
```
