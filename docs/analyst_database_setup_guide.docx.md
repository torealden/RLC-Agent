# RLC Commodities Database — Analyst Setup Guide

## Overview

The RLC Commodities database is a PostgreSQL database hosted on AWS (Amazon RDS). It contains agricultural commodity data organized in three layers:

- **Bronze** — Raw data as collected from sources (USDA, EIA, CFTC, etc.)
- **Silver** — Cleaned and standardized data with calculated fields
- **Gold** — Analytics-ready views, balance sheets, and matrix views for spreadsheets

You can connect to the database from:
1. **pgAdmin 4** — Free GUI for browsing tables and running SQL queries
2. **Excel VBA macros** — One-click spreadsheet updates via Ctrl shortcuts
3. **DBeaver** — Alternative database GUI (optional)

---

## Step 1: Install pgAdmin 4

pgAdmin is the standard PostgreSQL GUI. Download and install it:

1. Go to https://www.pgadmin.org/download/pgadmin-4-windows/
2. Download the latest Windows installer (`.exe`)
3. Run the installer with default settings
4. When it opens, it will ask you to set a **master password** — this is for pgAdmin itself (pick anything you'll remember)

### Connect to RLC Database

1. In pgAdmin, right-click **Servers** in the left panel → **Register** → **Server**
2. **General** tab:
   - Name: `RLC Commodities`
3. **Connection** tab:
   - Host: `rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com`
   - Port: `5432`
   - Maintenance database: `rlc_commodities`
   - Username: `postgres`
   - Password: `SoupBoss1`
   - Check "Save password"
4. **SSL** tab:
   - SSL mode: `Require`
5. Click **Save**

You should now see the database in the left panel. Expand it to browse schemas (bronze, silver, gold) and their tables/views.

### Running Queries

1. Click on the `rlc_commodities` database in the left panel
2. Click the **Query Tool** icon (or Tools → Query Tool)
3. Type a SQL query and press F5 (or the play button) to run it

**Example queries to try:**

```sql
-- US corn balance sheet (latest 3 years)
SELECT * FROM gold.fas_us_corn_balance_sheet
ORDER BY marketing_year DESC LIMIT 3;

-- Current CFTC managed money positioning
SELECT * FROM gold.cftc_sentiment;

-- Weekly ethanol production
SELECT * FROM gold.eia_ethanol_weekly
ORDER BY week_ending DESC LIMIT 10;

-- Brazil soybean production by state
SELECT * FROM gold.brazil_soybean_production
WHERE crop_year = '2024/25'
ORDER BY production DESC;
```

---

## Step 2: Install PostgreSQL ODBC Driver (for Excel VBA)

The Excel spreadsheet updaters connect to the database via ODBC. You need the PostgreSQL ODBC driver installed.

1. Go to https://www.postgresql.org/ftp/odbc/versions/msi/
2. Download the latest **64-bit** installer: `psqlodbc_xx_xx_xxxx-x64.zip`
3. Extract the ZIP and run the `.msi` installer
4. Use default settings — just click through

To verify it installed correctly:
1. Open Windows search → type "ODBC" → open **ODBC Data Sources (64-bit)**
2. Click the **Drivers** tab
3. You should see `PostgreSQL UNICODE(x64)` in the list

---

## Step 3: Install Microsoft ActiveX Data Objects (for Excel VBA)

This is usually already installed with Office, but verify:

1. Open any Excel workbook
2. Press **Alt+F11** to open the VBA editor
3. Go to **Tools** → **References**
4. Scroll down and check **Microsoft ActiveX Data Objects 6.1 Library**
5. Click OK

---

## Step 4: Import VBA Modules into Excel Workbooks

Each spreadsheet workbook has VBA modules that pull data from the database. To import them:

1. Open the workbook in Excel
2. Press **Alt+F11** to open the VBA editor
3. In the left panel, right-click on the workbook name → **Import File**
4. Navigate to `C:\dev\rlc-agent\src\tools\` (or wherever you have the project)
5. Select the appropriate `.bas` file and click **Open**
6. Repeat for any additional modules the workbook needs

### VBA Module Reference

| Module File | Workbook | Shortcut | What It Does |
|---|---|---|---|
| `TradeUpdaterSQL.bas` | Census Trade workbook | Ctrl+I | US import/export trade data |
| `InspectionsUpdaterSQL.bas` | FGIS Inspections workbook | Ctrl+G | Export inspections by destination (thousand bushels) |
| `CrushUpdaterSQL.bas` | Crush data workbook | Ctrl+U | Soybean crush data |
| `BiofuelDataUpdater.bas` | Biofuel S&D workbook | Ctrl+B | Biofuel balance sheet data |
| `FeedstockUpdaterSQL.bas` | US Feedstock workbook | Ctrl+E | EIA ethanol + petroleum weekly |
| `EMTSDataUpdater.bas` | EMTS RIN workbook | Ctrl+E | EPA RIN generation data |
| `RINUpdaterSQL.bas` | RIN data workbook | Ctrl+R | RIN transaction data |
| `EIAFeedstockUpdater.bas` | EIA Feedstock workbook | Ctrl+D | EIA feedstock data |

### Keyboard Shortcut Pattern

Each workbook follows the same pattern:
- **Ctrl+[letter]** = Quick update (latest N periods)
- **Ctrl+Shift+[letter]** = Custom update (you choose how many periods)

### WorkbookEvents Module

Each workbook also needs a small piece of code in the **ThisWorkbook** module to auto-assign keyboard shortcuts when the file opens. This is already set up in existing workbooks — if you create a new one, ask Tore for the ThisWorkbook event code.

---

## Step 5: Verify Everything Works

### Test pgAdmin Connection
Run this query to verify you can see data:
```sql
SELECT COUNT(*) as total_rows, 'bronze' as schema
FROM information_schema.tables
WHERE table_schema = 'bronze';
```

### Test Excel VBA Connection
1. Open a workbook with VBA modules imported
2. Press the keyboard shortcut (e.g., Ctrl+I for trade data)
3. You should see a progress message and then a summary of cells updated

---

## Troubleshooting

### "Database connection failed" in Excel
- Verify the ODBC driver is installed (Step 2)
- Verify ActiveX Data Objects reference is checked (Step 3)
- Check your internet connection — the database is on AWS

### "no pg_hba.conf entry for host" error
- Your IP address needs to be added to the AWS security group
- Contact Tore with your public IP (find it at https://whatismyip.com)
- Your ISP may change your IP periodically — if it stops working, check again

### "sslmode" or "no encryption" error
- Make sure you're using the latest `.bas` files — they include `sslmode=require`
- In pgAdmin, set SSL mode to "Require" on the SSL tab

### VBA module not working after import
- Close and reopen the workbook
- Make sure you saved the workbook as `.xlsm` (macro-enabled)
- Check that the ThisWorkbook module has the shortcut assignment code

---

## Key Database Views for Analysts

### Balance Sheets
| View | Description |
|---|---|
| `gold.fas_us_corn_balance_sheet` | US corn S&D |
| `gold.fas_us_soybeans_balance_sheet` | US soybeans S&D |
| `gold.fas_us_wheat_balance_sheet` | US wheat S&D |
| `gold.us_soybean_balance_sheet` | Historical US soybean S&D (ERS) |
| `gold.us_soybean_oil_balance_sheet` | US soybean oil S&D |
| `gold.us_soybean_meal_balance_sheet` | US soybean meal S&D |
| `gold.brazil_balance_sheet` | Brazil S&D |

### Crop Conditions
| View | Description |
|---|---|
| `gold.corn_condition_latest` | Current corn condition vs 5-year avg |
| `gold.soybean_condition_latest` | Current soybean condition |
| `gold.wheat_condition_latest` | Current wheat condition |

### Positioning
| View | Description |
|---|---|
| `gold.cftc_sentiment` | Current managed money summary |
| `gold.cftc_corn_positioning` | Corn MM positions |
| `gold.cftc_soybean_positioning` | Soybean MM positions |

### Energy / Biofuels
| View | Description |
|---|---|
| `gold.eia_ethanol_weekly` | Ethanol production + stocks |
| `gold.eia_petroleum_weekly` | Petroleum data |
| `gold.emts_monthly_matrix` | EPA RIN generation by type |

### Inspections & Trade
| View | Description |
|---|---|
| `gold.fgis_inspections_monthly_matrix_kbu` | Monthly export inspections (thousand bushels) |
| `gold.fgis_inspections_weekly_matrix_kbu` | Weekly export inspections (thousand bushels) |

---

## Database Connection Details

| Setting | Value |
|---|---|
| Host | `rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com` |
| Port | `5432` |
| Database | `rlc_commodities` |
| Username | `postgres` |
| Password | `SoupBoss1` |
| SSL | Required |
