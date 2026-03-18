# RLC-Agent Onboarding Guide

Complete setup instructions for a new team member on a fresh Windows machine.
By the end of this guide you will have: the full codebase, database access,
Excel VBA macros pulling live data, and Claude Code with direct database access.

---

## Quick Start (Automated)

If you just want to get running fast, open **PowerShell as Administrator** and run:

```powershell
# Allow scripts to run
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned

# Download and run setup script
cd C:\
mkdir dev -ErrorAction SilentlyContinue
cd dev
git clone https://github.com/torealden/RLC-Agent.git rlc-agent
cd rlc-agent
.\scripts\deployment\setup_new_machine.ps1
```

If you don't have `git` yet, install it first (Step 1 below), then run the above.

---

## Step-by-Step Manual Setup

### Step 1: Install Prerequisites

Install these in order. All are free.

| Software | Download | Notes |
|----------|----------|-------|
| **Git for Windows** | https://git-scm.com/download/win | Default options. Adds `git` to PATH. |
| **Python 3.11** | https://www.python.org/downloads/ | **Check "Add to PATH"** during install. |
| **Node.js LTS** | https://nodejs.org/ | Required for Claude Code. |
| **pgAdmin 4** | https://www.pgadmin.org/download/ | Free database browser (optional but recommended). |
| **VS Code** | https://code.visualstudio.com/ | Optional — for viewing/editing code. |

After installing, open a **new** PowerShell window and verify:

```powershell
git --version       # Should show 2.x
python --version    # Should show 3.11.x
node --version      # Should show 20.x or higher
```

---

### Step 2: Clone the Repository

```powershell
cd C:\
mkdir dev -ErrorAction SilentlyContinue
cd dev
git clone https://github.com/torealden/RLC-Agent.git rlc-agent
cd rlc-agent
```

This downloads the entire project (~500 MB). Everything lives in `C:\dev\rlc-agent`.

**To get updates later** (do this regularly):

```powershell
cd C:\dev\rlc-agent
git pull origin main
```

---

### Step 3: Install Python Dependencies

```powershell
cd C:\dev\rlc-agent
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

This installs pandas, psycopg2, APScheduler, and everything else the system needs.

---

### Step 4: Set Up Environment Variables

The `.env` file contains database credentials and API keys. It is **not** stored in
GitHub (for security). You need to create it manually.

Create the file `C:\dev\rlc-agent\.env` with this content:

```ini
# Database - AWS RDS
DB_TYPE=postgresql
RLC_PG_HOST=rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com
RLC_PG_PORT=5432
RLC_PG_DATABASE=rlc_commodities
RLC_PG_USER=postgres
RLC_PG_PASSWORD=SoupBoss1
RLC_PG_SSLMODE=require

# Legacy aliases
DB_HOST=localhost
DB_PORT=5432
DB_NAME=rlc_commodities
DB_USER=postgres
DATABASE_URL=postgresql://postgres:SoupBoss1@rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com:5432/rlc_commodities
```

Ask Tore for additional API keys (USDA, EIA, Census, etc.) if you need to run
data collectors.

**Test the connection:**

```powershell
cd C:\dev\rlc-agent
python -c "from dotenv import load_dotenv; load_dotenv(); from src.services.database.db_config import get_connection; conn = get_connection(); print('Connected to RDS!'); conn.close()"
```

If you get a connection error about your IP, ask Tore to add your public IP to
the AWS RDS security group. Find your IP at https://whatismyipaddress.com.

---

### Step 5: Set Up pgAdmin (Database Browser)

pgAdmin lets you browse tables, run SQL queries, and explore the data visually.

1. Open pgAdmin 4
2. Right-click **Servers** > **Register** > **Server**
3. **General** tab:
   - Name: `RLC Commodities (RDS)`
4. **Connection** tab:
   - Host: `rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com`
   - Port: `5432`
   - Database: `rlc_commodities`
   - Username: `postgres`
   - Password: `SoupBoss1`
5. **SSL** tab:
   - SSL Mode: `Require`
6. Click **Save**

You should now see the database with three schemas:
- **bronze** — raw ingested data (31 tables)
- **silver** — cleaned and standardized (26 tables)
- **gold** — analytics-ready views (53 views)

---

### Step 6: Set Up Excel VBA Macros

The Excel workbooks pull live data from the database using ODBC.

#### 6a. Install PostgreSQL ODBC Driver

1. Download the **64-bit** driver from:
   https://ftp.postgresql.org/pub/odbc/releases/
   (Look for `psqlodbc_16_00_xxxx-x64.zip` or `.msi`)
2. Install with default options

#### 6b. Verify ODBC Installation

```powershell
Get-OdbcDriver -Name "*PostgreSQL*"
```

Should show `PostgreSQL Unicode(x64)`.

#### 6c. Import VBA Modules into Excel

1. Open the target Excel workbook
2. Press `Alt+F11` to open the VBA Editor
3. **File** > **Import File**
4. Navigate to `C:\dev\rlc-agent\src\tools\`
5. Import these modules:

| File | Keyboard Shortcut | What It Does |
|------|-------------------|--------------|
| `FatsOilsUpdaterSQL.bas` | Ctrl+U | Fats & oils crush data |
| `TradeUpdaterSQL.bas` | Ctrl+I | Census trade flows |
| `BiofuelDataUpdater.bas` | Ctrl+B | Biofuel S&D data |
| `EIAFeedstockUpdater.bas` | Ctrl+D | EIA feedstock data |
| `RINUpdaterSQL.bas` | Ctrl+R | RIN generation data |
| `EMTSDataUpdater.bas` | Ctrl+E | EMTS/feedstock data |
| `WorkbookEvents.bas` | (auto) | Assigns shortcuts on open |

6. Also import `WorkbookEvents.bas` into the **ThisWorkbook** module
   (copy the code from the `.bas` file into the ThisWorkbook code window)

#### 6d. Verify in Excel

1. Close and reopen the workbook
2. Press `Ctrl+U` — should pull fats & oils data from the database
3. If it prompts for a connection, the ODBC string is:
   ```
   Driver={PostgreSQL Unicode(x64)};Server=rlc-commodities.c16c6wm826t7.us-east-2.rds.amazonaws.com;Port=5432;Database=rlc_commodities;Uid=postgres;Pwd=SoupBoss1;sslmode=require;
   ```

---

### Step 7: Install Claude Code

Claude Code gives you an AI assistant with direct access to the commodities
database, knowledge graph, and all project context.

```powershell
npm install -g @anthropic-ai/claude-code
```

You need either:
- A **Claude Max subscription** ($100/mo at claude.ai — includes unlimited Claude Code), OR
- An **Anthropic API key** (pay-per-use at console.anthropic.com)

#### Start Claude Code:

```powershell
cd C:\dev\rlc-agent
claude
```

On first run, it will ask you to authenticate. Follow the prompts.

#### What Claude Code Can Do:

Once running, Claude has access to the full commodities database via MCP tools.
Try these:

```
> What is the latest CFTC positioning for soybeans?
> Show me the US corn balance sheet for MY 2024/25
> How does Brazil soybean production compare to last year by state?
> What data sources are overdue?
```

It reads the `CLAUDE.md` file automatically, which gives it full context about
the database schema, marketing years, unit conversions, and analytical frameworks.

---

## Database Quick Reference

### Key Gold Views (analytics-ready)

| View | Description |
|------|-------------|
| `gold.fas_us_corn_balance_sheet` | US corn S&D |
| `gold.fas_us_soybeans_balance_sheet` | US soybeans S&D |
| `gold.fas_us_wheat_balance_sheet` | US wheat S&D |
| `gold.brazil_soybean_production` | Brazil soy by state |
| `gold.cftc_sentiment` | Current CFTC positioning sentiment |
| `gold.cftc_corn_positioning` | Corn managed money history |
| `gold.eia_ethanol_weekly` | Ethanol production/stocks |
| `gold.fats_oils_crush_matrix` | Monthly crush by commodity |
| `gold.futures_daily_validated` | Daily futures settlements |

### Example Queries (run in pgAdmin or Claude Code)

```sql
-- US corn balance sheet
SELECT * FROM gold.fas_us_corn_balance_sheet ORDER BY marketing_year DESC LIMIT 3;

-- Current CFTC positioning
SELECT * FROM gold.cftc_sentiment;

-- Brazil soy production by state
SELECT * FROM gold.brazil_soybean_production
WHERE crop_year = '2024/25' ORDER BY production DESC LIMIT 10;

-- Weekly ethanol
SELECT * FROM gold.eia_ethanol_weekly ORDER BY week_ending DESC LIMIT 10;
```

---

## Staying Up to Date

### Pull Code Updates

```powershell
cd C:\dev\rlc-agent
git pull origin main
```

Do this at least weekly, or whenever Tore says there are updates.

### Update Python Dependencies

After pulling, if `requirements.txt` changed:

```powershell
python -m pip install -r requirements.txt
```

---

## Troubleshooting

### "Connection refused" or timeout on database

Your public IP may not be in the AWS security group. Tell Tore your IP
(from https://whatismyipaddress.com) and he will add it.

### ODBC "driver not found" in Excel

Make sure you installed the **64-bit** PostgreSQL ODBC driver, and that your
Excel is also 64-bit. Check with:

```powershell
Get-OdbcDriver -Name "*PostgreSQL*"
```

### `git pull` asks for credentials

Use the HTTPS URL (not SSH). Git Credential Manager should handle auth
automatically after the first login. If prompted, use your GitHub username
and a **Personal Access Token** (not your password):
Settings > Developer Settings > Personal Access Tokens > Generate new token.

### Claude Code "MCP server failed to start"

Check that Python path in `.mcp.json` matches your actual Python install:

```powershell
(Get-Command python).Source
```

Update the `command` field in `.mcp.json` if different.

### VBA macro doesn't run

1. Enable macros: File > Options > Trust Center > Trust Center Settings > Macro Settings > Enable all macros
2. Check References: In VBA Editor, Tools > References — ensure "Microsoft ActiveX Data Objects 6.1 Library" is checked

---

## Contact

- **Tore Alden** — tore.alden@roundlakescommodities.com (project lead, system admin)
- **GitHub repo** — https://github.com/torealden/RLC-Agent
