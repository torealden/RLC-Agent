# Database Restore Guide

## Installing the Complete Database

This guide walks you through restoring the complete RLC database with all tables, views, and data.

---

## Prerequisites

Before starting, make sure you have:
1. PostgreSQL 14+ installed and running
2. Python 3.x installed
3. The export file (provided by Tore)

---

## Step-by-Step Restore Instructions

### Step 1: Open PowerShell or Command Prompt

Press `Win + X` and select "Windows PowerShell" or "Terminal"

### Step 2: Navigate to the RLC-Agent Folder

```powershell
cd "C:\Users\YourName\RLC Dropbox\RLC Team Folder\RLC-Agent"
```

Replace `YourName` with your actual Windows username.

### Step 3: Verify PostgreSQL is in Your PATH

```powershell
pg_restore --version
```

If you get "not recognized", add PostgreSQL to your PATH:
```powershell
$env:PATH += ";C:\Program Files\PostgreSQL\16\bin"
```

### Step 4: Verify Your .env File

```powershell
type .env
```

Make sure it contains:
```
DB_PASSWORD=your_postgres_password
```

### Step 5: Find the Export File

The complete database export will be in:
```
RLC-Agent/database/exports/COMPLETE_DATABASE/rlc_commodities.dump
```

Or Tore will tell you the exact path.

### Step 6: Run the Restore Script

```powershell
python scripts/restore_database.py --file database/exports/COMPLETE_DATABASE/rlc_commodities.dump
```

### Step 7: Confirm the Restore

The script will warn you that this replaces your current database:

```
======================================================================
RLC DATABASE RESTORE TOOL
======================================================================

Restore file: database/exports/COMPLETE_DATABASE/rlc_commodities.dump
File size: 125.3 MB

----------------------------------------------------------------------
WARNING: This will REPLACE your current database!
----------------------------------------------------------------------
Type 'YES' to continue:
```

Type `YES` and press Enter.

### Step 8: Wait for Restore to Complete

The restore may take several minutes. You'll see:

```
[BACKUP] Creating backup of current database...
         Backup saved: database/backups/pre_restore_backup_20260130_093045.dump

[RESTORE] Restoring from: database/exports/COMPLETE_DATABASE/rlc_commodities.dump
          Dropping and recreating database...
          Database recreated
          Running pg_restore (this may take several minutes)...
          [OK] Restore completed successfully

[VERIFY] Checking restored database...
         Tables: 71
         Views: 66
         Schemas: audit, bronze, gold, meta, public, reference, silver

         [OK] Database looks complete!
```

### Step 9: Verify in Power BI

1. Open Power BI Desktop
2. Click "Refresh" on your data connection
3. You should now see all 137+ tables and views

---

## Alternative: Manual Restore (if script fails)

If the Python script doesn't work, you can restore manually:

### Option A: Using pg_restore (Binary Dump)

```powershell
# Set password
$env:PGPASSWORD = "your_password"

# Drop existing database (optional - if you want fresh start)
psql -U postgres -c "DROP DATABASE IF EXISTS rlc_commodities;"
psql -U postgres -c "CREATE DATABASE rlc_commodities;"

# Restore
pg_restore -U postgres -d rlc_commodities -v --no-owner --no-acl database/exports/COMPLETE_DATABASE/rlc_commodities.dump
```

### Option B: Using psql (SQL File)

```powershell
# Set password
$env:PGPASSWORD = "your_password"

# Drop existing database
psql -U postgres -c "DROP DATABASE IF EXISTS rlc_commodities;"
psql -U postgres -c "CREATE DATABASE rlc_commodities;"

# Restore from SQL
psql -U postgres -d rlc_commodities -f database/exports/COMPLETE_DATABASE/rlc_commodities.sql
```

---

## After Restore: Verify Everything Works

### Check 1: Count Objects

```powershell
python scripts/sync_database_schema.py --check
```

Expected output:
```
Schema          Tables     Views
-----------------------------------
audit           2          0
bronze          34         0
gold            0          57
meta            1          0
public          9          0
reference       5          0
silver          20         9
-----------------------------------
TOTAL           71         66
```

### Check 2: Query a Table

```powershell
psql -U postgres -d rlc_commodities -c "SELECT COUNT(*) FROM bronze.fas_psd;"
```

### Check 3: Open Power BI

Connect to `localhost:5432/rlc_commodities` and verify you see all tables.

---

## Troubleshooting

### "pg_restore: command not found"

Add PostgreSQL bin directory to PATH:
```powershell
$env:PATH += ";C:\Program Files\PostgreSQL\16\bin"
```

### "Database is being accessed by other users"

Close Power BI and any other apps connected to the database, then try again.

Or force disconnect:
```powershell
psql -U postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='rlc_commodities' AND pid <> pg_backend_pid();"
```

### "Permission denied"

Run PowerShell as Administrator.

### "Password authentication failed"

Update your `.env` file with the correct password:
```powershell
notepad .env
```

### Restore Takes Too Long

Large databases can take 5-10 minutes to restore. Be patient.

---

## Rolling Back (If Something Goes Wrong)

The restore script automatically creates a backup before restoring. To roll back:

```powershell
python scripts/restore_database.py --file database/backups/pre_restore_backup_YYYYMMDD_HHMMSS.dump
```

---

## Quick Reference

| Command | Purpose |
|---------|---------|
| `python scripts/restore_database.py --file <path>` | Restore database |
| `python scripts/sync_database_schema.py --check` | Verify database |
| `python scripts/export_database.py` | Export your database |

---

## What the Complete Database Contains

After restore, you'll have:

| Schema | Tables | Views | Description |
|--------|--------|-------|-------------|
| bronze | 34 | 0 | Raw data (USDA, EIA, CFTC, Weather, etc.) |
| silver | 20 | 9 | Cleaned and transformed data |
| gold | 0 | 57 | Analytics-ready views for dashboards |
| reference | 5 | 0 | Lookup tables (crop regions, codes) |
| public | 9 | 0 | Core dimension tables |
| audit | 2 | 0 | Data quality tracking |
| meta | 1 | 0 | Ingestion logs |

---

*Last updated: January 30, 2026*
