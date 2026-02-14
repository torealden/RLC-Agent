# Database Export Guide

## For Felipe (and anyone exporting their database)

This guide walks you through exporting your local PostgreSQL database so we can merge it with the main database.

---

## Prerequisites

Before starting, make sure you have:
1. PostgreSQL installed and running
2. Python 3.x installed
3. Access to the RLC-Agent folder via Dropbox

---

## Step-by-Step Export Instructions

### Step 1: Open PowerShell or Command Prompt

Press `Win + X` and select "Windows PowerShell" or "Terminal"

### Step 2: Navigate to the RLC-Agent Folder

```powershell
cd "C:\Users\Felipe\RLC Dropbox\RLC Team Folder\RLC-Agent"
```

**Note:** Replace `Felipe` with your actual Windows username if different.

### Step 3: Verify Your .env File Has the Database Password

Check that your `.env` file exists and has the password:

```powershell
type .env
```

You should see something like:
```
DB_PASSWORD=your_password_here
```

If the password is missing, add it:
```powershell
echo DB_PASSWORD=your_password_here >> .env
```

### Step 4: Install Required Python Packages (if needed)

```powershell
pip install psycopg2-binary python-dotenv
```

### Step 5: Add PostgreSQL to Your PATH (if needed)

The export script uses `pg_dump`. If you get a "pg_dump not found" error, add PostgreSQL to your PATH:

```powershell
$env:PATH += ";C:\Program Files\PostgreSQL\16\bin"
```

Or permanently add it via System Properties > Environment Variables.

### Step 6: Run the Export Script

```powershell
python scripts/export_database.py
```

### Step 7: What to Expect

The script will:
1. Connect to your database
2. Create an inventory of all tables and views
3. Export everything to a dump file

You'll see output like:
```
======================================================================
RLC DATABASE EXPORT TOOL
======================================================================

Computer: FELIPES_LAPTOP
Export directory: database\exports\FELIPES_LAPTOP_20260130

[1/3] Connecting to database and getting inventory...
      Connected to: rlc_commodities@localhost
      Found: 26 tables, 5 views, 50,000 total rows

[2/3] Creating database dump (this may take a minute)...
      Created: rlc_commodities.dump (15.2 MB)
      Created: rlc_commodities.sql (45.3 MB)

[3/3] Export complete!
```

### Step 8: Verify the Export

After the script completes, you should have a new folder:
```
RLC-Agent/
  database/
    exports/
      FELIPES_LAPTOP_20260130/    <-- New folder
        inventory.json            <-- List of what's in your database
        rlc_commodities.dump      <-- Binary dump (main file)
        rlc_commodities.sql       <-- SQL backup (readable)
```

### Step 9: Done!

The export folder is already in the shared Dropbox folder, so Tore will automatically have access to it. No need to send anything manually.

Just message Tore that the export is complete.

---

## Troubleshooting

### "pg_dump not found"

Add PostgreSQL to your PATH:
```powershell
$env:PATH += ";C:\Program Files\PostgreSQL\16\bin"
```

Then run the export again.

### "Connection refused" or "Could not connect"

Make sure PostgreSQL is running:
1. Open Services (Win + R, type `services.msc`)
2. Find "postgresql-x64-16" (or similar)
3. Make sure it's Running. If not, right-click > Start

### "Database does not exist"

Your database might have a different name. Check with:
```powershell
psql -U postgres -c "\l"
```

### "Password authentication failed"

Your `.env` file has the wrong password. Update it with the correct one.

---

## What Happens Next?

1. Tore will merge your export with the main database
2. Tore will create a complete export with all data
3. You'll receive instructions to restore the complete database
4. After restore, everyone will have the same data

---

## Quick Reference

| Command | Purpose |
|---------|---------|
| `python scripts/export_database.py` | Export your database |
| `python scripts/sync_database_schema.py --check` | Check what's in your database |
| `type .env` | View your environment variables |

---

*Last updated: January 30, 2026*
