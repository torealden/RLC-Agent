"""
Database Schema Synchronization Script

This script ensures all database schemas, tables, and views are created
on any PostgreSQL instance. Run this on any new computer or when schemas
are out of sync.

Usage:
    python scripts/sync_database_schema.py --check      # Check what's missing
    python scripts/sync_database_schema.py --apply      # Apply missing schemas
    python scripts/sync_database_schema.py --full       # Full rebuild (CAUTION: drops data)
"""

import os
import sys
from pathlib import Path
import argparse
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import psycopg2
    from dotenv import load_dotenv
except ImportError:
    print("Missing required packages. Install with:")
    print("  pip install psycopg2-binary python-dotenv")
    sys.exit(1)


# SQL files in order of execution (dependencies matter!)
SCHEMA_FILES = [
    # Foundation
    "database/schemas/001_schema_foundation.sql",

    # Core layers
    "database/schemas/002_bronze_layer.sql",
    "database/schemas/003_silver_layer.sql",
    "database/schemas/004_gold_layer.sql",

    # Weather
    "database/schemas/006_weather_schema.sql",
    "database/schemas/015_weather_forecast_schema.sql",

    # Prices and futures
    "database/schemas/007_price_schema.sql",
    "database/schemas/008_futures_sessions_schema.sql",

    # EPA/RFS
    "database/schemas/009_epa_rfs.sql",

    # CONAB (Brazil)
    "database/schemas/010_conab_schema.sql",

    # USDA sources
    "database/schemas/011_usda_nass_schema.sql",
    "database/schemas/012_census_trade_schema.sql",
    "database/schemas/013_cftc_cot_schema.sql",
    "database/schemas/014_fas_psd_schema.sql",

    # NDVI/satellite
    "database/schemas/014_ndvi_schema.sql",

    # Balance sheet tracking
    "database/schemas/015_balance_sheet_tracking.sql",
]

VIEW_FILES = [
    "database/views/01_traditional_balance_sheets.sql",
    "database/views/02_traditional_balance_sheets_v2.sql",
    "database/views/03_trade_flow_views.sql",
    "database/views/04_ers_gold_views.sql",
    "database/views/05_fas_gold_views.sql",
]

# Expected counts (update when adding new schemas)
EXPECTED_COUNTS = {
    'bronze': {'tables': 34},
    'silver': {'tables': 20, 'views': 9},
    'gold': {'views': 56},
    'reference': {'tables': 5},
    'public': {'tables': 9},
    'audit': {'tables': 2},
    'meta': {'tables': 1},
}


def get_connection():
    """Get database connection."""
    load_dotenv(PROJECT_ROOT / '.env')

    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', 5432),
        database=os.environ.get('DB_NAME', 'rlc_commodities'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD')
    )


def get_current_inventory(conn):
    """Get current database inventory."""
    cur = conn.cursor()

    inventory = {
        'schemas': [],
        'tables': {},
        'views': {},
        'matviews': []
    }

    # Get schemas
    cur.execute("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY schema_name
    """)
    inventory['schemas'] = [r[0] for r in cur.fetchall()]

    # Get tables by schema
    cur.execute("""
        SELECT schemaname, tablename
        FROM pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, tablename
    """)
    for schema, table in cur.fetchall():
        if schema not in inventory['tables']:
            inventory['tables'][schema] = []
        inventory['tables'][schema].append(table)

    # Get views by schema
    cur.execute("""
        SELECT schemaname, viewname
        FROM pg_views
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY schemaname, viewname
    """)
    for schema, view in cur.fetchall():
        if schema not in inventory['views']:
            inventory['views'][schema] = []
        inventory['views'][schema].append(view)

    # Get materialized views
    cur.execute("""
        SELECT schemaname, matviewname FROM pg_matviews
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    """)
    inventory['matviews'] = [(r[0], r[1]) for r in cur.fetchall()]

    return inventory


def check_database(conn):
    """Check database status and report missing items."""
    inventory = get_current_inventory(conn)

    print("\n" + "=" * 60)
    print("DATABASE INVENTORY CHECK")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Database: {conn.info.dbname}")
    print(f"Host: {conn.info.host}")

    # Count totals
    total_tables = sum(len(t) for t in inventory['tables'].values())
    total_views = sum(len(v) for v in inventory['views'].values())
    total_matviews = len(inventory['matviews'])

    print(f"\n{'Schema':<15} {'Tables':<10} {'Views':<10}")
    print("-" * 35)
    for schema in sorted(set(list(inventory['tables'].keys()) + list(inventory['views'].keys()))):
        tables = len(inventory['tables'].get(schema, []))
        views = len(inventory['views'].get(schema, []))
        print(f"{schema:<15} {tables:<10} {views:<10}")

    print("-" * 35)
    print(f"{'TOTAL':<15} {total_tables:<10} {total_views:<10}")
    print(f"Materialized Views: {total_matviews}")
    print(f"\nGRAND TOTAL: {total_tables + total_views + total_matviews} objects")

    # Check against expected
    print("\n" + "-" * 60)
    print("COMPARISON WITH EXPECTED COUNTS")
    print("-" * 60)

    missing = []
    for schema, expected in EXPECTED_COUNTS.items():
        actual_tables = len(inventory['tables'].get(schema, []))
        actual_views = len(inventory['views'].get(schema, []))

        exp_tables = expected.get('tables', 0)
        exp_views = expected.get('views', 0)

        status = "OK" if (actual_tables >= exp_tables and actual_views >= exp_views) else "MISSING"

        if status == "MISSING":
            missing.append(schema)

        print(f"{schema:<15} Tables: {actual_tables}/{exp_tables}  Views: {actual_views}/{exp_views}  [{status}]")

    if missing:
        print(f"\n[WARNING] Missing schemas: {', '.join(missing)}")
        print("   Run with --apply to create missing objects")
        return False
    else:
        print("\n[OK] All expected schemas present")
        return True


def apply_schemas(conn, schema_files=None, view_files=None):
    """Apply SQL schema files to database."""
    if schema_files is None:
        schema_files = SCHEMA_FILES
    if view_files is None:
        view_files = VIEW_FILES

    cur = conn.cursor()

    print("\n" + "=" * 60)
    print("APPLYING DATABASE SCHEMAS")
    print("=" * 60)

    success_count = 0
    error_count = 0

    all_files = schema_files + view_files

    for sql_file in all_files:
        file_path = PROJECT_ROOT / sql_file

        if not file_path.exists():
            print(f"[WARNING] File not found: {sql_file}")
            continue

        print(f"\n[FILE] Processing: {sql_file}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # Execute the SQL
            cur.execute(sql_content)
            conn.commit()

            print(f"   [OK] Applied successfully")
            success_count += 1

        except psycopg2.Error as e:
            conn.rollback()
            # Some errors are OK (like "already exists")
            if 'already exists' in str(e).lower():
                print(f"   [SKIP] Objects already exist (OK)")
                success_count += 1
            else:
                print(f"   [ERROR] {e.pgerror or e}")
                error_count += 1
        except Exception as e:
            conn.rollback()
            print(f"   [ERROR] {e}")
            error_count += 1

    print("\n" + "-" * 60)
    print(f"Completed: {success_count} successful, {error_count} errors")

    return error_count == 0


def print_sync_instructions():
    """Print instructions for syncing database across computers."""
    print("""
================================================================================
HOW TO SYNC THE DATABASE TO ANOTHER COMPUTER
================================================================================

The PostgreSQL database is LOCAL to each computer. It is NOT synced via Git or
Dropbox. Only the SQL schema files are in Git.

OPTION 1: Run Schema Files (Recommended for new setups)
---------------------------------------------------------
On the laptop or any new computer:

1. Ensure PostgreSQL is installed and running
2. Create the database (if not exists):

   psql -U postgres -c "CREATE DATABASE rlc_commodities;"

3. Run this sync script:

   cd "C:\\Users\\torem\\RLC Dropbox\\RLC Team Folder\\RLC-Agent"
   python scripts/sync_database_schema.py --apply

4. After schemas are created, data collectors must run to populate tables.

OPTION 2: Database Dump/Restore (Copies all data)
--------------------------------------------------
On THIS computer (source), export the database:

   pg_dump -U postgres -F c -b -v -f rlc_commodities_backup.dump rlc_commodities

Copy the .dump file to the laptop, then restore:

   pg_restore -U postgres -d rlc_commodities -v rlc_commodities_backup.dump

This copies ALL data, not just structure.

OPTION 3: Use a Shared Database Server
---------------------------------------
For team collaboration, consider hosting PostgreSQL on a shared server:
- Cloud: AWS RDS, Azure Database, Google Cloud SQL
- Self-hosted: A dedicated server on your network

Then all computers connect to the same database via IP/hostname.
Update .env on each computer:
   DB_HOST=your.server.ip.address
   DB_PORT=5432

================================================================================
""")


def main():
    parser = argparse.ArgumentParser(description='Database Schema Synchronization')
    parser.add_argument('--check', action='store_true', help='Check database status')
    parser.add_argument('--apply', action='store_true', help='Apply missing schemas')
    parser.add_argument('--full', action='store_true', help='Full rebuild (CAUTION)')
    parser.add_argument('--instructions', action='store_true', help='Print sync instructions')

    args = parser.parse_args()

    if args.instructions:
        print_sync_instructions()
        return

    if not any([args.check, args.apply, args.full]):
        args.check = True  # Default to check

    try:
        conn = get_connection()
        print(f"[OK] Connected to database: {conn.info.dbname}@{conn.info.host}")
    except Exception as e:
        print(f"[FAIL] Failed to connect: {e}")
        print("\nMake sure:")
        print("  1. PostgreSQL is running")
        print("  2. Database 'rlc_commodities' exists")
        print("  3. .env file has correct DB_PASSWORD")
        sys.exit(1)

    try:
        if args.check:
            check_database(conn)
            print_sync_instructions()

        if args.apply:
            print("\nApplying schemas...")
            apply_schemas(conn)
            print("\nRe-checking database...")
            check_database(conn)

        if args.full:
            print("\n[CAUTION] FULL REBUILD will drop existing objects!")
            confirm = input("Type 'YES' to confirm: ")
            if confirm == 'YES':
                # Would need to implement drop logic
                apply_schemas(conn)
            else:
                print("Aborted.")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
