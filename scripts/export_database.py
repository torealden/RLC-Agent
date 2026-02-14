"""
Database Export Script
======================
Exports the PostgreSQL database structure and data to portable files.

This creates:
1. A SQL dump of all schemas, tables, views, and data
2. A JSON inventory of what's in the database
3. Individual CSV exports of key tables (optional)

Usage:
    python scripts/export_database.py

Output will be saved to: database/exports/<computername>_<date>/
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
import socket

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


def get_computer_name():
    """Get a safe computer name for the export folder."""
    name = socket.gethostname()
    # Make it filesystem-safe
    safe_name = "".join(c if c.isalnum() else "_" for c in name)
    return safe_name[:20]  # Limit length


def get_connection():
    """Get database connection."""
    load_dotenv(PROJECT_ROOT / '.env')

    password = os.environ.get('DB_PASSWORD')
    if not password:
        print("[ERROR] DB_PASSWORD not found in .env file")
        print("Please ensure your .env file contains: DB_PASSWORD=your_password")
        sys.exit(1)

    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', 5432),
        database=os.environ.get('DB_NAME', 'rlc_commodities'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=password
    )


def get_database_inventory(conn):
    """Get complete inventory of database objects."""
    cur = conn.cursor()

    inventory = {
        'export_date': datetime.now().isoformat(),
        'computer_name': get_computer_name(),
        'database': 'rlc_commodities',
        'schemas': [],
        'tables': {},
        'views': {},
        'table_row_counts': {},
        'total_tables': 0,
        'total_views': 0,
        'total_rows': 0
    }

    # Get schemas
    cur.execute("""
        SELECT schema_name FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY schema_name
    """)
    inventory['schemas'] = [r[0] for r in cur.fetchall()]

    # Get tables by schema with row counts
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
        inventory['total_tables'] += 1

        # Get row count
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
            count = cur.fetchone()[0]
            inventory['table_row_counts'][f"{schema}.{table}"] = count
            inventory['total_rows'] += count
        except:
            inventory['table_row_counts'][f"{schema}.{table}"] = -1

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
        inventory['total_views'] += 1

    # Get materialized views
    cur.execute("""
        SELECT schemaname, matviewname FROM pg_matviews
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
    """)
    inventory['materialized_views'] = [f"{r[0]}.{r[1]}" for r in cur.fetchall()]

    return inventory


def export_database():
    """Main export function."""
    print("=" * 70)
    print("RLC DATABASE EXPORT TOOL")
    print("=" * 70)

    # Create export directory
    computer_name = get_computer_name()
    date_str = datetime.now().strftime("%Y%m%d")
    export_dir = PROJECT_ROOT / "database" / "exports" / f"{computer_name}_{date_str}"
    export_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nComputer: {computer_name}")
    print(f"Export directory: {export_dir}")

    # Step 1: Connect and get inventory
    print("\n[1/3] Connecting to database and getting inventory...")
    try:
        conn = get_connection()
        print(f"      Connected to: {conn.info.dbname}@{conn.info.host}")
    except Exception as e:
        print(f"[ERROR] Failed to connect: {e}")
        print("\nMake sure:")
        print("  1. PostgreSQL is running")
        print("  2. Database 'rlc_commodities' exists")
        print("  3. .env file has correct DB_PASSWORD")
        sys.exit(1)

    inventory = get_database_inventory(conn)
    conn.close()

    # Save inventory
    inventory_file = export_dir / "inventory.json"
    with open(inventory_file, 'w') as f:
        json.dump(inventory, f, indent=2)
    print(f"      Saved inventory: {inventory_file.name}")
    print(f"      Found: {inventory['total_tables']} tables, {inventory['total_views']} views, {inventory['total_rows']:,} total rows")

    # Step 2: Create pg_dump export
    print("\n[2/3] Creating database dump (this may take a minute)...")

    load_dotenv(PROJECT_ROOT / '.env')
    password = os.environ.get('DB_PASSWORD')

    dump_file = export_dir / "rlc_commodities.dump"
    sql_file = export_dir / "rlc_commodities.sql"

    # Set password in environment for pg_dump
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    # Try custom format first (more efficient for restore)
    try:
        result = subprocess.run([
            'pg_dump',
            '-U', 'postgres',
            '-h', 'localhost',
            '-F', 'c',  # Custom format
            '-b',       # Include blobs
            '-v',       # Verbose
            '-f', str(dump_file),
            'rlc_commodities'
        ], env=env, capture_output=True, text=True, timeout=600)

        if result.returncode == 0:
            size_mb = dump_file.stat().st_size / (1024 * 1024)
            print(f"      Created: {dump_file.name} ({size_mb:.1f} MB)")
        else:
            print(f"      [WARNING] pg_dump failed: {result.stderr}")
            dump_file = None
    except FileNotFoundError:
        print("      [WARNING] pg_dump not found in PATH")
        print("      Make sure PostgreSQL bin directory is in your PATH")
        print("      Usually: C:\\Program Files\\PostgreSQL\\16\\bin")
        dump_file = None
    except Exception as e:
        print(f"      [WARNING] pg_dump error: {e}")
        dump_file = None

    # Also create plain SQL format (human readable, useful for debugging)
    try:
        result = subprocess.run([
            'pg_dump',
            '-U', 'postgres',
            '-h', 'localhost',
            '-F', 'p',  # Plain SQL format
            '--no-owner',
            '--no-acl',
            '-f', str(sql_file),
            'rlc_commodities'
        ], env=env, capture_output=True, text=True, timeout=600)

        if result.returncode == 0:
            size_mb = sql_file.stat().st_size / (1024 * 1024)
            print(f"      Created: {sql_file.name} ({size_mb:.1f} MB)")
        else:
            print(f"      [WARNING] SQL dump failed")
    except Exception as e:
        print(f"      [WARNING] SQL dump error: {e}")

    # Step 3: Summary
    print("\n[3/3] Export complete!")
    print("\n" + "-" * 70)
    print("EXPORT SUMMARY")
    print("-" * 70)
    print(f"Location: {export_dir}")
    print(f"\nFiles created:")
    for f in export_dir.iterdir():
        size = f.stat().st_size
        if size > 1024 * 1024:
            size_str = f"{size / (1024*1024):.1f} MB"
        elif size > 1024:
            size_str = f"{size / 1024:.1f} KB"
        else:
            size_str = f"{size} bytes"
        print(f"  - {f.name} ({size_str})")

    print(f"\nDatabase contents:")
    print(f"  Schemas: {', '.join(inventory['schemas'])}")
    print(f"  Tables: {inventory['total_tables']}")
    print(f"  Views: {inventory['total_views']}")
    print(f"  Total rows: {inventory['total_rows']:,}")

    # Tables with most data
    print(f"\nLargest tables:")
    sorted_tables = sorted(inventory['table_row_counts'].items(), key=lambda x: x[1], reverse=True)
    for table, count in sorted_tables[:10]:
        if count > 0:
            print(f"  {table}: {count:,} rows")

    print("\n" + "=" * 70)
    print("NEXT STEPS")
    print("=" * 70)
    print("""
1. Share this export folder with the team (via Dropbox)
   The folder is already in the RLC-Agent repository.

2. The key file for restoration is:
   - rlc_commodities.dump (binary, efficient)
   - rlc_commodities.sql (readable backup)

3. To restore on another computer, use:
   python scripts/restore_database.py --file <path_to_dump>
""")

    return str(export_dir)


if __name__ == '__main__':
    export_database()
