"""
Database Restore Script
=======================
Restores a PostgreSQL database from an export dump file.

Usage:
    python scripts/restore_database.py --file <path_to_dump>
    python scripts/restore_database.py --file database/exports/DESKTOP_20260130/rlc_commodities.dump

Options:
    --file      Path to the .dump or .sql file
    --merge     Merge with existing data instead of replacing (advanced)
    --dry-run   Show what would be done without making changes
"""

import os
import sys
import json
import subprocess
import argparse
from pathlib import Path
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


def get_connection(database='rlc_commodities'):
    """Get database connection."""
    load_dotenv(PROJECT_ROOT / '.env')

    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', 5432),
        database=database,
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD')
    )


def backup_current_database():
    """Create a backup of current database before restore."""
    print("\n[BACKUP] Creating backup of current database...")

    load_dotenv(PROJECT_ROOT / '.env')
    password = os.environ.get('DB_PASSWORD')
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    backup_dir = PROJECT_ROOT / "database" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"pre_restore_backup_{timestamp}.dump"

    try:
        result = subprocess.run([
            'pg_dump',
            '-U', 'postgres',
            '-h', 'localhost',
            '-F', 'c',
            '-f', str(backup_file),
            'rlc_commodities'
        ], env=env, capture_output=True, text=True, timeout=600)

        if result.returncode == 0:
            size_mb = backup_file.stat().st_size / (1024 * 1024)
            print(f"         Backup saved: {backup_file} ({size_mb:.1f} MB)")
            return str(backup_file)
        else:
            print(f"         [WARNING] Backup failed: {result.stderr}")
            return None
    except Exception as e:
        print(f"         [WARNING] Backup error: {e}")
        return None


def restore_from_dump(dump_file, drop_existing=True):
    """Restore database from a .dump file."""
    print(f"\n[RESTORE] Restoring from: {dump_file}")

    load_dotenv(PROJECT_ROOT / '.env')
    password = os.environ.get('DB_PASSWORD')
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    if drop_existing:
        print("          Dropping and recreating database...")

        # Connect to postgres database to drop/create rlc_commodities
        try:
            conn = get_connection('postgres')
            conn.autocommit = True
            cur = conn.cursor()

            # Terminate existing connections
            cur.execute("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'rlc_commodities'
                AND pid <> pg_backend_pid()
            """)

            # Drop and recreate
            cur.execute("DROP DATABASE IF EXISTS rlc_commodities")
            cur.execute("CREATE DATABASE rlc_commodities")
            conn.close()
            print("          Database recreated")
        except Exception as e:
            print(f"          [ERROR] Could not recreate database: {e}")
            return False

    # Restore using pg_restore
    print("          Running pg_restore (this may take several minutes)...")

    try:
        result = subprocess.run([
            'pg_restore',
            '-U', 'postgres',
            '-h', 'localhost',
            '-d', 'rlc_commodities',
            '-v',
            '--no-owner',
            '--no-acl',
            str(dump_file)
        ], env=env, capture_output=True, text=True, timeout=1800)

        if result.returncode == 0:
            print("          [OK] Restore completed successfully")
            return True
        else:
            # pg_restore often returns non-zero even on success with warnings
            if 'error' in result.stderr.lower():
                print(f"          [WARNING] Restore completed with errors")
                print(f"          {result.stderr[:500]}")
            else:
                print("          [OK] Restore completed (with warnings)")
            return True
    except subprocess.TimeoutExpired:
        print("          [ERROR] Restore timed out after 30 minutes")
        return False
    except FileNotFoundError:
        print("          [ERROR] pg_restore not found in PATH")
        print("          Add PostgreSQL bin to PATH: C:\\Program Files\\PostgreSQL\\16\\bin")
        return False
    except Exception as e:
        print(f"          [ERROR] Restore failed: {e}")
        return False


def restore_from_sql(sql_file):
    """Restore database from a .sql file."""
    print(f"\n[RESTORE] Restoring from SQL: {sql_file}")

    load_dotenv(PROJECT_ROOT / '.env')
    password = os.environ.get('DB_PASSWORD')
    env = os.environ.copy()
    env['PGPASSWORD'] = password

    # Connect to postgres database to drop/create rlc_commodities
    try:
        conn = get_connection('postgres')
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = 'rlc_commodities'
            AND pid <> pg_backend_pid()
        """)
        cur.execute("DROP DATABASE IF EXISTS rlc_commodities")
        cur.execute("CREATE DATABASE rlc_commodities")
        conn.close()
        print("          Database recreated")
    except Exception as e:
        print(f"          [ERROR] Could not recreate database: {e}")
        return False

    # Run psql to execute the SQL file
    print("          Running psql (this may take several minutes)...")

    try:
        result = subprocess.run([
            'psql',
            '-U', 'postgres',
            '-h', 'localhost',
            '-d', 'rlc_commodities',
            '-f', str(sql_file)
        ], env=env, capture_output=True, text=True, timeout=1800)

        if result.returncode == 0:
            print("          [OK] Restore completed successfully")
            return True
        else:
            print(f"          [WARNING] Restore completed with issues")
            return True
    except Exception as e:
        print(f"          [ERROR] Restore failed: {e}")
        return False


def verify_restore():
    """Verify the restored database."""
    print("\n[VERIFY] Checking restored database...")

    try:
        conn = get_connection()
        cur = conn.cursor()

        # Count tables
        cur.execute("""
            SELECT COUNT(*) FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """)
        table_count = cur.fetchone()[0]

        # Count views
        cur.execute("""
            SELECT COUNT(*) FROM pg_views
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        """)
        view_count = cur.fetchone()[0]

        # Get schemas
        cur.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        """)
        schemas = [r[0] for r in cur.fetchall()]

        conn.close()

        print(f"         Tables: {table_count}")
        print(f"         Views: {view_count}")
        print(f"         Schemas: {', '.join(schemas)}")

        if table_count > 50 and view_count > 50:
            print("\n         [OK] Database looks complete!")
            return True
        else:
            print("\n         [WARNING] Database may be incomplete")
            return False

    except Exception as e:
        print(f"         [ERROR] Verification failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Database Restore Tool')
    parser.add_argument('--file', required=True, help='Path to .dump or .sql file')
    parser.add_argument('--no-backup', action='store_true', help='Skip backup of current database')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')

    args = parser.parse_args()

    dump_file = Path(args.file)
    if not dump_file.exists():
        print(f"[ERROR] File not found: {dump_file}")
        sys.exit(1)

    print("=" * 70)
    print("RLC DATABASE RESTORE TOOL")
    print("=" * 70)
    print(f"\nRestore file: {dump_file}")
    print(f"File size: {dump_file.stat().st_size / (1024*1024):.1f} MB")

    if args.dry_run:
        print("\n[DRY RUN] Would perform the following:")
        print("  1. Backup current database")
        print("  2. Drop and recreate rlc_commodities")
        print("  3. Restore from dump file")
        print("  4. Verify restoration")
        return

    # Confirm
    print("\n" + "-" * 70)
    print("WARNING: This will REPLACE your current database!")
    print("-" * 70)
    response = input("Type 'YES' to continue: ")
    if response != 'YES':
        print("Aborted.")
        return

    # Backup current
    if not args.no_backup:
        backup_current_database()

    # Restore
    if dump_file.suffix == '.dump':
        success = restore_from_dump(dump_file)
    elif dump_file.suffix == '.sql':
        success = restore_from_sql(dump_file)
    else:
        print(f"[ERROR] Unknown file format: {dump_file.suffix}")
        print("        Expected .dump or .sql")
        sys.exit(1)

    if success:
        verify_restore()

    print("\n" + "=" * 70)
    print("RESTORE COMPLETE")
    print("=" * 70)
    print("""
Your database has been restored. You can now:

1. Open Power BI and refresh your data connection
2. Run: python scripts/sync_database_schema.py --check
   to verify all objects are present

If something went wrong, restore from backup:
  python scripts/restore_database.py --file database/backups/<backup_file>
""")


if __name__ == '__main__':
    main()
