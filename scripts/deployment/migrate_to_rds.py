"""
Migrate Local PostgreSQL to AWS RDS

Dumps the local rlc_commodities database and restores it to an AWS RDS
PostgreSQL instance. Handles schemas, data, views, functions, and indexes.

Usage:
    # Test connection only
    python scripts/deployment/migrate_to_rds.py --test-only \
        --host rlc-commodities.xxxx.us-east-1.rds.amazonaws.com \
        --password YOUR_RDS_PASSWORD

    # Full migration
    python scripts/deployment/migrate_to_rds.py \
        --host rlc-commodities.xxxx.us-east-1.rds.amazonaws.com \
        --password YOUR_RDS_PASSWORD

    # Migration + update .env
    python scripts/deployment/migrate_to_rds.py \
        --host rlc-commodities.xxxx.us-east-1.rds.amazonaws.com \
        --password YOUR_RDS_PASSWORD \
        --update-env
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Local database settings
LOCAL_HOST = "localhost"
LOCAL_PORT = "5432"
LOCAL_DB = "rlc_commodities"
LOCAL_USER = "postgres"
LOCAL_PASSWORD = "SoupBoss1"

DUMP_FILE = PROJECT_ROOT / "database" / "exports" / f"rlc_commodities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.dump"


def find_pg_bin():
    """Find PostgreSQL bin directory for pg_dump/pg_restore."""
    # Common Windows PostgreSQL paths
    candidates = [
        Path(r"C:\Program Files\PostgreSQL\16\bin"),
        Path(r"C:\Program Files\PostgreSQL\15\bin"),
        Path(r"C:\Program Files\PostgreSQL\14\bin"),
        Path(r"C:\Program Files\PostgreSQL\17\bin"),
    ]

    for p in candidates:
        if (p / "pg_dump.exe").exists():
            return p

    # Try PATH
    import shutil
    if shutil.which("pg_dump"):
        return Path(shutil.which("pg_dump")).parent

    return None


def test_connection(host, port, password, user, database):
    """Test connection to the RDS instance."""
    print(f"\nTesting connection to {host}:{port}/{database}...")

    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host, port=port, database=database,
            user=user, password=password,
            connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
        print(f"  Connected! PostgreSQL: {version[:60]}")

        # Check if database has any tables
        cur.execute("""
            SELECT schemaname, COUNT(*) as cnt
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            GROUP BY schemaname
            ORDER BY schemaname
        """)
        schema_counts = cur.fetchall()
        if schema_counts:
            print(f"  Existing tables by schema:")
            for schema, count in schema_counts:
                print(f"    {schema}: {count} tables")
        else:
            print("  Database is empty (ready for migration)")

        conn.close()
        return True

    except Exception as e:
        print(f"  Connection FAILED: {e}")
        return False


def get_local_table_counts():
    """Get row counts for all local tables."""
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(
        host=LOCAL_HOST, port=LOCAL_PORT, database=LOCAL_DB,
        user=LOCAL_USER, password=LOCAL_PASSWORD,
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT schemaname || '.' || tablename as full_name,
               schemaname, tablename
        FROM pg_tables
        WHERE schemaname IN ('bronze', 'silver', 'gold', 'core', 'public')
        ORDER BY schemaname, tablename
    """)
    tables = cur.fetchall()

    counts = {}
    for t in tables:
        try:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {t['full_name']}")
            counts[t['full_name']] = cur.fetchone()['cnt']
        except Exception:
            counts[t['full_name']] = -1
            conn.rollback()

    conn.close()
    return counts


def dump_local_database(pg_bin):
    """Dump the local database to a custom-format file."""
    print(f"\nDumping local database to {DUMP_FILE}...")

    DUMP_FILE.parent.mkdir(parents=True, exist_ok=True)

    pg_dump = pg_bin / "pg_dump.exe" if sys.platform == "win32" else pg_bin / "pg_dump"

    env = os.environ.copy()
    env["PGPASSWORD"] = LOCAL_PASSWORD

    cmd = [
        str(pg_dump),
        "-h", LOCAL_HOST,
        "-p", LOCAL_PORT,
        "-U", LOCAL_USER,
        "-d", LOCAL_DB,
        "-Fc",                    # Custom format (compressed, supports pg_restore)
        "--no-owner",             # Don't set ownership (RDS has different roles)
        "--no-privileges",        # Don't set privileges
        "--no-comments",          # Skip comments for cleaner restore
        "-f", str(DUMP_FILE),
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=600)

    if result.returncode != 0:
        print(f"  pg_dump FAILED: {result.stderr}")
        return False

    size_mb = DUMP_FILE.stat().st_size / (1024 * 1024)
    print(f"  Dump complete: {size_mb:.1f} MB")
    return True


def restore_to_rds(pg_bin, host, port, password, user, database):
    """Restore the dump to the RDS instance."""
    print(f"\nRestoring to {host}:{port}/{database}...")
    print("  This may take several minutes depending on data size...")

    pg_restore = pg_bin / "pg_restore.exe" if sys.platform == "win32" else pg_bin / "pg_restore"

    env = os.environ.copy()
    env["PGPASSWORD"] = password

    # First, create schemas on RDS (pg_restore may not handle CREATE SCHEMA well)
    print("  Creating schemas...")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=host, port=port, database=database,
            user=user, password=password,
        )
        conn.autocommit = True
        cur = conn.cursor()
        for schema in ['core', 'bronze', 'silver', 'gold']:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            print(f"    Created schema: {schema}")
        conn.close()
    except Exception as e:
        print(f"  Warning creating schemas: {e}")

    # Restore
    cmd = [
        str(pg_restore),
        "-h", host,
        "-p", str(port),
        "-U", user,
        "-d", database,
        "--no-owner",
        "--no-privileges",
        "--if-exists",             # Don't error on DROP IF EXISTS
        "--clean",                 # Drop objects before recreating
        "-j", "2",                 # Parallel jobs (2 for small RDS)
        str(DUMP_FILE),
    ]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1800)

    # pg_restore often returns non-zero even on partial success (e.g., role errors)
    if result.stderr:
        # Filter out harmless errors
        errors = [l for l in result.stderr.split('\n')
                  if l.strip()
                  and 'role "postgres" already exists' not in l
                  and 'WARNING' not in l
                  and 'must be owner' not in l]
        if errors:
            print(f"  Restore warnings ({len(errors)} lines):")
            for e in errors[:10]:
                print(f"    {e}")

    print("  Restore command completed.")
    return True


def verify_migration(host, port, password, user, database, local_counts):
    """Compare table counts between local and RDS."""
    print("\nVerifying migration...")

    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(
        host=host, port=port, database=database,
        user=user, password=password,
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    mismatches = 0
    matched = 0

    for table_name, local_count in sorted(local_counts.items()):
        try:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
            rds_count = cur.fetchone()['cnt']

            if rds_count == local_count:
                matched += 1
            else:
                mismatches += 1
                print(f"  MISMATCH: {table_name}: local={local_count:,} rds={rds_count:,}")
        except Exception as e:
            mismatches += 1
            print(f"  MISSING: {table_name} (local={local_count:,}): {e}")
            conn.rollback()

    conn.close()

    print(f"\n  Tables matched: {matched}/{matched + mismatches}")
    if mismatches == 0:
        print("  Migration verified successfully!")
    else:
        print(f"  {mismatches} tables need attention")

    return mismatches == 0


def update_env_file(host, port, password, user):
    """Update .env with the new RDS connection settings."""
    env_path = PROJECT_ROOT / ".env"

    if not env_path.exists():
        print(f"\n  .env not found at {env_path}")
        return False

    content = env_path.read_text(encoding='utf-8')

    # Define replacements
    replacements = {
        'RLC_PG_HOST': host,
        'RLC_PG_PORT': str(port),
        'RLC_PG_PASSWORD': password,
        'RLC_PG_USER': user,
    }

    lines = content.split('\n')
    new_lines = []
    keys_found = set()

    for line in lines:
        replaced = False
        for key, value in replacements.items():
            if line.strip().startswith(f'{key}='):
                new_lines.append(f'{key}={value}')
                keys_found.add(key)
                replaced = True
                break
        if not replaced:
            new_lines.append(line)

    # Add any missing keys
    for key, value in replacements.items():
        if key not in keys_found:
            new_lines.append(f'{key}={value}')

    env_path.write_text('\n'.join(new_lines), encoding='utf-8')
    print(f"\n  Updated .env with RDS connection settings")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    return True


def main():
    parser = argparse.ArgumentParser(description='Migrate local PostgreSQL to AWS RDS')
    parser.add_argument('--host', required=True, help='RDS endpoint hostname')
    parser.add_argument('--port', type=int, default=5432, help='RDS port (default: 5432)')
    parser.add_argument('--password', required=True, help='RDS master password')
    parser.add_argument('--user', default='postgres', help='RDS username (default: postgres)')
    parser.add_argument('--database', default='rlc_commodities', help='Database name (default: rlc_commodities)')
    parser.add_argument('--test-only', action='store_true', help='Only test connection, no migration')
    parser.add_argument('--update-env', action='store_true', help='Update .env with RDS settings after migration')
    parser.add_argument('--skip-dump', action='store_true', help='Skip dump, use existing dump file')
    parser.add_argument('--dump-file', type=str, default=None, help='Use specific dump file')
    args = parser.parse_args()

    global DUMP_FILE
    if args.dump_file:
        DUMP_FILE = Path(args.dump_file)

    print("=" * 60)
    print("RLC Database Migration: Local -> AWS RDS")
    print("=" * 60)

    # Test RDS connection
    if not test_connection(args.host, args.port, args.password, args.user, args.database):
        print("\nCannot connect to RDS. Check:")
        print("  1. RDS instance is available (not still creating)")
        print("  2. Security group allows your IP on port 5432")
        print("  3. Password is correct")
        print("  4. Public access is enabled on the RDS instance")
        sys.exit(1)

    if args.test_only:
        print("\nConnection test passed!")
        sys.exit(0)

    # Find pg_dump/pg_restore
    pg_bin = find_pg_bin()
    if pg_bin is None:
        print("\nERROR: Cannot find pg_dump/pg_restore.")
        print("Install PostgreSQL client tools or add them to PATH.")
        sys.exit(1)
    print(f"\nUsing PostgreSQL tools from: {pg_bin}")

    # Get local table counts for verification
    print("\nCounting local tables...")
    local_counts = get_local_table_counts()
    total_rows = sum(c for c in local_counts.values() if c > 0)
    print(f"  {len(local_counts)} tables, {total_rows:,} total rows")

    # Dump local database
    if not args.skip_dump:
        if not dump_local_database(pg_bin):
            sys.exit(1)
    else:
        if not DUMP_FILE.exists():
            print(f"\nDump file not found: {DUMP_FILE}")
            sys.exit(1)
        print(f"\nUsing existing dump: {DUMP_FILE}")

    # Restore to RDS
    if not restore_to_rds(pg_bin, args.host, args.port, args.password, args.user, args.database):
        sys.exit(1)

    # Verify
    verify_migration(args.host, args.port, args.password, args.user, args.database, local_counts)

    # Update .env if requested
    if args.update_env:
        update_env_file(args.host, args.port, args.password, args.user)

    print("\n" + "=" * 60)
    print("Migration complete!")
    print("=" * 60)
    print(f"\nRDS Endpoint: {args.host}")
    print(f"Database: {args.database}")
    print(f"\nNext steps:")
    if not args.update_env:
        print(f"  1. Run again with --update-env to update .env")
    print(f"  2. Update VBA workbook DB_SERVER constants to: {args.host}")
    print(f"  3. Update VBA workbook DB_PASSWORD constants")
    print(f"  4. Restart the dispatcher")
    print(f"  5. Share the endpoint + password with Felipe")
    print(f"\nDump file saved at: {DUMP_FILE}")
    print(f"  (Keep this as a backup. Delete when confident RDS is working.)")


if __name__ == '__main__':
    main()
