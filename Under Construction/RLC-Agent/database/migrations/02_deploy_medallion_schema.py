"""
Step 2: Deploy Bronze/Silver/Gold Medallion Schema
Creates the core, audit, bronze, silver, and gold schemas with all tables.

Usage:
    python 02_deploy_medallion_schema.py
"""

import psycopg2
from pathlib import Path
import sys

# Database configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "rlc_commodities"
PG_USER = "postgres"
PG_PASSWORD = "SoupBoss1"

# Path to SQL files
SCRIPT_DIR = Path(__file__).parent.parent
SQL_FILES = [
    "001_schema_foundation.sql",
    "002_bronze_layer.sql",
    "003_silver_layer.sql",
    "004_gold_layer.sql",
    "005_roles_and_security.sql",
]


def deploy_schema():
    """Deploy the medallion schema to PostgreSQL."""

    print("=" * 60)
    print("Deploying Medallion Schema (Bronze/Silver/Gold)")
    print("=" * 60)

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        conn.autocommit = True
        cursor = conn.cursor()
        print(f"Connected to {PG_DATABASE}\n")

        # Check existing schemas
        cursor.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name IN ('core', 'audit', 'bronze', 'silver', 'gold')
        """)
        existing = [row[0] for row in cursor.fetchall()]

        if existing:
            print(f"WARNING: These schemas already exist: {existing}")
            response = input("Continue and update? (y/N): ").strip().lower()
            if response != 'y':
                print("Aborted.")
                return False

        # Execute each SQL file in order
        for sql_file in SQL_FILES:
            sql_path = SCRIPT_DIR / sql_file

            if not sql_path.exists():
                print(f"WARNING: {sql_file} not found, skipping...")
                continue

            print(f"\nExecuting {sql_file}...")

            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # Split by statements and execute (handling functions with $$ delimiters)
            try:
                cursor.execute(sql_content)
                print(f"  OK")
            except psycopg2.Error as e:
                # Try executing statement by statement
                print(f"  Bulk execute failed, trying statement by statement...")

                # Simple split - won't work perfectly for all SQL but handles most cases
                statements = []
                current = []
                in_function = False

                for line in sql_content.split('\n'):
                    current.append(line)

                    if '$$' in line:
                        in_function = not in_function

                    if not in_function and line.strip().endswith(';'):
                        statements.append('\n'.join(current))
                        current = []

                success = 0
                errors = 0
                for stmt in statements:
                    stmt = stmt.strip()
                    if not stmt or stmt.startswith('--'):
                        continue
                    try:
                        cursor.execute(stmt)
                        success += 1
                    except psycopg2.Error as stmt_error:
                        # Ignore "already exists" errors
                        if 'already exists' in str(stmt_error):
                            pass
                        else:
                            errors += 1
                            print(f"    Error: {stmt_error}")

                print(f"  Executed {success} statements, {errors} errors")

        # Verify schemas were created
        cursor.execute("""
            SELECT schema_name FROM information_schema.schemata
            WHERE schema_name IN ('core', 'audit', 'bronze', 'silver', 'gold')
            ORDER BY schema_name
        """)
        created = [row[0] for row in cursor.fetchall()]

        print("\n" + "=" * 60)
        print("SCHEMA DEPLOYMENT COMPLETE")
        print("=" * 60)
        print(f"Created schemas: {created}")

        # Count tables per schema
        for schema in created:
            cursor.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = '{schema}'
            """)
            count = cursor.fetchone()[0]
            print(f"  {schema}: {count} tables")

        conn.close()

        print("\nNEXT STEPS:")
        print("  1. Run 03_migrate_to_bronze.py to move existing data")
        print("  2. Run 04_migrate_sqlite.py to import SQLite data")

        return True

    except psycopg2.OperationalError as e:
        print(f"\nCONNECTION FAILED: {e}")
        return False


if __name__ == "__main__":
    deploy_schema()
