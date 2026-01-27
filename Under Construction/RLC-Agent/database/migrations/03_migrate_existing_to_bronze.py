"""
Step 3: Migrate existing PostgreSQL data to Bronze layer
Moves data from public schema tables to bronze schema.

Usage:
    python 03_migrate_existing_to_bronze.py
"""

import psycopg2
import psycopg2.extras
from datetime import datetime

# Database configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "rlc_commodities"
PG_USER = "postgres"
PG_PASSWORD = "SoupBoss1"


def migrate_to_bronze():
    """Migrate existing public schema data to Bronze layer."""

    print("=" * 60)
    print("Migrating Existing Data to Bronze Layer")
    print("=" * 60)

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        print(f"Connected to {PG_DATABASE}\n")

        # Check if bronze schema exists
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze'")
        if not cursor.fetchone():
            print("ERROR: Bronze schema does not exist!")
            print("Run 02_deploy_medallion_schema.py first.")
            return False

        # Get all tables in public schema with data
        cursor.execute("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        public_tables = [row[0] for row in cursor.fetchall()]

        if not public_tables:
            print("No tables found in public schema.")
            return True

        print(f"Found {len(public_tables)} tables in public schema:\n")

        migrated = []
        for table in public_tables:
            # Get row count
            cursor.execute(f'SELECT COUNT(*) FROM public."{table}"')
            count = cursor.fetchone()[0]

            if count == 0:
                print(f"  {table}: (empty, skipping)")
                continue

            print(f"  {table}: {count:,} rows")

            # Get column info
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table}'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()

            # Create bronze table if it doesn't exist
            bronze_table = f"bronze.raw_{table}"

            # Check if bronze table exists
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'bronze' AND table_name = 'raw_{table}'
                )
            """)
            exists = cursor.fetchone()[0]

            if not exists:
                # Create bronze table with same structure + audit columns
                col_defs = ", ".join([f'"{col}" {dtype}' for col, dtype in columns])

                create_sql = f"""
                    CREATE TABLE {bronze_table} (
                        bronze_id SERIAL PRIMARY KEY,
                        {col_defs},
                        source_table TEXT DEFAULT 'public.{table}',
                        migrated_at TIMESTAMPTZ DEFAULT NOW(),
                        ingest_run_id INTEGER
                    )
                """

                try:
                    cursor.execute(create_sql)
                    conn.commit()
                    print(f"    Created {bronze_table}")
                except psycopg2.Error as e:
                    print(f"    Error creating table: {e}")
                    conn.rollback()
                    continue

            # Copy data
            col_names = ", ".join([f'"{col}"' for col, _ in columns])

            insert_sql = f"""
                INSERT INTO {bronze_table} ({col_names}, source_table, migrated_at)
                SELECT {col_names}, 'public.{table}', NOW()
                FROM public."{table}"
            """

            try:
                cursor.execute(insert_sql)
                inserted = cursor.rowcount
                conn.commit()
                print(f"    Migrated {inserted:,} rows to {bronze_table}")
                migrated.append((table, inserted))
            except psycopg2.Error as e:
                print(f"    Error migrating: {e}")
                conn.rollback()

        # Summary
        print("\n" + "=" * 60)
        print("MIGRATION COMPLETE")
        print("=" * 60)
        total = sum(count for _, count in migrated)
        print(f"Migrated {len(migrated)} tables with {total:,} total rows")

        for table, count in migrated:
            print(f"  bronze.raw_{table}: {count:,} rows")

        print("\nNOTE: Original tables in 'public' schema are preserved.")
        print("You can drop them after verifying the migration.")

        conn.close()
        return True

    except psycopg2.OperationalError as e:
        print(f"\nCONNECTION FAILED: {e}")
        return False


if __name__ == "__main__":
    migrate_to_bronze()
