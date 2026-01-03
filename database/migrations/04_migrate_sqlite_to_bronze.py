"""
Step 4: Migrate SQLite data to PostgreSQL Bronze layer
Imports data from local SQLite database to bronze schema.

Usage:
    python 04_migrate_sqlite_to_bronze.py
"""

import sqlite3
import psycopg2
import psycopg2.extras
from pathlib import Path
from datetime import datetime

# Database configuration
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "rlc_commodities"
PG_USER = "postgres"
PG_PASSWORD = "SoupBoss1"

# SQLite path - adjust if different on your system
SQLITE_PATH = Path(__file__).parent.parent.parent / "data" / "rlc_commodities.db"

# Batch size for inserts
BATCH_SIZE = 5000


def migrate_sqlite_to_bronze():
    """Migrate SQLite data to PostgreSQL Bronze layer."""

    print("=" * 60)
    print("Migrating SQLite Data to PostgreSQL Bronze Layer")
    print("=" * 60)

    # Find SQLite database
    sqlite_path = SQLITE_PATH

    if not sqlite_path.exists():
        # Try alternate paths
        alt_paths = [
            Path("C:/RLC/projects/rlc-agent/data/rlc_commodities.db"),
            Path("./data/rlc_commodities.db"),
            Path("../data/rlc_commodities.db"),
            Path("../../data/rlc_commodities.db"),
        ]
        for alt in alt_paths:
            if alt.exists():
                sqlite_path = alt
                break
        else:
            print(f"ERROR: SQLite database not found at {SQLITE_PATH}")
            print("Tried alternate paths:")
            for p in alt_paths:
                print(f"  - {p}")
            print("Please update SQLITE_PATH in this script.")
            return False

    print(f"SQLite: {sqlite_path}")

    # Connect to SQLite
    sqlite_conn = sqlite3.connect(str(sqlite_path))
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cursor = sqlite_conn.cursor()

    # Get SQLite tables with data
    sqlite_cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    sqlite_tables = [row[0] for row in sqlite_cursor.fetchall()]

    print(f"\nFound {len(sqlite_tables)} tables in SQLite:")
    for table in sqlite_tables:
        sqlite_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = sqlite_cursor.fetchone()[0]
        print(f"  {table}: {count:,} rows")

    # Connect to PostgreSQL
    try:
        pg_conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        pg_cursor = pg_conn.cursor()
        print(f"\nConnected to PostgreSQL: {PG_DATABASE}")
    except psycopg2.OperationalError as e:
        print(f"\nPostgreSQL connection failed: {e}")
        return False

    # Check bronze schema exists
    pg_cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze'")
    if not pg_cursor.fetchone():
        print("ERROR: Bronze schema does not exist!")
        print("Run 02_deploy_medallion_schema.py first.")
        return False

    # Migrate each table
    migrated = []

    for table in sqlite_tables:
        # Get row count
        sqlite_cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
        count = sqlite_cursor.fetchone()[0]

        if count == 0:
            print(f"\n{table}: (empty, skipping)")
            continue

        print(f"\n{table}: Migrating {count:,} rows...")

        # Get column info from SQLite
        sqlite_cursor.execute(f'PRAGMA table_info("{table}")')
        columns_info = sqlite_cursor.fetchall()
        columns = [col[1] for col in columns_info]  # column names
        col_types = {col[1]: col[2] for col in columns_info}  # name -> type mapping

        # Map SQLite types to PostgreSQL
        type_map = {
            'INTEGER': 'BIGINT',
            'REAL': 'DOUBLE PRECISION',
            'TEXT': 'TEXT',
            'BLOB': 'BYTEA',
            'TIMESTAMP': 'TIMESTAMPTZ',
            'DATETIME': 'TIMESTAMPTZ',
            'DATE': 'DATE',
            'BOOLEAN': 'BOOLEAN',
            '': 'TEXT',  # Default
        }

        # Create bronze table
        bronze_table = f"bronze.sqlite_{table}"

        # Build column definitions
        col_defs = []
        for col in columns:
            sqlite_type = col_types.get(col, '').upper()
            pg_type = type_map.get(sqlite_type, 'TEXT')
            col_defs.append(f'"{col}" {pg_type}')

        # Check if table exists
        pg_cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'bronze' AND table_name = 'sqlite_{table}'
            )
        """)
        exists = pg_cursor.fetchone()[0]

        if not exists:
            create_sql = f"""
                CREATE TABLE {bronze_table} (
                    bronze_id SERIAL PRIMARY KEY,
                    {", ".join(col_defs)},
                    source_database TEXT DEFAULT 'sqlite',
                    source_table TEXT DEFAULT '{table}',
                    migrated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """
            try:
                pg_cursor.execute(create_sql)
                pg_conn.commit()
                print(f"  Created {bronze_table}")
            except psycopg2.Error as e:
                print(f"  Error creating table: {e}")
                pg_conn.rollback()
                continue
        else:
            print(f"  Table {bronze_table} exists, appending...")

        # Fetch and insert data in batches
        col_names = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))

        insert_sql = f"""
            INSERT INTO {bronze_table} ({col_names}, source_database, source_table, migrated_at)
            VALUES ({placeholders}, 'sqlite', '{table}', NOW())
        """

        sqlite_cursor.execute(f'SELECT * FROM "{table}"')

        batch = []
        inserted = 0

        for row in sqlite_cursor:
            batch.append(tuple(row))

            if len(batch) >= BATCH_SIZE:
                try:
                    pg_cursor.executemany(insert_sql, batch)
                    pg_conn.commit()
                    inserted += len(batch)
                    print(f"  Inserted {inserted:,} rows...", end='\r')
                except psycopg2.Error as e:
                    print(f"  Error inserting batch: {e}")
                    pg_conn.rollback()
                batch = []

        # Insert remaining rows
        if batch:
            try:
                pg_cursor.executemany(insert_sql, batch)
                pg_conn.commit()
                inserted += len(batch)
            except psycopg2.Error as e:
                print(f"  Error inserting final batch: {e}")
                pg_conn.rollback()

        print(f"  Migrated {inserted:,} rows to {bronze_table}")
        migrated.append((table, inserted))

    # Close connections
    sqlite_conn.close()
    pg_conn.close()

    # Summary
    print("\n" + "=" * 60)
    print("SQLITE MIGRATION COMPLETE")
    print("=" * 60)
    total = sum(count for _, count in migrated)
    print(f"Migrated {len(migrated)} tables with {total:,} total rows")

    for table, count in migrated:
        print(f"  bronze.sqlite_{table}: {count:,} rows")

    print("\nData is now in Bronze layer. Next steps:")
    print("  1. Verify data in Power BI")
    print("  2. Run silver layer transformations")
    print("  3. Create gold views for reporting")

    return True


if __name__ == "__main__":
    migrate_sqlite_to_bronze()
