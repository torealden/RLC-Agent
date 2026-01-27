"""
Step 1: Inventory existing PostgreSQL database
Run this first to see what data is already in your PostgreSQL database.

Usage:
    python 01_inventory_postgres.py
"""

import psycopg2
import psycopg2.extras
from pathlib import Path
import json

# Database configuration - update if different
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DATABASE = "rlc_commodities"
PG_USER = "postgres"
PG_PASSWORD = "SoupBoss1"

def inventory_database():
    """Connect to PostgreSQL and inventory all schemas, tables, and row counts."""

    print("=" * 60)
    print("RLC PostgreSQL Database Inventory")
    print("=" * 60)
    print(f"\nConnecting to {PG_DATABASE}@{PG_HOST}:{PG_PORT}...")

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
        print("Connected successfully!\n")

        # Get all schemas
        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
        """)
        schemas = [row[0] for row in cursor.fetchall()]

        print("SCHEMAS:")
        for s in schemas:
            print(f"  - {s}")

        # Check if medallion schemas exist
        medallion_schemas = ['core', 'audit', 'bronze', 'silver', 'gold']
        existing_medallion = [s for s in medallion_schemas if s in schemas]
        missing_medallion = [s for s in medallion_schemas if s not in schemas]

        print(f"\nMEDALLION SCHEMA STATUS:")
        print(f"  Existing: {existing_medallion if existing_medallion else 'None'}")
        print(f"  Missing:  {missing_medallion if missing_medallion else 'None (all deployed!)'}")

        # Get all tables with row counts
        cursor.execute("""
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schemaname, tablename
        """)
        tables = cursor.fetchall()

        print(f"\nTABLES ({len(tables)} total):")

        inventory = {}
        for schema, table in tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                count = cursor.fetchone()[0]

                if schema not in inventory:
                    inventory[schema] = {}
                inventory[schema][table] = count

                if count > 0:
                    print(f"  {schema}.{table}: {count:,} rows")
                else:
                    print(f"  {schema}.{table}: (empty)")
            except Exception as e:
                print(f"  {schema}.{table}: ERROR - {e}")
                conn.rollback()

        # Summary
        total_rows = sum(sum(t.values()) for t in inventory.values())
        tables_with_data = sum(1 for schema in inventory.values() for count in schema.values() if count > 0)

        print(f"\nSUMMARY:")
        print(f"  Total schemas: {len(schemas)}")
        print(f"  Total tables: {len(tables)}")
        print(f"  Tables with data: {tables_with_data}")
        print(f"  Total rows: {total_rows:,}")

        # Save inventory to file
        output = {
            "database": PG_DATABASE,
            "schemas": schemas,
            "medallion_status": {
                "existing": existing_medallion,
                "missing": missing_medallion
            },
            "tables": inventory,
            "summary": {
                "total_schemas": len(schemas),
                "total_tables": len(tables),
                "tables_with_data": tables_with_data,
                "total_rows": total_rows
            }
        }

        output_file = Path("postgres_inventory.json")
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\nInventory saved to: {output_file}")

        conn.close()

        # Recommendations
        print("\n" + "=" * 60)
        print("NEXT STEPS:")
        print("=" * 60)

        if missing_medallion:
            print("1. Run 02_deploy_medallion_schema.py to create Bronze/Silver/Gold schemas")
        else:
            print("1. Medallion schema already deployed!")

        if 'public' in inventory and inventory['public']:
            print("2. Run 03_migrate_to_bronze.py to move existing data to Bronze layer")

        print("3. Run 04_migrate_sqlite.py to import SQLite data to Bronze layer")

        return output

    except psycopg2.OperationalError as e:
        print(f"\nCONNECTION FAILED: {e}")
        print("\nTroubleshooting:")
        print("  1. Is PostgreSQL running?")
        print("  2. Check host/port/database/user/password")
        print("  3. Check pg_hba.conf allows connections")
        return None


if __name__ == "__main__":
    inventory_database()
