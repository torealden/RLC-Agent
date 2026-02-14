#!/usr/bin/env python3
"""Run the EPA RFS schema to create database tables."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from dotenv import load_dotenv
import os

load_dotenv(PROJECT_ROOT / '.env')

def run_schema():
    """Execute the EPA RFS schema SQL file."""
    schema_file = PROJECT_ROOT / 'database' / 'schemas' / '009_epa_rfs.sql'

    if not schema_file.exists():
        print(f"Schema file not found: {schema_file}")
        return False

    # Read schema SQL
    with open(schema_file, 'r', encoding='utf-8') as f:
        sql = f.read()

    # Connect to database
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=int(os.environ.get('DB_PORT', '5432')),
        database=os.environ.get('DB_NAME', 'rlc_commodities'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', '')
    )

    cursor = conn.cursor()

    try:
        # Execute the entire schema as one script
        cursor.execute(sql)
        conn.commit()
        print("Schema executed successfully!")

        # Verify tables created
        cursor.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name LIKE 'epa_rfs%'
            ORDER BY table_schema, table_name
        """)

        tables = cursor.fetchall()
        print(f"\nEPA RFS tables created ({len(tables)}):")
        for schema, table in tables:
            print(f"  {schema}.{table}")

        # Check views
        cursor.execute("""
            SELECT table_schema, table_name
            FROM information_schema.views
            WHERE table_name LIKE '%rin%' OR table_name LIKE 'd4%' OR table_name LIKE 'd6%'
            ORDER BY table_schema, table_name
        """)

        views = cursor.fetchall()
        print(f"\nGold views created ({len(views)}):")
        for schema, view in views:
            print(f"  {schema}.{view}")

        return True

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        return False

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    success = run_schema()
    sys.exit(0 if success else 1)
