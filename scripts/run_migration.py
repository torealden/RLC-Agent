#!/usr/bin/env python3
"""
Quick script to run a SQL migration file using .env credentials
"""
import os
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
import psycopg2

# Load .env
load_dotenv(PROJECT_ROOT / '.env')

def run_migration(sql_file: str):
    """Run a SQL file against the database"""

    # Get connection params from environment
    password = (os.environ.get('RLC_PG_PASSWORD') or
                os.environ.get('DATABASE_PASSWORD') or
                os.environ.get('DB_PASSWORD'))

    conn_params = {
        'host': os.environ.get('DATABASE_HOST', 'localhost'),
        'port': os.environ.get('DATABASE_PORT', '5432'),
        'database': os.environ.get('DATABASE_NAME', 'rlc_commodities'),
        'user': os.environ.get('DATABASE_USER', 'postgres'),
        'password': password
    }

    print(f"Connecting to {conn_params['database']} as {conn_params['user']}...")

    try:
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True
        cursor = conn.cursor()

        # Read and execute SQL file
        sql_path = Path(sql_file)
        if not sql_path.is_absolute():
            sql_path = PROJECT_ROOT / sql_file

        print(f"Running: {sql_path}")

        with open(sql_path, 'r') as f:
            sql = f.read()

        cursor.execute(sql)

        # Try to fetch results if any
        try:
            results = cursor.fetchall()
            if results:
                print("\nResults:")
                for row in results:
                    print(f"  {row}")
        except psycopg2.ProgrammingError:
            # No results to fetch
            pass

        print("\nMigration completed successfully!")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        # Default to the soybean meal migration
        sql_file = 'database/migrations/004_add_soybean_meal_import_codes.sql'
    else:
        sql_file = sys.argv[1]

    run_migration(sql_file)
