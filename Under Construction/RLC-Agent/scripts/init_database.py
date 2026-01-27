#!/usr/bin/env python3
"""
Database Initialization Script

Creates the commodity database schema and reference data.
Supports PostgreSQL and SQLite.

Usage:
    python scripts/init_database.py
    python scripts/init_database.py --drop-existing  # WARNING: Drops all data
    python scripts/init_database.py --sqlite ./data/commodity.db
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_database_connection(database_url: str = None):
    """Get database connection based on URL"""
    url = database_url or os.getenv('DATABASE_URL', 'sqlite:///./data/commodity.db')

    if url.startswith('postgresql'):
        try:
            import psycopg2
            # Parse PostgreSQL URL
            # Format: postgresql://user:password@host:port/dbname
            from urllib.parse import urlparse
            parsed = urlparse(url)
            conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port or 5432,
                database=parsed.path[1:],  # Remove leading /
                user=parsed.username,
                password=parsed.password
            )
            return conn, 'postgresql'
        except ImportError:
            logger.error("psycopg2 not installed. Run: pip install psycopg2-binary")
            sys.exit(1)

    elif url.startswith('sqlite'):
        import sqlite3
        db_path = url.replace('sqlite:///', '')
        os.makedirs(os.path.dirname(db_path) or '.', exist_ok=True)
        conn = sqlite3.connect(db_path)
        return conn, 'sqlite'

    else:
        raise ValueError(f"Unsupported database URL: {url}")


def read_migration_file(filepath: str, db_type: str) -> str:
    """Read and adapt migration SQL for database type"""
    import re

    with open(filepath, 'r') as f:
        sql = f.read()

    if db_type == 'sqlite':
        # Adapt PostgreSQL syntax to SQLite
        # Handle SERIAL/BIGSERIAL PRIMARY KEY - SQLite uses INTEGER PRIMARY KEY (no AUTOINCREMENT needed for rowid)
        sql = re.sub(r'BIGSERIAL\s+PRIMARY\s+KEY', 'INTEGER PRIMARY KEY', sql, flags=re.IGNORECASE)
        sql = re.sub(r'SERIAL\s+PRIMARY\s+KEY', 'INTEGER PRIMARY KEY', sql, flags=re.IGNORECASE)

        # Handle standalone SERIAL/BIGSERIAL (not followed by PRIMARY KEY)
        sql = re.sub(r'\bBIGSERIAL\b(?!\s+PRIMARY)', 'INTEGER', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bSERIAL\b(?!\s+PRIMARY)', 'INTEGER', sql, flags=re.IGNORECASE)

        # Other type conversions
        sql = sql.replace('TIMESTAMP DEFAULT CURRENT_TIMESTAMP', 'TEXT DEFAULT CURRENT_TIMESTAMP')
        sql = sql.replace('TIMESTAMP', 'TEXT')
        sql = re.sub(r'\bNUMERIC\b', 'REAL', sql)
        sql = sql.replace('JSONB', 'TEXT')
        sql = sql.replace('BOOLEAN', 'INTEGER')
        sql = re.sub(r'\btrue\b', '1', sql)
        sql = re.sub(r'\bfalse\b', '0', sql)

        # Remove PostgreSQL-specific ON CONFLICT clauses
        sql = re.sub(r'ON CONFLICT\s*\([^)]+\)\s*DO\s+NOTHING', '', sql, flags=re.IGNORECASE)

        # Remove CASCADE from DROP TABLE (SQLite doesn't support it)
        sql = re.sub(r'\s+CASCADE', '', sql, flags=re.IGNORECASE)

        # Remove IF NOT EXISTS from CREATE INDEX (older SQLite versions)
        # Actually keep it - modern SQLite supports it

    return sql


def execute_sql(conn, sql: str, db_type: str):
    """Execute SQL, handling multi-statement scripts"""
    cursor = conn.cursor()

    if db_type == 'postgresql':
        cursor.execute(sql)
    else:
        # SQLite needs statement-by-statement execution for some cases
        # Split on semicolons but be careful with strings
        statements = []
        current = []
        in_string = False

        for line in sql.split('\n'):
            # Skip comments
            stripped = line.strip()
            if stripped.startswith('--'):
                continue

            current.append(line)

            if ';' in line and not in_string:
                stmt = '\n'.join(current).strip()
                if stmt and not stmt.startswith('--'):
                    statements.append(stmt)
                current = []

        for stmt in statements:
            if stmt.strip():
                try:
                    cursor.execute(stmt)
                except Exception as e:
                    # Log but continue for SQLite (some PostgreSQL syntax won't work)
                    if 'syntax error' not in str(e).lower():
                        logger.warning(f"Statement failed: {str(e)[:100]}")

    conn.commit()
    cursor.close()


def drop_tables(conn, db_type: str):
    """Drop all commodity tables"""
    tables = [
        'collection_status',
        'collection_runs',
        'drought_data',
        'cot_positions',
        'energy_prices',
        'feedstock_prices',
        'cash_prices',
        'futures_settlements',
        'rin_data',
        'ethanol_data',
        'crush_data',
        'crop_progress',
        'supply_demand',
        'export_sales',
        'trade_flows',
        'data_sources',
        'countries',
        'commodities',
    ]

    cursor = conn.cursor()
    for table in tables:
        try:
            cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
            logger.info(f"Dropped table: {table}")
        except Exception as e:
            logger.warning(f"Could not drop {table}: {e}")

    conn.commit()
    cursor.close()


def verify_tables(conn, db_type: str):
    """Verify tables were created"""
    cursor = conn.cursor()

    if db_type == 'postgresql':
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
    else:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")

    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()

    return tables


def main():
    parser = argparse.ArgumentParser(description='Initialize commodity database')
    parser.add_argument('--database-url', help='Database URL (or use DATABASE_URL env var)')
    parser.add_argument('--sqlite', help='Path to SQLite database file')
    parser.add_argument('--drop-existing', action='store_true',
                       help='Drop existing tables before creating')
    parser.add_argument('--verify-only', action='store_true',
                       help='Only verify existing tables')

    args = parser.parse_args()

    # Determine database URL
    if args.sqlite:
        database_url = f'sqlite:///{args.sqlite}'
    else:
        database_url = args.database_url or os.getenv('DATABASE_URL')

    if not database_url:
        logger.error("No database URL provided. Set DATABASE_URL or use --database-url")
        sys.exit(1)

    logger.info(f"Connecting to database...")

    try:
        conn, db_type = get_database_connection(database_url)
        logger.info(f"Connected to {db_type} database")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)

    if args.verify_only:
        tables = verify_tables(conn, db_type)
        logger.info(f"Found {len(tables)} tables:")
        for t in tables:
            logger.info(f"  - {t}")
        conn.close()
        return

    if args.drop_existing:
        logger.warning("Dropping existing tables...")
        drop_tables(conn, db_type)

    # Find and execute migrations
    migrations_dir = Path(__file__).parent.parent / 'docs' / 'migrations'
    migration_files = sorted(migrations_dir.glob('*.sql'))

    if not migration_files:
        logger.error(f"No migration files found in {migrations_dir}")
        sys.exit(1)

    for migration_file in migration_files:
        logger.info(f"Applying migration: {migration_file.name}")
        try:
            sql = read_migration_file(str(migration_file), db_type)
            execute_sql(conn, sql, db_type)
            logger.info(f"  ✓ Applied successfully")
        except Exception as e:
            logger.error(f"  ✗ Failed: {e}")
            if db_type == 'postgresql':
                conn.rollback()

    # Verify
    tables = verify_tables(conn, db_type)
    logger.info(f"\nDatabase initialized with {len(tables)} tables:")

    expected_tables = [
        'commodities', 'countries', 'data_sources',
        'trade_flows', 'export_sales', 'supply_demand',
        'crop_progress', 'crush_data', 'ethanol_data',
        'rin_data', 'futures_settlements', 'cash_prices',
        'feedstock_prices', 'energy_prices', 'cot_positions',
        'drought_data', 'collection_runs', 'collection_status'
    ]

    for table in expected_tables:
        status = '✓' if table in tables else '✗'
        logger.info(f"  {status} {table}")

    conn.close()
    logger.info("\nDatabase initialization complete!")


if __name__ == '__main__':
    main()
