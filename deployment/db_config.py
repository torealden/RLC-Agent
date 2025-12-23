"""
Database Configuration for RLC Agent
Supports both SQLite (development/backup) and PostgreSQL (production).

Usage:
    from db_config import get_connection, get_engine, DB_TYPE

    # For raw SQL with cursor
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM commodity_balance_sheets LIMIT 10")
        rows = cursor.fetchall()

    # For pandas/SQLAlchemy
    engine = get_engine()
    df = pd.read_sql("SELECT * FROM commodity_balance_sheets", engine)
"""

import os
import sys
from pathlib import Path
from contextlib import contextmanager

# =============================================================================
# CONFIGURATION
# =============================================================================

# Database type: "postgresql" or "sqlite"
# Can be overridden by environment variable RLC_DB_TYPE
DB_TYPE = os.environ.get("RLC_DB_TYPE", "postgresql")

# PostgreSQL settings (production)
PG_HOST = os.environ.get("RLC_PG_HOST", "localhost")
PG_PORT = os.environ.get("RLC_PG_PORT", "5432")
PG_DATABASE = os.environ.get("RLC_PG_DATABASE", "rlc_commodities")
PG_USER = os.environ.get("RLC_PG_USER", "postgres")
PG_PASSWORD = os.environ.get("RLC_PG_PASSWORD", "rlc2024!")

# SQLite settings (backup/development)
RLC_ROOT = Path("C:/RLC") if sys.platform == "win32" else Path("/home/user/RLC-Agent")
PROJECT_ROOT = RLC_ROOT / "projects" / "rlc-agent" if sys.platform == "win32" else Path("/home/user/RLC-Agent")
DATA_DIR = PROJECT_ROOT / "data"
SQLITE_PATH = DATA_DIR / "rlc_commodities.db"


def get_connection_string():
    """Get the database connection string based on configuration."""
    if DB_TYPE == "postgresql":
        return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    else:
        return f"sqlite:///{SQLITE_PATH}"


def get_engine():
    """
    Get SQLAlchemy engine for the configured database.
    Use for pandas operations and bulk inserts.
    """
    from sqlalchemy import create_engine
    return create_engine(get_connection_string())


@contextmanager
def get_connection():
    """
    Get a database connection (context manager).
    Works with both PostgreSQL and SQLite.

    Usage:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
    """
    conn = None
    try:
        if DB_TYPE == "postgresql":
            import psycopg2
            import psycopg2.extras
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                user=PG_USER,
                password=PG_PASSWORD
            )
            # Use RealDictCursor for dict-like row access
            conn.cursor_factory = psycopg2.extras.RealDictCursor
        else:
            import sqlite3
            conn = sqlite3.connect(str(SQLITE_PATH))
            conn.row_factory = sqlite3.Row

        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


def get_cursor(conn):
    """
    Get a cursor from a connection.
    For PostgreSQL, uses RealDictCursor for dict-like access.
    """
    if DB_TYPE == "postgresql":
        import psycopg2.extras
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        return conn.cursor()


def list_tables_query():
    """Get the SQL query to list all tables based on database type."""
    if DB_TYPE == "postgresql":
        return """
            SELECT table_name as name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """
    else:
        return "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"


def get_table_columns_query(table_name: str):
    """Get the SQL query to get column info for a table."""
    if DB_TYPE == "postgresql":
        return f"""
            SELECT column_name as name, data_type as type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
    else:
        return f"PRAGMA table_info({table_name})"


def format_column_info(row, db_type: str = None):
    """Format column info from query result based on database type."""
    db = db_type or DB_TYPE
    if db == "postgresql":
        return {"name": row["name"], "type": row["type"]}
    else:
        # SQLite PRAGMA returns: cid, name, type, notnull, dflt_value, pk
        return {"name": row[1], "type": row[2]}


def check_database():
    """
    Check if the database is accessible and return status info.
    """
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(list_tables_query())
            tables = cursor.fetchall()

            return {
                "connected": True,
                "type": DB_TYPE,
                "host": PG_HOST if DB_TYPE == "postgresql" else str(SQLITE_PATH),
                "database": PG_DATABASE if DB_TYPE == "postgresql" else "rlc_commodities.db",
                "tables": len(tables)
            }
    except Exception as e:
        return {
            "connected": False,
            "type": DB_TYPE,
            "error": str(e)
        }


def test_connection():
    """Test the database connection and print status."""
    print(f"Database Type: {DB_TYPE}")

    if DB_TYPE == "postgresql":
        print(f"Host: {PG_HOST}:{PG_PORT}")
        print(f"Database: {PG_DATABASE}")
    else:
        print(f"Path: {SQLITE_PATH}")

    status = check_database()

    if status["connected"]:
        print(f"Connected! Found {status['tables']} tables.")

        # List tables
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(list_tables_query())
            tables = cursor.fetchall()

            print("\nTables:")
            for t in tables:
                name = t["name"] if DB_TYPE == "postgresql" else t[0]
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {name}")
                count = cursor.fetchone()
                cnt = count["cnt"] if DB_TYPE == "postgresql" else count[0]
                print(f"  - {name}: {cnt:,} rows")
    else:
        print(f"Connection FAILED: {status['error']}")

    return status["connected"]


if __name__ == "__main__":
    test_connection()
