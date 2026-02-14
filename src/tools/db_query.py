"""
Database Query Tool for LLM Analysis

This tool allows an LLM to query the RLC Commodities database.
It can be run directly from command line or imported as a module.

Usage:
    python db_query.py "SELECT * FROM gold.fas_us_corn_balance_sheet"
    python db_query.py --help

For LLM use, always use parameterized queries to prevent SQL injection.
"""

import os
import sys
import json
import argparse
from datetime import datetime, date
from decimal import Decimal

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def load_env():
    """Load environment variables from .env file."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ.setdefault(key, value)


def get_connection():
    """Get database connection."""
    load_env()
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'localhost'),
        port=os.environ.get('DB_PORT', '5432'),
        dbname=os.environ.get('DB_NAME', 'rlc_commodities'),
        user=os.environ.get('DB_USER', 'postgres'),
        password=os.environ.get('DB_PASSWORD', '')
    )


def json_serializer(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def query(sql: str, params: tuple = None, format: str = 'json', limit: int = 1000) -> str:
    """
    Execute a SQL query and return results.

    Args:
        sql: SQL query string
        params: Optional tuple of parameters for parameterized query
        format: Output format - 'json', 'csv', 'table', or 'pandas'
        limit: Maximum rows to return (default 1000)

    Returns:
        Query results in the specified format
    """
    # Add LIMIT if not present (safety measure)
    sql_upper = sql.upper().strip()
    if 'SELECT' in sql_upper and 'LIMIT' not in sql_upper:
        sql = f"{sql.rstrip(';')} LIMIT {limit}"

    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)

        # For SELECT queries
        if cur.description:
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            if format == 'pandas' and PANDAS_AVAILABLE:
                import pandas as pd
                return pd.DataFrame(rows, columns=columns)

            elif format == 'csv':
                lines = [','.join(columns)]
                for row in rows:
                    lines.append(','.join(str(row.get(c, '')) for c in columns))
                return '\n'.join(lines)

            elif format == 'table':
                # Simple ASCII table
                widths = {c: max(len(c), max(len(str(row.get(c, ''))) for row in rows) if rows else 0) for c in columns}
                header = ' | '.join(c.ljust(widths[c]) for c in columns)
                separator = '-+-'.join('-' * widths[c] for c in columns)
                lines = [header, separator]
                for row in rows:
                    lines.append(' | '.join(str(row.get(c, '')).ljust(widths[c]) for c in columns))
                return '\n'.join(lines)

            else:  # json
                return json.dumps(rows, indent=2, default=json_serializer)

        # For non-SELECT queries
        conn.commit()
        return json.dumps({"status": "success", "rowcount": cur.rowcount})

    except Exception as e:
        conn.rollback()
        return json.dumps({"error": str(e)})

    finally:
        conn.close()


def list_tables(schema: str = None) -> str:
    """List all tables in the database or a specific schema."""
    if schema:
        sql = """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """
        return query(sql, (schema,))
    else:
        sql = """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name
        """
        return query(sql)


def describe_table(table_name: str) -> str:
    """Get column information for a table."""
    # Parse schema.table format
    if '.' in table_name:
        schema, table = table_name.split('.', 1)
    else:
        schema = 'public'
        table = table_name

    sql = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    return query(sql, (schema, table))


def sample_data(table_name: str, n: int = 5) -> str:
    """Get sample rows from a table."""
    # Basic SQL injection prevention
    if not all(c.isalnum() or c in '._' for c in table_name):
        return json.dumps({"error": "Invalid table name"})
    return query(f"SELECT * FROM {table_name} LIMIT {n}")


# Pre-built analysis queries
ANALYSIS_QUERIES = {
    'us_corn_balance': "SELECT * FROM gold.fas_us_corn_balance_sheet ORDER BY marketing_year DESC LIMIT 5",

    'us_soy_balance': "SELECT * FROM gold.fas_us_soybeans_balance_sheet ORDER BY marketing_year DESC LIMIT 5",

    'global_corn_production': """
        SELECT country, country_code, marketing_year, production, exports, ending_stocks
        FROM bronze.fas_psd
        WHERE commodity = 'corn' AND marketing_year >= 2024
        ORDER BY production DESC
    """,

    'global_soy_production': """
        SELECT country, country_code, marketing_year, production, exports, ending_stocks
        FROM bronze.fas_psd
        WHERE commodity = 'soybeans' AND marketing_year >= 2024
        ORDER BY production DESC
    """,

    'brazil_soy_by_state': """
        SELECT state, crop_year, area_planted_ha, production_tonnes, yield_kg_ha
        FROM gold.brazil_soybean_production
        WHERE crop_year >= '2023/24'
        ORDER BY production_tonnes DESC
    """,

    'cftc_positioning': "SELECT * FROM gold.cftc_sentiment",

    'weather_latest': "SELECT * FROM gold.weather_latest ORDER BY state, location",

    'commodity_coverage': """
        SELECT commodity,
               COUNT(DISTINCT country_code) as countries,
               MIN(marketing_year) as min_year,
               MAX(marketing_year) as max_year,
               COUNT(*) as records
        FROM bronze.fas_psd
        GROUP BY commodity
        ORDER BY commodity
    """,

    'stocks_to_use': """
        SELECT commodity, country_code, marketing_year,
               ending_stocks,
               domestic_consumption + exports as total_use,
               ROUND(ending_stocks / NULLIF(domestic_consumption + exports, 0) * 100, 1) as stocks_use_pct
        FROM bronze.fas_psd
        WHERE country_code = 'US' AND marketing_year >= 2024
        ORDER BY commodity, marketing_year
    """,
}


def run_analysis(analysis_name: str) -> str:
    """Run a pre-built analysis query."""
    if analysis_name not in ANALYSIS_QUERIES:
        return json.dumps({
            "error": f"Unknown analysis: {analysis_name}",
            "available": list(ANALYSIS_QUERIES.keys())
        })
    return query(ANALYSIS_QUERIES[analysis_name])


def main():
    parser = argparse.ArgumentParser(description='Query RLC Commodities Database')
    parser.add_argument('sql', nargs='?', help='SQL query to execute')
    parser.add_argument('--format', '-f', choices=['json', 'csv', 'table'], default='table',
                        help='Output format (default: table)')
    parser.add_argument('--limit', '-l', type=int, default=100, help='Max rows (default: 100)')
    parser.add_argument('--tables', '-t', nargs='?', const='all', help='List tables (optionally specify schema)')
    parser.add_argument('--describe', '-d', help='Describe a table')
    parser.add_argument('--sample', '-s', help='Sample data from a table')
    parser.add_argument('--analysis', '-a', help='Run pre-built analysis')
    parser.add_argument('--list-analyses', action='store_true', help='List available analyses')

    args = parser.parse_args()

    if args.list_analyses:
        print("Available analyses:")
        for name in sorted(ANALYSIS_QUERIES.keys()):
            print(f"  {name}")
        return

    if args.tables:
        schema = None if args.tables == 'all' else args.tables
        print(list_tables(schema))
    elif args.describe:
        print(describe_table(args.describe))
    elif args.sample:
        print(sample_data(args.sample))
    elif args.analysis:
        print(run_analysis(args.analysis))
    elif args.sql:
        print(query(args.sql, format=args.format, limit=args.limit))
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
