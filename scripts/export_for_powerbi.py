#!/usr/bin/env python3
"""
Export Data for Power BI

Exports commodity database tables to CSV or Excel format for use in
Power BI, Tableau, or other visualization tools.

Usage:
    python scripts/export_for_powerbi.py --format csv --output ./exports/
    python scripts/export_for_powerbi.py --format xlsx --output ./exports/data.xlsx
    python scripts/export_for_powerbi.py --tables cot_positions export_sales --format csv
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime
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


# Tables available for export with descriptions
EXPORT_TABLES = {
    'commodities': {
        'description': 'Master commodity reference table',
        'relationships': ['All other tables via commodity_code'],
    },
    'countries': {
        'description': 'Countries reference table',
        'relationships': ['trade_flows, export_sales via country codes'],
    },
    'trade_flows': {
        'description': 'International trade data (imports/exports)',
        'key_columns': ['commodity_code', 'reporter_country', 'partner_country', 'trade_date'],
    },
    'export_sales': {
        'description': 'USDA FAS weekly export sales',
        'key_columns': ['commodity_code', 'destination_country', 'week_ending', 'marketing_year'],
    },
    'supply_demand': {
        'description': 'Production Supply Demand balance sheets',
        'key_columns': ['commodity_code', 'country_code', 'marketing_year', 'report_date'],
    },
    'crop_progress': {
        'description': 'Crop planting/harvest progress and condition',
        'key_columns': ['commodity_code', 'region', 'week_ending'],
    },
    'crush_data': {
        'description': 'Oilseed crushing statistics',
        'key_columns': ['commodity_code', 'country_code', 'period_end'],
    },
    'ethanol_data': {
        'description': 'Weekly ethanol production and stocks',
        'key_columns': ['week_ending'],
    },
    'rin_data': {
        'description': 'EPA RIN generation by D-code',
        'key_columns': ['report_date', 'd_code'],
    },
    'futures_settlements': {
        'description': 'CME futures settlement prices',
        'key_columns': ['contract_symbol', 'contract_month', 'trade_date'],
    },
    'cash_prices': {
        'description': 'Cash/spot commodity prices',
        'key_columns': ['commodity_code', 'location', 'price_date'],
    },
    'feedstock_prices': {
        'description': 'Biofuel feedstock prices (tallow, grease, etc.)',
        'key_columns': ['commodity_code', 'location', 'price_date'],
    },
    'energy_prices': {
        'description': 'Petroleum and natural gas prices',
        'key_columns': ['commodity_code', 'price_date'],
    },
    'cot_positions': {
        'description': 'CFTC Commitments of Traders positioning',
        'key_columns': ['commodity_code', 'report_date'],
    },
    'drought_data': {
        'description': 'US Drought Monitor conditions',
        'key_columns': ['region', 'report_date'],
    },
}


# Analytical views for Power BI
POWERBI_VIEWS = {
    'v_weekly_export_summary': """
        SELECT
            commodity_code,
            week_ending,
            marketing_year,
            SUM(net_sales_week) as total_net_sales,
            SUM(shipments_week) as total_shipments,
            SUM(outstanding_sales) as total_outstanding,
            COUNT(DISTINCT destination_country) as num_destinations
        FROM export_sales
        GROUP BY commodity_code, week_ending, marketing_year
    """,
    'v_cot_net_positions': """
        SELECT
            commodity_code,
            report_date,
            noncommercial_net as spec_net,
            commercial_net as comm_net,
            open_interest,
            CASE WHEN open_interest > 0
                 THEN ROUND(CAST(noncommercial_net AS NUMERIC) / open_interest * 100, 2)
                 ELSE 0 END as spec_net_pct
        FROM cot_positions
        WHERE report_type = 'legacy' OR report_type IS NULL
    """,
    'v_ethanol_weekly': """
        SELECT
            week_ending,
            production_kbd,
            stocks_kb,
            implied_demand_kbd,
            CASE WHEN production_kbd > 0
                 THEN ROUND(CAST(stocks_kb AS NUMERIC) / (production_kbd * 7), 1)
                 ELSE 0 END as days_supply
        FROM ethanol_data
    """,
    'v_supply_demand_latest': """
        SELECT DISTINCT ON (commodity_code, country_code, marketing_year)
            commodity_code,
            country_code,
            marketing_year,
            report_date,
            beginning_stocks,
            production,
            imports,
            total_supply,
            domestic_use,
            exports,
            ending_stocks,
            stocks_to_use_ratio
        FROM supply_demand
        ORDER BY commodity_code, country_code, marketing_year, report_date DESC
    """,
}

# SQLite equivalent views (no DISTINCT ON)
SQLITE_VIEWS = {
    'v_supply_demand_latest': """
        SELECT
            commodity_code,
            country_code,
            marketing_year,
            MAX(report_date) as report_date,
            beginning_stocks,
            production,
            imports,
            total_supply,
            domestic_use,
            exports,
            ending_stocks,
            stocks_to_use_ratio
        FROM supply_demand
        GROUP BY commodity_code, country_code, marketing_year
    """,
}


def get_database_connection():
    """Get database connection"""
    database_url = os.getenv('DATABASE_URL', 'sqlite:///./data/commodity.db')

    if database_url.startswith('postgresql'):
        import psycopg2
        from urllib.parse import urlparse
        parsed = urlparse(database_url)
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:],
            user=parsed.username,
            password=parsed.password
        )
        return conn, 'postgresql'
    else:
        import sqlite3
        db_path = database_url.replace('sqlite:///', '')
        conn = sqlite3.connect(db_path)
        return conn, 'sqlite'


def get_table_data(conn, table_name: str, db_type: str, limit: int = None):
    """Fetch data from a table"""
    import pandas as pd

    query = f"SELECT * FROM {table_name}"
    if limit:
        query += f" LIMIT {limit}"

    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        logger.error(f"Error reading {table_name}: {e}")
        return None


def get_view_data(conn, view_name: str, view_sql: str, db_type: str):
    """Execute view query and return data"""
    import pandas as pd

    # Use SQLite-specific version if available
    if db_type == 'sqlite' and view_name in SQLITE_VIEWS:
        view_sql = SQLITE_VIEWS[view_name]

    try:
        df = pd.read_sql(view_sql, conn)
        return df
    except Exception as e:
        logger.error(f"Error executing view {view_name}: {e}")
        return None


def export_to_csv(conn, db_type: str, output_dir: str, tables: list = None, include_views: bool = True):
    """Export tables to CSV files"""
    import pandas as pd

    os.makedirs(output_dir, exist_ok=True)

    tables = tables or list(EXPORT_TABLES.keys())
    exported = []

    # Export tables
    for table in tables:
        if table not in EXPORT_TABLES:
            logger.warning(f"Unknown table: {table}")
            continue

        logger.info(f"Exporting {table}...")
        df = get_table_data(conn, table, db_type)

        if df is not None and not df.empty:
            filepath = os.path.join(output_dir, f"{table}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"  ✓ {len(df)} rows → {filepath}")
            exported.append({'table': table, 'rows': len(df), 'file': filepath})
        else:
            logger.warning(f"  ○ No data in {table}")

    # Export views
    if include_views:
        views_dir = os.path.join(output_dir, 'views')
        os.makedirs(views_dir, exist_ok=True)

        for view_name, view_sql in POWERBI_VIEWS.items():
            logger.info(f"Exporting view {view_name}...")
            df = get_view_data(conn, view_name, view_sql, db_type)

            if df is not None and not df.empty:
                filepath = os.path.join(views_dir, f"{view_name}.csv")
                df.to_csv(filepath, index=False)
                logger.info(f"  ✓ {len(df)} rows → {filepath}")
                exported.append({'table': view_name, 'rows': len(df), 'file': filepath})

    return exported


def export_to_excel(conn, db_type: str, output_file: str, tables: list = None, include_views: bool = True):
    """Export tables to Excel workbook"""
    import pandas as pd

    os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

    tables = tables or list(EXPORT_TABLES.keys())
    exported = []

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Export tables
        for table in tables:
            if table not in EXPORT_TABLES:
                logger.warning(f"Unknown table: {table}")
                continue

            logger.info(f"Exporting {table}...")
            df = get_table_data(conn, table, db_type)

            if df is not None and not df.empty:
                # Excel sheet names max 31 chars
                sheet_name = table[:31]
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                logger.info(f"  ✓ {len(df)} rows → sheet '{sheet_name}'")
                exported.append({'table': table, 'rows': len(df), 'sheet': sheet_name})
            else:
                logger.warning(f"  ○ No data in {table}")

        # Export views
        if include_views:
            for view_name, view_sql in POWERBI_VIEWS.items():
                logger.info(f"Exporting view {view_name}...")
                df = get_view_data(conn, view_name, view_sql, db_type)

                if df is not None and not df.empty:
                    sheet_name = view_name[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"  ✓ {len(df)} rows → sheet '{sheet_name}'")
                    exported.append({'table': view_name, 'rows': len(df), 'sheet': sheet_name})

    logger.info(f"\nExported to: {output_file}")
    return exported


def list_tables():
    """List available tables for export"""
    print("\n" + "="*70)
    print("TABLES AVAILABLE FOR EXPORT")
    print("="*70 + "\n")

    for table, info in EXPORT_TABLES.items():
        print(f"  {table}")
        print(f"    {info['description']}")
        if 'key_columns' in info:
            print(f"    Key columns: {', '.join(info['key_columns'])}")
        print()

    print("ANALYTICAL VIEWS")
    print("-"*40 + "\n")

    for view in POWERBI_VIEWS.keys():
        print(f"  {view}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description='Export commodity data for Power BI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/export_for_powerbi.py --list
  python scripts/export_for_powerbi.py --format csv --output ./exports/
  python scripts/export_for_powerbi.py --format xlsx --output ./exports/data.xlsx
  python scripts/export_for_powerbi.py --tables cot_positions export_sales --format csv
        """
    )

    parser.add_argument('--list', action='store_true',
                       help='List available tables')
    parser.add_argument('--format', '-f', choices=['csv', 'xlsx'], default='csv',
                       help='Export format (default: csv)')
    parser.add_argument('--output', '-o', required=False,
                       help='Output directory (csv) or file (xlsx)')
    parser.add_argument('--tables', '-t', nargs='+',
                       help='Specific tables to export')
    parser.add_argument('--no-views', action='store_true',
                       help='Skip exporting analytical views')

    args = parser.parse_args()

    if args.list:
        list_tables()
        return

    if not args.output:
        args.output = f"./exports/commodity_data.{args.format}" if args.format == 'xlsx' else './exports/'

    # Connect to database
    try:
        conn, db_type = get_database_connection()
        logger.info(f"Connected to {db_type} database")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

    # Export
    if args.format == 'csv':
        exported = export_to_csv(
            conn, db_type,
            output_dir=args.output,
            tables=args.tables,
            include_views=not args.no_views
        )
    else:
        exported = export_to_excel(
            conn, db_type,
            output_file=args.output,
            tables=args.tables,
            include_views=not args.no_views
        )

    conn.close()

    # Summary
    print("\n" + "="*70)
    print("EXPORT SUMMARY")
    print("="*70)
    print(f"Format: {args.format.upper()}")
    print(f"Output: {args.output}")
    print(f"Tables exported: {len(exported)}")
    print(f"Total rows: {sum(e['rows'] for e in exported):,}")


if __name__ == '__main__':
    main()
