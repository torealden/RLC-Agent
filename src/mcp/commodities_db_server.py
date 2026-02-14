#!/usr/bin/env python3
"""
RLC Commodities Database MCP Server

This MCP server provides Claude with direct access to the agricultural commodities database.
It enables autonomous data exploration, analysis, and insight generation.

To use with Claude Code, add to your MCP settings:
{
    "mcpServers": {
        "commodities-db": {
            "command": "python",
            "args": ["path/to/src/mcp/commodities_db_server.py"]
        }
    }
}

Tools provided:
- query_database: Execute SQL queries
- list_tables: List available tables
- describe_table: Get table schema
- get_balance_sheet: Get S&D balance sheet for commodity/country
- get_production_ranking: Get global production rankings
- get_stocks_to_use: Calculate stocks-to-use ratios
- get_trade_flows: Get export/import data
- analyze_supply_demand: Run comprehensive S&D analysis
"""

import os
import sys
import json
import asyncio
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Optional

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    print("MCP library not installed. Install with: pip install mcp", file=sys.stderr)

import psycopg2
import psycopg2.extras


def load_env():
    """Load environment variables from .env file."""
    env_path = os.path.join(PROJECT_ROOT, '.env')
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


def execute_query(sql: str, params: tuple = None, limit: int = 500) -> dict:
    """Execute a SQL query and return results as dict."""
    # Safety: add LIMIT if not present for SELECT queries
    sql_upper = sql.upper().strip()
    if sql_upper.startswith('SELECT') and 'LIMIT' not in sql_upper:
        sql = f"{sql.rstrip(';')} LIMIT {limit}"

    # Safety: block dangerous operations
    dangerous = ['DROP', 'DELETE', 'TRUNCATE', 'ALTER', 'CREATE', 'INSERT', 'UPDATE']
    if any(d in sql_upper for d in dangerous):
        return {"error": "Only SELECT queries are allowed"}

    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)

        if cur.description:
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return {
                "columns": columns,
                "rows": [dict(row) for row in rows],
                "row_count": len(rows)
            }
        return {"status": "success", "rowcount": cur.rowcount}

    except Exception as e:
        return {"error": str(e)}

    finally:
        conn.close()


# ============================================================================
# TOOL IMPLEMENTATIONS
# ============================================================================

def query_database(sql: str, limit: int = 100) -> str:
    """
    Execute a SQL query against the commodities database.

    Args:
        sql: SELECT query to execute
        limit: Maximum rows to return (default 100)

    Returns:
        JSON string with query results
    """
    result = execute_query(sql, limit=limit)
    return json.dumps(result, indent=2, default=json_serializer)


def list_tables(schema: str = None) -> str:
    """
    List all tables in the database.

    Args:
        schema: Optional schema filter ('bronze', 'silver', 'gold')

    Returns:
        JSON list of tables with row counts
    """
    if schema:
        sql = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """
        result = execute_query(sql, (schema,), limit=200)
    else:
        sql = """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name
        """
        result = execute_query(sql, limit=200)

    return json.dumps(result, indent=2, default=json_serializer)


def describe_table(table_name: str) -> str:
    """
    Get schema information for a table.

    Args:
        table_name: Table name (can be schema.table format)

    Returns:
        JSON with column information
    """
    if '.' in table_name:
        schema, table = table_name.split('.', 1)
    else:
        schema = 'bronze'
        table = table_name

    sql = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    result = execute_query(sql, (schema, table), limit=100)
    return json.dumps(result, indent=2, default=json_serializer)


def get_balance_sheet(commodity: str, country: str = 'US', years: int = 3) -> str:
    """
    Get supply & demand balance sheet for a commodity/country.

    Args:
        commodity: Commodity name (corn, soybeans, wheat, etc.)
        country: Country code (US, BR, AR, CH, E4, etc.)
        years: Number of years to return

    Returns:
        JSON balance sheet data
    """
    sql = """
        SELECT
            marketing_year,
            area_harvested,
            yield as yield_mt_ha,
            beginning_stocks,
            production,
            imports,
            total_supply,
            feed_dom_consumption,
            fsi_consumption,
            crush,
            domestic_consumption,
            exports,
            total_distribution,
            ending_stocks,
            ROUND(ending_stocks / NULLIF(total_distribution, 0) * 100, 1) as stocks_use_pct,
            unit
        FROM bronze.fas_psd
        WHERE commodity = %s AND country_code = %s
        ORDER BY marketing_year DESC
        LIMIT %s
    """
    result = execute_query(sql, (commodity.lower(), country.upper(), years), limit=years)
    return json.dumps(result, indent=2, default=json_serializer)


def get_production_ranking(commodity: str, year: int = 2025, top_n: int = 15) -> str:
    """
    Get global production rankings for a commodity.

    Args:
        commodity: Commodity name
        year: Marketing year
        top_n: Number of top producers to return

    Returns:
        JSON with production rankings and market share
    """
    sql = """
        WITH totals AS (
            SELECT SUM(production) as world_production
            FROM bronze.fas_psd
            WHERE commodity = %s AND marketing_year = %s AND production > 0
        )
        SELECT
            country,
            country_code,
            production,
            exports,
            ending_stocks,
            ROUND(production / NULLIF(t.world_production, 0) * 100, 1) as market_share_pct
        FROM bronze.fas_psd, totals t
        WHERE commodity = %s AND marketing_year = %s AND production > 0
        ORDER BY production DESC
        LIMIT %s
    """
    result = execute_query(sql, (commodity.lower(), year, commodity.lower(), year, top_n), limit=top_n)
    return json.dumps(result, indent=2, default=json_serializer)


def get_stocks_to_use(commodity: str = None, country: str = None) -> str:
    """
    Calculate stocks-to-use ratios across commodities/countries.

    Args:
        commodity: Optional commodity filter
        country: Optional country filter

    Returns:
        JSON with S/U ratios
    """
    conditions = ["marketing_year >= 2024"]
    params = []

    if commodity:
        conditions.append("commodity = %s")
        params.append(commodity.lower())
    if country:
        conditions.append("country_code = %s")
        params.append(country.upper())

    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT
            commodity,
            country_code,
            marketing_year,
            ending_stocks,
            total_distribution as total_use,
            ROUND(ending_stocks / NULLIF(total_distribution, 0) * 100, 1) as stocks_use_pct
        FROM bronze.fas_psd
        WHERE {where_clause} AND total_distribution > 0
        ORDER BY commodity, country_code, marketing_year
    """
    result = execute_query(sql, tuple(params) if params else None, limit=200)
    return json.dumps(result, indent=2, default=json_serializer)


def get_commodity_summary() -> str:
    """
    Get summary of all commodities in the database.

    Returns:
        JSON summary of commodity coverage
    """
    sql = """
        SELECT
            commodity,
            COUNT(DISTINCT country_code) as countries,
            MIN(marketing_year) as min_year,
            MAX(marketing_year) as max_year,
            COUNT(*) as total_records,
            SUM(production) as total_production_1000mt
        FROM bronze.fas_psd
        GROUP BY commodity
        ORDER BY total_production_1000mt DESC
    """
    result = execute_query(sql, limit=50)
    return json.dumps(result, indent=2, default=json_serializer)


def analyze_supply_demand(commodity: str, country: str = 'US') -> str:
    """
    Run comprehensive S&D analysis for a commodity/country.

    Args:
        commodity: Commodity name
        country: Country code

    Returns:
        JSON with analysis including trends, YoY changes, outlook
    """
    # Get multi-year data
    sql = """
        SELECT
            marketing_year,
            beginning_stocks,
            production,
            imports,
            total_supply,
            domestic_consumption,
            exports,
            total_distribution,
            ending_stocks,
            ROUND(ending_stocks / NULLIF(total_distribution, 0) * 100, 1) as stocks_use_pct
        FROM bronze.fas_psd
        WHERE commodity = %s AND country_code = %s
        ORDER BY marketing_year
    """
    result = execute_query(sql, (commodity.lower(), country.upper()), limit=10)

    if 'error' in result or not result.get('rows'):
        return json.dumps(result, indent=2, default=json_serializer)

    rows = result['rows']

    # Calculate YoY changes for latest year
    analysis = {
        "commodity": commodity,
        "country": country,
        "data": rows,
        "analysis": {}
    }

    if len(rows) >= 2:
        latest = rows[-1]
        prior = rows[-2]

        analysis["analysis"]["latest_year"] = latest["marketing_year"]
        analysis["analysis"]["prior_year"] = prior["marketing_year"]

        for field in ['production', 'exports', 'ending_stocks', 'stocks_use_pct']:
            if latest.get(field) and prior.get(field):
                change = latest[field] - prior[field]
                pct_change = (change / prior[field] * 100) if prior[field] != 0 else 0
                analysis["analysis"][f"{field}_change"] = round(change, 1)
                analysis["analysis"][f"{field}_pct_change"] = round(pct_change, 1)

    return json.dumps(analysis, indent=2, default=json_serializer)


def get_brazil_production(commodity: str = 'soybeans', crop_year: str = None) -> str:
    """
    Get Brazil production by state from CONAB data.

    Args:
        commodity: Commodity (soybeans, corn)
        crop_year: Optional crop year filter (e.g., '2024/25')

    Returns:
        JSON with state-level production
    """
    if commodity.lower() == 'soybeans':
        table = 'gold.brazil_soybean_production'
    elif commodity.lower() == 'corn':
        table = 'gold.brazil_corn_production'
    else:
        table = 'gold.brazil_production_by_state'

    if crop_year:
        sql = f"""
            SELECT * FROM {table}
            WHERE crop_year = %s
            ORDER BY production_tonnes DESC
        """
        result = execute_query(sql, (crop_year,), limit=50)
    else:
        sql = f"""
            SELECT * FROM {table}
            ORDER BY crop_year DESC, production_tonnes DESC
            LIMIT 50
        """
        result = execute_query(sql, limit=50)

    return json.dumps(result, indent=2, default=json_serializer)


# ============================================================================
# MCP SERVER SETUP
# ============================================================================

if MCP_AVAILABLE:
    server = Server("commodities-db")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="query_database",
                description="Execute a SQL SELECT query against the agricultural commodities database. Use for custom queries when pre-built tools don't meet your needs.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "sql": {"type": "string", "description": "SQL SELECT query"},
                        "limit": {"type": "integer", "description": "Max rows (default 100)", "default": 100}
                    },
                    "required": ["sql"]
                }
            ),
            Tool(
                name="list_tables",
                description="List all tables in the database. Optionally filter by schema (bronze, silver, gold).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schema": {"type": "string", "description": "Schema filter: bronze, silver, or gold"}
                    }
                }
            ),
            Tool(
                name="describe_table",
                description="Get column information for a specific table.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string", "description": "Table name (e.g., bronze.fas_psd)"}
                    },
                    "required": ["table_name"]
                }
            ),
            Tool(
                name="get_balance_sheet",
                description="Get S&D balance sheet for a commodity and country. Returns production, supply, use, and stocks data.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "commodity": {"type": "string", "description": "Commodity: corn, soybeans, wheat, barley, rice, etc."},
                        "country": {"type": "string", "description": "Country code: US, BR, AR, CH, E4, RS, UP, etc.", "default": "US"},
                        "years": {"type": "integer", "description": "Number of years to return", "default": 3}
                    },
                    "required": ["commodity"]
                }
            ),
            Tool(
                name="get_production_ranking",
                description="Get global production rankings for a commodity, showing top producers and market share.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "commodity": {"type": "string", "description": "Commodity name"},
                        "year": {"type": "integer", "description": "Marketing year", "default": 2025},
                        "top_n": {"type": "integer", "description": "Number of top producers", "default": 15}
                    },
                    "required": ["commodity"]
                }
            ),
            Tool(
                name="get_stocks_to_use",
                description="Calculate stocks-to-use ratios. Can filter by commodity and/or country.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "commodity": {"type": "string", "description": "Optional commodity filter"},
                        "country": {"type": "string", "description": "Optional country filter"}
                    }
                }
            ),
            Tool(
                name="get_commodity_summary",
                description="Get summary of all commodities in the database with coverage statistics.",
                inputSchema={
                    "type": "object",
                    "properties": {}
                }
            ),
            Tool(
                name="analyze_supply_demand",
                description="Run comprehensive S&D analysis for a commodity/country including trends and YoY changes.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "commodity": {"type": "string", "description": "Commodity name"},
                        "country": {"type": "string", "description": "Country code", "default": "US"}
                    },
                    "required": ["commodity"]
                }
            ),
            Tool(
                name="get_brazil_production",
                description="Get Brazil crop production by state from CONAB data.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "commodity": {"type": "string", "description": "Commodity: soybeans or corn", "default": "soybeans"},
                        "crop_year": {"type": "string", "description": "Crop year (e.g., '2024/25')"}
                    }
                }
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        try:
            if name == "query_database":
                result = query_database(arguments["sql"], arguments.get("limit", 100))
            elif name == "list_tables":
                result = list_tables(arguments.get("schema"))
            elif name == "describe_table":
                result = describe_table(arguments["table_name"])
            elif name == "get_balance_sheet":
                result = get_balance_sheet(
                    arguments["commodity"],
                    arguments.get("country", "US"),
                    arguments.get("years", 3)
                )
            elif name == "get_production_ranking":
                result = get_production_ranking(
                    arguments["commodity"],
                    arguments.get("year", 2025),
                    arguments.get("top_n", 15)
                )
            elif name == "get_stocks_to_use":
                result = get_stocks_to_use(
                    arguments.get("commodity"),
                    arguments.get("country")
                )
            elif name == "get_commodity_summary":
                result = get_commodity_summary()
            elif name == "analyze_supply_demand":
                result = analyze_supply_demand(
                    arguments["commodity"],
                    arguments.get("country", "US")
                )
            elif name == "get_brazil_production":
                result = get_brazil_production(
                    arguments.get("commodity", "soybeans"),
                    arguments.get("crop_year")
                )
            else:
                result = json.dumps({"error": f"Unknown tool: {name}"})

            return [TextContent(type="text", text=result)]

        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    async def main():
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())

    if __name__ == "__main__":
        asyncio.run(main())

else:
    # Fallback CLI mode when MCP not available
    if __name__ == "__main__":
        import argparse
        parser = argparse.ArgumentParser(description='Commodities DB Query Tool')
        parser.add_argument('--query', '-q', help='SQL query')
        parser.add_argument('--balance-sheet', '-b', nargs='+', help='Get balance sheet: commodity [country]')
        parser.add_argument('--ranking', '-r', help='Get production ranking for commodity')
        parser.add_argument('--summary', '-s', action='store_true', help='Get commodity summary')
        parser.add_argument('--analyze', '-a', nargs='+', help='Analyze S&D: commodity [country]')

        args = parser.parse_args()

        if args.query:
            print(query_database(args.query))
        elif args.balance_sheet:
            commodity = args.balance_sheet[0]
            country = args.balance_sheet[1] if len(args.balance_sheet) > 1 else 'US'
            print(get_balance_sheet(commodity, country))
        elif args.ranking:
            print(get_production_ranking(args.ranking))
        elif args.summary:
            print(get_commodity_summary())
        elif args.analyze:
            commodity = args.analyze[0]
            country = args.analyze[1] if len(args.analyze) > 1 else 'US'
            print(analyze_supply_demand(commodity, country))
        else:
            parser.print_help()
