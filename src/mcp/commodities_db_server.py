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
- get_data_freshness: Check data collection freshness/staleness
- get_briefing: Get unacknowledged system events (LLM inbox)
- acknowledge_events: Mark briefing events as read
- get_collection_history: Recent run history for a collector
- search_knowledge_graph: Search analyst knowledge graph nodes
- get_kg_context: Get full analyst context for a KG node
- get_kg_relationships: Get relationships for a KG node
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
    """Get database connection.

    Uses the same RLC_PG_* env vars as the rest of the system (collectors,
    dispatcher, etc.) so that the MCP server reads from the same database
    that collectors write to.  Falls back to DB_HOST/DB_PORT for backwards
    compatibility.
    """
    load_env()
    return psycopg2.connect(
        host=os.environ.get('RLC_PG_HOST', os.environ.get('DB_HOST', 'localhost')),
        port=os.environ.get('RLC_PG_PORT', os.environ.get('DB_PORT', '5432')),
        dbname=os.environ.get('RLC_PG_DATABASE', os.environ.get('DB_NAME', 'rlc_commodities')),
        user=os.environ.get('RLC_PG_USER', os.environ.get('DB_USER', 'postgres')),
        password=os.environ.get('RLC_PG_PASSWORD', os.environ.get('DB_PASSWORD', '')),
        sslmode=os.environ.get('RLC_PG_SSLMODE', 'prefer'),
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
# CNS TOOLS: Data Freshness, Briefing, Event Acknowledgment
# ============================================================================

def get_data_freshness(collector_name: str = None, category: str = None) -> str:
    """
    Check data freshness across all collectors.

    Args:
        collector_name: Optional filter (partial match)
        category: Optional category filter (grains, energy, etc.)

    Returns:
        JSON with freshness data sorted by staleness
    """
    conditions = []
    params = []

    if collector_name:
        conditions.append("collector_name ILIKE %s")
        params.append(f"%{collector_name}%")
    if category:
        conditions.append("category = %s")
        params.append(category)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT collector_name, display_name, category,
               last_collected, last_status, last_row_count,
               data_period, is_new_data,
               ROUND(hours_since_collection::numeric, 1) AS hours_since_collection,
               expected_frequency, expected_release_day, expected_release_time_et,
               is_overdue
        FROM core.data_freshness
        {where}
        ORDER BY is_overdue DESC, hours_since_collection DESC NULLS LAST
    """
    result = execute_query(sql, tuple(params) if params else None, limit=100)
    return json.dumps(result, indent=2, default=json_serializer)


def get_briefing(priority: int = None, event_type: str = None) -> str:
    """
    Get unacknowledged events from the LLM briefing view.
    This is the LLM's "inbox" -- what happened since last session.

    Args:
        priority: Optional max priority (1=critical only, 2=+important, 3=all)
        event_type: Optional event type filter

    Returns:
        JSON with unread events
    """
    conditions = []
    params = []

    if priority is not None:
        conditions.append("priority <= %s")
        params.append(priority)
    if event_type:
        conditions.append("event_type = %s")
        params.append(event_type)

    where = " WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"""
        SELECT id, event_type, event_time, source, summary, details, priority
        FROM core.llm_briefing
        {where}
        ORDER BY priority ASC, event_time DESC
    """
    result = execute_query(sql, tuple(params) if params else None, limit=200)
    return json.dumps(result, indent=2, default=json_serializer)


def acknowledge_events(event_ids: list) -> str:
    """
    Mark events as read. Calls core.acknowledge_events() stored function.

    This is a WRITE operation -- bypasses execute_query() safety filter
    intentionally, calling only the specific acknowledge function.

    Args:
        event_ids: List of integer event IDs to acknowledge

    Returns:
        JSON with count of events acknowledged
    """
    if not event_ids:
        return json.dumps({"error": "No event_ids provided"})

    try:
        event_ids = [int(eid) for eid in event_ids]
    except (ValueError, TypeError):
        return json.dumps({"error": "event_ids must be a list of integers"})

    conn = get_connection()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            "SELECT core.acknowledge_events(%s::integer[]) AS acknowledged_count",
            (event_ids,)
        )
        result = cur.fetchone()
        conn.commit()

        return json.dumps({
            "acknowledged_count": result['acknowledged_count'],
            "event_ids": event_ids,
            "status": "success"
        }, indent=2, default=json_serializer)
    except Exception as e:
        conn.rollback()
        return json.dumps({"error": str(e)})
    finally:
        conn.close()


def get_collection_history(collector_name: str, last_n: int = 10) -> str:
    """
    Get recent collection history for a specific collector.

    Args:
        collector_name: Exact collector name (e.g., 'cftc_cot')
        last_n: Number of recent runs to return (default 10)

    Returns:
        JSON with run history and summary statistics
    """
    sql = """
        SELECT id, collector_name, run_started_at, run_finished_at,
               status, rows_collected,
               data_period, is_new_data, triggered_by, error_message, notes,
               EXTRACT(EPOCH FROM (run_finished_at - run_started_at)) AS elapsed_seconds
        FROM core.collection_status
        WHERE collector_name = %s
        ORDER BY run_started_at DESC
    """
    result = execute_query(sql, (collector_name, ), limit=last_n)

    if 'error' in result or not result.get('rows'):
        return json.dumps(result, indent=2, default=json_serializer)

    rows = result['rows']
    total = len(rows)
    successes = sum(1 for r in rows if r.get('status') == 'success')
    failures = sum(1 for r in rows if r.get('status') == 'failed')
    avg_rows = sum(r.get('rows_collected') or 0 for r in rows) / total if total else 0
    elapsed_vals = [r.get('elapsed_seconds') or 0 for r in rows if r.get('elapsed_seconds')]
    avg_elapsed = sum(elapsed_vals) / len(elapsed_vals) if elapsed_vals else 0

    output = {
        "collector_name": collector_name,
        "runs": rows,
        "summary": {
            "total_runs": total,
            "successes": successes,
            "failures": failures,
            "success_rate_pct": round(successes / total * 100, 1) if total else 0,
            "avg_rows_collected": round(avg_rows),
            "avg_elapsed_seconds": round(avg_elapsed, 1),
        }
    }
    return json.dumps(output, indent=2, default=json_serializer)


# ============================================================================
# KNOWLEDGE GRAPH TOOLS
# ============================================================================

# Lazy-init singleton for KGManager
_kg_manager = None

def _get_kg_manager():
    """Get or create KGManager singleton."""
    global _kg_manager
    if _kg_manager is None:
        from src.knowledge_graph.kg_manager import KGManager
        _kg_manager = KGManager()
    return _kg_manager


def search_knowledge_graph(node_type: str = None, label: str = None,
                           key_pattern: str = None, limit: int = 50) -> str:
    """Search the analyst knowledge graph for nodes."""
    kg = _get_kg_manager()
    nodes = kg.search_nodes(
        node_type=node_type,
        label_pattern=label,
        key_pattern=key_pattern,
        limit=limit,
    )
    return json.dumps({"nodes": nodes, "count": len(nodes)}, indent=2, default=json_serializer)


def get_kg_context(node_key: str) -> str:
    """Get full analyst context for a KG node: node + contexts + edges."""
    kg = _get_kg_manager()
    enriched = kg.get_enriched_context(node_key)
    if enriched is None:
        return json.dumps({"error": f"Node not found: {node_key}"})
    return json.dumps(enriched, indent=2, default=json_serializer)


def get_kg_relationships(node_key: str, edge_type: str = None,
                         direction: str = 'both') -> str:
    """Get relationships for a knowledge graph node."""
    kg = _get_kg_manager()
    node = kg.get_node(node_key)
    if node is None:
        return json.dumps({"error": f"Node not found: {node_key}"})

    edges = kg.get_node_edges(node_key, edge_type=edge_type, direction=direction)
    return json.dumps({
        "node": {"node_key": node["node_key"], "label": node["label"]},
        "edges": edges,
        "count": len(edges)
    }, indent=2, default=json_serializer)


# ============================================================================
# REPORT GENERATION TOOLS
# ============================================================================

def generate_report(report_type: str, commodity: str = None,
                    country: str = 'US', include_kg: bool = True) -> str:
    """
    Generate an analytical report using the template pipeline.

    Supported report_type values:
        - 'wasde'           Full WASDE analysis with balance sheets and KG context
        - 'crop_progress'   Current crop conditions vs 5-year averages
        - 'ethanol'         Ethanol production, stocks, and margin analysis
        - 'positioning'     CFTC managed money positioning summary

    Args:
        report_type: Type of report to generate
        commodity: Optional commodity filter (e.g. 'corn', 'soybeans')
        country: Country code (default 'US')
        include_kg: Whether to include Knowledge Graph analyst context

    Returns:
        JSON with report sections, data tables, and optional KG context
    """
    try:
        if report_type == 'wasde':
            return _generate_wasde_report(commodity, country, include_kg)
        elif report_type == 'crop_progress':
            return _generate_crop_progress_report(commodity)
        elif report_type == 'ethanol':
            return _generate_ethanol_report()
        elif report_type == 'positioning':
            return _generate_positioning_report(commodity)
        else:
            return json.dumps({
                "error": f"Unknown report_type: {report_type}",
                "supported": ["wasde", "crop_progress", "ethanol", "positioning"]
            })
    except Exception as e:
        return json.dumps({"error": str(e)}, default=json_serializer)


def _generate_wasde_report(commodity: str = None, country: str = 'US',
                           include_kg: bool = True) -> str:
    """Generate WASDE balance sheet analysis using the analysis template."""
    try:
        from src.analysis.templates.wasde_template import WASDeAnalysisTemplate
        template = WASDeAnalysisTemplate()

        if not template.check_data_ready():
            return json.dumps({"error": "WASDE data not ready — no recent report data in bronze.fas_psd"})

        context = template.build_prompt_context()
        analysis = context.get('analysis', {})

        # Build result
        result = {
            "report_type": "wasde",
            "report_date": analysis.get('report_date'),
            "marketing_year": analysis.get('marketing_year'),
            "balance_sheet_table": analysis.get('balance_sheet_table'),
            "delta_summary": analysis.get('delta_summary'),
            "global_context": analysis.get('global_context'),
            "is_august_wasde": analysis.get('is_august_wasde', False),
        }

        if include_kg:
            kg_context = context.get('kg_context', {})
            kg_summary = {}
            for node_key, enriched in kg_context.items():
                node = enriched.get('node', {})
                contexts = enriched.get('contexts', [])
                kg_summary[node_key] = {
                    'label': node.get('label', node_key),
                    'top_contexts': [
                        {'type': c.get('context_type'), 'value': c.get('context_value')}
                        for c in contexts[:3]
                    ]
                }
            result['kg_context'] = kg_summary

        return json.dumps(result, indent=2, default=json_serializer)

    except Exception as e:
        return json.dumps({"error": f"WASDE report generation failed: {e}"}, default=json_serializer)


def _generate_crop_progress_report(commodity: str = None) -> str:
    """Generate crop conditions report from NASS data."""
    commodities = [commodity] if commodity else ['corn', 'soybeans', 'wheat']
    sections = {}

    for comm in commodities:
        # Current condition
        condition_sql = """
            SELECT state, week_ending, excellent, good, fair, poor, very_poor,
                   (COALESCE(excellent, 0) + COALESCE(good, 0)) as good_excellent
            FROM bronze.nass_crop_condition
            WHERE commodity = %s
            ORDER BY week_ending DESC, state
            LIMIT 50
        """
        result = execute_query(condition_sql, (comm,), limit=50)

        # 5-year average from silver
        avg_sql = """
            SELECT commodity, week_ending, ge_pct
            FROM silver.nass_crop_condition_ge
            WHERE commodity = %s
            ORDER BY week_ending DESC
            LIMIT 5
        """
        avg_result = execute_query(avg_sql, (comm,), limit=5)

        sections[comm] = {
            'current_condition': result.get('rows', []),
            'historical_ge': avg_result.get('rows', []),
        }

    return json.dumps({
        "report_type": "crop_progress",
        "commodities": sections,
    }, indent=2, default=json_serializer)


def _generate_ethanol_report() -> str:
    """Generate ethanol production and stocks summary."""
    production_sql = """
        SELECT period, value, series_id
        FROM bronze.eia_ethanol
        WHERE series_id IN (
            'PET.W_EPOOXE_YOP_NUS_MBBLD.W',
            'PET.W_EPOOXE_SAE_NUS_MBBL.W'
        )
        ORDER BY period DESC
        LIMIT 20
    """
    result = execute_query(production_sql, limit=20)

    return json.dumps({
        "report_type": "ethanol",
        "weekly_data": result.get('rows', []),
    }, indent=2, default=json_serializer)


def _generate_positioning_report(commodity: str = None) -> str:
    """Generate CFTC managed money positioning report."""
    if commodity:
        sql = """
            SELECT * FROM gold.cftc_sentiment
            WHERE commodity ILIKE %s
        """
        result = execute_query(sql, (f'%{commodity}%',), limit=20)
    else:
        sql = "SELECT * FROM gold.cftc_sentiment"
        result = execute_query(sql, limit=20)

    return json.dumps({
        "report_type": "positioning",
        "sentiment": result.get('rows', []),
    }, indent=2, default=json_serializer)


def get_forecast_accuracy(commodity: str = None, forecast_type: str = None,
                          period: str = '12_months') -> str:
    """
    Get forecast accuracy metrics from the forecast tracker.

    Args:
        commodity: Filter by commodity (e.g. 'corn', 'soybeans')
        forecast_type: Filter by type (e.g. 'yield', 'production', 'ending_stocks')
        period: Lookback period ('12_months', '6_months', 'all')

    Returns:
        JSON with accuracy metrics (MAPE, directional accuracy, bias)
    """
    conditions = ["1=1"]
    params = []

    if commodity:
        conditions.append("f.commodity = %s")
        params.append(commodity.lower())
    if forecast_type:
        conditions.append("f.forecast_type = %s")
        params.append(forecast_type)

    interval = {'12_months': '12 months', '6_months': '6 months'}.get(period)
    if interval:
        conditions.append(f"f.target_date >= CURRENT_DATE - INTERVAL '{interval}'")

    where = " AND ".join(conditions)

    sql = f"""
        SELECT
            f.commodity,
            f.forecast_type,
            COUNT(*) as n_forecasts,
            ROUND(AVG(p.absolute_percentage_error)::numeric, 2) as mape,
            ROUND(AVG(CASE WHEN p.direction_correct = 1 THEN 100.0 ELSE 0.0 END)::numeric, 1)
                as directional_accuracy,
            ROUND(AVG(p.percentage_error)::numeric, 2) as mean_bias_pct
        FROM core.forecast_actual_pairs p
        JOIN core.forecasts f ON p.forecast_id = f.forecast_id
        WHERE {where}
        GROUP BY f.commodity, f.forecast_type
        ORDER BY COUNT(*) DESC
    """
    result = execute_query(sql, tuple(params) if params else None, limit=50)

    if 'error' in result:
        return json.dumps(result, default=json_serializer)

    return json.dumps({
        "period": period,
        "commodity_filter": commodity,
        "type_filter": forecast_type,
        "accuracy": result.get('rows', []),
    }, indent=2, default=json_serializer)


def record_forecast(commodity: str, forecast_type: str, value: float,
                    target_date: str, marketing_year: str = None,
                    unit: str = None, source: str = 'user',
                    notes: str = None, country: str = 'US') -> str:
    """
    Record a new forecast for accuracy tracking.

    Args:
        commodity: e.g. 'corn', 'soybeans'
        forecast_type: e.g. 'yield', 'production', 'ending_stocks', 'price'
        value: Forecasted value
        target_date: What period is being forecast (YYYY-MM-DD)
        marketing_year: e.g. '2025/26' (optional)
        unit: e.g. 'bu/acre', 'MMT', 'USD/bu'
        source: Who made the forecast (default 'user')
        notes: Optional notes
        country: Country code (default 'US')

    Returns:
        JSON with recorded forecast ID
    """
    from datetime import date as date_type

    forecast_date = date_type.today().isoformat()
    forecast_id = "_".join([
        forecast_date, target_date, commodity.lower(), country, forecast_type, source
    ]).replace("/", "-").replace(" ", "_")

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO core.forecasts
                (forecast_id, forecast_date, target_date, commodity, country,
                 forecast_type, value, unit, marketing_year, source, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (forecast_id) DO UPDATE SET
                value = EXCLUDED.value,
                notes = EXCLUDED.notes,
                created_at = NOW()
            RETURNING id, forecast_id
        """, (
            forecast_id, forecast_date, target_date, commodity.lower(),
            country, forecast_type, value, unit, marketing_year, source, notes
        ))
        row = cur.fetchone()
        conn.commit()
        conn.close()

        return json.dumps({
            "success": True,
            "id": row[0],
            "forecast_id": row[1],
            "commodity": commodity,
            "forecast_type": forecast_type,
            "value": value,
            "target_date": target_date,
        }, indent=2, default=json_serializer)

    except Exception as e:
        return json.dumps({"error": str(e)}, default=json_serializer)


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
            # --- CNS Tools ---
            Tool(
                name="get_data_freshness",
                description="Check data freshness across all collectors. Shows when each data source was last collected, whether it's overdue, and its expected schedule. Use to answer 'is CFTC data current?' or 'what data is stale?'",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "collector_name": {"type": "string", "description": "Filter by collector name (partial match). E.g., 'cftc', 'eia', 'nass'"},
                        "category": {"type": "string", "description": "Filter by category: grains, oilseeds, energy, biofuels, weather, positioning, trade"}
                    }
                }
            ),
            Tool(
                name="get_briefing",
                description="Get your briefing: unacknowledged system events since last session. Shows collection completions, failures, data anomalies, overdue alerts. Read this first at session start.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "priority": {"type": "integer", "description": "Max priority level (1=critical only, 2=+important, 3=all). Default: all."},
                        "event_type": {"type": "string", "description": "Filter: collection_complete, collection_failed, schedule_overdue, data_anomaly, report_generated, system_alert"}
                    }
                }
            ),
            Tool(
                name="acknowledge_events",
                description="Mark briefing events as read after processing them. Pass event IDs from get_briefing. Clears them from future briefings.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_ids": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Array of event IDs to acknowledge"
                        }
                    },
                    "required": ["event_ids"]
                }
            ),
            Tool(
                name="get_collection_history",
                description="Get recent run history for a data collector. Shows success/failure trend, row counts, timing, errors. Use to diagnose collection issues.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "collector_name": {"type": "string", "description": "Collector name (e.g., 'cftc_cot', 'eia_ethanol', 'usda_nass_crop_progress')"},
                        "last_n": {"type": "integer", "description": "Number of recent runs (default 10)", "default": 10}
                    },
                    "required": ["collector_name"]
                }
            ),
            # --- Report Generation Tools ---
            Tool(
                name="generate_report",
                description="Generate an analytical report. Types: 'wasde' (S&D balance sheet analysis), 'crop_progress' (crop conditions vs 5-yr avg), 'ethanol' (production/stocks), 'positioning' (CFTC managed money). Returns structured data with optional Knowledge Graph analyst context.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "report_type": {
                            "type": "string",
                            "description": "Report type: wasde, crop_progress, ethanol, positioning",
                            "enum": ["wasde", "crop_progress", "ethanol", "positioning"]
                        },
                        "commodity": {"type": "string", "description": "Optional commodity filter (corn, soybeans, wheat)"},
                        "country": {"type": "string", "description": "Country code (default US)", "default": "US"},
                        "include_kg": {"type": "boolean", "description": "Include Knowledge Graph analyst context", "default": True}
                    },
                    "required": ["report_type"]
                }
            ),
            Tool(
                name="get_forecast_accuracy",
                description="Get forecast accuracy metrics from the tracking system. Shows MAPE, directional accuracy, and bias for recorded forecasts vs actuals.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "commodity": {"type": "string", "description": "Filter by commodity"},
                        "forecast_type": {"type": "string", "description": "Filter by type: yield, production, ending_stocks, price, exports, crush"},
                        "period": {"type": "string", "description": "Lookback: 12_months, 6_months, or all", "default": "12_months"}
                    }
                }
            ),
            Tool(
                name="record_forecast",
                description="Record a new forecast for accuracy tracking. The system will match it to actual values when they arrive.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "commodity": {"type": "string", "description": "e.g. corn, soybeans, wheat"},
                        "forecast_type": {"type": "string", "description": "yield, production, ending_stocks, price, exports, crush"},
                        "value": {"type": "number", "description": "The forecasted value"},
                        "target_date": {"type": "string", "description": "Target date (YYYY-MM-DD)"},
                        "marketing_year": {"type": "string", "description": "e.g. 2025/26"},
                        "unit": {"type": "string", "description": "e.g. bu/acre, MMT, USD/bu"},
                        "source": {"type": "string", "description": "Who made it (default: user)", "default": "user"},
                        "notes": {"type": "string", "description": "Optional notes"},
                        "country": {"type": "string", "description": "Country code (default US)", "default": "US"}
                    },
                    "required": ["commodity", "forecast_type", "value", "target_date"]
                }
            ),
            # --- Knowledge Graph Tools ---
            Tool(
                name="search_knowledge_graph",
                description="Search the analyst knowledge graph for entities: commodities, data series, reports, regions, policies, seasonal events, market participants. Search by type, name, or key pattern.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "node_type": {"type": "string", "description": "Node type: data_series, commodity, region, report, policy, seasonal_event, market_participant, balance_sheet_line, price_level"},
                        "label": {"type": "string", "description": "Search label text (partial match, case-insensitive)"},
                        "key_pattern": {"type": "string", "description": "Search node_key pattern (partial match). E.g., 'cftc.corn' or 'seasonal'"},
                        "limit": {"type": "integer", "description": "Max results (default 50)", "default": 50}
                    }
                }
            ),
            Tool(
                name="get_kg_context",
                description="Get full analyst context for a knowledge graph node. Returns the node, all analyst contexts (seasonal norms, risk thresholds, expert rules), and all relationships. The 'what would an analyst think?' query.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "node_key": {"type": "string", "description": "Node key (e.g., 'corn', 'soybean_oil', 'managed_money', 'noaa.enso.oni')"}
                    },
                    "required": ["node_key"]
                }
            ),
            Tool(
                name="get_kg_relationships",
                description="Get relationships (edges) for a knowledge graph node. Shows causal links, cross-market dynamics, substitution hierarchies.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "node_key": {"type": "string", "description": "Node key to get relationships for"},
                        "edge_type": {"type": "string", "description": "Filter: CAUSES, COMPETES_WITH, SUBSTITUTES, LEADS, SEASONAL_PATTERN, RISK_THRESHOLD, CROSS_MARKET, TRIGGERS"},
                        "direction": {"type": "string", "description": "'outgoing', 'incoming', or 'both' (default)", "default": "both", "enum": ["outgoing", "incoming", "both"]}
                    },
                    "required": ["node_key"]
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
            # CNS Tools
            elif name == "get_data_freshness":
                result = get_data_freshness(
                    arguments.get("collector_name"),
                    arguments.get("category")
                )
            elif name == "get_briefing":
                result = get_briefing(
                    arguments.get("priority"),
                    arguments.get("event_type")
                )
            elif name == "acknowledge_events":
                result = acknowledge_events(arguments["event_ids"])
            elif name == "get_collection_history":
                result = get_collection_history(
                    arguments["collector_name"],
                    arguments.get("last_n", 10)
                )
            # Report Generation Tools
            elif name == "generate_report":
                result = generate_report(
                    arguments["report_type"],
                    arguments.get("commodity"),
                    arguments.get("country", "US"),
                    arguments.get("include_kg", True)
                )
            elif name == "get_forecast_accuracy":
                result = get_forecast_accuracy(
                    arguments.get("commodity"),
                    arguments.get("forecast_type"),
                    arguments.get("period", "12_months")
                )
            elif name == "record_forecast":
                result = record_forecast(
                    arguments["commodity"],
                    arguments["forecast_type"],
                    arguments["value"],
                    arguments["target_date"],
                    arguments.get("marketing_year"),
                    arguments.get("unit"),
                    arguments.get("source", "user"),
                    arguments.get("notes"),
                    arguments.get("country", "US")
                )
            # Knowledge Graph Tools
            elif name == "search_knowledge_graph":
                result = search_knowledge_graph(
                    arguments.get("node_type"),
                    arguments.get("label"),
                    arguments.get("key_pattern"),
                    arguments.get("limit", 50)
                )
            elif name == "get_kg_context":
                result = get_kg_context(arguments["node_key"])
            elif name == "get_kg_relationships":
                result = get_kg_relationships(
                    arguments["node_key"],
                    arguments.get("edge_type"),
                    arguments.get("direction", "both")
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
