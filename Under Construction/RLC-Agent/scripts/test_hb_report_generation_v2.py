#!/usr/bin/env python3
"""
HigbyBarrett Weekly Report Generation Test - V2
================================================
Enhanced version with comprehensive data utilization.

Improvements over V1:
- Uses ALL available gold-layer data
- Structured data sections for future econometric model integration
- Better prompt engineering for analytical output
- Calculates derived metrics (YoY changes, trends)

Usage:
    python scripts/test_hb_report_generation_v2.py
"""

import os
import sys
import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from decimal import Decimal

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

# Configure comprehensive logging
LOG_DIR = PROJECT_ROOT / "output" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"hb_report_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("HB-ReportV2")


def json_serializer(obj):
    """JSON serializer for special types."""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def get_database_connection():
    """Get database connection using .env settings."""
    import psycopg2
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")

    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'rlc_commodities'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', '')
    )
    return conn


def fetch_comprehensive_data(days_back: int = 7) -> dict:
    """Fetch ALL available market data from the database."""
    logger.info(f"Fetching comprehensive market data...")

    conn = get_database_connection()
    cur = conn.cursor()

    start_date = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    data = {}

    # =========================================================================
    # 1. CASH PRICES
    # =========================================================================
    logger.info("1. Fetching cash prices...")
    cur.execute(f"""
        SELECT commodity, report_date, location_name, location_state,
               price_cash, basis, basis_month, change_daily, unit
        FROM silver.cash_price
        WHERE report_date >= '{start_date}'
        ORDER BY commodity, report_date DESC
    """)
    rows = cur.fetchall()
    data['cash_prices'] = [
        {
            'commodity': r[0], 'date': str(r[1]), 'location': r[2], 'state': r[3],
            'price': float(r[4]) if r[4] else None,
            'basis': float(r[5]) if r[5] else None,
            'basis_month': r[6],
            'daily_change': float(r[7]) if r[7] else None,
            'unit': r[8]
        }
        for r in rows
    ]
    logger.info(f"   Found {len(data['cash_prices'])} cash price records")

    # =========================================================================
    # 2. WEATHER DATA (with regional aggregation)
    # =========================================================================
    logger.info("2. Fetching weather summary by region...")
    cur.execute(f"""
        SELECT region,
               AVG(temp_high_f) as avg_high,
               AVG(temp_low_f) as avg_low,
               AVG(temp_avg_f) as avg_temp,
               SUM(precipitation_in) as total_precip,
               AVG(humidity_pct) as avg_humidity,
               COUNT(*) as observations
        FROM gold.weather_summary
        WHERE observation_date >= '{start_date}'
        AND region IS NOT NULL
        GROUP BY region
        ORDER BY region
    """)
    rows = cur.fetchall()
    data['weather_by_region'] = [
        {
            'region': r[0],
            'avg_high_f': round(float(r[1]), 1) if r[1] else None,
            'avg_low_f': round(float(r[2]), 1) if r[2] else None,
            'avg_temp_f': round(float(r[3]), 1) if r[3] else None,
            'total_precip_in': round(float(r[4]), 2) if r[4] else 0,
            'avg_humidity_pct': round(float(r[5]), 1) if r[5] else None,
            'observations': r[6]
        }
        for r in rows
    ]
    logger.info(f"   Found {len(data['weather_by_region'])} regions")

    # =========================================================================
    # 3. WEATHER ALERTS
    # =========================================================================
    logger.info("3. Fetching weather alerts...")
    try:
        cur.execute(f"""
            SELECT location_id, display_name, region, observation_date,
                   alert_type, alert_description, temp_high_f, temp_low_f, precipitation_in
            FROM gold.weather_alerts
            ORDER BY observation_date DESC
            LIMIT 20
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data['weather_alerts'] = [dict(zip(columns, row)) for row in rows]
        logger.info(f"   Found {len(data['weather_alerts'])} weather alerts")
    except Exception as e:
        logger.warning(f"   Weather alerts query failed: {e}")
        conn.rollback()
        data['weather_alerts'] = []

    # =========================================================================
    # 4. EIA ETHANOL DATA
    # =========================================================================
    logger.info("4. Fetching EIA ethanol data...")
    try:
        cur.execute("""
            SELECT week_ending,
                   ethanol_production_kbd,
                   ethanol_stocks_kb,
                   ethanol_blender_input_kbd
            FROM gold.eia_ethanol_weekly
            ORDER BY week_ending DESC
            LIMIT 12
        """)
        rows = cur.fetchall()
        data['eia_ethanol'] = [
            {
                'week_ending': str(r[0]),
                'production_kbd': float(r[1]) if r[1] else None,
                'stocks_kb': float(r[2]) if r[2] else None,
                'blender_input_kbd': float(r[3]) if r[3] else None
            }
            for r in rows
        ]
        logger.info(f"   Found {len(data['eia_ethanol'])} ethanol records")

        # Calculate week-over-week changes
        if len(data['eia_ethanol']) >= 2:
            latest = data['eia_ethanol'][0]
            previous = data['eia_ethanol'][1]
            if latest['production_kbd'] and previous['production_kbd']:
                data['ethanol_production_change'] = round(latest['production_kbd'] - previous['production_kbd'], 1)
            if latest['stocks_kb'] and previous['stocks_kb']:
                data['ethanol_stocks_change'] = round(latest['stocks_kb'] - previous['stocks_kb'], 0)
    except Exception as e:
        logger.warning(f"   EIA ethanol query failed: {e}")
        conn.rollback()
        data['eia_ethanol'] = []

    # =========================================================================
    # 5. EIA PETROLEUM DATA
    # =========================================================================
    logger.info("5. Fetching EIA petroleum data...")
    try:
        cur.execute("""
            SELECT * FROM gold.eia_petroleum_weekly
            ORDER BY week_ending DESC
            LIMIT 8
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data['eia_petroleum'] = [dict(zip(columns, row)) for row in rows]
        logger.info(f"   Found {len(data['eia_petroleum'])} petroleum records")
    except Exception as e:
        logger.warning(f"   EIA petroleum query failed: {e}")
        conn.rollback()
        data['eia_petroleum'] = []

    # =========================================================================
    # 6. SOYBEAN BALANCE SHEET (with YoY comparison)
    # =========================================================================
    logger.info("6. Fetching soybean balance sheet...")
    try:
        cur.execute("""
            SELECT marketing_year,
                   beginning_stocks_mil_bu as beginning_stocks,
                   production_mil_bu as production,
                   imports_mil_bu as imports,
                   total_supply_mil_bu as total_supply,
                   crush_mil_bu as crush,
                   exports_mil_bu as exports,
                   seed_mil_bu as seed,
                   residual_mil_bu as residual,
                   total_use_mil_bu as total_use,
                   ending_stocks_mil_bu as ending_stocks,
                   stocks_to_use_pct
            FROM gold.us_soybean_balance_sheet
            ORDER BY marketing_year DESC
            LIMIT 3
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data['soybean_balance'] = [dict(zip(columns, row)) for row in rows]
        logger.info(f"   Found {len(data['soybean_balance'])} soybean balance records")
    except Exception as e:
        logger.warning(f"   Soybean balance query failed: {e}")
        conn.rollback()
        data['soybean_balance'] = []

    # =========================================================================
    # 7. SOYBEAN MEAL BALANCE SHEET
    # =========================================================================
    logger.info("7. Fetching soybean meal balance sheet...")
    try:
        cur.execute("""
            SELECT * FROM gold.us_soybean_meal_balance_sheet
            ORDER BY marketing_year DESC
            LIMIT 3
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data['soybean_meal_balance'] = [dict(zip(columns, row)) for row in rows]
        logger.info(f"   Found {len(data['soybean_meal_balance'])} meal balance records")
    except Exception as e:
        logger.warning(f"   Soybean meal balance query failed: {e}")
        conn.rollback()
        data['soybean_meal_balance'] = []

    # =========================================================================
    # 8. SOYBEAN OIL BALANCE SHEET
    # =========================================================================
    logger.info("8. Fetching soybean oil balance sheet...")
    try:
        cur.execute("""
            SELECT * FROM gold.us_soybean_oil_balance_sheet
            ORDER BY marketing_year DESC
            LIMIT 3
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data['soybean_oil_balance'] = [dict(zip(columns, row)) for row in rows]
        logger.info(f"   Found {len(data['soybean_oil_balance'])} oil balance records")
    except Exception as e:
        logger.warning(f"   Soybean oil balance query failed: {e}")
        conn.rollback()
        data['soybean_oil_balance'] = []

    # =========================================================================
    # 9. WHEAT BALANCE SHEET
    # =========================================================================
    logger.info("9. Fetching wheat balance sheet...")
    try:
        cur.execute("""
            SELECT marketing_year,
                   beginning_stocks_mil_bu as beginning_stocks,
                   production_mil_bu as production,
                   imports_mil_bu as imports,
                   total_supply_mil_bu as total_supply,
                   food_use_mil_bu as food_use,
                   feed_residual_mil_bu as feed_residual,
                   exports_mil_bu as exports,
                   total_use_mil_bu as total_use,
                   ending_stocks_mil_bu as ending_stocks,
                   stocks_to_use_pct
            FROM gold.us_wheat_balance_sheet
            ORDER BY marketing_year DESC
            LIMIT 3
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data['wheat_balance'] = [dict(zip(columns, row)) for row in rows]
        logger.info(f"   Found {len(data['wheat_balance'])} wheat balance records")
    except Exception as e:
        logger.warning(f"   Wheat balance query failed: {e}")
        conn.rollback()
        data['wheat_balance'] = []

    # =========================================================================
    # 10. PRICE REPORT NARRATIVES (Market Commentary)
    # =========================================================================
    logger.info("10. Fetching price report narratives...")
    cur.execute(f"""
        SELECT report_title, report_date, market_type,
               LEFT(report_narrative, 400) as narrative_snippet
        FROM bronze.price_report_raw
        WHERE report_date >= '{start_date}'
        AND report_narrative IS NOT NULL
        AND LENGTH(report_narrative) > 50
        ORDER BY report_date DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    data['market_narratives'] = [
        {
            'title': r[0], 'date': str(r[1]), 'market': r[2],
            'narrative': r[3]
        }
        for r in rows
    ]
    logger.info(f"   Found {len(data['market_narratives'])} market narratives")

    # =========================================================================
    # 11. CONAB BRAZIL DATA (When available)
    # =========================================================================
    logger.info("11. Checking for CONAB Brazil data...")
    try:
        cur.execute("""
            SELECT commodity, crop_year, state,
                   planted_area_1000ha, production_1000t, yield_kg_ha,
                   collected_at
            FROM bronze.conab_production
            WHERE collected_at >= NOW() - INTERVAL '30 days'
            ORDER BY crop_year DESC, commodity
            LIMIT 20
        """)
        rows = cur.fetchall()
        data['conab_brazil'] = [
            {
                'commodity': r[0],
                'crop_year': r[1],
                'state': r[2],
                'planted_area_1000ha': float(r[3]) if r[3] else None,
                'production_1000t': float(r[4]) if r[4] else None,
                'yield_kg_ha': float(r[5]) if r[5] else None
            }
            for r in rows
        ]
        logger.info(f"   Found {len(data['conab_brazil'])} CONAB records")
    except Exception as e:
        logger.info(f"   CONAB data not yet available: {e}")
        conn.rollback()
        data['conab_brazil'] = []

    # =========================================================================
    # 12. FUTURES PRICES (From Yahoo Finance)
    # =========================================================================
    logger.info("12. Fetching futures prices...")
    try:
        cur.execute(f"""
            SELECT symbol, trade_date, contract_month,
                   open_price, high_price, low_price, settlement,
                   volume, exchange, source
            FROM silver.futures_price
            WHERE trade_date >= '{start_date}'
            ORDER BY trade_date DESC, symbol
        """)
        rows = cur.fetchall()
        data['futures_prices'] = [
            {
                'symbol': r[0],
                'trade_date': str(r[1]),
                'contract_month': r[2],
                'open': float(r[3]) if r[3] else None,
                'high': float(r[4]) if r[4] else None,
                'low': float(r[5]) if r[5] else None,
                'settlement': float(r[6]) if r[6] else None,
                'volume': int(r[7]) if r[7] else 0,
                'exchange': r[8],
                'source': r[9]
            }
            for r in rows
        ]
        logger.info(f"   Found {len(data['futures_prices'])} futures price records")
    except Exception as e:
        logger.warning(f"   Futures prices query failed: {e}")
        conn.rollback()
        data['futures_prices'] = []

    conn.close()
    return data


def calculate_derived_metrics(data: dict) -> dict:
    """Calculate derived metrics for econometric analysis."""
    metrics = {}

    # Soybean stocks-to-use ratio
    if data.get('soybean_balance') and len(data['soybean_balance']) > 0:
        latest = data['soybean_balance'][0]
        if latest.get('ending_stocks') and latest.get('total_use'):
            es = float(latest['ending_stocks']) if latest['ending_stocks'] else 0
            tu = float(latest['total_use']) if latest['total_use'] else 1
            metrics['soybean_stocks_to_use'] = round(es / tu * 100, 1) if tu > 0 else None

    # Wheat stocks-to-use ratio
    if data.get('wheat_balance') and len(data['wheat_balance']) > 0:
        latest = data['wheat_balance'][0]
        if latest.get('ending_stocks') and latest.get('total_use'):
            es = float(latest['ending_stocks']) if latest['ending_stocks'] else 0
            tu = float(latest['total_use']) if latest['total_use'] else 1
            metrics['wheat_stocks_to_use'] = round(es / tu * 100, 1) if tu > 0 else None

    # Ethanol implied corn grind (approx 2.8 gal ethanol per bushel)
    if data.get('eia_ethanol') and len(data['eia_ethanol']) > 0:
        latest = data['eia_ethanol'][0]
        if latest.get('production_kbd'):
            # kbd = thousand barrels/day, 42 gal/barrel
            # Annual corn use = (kbd * 1000 * 42 * 365) / 2.8 / 1,000,000 million bushels
            daily_gal = latest['production_kbd'] * 1000 * 42
            annual_corn_mb = (daily_gal * 365) / 2.8 / 1_000_000
            metrics['implied_annual_corn_grind_mb'] = round(annual_corn_mb, 0)

    return metrics


def format_balance_sheet(name: str, data: list) -> str:
    """Format a balance sheet for the prompt."""
    if not data:
        return f"  {name}: No data available"

    lines = [f"  **{name}:**"]
    for record in data[:2]:  # Latest 2 years
        my = record.get('marketing_year', 'N/A')
        prod = record.get('production', 'N/A')
        exports = record.get('exports', 'N/A')
        ending = record.get('ending_stocks', 'N/A')
        lines.append(f"    MY {my}: Prod={prod}, Exports={exports}, End Stocks={ending}")
    return '\n'.join(lines)


def format_conab_data(data: list) -> str:
    """Format CONAB Brazil crop data for the prompt."""
    if not data:
        return "  Note: Brazilian crop estimates from CONAB typically released mid-month.\n  Consider SA harvest progress as key factor for US export competitiveness."

    # Group by commodity
    by_commodity = {}
    for record in data:
        comm = record['commodity']
        if comm not in by_commodity:
            by_commodity[comm] = []
        by_commodity[comm].append(record)

    lines = []
    for commodity, records in by_commodity.items():
        latest = records[0]  # Most recent
        prod = latest.get('production_1000t', 'N/A')
        area = latest.get('planted_area_1000ha', 'N/A')
        crop_year = latest.get('crop_year', 'N/A')
        lines.append(f"  - {commodity.title()} ({crop_year}): {prod} MMT production, {area} M ha planted")

    return '\n'.join(lines) if lines else "  CONAB data pending next release."


def build_enhanced_prompt(data: dict, metrics: dict) -> str:
    """Build enhanced prompt with all available data."""

    report_period = f"{(date.today() - timedelta(days=7)).strftime('%B %d')} - {date.today().strftime('%B %d, %Y')}"

    # Format cash prices
    cash_section = ""
    if data['cash_prices']:
        for p in data['cash_prices'][:6]:
            if p['price']:
                change_str = f" ({p['daily_change']:+.2f})" if p['daily_change'] else ""
                cash_section += f"  - {p['commodity'].upper()}: ${p['price']:.2f}/bu at {p['location']}, {p['state']}{change_str}\n"

    # Format futures prices
    futures_section = ""
    if data.get('futures_prices'):
        # Symbol name mapping
        symbol_names = {
            'ZC': 'Corn', 'ZW': 'Chicago Wheat', 'KE': 'KC Wheat',
            'ZS': 'Soybeans', 'ZM': 'Soybean Meal', 'ZL': 'Soybean Oil',
            'ZO': 'Oats', 'CL': 'WTI Crude', 'NG': 'Natural Gas',
            'RB': 'RBOB Gasoline', 'HO': 'Heating Oil',
            'LE': 'Live Cattle', 'HE': 'Lean Hogs', 'GF': 'Feeder Cattle'
        }
        # Get latest price for each symbol
        latest_by_symbol = {}
        for fp in data['futures_prices']:
            sym = fp['symbol']
            if sym not in latest_by_symbol:
                latest_by_symbol[sym] = fp

        # Sort by exchange/category
        ag_futures = ['ZC', 'ZW', 'KE', 'ZS', 'ZM', 'ZL', 'ZO']
        energy_futures = ['CL', 'NG', 'RB', 'HO']

        if any(s in latest_by_symbol for s in ag_futures):
            futures_section += "  Grains & Oilseeds:\n"
            for sym in ag_futures:
                if sym in latest_by_symbol:
                    fp = latest_by_symbol[sym]
                    name = symbol_names.get(sym, sym)
                    settle = fp['settlement']
                    if settle:
                        # Calculate daily change from open if available
                        change_str = ""
                        if fp['open'] and fp['open'] > 0:
                            change = settle - fp['open']
                            change_pct = (change / fp['open']) * 100
                            change_str = f" ({change:+.2f}, {change_pct:+.2f}%)"
                        futures_section += f"    - {sym} ({name}): {settle:.2f}{change_str}\n"

        if any(s in latest_by_symbol for s in energy_futures):
            futures_section += "  Energy:\n"
            for sym in energy_futures:
                if sym in latest_by_symbol:
                    fp = latest_by_symbol[sym]
                    name = symbol_names.get(sym, sym)
                    settle = fp['settlement']
                    if settle:
                        change_str = ""
                        if fp['open'] and fp['open'] > 0:
                            change = settle - fp['open']
                            change_pct = (change / fp['open']) * 100
                            change_str = f" ({change:+.2f}, {change_pct:+.2f}%)"
                        futures_section += f"    - {sym} ({name}): ${settle:.2f}{change_str}\n"

    # Format weather by region
    weather_section = ""
    if data['weather_by_region']:
        for w in data['weather_by_region']:
            if w['avg_temp_f']:
                weather_section += f"  - {w['region']}: Avg {w['avg_temp_f']}°F, Precip {w['total_precip_in']}in ({w['observations']} obs)\n"

    # Format EIA ethanol
    ethanol_section = ""
    if data.get('eia_ethanol'):
        latest = data['eia_ethanol'][0]
        ethanol_section = f"""  - Production: {latest['production_kbd']} kbd"""
        if data.get('ethanol_production_change'):
            ethanol_section += f" ({data['ethanol_production_change']:+.1f} vs prior week)"
        ethanol_section += f"\n  - Stocks: {latest['stocks_kb']:,.0f} thousand barrels"
        if data.get('ethanol_stocks_change'):
            ethanol_section += f" ({data['ethanol_stocks_change']:+,.0f} vs prior week)"
        ethanol_section += f"\n  - Blender Input: {latest['blender_input_kbd']} kbd"
        if metrics.get('implied_annual_corn_grind_mb'):
            ethanol_section += f"\n  - Implied Annual Corn Grind: ~{metrics['implied_annual_corn_grind_mb']:,.0f} million bushels"

    # Format balance sheets
    soy_balance = format_balance_sheet("US Soybean S&D", data.get('soybean_balance', []))
    meal_balance = format_balance_sheet("US Soybean Meal S&D", data.get('soybean_meal_balance', []))
    oil_balance = format_balance_sheet("US Soybean Oil S&D", data.get('soybean_oil_balance', []))
    wheat_balance = format_balance_sheet("US Wheat S&D", data.get('wheat_balance', []))

    # Format derived metrics
    metrics_section = ""
    if metrics:
        if metrics.get('soybean_stocks_to_use'):
            metrics_section += f"  - Soybean Stocks/Use Ratio: {metrics['soybean_stocks_to_use']}%\n"
        if metrics.get('wheat_stocks_to_use'):
            metrics_section += f"  - Wheat Stocks/Use Ratio: {metrics['wheat_stocks_to_use']}%\n"

    # Format market narratives
    narrative_section = ""
    if data.get('market_narratives'):
        for n in data['market_narratives'][:3]:
            if n['narrative']:
                narrative_section += f"  - {n['title']} ({n['date']}): {n['narrative'][:200]}...\n"

    prompt = f"""You are a senior agricultural commodity analyst at HigbyBarrett, a respected commodity research firm known for data-driven market analysis.

Generate a professional weekly market report for the period: {report_period}

================================================================================
REPORT STRUCTURE (Follow Exactly)
================================================================================

1. **EXECUTIVE SUMMARY** (200-250 words)
   - Key price movements and drivers
   - Most significant supply/demand developments
   - Overall market sentiment assessment

2. **MACRO & ENERGY UPDATE** (150 words)
   - USD trends and impact on exports
   - Energy prices affecting input costs
   - Ethanol market fundamentals

3. **WEATHER & CROP CONDITIONS** (150 words)
   - Current conditions in key growing regions
   - Impact on crop development/yields
   - Near-term forecast implications

4. **CORN ANALYSIS** (200 words)
   - Price action and technical levels
   - Demand factors: ethanol grind, feed, exports
   - Supply considerations
   - Near-term outlook

5. **WHEAT ANALYSIS** (200 words)
   - Price action by class (SRW, HRW, HRS)
   - Export competitiveness vs Black Sea
   - Domestic demand trends
   - Near-term outlook

6. **SOYBEAN COMPLEX ANALYSIS** (250 words)
   - Soybeans: Price action, China demand, SA competition
   - Soybean Meal: Domestic vs export demand, crush margins
   - Soybean Oil: Biofuel demand, food use trends
   - Near-term outlook

7. **KEY TRIGGERS TO WATCH** (5-7 bullet points)
   - Upcoming reports/events
   - Critical price levels
   - Weather developments to monitor

================================================================================
MARKET DATA
================================================================================

**FUTURES PRICES (Front Month Settlements):**
{futures_section if futures_section else "  No futures price data available."}

**CASH PRICES (Recent Bids):**
{cash_section if cash_section else "  Limited cash price data available for this period."}

**WEATHER CONDITIONS BY REGION:**
{weather_section if weather_section else "  Weather data shows normal seasonal conditions."}

**EIA ETHANOL DATA (Weekly):**
{ethanol_section if ethanol_section else "  No recent ethanol data available."}

**SUPPLY & DEMAND BALANCE SHEETS:**
{soy_balance}

{meal_balance}

{oil_balance}

{wheat_balance}

**DERIVED ANALYTICAL METRICS:**
{metrics_section if metrics_section else "  Insufficient data for derived metrics."}

**BRAZIL (CONAB) - South America Competition:**
{format_conab_data(data.get('conab_brazil', []))}

**MARKET COMMENTARY SNIPPETS:**
{narrative_section if narrative_section else "  No recent market narratives available."}

================================================================================
WRITING GUIDELINES
================================================================================

STYLE:
- Professional, analytical tone appropriate for institutional clients
- Reference specific data points from above where available
- Use precise language ("increased 2.3%" not "went up a bit")
- Where data is limited, state assumptions clearly

ANALYTICAL FRAMEWORK:
- Connect supply/demand fundamentals to price implications
- Consider both bullish and bearish factors objectively
- Note significant YoY changes in balance sheets
- Highlight key stocks-to-use ratios and their implications

DATA LIMITATIONS:
- If key data is missing, note it explicitly
- Use hedged language for projections ("likely," "appears to suggest")
- Do not fabricate specific numbers not provided in the data

Generate the complete report now:
"""

    return prompt


def call_ollama(prompt: str, model: str = "phi3:mini") -> str:
    """Call Ollama API to generate the report."""
    import requests

    logger.info(f"Calling Ollama with model: {model}")
    logger.info(f"Prompt length: {len(prompt)} characters")

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "num_predict": 3000,  # Increased for longer report
                    "top_p": 0.9,
                }
            },
            timeout=600  # 10 minute timeout for longer generation
        )

        if response.status_code == 200:
            result = response.json()
            generated_text = result.get("response", "")

            logger.info(f"Generation complete:")
            logger.info(f"  Response length: {len(generated_text)} characters")
            logger.info(f"  Total duration: {result.get('total_duration', 0) / 1e9:.2f} seconds")
            logger.info(f"  Eval count: {result.get('eval_count', 0)} tokens")

            return generated_text
        else:
            logger.error(f"Ollama API error: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.Timeout:
        logger.error("Ollama request timed out")
        return None
    except Exception as e:
        logger.error(f"Ollama call failed: {e}")
        return None


def save_outputs(report_text: str, data: dict, metrics: dict, prompt: str):
    """Save all outputs for analysis."""
    output_dir = PROJECT_ROOT / "output" / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save report
    report_file = output_dir / f"HB_Weekly_Report_V2_{timestamp}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"# HigbyBarrett Weekly Report (Enhanced V2)\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Model:** phi3:mini via Ollama\n")
        f.write(f"**Data Period:** {(date.today() - timedelta(days=7)).strftime('%Y-%m-%d')} to {date.today().strftime('%Y-%m-%d')}\n\n")
        f.write("---\n\n")
        f.write(report_text)

    logger.info(f"Report saved to: {report_file}")

    # Save prompt for analysis
    prompt_file = output_dir / f"HB_Weekly_PROMPT_V2_{timestamp}.txt"
    with open(prompt_file, 'w', encoding='utf-8') as f:
        f.write(prompt)
    logger.info(f"Prompt saved to: {prompt_file}")

    # Save data and metrics as JSON
    data_file = output_dir / f"HB_Weekly_DATA_V2_{timestamp}.json"
    with open(data_file, 'w', encoding='utf-8') as f:
        output_data = {
            'generated_at': datetime.now().isoformat(),
            'derived_metrics': metrics,
            'data_summary': {
                'futures_prices': len(data.get('futures_prices', [])),
                'cash_prices': len(data.get('cash_prices', [])),
                'weather_regions': len(data.get('weather_by_region', [])),
                'weather_alerts': len(data.get('weather_alerts', [])),
                'eia_ethanol_weeks': len(data.get('eia_ethanol', [])),
                'soybean_balance_years': len(data.get('soybean_balance', [])),
                'wheat_balance_years': len(data.get('wheat_balance', [])),
                'market_narratives': len(data.get('market_narratives', [])),
            },
            'raw_data': data
        }
        json.dump(output_data, f, indent=2, default=json_serializer)

    logger.info(f"Data saved to: {data_file}")

    return report_file, prompt_file, data_file


def main():
    """Main entry point for enhanced HB report generation."""
    logger.info("=" * 70)
    logger.info("HIGBYBARRETT WEEKLY REPORT V2 - ENHANCED DATA UTILIZATION")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    try:
        # Step 1: Fetch comprehensive data
        logger.info("\n--- Step 1: Fetching Comprehensive Market Data ---")
        data = fetch_comprehensive_data(days_back=7)

        # Step 2: Calculate derived metrics
        logger.info("\n--- Step 2: Calculating Derived Metrics ---")
        metrics = calculate_derived_metrics(data)
        logger.info(f"Derived metrics: {metrics}")

        # Step 3: Build enhanced prompt
        logger.info("\n--- Step 3: Building Enhanced Prompt ---")
        prompt = build_enhanced_prompt(data, metrics)
        logger.info(f"Prompt length: {len(prompt)} characters")

        # Step 4: Generate report
        logger.info("\n--- Step 4: Generating Report via Ollama ---")
        report_text = call_ollama(prompt, model="phi3:mini")

        if report_text:
            # Step 5: Save outputs
            logger.info("\n--- Step 5: Saving Outputs ---")
            report_file, prompt_file, data_file = save_outputs(
                report_text, data, metrics, prompt
            )

            logger.info("\n" + "=" * 70)
            logger.info("TEST COMPLETED SUCCESSFULLY")
            logger.info(f"Report: {report_file}")
            logger.info(f"Prompt: {prompt_file}")
            logger.info(f"Data: {data_file}")
            logger.info(f"Log: {log_file}")
            logger.info("=" * 70)

            # Print report preview
            print("\n" + "=" * 70)
            print("GENERATED REPORT PREVIEW (first 2000 chars):")
            print("=" * 70)
            print(report_text[:2000])
            if len(report_text) > 2000:
                print(f"\n... [{len(report_text) - 2000} more characters in full report]")

        else:
            logger.error("Report generation failed - no output from LLM")

    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
