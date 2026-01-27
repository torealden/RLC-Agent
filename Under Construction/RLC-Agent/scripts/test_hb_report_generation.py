#!/usr/bin/env python3
"""
HigbyBarrett Weekly Report Generation Test
==========================================
Tests the LLM's ability to generate a HigbyBarrett-style weekly commodity report
using available database data.

This script:
1. Queries all available market data from the past 7 days
2. Sends data to Ollama LLM with a structured prompt
3. Generates a HigbyBarrett-style report
4. Logs everything for analysis

Usage:
    python scripts/test_hb_report_generation.py
"""

import os
import sys
import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)

# Configure comprehensive logging
LOG_DIR = PROJECT_ROOT / "output" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

log_file = LOG_DIR / f"hb_report_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("HB-ReportTest")


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


def fetch_market_data(days_back: int = 7) -> dict:
    """Fetch all available market data from the database."""
    logger.info(f"Fetching market data for the last {days_back} days...")

    conn = get_database_connection()
    cur = conn.cursor()

    start_date = (date.today() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    data = {}

    # 1. Cash Prices
    logger.info("Fetching cash prices...")
    cur.execute(f"""
        SELECT commodity, report_date, location_name, price_cash, basis, change_daily, unit
        FROM silver.cash_price
        WHERE report_date >= '{start_date}'
        ORDER BY commodity, report_date DESC
    """)
    rows = cur.fetchall()
    data['cash_prices'] = [
        {
            'commodity': r[0], 'date': str(r[1]), 'location': r[2],
            'price': float(r[3]) if r[3] else None, 'basis': float(r[4]) if r[4] else None,
            'daily_change': float(r[5]) if r[5] else None, 'unit': r[6]
        }
        for r in rows
    ]
    logger.info(f"  Found {len(data['cash_prices'])} cash price records")

    # 2. Weather Summary (Gold layer)
    logger.info("Fetching weather summaries...")
    cur.execute(f"""
        SELECT display_name, region, observation_date,
               temp_high_f, temp_low_f, temp_avg_f,
               precipitation_in, humidity_pct, conditions_text,
               temp_7day_avg_f, precip_7day_total_in
        FROM gold.weather_summary
        WHERE observation_date >= '{start_date}'
        ORDER BY region, observation_date DESC
    """)
    rows = cur.fetchall()
    data['weather'] = [
        {
            'location': r[0], 'region': r[1], 'date': str(r[2]),
            'temp_high': float(r[3]) if r[3] else None,
            'temp_low': float(r[4]) if r[4] else None,
            'temp_avg': float(r[5]) if r[5] else None,
            'precipitation': float(r[6]) if r[6] else None,
            'humidity': float(r[7]) if r[7] else None,
            'conditions': r[8],
            'temp_7day_avg': float(r[9]) if r[9] else None,
            'precip_7day_total': float(r[10]) if r[10] else None
        }
        for r in rows
    ]
    logger.info(f"  Found {len(data['weather'])} weather records")

    # 3. Price Report Narratives (Bronze layer)
    logger.info("Fetching price report narratives...")
    cur.execute(f"""
        SELECT report_title, report_date, market_type, report_narrative
        FROM bronze.price_report_raw
        WHERE report_date >= '{start_date}'
        ORDER BY report_date DESC
        LIMIT 20
    """)
    rows = cur.fetchall()
    data['price_reports'] = [
        {
            'title': r[0], 'date': str(r[1]), 'market': r[2],
            'narrative': r[3][:500] if r[3] else None  # Limit narrative length
        }
        for r in rows
    ]
    logger.info(f"  Found {len(data['price_reports'])} price report narratives")

    # 4. Balance Sheet Data (Gold layer)
    logger.info("Fetching soybean balance sheet...")
    cur.execute("""
        SELECT * FROM gold.us_soybean_balance_sheet
        ORDER BY marketing_year DESC
        LIMIT 5
    """)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    data['soybean_balance'] = [dict(zip(columns, row)) for row in rows]
    logger.info(f"  Found {len(data['soybean_balance'])} balance sheet records")

    # 5. EIA Energy Data
    logger.info("Fetching EIA data...")
    try:
        cur.execute("""
            SELECT * FROM gold.eia_ethanol_weekly
            ORDER BY week_ending DESC
            LIMIT 20
        """)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        data['eia_ethanol'] = [dict(zip(columns, row)) for row in rows]
        logger.info(f"  Found {len(data['eia_ethanol'])} EIA ethanol records")
    except Exception as e:
        logger.warning(f"  EIA query failed: {e}")
        conn.rollback()  # Rollback to continue with other queries
        data['eia_ethanol'] = []

    # 6. Weather Locations
    logger.info("Fetching weather locations...")
    try:
        cur.execute("SELECT id, display_name, region, country FROM public.weather_location")
        rows = cur.fetchall()
        data['weather_locations'] = [
            {'id': r[0], 'name': r[1], 'region': r[2], 'country': r[3]}
            for r in rows
        ]
        logger.info(f"  Found {len(data['weather_locations'])} weather locations")
    except Exception as e:
        logger.warning(f"  Weather locations query failed: {e}")
        conn.rollback()
        data['weather_locations'] = []

    conn.close()

    return data


def read_sample_report() -> str:
    """Read a sample HigbyBarrett report structure for reference."""
    # Read the automation guide for report structure
    guide_path = PROJECT_ROOT / "docs" / "HB_REPORT_AUTOMATION_GUIDE.md"
    if guide_path.exists():
        with open(guide_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Extract just the commodity section
            return content[:3000]  # First 3000 chars for structure reference
    return ""


def build_report_prompt(market_data: dict) -> str:
    """Build the comprehensive prompt for report generation."""

    # Format the data for the prompt
    cash_summary = ""
    if market_data['cash_prices']:
        for p in market_data['cash_prices'][:10]:
            cash_summary += f"  - {p['commodity']}: ${p['price']:.2f} at {p['location']} ({p['date']})\n" if p['price'] else ""

    weather_summary = ""
    if market_data['weather']:
        # Group by region
        regions = {}
        for w in market_data['weather'][:30]:
            region = w['region'] or 'Unknown'
            if region not in regions:
                regions[region] = []
            regions[region].append(w)

        for region, records in regions.items():
            if records:
                avg_temp = sum(r['temp_avg'] for r in records if r['temp_avg']) / max(1, len([r for r in records if r['temp_avg']]))
                total_precip = sum(r['precipitation'] or 0 for r in records)
                weather_summary += f"  - {region}: Avg temp {avg_temp:.1f}F, Total precip {total_precip:.2f}in\n"

    narrative_summary = ""
    if market_data['price_reports']:
        for rpt in market_data['price_reports'][:5]:
            if rpt['narrative']:
                narrative_summary += f"  - {rpt['title']} ({rpt['date']}): {rpt['narrative'][:200]}...\n"

    balance_summary = ""
    if market_data['soybean_balance']:
        for b in market_data['soybean_balance'][:2]:
            balance_summary += f"  - MY {b.get('marketing_year', 'N/A')}: Production={b.get('production', 'N/A')}, Exports={b.get('exports', 'N/A')}, Ending Stocks={b.get('ending_stocks', 'N/A')}\n"

    prompt = f"""You are an expert agricultural commodity analyst at HigbyBarrett, a respected commodity research firm.
Write a professional weekly market report covering the last 7 days of agricultural commodity market activity.

REPORT STRUCTURE (follow this format exactly):
1. Executive Summary (2-3 paragraphs summarizing key market moves)
2. Macro Update (USD, energy prices, general economic factors)
3. Weather Update (conditions in major growing regions)
4. Corn Analysis (price action, fundamentals, outlook)
5. Wheat Analysis (price action, fundamentals, outlook)
6. Soybeans & Products Analysis (soybeans, meal, oil)
7. Key Triggers to Watch (bullet points of upcoming catalysts)

AVAILABLE MARKET DATA:

**Cash Prices (Recent):**
{cash_summary if cash_summary else "  Limited cash price data available for this period."}

**Weather Conditions by Region:**
{weather_summary if weather_summary else "  Weather data shows normal seasonal conditions."}

**Market Reports/Narratives:**
{narrative_summary if narrative_summary else "  No recent market narratives available."}

**US Soybean Balance Sheet:**
{balance_summary if balance_summary else "  Balance sheet data not available."}

**Report Period:** {(date.today() - timedelta(days=7)).strftime('%B %d')} - {date.today().strftime('%B %d, %Y')}

WRITING STYLE:
- Professional, analytical tone (like a commodity research house)
- Reference specific data points where available
- Use hedged language ("appears to," "likely," "suggests") where data is limited
- Note data limitations transparently
- Target length: 800-1200 words

Generate the complete weekly report now:
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
                    "num_predict": 2000
                }
            },
            timeout=300  # 5 minute timeout
        )

        if response.status_code == 200:
            result = response.json()
            generated_text = result.get("response", "")

            # Log generation stats
            logger.info(f"Generation complete:")
            logger.info(f"  Response length: {len(generated_text)} characters")
            logger.info(f"  Total duration: {result.get('total_duration', 0) / 1e9:.2f} seconds")
            logger.info(f"  Eval count: {result.get('eval_count', 0)} tokens")

            return generated_text
        else:
            logger.error(f"Ollama API error: {response.status_code} - {response.text}")
            return None

    except requests.exceptions.Timeout:
        logger.error("Ollama request timed out after 5 minutes")
        return None
    except Exception as e:
        logger.error(f"Ollama call failed: {e}")
        return None


def save_report(report_text: str, market_data: dict):
    """Save the generated report and supporting data."""
    output_dir = PROJECT_ROOT / "output" / "reports"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save report
    report_file = output_dir / f"HB_Weekly_Report_TEST_{timestamp}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(f"# HigbyBarrett Weekly Report (LLM Generated Test)\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"**Model:** phi3:mini via Ollama\n")
        f.write(f"**Data Period:** {(date.today() - timedelta(days=7)).strftime('%Y-%m-%d')} to {date.today().strftime('%Y-%m-%d')}\n\n")
        f.write("---\n\n")
        f.write(report_text)

    logger.info(f"Report saved to: {report_file}")

    # Save supporting data as JSON
    data_file = output_dir / f"HB_Weekly_Report_DATA_{timestamp}.json"
    with open(data_file, 'w', encoding='utf-8') as f:
        # Convert dates to strings for JSON serialization
        serializable_data = {
            'generated_at': datetime.now().isoformat(),
            'data_summary': {
                'cash_prices_count': len(market_data['cash_prices']),
                'weather_records_count': len(market_data['weather']),
                'price_reports_count': len(market_data['price_reports']),
                'balance_sheet_records': len(market_data['soybean_balance']),
                'weather_locations': len(market_data['weather_locations'])
            },
            'raw_data': {k: v for k, v in market_data.items() if k != 'eia_ethanol'}  # Skip large EIA data
        }
        json.dump(serializable_data, f, indent=2, default=str)

    logger.info(f"Data saved to: {data_file}")

    return report_file, data_file


def main():
    """Main entry point for the HB report generation test."""
    logger.info("=" * 70)
    logger.info("HIGBYBARRETT WEEKLY REPORT GENERATION TEST")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    try:
        # Step 1: Fetch market data
        logger.info("\n--- Step 1: Fetching Market Data ---")
        market_data = fetch_market_data(days_back=7)

        # Log data summary
        logger.info("\n--- Data Summary ---")
        logger.info(f"Cash Prices: {len(market_data['cash_prices'])} records")
        logger.info(f"Weather: {len(market_data['weather'])} records")
        logger.info(f"Price Reports: {len(market_data['price_reports'])} records")
        logger.info(f"Soybean Balance: {len(market_data['soybean_balance'])} records")
        logger.info(f"Weather Locations: {len(market_data['weather_locations'])} locations")

        # Step 2: Build prompt
        logger.info("\n--- Step 2: Building Report Prompt ---")
        prompt = build_report_prompt(market_data)
        logger.debug(f"Prompt preview:\n{prompt[:1000]}...")

        # Step 3: Generate report via LLM
        logger.info("\n--- Step 3: Generating Report via Ollama ---")
        report_text = call_ollama(prompt, model="phi3:mini")

        if report_text:
            logger.info("\n--- Generated Report Preview ---")
            logger.info(report_text[:500] + "...")

            # Step 4: Save outputs
            logger.info("\n--- Step 4: Saving Outputs ---")
            report_file, data_file = save_report(report_text, market_data)

            logger.info("\n" + "=" * 70)
            logger.info("TEST COMPLETED SUCCESSFULLY")
            logger.info(f"Report: {report_file}")
            logger.info(f"Data: {data_file}")
            logger.info(f"Log: {log_file}")
            logger.info("=" * 70)

            # Print the full report to console
            print("\n" + "=" * 70)
            print("GENERATED REPORT:")
            print("=" * 70)
            print(report_text)

        else:
            logger.error("Report generation failed - no output from LLM")

    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
