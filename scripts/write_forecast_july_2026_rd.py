"""
Write a structured forecast for US Renewable Diesel monthly production,
July 2026, to core.forecasts.

Methodology + rationale documented at
docs/specs/forecast_july_2026_rd_production.md.

Designed to be re-runnable; uses ON CONFLICT to update if the row exists.
"""
import os
import sys
from datetime import date, datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv
load_dotenv()
import psycopg2

# ---- Forecast values (rationale in companion markdown) ---------------

FORECAST = {
    'forecast_id':       'rd_us_monthly_2026-07_v1',
    'forecast_date':     date(2026, 5, 21),
    'target_date':       date(2026, 7, 1),
    'commodity':         'renewable_diesel',
    'country':           'US',
    'forecast_type':     'monthly_production_mbbl',
    'value':             5800.0,                 # central
    'confidence_low':    5500.0,
    'confidence_high':   6100.0,
    'unit':              'MBBL',                 # thousand barrels
    'marketing_year':    None,                   # RD doesn't have a USDA MY
    'source':            'llm_forecast_book',
    'analyst':           'claude_ui_2026-05-21',
    'notes': (
        "Central forecast 5,800 MBBL. Range 5,500-6,100 MBBL.\n"
        "Anchored to July 2025 baseline 6,224 MBBL with a -7% YoY adjustment\n"
        "consistent with trailing-12 monthly trend. Subtracts ~115 MBBL/mo\n"
        "from Chevron REG Ralston + Madison idle (50 mmgy total offline).\n"
        "Assumes 45Z PTC remains active (extension_2031 scenario), feedstock\n"
        "economics in BBD-calibrated range (UCO ~42c/lb, SBO ~48c/lb), no\n"
        "major refinery turnaround announced for July at the four largest\n"
        "operators (DGD, Marathon Martinez, Valero Diamond, Phillips 66 Rodeo).\n"
        "Re-evaluate after June EIA monthly release; revise if D4 RIN drops\n"
        "below $1.20 or any major capacity goes offline."
    ),
}


# ---- Persist -----------------------------------------------------------

conn = psycopg2.connect(
    host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT", "5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"),
    user=os.getenv("RLC_PG_USER"), password=os.getenv("RLC_PG_PASSWORD"),
    sslmode="require",
)
cur = conn.cursor()

# Check if forecast_id exists, then INSERT or UPDATE
cur.execute("""
    SELECT EXISTS (
      SELECT 1 FROM core.forecasts WHERE forecast_id = %s
    )
""", (FORECAST['forecast_id'],))
exists = cur.fetchone()[0]

if exists:
    cur.execute("""
        UPDATE core.forecasts SET
            forecast_date = %(forecast_date)s,
            target_date = %(target_date)s,
            commodity = %(commodity)s,
            country = %(country)s,
            forecast_type = %(forecast_type)s,
            value = %(value)s,
            confidence_low = %(confidence_low)s,
            confidence_high = %(confidence_high)s,
            unit = %(unit)s,
            marketing_year = %(marketing_year)s,
            source = %(source)s,
            analyst = %(analyst)s,
            notes = %(notes)s
        WHERE forecast_id = %(forecast_id)s
    """, FORECAST)
    print(f"Updated existing forecast: {FORECAST['forecast_id']}")
else:
    cur.execute("""
        INSERT INTO core.forecasts
            (forecast_id, forecast_date, target_date, commodity, country,
             forecast_type, value, confidence_low, confidence_high, unit,
             marketing_year, source, analyst, notes, created_at)
        VALUES
            (%(forecast_id)s, %(forecast_date)s, %(target_date)s,
             %(commodity)s, %(country)s, %(forecast_type)s, %(value)s,
             %(confidence_low)s, %(confidence_high)s, %(unit)s,
             %(marketing_year)s, %(source)s, %(analyst)s, %(notes)s,
             NOW())
    """, FORECAST)
    print(f"Inserted new forecast: {FORECAST['forecast_id']}")

conn.commit()
print(f"  target_date:    {FORECAST['target_date']}")
print(f"  central:        {FORECAST['value']:,.0f} {FORECAST['unit']}")
print(f"  confidence:     {FORECAST['confidence_low']:,.0f} - {FORECAST['confidence_high']:,.0f}")
print(f"  analyst:        {FORECAST['analyst']}")

conn.close()
