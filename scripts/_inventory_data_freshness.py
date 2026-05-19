"""
Data-freshness inventory: for every major bronze data source, find the most
recent record, compare to expected release cadence, and tag as
current / behind / stale / empty.
"""
import os
import sys
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(
    host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT", "5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"),
    user=os.getenv("RLC_PG_USER"), password=os.getenv("RLC_PG_PASSWORD"),
    sslmode="require",
)
cur = conn.cursor(cursor_factory=RealDictCursor)

# (schema.table, date_column_sql, label, expected_lag_days, expected_release_freq, automation_status)
probes = [
    ("bronze.fas_psd",              "MAX(make_date(marketing_year, 1, 1))",                        "USDA FAS PSD",                       400, "monthly (WASDE day 12)", "auto"),
    ("bronze.usda_nass",            "MAX(week_ending::date)",                                       "USDA NASS multi-series",            14,  "weekly + monthly", "auto"),
    ("bronze.nass_crop_progress",   "MAX(week_ending::date)",                                       "NASS crop progress",                14,  "weekly Mon", "auto"),
    ("bronze.nass_crop_condition",  "MAX(week_ending::date)",                                       "NASS crop condition",               14,  "weekly Mon", "auto"),
    ("bronze.nass_livestock_slaughter", "MAX(period_date)",                                         "NASS livestock slaughter",          45,  "monthly", "auto"),
    ("bronze.fas_export_sales",     "MAX(week_ending::date)",                                       "USDA FAS Export Sales",             10,  "weekly Thu 8:30 ET", "auto"),
    ("bronze.nopa_monthly_crush",   "MAX(make_date(year, month, 1))",                               "NOPA monthly crush",                30,  "monthly ~15th", "auto (disabled?)"),
    ("bronze.copa_weekly_crush",    "MAX(week_ending::date)",                                       "COPA Canada crush",                 14,  "weekly", "auto"),
    ("bronze.ams_price_record",     "MAX(report_date)",                                             "USDA AMS cash prices",              14,  "weekly + daily", "auto"),
    ("bronze.ers_oilcrops_raw",     "MAX(make_date(year::int, 1, 1))",                              "USDA ERS oilcrops",                 60,  "monthly", "auto"),
    ("bronze.ers_wheat_raw",        "MAX(make_date(year::int, 1, 1))",                              "USDA ERS wheat",                    60,  "monthly", "auto"),
    ("bronze.ers_food_sales_monthly", "MAX(make_date(year, month, 1))",                             "USDA ERS food sales",               45,  "monthly", "auto"),
    ("bronze.eia_raw_ingestion",    "MAX(period::date)",                                            "EIA raw (multi)",                   14,  "weekly + monthly", "auto"),
    ("bronze.eia_ethanol",          "MAX(period::date)",                                            "EIA ethanol weekly",                8,   "weekly Wed", "auto"),
    ("bronze.eia_petroleum",        "MAX(period::date)",                                            "EIA petroleum weekly",              8,   "weekly Wed", "auto"),
    ("bronze.eia_monthly_biofuels", "MAX(period_month)",                                            "EIA monthly biofuels",              75,  "monthly (~75d lag)", "auto"),
    ("bronze.eia_capacity_monthly", "MAX(make_date(year, month, 1))",                               "EIA monthly BBD capacity",          90,  "monthly", "auto"),
    ("bronze.eia_feedstock_monthly", "MAX(make_date(year, month, 1))",                              "EIA monthly feedstock",             75,  "monthly", "auto"),
    ("bronze.eia_natural_gas",      "MAX(period::date)",                                            "EIA natural gas",                   8,   "weekly", "auto"),
    ("bronze.epa_rfs_rin_monthly",  "MAX(make_date(year, month, 1))",                               "EPA RFS RIN monthly",               75,  "monthly", "MANUAL"),
    ("bronze.epa_rfs_generation",   "MAX(make_date(year, month, 1))",                               "EPA RFS RIN generation",            75,  "monthly", "MANUAL"),
    ("bronze.epa_rfs_retirement",   "MAX(make_date(year, month, 1))",                               "EPA RFS RIN retirement",            75,  "monthly", "MANUAL"),
    ("bronze.epa_rfs_separation",   "MAX(make_date(year, month, 1))",                               "EPA RFS RIN separation",            75,  "monthly", "MANUAL"),
    ("bronze.epa_rfs_fuel_production", "MAX(make_date(year, month, 1))",                            "EPA RFS fuel production",           75,  "monthly", "MANUAL"),
    ("bronze.epa_emts_monthly",     "MAX(make_date(year, month, 1))",                               "EPA EMTS monthly",                  75,  "monthly", "MANUAL"),
    ("bronze.cftc_cot",             "MAX(report_date)",                                             "CFTC COT",                          10,  "weekly Fri 15:30 ET", "auto"),
    ("bronze.census_trade",         "MAX(period_date)",                                             "Census trade (US imp/exp)",         60,  "monthly ~6th", "auto"),
    ("bronze.drought_conditions",   "MAX(valid_date::date)",                                        "US Drought Monitor",                10,  "weekly Thu", "auto"),
    ("bronze.canada_cgc_weekly",    "MAX(week_ending::date)",                                       "Canada CGC weekly",                 14,  "weekly", "auto"),
    ("bronze.canada_cgc_exports",   "MAX(week_ending::date)",                                       "Canada CGC exports",                14,  "weekly", "auto"),
    ("bronze.canada_statscan",      "MAX(period::date)",                                            "Canada StatsCan",                   60,  "monthly", "auto"),
    ("bronze.conab_supply_demand",  "MAX(loaded_at::date)",                                         "CONAB S&D (Brazil)",                90,  "monthly", "auto"),
    ("bronze.conab_prices",         "MAX(price_date)",                                              "CONAB Brazil prices",               60,  "weekly", "auto"),
    ("bronze.mpob_industry_overview", "MAX(make_date(year, month, 1))",                             "Malaysia MPOB palm",                45,  "monthly", "auto"),
    ("bronze.weather_observations", "MAX(observation_time::date)",                                  "Weather obs (hourly)",              2,   "hourly", "auto"),
    ("bronze.weather_emails",       "MAX(email_date::date)",                                        "Weather meteorologist emails",      7,   "daily/weekly", "auto"),
    ("bronze.ndvi_data",            "MAX(observation_date::date)",                                  "NDVI satellite",                    14,  "~daily", "auto"),
    ("bronze.cme_settlements",      "MAX(settle_date)",                                             "CME settlements",                   5,   "daily", "auto"),
    ("bronze.carb_lcfs_pathways",   "MAX(snapshot_date)",                                           "CARB LCFS pathways",                30,  "monthly snapshot", "auto"),
    ("bronze.credit_prices",        "MAX(price_date)",                                              "Credit prices (D4/D6/LCFS)",        9999, "static FM snapshot (ends 2025-04)", "MANUAL replace via DTN"),
    ("bronze.fuel_prices",          "MAX(price_date)",                                              "Fuel prices (ULSD/jet/B100)",       9999, "static FM snapshot (ends 2025-04)", "MANUAL replace via DTN"),
    ("bronze.wheat_tenders",        "MAX(tender_date::date)",                                       "Wheat tenders",                     30,  "event-driven", "auto"),
    ("bronze.state_air_permits",    "MAX(loaded_at::date)",                                         "State air permits (PDF)",           365, "event-driven (Title V)", "MANUAL PDF parse"),
    ("bronze.bbd_capacity_history", "MAX(make_date(year, month, 1))",                               "BBD capacity history",              90,  "monthly", "auto"),
    ("bronze.rd_capacity_projection", "MAX(make_date(year, month, 1))",                             "RD capacity projection",            90,  "quarterly", "auto"),
    ("bronze.rail_network",         "MAX(loaded_at::date)",                                         "NTAD rail network (KMZ)",           365, "annual (Sep refresh)", "MANUAL (KMZ)"),
    ("bronze.us_ports",             "MAX(loaded_at::date)",                                         "USACE major ports",                 365, "annual", "auto (one-time pull)"),
    ("bronze.marine_highways",      "MAX(loaded_at::date)",                                         "BTS Marine Highway routes",         365, "annual", "auto (one-time pull)"),
]

today = datetime.now().date()
results = []
for tbl, date_expr, label, lag, freq, status in probes:
    try:
        cur.execute(f"SELECT {date_expr} AS d, COUNT(*) AS n FROM {tbl}")
        row = cur.fetchone()
        latest = row["d"]
        n = row["n"]
        if latest is None:
            health = "EMPTY"
            age = None
        else:
            age = (today - latest).days
            if "static" in (status + " " + freq).lower():
                health = "STATIC"
            elif age > lag * 2:
                health = "STALE"
            elif age > lag:
                health = "BEHIND"
            else:
                health = "OK"
        results.append({
            "table": tbl, "label": label, "latest": latest, "rows": n,
            "age_days": age, "expected_lag": lag, "freq": freq,
            "status": status, "health": health,
        })
    except Exception:
        conn.rollback()
        results.append({
            "table": tbl, "label": label, "latest": None, "rows": 0,
            "age_days": None, "expected_lag": lag, "freq": freq,
            "status": status, "health": "ERROR",
        })

# Output sorted by health bucket
print(f"\n{'TABLE':<42s} {'LABEL':<38s} {'LATEST':<12s} {'AGE':>6s} {'HEALTH':<8s} {'STATUS':<28s} FREQ")
print("-" * 175)

def sort_key(r):
    order = {"STALE": 0, "BEHIND": 1, "EMPTY": 2, "ERROR": 3, "STATIC": 4, "OK": 5}
    return (order.get(r["health"], 99), r["table"])

for r in sorted(results, key=sort_key):
    age_s = (str(r["age_days"]) + "d") if r["age_days"] is not None else "-"
    latest_s = str(r["latest"]) if r["latest"] else "-"
    print(f"  {r['table']:<40s} {r['label']:<38s} {latest_s:<12s} {age_s:>6s} {r['health']:<8s} {r['status']:<28s} {r['freq']}")

# Totals by health
print("\n=== Health summary ===")
buckets = {}
for r in results:
    buckets[r["health"]] = buckets.get(r["health"], 0) + 1
for h, n in sorted(buckets.items()):
    print(f"  {h:<10s} {n}")

conn.close()
