"""Inspect failed AMS runs: what error messages and what's the timing?"""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

# Last 25 runs for usda_ams_cash_prices
print("=== Last 25 usda_ams_cash_prices runs (all statuses) ===")
cur.execute("""SELECT run_started_at, run_finished_at, status,
    rows_collected, rows_inserted, error_message, data_period
FROM core.collection_status
WHERE collector_name = 'usda_ams_cash_prices'
ORDER BY run_started_at DESC LIMIT 25""")
for r in cur.fetchall():
    dur = None
    if r['run_finished_at'] and r['run_started_at']:
        dur = (r['run_finished_at'] - r['run_started_at']).total_seconds()
    print(f"\n  {r['run_started_at']}  status={r['status']!s:<10s}  "
          f"in={r['rows_collected']!s:<6s}  out={r['rows_inserted']!s:<6s}  "
          f"dur={dur!s:<6s}s  period={r['data_period']!s}")
    if r['error_message']:
        print(f"    ERROR: {r['error_message'][:400]}")

# Compare: pattern across daily runs — what's the trend in rows_inserted?
print("\n\n=== usda_ams_cash_prices: daily aggregate, April-May ===")
cur.execute("""SELECT run_started_at::date AS d,
    COUNT(*) AS runs,
    SUM(rows_collected) AS total_in,
    SUM(rows_inserted) AS total_out,
    BOOL_OR(status='success') AS any_success,
    STRING_AGG(DISTINCT status, ',') AS statuses
FROM core.collection_status
WHERE collector_name = 'usda_ams_cash_prices'
  AND run_started_at >= '2026-04-15'
GROUP BY 1 ORDER BY 1""")
for r in cur.fetchall():
    print(f"  {r['d']}  runs={r['runs']:>2d}  in={r['total_in']!s:<8s}  out={r['total_out']!s:<8s}  statuses={r['statuses']}")

conn.close()
