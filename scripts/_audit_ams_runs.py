"""Find the AMS collector failure point in core.collection_status."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

# Distinct collector names — find any ams variant
print("=== distinct collector_name values containing 'ams' or 'price' or 'cash' ===")
cur.execute("""SELECT DISTINCT collector_name, COUNT(*) AS runs
FROM core.collection_status
WHERE collector_name ILIKE '%ams%' OR collector_name ILIKE '%price%' OR collector_name ILIKE '%cash%'
GROUP BY 1 ORDER BY 2 DESC""")
for r in cur.fetchall():
    print(f"  {r['collector_name']:<35s} runs={r['runs']}")

# Recent runs for AMS-related collectors
print("\n=== Last 15 AMS-related collector runs ===")
cur.execute("""SELECT collector_name, run_started_at, run_finished_at, status,
rows_collected, rows_inserted, is_new_data, error_message, data_period, triggered_by
FROM core.collection_status
WHERE collector_name ILIKE '%ams%' OR collector_name ILIKE '%cash%price%'
ORDER BY run_started_at DESC LIMIT 15""")
for r in cur.fetchall():
    print(f"\n  {r['run_started_at']}  {r['collector_name']}")
    print(f"    status={r['status']!s:<10s} rows_in={r['rows_collected']!s:<6s} rows_out={r['rows_inserted']!s:<6s} period={r['data_period']!s}")
    print(f"    triggered_by={r['triggered_by']}  is_new={r['is_new_data']}")
    if r['error_message']:
        print(f"    ERROR: {r['error_message'][:300]}")

# What does the most-recent AMS failure look like? Sort by failure pattern
print("\n=== AMS run pattern: status counts in last 90 days ===")
cur.execute("""SELECT status, COUNT(*) AS n,
MIN(run_started_at) AS first_seen, MAX(run_started_at) AS last_seen
FROM core.collection_status
WHERE collector_name ILIKE '%ams%'
  AND run_started_at >= NOW() - INTERVAL '90 days'
GROUP BY status ORDER BY 2 DESC""")
for r in cur.fetchall():
    print(f"  status={r['status']!s:<12s} n={r['n']:>4d}  first={r['first_seen']}  last={r['last_seen']}")

# Day-by-day for the last 30 days
print("\n=== Run-or-not by day (last 25 days) ===")
cur.execute("""SELECT run_started_at::date AS day, COUNT(*) AS runs,
MAX(status) AS sample_status,
MAX(rows_collected) AS max_rows,
BOOL_OR(status='success') AS any_success,
BOOL_OR(status='failed' OR error_message IS NOT NULL) AS any_failure
FROM core.collection_status
WHERE collector_name ILIKE '%ams%'
  AND run_started_at >= NOW() - INTERVAL '30 days'
GROUP BY 1 ORDER BY 1 DESC""")
for r in cur.fetchall():
    flag = '✓' if r['any_success'] else 'X' if r['any_failure'] else '?'
    print(f"  {r['day']}  runs={r['runs']:>2d}  rows={r['max_rows']!s:<6s}  status={r['sample_status']!s:<10s}  {flag}")

conn.close()
