"""Morning briefing: facility agents, AMS overnight, freshness signals."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=== EPA ECHO runs overnight (since 2026-05-20 00:00 UTC) ===")
cur.execute("""SELECT collector_name, run_started_at, run_finished_at, status,
    rows_collected, rows_inserted, error_message
FROM core.collection_status
WHERE collector_name LIKE 'epa_echo_%'
  AND run_started_at >= '2026-05-20 00:00:00'
ORDER BY run_started_at""")
rows = cur.fetchall()
if rows:
    for r in rows:
        dur = ((r['run_finished_at'] - r['run_started_at']).total_seconds()
               if r['run_finished_at'] else None)
        print(f"  {r['run_started_at']}  {r['collector_name']:<22s}  {r['status']:<10s}  "
              f"in={r['rows_collected']!s:<5s}  out={r['rows_inserted']!s:<5s}  "
              f"dur={dur!s}s")
        if r['error_message']:
            print(f"    ERROR: {r['error_message'][:200]}")
else:
    print("  (no runs since 2026-05-20 00:00 UTC)")

print("\n=== AMS overnight run (5/19 21:00 UTC, the first one with the .collect() fix) ===")
cur.execute("""SELECT collector_name, run_started_at, run_finished_at, status,
    rows_collected, rows_inserted, data_period
FROM core.collection_status
WHERE collector_name = 'usda_ams_cash_prices'
  AND run_started_at >= '2026-05-19 20:00:00'
ORDER BY run_started_at DESC""")
for r in cur.fetchall():
    print(f"  {r['run_started_at']}  status={r['status']}  in={r['rows_collected']}  out={r['rows_inserted']}  period={r['data_period']}")

print("\n=== bronze.ams_price_record for 2026-05-19 (yesterday's data should now be there) ===")
cur.execute("""SELECT COUNT(*), COUNT(DISTINCT slug_id) AS slugs
FROM bronze.ams_price_record WHERE report_date = '2026-05-19'""")
r = cur.fetchone()
print(f"  records={r['count']}  distinct slugs={r['slugs']}")

print("\n=== bronze.epa_echo_facility freshness ===")
cur.execute("""SELECT search_profile, COUNT(*) AS n, MAX(collected_at) AS last_seen
FROM bronze.epa_echo_facility
GROUP BY search_profile ORDER BY search_profile""")
for r in cur.fetchall():
    print(f"  {r['search_profile']!s:<28s}  n={r['n']:>5d}  last={r['last_seen']}")

print("\n=== Last 15 dispatcher runs overall ===")
cur.execute("""SELECT collector_name, run_started_at, status, rows_collected
FROM core.collection_status
WHERE run_started_at >= '2026-05-19 20:00:00'
ORDER BY run_started_at DESC LIMIT 15""")
for r in cur.fetchall():
    print(f"  {r['run_started_at']}  {r['collector_name']:<28s}  {r['status']:<8s}  rows={r['rows_collected']}")

conn.close()
