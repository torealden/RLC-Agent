"""Audit the CFTC COT 2-week data gap — what's missing, what runs ran, what failed."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=== bronze.cftc_cot — most recent report dates (top 25) ===")
cur.execute("""SELECT report_date, COUNT(*) AS n,
    COUNT(DISTINCT commodity) AS commodities,
    MAX(collected_at) AS collected_at
FROM bronze.cftc_cot
WHERE report_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY report_date ORDER BY report_date DESC LIMIT 25""")
for r in cur.fetchall():
    print(f"  {r['report_date']}  n={r['n']:>5d}  commodities={r['commodities']:>2d}  collected={r['collected_at']}")

print("\n=== core.collection_status — cftc_cot runs since 2026-04-01 ===")
cur.execute("""SELECT run_started_at, run_finished_at, status, rows_collected,
    rows_inserted, data_period, error_message, triggered_by
FROM core.collection_status
WHERE collector_name = 'cftc_cot'
  AND run_started_at >= '2026-04-01'
ORDER BY run_started_at DESC""")
for r in cur.fetchall():
    dur = ((r['run_finished_at'] - r['run_started_at']).total_seconds()
           if r['run_finished_at'] else None)
    print(f"  {r['run_started_at']}  status={r['status']:<8s}  in={r['rows_collected']!s:<5s}  "
          f"out={r['rows_inserted']!s:<5s}  dur={dur!s}s  period={r['data_period']}  by={r['triggered_by']}")
    if r['error_message']:
        print(f"    ERROR: {r['error_message'][:300]}")

print("\n=== CFTC release schedule — Friday releases of COT for the previous Tuesday ===")
# Expected CFTC publish dates over the past 6 weeks (Fridays)
cur.execute("""SELECT day::date AS friday
FROM generate_series(CURRENT_DATE - INTERVAL '60 days', CURRENT_DATE, INTERVAL '1 day') day
WHERE EXTRACT(DOW FROM day) = 5
ORDER BY day DESC""")
fridays = [r['friday'] for r in cur.fetchall()]
print(f"  Recent Fridays: {fridays}")

# Map each Friday to the report_date it should contain (the prior Tuesday)
print("\n  Expected report_date in DB for each Friday release:")
for fri in fridays:
    cur.execute("""SELECT COUNT(*) AS n FROM bronze.cftc_cot WHERE report_date = %s""",
                (fri - __import__('datetime').timedelta(days=3),))
    n = cur.fetchone()['n']
    flag = "OK" if n > 0 else "MISSING"
    print(f"    Fri {fri} -> Tue {fri - __import__('datetime').timedelta(days=3)}: {flag} (n={n})")

conn.close()
