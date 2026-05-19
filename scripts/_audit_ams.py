"""Investigate AMS cash-price collector: data freshness, event log, schedule."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor(cursor_factory=RealDictCursor)

# 1. What tables track collector runs?
print("=== Tables that track collector runs ===")
cur.execute("""SELECT table_schema, table_name FROM information_schema.tables
WHERE (table_name ILIKE '%event_log%' OR table_name ILIKE '%collection%'
    OR table_name ILIKE '%run%' OR table_name ILIKE '%status%' OR table_name ILIKE '%freshness%')
AND table_schema IN ('core','bronze','silver','gold','reference','audit','meta','config')
ORDER BY 1,2""")
for r in cur.fetchall():
    cur2 = conn.cursor()
    try:
        cur2.execute(f"SELECT COUNT(*) AS n FROM {r['table_schema']}.{r['table_name']}")
        print(f"  {r['table_schema']}.{r['table_name']:<42s} n={cur2.fetchone()[0]}")
    except Exception:
        conn.rollback()
    cur2.close()

# 2. AMS column schema first
print("\n=== bronze.ams_price_record columns ===")
cur.execute("""SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='bronze' AND table_name='ams_price_record' ORDER BY ordinal_position""")
for r in cur.fetchall(): print(f"  {r['column_name']:<28s} {r['data_type']}")

print("\n=== Most recent AMS dates (top 15) ===")
cur.execute("""SELECT report_date, COUNT(*) AS n
FROM bronze.ams_price_record
WHERE report_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY report_date ORDER BY report_date DESC LIMIT 15""")
for r in cur.fetchall():
    print(f"  {r['report_date']}  n={r['n']:>5d}")

# 3. Show last 30 days of AMS records
print("\n=== Most recent AMS dates (top 10) ===")
cur.execute("""SELECT report_date, COUNT(*) AS n, COUNT(DISTINCT commodity) AS commodities
FROM bronze.ams_price_record
WHERE report_date >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY report_date ORDER BY report_date DESC LIMIT 10""")
for r in cur.fetchall():
    print(f"  {r['report_date']}  n={r['n']:>5d}  commodities={r['commodities']}")

# 4. AMS-related event log entries
print("\n=== Recent collector runs (core.event_log if exists) ===")
try:
    cur.execute("""SELECT event_type, source, status, created_at, details::text AS d
    FROM core.event_log WHERE created_at >= NOW() - INTERVAL '30 days'
      AND (source ILIKE '%ams%' OR event_type ILIKE '%ams%' OR details::text ILIKE '%ams%')
    ORDER BY created_at DESC LIMIT 20""")
    for r in cur.fetchall():
        d = (r['d'] or '')[:160]
        print(f"  {r['created_at']}  {r['event_type']!s:<28s} {r['source']!s:<18s} {r['status']!s:<10s}")
        if d: print(f"      {d}")
except Exception as e:
    print(f"  event_log query failed: {type(e).__name__}: {e}")
    conn.rollback()

# 5. Collector registry / schedule
print("\n=== Collector registry / config ===")
for cand in ['config.collectors', 'reference.collectors', 'reference.collector_registry',
             'config.scheduled_jobs', 'core.collection_status']:
    try:
        cur.execute(f"SELECT * FROM {cand} LIMIT 1")
        cols = [d.name for d in cur.description]
        print(f"  {cand}: cols = {cols}")
    except Exception:
        conn.rollback()

conn.close()
