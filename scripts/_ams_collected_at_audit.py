"""When were the April records ACTUALLY written to bronze?
If daily, collected_at should be ~1 day after report_date.
If one-shot backfill, all records will share a single collected_at window.
"""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()

print("=== collected_at distribution for April 2026 report_dates ===")
cur.execute("""SELECT report_date, MIN(collected_at), MAX(collected_at), COUNT(*)
FROM bronze.ams_price_record
WHERE report_date BETWEEN '2026-04-01' AND '2026-04-30'
GROUP BY report_date ORDER BY report_date""")
for r in cur.fetchall():
    print(f"  report={r[0]}  collected_min={r[1]}  collected_max={r[2]}  n={r[3]}")

conn.close()
