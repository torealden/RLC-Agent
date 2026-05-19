"""Check current state of AMS data in bronze + silver after partial backfill."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()

print("=== bronze.ams_price_record  May 2026 ===")
cur.execute("""SELECT report_date, COUNT(*) FROM bronze.ams_price_record
WHERE report_date >= '2026-05-01' GROUP BY report_date ORDER BY report_date""")
for r in cur.fetchall(): print(f"  {r[0]}  n={r[1]}")

print("\n=== silver.cash_price  May 2026 ===")
cur.execute("""SELECT report_date, COUNT(*) FROM silver.cash_price
WHERE report_date >= '2026-05-01' GROUP BY report_date ORDER BY report_date""")
for r in cur.fetchall(): print(f"  {r[0]}  n={r[1]}")

print("\n=== bronze.ams_price_record  past 60 days, weekday histogram ===")
cur.execute("""SELECT report_date::date AS d,
    TO_CHAR(report_date::date, 'Dy') AS dow, COUNT(*)
FROM bronze.ams_price_record
WHERE report_date >= CURRENT_DATE - INTERVAL '60 days'
GROUP BY 1,2 ORDER BY 1""")
for r in cur.fetchall(): print(f"  {r[0]}  {r[1]}  n={r[2]}")

conn.close()
