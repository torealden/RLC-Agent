"""Final verification: bronze + silver coverage by date."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2

conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()

print("=== bronze.ams_price_record  past 30 days ===")
cur.execute("""SELECT report_date::date AS d, TO_CHAR(report_date::date, 'Dy') AS dow,
    COUNT(*), COUNT(DISTINCT slug_id) AS slugs
FROM bronze.ams_price_record
WHERE report_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1,2 ORDER BY 1""")
for r in cur.fetchall():
    flag = "" if r[1] in ("Sat","Sun") else " *"
    print(f"  {r[0]}  {r[1]}  n={r[2]:>4d}  slugs={r[3]:>2d}{flag}")

print("\n=== silver.cash_price  past 30 days ===")
cur.execute("""SELECT report_date::date AS d, COUNT(*),
    COUNT(DISTINCT commodity) AS commodities,
    COUNT(DISTINCT location_state) AS states
FROM silver.cash_price
WHERE report_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1 ORDER BY 1""")
for r in cur.fetchall():
    print(f"  {r[0]}  n={r[1]:>4d}  commodities={r[2]:>2d}  states={r[3]:>2d}")

conn.close()
