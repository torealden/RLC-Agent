import os, sys
sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import psycopg2
conn = psycopg2.connect(host=os.getenv("RLC_PG_HOST"), port=os.getenv("RLC_PG_PORT","5432"),
    dbname=os.getenv("RLC_PG_DB", "rlc_commodities"), user=os.getenv("RLC_PG_USER"),
    password=os.getenv("RLC_PG_PASSWORD"), sslmode="require")
cur = conn.cursor()
cur.execute("""SELECT report_date, COUNT(*) FROM bronze.ams_price_record
WHERE report_date >= '2026-05-01' GROUP BY report_date ORDER BY report_date DESC LIMIT 12""")
for r in cur.fetchall(): print(f"  {r[0]}  n={r[1]}")
# also count silver
cur.execute("""SELECT report_date, COUNT(*) FROM silver.cash_price
WHERE report_date >= '2026-05-01' GROUP BY report_date ORDER BY report_date DESC LIMIT 12""")
print("\nsilver.cash_price:")
for r in cur.fetchall(): print(f"  {r[0]}  n={r[1]}")
conn.close()
