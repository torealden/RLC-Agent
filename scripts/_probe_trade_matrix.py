import os, sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(host=os.getenv('RLC_PG_HOST'), port=os.getenv('RLC_PG_PORT','5432'),
    dbname=os.getenv('RLC_PG_DB','rlc_commodities'), user=os.getenv('RLC_PG_USER'),
    password=os.getenv('RLC_PG_PASSWORD'), sslmode='require', connect_timeout=10)
cur = conn.cursor(cursor_factory=RealDictCursor)

print('=== silver.trade_country_reference sample ===', flush=True)
cur.execute("SELECT * FROM silver.trade_country_reference WHERE country_name IN ('ARGENTINA','CANADA','GERMANY','BRAZIL') ORDER BY country_name LIMIT 10")
for r in cur.fetchall(): print(f'  {dict(r)}', flush=True)

print('=== silver.trade_commodity_reference for HS 3826 ===', flush=True)
cur.execute("SELECT * FROM silver.trade_commodity_reference WHERE hs_code_10 LIKE '3826%'")
for r in cur.fetchall(): print(f'  {dict(r)}', flush=True)

print('=== gold.trade_export_matrix sample row for BIODIESEL ===', flush=True)
cur.execute("SELECT * FROM gold.trade_export_matrix WHERE commodity_group='BIODIESEL' AND flow='imports' AND year=2024 AND month=6 LIMIT 5")
for r in cur.fetchall(): print(f'  {dict(r)}', flush=True)
conn.close()
