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
with open('database/migrations/088_trade_export_mapped_biofuel_split.sql') as f:
    sql = f.read()
try:
    cur.execute(sql)
    conn.commit()
    print('mig 088 re-applied OK', flush=True)
except Exception as e:
    print(f'FAIL: {type(e).__name__}: {e}', flush=True)
    conn.rollback()
    conn.close()
    sys.exit(1)

# Re-verify
print('\n--- Jan 2026 sums vs WORLD TOTAL ---', flush=True)
for commodity in ('BIODIESEL','RENEWABLE_DIESEL'):
    cur.execute("""SELECT SUM(quantity) AS s FROM gold.trade_export_matrix
WHERE commodity_group=%s AND flow='imports' AND year=2026 AND month=1 AND NOT is_regional_total""", (commodity,))
    s = cur.fetchone()['s']
    cur.execute("""SELECT quantity FROM gold.trade_export_matrix
WHERE commodity_group=%s AND flow='imports' AND year=2026 AND month=1 AND country_name='WORLD TOTAL'""", (commodity,))
    w = cur.fetchone()
    w = w['quantity'] if w else None
    print(f"  {commodity}: country_sum={s}  WORLD_TOTAL={w}", flush=True)
conn.close()
