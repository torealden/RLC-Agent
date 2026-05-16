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
with open('database/migrations/090_saf_trade_candidates.sql') as f:
    cur.execute(f.read())
conn.commit()
print('mig 090 v2 applied (with volume cap)', flush=True)

print('\n=== Annual SAF candidate flows (post-fix) ===', flush=True)
cur.execute("""SELECT year, flow,
SUM(quantity_gal)/1e3 AS k_gal, SUM(value_usd)/1e6 AS mil_usd, COUNT(*) n
FROM gold.saf_trade_candidates GROUP BY year, flow ORDER BY year, flow""")
for r in cur.fetchall():
    print(f"  {r['year']} {r['flow']:8s} n={r['n']:>3d}  {float(r['k_gal']):>7.1f}k gal  ${float(r['mil_usd']):>6.2f}M", flush=True)

print('\n=== 2024-2025 top SAF cargoes by origin country ===', flush=True)
cur.execute("""SELECT country_code, country_name, flow, COUNT(*) n,
SUM(quantity_gal)/1e3 AS k_gal, SUM(value_usd)/1e6 AS mil_usd
FROM gold.saf_trade_candidates WHERE year IN (2024, 2025) AND flow='imports'
GROUP BY country_code, country_name, flow ORDER BY mil_usd DESC LIMIT 15""")
for r in cur.fetchall():
    print(f"  {r['country_code']:6s} {r['country_name']:25s} n={r['n']:>3d} {float(r['k_gal']):>6.1f}k gal ${float(r['mil_usd']):>5.2f}M", flush=True)
conn.close()
