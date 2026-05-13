"""Quick diagnostic for biodiesel trade flow state."""
import os, sys
sys.stdout.reconfigure(encoding='utf-8')
from dotenv import load_dotenv
load_dotenv()
import psycopg2
from psycopg2.extras import RealDictCursor

conn = psycopg2.connect(
    host=os.getenv('RLC_PG_HOST'), port=os.getenv('RLC_PG_PORT','5432'),
    dbname=os.getenv('RLC_PG_DB','rlc_commodities'),
    user=os.getenv('RLC_PG_USER'), password=os.getenv('RLC_PG_PASSWORD'),
    sslmode='require',
)
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=== gold.trade_export_matrix schema + content ===")
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='gold' AND table_name='trade_export_matrix' ORDER BY ordinal_position")
cols = [r['column_name'] for r in cur.fetchall()]
print('cols:', cols)

cur.execute("SELECT DISTINCT commodity_group FROM gold.trade_export_matrix ORDER BY 1")
print('groups:', [r['commodity_group'] for r in cur.fetchall()])

cur.execute("SELECT DISTINCT flow FROM gold.trade_export_matrix ORDER BY 1")
print('flows:', [r['flow'] for r in cur.fetchall()])

cur.execute("""SELECT commodity_group, flow, COUNT(*) AS n
FROM gold.trade_export_matrix WHERE UPPER(commodity_group) LIKE '%BIODIESEL%' OR UPPER(commodity_group) LIKE '%FAME%' OR UPPER(commodity_group) LIKE '%FUEL%'
GROUP BY commodity_group, flow ORDER BY commodity_group, flow""")
print('\nbiodiesel/fuel rows in matrix:')
for r in cur.fetchall():
    print(f"  {r['commodity_group']:25s} | {r['flow']:10s} | {r['n']}")

cur.execute("SELECT pg_get_viewdef('gold.trade_export_matrix'::regclass, TRUE) AS d")
d = cur.fetchone()['d']
print(f"\n=== view definition (first 2000 chars) ===")
print(d[:2000])

print("\n=== bronze.census_trade flows for biodiesel HS codes ===")
cur.execute("""SELECT hs_code, flow, COUNT(*) AS n, MIN(year*100+month) AS first_pm, MAX(year*100+month) AS last_pm
FROM bronze.census_trade WHERE hs_code IN ('3826001000','3826003000')
GROUP BY hs_code, flow ORDER BY hs_code, flow""")
for r in cur.fetchall():
    print(f"  HS {r['hs_code']} flow={r['flow']:10s} | n={r['n']:>5d} | {r['first_pm']}-{r['last_pm']}")

print("\n=== biodiesel-related tables/views ===")
cur.execute("""SELECT table_schema, table_name FROM information_schema.tables
WHERE table_name ILIKE '%biodiesel%' OR table_name ILIKE '%biofuel%trade%' OR table_name ILIKE '%fuel%trade%'
ORDER BY table_schema, table_name""")
for r in cur.fetchall():
    print(f"  {r['table_schema']}.{r['table_name']}")
