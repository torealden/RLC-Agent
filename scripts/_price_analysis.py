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

# Aggregate 2024 imports by country, computing weighted avg unit price
print('=== 2024 HS 3826 imports by country, weighted avg unit price ===', flush=True)
print('Industry benchmarks: BD ~$3.50-4.50/gal, RD ~$5-6/gal, SAF $6-15/gal', flush=True)
print('', flush=True)
cur.execute("""
SELECT country_code, country_name,
       SUM(quantity)::bigint AS gross_kg,
       SUM(value_usd)::bigint AS val_usd,
       ROUND((SUM(value_usd) / NULLIF(SUM(quantity)*0.301, 0))::numeric, 2) AS usd_per_gal,
       SUM(quantity) * 0.301 / 1e6 AS gross_mil_gal
FROM bronze.census_trade
WHERE hs_code LIKE '3826%' AND flow='imports' AND year=2024
  AND country_code <> '-' AND country_code NOT LIKE '0%' AND country_code NOT LIKE '%XXX'
GROUP BY country_code, country_name
HAVING SUM(quantity) > 1000000
ORDER BY gross_kg DESC
""")
print(f"  {'code':6s} {'country':25s} {'gross mil_gal':>12s} {'$/gal':>8s}  classification", flush=True)
for r in cur.fetchall():
    p = float(r['usd_per_gal'])
    # Classification rule of thumb
    if p < 3.5: classification = 'cheap blend (petroleum dominant)'
    elif p < 4.8: classification = 'BIODIESEL'
    elif p < 5.5: classification = 'BD/RD mix'
    elif p < 7.0: classification = 'RENEWABLE DIESEL'
    else: classification = 'PREMIUM (SAF / specialty)'
    print(f"  {r['country_code']:6s} {r['country_name']:25s} {float(r['gross_mil_gal']):>10.1f}  ${p:>5.2f}  {classification}", flush=True)

print('\n=== 2023 same view ===', flush=True)
cur.execute("""
SELECT country_code, country_name,
       SUM(quantity) * 0.301 / 1e6 AS gross_mil_gal,
       ROUND((SUM(value_usd) / NULLIF(SUM(quantity)*0.301, 0))::numeric, 2) AS usd_per_gal
FROM bronze.census_trade
WHERE hs_code LIKE '3826%' AND flow='imports' AND year=2023
  AND country_code <> '-' AND country_code NOT LIKE '0%' AND country_code NOT LIKE '%XXX'
GROUP BY country_code, country_name
HAVING SUM(quantity) > 1000000
ORDER BY SUM(quantity) DESC
""")
for r in cur.fetchall():
    p = float(r['usd_per_gal'])
    if p < 3.5: cls = 'cheap blend'
    elif p < 4.8: cls = 'BIODIESEL'
    elif p < 5.5: cls = 'BD/RD mix'
    elif p < 7.0: cls = 'RENEWABLE DIESEL'
    else: cls = 'PREMIUM'
    print(f"  {r['country_code']:6s} {r['country_name']:25s} {float(r['gross_mil_gal']):>10.1f}  ${p:>5.2f}  {cls}", flush=True)

# Look at very-recent shipments to see if anything looks like SAF
print('\n=== Highest-priced 2024 imports (RD + SAF candidates) ===', flush=True)
cur.execute("""
SELECT hs_code, country_code, country_name, month,
       quantity::int gross_kg,
       ROUND((value_usd / NULLIF(quantity*0.301, 0))::numeric, 2) AS usd_per_gal
FROM bronze.census_trade
WHERE hs_code LIKE '3826%' AND flow='imports' AND year=2024
  AND country_code NOT LIKE '0%' AND country_code NOT LIKE '%XXX' AND country_code <> '-'
  AND quantity > 50000
  AND (value_usd / NULLIF(quantity*0.301, 0)) > 5.5
ORDER BY value_usd / quantity DESC LIMIT 20
""")
for r in cur.fetchall():
    print(f"  {r['hs_code']} {r['country_code']:6s} {r['country_name']:25s} mo={r['month']:2d} ${r['usd_per_gal']}/gal ({r['gross_kg']:,}kg)", flush=True)

conn.close()
