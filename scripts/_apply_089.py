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
with open('database/migrations/089_biofuel_trade_split_rules_v2_price_calibrated.sql') as f:
    sql = f.read()
try:
    cur.execute(sql)
    conn.commit()
    print('mig 089 applied', flush=True)
except Exception as e:
    print(f'FAIL: {type(e).__name__}: {e}', flush=True)
    conn.rollback()
    conn.close()
    sys.exit(1)

# Validate: 2024 + 2023 BD vs RD imports + WORLD TOTAL reconciles
print('\n--- BD/RD imports by year, post-mig 089 ---', flush=True)
cur.execute("""
SELECT s.year, s.commodity_split,
       SUM(s.quantity_gal)/1e6 AS mgal
FROM gold.biofuel_trade_split s
WHERE s.flow='imports' AND s.year BETWEEN 2018 AND 2025
GROUP BY s.year, s.commodity_split
ORDER BY s.year, s.commodity_split""")
for r in cur.fetchall():
    print(f"  {r['year']} {r['commodity_split']:18s} {float(r['mgal']):>7.1f} mil gal", flush=True)

print('\n--- Compare to EIA BD imports ---', flush=True)
cur.execute("""SELECT EXTRACT(YEAR FROM period_month)::int yr, SUM(value)*42/1000 AS mgal
FROM bronze.eia_monthly_biofuels
WHERE fuel_type='biodiesel' AND attribute='imports' AND EXTRACT(YEAR FROM period_month) BETWEEN 2018 AND 2024
GROUP BY 1 ORDER BY 1""")
for r in cur.fetchall(): print(f"  {r['yr']}: EIA BD imports = {float(r['mgal']):>6.1f} mil gal", flush=True)
conn.close()
