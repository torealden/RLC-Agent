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
with open('database/migrations/091_fill_saf_coproc_gaps.sql') as f:
    cur.execute(f.read())
conn.commit()
print('mig 091 applied', flush=True)

print('\n=== Recent Domestic Use values (kgal) ===', flush=True)
cur.execute("""SELECT year, month, biodiesel_kgal, renewable_diesel_kgal, co_processing_kgal, saf_kgal, ethanol_kgal
FROM gold.us_liquid_fuel_domestic_use_monthly ORDER BY year DESC, month DESC LIMIT 6""")
for r in cur.fetchall():
    print(f"  {r['year']}-{r['month']:02d} BD={r['biodiesel_kgal']!s:>10s} RD={r['renewable_diesel_kgal']!s:>10s} COP={r['co_processing_kgal']!s:>10s} SAF={r['saf_kgal']!s:>10s} ETH={r['ethanol_kgal']!s:>10s}", flush=True)

print('\n=== Recent Stocks values (kgal) ===', flush=True)
cur.execute("""SELECT year, month, biodiesel_kgal, renewable_diesel_kgal, co_processing_kgal, saf_kgal, ethanol_kgal
FROM gold.us_liquid_fuel_stocks_monthly ORDER BY year DESC, month DESC LIMIT 6""")
for r in cur.fetchall():
    print(f"  {r['year']}-{r['month']:02d} BD={r['biodiesel_kgal']!s:>10s} RD={r['renewable_diesel_kgal']!s:>10s} COP={r['co_processing_kgal']!s:>10s} SAF={r['saf_kgal']!s:>10s} ETH={r['ethanol_kgal']!s:>10s}", flush=True)
conn.close()
