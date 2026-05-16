"""Investigate what SAF data we have across bronze tables."""
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

# 1. EMTS production category for SAF
print('=== EMTS production: SAF rows ===', flush=True)
cur.execute("""SELECT MIN(year) y0, MAX(year) y1, COUNT(*) n, SUM(volume_gal)/1e6 AS total_mil_gal
FROM silver.emts_production_canonical WHERE production_category='saf'""")
r = cur.fetchone()
print(f"  rows: {r['n']}, {r['y0']}-{r['y1']}, lifetime production: {float(r['total_mil_gal'] or 0):.2f} mil gal", flush=True)
cur.execute("SELECT year, month, volume_gal/1e6 mil_gal FROM silver.emts_production_canonical WHERE production_category='saf' ORDER BY year DESC, month DESC LIMIT 6")
print('  recent monthly:', flush=True)
for r in cur.fetchall(): print(f"    {r['year']}-{r['month']:02d}: {float(r['mil_gal']):.3f} mil gal", flush=True)

# 2. EIA biofuels — any SAF entries
print('\n=== EIA biofuels — anything SAF/aviation/jet-renewable ===', flush=True)
cur.execute("""SELECT DISTINCT fuel_type, attribute, region, description
FROM bronze.eia_monthly_biofuels
WHERE fuel_type ILIKE '%saf%' OR fuel_type ILIKE '%aviation%' OR fuel_type ILIKE '%jet%'
   OR description ILIKE '%saf%' OR description ILIKE '%aviation%' OR description ILIKE '%sustain%'""")
rows = cur.fetchall()
if not rows: print('  (no SAF-specific EIA series ingested)', flush=True)
for r in rows: print(f"  {r['fuel_type']:25s} {r['attribute']:15s} {r['region']}  {r['description']}", flush=True)

# 3. HS 2710.19.11 (Kerosene-type jet fuel) — likely SAF landing zone
print('\n=== HS 2710.19.11 (Kerosene-type jet fuel) in bronze ===', flush=True)
cur.execute("""SELECT flow, COUNT(*) n, MIN(year*100+month) first, MAX(year*100+month) last
FROM bronze.census_trade WHERE hs_code='2710191100' GROUP BY flow""")
rows = cur.fetchall()
if not rows: print('  (no data — code in reference table but never collected)', flush=True)
for r in rows: print(f"  flow={r['flow']:8s} n={r['n']:>6d} {r['first']}-{r['last']}", flush=True)

# 4. Any HS code mentioning SAF/HEFA/sustain in commodity_name
print('\n=== silver.trade_commodity_reference — any SAF mention ===', flush=True)
cur.execute("""SELECT hs_code_10, commodity_group, commodity_name FROM silver.trade_commodity_reference
WHERE commodity_name ILIKE '%saf%' OR commodity_name ILIKE '%aviation%' OR commodity_name ILIKE '%sustain%' OR commodity_name ILIKE '%hefa%' OR commodity_name ILIKE '%alternative jet%'""")
rows = cur.fetchall()
if not rows: print('  (no SAF/HEFA entries in reference)', flush=True)
for r in rows: print(f"  {r['hs_code_10']} {r['commodity_group']:18s} {r['commodity_name']}", flush=True)

# 5. CARB pathways with SAF/AJF for context (production-side intel)
print('\n=== CARB pathways: AJF/SAF certified producers ===', flush=True)
cur.execute("""SELECT DISTINCT facility_name, facility_location, feedstock, ci_current
FROM bronze.carb_lcfs_pathways
WHERE fuel_type ILIKE '%jet%' OR fuel_type ILIKE '%ajf%' OR fuel_type ILIKE '%saf%' OR fuel_type ILIKE '%aviation%'
ORDER BY facility_name LIMIT 20""")
for r in cur.fetchall(): print(f"  {r['facility_name']:45s} {r['facility_location']:25s} fs={r['feedstock'][:35] if r['feedstock'] else '':35s} CI={r['ci_current']}", flush=True)

conn.close()
