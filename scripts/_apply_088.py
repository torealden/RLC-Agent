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

# Look for ANY SAF/AJF/jet pathway nationwide (small list expected)
print('--- ALL CARB pathways: SAF/AJF/Aviation/Jet ---', flush=True)
cur.execute("""SELECT DISTINCT facility_name, facility_location, fuel_type, feedstock
FROM bronze.carb_lcfs_pathways
WHERE fuel_type ILIKE '%saf%' OR fuel_type ILIKE '%ajf%' OR fuel_type ILIKE '%aviation%' OR fuel_type ILIKE '%jet%'
   OR fuel_type ILIKE '%hefa%'
ORDER BY facility_name LIMIT 30""")
for r in cur.fetchall(): print(f"  {r['facility_name']:50s} {r['facility_location']:25s} {r['fuel_type']:20s} feedstock={r['feedstock']}", flush=True)

# Search applicant_description for TX SAF + Netherlands references
print('\n--- CARB pathways mentioning Netherlands import or HEFA ---', flush=True)
cur.execute("SELECT applicant_description FROM bronze.carb_lcfs_pathways WHERE applicant_description ILIKE '%netherlands%' OR applicant_description ILIKE '%hefa%' LIMIT 5")
for r in cur.fetchall():
    desc = (r['applicant_description'] or '')[:200]
    print(f"  {desc}", flush=True)

conn.close()
