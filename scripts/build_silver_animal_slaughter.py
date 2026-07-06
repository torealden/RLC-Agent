"""Tidy bronze.nass_livestock_slaughter -> silver.animal_slaughter (tallow-family driver, ruling §9.1).

Per-species commercial aggregate (cattle/hogs=COMMERCIAL, poultry=FI), head + live-weight, monthly.
Live-weight is the yield key (ruling §3 — cattle weights drift over 1944-2026, head would bias).
v1 uses cattle (beef tallow); hogs/poultry staged for Phase 2 (CWG/PLT). BATCH insert via
execute_values — per-row inserts to RDS time out and leave locks.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection

AGG = {  # species -> commercial/FI aggregate short_desc pattern (avoids class sub-breakdowns)
    'cattle':   'CATTLE, GE 500 LBS, SLAUGHTER, COMMERCIAL%',
    'hogs':     'HOGS, SLAUGHTER, COMMERCIAL, FI%',
    'broilers': 'CHICKENS, BROILERS, SLAUGHTER, FI%',
    'turkeys':  'TURKEYS, SLAUGHTER, FI%',
}
UMAP = {'HEAD': 'head', 'LB_LIVE': 'live_wt_lb', 'LB_PER_HEAD_LIVE': 'live_wt_per_head_lb'}
DDL = """CREATE TABLE IF NOT EXISTS silver.animal_slaughter (
  period date, year int, month int, species text, measure text, value numeric, unit text,
  loaded_at timestamptz DEFAULT now(), PRIMARY KEY (period, species, measure));"""

with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("TRUNCATE silver.animal_slaughter")
    seen = {}
    for sp, pat in AGG.items():
        cur.execute("""SELECT year, month, unit, value FROM bronze.nass_livestock_slaughter
                       WHERE species=%s AND short_desc ILIKE %s AND value IS NOT NULL AND month BETWEEN 1 AND 12""", (sp, pat))
        for r in cur.fetchall():
            m = UMAP.get(r['unit'])
            if m:
                seen[(r['year'], r['month'], sp, m)] = (r['year'], r['month'], sp, m, float(r['value']), r['unit'])
    execute_values(cur, """INSERT INTO silver.animal_slaughter (year,month,period,species,measure,value,unit)
        VALUES %s ON CONFLICT (period,species,measure) DO NOTHING""",
        [(v[0], v[1], f"{v[0]}-{v[1]:02d}-01", v[2], v[3], v[4], v[5]) for v in seen.values()])
    c.commit()
    cur.execute("SELECT species,measure,count(*) n,min(year) mn,max(year) mx FROM silver.animal_slaughter GROUP BY 1,2 ORDER BY 1,2")
    for r in cur.fetchall():
        print(f"  {r['species']:9} {r['measure']:20} {r['n']:4} {r['mn']}-{r['mx']}")
