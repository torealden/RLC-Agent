"""Rake the allocator output to EIA feedstock control totals (design v1.6 Layer D).

Per (period, EIA feedstock): rake_factor = EIA_total / allocator_total; scale every per-facility
allocation row by its feedstock's factor so national totals tie to EIA exactly and the per-facility
distribution is preserved. Writes gold.bbd_feedstock_raked (facility grain, + rake_factor) and
reports pre/post national deltas. EIA control total = plant_type='total' quantity_mil_lbs (not
withheld); where withheld (canola/corn-oil redaction), rake_factor=1 (left unraked, flagged).
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

RUN_DAY = '2026-07-04'
# allocator feedstock_code -> EIA feedstock_name (EIA lumps UCO into Yellow Grease, tallow combined)
A2E = {'SBO':'Soybean Oil','CO':'Canola Oil','CAN':'Canola Oil','DCO':'Corn Oil',
       'EBFT':'Tallow','IBFT':'Tallow','BFT':'Tallow','YG':'Yellow Grease','UCO':'Yellow Grease',
       'CWG':'White Grease','PF':'Poultry','PLT':'Poultry','CSO':'Cottonseed Oil'}

# RLC-canonical feedstocks (Ruling 1): RLC supply build is authoritative, EIA disregarded.
# These are EXEMPT from the rake — kept at allocator totals, rake_factor forced to 1.0.
# Tallow only for now; UCO joins when its canonical supply is wired (allocator currently
# allocates ~0 UCO, so exempting it now would drop it from output). CWG stays raked (treat
# like PF, per Tore 2026-07-07). Veg oils (SBO/CO/DCO) stay EIA-pinned.
RLC_CANONICAL = {'EBFT', 'IBFT', 'BFT'}

DDL = """
CREATE TABLE IF NOT EXISTS gold.bbd_feedstock_raked (
    facility_id int, period date, feedstock_code text, fuel_type text,
    raked_mil_lbs numeric, pre_rake_mil_lbs numeric, rake_factor numeric,
    eia_feedstock text, run_day date, created_at timestamptz DEFAULT now()
);
"""

with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("DELETE FROM gold.bbd_feedstock_raked WHERE run_day=%s", (RUN_DAY,))

    # 1. latest run per period, per-facility allocation
    cur.execute("""
        WITH latest AS (SELECT DISTINCT ON (period) period, run_id FROM gold.feedstock_allocation
                        WHERE created_at::date=%s ORDER BY period, created_at DESC)
        SELECT a.facility_id, a.period, a.feedstock_code, a.fuel_type, a.allocated_mil_lbs
        FROM gold.feedstock_allocation a JOIN latest l ON a.period=l.period AND a.run_id=l.run_id
    """, (RUN_DAY,))
    rows = cur.fetchall()

    # 2. allocator totals per (period, eia_name)
    alloc_tot = {}
    for r in rows:
        eia = A2E.get(r['feedstock_code'])
        if not eia: continue
        alloc_tot[(r['period'], eia)] = alloc_tot.get((r['period'], eia), 0) + float(r['allocated_mil_lbs'] or 0)

    # 3. EIA control totals per (period, eia_name), plant_type='total', not withheld
    cur.execute("""SELECT make_date(year, month, 1) period, feedstock_name, sum(quantity_mil_lbs) q
                   FROM bronze.eia_feedstock_monthly WHERE plant_type='total' AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL
                   GROUP BY 1,2""")
    eia_tot = {(r['period'], r['feedstock_name']): float(r['q']) for r in cur.fetchall()}

    # 4. rake_factor per (period, eia_name)
    rake = {}
    for k, a in alloc_tot.items():
        e = eia_tot.get(k)
        rake[k] = (e / a) if (e and a > 0) else 1.0

    # 5. write raked per-facility rows
    n = 0
    for r in rows:
        eia = A2E.get(r['feedstock_code'])
        if not eia: continue
        # RLC-canonical: exempt from rake (keep allocator total, don't scale to EIA)
        rf = 1.0 if r['feedstock_code'] in RLC_CANONICAL else rake.get((r['period'], eia), 1.0)
        pre = float(r['allocated_mil_lbs'] or 0)
        cur.execute("""INSERT INTO gold.bbd_feedstock_raked
            (facility_id,period,feedstock_code,fuel_type,raked_mil_lbs,pre_rake_mil_lbs,rake_factor,eia_feedstock,run_day)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (r['facility_id'], r['period'], r['feedstock_code'], r['fuel_type'], pre*rf, pre, rf, eia, RUN_DAY))
        n += 1
    c.commit()
    print(f"raked {n} rows -> gold.bbd_feedstock_raked")

    # 6. report: trailing-12mo national pre/post vs EIA
    cur.execute("""SELECT eia_feedstock,
                       round(sum(pre_rake_mil_lbs)/1000.0,2) pre, round(sum(raked_mil_lbs)/1000.0,2) post,
                       round(avg(rake_factor),3) avg_rf
                   FROM gold.bbd_feedstock_raked WHERE run_day=%s AND period BETWEEN '2024-10-01' AND '2025-09-01'
                   GROUP BY 1 ORDER BY 2 DESC""", (RUN_DAY,))
    print(f"\ntrailing-12mo national (B lb): pre-rake -> post-rake (avg rake factor)")
    for r in cur.fetchall():
        print(f"  {r['eia_feedstock']:16} {float(r['pre']):6.2f} -> {float(r['post']):6.2f}  (x{r['avg_rf']})")
