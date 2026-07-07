"""Extend silver.uco_yg_balance through 2025 (FAFH-driven collection ends 2024-12).

Stopgap so the allocator re-run has UCO supply for 2025 months. Per Amendment 1 coverage note:
collection carried forward at the 2024 same-month level (vintage PROXY_FOOD_CARRYFWD), imports at
ACTUAL 2025 Census (TOTAL row). Not the §7 forecast — a recent-months carry-forward pending it.
uco_biofuel_use = collection + net_imports (R1: YG_BIOFUEL=0).
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from datetime import date
from src.services.database.db_config import get_connection

with get_connection() as c:
    cur=c.cursor()
    cur.execute("DELETE FROM silver.uco_yg_balance WHERE year=2025")  # idempotent
    # 2024 collection by month (carry-forward source)
    cur.execute("""SELECT month, value_lbs FROM silver.uco_yg_balance
                   WHERE series='uco_collection' AND year=2024""")
    coll2024={r['month']:float(r['value_lbs']) for r in cur.fetchall()}
    # 2025 actual net imports (TOTAL)
    cur.execute("""SELECT period, month,
        coalesce(sum(mil_lbs) FILTER (WHERE flow='import'),0)*1e6
       -coalesce(sum(mil_lbs) FILTER (WHERE flow='export'),0)*1e6 net
       FROM silver.uco_imports WHERE country='TOTAL' AND year=2025 GROUP BY 1,2 ORDER BY 1""")
    n=0
    for r in cur.fetchall():
        p=r['period']; m=r['month']; ni=float(r['net'])
        coll=coll2024.get(m)
        if coll is None: continue
        for series,val,vint in [('uco_collection',coll,'PROXY_FOOD_CARRYFWD'),
                                ('net_imports',ni,'CENSUS'),
                                ('uco_biofuel_use',coll+ni,'DERIVED')]:
            cur.execute("""INSERT INTO silver.uco_yg_balance (period,year,month,series,value_lbs,vintage)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (period,series) DO UPDATE SET value_lbs=EXCLUDED.value_lbs""",
                (p,2025,m,series,val,vint))
        n+=1
    c.commit()
    cur.execute("SELECT round(sum(value_lbs)/1e9,2) bn FROM silver.uco_yg_balance WHERE series='uco_biofuel_use' AND year=2025")
    print(f"extended {n} months of 2025; 2025 uco_biofuel_use = {float(cur.fetchone()['bn']):.2f}B lb")
