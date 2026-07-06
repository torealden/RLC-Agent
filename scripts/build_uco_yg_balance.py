"""§2 identity: the national UCO/YG use-based split (Desktop ruling doc §2, §4).

Computes monthly: uco_collection, yg_collection, net_imports, and the min()-capped biofuel-use split
+ non_bio residual + overflow (the flagged residual for k_uco adjudication). Anchors are CONFIG
params (FM value is internal_only/licensed — never surfaces client-facing; only derived series do).
Stores silver.uco_yg_balance. This is the cheap analytical core BEFORE the allocator re-run.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

# ---- CONFIG (anchors as parameters; ANCHOR_UCO is licensed Fastmarkets -> internal_only) ----
ANCHOR_UCO_LBS   = 3.30e9   # Fastmarkets UCO-grade collection, 2024   [INTERNAL_ONLY / licensed]
ANCHOR_UCO_YEAR  = 2024
ANCHOR_COMB_LBS  = 6.50e9   # LMC/GlobalData combined UCO+YG collection, 2022
ANCHOR_COMB_YEAR = 2022

DDL = """
CREATE TABLE IF NOT EXISTS silver.uco_yg_balance (
    period date NOT NULL, year int, month int, series text NOT NULL,
    value_lbs numeric, vintage text, loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (period, series)
);
"""
with get_connection() as c:
    cur = c.cursor()
    cur.execute(DDL)
    cur.execute("TRUNCATE silver.uco_yg_balance")

    # annual real FAFH (for k calibration)
    cur.execute("""SELECT year, sum(value_mil_usd) v FROM silver.food_expenditure
                   WHERE category='FAFH' AND outlet='total' AND unit='real' GROUP BY 1""")
    fafh_yr = {r['year']: float(r['v']) for r in cur.fetchall()}
    k_uco = ANCHOR_UCO_LBS / fafh_yr[ANCHOR_UCO_YEAR]
    uco_comb_yr = k_uco * fafh_yr[ANCHOR_COMB_YEAR]
    k_yg = (ANCHOR_COMB_LBS - uco_comb_yr) / fafh_yr[ANCHOR_COMB_YEAR]
    print(f"k_uco={k_uco:.1f} k_yg={k_yg:.1f} lb/$M")

    # monthly drivers: real FAFH, net imports, EIA_YG
    cur.execute("""SELECT period, value_mil_usd v FROM silver.food_expenditure
                   WHERE category='FAFH' AND outlet='total' AND unit='real'""")
    fafh = {r['period']: float(r['v']) for r in cur.fetchall()}
    cur.execute("""SELECT period, sum(mil_lbs) filter (where flow='import')*1e6 imp,
                          coalesce(sum(mil_lbs) filter (where flow='export'),0)*1e6 exp
                   FROM silver.uco_imports WHERE country='TOTAL' GROUP BY 1""")
    netimp = {r['period']: float(r['imp'] or 0) - float(r['exp'] or 0) for r in cur.fetchall()}
    cur.execute("""SELECT make_date(year,month,1) period, sum(quantity_mil_lbs)*1e6 yg
                   FROM bronze.eia_feedstock_monthly WHERE feedstock_name='Yellow Grease' AND plant_type='total'
                     AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL GROUP BY 1""")
    eia_yg = {r['period']: float(r['yg']) for r in cur.fetchall()}

    def put(period, series, val, vintage):
        cur.execute("""INSERT INTO silver.uco_yg_balance (period,year,month,series,value_lbs,vintage)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (period,series) DO UPDATE SET value_lbs=EXCLUDED.value_lbs""",
            (period, period.year, period.month, series, val, vintage))

    n = 0
    for period, f in fafh.items():
        uco_coll = k_uco * f
        yg_coll = k_yg * f
        ni = netimp.get(period, 0.0)
        put(period, 'uco_collection', uco_coll, 'PROXY_FOOD')
        put(period, 'yg_collection', yg_coll, 'PROXY_FOOD')
        put(period, 'net_imports', ni, 'CENSUS')
        # Ruling 1 (Tore 2026-07-06): EIA under-captures UCO/tallow -> RLC supply build is CANONICAL,
        # NOT capped at EIA. uco_biofuel = collection + net imports (non-bio UCO ~ 0 in modern era).
        # EIA "Yellow Grease" is kept only as a comparison to quantify+defend the divergence to a room
        # of 300 -- never as a control. (yg_biofuel + non_bio split moves to the allocator demand side.)
        uco_bio = uco_coll + ni
        put(period, 'uco_biofuel_use', uco_bio, 'DERIVED')
        yg = eia_yg.get(period)
        if yg is not None:
            put(period, 'eia_yg_reported', yg, 'EIA')
            put(period, 'rlc_minus_eia', uco_bio - yg, 'DERIVED')  # >0 = RLC UCO alone exceeds EIA's whole YG bucket
        n += 1
    c.commit()
    print(f"silver.uco_yg_balance: {n} months")

    # report: RLC-canonical UCO biofuel use vs EIA (the divergence we now embrace, Ruling 1)
    cur.execute("""SELECT year, series, round(sum(value_lbs)/1e9,2) bn FROM silver.uco_yg_balance
                   WHERE series IN ('uco_collection','net_imports','uco_biofuel_use','eia_yg_reported','rlc_minus_eia')
                     AND year IN (2019,2022,2024) GROUP BY 1,2 ORDER BY 1,2""")
    from collections import defaultdict
    tbl = defaultdict(dict)
    for r in cur.fetchall(): tbl[r['year']][r['series']] = float(r['bn'])
    print("\nyear | uco_coll | net_imp | UCO_BIOFUEL(RLC) | EIA_YG | RLC-EIA  (B lb)")
    for y in sorted(tbl):
        t = tbl[y]
        print(f"  {y}  {t.get('uco_collection','-'):>6} {t.get('net_imports','-'):>6}   {t.get('uco_biofuel_use','-'):>6}          {t.get('eia_yg_reported','-'):>6}  {t.get('rlc_minus_eia','-'):>6}")
