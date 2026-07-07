"""Wire canonical UCO into silver.feedstock_supply per UCO Amendment 1.

- View silver.uco_imports_country (excludes the TOTAL aggregate row) + validation
  SUM(country) = TOTAL +/- tol (closes the double-count trap structurally, wiring step 4).
- Swap the pool's representation in silver.feedstock_supply: DELETE the EIA-derived YG rows
  (8.57B/yr) and any UCO rows, WRITE canonical UCO rows. Provenance RULING1_UCO_AMENDMENT1.
    monthly UCO supply = collection(T12M-smoothed) + net_imports(actual)   [R1: YG_BIOFUEL=0]
    PADD-distributed (same weights as existing rows), UCO price from bronze.feedstock_prices.
- YG rows: none written (YG_BIOFUEL = 0). Old rows ran 8.57B/yr vs EIA_YG 7.39 — a
  WG-inclusion/window artifact in the dying rows; one line of lineage, not chased (Amendment 1 §3).
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from src.services.database.db_config import get_connection

PADD_W = {'PADD1':0.08,'PADD2':0.45,'PADD3':0.30,'PADD4':0.05,'PADD5':0.12}
PROV = 'RULING1_UCO_AMENDMENT1'

with get_connection() as c:
    cur=c.cursor()

    # --- 1. TOTAL-trap view + validation (wiring step 4) ---
    cur.execute("""CREATE OR REPLACE VIEW silver.uco_imports_country AS
        SELECT period, year, month, country, flow, mil_lbs, hs_code
        FROM silver.uco_imports WHERE country <> 'TOTAL'""")
    cur.execute("""SELECT year,
        round(sum(mil_lbs) FILTER (WHERE country='TOTAL')/1e3,2) total_row,
        round((SELECT sum(mil_lbs) FROM silver.uco_imports_country v
               WHERE v.year=u.year AND v.flow='import')/1e3,2) country_sum
      FROM silver.uco_imports u WHERE flow='import' AND year IN (2023,2024) GROUP BY 1 ORDER BY 1""")
    print("TOTAL-trap validation (country_sum should = total_row):")
    for r in cur.fetchall():
        tr=float(r['total_row'] or 0); cs=float(r['country_sum'] or 0)
        ok='OK' if abs(tr-cs)<0.05 else 'MISMATCH'
        print(f"  {r['year']}: TOTAL_row {tr:.2f}B  country_sum {cs:.2f}B  {ok}")

    # --- 2. monthly collection (T12M-smoothed) + net imports (actual) ---
    cur.execute("""SELECT period, value_lbs FROM silver.uco_yg_balance
                   WHERE series='uco_collection' ORDER BY period""")
    coll={r['period']:float(r['value_lbs']) for r in cur.fetchall()}
    cur.execute("""SELECT period, value_lbs FROM silver.uco_yg_balance WHERE series='net_imports'""")
    net={r['period']:float(r['value_lbs']) for r in cur.fetchall()}
    cperiods=sorted(coll)
    def t12m_coll(p):
        i=cperiods.index(p); w=[coll[cperiods[j]] for j in range(max(0,i-11),i+1)]
        return sum(w)/len(w)
    # UCO price: monthly avg from bronze.feedstock_prices, fallback 0.40
    cur.execute("""SELECT date_trunc('month',price_date)::date pm, avg(price_per_lb) p
                   FROM bronze.feedstock_prices WHERE feedstock_code='UCO' AND price_per_lb>0 GROUP BY 1""")
    price={r['pm']:(float(r['p'])/100.0 if float(r['p'])>1 else float(r['p'])) for r in cur.fetchall()}
    def uco_price(p):
        return price.get(p) or price.get(p.replace(day=1)) or 0.40

    # --- 3. swap representation in feedstock_supply ---
    cur.execute("DELETE FROM silver.feedstock_supply WHERE feedstock_code IN ('UCO','YG')")
    rows=0
    for p in cperiods:
        ni=net.get(p,0.0)
        monthly = t12m_coll(p) + ni            # raw lb, R1: no YG leg
        mil = monthly/1e6
        pr=uco_price(p)
        for padd,w in PADD_W.items():
            cur.execute("""INSERT INTO silver.feedstock_supply
                (period,feedstock_code,region,net_available_biofuel,avg_price_per_lb,domestic_production,imports,total_available)
                VALUES (%s,'UCO',%s,%s,%s,%s,%s,%s)""",
                (p,padd, mil*w, pr, (t12m_coll(p)/1e6)*w, (ni/1e6)*w, mil*w))
            rows+=1
    c.commit()
    print(f"\nwrote {rows} UCO rows to feedstock_supply ({PROV}); YG rows deleted (YG_BIOFUEL=0)")

    # --- acceptance check 1: CY2024 UCO = 8.73B, YG = 0 ---
    cur.execute("""SELECT feedstock_code, round(sum(net_available_biofuel)/1e3,2) bn
        FROM silver.feedstock_supply WHERE feedstock_code IN ('UCO','YG') AND period BETWEEN '2024-01-01' AND '2024-12-01'
        GROUP BY 1""")
    d={r['feedstock_code']:float(r['bn']) for r in cur.fetchall()}
    print(f"  CY2024 feedstock_supply: UCO={d.get('UCO',0):.2f}B (target 8.73), YG={d.get('YG',0):.2f}B (target 0)")
