"""Build silver.tallow_balance — monthly RLC tallow balance per Tallow Ruling Doc + Addendum A.

Long format, RAW pounds (flat-file contract v1.1 §8). Series:
  FACTUAL (pinned):
    tallow_production          EBFT/IBFT/ALL  vintage ladder: NASS(90)>CIR(80)>SLAUGHTER(60)
    tallow_imports/exports     EBFT/IBFT      Census HS1502 (90): .10.0020->EBFT, .10.0040+.90->IBFT
    eia_tallow_comparison      ALL            rank 95, NEVER load-bearing (§2)
    cir_ibft_biodiesel_comp    IBFT           CIR col242 ME-from-IBFT, rank 95 (A2/A8.2)
  CIR-ERA non-bio (factual, 1994-2011):
    nonbio_oleo_ibft           IBFT   c233 fatty acids  x ibft_share  (A2/A3)
    nonbio_other_ibft          IBFT   (c239+c240+c245)  x ibft_share  (A2/A3)
    feed_ibft_measured         IBFT   c235 measured feed baseline     (A2/A4)
  DERIVED, modern (PENDING_DESKTOP — default per tallow_nonbio_nowcast_code_to_desktop.md):
    non_bio_trend / feed_use / non_bio_use / tallow_biofuel_use

Units in source: animal_fat_production.value_lbs = RAW lb; tallow_production.value_lbs = RAW lb
(verified below); census quantity_display = '000 Pounds' -> x1000; CIR value_mil_lbs -> x1e6;
EIA quantity_mil_lbs -> x1e6.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection

DDL = """
CREATE TABLE IF NOT EXISTS silver.tallow_balance (
    commodity text NOT NULL DEFAULT 'tallow',
    class text NOT NULL,               -- EBFT | IBFT | ALL
    series text NOT NULL,
    period date NOT NULL,
    year int, month int,
    value_lbs numeric,                 -- RAW pounds
    vintage text, vintage_rank int,
    meta_flag text,                    -- e.g. PENDING_DESKTOP
    loaded_at timestamptz DEFAULT now(),
    PRIMARY KEY (class, series, period, vintage)
);
"""

def verify_units(cur):
    # tallow_production should sum to ~5B lb for a recent full year if RAW lb
    cur.execute("""SELECT round(sum(value_lbs)/1e9,2) bn FROM silver.tallow_production
                   WHERE year=2020""")
    tp = cur.fetchone()['bn']
    assert tp and 3 < float(tp) < 8, f"tallow_production 2020 sum={tp}B — unit check FAILED (expect ~5-6B raw lb)"
    print(f"  unit check: tallow_production 2020 = {tp}B lb (raw) OK")

def main():
    with get_connection() as c:
        cur=c.cursor(); cur.execute(DDL); cur.execute("TRUNCATE silver.tallow_balance")
        verify_units(cur)
        rows=[]  # (class,series,period,year,month,value_lbs,vintage,vintage_rank,meta_flag)

        # ---- 1. PRODUCTION: vintage ladder NASS(90) > CIR(80) > SLAUGHTER(60) ----
        # real NASS (rank 90, 2015+), raw lb
        cur.execute("""SELECT feedstock_code cls, period, year, month, value_lbs v
                       FROM silver.animal_fat_production WHERE feedstock_code IN ('EBFT','IBFT')""")
        for r in cur.fetchall():
            rows.append((r['cls'],'tallow_production',r['period'],r['year'],r['month'],float(r['v']),'NASS_FATS_OILS',90,None))
        # CIR(80) + SLAUGHTER(60) from tallow_production, raw lb
        cur.execute("""SELECT class cls, period, year, month, value_lbs v, vintage, vintage_rank
                       FROM silver.tallow_production""")
        for r in cur.fetchall():
            rows.append((r['cls'],'tallow_production',r['period'],r['year'],r['month'],float(r['v']),r['vintage'],r['vintage_rank'],None))

        # ---- 2. TRADE by grade, Census HS1502 grand-total only (rank 90), raw lb ----
        # grade map: 1502100020->EBFT ; 1502100040 + 1502900000 -> IBFT
        grade_case = "CASE WHEN hs_code='1502100020' THEN 'EBFT' ELSE 'IBFT' END"
        for flow, series in [('imports','tallow_imports'),('exports','tallow_exports')]:
            cur.execute(f"""SELECT {grade_case} cls, make_date(year,month,1) period, year, month,
                                   sum(quantity_display)*1000.0 v
                            FROM silver.census_trade_monthly
                            WHERE hs_code LIKE '1502%%' AND flow=%s AND country_name='TOTAL FOR ALL COUNTRIES'
                            GROUP BY 1,2,3,4""",(flow,))
            for r in cur.fetchall():
                rows.append((r['cls'],series,r['period'],r['year'],r['month'],float(r['v'] or 0),'CENSUS',90,None))

        # ---- 3. COMPARISON series (rank 95, never load-bearing) ----
        cur.execute("""SELECT year, month, sum(quantity_mil_lbs)*1e6 v FROM bronze.eia_feedstock_monthly
                       WHERE LOWER(feedstock_name) LIKE '%%tallow%%' AND is_withheld=FALSE
                         AND quantity_mil_lbs IS NOT NULL GROUP BY 1,2""")
        for r in cur.fetchall():
            if r['year'] and r['month']:
                rows.append(('ALL','eia_tallow_comparison',f"{r['year']}-{r['month']:02d}-01",r['year'],r['month'],float(r['v']),'EIA',95,None))
        # CIR col242 ME-from-IBFT
        cur.execute("""SELECT period, year, month, value_mil_lbs*1e6 v FROM bronze.census_cir_fats
                       WHERE col_idx=242 AND value_mil_lbs IS NOT NULL""")
        for r in cur.fetchall():
            rows.append(('IBFT','cir_ibft_biodiesel_comparison',r['period'],r['year'],r['month'],float(r['v']),'CIR',95,None))

        # ---- 4. CIR-era non-bio segments (factual 1994-2011), IBFT-allocated via ibft_share ----
        # ibft_share = c59/c64, 12-mo trailing smoothed (A3). Build monthly share dict.
        cur.execute("""SELECT period, year, month, value_mil_lbs v, col_idx FROM bronze.census_cir_fats
                       WHERE col_idx IN (59,64,233,239,240,245,235) ORDER BY period""")
        cir={}
        for r in cur.fetchall():
            cir.setdefault(r['period'],{})[r['col_idx']]=float(r['v']) if r['v'] is not None else None
        periods=sorted(cir)
        # raw monthly ibft_share
        raw_share={p:(cir[p].get(59)/cir[p].get(64) if cir[p].get(59) and cir[p].get(64) else None) for p in periods}
        def smooth(p):
            idx=periods.index(p); window=[raw_share[periods[j]] for j in range(max(0,idx-11),idx+1) if raw_share[periods[j]] is not None]
            return sum(window)/len(window) if window else None
        for p in periods:
            sh=smooth(p); d=cir[p]; y,m=p.year,p.month
            oleo=d.get(233); other=sum(v for v in [d.get(239),d.get(240),d.get(245)] if v is not None)
            feed_ibft=d.get(235)
            if oleo is not None and sh is not None:
                rows.append(('IBFT','nonbio_oleo_ibft',p,y,m,oleo*1e6*sh,'CIR',85,None))
            if other and sh is not None:
                rows.append(('IBFT','nonbio_other_ibft',p,y,m,other*1e6*sh,'CIR',85,None))
            if feed_ibft is not None:
                rows.append(('IBFT','feed_ibft_measured',p,y,m,feed_ibft*1e6,'CIR',85,None))

        execute_values(cur,"""INSERT INTO silver.tallow_balance
            (class,series,period,year,month,value_lbs,vintage,vintage_rank,meta_flag) VALUES %s
            ON CONFLICT (class,series,period,vintage) DO UPDATE SET value_lbs=EXCLUDED.value_lbs""",rows)
        c.commit()
        print(f"  inserted {len(rows)} factual rows")

        # ---- sanity: 2024 selected-vintage production + net imports ----
        cur.execute("""WITH prod AS (
            SELECT class, period, value_lbs, vintage_rank,
                   row_number() OVER (PARTITION BY class,period ORDER BY vintage_rank DESC) rn
            FROM silver.tallow_balance WHERE series='tallow_production' AND year=2024)
          SELECT round(sum(value_lbs) FILTER (WHERE rn=1)/1e9,2) bn FROM prod""")
        print("  2024 production (MAXIFS-selected):", cur.fetchone()['bn'],"B lb")
        cur.execute("""SELECT round((sum(value_lbs) FILTER (WHERE series='tallow_imports')
                          - sum(value_lbs) FILTER (WHERE series='tallow_exports'))/1e9,3) net
                       FROM silver.tallow_balance WHERE year=2024""")
        print("  2024 net imports:", cur.fetchone()['net'],"B lb")

if __name__=='__main__':
    main()
