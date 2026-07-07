"""Derive modern tallow biofuel-available guardrail into silver.tallow_balance.

PENDING_DESKTOP: the 2011->present non_bio nowcast is an OPEN methodology item
(docs/specs/tallow_nonbio_nowcast_code_to_desktop.md). This script uses the Code
DEFAULT so the pipeline runs end-to-end; every assumption is a named parameter
below, so Desktop's ruling is a one-line change. All derived rows tagged PENDING_DESKTOP.

Identity (Ruling §2 / §6 physical-max):
  tallow_biofuel_use = total_prod + net_imports - non_bio_trend - ebft_food - feed_floor
where (defaults):
  non_bio_trend  = NONBIO_TREND_SHARE x total_prod        (oleo+other, IBFT-attributed; A2/A3)
  ebft_food      = (1 - EBFT_BIO_SHARE) x ebft_prod       (A5: EBFT ~all food, EBFT bio~0)
  feed_floor     = FEED_FLOOR_ANN / 12                    (RD bid feed ~0 in modern era; A4)
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection

# ---- DEFAULT PARAMETERS (PENDING_DESKTOP ruling) ----
NONBIO_TREND_SHARE = 0.0987   # oleo+other IBFT-attributed / total prod, fit 2007-2010 (=9.9% shorthand)
EBFT_BIO_SHARE     = 0.00     # share of edible tallow going to biofuel (default 0 = all food; A5/Q4)
FEED_FLOOR_ANN     = 100e6    # modern structural feed floor, lb/yr (RD bid feed ~0; A4/Q2). FLAGGED.
START_YEAR         = 2013     # census trade (net imports) starts 2013; earlier needs curated (rank92)

def main():
    with get_connection() as c:
        cur=c.cursor()
        cur.execute("DELETE FROM silver.tallow_balance WHERE meta_flag='PENDING_DESKTOP'")
        # MAXIFS-selected monthly production by class
        cur.execute("""WITH ranked AS (
            SELECT class, period, year, month, value_lbs,
                   row_number() OVER (PARTITION BY class,period ORDER BY vintage_rank DESC) rn
            FROM silver.tallow_balance WHERE series='tallow_production')
          SELECT class, period, year, month, value_lbs FROM ranked WHERE rn=1 AND year>=%s""",(START_YEAR,))
        prod={}  # period -> {EBFT,IBFT}
        for r in cur.fetchall():
            prod.setdefault(r['period'],{})[r['class']]=float(r['value_lbs'])
        # net imports (all grades) monthly
        cur.execute("""SELECT period,
              sum(value_lbs) FILTER (WHERE series='tallow_imports') imp,
              sum(value_lbs) FILTER (WHERE series='tallow_exports') exp
            FROM silver.tallow_balance WHERE year>=%s GROUP BY period""",(START_YEAR,))
        net={r['period']:(float(r['imp'] or 0)-float(r['exp'] or 0)) for r in cur.fetchall()}

        rows=[]; feed_mo=FEED_FLOOR_ANN/12.0
        for p,pc in sorted(prod.items()):
            ebft=pc.get('EBFT',0.0); ibft=pc.get('IBFT',0.0); total=ebft+ibft
            ni=net.get(p,0.0)
            non_bio_trend = NONBIO_TREND_SHARE*total
            ebft_food = (1-EBFT_BIO_SHARE)*ebft
            non_bio_use = non_bio_trend + feed_mo
            biofuel = total + ni - non_bio_trend - ebft_food - feed_mo
            y,m=p.year,p.month
            rows.append(('IBFT','non_bio_trend',p,y,m,non_bio_trend,'MODEL',30,'PENDING_DESKTOP'))
            rows.append(('IBFT','feed_use',p,y,m,feed_mo,'MODEL',30,'PENDING_DESKTOP'))
            rows.append(('IBFT','non_bio_use',p,y,m,non_bio_use,'MODEL',30,'PENDING_DESKTOP'))
            rows.append(('ALL','tallow_biofuel_use',p,y,m,biofuel,'MODEL',30,'PENDING_DESKTOP'))
        execute_values(cur,"""INSERT INTO silver.tallow_balance
            (class,series,period,year,month,value_lbs,vintage,vintage_rank,meta_flag) VALUES %s
            ON CONFLICT (class,series,period,vintage) DO UPDATE SET value_lbs=EXCLUDED.value_lbs, meta_flag=EXCLUDED.meta_flag""",rows)
        c.commit()
        print(f"  inserted {len(rows)} PENDING_DESKTOP derived rows, {START_YEAR}+")
        # annual guardrail summary
        cur.execute("""SELECT year, round(sum(value_lbs)/1e9,2) bn FROM silver.tallow_balance
            WHERE series='tallow_biofuel_use' GROUP BY 1 ORDER BY 1""")
        print("  tallow_biofuel_use (guardrail) by year, B lb:")
        for r in cur.fetchall(): print(f"    {r['year']}: {float(r['bn']):.2f}")

if __name__=='__main__':
    main()
