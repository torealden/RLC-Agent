import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection
YIELD=0.1270; EDIBLE_SHARE=0.335   # calibrated 2005-2010 (config params)
with get_connection() as c:
    cur=c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS silver.tallow_production (period date, year int, month int,
        class text, value_lbs numeric, vintage text, vintage_rank int,
        loaded_at timestamptz DEFAULT now(), PRIMARY KEY (period, class, vintage));""")
    cur.execute("TRUNCATE silver.tallow_production")
    rows=[]
    # slaughter-derived (rank 60), all months with cattle live weight
    cur.execute("SELECT period, value FROM silver.animal_slaughter WHERE species='cattle' AND measure='live_wt_lb'")
    for r in cur.fetchall():
        prod=YIELD*float(r['value'])
        rows.append((r['period'],r['period'].year,r['period'].month,'EBFT',prod*EDIBLE_SHARE,'SLAUGHTER_DERIVED',60))
        rows.append((r['period'],r['period'].year,r['period'].month,'IBFT',prod*(1-EDIBLE_SHARE),'SLAUGHTER_DERIVED',60))
    # CIR measured (rank 80) where it exists — supersedes slaughter-derived via MAXIFS
    cur.execute("""SELECT period, series, value_mil_lbs*1e6 v FROM bronze.census_cir_fats
                   WHERE series IN ('c55:Edible Tallow Production','c59:Inedible Tallow Production')""")
    for r in cur.fetchall():
        cls='EBFT' if 'Edible' in r['series'] and 'Ined' not in r['series'] else 'IBFT'
        rows.append((r['period'],r['period'].year,r['period'].month,cls,float(r['v']),'CENSUS_CIR',80))
    seen={}
    for r in rows: seen[(r[0],r[3],r[5])]=r
    execute_values(cur,"""INSERT INTO silver.tallow_production (period,year,month,class,value_lbs,vintage,vintage_rank)
        VALUES %s ON CONFLICT (period,class,vintage) DO NOTHING""",list(seen.values()))
    c.commit()
    cur.execute("""SELECT vintage, count(*) n, min(year) mn, max(year) mx FROM silver.tallow_production GROUP BY 1""")
    for r in cur.fetchall(): print(f"  {r['vintage']:18} {r['n']:5} {r['mn']}-{r['mx']}")
    # MAXIFS-picked annual total (measured wins 2000-2011, slaughter elsewhere)
    cur.execute("""WITH pick AS (SELECT DISTINCT ON (period,class) period,class,value_lbs FROM silver.tallow_production ORDER BY period,class,vintage_rank DESC)
        SELECT y, round(sum(value_lbs)/1e9,2) bn FROM (SELECT extract(year from period)::int y, value_lbs FROM pick) x WHERE y IN (2008,2018,2024) GROUP BY 1 ORDER BY 1""")
    print("MAXIFS-picked tallow production (B lb):", [(int(r['y']),float(r['bn'])) for r in cur.fetchall()])
