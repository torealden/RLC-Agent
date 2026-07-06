import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection
# NASS Fats&Oils attribute -> feedstock_code (real measured production 2015+, rank 90)
MAP = {
 'TALLOW, EDIBLE - PRODUCTION, MEASURED IN LB':'EBFT',
 'TALLOW, INEDIBLE - PRODUCTION, MEASURED IN LB':'IBFT',
 'GREASE, CHOICE WHITE - PRODUCTION, MEASURED IN LB':'CWG',
 'GREASE, YELLOW - PRODUCTION, MEASURED IN LB':'YG',
 'POULTRY FATS - PRODUCTION, MEASURED IN LB':'PLT',
 'LARD, INCL RENDERED PORK FAT - PRODUCTION, MEASURED IN LB':'LARD',
}
with get_connection() as c:
    cur=c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS silver.animal_fat_production (period date, year int, month int,
        feedstock_code text, value_lbs numeric, vintage text, vintage_rank int,
        loaded_at timestamptz DEFAULT now(), PRIMARY KEY (period, feedstock_code, vintage));""")
    cur.execute("TRUNCATE silver.animal_fat_production")
    rows=[]
    for attr,code in MAP.items():
        cur.execute("""SELECT calendar_year y, month m, realized_value v FROM silver.monthly_realized
                       WHERE source LIKE 'NASS_FATS_OILS%%' AND attribute=%s AND realized_value IS NOT NULL AND month BETWEEN 1 AND 12""",(attr,))
        for r in cur.fetchall():
            rows.append((f"{r['y']}-{r['m']:02d}-01",r['y'],r['m'],code,float(r['v']),'NASS_FATS_OILS',90))
    seen={}
    for r in rows: seen[(r[0],r[3])]=r
    execute_values(cur,"""INSERT INTO silver.animal_fat_production (period,year,month,feedstock_code,value_lbs,vintage,vintage_rank)
        VALUES %s ON CONFLICT (period,feedstock_code,vintage) DO NOTHING""",list(seen.values()))
    c.commit()
    cur.execute("""SELECT feedstock_code, round(sum(value_lbs)/1e9,2) bn FROM silver.animal_fat_production
                   WHERE year=2024 GROUP BY 1 ORDER BY 2 DESC""")
    print("2024 real NASS animal-fat production (B lb):")
    for r in cur.fetchall(): print(f"  {r['feedstock_code']:5} {float(r['bn']):.2f}")
