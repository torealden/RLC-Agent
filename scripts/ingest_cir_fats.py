import sys, openpyxl
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from psycopg2.extras import execute_values
from src.services.database.db_config import get_connection
p = Path(r"C:/Users/torem/RLC Dropbox/RLC Team Folder/RLC Documents/Models/Oilseeds/World Crush and Other Stuff.xlsx")
wb = openpyxl.load_workbook(p, read_only=True, data_only=True); ws = wb['Census Crush']
hdr = next(ws.iter_rows(max_row=1, values_only=True))
# Desktop A9.1: cols 55-70 (production/stocks) + 229-245 (inedible T&G consumption block) ONLY
IDX = list(range(55,71)) + list(range(229,246))
cols = {i: (str(hdr[i]).replace(' (million pounds)','').replace(' (Million Pounds)','').strip() if hdr[i] else f'col{i}') for i in IDX}
print("cols 55-70 sample:", [cols[i] for i in range(55,62)])
print("cols 229-245 sample:", [cols[i] for i in range(229,238)])
rows=[]
for row in ws.iter_rows(min_row=2, values_only=True):
    d=row[0]
    if d is None or not hasattr(d,'year'): continue
    for i in IDX:
        v=row[i] if i < len(row) else None
        try: v=float(v)
        except (TypeError,ValueError): continue
        rows.append((d.date().isoformat(), d.year, d.month, f"c{i}:{cols[i]}", v, i))
seen={}
for r in rows: seen[(r[0],r[3])]=r
with get_connection() as c:
    cur=c.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS bronze.census_cir_fats (period date, year int, month int,
        series text, value_mil_lbs numeric, col_idx int, source text DEFAULT 'census_cir_m311k',
        collected_at timestamptz DEFAULT now(), PRIMARY KEY (period, series));""")
    cur.execute("DROP TABLE IF EXISTS bronze.census_cir_fats_oils")  # drop the keyword-contaminated table
    cur.execute("TRUNCATE bronze.census_cir_fats")
    execute_values(cur,"""INSERT INTO bronze.census_cir_fats (period,year,month,series,value_mil_lbs,col_idx)
        VALUES %s ON CONFLICT (period,series) DO NOTHING""",[(r[0],r[1],r[2],r[3],r[4],r[5]) for r in seen.values()])
    c.commit()
    cur.execute("SELECT count(*) n, count(distinct series) s, min(year) mn, max(year) mx FROM bronze.census_cir_fats")
    r=cur.fetchone(); print(f"\nbronze.census_cir_fats: {r['n']} rows, {r['s']} series (cols 55-70+229-245), {r['mn']}-{r['mx']}")
