"""Exploratory: fit tallow non-bio segmentation params from CIR (Addendum A).
Read-only — computes shares/params and sanity-checks vs ruled expectations.
No DB writes."""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from src.services.database.db_config import get_connection

# CIR col_idx we need
COLS = {55:'ebft_prod',56:'ebft_stock',59:'ibft_prod',64:'total_ined_tg_prod',
        233:'fatty_acids',234:'feed',235:'feed_ibft',239:'lubricants',240:'soap',
        242:'me_ibft',245:'other_ined'}

def annual(cur, col, y0, y1):
    cur.execute("""SELECT year, sum(value_mil_lbs) v, count(*) n FROM bronze.census_cir_fats
                   WHERE col_idx=%s AND year BETWEEN %s AND %s GROUP BY 1 ORDER BY 1""",(col,y0,y1))
    return {r['year']:(float(r['v'] or 0), r['n']) for r in cur.fetchall()}

with get_connection() as c:
    cur=c.cursor()
    print("=== A8.1 CIR total tallow production (55+59), annual (should be 5.2-5.7B) ===")
    eb=annual(cur,55,2005,2010); ib=annual(cur,59,2005,2010)
    for y in range(2005,2011):
        e=eb.get(y,(0,0)); i=ib.get(y,(0,0))
        tot=(e[0]+i[0])/1000
        print(f"  {y}: EBFT {e[0]/1000:.2f}B ({e[1]}mo) + IBFT {i[0]/1000:.2f}B ({i[1]}mo) = {tot:.2f}B")

    print("\n=== A3 ibft_share = col59/col64 (should be ~0.55-0.60) ===")
    ib64=annual(cur,64,2005,2010)
    for y in range(2005,2011):
        i=ib.get(y,(0,0))[0]; t=ib64.get(y,(0,0))[0]
        print(f"  {y}: {i/t:.3f}" if t else f"  {y}: n/a")

    print("\n=== A6 EDIBLE_SHARE = 55/(55+59), 2005-2010 (should be 0.32-0.35) ===")
    num=den=0
    for y in range(2005,2011):
        e=eb.get(y,(0,0))[0]; i=ib.get(y,(0,0))[0]
        num+=e; den+=(e+i)
    print(f"  pooled 2005-2010: {num/den:.3f}")

    print("\n=== A2 non-bio segments, annual 2007-2010 (calibration window) ===")
    for col in [233,239,240,245,234,235,242]:
        a=annual(cur,col,2007,2010)
        vals=" ".join(f"{y}:{a.get(y,(0,0))[0]/1000:.2f}B" for y in range(2007,2011))
        print(f"  c{col} {COLS.get(col,''):14}: {vals}")

    print("\n=== A4 feed IBFT (col235) two-chapter: 2005 vs 2010 (should fall 1193->332M) ===")
    f235=annual(cur,235,2005,2011)
    for y in range(2005,2012):
        v=f235.get(y,(0,0)); print(f"  {y}: {v[0]:.0f}M lb ({v[1]}mo)")
