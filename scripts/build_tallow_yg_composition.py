"""Standing diagnostic: EIA tallow + yellow-grease combined control total + composition share.

Addendum B.1 Ruling 2.3 — the relabeling signature. Respondents appear to re-label within a
stable animal-fat/UCO envelope: tallow rises ~1.6B/yr while YG falls ~2.2B/yr, combined ~stable.
This is the swap hypothesis visible in EIA's OWN reporting — strongest single exhibit.

Source: bronze.eia_feedstock_monthly, plant_type='total', NOT withheld/no-data (verified 0
withheld 2023-2025 so the 2025 YG collapse is real, not a suppression artifact). Monthly +
T12M combined + tallow composition share -> silver.eia_tallow_yg_composition.
"""
import sys
from pathlib import Path
ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT/".env")
from src.services.database.db_config import get_connection

DDL = """
CREATE TABLE IF NOT EXISTS silver.eia_tallow_yg_composition (
    period date PRIMARY KEY, year int, month int,
    tallow_mil_lbs numeric, yg_mil_lbs numeric, combined_mil_lbs numeric,
    tallow_share numeric,
    combined_t12m_mil_lbs numeric, tallow_share_t12m numeric,
    loaded_at timestamptz DEFAULT now()
);
"""

with get_connection() as c:
    cur=c.cursor(); cur.execute(DDL); cur.execute("TRUNCATE silver.eia_tallow_yg_composition")
    cur.execute("""
        SELECT make_date(year,month,1) period, year, month,
            sum(quantity_mil_lbs) FILTER (WHERE LOWER(feedstock_name) LIKE '%tallow%') tallow,
            sum(quantity_mil_lbs) FILTER (WHERE LOWER(feedstock_name) LIKE '%yellow grease%') yg
        FROM bronze.eia_feedstock_monthly
        WHERE plant_type='total' AND NOT is_withheld AND quantity_mil_lbs IS NOT NULL
          AND (LOWER(feedstock_name) LIKE '%tallow%' OR LOWER(feedstock_name) LIKE '%yellow grease%')
        GROUP BY 1,2,3 ORDER BY 1
    """)
    recs=[dict(r) for r in cur.fetchall()]
    for i,r in enumerate(recs):
        t=float(r['tallow'] or 0); y=float(r['yg'] or 0); comb=t+y
        share=t/comb if comb else None
        w=recs[max(0,i-11):i+1]
        if len(w)==12:
            ct=sum(float(x['tallow'] or 0)+float(x['yg'] or 0) for x in w)
            tt=sum(float(x['tallow'] or 0) for x in w)
            t12=ct; sh12=tt/ct if ct else None
        else:
            t12=sh12=None
        cur.execute("""INSERT INTO silver.eia_tallow_yg_composition
            (period,year,month,tallow_mil_lbs,yg_mil_lbs,combined_mil_lbs,tallow_share,combined_t12m_mil_lbs,tallow_share_t12m)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (r['period'],r['year'],r['month'],t,y,comb,share,t12,sh12))
    c.commit()
    print(f"  {len(recs)} months -> silver.eia_tallow_yg_composition")
    cur.execute("""SELECT year, round(sum(tallow_mil_lbs)/1e3,2) tallow, round(sum(yg_mil_lbs)/1e3,2) yg,
        round(sum(combined_mil_lbs)/1e3,2) combined, count(*) mo FROM silver.eia_tallow_yg_composition
        WHERE year BETWEEN 2023 AND 2025 GROUP BY 1 ORDER BY 1""")
    print("  year: tallow / YG / combined (B lb)")
    for r in cur.fetchall():
        print(f"    {r['year']}: {float(r['tallow']):.2f} / {float(r['yg']):.2f} / {float(r['combined']):.2f}  ({r['mo']}mo)")
