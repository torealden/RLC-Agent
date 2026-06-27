"""
Load RLC bespoke feedstock conversion rates (lb feedstock per gallon of fuel) into
reference.feedstock_conversion_rates. Source: Tore's Jacobsen-era long-term-outlook calc
(~2020), adopted as RLC numbers. Used to hook RFS/EMTS production -> feedstock demand.

NOTE (flagged 2026-06-27): the per-feedstock yields are physically sensible (~7.4 lb soy
oil/gal biodiesel is standard). The "Total Feedstocks" blended yields Tore listed (BD 7.20,
RD 7.58) are BELOW every component yield -> a different basis (not a share-weighted avg);
stored as blended_total_note only, NOT used as conversion factors. Yields may improve with
tech over time (asymptotic to stoichiometric floor) — out of scope to model now.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

# CANON yields lb feedstock/gal (Tore, 2026 — "use as canon, won't move much").
# (fuel_type, feedstock, yield_lb_per_gal, historical_share_pct or None)
RATES = [
    # biodiesel (FAME) — canon yields; shares not specified (mix comes from EIA/S&D)
    ("biodiesel","canola_oil",7.45,None),("biodiesel","corn_oil",8.20,None),
    ("biodiesel","cottonseed_oil",7.45,None),("biodiesel","palm_oil",7.45,None),
    ("biodiesel","soybean_oil",7.50,None),("biodiesel","sunflower_oil",7.45,None),
    ("biodiesel","poultry_fat",7.45,None),("biodiesel","tallow",7.75,None),
    ("biodiesel","white_grease",7.86,None),("biodiesel","lard",7.80,None),
    ("biodiesel","yellow_grease",8.23,None),("biodiesel","other_grease",8.40,None),
    # renewable diesel (HEFA) — canon yields (yellow grease + UCO now split; DCO 9.20)
    ("renewable_diesel","yellow_grease",8.50,None),("renewable_diesel","used_cooking_oil",8.01,None),
    ("renewable_diesel","soybean_oil",7.50,None),("renewable_diesel","canola_oil",7.55,None),
    ("renewable_diesel","tallow",9.38,None),("renewable_diesel","distillers_corn_oil",9.20,None),
    ("renewable_diesel","choice_white_grease",9.38,None),("renewable_diesel","poultry_fat",8.12,None),
    # co-processing — 2021 era (not re-specified in 2026 canon; yields + shares retained)
    ("co_processing","soybean_oil",7.40,50.0),("co_processing","canola_oil",7.45,25.0),
    ("co_processing","tallow",7.75,10.0),("co_processing","uco_yellow_grease",8.01,10.0),
    ("co_processing","corn_oil",8.20,1.7),("co_processing","white_grease",7.86,1.7),
    ("co_processing","poultry_fat",7.45,1.7),
]
CANON_FUELS = {"biodiesel", "renewable_diesel"}  # 2026 canon; co_processing = 2021

def main():
    with get_connection() as c:
        cur = c.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS reference.feedstock_conversion_rates (
            fuel_type text, feedstock text, yield_lb_per_gal numeric,
            historical_share_pct numeric, source text, as_of text,
            notes text, PRIMARY KEY (fuel_type, feedstock))""")
        cur.execute("TRUNCATE reference.feedstock_conversion_rates")
        for ft, fs, y, sh in RATES:
            canon = ft in CANON_FUELS
            src = "RLC canon (Tore 2026)" if canon else "RLC (Tore 2021)"
            asof = "2026" if canon else "2021"
            note = None if canon else "co-processing yields/shares from 2021; not re-specified in 2026 canon"
            cur.execute("""INSERT INTO reference.feedstock_conversion_rates
                (fuel_type,feedstock,yield_lb_per_gal,historical_share_pct,source,as_of,notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (ft, fs, y, sh, src, asof, note))
        c.commit()
        cur.execute("SELECT fuel_type, count(*), round(avg(yield_lb_per_gal),2) FROM reference.feedstock_conversion_rates GROUP BY 1 ORDER BY 1")
        print("loaded reference.feedstock_conversion_rates:")
        for r in cur.fetchall():
            r = r if not isinstance(r, dict) else list(r.values())
            print(f"   {r[0]:18s} {r[1]} feedstocks, avg yield {r[2]} lb/gal")

if __name__ == "__main__":
    main()
