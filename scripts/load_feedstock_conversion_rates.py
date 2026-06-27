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

SOURCE = "RLC (Tore, Jacobsen-era 2021)"
# (fuel_type, feedstock, yield_lb_per_gal, historical_share_pct)
RATES = [
    # biodiesel (FAME)
    ("biodiesel","canola_oil",7.45,10.8),("biodiesel","corn_oil",8.20,12.3),
    ("biodiesel","cottonseed_oil",7.45,0.0),("biodiesel","palm_oil",7.45,0.0),
    ("biodiesel","soybean_oil",7.40,52.5),("biodiesel","sunflower_oil",7.45,0.3),
    ("biodiesel","poultry_fat",7.45,1.7),("biodiesel","tallow",7.75,3.1),
    ("biodiesel","white_grease",7.86,5.0),("biodiesel","lard",7.80,0.6),
    ("biodiesel","uco_yellow_grease",8.23,12.2),("biodiesel","other_grease",8.40,0.9),
    # renewable diesel (HEFA)
    ("renewable_diesel","corn_oil",9.38,9.6),("renewable_diesel","fish_oil",9.38,3.2),
    ("renewable_diesel","other",9.38,0.0),("renewable_diesel","tallow",9.38,69.5),
    ("renewable_diesel","uco_yellow_grease",8.01,17.6),
    # RD veg oils absent from the 2021 file (RD didn't use them then); RLC estimate ~BD yield,
    # pending Tore's more-recent RD veg-oil yields. Flagged via EST_RD note below.
    ("renewable_diesel","soybean_oil",7.60,0.0),("renewable_diesel","canola_oil",7.60,0.0),
    # co-processing
    ("co_processing","soybean_oil",7.40,50.0),("co_processing","canola_oil",7.45,25.0),
    ("co_processing","tallow",7.75,10.0),("co_processing","uco_yellow_grease",8.01,10.0),
    ("co_processing","corn_oil",8.20,1.7),("co_processing","white_grease",7.86,1.7),
    ("co_processing","poultry_fat",7.45,1.7),
]
BLENDED_NOTE = {"biodiesel": 7.20, "renewable_diesel": 7.58}  # Tore's stated totals (diff basis; not used)

def main():
    with get_connection() as c:
        cur = c.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS reference.feedstock_conversion_rates (
            fuel_type text, feedstock text, yield_lb_per_gal numeric,
            historical_share_pct numeric, source text, as_of text,
            notes text, PRIMARY KEY (fuel_type, feedstock))""")
        cur.execute("TRUNCATE reference.feedstock_conversion_rates")
        EST_RD = {("renewable_diesel","soybean_oil"), ("renewable_diesel","canola_oil")}
        for ft, fs, y, sh in RATES:
            if (ft, fs) in EST_RD:
                note = "RLC ESTIMATE (~BD yield); 2021 file predates RD veg-oil use — replace w/ Tore's RD yields when found"
            elif ft in BLENDED_NOTE:
                note = (f"Tore stated blended-total yield for {ft}={BLENDED_NOTE[ft]} (different "
                        f"basis, below components; not used)")
            else:
                note = None
            cur.execute("""INSERT INTO reference.feedstock_conversion_rates
                (fuel_type,feedstock,yield_lb_per_gal,historical_share_pct,source,as_of,notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (ft, fs, y, sh, SOURCE, "2021", note))
        c.commit()
        cur.execute("SELECT fuel_type, count(*), round(avg(yield_lb_per_gal),2) FROM reference.feedstock_conversion_rates GROUP BY 1 ORDER BY 1")
        print("loaded reference.feedstock_conversion_rates:")
        for r in cur.fetchall():
            r = r if not isinstance(r, dict) else list(r.values())
            print(f"   {r[0]:18s} {r[1]} feedstocks, avg yield {r[2]} lb/gal")

if __name__ == "__main__":
    main()
