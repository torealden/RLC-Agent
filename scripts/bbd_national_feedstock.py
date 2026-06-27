"""
National BBD feedstock hook — v1, NON-CIRCULAR by construction.

Backbone (independent of EIA feedstock survey):
    TOTAL feedstock (lbs) = SUM over fuel of  production_gal(fuel) x blended_yield(fuel)
    production = EMTS/RFS (gold.us_liquid_fuel_production_monthly) — complete via RINs.
    blended_yield from RLC canon per-feedstock yields x a seed mix (reference.feedstock_conversion_rates).

Mix pinned by PHYSICAL supply, not by itself:
    waste fats (tallow, UCO/yellow grease, DCO, white grease, poultry) + canola are capped at
    national availability (seeded from EIA actuals — FLAGGED, the tunable knobs).
    SOYBEAN OIL = the balance (abundant veg oil residual). Checked vs Tore's S&D ~14B.

Why not circular: the TOTAL comes from production (not EIA feedstock), the waste-fat caps are
physical availability (external), soy is the residual, and EIA only informs the caps where its
survey is reliable. Tune the caps/yields until soy ~= 14B; that becomes the national CONTROL
TOTAL the regional balances must later sum back to.
"""
import sys, argparse
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

def _v(r,k,i):
    try: return r[k]
    except Exception: return r[i]

# Seed BLENDED yield per fuel (lb feedstock/gal), derived from canon per-feedstock yields x
# typical mix. FLAGGED tunable — BD is veg-heavy (~7.4-8.4), RD/SAF waste-fat-heavy (~8-9.4).
BLENDED_YIELD = {"biodiesel": 7.60, "renewable_diesel": 8.60, "saf": 8.60, "coprocessing": 7.60}
SOY_TARGET_BLBS = 14.0   # Tore S&D: total soy oil to BBD ~14 B lb/MY

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--soy-target", type=float, default=SOY_TARGET_BLBS)
    args = ap.parse_args()
    with get_connection() as c:
        cur = c.cursor()
        # 1. production, trailing 12mo (mil gal)
        cur.execute("""SELECT sum(biodiesel_kgal)/1000.0 bd, sum(renewable_diesel_kgal)/1000.0 rd,
                  sum(coalesce(saf_kgal,0))/1000.0 saf, sum(coalesce(co_processing_kgal,0))/1000.0 cop
            FROM gold.us_liquid_fuel_production_monthly
            WHERE period_date > (SELECT max(period_date) FROM gold.us_liquid_fuel_production_monthly)-interval '12 months'""")
        r=cur.fetchone()
        prod={"biodiesel":float(_v(r,'bd',0)),"renewable_diesel":float(_v(r,'rd',1)),
              "saf":float(_v(r,'saf',2)),"coprocessing":float(_v(r,'cop',3))}
        # 2. EIA actual waste-fat + canola consumption (plant_type='total'), trailing 12mo (mil lbs)
        #    = the physical-availability caps (FLAGGED, tunable). Soy excluded -> it's the balance.
        W="year*100+month > (SELECT max(year*100+month)-100 FROM bronze.eia_feedstock_monthly)"
        cur.execute(f"""SELECT feedstock_name, round(sum(quantity_mil_lbs)) lbs FROM bronze.eia_feedstock_monthly
            WHERE plant_type='total' AND {W} AND quantity_mil_lbs IS NOT NULL
              AND feedstock_name NOT IN ('Corn','Grain Sorghum','Biogas','Energy Crops',
                'Municipal Solid Waste','Yard Food Waste','Algae Oil','Other Waste','Soybean Oil')
            GROUP BY 1 ORDER BY 2 DESC NULLS LAST""")
        caps=[(_v(r,'feedstock_name',0), float(_v(r,'lbs',1) or 0)) for r in cur.fetchall()]

    # 3. total feedstock (lbs) = production x blended yield — the independent backbone
    total_lbs = sum(prod[f]*BLENDED_YIELD[f] for f in prod)
    nonsoy_lbs = sum(l for _,l in caps)
    soy_lbs = total_lbs - nonsoy_lbs   # soy = balance

    print("=== NATIONAL BBD FEEDSTOCK v1 (non-circular) ===")
    print("PRODUCTION (trailing 12mo, EMTS, mil gal):",
          " ".join(f"{k}={v:.0f}" for k,v in prod.items()))
    print(f"\nTOTAL feedstock = production x blended yield = {total_lbs/1000:.2f} B lb")
    print(f"  (blended yields: BD {BLENDED_YIELD['biodiesel']}, RD {BLENDED_YIELD['renewable_diesel']}, "
          f"SAF {BLENDED_YIELD['saf']}, coproc {BLENDED_YIELD['coprocessing']} lb/gal — FLAGGED)")
    print(f"\nWASTE FATS + CANOLA (caps seeded from EIA actuals — FLAGGED tunable, mil lbs):")
    for n,l in caps: print(f"    {n:24s} {l/1000:6.2f} B lb")
    print(f"    {'(subtotal non-soy)':24s} {nonsoy_lbs/1000:6.2f} B lb")
    print(f"\n*** SOYBEAN OIL = balance (residual): {soy_lbs/1000:.2f} B lb ***")
    print(f"    vs S&D target ~{args.soy_target:.0f} B lb  -> "
          f"{'MATCH' if abs(soy_lbs/1000-args.soy_target)<1.5 else 'GAP '+format((soy_lbs/1000-args.soy_target),'+.1f')+'B'}")
    print(f"    soy = {100*soy_lbs/total_lbs:.0f}% of total BBD feedstock")
    print(f"\nKNOBS to hit {args.soy_target:.0f}B soy (non-circular — tune these, not the answer):")
    print(f"  - blended yields (total scales linearly): total now {total_lbs/1000:.1f}B")
    print(f"  - waste-fat caps (EIA undercounts RD -> these are a FLOOR; raising them lowers soy)")
    print(f"  - to hit {args.soy_target:.0f}B soy exactly: non-soy must = {(total_lbs/1000-args.soy_target):.1f}B "
          f"(now {nonsoy_lbs/1000:.1f}B)")

if __name__ == "__main__":
    main()
