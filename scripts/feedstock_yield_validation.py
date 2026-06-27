"""
Validate RLC feedstock conversion yields against the production<->feedstock identity:
  implied gallons = sum(EIA feedstock_lbs(f) / RLC yield(f))   should ~= EMTS production.

FINDING (2026-06-27): biodiesel reconciles ~93% (yields good); renewable diesel only ~34%
because EIA's RD feedstock survey is materially INCOMPLETE (~35% of production-implied).
=> derive RD feedstock from PRODUCTION x yield (EMTS production is complete via RINs), NOT
from the EIA feedstock survey. This is why EIA soy-oil-to-BBD (~11.8B) understates the
true ~14B target. Gap: RLC 2021 rate file has NO renewable_diesel soybean/canola yields
(RD didn't use them in 2021) -- needs Tore's RD veg-oil yields to complete the RD hook.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

EIA_TO_RATE = {'soybean oil':'soybean_oil','tallow':'tallow','yellow grease':'uco_yellow_grease',
    'corn oil':'corn_oil','canola oil':'canola_oil','white grease':'white_grease',
    'poultry':'poultry_fat','fish oil':'fish_oil'}
# placeholder yields for RD veg oils absent from the 2021 file — FLAGGED, pending Tore
PLACEHOLDER = {('renewable_diesel','soybean_oil'):7.60, ('renewable_diesel','canola_oil'):7.60}

def _v(r,k,i):
    try: return r[k]
    except Exception: return r[i]

def main():
    with get_connection() as c:
        cur=c.cursor()
        cur.execute("SELECT fuel_type,feedstock,yield_lb_per_gal FROM reference.feedstock_conversion_rates")
        Y={(_v(r,'fuel_type',0),_v(r,'feedstock',1)):float(_v(r,'yield_lb_per_gal',2)) for r in cur.fetchall()}
        Y.update(PLACEHOLDER)
        W="year*100+month > (SELECT max(year*100+month)-100 FROM bronze.eia_feedstock_monthly)"
        cur.execute("""SELECT sum(biodiesel_kgal)/1000.0 bd, sum(renewable_diesel_kgal)/1000.0 rd
            FROM gold.us_liquid_fuel_production_monthly
            WHERE period_date > (SELECT max(period_date) FROM gold.us_liquid_fuel_production_monthly)-interval '12 months'""")
        r=cur.fetchone(); prod={'biodiesel':float(_v(r,'bd',0)),'renewable_diesel':float(_v(r,'rd',1))}
        print("VALIDATION: sum(EIA feedstock / RLC yield) = implied production vs EMTS actual (mil gal)")
        for ft in ('biodiesel','renewable_diesel'):
            cur.execute(f"""SELECT feedstock_name, sum(quantity_mil_lbs) lbs FROM bronze.eia_feedstock_monthly
                WHERE plant_type=%s AND {W} AND quantity_mil_lbs IS NOT NULL GROUP BY 1""",(ft,))
            impl=0; unm=[]
            for rr in cur.fetchall():
                nm=_v(rr,'feedstock_name',0); lbs=float(_v(rr,'lbs',1) or 0)
                y=Y.get((ft,EIA_TO_RATE.get((nm or '').lower())))
                if y: impl+=lbs/y
                elif lbs>0: unm.append(nm)
            print(f"  {ft:18s} implied {impl:7.0f} vs EMTS {prod[ft]:7.0f}  ({100*impl/prod[ft]:.0f}%)"
                  + (f"  [EIA feedstock INCOMPLETE]" if impl < 0.6*prod[ft] else ""))
            if unm: print(f"     unmapped (no RLC yield): {unm}")

if __name__ == "__main__":
    main()
