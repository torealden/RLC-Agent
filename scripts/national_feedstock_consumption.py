"""
National feedstock consumption by commodity — bottom-up from per-facility mixes (FFA payoff v1).

NOT the economic allocator (allocator.py, still stubbed). This is the direct mix-based rollup:
    national fuel production (EMTS, trailing 12mo, by fuel class)
      -> allocated to facilities by operating-nameplate share
      -> x blended yield (lb feedstock/gal)
      -> distributed across commodities by each facility's MIX
         (reference.facility_assumed_mix if present; else default-by-fuel)
      -> summed by commodity, nationally and by PADD.

Reconciled against EIA national feedstock actuals (plant_type='total', trailing 12mo) — the
gap IS the signal (where our per-facility mixes diverge from reality). This rollup is the
baseline the economic allocator must later reproduce/beat.
"""
import sys
from collections import defaultdict
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

YIELD = {'bd': 7.60, 'rd': 8.60}                 # lb feedstock / gal (from bbd_national_feedstock)
# EIA Form 819 splits ONLY vegetable oils by plant type (Table 2c); fats/greases (Table 2b) are total-only.
# So BD/RD default mixes are DERIVED fats-inclusive (see derive_defaults): veg from the plant_type split,
# fats apportioned via BD_fats = BD_prod*yield - BD_veg, RD_fats = total_fats - BD_fats.
VEG_NAME = {'Soybean Oil': 'SBO', 'Canola Oil': 'CAN', 'Corn Oil': 'CO'}
FAT_NAME = {'Tallow': 'BFT', 'Yellow Grease': 'UCO', 'White Grease': 'CWG', 'Poultry': 'PLT'}
# my code -> EIA 'total' feedstock_name, for reconciliation (EIA lumps DCO into Corn Oil, UCO~Yellow Grease)
CODE2EIA = {'SBO':'Soybean Oil','CAN':'Canola Oil','CO':'Corn Oil','DCO':'Corn Oil',
            'BFT':'Tallow','CWG':'White Grease','PLT':'Poultry','YG':'Yellow Grease','UCO':'Yellow Grease'}

def g(r, k, i):
    try: return r[k]
    except Exception: return r[i]

def derive_defaults(cur, prod_bd, prod_rd):
    """Fats-inclusive BD/RD default mixes from EIA Form 819 (veg split from plant_type + apportioned
    total fats). Returns (bd_default, rd_default), each a dict of my codes -> share summing to 1.0."""
    W = "year*100+month > (SELECT max(year*100+month)-100 FROM bronze.eia_feedstock_monthly)"
    def pull(pt, names):
        cur.execute(f"""SELECT feedstock_name, sum(quantity_mil_lbs) q FROM bronze.eia_feedstock_monthly
            WHERE plant_type=%s AND {W} AND quantity_mil_lbs IS NOT NULL AND feedstock_name = ANY(%s)
            GROUP BY 1""", (pt, list(names)))
        return {g(r, 'feedstock_name', 0): float(g(r, 'q', 1) or 0) for r in cur.fetchall()}
    bd_veg = pull('biodiesel', VEG_NAME); rd_veg = pull('renewable_diesel', VEG_NAME)
    tot_fat = pull('total', FAT_NAME)
    fat_sum = sum(tot_fat.values()) or 1.0
    bd_fats = max(0.0, prod_bd * YIELD['bd'] - sum(bd_veg.values()))   # BD's fats = its total feedstock minus its veg
    rd_fats = max(0.0, fat_sum - bd_fats)                              # rest of national fats -> RD (+SAF/coproc)
    fatcomp = {FAT_NAME[k]: v / fat_sum for k, v in tot_fat.items()}   # tallow/YG/WG/poultry shares of total fats
    def build(veg, fats):
        m = defaultdict(float)
        for nm, q in veg.items(): m[VEG_NAME[nm]] += q
        for code, sh in fatcomp.items(): m[code] += fats * sh
        s = sum(m.values()) or 1.0
        return {k: v / s for k, v in m.items()}
    return build(bd_veg, bd_fats), build(rd_veg, rd_fats)

def main():
    with get_connection() as c:
        cur = c.cursor()
        # 1. national production trailing 12mo (mil gal)
        cur.execute("""SELECT sum(biodiesel_kgal)/1000.0 bd, sum(renewable_diesel_kgal)/1000.0 rd,
                  sum(coalesce(saf_kgal,0))/1000.0 saf, sum(coalesce(co_processing_kgal,0))/1000.0 cop
            FROM gold.us_liquid_fuel_production_monthly
            WHERE period_date > (SELECT max(period_date) FROM gold.us_liquid_fuel_production_monthly)-interval '12 months'""")
        r = cur.fetchone()
        prod_bd = float(g(r,'bd',0) or 0)
        prod_rd = float(g(r,'rd',1) or 0) + float(g(r,'saf',2) or 0) + float(g(r,'cop',3) or 0)  # RD+SAF+coproc share yield ~8.6
        bd_default, rd_default = derive_defaults(cur, prod_bd, prod_rd)  # EIA-derived fats-inclusive priors

        # 2. operating facilities + assumed mix
        cur.execute("""SELECT f.facility_id, f.facility_name, f.padd, f.fuel_type, f.nameplate_mmgy
                       FROM reference.biofuel_facilities f
                       WHERE f.status ILIKE '%oper%' AND f.nameplate_mmgy IS NOT NULL
                         AND (f.padd IS NULL OR f.padd <> 'NON-US')
                         -- exclude non-lipid technologies (consume no oils/fats)
                         AND (f.technology IS NULL OR lower(f.technology) NOT IN
                              ('fractionation','pyrolysis','fischer_tropsch','atj','gasification','fermentation'))""")
        facs = cur.fetchall()
        cur.execute("SELECT facility_id, feedstock_code, pct FROM reference.facility_assumed_mix")  # all sources (xlsx + screenshot est)
        mix = defaultdict(dict)
        for r in cur.fetchall():
            mix[g(r,'facility_id',0)][g(r,'feedstock_code',1)] = float(g(r,'pct',2)) / 100.0
        # EIA actuals for reconciliation
        W = "year*100+month > (SELECT max(year*100+month)-100 FROM bronze.eia_feedstock_monthly)"
        cur.execute(f"""SELECT feedstock_name, round(sum(quantity_mil_lbs)) lbs FROM bronze.eia_feedstock_monthly
            WHERE plant_type='total' AND {W} AND quantity_mil_lbs IS NOT NULL GROUP BY 1""")
        eia = {g(r,'feedstock_name',0): float(g(r,'lbs',1) or 0)/1000.0 for r in cur.fetchall()}

    # classify + capacity per class
    def is_bd(ft): ft = str(ft or '').lower(); return 'biodiesel' in ft and 'renewable' not in ft
    cap_bd = sum(float(g(f,'nameplate_mmgy',4)) for f in facs if is_bd(g(f,'fuel_type',3)))
    cap_rd = sum(float(g(f,'nameplate_mmgy',4)) for f in facs if not is_bd(g(f,'fuel_type',3)))

    def norm(m):                                   # normalize a mix to shares summing to 1.0
        s = sum(m.values()) or 1.0
        return {k: v/s for k, v in m.items()}
    # bd_default / rd_default are EIA-derived fats-inclusive priors (computed above via derive_defaults)

    # 3. distribute
    by_comm = defaultdict(float); by_padd_comm = defaultdict(lambda: defaultdict(float))
    covered_cap = uncovered_cap = 0.0
    for f in facs:
        fid = g(f,'facility_id',0); cap = float(g(f,'nameplate_mmgy',4)); padd = g(f,'padd',2) or '?'
        bd = is_bd(g(f,'fuel_type',3))
        prod = (prod_bd * cap/cap_bd) if bd else (prod_rd * cap/cap_rd if cap_rd else 0)
        lbs = prod * (YIELD['bd'] if bd else YIELD['rd'])
        fmix = norm(mix.get(fid) or (bd_default if bd else rd_default))   # shares sum to 1.0
        if fid in mix: covered_cap += cap
        else: uncovered_cap += cap
        for code, sh in fmix.items():
            by_comm[code] += lbs * sh
            by_padd_comm[padd][code] += lbs * sh

    # ---- report ----
    print(f"=== NATIONAL FEEDSTOCK CONSUMPTION — bottom-up from facility mixes (B lb/yr) ===")
    print(f"production: BD {prod_bd/1000:.2f} B gal, RD+SAF+coproc {prod_rd/1000:.2f} B gal (trailing 12mo)")
    print(f"mix coverage: {covered_cap:.0f} mmgy curated / {covered_cap+uncovered_cap:.0f} mmgy "
          f"({100*covered_cap/(covered_cap+uncovered_cap):.0f}%); uncovered uses default-by-fuel\n")
    # reconcile vs EIA (map my codes to EIA names)
    eia_cmp = defaultdict(float)
    for code, lbs in by_comm.items():
        eia_cmp[CODE2EIA.get(code, code)] += lbs/1000.0
    print(f"  {'feedstock (EIA grouping)':26s} {'bottom-up':>10} {'EIA actual':>11} {'gap':>8}")
    names = sorted(set(list(eia_cmp.keys()) + [k for k in eia if k in CODE2EIA.values()]),
                   key=lambda n: -eia_cmp.get(n, 0))
    for nm in names:
        bu = eia_cmp.get(nm, 0); ea = eia.get(nm, 0)
        print(f"  {nm:26s} {bu:10.2f} {ea:11.2f} {bu-ea:+8.2f}")
    tot_bu = sum(eia_cmp.values())
    print(f"  {'TOTAL':26s} {tot_bu:10.2f}")
    print(f"\n  (gap = bottom-up minus EIA; EIA lumps DCO into Corn Oil and UCO into Yellow Grease)")

    print(f"\n=== by PADD x commodity (B lb/yr, my codes) ===")
    codes = ['SBO','DCO','CO','CAN','UCO','BFT','CWG','YG','PLT','LCI']
    print("  PADD     " + " ".join(f"{c:>5}" for c in codes))
    for p in sorted(by_padd_comm, key=lambda x: str(x)):
        d = by_padd_comm[p]
        print(f"  {str(p):8s} " + " ".join(f"{d.get(c,0)/1000:5.1f}" for c in codes))

if __name__ == "__main__":
    main()
