"""
Per-facility pathway-backed feedstock slate — FFA step 1 (eligibility prior).

Builds, for every US BBD facility with an LCFS pathway, the set of feedstocks it is
APPROVED to run (silver.lcfs_pathway_ci.feedstock_code — already canonical), with:
  - pathway count per feedstock (rough prior on feedstock importance; NOT volume)
  - min CI per feedstock (the LCFS economic signal — lower CI = more credit value)
  - fuel type, location -> PADD

This is an ELIGIBILITY PRIOR, not realized consumption. A plant approved for UCO+tallow+soy
can run any blend within that set; what it ACTUALLY runs comes from the allocator running
economics/prices over this eligible set. Output feeds (a) the allocator's pathway gate and
(b) a PADD rollup that replaces the FLAGGED SOY_INTENSITY guess in padd_soyoil_balance.py.

Step 2 (separate) attaches nameplate capacity by matching these ~67 facilities to the
facility master -> capacity-weighted slate. Count-weighting here is the validation checkpoint.
"""
import sys, argparse
from collections import defaultdict
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

STATE2PADD = {
    **{s: 'PADD1' for s in ['Connecticut','Maine','Massachusetts','New Hampshire','Rhode Island',
        'Vermont','New York','New Jersey','Pennsylvania','Delaware','Maryland','District of Columbia',
        'West Virginia','Virginia','North Carolina','South Carolina','Georgia','Florida']},
    **{s: 'PADD2' for s in ['Illinois','Indiana','Iowa','Kansas','Kentucky','Michigan','Minnesota',
        'Missouri','Nebraska','North Dakota','Ohio','Oklahoma','South Dakota','Tennessee','Wisconsin']},
    **{s: 'PADD3' for s in ['Alabama','Arkansas','Louisiana','Mississippi','New Mexico','Texas']},
    **{s: 'PADD4' for s in ['Colorado','Idaho','Montana','Utah','Wyoming']},
    **{s: 'PADD5' for s in ['Alaska','Arizona','California','Hawaii','Nevada','Oregon','Washington']},
}
# soy-as-share-of-lipid is the empirical analogue of the flagged SOY_INTENSITY in padd_soyoil_balance
FLAGGED_INTENSITY = {'PADD1':1.0,'PADD2':1.8,'PADD3':0.65,'PADD4':1.1,'PADD5':0.45}

def g(r, k, i):
    try: return r[k]
    except Exception: return r[i]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fuel", default="diesel", help="fuel filter (ILIKE), default 'diesel' = RD+BD")
    ap.add_argument("--per-facility", action="store_true", help="print every facility's slate")
    args = ap.parse_args()

    with get_connection() as c:
        cur = c.cursor()
        cur.execute("""SELECT fuel_producer, facility_name, facility_location, fuel,
                              feedstock_code, count(*) n, min(ci_score) min_ci
                       FROM silver.lcfs_pathway_ci
                       WHERE is_us_facility AND fuel ILIKE %s AND feedstock_code IS NOT NULL
                       GROUP BY 1,2,3,4,5""", (f"%{args.fuel}%",))
        rows = cur.fetchall()

    # per-facility slate: {(producer,facility): {padd, fuel, feedstocks:{code:{n,min_ci}}}}
    fac = {}
    for r in rows:
        key = (g(r,'fuel_producer',0), g(r,'facility_name',1))
        loc = g(r,'facility_location',2); padd = STATE2PADD.get(loc, '?')
        f = fac.setdefault(key, {'padd': padd, 'loc': loc, 'fuels': set(), 'fs': {}})
        f['fuels'].add(g(r,'fuel',3))
        f['fs'][g(r,'feedstock_code',4)] = {'n': int(g(r,'n',5)), 'min_ci': g(r,'min_ci',6)}

    print(f"=== PER-FACILITY FEEDSTOCK SLATE (eligibility prior) — {len(fac)} US BBD facilities ===")
    if args.per_facility:
        for (prod, name), f in sorted(fac.items(), key=lambda x: (x[1]['padd'], x[0][1])):
            slate = ", ".join(f"{code}(n={d['n']},CI{d['min_ci']:.0f})" for code, d in
                              sorted(f['fs'].items(), key=lambda x: -x[1]['n']))
            print(f"  [{f['padd']}] {str(name)[:34]:34s} {slate}")

    # PADD rollup: count-weighted feedstock share + soy-as-share-of-lipid
    LIPID = {'soybean_oil','canola_oil','distillers_corn_oil','corn_oil','used_cooking_oil','tallow','white_grease'}
    padd_fs = defaultdict(lambda: defaultdict(int)); padd_facs = defaultdict(set)
    for (prod, name), f in fac.items():
        padd_facs[f['padd']].add(name)
        for code, d in f['fs'].items():
            padd_fs[f['padd']][code] += d['n']

    print(f"\n=== PADD ROLLUP — count-weighted feedstock mix (pathway counts, NOT volume) ===")
    print(f"  {'PADD':14s} {'facs':>4} {'soy%':>5} {'canola%':>7} {'DCO%':>5} {'UCO%':>5} {'tallow%':>7}  vs flagged")
    emp = {}
    for p in ['PADD1','PADD2','PADD3','PADD4','PADD5']:
        d = padd_fs.get(p, {}); tot = sum(v for k, v in d.items() if k in LIPID)
        if not tot:
            print(f"  {p:14s} {len(padd_facs.get(p,[])):4d}   (no pathways)"); continue
        sh = lambda code: 100 * d.get(code, 0) / tot
        emp[p] = sh('soybean_oil')
        print(f"  {p:14s} {len(padd_facs[p]):4d} {sh('soybean_oil'):5.0f} {sh('canola_oil'):7.0f} "
              f"{sh('distillers_corn_oil')+sh('corn_oil'):5.0f} {sh('used_cooking_oil'):5.0f} {sh('tallow'):7.0f}"
              f"   [flag {FLAGGED_INTENSITY[p]}]")

    # normalize empirical soy-share to a relative intensity (mean=1) to compare directly with flag
    if emp:
        mean = sum(emp.values()) / len(emp)
        print(f"\n  EMPIRICAL soy-intensity (soy% normalized to mean=1) vs my FLAGGED guess:")
        for p in sorted(emp):
            ei = emp[p] / mean if mean else 0
            verdict = "ok" if abs(ei - FLAGGED_INTENSITY[p]) < 0.4 else ">> OFF"
            print(f"    {p}: empirical {ei:.2f}  vs flagged {FLAGGED_INTENSITY[p]:.2f}   {verdict}")
        print(f"\n  NOTE: count-weighted (pathway counts), eligibility not consumption. Capacity-weighting"
              f"\n  (step 2) + allocator economics give the realized intensity. This is the directional check.")

if __name__ == "__main__":
    main()
