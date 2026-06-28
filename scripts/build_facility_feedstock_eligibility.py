"""
FFA step-2: two-tier facility feedstock-eligibility builder.

Fills reference.biofuel_facilities.eligible_feedstocks for every US BBD facility, using:

  TIER 0 (preserve)  — facility already has hand-curated eligible_feedstocks. Keep it.
                       Flag any CONFLICT where a matched CARB pathway implies a different set.
  TIER 1 (pathway)   — facility name-matches a CARB LCFS pathway producer. eligible = the
                       feedstocks it is APPROVED to run (silver.lcfs_pathway_ci.feedstock_code,
                       crosswalked to the master's code vocab). Authoritative for coastal/low-CI RD.
  TIER 2 (tech default) — no curated set, no pathway. Default eligible slate by facility
                       TECHNOLOGY (the generic fleet runs under EPA Table-1 generally-applicable
                       pathways and files no letter — invisible to pathway data by design).

Realized mix is NOT set here — that comes from the allocator running economics over the
eligible set. This builds the ELIGIBILITY PRIOR only. EIA national actuals are the control.

DRY-RUN by default: prints tier coverage, ambiguous name-matches (for human review), and
curated-vs-CARB conflicts. Pass --write to persist (only fills NULLs + flagged updates).

See memory reference_carb_pathway_selection_bias for the why.
"""
import sys, argparse, re
from collections import defaultdict
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))   # for clean_facility_master import
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection
from clean_facility_master import norm_state           # reuse the state normalizer

# Feedstock code legend (master vocab — precise, fats are NOT interchangeable; use real
# commodity names, NOT an "advanced" bucket — camelina/carinata are being commercialized):
#   SBO soybean oil | CO corn oil | DCO distillers corn oil | CAN canola/rapeseed oil
#   UCO used cooking oil | BFT beef tallow (CATTLE) | CWG choice white grease (PORK)
#   PLT poultry fat (small volumes) | YG yellow grease | CAM camelina | CAR carinata | FSH fish oil
CARB2MASTER = {
    'soybean_oil': 'SBO', 'canola_oil': 'CAN', 'distillers_corn_oil': 'DCO',
    'corn_oil': 'CO', 'used_cooking_oil': 'UCO', 'white_grease': 'CWG',
    # CARB code 002 = "Tallow (animal AND poultry fat)" — lumps cattle + poultry. We map to BFT
    # (the dominant component); CARB granularity cannot recover the poultry (PLT) slice. KNOWN GAP.
    'tallow': 'BFT',
}
# TIER 2 default eligible slate by technology (master vocab). FT/pyrolysis/ATJ/fermentation
# are non-lipid (cellulosic/alcohol/gas) -> no lipid feedstock, excluded from BBD-lipid competition.
TECH_DEFAULT = {
    'transesterification': ['SBO', 'DCO', 'CO', 'CAN', 'BFT', 'CWG', 'PLT', 'UCO'],       # veg-oil + fats, SBO primary
    'hefa':                ['SBO', 'DCO', 'CO', 'CAN', 'BFT', 'CWG', 'PLT', 'UCO', 'YG'],  # all lipids
    'coprocessing':        ['DCO', 'CO', 'BFT', 'CWG', 'PLT', 'UCO', 'YG'],               # low-CI fats/oils
}
NON_LIPID_TECH = {'fischer_tropsch', 'pyrolysis', 'fast_pyrolysis', 'atj', 'fermentation'}

SUFFIX = re.compile(r'\b(llc|l\.l\.c|lp|l\.p|inc|incorporated|corp|corporation|company|co|ltd|holdings'
                    r'|energy|fuels?|renewables?|renewable|biodiesel|biofuels?|bio|bioenergy|diesel'
                    r'|products|refining|refinery|group|new|green|north|america|american)\b')
def norm(name):
    if not name: return set()
    s = str(name).lower().split(' - ')[0]          # drop "- Location" suffix in master names
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    s = SUFFIX.sub(' ', s)
    return {t for t in s.split() if len(t) > 2}

def match_score(carb_name, master_name, master_company):
    a = norm(carb_name)
    b = norm(master_name) | norm(master_company)
    if not a or not b: return 0.0
    inter = a & b
    if not inter: return 0.0
    # containment-favoring score: strong if all of the shorter side's tokens are present
    return len(inter) / min(len(a), len(b))

def g(r, k, i):
    try: return r[k]
    except Exception: return r[i]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="persist assignments (fills NULLs + conflicts)")
    ap.add_argument("--strong", type=float, default=0.6, help="match score >= this = strong (auto)")
    ap.add_argument("--weak", type=float, default=0.34, help="score in [weak,strong) = ambiguous (review)")
    args = ap.parse_args()

    with get_connection() as c:
        cur = c.cursor()
        # CARB: producer/facility -> set of crosswalked feedstock codes + min CI per code
        cur.execute("""SELECT fuel_producer, facility_name, facility_location, feedstock_code, min(ci_score) min_ci
                       FROM silver.lcfs_pathway_ci
                       WHERE is_us_facility AND fuel ILIKE '%diesel%' AND feedstock_code IS NOT NULL
                       GROUP BY 1,2,3,4""")
        carb = {}
        for r in cur.fetchall():
            key = (g(r,'fuel_producer',0), g(r,'facility_name',1))
            code = CARB2MASTER.get(g(r,'feedstock_code',3))
            if not code: continue
            e = carb.setdefault(key, {'codes': {}, 'name': g(r,'fuel_producer',0) or g(r,'facility_name',1),
                                      'state': norm_state(g(r,'facility_location',2))[0]})
            ci = g(r,'min_ci',4)
            e['codes'][code] = min(e['codes'].get(code, 999), ci) if ci is not None else e['codes'].get(code, None)

        # Facility master (US BBD)
        cur.execute("""SELECT facility_id, company, facility_name, state, padd, technology, fuel_type,
                              status, nameplate_mmgy, eligible_feedstocks
                       FROM reference.biofuel_facilities""")
        facs = cur.fetchall()

    carb_list = list(carb.values())
    rows, conflicts, ambiguous = [], [], []
    tier_count = defaultdict(int)

    for f in facs:
        fid = g(f,'facility_id',0); comp = g(f,'company',1); fname = g(f,'facility_name',2)
        tech = g(f,'technology',5); curated = g(f,'eligible_feedstocks',9)

        # best CARB match — gated on state agreement (kills cross-state false matches like
        # Iowa 'Western Dubuque' ~ California 'Imperial Western', and same-company-other-site)
        mstate = g(f,'state',3)
        best, best_s = None, 0.0
        for e in carb_list:
            if e.get('state') and mstate and e['state'] != mstate:
                continue
            s = match_score(e['name'], fname, comp)
            if s > best_s: best, best_s = e, s
        carb_set = sorted(best['codes'].keys()) if (best and best_s >= args.weak) else None

        fuel = str(g(f,'fuel_type',6) or '').lower()
        is_bd = ('biodiesel' in fuel) or (tech == 'transesterification')
        primary, uncal = None, False

        if curated:
            if carb_set and best_s >= args.strong and set(carb_set) != set(curated):
                tier = 'TIER0_carb_override'; assigned = carb_set    # CARB wins (Tore: always defer to CARB)
                conflicts.append((fname, sorted(curated), carb_set, round(best_s,2), best['name']))
            else:
                tier = 'TIER0_curated'; assigned = sorted(curated)
        elif carb_set and best_s >= args.strong:
            tier = 'TIER1_pathway'; assigned = carb_set                 # authoritative (coastal/low-CI RD)
        elif carb_set and best_s >= args.weak:
            tier = 'TIER1_ambiguous'; assigned = carb_set; uncal = True
            ambiguous.append((fname, mstate, best['name'], best.get('state'), round(best_s,2), carb_set))
        elif is_bd:
            # generic BD fleet: default ASSUMED mix to soy oil (easy, experienced, Midwest supply);
            # keep eligible set broad so the allocator can still pick fats on economics. FLAG uncalibrated.
            tier = 'TIER2_bd_soy'; assigned = TECH_DEFAULT['transesterification']; primary = 'SBO'; uncal = True
        elif tech in TECH_DEFAULT:                                       # hefa / coprocessing RD
            tier = 'TIER2_rd_tech'; assigned = TECH_DEFAULT[tech]; uncal = True
        elif tech in NON_LIPID_TECH:
            tier = 'NON_LIPID'; assigned = []
        else:
            tier = 'UNRESOLVED'; assigned = []; uncal = True            # no fuel/tech signal -> needs you
        tier_count[tier] += 1
        rows.append(dict(fid=fid, name=fname, company=comp, state=g(f,'state',3), padd=g(f,'padd',4),
                         tech=tech, fuel=g(f,'fuel_type',6), status=g(f,'status',7),
                         mmgy=g(f,'nameplate_mmgy',8), tier=tier, elig=assigned, primary=primary, uncal=uncal))

    # ---- report ----
    print(f"=== FFA two-tier feedstock-eligibility build — {len(facs)} facilities (DRY-RUN={'no' if args.write else 'yes'}) ===\n")
    print("Tier coverage:")
    for t in ['TIER0_curated','TIER0_carb_override','TIER1_pathway','TIER1_ambiguous','TIER2_bd_soy','TIER2_rd_tech','NON_LIPID','UNRESOLVED']:
        print(f"  {t:20s} {tier_count[t]:3d}")
    print(f"\nAmbiguous CARB matches (score {args.weak}-{args.strong}) — REVIEW THESE:")
    print(f"  {'master facility':32s} {'mST':>3} | {'CARB candidate':28s} {'cST':>3} | need")
    if not ambiguous: print("  (none)")
    for fn, mst, carbn, cst, s, codes in sorted(ambiguous, key=lambda x: -x[4]):
        mst = mst or '--'; cst = cst or '--'
        if mst == '--':
            need = "STATE (you) -> auto-resolves"
        elif cst != '--' and mst != cst:
            need = f"REJECT? cross-state ({mst}!={cst})"
        else:
            need = "SAME SITE? yes/no"
        print(f"  {str(fn)[:32]:32s} {mst:>3} | {str(carbn)[:28]:28s} {cst:>3} | {need}")
    print(f"\nCurated-vs-CARB conflicts (strong match, different set) — REVIEW:")
    if not conflicts: print("  (none)")
    for fn, cur_set, carb_set, s, carbn in conflicts:
        print(f"  '{str(fn)[:30]}' curated={cur_set} vs CARB[{s}]={carb_set}")

    # PADD x soy-eligibility rollup (capacity-weighted, the step-2 payoff)
    print(f"\n=== PADD rollup — share of OPERATING BBD nameplate ELIGIBLE for soy (SBO) ===")
    by = defaultdict(lambda: [0.0, 0.0])   # padd -> [soy_eligible_mmgy, total_mmgy]
    excluded = 0.0
    for r in rows:
        if not r['mmgy'] or r['tier'] == 'NON_LIPID': continue
        # CLEAN filter: must have a real PADD and an explicit operating status. NULL padd / NULL
        # status = announced/planned/unlocated -> excluded (facility-master hygiene gap, see notes).
        if not r['padd'] or not r['status'] or 'oper' not in str(r['status']).lower():
            excluded += float(r['mmgy']); continue
        mm = float(r['mmgy']); by[r['padd']][1] += mm
        if 'SBO' in r['elig']: by[r['padd']][0] += mm
    for p in sorted(by, key=lambda x: str(x)):
        soy, tot = by[p]
        print(f"  {str(p):8s} {100*soy/tot if tot else 0:5.0f}% soy-eligible  ({soy:.0f}/{tot:.0f} mmgy)")
    print(f"  [excluded {excluded:.0f} mmgy as NULL-padd / non-operating / unlocated — master hygiene gap]")

    # ---- review list: every uncalibrated/ambiguous/unresolved facility -> for bespoke investigation ----
    review = [r for r in rows if r['uncal']]
    review.sort(key=lambda r: (r['tier'], -(float(r['mmgy']) if r['mmgy'] else 0)))
    review_path = ROOT / "docs" / "planning" / "ffa_feedstock_calibration_queue.md"
    lines = ["# FFA feedstock calibration queue",
             "",
             f"Facilities with an UNCALIBRATED assumed feedstock mix (auto-defaulted), needing Tore's "
             f"bespoke review. Generated by `scripts/build_facility_feedstock_eligibility.py`.",
             "",
             "- **TIER2_bd_soy** — generic biodiesel, assumed 100% soy oil until calibrated (eligible set kept broad).",
             "- **TIER1_ambiguous** — weak CARB name-match; confirm or reject the pathway link.",
             "- **UNRESOLVED** — no fuel_type/technology signal; needs identification.",
             "",
             "| tier | facility | company | state | padd | tech | fuel | mmgy | assumed_primary | eligible |",
             "|------|----------|---------|-------|------|------|------|-----:|-----------------|----------|"]
    for r in review:
        mm = f"{float(r['mmgy']):.0f}" if r['mmgy'] else ""
        lines.append(f"| {r['tier']} | {str(r['name'])[:34]} | {str(r['company'] or '')[:22]} | "
                     f"{r['state'] or ''} | {r['padd'] or ''} | {r['tech'] or ''} | "
                     f"{r['fuel'] or ''} | {mm} | {r['primary'] or ''} | {','.join(r['elig'])} |")
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n[REVIEW LIST] {len(review)} uncalibrated facilities -> {review_path.relative_to(ROOT)}")

    if args.write:
        with get_connection() as c:
            cur = c.cursor(); n = ov = 0
            for r in rows:
                if r['tier'] == 'TIER0_carb_override' and r['elig']:
                    cur.execute("UPDATE reference.biofuel_facilities SET eligible_feedstocks=%s, updated_at=now() "
                                "WHERE facility_id=%s", (r['elig'], r['fid']))      # CARB overwrites curated
                    ov += cur.rowcount
                elif r['tier'] in ('TIER1_pathway','TIER2_bd_soy','TIER2_rd_tech') and r['elig']:
                    cur.execute("UPDATE reference.biofuel_facilities SET eligible_feedstocks=%s, updated_at=now() "
                                "WHERE facility_id=%s AND eligible_feedstocks IS NULL", (r['elig'], r['fid']))
                    n += cur.rowcount
            c.commit()
        print(f"\n[WROTE] filled {n} NULL eligible_feedstocks + {ov} CARB overrides "
              f"(uncurated preserved where no strong match; ambiguous skipped for review)")
    else:
        print(f"\n[DRY-RUN] no DB writes. Review ambiguous + conflicts above, then re-run with --write.")

if __name__ == "__main__":
    main()
