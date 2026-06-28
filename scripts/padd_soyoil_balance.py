"""
Regional (PADD) soybean oil balance — v1, full reconciliation.

Extends the crush-belt (PADD2) balance to ALL five PADDs and proves the framework's
consistency test: NET SHIPMENTS ACROSS PADDS SUM TO ZERO. Every lb of soy oil shipped
OUT of the crush belt must land as net-IN somewhere — that flow IS the BBD-feedstock
logistics map.

Requested deliverable: PADD3 (Gulf) and PADD5 (West Coast) — the two RD-heavy DEMAND
regions (84% of US RD capacity between them) with almost no crush. They are the soy oil
SINKS that absorb PADD2's ~15B lb net-out.

HARD (from facilities, queried live):
    - crush soy oil supply per PADD       (reference.oilseed_crush_facilities)
    - RD soy feedstock per PADD            (reference.renewable_diesel_facilities capacity share)
FLAGGED (tunable knobs, list lacks state for these):
    - biodiesel geographic share          (BD is Midwest-concentrated)
    - food/industrial + export share      (population + export-port weighted)
    - national BD vs RD soy split

National control totals (from bbd_national_feedstock + S&D), B lb/yr:
    crush oil supply 29.0 | BBD soy 12.5 (BD 7.0 + RD 5.5) | food/ind/export 16.5
"""
import sys, argparse
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

PADD = {
    'PADD1': {'CT','ME','MA','NH','RI','VT','NY','NJ','PA','DE','MD','DC','WV','VA','NC','SC','GA','FL'},
    'PADD2': {'IL','IN','IA','KS','KY','MI','MN','MO','NE','ND','OH','OK','SD','TN','WI'},
    'PADD3': {'AL','AR','LA','MS','NM','TX'},
    'PADD4': {'CO','ID','MT','UT','WY'},
    'PADD5': {'AK','AZ','CA','HI','NV','OR','WA'},
}
PADD_NAME = {'PADD1':'East Coast','PADD2':'Crush Belt','PADD3':'Gulf','PADD4':'Rockies','PADD5':'West Coast'}
UTIL, OIL_LB = 0.90, 11.0

# National control totals (B lb/yr)
NAT_CRUSH_OIL = 29.0
NAT_BD_SOY, NAT_RD_SOY = 7.0, 5.5          # national BD vs RD soy split — FLAGGED
NAT_BBD_SOY = NAT_BD_SOY + NAT_RD_SOY      # 12.5
NAT_FOOD_EXPORT = 16.5
# net-ship checksum target = 29.0 - 12.5 - 16.5 = 0.0 by construction

# FLAGGED share dicts (must each sum to ~1.0). RD share is HARD (from capacity) — not here.
BD_SHARE   = {'PADD1':0.04,'PADD2':0.75,'PADD3':0.10,'PADD4':0.03,'PADD5':0.08}  # BD Midwest-concentrated
FOOD_SHARE = {'PADD1':0.22,'PADD2':0.30,'PADD3':0.25,'PADD4':0.03,'PADD5':0.20}  # pop + Gulf/PNW export ports

def padd_of(st):
    for p, s in PADD.items():
        if st in s: return p
    return 'UNK'

def g(r, k, i):
    try: return r[k]
    except Exception: return r[i]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--detail", nargs="*", default=["PADD3","PADD5"],
                    help="PADDs to print full balance for (default: PADD3 PADD5)")
    args = ap.parse_args()

    with get_connection() as c:
        cur = c.cursor()
        # HARD: crush soy oil supply per PADD
        cur.execute("""SELECT state, coalesce(nameplate_mmbu_yr*1e6, nameplate_tpd*365*2000/60.0) bu
                       FROM reference.oilseed_crush_facilities
                       WHERE status ILIKE '%oper%' AND (nameplate_mmbu_yr IS NOT NULL OR nameplate_tpd IS NOT NULL)""")
        crush_oil = {p: 0.0 for p in PADD}
        for r in cur.fetchall():
            p = padd_of(g(r,'state',0))
            if p in crush_oil: crush_oil[p] += float(g(r,'bu',1) or 0) * UTIL * OIL_LB / 1e9
        # HARD: RD capacity share per PADD
        cur.execute("""SELECT state, nameplate_mmgy FROM reference.renewable_diesel_facilities
                       WHERE status ILIKE '%oper%' AND nameplate_mmgy IS NOT NULL AND state IS NOT NULL""")
        rd_cap = {p: 0.0 for p in PADD}; rd_tot = 0.0
        for r in cur.fetchall():
            cap = float(g(r,'nameplate_mmgy',1) or 0); rd_tot += cap
            p = padd_of(g(r,'state',0))
            if p in rd_cap: rd_cap[p] += cap

    # Build per-PADD balance
    rows = {}
    for p in PADD:
        rd_share = rd_cap[p] / rd_tot if rd_tot else 0
        bd  = NAT_BD_SOY * BD_SHARE.get(p, 0)
        rd  = NAT_RD_SOY * rd_share
        food = NAT_FOOD_EXPORT * FOOD_SHARE.get(p, 0)
        supply = crush_oil[p]
        net_out = supply - bd - rd - food         # + = ships OUT, - = net IN
        rows[p] = dict(supply=supply, bd=bd, rd=rd, food=food, net_out=net_out, rd_share=rd_share)

    # ---- Detailed balances for requested PADDs ----
    for p in args.detail:
        d = rows[p]
        net_in = -d['net_out']
        print(f"=== {p} ({PADD_NAME[p]}) SOYBEAN OIL BALANCE — v1 (B lb/yr) ===\n")
        print("SUPPLY")
        print(f"  Crush production              {d['supply']:6.2f}   [HARD, {100*d['supply']/NAT_CRUSH_OIL:.0f}% of national crush oil]")
        print(f"  >> NET SHIPMENTS IN           {net_in:6.2f}   [balance <- imported from crush belt (PADD2) / ports]")
        print(f"  {'TOTAL SUPPLY':28s}  {d['supply']+net_in:6.2f}\n")
        print("DEMAND")
        print(f"  BBD — renewable diesel        {d['rd']:6.2f}   [{d['rd_share']:.0%} of nat RD soy {NAT_RD_SOY} — RD capacity share, HARD]")
        print(f"  BBD — biodiesel               {d['bd']:6.2f}   [{BD_SHARE.get(p,0):.0%} of nat BD soy {NAT_BD_SOY} — FLAGGED]")
        print(f"  Food / industrial / export    {d['food']:6.2f}   [{FOOD_SHARE.get(p,0):.0%} of nat food/export — FLAGGED]")
        print(f"  {'TOTAL DEMAND':28s}  {d['rd']+d['bd']+d['food']:6.2f}\n")
        print(f"HEADLINE: {PADD_NAME[p]} crushes only {d['supply']:.1f}B lb soy oil but consumes "
              f"{d['rd']+d['bd']+d['food']:.1f}B (RD {d['rd']:.1f} + BD {d['bd']:.1f} + food {d['food']:.1f}),")
        print(f"  so it must IMPORT {net_in:.1f}B lb/yr. It holds {d['rd_share']:.0%} of US RD capacity on "
              f"~{100*d['supply']/NAT_CRUSH_OIL:.0f}% of US crush -> structural feedstock-deficit region.\n")

    # ---- National reconciliation: net-ship must sum to ~0 ----
    print("=== NATIONAL RECONCILIATION — net shipments sum to zero (B lb/yr) ===\n")
    print(f"  {'PADD':14s} {'crush':>7} {'BD':>6} {'RD':>6} {'food':>6} {'net-ship':>9}")
    tot = dict(supply=0,bd=0,rd=0,food=0,net_out=0)
    for p in ['PADD1','PADD2','PADD3','PADD4','PADD5']:
        d = rows[p]
        for k in tot: tot[k] += d[k]
        flag = 'OUT' if d['net_out'] > 0 else 'in '
        print(f"  {p} {PADD_NAME[p]:8s} {d['supply']:7.2f} {d['bd']:6.2f} {d['rd']:6.2f} {d['food']:6.2f} "
              f"{d['net_out']:+8.2f} {flag}")
    print(f"  {'-'*52}")
    print(f"  {'NATIONAL':14s} {tot['supply']:7.2f} {tot['bd']:6.2f} {tot['rd']:6.2f} {tot['food']:6.2f} "
          f"{tot['net_out']:+8.2f}")
    print(f"\n  control totals: crush {NAT_CRUSH_OIL} | BD {NAT_BD_SOY} | RD {NAT_RD_SOY} | food/exp {NAT_FOOD_EXPORT}")
    chk = abs(tot['net_out'])
    print(f"  net-ship checksum: {tot['net_out']:+.2f}B  -> {'BALANCED (sums to ~0)' if chk < 0.5 else 'IMBALANCE - shares need retuning'}")
    out = sum(d['net_out'] for d in rows.values() if d['net_out'] > 0)
    print(f"\n  HEADLINE: crush belt ships OUT {rows['PADD2']['net_out']:.1f}B lb; absorbed as net-IN by "
          f"Gulf {-rows['PADD3']['net_out']:.1f} + West Coast {-rows['PADD5']['net_out']:.1f} + "
          f"East {-rows['PADD1']['net_out']:.1f} + Rockies {-rows['PADD4']['net_out']:.1f}.")
    print(f"  That {out:.0f}B lb interregional flow is the soy-oil feedstock-logistics signal.")

if __name__ == "__main__":
    main()
