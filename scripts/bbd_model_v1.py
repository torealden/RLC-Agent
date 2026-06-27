"""
Biomass-based diesel (BBD) vertical — v1.  RLC's PRIMARY market.

Mirrors crush v1: national production (EMTS = the anchor, like S&D was for crush) ->
feedstock demand -> per-facility capacity/utilization. The headline connection:
crush SOY OIL SUPPLY (from crush_model_v1) feeds BBD SOY OIL DEMAND -> soybean oil balance.

BBD = biodiesel (FAME) + renewable diesel (HEFA) + SAF + co-processing.
Feedstock conversion ~7.5 lb feedstock per gallon (FAME ~7.35, HEFA ~7.5-7.7) — v1 flat.
Feedstock demand + mix come from gold.feedstock_allocation_national (actual allocation),
cross-checked against production x 7.5 lb/gal.
"""
import sys, argparse
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

LB_PER_GAL = 7.5            # feedstock lb per gallon BBD (flat v1)
OIL_LB_PER_BU = 11.0        # crush soy oil yield (matches crush_model_v1)
CRUSH_UTIL_SD = 2630.0      # mil bu, 2025/26 RLC S&D (crush soy oil supply anchor)


def trailing12(cur, table, datecol):
    cur.execute(f"SELECT max({datecol}) FROM {table}")
    mx = cur.fetchone(); mx = mx[0] if not isinstance(mx, dict) else list(mx.values())[0]
    return mx


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lb-per-gal", type=float, default=LB_PER_GAL)
    args = ap.parse_args()
    with get_connection() as c:
        cur = c.cursor()
        # 1. national BBD production, trailing 12 mo (EMTS), kgal -> mil gal
        cur.execute("""SELECT
              sum(biodiesel_kgal)/1000.0 bd, sum(renewable_diesel_kgal)/1000.0 rd,
              sum(coalesce(saf_kgal,0))/1000.0 saf, sum(coalesce(co_processing_kgal,0))/1000.0 cop
            FROM gold.us_liquid_fuel_production_monthly
            WHERE period_date > (SELECT max(period_date) FROM gold.us_liquid_fuel_production_monthly) - interval '12 months'""")
        p = cur.fetchone(); p = dict(p) if isinstance(p, dict) else dict(zip(['bd','rd','saf','cop'], p))
        prod = {k: float(v or 0) for k, v in p.items()}
        bbd_total = sum(prod.values())

        # 2. feedstock demand + mix from allocation, trailing 12 mo (mil lbs)
        cur.execute("""SELECT feedstock_name, sum(total_mil_lbs) lbs
            FROM gold.feedstock_allocation_national
            WHERE scenario='base' AND fuel_type IN ('biodiesel','renewable_diesel','saf')
              AND period > (SELECT max(period) FROM gold.feedstock_allocation_national) - interval '12 months'
            GROUP BY feedstock_name ORDER BY 2 DESC""")
        feed = [(r[0], float(r[1] or 0)) if not isinstance(r, dict) else (r['feedstock_name'], float(r['lbs'] or 0)) for r in cur.fetchall()]
        feed_total = sum(l for _, l in feed)
        soy_oil_demand = next((l for n, l in feed if 'soy' in (n or '').lower()), 0.0)

        # 3. crush soy oil SUPPLY (national, from crush logic)
        cur.execute("""SELECT sum(coalesce(nameplate_mmbu_yr*1e6, nameplate_tpd*365*2000/60.0)) cap
            FROM reference.oilseed_crush_facilities WHERE status ILIKE '%oper%'
              AND (nameplate_mmbu_yr IS NOT NULL OR nameplate_tpd IS NOT NULL)""")
        cap = cur.fetchone(); cap = float((cap[0] if not isinstance(cap, dict) else cap['cap']) or 0)
        crush_util = min(CRUSH_UTIL_SD * 1e6 / cap, 1.0) if cap else 0.9
        crush_vol = cap * crush_util
        crush_soy_oil = crush_vol * OIL_LB_PER_BU / 1e6   # mil lbs

        # 4. per-facility BBD capacity/utilization
        rd_cap = bd_cap = 0.0; rd_n = bd_n = 0
        cur.execute("SELECT sum(nameplate_mmgy) cap, count(nameplate_mmgy) n FROM reference.renewable_diesel_facilities WHERE status ILIKE '%oper%'")
        r = cur.fetchone(); rd_cap = float((r[0] if not isinstance(r, dict) else r['cap']) or 0); rd_n = (r[1] if not isinstance(r, dict) else r['n'])
        cur.execute("SELECT sum(nameplate_mmgy) cap, count(nameplate_mmgy) n FROM reference.biodiesel_facilities WHERE status ILIKE '%oper%'")
        r = cur.fetchone(); bd_cap = float((r[0] if not isinstance(r, dict) else r['cap']) or 0); bd_n = (r[1] if not isinstance(r, dict) else r['n'])

    # report
    print("=== BBD VERTICAL v1 (RLC primary market) ===")
    print(f"\nNATIONAL BBD PRODUCTION (trailing 12mo, EMTS):")
    print(f"  renewable diesel : {prod['rd']/1000:.2f} B gal")
    print(f"  biodiesel        : {prod['bd']/1000:.2f} B gal")
    print(f"  SAF              : {prod['saf']/1000:.3f} B gal")
    print(f"  co-processing    : {prod['cop']/1000:.3f} B gal")
    print(f"  TOTAL BBD        : {bbd_total/1000:.2f} B gal")
    print(f"\nCAPACITY (operating nameplate): RD {rd_cap/1000:.2f} B gal ({rd_n} fac), "
          f"BD {bd_cap/1000:.2f} B gal ({bd_n} fac)")
    if (rd_cap+bd_cap): print(f"  implied BBD utilization: {bbd_total/(rd_cap+bd_cap):.0%}")
    print(f"\nFEEDSTOCK DEMAND (trailing 12mo, allocation): {feed_total/1000:.2f} B lb total")
    print(f"  vs production x {args.lb_per_gal} lb/gal = {bbd_total*args.lb_per_gal/1000:.2f} B lb (consistency check)")
    print("  mix:")
    for n, l in feed[:8]:
        print(f"    {n:22s} {l/1000:6.2f} B lb  ({100*l/feed_total:4.1f}%)")
    print(f"\n*** SOYBEAN OIL BALANCE (the BBD-feedstock connection) ***")
    print(f"  crush SOY OIL SUPPLY (national):  {crush_soy_oil/1000:.2f} B lb/yr  (crush {crush_vol/1e9:.2f}B bu x {OIL_LB_PER_BU} lb, util {crush_util:.0%})")
    print(f"  BBD SOY OIL DEMAND:               {soy_oil_demand/1000:.2f} B lb/yr  ({100*soy_oil_demand/crush_soy_oil:.0f}% of crush oil)")
    print(f"  -> available for food/export:     {(crush_soy_oil-soy_oil_demand)/1000:.2f} B lb/yr")
    print(f"\n  Soy oil = {100*soy_oil_demand/feed_total:.0f}% of BBD feedstock; BBD consumes "
          f"{100*soy_oil_demand/crush_soy_oil:.0f}% of US crush soy oil output.")


if __name__ == "__main__":
    main()
