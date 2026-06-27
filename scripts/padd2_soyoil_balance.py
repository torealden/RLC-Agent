"""
Crush-belt (PADD2) soybean oil regional balance sheet — v1.

First regional balance in the PADD-as-reconciliation-unit framework. Must sum back to the
national control total (national soy oil: crush supply ~29B, BBD demand ~12.5B, food/export
~16.5B lb). The crush belt is the soy oil SOURCE; this balance exposes how much it ships out
to the coastal/Gulf RD plants + coastal food markets — the core BBD-feedstock logistics signal.

SUPPLY = crush production (+ imports/begin stocks, ~0 for v1)
DEMAND = BBD (soy oil to biodiesel + RD in region) + food/industrial + NET SHIPMENTS OUT (balance)

HARD (from facilities): crush supply, RD capacity share. FLAGGED assumptions: biodiesel
geographic share (list lacks state; biodiesel is Midwest-concentrated), food share, BD/RD
national soy split. These are the tunable knobs; crush supply + net-out are the real signal.
"""
import sys, argparse
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]; sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

PADD2 = {'IL','IN','IA','KS','KY','MI','MN','MO','NE','ND','OH','OK','SD','TN','WI'}
UTIL, OIL_LB = 0.90, 11.0
# national control totals (B lb) from national hook + S&D
NAT_CRUSH_OIL = 29.0; NAT_BBD_SOY = 12.5; NAT_FOOD_EXPORT = 16.5
# national BD vs RD soy split (EIA-derived, scaled to 12.5) — FLAGGED
NAT_BD_SOY, NAT_RD_SOY = 7.0, 5.5
# FLAGGED regional shares
BD_PADD2_SHARE = 0.75    # biodiesel is Midwest-concentrated (IA #1, IL/MO/MN/IN/NE/OH) — list lacks state
FOOD_PADD2_SHARE = 0.30  # food/industrial soy oil ~ population/processing share

def pog(r,k,i):
    try: return r[k]
    except Exception: return r[i]

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--region", default="PADD2"); args = ap.parse_args()
    with get_connection() as c:
        cur = c.cursor()
        # SUPPLY: crush soy oil in PADD2 (hard, from facilities)
        cur.execute("""SELECT state, coalesce(nameplate_mmbu_yr*1e6, nameplate_tpd*365*2000/60.0) bu
                       FROM reference.oilseed_crush_facilities
                       WHERE status ILIKE '%oper%' AND (nameplate_mmbu_yr IS NOT NULL OR nameplate_tpd IS NOT NULL)""")
        nat_bu=p2_bu=0
        for r in cur.fetchall():
            st=pog(r,'state',0); bu=float(pog(r,'bu',1) or 0); nat_bu+=bu
            if st in PADD2: p2_bu+=bu
        p2_crush_oil = p2_bu*UTIL*OIL_LB/1e9          # B lb
        # RD capacity share in PADD2 (hard)
        cur.execute("SELECT state, nameplate_mmgy FROM reference.renewable_diesel_facilities WHERE status ILIKE '%oper%' AND nameplate_mmgy IS NOT NULL AND state IS NOT NULL")
        nat_rd=p2_rd=0
        for r in cur.fetchall():
            st=pog(r,'state',0); cap=float(pog(r,'nameplate_mmgy',1) or 0); nat_rd+=cap
            if st in PADD2: p2_rd+=cap
        rd_p2_share = p2_rd/nat_rd if nat_rd else 0

    # DEMAND (PADD2)
    bbd_bd  = NAT_BD_SOY * BD_PADD2_SHARE       # biodiesel soy oil in region
    bbd_rd  = NAT_RD_SOY * rd_p2_share          # RD soy oil in region (from capacity share)
    bbd_soy = bbd_bd + bbd_rd
    food    = NAT_FOOD_EXPORT * FOOD_PADD2_SHARE
    net_out = p2_crush_oil - bbd_soy - food     # balance: shipped out of crush belt

    print(f"=== CRUSH-BELT ({args.region}) SOYBEAN OIL BALANCE — v1 (B lb/yr) ===\n")
    print(f"SUPPLY")
    print(f"  Crush production              {p2_crush_oil:6.2f}   ({100*p2_bu/nat_bu:.0f}% of national crush oil) [HARD]")
    print(f"  Imports / beginning stocks      0.00   [v1 ~0]")
    print(f"  {'TOTAL SUPPLY':28s}  {p2_crush_oil:6.2f}\n")
    print(f"DEMAND")
    print(f"  BBD — biodiesel               {bbd_bd:6.2f}   [{BD_PADD2_SHARE:.0%} of nat BD soy {NAT_BD_SOY} — FLAGGED, list lacks state]")
    print(f"  BBD — renewable diesel        {bbd_rd:6.2f}   [{rd_p2_share:.0%} of nat RD soy {NAT_RD_SOY} — from RD capacity share, HARD]")
    print(f"  Food / industrial             {food:6.2f}   [{FOOD_PADD2_SHARE:.0%} of nat food/export — FLAGGED]")
    print(f"  >> NET SHIPMENTS OUT          {net_out:6.2f}   [balance -> to coastal/Gulf RD + coastal food + export]")
    print(f"  {'TOTAL DEMAND':28s}  {bbd_soy+food+net_out:6.2f}\n")
    print(f"HEADLINE: the crush belt produces {p2_crush_oil:.1f}B lb soy oil, consumes {bbd_soy+food:.1f}B in-region")
    print(f"  (BBD {bbd_soy:.1f} + food {food:.1f}), and SHIPS OUT {net_out:.1f}B lb/yr -> that outflow IS the")
    print(f"  feedstock supply for coastal/Gulf RD plants (PADD2 has only {rd_p2_share:.0%} of US RD capacity).")
    print(f"\nRECONCILE TO NATIONAL: PADD2 crush {p2_crush_oil:.1f}B of {NAT_CRUSH_OIL}B ({100*p2_crush_oil/NAT_CRUSH_OIL:.0f}%); "
          f"other PADDs supply the rest. PADD2 BBD soy {bbd_soy:.1f}B of national {NAT_BBD_SOY}B. "
          f"Net-out {net_out:.1f}B must land as other-PADD net-IN (next: build PADD3/5).")

if __name__ == "__main__":
    main()
