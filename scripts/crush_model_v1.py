"""
Oilseed crush economic model — v1 (board crush, flat basis).

Per-facility soybean crush economics using CBOT board prices (no local basis yet),
aggregated to a NATIONAL crush marker. Headline output per RLC's primary market
(BBD feedstock): national SOYBEAN OIL production (feedstock supply into BBD).

Board crush margin ($/bu) = oil(¢/lb)*OIL_LB/100 + meal($/ton)*MEAL_LB/2000 - bean($/bu)
  CBOT board-crush yields: 11 lb crude soy oil + 44 lb 48% meal per 60-lb bushel.
Net margin = gross - CONVERSION_COST ($/bu cash opex).

Per facility:
  capacity_bu_yr (nameplate annual) -> crush_volume = capacity * UTILIZATION
  soy_oil_lb_yr  = crush_volume * OIL_LB        <- the BBD-feedstock headline
  soy_meal_st_yr = crush_volume * MEAL_LB / 2000
  gross_margin_usd_yr = crush_volume * board_margin_per_bu

v1 simplifications (documented, refine later): flat board prices (no local bean basis),
flat utilization, flat conversion cost. Per-facility basis is the v2 differentiator.
"""
import csv
import sys
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection

OUT = ROOT / "data" / "exports" / "crush_model_v1.csv"

# board-crush yields per 60-lb bushel
OIL_LB = 11.0
MEAL_LB = 44.0
CONVERSION_COST = 0.45      # $/bu cash operating cost (assumption)
UTILIZATION = 0.90          # nameplate -> actual run rate (assumption)
DAYS_YR = 365               # for tpd -> annual nameplate conversion


def latest_prices(cur):
    px = {}
    for sym in ("ZS", "ZL", "ZM"):
        cur.execute("""SELECT settlement FROM silver.futures_price
                       WHERE symbol=%s AND settlement IS NOT NULL
                       ORDER BY trade_date DESC, contract_month LIMIT 1""", (sym,))
        r = cur.fetchone()
        px[sym] = float(r["settlement"] if isinstance(r, dict) else r[0])
    return px  # ZS ¢/bu, ZL ¢/lb, ZM $/short ton


def board_margin(px):
    bean = px["ZS"] / 100.0           # ¢/bu -> $/bu
    oil_rev = px["ZL"] * OIL_LB / 100.0
    meal_rev = px["ZM"] * MEAL_LB / 2000.0
    gross = oil_rev + meal_rev - bean
    return {"bean_usd_bu": bean, "oil_rev": oil_rev, "meal_rev": meal_rev,
            "gross_bu": gross, "net_bu": gross - CONVERSION_COST}


def capacity_bu_yr(f):
    if f.get("nameplate_mmbu_yr"):
        return float(f["nameplate_mmbu_yr"]) * 1e6
    if f.get("nameplate_tpd"):
        return float(f["nameplate_tpd"]) * DAYS_YR * 2000.0 / 60.0
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--utilization", type=float, default=UTILIZATION)
    ap.add_argument("--conversion-cost", type=float, default=CONVERSION_COST)
    args = ap.parse_args()

    with get_connection() as c:
        cur = c.cursor()
        px = latest_prices(cur)
        bm = board_margin(px)
        cur.execute("""SELECT name, operator, state, status, nameplate_mmbu_yr, nameplate_tpd,
                              operating_days_year, refining_capability
                       FROM reference.oilseed_crush_facilities ORDER BY state, name""")
        facs = [dict(r) if isinstance(r, dict) else dict(zip(
            ['name','operator','state','status','nameplate_mmbu_yr','nameplate_tpd',
             'operating_days_year','refining_capability'], r)) for r in cur.fetchall()]

    rows = []
    nat = {"cap_bu": 0.0, "vol_bu": 0.0, "oil_lb": 0.0, "meal_st": 0.0, "gm_usd": 0.0,
           "n": 0, "n_cap": 0, "n_oper": 0}
    for f in facs:
        nat["n"] += 1
        operating = (f.get("status") or "").lower().startswith("oper")
        if operating:
            nat["n_oper"] += 1
        cap = capacity_bu_yr(f)
        rec = {"name": f["name"], "state": f.get("state"), "status": f.get("status"),
               "operating": operating, "capacity_bu_yr": cap}
        if cap and operating:
            vol = cap * args.utilization
            oil_lb = vol * OIL_LB
            meal_st = vol * MEAL_LB / 2000.0
            gm = vol * bm["net_bu"]
            rec.update({"crush_volume_bu_yr": round(vol), "soy_oil_lb_yr": round(oil_lb),
                        "soy_meal_st_yr": round(meal_st), "net_margin_bu": round(bm["net_bu"], 3),
                        "gross_margin_usd_yr": round(gm)})
            nat["cap_bu"] += cap; nat["vol_bu"] += vol; nat["oil_lb"] += oil_lb
            nat["meal_st"] += meal_st; nat["gm_usd"] += gm; nat["n_cap"] += 1
        rows.append(rec)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["name","state","status","operating","capacity_bu_yr","crush_volume_bu_yr",
                    "soy_oil_lb_yr","soy_meal_st_yr","net_margin_bu","gross_margin_usd_yr"])
        for r in rows:
            w.writerow([r["name"],r.get("state"),r.get("status"),r.get("operating"),
                        r.get("capacity_bu_yr"),r.get("crush_volume_bu_yr"),r.get("soy_oil_lb_yr"),
                        r.get("soy_meal_st_yr"),r.get("net_margin_bu"),r.get("gross_margin_usd_yr")])

    print("=== CRUSH MODEL v1 (board crush, flat basis) ===")
    print(f"prices: bean ${bm['bean_usd_bu']:.2f}/bu | oil {px['ZL']:.2f}c/lb | meal ${px['ZM']:.0f}/ton")
    print(f"board margin: gross ${bm['gross_bu']:.2f}/bu, net ${bm['net_bu']:.2f}/bu "
          f"(conv ${args.conversion_cost}/bu, util {args.utilization:.0%})")
    print(f"facilities: {nat['n']} total, {nat['n_oper']} operating, {nat['n_cap']} modeled (operating + capacity)")
    print("\nNATIONAL (modeled subset — operating facilities w/ capacity):")
    print(f"  crush capacity : {nat['cap_bu']/1e9:.2f} B bu/yr")
    print(f"  crush volume   : {nat['vol_bu']/1e9:.2f} B bu/yr")
    print(f"  SOY OIL output : {nat['oil_lb']/1e9:.2f} B lb/yr   <- BBD feedstock marker")
    print(f"  soy meal output: {nat['meal_st']/1e6:.2f} M short tons/yr")
    print(f"  gross margin   : ${nat['gm_usd']/1e9:.2f} B/yr")
    print("\nSANITY vs known US (analyst): crush ~2.4B bu/yr, capacity ~2.6B bu/yr, "
          "soy oil ~27B lb/yr.")
    print(f"  -> our modeled coverage: capacity {nat['cap_bu']/2.6e9:.0%} of national, "
          f"oil {nat['oil_lb']/27e9:.0%} of national.")
    print(f"\n-> {OUT}")


if __name__ == "__main__":
    main()
