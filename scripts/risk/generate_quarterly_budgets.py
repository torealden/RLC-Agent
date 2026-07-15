"""Quarterly VaR risk-budget generator for BBD facilities.

For each active biofuel facility and a target quarter, produce a concrete procurement
instruction — pounds per feedstock — via the covered/open VaR optimizer. Writes
risk.facility_quarterly_budget (the plan). The post-allocation override layer
(apply_risk_budget_to_allocation.py) then rewrites each facility's feedstock mix to match.

Prices are pulled AS-OF the target quarter (silver.feedstock_supply, that quarter's months),
so historical budgets use historical prices. Vol/corr are static (full-history estimate).
Coverage: config override, else the forward ladder for future quarters, else a realized
0.80 for past/current quarters (no "quarters ahead" for the past).

Usage:
  python scripts/risk/generate_quarterly_budgets.py            # next quarter
  python scripts/risk/generate_quarterly_budgets.py 2026Q4     # one quarter
  python scripts/risk/generate_quarterly_budgets.py --backfill 2010  # all quarters 2010..next
"""
import json
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection
from src.engines.risk_budget.var_optimizer import optimize_quarter

LPG = {'biodiesel': 7.60, 'renewable_diesel': 8.60, 'saf': 8.60, 'coprocessing': 7.60}
COVERAGE_LADDER = {0: 0.80, 1: 0.55, 2: 0.30, 3: 0.10}
REALIZED_COVERAGE = 0.80   # past/current quarters: treat as procured
DEFAULT_UTIL = 0.85
CI_BONUS = {'SBO': 0.00, 'CO': 0.05, 'DCO': 0.15, 'BFT': 0.20, 'EBFT': 0.20, 'IBFT': 0.20,
            'UCO': 0.30, 'YG': 0.25, 'CWG': 0.18, 'PF': 0.15, 'CSO': 0.02}
BASE_FUEL_REV = 4.50
NAME2CODE = {'SBO': 'SBO', 'SOY': 'SBO', 'SOYBEAN': 'SBO', 'TALLOW': 'BFT', 'BEEF': 'BFT',
             'CANOLA': 'CO', 'CAN': 'CO', 'CO': 'CO', 'DCO': 'DCO', 'CORN': 'DCO',
             'UCO': 'UCO', 'YG': 'YG', 'GREASE': 'YG', 'CWG': 'CWG', 'WHITE': 'CWG',
             'PF': 'PF', 'POULTRY': 'PF'}


def parse_mix(text, eligible):
    if text:
        found = {}
        for pct, name in re.findall(r'(\d+)\s*%\s*([A-Za-z]+)', text):
            code = NAME2CODE.get(name.upper())
            if code and (not eligible or code in eligible):
                found[code] = found.get(code, 0) + float(pct) / 100.0
        if found:
            tot = sum(found.values())
            return {k: v / tot for k, v in found.items()}
    if eligible:
        return {c: 1.0 / len(eligible) for c in eligible}
    return {}


def quarter_prices(cur, ty, tq):
    """Avg feedstock price over the target quarter's 3 months (silver.feedstock_supply)."""
    m0 = (tq - 1) * 3 + 1
    start = date(ty, m0, 1)
    end = date(ty + (1 if m0 + 3 > 12 else 0), (m0 + 3 - 1) % 12 + 1, 1)
    cur.execute("""SELECT feedstock_code, avg(avg_price_per_lb) p FROM silver.feedstock_supply
                   WHERE period >= %s AND period < %s AND avg_price_per_lb > 0 GROUP BY 1""",
                (start, end))
    px = {x['feedstock_code']: float(x['p']) for x in cur.fetchall()}
    if not px:  # early quarter with no priced feedstock -> nearest prior month
        cur.execute("""SELECT DISTINCT ON (feedstock_code) feedstock_code, avg_price_per_lb p
                       FROM silver.feedstock_supply WHERE period < %s AND avg_price_per_lb > 0
                       ORDER BY feedstock_code, period DESC""", (end,))
        px = {x['feedstock_code']: float(x['p']) for x in cur.fetchall()}
    for g in ('EBFT', 'IBFT'):
        px.setdefault(g, px.get('BFT', 0.45))
    px.setdefault('YG', px.get('UCO', 0.40))
    return px


def generate_for_quarter(cur, ty, tq, cy, cq, vol, corr, facs):
    qstr = f"{ty}Q{tq}"
    qa = (ty - cy) * 4 + (tq - cq)
    px = quarter_prices(cur, ty, tq)
    cur.execute("DELETE FROM risk.facility_quarterly_budget WHERE quarter=%s", (qstr,))
    written, coproc = 0, []
    for f in facs:
        ft = f['fuel_type']
        elig = list(f['eligible_feedstocks'] or [])
        if 'BFT' in elig:
            elig += [g for g in ('EBFT', 'IBFT') if g not in elig]
        elig = [c for c in elig if c in px and c in vol]
        if not elig:
            continue
        lpg = {c: LPG.get(ft, 8.0) for c in elig}
        mgn = {c: BASE_FUEL_REV - px[c] * lpg[c] + CI_BONUS.get(c, 0.0) for c in elig}
        anchor = parse_mix(f['feedstock_mix'], elig)
        cov = f['coverage_override_pct']
        cov = float(cov) if cov is not None else (
            COVERAGE_LADDER.get(qa, 0.30) if qa > 0 else REALIZED_COVERAGE)
        bpct = float(f['var_budget_pct']) if f['var_budget_pct'] is not None else 0.08
        need_gal = float(f['nameplate_mmgy']) * 1e6 * DEFAULT_UTIL / 4.0
        try:
            plan = optimize_quarter(elig, need_gal, px, lpg, mgn, vol, corr,
                                    anchor_shares=anchor, coverage=cov, budget_pct=bpct)
        except ValueError:
            continue
        cur.execute("""INSERT INTO risk.facility_quarterly_budget
            (facility_id, quarter, fuel_type, need_gallons, coverage_pct, budget_pct,
             buy_by_feedstock, var_dollars, notional_dollars, var_ratio, margin_dollars,
             feasible, anchor_mix) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (facility_id, quarter, fuel_type) DO UPDATE SET
             buy_by_feedstock=EXCLUDED.buy_by_feedstock, var_dollars=EXCLUDED.var_dollars,
             coverage_pct=EXCLUDED.coverage_pct, generated_at=now()""",
            (f['facility_id'], qstr, ft, need_gal, cov, bpct,
             json.dumps({k: round(v, 0) for k, v in plan.lbs_by_feedstock.items() if v > 1e4}),
             plan.var_dollars, plan.notional_dollars, plan.var_ratio, plan.margin_dollars,
             plan.feasible, json.dumps(anchor)))
        written += 1
        if ft == 'coprocessing':
            coproc.append((f['facility_name'], plan))
    return qstr, qa, written, coproc


def main():
    args = sys.argv[1:]
    backfill_from = None
    target = None
    if args and args[0] == '--backfill':
        backfill_from = int(args[1])
    elif args:
        target = args[0]

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS risk.facility_quarterly_budget (
            facility_id int, quarter text, fuel_type text,
            need_gallons numeric, coverage_pct numeric, budget_pct numeric,
            buy_by_feedstock jsonb, var_dollars numeric, notional_dollars numeric,
            var_ratio numeric, margin_dollars numeric, feasible text, anchor_mix jsonb,
            generated_at timestamptz DEFAULT now(),
            PRIMARY KEY (facility_id, quarter, fuel_type))""")
        cur.execute("""CREATE TABLE IF NOT EXISTS risk.facility_coverage_actual (
            facility_id int, period date, fuel_type text,
            committed_mil_lbs numeric, need_mil_lbs numeric, realized_coverage_pct numeric,
            realized_mix jsonb, var_open_dollars numeric, updated_at timestamptz DEFAULT now(),
            PRIMARY KEY (facility_id, period, fuel_type))""")

        cur.execute("SELECT extract(year from current_date)::int y, extract(quarter from current_date)::int q")
        r = cur.fetchone(); cy, cq = int(r['y']), int(r['q'])

        cur.execute("SELECT feedstock_code, ann_vol FROM risk.feedstock_volatility")
        vol = {x['feedstock_code']: float(x['ann_vol']) for x in cur.fetchall()}
        cur.execute("SELECT feedstock_a, feedstock_b, corr FROM risk.feedstock_correlation")
        corr = {(x['feedstock_a'], x['feedstock_b']): float(x['corr']) for x in cur.fetchall()}
        cur.execute("""SELECT f.facility_id, f.facility_name, f.fuel_type, f.technology,
                         f.eligible_feedstocks, f.feedstock_mix, h.nameplate_mmgy,
                         cfg.var_budget_pct, cfg.coverage_override_pct
                       FROM reference.biofuel_facilities f
                       JOIN LATERAL (SELECT nameplate_mmgy, status FROM reference.facility_capacity_history hh
                                     WHERE hh.facility_id=f.facility_id ORDER BY effective_date DESC LIMIT 1) h ON true
                       LEFT JOIN risk.facility_budget_config cfg ON cfg.facility_id=f.facility_id
                       WHERE f.fuel_type IN ('biodiesel','renewable_diesel','saf','coprocessing')
                         AND h.status='operating' AND h.nameplate_mmgy>0""")
        facs = cur.fetchall()

        # quarter list
        if backfill_from:
            quarters = []
            y, q = backfill_from, 1
            end_y, end_q = (cy, cq + 1) if cq < 4 else (cy + 1, 1)
            while (y, q) <= (end_y, end_q):
                quarters.append((y, q)); q += 1
                if q > 4: q = 1; y += 1
        elif target:
            quarters = [(int(target[:4]), int(target[5]))]
        else:
            ty, tq = (cy, cq + 1) if cq < 4 else (cy + 1, 1)
            quarters = [(ty, tq)]

        total = 0
        for (ty, tq) in quarters:
            qstr, qa, n, coproc = generate_for_quarter(cur, ty, tq, cy, cq, vol, corr, facs)
            conn.commit()
            total += n
            if len(quarters) == 1:
                print(f"Generated {n} budgets for {qstr} (quarters-ahead {qa})\n")
                print("CO-PROCESSING facilities:")
                for name, p in coproc:
                    print(f"  {str(name)[:34]:34s} [{p.feasible}] VaR ${p.var_dollars/1e6:.1f}M "
                          f"buy: " + ", ".join(f"{c} {p.lbs_by_feedstock[c]/1e6:.0f}M"
                                               for c in p.feedstocks if p.lbs_by_feedstock[c] > 1e5))
            else:
                print(f"  {qstr}: {n} budgets")
        if len(quarters) > 1:
            print(f"\nBackfilled {len(quarters)} quarters, {total} total facility-budgets.")


if __name__ == "__main__":
    main()
