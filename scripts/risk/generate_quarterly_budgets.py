"""Quarterly VaR risk-budget generator for BBD facilities.

For each active biofuel facility and a target quarter, produce a concrete procurement
instruction — pounds per feedstock — via the covered/open VaR optimizer. Writes
risk.facility_quarterly_budget (the plan) and creates risk.facility_coverage_actual
(the live coverage-vs-budget tracking ledger, populated by the accounting step).

Inputs:
  - need_gallons  = nameplate_mmgy * utilization / 4        (quarterly production)
  - eligible      = facility eligible_feedstocks (BFT expanded to EBFT/IBFT)
  - anchor mix    = parsed from reference.biofuel_facilities.feedstock_mix
  - coverage      = risk.facility_budget_config.coverage_override_pct, else the default ladder
  - budget_pct    = risk.facility_budget_config.var_budget_pct
  - vol/corr      = risk.feedstock_* ; prices = silver.feedstock_supply (latest quarter avg)
  - margin proxy  = fuel_rev - feedstock_cost + CI credit bonus   [V1 PROXY, flagged;
                    the open-leg tilt only needs relative economics — see MARGIN note]

SAF and coprocessing are generated as their own fuel_type budgets (kept separate per ruling).
Usage: python scripts/risk/generate_quarterly_budgets.py [YYYYQn]   (default: next quarter)
"""
import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
from src.services.database.db_config import get_connection
from src.engines.risk_budget.var_optimizer import optimize_quarter

# per-fuel-type blended feedstock yield (lb feedstock / gal fuel) — from bbd_national_feedstock
LPG = {'biodiesel': 7.60, 'renewable_diesel': 8.60, 'saf': 8.60, 'coprocessing': 7.60}
# default coverage ladder by quarters-ahead (overridable per facility via config)
COVERAGE_LADDER = {0: 0.80, 1: 0.55, 2: 0.30, 3: 0.10}
DEFAULT_UTIL = 0.85
# V1 MARGIN PROXY: relative CI credit advantage ($/gal) of lower-CI feedstocks. The open-leg
# argmax only needs relative economics (fuel revenue is common across a facility's feedstocks),
# so cost + this CI tilt is enough to order them. Replace with the IFV credit engine later.
CI_BONUS = {'SBO': 0.00, 'CO': 0.05, 'DCO': 0.15, 'BFT': 0.20, 'EBFT': 0.20, 'IBFT': 0.20,
            'UCO': 0.30, 'YG': 0.25, 'CWG': 0.18, 'PF': 0.15, 'CSO': 0.02}
BASE_FUEL_REV = 4.50   # $/gal nominal (cancels in open-leg ordering; keeps margin $ positive)

# feedstock_mix free-text name -> allocator code
NAME2CODE = {'SBO': 'SBO', 'SOY': 'SBO', 'SOYBEAN': 'SBO', 'TALLOW': 'BFT', 'BEEF': 'BFT',
             'CANOLA': 'CO', 'CAN': 'CO', 'CO': 'CO', 'DCO': 'DCO', 'CORN': 'DCO',
             'UCO': 'UCO', 'YG': 'YG', 'GREASE': 'YG', 'CWG': 'CWG', 'WHITE': 'CWG',
             'PF': 'PF', 'POULTRY': 'PF'}


def parse_mix(text, eligible):
    """Parse 'X% SBO - Y% Tallow' -> {code: share}. Fallback: uniform over eligible."""
    if text:
        found = {}
        for pct, name in re.findall(r'(\d+)\s*%\s*([A-Za-z]+)', text):
            code = NAME2CODE.get(name.upper())
            if code and (not eligible or code in eligible):
                found[code] = found.get(code, 0) + float(pct) / 100.0
        if found and abs(sum(found.values()) - 1.0) < 0.5:
            tot = sum(found.values())
            return {k: v / tot for k, v in found.items()}
    if eligible:
        return {c: 1.0 / len(eligible) for c in eligible}
    return {}


def quarter_bounds(q):
    y, n = q
    m0 = (n - 1) * 3 + 1
    return date(y, m0, 1)


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else None
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

        # target quarter (default: next quarter from today = quarters_ahead 1)
        cur.execute("SELECT extract(year from current_date)::int y, extract(quarter from current_date)::int q")
        r = cur.fetchone(); cy, cq = int(r['y']), int(r['q'])
        if target:
            ty, tq = int(target[:4]), int(target[5])
        else:
            tq = cq + 1; ty = cy
            if tq > 4: tq = 1; ty += 1
        qstr = f"{ty}Q{tq}"
        qa = (ty - cy) * 4 + (tq - cq)   # quarters ahead

        # vol / corr / prices
        cur.execute("SELECT feedstock_code, ann_vol FROM risk.feedstock_volatility")
        vol = {x['feedstock_code']: float(x['ann_vol']) for x in cur.fetchall()}
        cur.execute("SELECT feedstock_a, feedstock_b, corr FROM risk.feedstock_correlation")
        corr = {(x['feedstock_a'], x['feedstock_b']): float(x['corr']) for x in cur.fetchall()}
        cur.execute("""SELECT feedstock_code, avg(avg_price_per_lb) p FROM silver.feedstock_supply
                       WHERE period >= (SELECT max(period) FROM silver.feedstock_supply) - interval '3 months'
                       GROUP BY 1""")
        px = {x['feedstock_code']: float(x['p']) for x in cur.fetchall()}
        # grade/proxy prices
        for g in ('EBFT', 'IBFT'): px.setdefault(g, px.get('BFT', 0.45))
        px.setdefault('YG', px.get('UCO', 0.40))

        # facilities + coverage/budget config
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

        written, coproc_rows = 0, []
        cur.execute("DELETE FROM risk.facility_quarterly_budget WHERE quarter=%s", (qstr,))
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
            cov = float(cov) if cov is not None else COVERAGE_LADDER.get(qa, 0.30)
            bpct = float(f['var_budget_pct']) if f['var_budget_pct'] is not None else 0.08
            need_gal = float(f['nameplate_mmgy']) * 1e6 * DEFAULT_UTIL / 4.0
            try:
                plan = optimize_quarter(elig, need_gal, px, lpg, mgn, vol, corr,
                                        anchor_shares=anchor, coverage=cov, budget_pct=bpct)
            except ValueError:
                continue
            import json
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
                coproc_rows.append((f['facility_name'], cov, plan))
        conn.commit()

        print(f"Generated {written} facility budgets for {qstr} (quarters-ahead {qa}, "
              f"coverage default {COVERAGE_LADDER.get(qa, 0.30):.0%})\n")
        print("CO-PROCESSING facilities (the ones that were broken — SBO should now appear):")
        for name, cov, p in coproc_rows:
            mix = ", ".join(f"{c} {p.shares[c]*100:.0f}%" for c in p.feedstocks if p.shares[c] > 0.01)
            print(f"  {str(name)[:34]:34s} cov={cov:.0%} [{p.feasible}] VaR ${p.var_dollars/1e6:.1f}M")
            print(f"     buy: " + ", ".join(f"{c} {p.lbs_by_feedstock[c]/1e6:.0f}M" for c in p.feedstocks
                                            if p.lbs_by_feedstock[c] > 1e5))
        cur.execute("""SELECT fuel_type, count(*) n, round(sum(var_dollars)/1e6,1) var_m
                       FROM risk.facility_quarterly_budget WHERE quarter=%s GROUP BY 1 ORDER BY 1""", (qstr,))
        print("\nBy fuel type:")
        for x in cur.fetchall():
            print(f"  {x['fuel_type']:18s} {x['n']:3d} facilities  total VaR ${x['var_m']}M")


if __name__ == "__main__":
    main()
