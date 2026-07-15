"""Parametric variance-covariance VaR optimizer for the quarterly feedstock risk budget.

Turns a dollar/VaR budget into a concrete pounds-per-feedstock purchase plan for one
facility for one quarter. The VaR cap is what forces a diversified, forward-bought book
instead of the 100%-highest-margin corner solution the raw margin allocator produces.

Problem (per facility, per quarter):
    decide g_i = gallons produced from feedstock i  (i in eligible set)
    maximize   sum_i margin_i * g_i                       (IFV margin, $/gal)
    s.t.       sum_i g_i = G                              (meet production need)
               g_i >= 0                                   (+ optional supply caps)
               VaR($) / notional($) <= budget_pct         (the risk budget)
    where  w_i = g_i * lbs_per_gal_i          (lbs to buy)
           P_i = w_i * price_i                (dollar position in feedstock i)
           VaR = z * sqrt(h) * sqrt( P' C P ) , C_ij = vol_i vol_j corr_ij  (annualized)
           notional = sum_i P_i ;  z = Phi^-1(confidence) ;  h = horizon_months/12

A soft anchor pulls the mix toward the facility's known feedstock_mix so single-feedstock
plants (e.g. Chevron El Segundo = 100% SBO) don't drift on margin noise. If the VaR cap is
tighter than the min-variance frontier allows, we fall back to the min-VaR portfolio and
flag it (feasible='min_var_floor') rather than failing — still kills the whipsaw.
"""
from __future__ import annotations
from dataclasses import dataclass, field

import numpy as np
from scipy.optimize import minimize
from scipy.stats import norm


@dataclass
class BudgetPlan:
    feedstocks: list                       # eligible codes, in solve order
    lbs_by_feedstock: dict                 # code -> lbs to buy this quarter
    gal_by_feedstock: dict                 # code -> gallons produced
    shares: dict                           # code -> % of gallons
    var_dollars: float                     # position VaR ($)
    notional_dollars: float                # position value ($)
    var_ratio: float                       # VaR / notional (achieved)
    budget_ratio: float                    # the cap it was solved against
    margin_dollars: float                  # total IFV margin ($)
    feasible: str                          # 'ok' | 'min_var_floor'
    meta: dict = field(default_factory=dict)


def optimize_quarter(
    feedstocks: list,
    need_gallons: float,
    price_per_lb: dict,        # code -> $/lb
    lbs_per_gal: dict,         # code -> lbs feedstock per gal fuel
    margin_per_gal: dict,      # code -> $/gal IFV margin
    ann_vol: dict,             # code -> annualized vol (fraction)
    corr: dict,                # (a,b) -> correlation
    anchor_shares: dict,       # code -> contracted baseline gallon-share (the covered mix)
    coverage: float = 0.70,    # fraction of the quarter bought forward at the anchor mix
    budget_pct: float = 0.08,  # VaR budget as % of total quarterly feedstock notional
    confidence: float = 0.95,
    horizon_months: int = 3,
    supply_cap_lbs: dict | None = None,
) -> BudgetPlan:
    """Covered/open procurement:
      covered = coverage * G at the contracted anchor mix (price-locked, no VaR)
      open    = (1-coverage) * G, margin-maximized across eligible feedstocks,
                subject to VaR(open $ position) <= budget_pct * total notional.
    The covered book keeps the plan anchored to how the plant actually contracts;
    the open book is where margin opportunism lives, capped so it can't whipsaw.
    """
    codes = [c for c in feedstocks if c in price_per_lb and c in lbs_per_gal
             and price_per_lb[c] and lbs_per_gal[c]]
    if not codes:
        raise ValueError("no usable feedstocks (missing price or yield)")
    n = len(codes)
    z = float(norm.ppf(confidence))
    h = horizon_months / 12.0
    G = float(need_gallons)
    coverage = min(max(coverage, 0.0), 1.0)

    lpg = np.array([lbs_per_gal[c] for c in codes])
    px = np.array([price_per_lb[c] for c in codes])
    mgn = np.array([margin_per_gal.get(c, 0.0) for c in codes])
    vol = np.array([max(ann_vol.get(c, 0.0), 1e-4) for c in codes])
    C = np.array([[(1.0 if i == j else corr.get((codes[i], codes[j]),
                    corr.get((codes[j], codes[i]), 0.0))) * vol[i] * vol[j]
                   for j in range(n)] for i in range(n)])

    # anchor shares -> normalized array over eligible codes (fallback: cheapest feedstock)
    a = np.array([anchor_shares.get(c, 0.0) for c in codes], dtype=float)
    if a.sum() <= 0:
        a = np.zeros(n); a[int(np.argmin(px * lpg))] = 1.0
    a = a / a.sum()

    covered_gal = coverage * G * a                     # gallons per feedstock, covered leg
    open_gal_total = (1.0 - coverage) * G

    # reference total notional (stable; used to size the $ VaR budget)
    ref_notional = float((a * G * lpg * px).sum())
    budget_dollars = budget_pct * ref_notional

    def open_positions(s):
        g_open = s * open_gal_total
        P = g_open * lpg * px
        return g_open, P

    def open_var(s):
        _, P = open_positions(s)
        return z * np.sqrt(h) * np.sqrt(max(P @ C @ P, 0.0))

    feasible = 'ok'
    if open_gal_total <= 1e-6:
        s = a.copy()                                   # fully covered -> anchor mix
    else:
        ub = np.ones(n)
        if supply_cap_lbs:
            for i, c in enumerate(codes):
                cap = supply_cap_lbs.get(c)
                if cap is not None and open_gal_total > 0:
                    ub[i] = min(1.0, (cap / lpg[i]) / open_gal_total)
        bounds = [(0.0, float(ub[i])) for i in range(n)]
        eq = {'type': 'eq', 'fun': lambda s: s.sum() - 1.0}
        # feasible warm start = min-VaR of the open leg
        mv = minimize(open_var, a.copy(), method='SLSQP', bounds=bounds,
                      constraints=[eq], options={'maxiter': 400, 'ftol': 1e-12})
        s_mv = np.clip(mv.x, 0, None); s_mv = s_mv / s_mv.sum()
        if open_var(s_mv) > budget_dollars + 1e-6:
            s, feasible = s_mv, 'min_var_floor'        # cap tighter than open frontier
        else:
            budget_con = {'type': 'ineq', 'fun': lambda s: budget_dollars - open_var(s)}
            mm = minimize(lambda s: -(mgn @ (s * open_gal_total)), s_mv, method='SLSQP',
                          bounds=bounds, constraints=[eq, budget_con],
                          options={'maxiter': 400, 'ftol': 1e-9})
            s_mm = np.clip(mm.x, 0, None); s_mm = s_mm / s_mm.sum() if s_mm.sum() > 0 else s_mv
            s = s_mm if open_var(s_mm) <= budget_dollars + 1e-6 else s_mv

    open_gal = s * open_gal_total
    g = covered_gal + open_gal                          # total gallons per feedstock
    w = g * lpg                                          # total lbs to buy
    P_open = open_gal * lpg * px
    notional = float((w * px).sum())
    var_d = float(z * np.sqrt(h) * np.sqrt(max(P_open @ C @ P_open, 0.0)))
    tot_gal = g.sum()

    return BudgetPlan(
        feedstocks=codes,
        lbs_by_feedstock={c: float(w[i]) for i, c in enumerate(codes)},
        gal_by_feedstock={c: float(g[i]) for i, c in enumerate(codes)},
        shares={c: float(g[i] / tot_gal if tot_gal else 0.0) for i, c in enumerate(codes)},
        var_dollars=var_d,
        notional_dollars=notional,
        var_ratio=(var_d / notional if notional > 0 else 0.0),
        budget_ratio=budget_pct,
        margin_dollars=float(mgn @ g),
        feasible=feasible,
        meta={'z': z, 'horizon_months': horizon_months, 'n_feedstocks': n,
              'coverage': coverage, 'budget_dollars': budget_dollars,
              'covered_gal': float(covered_gal.sum()), 'open_gal': float(open_gal_total)},
    )


if __name__ == "__main__":
    # Self-test with REAL vol/corr from the DB, a BP Cherry Point-like coprocessing facility.
    import sys
    from pathlib import Path
    ROOT = Path(r"C:/dev/RLC-Agent"); sys.path.insert(0, str(ROOT))
    from dotenv import load_dotenv; load_dotenv(ROOT / ".env")
    from src.services.database.db_config import get_connection

    with get_connection() as c:
        cur = c.cursor()
        cur.execute("SELECT feedstock_code, ann_vol FROM risk.feedstock_volatility")
        vol = {r['feedstock_code']: float(r['ann_vol']) for r in cur.fetchall()}
        cur.execute("SELECT feedstock_a, feedstock_b, corr FROM risk.feedstock_correlation")
        corr = {(r['feedstock_a'], r['feedstock_b']): float(r['corr']) for r in cur.fetchall()}
        cur.execute("""SELECT feedstock_code, avg_price_per_lb FROM silver.feedstock_supply
                       WHERE period=(SELECT max(period) FROM silver.feedstock_supply)""")
        px = {r['feedstock_code']: float(r['avg_price_per_lb']) for r in cur.fetchall()}

    # coprocessing at BP Cherry Point: low-FFA feedstocks feasible; ~110 MMgy -> ~27.5 MMgal/qtr
    elig = ['SBO', 'DCO', 'BFT', 'CO', 'UCO']
    px.setdefault('CO', 0.49); px.setdefault('BFT', 0.43)
    lpg = {c: 7.7 for c in elig}          # ~7.7 lb/gal coprocessing
    # margin: SBO easiest to process (low FFA) -> small premium; DCO cheapest feedstock
    mgn = {'SBO': 0.55, 'DCO': 0.70, 'BFT': 0.60, 'CO': 0.45, 'UCO': 0.65}
    print("prices:", {c: round(px.get(c, 0), 3) for c in elig})
    print("vols:  ", {c: round(vol.get(c, 0), 3) for c in elig})
    # BP Stage 2 anchor: contracted SBO-heavy (matches reference.biofuel_facilities.feedstock_mix)
    anchor = {'SBO': 0.7, 'CO': 0.3}
    for cov in (0.60, 0.80):
        for bp in (0.08, 0.15):
            plan = optimize_quarter(elig, 27.5e6, px, lpg, mgn, vol, corr,
                                    anchor_shares=anchor, coverage=cov, budget_pct=bp)
            mix = ", ".join(f"{c} {plan.shares[c]*100:4.1f}%" for c in plan.feedstocks)
            print(f"\ncoverage {cov:.0%}, budget {bp:.0%}: [{plan.feasible}] "
                  f"VaR ${plan.var_dollars/1e6:.2f}M ({plan.var_ratio:.1%} of notional)  "
                  f"margin ${plan.margin_dollars/1e6:.2f}M")
            print(f"   mix: {mix}")
            print("   buy: " + ", ".join(f"{c} {plan.lbs_by_feedstock[c]/1e6:.0f}M lb"
                                         for c in plan.feedstocks if plan.lbs_by_feedstock[c] > 1e5))
