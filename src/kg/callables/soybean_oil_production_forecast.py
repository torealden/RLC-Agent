"""soybean_oil_production_forecast -- the D5 SUPPLY forecast callable (ledger 6d).

Forecasts US soybean-OIL production over the forward gap between the last NASS oil-production actual
(2026-05) and the oils supply frontier (2028-09), so the forward #VALUE! in the soyoil balance sheet
clear. Companion to biofuel_feedstock_use_forecast (6c, the demand side); together they close the
soyoil sheet forward. See docs/specs/soyoil_supply_forecast_6d_findings.md.

WHY PRODUCTION ONLY (verified in the .xlsm, 6d findings sec 1). The balance sheet computes ending
stocks as a pure ROLL-FORWARD identity (stock[m] = stock[m-1] + production + imports - exports -
domestic_use), so stocks need no forecast -- forecasting them would double-model and fight the
identity. Imports/exports fall back to seasonalized constants (the trade matrix will drive exports
later). The one genuine forward supply gap the sheet cannot self-source is PRODUCTION, whose internal
crush x yield fallback is poisoned by a stale Excel crush link. Publishing a `production` row makes the
sheet's SUMIFS branch fire and bypass that broken fallback (same seam 6c used for biofuel_use).

METHOD (Tore 2026-07-24: "orient our base around the hard-coded numbers, adjusted by your model").

    production_lb[month]  =  crush_bu[month]  x  oil_yield_lb_per_bu[month]

  CRUSH is ANCHORED to Tore's judged annual crush (soybean-sheet hard-codes 2620/2700/2850 mil bu by
  oil marketing year), and the MODEL supplies the ADJUSTMENT: the seasonal spread of that annual into
  months, and the oil-yield conversion. For the current (partial) MY, forecast months carry the
  RESIDUAL (annual - actual-to-date) distributed on the trailing seasonal crush share, so actuals +
  forecast tie to the annual anchor exactly.

    crush_month[m] = residual_or_annual * seasonal_share[m] / share_norm
    oil_yield[m]   = trailing mean of (oil_prod_lb / crush_bu) for calendar month m (~11.6 lb/bu)

  This is the ruled feedstock/crush method at the supply side (project_feedstock_forecast_method): a
  judged annual level + mechanical monthly structure. The pure MECHANICAL demand-pull crush (validated
  to 0.27% MAPE on history, 6d findings sec 3) is carried as `crush_annual_mechanical` and used only to
  set the LOW edge of the band -- it runs ~10-17% below the judged anchor today because it rides the
  flat 6c biofuel baseline; that gap is the reconciliation signal (project_symbiotic_forecasting), not
  a level override. The full demand-pull production (domestic oil+meal demand econometric models +
  trade-matrix exports) is deferred to the Aegus modeling work.

BANDS (D4, mandatory). A SCENARIO interval, not a confidence level:
    value      = judged crush  x  mean seasonal yield
    value_low  = mechanical crush x p10 seasonal yield   (mechanical demand + low yield)
    value_high = judged crush   x p90 seasonal yield     (judged demand   + high yield)
  clamped so value_low <= value <= value_high. It literally spans the mechanical-baseline-to-judged
  reconciliation range on the demand side and the yield dispersion on the technical side.

PURITY (D5). Core forecast(data, assumptions) has no now(), no DB, no ambient latest; horizon, anchors
  and windows come from `assumptions`, all data via `data`. load_data()/run() are the impure shell.

Emits series 'production' at vintage MODEL_BASE / rank 1, value in raw LB, marketing_year = CALENDAR
year, period = 'M06' etc -- the US oils flat-file convention, so write_oils_supply_flat_files.py merges
it straight into the SUPPLY tab (routing added in 6d).
"""
from __future__ import annotations

from statistics import mean
from typing import Any


CALLABLE = 'soybean_oil_production_forecast'
CALLABLE_VERSION = 'v1.0'

PRODUCED_VINTAGE = 'MODEL_BASE'
PRODUCED_RANK = 1

ST_LB = 2000.0      # short ton -> lb
BU_LB = 60.0        # soybeans lb per bushel

DEFAULT_ASSUMPTIONS: dict[str, Any] = {
    'commodity':      'soybean_oil',
    'horizon_start':  '2026-06',   # first forecast month = first past the last NASS oil-prod actual
    'horizon_end':    '2028-09',   # co-terminal with the oils demand frontier (biofuel/non-bio)
    # Tore's judged annual crush anchor, mil bu, keyed by OIL marketing year (Oct-Sep) start year.
    # These are the soybean-sheet hard-codes (soy_balance_sheet!AK12/AL12/AM12/AN12).
    'crush_annual_mil_bu': {2025: 2620, 2026: 2700, 2027: 2850, 2028: 2850},
    # Provisional MECHANICAL demand-pull crush (mil bu) -- band LOW edge + reconciliation companion.
    # Computed 6d from the 6c biofuel forecast + trailing non-bio + oil-leg identity (findings sec 4).
    # Replaced by the Aegus domestic-demand econometric models when they land.
    'crush_annual_mechanical': {2025: 2370, 2026: 2440, 2027: 2364, 2028: 2364},
    'trailing_my':    3,           # complete oil-MYs for seasonal crush shares + yield profile
    'band_low_pct':   10,
    'band_high_pct':  90,
}


# ---------------------------------------------------------------------------------------------
# small pure helpers
# ---------------------------------------------------------------------------------------------

def _ym(s: str) -> tuple[int, int]:
    y, m = s.split('-')
    return int(y), int(m)


def _month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    (ys, ms), (ye, me) = start, end
    out, y, m = [], ys, ms
    while (y, m) <= (ye, me):
        out.append((y, m))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def _oil_my(y: int, m: int) -> int:
    """US oil marketing year (Oct-Sep), labelled by its start year."""
    return y if m >= 10 else y - 1


def _percentile(vals: list[float], pct: float) -> float:
    if not vals:
        raise ValueError('percentile of empty list')
    s = sorted(vals)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


# ---------------------------------------------------------------------------------------------
# PURE CORE
# ---------------------------------------------------------------------------------------------

def forecast(data: dict, assumptions: dict | None = None) -> dict:
    """Pure (data, assumptions) -> production forecast rows + diagnostics. No I/O, no now().

    data = {
      'crush':     {(y,m): mil_bu},          # NASS crush actuals (unit-converted to mil bu)
      'oil_yield': {(y,m): lb_per_bu},       # NASS oil_prod_lb / crush_bu actuals
    }
    Returns {'rows': [...], 'assumptions': {...}, 'diagnostics': {...}}.
    """
    a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}
    hs, he = _ym(a['horizon_start']), _ym(a['horizon_end'])
    horizon = _month_range(hs, he)
    crush = data['crush']
    yld = data['oil_yield']
    # assumption dicts may arrive with str keys (jsonb round-trip); normalize to int
    hard = {int(k): float(v) for k, v in a['crush_annual_mil_bu'].items()}
    mech = {int(k): float(v) for k, v in a['crush_annual_mechanical'].items()}

    # --- trailing complete oil-MYs for seasonal crush shares + yield profile ---
    by_my: dict[int, dict[int, float]] = {}
    for (y, m), v in crush.items():
        by_my.setdefault(_oil_my(y, m), {})[m] = v
    complete = sorted(my for my, mm in by_my.items() if len(mm) == 12)
    use_my = complete[-a['trailing_my']:]
    if not use_my:
        raise ValueError('no complete oil marketing year in crush actuals for seasonal shares')
    # seasonal crush share by calendar month (mean over trailing MYs of month/MY-total)
    share = {}
    for m in range(1, 13):
        share[m] = mean(by_my[my][m] / sum(by_my[my].values()) for my in use_my)
    snorm = sum(share.values()) or 1.0
    share = {m: share[m] / snorm for m in share}
    # seasonal oil-yield profile by calendar month (mean + p10/p90 over trailing window)
    yr_by_month: dict[int, list[float]] = {}
    for (y, m), v in yld.items():
        if _oil_my(y, m) in use_my:
            yr_by_month.setdefault(m, []).append(v)
    ymean = {m: mean(vs) for m, vs in yr_by_month.items()}
    ylo = {m: _percentile(vs, a['band_low_pct']) for m, vs in yr_by_month.items()}
    yhi = {m: _percentile(vs, a['band_high_pct']) for m, vs in yr_by_month.items()}

    # --- per oil-MY: distribute the annual crush anchor across its FORECAST months ---
    # forecast months of each MY, and the actual-to-date crush already booked in that MY.
    fc_by_my: dict[int, list[tuple[int, int]]] = {}
    for (y, m) in horizon:
        fc_by_my.setdefault(_oil_my(y, m), []).append((y, m))

    rows: list[dict] = []
    diag_my: dict[int, Any] = {}
    for my, fmonths in sorted(fc_by_my.items()):
        if my not in hard:
            diag_my[my] = {'skipped': 'no crush anchor for this oil MY'}
            continue
        actual_sum = sum(v for (y, m), v in crush.items() if _oil_my(y, m) == my)
        residual_hard = hard[my] - actual_sum
        residual_mech = mech.get(my, hard[my]) - actual_sum
        fc_share_sum = sum(share[m] for (_, m) in fmonths) or 1.0
        for (y, m) in fmonths:
            w = share[m] / fc_share_sum
            crush_hard = residual_hard * w
            crush_mech = residual_mech * w
            ym_, yl_, yh_ = ymean.get(m, 0.0), ylo.get(m, 0.0), yhi.get(m, 0.0)
            value = crush_hard * ym_ * 1e6            # mil bu * lb/bu * 1e6 -> raw lb
            low = min(crush_mech * yl_, crush_hard * ym_) * 1e6
            high = crush_hard * yh_ * 1e6
            low = min(low, value)
            high = max(high, value)
            rows.append(_row(a, y, m, value, low, high,
                             _crush_hard=round(crush_hard, 2), _crush_mech=round(crush_mech, 2),
                             _yield=round(ym_, 4)))
        diag_my[my] = {
            'crush_anchor_mil_bu': round(hard[my], 1),
            'crush_mechanical_mil_bu': round(mech.get(my, hard[my]), 1),
            'actual_to_date_mil_bu': round(actual_sum, 1),
            'forecast_residual_mil_bu': round(residual_hard, 1),
            'n_forecast_months': len(fmonths),
        }

    return {
        'rows': rows,
        'assumptions': a,
        'diagnostics': {
            'callable': CALLABLE,
            'callable_version': CALLABLE_VERSION,
            'horizon': [f'{y}-{m:02d}' for (y, m) in horizon],
            'trailing_oil_my': use_my,
            'seasonal_crush_share': {m: round(s, 4) for m, s in share.items()},
            'seasonal_oil_yield': {m: round(v, 3) for m, v in ymean.items()},
            'n_rows': len(rows),
            'by_marketing_year': diag_my,
        },
    }


def _row(a: dict, y: int, m: int, value: float, low: float, high: float,
         _crush_hard: float, _crush_mech: float, _yield: float) -> dict:
    return {
        'commodity':      a['commodity'],
        'class':          'ALL',
        'series':         'production',
        'marketing_year': y,                  # CALENDAR year (oils flat-file convention)
        'period_type':    'cal_month',
        'period':         f'M{m:02d}',
        'vintage':        PRODUCED_VINTAGE,
        'vintage_rank':   PRODUCED_RANK,
        'value':          float(value),
        'value_low':      float(low),
        'value_high':     float(high),
        'unit':           'LB',
        'source':         CALLABLE,
        '_crush_hard':    _crush_hard,
        '_crush_mech':    _crush_mech,
        '_yield':         _yield,
    }


# ---------------------------------------------------------------------------------------------
# IMPURE SHELL: data load + run wrapper
# ---------------------------------------------------------------------------------------------

def load_data(cur, assumptions: dict | None = None) -> dict:
    """Fetch NASS soybean crush + oil-production actuals; derive crush (mil bu) and oil yield (lb/bu).

    NASS_SOY_CRUSH stores crush in SHORT TONS and oil_production_crude in LB (the mixed-unit landmine,
    CLAUDE.md oilseed units). Convert crush -> mil bu (x2000 / 60 / 1e6); yield = oil_lb / crush_bu.
    """
    cur.execute("""
        SELECT attribute, calendar_year AS y, month AS m, realized_value AS v
        FROM silver.monthly_realized
        WHERE commodity='soybeans' AND source='NASS_SOY_CRUSH'
          AND attribute IN ('crush', 'oil_production_crude') AND realized_value IS NOT NULL
    """)
    crush_bu: dict[tuple[int, int], float] = {}
    oil_lb: dict[tuple[int, int], float] = {}
    for r in cur.fetchall():
        k = (int(r['y']), int(r['m']))
        if r['attribute'] == 'crush':
            crush_bu[k] = float(r['v']) * ST_LB / BU_LB / 1e6   # mil bu
        else:
            oil_lb[k] = float(r['v']) / 1e6                     # mil lb
    oil_yield = {k: oil_lb[k] / crush_bu[k] for k in crush_bu
                 if k in oil_lb and crush_bu[k] > 0}
    return {'crush': crush_bu, 'oil_yield': oil_yield}


def run(cur, assumptions: dict | None = None) -> dict:
    a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}
    data = load_data(cur, a)
    result = forecast(data, a)
    last_crush = max(data['crush']) if data['crush'] else None
    result['input_snapshot_ref'] = (
        f"silver.monthly_realized NASS_SOY_CRUSH crush+oil_production_crude "
        f"through {last_crush[0]}-{last_crush[1]:02d}" if last_crush else "no crush actuals")
    return result


if __name__ == '__main__':
    import json
    from src.services.database.db_config import get_connection
    with get_connection() as conn:
        out = run(conn.cursor())
    print(json.dumps(out['diagnostics'], indent=2, default=str))
    print(f"\n{out['diagnostics']['n_rows']} rows. Sample (first month of each MY):")
    seen = set()
    for r in out['rows']:
        if r['marketing_year'] not in seen:
            seen.add(r['marketing_year'])
            print(f"  {r['marketing_year']}-{r['period']} value={r['value']/1e6:8.1f}mm lb  "
                  f"[{r['value_low']/1e6:.1f}, {r['value_high']/1e6:.1f}]  "
                  f"crush_hard={r['_crush_hard']} crush_mech={r['_crush_mech']} yld={r['_yield']}")
