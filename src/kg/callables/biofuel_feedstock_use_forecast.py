"""biofuel_feedstock_use_forecast — the first D5 mechanical forecast callable.

Forecasts US soybean-oil biofuel feedstock use over the forward gap between the last raked EIA actual
(2026-04) and the oils demand frontier (2028-09, co-terminal with the non_biofuel_use forecast). That
is 29 months, May-2026..Sep-2028 -- the 2026-07-24 handoff's "~17 mo / Sep-2027" under-scoped it.
Closing this gap is what clears the forward #VALUE! in the soyoil balance sheet (design
forecast_layer_design_v1.md §2; handoff 2026-07-24 §5).

METHOD (the ruled feedstock method, project_feedstock_forecast_method 2026-07-09: fuel-prod x mix):

    forward_sbo_lb[fuel, month]  =  fuel_production_gal[fuel, month]  x  intensity[fuel]

  where the per-fuel INTENSITY bundles yield x SBO-mix-share into a single, self-calibrating ratio:

    intensity[fuel]  =  sum_trailing SBO_lb[fuel]  /  sum_trailing fuel_gal[fuel]      (lb SBO per gal)

  The intensity is a FLAT trailing ratio -- a pure mechanical MODEL_BASE (vintage_rank 1, D3): zero
  human judgment, reproducible from inputs alone. It deliberately does NOT model mix drift (SBO's share
  is slowly ceding to tallow/UCO/DCO); that judged view is Tore's to add on top as MODEL_ADJUSTED(6),
  and the gap between this baseline and his view is the reconciliation signal the whole symbiotic-
  forecasting endpoint exists to measure (project_symbiotic_forecasting).

  Sanity of the trailing intensities (2025-05..2026-04): biodiesel 5.81, RD 1.92, SAF 0.56 lb SBO/gal
  -- consistent with biodiesel being soy-heavy (~77% of a 7.5 lb/gal feedstock draw) and RD being
  tallow/UCO-heavy (SBO ~25% of its mix).

FUEL-PRODUCTION GAP FILL. silver.fuel_production_forecast covers the horizon UNEVENLY: renewable_diesel
  is complete (17/17 months), biodiesel is missing Jun-Oct 2026 (5 months), SAF has almost no forward
  forecast, and coprocessing has neither forward production nor any SBO draw since 2024-12. Where the
  forward fuel-production number is absent, we project it with the same seasonal baseline the oils writer
  already trusts for non-bio use: mean of the last N complete calendar-year totals x mean monthly share.
  Every forecast month records which route produced its gallons ('fuel_production_forecast' vs
  'seasonal_projected') in the diagnostics, so the modeled portion is never silent.

BANDS (D4, mandatory). value_low/value_high come from the DISPERSION of the trailing monthly intensity
  (p10/p90 of lb-SBO-per-gal across the trailing window), applied to the same forward gallons, then
  clamped so value_low <= value <= value_high always holds. This is a SCENARIO/subjective interval
  (recent mix could stay, tighten, or loosen), not a statistical confidence level -- documented per the
  design's band-semantics rule. The total's band is the comonotone sum of the per-fuel bands
  (conservative: assumes the fuels' mix surprises move together).

PURITY (D5). The core `forecast(data, assumptions)` is a pure function: no now(), no ambient "latest",
  no DB. The horizon and the trailing window are derived from `assumptions` alone; all data arrives via
  `data`. Same (data, assumptions) -> byte-identical output, forever. `load_data()` and `run()` are the
  impure shell that fetches inputs and orchestrates -- kept separate on purpose.

Emits rows for series biofuel_use_{biodiesel,renewable_diesel,saf,total} at vintage MODEL_BASE / rank 1,
value in raw LB, marketing_year = CALENDAR year, period = 'M05' etc -- matching the US oils flat-file
convention so write_oils_supply_flat_files.py can merge them straight through.
"""
from __future__ import annotations

from statistics import mean
from typing import Any


CALLABLE = 'biofuel_feedstock_use_forecast'
CALLABLE_VERSION = 'v1.0'

# The forward vintage this callable emits (design D3): raw callable output, the audit anchor.
PRODUCED_VINTAGE = 'MODEL_BASE'
PRODUCED_RANK = 1

DEFAULT_ASSUMPTIONS: dict[str, Any] = {
    'feedstock_code':   'SBO',
    'commodity':        'soybean_oil',
    'horizon_start':    '2026-05',   # first forecast month (inclusive) = first month past raked actuals
    'horizon_end':      '2028-09',   # last forecast month (inclusive). Set CO-TERMINAL with the oils
                                     # non_biofuel_use forecast frontier (write_oils_supply_flat_files.py
                                     # projects non-bio to last_actual + cur_my..cur_my+2 = 2028-09 today),
                                     # so biofuel_use and non_biofuel_use share one forward frontier and no
                                     # residual biofuel hole is left in the balance sheet. (The 2026-07-24
                                     # handoff's "~17 mo / Sep-2027" under-scoped this; the real forward
                                     # demand range is 29 months, May-2026..Sep-2028.)
    'trailing_months':  12,          # window ending just before horizon_start (intensity + band)
    'fuels':            ['biodiesel', 'renewable_diesel', 'saf'],
    # coprocessing is excluded: no forward fuel-production forecast and 0 SBO draw since 2024-12.
    'intensity_method': 'trailing_ratio',      # lb SBO per gal = sum_trailing lb / sum_trailing gal
    'band_method':      'intensity_dispersion',
    'band_low_pct':     10,
    'band_high_pct':    90,
    'fuel_gap_fill':    'seasonal_baseline',    # fill missing forward fuel-prod months
    'seasonal_years':   3,                      # complete calendar years for the seasonal fuel projection
    'emit_total':       True,
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


def _trailing_window(horizon_start: tuple[int, int], n: int) -> list[tuple[int, int]]:
    """The n calendar months immediately BEFORE horizon_start."""
    y, m = horizon_start
    out = []
    for _ in range(n):
        m -= 1
        if m == 0:
            y, m = y - 1, 12
        out.append((y, m))
    return sorted(out)


def _percentile(vals: list[float], pct: float) -> float:
    """Linear-interpolation percentile on a sorted copy (pct in 0..100). Pure, no numpy."""
    if not vals:
        raise ValueError('percentile of empty list')
    s = sorted(vals)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _seasonal_projection(gal_by_fm: dict[tuple[int, int], float],
                         missing: list[tuple[int, int]],
                         n_years: int) -> dict[tuple[int, int], float]:
    """Project fuel production for `missing` months from the fuel's own history.

    annual = mean of the last `n_years` COMPLETE calendar-year totals;
    share[m] = mean over those years of (month gal / year total);
    gal[y,m] = annual * share[m].
    Falls back to a flat 1/12 seasonal on the trailing mean if no complete year exists.
    Returns {(y,m): gal} for the missing months only.
    """
    if not missing:
        return {}
    by_year: dict[int, dict[int, float]] = {}
    for (y, m), g in gal_by_fm.items():
        by_year.setdefault(y, {})[m] = g
    complete = sorted(y for y, mm in by_year.items() if len(mm) == 12)
    if complete:
        use = complete[-n_years:]
        annual = mean(sum(by_year[y].values()) for y in use)
        share = {}
        for m in range(1, 13):
            share[m] = mean(by_year[y][m] / sum(by_year[y].values()) for y in use
                            if sum(by_year[y].values()) > 0)
        ssum = sum(share.values()) or 1.0
        share = {m: share[m] / ssum for m in share}
    else:  # no complete year -> flat seasonal on the mean monthly level available
        lvl = mean(gal_by_fm.values()) if gal_by_fm else 0.0
        annual = lvl * 12
        share = {m: 1 / 12 for m in range(1, 13)}
    return {(y, m): annual * share[m] for (y, m) in missing}


# ---------------------------------------------------------------------------------------------
# PURE CORE
# ---------------------------------------------------------------------------------------------

def forecast(data: dict, assumptions: dict | None = None) -> dict:
    """Pure (data, assumptions) -> forecast rows + diagnostics. No I/O, no now().

    data = {
      'sbo_lb':   {fuel: {(y,m): lb}},     # raked SBO actuals (>= the trailing window)
      'fuel_gal': {fuel: {(y,m): gal}},    # fuel production gal, actual+forecast (history..horizon_end)
    }
    Returns {'rows': [...], 'assumptions': {...}, 'diagnostics': {...}}.
    Each row: commodity, class, series, marketing_year(cal yr), period_type, period, vintage,
              vintage_rank, value, unit, value_low, value_high, source, plus _fuel/_gal_source (diag).
    """
    a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}
    hs, he = _ym(a['horizon_start']), _ym(a['horizon_end'])
    horizon = _month_range(hs, he)
    trailing = _trailing_window(hs, a['trailing_months'])
    trailing_set = set(trailing)

    sbo = data['sbo_lb']
    gal = data['fuel_gal']

    rows: list[dict] = []
    diag_fuels: dict[str, Any] = {}
    total_by_fm: dict[tuple[int, int], list[float]] = {}  # (y,m) -> [value, low, high] accumulators

    for fuel in a['fuels']:
        sbo_f = sbo.get(fuel, {})
        gal_f = gal.get(fuel, {})

        # --- intensity point + band over the trailing window ---
        # UNCONDITIONAL intensity: total SBO lb / total fuel gal across the fuel's SBO-"live" window
        # (from the first month SBO is observed for this fuel onward). A month where the fuel produced
        # gallons but drew NO SBO counts as 0 lb, correctly diluting the ratio -- otherwise an
        # intermittent SBO user like SAF (SBO only some months) would show an inflated per-gal draw.
        # Months BEFORE the SBO series starts are excluded from both sides (data-absence, not a real 0).
        if not sbo_f:
            diag_fuels[fuel] = {'skipped': 'no raked SBO for this fuel'}
            continue
        live_lo, live_hi = min(sbo_f), max(sbo_f)
        elig = [k for k in trailing_set
                if k in gal_f and gal_f[k] > 0 and live_lo <= k <= live_hi]
        if not elig:
            diag_fuels[fuel] = {'skipped': 'no trailing fuel-gal months within SBO-live window'}
            continue
        sum_lb = sum(sbo_f.get(k, 0.0) for k in elig)
        sum_gal = sum(gal_f[k] for k in elig)
        intensity = sum_lb / sum_gal
        monthly_ratios = [sbo_f.get(k, 0.0) / gal_f[k] for k in elig]
        ratio_lo = _percentile(monthly_ratios, a['band_low_pct'])
        ratio_hi = _percentile(monthly_ratios, a['band_high_pct'])
        # clamp so the band always brackets the point (D4 CHECK requirement)
        ratio_lo = min(ratio_lo, intensity)
        ratio_hi = max(ratio_hi, intensity)

        # --- forward gallons: observed forecast where present, seasonal projection where not ---
        observed = {k: gal_f[k] for k in horizon if k in gal_f}
        missing = [k for k in horizon if k not in gal_f]
        projected = _seasonal_projection(gal_f, missing, a['seasonal_years'])
        fwd_gal = {**observed, **projected}
        gal_source = {k: ('fuel_production_forecast' if k in observed else 'seasonal_projected')
                      for k in horizon}

        for (y, m) in horizon:
            g = fwd_gal.get((y, m), 0.0)
            v = g * intensity
            lo = g * ratio_lo
            hi = g * ratio_hi
            rows.append(_row(a, 'biofuel_use_' + fuel, y, m, v, lo, hi,
                             _fuel=fuel, _gal_source=gal_source[(y, m)]))
            acc = total_by_fm.setdefault((y, m), [0.0, 0.0, 0.0])
            acc[0] += v; acc[1] += lo; acc[2] += hi

        diag_fuels[fuel] = {
            'intensity_lb_per_gal': round(intensity, 5),
            'band_ratio_lo': round(ratio_lo, 5),
            'band_ratio_hi': round(ratio_hi, 5),
            'trailing_months_used': len(elig),
            'trailing_sbo_mil_lb': round(sum_lb / 1e6, 1),
            'trailing_gal_mm': round(sum_gal / 1e6, 1),
            'n_gap_filled_months': len(missing),
            'gap_filled_periods': [f'{y}-{m:02d}' for (y, m) in sorted(missing)],
        }

    # --- biofuel_use_total: comonotone sum of the per-fuel bands ---
    if a['emit_total']:
        for (y, m) in horizon:
            if (y, m) in total_by_fm:
                v, lo, hi = total_by_fm[(y, m)]
                rows.append(_row(a, 'biofuel_use_total', y, m, v, lo, hi,
                                 _fuel='total', _gal_source='sum'))

    return {
        'rows': rows,
        'assumptions': a,
        'diagnostics': {
            'callable': CALLABLE,
            'callable_version': CALLABLE_VERSION,
            'horizon': [f'{y}-{m:02d}' for (y, m) in horizon],
            'trailing_window': [f'{y}-{m:02d}' for (y, m) in trailing],
            'n_rows': len(rows),
            'fuels': diag_fuels,
        },
    }


def _row(a: dict, series: str, y: int, m: int, value: float, low: float, high: float,
         _fuel: str, _gal_source: str) -> dict:
    return {
        'commodity':      a['commodity'],
        'class':          'ALL',
        'series':         series,
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
        '_fuel':          _fuel,
        '_gal_source':    _gal_source,
    }


# ---------------------------------------------------------------------------------------------
# IMPURE SHELL: data load + run wrapper
# ---------------------------------------------------------------------------------------------

def load_data(cur, assumptions: dict | None = None) -> dict:
    """Fetch the (impure) inputs the pure core needs. RealDictCursor expected.

    - SBO raked actuals by fuel (latest run), over the trailing window.
    - Fuel production gallons by fuel (actual+forecast), from a wide history through horizon_end,
      so the pure core can compute the trailing intensity, the seasonal shares, and read forward gal.
    """
    a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}
    he = _ym(a['horizon_end'])
    # pull a generous history for seasonal stability (2016-01 .. horizon_end)
    hist_end = f'{he[0]:04d}-{he[1]:02d}-01'

    sbo: dict[str, dict[tuple[int, int], float]] = {}
    cur.execute("""
        SELECT fuel_type,
               extract(year  FROM period)::int AS y,
               extract(month FROM period)::int AS m,
               sum(raked_mil_lbs) * 1e6 AS lb
        FROM gold.bbd_feedstock_raked
        WHERE run_day = (SELECT max(run_day) FROM gold.bbd_feedstock_raked)
          AND feedstock_code = %s
        GROUP BY 1, 2, 3
    """, (a['feedstock_code'],))
    for r in cur.fetchall():
        sbo.setdefault(r['fuel_type'], {})[(r['y'], r['m'])] = float(r['lb'])

    gal: dict[str, dict[tuple[int, int], float]] = {}
    cur.execute("""
        SELECT fuel_type,
               extract(year  FROM period)::int AS y,
               extract(month FROM period)::int AS m,
               production_mmgal * 1e6 AS g
        FROM silver.fuel_production_forecast
        WHERE production_mmgal IS NOT NULL
          AND period <= %s::date
        GROUP BY 1, 2, 3, production_mmgal
    """, (hist_end,))
    for r in cur.fetchall():
        gal.setdefault(r['fuel_type'], {})[(r['y'], r['m'])] = float(r['g'])

    return {'sbo_lb': sbo, 'fuel_gal': gal}


def run(cur, assumptions: dict | None = None) -> dict:
    """Load inputs and run the pure core. Returns the pure result plus an input_snapshot_ref."""
    a = {**DEFAULT_ASSUMPTIONS, **(assumptions or {})}
    data = load_data(cur, a)
    result = forecast(data, a)
    cur.execute("SELECT max(run_day) AS rd FROM gold.bbd_feedstock_raked")
    raked_run = cur.fetchone()['rd']
    result['input_snapshot_ref'] = (
        f"gold.bbd_feedstock_raked@run_day={raked_run}; "
        f"silver.fuel_production_forecast<= {a['horizon_end']}"
    )
    return result


if __name__ == '__main__':
    import json
    from src.services.database.db_config import get_connection
    with get_connection() as conn:
        out = run(conn.cursor())
    print(json.dumps(out['diagnostics'], indent=2, default=str))
    print(f"\n{out['diagnostics']['n_rows']} rows. Sample (first month of each series):")
    seen = set()
    for r in out['rows']:
        if r['series'] not in seen:
            seen.add(r['series'])
            print(f"  {r['series']:32s} {r['marketing_year']}-{r['period']} "
                  f"value={r['value']/1e6:8.1f}mm  [{r['value_low']/1e6:.1f}, {r['value_high']/1e6:.1f}] "
                  f"src={r['_gal_source']}")
