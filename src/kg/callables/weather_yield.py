"""
weather_adjusted_yield — the driving example for kg_callable.

Given a commodity + region + current USDA yield estimate + forecast rain + temp,
returns an adjusted yield estimate with confidence and analog reasoning.

This implementation is deliberately minimal for the pilot — the framework
(kg_callable, invocation logging, self-exploration) is the valuable part;
the coefficients here should be calibrated against analog years as we iterate.

Sources of truth:
- kg_context #40 'yield_model_parameters' (HB signature methodology — G/E → yield)
- Analog years hardcoded as placeholders: 2012 (drought), 2020 (derecho), 2023 (partial stress)
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


# =============================================================================
# Calibration — coefficients for pilot. Replace with fitted values as we collect
# forecast_actual pairs.
# =============================================================================

# Rain sensitivity: bpa change per inch-of-rain deviation from "normal" for the
# 30-day window, modulated by growth stage.
_RAIN_SENSITIVITY_BPA_PER_INCH = {
    # (commodity, growth_stage) -> bpa per inch deviation
    ('corn', 'vegetative'):        0.8,
    ('corn', 'pollination'):       3.2,   # high — silking is the critical window
    ('corn', 'grain_fill'):        1.8,
    ('corn', 'mature'):            0.2,
    ('soybeans', 'vegetative'):    0.5,
    ('soybeans', 'flowering'):     1.5,
    ('soybeans', 'pod_fill'):      2.4,   # critical for soy yield
    ('soybeans', 'mature'):        0.2,
    ('wheat', 'jointing'):         0.6,
    ('wheat', 'heading'):          1.2,
    ('wheat', 'grain_fill'):       0.9,
}

# Temperature penalty per degree above stress threshold
_TEMP_PENALTY_BPA_PER_DEG_OVER = {
    ('corn', 'pollination'):       1.5,   # heat during silking is devastating
    ('corn', 'grain_fill'):        0.6,
    ('soybeans', 'pod_fill'):      0.8,
    ('wheat', 'grain_fill'):       0.5,
}
_STRESS_TEMP_F = 86.0  # above this counts as heat stress

# "Normal" 30-day rain by region + month. Placeholder — replace with climatology.
_NORMAL_RAIN_IN = {
    'us.corn_belt':    {7: 4.0, 8: 3.5, 9: 3.0},
    'us.soy_belt':     {8: 3.5, 9: 3.0, 10: 2.5},
    'us.wheat_belt':   {5: 3.2, 6: 2.8},
    'br.mato_grosso':  {1: 10.0, 2: 9.0, 3: 7.0},
    'ar.pampas':       {12: 4.0, 1: 4.2, 2: 3.8},
}

_ANALOG_YEARS = {
    ('corn', 'pollination', 'dry_hot'):   [(2012, 'US_drought_devastating'), (2023, 'partial_stress')],
    ('corn', 'grain_fill', 'wet'):        [(2019, 'late_planting_cool_wet')],
    ('soybeans', 'pod_fill', 'dry'):      [(2012, 'late_season_stress')],
    ('corn', 'vegetative', 'storm'):      [(2020, 'derecho_IA_direct_damage')],
}


@dataclass
class YieldAdjustment:
    predicted_yield_bpa: float
    delta_bpa: float
    confidence: float           # 0..1
    reasoning: str
    analog_years: list
    warnings: list


def _get_normal_rain(region: str, month: int) -> Optional[float]:
    r = _NORMAL_RAIN_IN.get(region.lower())
    return r.get(month) if r else None


def _classify_pattern(rain_deviation_in: float, temp_over_stress_f: float) -> str:
    if rain_deviation_in < -1.0 and temp_over_stress_f > 2:
        return 'dry_hot'
    if rain_deviation_in < -0.5:
        return 'dry'
    if rain_deviation_in > 1.5:
        return 'wet'
    if temp_over_stress_f > 3:
        return 'hot'
    return 'normal'


def run(
    commodity: str,
    region: str,
    growth_stage: str,
    current_yield_bpa: float,
    forecast_rain_in_30d: float,
    forecast_temp_f_avg_30d: float,
    forecast_month: int,
    soil_moisture_pct: Optional[float] = None,
) -> dict:
    """
    Adjust a USDA-style yield estimate based on 30-day forecast weather.

    Parameters
    ----------
    commodity         : 'corn' | 'soybeans' | 'wheat'
    region            : node_key-style region, e.g. 'us.corn_belt'
    growth_stage      : 'vegetative', 'pollination', 'grain_fill', 'pod_fill', etc.
    current_yield_bpa : the USDA or analyst baseline yield estimate
    forecast_rain_in_30d : forecast cumulative rainfall over next 30 days (inches)
    forecast_temp_f_avg_30d : forecast average daily temp over next 30 days (F)
    forecast_month    : month (1-12) the 30-day window is centered on
    soil_moisture_pct : current soil moisture % if known (used for confidence)

    Returns
    -------
    dict matching YieldAdjustment fields, plus input echo.
    """
    warnings = []

    # Rain impact
    normal_rain = _get_normal_rain(region, forecast_month)
    if normal_rain is None:
        warnings.append(f"No climatology for region={region} month={forecast_month}; assuming 3.0 in")
        normal_rain = 3.0
    rain_dev = forecast_rain_in_30d - normal_rain
    rain_sens = _RAIN_SENSITIVITY_BPA_PER_INCH.get((commodity, growth_stage), 1.0)
    rain_delta = rain_dev * rain_sens

    # Temperature impact (only penalizes when over stress threshold)
    temp_over = max(0.0, forecast_temp_f_avg_30d - _STRESS_TEMP_F)
    temp_pen = _TEMP_PENALTY_BPA_PER_DEG_OVER.get((commodity, growth_stage), 0.3)
    temp_delta = -temp_over * temp_pen

    delta_bpa = rain_delta + temp_delta
    predicted = current_yield_bpa + delta_bpa

    # Confidence heuristic: baseline 0.7, down for missing climatology or
    # unfamiliar growth stage, up if soil_moisture supports the forecast direction.
    confidence = 0.7
    if (commodity, growth_stage) not in _RAIN_SENSITIVITY_BPA_PER_INCH:
        warnings.append(f"Unknown rain sensitivity for ({commodity}, {growth_stage}); used default 1.0")
        confidence -= 0.15
    if normal_rain == 3.0 and region.lower() not in _NORMAL_RAIN_IN:
        confidence -= 0.1
    if soil_moisture_pct is not None:
        # If dry forecast matches already-dry soil, extra confidence (worse outcome more likely)
        if rain_dev < -0.5 and soil_moisture_pct < 40:
            confidence = min(1.0, confidence + 0.1)
        if rain_dev > 0.5 and soil_moisture_pct > 70:
            confidence = min(1.0, confidence + 0.1)

    # Analog lookup
    pattern = _classify_pattern(rain_dev, temp_over)
    analogs = _ANALOG_YEARS.get((commodity, growth_stage, pattern), [])

    # Narrative reasoning
    reasoning_bits = []
    if abs(rain_dev) > 0.2:
        sign = 'above' if rain_dev > 0 else 'below'
        reasoning_bits.append(
            f"Rain forecast {forecast_rain_in_30d:.1f}in is {abs(rain_dev):.1f}in {sign} "
            f"normal {normal_rain:.1f}in for {region} month {forecast_month} "
            f"(sensitivity {rain_sens} bpa/in at {growth_stage}) → {rain_delta:+.2f} bpa"
        )
    if temp_over > 0:
        reasoning_bits.append(
            f"Avg temp {forecast_temp_f_avg_30d:.0f}F is {temp_over:.0f}F over stress threshold "
            f"(penalty {temp_pen} bpa/deg) → {temp_delta:+.2f} bpa"
        )
    if not reasoning_bits:
        reasoning_bits.append(f"Forecast near normal; no material adjustment")
    if analogs:
        reasoning_bits.append(
            f"Analogs ({pattern}): " + "; ".join(f"{y} {label}" for y, label in analogs)
        )

    return {
        'predicted_yield_bpa': round(predicted, 2),
        'delta_bpa': round(delta_bpa, 2),
        'confidence': round(confidence, 2),
        'reasoning': " | ".join(reasoning_bits),
        'analog_years': [y for y, _ in analogs],
        'warnings': warnings,
        'inputs': {
            'commodity': commodity,
            'region': region,
            'growth_stage': growth_stage,
            'current_yield_bpa': current_yield_bpa,
            'forecast_rain_in_30d': forecast_rain_in_30d,
            'forecast_temp_f_avg_30d': forecast_temp_f_avg_30d,
            'forecast_month': forecast_month,
            'soil_moisture_pct': soil_moisture_pct,
        },
    }


# =============================================================================
# Self-exploration: sweep inputs, report sensitivities and breakpoints.
# =============================================================================

def self_explore(
    commodity: str,
    region: str,
    growth_stage: str,
    current_yield_bpa: float,
    forecast_month: int,
    baseline_rain_in_30d: float = 3.0,
    baseline_temp_f: float = 82.0,
) -> dict:
    """
    Sweep the weather inputs around a baseline and report how yield responds.

    Output includes:
    - rain_sensitivity:  yield change per 1in rain change
    - temp_sensitivity:  yield change per 2F temp change above stress
    - breakpoints:       rain/temp values where delta crosses -2 bpa threshold
    - scenario_grid:     P10 / P50 / P90 rain × stress / hot temp scenarios
    """
    # Rain sweep: -3 to +3 in from baseline, step 0.5
    rain_sweep = []
    for offset in [-3, -2, -1, -0.5, 0, 0.5, 1, 2, 3]:
        r = baseline_rain_in_30d + offset
        out = run(commodity, region, growth_stage, current_yield_bpa,
                  r, baseline_temp_f, forecast_month)
        rain_sweep.append({'rain_in': r, 'delta_bpa': out['delta_bpa']})

    # Temp sweep: -4 to +8 F from baseline
    temp_sweep = []
    for offset in [-4, -2, 0, 2, 4, 6, 8, 10]:
        t = baseline_temp_f + offset
        out = run(commodity, region, growth_stage, current_yield_bpa,
                  baseline_rain_in_30d, t, forecast_month)
        temp_sweep.append({'temp_f': t, 'delta_bpa': out['delta_bpa']})

    # Sensitivities (local slope around baseline)
    r_hi = run(commodity, region, growth_stage, current_yield_bpa,
               baseline_rain_in_30d + 1, baseline_temp_f, forecast_month)['delta_bpa']
    r_lo = run(commodity, region, growth_stage, current_yield_bpa,
               baseline_rain_in_30d - 1, baseline_temp_f, forecast_month)['delta_bpa']
    rain_sensitivity = (r_hi - r_lo) / 2.0

    t_hi = run(commodity, region, growth_stage, current_yield_bpa,
               baseline_rain_in_30d, baseline_temp_f + 2, forecast_month)['delta_bpa']
    t_lo = run(commodity, region, growth_stage, current_yield_bpa,
               baseline_rain_in_30d, baseline_temp_f - 2, forecast_month)['delta_bpa']
    temp_sensitivity = (t_hi - t_lo) / 4.0  # per 1F

    # Breakpoints: find first rain offset where delta < -2 bpa
    dry_breakpoint = None
    for pt in rain_sweep:
        if pt['delta_bpa'] < -2.0:
            dry_breakpoint = pt['rain_in']
            break

    hot_breakpoint = None
    for pt in temp_sweep:
        if pt['delta_bpa'] < -2.0:
            hot_breakpoint = pt['temp_f']
            break

    # Scenario grid
    scenarios = {
        'P10_dry_hot': run(commodity, region, growth_stage, current_yield_bpa,
                           baseline_rain_in_30d - 2, baseline_temp_f + 4, forecast_month),
        'P50_normal':  run(commodity, region, growth_stage, current_yield_bpa,
                           baseline_rain_in_30d, baseline_temp_f, forecast_month),
        'P90_wet_cool': run(commodity, region, growth_stage, current_yield_bpa,
                            baseline_rain_in_30d + 2, baseline_temp_f - 2, forecast_month),
    }

    return {
        'mode': 'self_exploration',
        'baseline': {
            'commodity': commodity, 'region': region, 'growth_stage': growth_stage,
            'current_yield_bpa': current_yield_bpa, 'forecast_month': forecast_month,
            'baseline_rain_in_30d': baseline_rain_in_30d, 'baseline_temp_f': baseline_temp_f,
        },
        'rain_sensitivity_bpa_per_inch': round(rain_sensitivity, 2),
        'temp_sensitivity_bpa_per_deg_f': round(temp_sensitivity, 2),
        'dry_breakpoint_rain_in': dry_breakpoint,
        'hot_breakpoint_temp_f': hot_breakpoint,
        'rain_sweep': rain_sweep,
        'temp_sweep': temp_sweep,
        'scenarios': scenarios,
    }


if __name__ == '__main__':
    # Smoke test
    import json
    print("=== Scenario: IL corn, Sep, dry forecast ===")
    print(json.dumps(run(
        commodity='corn', region='us.corn_belt', growth_stage='grain_fill',
        current_yield_bpa=183.0, forecast_rain_in_30d=1.5,
        forecast_temp_f_avg_30d=84.0, forecast_month=9, soil_moisture_pct=38,
    ), indent=2))

    print("\n=== Self-exploration: IA corn pollination ===")
    r = self_explore(
        commodity='corn', region='us.corn_belt', growth_stage='pollination',
        current_yield_bpa=183.0, forecast_month=7,
    )
    print(f"Rain sensitivity: {r['rain_sensitivity_bpa_per_inch']} bpa/in")
    print(f"Temp sensitivity: {r['temp_sensitivity_bpa_per_deg_f']} bpa/degF")
    print(f"Dry breakpoint (delta<-2): rain={r['dry_breakpoint_rain_in']}in")
    print(f"Hot breakpoint (delta<-2): temp={r['hot_breakpoint_temp_f']}F")
