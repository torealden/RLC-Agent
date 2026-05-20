"""
HEFA economics — pure-function math, no I/O, single source of truth.

Parallel to crush_economics.py but for HEFA refining (feedstock_lb -> fuel_gal
via the BBD four-component price stack). Used by:

- src.kg.callables.implied_feedstock_value (the IFV kg_callable wrapper)
- src.agents.facility.buyer_agent  (for HEFA-side facilities)
- src.agents.strategic.strategic_agent (breakeven / scenario analysis)

All functions are pure: same inputs -> same outputs, no clock reads, no DB
calls, no logging. This is what makes both backtest and live mode reproducible.

Calibration anchors (from KG):
- `rd_price_stack.price_stack_decomposition`  (ctx 137)
    Base ULSD $2.50 + D4 $2.55 (1.7 x $1.50) + LCFS $0.50-0.75 + 45Z $0.65-1.00
    = effective selling price $5.50-6.50/gal
- `feedstock_sensitivity_rule.feedstock_cost_is_everything`  (ctx 132)
    1c/lb feedstock = $0.08/gal margin   (=> yield ~= 7.7 lb feedstock per gal)
- `base_case_margins.margin_scenarios_spread`  (ctx 140)
    IL RD base case $0.40-0.50/gal, CA $0.65-0.70/gal; worst-case ~$0
- `cfpc_45z.45z_extension_scenario`  (ctx 113)
    Four policy scenarios materially reshape the stack.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date


# ============================================================================
# Constants  (HEFA / fuel-type physical conversions)
# ============================================================================

# Energy density used for LCFS $/MT  -->  $/gal conversion.
#   LCFS credit value per gal = ($/MT credit) * (g CO2e/MJ avoided) * (MJ/gal) * 1e-6
# Values are the CARB-published regulatory energy densities used in the LCFS
# credit formula (NOT the BTU-derived values). RD and BD both sit in the
# diesel pool. Patched 2026-05-20 per IFVS spec ADR IFVS-010.
ENERGY_DENSITY_MJ_PER_GAL = {
    'renewable_diesel':           134.47,  # CARB LCFS diesel-pool regulatory
    'biodiesel':                  134.47,  # also in diesel pool per CARB
    'saf':                        130.8,
}

# LCFS baseline carbon intensity by fuel substituted (g CO2e/MJ).
# These are the published CARB diesel-pool and jet-pool baselines.
# 2026 values reflect CARB's declining-baseline schedule (drops ~1.5 g/MJ/yr).
# Patched 2026-05-20 per IFVS spec ADR IFVS-010.
LCFS_BASELINE_CI = {
    'renewable_diesel':            88.62,  # CARB diesel pool baseline, 2026
    'biodiesel':                   88.62,  # uses diesel baseline
    'saf':                         89.37,  # CARB jet baseline (illustrative)
}

# D4 equivalence value (RINs per gallon of fuel) per EPA RFS.
D4_EQUIVALENCE_PER_GAL = {
    'renewable_diesel':            1.7,
    'biodiesel':                   1.5,
    'saf':                         1.7,    # qualifies as BBD under current rules
}


# ============================================================================
# Inputs
# ============================================================================

@dataclass(frozen=True)
class HefaPriceStack:
    """Per-gallon revenue components — the four-leg BBD price stack.

    Note on CI scores: LCFS uses CARB-certified CI (CA-GREET, always includes
    ILUC). 45Z uses IRS 45ZCF-GREET CI which is similar but not identical
    (typically 5-15% more favorable). For approximation we let _45z_ci_score
    default to lcfs_pathway_ci_score; when 45ZCF-GREET data is wired we set
    them independently. The iluc_removed scenario is the policy-divergence
    case where 45Z drops ILUC but LCFS keeps it (KG ctx 127)."""
    base_refined_product_per_gal: float    # ULSD spot for RD/BD; jet for SAF
    d4_rin_price_per_rin:         float    # D4 RIN spot, $/RIN
    lcfs_credit_per_mt:           float    # CA LCFS credit, $/MT CO2e
    lcfs_pathway_ci_score:        float    # CARB-certified pathway CI, g/MJ
    cfpc_45z_per_gal:             float    # 45Z value, already CI-adjusted, $/gal
    _45z_ci_score:                float | None = None    # 45ZCF-GREET CI; None = use lcfs_pathway_ci_score


@dataclass(frozen=True)
class HefaCosts:
    """Per-gallon variable + fixed cost (everything EXCEPT feedstock)."""
    opex_per_gal:         float           # variable: H2, methanol, utilities, labor
    fixed_cost_per_gal:   float           # capex amortization + insurance + property
    target_margin_per_gal: float = 0.0    # required gross margin (mode='target_margin')


@dataclass(frozen=True)
class HefaParams:
    """HEFA process yields and conversion factors."""
    yield_lb_per_gal:        float = 7.7   # BBD-calibrated default for HEFA RD
    fuel_type:               str   = 'renewable_diesel'  # RD | BD | SAF


# ============================================================================
# Output breakdown — every line item visible for the weekly report / demo
# ============================================================================

@dataclass(frozen=True)
class HefaBreakdown:
    # Revenue side (per gal of fuel)
    base_refined_product_per_gal: float
    d4_rin_value_per_gal:         float
    lcfs_value_per_gal:           float
    cfpc_45z_value_per_gal:       float
    effective_selling_price_per_gal: float
    # Cost side (per gal, excluding feedstock)
    opex_per_gal:                 float
    fixed_cost_per_gal:           float
    target_margin_per_gal:        float
    net_available_for_feedstock_per_gal: float
    # Implied feedstock bid
    yield_lb_per_gal:             float
    implied_bid_per_lb:           float
    implied_bid_per_short_ton:    float


# ============================================================================
# Core HEFA primitives
# ============================================================================

def d4_rin_value_per_gal(d4_rin_price_per_rin: float, fuel_type: str) -> float:
    """
    Convert D4 spot to $/gal of refined fuel by multiplying by the equivalence
    value (RINs per gal): RD 1.7, BD 1.5, SAF 1.7.
    """
    if fuel_type not in D4_EQUIVALENCE_PER_GAL:
        raise ValueError(f"Unknown fuel_type for D4 equivalence: {fuel_type!r}")
    return d4_rin_price_per_rin * D4_EQUIVALENCE_PER_GAL[fuel_type]


def lcfs_value_per_gal(
    lcfs_credit_per_mt: float,
    lcfs_pathway_ci_score: float,
    fuel_type: str,
) -> float:
    """
    LCFS credit value per gallon of fuel:

        $/gal = ($/MT credit) * (baseline_CI - pathway_CI in g/MJ) * (MJ/gal) * 1e-6

    Negative CI advantage (pathway worse than baseline) yields negative LCFS value,
    which is the correct economic interpretation (this fuel generates a *deficit*).
    """
    if fuel_type not in ENERGY_DENSITY_MJ_PER_GAL:
        raise ValueError(f"Unknown fuel_type for LCFS conversion: {fuel_type!r}")
    baseline_ci = LCFS_BASELINE_CI[fuel_type]
    ci_advantage = baseline_ci - lcfs_pathway_ci_score        # g CO2e/MJ avoided
    mj_per_gal = ENERGY_DENSITY_MJ_PER_GAL[fuel_type]
    return (lcfs_credit_per_mt * ci_advantage * mj_per_gal) / 1_000_000


def effective_selling_price_per_gal(stack: HefaPriceStack, fuel_type: str) -> float:
    """Sum of the four BBD stack legs."""
    return (
        stack.base_refined_product_per_gal
        + d4_rin_value_per_gal(stack.d4_rin_price_per_rin, fuel_type)
        + lcfs_value_per_gal(stack.lcfs_credit_per_mt, stack.lcfs_pathway_ci_score, fuel_type)
        + stack.cfpc_45z_per_gal
    )


def implied_feedstock_bid_per_lb(
    stack: HefaPriceStack,
    costs: HefaCosts,
    params: HefaParams,
    return_breakdown: bool = False,
) -> float | tuple[float, HefaBreakdown]:
    """
    The BBD implied-bid math:

        net_available  =  effective_selling_price - opex - fixed - target_margin
        implied_bid    =  net_available / yield_lb_per_gal

    Returns $/lb (breakeven if target_margin=0; practical bid if target_margin>0).
    Negative result means the credit stack does not cover non-feedstock cost +
    required margin — sanity flag, not silently zero.
    """
    fuel_type = params.fuel_type
    d4_val   = d4_rin_value_per_gal(stack.d4_rin_price_per_rin, fuel_type)
    lcfs_val = lcfs_value_per_gal(stack.lcfs_credit_per_mt, stack.lcfs_pathway_ci_score, fuel_type)
    eff_sell = (
        stack.base_refined_product_per_gal
        + d4_val
        + lcfs_val
        + stack.cfpc_45z_per_gal
    )
    net_avail = (
        eff_sell
        - costs.opex_per_gal
        - costs.fixed_cost_per_gal
        - costs.target_margin_per_gal
    )
    implied_bid_per_lb_ = net_avail / params.yield_lb_per_gal

    if not return_breakdown:
        return implied_bid_per_lb_

    breakdown = HefaBreakdown(
        base_refined_product_per_gal     = stack.base_refined_product_per_gal,
        d4_rin_value_per_gal             = d4_val,
        lcfs_value_per_gal               = lcfs_val,
        cfpc_45z_value_per_gal           = stack.cfpc_45z_per_gal,
        effective_selling_price_per_gal  = eff_sell,
        opex_per_gal                     = costs.opex_per_gal,
        fixed_cost_per_gal               = costs.fixed_cost_per_gal,
        target_margin_per_gal            = costs.target_margin_per_gal,
        net_available_for_feedstock_per_gal = net_avail,
        yield_lb_per_gal                 = params.yield_lb_per_gal,
        implied_bid_per_lb               = implied_bid_per_lb_,
        implied_bid_per_short_ton        = implied_bid_per_lb_ * 2000.0,
    )
    return implied_bid_per_lb_, breakdown


def producer_margin_per_gal(
    stack: HefaPriceStack,
    costs: HefaCosts,
    params: HefaParams,
    observed_feedstock_per_lb: float,
) -> float:
    """
    The other direction: given an observed cash feedstock price, what gross margin
    is the HEFA refiner actually making per gallon of fuel?

        margin = effective_selling_price
                 - feedstock_cost_per_gal
                 - opex - fixed
        where feedstock_cost_per_gal = observed_feedstock_per_lb * yield_lb_per_gal
    """
    feedstock_cost = observed_feedstock_per_lb * params.yield_lb_per_gal
    eff_sell = effective_selling_price_per_gal(stack, params.fuel_type)
    return eff_sell - feedstock_cost - costs.opex_per_gal - costs.fixed_cost_per_gal


# ============================================================================
# 45Z formula (per IRS proposed guidance) + policy-scenario logic
# ============================================================================
#
# 45Z is a CI-based credit that uses the formula:
#
#     45Z $/gal = base_credit_per_gal * max(0, (50 - CI) / 50)
#
# where base_credit assumes prevailing-wage compliance:
#     RD / BD: $1.00 / gal  (2025 base; adjusts with inflation)
#     SAF    : $1.75 / gal
#
# The "50" threshold is a fixed value in IRS guidance — fuels with CI >= 50
# get no credit. Note this is DIFFERENT from the LCFS baseline CI (95.61 for
# diesel pool, 89.37 for jet pool) used in the LCFS math.
#
# Worked example (Tore-confirmed 2026-05-18):
#   Tallow CI = 30  ->  RD 45Z  = $1.00 * (50-30)/50 = $0.40/gal
#                       SAF 45Z = $1.75 * (50-30)/50 = $0.70/gal
#
# Feedstock categories (must match silver.bbd_feedstock_dim.feedstock_category):
#   waste_animal_fat:   tallow, choice white grease, poultry fat
#   waste_oil:          UCO, brown grease, DCO (distillers corn oil)
#   crop_oil:           soybean oil, canola oil, corn oil, sunflower oil
#   palm_derivative:    PFAD, palm fatty acid

# Base credit per gallon at 100% emissions reduction (CI=0), 2025 IRS guidance
# with prevailing-wage compliance. Adjusts annually with inflation in production.
_45Z_BASE_CREDIT_PER_GAL = {
    'renewable_diesel': 1.00,
    'biodiesel':        1.00,
    'saf':              1.75,
}

_45Z_CI_THRESHOLD = 50.0    # IRS guidance: fuels with CI >= 50 g/MJ get no credit

# ILUC penalty by feedstock_category — used by the iluc_removed scenario which
# recomputes CI without the indirect-land-use-change component. KG ctx 127
# notes that ILUC removal narrows the differentiation between waste and crop
# feedstocks substantially.
_ILUC_PENALTY_BY_CATEGORY = {
    'waste_animal_fat':  0.0,        # no ILUC penalty for waste
    'waste_oil':         0.0,        # no ILUC penalty for waste
    'crop_oil':         22.0,        # typical ILUC ~ 20-25 g/MJ for soybean oil
    'palm_derivative':  35.0,        # palm ILUC is massive
}


def cfpc_45z_value_per_gal(
    fuel_type: str,
    feedstock_category: str,
    pathway_ci_score: float,
    policy_scenario: str,
    as_of_date: date,
    is_domestic_feedstock: bool = True,
    base_credit_per_gal: float | None = None,
) -> float:
    """
    Compute 45Z value per gallon as base_credit * max(0, (50 - CI) / 50),
    with policy_scenario adjusting either the credit or the effective CI.

    Parameters
    ----------
    fuel_type : 'renewable_diesel' | 'biodiesel' | 'saf'
    feedstock_category : 'waste_animal_fat' | 'waste_oil' | 'crop_oil' | 'palm_derivative'
    pathway_ci_score : g CO2e/MJ for this (fuel, feedstock, region) pathway
    policy_scenario  : extension_2031 | expiry_2027 | iluc_removed |
        domestic_restriction | none
    as_of_date : date of the computation; matters for expiry_2027 cliff
    is_domestic_feedstock : matters for domestic_restriction scenario
    base_credit_per_gal : override base credit if explicit (e.g. for inflation
        adjustment in future years). Defaults to 2025 IRS values.

    Returns
    -------
    float : 45Z value in $/gal of refined fuel
    """
    if policy_scenario == 'none':
        return 0.0
    if policy_scenario == 'expiry_2027' and as_of_date > date(2027, 12, 31):
        return 0.0
    if policy_scenario == 'domestic_restriction' and not is_domestic_feedstock:
        return 0.0

    base = base_credit_per_gal if base_credit_per_gal is not None else _45Z_BASE_CREDIT_PER_GAL.get(fuel_type, 0.0)

    # Effective CI for the 45Z formula. Under iluc_removed, subtract the ILUC
    # penalty for this category (LCFS still uses the ILUC-inclusive CI — that
    # asymmetry is KG ctx 127's structural insight).
    effective_ci = pathway_ci_score
    if policy_scenario == 'iluc_removed':
        effective_ci = max(0.0, pathway_ci_score - _ILUC_PENALTY_BY_CATEGORY.get(feedstock_category, 0.0))

    emissions_factor = max(0.0, (_45Z_CI_THRESHOLD - effective_ci) / _45Z_CI_THRESHOLD)
    return base * emissions_factor


# ============================================================================
# Calibration defaults (BBD-anchored)
# ============================================================================

# Per BBD calibration: HEFA OPEX (variable) ≈ $0.30-0.50/gal; fixed ≈ $0.10/gal.
HEFA_RD_DEFAULT_COSTS = HefaCosts(
    opex_per_gal       = 0.40,
    fixed_cost_per_gal = 0.10,
)

HEFA_SAF_DEFAULT_COSTS = HefaCosts(
    opex_per_gal       = 1.10,    # SAF OPEX higher per BBD calibration
    fixed_cost_per_gal = 0.15,
)

HEFA_BD_DEFAULT_COSTS = HefaCosts(
    opex_per_gal       = 0.40,
    fixed_cost_per_gal = 0.10,
)

# Yield defaults (lb feedstock per gal fuel). All HEFA-class.
HEFA_RD_PARAMS  = HefaParams(yield_lb_per_gal=7.7, fuel_type='renewable_diesel')
HEFA_SAF_PARAMS = HefaParams(yield_lb_per_gal=7.7, fuel_type='saf')
HEFA_BD_PARAMS  = HefaParams(yield_lb_per_gal=7.5, fuel_type='biodiesel')
